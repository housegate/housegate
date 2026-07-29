package proxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/housegate/housegate/pkg/log"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/plugin"
)

// PacketObserver is the narrow, wire-level metrics surface the Relay
// calls at codec boundaries. It is deliberately separate from the
// plugin-chain Hooks: per-packet and per-byte events fire at 10^4-10^6
// Hz and must not go through a variadic plugin dispatch. Hooks express
// semantic events ("a query passed all plugins"); PacketObserver
// expresses wire-level counters ("a Data packet was read").
//
// *MetricsObserver satisfies this interface; the interface lives here
// so Relay can be unit-tested with a tiny fake.
type PacketObserver interface {
	ClientPacket(pktType string)
	ServerPacket(pktType string)
	BytesTransferred(direction string, bytes float64)
	StreamingDataBlock(mode string)
}

// Relay drives the bidirectional packet loop between a client and its
// bound upstream. It is constructed once per session and never shared.
//
// Design: docs/superpowers/specs/2026-04-21-clickhouse-tcp-conn-interface-design.md
type Relay struct {
	sess   chsession.Session
	hooks  plugin.Hooks
	obs    PacketObserver
	dialer UpstreamDialer
}

// UpstreamDialer dials and returns the codec for this session's upstream.
// Called inside Relay.handshake AFTER OnHello so plugins (notably
// routeplugin.Stripper) get to populate SessionState — without that
// ordering the dialer would always see RouteTarget="" and routed
// sessions would silently fall through to cfg.Upstream.
type UpstreamDialer func(ctx context.Context, sess chsession.Session) (*chproto.Codec, error)

// NewRelay constructs a Relay.
//
// dialer is the post-OnHello upstream dialer. When dialer is nil the
// caller is responsible for having pre-bound an upstream via
// sess.BindUpstream — used by tests that exercise the relay loop in
// isolation. obs may be nil, in which case per-packet and per-byte
// metrics are not emitted.
func NewRelay(sess chsession.Session, hooks plugin.Hooks, obs PacketObserver, dialer UpstreamDialer) *Relay {
	return &Relay{sess: sess, hooks: hooks, obs: obs, dialer: dialer}
}

// handshake performs the three-phase ClickHouse handshake:
//  1. Read the client's ClientHello; run OnHello hooks (which may inject
//     credentials, strip a __route__ prefix, etc.).
//  2. Forward the possibly-mutated hello to the upstream; read its
//     ServerHello; echo the ServerHello back to the client.
//  3. If the negotiated revision supports it, run addendum negotiation on
//     both sides.
//
// On completion, Codec revisions are set and SessionState is populated.
func (r *Relay) handshake(ctx context.Context) error {
	ctx, logger := log.FromContext(ctx)
	logger.Debugw("chsession starting handshake")
	start := time.Now()
	client := r.sess.Client()

	pkt, err := client.ReadPacket(uint64(chproto.ClientHelloCode))
	if err != nil {
		return fmt.Errorf("read client hello: %w", err)
	}
	hello, ok := pkt.Decoded.(*chproto.ClientHello)
	if !ok {
		// Per spec D8, fail-open on decode errors — but for Hello we cannot
		// proceed without valid fields, so close.
		return fmt.Errorf("client hello: %w", chproto.ErrDecode)
	}

	if err := r.hooks.OnHello(ctx, r.sess, hello); err != nil {
		return fmt.Errorf("on-hello: %w", err)
	}

	// Dial the upstream NOW, after OnHello has run. This is the only
	// point where SessionState reflects the post-Stripper RouteTarget,
	// so dialers that branch on it (production server-mode dialer
	// honours RouteTarget for routed sessions) see the right value.
	// Tests that pre-bind via sess.BindUpstream pass dialer=nil and
	// reuse the existing binding.
	upstream := r.sess.Upstream()
	if upstream == nil {
		if r.dialer == nil {
			return chsession.ErrNoUpstream
		}
		up, dialErr := r.dialer(ctx, r.sess)
		if dialErr != nil {
			return fmt.Errorf("dial upstream: %w", dialErr)
		}
		if err := r.sess.BindUpstream(ctx, up); err != nil {
			if closer, ok := up.Conn().(interface{ Close() error }); ok {
				_ = closer.Close()
			}
			return fmt.Errorf("bind upstream: %w", err)
		}
		upstream = up
	}

	state := r.sess.State()
	state.ClientHostname = hello.Name
	state.ClientVersion = chsession.VersionTriple{
		Major: int(hello.Major),
		Minor: int(hello.Minor),
		Patch: 0,
	}
	state.Database = hello.Database
	if state.MappedUser == "" { // plugins may have set it in OnHello
		state.MappedUser = hello.User
	}
	if state.MappedPassword == "" {
		state.MappedPassword = hello.Password
	}

	// When the forward plugin pivoted the session to a peer's internal-port
	// (IsForwarding=true), RebindToPeer already completed the full upstream
	// hello exchange (write ClientHello → read ServerHello → send addendum).
	// relay.handshake must NOT repeat those steps: the peer codec has advanced
	// past the hello phase and any re-write would be interpreted as a malformed
	// packet. Instead, use the raw ServerHello bytes and negotiated revision
	// stored by RebindToPeer, echo them to the client, and if the revision
	// supports addendum, drive only the client-side addendum negotiation
	// (upstream already received its addendum inside RebindToPeer).
	if state.IsForwarding {
		raw := state.PeerServerHelloRaw
		rev := state.PeerRevision
		client.SetRevision(rev)
		upstream.SetRevision(rev)
		state.ClientRevision = rev
		logger.Debugw("forwarding: using peer server hello from RebindToPeer",
			"upstream", upstreamAddr(upstream),
			"rev", rev, "raw_len", len(raw))
		if _, err := client.Conn().Write(raw); err != nil {
			return fmt.Errorf("forwarding: echo peer server hello: %w", err)
		}
		if chproto.SupportsAddendum(rev) {
			res, err := client.NegotiateAddendum(chproto.AddendumOpts{
				ProposedRecv: "notchunked",
				ProposedSend: "notchunked",
			})
			if err != nil {
				return fmt.Errorf("forwarding: client addendum: %w", err)
			}
			state.ChunkedRecv = res.NegotiatedRecv
			state.ChunkedSend = res.NegotiatedSend
			// Upstream addendum was already sent by RebindToPeer; do not repeat.
		}
	} else {
		if err := upstream.WriteClientHello(hello); err != nil {
			return fmt.Errorf("forward client hello: %w", err)
		}
		upstream.SetServerHelloRevisionHint(int(hello.ProtocolVersion))
		logger.Debugw("client hello processed and forwarded to upstream",
			"upstream", upstreamAddr(upstream),
			"client_hostname", state.ClientHostname,
			"client_version", state.ClientVersion,
			"database", state.Database,
			"mapped_user", state.MappedUser,
		)

		srvPkt, err := upstream.ReadPacket(uint64(chproto.ServerHelloCode), uint64(chproto.ServerExceptionCode))
		if err != nil {
			return fmt.Errorf("read server hello: %w", err)
		}
		// Upstream may answer ClientHello with a ServerException instead —
		// e.g. invalid credentials forwarded by routing / peer-trust paths,
		// or a misconfigured peer port that lands on plain CH which doesn't
		// recognise our envelope user. Decode the Exception and surface its
		// message so operators don't see an opaque "decode failed".
		if exc, ok := srvPkt.Decoded.(*chproto.Exception); ok {
			return fmt.Errorf("upstream rejected handshake: code=%d %s: %s", exc.Code, exc.Name, exc.Message)
		}
		logger.Debugw("received server hello from upstream")
		srv, ok := srvPkt.Decoded.(*chproto.ServerHello)
		if !ok {
			return fmt.Errorf("server hello: unexpected packet type=%d (want ServerHello=%d): %w",
				srvPkt.Type, chproto.ServerHelloCode, chproto.ErrDecode)
		}

		rev := int(hello.ProtocolVersion)
		if int(srv.Revision) < rev {
			rev = int(srv.Revision)
		}
		client.SetRevision(rev)
		upstream.SetRevision(rev)
		state.ClientRevision = rev
		state.ServerDisplayName = srv.DisplayName
		state.Timezone = srv.Timezone
		logger.Debugw("server hello processed",
			"server_display_name", state.ServerDisplayName,
			"timezone", state.Timezone,
		)

		// Echo the raw ServerHello bytes — NOT a re-encode via WriteServerHello.
		// ch-go's ServerHello struct stops at Patch (FeatureVersionPatch). Modern
		// ClickHouse (26.x) appends chunked-protocol, parallel-replicas, password-
		// rules, nonce, and other trailing fields that EncodeAware would silently
		// drop. If the client receives a truncated ServerHello it hangs in its own
		// receiveHello() and never sends the addendum, deadlocking NegotiateAddendum
		// below. srvPkt.Raw carries the exact bytes upstream wrote to the wire,
		// trailing fields included (see Codec.ReadPacket for the capture contract).
		if _, err := client.Conn().Write(srvPkt.Raw); err != nil {
			return fmt.Errorf("echo server hello: %w", err)
		}
		logger.Debugw("server hello echoed to client", "bytes", len(srvPkt.Raw))

		if chproto.SupportsAddendum(rev) {
			// Force the negotiated chunked mode to "notchunked" on both directions.
			// The codec does not yet wrap reads/writes in ChunkedReader/Writer, so
			// any chunked framing in the packet loop would be parsed as malformed
			// packets. The server-side advertisement in ServerHello is already
			// rewritten to "notchunked" on its way to the client (see
			// rewriteServerHelloChunkedToNotChunked), and this matching proposal
			// ensures the addendum we forward to upstream also lands on
			// "notchunked" regardless of what the client itself proposed — because
			// resolveChunkedMode with any "notchunked" operand returns "notchunked".
			//
			// TODO: switch back to "chunked_optional" once the codec supports
			// chunked framing end-to-end.
			res, err := client.NegotiateAddendum(chproto.AddendumOpts{
				ProposedRecv: "notchunked",
				ProposedSend: "notchunked",
			})
			if err != nil {
				return fmt.Errorf("client addendum: %w", err)
			}
			state.ChunkedRecv = res.NegotiatedRecv
			state.ChunkedSend = res.NegotiatedSend
			if err := upstream.SendAddendum(res); err != nil {
				return fmt.Errorf("upstream addendum: %w", err)
			}
		}
	}

	elapsed := time.Since(start)
	logger.Infow("chsession handshake complete",
		"user", state.MappedUser,
		"database", state.Database,
		"upstream", upstreamAddr(upstream),
		"forwarding", state.IsForwarding,
		"revision", state.ClientRevision,
		"elapsed", elapsed,
	)
	r.hooks.OnHandshakeComplete(ctx, r.sess, elapsed)
	return nil
}

// Run performs the handshake, then drives two goroutines: client→upstream
// and upstream→client. Returns when either side terminates. Session is
// closed exactly once via defer.
func (r *Relay) Run(ctx context.Context) error {
	if err := r.handshake(ctx); err != nil {
		_ = r.sess.Close()
		r.hooks.OnClose(r.sess)
		return err
	}
	defer func() {
		_ = r.sess.Close()
		r.hooks.OnClose(r.sess)
	}()

	errCh := make(chan error, 2)
	go func() { errCh <- r.clientToUpstream(ctx) }()
	go func() { errCh <- r.upstreamToClient(ctx) }()

	first := <-errCh
	_ = r.sess.Close() // nudge the second goroutine to exit
	<-errCh
	if errors.Is(first, io.EOF) || first == nil {
		return nil
	}
	return first
}

// clientToUpstream reads client packets, decoding Query for the OnQuery
// plugin chain; everything else is spliced through unchanged.
func (r *Relay) clientToUpstream(ctx context.Context) error {
	client := r.sess.Client()
	// curQctx is the QueryContext of the most recent successfully
	// forwarded Query — Data packets that follow belong to it. It is the
	// context handed to OnClientData; nil means "no query owns the data"
	// (rejected query, or data before any query) and the hook is skipped.
	var (
		curQctx                *plugin.QueryContext
		curQctxSawInitialEmpty bool
		curQctxSawPayload      bool
		rejectedQctx           *plugin.QueryContext
	)
	for {
		up := r.sess.Upstream() // atomic: picks up rebinds (future C3)
		if up == nil {
			return chsession.ErrNoUpstream
		}

		limitQctx := curQctx
		if limitQctx == nil {
			limitQctx = rejectedQctx
		}
		limit, enforceLimit := r.hooks.ClientDataReadLimit(limitQctx)
		var (
			pkt    *chproto.Packet
			decErr error
		)
		if enforceLimit {
			pkt, decErr = client.ReadPacketWithDataLimit(limit, uint64(chproto.ClientQueryCode))
		} else {
			pkt, decErr = client.ReadPacket(uint64(chproto.ClientQueryCode))
		}
		if errors.Is(decErr, chproto.ErrPacketTooLarge) {
			err := fmt.Errorf("client Data packet exceeds remaining payload limit %d: %w", limit, decErr)
			r.writeExceptionToClient(ctx, err)
			if limitQctx != nil {
				r.hooks.OnQueryAbort(ctx, limitQctx)
				r.hooks.OnQueryComplete(ctx, r.sess)
			}
			return err
		}
		if errors.Is(decErr, io.EOF) {
			return io.EOF
		}
		if pkt == nil {
			return decErr
		}
		if decErr != nil && !errors.Is(decErr, chproto.ErrDecode) {
			return decErr
		}
		if errors.Is(decErr, chproto.ErrDecode) && pkt.Type == uint64(chproto.ClientQueryCode) && r.hooks.RejectUndecodableQuery(r.sess) {
			err := fmt.Errorf("strict query decode: %w", decErr)
			r.writeExceptionToClient(ctx, err)
			return err
		}

		// Wire-level counters. Emit for every successful ReadPacket
		// regardless of whether the plugin chain later rejects the query
		// — these counters reflect what came off the socket, not what was
		// forwarded.
		if r.obs != nil {
			r.obs.ClientPacket(clientPacketName(pkt.Type))
			if pkt.RawLen > 0 {
				r.obs.BytesTransferred("client_to_upstream", float64(pkt.RawLen))
			}
			if pkt.Type == uint64(chproto.ClientDataCode) || pkt.Type == uint64(chproto.ClientScalar) {
				r.obs.StreamingDataBlock(compressionMode(client.Compression()))
			}
		}

		_, logger := log.FromContext(ctx)

		if decErr == nil && pkt.Decoded != nil && pkt.Type == uint64(chproto.ClientQueryCode) {
			q := pkt.Decoded.(*chproto.Query)
			clientCompression := q.Compression
			// ClientData framing is declared by the Query packet even when the
			// query is rejected and its remaining input must only be drained.
			client.SetCompression(clientCompression)
			qctx := &plugin.QueryContext{
				Session:     r.sess,
				OriginalSQL: q.Body,
				Query:       q,
				Values:      make(map[string]any),
			}
			logger.Debugw("client query received",
				"query_id", q.ID,
				"sql_len", len(q.Body),
				"settings", len(q.Settings),
			)
			if curQctx != nil {
				err := fmt.Errorf("client sent query %q before completing input for query %q", q.ID, curQctx.Query.ID)
				r.writeExceptionToClient(ctx, err)
				r.hooks.OnQueryAbort(ctx, curQctx)
				r.hooks.OnQueryComplete(ctx, r.sess)
				return err
			}
			if rejectedQctx != nil {
				err := fmt.Errorf("client sent query %q before completing rejected input for query %q", q.ID, rejectedQctx.Query.ID)
				r.writeExceptionToClient(ctx, err)
				return err
			}
			if err := r.hooks.OnQuery(ctx, qctx); err != nil {
				r.writeExceptionToClient(ctx, err)
				// Chain rejected the query — its lifecycle ends here.
				r.hooks.OnQueryAbort(ctx, qctx)
				r.hooks.OnQueryComplete(ctx, r.sess)
				rejectedQctx = qctx
				continue
			}
			// AbortWithSuccess: a plugin (commitgate via ErrAbortWithSuccess)
			// handled the statement out-of-band and signalled the relay to
			// reply success without contacting upstream. Synthesize a
			// single-byte EndOfStream packet, fire OnQueryComplete to release
			// per-query state, and skip forwarding.
			if qctx.AbortWithSuccess {
				r.hooks.OnQueryAbort(ctx, qctx)
				if err := writeEndOfStreamToClient(client.Conn()); err != nil {
					// Transport-level failure on the client socket; treat
					// the same as any other unrecoverable client write —
					// the connection is dead. Match the existing pattern
					// in this loop (return wrapped error).
					r.hooks.OnQueryComplete(ctx, r.sess)
					return fmt.Errorf("write end-of-stream: %w", err)
				}
				r.hooks.OnQueryComplete(ctx, r.sess)
				rejectedQctx = qctx
				continue
			}
			// Re-fetch upstream after OnQuery: a USE-triggered pivot
			// (forward plugin's OnQuery) calls RebindToPeer which atomically
			// swaps the upstream codec to the peer and closes the old one.
			// Using the stale `up` captured at the top of the loop would
			// write to a closed connection — always refresh here.
			up = r.sess.Upstream()
			if up == nil {
				r.hooks.OnQueryAbort(ctx, qctx)
				r.hooks.OnQueryComplete(ctx, r.sess)
				return chsession.ErrNoUpstream
			}
			// Raw ClientData is not transcoded, so plugins cannot change the
			// compression mode declared by the client Query.
			qctx.Query.Compression = clientCompression
			up.SetCompression(clientCompression)
			if err := up.WriteQuery(qctx.Query); err != nil {
				// Forwarding failed — fire OnQueryComplete so per-query
				// resources held by plugins are released before we tear down.
				r.hooks.OnQueryAbort(ctx, qctx)
				r.hooks.OnQueryComplete(ctx, r.sess)
				return fmt.Errorf("forward query to %s: %w", upstreamAddr(up), err)
			}
			// Do NOT proactively write an empty-Data terminator here.
			// Both real CH clients and inter-server Connection emit one
			// inline; injecting our own duplicates the marker, and CH's
			// runImpl post-executeQuery main loop treats the second
			// empty Data as an unexpected packet and drops the
			// connection (clickhouse-client then reconnects between
			// every query). The previously observed CH-B hang on
			// remote() was actually caused by SQL_x_auth_token going
			// out without Custom=true + Field-dump quoting, which
			// failed setting application before executeQuery — fixed in
			// the route/agent Signer.
			logger.Debugw("query forwarded to upstream",
				"query_id", q.ID,
				"upstream", upstreamAddr(up),
			)
			curQctx = qctx
			curQctxSawInitialEmpty = false
			curQctxSawPayload = false
			continue
		}

		// Data packets belong to the most recent forwarded query. Strict
		// capture hooks run before splice and may fail closed; legacy
		// DataPlugins remain fail-open observers.
		inputComplete := false
		if pkt.Type == uint64(chproto.ClientDataCode) {
			var err error
			inputComplete, err = chproto.ClientDataPacketIsEmpty(pkt.Raw, client.Compression())
			if err != nil {
				if curQctx != nil {
					r.hooks.OnQueryAbort(ctx, curQctx)
					r.hooks.OnQueryComplete(ctx, r.sess)
				}
				return fmt.Errorf("classify client data packet: %w", err)
			}
			if rejectedQctx != nil {
				if inputComplete {
					rejectedQctx = nil
				}
				continue
			}
			if curQctx == nil {
				return fmt.Errorf("client Data packet has no active query")
			}
			deferInputComplete := inputComplete &&
				queryMayStreamClientData(curQctx) &&
				!curQctxSawPayload &&
				!curQctxSawInitialEmpty
			if deferInputComplete {
				curQctxSawInitialEmpty = true
				inputComplete = false
			} else {
				if !inputComplete {
					curQctxSawPayload = true
				}
				if err := r.hooks.OnClientDataStrict(ctx, curQctx, pkt.Raw); err != nil {
					r.writeExceptionToClient(ctx, err)
					r.hooks.OnQueryAbort(ctx, curQctx)
					r.hooks.OnQueryComplete(ctx, r.sess)
					return fmt.Errorf("client data strict hook: %w", err)
				}
				if err := r.hooks.OnClientData(ctx, curQctx, pkt.Raw); err != nil {
					logger.Warnw("client data hook failed (fail-open)",
						"raw_len", pkt.RawLen,
						"err", err,
					)
				}
			}
		}

		// At the terminating empty Data block, run the strict end-of-input chain
		// before any commit boundary. An error here is fail-closed: the normal
		// path does not forward the terminator, and the staged-input path never
		// lets a captured payload reach ordinary upstream. On success, both paths
		// continue to the ordinary splice below for the terminator; for staged
		// input this closes the upstream's sample-block negotiation with zero
		// ordinary rows, and the upstream EndOfStream remains the client-visible
		// success.
		if inputComplete && curQctx != nil {
			if err := r.hooks.OnQueryInputCompleteStrict(ctx, curQctx); err != nil {
				r.writeExceptionToClient(ctx, err)
				r.hooks.OnQueryAbort(ctx, curQctx)
				r.hooks.OnQueryComplete(ctx, r.sess)
				return fmt.Errorf("query input complete strict hook: %w", err)
			}
		}

		if pkt.Type == uint64(chproto.ClientDataCode) && curQctx != nil && curQctx.SuppressUpstreamExecution {
			if inputComplete {
				logger.Debugw("staged client data terminator forwarded to upstream",
					"query_id", curQctx.Query.ID,
					"raw_len", pkt.RawLen,
				)
			} else {
				logger.Debugw("staged client data payload suppressed",
					"query_id", curQctx.Query.ID,
					"raw_len", pkt.RawLen,
				)
				continue
			}
		}

		// Splice any non-decoded / decode-failed packet.
		logger.Debugw("client packet spliced to upstream",
			"type", pkt.Type,
			"name", clientPacketName(pkt.Type),
			"raw_len", pkt.RawLen,
		)
		if err := client.Splice(up.Conn(), pkt); err != nil {
			return fmt.Errorf("splice client→upstream type=%d: %w", pkt.Type, err)
		}
		if inputComplete {
			r.hooks.OnQueryInputComplete(ctx, curQctx)
			curQctx = nil
			curQctxSawInitialEmpty = false
			curQctxSawPayload = false
		}
	}
}

func queryMayStreamClientData(qctx *plugin.QueryContext) bool {
	if qctx == nil || qctx.Query == nil {
		return false
	}
	source, ok := insertSourceKeyword(qctx.Query.Body)
	if !ok {
		return false
	}
	switch source {
	case "VALUES", "SELECT", "WITH":
		return false
	default:
		return true
	}
}

func insertSourceKeyword(sql string) (string, bool) {
	tok, pos, ok := nextSQLToken(sql, 0)
	if !ok || tok.kind != sqlTokenWord || tok.text != "INSERT" {
		return "", false
	}

	depth := 0
	for {
		tok, next, ok := nextSQLToken(sql, pos)
		if !ok {
			return "", true
		}
		pos = next
		switch tok.kind {
		case sqlTokenLParen:
			depth++
		case sqlTokenRParen:
			if depth > 0 {
				depth--
			}
		case sqlTokenWord:
			if depth != 0 {
				continue
			}
			switch tok.text {
			case "FORMAT", "VALUES", "SELECT", "WITH":
				return tok.text, true
			}
		}
	}
}

type sqlTokenKind int

const (
	sqlTokenOther sqlTokenKind = iota
	sqlTokenWord
	sqlTokenLParen
	sqlTokenRParen
)

type sqlToken struct {
	kind sqlTokenKind
	text string
}

func nextSQLToken(sql string, pos int) (sqlToken, int, bool) {
	for {
		pos = skipSQLSpaceAndComments(sql, pos)
		if pos >= len(sql) {
			return sqlToken{}, pos, false
		}
		switch sql[pos] {
		case '\'', '"', '`':
			pos = skipSQLQuoted(sql, pos)
			continue
		}
		break
	}

	switch sql[pos] {
	case '(':
		return sqlToken{kind: sqlTokenLParen}, pos + 1, true
	case ')':
		return sqlToken{kind: sqlTokenRParen}, pos + 1, true
	}
	if isSQLIdentStart(sql[pos]) {
		start := pos
		pos++
		for pos < len(sql) && isSQLIdentPart(sql[pos]) {
			pos++
		}
		return sqlToken{
			kind: sqlTokenWord,
			text: strings.ToUpper(sql[start:pos]),
		}, pos, true
	}
	return sqlToken{kind: sqlTokenOther}, pos + 1, true
}

func skipSQLSpaceAndComments(sql string, pos int) int {
	for pos < len(sql) {
		switch {
		case sql[pos] == ' ' || sql[pos] == '\t' || sql[pos] == '\n' || sql[pos] == '\r' || sql[pos] == '\f':
			pos++
		case sql[pos] == '#':
			pos = skipSQLLineComment(sql, pos+1)
		case pos+1 < len(sql) && sql[pos] == '-' && sql[pos+1] == '-':
			pos = skipSQLLineComment(sql, pos+2)
		case pos+1 < len(sql) && sql[pos] == '/' && sql[pos+1] == '*':
			pos += 2
			for pos+1 < len(sql) && !(sql[pos] == '*' && sql[pos+1] == '/') {
				pos++
			}
			if pos+1 < len(sql) {
				pos += 2
			}
		default:
			return pos
		}
	}
	return pos
}

func skipSQLLineComment(sql string, pos int) int {
	for pos < len(sql) && sql[pos] != '\n' && sql[pos] != '\r' {
		pos++
	}
	return pos
}

func skipSQLQuoted(sql string, pos int) int {
	quote := sql[pos]
	pos++
	for pos < len(sql) {
		if sql[pos] == '\\' {
			pos += 2
			continue
		}
		if sql[pos] == quote {
			pos++
			if pos < len(sql) && sql[pos] == quote {
				pos++
				continue
			}
			return pos
		}
		pos++
	}
	return pos
}

func isSQLIdentStart(c byte) bool {
	return c == '_' || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z'
}

func isSQLIdentPart(c byte) bool {
	return isSQLIdentStart(c) || c >= '0' && c <= '9'
}

// upstreamToClient streams upstream bytes straight through to the client.
//
// Design note: we deliberately do NOT parse packets on this path. The
// previous packet-by-packet implementation kept hitting forward-compat
// gaps — ch-go's decoders for Progress / Profile / TableColumns /
// ProfileEvents / etc. lag the real ClickHouse wire format, and any single
// decoder that under-reads its body leaves a stray byte that the next
// ReadPacket interprets as a bogus packet type (e.g. "unknown server
// type=0" after a short-decoded Progress). Legacy [pkg/proxy/proxy.go]'s
// copyUpstreamToClientFromReader has the same "just copy bytes" structure
// for the same reason — its own comment calls out that precise per-packet
// detection is best-effort in this direction.
//
// Trade-off: we can only fire OnQueryComplete (and clear ActiveRewrite) via
// a first-byte heuristic on each read chunk. ClickHouse's TCP handler
// typically flushes EndOfStream / Exception as its own socket write, so in
// practice the chunk starts with 0x05 / 0x02 and the heuristic fires
// correctly. If a chunk starts mid-packet (e.g. a large Data block that
// spilled across reads) we'll miss the boundary for that query; plugins
// needing strict per-query cleanup should either live with that or move to
// a chunked-protocol implementation that gives explicit frame boundaries.
//
// Exception decoding is best-effort: when the first byte of a chunk is
// ServerExceptionCode (0x02) we decode the body from the buffered bytes
// and fire OnException before OnQueryComplete. If the chunk starts
// mid-packet the decode fails and the hook is silently skipped — see
// tryDecodeException for the contract. Plugins that depend on this for
// state rollback (e.g. commitgate) must accept best-effort delivery and
// pair their state mutations with idempotent re-application logic.
func (r *Relay) upstreamToClient(ctx context.Context) error {
	client := r.sess.Client()
	buf := make([]byte, 64*1024)
	for {
		up := r.sess.Upstream()
		if up == nil {
			return chsession.ErrNoUpstream
		}

		n, err := up.ReadRaw(buf)
		if n > 0 {
			// First-byte heuristic for query-boundary detection AND per-
			// type server-packet counting. Only meaningful if this chunk
			// starts at a packet boundary, which is the usual case because
			// ClickHouse flushes these small trailers as their own socket
			// write. Large Data blocks can span reads, in which case the
			// first byte is mid-packet payload — detectServerPacketType
			// returns "unknown" for those and we simply don't emit.
			first := buf[0]
			isBoundary := first == byte(chproto.ServerEndOfStreamCode) ||
				first == byte(chproto.ServerExceptionCode)

			_, logger := log.FromContext(ctx)
			logger.Debugw("upstream chunk relayed to client",
				"bytes", n,
				"first_byte_type", detectServerPacketType(buf[:n]),
				"boundary", isBoundary,
			)

			if r.obs != nil {
				r.obs.BytesTransferred("upstream_to_client", float64(n))
				if name := detectServerPacketType(buf[:n]); name != "unknown" {
					r.obs.ServerPacket(name)
				}
			}

			// Exception chunks take a special path: decode → run
			// OnException (which lets the rewrite plugin reverse-map
			// physical names back to logical ones via the rewriter
			// service's RewriteErrorMessage RPC) → re-encode and write
			// the rewritten exception to the client. If decoding fails
			// (chunk started mid-packet, malformed body, revision=0)
			// we fall through to the byte-splice path so the client
			// still sees something — the OnException hook is then
			// silently skipped, matching the existing best-effort
			// contract documented above.
			handled := false
			if first == byte(chproto.ServerExceptionCode) {
				if exc := tryDecodeException(buf[:n], up.Revision()); exc != nil {
					if err := r.hooks.OnException(ctx, r.sess, exc); err != nil {
						logger.Warne(err, "OnException hook returned error")
					}
					if werr := client.WriteException(exc); werr != nil {
						return fmt.Errorf("write rewritten exception to client: %w", werr)
					}
					handled = true
				}
			}
			if !handled {
				if _, werr := client.Conn().Write(buf[:n]); werr != nil {
					return fmt.Errorf("splice upstream→client: %w", werr)
				}
			}

			if isBoundary {
				r.sess.State().ClearActiveRewrite()
				r.hooks.OnQueryComplete(ctx, r.sess)
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return io.EOF
			}
			// A non-EOF read error on the current upstream codec may have been
			// caused by a planned rebind: RebindToPeer closes the old upstream
			// codec (asynchronously) while clientToUpstream's OnQuery is
			// pivoting the session to a peer. If the session now holds a
			// different upstream codec, the close was intentional — continue
			// reading from the new upstream instead of tearing down the
			// session.
			if newUp := r.sess.Upstream(); newUp != nil && newUp != up {
				_, logger := log.FromContext(ctx)
				logger.Debugw("upstreamToClient: upstream replaced mid-read (USE rebind), continuing with new upstream",
					"old_up_addr", upstreamAddr(up),
					"new_up_addr", upstreamAddr(newUp),
				)
				continue
			}
			return fmt.Errorf("upstream read: %w", err)
		}
	}
}

// upstreamAddr returns the remote address of the codec's underlying
// connection ("" if the codec is nil or its conn is not a net.Conn).
// Used by Relay log lines so operators can see which physical CH /
// peer-housegate the session is currently bound to — including after
// a USE-triggered rebind.
func upstreamAddr(up *chproto.Codec) string {
	if up == nil {
		return ""
	}
	if nc, ok := up.Conn().(net.Conn); ok {
		return nc.RemoteAddr().String()
	}
	return ""
}

// clientPacketName resolves a ClientCode value to the label the Relay
// emits for per-type counters via the shared packetNames map.
func clientPacketName(typ uint64) string {
	if name, ok := packetNames[typ]; ok {
		return name
	}
	if typ < 32 {
		return fmt.Sprintf("type_%d", typ)
	}
	return "unknown"
}

// compressionMode returns the label used for the streaming_data_blocks
// counter ("compressed" / "uncompressed").
func compressionMode(c proto.Compression) string {
	if c == proto.CompressionEnabled {
		return "compressed"
	}
	return "uncompressed"
}

// tryDecodeException best-effort decodes a server-direction Exception
// packet from the given raw bytes. raw[0] must be the type byte
// (ServerExceptionCode == 0x02); the body follows.
//
// Returns nil if decoding fails (the exception spanned multiple read
// chunks, the revision is zero / unknown, or the packet body is
// malformed). Best-effort matches the OnQueryComplete first-byte
// heuristic: callers must NOT rely on this firing for every
// upstream-side exception.
func tryDecodeException(raw []byte, rev int) *chproto.Exception {
	if len(raw) < 2 || raw[0] != byte(chproto.ServerExceptionCode) {
		return nil
	}
	r := proto.NewReader(bytes.NewReader(raw))
	// Consume the leading type byte the same way Codec.ReadPacket does;
	// it's a varint, but for low type codes (Exception=2) it is a
	// single-byte varint, so UVarInt advances exactly one byte.
	if _, err := r.UVarInt(); err != nil {
		return nil
	}
	var exc chproto.Exception
	if err := exc.DecodeAware(r, rev); err != nil {
		return nil
	}
	return &exc
}

// writeEndOfStreamToClient writes a single-byte ServerEndOfStreamCode
// packet to the client. Used by the AbortWithSuccess branch in the
// per-query loop: a plugin (commitgate via ErrAbortWithSuccess) has
// handled the statement out-of-band, and the relay must reply success
// without contacting ClickHouse.
//
// Takes io.Writer rather than net.Conn so unit tests can drive it with
// any byte sink; in production the caller passes Codec.Conn() which is
// backed by a *net.TCPConn.
func writeEndOfStreamToClient(w io.Writer) error {
	_, err := w.Write([]byte{byte(chproto.ServerEndOfStreamCode)})
	return err
}

// writeExceptionToClient converts a plugin error into a synthetic
// ClickHouse Exception and writes it to the client side. Used when the
// OnQuery chain short-circuits (e.g. JWS invalid, rate-limit).
func (r *Relay) writeExceptionToClient(ctx context.Context, pluginErr error) {
	exc := &chproto.Exception{
		Code:    403, // ClickHouse AUTHENTICATION_FAILED; generic plugin-reject
		Name:    "DB::Exception",
		Message: pluginErr.Error(),
	}
	if err := r.sess.Client().WriteException(exc); err != nil {
		_, logger := log.FromContext(ctx)
		logger.Warne(err, "failed to write plugin-error exception")
	}
}
