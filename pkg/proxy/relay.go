package proxy

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
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

	queryMu       sync.Mutex
	activeQuery   bool
	activeQueryID string
	queryCanceled bool
	// pendingRejection replaces the terminal packet of a query that Housegate
	// rejected locally after its input was complete. The staged payload was
	// withheld, but upstream still has to finish its zero-row INSERT before the
	// connection is reusable. Guarded by queryMu.
	pendingRejectionID  string
	pendingRejectionExc *chproto.Exception

	// completionProbe is replaceable in unit tests. Production uses
	// probeQueryCompletion, which watches the exact upstream server's
	// system.processes view on a short-lived auxiliary connection.
	completionProbe func(context.Context, string) error

	opaqueMu   sync.Mutex
	opaqueMode bool
	opaqueDone chan struct{}
	opaqueErr  error

	// deferredGate coordinates the deferred-INSERT sample step between the
	// two relay goroutines: clientToUpstream arms it right before writing the
	// deferred Query upstream; upstreamToClient settles it when the upstream
	// answers with its own sample Data block (consumed, never forwarded), an
	// Exception (forwarded to the client), or fails.
	deferredMu    sync.Mutex
	deferredGate  *deferredSampleGate
	deferredInput *deferredInputGate

	// prefetchedClient is owned by clientToUpstream. While a deferred INSERT
	// awaits its upstream terminal, that goroutine must keep observing client
	// Cancel/EOF. If the terminal wins concurrently with a legitimate next
	// packet, the already-framed packet is handed back to the outer loop here
	// rather than starting a second reader on the same Codec.
	prefetchedClient *clientPacketResult
}

// UpstreamDialer dials and returns the codec for this session's upstream.
// Called inside Relay.handshake AFTER OnHello so plugins (notably
// routeplugin.Stripper) get to populate SessionState — without that
// ordering the dialer would always see RouteTarget="" and routed
// sessions would silently fall through to cfg.Upstream.
type UpstreamDialer func(ctx context.Context, sess chsession.Session) (*chproto.Codec, error)

var errOpaqueConnectionNotReusable = errors.New(
	"legacy non-chunked opaque result connection cannot be safely reused",
)

// errQueryRejectedResume marks a deferred INSERT that Housegate rejected
// after consuming the client's complete input. Nothing reached upstream, so
// clientToUpstream can continue on the same packet boundary.
var errQueryRejectedResume = errors.New("query rejected without closing the session")

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

type deferredSampleGate struct {
	queryID string
	done    chan deferredSampleResult // buffered(1): settle never blocks
}

type deferredSampleResult struct {
	exception *chproto.Exception // upstream rejected the Query at the sample step
	err       error              // transport / protocol failure; session closes
}

type clientPacketResult struct {
	pkt *chproto.Packet
	err error
}

// deferredInputGate coordinates the post-sample writer with terminal packets.
// done closes after the writer has either finished OnQueryInputComplete or
// observed a fatal terminal and stopped. terminal stops further writes;
// delivered keeps runDeferredInsert alive until upstreamToClient has exposed
// or rejected the terminal and completed the query lifecycle.
type deferredInputGate struct {
	done           chan struct{}
	terminal       chan struct{}
	exposed        chan struct{}
	exposureDone   chan struct{}
	delivered      chan struct{}
	qctx           *plugin.QueryContext
	marker         atomic.Bool
	terminator     atomic.Bool
	lifecycleOwner atomic.Uint32
	terminalClaim  atomic.Bool
	doneOnce       sync.Once
	terminalOnce   sync.Once
	exposedOnce    sync.Once
	exposureOnce   sync.Once
	deliveredOnce  sync.Once
	abortOnce      sync.Once
	completeOnce   sync.Once
}

const (
	deferredLifecycleUnclaimed uint32 = iota
	deferredLifecycleUpstream
	deferredLifecycleWriter
)

func newDeferredInputGate(qctx *plugin.QueryContext) *deferredInputGate {
	return &deferredInputGate{
		done:         make(chan struct{}),
		terminal:     make(chan struct{}),
		exposed:      make(chan struct{}),
		exposureDone: make(chan struct{}),
		delivered:    make(chan struct{}),
		qctx:         qctx,
	}
}

func (g *deferredInputGate) finishWriter() { g.doneOnce.Do(func() { close(g.done) }) }
func (g *deferredInputGate) stopWriter()   { g.terminalOnce.Do(func() { close(g.terminal) }) }
func (g *deferredInputGate) claimUpstreamLifecycle() bool {
	return g.lifecycleOwner.CompareAndSwap(deferredLifecycleUnclaimed, deferredLifecycleUpstream) ||
		g.lifecycleOwner.Load() == deferredLifecycleUpstream
}
func (g *deferredInputGate) claimWriterLifecycle() bool {
	return g.lifecycleOwner.CompareAndSwap(deferredLifecycleUnclaimed, deferredLifecycleWriter) ||
		g.lifecycleOwner.Load() == deferredLifecycleWriter
}
func (g *deferredInputGate) publishUpstreamTerminal() { g.terminalClaim.Store(true) }
func (g *deferredInputGate) finishTerminalExposure(ok bool) {
	if ok {
		g.exposedOnce.Do(func() { close(g.exposed) })
	}
	g.exposureOnce.Do(func() { close(g.exposureDone) })
}
func (g *deferredInputGate) markDelivered() {
	g.deliveredOnce.Do(func() { close(g.delivered) })
}
func (g *deferredInputGate) markMarkerWritten()      { g.marker.Store(true) }
func (g *deferredInputGate) markerWritten() bool     { return g.marker.Load() }
func (g *deferredInputGate) markTerminatorWritten()  { g.terminator.Store(true) }
func (g *deferredInputGate) terminatorWritten() bool { return g.terminator.Load() }
func (g *deferredInputGate) abort(ctx context.Context, hooks plugin.Hooks) {
	g.abortOnce.Do(func() { hooks.OnQueryAbort(ctx, g.qctx) })
}
func (g *deferredInputGate) complete(ctx context.Context, hooks plugin.Hooks, sess chsession.Session) {
	g.completeOnce.Do(func() { hooks.OnQueryComplete(ctx, sess) })
}

func (g *deferredInputGate) terminalObserved() bool {
	select {
	case <-g.terminal:
		return true
	default:
		return false
	}
}

func (g *deferredInputGate) terminalExposed() bool {
	select {
	case <-g.exposed:
		return true
	default:
		return false
	}
}

func (g *deferredInputGate) terminalClaimed() bool {
	return g.terminalClaim.Load()
}

func (r *Relay) armDeferredSample(queryID string) <-chan deferredSampleResult {
	r.deferredMu.Lock()
	defer r.deferredMu.Unlock()
	g := &deferredSampleGate{queryID: queryID, done: make(chan deferredSampleResult, 1)}
	r.deferredGate = g
	return g.done
}

// settleDeferredSample delivers res to the armed gate (no-op when none) and
// disarms it. Returns whether a gate was armed.
func (r *Relay) settleDeferredSample(res deferredSampleResult) bool {
	r.deferredMu.Lock()
	defer r.deferredMu.Unlock()
	if r.deferredGate == nil {
		return false
	}
	r.deferredGate.done <- res
	r.deferredGate = nil
	return true
}

func (r *Relay) disarmDeferredSample() {
	r.deferredMu.Lock()
	r.deferredGate = nil
	r.deferredMu.Unlock()
}

func (r *Relay) deferredSampleArmed() bool {
	r.deferredMu.Lock()
	defer r.deferredMu.Unlock()
	return r.deferredGate != nil
}

func (r *Relay) armDeferredInput(qctx *plugin.QueryContext) *deferredInputGate {
	r.deferredMu.Lock()
	defer r.deferredMu.Unlock()
	g := newDeferredInputGate(qctx)
	r.deferredInput = g
	return g
}

func (r *Relay) finishDeferredInput(g *deferredInputGate) {
	if g == nil {
		return
	}
	g.finishWriter()
	// Keep the gate installed until the real upstream terminal arrives. Local
	// writes can complete into a kernel buffer before ClickHouse parses them;
	// clearing here would misclassify a later payload Exception as reusable.
}

func (r *Relay) clearDeferredInput(g *deferredInputGate) {
	if g == nil {
		return
	}
	r.deferredMu.Lock()
	if r.deferredInput == g {
		r.deferredInput = nil
	}
	r.deferredMu.Unlock()
}

func (r *Relay) stopDeferredInput(ctx context.Context, up *chproto.Codec, g *deferredInputGate) error {
	if g == nil {
		return nil
	}
	g.stopWriter()
	closeCodec(up)
	g.abort(ctx, r.hooks)
	select {
	case <-g.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Relay) deliverDeferredInput(g *deferredInputGate) {
	if g == nil {
		return
	}
	g.markDelivered()
	r.clearDeferredInput(g)
}

func (r *Relay) currentDeferredInput() *deferredInputGate {
	r.deferredMu.Lock()
	defer r.deferredMu.Unlock()
	return r.deferredInput
}

func (r *Relay) unblockDeferredInputOnServerExit(ctx context.Context) {
	if g := r.currentDeferredInput(); g != nil {
		if !g.claimUpstreamLifecycle() {
			// The writer already settled abort+complete. It deliberately kept the
			// gate as an ownership tombstone until this reader exited.
			<-g.done
			r.clearDeferredInput(g)
			return
		}
		g.stopWriter()
		closeCodec(r.sess.Upstream())
		g.abort(ctx, r.hooks)
		<-g.done
		r.sess.State().ClearActiveRewrite()
		r.takeActiveQuery()
		g.complete(ctx, r.hooks, r.sess)
		r.deliverDeferredInput(g)
	}
}

func (r *Relay) beginActiveQuery(queryID string) bool {
	r.queryMu.Lock()
	defer r.queryMu.Unlock()
	if r.activeQuery {
		return false
	}
	r.activeQuery = true
	r.activeQueryID = queryID
	r.queryCanceled = false
	return true
}

func (r *Relay) takeActiveQuery() (string, bool) {
	queryID, _, active := r.takeActiveQueryState()
	return queryID, active
}

func (r *Relay) takeActiveQueryState() (queryID string, canceled, active bool) {
	r.queryMu.Lock()
	defer r.queryMu.Unlock()
	if !r.activeQuery {
		return "", false, false
	}
	queryID = r.activeQueryID
	canceled = r.queryCanceled
	r.activeQuery = false
	r.activeQueryID = ""
	r.queryCanceled = false
	return queryID, canceled, true
}

func (r *Relay) currentActiveQuery() (string, bool) {
	r.queryMu.Lock()
	defer r.queryMu.Unlock()
	return r.activeQueryID, r.activeQuery
}

func (r *Relay) cancelActiveQuery() (string, bool) {
	r.queryMu.Lock()
	defer r.queryMu.Unlock()
	if !r.activeQuery {
		return "", false
	}
	r.queryCanceled = true
	return r.activeQueryID, true
}

// rejectActiveQueryTerminal arms the terminal swap for queryID. It returns
// false when that query is no longer the active one, in which case the caller
// must fall back to closing the session.
func (r *Relay) rejectActiveQueryTerminal(queryID string, exc *chproto.Exception) bool {
	r.queryMu.Lock()
	defer r.queryMu.Unlock()
	if !r.activeQuery || r.activeQueryID != queryID || exc == nil {
		return false
	}
	r.pendingRejectionID = queryID
	r.pendingRejectionExc = exc
	return true
}

// takePendingRejection consumes the armed rejection for queryID, if any.
func (r *Relay) takePendingRejection(queryID string) *chproto.Exception {
	r.queryMu.Lock()
	defer r.queryMu.Unlock()
	if r.pendingRejectionExc == nil || r.pendingRejectionID != queryID {
		return nil
	}
	exc := r.pendingRejectionExc
	r.pendingRejectionID, r.pendingRejectionExc = "", nil
	return exc
}

func (r *Relay) startOpaqueCompletionProbe(ctx context.Context, queryID string) error {
	r.opaqueMu.Lock()
	if r.opaqueMode {
		r.opaqueMu.Unlock()
		return fmt.Errorf("opaque result mode already active")
	}
	r.opaqueMode = true
	r.opaqueDone = make(chan struct{})
	r.opaqueErr = nil
	done := r.opaqueDone
	r.opaqueMu.Unlock()

	probe := r.completionProbe
	if probe == nil {
		probe = r.probeQueryCompletion
	}
	go func() {
		err := probe(ctx, queryID)
		if err == nil {
			r.sess.State().ClearActiveRewrite()
			if _, active := r.takeActiveQuery(); active {
				r.hooks.OnQueryComplete(ctx, r.sess)
			}
		} else {
			// Wake both relay directions. The normal read-error cleanup path
			// consumes any still-active query and releases plugin resources.
			_, logger := log.FromContext(ctx)
			logger.Warnw("opaque result completion probe failed",
				"query_id", queryID,
				"err", err,
			)
			_ = r.sess.Close()
		}

		r.opaqueMu.Lock()
		r.opaqueErr = err
		close(done)
		r.opaqueMu.Unlock()
	}()
	return nil
}

func (r *Relay) opaqueSnapshot() (active bool, done <-chan struct{}, err error) {
	r.opaqueMu.Lock()
	defer r.opaqueMu.Unlock()
	return r.opaqueMode, r.opaqueDone, r.opaqueErr
}

func (r *Relay) waitAndRejectOpaqueReuse(ctx context.Context) error {
	active, done, _ := r.opaqueSnapshot()
	if !active {
		return nil
	}
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}
	_, _, probeErr := r.opaqueSnapshot()
	if probeErr != nil {
		return fmt.Errorf("opaque result completion probe: %w", probeErr)
	}
	// The process probe proves only that ClickHouse stopped executing the
	// query. It does not prove that this connection's terminal packet has
	// reached Housegate: a pipelined client Query may arrive first. Since an
	// opaque Native value prevents authoritative packet framing, never resume
	// this upstream codec. Relay.Run closes the session on this error.
	return errOpaqueConnectionNotReusable
}

func (r *Relay) handshakeFreshUpstream(
	ctx context.Context,
	up *chproto.Codec,
	opts chproto.AddendumOpts,
) (*chproto.ClientHello, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	hello := r.sess.State().UpstreamHello()
	if hello == nil {
		return nil, fmt.Errorf("missing stored upstream hello")
	}
	snap := r.sess.State().Snapshot()
	if !snap.IsForwarding && snap.RouteTarget == "" && snap.Database != "" {
		hello.Database = snap.Database
	}
	hello = chproto.ClientHelloForUpstream(hello)
	if err := up.WriteClientHello(hello); err != nil {
		return nil, fmt.Errorf("write hello: %w", err)
	}
	up.SetServerHelloRevisionHint(int(hello.ProtocolVersion))
	pkt, err := up.ReadPacket(uint64(chproto.ServerHelloCode), uint64(chproto.ServerExceptionCode))
	if err != nil {
		return nil, fmt.Errorf("read server hello: %w", err)
	}
	if exc, ok := pkt.Decoded.(*chproto.Exception); ok {
		return nil, fmt.Errorf("upstream rejected hello: code=%d %s: %s", exc.Code, exc.Name, exc.Message)
	}
	srv, ok := pkt.Decoded.(*chproto.ServerHello)
	if !ok {
		return nil, fmt.Errorf("unexpected hello packet type=%d", pkt.Type)
	}
	rev := int(hello.ProtocolVersion)
	if int(srv.Revision) < rev {
		rev = int(srv.Revision)
	}
	up.SetRevision(rev)
	up.SetCompression(proto.CompressionDisabled)
	if chproto.SupportsAddendum(rev) {
		res := up.ResolveUpstreamAddendum(chproto.AddendumResult{}, opts)
		if err := up.SendAddendum(res); err != nil {
			return nil, fmt.Errorf("send addendum: %w", err)
		}
	}
	return hello, nil
}

func (r *Relay) probeQueryCompletion(ctx context.Context, queryID string) error {
	up, err := r.dialExactCurrentUpstream(ctx)
	if err != nil {
		return err
	}
	defer closeCodec(up)
	conn, _ := up.Conn().(net.Conn)
	if conn != nil {
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	}
	if _, err := r.handshakeFreshUpstream(ctx, up, chproto.AddendumOpts{
		ProposedRecv: "notchunked",
		ProposedSend: "notchunked",
	}); err != nil {
		return err
	}

	for {
		if conn != nil {
			_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		}
		running, err := r.probeQueryRunning(up, queryID)
		if err != nil {
			return err
		}
		if !running {
			return nil
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		}
	}
}

func (r *Relay) probeQueryRunning(up *chproto.Codec, queryID string) (bool, error) {
	const runningMarker = "__HOUSEGATE_TARGET_QUERY_RUNNING__"
	targetHex := strings.ToUpper(hex.EncodeToString([]byte(queryID)))
	q := &chproto.Query{
		ID: fmt.Sprintf("__housegate_probe_%d", time.Now().UnixNano()),
		Info: chproto.ClientInfo{
			Query:           proto.ClientQueryInitial,
			InitialUser:     "housegate",
			InitialQueryID:  queryID,
			InitialAddress:  "127.0.0.1:0",
			Interface:       proto.InterfaceTCP,
			OSUser:          "housegate",
			ClientHostname:  "housegate",
			ClientName:      "housegate-probe",
			Major:           1,
			Minor:           0,
			ProtocolVersion: up.Revision(),
		},
		Stage:       proto.StageComplete,
		Compression: proto.CompressionDisabled,
		Body: fmt.Sprintf(
			"SELECT throwIf(count() > 0, '%s') FROM system.processes WHERE query_id = unhex('%s')",
			runningMarker,
			targetHex,
		),
	}
	if err := up.WriteQuery(q); err != nil {
		return false, fmt.Errorf("write completion probe: %w", err)
	}
	if err := up.WriteEmptyDataBlock(); err != nil {
		return false, fmt.Errorf("write completion probe terminator: %w", err)
	}
	for {
		pkt, err := up.ReadPacket(uint64(chproto.ServerExceptionCode))
		if err != nil {
			return false, fmt.Errorf("read completion probe: %w", err)
		}
		switch pkt.Type {
		case uint64(chproto.ServerEndOfStreamCode):
			return false, nil
		case uint64(chproto.ServerExceptionCode):
			exc, _ := pkt.Decoded.(*chproto.Exception)
			if exc != nil && strings.Contains(exc.Message, runningMarker) {
				return true, nil
			}
			if exc == nil {
				return false, fmt.Errorf("completion probe returned malformed exception")
			}
			return false, fmt.Errorf("completion probe rejected: code=%d %s: %s", exc.Code, exc.Name, exc.Message)
		}
	}
}

func (r *Relay) dialExactCurrentUpstream(ctx context.Context) (*chproto.Codec, error) {
	current := r.sess.Upstream()
	if current != nil {
		if nc, ok := current.Conn().(net.Conn); ok && nc.RemoteAddr() != nil {
			network := nc.RemoteAddr().Network()
			if network == "" {
				network = "tcp"
			}
			conn, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, network, nc.RemoteAddr().String())
			if err != nil {
				return nil, fmt.Errorf("dial exact upstream %s: %w", nc.RemoteAddr(), err)
			}
			return chproto.NewCodec(conn, chproto.DirToUpstream), nil
		}
	}
	if r.dialer == nil {
		return nil, fmt.Errorf("no probe dialer")
	}
	return r.dialer(ctx, r.sess)
}

func closeCodec(codec *chproto.Codec) {
	if codec == nil {
		return
	}
	if closer, ok := codec.Conn().(io.Closer); ok {
		_ = closer.Close()
	}
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
		if err := client.WriteRawPacket(raw); err != nil {
			return fmt.Errorf("forwarding: echo peer server hello: %w", err)
		}
		if chproto.SupportsAddendum(rev) {
			res, err := client.NegotiateAddendum(chproto.AddendumOpts{
				ProposedRecv: "chunked_optional",
				ProposedSend: "chunked_optional",
			})
			if err != nil {
				return fmt.Errorf("forwarding: client addendum: %w", err)
			}
			state.ChunkedRecv = res.NegotiatedRecv
			state.ChunkedSend = res.NegotiatedSend
			// Upstream addendum was already sent by RebindToPeer; do not repeat.
		}
	} else {
		upstreamHello := chproto.ClientHelloForUpstream(hello)
		if err := upstream.WriteClientHello(upstreamHello); err != nil {
			return fmt.Errorf("forward client hello: %w", err)
		}
		upstream.SetServerHelloRevisionHint(int(upstreamHello.ProtocolVersion))
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

		rev := int(upstreamHello.ProtocolVersion)
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
		if err := client.WriteRawPacket(srvPkt.Raw); err != nil {
			return fmt.Errorf("echo server hello: %w", err)
		}
		logger.Debugw("server hello echoed to client", "bytes", len(srvPkt.Raw))

		if chproto.SupportsAddendum(rev) {
			// Housegate terminates chunked transport on each leg. The client
			// result enables its codec; the upstream result is resolved
			// independently against the original ServerHello capabilities.
			res, err := client.NegotiateAddendum(chproto.AddendumOpts{
				ProposedRecv: "chunked_optional",
				ProposedSend: "chunked_optional",
			})
			if err != nil {
				return fmt.Errorf("client addendum: %w", err)
			}
			state.ChunkedRecv = res.NegotiatedRecv
			state.ChunkedSend = res.NegotiatedSend
			upstreamRes := upstream.ResolveUpstreamAddendum(res, chproto.AddendumOpts{
				ProposedRecv: "chunked_optional",
				ProposedSend: "chunked_optional",
			})
			if err := upstream.SendAddendum(upstreamRes); err != nil {
				return fmt.Errorf("upstream addendum: %w", err)
			}
		}
		state.SetUpstreamHello(upstreamHello)
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
	return r.runPostHandshake(ctx)
}

// runPostHandshake owns the lifetime of an already-bound session. Keeping the
// pump-and-close seam separate from handshake makes the fail-closed guarantee
// directly testable with codecs prepared at an exact protocol state.
func (r *Relay) runPostHandshake(ctx context.Context) error {
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
		curQctxRejected        bool
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
		if prefetched := r.prefetchedClient; prefetched != nil {
			r.prefetchedClient = nil
			pkt, decErr = prefetched.pkt, prefetched.err
		} else if enforceLimit {
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
			if q.ID == "" {
				q.ID = fmt.Sprintf("__housegate_query_%d_%d", r.sess.ID(), time.Now().UnixNano())
			}
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
			// A previous legacy non-chunked result entered opaque raw mode.
			// Even after its process probe completes, a new client Query does
			// not prove that the old EndOfStream crossed this connection.
			// Fail the session closed instead of risking attribution of that
			// terminal packet to the new query.
			if err := r.waitAndRejectOpaqueReuse(ctx); err != nil {
				if !errors.Is(err, errOpaqueConnectionNotReusable) {
					r.writeExceptionToClient(ctx, err)
				}
				return err
			}
			if previousQueryID, active := r.currentActiveQuery(); active {
				err := fmt.Errorf("client sent query %q before upstream completed query %q", q.ID, previousQueryID)
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
				if err := client.WriteRawPacket([]byte{byte(chproto.ServerEndOfStreamCode)}); err != nil {
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
			if qctx.DeferredInsert != nil {
				if qctx.SuppressUpstreamExecution {
					err := fmt.Errorf("query %q: DeferredInsert and SuppressUpstreamExecution are mutually exclusive", q.ID)
					r.hooks.OnQueryAbort(ctx, qctx)
					r.hooks.OnQueryComplete(ctx, r.sess)
					r.writeExceptionToClient(ctx, err)
					rejectedQctx = qctx
					continue
				}
				if err := r.runDeferredInsert(ctx, qctx, clientCompression); err != nil {
					if errors.Is(err, errQueryRejectedResume) {
						logger.Infow("deferred INSERT rejected without closing the session",
							"query_id", q.ID, "err", err)
						continue
					}
					return err
				}
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
			if !r.beginActiveQuery(q.ID) {
				err := fmt.Errorf("query %q raced with another active upstream query", q.ID)
				r.writeExceptionToClient(ctx, err)
				r.hooks.OnQueryAbort(ctx, qctx)
				r.hooks.OnQueryComplete(ctx, r.sess)
				return err
			}
			if err := up.WriteQuery(qctx.Query); err != nil {
				r.takeActiveQuery()
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
		initialEmptyMarker := false
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
				initialEmptyMarker = true
				inputComplete = false
			} else if !inputComplete {
				curQctxSawPayload = true
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
				if chproto.KeepsSession(err) && curQctx.SuppressUpstreamExecution &&
					r.rejectActiveQueryTerminal(curQctx.Query.ID, exceptionForPluginError(err)) {
					// The staged payload never reached upstream. Forwarding the
					// terminator below completes its zero-row negotiation; the server
					// loop then swaps that terminal for our local Exception.
					r.hooks.OnQueryAbort(ctx, curQctx)
					curQctxRejected = true
					logger.Infow("query rejected without closing the session",
						"query_id", curQctx.Query.ID, "err", err)
				} else {
					r.writeExceptionToClient(ctx, err)
					r.hooks.OnQueryAbort(ctx, curQctx)
					r.hooks.OnQueryComplete(ctx, r.sess)
					return fmt.Errorf("query input complete strict hook: %w", err)
				}
			}
		}

		if pkt.Type == uint64(chproto.ClientDataCode) && curQctx != nil && curQctx.SuppressUpstreamExecution {
			if inputComplete || initialEmptyMarker {
				logger.Debugw("staged client data control packet forwarded to upstream",
					"query_id", curQctx.Query.ID,
					"initial_marker", initialEmptyMarker,
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

		// ClickHouse serializes QUERY_WAS_CANCELLED_BY_CLIENT as EndOfStream,
		// not Exception. Mark the active query before forwarding Cancel so that
		// terminal packet performs cleanup without being mistaken for success.
		if pkt.Type == uint64(chproto.ClientCancelCode) {
			if queryID, active := r.cancelActiveQuery(); active {
				logger.Debugw("active query marked canceled",
					"query_id", queryID,
				)
			}
		}

		// Splice any non-decoded / decode-failed packet.
		logger.Debugw("client packet spliced to upstream",
			"type", pkt.Type,
			"name", clientPacketName(pkt.Type),
			"raw_len", pkt.RawLen,
		)
		if err := up.WriteRawPacket(pkt.Raw); err != nil {
			return fmt.Errorf("splice client→upstream type=%d: %w", pkt.Type, err)
		}
		if inputComplete {
			if !curQctxRejected {
				r.hooks.OnQueryInputComplete(ctx, curQctx)
			}
			curQctx = nil
			curQctxRejected = false
			curQctxSawInitialEmpty = false
			curQctxSawPayload = false
		}
	}
}

// runDeferredInsert drives the deferred-INSERT protocol (spec 2026-08-18
// signed envelope v2 §5.2) for one query whose OnQuery chain set
// qctx.DeferredInsert:
//
//  1. write the plan's 0-row sample block to the client (the Query is NOT
//     forwarded yet);
//  2. read client packets: a first empty Data before any payload is always the
//     end-of-external-tables marker; non-empty Data may instead arrive first
//     when the marker was omitted. Non-empty Data runs
//     OnClientDataStrict/OnClientData and is buffered under MaxPayloadBytes;
//     the next empty Data is the terminator and runs
//     OnQueryInputCompleteStrict. This state rule is deterministic: packet
//     timing never changes how a valid producer is parsed. A markerless
//     zero-row stream is inherently ambiguous and therefore remains pending
//     until the client sends a terminator, Cancel, or closes.
//  3. forward Query plus exactly one external-tables marker (synthesized from
//     the terminator when the client omitted it), wait for upstreamToClient to
//     consume the upstream's own sample block (or forward its Exception),
//     then forward every buffered Data packet and the terminator via
//     WriteRawPacket, and fire OnQueryInputComplete. The ordinary
//     upstreamToClient loop delivers the terminal EndOfStream/Exception.
//
// A nil return means the session stays usable (success, upstream Exception,
// or client Cancel); a non-nil return closes the session (fail-closed on
// strict-hook errors, limits, protocol violations, transport errors). The
// buffer is a local variable and is released on every path.
func (r *Relay) runDeferredInsert(ctx context.Context, qctx *plugin.QueryContext, compression proto.Compression) error {
	_, logger := log.FromContext(ctx)
	client := r.sess.Client()
	plan := qctx.DeferredInsert
	q := qctx.Query
	if plan.MaxPayloadBytes == 0 {
		err := fmt.Errorf("query %q: deferred INSERT plan requires MaxPayloadBytes > 0", q.ID)
		r.hooks.OnQueryAbort(ctx, qctx)
		r.hooks.OnQueryComplete(ctx, r.sess)
		r.writeExceptionToClient(ctx, err)
		return err
	}
	rejectClose := func(err error) error {
		r.hooks.OnQueryAbort(ctx, qctx)
		r.hooks.OnQueryComplete(ctx, r.sess)
		r.writeExceptionToClient(ctx, err)
		return err
	}
	// The complete terminator has already been consumed and nothing has been
	// written upstream, so a retryable admission rejection can end only this
	// query while leaving both packet streams on a clean boundary.
	rejectResume := func(err error) error {
		r.hooks.OnQueryAbort(ctx, qctx)
		r.hooks.OnQueryComplete(ctx, r.sess)
		r.writeExceptionToClient(ctx, err)
		return fmt.Errorf("%w: %w", errQueryRejectedResume, err)
	}
	if err := client.WriteSampleBlock(plan.SampleColumns); err != nil {
		r.hooks.OnQueryAbort(ctx, qctx)
		r.hooks.OnQueryComplete(ctx, r.sess)
		return fmt.Errorf("write deferred sample block: %w", err)
	}

	var (
		buffered        [][]byte
		bufferedBytes   uint64
		initialEmptyRaw []byte
		terminatorRaw   []byte
		sawPayload      bool
	)
	for terminatorRaw == nil {
		remaining := plan.MaxPayloadBytes - bufferedBytes
		if limit, enforce := r.hooks.ClientDataReadLimit(qctx); enforce && limit < remaining {
			remaining = limit
		}
		// Empty Data control packets (external-tables marker + terminator) do
		// not belong to the signed payload. Permit one small, bounded parsing
		// allowance so they remain classifiable even when payload bytes have
		// reached the exact limit; a non-empty packet is still rejected below
		// whenever its full on-wire size exceeds remaining.
		const controlPacketAllowance uint64 = 4 << 10
		readLimit := remaining
		if ^uint64(0)-readLimit < controlPacketAllowance {
			readLimit = ^uint64(0)
		} else {
			readLimit += controlPacketAllowance
		}
		pkt, decErr := client.ReadPacketWithDataLimit(readLimit, uint64(chproto.ClientQueryCode))
		if errors.Is(decErr, chproto.ErrPacketTooLarge) {
			return rejectClose(fmt.Errorf("deferred INSERT %q payload exceeds limit of %d bytes: %w", q.ID, plan.MaxPayloadBytes, decErr))
		}
		if pkt == nil || (decErr != nil && !errors.Is(decErr, chproto.ErrDecode)) {
			r.hooks.OnQueryAbort(ctx, qctx)
			r.hooks.OnQueryComplete(ctx, r.sess)
			if decErr == nil {
				decErr = io.EOF
			}
			return decErr
		}
		if r.obs != nil {
			r.obs.ClientPacket(clientPacketName(pkt.Type))
			if pkt.RawLen > 0 {
				r.obs.BytesTransferred("client_to_upstream", float64(pkt.RawLen))
			}
		}
		switch pkt.Type {
		case uint64(chproto.ClientDataCode):
			info, err := chproto.InspectClientDataPacket(pkt.Raw, compression)
			if err != nil {
				r.hooks.OnQueryAbort(ctx, qctx)
				r.hooks.OnQueryComplete(ctx, r.sess)
				return fmt.Errorf("classify deferred client data packet: %w", err)
			}
			if info.BlockName != "" {
				// A named block is external-temporary-table data (or that
				// table's own zero-row terminator). Folding it into
				// payload_hash would sign bytes the ingress never stores, and
				// consuming its terminator would strand the real payload, so
				// the signed lane refuses it outright.
				return rejectClose(fmt.Errorf(
					"deferred INSERT %q received external table block %q; external tables are not supported on the storage-integrity signed lane",
					q.ID, info.BlockName))
			}
			switch {
			case info.Empty && !sawPayload && initialEmptyRaw == nil:
				// Protocol state, never a timeout, identifies the first empty
				// packet as the external-tables marker. A standard zero-row
				// producer sends a second empty packet as its terminator.
				initialEmptyRaw = append([]byte(nil), pkt.Raw...)
			case info.Empty:
				terminatorRaw = append([]byte(nil), pkt.Raw...)
			default:
				// The client-side marker is optional. When absent, terminatorRaw is
				// later replayed once before the sample and once after payload.
				sawPayload = true
				if uint64(len(pkt.Raw)) > remaining {
					return rejectClose(fmt.Errorf("deferred INSERT %q payload exceeds limit (remaining %d bytes): %w", q.ID, remaining, chproto.ErrPacketTooLarge))
				}
				if err := r.hooks.OnClientDataStrict(ctx, qctx, pkt.Raw); err != nil {
					return rejectClose(fmt.Errorf("client data strict hook: %w", err))
				}
				if err := r.hooks.OnClientData(ctx, qctx, pkt.Raw); err != nil {
					logger.Warnw("client data hook failed (fail-open)", "raw_len", pkt.RawLen, "err", err)
				}
				bufferedBytes += uint64(len(pkt.Raw))
				if bufferedBytes > plan.MaxPayloadBytes {
					return rejectClose(fmt.Errorf("deferred INSERT %q payload exceeds limit of %d bytes", q.ID, plan.MaxPayloadBytes))
				}
				buffered = append(buffered, append([]byte(nil), pkt.Raw...))
			}
		case uint64(chproto.ClientCancelCode):
			// Nothing reached upstream. ClickHouse answers a cancelled query
			// with EndOfStream; do the same locally and drop the buffer.
			r.hooks.OnQueryAbort(ctx, qctx)
			r.hooks.OnQueryComplete(ctx, r.sess)
			if err := client.WriteRawPacket([]byte{byte(chproto.ServerEndOfStreamCode)}); err != nil {
				return fmt.Errorf("write end-of-stream after deferred cancel: %w", err)
			}
			logger.Debugw("deferred INSERT cancelled by client before forwarding", "query_id", q.ID)
			return nil
		default:
			return rejectClose(fmt.Errorf("client sent packet type %d (%s) while deferred INSERT %q was collecting its payload", pkt.Type, clientPacketName(pkt.Type), q.ID))
		}
	}
	if err := r.hooks.OnQueryInputCompleteStrict(ctx, qctx); err != nil {
		if chproto.KeepsSession(err) {
			return rejectResume(fmt.Errorf("query input complete strict hook: %w", err))
		}
		return rejectClose(fmt.Errorf("query input complete strict hook: %w", err))
	}

	up := r.sess.Upstream()
	if up == nil {
		r.hooks.OnQueryAbort(ctx, qctx)
		r.hooks.OnQueryComplete(ctx, r.sess)
		return chsession.ErrNoUpstream
	}
	up.SetCompression(compression)
	if !r.beginActiveQuery(q.ID) {
		return rejectClose(fmt.Errorf("query %q raced with another active upstream query", q.ID))
	}
	inputGate := r.armDeferredInput(qctx)
	defer r.finishDeferredInput(inputGate)
	gate := r.armDeferredSample(q.ID)
	abortFromWriter := func(stage string, cause error) error {
		r.disarmDeferredSample()
		if !inputGate.claimWriterLifecycle() {
			// The upstream reader won the single lifecycle owner CAS. Close the
			// downstream so a pending terminal write cannot block, release writer
			// ownership, and wait for that owner to settle exactly once.
			closeCodec(client)
			r.finishDeferredInput(inputGate)
			select {
			case <-inputGate.delivered:
				return fmt.Errorf("deferred INSERT %q %s after upstream lifecycle ownership: %w", q.ID, stage, cause)
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		inputGate.stopWriter()
		closeCodec(up)
		inputGate.abort(ctx, r.hooks)
		r.finishDeferredInput(inputGate)
		r.sess.State().ClearActiveRewrite()
		r.takeActiveQuery()
		inputGate.complete(ctx, r.hooks, r.sess)
		// Retain the writer-owned tombstone until the upstream reader exits so
		// an already-decoded packet cannot be mistaken for a fresh lifecycle.
		inputGate.markDelivered()
		return fmt.Errorf("deferred INSERT %q %s: %w", q.ID, stage, cause)
	}
	forwardFail := func(stage string, err error) error {
		return abortFromWriter(fmt.Sprintf("%s to %s", stage, upstreamAddr(up)), err)
	}
	abortAfterTerminal := func(stage string) error {
		r.finishDeferredInput(inputGate)
		select {
		case <-inputGate.delivered:
			return fmt.Errorf("deferred INSERT %q terminated while %s", q.ID, stage)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	abortFromClient := func(cause error) error {
		return abortFromWriter("client terminated while awaiting upstream", cause)
	}
	// Raw buffered Data uses the client's original framing regardless of any
	// QueryPlugin mutation, exactly like the ordinary relay path.
	q.Compression = compression
	if err := up.WriteQuery(q); err != nil {
		return forwardFail("forward query", err)
	}
	markerRaw := initialEmptyRaw
	if markerRaw == nil {
		markerRaw = terminatorRaw
	}
	if err := up.WriteRawPacket(markerRaw); err != nil {
		if inputGate.terminalObserved() {
			return abortAfterTerminal("writing external-tables marker")
		}
		return forwardFail("forward external-tables marker", err)
	}
	inputGate.markMarkerWritten()
	var sampleResult deferredSampleResult
	for {
		select {
		case sampleResult = <-gate:
			goto sampleSettled
		case <-ctx.Done():
			return abortFromClient(ctx.Err())
		default:
		}

		// The same sole client reader that collected the body continues to
		// arbitrate liveness while ClickHouse prepares its sample. Otherwise a
		// client EOF/Cancel plus an upstream that never answers the sample leaks
		// both relay loops for the server-lifetime context.
		available, err := client.WaitForPacketStart(25 * time.Millisecond)
		if err != nil {
			return abortFromClient(err)
		}
		if !available {
			continue
		}
		pkt, decErr := client.ReadPacket(uint64(chproto.ClientQueryCode))
		switch {
		case decErr != nil && pkt == nil:
			return abortFromClient(decErr)
		case pkt == nil:
			return abortFromClient(io.EOF)
		case pkt.Type == uint64(chproto.ClientCancelCode):
			return abortFromClient(errors.New("client cancelled deferred INSERT while awaiting upstream sample"))
		case decErr != nil:
			return abortFromClient(decErr)
		default:
			return abortFromClient(fmt.Errorf("client sent packet type %d (%s) before deferred INSERT sample", pkt.Type, clientPacketName(pkt.Type)))
		}
	}

sampleSettled:
	if sampleResult.err != nil {
		r.finishDeferredInput(inputGate)
		select {
		case <-inputGate.delivered:
			return fmt.Errorf("deferred INSERT %q sample negotiation: %w", q.ID, sampleResult.err)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if sampleResult.exception != nil {
		// upstreamToClient already forwarded the Exception and ran the
		// terminal hooks (takeActiveQuery + OnQueryComplete); only the
		// plugin-side buffer state is left to drop.
		logger.Debugw("deferred INSERT rejected by upstream at sample step", "query_id", q.ID, "code", sampleResult.exception.Code, "message", sampleResult.exception.Message)
		r.finishDeferredInput(inputGate)
		select {
		case <-inputGate.delivered:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for _, raw := range buffered {
		if inputGate.terminalObserved() {
			return abortAfterTerminal("forwarding payload")
		}
		if err := up.WriteRawPacket(raw); err != nil {
			if inputGate.terminalObserved() {
				return abortAfterTerminal("forwarding payload")
			}
			return forwardFail("forward payload", err)
		}
		if inputGate.terminalObserved() {
			return abortAfterTerminal("forwarding payload")
		}
	}
	if err := up.WriteRawPacket(terminatorRaw); err != nil {
		if inputGate.terminalObserved() {
			return abortAfterTerminal("writing terminator")
		}
		return forwardFail("forward terminator", err)
	}
	inputGate.markTerminatorWritten()
	if inputGate.terminalObserved() {
		return abortAfterTerminal("completing input")
	}
	r.hooks.OnQueryInputComplete(ctx, qctx)
	r.finishDeferredInput(inputGate)
	logger.Debugw("deferred INSERT forwarded", "query_id", q.ID, "packets", len(buffered), "payload_bytes", bufferedBytes)
	awaitTerminalExposure := func(prefetched *clientPacketResult) (bool, error) {
		if !inputGate.terminalExposed() && !inputGate.terminalClaimed() {
			return false, nil
		}
		if !inputGate.terminalExposed() {
			select {
			case <-inputGate.exposureDone:
			case <-inputGate.terminal:
				return true, abortAfterTerminal("awaiting terminal exposure")
			case <-ctx.Done():
				return true, ctx.Err()
			}
		}
		if inputGate.terminalObserved() {
			return true, abortAfterTerminal("awaiting terminal exposure")
		}
		if !inputGate.terminalExposed() {
			// The client-facing terminal write failed after it began. The
			// upstream loop owns the already-decided query lifecycle, but this
			// side must still stop instead of reading another packet forever.
			select {
			case <-inputGate.delivered:
				return true, errors.New("deferred INSERT terminal could not be exposed to client")
			case <-ctx.Done():
				return true, ctx.Err()
			}
		}
		if prefetched != nil {
			r.prefetchedClient = prefetched
		}
		select {
		case <-inputGate.terminal:
			return true, abortAfterTerminal("awaiting upstream terminal")
		case <-inputGate.delivered:
			if inputGate.terminalObserved() {
				return true, abortAfterTerminal("awaiting upstream terminal")
			}
			return true, nil
		case <-ctx.Done():
			return true, ctx.Err()
		}
	}
	for {
		if inputGate.terminalObserved() {
			return abortAfterTerminal("awaiting upstream terminal")
		}
		select {
		case <-inputGate.terminal:
			return abortAfterTerminal("awaiting upstream terminal")
		case <-inputGate.delivered:
			if inputGate.terminalObserved() {
				return abortAfterTerminal("awaiting upstream terminal")
			}
			return nil
		case <-ctx.Done():
			return abortFromClient(ctx.Err())
		default:
		}

		// Do not leave the client side parked once its payload terminator has
		// been written. A pending INSERT may otherwise outlive a disconnected
		// client forever. The bounded wait is only a cancellation polling
		// mechanism; unlike payload classification, it never changes protocol
		// meaning.
		available, err := client.WaitForPacketStart(25 * time.Millisecond)
		if err != nil {
			if handled, exposureErr := awaitTerminalExposure(nil); handled {
				return exposureErr
			}
			select {
			case <-inputGate.delivered:
				if inputGate.terminalObserved() {
					return abortAfterTerminal("awaiting upstream terminal")
				}
				return nil
			default:
				return abortFromClient(err)
			}
		}
		if !available {
			continue
		}
		pkt, decErr := client.ReadPacket(uint64(chproto.ClientQueryCode))
		if handled, exposureErr := awaitTerminalExposure(&clientPacketResult{pkt: pkt, err: decErr}); handled {
			return exposureErr
		}
		select {
		case <-inputGate.delivered:
			if inputGate.terminalObserved() {
				return abortAfterTerminal("awaiting upstream terminal")
			}
			// The upstream terminal became visible while this packet was being
			// framed. Preserve it for the ordinary client loop; it belongs to
			// the now-reusable session rather than the completed INSERT.
			r.prefetchedClient = &clientPacketResult{pkt: pkt, err: decErr}
			return nil
		default:
		}
		if decErr != nil && pkt == nil {
			return abortFromClient(decErr)
		}
		if pkt != nil && pkt.Type == uint64(chproto.ClientCancelCode) {
			return abortFromClient(errors.New("client cancelled deferred INSERT"))
		}
		if decErr != nil {
			return abortFromClient(decErr)
		}
		if pkt == nil {
			return abortFromClient(io.EOF)
		}
		return abortFromClient(fmt.Errorf("client sent packet type %d (%s) before deferred INSERT terminal", pkt.Type, clientPacketName(pkt.Type)))
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

// upstreamToClient frames every server packet before relaying it. Query
// lifecycle hooks are therefore driven by decoded packet types, not TCP read
// boundaries: a payload byte equal to EndOfStream cannot be mistaken for
// success, and coalesced Progress+EndOfStream packets remain two events.
func (r *Relay) upstreamToClient(ctx context.Context) error {
	client := r.sess.Client()
	// A deferred INSERT may be waiting for this loop to consume the upstream
	// sample block; never leave it parked when this loop exits.
	defer func() {
		// Settle the sample first so runDeferredInsert can relinquish writer
		// ownership before the terminal fallback waits for done.
		r.settleDeferredSample(deferredSampleResult{err: errors.New("upstream relay loop exited")})
		r.unblockDeferredInputOnServerExit(ctx)
	}()
	for {
		up := r.sess.Upstream()
		if up == nil {
			return chsession.ErrNoUpstream
		}

		pkt, err := up.ReadPacket(uint64(chproto.ServerExceptionCode))
		if err != nil {
			if errors.Is(err, chproto.ErrUnsupportedResultType) && pkt != nil {
				err = r.relayOpaqueResult(ctx, up, pkt)
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
			if g := r.currentDeferredInput(); g != nil {
				if !g.claimUpstreamLifecycle() {
					return fmt.Errorf("upstream read failed after deferred writer owned lifecycle: %w", err)
				}
				// Wake a writer that is still negotiating the sample before
				// waiting for it. Fatal ordering is always abort -> writer stopped
				// -> one complete; never complete -> abort.
				r.settleDeferredSample(deferredSampleResult{err: err})
				if stopErr := r.stopDeferredInput(ctx, up, g); stopErr != nil {
					return stopErr
				}
				r.sess.State().ClearActiveRewrite()
				r.takeActiveQuery()
				g.complete(ctx, r.hooks, r.sess)
				r.deliverDeferredInput(g)
				if errors.Is(err, io.EOF) {
					return io.EOF
				}
				return fmt.Errorf("upstream read packet: %w", err)
			}
			if _, active := r.takeActiveQuery(); active {
				r.sess.State().ClearActiveRewrite()
				r.hooks.OnQueryComplete(ctx, r.sess)
			}
			if errors.Is(err, io.EOF) {
				return io.EOF
			}
			return fmt.Errorf("upstream read packet: %w", err)
		}

		// The packet decode boundary is the lifecycle arbitration boundary. Once
		// EOS/Exception has been identified, claim upstream ownership before any
		// logging, metrics observer, hook, or writer wait can block. Conversely,
		// a writer abort that won before decode makes this terminal a loser that
		// must never be forwarded or treated as reusable success.
		waitingDeferredSample := r.deferredSampleArmed()
		inputGateAtPacket := r.currentDeferredInput()
		deferredTerminatorAtDecode := inputGateAtPacket != nil && inputGateAtPacket.terminatorWritten()
		isEndOfStream := pkt.Type == uint64(chproto.ServerEndOfStreamCode)
		isException := pkt.Type == uint64(chproto.ServerExceptionCode)
		if inputGateAtPacket != nil && (isEndOfStream || isException) {
			if !inputGateAtPacket.claimUpstreamLifecycle() {
				return fmt.Errorf("deferred writer owned lifecycle before upstream %s decode", serverPacketName(pkt.Type))
			}
			if isException || isEndOfStream && !waitingDeferredSample && deferredTerminatorAtDecode {
				inputGateAtPacket.publishUpstreamTerminal()
			}
		}

		packetName := serverPacketName(pkt.Type)
		_, logger := log.FromContext(ctx)
		logger.Debugw("upstream packet relayed to client",
			"bytes", pkt.RawLen,
			"type", packetName,
		)
		if r.obs != nil {
			r.obs.BytesTransferred("upstream_to_client", float64(pkt.RawLen))
			r.obs.ServerPacket(packetName)
		}

		if waitingDeferredSample {
			switch pkt.Type {
			case uint64(chproto.ServerDataCode):
				// The upstream's INSERT sample block for a deferred INSERT: the
				// client already received housegate's locally synthesized sample
				// and is past its data phase, so this one is consumed here.
				r.settleDeferredSample(deferredSampleResult{})
				logger.Debugw("deferred INSERT upstream sample block consumed")
				continue
			case uint64(chproto.ServerTableColumnsCode):
				// ClickHouse sends TableColumns ahead of the sample when
				// input_format_defaults_for_omitted_fields is on; the client
				// would treat it as an unexpected packet after its data phase.
				continue
			case uint64(chproto.ServerEndOfStreamCode):
				err := fmt.Errorf("%w: upstream ended a deferred INSERT before its sample block", chproto.ErrMalformed)
				r.settleDeferredSample(deferredSampleResult{err: err})
				if inputGateAtPacket != nil {
					if stopErr := r.stopDeferredInput(ctx, up, inputGateAtPacket); stopErr != nil {
						return stopErr
					}
					r.sess.State().ClearActiveRewrite()
					r.takeActiveQuery()
					inputGateAtPacket.complete(ctx, r.hooks, r.sess)
					r.deliverDeferredInput(inputGateAtPacket)
				} else if _, active := r.takeActiveQuery(); active {
					r.hooks.OnQueryComplete(ctx, r.sess)
				}
				return err
			case uint64(chproto.ServerExceptionCode):
				if exc, ok := pkt.Decoded.(*chproto.Exception); ok {
					r.settleDeferredSample(deferredSampleResult{exception: exc})
				} else {
					r.settleDeferredSample(deferredSampleResult{err: fmt.Errorf("upstream exception packet decoded as %T", pkt.Decoded)})
				}
				// Fall through: the Exception is forwarded to the client and the
				// terminal hooks run exactly as for any other query.
			}
		}

		deferredTerminal := inputGateAtPacket
		deferredTerminalFatal := false
		if isException && deferredTerminal != nil {
			if waitingDeferredSample && deferredTerminal.markerWritten() {
				// A sample-step rejection is recoverable: no payload write began.
				// The complete marker write proves no client→upstream write is
				// still blocked behind the Exception. Abort before completion,
				// wake runDeferredInsert through the sample gate above, then wait
				// for its writer ownership to end.
				deferredTerminal.abort(ctx, r.hooks)
				select {
				case <-deferredTerminal.done:
				case <-ctx.Done():
					deferredTerminal.stopWriter()
					closeCodec(up)
					deferredTerminal.abort(ctx, r.hooks)
					return ctx.Err()
				}
			} else if isSessionPreservingBackpressureException(pkt.Decoded) {
				// A server-mode Housegate emits this narrow wire class only after
				// it consumed the complete staged input and returned its own upstream
				// connection to a framed terminal boundary. Wait for our writer-side
				// input hook, then abort this query without poisoning the agent
				// session. Generic ClickHouse payload exceptions remain fatal below.
				select {
				case <-deferredTerminal.done:
				case <-ctx.Done():
					deferredTerminal.stopWriter()
					closeCodec(up)
					deferredTerminal.abort(ctx, r.hooks)
					return ctx.Err()
				}
				deferredTerminal.abort(ctx, r.hooks)
			} else {
				// An Exception before the marker write completes can deadlock a
				// backpressured writer just like a post-sample Exception. Neither
				// case proves that ClickHouse consumed a complete input stream, so
				// stop/close the writer and fail the session.
				if err := r.stopDeferredInput(ctx, up, deferredTerminal); err != nil {
					return err
				}
				deferredTerminalFatal = true
			}
		}
		if isEndOfStream && deferredTerminal != nil {
			if !deferredTerminatorAtDecode {
				// The decode-time snapshot is authoritative. A write that happens
				// to finish later cannot turn an already-observed premature EOS
				// into success.
				err := fmt.Errorf("%w: upstream ended deferred INSERT before the terminator was written", chproto.ErrMalformed)
				if stopErr := r.stopDeferredInput(ctx, up, deferredTerminal); stopErr != nil {
					return stopErr
				}
				r.sess.State().ClearActiveRewrite()
				r.takeActiveQuery()
				deferredTerminal.complete(ctx, r.hooks, r.sess)
				r.deliverDeferredInput(deferredTerminal)
				return err
			}
			// The decode-time terminal decision owns the query before waiting for
			// the writer's input-complete hook. Client EOF/Cancel in that window
			// must not install a contradictory abort outcome.
			// A fast upstream can send EOS immediately after the local
			// terminator write. Wait for input-complete before success/completion.
			select {
			case <-deferredTerminal.done:
			case <-ctx.Done():
				deferredTerminal.stopWriter()
				closeCodec(up)
				deferredTerminal.abort(ctx, r.hooks)
				return ctx.Err()
			}
		}
		var rewrittenException *chproto.Exception
		if isException {
			exc, ok := pkt.Decoded.(*chproto.Exception)
			if !ok {
				// Keep this defensive branch resource-safe even though Codec
				// currently guarantees decoded Exception packets have this type.
				r.sess.State().ClearActiveRewrite()
				if deferredTerminal != nil {
					deferredTerminal.abort(ctx, r.hooks)
					r.takeActiveQuery()
					deferredTerminal.complete(ctx, r.hooks, r.sess)
					deferredTerminal.finishTerminalExposure(false)
					r.deliverDeferredInput(deferredTerminal)
				} else if _, active := r.takeActiveQuery(); active {
					r.hooks.OnQueryComplete(ctx, r.sess)
				}
				return fmt.Errorf("upstream exception packet decoded as %T", pkt.Decoded)
			}
			rewrittenException = exc
		}
		if rewrittenException != nil {
			if err := r.hooks.OnException(ctx, r.sess, rewrittenException); err != nil {
				logger.Warne(err, "OnException hook returned error")
			}
		}

		if isEndOfStream || isException {
			r.sess.State().ClearActiveRewrite()
			queryID, canceled, active := r.takeActiveQueryState()
			rejection := r.takePendingRejection(queryID)
			if active && isEndOfStream && !canceled && rejection == nil {
				r.hooks.OnQuerySuccess(ctx, r.sess, queryID)
			}
			if deferredTerminal != nil {
				deferredTerminal.complete(ctx, r.hooks, r.sess)
			} else if active {
				r.hooks.OnQueryComplete(ctx, r.sess)
			}
			if rejection != nil {
				if rewrittenException != nil {
					logger.Debugw("upstream exception superseded by a local rejection",
						"query_id", queryID, "upstream_code", rewrittenException.Code)
				}
				rewrittenException = rejection
			}
		}

		// Terminal hooks run before terminal bytes are exposed to the client.
		// A client may submit its next query immediately after reading them.
		if rewrittenException != nil {
			if err := client.WriteException(rewrittenException); err != nil {
				if deferredTerminal != nil {
					deferredTerminal.finishTerminalExposure(false)
					r.deliverDeferredInput(deferredTerminal)
				}
				return fmt.Errorf("write rewritten exception to client: %w", err)
			}
			if deferredTerminal != nil {
				deferredTerminal.finishTerminalExposure(true)
				r.deliverDeferredInput(deferredTerminal)
				if deferredTerminalFatal {
					return fmt.Errorf("deferred INSERT upstream terminated after sample: code=%d %s", rewrittenException.Code, rewrittenException.Message)
				}
			}
			continue
		}
		if err := client.WriteRawPacket(pkt.Raw); err != nil {
			if deferredTerminal != nil {
				if isEndOfStream {
					deferredTerminal.finishTerminalExposure(false)
				}
				if !isEndOfStream && !isException {
					// A post-sample Progress/Profile/Data packet is part of the
					// active deferred query. If it cannot be exposed to the client,
					// the connection is no longer recoverable: stop the upstream,
					// abort, and complete exactly once before releasing the gate.
					if !deferredTerminal.claimUpstreamLifecycle() {
						return fmt.Errorf("splice upstream packet after deferred writer owned lifecycle: %w", err)
					}
					if waitingDeferredSample {
						r.settleDeferredSample(deferredSampleResult{err: fmt.Errorf("forward pre-sample server packet to client: %w", err)})
					}
					if stopErr := r.stopDeferredInput(ctx, up, deferredTerminal); stopErr != nil {
						return stopErr
					}
					r.sess.State().ClearActiveRewrite()
					r.takeActiveQuery()
					deferredTerminal.complete(ctx, r.hooks, r.sess)
				}
				r.deliverDeferredInput(deferredTerminal)
			}
			return fmt.Errorf("splice upstream packet to client: %w", err)
		}
		if isEndOfStream && deferredTerminal != nil {
			deferredTerminal.finishTerminalExposure(true)
			r.deliverDeferredInput(deferredTerminal)
		}
	}
}

func (r *Relay) relayOpaqueResult(ctx context.Context, up *chproto.Codec, first *chproto.Packet) error {
	queryID, active := r.currentActiveQuery()
	if !active {
		return fmt.Errorf("opaque result without an active query")
	}
	if r.sess.State().Snapshot().CommitGateEvent != nil {
		// Do not wrap ErrUnsupportedResultType here: the caller uses that
		// sentinel to enter this fallback, so exposing it again makes a future
		// refactor vulnerable to recursively re-entering opaque mode.
		return fmt.Errorf(
			"%w: opaque result for commit-gated query %q (%v)",
			chproto.ErrMalformed,
			queryID,
			chproto.ErrUnsupportedResultType,
		)
	}
	client := r.sess.Client()
	if client.ChunkedSendEnabled() {
		return fmt.Errorf("%w: opaque result cannot enter raw mode on a chunked downstream", chproto.ErrMalformed)
	}
	if err := r.startOpaqueCompletionProbe(ctx, queryID); err != nil {
		return err
	}

	if len(first.Raw) == 0 {
		return fmt.Errorf("opaque result has no captured prefix")
	}
	if err := writeAll(client.Conn(), first.Raw); err != nil {
		return fmt.Errorf("write opaque result prefix: %w", err)
	}
	if r.obs != nil {
		r.obs.BytesTransferred("upstream_to_client", float64(len(first.Raw)))
		r.obs.ServerPacket(serverPacketName(first.Type))
	}

	buf := make([]byte, 128*1024)
	for {
		n, readErr := up.ReadRaw(buf)
		if n > 0 {
			if err := writeAll(client.Conn(), buf[:n]); err != nil {
				return fmt.Errorf("write opaque result stream: %w", err)
			}
			if r.obs != nil {
				r.obs.BytesTransferred("upstream_to_client", float64(n))
			}
		}
		if readErr != nil {
			return readErr
		}
	}
}

func writeAll(w io.Writer, p []byte) error {
	for len(p) > 0 {
		n, err := w.Write(p)
		if err != nil {
			return err
		}
		if n <= 0 {
			return io.ErrShortWrite
		}
		p = p[n:]
	}
	return nil
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

// exceptionForPluginError maps a plugin error to the synthetic Exception the
// client sees. ClientError selects an explicit code and message; all other
// plugin rejections keep the generic 403 behavior.
func exceptionForPluginError(pluginErr error) *chproto.Exception {
	var clientErr *chproto.ClientError
	if errors.As(pluginErr, &clientErr) {
		return &chproto.Exception{Code: proto.Error(clientErr.Code), Name: "DB::Exception", Message: clientErr.Message}
	}
	return &chproto.Exception{
		Code:    403, // ClickHouse AUTHENTICATION_FAILED; generic plugin-reject
		Name:    "DB::Exception",
		Message: pluginErr.Error(),
	}
}

// isSessionPreservingBackpressureException recognises the on-wire projection
// of a KeepSession ClientError. The ClickHouse Exception frame has no metadata
// bit for this property, so the contract is deliberately narrower than code
// 252 alone: only Housegate's storage-integrity back-pressure prefix qualifies.
// A native ClickHouse TOO_MANY_PARTS or any other late payload exception stays
// fail-closed because it does not prove the INSERT stream was fully consumed.
func isSessionPreservingBackpressureException(decoded any) bool {
	exc, ok := decoded.(*chproto.Exception)
	return ok && exc != nil && int32(exc.Code) == chproto.CodeTooManyParts &&
		strings.HasPrefix(strings.TrimSpace(exc.Message), "storage_integrity: back-pressure:")
}

// writeExceptionToClient converts a plugin error into a synthetic ClickHouse
// Exception and writes it to the client side.
func (r *Relay) writeExceptionToClient(ctx context.Context, pluginErr error) {
	if err := r.sess.Client().WriteException(exceptionForPluginError(pluginErr)); err != nil {
		_, logger := log.FromContext(ctx)
		logger.Warne(err, "failed to write plugin-error exception")
	}
}
