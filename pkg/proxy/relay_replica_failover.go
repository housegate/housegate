package proxy

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/log"
	"github.com/housegate/housegate/pkg/plugin"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

// The failover port of spec 2026-10-14 §11 (Plan S3-C4 K46), defined in
// pkg/plugin so that a plugin package (agent.ReplicaFailover) implements it
// without importing pkg/proxy.
type (
	FailoverPhase    = plugin.FailoverPhase
	UpstreamFailover = plugin.UpstreamFailover
	FailoverReporter = plugin.FailoverReporter
)

const (
	FailoverAtHello = plugin.FailoverAtHello
	FailoverAtRead  = plugin.FailoverAtRead
)

// replicaFailoverObserver is the metrics side of a failover, implemented by
// MetricsObserver.
type replicaFailoverObserver interface {
	ReplicaFailover(reason string)
	ReplicaFailoverExhausted(reason string)
}

// replicaFailoverAttemptTimeout bounds one failover step: the candidate
// lookup, and each candidate's dial, handshake and resent query.
const replicaFailoverAttemptTimeout = 10 * time.Second

// failoverQuery is the failover state of the session's active query, guarded
// by Relay.failoverMu. Only a SELECT, WITH or standalone USE of a session
// that is still ReplicaFailoverEligible is armed.
type failoverQuery struct {
	query      *chproto.Query // the Query packet exactly as it was written upstream
	terminator []byte         // the raw terminating empty Data block, once it was written upstream
	read       bool           // a SELECT or WITH that may still fail over at its first answer
	use        string         // a USE target, recorded as the session database at its EndOfStream
	answered   bool           // a packet of its answer was relayed to the client
	moving     *readFailover  // the read failover under way, once a candidate took the read
}

// readFailover is a read failover under way: a candidate took the read and
// its first answer decides whether it serves. It keeps the candidates not
// tried yet, the owner's failure the client gets when none serves (its
// Exception packet, or its read error) and what to report once one serves.
type readFailover struct {
	database, reason, from, to string
	remaining                  []string
	original                   *chproto.Packet
	originalErr                error
	failures                   []error
}

type failoverStatement uint8

const (
	failoverOther failoverStatement = iota
	failoverRead
	failoverUse
)

// classifyFailoverStatement returns failoverRead for a statement starting
// with SELECT, or with a WITH list whose main statement is a SELECT,
// failoverUse and its target for a standalone USE, and failoverOther for
// everything else, an unreadable statement included.
func classifyFailoverStatement(sql string) (failoverStatement, string) {
	words, err := sicore.LeadingKeywords(sql, 1)
	if err != nil || len(words) == 0 {
		return failoverOther, ""
	}
	switch words[0] {
	case "SELECT":
		return failoverRead, ""
	case "WITH":
		if sicore.WithListLeadsASelect(sql) {
			return failoverRead, ""
		}
	case "USE":
		if db, ok, err := sicore.ParseUseDatabaseStrict(sql); err == nil && ok && db != "" {
			return failoverUse, db
		}
	}
	return failoverOther, ""
}

// noteFailoverStatement clears the session's failover eligibility at the
// first statement that is not a read (a SELECT, or a WITH list leading a
// SELECT) or a standalone USE, or that runs under an ownership plan (every
// signed INSERT lane, query-only and abort plans): replaying the hello and one read on a new upstream must reproduce
// the session exactly.
func (r *Relay) noteFailoverStatement(qctx *plugin.QueryContext) {
	kind, _ := classifyFailoverStatement(qctx.Query.Body)
	planned := qctx.AgentPrepare != nil || qctx.QueryOnly != nil || qctx.SynthesizedInsert != nil ||
		qctx.DeferredInsert != nil || qctx.SuppressUpstreamExecution || qctx.AbortWithSuccess
	if kind == failoverOther || planned {
		r.sess.State().ReplicaFailoverEligible = false
	}
}

// writeQuery writes the Query of an ordinary forwarded query. With an
// UpstreamFailover it arms the query's failover state while the session is
// eligible, and writes under failoverMu, so a read failover never races it.
func (r *Relay) writeQuery(up *chproto.Codec, qctx *plugin.QueryContext) error {
	if r.failover == nil {
		return up.WriteQuery(qctx.Query)
	}
	r.failoverMu.Lock()
	defer r.failoverMu.Unlock()
	r.failoverQuery = nil
	if kind, db := classifyFailoverStatement(qctx.Query.Body); kind != failoverOther && r.sess.State().ReplicaFailoverEligible {
		r.failoverQuery = &failoverQuery{query: qctx.Query, read: kind == failoverRead, use: db}
	}
	return up.WriteQuery(qctx.Query)
}

// spliceClientPacket writes a client packet upstream. With an
// UpstreamFailover it writes to the session's current upstream under
// failoverMu, because a read failover may have replaced up, keeps the armed
// read's terminating empty Data block for a resend, and disarms the query at
// any other packet: client Data also ends the session's eligibility, and so
// does a USE whose outcome the relay can no longer follow.
func (r *Relay) spliceClientPacket(up *chproto.Codec, pkt *chproto.Packet, inputComplete bool) error {
	if r.failover == nil {
		return up.WriteRawPacket(pkt.Raw)
	}
	r.failoverMu.Lock()
	defer r.failoverMu.Unlock()
	if current := r.sess.Upstream(); current != nil {
		up = current
	}
	isData := pkt.Type == uint64(chproto.ClientDataCode)
	fq := r.failoverQuery
	switch {
	case fq != nil && isData && inputComplete && fq.terminator == nil:
		fq.terminator = append([]byte(nil), pkt.Raw...)
	case fq != nil:
		if isData || fq.use != "" {
			r.sess.State().ReplicaFailoverEligible = false
		}
		r.failoverQuery = nil
	case isData && !inputComplete:
		r.sess.State().ReplicaFailoverEligible = false
	}
	return up.WriteRawPacket(pkt.Raw)
}

// failoverRead runs before upstreamToClient handles a packet or a read error
// of up, and returns what the loop handles instead. It returns true once the
// armed read moved to a replica host that has been sent its Query and
// terminator: the loop then reads the answer from the new upstream and drops
// this packet or error. Otherwise it returns false with the packet or error
// the loop handles exactly as without a failover: the one it was given, or,
// once every candidate that took the read failed it too, the owner's original
// refusal or read error. A candidate whose first answer is itself a failure
// the failover moves on (the quarantined owner's refusal it relays, or a
// connection that broke before any packet) leaves the read to the next
// candidate, so a reachable host that cannot serve the read never ends the
// failover. The failover counts as served, and is reported, only at the first
// answer that is not such a failure. It never fails over once a packet of the
// answer reached the client.
func (r *Relay) failoverRead(ctx context.Context, up *chproto.Codec, pkt *chproto.Packet, readErr error) (bool, *chproto.Packet, error) {
	r.failoverMu.Lock()
	fq := r.failoverQuery
	if fq == nil {
		r.failoverMu.Unlock()
		return false, pkt, readErr
	}
	var exc *chproto.Exception
	failed := false
	switch {
	case readErr != nil:
		failed = fq.read && !fq.answered && !errors.Is(readErr, chproto.ErrUnsupportedResultType)
	case pkt.Type == uint64(chproto.ServerExceptionCode):
		exc, _ = pkt.Decoded.(*chproto.Exception)
		failed = exc != nil && fq.read && !fq.answered
	}
	reason := ""
	if failed {
		var ok bool
		reason, ok = r.failover.Reason(FailoverAtRead, readErr, exc)
		failed = ok && !r.closing.Load() && ctx.Err() == nil
	}
	if !failed {
		// The read's own answer, the owner's or the serving candidate's.
		mv := fq.moving
		fq.moving = nil
		switch {
		case readErr != nil && errors.Is(readErr, chproto.ErrUnsupportedResultType):
			fq.answered = true
		case readErr != nil, pkt.Type == uint64(chproto.ServerExceptionCode):
			r.failoverQuery = nil
		case pkt.Type == uint64(chproto.ServerEndOfStreamCode):
			if fq.use != "" {
				r.sess.State().SetLogicalDatabase(fq.use)
			}
			r.failoverQuery = nil
		default:
			fq.answered = true
		}
		r.failoverMu.Unlock()
		if mv != nil {
			r.failedOver(ctx, mv.database, mv.reason, mv.from, mv.to)
		}
		return false, pkt, readErr
	}
	mv := fq.moving
	if mv == nil {
		database := r.failoverDatabase(nil)
		addrs, err := r.failoverCandidates(ctx, database)
		if err != nil {
			r.failoverQuery = nil
			r.failoverMu.Unlock()
			r.failoverExhausted(ctx, database, reason, err)
			return false, pkt, readErr
		}
		mv = &readFailover{database: database, reason: reason, from: failedUpstreamAddr(up, readErr), remaining: addrs, original: pkt, originalErr: readErr}
		fq.moving = mv
	} else {
		mv.failures = append(mv.failures, fmt.Errorf("%s: %s", mv.to, describeReadFailure(readErr, exc)))
	}
	to, err := r.readFailoverTo(ctx, fq, mv)
	if err != nil {
		r.failoverQuery = nil
		r.failoverMu.Unlock()
		r.failoverExhausted(ctx, mv.database, mv.reason, errors.Join(append(mv.failures, err)...))
		return false, mv.original, mv.originalErr
	}
	mv.to = to
	r.failoverMu.Unlock()
	return true, nil, nil
}

// describeReadFailure names a candidate's failed first answer for the
// exhausted log line.
func describeReadFailure(readErr error, exc *chproto.Exception) string {
	if exc != nil {
		return fmt.Sprintf("answered code=%d %s", exc.Code, exc.Message)
	}
	return fmt.Sprintf("broke before answering: %v", readErr)
}

// readFailoverTo prepares the remaining candidates of mv in turn (dial, the
// session's hello with the failover database, the same revision and timezone
// the client received, then the armed Query and its terminator), consuming
// each, and rebinds the session to the first that takes them. The failed
// upstream stays bound until then. The caller holds failoverMu.
func (r *Relay) readFailoverTo(ctx context.Context, fq *failoverQuery, mv *readFailover) (string, error) {
	var errs []error
	for len(mv.remaining) > 0 {
		addr := mv.remaining[0]
		mv.remaining = mv.remaining[1:]
		up, sent, err := r.prepareReadCandidate(ctx, addr, mv.database, fq)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", addr, err))
			continue
		}
		if err := r.sess.RebindUpstream(ctx, up, false); err != nil {
			closeCodec(up)
			errs = append(errs, fmt.Errorf("%s: %w", addr, err))
			continue
		}
		if r.closing.Load() {
			// The session closed while the candidate was prepared; its Close
			// may already have run on the previous upstream.
			closeCodec(up)
			return "", errors.New("the session closed during the failover")
		}
		r.sess.State().SetUpstreamHello(sent)
		return addr, nil
	}
	if len(errs) == 0 {
		errs = append(errs, errors.New("no candidate left"))
	}
	return "", errors.Join(errs...)
}

func (r *Relay) prepareReadCandidate(ctx context.Context, addr, database string, fq *failoverQuery) (*chproto.Codec, *chproto.ClientHello, error) {
	state := r.sess.State()
	snap := state.Snapshot()
	hello := state.UpstreamHello()
	if hello == nil {
		return nil, nil, errors.New("the session has no replayable hello")
	}
	hello.Database = database
	hello.ProtocolVersion = snap.ClientRevision
	attemptCtx, cancel := context.WithTimeout(ctx, replicaFailoverAttemptTimeout)
	defer cancel()
	up, err := dialFailoverCandidate(attemptCtx, addr)
	if err != nil {
		return nil, nil, err
	}
	conn, _ := up.Conn().(net.Conn)
	if deadline, ok := attemptCtx.Deadline(); ok && conn != nil {
		_ = conn.SetDeadline(deadline)
	}
	sent, _, srv, _, err := exchangeUpstreamHello(up, hello)
	if err == nil {
		rev := int(sent.ProtocolVersion)
		if int(srv.Revision) < rev {
			rev = int(srv.Revision)
		}
		switch {
		case rev != snap.ClientRevision:
			err = fmt.Errorf("negotiated revision %d, the client leg uses %d", rev, snap.ClientRevision)
		case snap.Timezone != "" && srv.Timezone != snap.Timezone:
			err = fmt.Errorf("server timezone %q, the client received %q", srv.Timezone, snap.Timezone)
		default:
			up.SetRevision(rev)
			if chproto.SupportsAddendum(rev) {
				res := up.ResolveUpstreamAddendum(chproto.AddendumResult{}, chproto.AddendumOpts{
					ProposedRecv: "chunked_optional",
					ProposedSend: "chunked_optional",
				})
				err = up.SendAddendum(res)
			}
		}
	}
	if err == nil {
		up.SetCompression(fq.query.Compression)
		err = up.WriteQuery(fq.query)
	}
	if err == nil && fq.terminator != nil {
		err = up.WriteRawPacket(fq.terminator)
	}
	if err != nil {
		closeCodec(up)
		return nil, nil, err
	}
	if conn != nil {
		_ = conn.SetDeadline(time.Time{})
	}
	return up, sent, nil
}

// failoverAtHello moves a session whose first upstream failed phase 2 of the
// handshake (cause; exc when it answered an Exception instead of a
// ServerHello) to the first candidate that completes phase 2 with the
// session's ClientHello, its Database set to the failover database, and binds
// it in place of the failed upstream (nil when the dial failed). When Reason
// declines or every candidate fails it returns cause unchanged.
func (r *Relay) failoverAtHello(ctx context.Context, failed *chproto.Codec, hello *chproto.ClientHello, cause error, exc *chproto.Exception) (*chproto.Codec, *chproto.ClientHello, *chproto.Packet, *chproto.ServerHello, error) {
	reason, ok := r.failover.Reason(FailoverAtHello, cause, exc)
	if !ok {
		return failed, nil, nil, nil, cause
	}
	database := r.failoverDatabase(hello)
	from := failedUpstreamAddr(failed, cause)
	addrs, err := r.failoverCandidates(ctx, database)
	var errs []error
	if err != nil {
		errs = append(errs, err)
	}
	for _, addr := range addrs {
		candidate := *hello
		candidate.Database = database
		up, sent, srvPkt, srv, err := helloCandidate(ctx, addr, &candidate)
		if err == nil {
			if failed == nil {
				err = r.sess.BindUpstream(ctx, up)
			} else {
				err = r.sess.RebindUpstream(ctx, up, false)
			}
			if err != nil {
				closeCodec(up)
			}
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", addr, err))
			continue
		}
		r.failedOver(ctx, database, reason, from, addr)
		return up, sent, srvPkt, srv, nil
	}
	r.failoverExhausted(ctx, database, reason, errors.Join(errs...))
	return failed, nil, nil, nil, cause
}

func helloCandidate(ctx context.Context, addr string, hello *chproto.ClientHello) (*chproto.Codec, *chproto.ClientHello, *chproto.Packet, *chproto.ServerHello, error) {
	attemptCtx, cancel := context.WithTimeout(ctx, replicaFailoverAttemptTimeout)
	defer cancel()
	up, err := dialFailoverCandidate(attemptCtx, addr)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	conn, _ := up.Conn().(net.Conn)
	if deadline, ok := attemptCtx.Deadline(); ok && conn != nil {
		_ = conn.SetDeadline(deadline)
	}
	sent, srvPkt, srv, _, err := exchangeUpstreamHello(up, hello)
	if err != nil {
		closeCodec(up)
		return nil, nil, nil, nil, err
	}
	if conn != nil {
		_ = conn.SetDeadline(time.Time{})
	}
	return up, sent, srvPkt, srv, nil
}

// exchangeUpstreamHello is phase 2 of the handshake on upstream: it writes
// hello (revision-capped by ClientHelloForUpstream) and reads the
// ServerHello, returning the hello it sent. An Exception instead of a
// ServerHello is returned as exc, with the handshake's error.
func exchangeUpstreamHello(upstream *chproto.Codec, hello *chproto.ClientHello) (*chproto.ClientHello, *chproto.Packet, *chproto.ServerHello, *chproto.Exception, error) {
	upstreamHello := chproto.ClientHelloForUpstream(hello)
	if err := upstream.WriteClientHello(upstreamHello); err != nil {
		return nil, nil, nil, nil, fmt.Errorf("forward client hello: %w", err)
	}
	upstream.SetServerHelloRevisionHint(int(upstreamHello.ProtocolVersion))
	srvPkt, err := upstream.ReadPacket(uint64(chproto.ServerHelloCode), uint64(chproto.ServerExceptionCode))
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("read server hello: %w", err)
	}
	// Upstream may answer ClientHello with a ServerException instead — e.g.
	// invalid credentials forwarded by routing / peer-trust paths, or a
	// misconfigured peer port that lands on plain CH which doesn't recognise
	// our envelope user. Decode the Exception and surface its message so
	// operators don't see an opaque "decode failed".
	if exc, ok := srvPkt.Decoded.(*chproto.Exception); ok {
		return nil, nil, nil, exc, fmt.Errorf("upstream rejected handshake: code=%d %s: %s", exc.Code, exc.Name, exc.Message)
	}
	srv, ok := srvPkt.Decoded.(*chproto.ServerHello)
	if !ok {
		return nil, nil, nil, nil, fmt.Errorf("server hello: unexpected packet type=%d (want ServerHello=%d): %w",
			srvPkt.Type, chproto.ServerHelloCode, chproto.ErrDecode)
	}
	return upstreamHello, srvPkt, srv, nil, nil
}

// failoverCandidates asks the UpstreamFailover for the session database's
// candidates, bounded by replicaFailoverAttemptTimeout.
func (r *Relay) failoverCandidates(ctx context.Context, database string) ([]string, error) {
	lookupCtx, cancel := context.WithTimeout(ctx, replicaFailoverAttemptTimeout)
	defer cancel()
	addrs, err := r.failover.Candidates(lookupCtx, database)
	if err != nil {
		return nil, fmt.Errorf("replica hosts of %q: %w", database, err)
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("replica hosts of %q: no candidate", database)
	}
	return addrs, nil
}

// failoverDatabase is the database a failover replays and looks candidates up
// for: the session's logical database (the hello's, or the target of its last
// successful USE), else the hello's.
func (r *Relay) failoverDatabase(hello *chproto.ClientHello) string {
	state := r.sess.State()
	if db := state.LogicalDatabaseName(); db != "" {
		return db
	}
	if hello != nil {
		return hello.Database
	}
	return state.PhysicalDatabaseName()
}

func (r *Relay) failedOver(ctx context.Context, database, reason, from, to string) {
	if o, ok := r.obs.(replicaFailoverObserver); ok {
		o.ReplicaFailover(reason)
	}
	if rep, ok := r.failover.(FailoverReporter); ok {
		rep.FailedOver(ctx, database, reason, from, to)
	}
}

func (r *Relay) failoverExhausted(ctx context.Context, database, reason string, err error) {
	if o, ok := r.obs.(replicaFailoverObserver); ok {
		o.ReplicaFailoverExhausted(reason)
	}
	_, logger := log.FromContext(ctx)
	logger.Warnw("relay: no replica host served the failover; the client gets the original error",
		"database", database, "reason", reason, "err", err)
}

// failedUpstreamAddr is the failed upstream's configured address, its remote
// address, or, when the dial itself failed, the address in the dial error.
func failedUpstreamAddr(up *chproto.Codec, cause error) string {
	if up != nil {
		if named, ok := up.Conn().(interface{ UpstreamAddress() string }); ok {
			return named.UpstreamAddress()
		}
		return upstreamAddr(up)
	}
	var opErr *net.OpError
	if errors.As(cause, &opErr) && opErr.Addr != nil {
		return opErr.Addr.String()
	}
	return ""
}

// dialFailoverCandidate dials a replica host. Its conn reports the address it
// was dialed with, as the agent's own dialer's does, so the SI statement
// lane's upstream switch compares the same identity.
func dialFailoverCandidate(ctx context.Context, addr string) (*chproto.Codec, error) {
	conn, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	return chproto.NewCodec(&failoverConn{Conn: conn, address: addr}, chproto.DirToUpstream), nil
}

type failoverConn struct {
	net.Conn
	address string
}

func (c *failoverConn) UpstreamAddress() string { return c.address }
