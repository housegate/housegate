// Package rewrite implements the QueryPlugin / ExceptionPlugin /
// ConnLifecyclePlugin that funnels every query through the unified
// rewriter service.
//
// Lifecycle:
//   - OnConnect — no-op. Account is not yet known (auth runs in
//     OnQuery), so we cannot construct the per-connection Rewriter
//     here. We lazy-init in OnQuery.
//   - OnQuery — on the first query we build a Rewriter via the
//     Factory and cache it on the session via a sync.Map keyed by
//     session id. Every subsequent query reuses the same Rewriter so
//     RewriteErrorMessage has access to the most recent SQL.
//   - OnException — looks up the cached Rewriter and reverse-maps
//     the exception message.
//   - OnClose — evicts and Close()s the per-conn Rewriter.
//
// Every rewrite failure is returned to the client as an Exception, with or
// without storage integrity (spec 2026-09-26 T8): a *rewriter.RejectedError
// (every non-Success engine answer, every failure after the engine received
// the statement) and any unclassified error alike. The one exception is a
// *rewriter.UnavailableError (the engine provably never saw the statement)
// with FailOpenOnUnavailable set: the original SQL is then forwarded with a
// warning.
//
// Maintenance, platform-operator and peer-trusted sessions never reach the
// rewriter (see OnQuery and RunOnPeerTrust), so rejections and the switch do
// not apply to their queries; forwarded sessions are rewritten by the
// receiving host instead. RejectUndecodableQuery is the exception: it covers
// maintenance and platform-operator sessions too.
package rewrite

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	pb "github.com/housegate/rewriter-proto/gen/pb"

	"github.com/housegate/housegate/pkg/log"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/plugin"
	"github.com/housegate/housegate/pkg/rewriter"
	"github.com/housegate/housegate/pkg/sitable"
)

// Observer is the narrow metrics surface this plugin depends on.
// *proxy.MetricsObserver satisfies it.
type Observer interface {
	Rewritten(duration float64)
}

// Plugin is the unified rewriter plugin. It satisfies HelloPlugin,
// QueryPlugin, ExceptionPlugin, and the lifecycle hooks needed to
// manage per-connection Rewriters.
type Plugin struct {
	// Factory builds per-connection Rewriters. Required.
	Factory rewriter.Factory

	// PhysicalDatabase is the wire-level upstream database name. When
	// non-empty, OnHello rewrites hello.Database to this value and
	// records SessionState.Database = PhysicalDatabase, while leaving
	// SessionState.LogicalDatabase set to whatever the user typed.
	// Should be wired from the same source as the rewriter Factory's
	// Options.PhysicalDatabase so the two layers agree.
	PhysicalDatabase string

	// Observer is optional — when nil, timing is not emitted. The
	// rewrite call is timed regardless of success/failure so operators
	// can see fail-open latency on the same histogram.
	Observer Observer

	// FailOpenOnUnavailable is rewriter.fail_open_on_unavailable (spec
	// 2026-09-26 T8): a Rewrite error that is a *rewriter.UnavailableError
	// (the engine never received the statement) forwards the original SQL
	// with a warning instead of failing the query. It never applies to a
	// RejectedError or to any other error. buildServer sets it only
	// when storage integrity is disabled; Config.Validate refuses the switch
	// together with storage_integrity.enabled.
	FailOpenOnUnavailable bool

	// RequiredStorageIntegrityContractVersion is the defense-in-depth response
	// echo gate for every Rewriter implementation, including custom factories.
	RequiredStorageIntegrityContractVersion pb.StorageIntegrityContractVersion

	// TableState is the storage-integrity table-state port. Non-nil exactly
	// when storage_integrity.enabled: OnQuery then takes one snapshot per
	// query into QueryContext.TableSnapshot and the rewriter context, and
	// OnException scrubs protocol-owned names with the scrubber of the
	// session's last snapshot.
	TableState sitable.TableState

	// rewriters caches one Rewriter per session id (int64). Lifetime
	// is OnQuery-first-touch through OnClose.
	rewriters sync.Map // map[int64]rewriter.Rewriter

	// snapshots keeps each session's last query snapshot for OnException.
	snapshots sync.Map // map[int64]sitable.Snapshot

	scrubbers rewriter.StorageIntegrityScrubberCache
}

// OnHello mirrors hello.Database into SessionState.Database when
// PhysicalDatabase is configured (rewriting the wire value to the
// physical name) and otherwise records the database verbatim.
// SessionState.LogicalDatabase is set elsewhere (state plugin) — this
// hook only handles the upstream-facing physical name.
func (p *Plugin) OnHello(_ context.Context, sess chsession.Session, hello *chproto.ClientHello) error {
	if p.PhysicalDatabase != "" && hello.Database != "" && hello.Database != p.PhysicalDatabase {
		hello.Database = p.PhysicalDatabase
	}
	if hello.Database != "" {
		sess.State().SetDatabase(hello.Database)
	}
	return nil
}

// rewriterFor returns the cached Rewriter for sess, creating one if
// necessary. Always returns non-nil when Factory is wired.
func (p *Plugin) rewriterFor(sess chsession.Session) rewriter.Rewriter {
	if p.Factory == nil {
		return nil
	}
	id := sess.ID()
	if v, ok := p.rewriters.Load(id); ok {
		return v.(rewriter.Rewriter)
	}
	rw := p.Factory.NewRewriter(sess.State())
	actual, loaded := p.rewriters.LoadOrStore(id, rw)
	if loaded {
		// Lost the race; close the one we just built.
		_ = rw.Close()
	}
	return actual.(rewriter.Rewriter)
}

// OnQuery routes the query through the rewriter. Every error fails closed,
// except a *rewriter.UnavailableError under FailOpenOnUnavailable.
func (p *Plugin) OnQuery(ctx context.Context, qctx *plugin.QueryContext) error {
	if p.TableState != nil {
		// Spec 2026-09-24 H1: exactly one snapshot per query, taken here and
		// read by every later stage.
		tableSnap := p.TableState.Current()
		qctx.TableSnapshot = tableSnap
		ctx = rewriter.WithTableSnapshot(ctx, tableSnap)
		if qctx.Session != nil {
			p.snapshots.Store(qctx.Session.ID(), tableSnap)
		}
	}
	if qctx.Session != nil {
		snap := qctx.Session.State().Snapshot()
		// Maintenance sessions (indexer-signed) and platform-operator
		// sessions bypass rewrite — Query.Body is forwarded verbatim by
		// the relay. RewrittenSQL mirrors OriginalSQL so downstream
		// plugins / observers (commitgate Event, audit logs) that read
		// it see a coherent value rather than empty. The rewriter is
		// never consulted, so the fail-closed rejection policy (spec
		// 2026-09-26 T8) does not apply to these sessions; the bypass
		// is deliberate and T8 leaves it unchanged.
		//
		// Driver sessions (snap.IsDriver) deliberately do NOT bypass
		// rewrite — that's the whole reason IsDriver exists as a
		// separate flag from Maintenance. Drivers write logical names
		// (e.g. CREATE DATABASE x2y2_0 / INSERT INTO x2y2_0.events) and
		// rely on this plugin to translate them to physical names. Treat
		// driver traffic like normal user traffic for the rewrite step.
		if snap.Maintenance || snap.PlatformOperator {
			qctx.RewrittenSQL = qctx.OriginalSQL
			return nil
		}
	}
	if qctx.Query != nil {
		mode, ok, err := readModeFromQuery(qctx.Query)
		if err != nil {
			return err
		}
		if ok {
			ctx = rewriter.WithReadMode(ctx, mode)
		}
	}
	rw := p.rewriterFor(qctx.Session)
	if rw == nil {
		return nil
	}
	// Effective principal: when the auth plugin validated an
	// operator-on-behalf-of-owner relation it stamped qctx.Owner; the
	// rewriter's database_map must then be built from the OWNER's perms
	// so the operator can SELECT/INSERT against the owner's logical DBs.
	// Falls back to the JWS signer (Session.Account()) when no Owner is
	// in flight — the legacy single-principal path.
	effectiveAccount := qctx.Owner
	if effectiveAccount == "" {
		effectiveAccount = qctx.Session.State().Account()
	}
	start := time.Now()
	res, err := rw.Rewrite(ctx, qctx.OriginalSQL, effectiveAccount)
	if p.Observer != nil {
		p.Observer.Rewritten(time.Since(start).Seconds())
	}
	_, logger := log.FromContext(ctx)
	if err != nil {
		var rej *rewriter.RejectedError
		if errors.As(err, &rej) {
			if rej.Cause != nil {
				// The cause (rewriter address, engine internals) is logged
				// here only; the client sees rej.Message (review L4).
				logger.Warnw("rewriter: statement rejected (fail-closed)", "code", rej.Code.String(), "message", rej.Message, "cause", rej.Cause.Error())
			} else {
				logger.Infow("rewriter: statement rejected (fail-closed)", "code", rej.Code.String(), "message", rej.Message)
			}
			return rej
		}
		// Only a failure the rewriter proved happened before the engine saw
		// the statement may fail open (review M1); any other error, e.g. from
		// a caller-injected Rewriter, may be statement-dependent.
		var unavailable *rewriter.UnavailableError
		isUnavailable := errors.As(err, &unavailable)
		if isUnavailable && p.FailOpenOnUnavailable {
			logger.Warne(err, "rewriter: rewriter unavailable; forwarding original SQL (rewriter.fail_open_on_unavailable)")
			return nil
		}
		message := "rewriter failed to process the statement"
		if isUnavailable {
			message = "rewriter unavailable"
		}
		logger.Errorw("rewriter: rewrite failed (fail-closed)", "error", err)
		return &rewriter.RejectedError{Code: pb.RewriteCode_RewriteError, Message: message, Cause: err}
	}
	if p.RequiredStorageIntegrityContractVersion != pb.StorageIntegrityContractVersion_STORAGE_INTEGRITY_CONTRACT_UNSPECIFIED &&
		res.StorageIntegrityContractVersion != p.RequiredStorageIntegrityContractVersion {
		return &rewriter.RejectedError{Code: pb.RewriteCode_RewriteError,
			Message: fmt.Sprintf("storage-integrity rewriter contract acknowledgement unavailable: got %s, want %s",
				res.StorageIntegrityContractVersion, p.RequiredStorageIntegrityContractVersion)}
	}
	qctx.StatementType = res.StatementType
	qctx.AccessedTables = res.AccessedTables
	qctx.TableRewrites = res.TableRewrites
	qctx.DatabaseRewrites = res.DatabaseRewrites
	qctx.PrivilegesDeltas = res.PrivilegesDeltas
	qctx.ExistenceClause = res.ExistenceClause
	if res.SQL != qctx.OriginalSQL {
		qctx.RewrittenSQL = res.SQL
		qctx.Query.Body = res.SQL
		qctx.Session.State().MarkActiveRewrite(nil)
		// Project AccessedTables to []string of "db.table" (or bare
		// "table" when unqualified) so Zap emits a clean array rather
		// than reflect-marshalling each AccessedTable struct's five
		// fields. Preserves the log shape ops dashboards key off.
		accessedTableNames := make([]string, len(res.AccessedTables))
		for i, t := range res.AccessedTables {
			if t.OriginalDatabase == "" {
				accessedTableNames[i] = t.OriginalTable
			} else {
				accessedTableNames[i] = t.OriginalDatabase + "." + t.OriginalTable
			}
		}
		logger.Debugw("rewriter: SQL rewritten",
			"original", qctx.OriginalSQL,
			"rewritten", res.SQL,
			"statement_type", res.StatementType,
			"accessed_tables", accessedTableNames,
			"table_rewrites", res.TableRewrites,
			"database_rewrites", res.DatabaseRewrites,
		)
	}
	return nil
}

// readModeFromQuery reads SQL_x_read_mode from the per-query settings.
// ok=false when absent; err when present but invalid. The setting is NOT
// removed -- like SQL_x_auth_token / SQL_x_payer it flows to ClickHouse as
// a custom setting (plan deviation D-3).
func readModeFromQuery(q *chproto.Query) (rewriter.ReadMode, bool, error) {
	for _, s := range q.Settings {
		if s.Key != rewriter.ReadModeSettingKey {
			continue
		}
		m, err := rewriter.ParseReadMode(s.Value)
		if err != nil {
			return "", false, err
		}
		return m, true, nil
	}
	return "", false, nil
}

// OnException reverse-maps any rewritten table/database names in the
// exception message back to what the client used. No-op if no
// rewrite was active for this session.
func (p *Plugin) OnException(ctx context.Context, sess chsession.Session, exc *chproto.Exception) error {
	// Scrubbing is independent of the general reverse-map gate: a ClickHouse
	// Exception can expose the physical SI namespace even when this session did
	// not record a successful active rewrite.
	if scrubbed := p.scrubberFor(sess).Scrub(exc.Message); scrubbed != exc.Message {
		exc.Message = scrubbed
	}
	if !sess.State().Snapshot().HasActiveRewrite {
		return nil
	}
	rw := p.rewriterFor(sess)
	if rw == nil {
		return nil
	}
	rewritten, err := rw.RewriteErrorMessage(ctx, exc.Message)
	if err != nil {
		_, logger := log.FromContext(ctx)
		logger.Warne(err, "rewriter: error reverse-map failed, forwarding original message")
		return nil
	}
	if rewritten != exc.Message {
		exc.Message = rewritten
	}
	return nil
}

// scrubberFor returns the scrubber of the session's last query snapshot, or
// of the current snapshot when the session has not run a query yet. It is nil
// (a no-op) when storage integrity is disabled.
func (p *Plugin) scrubberFor(sess chsession.Session) *rewriter.StorageIntegrityScrubber {
	if p.TableState == nil {
		return nil
	}
	if v, ok := p.snapshots.Load(sess.ID()); ok {
		return p.scrubbers.For(v.(sitable.Snapshot))
	}
	return p.scrubbers.For(p.TableState.Current())
}

// OnConnect satisfies ConnLifecyclePlugin. We can't build the
// Rewriter here because Identity is set later (by the auth plugin on
// OnQuery), so we defer to lazy init.
func (p *Plugin) OnConnect(_ context.Context, _ chsession.Session) error { return nil }

// OnDisconnect satisfies ConnLifecyclePlugin. Cleanup happens in
// OnClose (paired with OnHello); OnDisconnect can fire for
// connections that never reached Hello (e.g. dial failure).
func (p *Plugin) OnDisconnect(sess chsession.Session) {
	p.evict(sess.ID())
}

// OnClose evicts and closes the cached Rewriter for sess.
func (p *Plugin) OnClose(sess chsession.Session) {
	p.evict(sess.ID())
}

func (p *Plugin) evict(id int64) {
	p.snapshots.Delete(id)
	if v, ok := p.rewriters.LoadAndDelete(id); ok {
		_ = v.(rewriter.Rewriter).Close()
	}
}

// RejectUndecodableQuery implements plugin.StrictQueryDecodePlugin. An
// undecodable Query cannot be rewritten, and Relay's raw-splice fallback would
// forward it to ClickHouse unexamined, which is the pass-through spec
// 2026-09-26 T8 removed. The rewrite plugin therefore fails it closed with or
// without storage integrity. The refusal happens before any query plugin runs,
// so it also covers maintenance and platform-operator sessions, whose decoded
// queries bypass rewrite (review L1: deliberate; an undecodable packet carries
// no trustworthy session settings). Sessions the chain filters out for this
// plugin (peer-trusted, routed, origin-side forwarding) keep the fallback.
func (p *Plugin) RejectUndecodableQuery() bool {
	return p != nil
}

// RunOnPeerTrust opts the rewrite plugin out of peer-trusted sessions.
// The inner SQL arriving on a peer-trusted connection was already
// rewritten by an upstream housegate (table names resolved to
// physical-DB form, remote() clauses inlined). Re-running this layer
// would either no-op (fine) or, worse, double-prefix already-prefixed
// table names. Skip applies to OnQuery / OnException / OnQueryComplete
// — OnHello / OnConnect / OnClose run unconditionally because the
// ConnLifecycle hooks are needed for the per-conn Rewriter cache
// teardown invariant.
func (p *Plugin) RunOnPeerTrust() bool { return false }

// RunOnForward implements plugin.ForwardAware. Returns false — when the
// session is transparently forwarded to a peer, the rewrite belongs to
// the host proxy that actually runs the SQL.
func (p *Plugin) RunOnForward() bool { return false }

var (
	_ plugin.HelloPlugin             = (*Plugin)(nil)
	_ plugin.QueryPlugin             = (*Plugin)(nil)
	_ plugin.StrictQueryDecodePlugin = (*Plugin)(nil)
	_ plugin.ExceptionPlugin         = (*Plugin)(nil)
	_ plugin.ConnLifecyclePlugin     = (*Plugin)(nil)
	_ plugin.ClosePlugin             = (*Plugin)(nil)
	_ plugin.PeerTrustAware          = (*Plugin)(nil)
	_ plugin.ForwardAware            = (*Plugin)(nil)
)
