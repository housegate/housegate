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
// Ordinary rewrite errors retain the legacy fail-open posture only when no
// storage-integrity surface is configured. RejectedError and configured-SI
// classification/acknowledgement failures are returned to the client.
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

	// FailClosedOnError turns otherwise ordinary rewriter failures into client
	// Exceptions. It is enabled whenever SI membership is configured, including
	// for caller-injected factories that do not return RejectedError themselves.
	FailClosedOnError bool

	// RequiredStorageIntegrityContractVersion is the defense-in-depth response
	// echo gate for every Rewriter implementation, including custom factories.
	RequiredStorageIntegrityContractVersion pb.StorageIntegrityContractVersion

	// rewriters caches one Rewriter per session id (int64). Lifetime
	// is OnQuery-first-touch through OnClose.
	rewriters sync.Map // map[int64]rewriter.Rewriter
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

// OnQuery routes the query through the rewriter. Ordinary errors fail open only
// when FailClosedOnError is false; RejectedError always fails closed.
func (p *Plugin) OnQuery(ctx context.Context, qctx *plugin.QueryContext) error {
	if qctx.Session != nil {
		snap := qctx.Session.State().Snapshot()
		// Maintenance sessions (indexer-signed) and platform-operator
		// sessions bypass rewrite — Query.Body is forwarded verbatim by
		// the relay. RewrittenSQL mirrors OriginalSQL so downstream
		// plugins / observers (commitgate Event, audit logs) that read
		// it see a coherent value rather than empty.
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
			logger.Infow("rewriter: statement rejected (fail-closed)", "code", rej.Code.String(), "message", rej.Message)
			return rej
		}
		if p.FailClosedOnError {
			logger.Errorw("rewriter: classification unavailable with storage-integrity configured (fail-closed)", "error", err)
			return fmt.Errorf("storage-integrity rewrite classification unavailable: %w", err)
		}
		logger.Warne(err, "rewriter: rewrite failed, forwarding original SQL")
		return nil
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
	if v, ok := p.rewriters.LoadAndDelete(id); ok {
		_ = v.(rewriter.Rewriter).Close()
	}
}

// RejectUndecodableQuery implements plugin.StrictQueryDecodePlugin. When SI
// membership is configured, an undecodable Query has no trustworthy
// classification or v1 acknowledgement and must not take Relay's raw-splice
// fallback. Empty-SI deployments retain the legacy decode fallback.
func (p *Plugin) RejectUndecodableQuery() bool {
	return p != nil && p.FailClosedOnError
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
