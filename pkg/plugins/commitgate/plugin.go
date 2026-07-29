package commitgate

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/housegate/housegate/pkg/log"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/plugin"
	"github.com/housegate/housegate/pkg/sqlmeta"
)

// Plugin is the QueryPlugin that dispatches BeforeStatement to
// registered observers based on classified StatementType.
//
// Plugin is NOT RouteAware: routed (proxy-to-proxy) sessions skip
// it because the destination proxy fires its own commitgate
// plugin and we must not double-fire.
type Plugin struct {
	byType map[sqlmeta.StatementType][]Observer
}

// NewPlugin builds a Plugin from the given Observers, indexed by
// StatementType for O(1) dispatch. Observers with no subscribed
// types are silently skipped (they cannot fire anyway).
func NewPlugin(observers []Observer) *Plugin {
	p := &Plugin{byType: make(map[sqlmeta.StatementType][]Observer)}
	for _, o := range observers {
		for _, t := range o.SubscribedTypes() {
			p.byType[t] = append(p.byType[t], o)
		}
	}
	return p
}

// SubscribedTypes returns the union of all observers' subscribed
// StatementTypes — a small helper for buildServer to log what's
// active.
func (p *Plugin) SubscribedTypes() []sqlmeta.StatementType {
	out := make([]sqlmeta.StatementType, 0, len(p.byType))
	for t := range p.byType {
		out = append(out, t)
	}
	return out
}

// OnQuery dispatches to subscribed observers if qctx.StatementType
// matches. Statements whose type has no subscriber are a no-op.
//
// The dispatcher does NOT validate Event content (empty
// AccessedTables, missing logical database, etc.). Policy lives in
// the observers — see PermissionCommitGateObserver and
// InMemoryCommitGateObserver. This is deliberate: legitimate empty
// shapes exist (e.g. `SELECT 1` has no AccessedTables) and only the
// observer knows whether its own policy is satisfied.
//
// On success (every observer's BeforeStatement returned nil) the
// Event is stashed on SessionState so OnException can replay it to
// observers that subscribe via OnStatementException. The stash is
// cleared by OnQueryComplete on both success and failure paths.
func (p *Plugin) OnQuery(ctx context.Context, qctx *plugin.QueryContext) error {
	obs, ok := p.byType[qctx.StatementType]
	if !ok || len(obs) == 0 {
		return nil
	}
	// Maintenance sessions short-circuit — the GC is itself the
	// consequence of an already-committed pendingDelete and must
	// not re-fire host observers. Platform-operator sessions get the
	// same skip — observer dispatch is reserved for end-user traffic;
	// operator workflows go around the gate. Driver sessions
	// (indexer-signed indexer-driver traffic) also skip — CREATE
	// DATABASE on a processor's logical DB must not be registered as a
	// user database on chain, and CREATE TABLE for processor tables is
	// owned by sentio-node's DatabaseRegistryService gRPC path rather
	// than the housegate commitgate. Placed AFTER the cheap
	// observer-lookup early-return so the skip only fires when there
	// would otherwise have been a dispatch.
	if qctx.Session != nil {
		snap := qctx.Session.State().Snapshot()
		if snap.Maintenance || snap.PlatformOperator || snap.IsDriver {
			return nil
		}
	}
	// Defensive clear: a previous query that aborted before
	// OnQueryComplete fired (e.g. transport error mid-stream) should
	// not leak its stashed Event into this query.
	if qctx.Session != nil {
		qctx.Session.State().ClearCommitGateEvent()
	}
	ev := buildEvent(qctx)
	ev.Values = make(map[string]any)
	for _, o := range obs {
		if err := o.BeforeStatement(ctx, ev); err != nil {
			// ErrAbortWithSuccess (direct or %w-wrapped) signals
			// "host has handled this DDL; do NOT forward to CH but
			// reply EndOfStream to the client." We deliberately do
			// NOT stash the Event — there will be no upstream
			// Exception to replay against, and any post-loop
			// SetCommitGateEvent below is bypassed by this return.
			if errors.Is(err, ErrAbortWithSuccess) {
				qctx.AbortWithSuccess = true
				return nil
			}
			return fmt.Errorf("commitgate (%s): %w", qctx.StatementType, err)
		}
	}
	if qctx.Session != nil {
		qctx.Session.State().SetCommitGateEvent(ev)
	}
	return nil
}

// OnException dispatches OnStatementException to observers when CH
// returned an Exception for a statement that previously cleared the
// gate. Reads the Event stashed by OnQuery. No-op when no event is
// stashed (statement type wasn't subscribed, or the chain aborted
// before stashing).
//
// Best-effort: panics inside an observer's OnStatementException are
// recovered and logged so one misbehaving observer doesn't poison
// the chain or kill the connection.
func (p *Plugin) OnException(ctx context.Context, sess chsession.Session, exc *chproto.Exception) error {
	if sess == nil {
		return nil
	}
	raw := sess.State().Snapshot().CommitGateEvent
	if raw == nil {
		return nil
	}
	ev, ok := raw.(*Event)
	if !ok || ev == nil {
		return nil
	}
	observers, ok := p.byType[ev.Type]
	if !ok || len(observers) == 0 {
		return nil
	}
	for _, o := range observers {
		dispatchOnException(ctx, o, ev, exc)
	}
	return nil
}

// dispatchOnException invokes a single observer with a panic guard.
// Extracted so the recover scope is bounded to one observer — a
// panicking observer must not abort dispatch to the rest.
func dispatchOnException(ctx context.Context, o Observer, ev *Event, exc *chproto.Exception) {
	defer func() {
		if r := recover(); r != nil {
			log.Errorw("commitgate observer panicked in OnStatementException",
				"type", ev.Type,
				"accessed_tables", ev.AccessedTables,
				"recover", r,
			)
		}
	}()
	o.OnStatementException(ctx, ev, exc)
}

// OnQueryComplete clears the commitgate Event stash so the next
// query starts clean. Fires on both success (EndOfStream) and
// failure (Exception) paths from the relay.
func (p *Plugin) OnQueryComplete(_ context.Context, sess chsession.Session) {
	if sess == nil {
		return
	}
	sess.State().ClearCommitGateEvent()
}

// RunOnForward implements plugin.ForwardAware. Returns false — when the
// session is transparently forwarded to a peer, the DDL gate fires on
// the host proxy that owns the database.
func (p *Plugin) RunOnForward() bool { return false }

// RunOnPeerTrust opts the commitgate plugin out of peer-trusted sessions.
// Inner SQL arriving on a peer-trusted connection (the inbound side of a
// remote() loopback between proxies) was already gated by commitgate on
// the originating proxy that ran the rewriter. The rewriter is also
// skipped on peer-trust (rewrite.Plugin.RunOnPeerTrust=false), so
// qctx.StatementType is left as Unspecified — running the
// PermissionCommitGateObserver here would unconditionally reject the
// query with "rewriter did not classify statement (Unspecified)". Skip
// the whole plugin instead so peer-trusted traffic is gated exactly once
// at its origin.
func (p *Plugin) RunOnPeerTrust() bool { return false }

var (
	_ plugin.QueryPlugin         = (*Plugin)(nil)
	_ plugin.ExceptionPlugin     = (*Plugin)(nil)
	_ plugin.QueryCompletePlugin = (*Plugin)(nil)
	_ plugin.ForwardAware        = (*Plugin)(nil)
	_ plugin.PeerTrustAware      = (*Plugin)(nil)
)

// buildEvent constructs an Event from qctx. It does NOT validate
// content — empty AccessedTables / missing LogicalDatabase / empty
// PrivilegesDeltas all flow through unmodified. Observers apply
// policy.
//
// Per-StatementType shape (see Event.AccessedTables godoc for the
// authoritative contract):
//
//   - GRANT / REVOKE: PrivilegesDeltas pass through verbatim. The
//     first delta's target is mirrored onto AccessedTables[0] for
//     symmetry with the DDL path; observers that need finer-grained
//     info iterate PrivilegesDeltas. PrivilegesCategory is the OR of
//     every delta's Category.
//   - USE / SHOW TABLES: rewriter populates DatabaseRewrites instead
//     of AccessedTables (see pkg/rewriter/sentio.go:386). buildEvent
//     synthesises a single AccessedTable from the (sole) entry; the
//     known-physical USE case (database_rewrites empty) falls back
//     to session.LogicalDatabaseName() which the rewrite plugin has
//     already updated via maybeUpdateLogicalDatabase.
//   - Everything else (SELECT / DML / table-DDL / database-DDL):
//     AccessedTables passes through from qctx unchanged. SELECT may
//     legitimately surface zero entries (`SELECT 1`); the dispatcher
//     does not reject this — observers decide.
func buildEvent(qctx *plugin.QueryContext) *Event {
	var (
		queryID  string
		user     string
		settings map[string]string
	)
	if qctx.Query != nil {
		queryID = qctx.Query.ID
		// Convert the wire-format []Setting slice into the read-only
		// map[string]string surface promised by Event.Settings. Done
		// once per gated query so observers don't pay the conversion
		// each time they probe a known key. Lifetime is bounded by
		// the Query — observers must not retain the map.
		if n := len(qctx.Query.Settings); n > 0 {
			settings = make(map[string]string, n)
			for _, s := range qctx.Query.Settings {
				settings[s.Key] = s.Value
			}
		}
	}
	if qctx.Session != nil {
		user = qctx.Session.State().Account()
	}

	// Operator-on-behalf-of-owner: prefer qctx.Owner (set by the auth
	// plugin after a successful State.IsOperator(owner, signer) check)
	// so the gate validates exactly what the rewriter already gated
	// against. Falls back to re-parsing SQL_x_payer from Settings for
	// deployments where auth-side resolution is disabled (e.g. tests
	// that don't wire State, or auth-disabled router-only proxies):
	// the PermissionCommitGateObserver still invokes IsOperator on its
	// own State handle in BeforeStatement, so the relation is enforced
	// either way. Same quote-stripping convention as
	// pkg/plugins/usage/usage.go (Field::restoreFromDump wraps Custom
	// strings in `'…'`).
	owner := strings.ToLower(strings.TrimSpace(qctx.Owner))
	if owner == "" {
		if v, ok := settings[auth.PayerSettingKey]; ok {
			owner = strings.ToLower(strings.Trim(strings.TrimSpace(v), "\"'"))
		}
	}
	if owner != "" && owner == strings.ToLower(user) {
		owner = ""
	}

	ev := &Event{
		Type:             qctx.StatementType,
		User:             user,
		Owner:            owner,
		QueryID:          queryID,
		OriginalSQL:      qctx.OriginalSQL,
		RewrittenSQL:     qctx.RewrittenSQL,
		Settings:         settings,
		AccessedTables:   qctx.AccessedTables,
		PrivilegesDeltas: qctx.PrivilegesDeltas,
		ExistenceClause:  qctx.ExistenceClause,
	}

	switch qctx.StatementType {
	case sqlmeta.StatementTypeGrant, sqlmeta.StatementTypeRevoke:
		// Mirror first delta's target onto AccessedTables for
		// observer symmetry. CH parser limits a GRANT/REVOKE to one
		// target so AccessedTables stays single-entry.
		if len(qctx.PrivilegesDeltas) > 0 {
			d := qctx.PrivilegesDeltas[0]
			ev.AccessedTables = []sqlmeta.AccessedTable{{
				OriginalDatabase: d.OriginalDatabase,
				OriginalTable:    d.OriginalTable,
				LogicalDatabase:  d.LogicalDatabase,
				PhysicalDatabase: d.PhysicalDatabase,
			}}
		}
		for _, d := range qctx.PrivilegesDeltas {
			ev.PrivilegesCategory |= d.Category
		}

	case sqlmeta.StatementTypeUse, sqlmeta.StatementTypeShowTables:
		if len(ev.AccessedTables) == 0 {
			if a := synthDatabaseScopedAccess(qctx); a != nil {
				ev.AccessedTables = []sqlmeta.AccessedTable{*a}
			}
		}
	}
	return ev
}

// synthDatabaseScopedAccess fabricates an AccessedTable for USE /
// SHOW TABLES from rewriter outputs the rewriter places outside of
// `original_accessed_tables`. Returns nil when no source is available
// (observer is then free to reject the empty-AccessedTables case).
//
// Source precedence:
//
//  1. qctx.DatabaseRewrites: the rewriter writes a single entry
//     {logical → physical} for mapped USE / SHOW TABLES. Logical is
//     the user-typed name, physical is the resolved CH database.
//  2. session.LogicalDatabaseName(): the known-physical USE case
//     where DatabaseRewrites is empty (the SQL was forwarded
//     unchanged). The rewrite plugin's maybeUpdateLogicalDatabase
//     has already populated this from the regex fallback in
//     pkg/rewriter/sentio.go:extractUseTarget.
func synthDatabaseScopedAccess(qctx *plugin.QueryContext) *sqlmeta.AccessedTable {
	for logical, physical := range qctx.DatabaseRewrites {
		return &sqlmeta.AccessedTable{
			OriginalDatabase: logical,
			LogicalDatabase:  logical,
			PhysicalDatabase: physical,
		}
	}
	if qctx.Session != nil {
		if ld := qctx.Session.State().LogicalDatabaseName(); ld != "" {
			return &sqlmeta.AccessedTable{
				OriginalDatabase: ld,
				LogicalDatabase:  ld,
			}
		}
	}
	return nil
}
