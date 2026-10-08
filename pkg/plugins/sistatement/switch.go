package sistatement

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/log"
	"github.com/housegate/housegate/pkg/registry"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

// defaultSwitchTimeout bounds the dial plus the replayed handshake of an
// upstream switch when Options.SwitchTimeout is zero.
const defaultSwitchTimeout = 10 * time.Second

// Upstream-switch outcomes, the result label of
// clickhouse_proxy_agent_si_upstream_switches_total (spec 2026-10-09 §10).
const (
	switchResultSwitched        = "switched"
	switchResultRefusedState    = "refused_state"
	switchResultRefusedDatabase = "refused_database"
	switchResultRefusedRevision = "refused_revision"
	switchResultDialFailed      = "dial_failed"
)

// HostingResolver answers which indexer hosts a database and where its
// housegate listens. registry.Registry satisfies it.
type HostingResolver interface {
	Get(id string) (registry.Database, bool)
	ProxyByIndexerId(indexerId uint64) (registry.ProxyAddress, bool)
}

// holdsServerState reports whether sql changes server-side session state the
// agent cannot replay on another server (spec 2026-10-09 §6.4 step 1): a
// session SET, a temporary table, or a transaction. CREATE OR REPLACE
// TEMPORARY and REPLACE TEMPORARY (both accepted by ClickHouse 26.8) create a
// temporary table too. An unreadable statement counts as stateful: refusing a
// later switch is safe.
func holdsServerState(sql string) bool {
	words, err := sicore.LeadingKeywords(sql, 4)
	if err != nil {
		return true
	}
	at := func(i int) string {
		if i < len(words) {
			return words[i]
		}
		return ""
	}
	switch at(0) {
	case "SET", "BEGIN":
		return true
	case "START":
		return at(1) == "TRANSACTION"
	case "REPLACE":
		return at(1) == "TEMPORARY"
	case "CREATE":
		if at(1) == "OR" && at(2) == "REPLACE" {
			return at(3) == "TEMPORARY"
		}
		return at(1) == "TEMPORARY"
	}
	return false
}

func (p *Plugin) observeSwitch(result string) {
	if o, ok := p.observer.(SwitchObserver); ok && o != nil {
		o.SIUpstreamSwitch(result)
	}
}

// currentUpstreamAddress is the configured endpoint the session's upstream was
// dialed with (dialRaw's wrapper), or "" when the conn does not carry one.
func currentUpstreamAddress(up *chproto.Codec) string {
	if up == nil {
		return ""
	}
	if named, ok := up.Conn().(interface{ UpstreamAddress() string }); ok {
		return named.UpstreamAddress()
	}
	return ""
}

// switchFailureResult classifies a failed dial or SwitchUpstream for the
// metric: a server the client leg cannot talk to (revision, timezone) is
// refused_revision; a session SwitchUpstream will not move is refused_state;
// every transport, handshake and deadline failure is dial_failed.
func switchFailureResult(err error) string {
	switch {
	case errors.Is(err, chsession.ErrUpstreamRevisionTooLow), errors.Is(err, chsession.ErrUpstreamTimezoneMismatch):
		return switchResultRefusedRevision
	case errors.Is(err, chsession.ErrRebindDenied):
		return switchResultRefusedState
	}
	return switchResultDialFailed
}

// maybeSwitch moves the session to the indexer hosting database before the
// statement is claimed (spec 2026-10-09 §6.4, D19): no client_seq is reserved
// and nothing is signed or forwarded yet. Every refusal is local and
// session-preserving (an OnQuery error ends only the query) and leaves the
// session on its current upstream. It runs inside OnQuery, where Relay has no
// query in flight.
func (p *Plugin) maybeSwitch(ctx context.Context, sess chsession.Session, database string) error {
	if p.hosting == nil || p.dial == nil || p.pinnedUpstream || sess == nil {
		return nil
	}
	db, ok := p.hosting.Get(database)
	if !ok || db.PendingDelete {
		return nil // unknown (or departing) database: the server decides
	}
	target, ok := p.hosting.ProxyByIndexerId(db.IndexerId)
	if !ok || target.Url == "" || target.HousegatePort == 0 {
		return nil // no reachable housegate advertised: the server decides
	}
	targetAddr := target.Addr()
	if currentUpstreamAddress(sess.Upstream()) == targetAddr {
		return nil
	}
	p.mu.Lock()
	stateful := p.nonSwitchable[sess.ID()]
	p.mu.Unlock()
	if stateful {
		return p.refuseSwitch(ctx, switchResultRefusedState, database, db.IndexerId,
			fmt.Errorf("storage_integrity agent: INSERT into %s must run on indexer %d, but this session holds server-side state; reconnect with --database %s", database, db.IndexerId, database))
	}
	// Step 2: the replayed hello keeps the session database when it is empty,
	// unknown to the registry (plan P10; a departing database counts as
	// unknown, as in routing) or hosted by the target.
	cur := p.sessionDatabase(sess)
	if cur != "" {
		if curInfo, ok := p.hosting.Get(cur); ok && !curInfo.PendingDelete && curInfo.IndexerId != db.IndexerId {
			return p.refuseSwitch(ctx, switchResultRefusedDatabase, database, db.IndexerId,
				fmt.Errorf("storage_integrity agent: INSERT into %s must run on indexer %d, but the session database %s lives on indexer %d; use a separate connection with --database %s or USE %s first", database, db.IndexerId, cur, curInfo.IndexerId, database, database))
		}
	}
	var hello *chproto.ClientHello
	if state := sess.State(); state != nil {
		hello = state.UpstreamHello() // a clone
	}
	if hello == nil {
		return p.refuseSwitch(ctx, switchResultRefusedState, database, db.IndexerId,
			fmt.Errorf("storage_integrity agent: INSERT into %s must run on indexer %d, but this session has no replayable hello; reconnect with --database %s", database, db.IndexerId, database))
	}
	// Step 3: SwitchUpstream clamps ProtocolVersion to the client leg's
	// revision as well; setting it here keeps the spec's hello explicit.
	hello.ProtocolVersion = sess.State().Snapshot().ClientRevision
	hello.Database = cur

	timeout := p.switchTimeout
	if timeout <= 0 {
		timeout = defaultSwitchTimeout
	}
	switchCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	newUp, err := p.dial(switchCtx, targetAddr)
	if err == nil && newUp == nil {
		err = errors.New("dial returned no connection")
	}
	if err == nil {
		err = sess.SwitchUpstream(switchCtx, newUp, hello)
	}
	if err != nil {
		return p.refuseSwitch(ctx, switchFailureResult(err), database, db.IndexerId,
			fmt.Errorf("storage_integrity agent: INSERT into %s must run on indexer %d (%s), but switching this session there failed: %w; reconnect with --database %s", database, db.IndexerId, targetAddr, err, database))
	}
	p.observeSwitch(switchResultSwitched)
	_, logger := log.FromContext(ctx)
	logger.Infow("sistatement: session switched to the hosting indexer", "database", database, "indexer_id", db.IndexerId, "upstream", targetAddr, "session_database", cur)
	return nil
}

// refuseSwitch counts and logs a refused switch and returns refusal.
func (p *Plugin) refuseSwitch(ctx context.Context, result, database string, indexerID uint64, refusal error) error {
	p.observeSwitch(result)
	_, logger := log.FromContext(ctx)
	logger.Infow("sistatement: upstream switch refused locally; the session keeps its upstream", "database", database, "indexer_id", indexerID, "result", result, "err", refusal)
	return refusal
}
