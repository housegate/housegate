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

// defaultSwitchTimeout bounds the hosting lookups, the dial and the replayed
// handshake of an upstream switch when Options.SwitchTimeout is zero.
const defaultSwitchTimeout = 10 * time.Second

// hostingCacheTTL bounds how long a successful hosting answer is reused, like
// the 5 s table-status cache: an INSERT on an already-switched session costs
// no lookup within it. Failed lookups are never cached.
const hostingCacheTTL = 5 * time.Second

// hostingCacheSweepSize is the cache size above which expired entries are
// swept on insert.
const hostingCacheSweepSize = 256

// switchRefusalLogEvery throttles the refusal log per reason.
const switchRefusalLogEvery = time.Minute

// Upstream-switch outcomes, the result label of
// clickhouse_proxy_agent_si_upstream_switches_total (spec 2026-10-09 §10).
const (
	switchResultSwitched        = "switched"
	switchResultRefusedState    = "refused_state"
	switchResultRefusedDatabase = "refused_database"
	switchResultRefusedRevision = "refused_revision"
	switchResultDialFailed      = "dial_failed"
)

// hostingEntry is a cached successful registry.DatabaseHosting answer.
type hostingEntry struct {
	addr      registry.ProxyAddress
	indexerID uint64
	hosted    bool
	until     time.Time
}

// holdsServerState reports whether sql changes server-side session state the
// agent cannot replay on another server (spec 2026-10-09 §6.4 step 1): a
// session SET, a temporary table, or a transaction. CREATE OR REPLACE
// TEMPORARY and REPLACE TEMPORARY (both accepted by ClickHouse 26.8) create a
// temporary table too. OnQuery tracks a standalone USE that
// ParseUseDatabaseStrict matches before calling this, so a leading USE here is
// a form the agent cannot track (e.g. USE DATABASE x) and counts as stateful.
// An unreadable statement counts as stateful too: refusing a later switch is
// safe.
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
	case "SET", "BEGIN", "USE":
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

// lookupHosting answers registry.DatabaseHosting for database through a
// hostingCacheTTL cache of successful answers; errors are not cached.
func (p *Plugin) lookupHosting(ctx context.Context, database string) (registry.ProxyAddress, uint64, bool, error) {
	now := p.now()
	p.mu.Lock()
	if e, ok := p.hostingCache[database]; ok && now.Before(e.until) {
		p.mu.Unlock()
		return e.addr, e.indexerID, e.hosted, nil
	}
	p.mu.Unlock()
	addr, indexerID, hosted, err := p.hosting.DatabaseHosting(ctx, database)
	if err != nil {
		return registry.ProxyAddress{}, 0, false, err
	}
	if hosted && (addr.Url == "" || addr.HousegatePort == 0) {
		return registry.ProxyAddress{}, 0, false, fmt.Errorf("indexer %d hosting %s advertises no housegate address", indexerID, database)
	}
	p.mu.Lock()
	if len(p.hostingCache) >= hostingCacheSweepSize {
		for db, e := range p.hostingCache {
			if !now.Before(e.until) {
				delete(p.hostingCache, db)
			}
		}
	}
	p.hostingCache[database] = hostingEntry{addr: addr, indexerID: indexerID, hosted: hosted, until: now.Add(hostingCacheTTL)}
	p.mu.Unlock()
	return addr, indexerID, hosted, nil
}

// maybeSwitch moves the session to the indexer hosting database before the
// statement is claimed (spec 2026-10-09 §6.4, D19): no client_seq is reserved
// and nothing is signed or forwarded yet. Every refusal is local and
// session-preserving (an OnQuery error ends only the query) and leaves the
// session on its current upstream. It runs inside OnQuery, where Relay has no
// query in flight. Only a database the registry genuinely does not host is
// left to the server; a failed hosting lookup refuses, because guessing would
// send the INSERT to an indexer that does not host it or replay a session
// database the target pivots away from.
func (p *Plugin) maybeSwitch(ctx context.Context, sess chsession.Session, database string) error {
	if p.hosting == nil || p.dial == nil || p.pinnedUpstream || sess == nil {
		return nil
	}
	timeout := p.switchTimeout
	if timeout <= 0 {
		timeout = defaultSwitchTimeout
	}
	switchCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	target, indexerID, hosted, err := p.lookupHosting(switchCtx, database)
	if err != nil {
		return p.refuseSwitch(ctx, switchResultDialFailed, database,
			fmt.Errorf("storage_integrity agent: cannot resolve the indexer hosting %s: %w; reconnect with --database %s", database, err, database))
	}
	if !hosted {
		return nil // not hosted by any indexer: the server decides
	}
	targetAddr := target.Addr()
	if currentUpstreamAddress(sess.Upstream()) == targetAddr {
		return nil
	}
	p.mu.Lock()
	stateful := p.nonSwitchable[sess.ID()]
	p.mu.Unlock()
	if stateful {
		return p.refuseSwitch(ctx, switchResultRefusedState, database,
			fmt.Errorf("storage_integrity agent: INSERT into %s must run on indexer %d, but this session holds server-side state; reconnect with --database %s", database, indexerID, database))
	}
	// Step 2: the replayed hello keeps the session database when it is empty,
	// not hosted by any indexer (plan P10; PendingDelete counts as not
	// hosted, as in routing) or hosted by the target.
	cur := p.sessionDatabase(sess)
	if cur != "" {
		_, curIndexer, curHosted, err := p.lookupHosting(switchCtx, cur)
		if err != nil {
			return p.refuseSwitch(ctx, switchResultDialFailed, database,
				fmt.Errorf("storage_integrity agent: INSERT into %s must run on indexer %d, but resolving the indexer hosting the session database %s failed: %w; reconnect with --database %s", database, indexerID, cur, err, database))
		}
		if curHosted && curIndexer != indexerID {
			return p.refuseSwitch(ctx, switchResultRefusedDatabase, database,
				fmt.Errorf("storage_integrity agent: INSERT into %s must run on indexer %d, but the session database %s lives on indexer %d; use a separate connection with --database %s or USE %s first", database, indexerID, cur, curIndexer, database, database))
		}
	}
	var hello *chproto.ClientHello
	if state := sess.State(); state != nil {
		hello = state.UpstreamHello() // a clone
	}
	if hello == nil {
		return p.refuseSwitch(ctx, switchResultRefusedState, database,
			fmt.Errorf("storage_integrity agent: INSERT into %s must run on indexer %d, but this session has no replayable hello; reconnect with --database %s", database, indexerID, database))
	}
	// Step 3: SwitchUpstream clamps ProtocolVersion to the client leg's
	// revision as well; setting it here keeps the spec's hello explicit.
	hello.ProtocolVersion = sess.State().Snapshot().ClientRevision
	hello.Database = cur

	newUp, err := p.dial(switchCtx, targetAddr)
	if err == nil && newUp == nil {
		err = errors.New("dial returned no connection")
	}
	if err == nil {
		err = sess.SwitchUpstream(switchCtx, newUp, hello)
	}
	if err != nil {
		return p.refuseSwitch(ctx, switchFailureResult(err), database,
			fmt.Errorf("storage_integrity agent: INSERT into %s must run on indexer %d (%s), but switching this session there failed: %w; reconnect with --database %s", database, indexerID, targetAddr, err, database))
	}
	p.observeSwitch(switchResultSwitched)
	_, logger := log.FromContext(ctx)
	logger.Infow("sistatement: session switched to the hosting indexer", "database", database, "indexer_id", indexerID, "upstream", targetAddr, "session_database", cur)
	return nil
}

// refuseSwitch counts a refused switch, logs it at most once per minute per
// reason, and returns refusal.
func (p *Plugin) refuseSwitch(ctx context.Context, result, database string, refusal error) error {
	p.observeSwitch(result)
	_, logger := log.FromContext(ctx)
	logger.InfoEvery(fmt.Sprintf("sistatement-switch-refused-%p-%s", p, result), switchRefusalLogEvery,
		"sistatement: upstream switch refused locally; the session keeps its upstream", "database", database, "result", result, "err", refusal)
	return refusal
}
