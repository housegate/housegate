package sistatement

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/log"
	"github.com/housegate/housegate/pkg/registry"
)

// maxClockSkew is the ingress's future-token tolerance (pkg/auth
// eth_validator.go): a local clock further ahead makes every statement token
// look issued in the future.
const maxClockSkew = 5 * time.Second

// infoFailureTTL bounds how long a failed info lookup is remembered per
// database: a burst of INSERTs costs one lookup (three RPCs, each up to the
// RpcNetworkState timeout), and recovery is noticed within the TTL. A hosting
// node that predates sentio_getStorageIntegrityInfo is a permanent condition
// on the P7 fallback path.
const infoFailureTTL = time.Minute

// infoSuccessTTL bounds how long a successful info answer is reused per
// database. An agent that cached client_lanes_enabled=false before the
// network activated client lanes picks the activation up within it, without a
// restart (spec 2026-10-09 §6.4, preflight F21). One info RPC per database per
// TTL is the cost.
const infoSuccessTTL = time.Minute

// infoRefreshRetry is the minimum interval between refresh attempts of an
// expired info answer whose last refresh failed; the stale answer is served
// meanwhile (stale-while-error).
const infoRefreshRetry = 5 * time.Second

// infoEntry is a cached successful info answer and when it was fetched.
type infoEntry struct {
	info registry.StorageIntegrityInfo
	at   time.Time
	// retryAt is the earliest next refresh after a failed one; refreshing
	// marks the one refresh in flight (single-flight). Both apply only to an
	// expired entry, which is served meanwhile.
	retryAt    time.Time
	refreshing bool
}

// discoveryWarnInterval throttles the P7 fallback warning per database.
const discoveryWarnInterval = time.Minute

// statusWarnInterval throttles the failed table-status lookup warning per
// table: an indexer that does not answer the status method would otherwise
// log once per INSERT.
const statusWarnInterval = time.Minute

// infoFailure is a remembered failed info lookup.
type infoFailure struct {
	step  string // "info" or "network_id"
	err   error
	until time.Time
}

func (p *Plugin) observeDiscovery(step string) {
	if o, ok := p.observer.(DiscoveryObserver); ok && o != nil {
		o.SIDiscoveryFailed(step)
	}
}

// siInfo returns the hosting indexer's SI info for database (spec 2026-10-09
// §6.4 step 3). A success is cached for infoSuccessTTL; an older entry
// triggers one refresh at a time (single-flight; concurrent statements are
// served the stale answer), and when that refresh fails the last good answer
// is still served (stale-while-error, with a throttled warning) and retried
// no sooner than infoRefreshRetry later: expiry never turns a good answer into
// the configured fallback or a refusal. Every successful lookup runs the clock-skew check,
// which warns once per database. A failure with no good answer, including an
// answer without a network_id, is remembered for infoFailureTTL and returned
// with its step; only an actual lookup counts toward the discovery-failure
// metric.
func (p *Plugin) siInfo(ctx context.Context, database string) (registry.StorageIntegrityInfo, string, error) {
	now := p.now()
	p.mu.Lock()
	stale, haveStale := p.infos[database]
	if haveStale && (now.Sub(stale.at) < infoSuccessTTL || stale.refreshing || now.Before(stale.retryAt)) {
		p.mu.Unlock()
		return stale.info, "", nil
	}
	if f, ok := p.infoFailures[database]; ok && !haveStale && now.Before(f.until) {
		p.mu.Unlock()
		return registry.StorageIntegrityInfo{}, f.step, f.err
	}
	if haveStale {
		stale.refreshing = true
		p.infos[database] = stale
	}
	p.mu.Unlock()
	info, err := p.discovery.StorageIntegrityInfo(ctx, database)
	step := "info"
	if err == nil && strings.TrimSpace(info.NetworkID) == "" {
		step, err = "network_id", errors.New("the indexer reported no network_id")
	}
	if err != nil {
		p.observeDiscovery(step)
		if haveStale {
			p.mu.Lock()
			if e, ok := p.infos[database]; ok {
				e.refreshing = false
				e.retryAt = now.Add(infoRefreshRetry)
				p.infos[database] = e
			}
			p.mu.Unlock()
			_, logger := log.FromContext(ctx)
			logger.WarnEvery(fmt.Sprintf("sistatement-info-refresh-%p-%s", p, database), p.discoveryWarnEvery,
				"sistatement: refreshing the hosting indexer's SI info failed; serving the last good answer",
				"database", database, "age", now.Sub(stale.at), "step", step, "err", err)
			return stale.info, "", nil
		}
		p.mu.Lock()
		p.infoFailures[database] = infoFailure{step: step, err: err, until: now.Add(infoFailureTTL)}
		p.mu.Unlock()
		return registry.StorageIntegrityInfo{}, step, err
	}
	p.mu.Lock()
	p.infos[database] = infoEntry{info: info, at: now}
	delete(p.infoFailures, database)
	p.mu.Unlock()
	p.checkSkew(ctx, database, info)
	return info, "", nil
}

// resolveNetworkID is spec 2026-10-09 §6.4 steps 3-4 with plan decision P7:
// the network id the indexer hosting database reports. Without discovery the
// configured id is used as before. When discovery fails, a configured id is
// used with a throttled warning; without one the INSERT is refused. A
// discovered id that differs from a configured one is refused: the agent never
// signs for the wrong network.
//
// lanesEnabled is whether that network uses client lanes: the hosting
// indexer's client_lanes_enabled with discovery (false when the lookup failed,
// which keeps the always-valid legacy lane), and Options.ClientLanesEnabled
// without it.
func (p *Plugin) resolveNetworkID(ctx context.Context, database string) (networkID string, lanesEnabled bool, err error) {
	if p.discovery == nil {
		return p.networkID, p.clientLanesEnabled != nil && p.clientLanesEnabled(), nil
	}
	info, step, err := p.siInfo(ctx, database)
	if err != nil {
		if p.networkID != "" {
			_, logger := log.FromContext(ctx)
			logger.WarnEvery(fmt.Sprintf("sistatement-discovery-%p-%s", p, database), p.discoveryWarnEvery,
				"sistatement: network discovery failed; signing for the configured storage_integrity.agent.network_id",
				"database", database, "network_id", p.networkID, "step", step, "err", err)
			return p.networkID, false, nil
		}
		return "", false, fmt.Errorf("storage_integrity agent: cannot discover network id from the indexer hosting %s: %w", database, err)
	}
	discovered := strings.TrimSpace(info.NetworkID)
	if p.networkID != "" && discovered != strings.TrimSpace(p.networkID) {
		return "", false, fmt.Errorf("storage_integrity agent: the indexer hosting %s reports network %s but storage_integrity.agent.network_id is %s; refusing to sign for the wrong network", database, discovered, p.networkID)
	}
	return discovered, info.ClientLanesEnabled, nil
}

// expireLanesInfo marks the cached info for database expired and clears its
// client_lanes_enabled, after the ingress refused a laned id as not enabled:
// the next statement re-reads the hosting indexer, and if that refresh fails
// the stale answer it falls back to (stale-while-error) keeps the network id
// but signs on the legacy lane instead of repeating the refused laned id.
func (p *Plugin) expireLanesInfo(database string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if e, ok := p.infos[database]; ok {
		e.info.ClientLanesEnabled = false
		e.at = time.Time{}
		e.retryAt = time.Time{}
		p.infos[database] = e
	}
}

// checkSkew warns once per database when the local clock is more than
// maxClockSkew away from the indexer's. It reports whether it warned. The
// consequence depends on the direction: a clock ahead makes the ingress refuse
// tokens as issued in the future; a clock behind is refused only once the lag
// exceeds the ingress's max_token_age.
func (p *Plugin) checkSkew(ctx context.Context, database string, info registry.StorageIntegrityInfo) bool {
	if info.ServerUnixTime == 0 {
		return false
	}
	skew := p.now().Sub(time.Unix(info.ServerUnixTime, 0)) // local minus indexer
	if skew <= maxClockSkew && skew >= -maxClockSkew {
		return false
	}
	p.mu.Lock()
	warned := p.skewWarned[database]
	p.skewWarned[database] = true
	p.mu.Unlock()
	if warned {
		return false
	}
	_, logger := log.FromContext(ctx)
	maxTokenAge := time.Duration(info.MaxTokenAgeSeconds) * time.Second
	if skew > 0 {
		logger.Warnw("sistatement: local clock is ahead of the indexer's; the ingress refuses statement tokens issued more than the tolerance in the future",
			"database", database, "skew", skew, "direction", "ahead", "tolerance", maxClockSkew)
	} else {
		logger.Warnw("sistatement: local clock is behind the indexer's; the ingress refuses statement tokens once the lag exceeds its max_token_age",
			"database", database, "skew", skew, "direction", "behind", "max_token_age", maxTokenAge)
	}
	return true
}

// precheckWriter is the advisory pre-check of spec 2026-10-09 §6.4 through
// sentio_isDatabaseWriter (plan decision P2). Only a definite "no" from the
// indexer hosting database refuses, locally and session-preserving. Any
// error is "unknown" and the server decides: an RPC failure or an indexer
// without the method (counted), or a database no indexer hosts (not a
// discovery failure; the bootstrap cannot know its writers).
func (p *Plugin) precheckWriter(ctx context.Context, database string) error {
	if p.discovery == nil || !p.writerPrecheck {
		return nil
	}
	principal := p.account
	if p.owner != "" {
		principal = strings.ToLower(p.owner)
	}
	ok, err := p.discovery.StorageIntegrityWriterCheck(ctx, database, principal)
	if err != nil {
		if !errors.Is(err, registry.ErrDatabaseNotHosted) {
			p.observeDiscovery("precheck")
		}
		_, logger := log.FromContext(ctx)
		logger.Debugw("sistatement: writer pre-check unavailable; the server decides", "database", database, "principal", principal, "err", err)
		return nil
	}
	if !ok {
		return &chproto.ClientError{
			Code:        chproto.CodeAccessDenied,
			Message:     fmt.Sprintf("storage_integrity agent: %s is not a writer of database %s (it needs Owner or Write on the database); the server would refuse this INSERT", principal, database),
			KeepSession: true,
		}
	}
	return nil
}

// seqFor returns the client_seq counter for a network, opening it at the
// network's first SI write (plan decision P4). A failed open is not cached,
// so the next INSERT retries it; OpenSeqCounter releases its lock on every
// failure. Options.Seq, when set, serves every network.
func (p *Plugin) seqFor(networkID string) (*SeqCounter, error) {
	if p.seq != nil {
		return p.seq, nil
	}
	p.seqMu.Lock()
	defer p.seqMu.Unlock()
	if p.seqClosed {
		return nil, fmt.Errorf("storage_integrity agent: client_seq store for network %s: %w", networkID, ErrSeqClosed)
	}
	if c, ok := p.seqs[networkID]; ok {
		return c, nil
	}
	c, err := p.openSeq(networkID)
	if err != nil {
		return nil, fmt.Errorf("storage_integrity agent: open client_seq store for network %s: %w", networkID, err)
	}
	p.seqs[networkID] = c
	return c, nil
}

// selectorFor returns the network's lane selector, creating it at the
// network's first SI write. It opens nothing: the lane pool is opened when a
// statement first uses a client lane.
func (p *Plugin) selectorFor(networkID string) (*laneSelector, error) {
	p.seqMu.Lock()
	defer p.seqMu.Unlock()
	if p.seqClosed {
		return nil, fmt.Errorf("storage_integrity agent: client_seq store for network %s: %w", networkID, ErrSeqClosed)
	}
	if s, ok := p.selectors[networkID]; ok {
		return s, nil
	}
	var observer LaneObserver
	if o, ok := p.observer.(LaneObserver); ok && o != nil {
		observer = o
	}
	s := newLaneSelector(p.lanes, func() (*LanePool, error) { return p.openLanesFor(networkID) }, p.maxInflight, observer)
	p.selectors[networkID] = s
	return s, nil
}

// existingSelector is the network's selector, or nil before its first SI
// write and after Close.
func (p *Plugin) existingSelector(networkID string) *laneSelector {
	p.seqMu.Lock()
	defer p.seqMu.Unlock()
	return p.selectors[networkID]
}

// openLanesFor opens the lane pool under the network's si directory.
func (p *Plugin) openLanesFor(networkID string) (*LanePool, error) {
	if p.laneDir == nil {
		return nil, errors.New("no client lane directory is configured")
	}
	dir, err := p.laneDir(networkID)
	if err != nil {
		return nil, fmt.Errorf("client lane directory for network %s: %w", networkID, err)
	}
	return p.openLanePool(dir)
}

// Close releases every counter and client lane this plugin holds,
// Options.Seq included, so their locks are free for the next agent; later
// INSERTs fail with ErrSeqClosed. It is idempotent.
func (p *Plugin) Close() error {
	if p == nil {
		return nil
	}
	p.seqMu.Lock()
	defer p.seqMu.Unlock()
	p.seqClosed = true
	var errs []error
	for _, s := range p.selectors {
		errs = append(errs, s.close())
	}
	for _, c := range p.seqs {
		errs = append(errs, c.Close())
	}
	if p.seq != nil {
		errs = append(errs, p.seq.Close())
	}
	return errors.Join(errs...)
}
