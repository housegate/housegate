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
// eth_validator.go): a larger skew makes every statement token look issued
// in the future.
const maxClockSkew = 5 * time.Second

func (p *Plugin) observeDiscovery(step string) {
	if o, ok := p.observer.(DiscoveryObserver); ok && o != nil {
		o.SIDiscoveryFailed(step)
	}
}

// siInfo returns the hosting indexer's SI info for database, cached for the
// process lifetime once it succeeds (spec 2026-10-09 §6.4 step 3). A failure
// is not cached: the next INSERT asks again.
func (p *Plugin) siInfo(ctx context.Context, database string) (registry.StorageIntegrityInfo, error) {
	p.mu.Lock()
	info, ok := p.infos[database]
	p.mu.Unlock()
	if ok {
		return info, nil
	}
	info, err := p.discovery.StorageIntegrityInfo(ctx, database)
	if err != nil {
		return registry.StorageIntegrityInfo{}, err
	}
	if strings.TrimSpace(info.NetworkID) == "" {
		return info, nil // not cached: resolveNetworkID reports it as a failure
	}
	p.mu.Lock()
	p.infos[database] = info
	p.mu.Unlock()
	p.checkSkew(database, info)
	return info, nil
}

// resolveNetworkID is spec 2026-10-09 §6.4 steps 3-4 with plan decision P7:
// the network id the indexer hosting database reports. Without discovery the
// configured id is used as before. When discovery fails, a configured id is
// used with a warning; without one the INSERT is refused. A discovered id that
// differs from a configured one is refused: the agent never signs for the
// wrong network.
func (p *Plugin) resolveNetworkID(ctx context.Context, database string) (string, error) {
	if p.discovery == nil {
		return p.networkID, nil
	}
	info, err := p.siInfo(ctx, database)
	step := "info"
	if err == nil && strings.TrimSpace(info.NetworkID) == "" {
		step, err = "network_id", errors.New("the indexer reported no network_id")
	}
	if err != nil {
		p.observeDiscovery(step)
		if p.networkID != "" {
			_, logger := log.FromContext(ctx)
			logger.Warnw("sistatement: network discovery failed; signing for the configured storage_integrity.agent.network_id",
				"database", database, "network_id", p.networkID, "step", step, "err", err)
			return p.networkID, nil
		}
		return "", fmt.Errorf("storage_integrity agent: cannot discover network id from the indexer hosting %s: %w", database, err)
	}
	if p.networkID != "" && info.NetworkID != p.networkID {
		return "", fmt.Errorf("storage_integrity agent: the indexer hosting %s reports network %s but storage_integrity.agent.network_id is %s; refusing to sign for the wrong network", database, info.NetworkID, p.networkID)
	}
	return info.NetworkID, nil
}

// checkSkew warns once per database when the local clock is more than
// maxClockSkew away from the indexer's. It reports whether it warned.
func (p *Plugin) checkSkew(database string, info registry.StorageIntegrityInfo) bool {
	if info.ServerUnixTime == 0 {
		return false
	}
	skew := p.now().Sub(time.Unix(info.ServerUnixTime, 0))
	if skew < 0 {
		skew = -skew
	}
	if skew <= maxClockSkew {
		return false
	}
	p.mu.Lock()
	warned := p.skewWarned[database]
	p.skewWarned[database] = true
	p.mu.Unlock()
	if warned {
		return false
	}
	log.Warnw("sistatement: local clock differs from the indexer's; the ingress refuses statement tokens issued in the future",
		"database", database, "skew", skew, "tolerance", maxClockSkew)
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

// Close releases every counter this plugin holds, Options.Seq included, so
// their locks are free for the next agent; later INSERTs fail with
// ErrSeqClosed. It is idempotent.
func (p *Plugin) Close() error {
	if p == nil {
		return nil
	}
	p.seqMu.Lock()
	defer p.seqMu.Unlock()
	p.seqClosed = true
	var errs []error
	for _, c := range p.seqs {
		errs = append(errs, c.Close())
	}
	if p.seq != nil {
		errs = append(errs, p.seq.Close())
	}
	return errors.Join(errs...)
}
