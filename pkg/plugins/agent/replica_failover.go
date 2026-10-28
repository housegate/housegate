package agent

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/log"
	"github.com/housegate/housegate/pkg/plugin"
	"github.com/housegate/housegate/pkg/registry"
)

// Replica failover reasons, the reason label of
// clickhouse_proxy_agent_replica_failovers_total and
// clickhouse_proxy_agent_replica_failover_exhausted_total.
const (
	FailoverReasonOwnerUnreachable = "owner_unreachable"
	FailoverReasonOwnerQuarantined = "owner_quarantined"
)

// ReplicaFailover is the agent's replica tier (spec 2026-10-14 §11, Plan
// S3-C4 K46): it fails a session's reads, never its writes, over to a host
// that serves the owner's database from its member's copy, when the owner
// refuses a read as quarantined or cannot be reached. It implements
// plugin.UpstreamFailover and plugin.FailoverReporter, which pkg/proxy
// re-exports as proxy.UpstreamFailover and proxy.FailoverReporter. The relay
// decides when a failover may happen; ReplicaFailover names the candidates
// and classifies the failure.
type ReplicaFailover struct {
	Discovery registry.StorageIntegrityReplicaDiscovery
	Databases registry.Databases
	Topology  registry.Topology

	// owners remembers each database's owner, as Candidates resolved it,
	// for the failover log.
	owners sync.Map
}

// Candidates returns the database's replica hosts in ascending indexer id,
// once each, as Url:HousegatePort, without the owner (Databases.Get) and
// without any indexer the topology does not know or that advertises no
// HouseGate port. An empty database, one Databases.Get does not know, and a
// failed discovery give no candidate: the original failure then reaches the
// client.
func (f *ReplicaFailover) Candidates(ctx context.Context, database string) ([]string, error) {
	if database == "" {
		return nil, errors.New("agent: replica failover: the session has no database")
	}
	db, ok := f.replicaDatabase(ctx, database)
	if !ok {
		return nil, fmt.Errorf("agent: replica failover: database %s is not in the network state", database)
	}
	f.owners.Store(database, db.IndexerId)
	hosts, err := f.Discovery.StorageIntegrityReplicaHosts(ctx, database, db.IndexerId)
	if err != nil {
		return nil, err
	}
	ids := append([]uint64(nil), hosts...)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	var out []string
	for i, id := range ids {
		if id == db.IndexerId || (i > 0 && ids[i-1] == id) {
			continue
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		addr, ok := f.replicaProxy(ctx, id)
		if !ok || addr.Url == "" || addr.HousegatePort == 0 {
			continue
		}
		out = append(out, addr.Addr())
	}
	return out, nil
}

// Use caller-context RPC accessors when available. Legacy in-memory ports
// keep their existing interface; custom I/O ports must expose these forms.
func (f *ReplicaFailover) replicaDatabase(ctx context.Context, database string) (registry.Database, bool) {
	if d, ok := f.Databases.(interface {
		GetContext(context.Context, string) (registry.Database, bool)
	}); ok {
		return d.GetContext(ctx, database)
	}
	return f.Databases.Get(database)
}
func (f *ReplicaFailover) replicaProxy(ctx context.Context, id uint64) (registry.ProxyAddress, bool) {
	if t, ok := f.Topology.(interface {
		ProxyByIndexerIdContext(context.Context, uint64) (registry.ProxyAddress, bool)
	}); ok {
		return t.ProxyByIndexerIdContext(ctx, id)
	}
	return f.Topology.ProxyByIndexerId(id)
}

// Reason classifies a failure. At FailoverAtRead only an Exception that
// chproto.IsIndexerQuarantinedReadMessage matches fails over
// (owner_quarantined); every other Exception of a read reaches the client
// unchanged. In either phase a failure without an Exception (dial, EOF,
// reset, read error) is owner_unreachable, and so is any Exception at
// FailoverAtHello, since the remote host's pivot to the owner failed.
func (f *ReplicaFailover) Reason(phase plugin.FailoverPhase, cause error, exc *chproto.Exception) (string, bool) {
	switch {
	case exc == nil && cause != nil:
		return FailoverReasonOwnerUnreachable, true
	case exc != nil && phase == plugin.FailoverAtHello:
		return FailoverReasonOwnerUnreachable, true
	case exc != nil && phase == plugin.FailoverAtRead && chproto.IsIndexerQuarantinedReadMessage(exc.Message):
		return FailoverReasonOwnerQuarantined, true
	}
	return "", false
}

// FailedOver logs a failover that reached a serving replica host.
func (f *ReplicaFailover) FailedOver(ctx context.Context, database, reason, from, to string) {
	owner, _ := f.owners.Load(database)
	_, logger := log.FromContext(ctx)
	logger.Infow("agent: replica failover", "database", database, "owner", owner, "reason", reason, "from", from, "to", to)
}

var (
	_ plugin.UpstreamFailover = (*ReplicaFailover)(nil)
	_ plugin.FailoverReporter = (*ReplicaFailover)(nil)
)
