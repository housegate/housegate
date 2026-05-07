package cluster

import "context"

// Cluster is the consumer-facing surface of a cluster manager.
//
// *Manager is the production implementation; library users may inject
// their own (e.g. a fake topology in integration tests). Start and
// Close stay on *Manager only — when the housegate library builds the
// manager itself, it owns Start/Close; when the caller injects a
// Cluster, the caller must hand it over already-Started and Close it
// after housegate's Run returns.
//
// GetConnection returns a *PooledConn (a concrete pkg/cluster type)
// rather than an interface; the connection-lifetime API
// (ReturnConnection / DiscardConnection / RecordQuerySuccess /
// RecordQueryError) lives on *Manager and stays internal to the
// cluster package. Custom Cluster implementations should construct
// PooledConns via cluster-package helpers if/when they're added.
type Cluster interface {
	GetConnection(ctx context.Context) (*PooledConn, error)
	HasReplica(addr string) bool
}

// Compile-time guard: *Manager must satisfy Cluster.
var _ Cluster = (*Manager)(nil)
