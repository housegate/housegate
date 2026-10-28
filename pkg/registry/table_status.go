package registry

import "context"

// Storage-integrity table status names of the JSON-RPC contract (spec
// 2026-09-24 §10.2). They equal sitable.Status.String().
const (
	TableStatusOrdinary = "ordinary"
	TableStatusPending  = "pending"
	TableStatusRefused  = "refused"
	TableStatusActive   = "active"
	TableStatusGone     = "gone"
	// TableStatusReplica is another SI indexer's table that the serving host
	// answers read-only from its member's copy while that owner is
	// quarantined or unreachable (spec 2026-10-14 §11).
	TableStatusReplica = "replica"
)

// TableStatus is one answer of sentio_getStorageIntegrityTableStatus, with the
// semantics of sitable.Snapshot.Lookup on the serving node. SchemaJSON and
// SchemaHash are set for active (and gone) tables, for a refused table whose
// schema the host keeps (refused_code "indexer_quarantined": the host's SI
// indexer is quarantined), for a replica table (its owner's registry
// incarnation, spec 2026-10-14 §11), and may be set for a pending table that
// is an Active incarnation the host holds back (spec 2026-10-14 §14). The
// agent claims only an active table, whatever schema another status carries.
//
// ReplicaOf, ServedFrom, OwnerEntry and FrozenSince are set only for a
// replica table and omitted for every other status, so earlier answers stay
// byte-identical and an older server's answer decodes them as absent.
// ReplicaOf is a pointer because indexer 0 is a valid owner.
type TableStatus struct {
	Status          string      `json:"status"`
	RefusedCode     string      `json:"refused_code"`
	RefusedReason   string      `json:"refused_reason"`
	SchemaJSON      string      `json:"schema_json"`
	SchemaHash      string      `json:"schema_hash"`
	RegistryVersion uint64      `json:"registry_version"`
	ReplicaOf       *uint64     `json:"replica_of,omitempty"`   // the owner's SI indexer id
	ServedFrom      string      `json:"served_from,omitempty"`  // snapshot id of the published manifest the copy equals
	OwnerEntry      *OwnerEntry `json:"owner_entry,omitempty"`  // the owner's track_frontiers entry in that manifest
	FrozenSince     int64       `json:"frozen_since,omitempty"` // unix ms since that entry has been unchanged on the member
}

// OwnerEntry is the owner's track_frontiers entry of a published manifest. Its
// JSON tags are those of housegate replay.TrackFrontier; this package imports
// no replay package.
type OwnerEntry struct {
	IndexerID           uint64 `json:"indexer_id"`
	SafeBlockSeq        uint64 `json:"safe_block_seq"`
	AppliedPromotionSeq uint64 `json:"applied_promotion_seq"`
}

// TableStatuses is the agent's per-INSERT status source. Like TableSchemas it
// stays out of Registry so routing-only implementations need not stub it.
type TableStatuses interface {
	StorageIntegrityTableStatus(ctx context.Context, database, table string) (TableStatus, error)
}

// TableStatusesFromSchemas adapts a declared-schema source (the YAML
// table_schemas fixture, or a host-injected TableSchemas) into a status
// source: a declared table is active with its latest declaration, and every
// other table is ordinary.
func TableStatusesFromSchemas(schemas TableSchemas) TableStatuses {
	return declaredSchemaStatuses{schemas: schemas}
}

type declaredSchemaStatuses struct{ schemas TableSchemas }

func (d declaredSchemaStatuses) StorageIntegrityTableStatus(_ context.Context, database, table string) (TableStatus, error) {
	latest, ok := d.schemas.LatestTableSchema(database, table)
	if !ok {
		return TableStatus{Status: TableStatusOrdinary}, nil
	}
	return TableStatus{Status: TableStatusActive, SchemaJSON: latest.SchemaJson, SchemaHash: latest.SchemaHash}, nil
}
