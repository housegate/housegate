package storageintegrity

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"housegate/housegate/pkg/sqlmeta"
	sicore "housegate/housegate/pkg/storageintegrity"
)

// CompanionMutationConsensusAvailable reports whether the Sentio companion
// topology exposes the P2 mutation-consensus seam a classified mutation plan is
// submitted to: SubmitMutation, partition-barrier install, and the 2-of-3
// post-state quorum FSM lane (design sections 4.4 / 4.5 / 4.7). It is false
// today because arbiter/arbiter-proto are INSERT-only — the StatementKind enum
// is INSERT-only, there is no mutation FSM lane, and no SubmitMutation RPC. No
// real MutationSubmitter can be implemented until the companion seam lands; this
// is the single honest gate for the plugin-side mutation submission path. It is
// a distinct capability from intake.go's CompanionStagedIntakeAvailable (C1
// staged prepare) and from the core package's mutation-consensus gate; the
// plugin package declares its own so the plugin-side submission test can be
// gated without importing the core package.
const CompanionMutationConsensusAvailable = false

const (
	// reservedColumnPrefix marks the storage-integrity protocol columns a
	// mutation may not modify (design section 4.2).
	reservedColumnPrefix = "_hg_"
	// reservedRowIDColumn is the per-row identity column an UPDATE must preserve.
	reservedRowIDColumn = "_hg_row_id"
)

// MutationRejectReason is the typed reason a mutation is refused bounded
// admission. There is one reason per design section 4.2 reject bullet.
type MutationRejectReason string

const (
	RejectUnsupportedKind          MutationRejectReason = "unsupported_kind"
	RejectUnboundedPredicate       MutationRejectReason = "unbounded_predicate"
	RejectAffectedPartitionsLimit  MutationRejectReason = "affected_partitions_over_limit"
	RejectTouchedPartsLimit        MutationRejectReason = "touched_parts_over_limit"
	RejectTouchedBytesLimit        MutationRejectReason = "touched_bytes_over_limit"
	RejectProtocolColumn           MutationRejectReason = "protocol_column"
	RejectRowIDMutated             MutationRejectReason = "row_id_mutated"
	RejectKeyColumn                MutationRejectReason = "key_column"
	RejectLightweightDelete        MutationRejectReason = "lightweight_delete"
	RejectTruncateOrDropPartition  MutationRejectReason = "truncate_or_drop_partition"
	RejectDirectSafeModification   MutationRejectReason = "direct_hg_safe_modification"
	RejectUnstableExpression       MutationRejectReason = "unstable_expression"
	RejectNondeterministicFunc     MutationRejectReason = "nondeterministic_func"
	RejectSchemaRootMismatch       MutationRejectReason = "schema_root_mismatch"
	RejectManifestPartitionMissing MutationRejectReason = "manifest_partition_missing"
)

// allMutationRejectReasons is the canonical set, used by a meta-test to guard
// against silently dropping a reject when the design matrix changes.
var allMutationRejectReasons = []MutationRejectReason{
	RejectUnsupportedKind, RejectUnboundedPredicate, RejectAffectedPartitionsLimit,
	RejectTouchedPartsLimit, RejectTouchedBytesLimit, RejectProtocolColumn,
	RejectRowIDMutated, RejectKeyColumn, RejectLightweightDelete,
	RejectTruncateOrDropPartition, RejectDirectSafeModification, RejectUnstableExpression,
	RejectNondeterministicFunc, RejectSchemaRootMismatch, RejectManifestPartitionMissing,
}

// MutationRejection is a typed bounded-admission refusal. It implements error so
// it can flow through ClassifyMutation's error return.
type MutationRejection struct {
	Reason MutationRejectReason
	Detail string
}

func (r MutationRejection) Error() string {
	if r.Detail == "" {
		return "mutation admission rejected: " + string(r.Reason)
	}
	return fmt.Sprintf("mutation admission rejected: %s: %s", r.Reason, r.Detail)
}

func reject(reason MutationRejectReason, format string, args ...any) MutationRejection {
	return MutationRejection{Reason: reason, Detail: fmt.Sprintf(format, args...)}
}

// BarrierKey is the (table_id, partition_id) unit a mutation acquires barriers
// on (design section 4.5). It is a comparable value type.
type BarrierKey struct {
	TableID     string
	PartitionID string
}

// PartitionCost is the latest-manifest active inventory for one partition.
type PartitionCost struct {
	PartitionID string
	Parts       uint64
	Bytes       uint64
	RowCount    uint64
}

// ManifestSnapshot is the narrow HouseGate-local latest-manifest view the
// classifier estimates cost against. It is not the full replay manifest; a later
// wiring PR adapts a real manifest into it. SchemaRoot is the manifest's schema
// root, checked against the worker's local schema-root assertion.
type ManifestSnapshot struct {
	TableID    string
	SchemaRoot string
	Partitions []PartitionCost
}

func (s ManifestSnapshot) costFor(partitionID string) (PartitionCost, bool) {
	for _, p := range s.Partitions {
		if p.PartitionID == partitionID {
			return p, true
		}
	}
	return PartitionCost{}, false
}

// MutationAdmissionConfig bounds mutation cost. A zero field means unlimited for
// that dimension.
type MutationAdmissionConfig struct {
	MaxAffectedPartitions uint64
	MaxTouchedParts       uint64
	MaxTouchedBytes       uint64
}

// MutationRequest is the pure set of inputs the classifier reads. AffectedPartitions
// is the provably-affected partition-id set the predicate resolved to (empty
// signals an unprovable/unbounded predicate); AssignedColumns is the columns an
// UPDATE writes (empty for DELETE); KeyColumns is the partition/order/primary key
// columns of the table; LightweightDelete flags a ClickHouse lightweight-delete
// mask request. HouseGate does not parse SQL itself — the caller supplies the
// rewriter-produced StatementType/AccessedTables and the resolved partition set.
type MutationRequest struct {
	Kind               Kind
	StatementType      sqlmeta.StatementType
	SQL                string
	TableID            string
	AccessedTables     []sqlmeta.AccessedTable
	AffectedPartitions []string
	AssignedColumns    []string
	KeyColumns         []string
	Snapshot           ManifestSnapshot
	WorkerSchemaRoot   string
	LightweightDelete  bool
}

// MutationPlan is the accepted output: the mutation kind, table, the canonical
// barrier keys (sorted so the Arbiter acquires all barriers in canonical order),
// and the estimated touched parts/bytes.
type MutationPlan struct {
	Kind                   Kind
	TableID                string
	BarrierKeys            []BarrierKey
	EstimatedTouchedParts  uint64
	EstimatedTouchedBytes  uint64
	AffectedPartitionCount uint64
}

func (c MutationAdmissionConfig) withDefaults() MutationAdmissionConfig {
	return c // zero = unlimited per field; explicit method for symmetry with the plugin Config style
}

// EstimateMutationCost sums the active parts and bytes over the affected
// partitions from the latest manifest snapshot (design section 4.2: cost is the
// whole-partition active inventory, never a data-skipping-index estimate). It
// returns the partitions that are absent from the snapshot so the caller can
// fail closed.
func EstimateMutationCost(snapshot ManifestSnapshot, affectedPartitions []string) (touchedParts uint64, touchedBytes uint64, missing []string) {
	for _, pid := range affectedPartitions {
		cost, ok := snapshot.costFor(pid)
		if !ok {
			missing = append(missing, pid)
			continue
		}
		touchedParts += cost.Parts
		touchedBytes += cost.Bytes
	}
	return touchedParts, touchedBytes, missing
}

// ClassifyMutation runs the full design section 4.2 support/reject matrix and,
// on acceptance, produces a bounded MutationPlan. It is a pure function over the
// provided inputs and needs no companion seam. Every reject is typed.
func ClassifyMutation(cfg MutationAdmissionConfig, req MutationRequest) (MutationPlan, error) {
	cfg = cfg.withDefaults()

	// Kind must be a mutation and the rewriter's statement type must agree, so a
	// text/type mismatch is a reject, not a silent accept.
	if req.Kind != KindUpdate && req.Kind != KindDelete {
		return MutationPlan{}, reject(RejectUnsupportedKind, "kind %q is not a mutation", req.Kind)
	}
	switch req.StatementType {
	case sqlmeta.StatementTypeUpdate, sqlmeta.StatementTypeDelete, sqlmeta.StatementTypeAlterTable:
	default:
		return MutationPlan{}, reject(RejectUnsupportedKind, "statement type %s is not a mutation", req.StatementType)
	}

	// Never a user-entry TRUNCATE / DROP PARTITION, and never a direct hg_safe
	// modification (design section 4.2).
	if hasTruncateOrDropPartition(req.SQL) {
		return MutationPlan{}, reject(RejectTruncateOrDropPartition, "TRUNCATE / DROP PARTITION is not an admissible mutation")
	}
	if modifiesSafeTable(req.TableID, req.AccessedTables) {
		return MutationPlan{}, reject(RejectDirectSafeModification, "direct modification of hg_safe is not admissible")
	}

	// Lightweight DELETE masks are rejected: they do not produce a rewritable
	// part inventory.
	if req.LightweightDelete {
		return MutationPlan{}, reject(RejectLightweightDelete, "ClickHouse lightweight DELETE mask is not admissible")
	}

	// No subquery / join / dictionary / remote / table-function expression that
	// cannot stably freeze the touched set. The rewriter's AccessedTables tells us
	// if any accessed table is remote or if more than the single target is
	// touched.
	if reason := rejectUnstableAccess(req.TableID, req.AccessedTables); reason != "" {
		return MutationPlan{}, reject(RejectUnstableExpression, "%s", reason)
	}

	// No unmaterialized nondeterministic function may remain after signing.
	if fn, ok := containsUnmaterializedNondeterminism(req.SQL); ok {
		return MutationPlan{}, reject(RejectNondeterministicFunc, "unmaterialized nondeterministic function %s", fn)
	}

	// Column constraints: never modify a protocol column, never mutate
	// _hg_row_id, never modify a key column.
	for _, col := range req.AssignedColumns {
		lc := strings.ToLower(strings.TrimSpace(col))
		if lc == reservedRowIDColumn {
			return MutationPlan{}, reject(RejectRowIDMutated, "UPDATE must preserve %s", reservedRowIDColumn)
		}
		if strings.HasPrefix(lc, reservedColumnPrefix) {
			return MutationPlan{}, reject(RejectProtocolColumn, "may not modify protocol column %s", col)
		}
		if containsFold(req.KeyColumns, col) {
			return MutationPlan{}, reject(RejectKeyColumn, "may not modify key column %s", col)
		}
	}

	// Schema snapshot must match the worker's local schema-root assertion.
	if req.WorkerSchemaRoot != "" && req.Snapshot.SchemaRoot != "" && req.WorkerSchemaRoot != req.Snapshot.SchemaRoot {
		return MutationPlan{}, reject(RejectSchemaRootMismatch, "worker schema root %q != manifest schema root %q", req.WorkerSchemaRoot, req.Snapshot.SchemaRoot)
	}

	// The predicate must resolve to a provable, non-empty affected-partition set.
	if len(req.AffectedPartitions) == 0 {
		return MutationPlan{}, reject(RejectUnboundedPredicate, "no provable affected partitions")
	}

	// Bounded cost: affected-partition count, then touched parts/bytes from the
	// latest manifest. Any affected partition absent from the manifest fails
	// closed.
	if cfg.MaxAffectedPartitions > 0 && uint64(len(req.AffectedPartitions)) > cfg.MaxAffectedPartitions {
		return MutationPlan{}, reject(RejectAffectedPartitionsLimit, "%d affected partitions exceeds limit %d", len(req.AffectedPartitions), cfg.MaxAffectedPartitions)
	}
	touchedParts, touchedBytes, missing := EstimateMutationCost(req.Snapshot, req.AffectedPartitions)
	if len(missing) > 0 {
		return MutationPlan{}, reject(RejectManifestPartitionMissing, "affected partitions absent from manifest: %s", strings.Join(missing, ", "))
	}
	if cfg.MaxTouchedParts > 0 && touchedParts > cfg.MaxTouchedParts {
		return MutationPlan{}, reject(RejectTouchedPartsLimit, "%d touched parts exceeds limit %d", touchedParts, cfg.MaxTouchedParts)
	}
	if cfg.MaxTouchedBytes > 0 && touchedBytes > cfg.MaxTouchedBytes {
		return MutationPlan{}, reject(RejectTouchedBytesLimit, "%d touched bytes exceeds limit %d", touchedBytes, cfg.MaxTouchedBytes)
	}

	return MutationPlan{
		Kind:                   req.Kind,
		TableID:                req.TableID,
		BarrierKeys:            canonicalBarrierKeys(req.TableID, req.AffectedPartitions),
		EstimatedTouchedParts:  touchedParts,
		EstimatedTouchedBytes:  touchedBytes,
		AffectedPartitionCount: uint64(len(req.AffectedPartitions)),
	}, nil
}

// canonicalBarrierKeys returns one barrier key per affected partition, sorted in
// canonical (table, partition) order so the Arbiter acquires all barriers at
// once without deadlock (design section 4.5).
func canonicalBarrierKeys(tableID string, partitions []string) []BarrierKey {
	keys := make([]BarrierKey, 0, len(partitions))
	for _, p := range partitions {
		keys = append(keys, BarrierKey{TableID: tableID, PartitionID: p})
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].TableID != keys[j].TableID {
			return keys[i].TableID < keys[j].TableID
		}
		return keys[i].PartitionID < keys[j].PartitionID
	})
	return keys
}

func hasTruncateOrDropPartition(sql string) bool {
	up := strings.ToUpper(sql)
	return strings.Contains(up, "TRUNCATE") || strings.Contains(up, "DROP PARTITION")
}

func modifiesSafeTable(tableID string, tables []sqlmeta.AccessedTable) bool {
	if strings.Contains(strings.ToLower(tableID), "hg_safe") {
		return true
	}
	for _, t := range tables {
		if strings.EqualFold(t.OriginalDatabase, "hg_safe") {
			return true
		}
	}
	return false
}

// rejectUnstableAccess returns a non-empty reason when the mutation touches a
// remote table or any table beyond its single target (join / subquery /
// dictionary / table function), which cannot stably freeze the touched set.
func rejectUnstableAccess(tableID string, tables []sqlmeta.AccessedTable) string {
	for _, t := range tables {
		if t.IsRemote {
			return "mutation over a remote table cannot freeze a stable touched set"
		}
	}
	if len(tables) > 1 {
		return "mutation touching more than the single target table cannot freeze a stable touched set"
	}
	return ""
}

func containsFold(hay []string, needle string) bool {
	for _, h := range hay {
		if strings.EqualFold(strings.TrimSpace(h), strings.TrimSpace(needle)) {
			return true
		}
	}
	return false
}

// MutationSubmitter submits an accepted mutation plan to the Arbiter's
// mutation-consensus FSM (SubmitMutation + barrier install, design section 4.4).
// No implementation exists; see CompanionMutationConsensusAvailable. HouseGate
// only DRIVES the FSM through this port and never installs barriers, decides the
// 2/3 quorum, or publishes a manifest itself.
type MutationSubmitter interface {
	SubmitMutation(ctx context.Context, plan MutationPlan) (sicore.SubmitOutcome, error)
}
