// Package storageintegrity contains the HouseGate-side protocol client types
// for the Sentio storage-integrity INSERT flow.
package storageintegrity

import (
	"context"
	"time"

	"housegate/housegate/pkg/replay"
)

type Ack struct {
	OK bool `json:"ok"`
}

type PayloadCommitment struct {
	Ref    string `json:"ref"`
	Hash   string `json:"hash"`
	Length uint64 `json:"length"`
}

type PutPayloadRequest struct {
	TableID     string
	StatementID string
	Payload     []byte
}

type PayloadStore interface {
	PutPayload(ctx context.Context, req PutPayloadRequest) (PayloadCommitment, error)
}

type InsertRecord struct {
	TableID             string            `json:"table_id"`
	StatementID         string            `json:"statement_id"`
	OriginalSQL         string            `json:"original_sql"`
	UnsafeSQL           string            `json:"unsafe_sql"`
	UnsafeTable         string            `json:"unsafe_table"`
	SafeTable           string            `json:"safe_table"`
	PartitionIDs        []string          `json:"partition_ids,omitempty"`
	UserJWS             string            `json:"user_jws,omitempty"`
	AuthenticatedSigner string            `json:"authenticated_signer,omitempty"`
	Payload             PayloadCommitment `json:"payload"`
	SourceClaimRoot     string            `json:"source_claim_root"`
	PayloadEncoding     string            `json:"payload_encoding,omitempty"`
	PayloadRevision     int               `json:"payload_revision,omitempty"`
	PrevSafeSnapshotID  string            `json:"prev_safe_snapshot_id,omitempty"`
	PrevStateRoot       string            `json:"prev_state_root,omitempty"`
	SchemaSnapshotID    string            `json:"schema_snapshot_id,omitempty"`
	ExecutorProfileID   string            `json:"executor_profile_id,omitempty"`
	SettingsHash        string            `json:"settings_hash,omitempty"`
	ReceivedAt          time.Time         `json:"received_at"`
}

const (
	MutationTypeUpdate = "update"
	MutationTypeDelete = "delete"
)

type MutationRecord struct {
	TableID             string    `json:"table_id"`
	StatementID         string    `json:"statement_id"`
	MutationType        string    `json:"mutation_type"`
	OriginalSQL         string    `json:"original_sql"`
	MutationSQL         string    `json:"mutation_sql"`
	SafeTable           string    `json:"safe_table"`
	PartitionIDs        []string  `json:"partition_ids,omitempty"`
	UserJWS             string    `json:"user_jws,omitempty"`
	AuthenticatedSigner string    `json:"authenticated_signer,omitempty"`
	ExecutionMode       string    `json:"execution_mode,omitempty"`
	ReceivedAt          time.Time `json:"received_at"`
}

type SequencerIngress interface {
	SubmitInsert(ctx context.Context, rec InsertRecord) error
	SubmitMutation(ctx context.Context, rec MutationRecord) error
}

type MutationClaimSigner interface {
	SignClaim(claimHash string) (string, error)
}

type ReplayVerifier interface {
	Verify(ctx context.Context, job replay.ReplayJob) (replay.ReplayAttestation, error)
}

type ByteSideScanner interface {
	ScanByteSide(ctx context.Context, task ByteSideScanTask) (ByteSideScanResult, error)
}

type Promoter interface {
	Promote(ctx context.Context, task PromotionTask) (PromotionResult, error)
}

type MutationExecutor interface {
	ExecuteMutation(ctx context.Context, task MutationTask) (MutationClaim, error)
	ReplayMutation(ctx context.Context, task MutationTask) (MutationReplayResult, error)
}

type SafeAuditor interface {
	AuditSafe(ctx context.Context, task SafeAuditTask) (SafeAuditVote, error)
}

type RollbackExecutor interface {
	Rollback(ctx context.Context, task RollbackTask) (RollbackResult, error)
}

type RepairSyncExecutor interface {
	RepairSync(ctx context.Context, task RepairSyncTask) (RepairSyncResult, error)
}

type CompactionExecutor interface {
	Compact(ctx context.Context, task CompactionTask) (CompactionResult, error)
}

type ByteSidePart struct {
	PartitionID   string `json:"partition_id"`
	PartName      string `json:"part_name"`
	RowCount      uint64 `json:"row_count"`
	PartRowLtHash string `json:"part_row_lthash"`
}

type ByteSideScanTask struct {
	ScanID         string         `json:"scan_id"`
	StatementID    string         `json:"statement_id"`
	TableID        string         `json:"table_id"`
	UnsafeTable    string         `json:"unsafe_table"`
	PartitionIDs   []string       `json:"partition_ids,omitempty"`
	CandidateParts []ByteSidePart `json:"candidate_parts,omitempty"`
}

type ByteSideScanResult struct {
	ScanID      string         `json:"scan_id"`
	StatementID string         `json:"statement_id"`
	TableID     string         `json:"table_id"`
	UnsafeTable string         `json:"unsafe_table"`
	WorkerID    string         `json:"worker_id"`
	Parts       []ByteSidePart `json:"parts,omitempty"`
	PartSetHash string         `json:"part_set_hash,omitempty"`
}

type MutationTask struct {
	StatementID           string                       `json:"statement_id"`
	TableID               string                       `json:"table_id"`
	MutationType          string                       `json:"mutation_type"`
	MutationSQL           string                       `json:"mutation_sql"`
	SafeTable             string                       `json:"safe_table"`
	BaseSafeSnapshotID    string                       `json:"base_safe_snapshot_id,omitempty"`
	BasePartitionRoot     string                       `json:"base_partition_root,omitempty"`
	BasePartitionRoots    []replay.PartitionCommitment `json:"base_partition_roots,omitempty"`
	SchemaSnapshotID      string                       `json:"schema_snapshot_id,omitempty"`
	PromotionSeq          uint64                       `json:"promotion_seq,omitempty"`
	PendingInsertBarrier  bool                         `json:"pending_insert_barrier,omitempty"`
	RebindCount           int                          `json:"rebind_count,omitempty"`
	InternalDropPartition bool                         `json:"internal_drop_partition,omitempty"`
	DropPartitionIDs      []string                     `json:"drop_partition_ids,omitempty"`
	PartitionIDs          []string                     `json:"partition_ids,omitempty"`
}

type PartitionDelta struct {
	TableID     string `json:"table_id,omitempty"`
	PartitionID string `json:"partition_id"`
	BaseRoot    string `json:"base_root,omitempty"`
	PostRoot    string `json:"post_root"`
	DeltaRoot   string `json:"delta_root"`
	RowsBefore  uint64 `json:"rows_before,omitempty"`
	RowsAfter   uint64 `json:"rows_after,omitempty"`
	RowsUpdated uint64 `json:"rows_updated,omitempty"`
	RowsDeleted uint64 `json:"rows_deleted,omitempty"`
}

type MutationClaim struct {
	StatementID              string                       `json:"statement_id"`
	WorkerID                 string                       `json:"worker_id"`
	ScratchTable             string                       `json:"scratch_table"`
	BaseSafeSnapshotID       string                       `json:"base_safe_snapshot_id,omitempty"`
	BasePartitionRoot        string                       `json:"base_partition_root,omitempty"`
	BasePartitionRoots       []replay.PartitionCommitment `json:"base_partition_roots,omitempty"`
	SchemaSnapshotID         string                       `json:"schema_snapshot_id,omitempty"`
	PromotionSeq             uint64                       `json:"promotion_seq,omitempty"`
	PostStateRoot            string                       `json:"post_state_root"`
	PostPartitionCommitments []replay.PartitionCommitment `json:"post_partition_commitments,omitempty"`
	PartitionDeltas          []PartitionDelta             `json:"partition_deltas,omitempty"`
	RowsBefore               uint64                       `json:"rows_before,omitempty"`
	RowsAfter                uint64                       `json:"rows_after,omitempty"`
	RowsUpdated              uint64                       `json:"rows_updated,omitempty"`
	RowsDeleted              uint64                       `json:"rows_deleted,omitempty"`
	Parts                    []ByteSidePart               `json:"parts,omitempty"`
	ClaimHash                string                       `json:"claim_hash,omitempty"`
	Signature                string                       `json:"signature,omitempty"`
	Error                    string                       `json:"error,omitempty"`
	PendingInsertBarrier     bool                         `json:"pending_insert_barrier,omitempty"`
	StaleRebind              bool                         `json:"stale_rebind,omitempty"`
	StaleReason              string                       `json:"stale_reason,omitempty"`
	LatestSafeSnapshotID     string                       `json:"latest_safe_snapshot_id,omitempty"`
	LatestStateRoot          string                       `json:"latest_state_root,omitempty"`
	RebindCount              int                          `json:"rebind_count,omitempty"`
}

type MutationReplayResult struct {
	StatementID              string                       `json:"statement_id"`
	WorkerID                 string                       `json:"worker_id"`
	BaseSafeSnapshotID       string                       `json:"base_safe_snapshot_id,omitempty"`
	BasePartitionRoot        string                       `json:"base_partition_root,omitempty"`
	BasePartitionRoots       []replay.PartitionCommitment `json:"base_partition_roots,omitempty"`
	SchemaSnapshotID         string                       `json:"schema_snapshot_id,omitempty"`
	PromotionSeq             uint64                       `json:"promotion_seq,omitempty"`
	PostStateRoot            string                       `json:"post_state_root"`
	PostPartitionCommitments []replay.PartitionCommitment `json:"post_partition_commitments,omitempty"`
	PartitionDeltas          []PartitionDelta             `json:"partition_deltas,omitempty"`
	RowsBefore               uint64                       `json:"rows_before,omitempty"`
	RowsAfter                uint64                       `json:"rows_after,omitempty"`
	RowsUpdated              uint64                       `json:"rows_updated,omitempty"`
	RowsDeleted              uint64                       `json:"rows_deleted,omitempty"`
	Parts                    []ByteSidePart               `json:"parts,omitempty"`
	ClaimHash                string                       `json:"claim_hash,omitempty"`
	Signature                string                       `json:"signature,omitempty"`
	Error                    string                       `json:"error,omitempty"`
	PendingInsertBarrier     bool                         `json:"pending_insert_barrier,omitempty"`
	StaleRebind              bool                         `json:"stale_rebind,omitempty"`
	StaleReason              string                       `json:"stale_reason,omitempty"`
	LatestSafeSnapshotID     string                       `json:"latest_safe_snapshot_id,omitempty"`
	LatestStateRoot          string                       `json:"latest_state_root,omitempty"`
	RebindCount              int                          `json:"rebind_count,omitempty"`
}

type PromotionTask struct {
	PromotionID             string         `json:"promotion_id"`
	PromotionSeq            uint64         `json:"promotion_seq,omitempty"`
	LeaseID                 string         `json:"lease_id,omitempty"`
	Kind                    string         `json:"kind,omitempty"`
	TableID                 string         `json:"table_id,omitempty"`
	BaseSafeSnapshotID      string         `json:"base_safe_snapshot_id,omitempty"`
	BasePartitionRoot       string         `json:"base_partition_root,omitempty"`
	ExpectedPostRoot        string         `json:"expected_post_root,omitempty"`
	UnsafeTable             string         `json:"unsafe_table,omitempty"`
	SafeTable               string         `json:"safe_table"`
	SourceTable             string         `json:"source_table,omitempty"`
	PromoteDatabase         string         `json:"promote_database,omitempty"`
	ReplacePartition        bool           `json:"replace_partition,omitempty"`
	SkipBasePartitionAttach bool           `json:"skip_base_partition_attach,omitempty"`
	CleanupUnsafe           bool           `json:"cleanup_unsafe,omitempty"`
	InternalDropPartition   bool           `json:"internal_drop_partition,omitempty"`
	RequireBaseRootCAS      bool           `json:"require_base_root_cas,omitempty"`
	RequirePostRootCAS      bool           `json:"require_post_root_cas,omitempty"`
	RequirePromotionSeq     bool           `json:"require_promotion_seq,omitempty"`
	PartitionIDs            []string       `json:"partition_ids,omitempty"`
	DropPartitionIDs        []string       `json:"drop_partition_ids,omitempty"`
	StatementIDs            []string       `json:"statement_ids,omitempty"`
	Statements              []string       `json:"statements,omitempty"`
	CandidateParts          []ByteSidePart `json:"candidate_parts,omitempty"`
	CleanupUnsafeParts      []ByteSidePart `json:"cleanup_unsafe_parts,omitempty"`
}

type PromotionResult struct {
	PromotionID        string                     `json:"promotion_id"`
	PromotionSeq       uint64                     `json:"promotion_seq,omitempty"`
	LeaseID            string                     `json:"lease_id,omitempty"`
	WorkerID           string                     `json:"worker_id"`
	TableID            string                     `json:"table_id,omitempty"`
	BaseSafeSnapshotID string                     `json:"base_safe_snapshot_id,omitempty"`
	BasePartitionRoot  string                     `json:"base_partition_root,omitempty"`
	SafeTable          string                     `json:"safe_table,omitempty"`
	SourceTable        string                     `json:"source_table,omitempty"`
	PartitionIDs       []string                   `json:"partition_ids,omitempty"`
	StatementIDs       []string                   `json:"statement_ids,omitempty"`
	ActiveParts        []replay.PartManifestEntry `json:"active_parts,omitempty"`
	CleanupUnsafeParts []ByteSidePart             `json:"cleanup_unsafe_parts,omitempty"`
	Error              string                     `json:"error,omitempty"`
}

type SafeWatermark struct {
	SnapshotID   string `json:"snapshot_id"`
	SafeBlockSeq uint64 `json:"safe_block_seq"`
	StateRoot    string `json:"state_root"`
	ManifestRoot string `json:"manifest_root"`
}

type PromotionReceipt struct {
	OK        bool                        `json:"ok"`
	Manifest  replay.SafeSnapshotManifest `json:"manifest,omitempty"`
	Watermark SafeWatermark               `json:"watermark,omitempty"`
}

type SafeAuditTask struct {
	AuditID      string                      `json:"audit_id"`
	SnapshotID   string                      `json:"snapshot_id"`
	TableID      string                      `json:"table_id,omitempty"`
	SafeTable    string                      `json:"safe_table"`
	StateRoot    string                      `json:"state_root"`
	Manifest     replay.SafeSnapshotManifest `json:"manifest,omitempty"`
	PartitionIDs []string                    `json:"partition_ids,omitempty"`
}

type SafeAuditVote struct {
	AuditID          string                     `json:"audit_id"`
	SnapshotID       string                     `json:"snapshot_id"`
	WorkerID         string                     `json:"worker_id"`
	StateRoot        string                     `json:"state_root"`
	ManifestRoot     string                     `json:"manifest_root,omitempty"`
	Match            bool                       `json:"match"`
	ActivePartsMatch bool                       `json:"active_parts_match,omitempty"`
	ActiveParts      []replay.PartManifestEntry `json:"active_parts,omitempty"`
	Error            string                     `json:"error,omitempty"`
}

type RollbackTask struct {
	RollbackID    string         `json:"rollback_id"`
	StatementID   string         `json:"statement_id,omitempty"`
	PromotionID   string         `json:"promotion_id,omitempty"`
	TableID       string         `json:"table_id,omitempty"`
	Reason        string         `json:"reason,omitempty"`
	SafeTable     string         `json:"safe_table,omitempty"`
	UnsafeTable   string         `json:"unsafe_table,omitempty"`
	ScratchTable  string         `json:"scratch_table,omitempty"`
	PromoteTable  string         `json:"promote_table,omitempty"`
	ScratchTables []string       `json:"scratch_tables,omitempty"`
	PromoteTables []string       `json:"promote_tables,omitempty"`
	PartitionIDs  []string       `json:"partition_ids,omitempty"`
	UnsafeParts   []ByteSidePart `json:"unsafe_parts,omitempty"`
}

type RollbackResult struct {
	RollbackID              string         `json:"rollback_id"`
	StatementID             string         `json:"statement_id,omitempty"`
	PromotionID             string         `json:"promotion_id,omitempty"`
	WorkerID                string         `json:"worker_id"`
	TableID                 string         `json:"table_id,omitempty"`
	Reason                  string         `json:"reason,omitempty"`
	UnsafeTable             string         `json:"unsafe_table,omitempty"`
	ScratchTable            string         `json:"scratch_table,omitempty"`
	PromoteTable            string         `json:"promote_table,omitempty"`
	PartitionIDs            []string       `json:"partition_ids,omitempty"`
	CleanedUnsafeParts      []ByteSidePart `json:"cleaned_unsafe_parts,omitempty"`
	DroppedUnsafePartitions []string       `json:"dropped_unsafe_partitions,omitempty"`
	DroppedScratch          bool           `json:"dropped_scratch,omitempty"`
	DroppedPromote          bool           `json:"dropped_promote,omitempty"`
	DroppedScratchTables    []string       `json:"dropped_scratch_tables,omitempty"`
	DroppedPromoteTables    []string       `json:"dropped_promote_tables,omitempty"`
	Error                   string         `json:"error,omitempty"`
}

type RepairSyncTask struct {
	RepairID             string                      `json:"repair_id"`
	SnapshotID           string                      `json:"snapshot_id,omitempty"`
	TableID              string                      `json:"table_id,omitempty"`
	SafeTable            string                      `json:"safe_table"`
	SourceTable          string                      `json:"source_table,omitempty"`
	Manifest             replay.SafeSnapshotManifest `json:"manifest,omitempty"`
	PartitionIDs         []string                    `json:"partition_ids,omitempty"`
	ExpectedStateRoot    string                      `json:"expected_state_root,omitempty"`
	ExpectedManifestRoot string                      `json:"expected_manifest_root,omitempty"`
	VerifyOnly           bool                        `json:"verify_only,omitempty"`
	RequireManifestMatch bool                        `json:"require_manifest_match,omitempty"`
}

type RepairSyncResult struct {
	RepairID         string                     `json:"repair_id"`
	WorkerID         string                     `json:"worker_id"`
	SnapshotID       string                     `json:"snapshot_id,omitempty"`
	TableID          string                     `json:"table_id,omitempty"`
	SafeTable        string                     `json:"safe_table,omitempty"`
	SourceTable      string                     `json:"source_table,omitempty"`
	PartitionIDs     []string                   `json:"partition_ids,omitempty"`
	StateRoot        string                     `json:"state_root,omitempty"`
	ManifestRoot     string                     `json:"manifest_root,omitempty"`
	ActiveParts      []replay.PartManifestEntry `json:"active_parts,omitempty"`
	Repaired         bool                       `json:"repaired,omitempty"`
	InSync           bool                       `json:"in_sync"`
	ActivePartsMatch bool                       `json:"active_parts_match,omitempty"`
	Error            string                     `json:"error,omitempty"`
}

type CompactionTask struct {
	CompactionID       string                     `json:"compaction_id"`
	PromotionSeq       uint64                     `json:"promotion_seq,omitempty"`
	TableID            string                     `json:"table_id,omitempty"`
	SafeTable          string                     `json:"safe_table"`
	CompactDatabase    string                     `json:"compact_database,omitempty"`
	CompactTable       string                     `json:"compact_table,omitempty"`
	BaseSafeSnapshotID string                     `json:"base_safe_snapshot_id,omitempty"`
	BasePartitionRoot  string                     `json:"base_partition_root,omitempty"`
	ExpectedPostRoot   string                     `json:"expected_post_root,omitempty"`
	PartitionIDs       []string                   `json:"partition_ids,omitempty"`
	InputParts         []replay.PartManifestEntry `json:"input_parts,omitempty"`
	RequireBaseRootCAS bool                       `json:"require_base_root_cas,omitempty"`
	RequirePostRootCAS bool                       `json:"require_post_root_cas,omitempty"`
	DropCompactTable   bool                       `json:"drop_compact_table,omitempty"`
}

type CompactionResult struct {
	CompactionID       string                     `json:"compaction_id"`
	PromotionSeq       uint64                     `json:"promotion_seq,omitempty"`
	WorkerID           string                     `json:"worker_id"`
	TableID            string                     `json:"table_id,omitempty"`
	SafeTable          string                     `json:"safe_table,omitempty"`
	CompactTable       string                     `json:"compact_table,omitempty"`
	BaseSafeSnapshotID string                     `json:"base_safe_snapshot_id,omitempty"`
	BasePartitionRoot  string                     `json:"base_partition_root,omitempty"`
	ExpectedPostRoot   string                     `json:"expected_post_root,omitempty"`
	PartitionIDs       []string                   `json:"partition_ids,omitempty"`
	ActiveParts        []replay.PartManifestEntry `json:"active_parts,omitempty"`
	Error              string                     `json:"error,omitempty"`
}

type SafeReadRequest struct {
	NodeID     string   `json:"node_id"`
	SnapshotID string   `json:"snapshot_id,omitempty"`
	TableIDs   []string `json:"table_ids,omitempty"`
}

type SafeReadDecision struct {
	Active       bool   `json:"active"`
	Reason       string `json:"reason,omitempty"`
	SnapshotID   string `json:"snapshot_id,omitempty"`
	SafeBlockSeq uint64 `json:"safe_block_seq,omitempty"`
}

type SequencerWorkerClient interface {
	ClaimReplayJob(ctx context.Context) (replay.ReplayJob, bool, error)
	SubmitReplayAttestation(ctx context.Context, att replay.ReplayAttestation) error
	ClaimByteSideScan(ctx context.Context) (ByteSideScanTask, bool, error)
	SubmitByteSideScan(ctx context.Context, result ByteSideScanResult) error
	ClaimPromotion(ctx context.Context) (PromotionTask, bool, error)
	SubmitPromotionResult(ctx context.Context, result PromotionResult) (PromotionReceipt, error)
	ClaimMutationTask(ctx context.Context) (MutationTask, bool, error)
	SubmitMutationClaim(ctx context.Context, claim MutationClaim) error
	ClaimMutationReplayTask(ctx context.Context) (MutationTask, bool, error)
	SubmitMutationReplay(ctx context.Context, result MutationReplayResult) error
	ClaimSafeAudit(ctx context.Context) (SafeAuditTask, bool, error)
	SubmitSafeAudit(ctx context.Context, vote SafeAuditVote) error
	ClaimRollback(ctx context.Context) (RollbackTask, bool, error)
	SubmitRollback(ctx context.Context, result RollbackResult) error
	ClaimRepairSync(ctx context.Context) (RepairSyncTask, bool, error)
	SubmitRepairSync(ctx context.Context, result RepairSyncResult) error
	ClaimCompaction(ctx context.Context) (CompactionTask, bool, error)
	SubmitCompaction(ctx context.Context, result CompactionResult) error
}

type SnapshotReader interface {
	GetSafeWatermark(ctx context.Context) (SafeWatermark, error)
	GetSafeSnapshot(ctx context.Context, snapshotID string) (replay.SafeSnapshotManifest, bool, error)
}

type ReadSetGate interface {
	CheckSafeRead(ctx context.Context, req SafeReadRequest) (SafeReadDecision, error)
}
