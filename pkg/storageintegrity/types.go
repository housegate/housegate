// Package storageintegrity contains HouseGate-side runtime primitives for the
// HouseKeeper storage-integrity protocol. HouseKeeper owns the authoritative
// state machine; this package owns local workers and mock adapters.
package storageintegrity

import (
	"context"
	"time"

	"housegate/housegate/pkg/replay"
)

// PayloadCommitment is the HouseGate-produced digest/ref pair submitted to
// HouseKeeper. The referenced bytes stay outside Keeper state.
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

// InsertRecord is the ingress claim produced after ClickHouse accepts an
// INSERT into the unsafe table. HouseKeeper owns the authoritative version of
// this state; the local coordinator uses the same shape for P0 demos/tests.
type InsertRecord struct {
	TableID     string
	StatementID string
	OriginalSQL string
	UnsafeSQL   string
	UnsafeTable string
	SafeTable   string
	Payload     PayloadCommitment
	ReceivedAt  time.Time
}

type IngressSink interface {
	SubmitInsert(ctx context.Context, rec InsertRecord) error
}

type ControlPlane interface {
	IngressSink
	FinalitySink
	ReplayJobSource
	ReplaySink
	UnsafeValidationSource
	UnsafeValidationSink
	PromotionSource
	PromotionSink
	RollbackSource
	RollbackSink
	SafeAuditSource
	SafeAuditSink
}

type FinalityRecord struct {
	Kind        string    `json:"kind"`
	BatchID     string    `json:"batch_id"`
	StatementID string    `json:"statement_id,omitempty"`
	PayloadRef  string    `json:"payload_ref,omitempty"`
	PayloadHash string    `json:"payload_hash,omitempty"`
	Finalized   bool      `json:"finalized"`
	FinalizedAt time.Time `json:"finalized_at"`
}

type FinalityEvent = FinalityRecord

type FinalitySink interface {
	SubmitFinality(ctx context.Context, rec FinalityRecord) error
}

// ReplayVerifier is satisfied by replay.Verifier and by tests.
type ReplayVerifier interface {
	Verify(ctx context.Context, job replay.ReplayJob) (replay.ReplayAttestation, error)
}

type ReplayJobSource interface {
	ClaimReplayJob(ctx context.Context) (replay.ReplayJob, bool, error)
}

type ReplaySink interface {
	SubmitReplayAttestation(ctx context.Context, att replay.ReplayAttestation) error
	SubmitReplayFailure(ctx context.Context, failure ReplayFailure) error
}

type ReplayFailure struct {
	BlockSeq uint64 `json:"block_seq"`
	Error    string `json:"error"`
}

type UnsafeReplica struct {
	ReplicaID string `json:"replica_id"`
	Addr      string `json:"addr"`
}

type UnsafeValidationTask struct {
	ValidationID string          `json:"validation_id"`
	StatementID  string          `json:"statement_id"`
	TableID      string          `json:"table_id"`
	UnsafeTable  string          `json:"unsafe_table"`
	Replicas     []UnsafeReplica `json:"replicas"`
}

type UnsafeReplicaDigest struct {
	ReplicaID string `json:"replica_id"`
	RowCount  uint64 `json:"row_count"`
	RowsHash  string `json:"rows_hash"`
}

type UnsafeValidationResult struct {
	ValidationID string                `json:"validation_id"`
	StatementID  string                `json:"statement_id"`
	TableID      string                `json:"table_id"`
	UnsafeTable  string                `json:"unsafe_table"`
	RowCount     uint64                `json:"row_count"`
	RowsHash     string                `json:"rows_hash"`
	Replicas     []UnsafeReplicaDigest `json:"replicas"`
}

type UnsafeValidationFailure struct {
	ValidationID string `json:"validation_id"`
	StatementID  string `json:"statement_id"`
	Error        string `json:"error"`
}

type UnsafeValidationSource interface {
	ClaimUnsafeValidation(ctx context.Context) (UnsafeValidationTask, bool, error)
}

type UnsafeValidationSink interface {
	SubmitUnsafeValidation(ctx context.Context, result UnsafeValidationResult) error
	SubmitUnsafeValidationFailure(ctx context.Context, failure UnsafeValidationFailure) error
}

type UnsafeTableVerifier interface {
	VerifyUnsafe(ctx context.Context, task UnsafeValidationTask) (UnsafeValidationResult, error)
}

type PromotionTask struct {
	PromotionID string
	LeaseID     string
	Statements  []string
	Readback    PromotionReadbackSpec
}

type PromotionReadbackSpec struct {
	Table         string
	PromotionExpr string
	ExpectedRows  uint64
	ExpectedHash  string
}

type PromotionReadbackResult struct {
	RowCount uint64
	RowsHash string
}

type PromotionResult struct {
	PromotionID string
	LeaseID     string
	Readback    PromotionReadbackResult
}

type PromotionFailure struct {
	PromotionID string
	LeaseID     string
	Error       string
}

type PromotionExecutor interface {
	ExecPromotionSQL(ctx context.Context, sql string) error
	ReadPromotionRows(ctx context.Context, spec PromotionReadbackSpec) (PromotionReadbackResult, error)
}

type PromotionSink interface {
	FinishPromotion(ctx context.Context, result PromotionResult) error
	FailPromotion(ctx context.Context, failure PromotionFailure) error
}

type PromotionSource interface {
	ClaimPromotion(ctx context.Context) (PromotionTask, bool, error)
}

type RollbackEvent struct {
	Kind        string    `json:"kind"`
	BatchID     string    `json:"batch_id"`
	StatementID string    `json:"statement_id,omitempty"`
	Reason      string    `json:"reason"`
	ReceivedAt  time.Time `json:"received_at"`
}

type RollbackTask struct {
	RollbackID  string
	LeaseID     string
	BatchID     string
	StatementID string
	Reason      string
	Statements  []string
}

type RollbackResult struct {
	RollbackID string
	LeaseID    string
}

type RollbackFailure struct {
	RollbackID string
	LeaseID    string
	Error      string
}

type RollbackExecutor interface {
	ExecRollbackSQL(ctx context.Context, sql string) error
}

type RollbackSink interface {
	SubmitRollback(ctx context.Context, event RollbackEvent) error
	FinishRollback(ctx context.Context, result RollbackResult) error
	FailRollback(ctx context.Context, failure RollbackFailure) error
}

type RollbackSource interface {
	ClaimRollback(ctx context.Context) (RollbackTask, bool, error)
}

type SafeAuditTask struct {
	AuditID    string
	ReplicaID  string
	NetworkID  string
	TableID    string
	SchemaHash string
	SnapshotID string
	Range      string
}

type SafeAuditReplica struct {
	ReplicaID string `json:"replica_id" yaml:"replica_id"`
}

type SafeRow struct {
	RowID  string
	Values []any
}

type SafeAuditVote struct {
	AuditID    string `json:"audit_id"`
	WorkerID   string `json:"worker_id"`
	ReplicaID  string `json:"replica_id"`
	SnapshotID string `json:"snapshot_id"`
	Range      string `json:"range"`
	BatchHash  string `json:"batch_hash"`
	RowCount   uint64 `json:"row_count"`
	VoteHash   string `json:"vote_hash"`
	Signature  string `json:"signature"`
}

type SafeAuditDecisionStatus string

const (
	SafeAuditStatusPending  SafeAuditDecisionStatus = "pending"
	SafeAuditStatusMajority SafeAuditDecisionStatus = "majority"
	SafeAuditStatusDispute  SafeAuditDecisionStatus = "dispute"
)

type SafeAuditDecision struct {
	AuditID          string                  `json:"audit_id"`
	Status           SafeAuditDecisionStatus `json:"status"`
	MajorityHash     string                  `json:"majority_hash,omitempty"`
	MajorityCount    int                     `json:"majority_count"`
	TotalVotes       int                     `json:"total_votes"`
	ExpectedVotes    int                     `json:"expected_votes"`
	MinorityReplicas []string                `json:"minority_replicas,omitempty"`
}

type SafeAuditReader interface {
	ReadSafeRows(ctx context.Context, task SafeAuditTask) ([]SafeRow, error)
}

type SafeAuditSigner interface {
	SignSafeAuditVote(ctx context.Context, voteHash string) (workerID, signature string, err error)
}

type SafeAuditSink interface {
	SubmitSafeAuditVote(ctx context.Context, vote SafeAuditVote) error
}

type SafeAuditSource interface {
	ClaimSafeAudit(ctx context.Context) (SafeAuditTask, bool, error)
}
