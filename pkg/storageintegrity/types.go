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

type FinalityRequest struct {
	BatchID     string
	StatementID string
	PayloadRef  string
	PayloadHash string
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

type FinalitySink interface {
	SubmitFinality(ctx context.Context, rec FinalityRecord) error
}

type FinalitySource interface {
	ClaimFinality(ctx context.Context) (FinalityRequest, bool, error)
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

type SafeAuditTask struct {
	AuditID    string
	ReplicaID  string
	NetworkID  string
	TableID    string
	SchemaHash string
	SnapshotID string
	Range      string
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
