package replay

import (
	"context"
	"fmt"
)

// SnapshotStore returns previously safe snapshot manifests.
type SnapshotStore interface {
	GetSafeSnapshot(ctx context.Context, snapshotID string) (SafeSnapshotManifest, error)
}

// PayloadStore returns the original signed payload bytes referenced by L3.
type PayloadStore interface {
	GetPayload(ctx context.Context, payloadRef string) ([]byte, error)
}

// Executor replays a validated job against a pinned scratch state.
type Executor interface {
	Replay(ctx context.Context, req ExecutionRequest) (ExecutionResult, error)
}

// Signer signs replay receipts. Implementations can use the relay/indexer key
// or a verifier-specific key.
type Signer interface {
	SignReplayReceipt(ctx context.Context, receiptHash string) (replicaID, signature string, err error)
}

// SchemaHashSource resolves the verifier's own Phase-B schema hash for a
// table. When set on Verifier, every statement that carries a non-empty
// SchemaHash is compared against it BEFORE execution: a mismatch is
// challenge evidence (base design C.4) and yields a signed non-matching
// receipt; a table the source cannot resolve is a local refusal to attest.
type SchemaHashSource interface {
	TableSchemaHash(tableID string) (string, bool)
}

// Verifier validates replay inputs, delegates execution to a pinned executor,
// and signs the resulting receipt.
type Verifier struct {
	Snapshots    SnapshotStore
	Payloads     PayloadStore
	Executor     Executor
	Signer       Signer
	SchemaHashes SchemaHashSource // optional
}

func (v *Verifier) Verify(ctx context.Context, job ReplayJob) (ReplayAttestation, error) {
	if v == nil {
		return ReplayAttestation{}, fmt.Errorf("replay verifier is nil")
	}
	if v.Snapshots == nil {
		return ReplayAttestation{}, fmt.Errorf("snapshot store is required")
	}
	if v.Payloads == nil {
		return ReplayAttestation{}, fmt.Errorf("payload store is required")
	}
	if v.Executor == nil {
		return ReplayAttestation{}, fmt.Errorf("executor is required")
	}
	if v.Signer == nil {
		return ReplayAttestation{}, fmt.Errorf("signer is required")
	}
	if err := validateJobShape(job); err != nil {
		return ReplayAttestation{}, err
	}

	snap, err := v.Snapshots.GetSafeSnapshot(ctx, job.PrevSafeSnapshotID)
	if err != nil {
		return ReplayAttestation{}, fmt.Errorf("load safe snapshot %q: %w", job.PrevSafeSnapshotID, err)
	}
	if err := snap.Validate(); err != nil {
		return ReplayAttestation{}, fmt.Errorf("invalid safe snapshot %q: %w", job.PrevSafeSnapshotID, err)
	}
	if snap.SnapshotID != job.PrevSafeSnapshotID {
		return ReplayAttestation{}, fmt.Errorf("snapshot id mismatch: store returned %q for %q", snap.SnapshotID, job.PrevSafeSnapshotID)
	}
	if snap.StateRoot != job.PrevStateRoot {
		return ReplayAttestation{}, fmt.Errorf("prev_state_root mismatch: job %s snapshot %s", job.PrevStateRoot, snap.StateRoot)
	}
	if snap.SchemaSnapshotID != job.SchemaSnapshotID {
		return ReplayAttestation{}, fmt.Errorf("schema_snapshot_id mismatch: job %s snapshot %s", job.SchemaSnapshotID, snap.SchemaSnapshotID)
	}
	if snap.ExecutorProfileID != job.ExecutorProfileID {
		return ReplayAttestation{}, fmt.Errorf("executor_profile_id mismatch: job %s snapshot %s", job.ExecutorProfileID, snap.ExecutorProfileID)
	}
	if job.BlockSeq <= snap.SafeBlockSeq {
		return ReplayAttestation{}, fmt.Errorf("block_seq %d must be greater than safe snapshot block %d", job.BlockSeq, snap.SafeBlockSeq)
	}

	prepared, err := v.prepareStatements(ctx, job.Statements)
	if err != nil {
		return ReplayAttestation{}, err
	}
	if v.SchemaHashes != nil {
		for i, st := range job.Statements {
			if st.SchemaHash == "" {
				continue
			}
			local, ok := v.SchemaHashes.TableSchemaHash(st.TargetTableID)
			if !ok {
				return ReplayAttestation{}, fmt.Errorf("statement %d: no local schema for table %q", i, st.TargetTableID)
			}
			if local != st.SchemaHash {
				return v.signSchemaMismatch(ctx, job, prepared, i, st, local)
			}
		}
	}

	result, err := v.Executor.Replay(ctx, ExecutionRequest{
		Job:        job,
		Snapshot:   snap,
		Statements: prepared,
	})
	if err != nil {
		return ReplayAttestation{}, fmt.Errorf("execute replay job %d: %w", job.BlockSeq, err)
	}
	if err := validateExecutionResult(job, result); err != nil {
		return ReplayAttestation{}, err
	}

	statementRoot, err := statementRoot(job.Statements)
	if err != nil {
		return ReplayAttestation{}, err
	}
	payloadRoot, err := payloadRoot(prepared)
	if err != nil {
		return ReplayAttestation{}, err
	}
	receipt := ExecutionReceipt{
		BlockSeq:                  job.BlockSeq,
		PrevSafeSnapshotID:        job.PrevSafeSnapshotID,
		PrevStateRoot:             job.PrevStateRoot,
		SchemaSnapshotID:          job.SchemaSnapshotID,
		ExecutorProfileID:         job.ExecutorProfileID,
		StatementRoot:             statementRoot,
		PayloadRoot:               payloadRoot,
		SourceClaimRoot:           job.SourceClaimRoot,
		ComputedStateRoot:         result.ComputedStateRoot,
		MatchSourceRoot:           result.ComputedStateRoot == job.SourceClaimRoot,
		PartitionCommitmentsAfter: result.PartitionCommitmentsAfter,
		AffectedParts:             result.AffectedParts,
		ReplayLogHash:             result.ReplayLogHash,
	}
	receiptHash, err := receipt.Hash()
	if err != nil {
		return ReplayAttestation{}, err
	}
	replicaID, sig, err := v.Signer.SignReplayReceipt(ctx, receiptHash)
	if err != nil {
		return ReplayAttestation{}, fmt.Errorf("sign replay receipt: %w", err)
	}
	if replicaID == "" {
		return ReplayAttestation{}, fmt.Errorf("signer returned empty replica id")
	}
	if sig == "" {
		return ReplayAttestation{}, fmt.Errorf("signer returned empty signature")
	}

	return ReplayAttestation{
		ReplicaID:       replicaID,
		Receipt:         receipt,
		ReceiptHash:     receiptHash,
		Signature:       sig,
		MatchSourceRoot: receipt.MatchSourceRoot,
	}, nil
}

// signSchemaMismatch signs a non-matching receipt when a statement's signed
// schema_hash differs from this verifier's schema source. Nothing is
// executed; ComputedStateRoot is empty and ReplayLogHash commits to the
// mismatch so the receipt is non-repudiable challenge evidence.
func (v *Verifier) signSchemaMismatch(ctx context.Context, job ReplayJob, prepared []PreparedStatement, index int, st Statement, local string) (ReplayAttestation, error) {
	statementRoot, err := statementRoot(job.Statements)
	if err != nil {
		return ReplayAttestation{}, err
	}
	payloadRoot, err := payloadRoot(prepared)
	if err != nil {
		return ReplayAttestation{}, err
	}
	logHash, err := canonicalDigest("replay-schema-hash-mismatch", struct {
		StatementIndex int    `json:"statement_index"`
		StatementID    string `json:"statement_id"`
		TableID        string `json:"table_id"`
		SignedHash     string `json:"signed_schema_hash"`
		LocalHash      string `json:"local_schema_hash"`
	}{index, st.StatementID, st.TargetTableID, st.SchemaHash, local})
	if err != nil {
		return ReplayAttestation{}, err
	}
	receipt := ExecutionReceipt{
		BlockSeq:           job.BlockSeq,
		PrevSafeSnapshotID: job.PrevSafeSnapshotID,
		PrevStateRoot:      job.PrevStateRoot,
		SchemaSnapshotID:   job.SchemaSnapshotID,
		ExecutorProfileID:  job.ExecutorProfileID,
		StatementRoot:      statementRoot,
		PayloadRoot:        payloadRoot,
		SourceClaimRoot:    job.SourceClaimRoot,
		ComputedStateRoot:  "",
		MatchSourceRoot:    false,
		ReplayLogHash:      logHash,
	}
	receiptHash, err := receipt.Hash()
	if err != nil {
		return ReplayAttestation{}, err
	}
	replicaID, sig, err := v.Signer.SignReplayReceipt(ctx, receiptHash)
	if err != nil {
		return ReplayAttestation{}, fmt.Errorf("sign schema-mismatch receipt: %w", err)
	}
	if replicaID == "" || sig == "" {
		return ReplayAttestation{}, fmt.Errorf("signer returned empty replica id or signature")
	}
	return ReplayAttestation{ReplicaID: replicaID, Receipt: receipt, ReceiptHash: receiptHash, Signature: sig, MatchSourceRoot: false}, nil
}

func validateJobShape(job ReplayJob) error {
	if job.BlockSeq == 0 {
		return fmt.Errorf("block_seq is required")
	}
	if job.PrevSafeSnapshotID == "" {
		return fmt.Errorf("prev_safe_snapshot_id is required")
	}
	if job.PrevStateRoot == "" {
		return fmt.Errorf("prev_state_root is required")
	}
	if job.SchemaSnapshotID == "" {
		return fmt.Errorf("schema_snapshot_id is required")
	}
	if job.ExecutorProfileID == "" {
		return fmt.Errorf("executor_profile_id is required")
	}
	if job.SourceClaimRoot == "" {
		return fmt.Errorf("source_claim_root is required")
	}
	if len(job.Statements) == 0 {
		return fmt.Errorf("at least one statement is required")
	}
	var lastSeq uint64
	seenIDs := map[string]struct{}{}
	for i, st := range job.Statements {
		if st.StatementID == "" {
			return fmt.Errorf("statement %d: statement_id is required", i)
		}
		if _, ok := seenIDs[st.StatementID]; ok {
			return fmt.Errorf("statement %d: duplicate statement_id %q", i, st.StatementID)
		}
		seenIDs[st.StatementID] = struct{}{}
		if st.StatementSeq == 0 {
			return fmt.Errorf("statement %d: statement_seq is required", i)
		}
		if st.StatementSeq <= lastSeq {
			return fmt.Errorf("statement %d: statement_seq %d must be greater than previous %d", i, st.StatementSeq, lastSeq)
		}
		lastSeq = st.StatementSeq
		if st.SQL == "" {
			return fmt.Errorf("statement %d: sql is required", i)
		}
		if st.SQLHash == "" {
			return fmt.Errorf("statement %d: sql_hash is required", i)
		}
		if got := DigestString(st.SQL); st.SQLHash != got {
			return fmt.Errorf("statement %d: sql_hash mismatch: got %s want %s", i, st.SQLHash, got)
		}
		if st.SettingsHash == "" {
			return fmt.Errorf("statement %d: settings_hash is required", i)
		}
		if st.TargetTableID == "" {
			return fmt.Errorf("statement %d: target_table_id is required", i)
		}
		if st.PayloadRef == "" {
			if st.PayloadHash != "" || st.PayloadLength != 0 {
				return fmt.Errorf("statement %d: payload hash/length set without payload_ref", i)
			}
			continue
		}
		if st.PayloadHash == "" {
			return fmt.Errorf("statement %d: payload_hash is required when payload_ref is set", i)
		}
	}
	return nil
}

func (v *Verifier) prepareStatements(ctx context.Context, statements []Statement) ([]PreparedStatement, error) {
	out := make([]PreparedStatement, 0, len(statements))
	for i, st := range statements {
		prepared := PreparedStatement{Statement: st}
		if st.PayloadRef != "" {
			payload, err := v.Payloads.GetPayload(ctx, st.PayloadRef)
			if err != nil {
				return nil, fmt.Errorf("statement %d: load payload %q: %w", i, st.PayloadRef, err)
			}
			if uint64(len(payload)) != st.PayloadLength {
				return nil, fmt.Errorf("statement %d: payload_length mismatch: got %d want %d", i, len(payload), st.PayloadLength)
			}
			if got := DigestBytes(payload); got != st.PayloadHash {
				return nil, fmt.Errorf("statement %d: payload_hash mismatch: got %s want %s", i, got, st.PayloadHash)
			}
			prepared.Payload = append([]byte(nil), payload...)
		}
		out = append(out, prepared)
	}
	return out, nil
}

func validateExecutionResult(job ReplayJob, result ExecutionResult) error {
	if result.BlockSeq != job.BlockSeq {
		return fmt.Errorf("executor block_seq mismatch: got %d want %d", result.BlockSeq, job.BlockSeq)
	}
	if result.PrevSafeSnapshotID != job.PrevSafeSnapshotID {
		return fmt.Errorf("executor prev_safe_snapshot_id mismatch: got %s want %s", result.PrevSafeSnapshotID, job.PrevSafeSnapshotID)
	}
	if result.PrevStateRoot != job.PrevStateRoot {
		return fmt.Errorf("executor prev_state_root mismatch: got %s want %s", result.PrevStateRoot, job.PrevStateRoot)
	}
	if result.SchemaSnapshotID != job.SchemaSnapshotID {
		return fmt.Errorf("executor schema_snapshot_id mismatch: got %s want %s", result.SchemaSnapshotID, job.SchemaSnapshotID)
	}
	if result.ExecutorProfileID != job.ExecutorProfileID {
		return fmt.Errorf("executor executor_profile_id mismatch: got %s want %s", result.ExecutorProfileID, job.ExecutorProfileID)
	}
	if result.ComputedStateRoot == "" {
		return fmt.Errorf("executor returned empty computed_state_root")
	}
	return nil
}

func statementRoot(statements []Statement) (string, error) {
	type statementCommitment struct {
		StatementID   string `json:"statement_id"`
		StatementSeq  uint64 `json:"statement_seq"`
		SQLHash       string `json:"sql_hash"`
		SettingsHash  string `json:"settings_hash"`
		PayloadRef    string `json:"payload_ref,omitempty"`
		PayloadHash   string `json:"payload_hash,omitempty"`
		PayloadLength uint64 `json:"payload_length,omitempty"`
		TargetTableID string `json:"target_table_id"`
	}
	out := make([]statementCommitment, 0, len(statements))
	for _, st := range statements {
		out = append(out, statementCommitment{
			StatementID:   st.StatementID,
			StatementSeq:  st.StatementSeq,
			SQLHash:       st.SQLHash,
			SettingsHash:  st.SettingsHash,
			PayloadRef:    st.PayloadRef,
			PayloadHash:   st.PayloadHash,
			PayloadLength: st.PayloadLength,
			TargetTableID: st.TargetTableID,
		})
	}
	return canonicalDigest("replay-statement-root", out)
}

func payloadRoot(statements []PreparedStatement) (string, error) {
	type payloadCommitment struct {
		StatementID string `json:"statement_id"`
		PayloadRef  string `json:"payload_ref,omitempty"`
		PayloadHash string `json:"payload_hash,omitempty"`
	}
	out := make([]payloadCommitment, 0, len(statements))
	for _, st := range statements {
		out = append(out, payloadCommitment{
			StatementID: st.StatementID,
			PayloadRef:  st.PayloadRef,
			PayloadHash: st.PayloadHash,
		})
	}
	return canonicalDigest("replay-payload-root", out)
}
