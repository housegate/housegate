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
// table. Every envelope-v2 statement is compared against it BEFORE execution:
// a mismatch is challenge evidence (base design C.4) and yields a signed
// non-matching receipt; a table the source cannot resolve is a local refusal
// to attest.
type SchemaHashSource interface {
	TableSchemaHash(tableID string) (string, bool)
}

// JobSchemaHashSource is a SchemaHashSource whose table set follows the
// replayed chain: ForJob returns the source to use for one job, including the
// schemas the job itself carries (ReplayJob.TableSchemas). An error is a local
// refusal to attest.
type JobSchemaHashSource interface {
	SchemaHashSource
	ForJob(job ReplayJob) (SchemaHashSource, error)
}

// GenesisSnapshotSource derives the empty pre-genesis safe snapshot from an
// executor's own pinned table set. A network that has promoted nothing yet has
// no manifest to chain from, so its first replayed block declares no previous
// safe snapshot and is replayed against this locally derived base instead of a
// stored one. Without it block 1 is unattestable and no network can ever reach
// its first safe state.
//
// The base is derived, never transported: each verifier computes it from the
// same configured tables and network id it already uses for TableSchemaHash, so
// a sequencer cannot choose it. Declaring genesis on a network that has already
// promoted blocks therefore cannot forge a match — the replay would omit every
// previously promoted row and yield a non-matching (but still signed) receipt,
// which is challenge evidence like any other mismatch.
type GenesisSnapshotSource interface {
	GenesisSnapshot(safeBlockSeq uint64, schemaSnapshotID, executorProfileID string) (SafeSnapshotManifest, error)
}

// Verifier validates replay inputs, delegates execution to a pinned executor,
// and signs the resulting receipt.
type Verifier struct {
	Snapshots    SnapshotStore
	Payloads     PayloadStore
	Executor     Executor
	Signer       Signer
	SchemaHashes SchemaHashSource
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
	if v.SchemaHashes == nil {
		return ReplayAttestation{}, fmt.Errorf("schema hash source is required")
	}
	if err := validateJobShape(job); err != nil {
		return ReplayAttestation{}, err
	}

	snap, err := v.prevSafeSnapshot(ctx, job)
	if err != nil {
		return ReplayAttestation{}, err
	}
	if job.BlockSeq <= snap.SafeBlockSeq {
		return ReplayAttestation{}, fmt.Errorf("block_seq %d must be greater than safe snapshot block %d", job.BlockSeq, snap.SafeBlockSeq)
	}

	prepared, err := v.prepareStatements(ctx, job.Statements)
	if err != nil {
		return ReplayAttestation{}, err
	}
	hashes := v.SchemaHashes
	if scoped, ok := hashes.(JobSchemaHashSource); ok {
		if hashes, err = scoped.ForJob(job); err != nil {
			return ReplayAttestation{}, fmt.Errorf("resolve job %d schemas: %w", job.BlockSeq, err)
		}
	}
	for i, st := range job.Statements {
		local, ok := hashes.TableSchemaHash(st.TargetTableID)
		if !ok {
			return ReplayAttestation{}, fmt.Errorf("statement %d: no local schema for table %q", i, st.TargetTableID)
		}
		if local != st.SchemaHash {
			return v.signSchemaMismatch(ctx, job, prepared, i, st, local)
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

// prevSafeSnapshot resolves the base state a job replays against: the stored
// manifest it names, or the locally derived genesis base when it names none.
// Either way the returned snapshot must carry the job's own schema and executor
// identity, so the receipt commits to one consistent pinning.
func (v *Verifier) prevSafeSnapshot(ctx context.Context, job ReplayJob) (SafeSnapshotManifest, error) {
	snap, err := v.resolvePrevSafeSnapshot(ctx, job)
	if err != nil {
		return SafeSnapshotManifest{}, err
	}
	if snap.SchemaSnapshotID != job.SchemaSnapshotID {
		return SafeSnapshotManifest{}, fmt.Errorf("schema_snapshot_id mismatch: job %s snapshot %s", job.SchemaSnapshotID, snap.SchemaSnapshotID)
	}
	if snap.ExecutorProfileID != job.ExecutorProfileID {
		return SafeSnapshotManifest{}, fmt.Errorf("executor_profile_id mismatch: job %s snapshot %s", job.ExecutorProfileID, snap.ExecutorProfileID)
	}
	return snap, nil
}

func (v *Verifier) resolvePrevSafeSnapshot(ctx context.Context, job ReplayJob) (SafeSnapshotManifest, error) {
	if job.PrevSafeSnapshotID == "" {
		return v.genesisSnapshot(job)
	}
	snap, err := v.Snapshots.GetSafeSnapshot(ctx, job.PrevSafeSnapshotID)
	if err != nil {
		return SafeSnapshotManifest{}, fmt.Errorf("load safe snapshot %q: %w", job.PrevSafeSnapshotID, err)
	}
	if err := snap.Validate(); err != nil {
		return SafeSnapshotManifest{}, fmt.Errorf("invalid safe snapshot %q: %w", job.PrevSafeSnapshotID, err)
	}
	if snap.SnapshotID != job.PrevSafeSnapshotID {
		return SafeSnapshotManifest{}, fmt.Errorf("snapshot id mismatch: store returned %q for %q", snap.SnapshotID, job.PrevSafeSnapshotID)
	}
	if snap.StateRoot != job.PrevStateRoot {
		return SafeSnapshotManifest{}, fmt.Errorf("prev_state_root mismatch: job %s snapshot %s", job.PrevStateRoot, snap.StateRoot)
	}
	return snap, nil
}

// genesisSnapshot derives the base for a job that declares no previous safe
// snapshot. The job supplies only the schema/executor identity it already
// commits to in its receipt; the table set and every root come from this
// verifier's own executor, so the base cannot be chosen by the sequencer. The
// derived base must be genuinely empty — a "genesis" carrying data would let
// state enter a network without ever having been replayed.
func (v *Verifier) genesisSnapshot(job ReplayJob) (SafeSnapshotManifest, error) {
	src, ok := v.Executor.(GenesisSnapshotSource)
	if !ok {
		return SafeSnapshotManifest{}, fmt.Errorf("block %d declares no previous safe snapshot, but executor %T cannot derive the genesis base", job.BlockSeq, v.Executor)
	}
	snap, err := src.GenesisSnapshot(0, job.SchemaSnapshotID, job.ExecutorProfileID)
	if err != nil {
		return SafeSnapshotManifest{}, fmt.Errorf("derive genesis snapshot: %w", err)
	}
	if err := snap.Validate(); err != nil {
		return SafeSnapshotManifest{}, fmt.Errorf("invalid genesis snapshot: %w", err)
	}
	if snap.SafeBlockSeq != 0 {
		return SafeSnapshotManifest{}, fmt.Errorf("genesis snapshot must have safe_block_seq 0, got %d", snap.SafeBlockSeq)
	}
	for _, t := range snap.Tables {
		if len(t.PartitionRoots) != 0 || len(t.ActiveParts) != 0 {
			return SafeSnapshotManifest{}, fmt.Errorf("genesis snapshot table %q is not empty: %d partition roots, %d active parts", t.TableID, len(t.PartitionRoots), len(t.ActiveParts))
		}
	}
	return snap, nil
}

func validateJobShape(job ReplayJob) error {
	if job.BlockSeq == 0 {
		return fmt.Errorf("block_seq is required")
	}
	// Both prev fields empty is the genesis case: nothing has been promoted on
	// this network yet, so there is no manifest to chain from and the base is
	// derived locally (see GenesisSnapshotSource). They describe one fact and
	// must move together — naming a snapshot without its state root, or the
	// reverse, is a malformed job rather than genesis.
	if (job.PrevSafeSnapshotID == "") != (job.PrevStateRoot == "") {
		return fmt.Errorf("prev_safe_snapshot_id and prev_state_root must be set together or both be empty (genesis): prev_safe_snapshot_id=%q prev_state_root=%q", job.PrevSafeSnapshotID, job.PrevStateRoot)
	}
	if job.SchemaSnapshotID == "" {
		return fmt.Errorf("schema_snapshot_id is required")
	}
	if job.ExecutorProfileID == "" {
		return fmt.Errorf("executor_profile_id is required")
	}
	// A table-set transition block has no statements and so no source claim:
	// its expected root is derived by the arbiter from the pinned base and the
	// transition alone.
	if job.TableSetTransition != nil {
		return validateTransitionJobShape(job)
	}
	if job.SourceClaimRoot == "" {
		return fmt.Errorf("source_claim_root is required")
	}
	if len(job.Statements) == 0 {
		return fmt.Errorf("at least one statement is required")
	}
	if err := ValidateReplayTableSchemas("table_schemas", job.TableSchemas); err != nil {
		return err
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
		if st.PayloadFormat == "" {
			return fmt.Errorf("statement %d: payload_format is required", i)
		}
		if st.PayloadFormat != PayloadFormatClickHouseNativeData {
			return fmt.Errorf("statement %d: payload_format must be %s", i, PayloadFormatClickHouseNativeData)
		}
		if st.ClientRevision == 0 {
			return fmt.Errorf("statement %d: client_revision is required", i)
		}
		if st.SchemaHash == "" {
			return fmt.Errorf("statement %d: schema_hash is required", i)
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

// validateTransitionJobShape accepts a transition job: no statements, no
// carried table schemas (the adds carry their own), and a canonical transition.
func validateTransitionJobShape(job ReplayJob) error {
	if len(job.Statements) != 0 {
		return fmt.Errorf("table_set_transition job must carry no statements, got %d", len(job.Statements))
	}
	if len(job.TableSchemas) != 0 {
		return fmt.Errorf("table_set_transition job must carry no table_schemas")
	}
	return job.TableSetTransition.Validate()
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
		StatementID    string `json:"statement_id"`
		StatementSeq   uint64 `json:"statement_seq"`
		SQLHash        string `json:"sql_hash"`
		SettingsHash   string `json:"settings_hash"`
		PayloadRef     string `json:"payload_ref,omitempty"`
		PayloadHash    string `json:"payload_hash,omitempty"`
		PayloadLength  uint64 `json:"payload_length,omitempty"`
		TargetTableID  string `json:"target_table_id"`
		PayloadFormat  string `json:"payload_format,omitempty"`
		ClientRevision uint32 `json:"client_revision,omitempty"`
		SchemaHash     string `json:"schema_hash,omitempty"`
	}
	out := make([]statementCommitment, 0, len(statements))
	for _, st := range statements {
		out = append(out, statementCommitment{
			StatementID:    st.StatementID,
			StatementSeq:   st.StatementSeq,
			SQLHash:        st.SQLHash,
			SettingsHash:   st.SettingsHash,
			PayloadRef:     st.PayloadRef,
			PayloadHash:    st.PayloadHash,
			PayloadLength:  st.PayloadLength,
			TargetTableID:  st.TargetTableID,
			PayloadFormat:  st.PayloadFormat,
			ClientRevision: st.ClientRevision,
			SchemaHash:     st.SchemaHash,
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
