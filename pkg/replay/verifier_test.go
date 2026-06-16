package replay

import (
	"context"
	"errors"
	"testing"
)

func TestVerifierSignsMatchingReplay(t *testing.T) {
	ctx := context.Background()
	snap := testSnapshot(t)
	payload := []byte("name,balance\nalice,10\n")
	root := DigestString("root-after")
	job := testJob(snap, payload, root)
	exec := &fakeExecutor{result: resultForJob(job, root)}
	signer := &fakeSigner{replicaID: "replica-a", signature: "sig-a"}

	got, err := (&Verifier{
		Snapshots: fakeSnapshotStore{snap.SnapshotID: snap},
		Payloads:  fakePayloadStore{"payload-1": payload},
		Executor:  exec,
		Signer:    signer,
	}).Verify(ctx, job)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if !got.MatchSourceRoot {
		t.Fatal("expected source root to match")
	}
	if got.ReplicaID != "replica-a" || got.Signature != "sig-a" {
		t.Fatalf("unexpected signature fields: %#v", got)
	}
	if got.ReceiptHash == "" || signer.seenHash != got.ReceiptHash {
		t.Fatalf("receipt hash was not signed: attestation=%q signer=%q", got.ReceiptHash, signer.seenHash)
	}
	if len(exec.seen.Statements) != 1 {
		t.Fatalf("executor saw %d statements, want 1", len(exec.seen.Statements))
	}
	if string(exec.seen.Statements[0].Payload) != string(payload) {
		t.Fatalf("executor saw payload %q, want %q", exec.seen.Statements[0].Payload, payload)
	}
}

func TestVerifierSignsMismatchingReplayForChallenge(t *testing.T) {
	ctx := context.Background()
	snap := testSnapshot(t)
	payload := []byte("name,balance\nalice,10\n")
	job := testJob(snap, payload, DigestString("source-claim"))
	computed := DigestString("verifier-computed")
	signer := &fakeSigner{replicaID: "replica-a", signature: "sig-a"}

	got, err := (&Verifier{
		Snapshots: fakeSnapshotStore{snap.SnapshotID: snap},
		Payloads:  fakePayloadStore{"payload-1": payload},
		Executor:  &fakeExecutor{result: resultForJob(job, computed)},
		Signer:    signer,
	}).Verify(ctx, job)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if got.MatchSourceRoot {
		t.Fatal("expected source root mismatch")
	}
	if got.Receipt.ComputedStateRoot != computed {
		t.Fatalf("computed root = %s, want %s", got.Receipt.ComputedStateRoot, computed)
	}
	if got.Signature == "" {
		t.Fatal("mismatch receipts must still be signed")
	}
}

func TestVerifierRejectsPayloadHashMismatchBeforeExecution(t *testing.T) {
	ctx := context.Background()
	snap := testSnapshot(t)
	payload := []byte("name,balance\nalice,10\n")
	job := testJob(snap, payload, DigestString("source-claim"))
	job.Statements[0].PayloadHash = DigestString("tampered")
	exec := &fakeExecutor{result: resultForJob(job, DigestString("unused"))}

	_, err := (&Verifier{
		Snapshots: fakeSnapshotStore{snap.SnapshotID: snap},
		Payloads:  fakePayloadStore{"payload-1": payload},
		Executor:  exec,
		Signer:    &fakeSigner{replicaID: "replica-a", signature: "sig-a"},
	}).Verify(ctx, job)
	if err == nil {
		t.Fatal("expected payload hash mismatch")
	}
	if exec.called {
		t.Fatal("executor must not run after payload validation fails")
	}
}

func TestVerifierRejectsNonIncreasingStatementSeq(t *testing.T) {
	ctx := context.Background()
	snap := testSnapshot(t)
	payload := []byte("name,balance\nalice,10\n")
	job := testJob(snap, payload, DigestString("source-claim"))
	job.Statements = append(job.Statements, job.Statements[0])
	job.Statements[1].StatementID = "stmt-2"

	_, err := (&Verifier{
		Snapshots: fakeSnapshotStore{snap.SnapshotID: snap},
		Payloads:  fakePayloadStore{"payload-1": payload},
		Executor:  &fakeExecutor{result: resultForJob(job, DigestString("unused"))},
		Signer:    &fakeSigner{replicaID: "replica-a", signature: "sig-a"},
	}).Verify(ctx, job)
	if err == nil {
		t.Fatal("expected non-increasing statement sequence error")
	}
}

// TestVerifierRejectsExecutorTamperingWithResultIdentity exercises the
// validateExecutionResult guards (Appendix C.1): an executor must not change the
// prev-safe/state/schema/profile identity or return an empty computed root. Each
// is a pre-receipt local refusal to attest — an error with no signed attestation
// — distinct from a signed source-root mismatch.
func TestVerifierRejectsExecutorTamperingWithResultIdentity(t *testing.T) {
	ctx := context.Background()
	snap := testSnapshot(t)
	payload := []byte("name,balance\nalice,10\n")
	job := testJob(snap, payload, DigestString("source-claim"))
	base := resultForJob(job, DigestString("computed-root"))

	cases := map[string]func(r *ExecutionResult){
		"wrong prev_state_root":       func(r *ExecutionResult) { r.PrevStateRoot = DigestString("wrong") },
		"wrong prev_safe_snapshot_id": func(r *ExecutionResult) { r.PrevSafeSnapshotID = "wrong" },
		"wrong block_seq":             func(r *ExecutionResult) { r.BlockSeq = job.BlockSeq + 1 },
		"wrong schema_snapshot_id":    func(r *ExecutionResult) { r.SchemaSnapshotID = "wrong" },
		"wrong executor_profile_id":   func(r *ExecutionResult) { r.ExecutorProfileID = "wrong" },
		"empty computed_state_root":   func(r *ExecutionResult) { r.ComputedStateRoot = "" },
	}

	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			r := base
			mutate(&r)
			signer := &fakeSigner{replicaID: "replica-a", signature: "sig-a"}
			got, err := (&Verifier{
				Snapshots: fakeSnapshotStore{snap.SnapshotID: snap},
				Payloads:  fakePayloadStore{"payload-1": payload},
				Executor:  &fakeExecutor{result: r},
				Signer:    signer,
			}).Verify(ctx, job)
			if err == nil {
				t.Fatal("expected the verifier to reject the tampered executor result")
			}
			if signer.seenHash != "" {
				t.Fatal("signer must not be invoked when the executor result is rejected")
			}
			if got.Signature != "" {
				t.Fatalf("rejected result must not yield a signed attestation: %#v", got)
			}
		})
	}
}

func TestSnapshotManifestSealAndValidateAreOrderIndependent(t *testing.T) {
	a := SafeSnapshotManifest{
		ParentSnapshotID:  "parent",
		SafeBlockSeq:      7,
		SchemaSnapshotID:  "schema-1",
		SchemaRoot:        DigestString("schema"),
		ExecutorProfileID: "executor-1",
		Tables: []TableManifest{
			{
				TableID:    "table-b",
				SchemaHash: DigestString("schema-b"),
				ActiveParts: []PartManifestEntry{
					{TableID: "table-b", PartitionID: "p2", PartName: "part-2", PartPhysHash: DigestString("phys-2"), PartRowLtHash: DigestString("row-2")},
					{TableID: "table-b", PartitionID: "p1", PartName: "part-1", PartPhysHash: DigestString("phys-1"), PartRowLtHash: DigestString("row-1")},
				},
			},
			{
				TableID:    "table-a",
				SchemaHash: DigestString("schema-a"),
			},
		},
	}
	b := a
	b.Tables = []TableManifest{a.Tables[1], a.Tables[0]}

	sealedA, err := a.Seal()
	if err != nil {
		t.Fatalf("Seal A: %v", err)
	}
	sealedB, err := b.Seal()
	if err != nil {
		t.Fatalf("Seal B: %v", err)
	}
	if sealedA.StateRoot != sealedB.StateRoot {
		t.Fatalf("state roots differ: %s vs %s", sealedA.StateRoot, sealedB.StateRoot)
	}
	if sealedA.ManifestRoot != sealedB.ManifestRoot {
		t.Fatalf("manifest roots differ: %s vs %s", sealedA.ManifestRoot, sealedB.ManifestRoot)
	}
	if err := sealedA.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func testSnapshot(t *testing.T) SafeSnapshotManifest {
	t.Helper()
	snap, err := (SafeSnapshotManifest{
		ParentSnapshotID:  "snapshot-0",
		SafeBlockSeq:      10,
		SchemaSnapshotID:  "schema-1",
		SchemaRoot:        DigestString("schema-root"),
		ExecutorProfileID: "executor-1",
		Tables: []TableManifest{
			{
				TableID:    "table-1",
				SchemaHash: DigestString("table-schema"),
			},
		},
	}).Seal()
	if err != nil {
		t.Fatalf("Seal snapshot: %v", err)
	}
	return snap
}

func testJob(snap SafeSnapshotManifest, payload []byte, sourceRoot string) ReplayJob {
	sql := "INSERT INTO balances FORMAT CSV"
	return ReplayJob{
		BlockSeq:           snap.SafeBlockSeq + 1,
		PrevSafeSnapshotID: snap.SnapshotID,
		PrevStateRoot:      snap.StateRoot,
		SchemaSnapshotID:   snap.SchemaSnapshotID,
		ExecutorProfileID:  snap.ExecutorProfileID,
		SourceClaimRoot:    sourceRoot,
		Statements: []Statement{
			{
				StatementID:   "stmt-1",
				StatementSeq:  snap.SafeBlockSeq + 1,
				SQL:           sql,
				SQLHash:       DigestString(sql),
				SettingsHash:  DigestString("settings"),
				PayloadRef:    "payload-1",
				PayloadHash:   DigestBytes(payload),
				PayloadLength: uint64(len(payload)),
				TargetTableID: "table-1",
				UserJWS:       "jws",
			},
		},
	}
}

func resultForJob(job ReplayJob, root string) ExecutionResult {
	return ExecutionResult{
		BlockSeq:           job.BlockSeq,
		PrevSafeSnapshotID: job.PrevSafeSnapshotID,
		PrevStateRoot:      job.PrevStateRoot,
		SchemaSnapshotID:   job.SchemaSnapshotID,
		ExecutorProfileID:  job.ExecutorProfileID,
		ComputedStateRoot:  root,
		ReplayLogHash:      DigestString("replay-log"),
	}
}

type fakeSnapshotStore map[string]SafeSnapshotManifest

func (s fakeSnapshotStore) GetSafeSnapshot(_ context.Context, snapshotID string) (SafeSnapshotManifest, error) {
	snap, ok := s[snapshotID]
	if !ok {
		return SafeSnapshotManifest{}, errors.New("not found")
	}
	return snap, nil
}

type fakePayloadStore map[string][]byte

func (s fakePayloadStore) GetPayload(_ context.Context, payloadRef string) ([]byte, error) {
	payload, ok := s[payloadRef]
	if !ok {
		return nil, errors.New("not found")
	}
	return append([]byte(nil), payload...), nil
}

type fakeExecutor struct {
	result ExecutionResult
	called bool
	seen   ExecutionRequest
}

func (e *fakeExecutor) Replay(_ context.Context, req ExecutionRequest) (ExecutionResult, error) {
	e.called = true
	e.seen = req
	return e.result, nil
}

type fakeSigner struct {
	replicaID string
	signature string
	seenHash  string
}

func (s *fakeSigner) SignReplayReceipt(_ context.Context, receiptHash string) (string, string, error) {
	s.seenHash = receiptHash
	return s.replicaID, s.signature, nil
}
