package replay

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

// A job without the two table-set fields must encode exactly as it did
// before they existed: the arbiter anchors nothing over a job, but arbiter-core
// round-trips it through the wire mirror and verifiers log it.
func TestReplayJobWithoutTableSetFieldsEncodesUnchanged(t *testing.T) {
	job := ReplayJob{
		BlockSeq: 3, PrevSafeSnapshotID: "snap-2", PrevStateRoot: "0x02", SchemaSnapshotID: "schema-1",
		ExecutorProfileID: "executor-1", SourceClaimRoot: "0x03",
		Statements: []Statement{{StatementID: "s", StatementSeq: 1, SQL: "q", SQLHash: "0x04", SettingsHash: "0x05", TargetTableID: "db.t"}},
	}
	got, err := json.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"block_seq":3,"prev_safe_snapshot_id":"snap-2","prev_state_root":"0x02","schema_snapshot_id":"schema-1","executor_profile_id":"executor-1","source_claim_root":"0x03","statements":[{"statement_id":"s","statement_seq":1,"sql":"q","sql_hash":"0x04","settings_hash":"0x05","target_table_id":"db.t"}]}`
	if string(got) != want {
		t.Fatalf("job encoding changed:\n got %s\nwant %s", got, want)
	}
}

func transitionJob(snap SafeSnapshotManifest) ReplayJob {
	return ReplayJob{
		BlockSeq:           snap.SafeBlockSeq + 1,
		PrevSafeSnapshotID: snap.SnapshotID,
		PrevStateRoot:      snap.StateRoot,
		SchemaSnapshotID:   snap.SchemaSnapshotID,
		ExecutorProfileID:  snap.ExecutorProfileID,
		TableSetTransition: &ReplayTableSetTransition{
			Adds:          []ReplayTableSchema{{TableID: "db.new", SchemaJSON: `{"table_id":"db.new","partition_by":"","columns":[{"name":"v","type":"UInt64"}]}`}},
			Retires:       []string{"table-1"},
			NewSchemaRoot: DigestString("new-schema-root"),
		},
	}
}

func TestValidateJobShapeTableSetTransition(t *testing.T) {
	snap := testSnapshot(t)
	if err := validateJobShape(transitionJob(snap)); err != nil {
		t.Fatalf("transition job without statements or source claim must be accepted: %v", err)
	}
	stmt := testJob(snap, []byte("x"), "0x01").Statements[0]
	for _, tc := range []struct {
		name string
		want string
		edit func(*ReplayJob)
	}{
		{"statements", "must carry no statements", func(j *ReplayJob) { j.Statements = []Statement{stmt} }},
		{"table schemas", "must carry no table_schemas", func(j *ReplayJob) {
			j.TableSchemas = []ReplayTableSchema{{TableID: "db.x", SchemaJSON: "{}"}}
		}},
		{"new schema root", "new_schema_root is required", func(j *ReplayJob) { j.TableSetTransition.NewSchemaRoot = "" }},
		{"empty change", "add or retire at least one table", func(j *ReplayJob) {
			j.TableSetTransition.Adds, j.TableSetTransition.Retires = nil, nil
		}},
		{"unsorted adds", "sorted, unique", func(j *ReplayJob) {
			j.TableSetTransition.Adds = []ReplayTableSchema{{TableID: "db.b", SchemaJSON: "{}"}, {TableID: "db.a", SchemaJSON: "{}"}}
		}},
		{"add without json", "schema_json is required", func(j *ReplayJob) { j.TableSetTransition.Adds[0].SchemaJSON = "" }},
		{"duplicate retires", "sorted, unique", func(j *ReplayJob) { j.TableSetTransition.Retires = []string{"table-1", "table-1"} }},
		{"added and retired", "both added and retired", func(j *ReplayJob) { j.TableSetTransition.Retires = []string{"db.new"} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			job := transitionJob(snap)
			tc.edit(&job)
			if err := validateJobShape(job); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("validateJobShape = %v, want %q", err, tc.want)
			}
		})
	}
	// Without a transition the old rules stand, and carried schemas must be canonical.
	empty := transitionJob(snap)
	empty.TableSetTransition = nil
	if err := validateJobShape(empty); err == nil || !strings.Contains(err.Error(), "source_claim_root is required") {
		t.Fatalf("statement-less job without a transition = %v", err)
	}
	unsorted := testJob(snap, []byte("x"), "0x01")
	unsorted.TableSchemas = []ReplayTableSchema{{TableID: "db.b", SchemaJSON: "{}"}, {TableID: "db.a", SchemaJSON: "{}"}}
	if err := validateJobShape(unsorted); err == nil || !strings.Contains(err.Error(), "table_schemas[1]") {
		t.Fatalf("unsorted table_schemas = %v", err)
	}
}

func TestVerifierSignsTransitionJob(t *testing.T) {
	snap := testSnapshot(t)
	job := transitionJob(snap)
	computed := DigestString("transition-root")
	exec := &fakeExecutor{result: resultForJob(job, computed)}
	signer := &fakeSigner{replicaID: "replica-a", signature: "sig-a"}
	got, err := (&Verifier{
		Snapshots:    fakeSnapshotStore{snap.SnapshotID: snap},
		Payloads:     fakePayloadStore{},
		Executor:     exec,
		Signer:       signer,
		SchemaHashes: fakeSchemaHashes{},
	}).Verify(context.Background(), job)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if !exec.called || exec.seen.Job.TableSetTransition == nil || len(exec.seen.Statements) != 0 {
		t.Fatalf("executor must see the transition and no statements: %+v", exec.seen)
	}
	emptyStatements, _ := statementRoot(nil)
	if got.Receipt.ComputedStateRoot != computed || got.Receipt.StatementRoot != emptyStatements || got.Receipt.SourceClaimRoot != "" || got.MatchSourceRoot {
		t.Fatalf("transition receipt = %+v", got.Receipt)
	}
	if signer.seenHash != got.ReceiptHash {
		t.Fatal("transition receipt must be signed")
	}
}

type jobScopedHashes struct {
	fakeSchemaHashes
	job fakeSchemaHashes
	err error
}

func (s jobScopedHashes) ForJob(job ReplayJob) (SchemaHashSource, error) {
	if s.err != nil {
		return nil, s.err
	}
	merged := fakeSchemaHashes{}
	for k, v := range s.fakeSchemaHashes {
		merged[k] = v
	}
	for _, ts := range job.TableSchemas {
		merged[ts.TableID] = s.job[ts.TableID]
	}
	return merged, nil
}

func TestVerifierResolvesJobCarriedSchemaHashes(t *testing.T) {
	snap := testSnapshot(t)
	payload := []byte("x")
	job := testJob(snap, payload, DigestString("root"))
	job.Statements[0].TargetTableID = "db.new"
	job.TableSchemas = []ReplayTableSchema{{TableID: "db.new", SchemaJSON: `{"table_id":"db.new"}`}}
	build := func(src SchemaHashSource) (*fakeExecutor, *Verifier) {
		exec := &fakeExecutor{result: resultForJob(job, DigestString("root"))}
		return exec, &Verifier{Snapshots: fakeSnapshotStore{snap.SnapshotID: snap}, Payloads: fakePayloadStore{"payload-1": payload},
			Executor: exec, Signer: &fakeSigner{replicaID: "r", signature: "s"}, SchemaHashes: src}
	}
	exec, v := build(jobScopedHashes{fakeSchemaHashes: fakeSchemaHashes{}, job: fakeSchemaHashes{"db.new": job.Statements[0].SchemaHash}})
	if _, err := v.Verify(context.Background(), job); err != nil || !exec.called {
		t.Fatalf("a job-carried schema must satisfy the statement's schema check: err=%v called=%v", err, exec.called)
	}
	exec, v = build(fakeSchemaHashes{})
	if _, err := v.Verify(context.Background(), job); err == nil || !strings.Contains(err.Error(), `no local schema for table "db.new"`) || exec.called {
		t.Fatalf("a static source must still refuse an unknown table: %v", err)
	}
	exec, v = build(jobScopedHashes{err: errors.New("bad schema json")})
	if _, err := v.Verify(context.Background(), job); err == nil || !strings.Contains(err.Error(), "bad schema json") || exec.called {
		t.Fatalf("a job-schema resolution failure must refuse before execution: %v", err)
	}
}
