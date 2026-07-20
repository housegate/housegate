package storageintegrity

import (
	"errors"
	"testing"

	"housegate/housegate/pkg/sqlmeta"
)

// requireCompanionMutationConsensus skips the plugin-side mutation submission
// test while the companion C2 mutation-consensus seam is absent (SubmitMutation
// / partition-barrier install / 2-of-3 post-state quorum). It is distinct from
// the INSERT C1 gate.
func requireCompanionMutationConsensus(t *testing.T) {
	t.Helper()
	if !CompanionMutationConsensusAvailable {
		t.Skip("mutation-consensus companion seam (SubmitMutation / partition-barrier / " +
			"2-of-3 post-state quorum) is not exposed by the Sentio arbiter/arbiter-proto " +
			"topology (INSERT-only); end-to-end submission stays gated until it lands (see " +
			"CompanionMutationConsensusAvailable)")
	}
}

func snapshotFixture() ManifestSnapshot {
	return ManifestSnapshot{
		TableID:    "net1.events",
		SchemaRoot: "schema-root-1",
		Partitions: []PartitionCost{
			{PartitionID: "p1", Parts: 2, Bytes: 100, RowCount: 10},
			{PartitionID: "p2", Parts: 1, Bytes: 50, RowCount: 5},
			{PartitionID: "p3", Parts: 3, Bytes: 200, RowCount: 20},
			{PartitionID: "p4", Parts: 1, Bytes: 40, RowCount: 4},
		},
	}
}

func acceptableUpdate() MutationRequest {
	return MutationRequest{
		Kind:               KindUpdate,
		StatementType:      sqlmeta.StatementTypeAlterTable,
		SQL:                "ALTER TABLE net1.events UPDATE v = 1 WHERE p = 'p1'",
		TableID:            "net1.events",
		AccessedTables:     []sqlmeta.AccessedTable{{OriginalDatabase: "net1", OriginalTable: "events"}},
		AffectedPartitions: []string{"p1"},
		AssignedColumns:    []string{"v"},
		KeyColumns:         []string{"p", "id"},
		Snapshot:           snapshotFixture(),
		WorkerSchemaRoot:   "schema-root-1",
	}
}

func TestClassifyMutation_AcceptsAlterUpdate(t *testing.T) {
	plan, err := ClassifyMutation(MutationAdmissionConfig{}, acceptableUpdate())
	if err != nil {
		t.Fatalf("acceptable UPDATE rejected: %v", err)
	}
	if plan.Kind != KindUpdate || plan.TableID != "net1.events" {
		t.Fatalf("plan kind/table wrong: %+v", plan)
	}
	if len(plan.BarrierKeys) != 1 || plan.BarrierKeys[0] != (BarrierKey{TableID: "net1.events", PartitionID: "p1"}) {
		t.Fatalf("barrier keys wrong: %+v", plan.BarrierKeys)
	}
	if plan.EstimatedTouchedParts != 2 || plan.EstimatedTouchedBytes != 100 {
		t.Fatalf("cost wrong: parts=%d bytes=%d", plan.EstimatedTouchedParts, plan.EstimatedTouchedBytes)
	}
}

func TestClassifyMutation_AcceptsAlterDelete(t *testing.T) {
	req := acceptableUpdate()
	req.Kind = KindDelete
	req.StatementType = sqlmeta.StatementTypeDelete
	req.SQL = "ALTER TABLE net1.events DELETE WHERE p = 'p2'"
	req.AffectedPartitions = []string{"p2"}
	req.AssignedColumns = nil
	plan, err := ClassifyMutation(MutationAdmissionConfig{}, req)
	if err != nil {
		t.Fatalf("acceptable DELETE rejected: %v", err)
	}
	if plan.Kind != KindDelete {
		t.Fatalf("plan kind: %v", plan.Kind)
	}
}

func TestClassifyMutation_AcceptsNormalizableUpdate(t *testing.T) {
	req := acceptableUpdate()
	req.StatementType = sqlmeta.StatementTypeUpdate
	req.SQL = "UPDATE net1.events SET v = 1 WHERE p = 'p1'"
	if _, err := ClassifyMutation(MutationAdmissionConfig{}, req); err != nil {
		t.Fatalf("normalizable UPDATE rejected: %v", err)
	}
}

func TestClassifyMutation_AcceptsMultiPartitionCanonicalBarrierOrder(t *testing.T) {
	req := acceptableUpdate()
	req.AffectedPartitions = []string{"p3", "p1", "p2"} // out of order
	plan, err := ClassifyMutation(MutationAdmissionConfig{}, req)
	if err != nil {
		t.Fatalf("multi-partition rejected: %v", err)
	}
	want := []string{"p1", "p2", "p3"}
	if len(plan.BarrierKeys) != 3 {
		t.Fatalf("expected 3 barrier keys, got %d", len(plan.BarrierKeys))
	}
	for i, w := range want {
		if plan.BarrierKeys[i].PartitionID != w {
			t.Fatalf("barrier key %d = %q, want canonical %q", i, plan.BarrierKeys[i].PartitionID, w)
		}
	}
	// cost = p1(2,100) + p2(1,50) + p3(3,200)
	if plan.EstimatedTouchedParts != 6 || plan.EstimatedTouchedBytes != 350 {
		t.Fatalf("multi-partition cost wrong: parts=%d bytes=%d", plan.EstimatedTouchedParts, plan.EstimatedTouchedBytes)
	}
}

func TestEstimateMutationCost_SumsAffectedPartitionsOnly(t *testing.T) {
	parts, bytes, missing := EstimateMutationCost(snapshotFixture(), []string{"p1", "p3"})
	if len(missing) != 0 {
		t.Fatalf("unexpected missing: %v", missing)
	}
	if parts != 5 || bytes != 300 { // p1(2,100)+p3(3,200)
		t.Fatalf("cost = parts %d bytes %d, want 5/300 (only affected partitions)", parts, bytes)
	}
}

func TestEstimateMutationCost_ReportsMissingPartitions(t *testing.T) {
	parts, bytes, missing := EstimateMutationCost(snapshotFixture(), []string{"p1", "pX"})
	if len(missing) != 1 || missing[0] != "pX" {
		t.Fatalf("expected pX missing, got %v", missing)
	}
	if parts != 2 || bytes != 100 { // only p1 counted; pX contributes 0
		t.Fatalf("missing partition must contribute 0: parts=%d bytes=%d", parts, bytes)
	}
}

// TestClassifyMutation_RejectMatrix drives one reject per design section 4.2
// bullet. Each perturbs the acceptable baseline to trigger exactly one reason.
func TestClassifyMutation_RejectMatrix(t *testing.T) {
	cases := []struct {
		name   string
		cfg    MutationAdmissionConfig
		mutate func(*MutationRequest)
		want   MutationRejectReason
	}{
		{"insert kind", MutationAdmissionConfig{}, func(r *MutationRequest) { r.Kind = KindInsert }, RejectUnsupportedKind},
		{"non-mutation statement type", MutationAdmissionConfig{}, func(r *MutationRequest) { r.StatementType = sqlmeta.StatementTypeSelect }, RejectUnsupportedKind},
		{"unbounded predicate", MutationAdmissionConfig{}, func(r *MutationRequest) { r.AffectedPartitions = nil }, RejectUnboundedPredicate},
		{"affected partitions over limit", MutationAdmissionConfig{MaxAffectedPartitions: 1}, func(r *MutationRequest) { r.AffectedPartitions = []string{"p1", "p2"} }, RejectAffectedPartitionsLimit},
		{"touched parts over limit", MutationAdmissionConfig{MaxTouchedParts: 1}, func(r *MutationRequest) { r.AffectedPartitions = []string{"p1"} }, RejectTouchedPartsLimit},
		{"touched bytes over limit", MutationAdmissionConfig{MaxTouchedBytes: 10}, func(r *MutationRequest) { r.AffectedPartitions = []string{"p1"} }, RejectTouchedBytesLimit},
		{"protocol column", MutationAdmissionConfig{}, func(r *MutationRequest) { r.AssignedColumns = []string{"_hg_seq"} }, RejectProtocolColumn},
		{"row id mutated", MutationAdmissionConfig{}, func(r *MutationRequest) { r.AssignedColumns = []string{"_hg_row_id"} }, RejectRowIDMutated},
		{"key column", MutationAdmissionConfig{}, func(r *MutationRequest) { r.AssignedColumns = []string{"p"} }, RejectKeyColumn},
		{"lightweight delete", MutationAdmissionConfig{}, func(r *MutationRequest) { r.LightweightDelete = true }, RejectLightweightDelete},
		{"truncate", MutationAdmissionConfig{}, func(r *MutationRequest) { r.SQL = "TRUNCATE TABLE net1.events" }, RejectTruncateOrDropPartition},
		{"drop partition", MutationAdmissionConfig{}, func(r *MutationRequest) { r.SQL = "ALTER TABLE net1.events DROP PARTITION 'p1'" }, RejectTruncateOrDropPartition},
		{"direct hg_safe", MutationAdmissionConfig{}, func(r *MutationRequest) {
			r.TableID = "hg_safe.events"
			r.AccessedTables = []sqlmeta.AccessedTable{{OriginalDatabase: "hg_safe", OriginalTable: "events"}}
		}, RejectDirectSafeModification},
		{"remote table", MutationAdmissionConfig{}, func(r *MutationRequest) {
			r.AccessedTables = []sqlmeta.AccessedTable{{OriginalDatabase: "net1", OriginalTable: "events", IsRemote: true}}
		}, RejectUnstableExpression},
		{"join/subquery multi-table", MutationAdmissionConfig{}, func(r *MutationRequest) {
			r.AccessedTables = []sqlmeta.AccessedTable{{OriginalTable: "events"}, {OriginalTable: "other"}}
		}, RejectUnstableExpression},
		{"nondeterministic func", MutationAdmissionConfig{}, func(r *MutationRequest) {
			r.SQL = "ALTER TABLE net1.events UPDATE v = now() WHERE p = 'p1'"
		}, RejectNondeterministicFunc},
		{"schema root mismatch", MutationAdmissionConfig{}, func(r *MutationRequest) { r.WorkerSchemaRoot = "other-root" }, RejectSchemaRootMismatch},
		{"manifest partition missing", MutationAdmissionConfig{}, func(r *MutationRequest) { r.AffectedPartitions = []string{"pX"} }, RejectManifestPartitionMissing},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := acceptableUpdate()
			tc.mutate(&req)
			_, err := ClassifyMutation(tc.cfg, req)
			if err == nil {
				t.Fatalf("expected reject %s, got accept", tc.want)
			}
			var rej MutationRejection
			if !errors.As(err, &rej) {
				t.Fatalf("expected MutationRejection, got %T: %v", err, err)
			}
			if rej.Reason != tc.want {
				t.Fatalf("reject reason = %s, want %s (%v)", rej.Reason, tc.want, err)
			}
		})
	}
}

func TestMutationRejection_ErrorString(t *testing.T) {
	rej := MutationRejection{Reason: RejectTouchedBytesLimit, Detail: "5 > 4"}
	msg := rej.Error()
	if !contains(msg, string(RejectTouchedBytesLimit)) || !contains(msg, "5 > 4") {
		t.Fatalf("error string must carry reason + detail: %q", msg)
	}
}

func contains(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

// TestMutationRejectReason_MatrixCovered guards against silently dropping a
// reject reason: every reason must be a non-empty, distinct slug.
func TestMutationRejectReason_MatrixCovered(t *testing.T) {
	seen := map[MutationRejectReason]bool{}
	for _, r := range allMutationRejectReasons {
		if r == "" {
			t.Fatal("empty reject reason in the matrix")
		}
		if seen[r] {
			t.Fatalf("duplicate reject reason %q", r)
		}
		seen[r] = true
	}
	if len(allMutationRejectReasons) < 15 {
		t.Fatalf("reject matrix shrank to %d reasons; every design 4.2 bullet must have one", len(allMutationRejectReasons))
	}
}

// TestSubmitMutation_SequencesPlan is the one gated end-to-end submission test:
// a classified plan submitted to the Arbiter reaches ACK1. It needs the absent
// C2 mutation-consensus seam and skips closed.
func TestSubmitMutation_SequencesPlan(t *testing.T) {
	requireCompanionMutationConsensus(t)
	t.Fatal("unreachable until the companion mutation-consensus seam lands")
}
