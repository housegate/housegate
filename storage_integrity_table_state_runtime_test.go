package housegate

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/sitable"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

// leavePendingRecord journals one non-terminal, prepared statement for
// net1.events: the source wrote, and the arbiter answered retryable.
func leavePendingRecord(t *testing.T, touched []string) sicore.IntakeJournal {
	t.Helper()
	journal, err := sicore.NewFileIntakeJournal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	owner := AdmissionRecordFromPlugin(bpEUAdmission())
	owner.TouchedPartitionIDs = touched
	first := sicore.NewOrchestrator(
		&rootRecordingSubmitter{outcome: sicore.SubmitOutcome{Category: sicore.OutcomeRetryable, Reason: "arbiter busy"}},
		&rootRecordingPreparer{
			source: "snode-A", claim: sicore.ClaimOutcome{Category: sicore.OutcomeAccepted, BoundSource: "snode-A"},
			candidates: []sicore.CandidatePart{{TableID: owner.TableID, PartitionID: "p_eu", PartName: "eu_part_1"}},
		},
		sicore.OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal},
	)
	if res, _ := first.Orchestrate(context.Background(), owner); res.IsTerminal() {
		t.Fatalf("fixture: the record must stay non-terminal, got %+v", res)
	}
	return journal
}

func recoverWith(t *testing.T, journal sicore.IntakeJournal, state sitable.TableState) (*fakePartsPressure, error) {
	t.Helper()
	pressure := &fakePartsPressure{}
	restarted := sicore.NewOrchestrator(
		&rootRecordingSubmitter{outcome: sicore.SubmitOutcome{Category: sicore.OutcomeRetryable, Reason: "arbiter busy"}},
		&rootRecordingPreparer{source: "snode-A", claim: sicore.ClaimOutcome{Category: sicore.OutcomeAccepted, BoundSource: "snode-A"}},
		sicore.OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal},
	)
	ingress, err := NewStorageIntegrityIngress(restarted, nil, sicore.MaterializerNative)
	if err != nil {
		t.Fatal(err)
	}
	ingress.WithPartsPressure(pressure, tableStateSchemaResolver(state))
	// Recovery keeps retrying the retryable submit after the restore hook ran;
	// the bound ends that loop once the hook's outcome is observable.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return pressure, ingress.RecoverPending(ctx)
}

func goneState() sitable.TableState {
	return sitable.NewFake(sitable.Pending, sitable.Table{ID: "net1.events", Status: sitable.Gone, Schema: bpSchemas()[0]})
}

// purgedState reports the name as unrecorded (default deny: Pending), which
// is how a host reports a Purged table (spec 2026-09-24 §5).
func purgedState() sitable.TableState { return sitable.NewFake(sitable.Pending) }

func TestRecoveryUsesJournaledPartitionsWithoutASchema(t *testing.T) {
	journal := leavePendingRecord(t, []string{"p_eu"})
	pressure, err := recoverWith(t, journal, purgedState())
	if errors.Is(err, ErrStorageIntegrityRecoverySchemaPurged) {
		t.Fatalf("a record with journaled partitions must not need a schema: %v", err)
	}
	pressure.mu.Lock()
	defer pressure.mu.Unlock()
	if pressure.restored != 1 {
		t.Fatalf("restored = %d, want the record restored from its journaled partitions", pressure.restored)
	}
}

func TestRecoveryOfAGoneTableResolvesItsSchema(t *testing.T) {
	journal := leavePendingRecord(t, nil)
	pressure, err := recoverWith(t, journal, goneState())
	if errors.Is(err, ErrStorageIntegrityRecoverySchemaPurged) {
		t.Fatalf("a Gone table's schema is still in the table state: %v", err)
	}
	pressure.mu.Lock()
	defer pressure.mu.Unlock()
	if pressure.restored != 1 {
		t.Fatalf("restored = %d, want 1", pressure.restored)
	}
}

func TestRecoveryNeedingAPurgedSchemaFailsWithTheNamedError(t *testing.T) {
	journal := leavePendingRecord(t, nil)
	_, err := recoverWith(t, journal, purgedState())
	if !errors.Is(err, ErrStorageIntegrityRecoverySchemaPurged) {
		t.Fatalf("err = %v, want ErrStorageIntegrityRecoverySchemaPurged", err)
	}
	// The operator needs both names to resolve the statement by hand.
	if msg := err.Error(); !strings.Contains(msg, "statement "+bpEUAdmission().StatementID) || !strings.Contains(msg, "net1.events") {
		t.Fatalf("err = %q, want it to name the statement id and the table", msg)
	}
}

// TestSchemaNotAllowedReachesTheClientAsNoLongerAcceptsWrites is spec
// 2026-09-24 §9.6.
func TestSchemaNotAllowedReachesTheClientAsNoLongerAcceptsWrites(t *testing.T) {
	ingress, _, submitter, _ := newBackpressureIngress(t, &fakePartsPressure{})
	submitter.outcome = sicore.SubmitOutcome{Category: sicore.OutcomeTerminalReject, Reason: "table retired", AdmissionCode: sicore.AdmissionCodeSchemaNotAllowed}
	err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), bpAdmission())
	var ce *chproto.ClientError
	if !errors.As(err, &ce) || ce.Code != chproto.CodeQueryIsProhibited || ce.Message != "storage_integrity: table net1.events no longer accepts writes" {
		t.Fatalf("err = %v, want the non-retryable no-longer-accepts-writes refusal", err)
	}
	// Spec §7.4: the refusal ends the query, not the session.
	if !ce.KeepSession || !chproto.KeepsSession(fmt.Errorf("wrapped by the plugin: %w", err)) {
		t.Fatalf("err = %+v, want KeepSession through the plugin's wrapping", ce)
	}
	// The arbiter's code and reason stay server-side, in Err only.
	if ce.Err == nil || ce.Err.Error() != "arbiter ADMISSION_CODE_SCHEMA_NOT_ALLOWED: table retired" {
		t.Fatalf("Err = %v, want the arbiter code and reason", ce.Err)
	}
}

func TestAdmissionUsesTheSnapshotSchemaNotTheResolver(t *testing.T) {
	ingress, _, _, _ := newBackpressureIngress(t, &fakePartsPressure{})
	ingress.schemas = StorageIntegrityTableSchemaResolverFunc(func(string) (payloadexec.TableSchema, bool) {
		t.Fatal("a new admission must not consult the recovery resolver")
		return payloadexec.TableSchema{}, false
	})
	adm := bpAdmission()
	schema := bpSchemas()[0]
	adm.TableSchema = &schema
	if err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), adm); err != nil {
		t.Fatalf("Consume: %v", err)
	}
}

// TestAdmissionBeforeTheMergeLatchIsAssertedIsRetryableAndKeepsTheSession is
// the controller ruling on Task 7 minor 1: a table that has just become Active
// has no merge-health entry until the change-triggered pass completes, so its
// admission is refused with the retryable, session-preserving 733 instead of
// the generic strict-input refusal that closes the connection.
func TestAdmissionBeforeTheMergeLatchIsAssertedIsRetryableAndKeepsTheSession(t *testing.T) {
	state := sitable.NewFake(sitable.Pending, sitable.Table{ID: "net1.events", Status: sitable.Active, Schema: bpSchemas()[0]})
	// No pass has run, so net1.events has no latch yet.
	supervisor := NewStorageIntegrityMergeSupervisor(&controllableMergeGuard{}, state, time.Hour)
	ingress, writer, submitter, _ := newBackpressureIngress(t, &fakePartsPressure{})
	ingress.guard = supervisor

	err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), bpAdmission())
	var ce *chproto.ClientError
	if !errors.As(err, &ce) || ce.Code != chproto.CodeTableIsBeingRestarted || !ce.KeepSession ||
		ce.Message != "storage_integrity: table net1.events is being activated; retry shortly (retryable)" {
		t.Fatalf("err = %v (%+v), want the retryable session-preserving 733 activation refusal", err, ce)
	}
	if !chproto.KeepsSession(fmt.Errorf("wrapped by the plugin: %w", err)) {
		t.Fatal("the refusal must keep the session through the plugin's wrapping")
	}
	if writer.calls != 0 || submitter.calls != 0 {
		t.Fatalf("payload/submit calls = %d/%d, want 0/0", writer.calls, submitter.calls)
	}
}

// TestAdmissionWithAnUnhealthyMergeLatchKeepsTheNonRetryableRefusal pins that
// only the not-yet-asserted latch is retryable: a real per-table guard error
// keeps today's refusal.
func TestAdmissionWithAnUnhealthyMergeLatchKeepsTheNonRetryableRefusal(t *testing.T) {
	state := sitable.NewFake(sitable.Pending, sitable.Table{ID: "net1.events", Status: sitable.Active, Schema: bpSchemas()[0]})
	pinErr := errors.New("max_bytes_to_merge_at_max_space_in_pool is not pinned to 0")
	supervisor := NewStorageIntegrityMergeSupervisor(&controllableMergeGuard{tableErrs: map[string]error{"net1.events": pinErr}}, state, time.Hour)
	if err := supervisor.Assert(context.Background()); !errors.Is(err, pinErr) {
		t.Fatalf("Assert = %v, want the per-table pin error", err)
	}
	ingress, _, _, _ := newBackpressureIngress(t, &fakePartsPressure{})
	ingress.guard = supervisor

	err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), bpAdmission())
	if err == nil || !errors.Is(err, pinErr) || !strings.HasPrefix(err.Error(), "storage_integrity ingress: merge health: ") {
		t.Fatalf("err = %v, want the merge-health refusal carrying the pin error", err)
	}
	var ce *chproto.ClientError
	if errors.As(err, &ce) || chproto.KeepsSession(err) {
		t.Fatalf("err = %v, an unhealthy latch must keep the generic session-closing refusal", err)
	}
}

// TestTableStateBackedAdmissionWithoutASnapshotSchemaIsRefused is defence in
// depth: the table-state-backed runtime never falls back to the recovery
// resolver for a new admission.
func TestTableStateBackedAdmissionWithoutASnapshotSchemaIsRefused(t *testing.T) {
	ingress, writer, submitter, _ := newBackpressureIngress(t, &fakePartsPressure{})
	ingress.requireAdmissionSchema = true
	adm := bpAdmission()
	adm.TableSchema = nil
	err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), adm)
	var ce *chproto.ClientError
	if !errors.As(err, &ce) || ce.Code != chproto.CodeQueryIsProhibited || ce.Message != "storage_integrity: table state is unavailable for this query" {
		t.Fatalf("err = %v, want the non-retryable table-state-unavailable refusal", err)
	}
	if writer.calls != 0 || submitter.calls != 0 {
		t.Fatalf("payload/submit calls = %d/%d, want 0/0", writer.calls, submitter.calls)
	}
}

// TestSnapshotSchemaIsCheckedWithoutResolverOrPressure pins that a supplied
// snapshot schema drives the hash check and the touched partitions even when
// the ingress has neither a resolver nor parts pressure.
func TestSnapshotSchemaIsCheckedWithoutResolverOrPressure(t *testing.T) {
	newBare := func(t *testing.T) (*StorageIntegrityIngress, sicore.IntakeJournal) {
		t.Helper()
		journal, err := sicore.NewFileIntakeJournal(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		orch := sicore.NewOrchestrator(
			&rootRecordingSubmitter{outcome: sicore.SubmitOutcome{Category: sicore.OutcomeAccepted}},
			&rootRecordingPreparer{source: "snode-A", claim: sicore.ClaimOutcome{Category: sicore.OutcomeAccepted, BoundSource: "snode-A"}, candidates: bpPreparedCandidates()},
			sicore.OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal},
		)
		ingress, err := NewStorageIntegrityIngress(orch, nil, sicore.MaterializerNative)
		if err != nil {
			t.Fatal(err)
		}
		return ingress, journal
	}
	schema := bpSchemas()[0]

	t.Run("hash mismatch refused", func(t *testing.T) {
		ingress, _ := newBare(t)
		adm := bpAdmission()
		adm.TableSchema = &schema
		adm.SchemaHash = "not-the-snapshot-hash"
		err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), adm)
		if err == nil || !strings.Contains(err.Error(), "does not match authoritative table schema") {
			t.Fatalf("err = %v, want the schema_hash mismatch refusal", err)
		}
	})
	t.Run("touched partitions journaled", func(t *testing.T) {
		ingress, journal := newBare(t)
		adm := bpAdmission()
		adm.TableSchema = &schema
		if err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), adm); err != nil {
			t.Fatalf("Consume: %v", err)
		}
		rec, ok, err := journal.LoadIntakeRecord(context.Background(), adm.StatementID)
		if err != nil || !ok {
			t.Fatalf("load record: ok=%v err=%v", ok, err)
		}
		if got := rec.Admission.TouchedPartitionIDs; len(got) != 2 || got[0] != "p_eu" || got[1] != "p_us" {
			t.Fatalf("touched partitions = %v, want [p_eu p_us] from the snapshot schema", got)
		}
	})
}
