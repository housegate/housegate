package housegate

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/chproto"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

// Spec 2026-10-09 §6.6 (2): every ConsumeStorageIntegrityAdmission refusal
// before Orchestrate is provably unspent.
func TestConsumeAdmission_PreOrchestrateRefusalsAreUnspent(t *testing.T) {
	orch := sicore.NewOrchestrator(&rootRecordingSubmitter{}, &rootRecordingPreparer{}, sicore.OrchestratorConfig{})
	ing, err := NewStorageIntegrityIngressWithPayloadWriter(orch, nil, sicore.MaterializerNative, &rootRecordingPayloadWriter{})
	if err != nil {
		t.Fatal(err)
	}
	err = ing.ConsumeStorageIntegrityAdmission(context.Background(), storageIntegrityAdmissionForEncoding(sicore.EncodingCSVWithNames, 54465))
	if err == nil || !chproto.IsSeqUnspent(err) {
		t.Fatalf("materializer mismatch err = %v, want marked unspent", err)
	}

	guarded, err := NewStorageIntegrityIngressWithPayloadWriter(orch, &unhealthyMergeGuard{err: errors.New("reconnect failed")}, sicore.MaterializerNative, &rootRecordingPayloadWriter{})
	if err != nil {
		t.Fatal(err)
	}
	err = guarded.ConsumeStorageIntegrityAdmission(context.Background(), storageIntegrityAdmissionForEncoding(sicore.PayloadEncodingClickHouseNativeData, 54465))
	if err == nil || !chproto.IsSeqUnspent(err) {
		t.Fatalf("merge-health err = %v, want marked unspent", err)
	}

	activating, err := NewStorageIntegrityIngressWithPayloadWriter(orch, &unhealthyMergeGuard{err: errStorageIntegrityMergeGuardNotAsserted}, sicore.MaterializerNative, &rootRecordingPayloadWriter{})
	if err != nil {
		t.Fatal(err)
	}
	err = activating.ConsumeStorageIntegrityAdmission(context.Background(), storageIntegrityAdmissionForEncoding(sicore.PayloadEncodingClickHouseNativeData, 54465))
	if err == nil || !chproto.IsSeqUnspent(err) || !chproto.KeepsSession(err) {
		t.Fatalf("table-activating err = %v, want a marked session-preserving refusal", err)
	}

	failingWriter := &rootRecordingPayloadWriter{err: errors.New("store down")}
	stored, err := NewStorageIntegrityIngressWithPayloadWriter(orch, nil, sicore.MaterializerNative, failingWriter)
	if err != nil {
		t.Fatal(err)
	}
	err = stored.ConsumeStorageIntegrityAdmission(context.Background(), storageIntegrityAdmissionForEncoding(sicore.PayloadEncodingClickHouseNativeData, 54465))
	if err == nil || !chproto.IsSeqUnspent(err) {
		t.Fatalf("PutPayload err = %v, want marked unspent", err)
	}
	if failingWriter.calls != 1 {
		t.Fatalf("payload writer calls = %d, want 1: the refusal must come from PutPayload", failingWriter.calls)
	}

	emptyRefWriter := &rootRecordingPayloadWriter{}
	emptyRef, err := NewStorageIntegrityIngressWithPayloadWriter(orch, nil, sicore.MaterializerNative, emptyRefWriter)
	if err != nil {
		t.Fatal(err)
	}
	err = emptyRef.ConsumeStorageIntegrityAdmission(context.Background(), storageIntegrityAdmissionForEncoding(sicore.PayloadEncodingClickHouseNativeData, 54465))
	if err == nil || !strings.Contains(err.Error(), "empty payload_ref") || !chproto.IsSeqUnspent(err) {
		t.Fatalf("empty payload_ref err = %v, want marked unspent", err)
	}
}

// The parts-pressure refusal (252) happens before the payload store and the
// orchestrator, so it is unspent too, and stays session-preserving.
func TestConsumeAdmission_BackpressureRefusalIsUnspent(t *testing.T) {
	pressure := &fakePartsPressure{refuse: map[string]error{
		"net1__events/p_us": &sicore.BackpressureError{Database: "hg_unsafe", Table: "net1__events", Partition: "p_us", Parts: 2400, Limit: 2400, Kind: "soft"},
	}}
	ingress, writer, submitter, _ := newBackpressureIngress(t, pressure)
	err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), bpAdmission())
	var clientErr *chproto.ClientError
	if !errors.As(err, &clientErr) || clientErr.Code != chproto.CodeTooManyParts || !chproto.KeepsSession(err) {
		t.Fatalf("err = %v, want session-preserving ClientError 252", err)
	}
	if !chproto.IsSeqUnspent(err) {
		t.Fatalf("back-pressure err = %v, want marked unspent", err)
	}
	if writer.calls != 0 || submitter.calls != 0 {
		t.Fatalf("writer/submit calls = %d/%d, want 0/0", writer.calls, submitter.calls)
	}
}

// A payload-store refusal for capacity or the per-indexer write quota is
// retryable back-pressure: session-preserving 252, unspent, never submitted.
func TestConsumeAdmission_PayloadStoreBackpressureIsRetryable(t *testing.T) {
	submitter := &rootRecordingSubmitter{}
	orch := sicore.NewOrchestrator(submitter, &rootRecordingPreparer{}, sicore.OrchestratorConfig{})
	writer := &rootRecordingPayloadWriter{err: fmt.Errorf("%w: payload store: write quota exceeded for indexer 7 (bytes)", sicore.ErrBackpressure)}
	ing, err := NewStorageIntegrityIngressWithPayloadWriter(orch, nil, sicore.MaterializerNative, writer)
	if err != nil {
		t.Fatal(err)
	}
	err = ing.ConsumeStorageIntegrityAdmission(context.Background(), storageIntegrityAdmissionForEncoding(sicore.PayloadEncodingClickHouseNativeData, 54465))
	var clientErr *chproto.ClientError
	if !errors.As(err, &clientErr) || clientErr.Code != chproto.CodeTooManyParts || !chproto.KeepsSession(err) || !chproto.IsSeqUnspent(err) {
		t.Fatalf("err = %v, want unspent session-preserving ClientError 252", err)
	}
	if !strings.Contains(err.Error(), "write quota exceeded") {
		t.Fatalf("err = %v, want the store's reason", err)
	}
	if writer.calls != 1 || submitter.calls != 0 {
		t.Fatalf("writer/submit calls = %d/%d, want 1/0", writer.calls, submitter.calls)
	}
}

func TestConsumeAdmission_PostOrchestrateMarking(t *testing.T) {
	for name, tc := range map[string]struct {
		outcome    sicore.SubmitOutcome
		submitErr  error
		wantMarked bool
		wantText   string
	}{
		"gap budget":         {outcome: sicore.SubmitOutcome{Category: sicore.OutcomeTerminalReject, Reason: "64 open ranges", AdmissionCode: "ADMISSION_CODE_GAP_BUDGET_EXCEEDED"}, wantMarked: true, wantText: "rejected by the arbiter: ADMISSION_CODE_GAP_BUDGET_EXCEEDED"},
		"lane budget":        {outcome: sicore.SubmitOutcome{Category: sicore.OutcomeTerminalReject, Reason: "account holds 256 client lanes", AdmissionCode: sicore.AdmissionCodeLaneBudgetExceeded}, wantMarked: true, wantText: "rejected by the arbiter: ADMISSION_CODE_LANE_BUDGET_EXCEEDED"},
		"malformed":          {outcome: sicore.SubmitOutcome{Category: sicore.OutcomeTerminalReject, Reason: "bad", AdmissionCode: "ADMISSION_CODE_MALFORMED"}, wantMarked: true, wantText: "rejected by the arbiter: ADMISSION_CODE_MALFORMED"},
		"schema":             {outcome: sicore.SubmitOutcome{Category: sicore.OutcomeTerminalReject, Reason: "retired", AdmissionCode: sicore.AdmissionCodeSchemaNotAllowed}, wantMarked: true, wantText: "no longer accepts writes"},
		"source unavailable": {outcome: sicore.SubmitOutcome{Category: sicore.OutcomeTerminalReject, Reason: "storage-integrity source snode-b of indexer 1 is not active", AdmissionCode: sicore.AdmissionCodeSourceUnavailable}, wantMarked: true, wantText: "the source of table net1.events is not active yet"},
		"quarantined":        {outcome: sicore.SubmitOutcome{Category: sicore.OutcomeTerminalReject, Reason: "storage-integrity indexer 1 is quarantined", AdmissionCode: sicore.AdmissionCodeIndexerQuarantined}, wantMarked: true, wantText: "the indexer that owns table net1.events is quarantined"},
		"verifiers":          {outcome: sicore.SubmitOutcome{Category: sicore.OutcomeTerminalReject, Reason: "storage-integrity verifiers unavailable for indexer 1: 2 eligible, at least 3 required", AdmissionCode: sicore.AdmissionCodeVerifiersUnavailable}, wantMarked: true, wantText: "the verifiers of table net1.events are not available yet"},
		"copy floor":         {outcome: sicore.SubmitOutcome{Category: sicore.OutcomeTerminalReject, Reason: "storage-integrity copy floor: net1.events holds 20001 uncleaned promoted parts", AdmissionCode: sicore.AdmissionCodeCopiesBackpressure}, wantMarked: true, wantText: "back-pressure: table net1.events waits for its replica copies"},
		"account budget":     {outcome: sicore.SubmitOutcome{Category: sicore.OutcomeTerminalReject, Reason: "account budget: account 0xabc exceeded its bytes budget (window 1800 L2 blocks); retry once the table-registry cursor reaches L2 block 9 (committed cursor 3)", AdmissionCode: sicore.AdmissionCodeAccountBudgetExceeded}, wantMarked: true, wantText: "exceeded its network write budget (bytes); retry in 60s (retryable)"},
		"network budget":     {outcome: sicore.SubmitOutcome{Category: sicore.OutcomeTerminalReject, Reason: "account budget: the network-wide total exceeded its statements budget (window 1800 L2 blocks); retry once the table-registry cursor reaches L2 block 9 (committed cursor 3)", AdmissionCode: sicore.AdmissionCodeAccountBudgetExceeded}, wantMarked: true, wantText: "storage_integrity: the network write budget is exhausted (statements); retry in 60s (retryable)"},
		"duplicate":          {outcome: sicore.SubmitOutcome{Category: sicore.OutcomeTerminalReject, Reason: "dup", AdmissionCode: sicore.AdmissionCodeDuplicateClientSeq}},
		"no code":            {outcome: sicore.SubmitOutcome{Category: sicore.OutcomeTerminalReject, Reason: "permission denied"}},
		"retryable":          {outcome: sicore.SubmitOutcome{Category: sicore.OutcomeRetryable, Reason: "fence"}},
		"unknown":            {outcome: sicore.SubmitOutcome{Category: sicore.OutcomeUnknown, Reason: "deadline"}},
		"submit err":         {submitErr: errors.New("transport reset")},
	} {
		t.Run(name, func(t *testing.T) {
			ingress, _, submitter, _ := newBackpressureIngress(t, &fakePartsPressure{})
			submitter.outcome = tc.outcome
			submitter.err = tc.submitErr
			adm := bpAdmission()
			err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), adm)
			if err == nil {
				t.Fatal("a non-ACK2 outcome must refuse")
			}
			if submitter.calls != 1 {
				t.Fatalf("submitter calls = %d, want 1: the outcome must come from the arbiter", submitter.calls)
			}
			if chproto.IsSeqUnspent(err) != tc.wantMarked {
				t.Fatalf("err = %v, marked = %v, want %v", err, chproto.IsSeqUnspent(err), tc.wantMarked)
			}
			if tc.wantText != "" && !strings.Contains(err.Error(), tc.wantText) {
				t.Fatalf("err = %v, want %q", err, tc.wantText)
			}
			if name == "gap budget" && err.Error() != "storage_integrity: statement "+adm.StatementID+" rejected by the arbiter: ADMISSION_CODE_GAP_BUDGET_EXCEEDED" {
				t.Fatalf("coded reject text = %q", err)
			}
		})
	}
}

// The unspent flag is sticky through wrapping (chproto.IsSeqUnspent walks the
// whole chain), so a statement refused unspent before Orchestrate and then
// retried under the same id must not carry that flag into a refusal raised
// after submission, where the coordinate may be spent.
func TestConsumeAdmission_PostSubmissionRefusalAfterUnspentRefusalIsUnmarked(t *testing.T) {
	for name, outcome := range map[string]sicore.SubmitOutcome{
		"duplicate": {Category: sicore.OutcomeTerminalReject, Reason: "dup", AdmissionCode: sicore.AdmissionCodeDuplicateClientSeq},
		"no code":   {Category: sicore.OutcomeTerminalReject, Reason: "permission denied"},
		"unknown":   {Category: sicore.OutcomeUnknown, Reason: "deadline"},
	} {
		t.Run(name, func(t *testing.T) {
			ingress, writer, submitter, _ := newBackpressureIngress(t, &fakePartsPressure{})
			adm := bpAdmission()

			writer.err = errors.New("store down")
			err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), adm)
			if err == nil || !chproto.IsSeqUnspent(err) || submitter.calls != 0 {
				t.Fatalf("first attempt err = %v (submit calls %d), want a marked pre-submission refusal", err, submitter.calls)
			}

			writer.err = nil
			submitter.outcome = outcome
			err = ingress.ConsumeStorageIntegrityAdmission(context.Background(), adm)
			if err == nil {
				t.Fatal("retry must refuse")
			}
			if submitter.calls != 1 {
				t.Fatalf("submitter calls = %d, want 1: the retry must reach the arbiter", submitter.calls)
			}
			if chproto.IsSeqUnspent(err) || strings.Contains(err.Error(), chproto.SeqUnspentSuffix) {
				t.Fatalf("post-submission err = %v carries the unspent flag", err)
			}
		})
	}
}

// Spec 2026-10-09 §6.6 marks the payload-store refusals only on the first
// presentation of a statement id. A statement that already prepared (its
// orchestrator record no longer requires a prepare) may have been submitted,
// so a payload-store refusal on its retry is unmarked (final review M2).
func TestConsumeAdmission_PayloadRefusalOfAResumedStatementIsUnmarked(t *testing.T) {
	for name, fail := range map[string]func(*rootRecordingPayloadWriter){
		"put payload":       func(w *rootRecordingPayloadWriter) { w.err = errors.New("store down") },
		"empty payload_ref": func(w *rootRecordingPayloadWriter) { w.result.PayloadRef = "" },
	} {
		t.Run(name, func(t *testing.T) {
			ingress, writer, submitter, preparer := newBackpressureIngress(t, &fakePartsPressure{})
			adm := bpAdmission()
			submitter.outcome = sicore.SubmitOutcome{Category: sicore.OutcomeUnknown, Reason: "deadline"}
			if err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), adm); err == nil || chproto.IsSeqUnspent(err) {
				t.Fatalf("first attempt err = %v, want an unmarked post-submission refusal", err)
			}
			if preparer.prepareCalls != 1 || submitter.calls != 1 {
				t.Fatalf("prepare/submit calls = %d/%d, want 1/1", preparer.prepareCalls, submitter.calls)
			}
			fail(writer)
			err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), adm)
			if err == nil {
				t.Fatal("the retry must refuse")
			}
			if writer.calls != 2 {
				t.Fatalf("payload writer calls = %d, want 2: the retry must reach the payload store", writer.calls)
			}
			if chproto.IsSeqUnspent(err) {
				t.Fatalf("retry err = %v carries the unspent flag although the statement was submitted", err)
			}
		})
	}
}

// A preflight failure is never marked: its "reused with a different envelope"
// class only arises when a statement id is presented again (final review M2).
func TestConsumeAdmission_PreflightRefusalIsUnmarked(t *testing.T) {
	ingress, _, submitter, _ := newBackpressureIngress(t, &fakePartsPressure{})
	adm := bpAdmission()
	submitter.outcome = sicore.SubmitOutcome{Category: sicore.OutcomeUnknown, Reason: "deadline"}
	if err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), adm); err == nil {
		t.Fatal("first attempt must refuse")
	}
	changed := bpEUAdmission() // same statement id, different payload
	err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), changed)
	if err == nil || !strings.Contains(err.Error(), "preflight") {
		t.Fatalf("err = %v, want a preflight refusal", err)
	}
	if chproto.IsSeqUnspent(err) {
		t.Fatalf("preflight err = %v carries the unspent flag", err)
	}
}

// A statement can be sequenced while it still needs a source Prepare: Prepare
// and Submit run concurrently, so the arbiter can accept the Submit while the
// Prepare is rejected before writing, or fails indeterminately and is not
// found. AdmissionRequiresPrepare is then true on the re-presentation, but the
// client_seq coordinate is spent. A payload-store refusal of that retry must
// not carry the unspent flag: a client honouring it would release the seq and
// reuse it for its next statement, whose id then collides with the sequenced
// one, while the sequenced statement is left unprepared.
func TestConsumeAdmission_PayloadRefusalOfASequencedUnpreparedStatementIsUnmarked(t *testing.T) {
	failures := map[string]func(*rootRecordingPayloadWriter){
		"put payload": func(w *rootRecordingPayloadWriter) { w.err = errors.New("store down") },
		"put back-pressure": func(w *rootRecordingPayloadWriter) {
			w.err = fmt.Errorf("%w: payload store: write quota exceeded", sicore.ErrBackpressure)
		},
		"empty payload_ref": func(w *rootRecordingPayloadWriter) { w.result.PayloadRef = "" },
	}
	prepareFailures := map[string]error{
		"prepare rejected before writing":     fmt.Errorf("schema hash mismatch: %w", sicore.ErrPrepareTerminalReject),
		"prepare indeterminate and not found": errors.New("connection reset by peer"),
	}
	for prepName, prepErr := range prepareFailures {
		for failName, fail := range failures {
			t.Run(prepName+"/"+failName, func(t *testing.T) {
				ingress, writer, submitter, preparer := newBackpressureIngress(t, &fakePartsPressure{})
				adm := bpAdmission()
				preparer.err = prepErr
				if err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), adm); err == nil || chproto.IsSeqUnspent(err) {
					t.Fatalf("first attempt err = %v, want an unmarked refusal", err)
				}
				if submitter.calls != 1 {
					t.Fatalf("submit calls = %d, want 1: the statement is sequenced", submitter.calls)
				}
				probe := AdmissionRecordFromPlugin(adm)
				probe.TouchedPartitionIDs = []string{"p_eu", "p_us"} // as ingress derives them
				requires, err := ingress.orch.AdmissionRequiresPrepare(context.Background(), probe)
				if err != nil || !requires {
					t.Fatalf("AdmissionRequiresPrepare = (%v, %v), want true: the sequenced statement still needs a Prepare", requires, err)
				}
				preparer.err = nil
				preparer.lookupFound = false
				fail(writer)
				err = ingress.ConsumeStorageIntegrityAdmission(context.Background(), adm)
				if err == nil {
					t.Fatal("the retry must refuse")
				}
				if chproto.IsSeqUnspent(err) {
					t.Fatalf("retry err = %v carries the unspent flag although the statement was sequenced", err)
				}
				if preparer.prepareCalls != 1 {
					t.Fatalf("prepare calls = %d, want 1", preparer.prepareCalls)
				}
			})
		}
	}
}

// The same holds for the parts-pressure refusal: a sequenced statement that
// still needs a Prepare and holds no tracked reservation (here seeded straight
// through the orchestrator, as after a restart that did not restore one)
// re-reserves, and a refusal of that reservation is not unspent.
func TestConsumeAdmission_BackpressureRefusalOfASequencedUnpreparedStatementIsUnmarked(t *testing.T) {
	pressure := &fakePartsPressure{}
	ingress, _, _, preparer := newBackpressureIngress(t, pressure)
	adm := bpAdmission()
	preparer.err = fmt.Errorf("schema hash mismatch: %w", sicore.ErrPrepareTerminalReject)
	rec := AdmissionRecordFromPlugin(adm)
	rec.PayloadRef = "payload://store/ref-1"
	rec.TouchedPartitionIDs = []string{"p_eu", "p_us"}
	if res, _ := ingress.orch.Orchestrate(context.Background(), rec); res.Ack2 || res.Submit.Category != sicore.OutcomeAccepted {
		t.Fatalf("seed = %+v, want an accepted submit without ACK2", res)
	}
	preparer.err = nil
	pressure.refuse = map[string]error{
		"net1__events/p_us": &sicore.BackpressureError{Database: "hg_unsafe", Table: "net1__events", Partition: "p_us", Parts: 2400, Limit: 2400, Kind: "soft"},
	}
	err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), adm)
	var clientErr *chproto.ClientError
	if !errors.As(err, &clientErr) || clientErr.Code != chproto.CodeTooManyParts {
		t.Fatalf("err = %v, want the 252 back-pressure refusal", err)
	}
	if chproto.IsSeqUnspent(err) {
		t.Fatalf("err = %v carries the unspent flag although the statement was sequenced", err)
	}
}

// TestConsumeAdmission_AccountBudgetRetryAfterIsConfigurable: the arbiter's
// write-budget refusal answers 201 with the configured retry hint, keeps the
// session and leaves the coordinate unspent.
func TestConsumeAdmission_AccountBudgetRetryAfterIsConfigurable(t *testing.T) {
	ingress, _, submitter, _ := newBackpressureIngress(t, &fakePartsPressure{})
	ingress.SetAccountBudgetRetryAfter(15 * time.Second)
	submitter.outcome = sicore.SubmitOutcome{Category: sicore.OutcomeTerminalReject, Reason: "account budget: account 0xabc exceeded its statements budget", AdmissionCode: sicore.AdmissionCodeAccountBudgetExceeded}
	err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), bpAdmission())
	var clientErr *chproto.ClientError
	if !errors.As(err, &clientErr) || clientErr.Code != chproto.CodeQuotaExceeded || !chproto.KeepsSession(err) || !chproto.IsSeqUnspent(err) {
		t.Fatalf("err = %v, want an unspent session-preserving 201", err)
	}
	if !strings.Contains(clientErr.Message, "(statements); retry in 15s (retryable)") || !chproto.IsWriteBudgetExceededMessage(clientErr.Message) {
		t.Fatalf("message = %q", clientErr.Message)
	}
}
