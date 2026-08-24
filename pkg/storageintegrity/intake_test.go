package storageintegrity

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// fixtureRevision is the pinned client protocol revision shared by
// admissionFixture and boundSource so their envelopes agree.
const fixtureRevision = 54465
const fixtureSigner = "0xabc"
const fixturePayload = "native-block-bytes"

// admissionFixture returns a complete, INSERT-shaped admission record that the
// intake orchestrator can turn into a source envelope.
func admissionFixture() AdmissionRecord {
	return AdmissionRecord{
		StatementID:     fixtureStatementID(1),
		Kind:            KindInsert,
		TableID:         "net1.events",
		SQL:             "INSERT INTO events FORMAT Native",
		SQLHash:         replay.DigestString("INSERT INTO events FORMAT Native"),
		Signer:          fixtureSigner,
		UserJWS:         "jws",
		Payload:         []byte(fixturePayload),
		PayloadLength:   uint64(len(fixturePayload)),
		PayloadHash:     replay.DigestBytes([]byte(fixturePayload)),
		PayloadEncoding: PayloadEncodingClickHouseNativeData,
		Revision:        fixtureRevision,
		EnvelopeVersion: EnvelopeVersionV2,
		NetworkID:       "testnet-v2",
		KeeperShardID:   0,
		SettingsHash:    EmptySettingsHash,
		SchemaHash:      "0x" + strings.Repeat("22", 32),
		RowIDProfileID:  payloadexec.RowIDProfileID,
	}
}

func validNativeAdmissionV2(t *testing.T) AdmissionRecord {
	t.Helper()
	sql := "INSERT INTO tenant.events FORMAT Native"
	payload := []byte{byte(2), 0, 0xab, 0xcd}
	return AdmissionRecord{
		StatementID:     "0xabc0000000000000000000000000000000000001:1:n1",
		Kind:            KindInsert,
		TableID:         "tenant.events",
		SQL:             sql,
		SQLHash:         replay.DigestString(sql),
		Signer:          "0xabc0000000000000000000000000000000000001",
		UserJWS:         "h.p.s",
		Payload:         payload,
		PayloadLength:   uint64(len(payload)),
		PayloadHash:     replay.DigestBytes(payload),
		PayloadEncoding: PayloadEncodingClickHouseNativeData,
		Revision:        54460,
		EnvelopeVersion: EnvelopeVersionV2,
		NetworkID:       "testnet-v2",
		KeeperShardID:   0,
		SettingsHash:    EmptySettingsHash,
		SchemaHash:      "0x" + strings.Repeat("33", 32),
		RowIDProfileID:  payloadexec.RowIDProfileID,
	}
}

func fixtureStatementID(seq uint64) string {
	return fixtureSigner + ":" + strconv.FormatUint(seq, 10) + ":n" + strconv.FormatUint(seq, 10)
}

// --- Pure HouseGate-local invariants: green today, no companion seam needed ---

func TestEnvelopeFromAdmission_MirrorsPayloadIdentity(t *testing.T) {
	adm := admissionFixture()
	env, err := EnvelopeFromAdmission(adm)
	if err != nil {
		t.Fatalf("EnvelopeFromAdmission: %v", err)
	}
	if env.StatementID != adm.StatementID {
		t.Fatalf("statement id: got %q want %q", env.StatementID, adm.StatementID)
	}
	if env.StatementKind != adm.Kind {
		t.Fatalf("statement kind: got %q want %q", env.StatementKind, adm.Kind)
	}
	if env.TargetTableID != adm.TableID {
		t.Fatalf("target table: got %q want %q", env.TargetTableID, adm.TableID)
	}
	if env.SQLHash != adm.SQLHash {
		t.Fatalf("sql hash: got %q want %q", env.SQLHash, adm.SQLHash)
	}
	if env.UserJWS != adm.UserJWS {
		t.Fatalf("user jws: got %q want %q", env.UserJWS, adm.UserJWS)
	}
	if env.PayloadHash != adm.PayloadHash {
		t.Fatalf("payload hash: got %q want %q", env.PayloadHash, adm.PayloadHash)
	}
	if env.PayloadRef != adm.PayloadHash {
		t.Fatalf("payload ref fallback: got %q want %q", env.PayloadRef, adm.PayloadHash)
	}
	if env.PayloadLength != adm.PayloadLength {
		t.Fatalf("payload length: got %d want %d", env.PayloadLength, adm.PayloadLength)
	}
	if env.PayloadEncoding != adm.PayloadEncoding {
		t.Fatalf("payload encoding: got %q want %q", env.PayloadEncoding, adm.PayloadEncoding)
	}
	if env.Signer != adm.Signer {
		t.Fatalf("signer: got %q want %q", env.Signer, adm.Signer)
	}
	if env.Revision != adm.Revision {
		t.Fatalf("revision: got %d want %d", env.Revision, adm.Revision)
	}
}

func TestEnvelopeFromAdmission_CarriesV2Fields(t *testing.T) {
	adm := validNativeAdmissionV2(t)
	env, err := EnvelopeFromAdmission(adm)
	if err != nil {
		t.Fatalf("EnvelopeFromAdmission: %v", err)
	}
	if env.EnvelopeVersion != EnvelopeVersionV2 || env.NetworkID != "testnet-v2" || env.KeeperShardID != 0 ||
		env.SettingsHash != EmptySettingsHash || env.SchemaHash != adm.SchemaHash ||
		env.RowIDProfileID != payloadexec.RowIDProfileID || env.PayloadEncoding != PayloadEncodingClickHouseNativeData ||
		env.Revision != 54460 {
		t.Fatalf("envelope v2 fields not carried: %+v", env)
	}
}

func TestEnvelopeFromAdmission_RejectsV2Violations(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*AdmissionRecord)
		want   string
	}{
		{"wrong envelope version", func(a *AdmissionRecord) { a.EnvelopeVersion = 1 }, "envelope_version"},
		{"missing network id", func(a *AdmissionRecord) { a.NetworkID = "" }, "network_id"},
		{"blank network id", func(a *AdmissionRecord) { a.NetworkID = " \t" }, "network_id"},
		{"non-zero shard", func(a *AdmissionRecord) { a.KeeperShardID = 1 }, "keeper_shard_id"},
		{"non-empty settings hash", func(a *AdmissionRecord) { a.SettingsHash = replay.DigestString("x") }, "settings_hash"},
		{"missing schema hash", func(a *AdmissionRecord) { a.SchemaHash = "" }, "schema_hash"},
		{"wrong row id profile", func(a *AdmissionRecord) { a.RowIDProfileID = "housegate-row-id-v0" }, "row_id_profile_id"},
		{"csv encoding no longer admitted", func(a *AdmissionRecord) { a.PayloadEncoding = EncodingCSVWithNames }, "payload encoding"},
		{"missing revision", func(a *AdmissionRecord) { a.Revision = 0 }, "revision"},
		{"negative revision", func(a *AdmissionRecord) { a.Revision = -1 }, "revision"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			adm := validNativeAdmissionV2(t)
			tc.mutate(&adm)
			_, err := EnvelopeFromAdmission(adm)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestEnvelopeFromAdmission_UsesOpaquePayloadRefWhenPresent(t *testing.T) {
	adm := admissionFixture()
	adm.PayloadRef = "payload://store/ref-1"

	env, err := EnvelopeFromAdmission(adm)
	if err != nil {
		t.Fatalf("EnvelopeFromAdmission: %v", err)
	}
	if env.PayloadRef != adm.PayloadRef {
		t.Fatalf("payload ref: got %q want %q", env.PayloadRef, adm.PayloadRef)
	}
	if env.PayloadHash != adm.PayloadHash {
		t.Fatalf("payload hash: got %q want %q", env.PayloadHash, adm.PayloadHash)
	}
}

func TestEnvelopeFromAdmission_RejectsPayloadEncodingSQLMismatch(t *testing.T) {
	adm := admissionFixture()
	adm.PayloadEncoding = EncodingCSVWithNames

	if _, err := EnvelopeFromAdmission(adm); err == nil {
		t.Fatal("EnvelopeFromAdmission accepted Native SQL with CSV payload encoding")
	}
}

func TestEnvelopeFromAdmission_RejectsPayloadHashMismatch(t *testing.T) {
	adm := admissionFixture()
	adm.PayloadHash = replay.DigestString("different payload")

	if _, err := EnvelopeFromAdmission(adm); err == nil {
		t.Fatal("EnvelopeFromAdmission accepted payload hash that does not match captured bytes")
	}
}

func TestEnvelopeFromAdmission_RejectsEmptyStatementID(t *testing.T) {
	adm := admissionFixture()
	adm.StatementID = ""
	if _, err := EnvelopeFromAdmission(adm); err == nil {
		t.Fatal("expected rejection of empty statement id")
	}
}

func TestEnvelopeFromAdmission_RejectsMissingWireFields(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*AdmissionRecord)
	}{
		{"missing sql hash", func(a *AdmissionRecord) { a.SQLHash = "" }},
		{"wrong sql hash", func(a *AdmissionRecord) { a.SQLHash = replay.DigestString("different") }},
		{"missing signer", func(a *AdmissionRecord) { a.Signer = "" }},
		{"missing user jws", func(a *AdmissionRecord) { a.UserJWS = "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			adm := admissionFixture()
			tc.mutate(&adm)
			if _, err := EnvelopeFromAdmission(adm); err == nil {
				t.Fatal("EnvelopeFromAdmission accepted incomplete wire fields")
			}
		})
	}
}

func TestEnvelopeFromAdmission_RejectsInvalidStructuredStatementID(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*AdmissionRecord)
	}{
		{"malformed", func(a *AdmissionRecord) { a.StatementID = "q-1" }},
		{"zero seq", func(a *AdmissionRecord) { a.StatementID = fixtureSigner + ":0:n0" }},
		{"signer mismatch", func(a *AdmissionRecord) { a.StatementID = "0xdef:1:n1" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			adm := admissionFixture()
			tc.mutate(&adm)
			if _, err := EnvelopeFromAdmission(adm); err == nil {
				t.Fatal("EnvelopeFromAdmission accepted invalid structured statement id")
			}
		})
	}
}

func TestEnvelopeFromAdmission_RejectsInlineInsertSQL(t *testing.T) {
	adm := admissionFixture()
	adm.SQL = "INSERT INTO events VALUES (1)"
	adm.SQLHash = replay.DigestString(adm.SQL)
	if _, err := EnvelopeFromAdmission(adm); err == nil {
		t.Fatal("EnvelopeFromAdmission accepted inline INSERT SQL")
	}
}

func TestEnvelopeFromAdmission_RejectsInsertWithoutPayload(t *testing.T) {
	adm := admissionFixture()
	adm.Payload = nil
	adm.PayloadLength = 0
	if _, err := EnvelopeFromAdmission(adm); err == nil {
		t.Fatal("expected rejection of INSERT admission without payload")
	}
}

// A payload-bearing admission must pin the client protocol revision; without it
// a source preparer cannot authenticate the revision it decodes the payload with.
func TestEnvelopeFromAdmission_RejectsInsertWithoutRevision(t *testing.T) {
	adm := admissionFixture()
	adm.Revision = 0
	if _, err := EnvelopeFromAdmission(adm); err == nil {
		t.Fatal("expected rejection of INSERT admission with revision 0")
	}
}

// The prepare and submit envelopes must be byte-identical so the payload
// identity a source claim binds matches the identity the Arbiter sequences.
func TestPrepareAndSubmitShareIdenticalEnvelope(t *testing.T) {
	adm := admissionFixture()
	env, err := EnvelopeFromAdmission(adm)
	if err != nil {
		t.Fatalf("EnvelopeFromAdmission: %v", err)
	}
	if env.PayloadHash == "" || env.PayloadLength == 0 {
		t.Fatal("envelope must carry a payload hash and length")
	}
	// The same envelope value is handed to both ports; equality here is what
	// the orchestrator relies on to keep prepare and submit consistent.
	env2, err := EnvelopeFromAdmission(adm)
	if err != nil {
		t.Fatalf("EnvelopeFromAdmission: %v", err)
	}
	if env != env2 {
		t.Fatal("EnvelopeFromAdmission is not deterministic for the same admission")
	}
}

func TestClassifyOutcome(t *testing.T) {
	cases := []struct {
		name string
		in   OutcomeCategory
		ack  bool
		term bool
	}{
		{"accepted", OutcomeAccepted, true, false},
		{"idempotent", OutcomeExactIdempotent, true, false},
		{"retryable", OutcomeRetryable, false, false},
		{"unknown", OutcomeUnknown, false, false},
		{"terminal", OutcomeTerminalReject, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.in.PermitsAck2(); got != tc.ack {
				t.Fatalf("PermitsAck2: got %v want %v", got, tc.ack)
			}
			if got := tc.in.RequiresAbort(); got != tc.term {
				t.Fatalf("RequiresAbort: got %v want %v", got, tc.term)
			}
		})
	}
}

// --- Orchestration invariants: exercised through in-test fakes. ---

// recordingPreparer is an in-test SourcePreparer that records call order. It is
// a test double for asserting orchestration ordering. It deliberately does not
// perform any unsafe write, payload store, or Arbiter call.
type recordingPreparer struct {
	prepared     PreparedLocalResult
	prepareErr   error
	claimOutcome ClaimOutcome
	claimErr     error
	abortErr     error

	seq          int64
	registerAt   int64
	abortAt      int64
	prepareCount int64
	abortParts   []CandidatePart
}

func (p *recordingPreparer) PrepareLocalStatement(_ context.Context, env StatementEnvelope, _ []byte) (PreparedLocalResult, error) {
	atomic.AddInt64(&p.prepareCount, 1)
	atomic.AddInt64(&p.seq, 1)
	if p.prepareErr != nil {
		return PreparedLocalResult{}, p.prepareErr
	}
	res := p.prepared
	res.StatementID = env.StatementID
	return res, nil
}

func (p *recordingPreparer) RegisterPreparedClaim(_ context.Context, _ string) (ClaimOutcome, error) {
	p.registerAt = atomic.AddInt64(&p.seq, 1)
	return p.claimOutcome, p.claimErr
}

func (p *recordingPreparer) AbortPreparedStatement(_ context.Context, _ string, parts []CandidatePart, _ string) error {
	p.abortAt = atomic.AddInt64(&p.seq, 1)
	p.abortParts = append([]CandidatePart(nil), parts...)
	return p.abortErr
}

type recordingSubmitter struct {
	outcome SubmitOutcome
	err     error
}

func (s *recordingSubmitter) SubmitStatement(_ context.Context, _ StatementEnvelope) (SubmitOutcome, error) {
	return s.outcome, s.err
}

// boundSource returns a complete, exact prepared binding for admissionFixture:
// every payload-identity field matches the envelope EnvelopeFromAdmission would
// build (PayloadRef == PayloadHash, content-addressed), and the source_node
// matches the expected deterministic source. It is the "everything correct"
// baseline the mismatch tests each perturb one field of.
func boundSource() PreparedLocalResult {
	return PreparedLocalResult{
		SourceNode:      "snode-A",
		PayloadRef:      replay.DigestBytes([]byte(fixturePayload)),
		PayloadHash:     replay.DigestBytes([]byte(fixturePayload)),
		PayloadLength:   uint64(len(fixturePayload)),
		PayloadEncoding: PayloadEncodingClickHouseNativeData,
		Revision:        fixtureRevision,
		SourceClaimRoot: "root-1",
		Lifecycle:       LifecycleUnsafeWritten,
	}
}

func TestOrchestrate_RegistersClaimOnlyAfterAcceptedSubmit(t *testing.T) {
	prep := &recordingPreparer{
		prepared:     boundSource(),
		claimOutcome: ClaimOutcome{Category: OutcomeAccepted, BoundSource: "snode-A"},
	}
	sub := &recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeAccepted}}
	orch := NewOrchestrator(sub, prep, OrchestratorConfig{ExpectedSource: "snode-A"})
	res, err := orch.Orchestrate(context.Background(), admissionFixture())
	if err != nil {
		t.Fatalf("Orchestrate: %v", err)
	}
	if !res.Ack2 {
		t.Fatal("expected ACK2 on accepted submit + bound claim")
	}
	if prep.registerAt == 0 {
		t.Fatal("RegisterPreparedClaim was never called")
	}
	if prep.abortAt != 0 {
		t.Fatal("a fully accepted intake must not abort")
	}
}

func TestOrchestrate_TerminalPrepareRejectAbortsWithoutLookupFence(t *testing.T) {
	prep := &recordingPreparer{
		prepared:     boundSource(),
		prepareErr:   fmt.Errorf("schema_hash mismatch: %w", ErrPrepareTerminalReject),
		claimOutcome: ClaimOutcome{Category: OutcomeAccepted, BoundSource: "snode-A"},
	}
	sub := &recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeAccepted}}
	orch := NewOrchestrator(sub, prep, OrchestratorConfig{ExpectedSource: "snode-A"})
	res, err := orch.Orchestrate(context.Background(), admissionFixture())
	if err != nil {
		t.Fatalf("terminal prepare reject must resolve cleanly, got %v", err)
	}
	if res.Ack2 || res.Lifecycle != LifecycleCleaned {
		t.Fatalf("result = %+v, want cleaned non-ACK2", res)
	}
	if atomic.LoadInt64(&prep.abortAt) == 0 {
		t.Fatal("terminal prepare reject must abort the empty candidate set")
	}
	if len(prep.abortParts) != 0 {
		t.Fatalf("terminal prepare reject abort parts = %+v, want exact empty set", prep.abortParts)
	}

	// The source rejected before any unsafe write could happen, so a retry may
	// re-prepare directly after the source's schema view catches up.
	orch.mu.Lock()
	rec := orch.records[admissionFixture().StatementID]
	lookupRequired := rec != nil && rec.requirePreparedLookup
	orch.mu.Unlock()
	if rec == nil {
		t.Fatal("terminal prepare reject record was not retained for idempotent replay")
	}
	if lookupRequired {
		t.Fatal("terminal prepare reject must not leave a prepared-source lookup fence")
	}

	prep.prepareErr = nil
	beforePrepare := atomic.LoadInt64(&prep.prepareCount)
	beforeAbort := atomic.LoadInt64(&prep.abortAt)
	retry, err := orch.Orchestrate(context.Background(), admissionFixture())
	if err != nil || !retry.Ack2 {
		t.Fatalf("retry after source schema catch-up = %+v, %v, want ACK2", retry, err)
	}
	if atomic.LoadInt64(&prep.prepareCount) != beforePrepare+1 {
		t.Fatal("retry after a terminal prepare reject must re-prepare without a lookup fence")
	}
	if atomic.LoadInt64(&prep.abortAt) != beforeAbort {
		t.Fatal("successful retry must not repeat the already-completed empty cleanup")
	}
}

func TestOrchestrate_TerminalPrepareRejectHonorsConcurrentTerminalSubmit(t *testing.T) {
	prep := &recordingPreparer{
		prepareErr: fmt.Errorf("schema_hash mismatch: %w", ErrPrepareTerminalReject),
	}
	sub := &recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeTerminalReject, Reason: "conflicting duplicate"}}
	orch := NewOrchestrator(sub, prep, OrchestratorConfig{ExpectedSource: "snode-A"})
	cleanupProofCalls := 0
	orch.SetBeforeExactCleanup(func(context.Context, IntakeResult) error {
		cleanupProofCalls++
		return errors.New("known-unwritten abort must not require candidate inventory")
	})

	res, err := orch.Orchestrate(context.Background(), admissionFixture())
	if err != nil {
		t.Fatalf("terminal prepare + submit reject: %v", err)
	}
	if res.Ack2 || res.Lifecycle != LifecycleCleaned || !res.IsTerminal() || res.RetainsSourceFrontier() {
		t.Fatalf("result = %+v, want terminal Cleaned", res)
	}
	if res.Submit.Category != OutcomeTerminalReject {
		t.Fatalf("submit category = %v, want terminal reject", res.Submit.Category)
	}
	if len(prep.abortParts) != 0 {
		t.Fatalf("abort parts = %+v, want exact empty set", prep.abortParts)
	}
	if cleanupProofCalls != 0 {
		t.Fatalf("known-unwritten abort ran %d exact-candidate proof hooks", cleanupProofCalls)
	}
	beforePrepare := atomic.LoadInt64(&prep.prepareCount)
	beforeAbort := atomic.LoadInt64(&prep.abortAt)
	second, err := orch.Orchestrate(context.Background(), admissionFixture())
	if err != nil {
		t.Fatalf("terminal replay: %v", err)
	}
	if !second.terminal || second.Lifecycle != LifecycleCleaned {
		t.Fatalf("terminal replay = %+v, want cached Cleaned", second)
	}
	if got := atomic.LoadInt64(&prep.prepareCount); got != beforePrepare {
		t.Fatalf("terminal replay prepare count = %d, want %d", got, beforePrepare)
	}
	if got := atomic.LoadInt64(&prep.abortAt); got != beforeAbort {
		t.Fatalf("terminal replay abort count = %d, want %d", got, beforeAbort)
	}
}

func TestOrchestrate_KnownUnwrittenTerminalAbortResumesAcrossRestart(t *testing.T) {
	journal, err := NewFileIntakeJournal(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileIntakeJournal: %v", err)
	}
	abortErr := errors.New("source abort temporarily unavailable")
	firstPreparer := &recordingPreparer{
		prepareErr: fmt.Errorf("schema_hash mismatch: %w", ErrPrepareTerminalReject),
		abortErr:   abortErr,
	}
	config := OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal}
	first := NewOrchestrator(
		&recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeTerminalReject, Reason: "conflicting duplicate"}},
		firstPreparer,
		config,
	)
	res, err := first.Orchestrate(context.Background(), admissionFixture())
	if !errors.Is(err, abortErr) || res.Lifecycle != LifecycleAbortPending {
		t.Fatalf("first known-unwritten abort=(%+v, %v), want AbortPending source error", res, err)
	}

	secondPreparer := &recordingPreparer{}
	second := NewOrchestrator(&recordingSubmitter{}, secondPreparer, config)
	res, err = second.Orchestrate(context.Background(), admissionFixture())
	if err != nil || res.Lifecycle != LifecycleCleaned || !res.terminal {
		t.Fatalf("restart known-unwritten abort=(%+v, %v), want terminal Cleaned", res, err)
	}
	if got := atomic.LoadInt64(&secondPreparer.prepareCount); got != 0 {
		t.Fatalf("restart prepare calls=%d, want 0", got)
	}
	if got := atomic.LoadInt64(&secondPreparer.abortAt); got != 1 {
		t.Fatalf("restart empty abort calls=%d, want 1", got)
	}
	if len(secondPreparer.abortParts) != 0 {
		t.Fatalf("restart abort parts=%+v, want exact empty set", secondPreparer.abortParts)
	}
	if res.Submit.Category != OutcomeTerminalReject {
		t.Fatalf("restart submit category=%v, want terminal reject", res.Submit.Category)
	}
	thirdPreparer := &recordingPreparer{}
	third := NewOrchestrator(&recordingSubmitter{}, thirdPreparer, config)
	replayed, err := third.Orchestrate(context.Background(), admissionFixture())
	if err != nil || replayed.Lifecycle != LifecycleCleaned || !replayed.IsTerminal() || replayed.RetainsSourceFrontier() {
		t.Fatalf("second-restart cleaned replay=(%+v, %v), want terminal without frontier", replayed, err)
	}
	if atomic.LoadInt64(&thirdPreparer.prepareCount) != 0 || atomic.LoadInt64(&thirdPreparer.abortAt) != 0 {
		t.Fatal("second-restart cleaned replay repeated source work")
	}
}

func TestOrchestrate_CleanupProofFailureRetainsPayloadLeaseUntilTerminal(t *testing.T) {
	prep := newFrontierProbePreparer()
	prep.prepared = boundSource()
	sub := &recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeTerminalReject, Reason: "conflict"}}
	lease := &recordingPayloadLeaseManager{}
	orch := NewOrchestrator(sub, prep, OrchestratorConfig{
		ExpectedSource:      "snode-A",
		PayloadLeaseManager: lease,
	})
	proofErr := errors.New("post-cleanup inventory unavailable")
	proofCalls := 0
	orch.SetAfterExactCleanup(func(context.Context, IntakeResult) error {
		proofCalls++
		if proofCalls == 1 {
			return proofErr
		}
		return nil
	})

	res, err := orch.Orchestrate(context.Background(), admissionFixture())
	if !errors.Is(err, proofErr) || res.Lifecycle != LifecycleAbortPending {
		t.Fatalf("first cleanup=(%+v, %v), want AbortPending proof error", res, err)
	}
	if got := atomic.LoadInt64(&lease.releases); got != 0 {
		t.Fatalf("payload lease releases after failed proof=%d, want 0", got)
	}

	res, err = orch.Orchestrate(context.Background(), admissionFixture())
	if err != nil || res.Lifecycle != LifecycleCleaned || !res.terminal {
		t.Fatalf("proof retry=(%+v, %v), want terminal Cleaned", res, err)
	}
	if got := atomic.LoadInt64(&lease.releases); got != 1 {
		t.Fatalf("payload lease releases after terminal cleanup=%d, want 1", got)
	}
}

func TestOrchestrate_TerminalPrepareRejectRetriesAfterRestartWithoutLookup(t *testing.T) {
	journal, err := NewFileIntakeJournal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	firstPrep := &recordingPreparer{prepareErr: fmt.Errorf("schema_hash mismatch: %w", ErrPrepareTerminalReject)}
	cfg := OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal}
	first := NewOrchestrator(&recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeAccepted}}, firstPrep, cfg)
	res, err := first.Orchestrate(context.Background(), admissionFixture())
	if err != nil || res.Lifecycle != LifecycleCleaned {
		t.Fatalf("first terminal prepare reject = %+v, %v", res, err)
	}

	secondPrep := &recordingPreparer{
		prepared:     boundSource(),
		claimOutcome: ClaimOutcome{Category: OutcomeAccepted, BoundSource: "snode-A"},
	}
	second := NewOrchestrator(&recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeExactIdempotent}}, secondPrep, cfg)
	retry, err := second.Orchestrate(context.Background(), admissionFixture())
	if err != nil || !retry.Ack2 {
		t.Fatalf("post-restart retry = %+v, %v, want ACK2", retry, err)
	}
	if got := atomic.LoadInt64(&secondPrep.prepareCount); got != 1 {
		t.Fatalf("post-restart prepare count = %d, want 1 without PreparedStatementLookup", got)
	}
}

func TestOrchestrate_CleanedPreWriteRejectFencesOtherStatementAfterRestart(t *testing.T) {
	journal, err := NewFileIntakeJournal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cfg := OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal}
	first := NewOrchestrator(
		&recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeAccepted}},
		&recordingPreparer{prepareErr: fmt.Errorf("schema_hash mismatch: %w", ErrPrepareTerminalReject)},
		cfg,
	)
	if _, err := first.Orchestrate(context.Background(), admissionFixture()); err != nil {
		t.Fatalf("first terminal prepare reject: %v", err)
	}

	secondPrep := newFrontierProbePreparer()
	secondPrep.prepared = boundSource()
	second := NewOrchestrator(&recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeAccepted}}, secondPrep, cfg)
	admA := admissionFixture()
	admB := admissionFixture()
	admB.StatementID = fixtureStatementID(2)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	bDone := make(chan error, 1)
	go func() {
		_, err := second.Orchestrate(ctx, admB)
		bDone <- err
	}()
	time.Sleep(30 * time.Millisecond)
	if got := secondPrep.prepareCountFor(admB.StatementID); got != 0 {
		t.Fatalf("B prepared %d times before safe-retry A converged; recovered frontier must preserve statement order", got)
	}

	resA, err := second.Orchestrate(ctx, admA)
	if err != nil || !resA.Ack2 {
		t.Fatalf("A retry after restart = %+v, %v, want ACK2", resA, err)
	}
	select {
	case err := <-bDone:
		if err != nil {
			t.Fatalf("B after A converged: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("B remained blocked after safe-retry A reached a terminal boundary")
	}
	if got := secondPrep.prepareCountFor(admB.StatementID); got != 1 {
		t.Fatalf("B prepare count = %d, want 1 after A converged", got)
	}
}

type preWriteRejectFrontierPreparer struct {
	mu        sync.Mutex
	aID       string
	counts    map[string]int
	retryGate chan struct{}
}

func (p *preWriteRejectFrontierPreparer) PrepareLocalStatement(_ context.Context, env StatementEnvelope, _ []byte) (PreparedLocalResult, error) {
	p.mu.Lock()
	p.counts[env.StatementID]++
	count := p.counts[env.StatementID]
	p.mu.Unlock()
	if env.StatementID == p.aID && count == 1 {
		return PreparedLocalResult{}, fmt.Errorf("schema_hash mismatch: %w", ErrPrepareTerminalReject)
	}
	if env.StatementID == p.aID && count == 2 && p.retryGate != nil {
		<-p.retryGate
	}
	res := boundSource()
	res.StatementID = env.StatementID
	return res, nil
}

func (p *preWriteRejectFrontierPreparer) RegisterPreparedClaim(_ context.Context, _ string) (ClaimOutcome, error) {
	return ClaimOutcome{Category: OutcomeAccepted, BoundSource: "snode-A"}, nil
}

func (p *preWriteRejectFrontierPreparer) AbortPreparedStatement(_ context.Context, _ string, parts []CandidatePart, _ string) error {
	if len(parts) != 0 {
		return fmt.Errorf("pre-write reject abort received %d candidate parts", len(parts))
	}
	return nil
}

func (p *preWriteRejectFrontierPreparer) count(statementID string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.counts[statementID]
}

func TestOrchestrate_PreWriteRejectRetainsFrontierUntilRetryConverges(t *testing.T) {
	admA := admissionFixture()
	admB := admissionFixture()
	admB.StatementID = fixtureStatementID(2)
	retryGate := make(chan struct{})
	prep := &preWriteRejectFrontierPreparer{
		aID:       admA.StatementID,
		counts:    map[string]int{},
		retryGate: retryGate,
	}
	orch := NewOrchestrator(
		&recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeAccepted}},
		prep,
		OrchestratorConfig{ExpectedSource: "snode-A"},
	)

	first, err := orch.Orchestrate(context.Background(), admA)
	if err != nil || first.Lifecycle != LifecycleCleaned || first.IsTerminal() || !first.RetainsSourceFrontier() {
		t.Fatalf("first pre-write reject = %+v, %v, want retryable Cleaned", first, err)
	}

	bDone := make(chan error, 1)
	go func() {
		_, err := orch.Orchestrate(context.Background(), admB)
		bDone <- err
	}()
	time.Sleep(30 * time.Millisecond)
	if got := prep.count(admB.StatementID); got != 0 {
		t.Fatalf("B prepared %d times while safe-retry A remained nonterminal", got)
	}

	aDone := make(chan error, 1)
	go func() {
		_, err := orch.Orchestrate(context.Background(), admA)
		aDone <- err
	}()
	deadline := time.Now().Add(2 * time.Second)
	for prep.count(admA.StatementID) < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := prep.count(admA.StatementID); got != 2 {
		t.Fatalf("A retry prepare count = %d, want 2", got)
	}
	if got := prep.count(admB.StatementID); got != 0 {
		t.Fatalf("B prepared %d times while A retry held the frontier", got)
	}
	close(retryGate)
	select {
	case err := <-aDone:
		if err != nil {
			t.Fatalf("A retry: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("A retry did not finish")
	}
	select {
	case err := <-bDone:
		if err != nil {
			t.Fatalf("B after A: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("B did not acquire the frontier after A converged")
	}
	if got := prep.count(admB.StatementID); got != 1 {
		t.Fatalf("B prepare count = %d, want 1", got)
	}
}

func TestAdmissionRequiresPrepare_PostRestartTerminalReplayIsFalse(t *testing.T) {
	journal, err := NewFileIntakeJournal(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileIntakeJournal: %v", err)
	}
	prep := &recordingPreparer{
		prepared:     boundSource(),
		claimOutcome: ClaimOutcome{Category: OutcomeAccepted, BoundSource: "snode-A"},
	}
	sub := &recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeAccepted}}
	adm := admissionFixture()
	first := NewOrchestrator(sub, prep, OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal})
	res, err := first.Orchestrate(context.Background(), adm)
	if err != nil || !res.Ack2 {
		t.Fatalf("first Orchestrate = %+v, %v", res, err)
	}

	restarted := NewOrchestrator(sub, prep, OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal})
	requires, err := restarted.AdmissionRequiresPrepare(context.Background(), adm)
	if err != nil {
		t.Fatalf("AdmissionRequiresPrepare: %v", err)
	}
	if requires {
		t.Fatal("post-restart terminal replay must not be gated as a new prepare")
	}
	replayed, err := restarted.Orchestrate(context.Background(), adm)
	if err != nil || !replayed.IsTerminal() || replayed.RetainsSourceFrontier() {
		t.Fatalf("post-restart terminal replay=%+v, %v, want terminal without frontier", replayed, err)
	}
}

// rcRetryThenAcceptPreparer prepares successfully and returns a retryable RC on
// the first RegisterPreparedClaim and an accepted RC afterwards, so a test can
// drive the SubmitAccepted resume path: first attempt accepts the submit but the
// RC is retryable (no ACK2, submit stays accepted, prepare stays cached); the
// retry resumes straight at the RC gate and must reach ACK2 through the dual
// gate. It never simulates the companion protocol.
type rcRetryThenAcceptPreparer struct {
	prepared     PreparedLocalResult
	registerCnt  int64
	prepareCount int64
}

func (p *rcRetryThenAcceptPreparer) PrepareLocalStatement(_ context.Context, env StatementEnvelope, _ []byte) (PreparedLocalResult, error) {
	atomic.AddInt64(&p.prepareCount, 1)
	res := p.prepared
	res.StatementID = env.StatementID
	return res, nil
}

func (p *rcRetryThenAcceptPreparer) RegisterPreparedClaim(_ context.Context, _ string) (ClaimOutcome, error) {
	if atomic.AddInt64(&p.registerCnt, 1) == 1 {
		return ClaimOutcome{Category: OutcomeRetryable, Reason: "NotLeader"}, nil
	}
	return ClaimOutcome{Category: OutcomeAccepted, BoundSource: "snode-A"}, nil
}

func (p *rcRetryThenAcceptPreparer) AbortPreparedStatement(_ context.Context, _ string, _ []CandidatePart, _ string) error {
	return nil
}

// TestOrchestrate_ResumeFromSubmitAcceptedReachesAck2 pins that a statement
// whose submit was accepted but whose first RC was retryable resumes from the
// SubmitAccepted stage on retry and reaches ACK2 through the dual gate — without
// re-running the unsafe write. It guards the resume path where res.Submit is
// restored from the record's cached accepted submit outcome: the gate must see
// the submit as accepted so a resume is not wrongly denied, and the resumed
// result must carry the true submit category rather than a synthesized one.
func TestOrchestrate_ResumeFromSubmitAcceptedReachesAck2(t *testing.T) {
	prep := &rcRetryThenAcceptPreparer{prepared: boundSource()}
	sub := &recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeAccepted}}
	orch := NewOrchestrator(sub, prep, OrchestratorConfig{ExpectedSource: "snode-A"})
	adm := admissionFixture()

	r1, err := orch.Orchestrate(context.Background(), adm)
	if err != nil {
		t.Fatalf("first Orchestrate: %v", err)
	}
	if r1.Ack2 {
		t.Fatal("a retryable RC must not ACK2 on the first attempt")
	}

	r2, err := orch.Orchestrate(context.Background(), adm)
	if err != nil {
		t.Fatalf("resume Orchestrate: %v", err)
	}
	if !r2.Ack2 {
		t.Fatal("a resume from SubmitAccepted with an accepted RC must reach ACK2")
	}
	if r2.Submit.Category != OutcomeAccepted {
		t.Fatalf("resumed result must report the true submit category; got %v", r2.Submit.Category)
	}
	if got := atomic.LoadInt64(&prep.prepareCount); got != 1 {
		t.Fatalf("prepare ran %d times; the resume must reuse the cached prepare", got)
	}
}

// TestOrchestrate_ExactIdempotentSubmitOutcomePreserved pins that an
// exact-idempotent submit acceptance is carried through to the intake result
// verbatim, not flattened to a fresh OutcomeAccepted. IntakeResult.Submit is the
// authoritative protocol outcome, so a caller must be able to tell an exact
// re-ack from a first-time acceptance. It checks both the fresh accept→ACK2 path
// and a resume from SubmitAccepted, since the resume reads the cached submit.
func TestOrchestrate_ExactIdempotentSubmitOutcomePreserved(t *testing.T) {
	// Fresh path: submit is exact-idempotent, RC accepted → ACK2 in one attempt.
	freshPrep := &recordingPreparer{
		prepared:     boundSource(),
		claimOutcome: ClaimOutcome{Category: OutcomeAccepted, BoundSource: "snode-A"},
	}
	freshSub := &recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeExactIdempotent}}
	fresh := NewOrchestrator(freshSub, freshPrep, OrchestratorConfig{ExpectedSource: "snode-A"})
	rf, err := fresh.Orchestrate(context.Background(), admissionFixture())
	if err != nil {
		t.Fatalf("fresh Orchestrate: %v", err)
	}
	if !rf.Ack2 {
		t.Fatal("exact-idempotent submit + accepted RC must reach ACK2")
	}
	if rf.Submit.Category != OutcomeExactIdempotent {
		t.Fatalf("fresh path must preserve the exact-idempotent submit category; got %v", rf.Submit.Category)
	}

	// Resume path: submit is exact-idempotent, first RC retryable, retry → ACK2.
	// The resume reads the cached submit, which must still be exact-idempotent.
	resumePrep := &rcRetryThenAcceptPreparer{prepared: boundSource()}
	resumeSub := &recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeExactIdempotent}}
	resume := NewOrchestrator(resumeSub, resumePrep, OrchestratorConfig{ExpectedSource: "snode-A"})
	adm := admissionFixture()
	if _, err := resume.Orchestrate(context.Background(), adm); err != nil {
		t.Fatalf("resume first attempt: %v", err)
	}
	rr, err := resume.Orchestrate(context.Background(), adm)
	if err != nil {
		t.Fatalf("resume retry: %v", err)
	}
	if !rr.Ack2 {
		t.Fatal("resume from SubmitAccepted with accepted RC must reach ACK2")
	}
	if rr.Submit.Category != OutcomeExactIdempotent {
		t.Fatalf("resume path must preserve the cached exact-idempotent submit category; got %v", rr.Submit.Category)
	}
}

func TestOrchestrate_NoClaimWhenSubmitTerminalReject(t *testing.T) {
	prep := &recordingPreparer{prepared: boundSource()}
	sub := &recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeTerminalReject, Reason: "conflicting duplicate"}}
	orch := NewOrchestrator(sub, prep, OrchestratorConfig{ExpectedSource: "snode-A"})
	res, err := orch.Orchestrate(context.Background(), admissionFixture())
	if err != nil {
		t.Fatalf("Orchestrate: %v", err)
	}
	if res.Ack2 {
		t.Fatal("terminal reject must not yield ACK2")
	}
	if prep.registerAt != 0 {
		t.Fatal("RegisterPreparedClaim must not run on terminal submit reject")
	}
	if prep.abortAt == 0 {
		t.Fatal("terminal submit reject must abort the prepared statement")
	}
}

func TestOrchestrate_RetryableSubmitDoesNotAbortOrAck(t *testing.T) {
	prep := &recordingPreparer{prepared: boundSource()}
	sub := &recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeRetryable, Reason: "NotLeader"}}
	orch := NewOrchestrator(sub, prep, OrchestratorConfig{ExpectedSource: "snode-A"})
	res, err := orch.Orchestrate(context.Background(), admissionFixture())
	if err != nil {
		t.Fatalf("Orchestrate: %v", err)
	}
	if res.Ack2 {
		t.Fatal("retryable submit must not yield ACK2")
	}
	if prep.registerAt != 0 {
		t.Fatal("retryable submit must not register a claim")
	}
	if prep.abortAt != 0 {
		t.Fatal("retryable submit must not abort candidate parts")
	}
}

func TestOrchestrate_CommittedSourceMismatchFailsClosed(t *testing.T) {
	prepared := boundSource()
	prepared.SourceNode = "snode-A"
	prep := &recordingPreparer{
		prepared:     prepared,
		claimOutcome: ClaimOutcome{Category: OutcomeAccepted, BoundSource: "snode-B"},
	}
	sub := &recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeAccepted}}
	orch := NewOrchestrator(sub, prep, OrchestratorConfig{ExpectedSource: "snode-A"})
	res, err := orch.Orchestrate(context.Background(), admissionFixture())
	if err != nil {
		t.Fatalf("Orchestrate: %v", err)
	}
	if res.Ack2 {
		t.Fatal("committed-source mismatch must fail closed, no ACK2")
	}
	if prep.abortAt == 0 {
		t.Fatal("committed-source mismatch must abort the prepared statement")
	}
}

func TestOrchestrate_PayloadIdentityMismatchFailsClosed(t *testing.T) {
	prepared := boundSource()
	prepared.PayloadHash = replay.DigestString("different payload") // disagrees with admission
	prep := &recordingPreparer{
		prepared:     prepared,
		claimOutcome: ClaimOutcome{Category: OutcomeAccepted, BoundSource: "snode-A"},
	}
	sub := &recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeAccepted}}
	orch := NewOrchestrator(sub, prep, OrchestratorConfig{ExpectedSource: "snode-A"})
	res, err := orch.Orchestrate(context.Background(), admissionFixture())
	if err != nil {
		t.Fatalf("Orchestrate: %v", err)
	}
	if res.Ack2 {
		t.Fatal("payload-identity mismatch between prepare and admission must fail closed")
	}
}

func TestOrchestrate_SameStatementIdDoesNotRePrepare(t *testing.T) {
	prep := &recordingPreparer{
		prepared:     boundSource(),
		claimOutcome: ClaimOutcome{Category: OutcomeAccepted, BoundSource: "snode-A"},
	}
	sub := &recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeAccepted}}
	orch := NewOrchestrator(sub, prep, OrchestratorConfig{ExpectedSource: "snode-A"})
	adm := admissionFixture()
	if _, err := orch.Orchestrate(context.Background(), adm); err != nil {
		t.Fatalf("first Orchestrate: %v", err)
	}
	if _, err := orch.Orchestrate(context.Background(), adm); err != nil {
		t.Fatalf("second Orchestrate: %v", err)
	}
	if got := atomic.LoadInt64(&prep.prepareCount); got != 1 {
		t.Fatalf("PrepareLocalStatement ran %d times; the same statement_id must not re-prepare", got)
	}
}

func TestOrchestrate_PrepareErrorSurfaces(t *testing.T) {
	prep := &recordingPreparer{prepareErr: errors.New("payload store put failed")}
	sub := &recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeAccepted}}
	orch := NewOrchestrator(sub, prep, OrchestratorConfig{ExpectedSource: "snode-A"})
	res, err := orch.Orchestrate(context.Background(), admissionFixture())
	if err == nil && res.Ack2 {
		t.Fatal("a failed prepare must not yield ACK2")
	}
}

// --- Non-skipped local-invariant coverage.
// These tests exercise HouseGate's own coordination and rejection logic and do
// not depend on, or claim, a working companion staged-intake. None assert an
// accepted ACK2 (the one behavior that genuinely needs the companion seam); they
// assert single-prepare, fail-closed binding, and abort-retry semantics that are
// pure HouseGate concerns and must hold today. ---

// TestPreparedConsistencyReject_RequiresCompleteExactBinding pins the fix for
// the empty-tolerant binding gap: every payload-identity field and the source
// must be present and exactly equal; a blank field is a mismatch, not a
// wildcard.
func TestPreparedConsistencyReject_RequiresCompleteExactBinding(t *testing.T) {
	adm := admissionFixture()
	env, err := EnvelopeFromAdmission(adm)
	if err != nil {
		t.Fatalf("EnvelopeFromAdmission: %v", err)
	}
	orch := NewOrchestrator(&recordingSubmitter{}, &recordingPreparer{}, OrchestratorConfig{ExpectedSource: "snode-A"})

	if reason := orch.preparedConsistencyReject(env, boundSource()); reason != "" {
		t.Fatalf("complete exact binding must be accepted, got reject %q", reason)
	}

	cases := []struct {
		name   string
		mutate func(*PreparedLocalResult)
	}{
		{"empty source", func(p *PreparedLocalResult) { p.SourceNode = "" }},
		{"wrong source", func(p *PreparedLocalResult) { p.SourceNode = "snode-B" }},
		{"empty payload ref", func(p *PreparedLocalResult) { p.PayloadRef = "" }},
		{"wrong payload ref", func(p *PreparedLocalResult) { p.PayloadRef = replay.DigestString("different ref") }},
		{"empty payload hash", func(p *PreparedLocalResult) { p.PayloadHash = "" }},
		{"wrong payload hash", func(p *PreparedLocalResult) { p.PayloadHash = replay.DigestString("different payload") }},
		{"zero payload length", func(p *PreparedLocalResult) { p.PayloadLength = 0 }},
		{"wrong payload length", func(p *PreparedLocalResult) { p.PayloadLength = 1 }},
		{"empty payload encoding", func(p *PreparedLocalResult) { p.PayloadEncoding = "" }},
		{"wrong payload encoding", func(p *PreparedLocalResult) { p.PayloadEncoding = "csv" }},
		{"zero revision", func(p *PreparedLocalResult) { p.Revision = 0 }},
		{"wrong revision", func(p *PreparedLocalResult) { p.Revision = fixtureRevision + 1 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			prepared := boundSource()
			tc.mutate(&prepared)
			if reason := orch.preparedConsistencyReject(env, prepared); reason == "" {
				t.Fatal("incomplete or mismatched binding must be rejected")
			}
		})
	}
}

func TestPreparedConsistencyReject_CandidateInventoryBindsTargetTable(t *testing.T) {
	adm := admissionFixture()
	env, err := EnvelopeFromAdmission(adm)
	if err != nil {
		t.Fatalf("EnvelopeFromAdmission: %v", err)
	}
	orch := NewOrchestrator(&recordingSubmitter{}, &recordingPreparer{}, OrchestratorConfig{ExpectedSource: "snode-A"})
	valid := boundSource()
	valid.CandidateParts = []CandidatePart{{TableID: env.TargetTableID, PartitionID: "p_eu", PartName: "eu_1_1_0"}}
	if reason := orch.preparedConsistencyReject(env, valid); reason != "" {
		t.Fatalf("valid exact candidate rejected: %s", reason)
	}
	for _, candidate := range []CandidatePart{
		// This logical id maps to the same legacy physical name as net1.events;
		// exact logical equality must still reject it.
		{TableID: "net1__events", PartitionID: "p_eu", PartName: "eu_1_1_0"},
		{TableID: env.TargetTableID, PartitionID: "", PartName: "eu_1_1_0"},
		{TableID: env.TargetTableID, PartitionID: "p_eu", PartName: ""},
	} {
		prepared := boundSource()
		prepared.CandidateParts = []CandidatePart{candidate}
		if reason := orch.preparedConsistencyReject(env, prepared); reason == "" {
			t.Fatalf("invalid exact candidate accepted: %+v", candidate)
		}
	}
}

func TestEnvelopeFromAdmissionRejectsNonInsertKind(t *testing.T) {
	adm := AdmissionRecord{StatementID: fixtureStatementID(1), Kind: Kind("DELETE"), TableID: "net1.events", SQL: "DELETE FROM events WHERE k=1"}
	if _, err := EnvelopeFromAdmission(adm); err == nil {
		t.Fatal("EnvelopeFromAdmission accepted non-INSERT kind")
	}
}

// blockingPreparer counts prepare calls and blocks until released, so a test can
// have several Orchestrate calls in flight for the same statement id at once. It
// only records call counts; it never simulates the companion protocol.
type blockingPreparer struct {
	release      chan struct{}
	prepareCount int64
	prepared     PreparedLocalResult
}

func (p *blockingPreparer) PrepareLocalStatement(_ context.Context, env StatementEnvelope, _ []byte) (PreparedLocalResult, error) {
	atomic.AddInt64(&p.prepareCount, 1)
	<-p.release
	res := p.prepared
	res.StatementID = env.StatementID
	return res, nil
}

func (p *blockingPreparer) RegisterPreparedClaim(_ context.Context, _ string) (ClaimOutcome, error) {
	return ClaimOutcome{Category: OutcomeRetryable}, nil
}

func (p *blockingPreparer) AbortPreparedStatement(_ context.Context, _ string, _ []CandidatePart, _ string) error {
	return nil
}

// TestOrchestrate_ConcurrentSameStatementPreparesOnce pins the fix for the
// concurrent double-prepare gap: while one intake is in flight for a statement
// id, concurrent callers for the same id must not start a second prepare — they
// wait and observe the same result. The winner is started first and confirmed
// blocked in prepare before the other callers launch, so they provably race the
// in-flight window (not a serial replay). The submit outcome is left retryable
// so no test here claims a green ACK2 intake.
func TestOrchestrate_ConcurrentSameStatementPreparesOnce(t *testing.T) {
	prep := &blockingPreparer{release: make(chan struct{}), prepared: boundSource()}
	sub := &recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeRetryable, Reason: "NotLeader"}}
	orch := NewOrchestrator(sub, prep, OrchestratorConfig{ExpectedSource: "snode-A"})
	adm := admissionFixture()

	const losers = 7
	results := make([]IntakeResult, losers+1)
	var wg sync.WaitGroup

	// Winner: reserves the in-flight slot and blocks inside PrepareLocalStatement.
	wg.Add(1)
	go func() {
		defer wg.Done()
		res, err := orch.Orchestrate(context.Background(), adm)
		if err != nil {
			t.Errorf("winner Orchestrate: %v", err)
		}
		results[0] = res
	}()
	waitForPrepareStarted(t, &prep.prepareCount)

	// Losers: all launch while the winner holds the reservation, so each must
	// find the in-flight entry and park on it rather than prepare again. Each
	// signals arrival before calling Orchestrate; we release only once all have
	// arrived AND the winner is still blocked in prepare, so every loser is
	// guaranteed to observe the in-flight entry (it cannot be cleared until
	// release). This removes any dependence on goroutine scheduling timing.
	var arrived int64
	wg.Add(losers)
	for i := 0; i < losers; i++ {
		go func(i int) {
			defer wg.Done()
			atomic.AddInt64(&arrived, 1)
			res, err := orch.Orchestrate(context.Background(), adm)
			if err != nil {
				t.Errorf("loser Orchestrate: %v", err)
			}
			results[i+1] = res
		}(i)
	}
	waitForCount(t, &arrived, losers)
	// The winner is still blocked in prepare (release not yet closed), so the
	// in-flight entry is still present; give the arrived losers a moment to
	// acquire the lock and park on it, then release.
	time.Sleep(20 * time.Millisecond)

	close(prep.release)
	wg.Wait()

	if got := atomic.LoadInt64(&prep.prepareCount); got != 1 {
		t.Fatalf("PrepareLocalStatement ran %d times; a concurrent same-statement caller must not start a second prepare", got)
	}
	for i := range results {
		if results[i].StatementID != adm.StatementID {
			t.Fatalf("caller %d observed statement id %q, want %q", i, results[i].StatementID, adm.StatementID)
		}
	}
}

func waitForPrepareStarted(t *testing.T, counter *int64) {
	t.Helper()
	waitForCount(t, counter, 1)
}

func waitForCount(t *testing.T, counter *int64, want int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt64(counter) >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("counter never reached %d (last=%d)", want, atomic.LoadInt64(counter))
}

// abortOncePreparer fails the first AbortPreparedStatement and succeeds after,
// so a test can prove a failed abort is retried rather than recorded as done. It
// also counts prepares to prove the abort retry does not re-run the unsafe write.
type abortOncePreparer struct {
	prepared     PreparedLocalResult
	abortCalls   int64
	failUntil    int64
	prepareCount int64
}

func (p *abortOncePreparer) PrepareLocalStatement(_ context.Context, env StatementEnvelope, _ []byte) (PreparedLocalResult, error) {
	atomic.AddInt64(&p.prepareCount, 1)
	res := p.prepared
	res.StatementID = env.StatementID
	return res, nil
}

func (p *abortOncePreparer) RegisterPreparedClaim(_ context.Context, _ string) (ClaimOutcome, error) {
	return ClaimOutcome{}, nil
}

func (p *abortOncePreparer) AbortPreparedStatement(_ context.Context, _ string, _ []CandidatePart, _ string) error {
	n := atomic.AddInt64(&p.abortCalls, 1)
	if n <= p.failUntil {
		return errors.New("DROP PART transient failure")
	}
	return nil
}

// TestOrchestrate_FailedAbortIsRetriedNotRecorded pins the fix for the
// failed-abort gap: a terminal reject whose cleanup fails must surface an error
// and must NOT be recorded as done, so a later call retries the abort. This
// drives a terminal-reject submit (no ACK2), so it makes no green-intake claim.
func TestOrchestrate_FailedAbortIsRetriedNotRecorded(t *testing.T) {
	prep := &abortOncePreparer{prepared: boundSource(), failUntil: 1}
	sub := &recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeTerminalReject, Reason: "conflicting duplicate"}}
	orch := NewOrchestrator(sub, prep, OrchestratorConfig{ExpectedSource: "snode-A"})
	adm := admissionFixture()

	res, err := orch.Orchestrate(context.Background(), adm)
	if err == nil {
		t.Fatal("a failed abort must surface an error")
	}
	if res.Ack2 {
		t.Fatal("terminal reject must never ACK2")
	}
	if res.Lifecycle == LifecycleCleaned {
		t.Fatal("a failed abort must not report Cleaned")
	}

	// The failed abort was not recorded as done, so a retry re-runs cleanup.
	res2, err2 := orch.Orchestrate(context.Background(), adm)
	if err2 != nil {
		t.Fatalf("retry after abort recovery: %v", err2)
	}
	if res2.Lifecycle != LifecycleCleaned {
		t.Fatalf("retry must complete cleanup, got lifecycle %q", res2.Lifecycle)
	}
	if got := atomic.LoadInt64(&prep.abortCalls); got != 2 {
		t.Fatalf("abort called %d times; the failed abort must be retried exactly once more", got)
	}
	if got := atomic.LoadInt64(&prep.prepareCount); got != 1 {
		t.Fatalf("prepare ran %d times; an abort retry must reuse the prepared record, not re-run the unsafe write", got)
	}
}

// retryablePreparer prepares successfully (counting prepares) and registers a
// retryable claim; the submitter is configured separately. It lets a test prove
// a retry reuses the cached prepare. It never simulates the companion protocol.
type retryablePreparer struct {
	prepared     PreparedLocalResult
	prepareCount int64
	registerCnt  int64
	abortCnt     int64
	claimOutcome ClaimOutcome
}

func (p *retryablePreparer) PrepareLocalStatement(_ context.Context, env StatementEnvelope, _ []byte) (PreparedLocalResult, error) {
	atomic.AddInt64(&p.prepareCount, 1)
	res := p.prepared
	res.StatementID = env.StatementID
	return res, nil
}

func (p *retryablePreparer) RegisterPreparedClaim(_ context.Context, _ string) (ClaimOutcome, error) {
	atomic.AddInt64(&p.registerCnt, 1)
	return p.claimOutcome, nil
}

func (p *retryablePreparer) AbortPreparedStatement(_ context.Context, _ string, _ []CandidatePart, _ string) error {
	atomic.AddInt64(&p.abortCnt, 1)
	return nil
}

// TestOrchestrate_ReusesPreparedOnRetry pins the fix for the retry-re-prepare
// gap: after a non-terminal (retryable) submit, a later call must reuse the
// cached prepared record and NOT run PrepareLocalStatement (the unsafe write)
// again. It stays on the retryable path, so it claims no green ACK2 intake.
func TestOrchestrate_ReusesPreparedOnRetry(t *testing.T) {
	prep := &retryablePreparer{prepared: boundSource()}
	sub := &recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeRetryable, Reason: "NotLeader"}}
	orch := NewOrchestrator(sub, prep, OrchestratorConfig{ExpectedSource: "snode-A"})
	adm := admissionFixture()

	r1, err := orch.Orchestrate(context.Background(), adm)
	if err != nil {
		t.Fatalf("first Orchestrate: %v", err)
	}
	r2, err := orch.Orchestrate(context.Background(), adm)
	if err != nil {
		t.Fatalf("retry Orchestrate: %v", err)
	}
	if r1.Ack2 || r2.Ack2 {
		t.Fatal("retryable submit must never yield ACK2")
	}
	if got := atomic.LoadInt64(&prep.prepareCount); got != 1 {
		t.Fatalf("prepare ran %d times; a retry after a retryable submit must reuse the prepared record", got)
	}
	if got := atomic.LoadInt64(&prep.registerCnt); got != 0 {
		t.Fatalf("register ran %d times; a retryable (unaccepted) submit must not register a claim", got)
	}
	if got := atomic.LoadInt64(&prep.abortCnt); got != 0 {
		t.Fatal("a retryable submit must not abort candidate parts")
	}
}

// envRecordingSubmitter records the last envelope it was asked to submit, so a
// test can prove a resume submits the ORIGINAL statement, not a mismatched one.
type envRecordingSubmitter struct {
	mu       sync.Mutex
	outcome  SubmitOutcome
	lastEnv  StatementEnvelope
	submits  int64
	seenSQLs []string
}

func (s *envRecordingSubmitter) SubmitStatement(_ context.Context, env StatementEnvelope) (SubmitOutcome, error) {
	s.mu.Lock()
	s.lastEnv = env
	s.submits++
	s.seenSQLs = append(s.seenSQLs, env.SQL)
	s.mu.Unlock()
	return s.outcome, nil
}

// TestOrchestrate_StatementIdReuseWithDifferentEnvelopeRejected pins the fix for
// the envelope-binding gap: once a statement id has a cached prepared unsafe
// write, a later call that reuses the id with a different envelope
// (SQL/target/kind/signer/JWS or payload bytes) must be rejected fail closed —
// it must not submit or register the new envelope against the original prepared
// write. A same-envelope retry still resumes, reusing the original.
func TestOrchestrate_StatementIdReuseWithDifferentEnvelopeRejected(t *testing.T) {
	// First attempt: prepare succeeds, submit is retryable, so the id is now
	// bound to the original envelope with a cached prepare.
	prep := &retryablePreparer{prepared: boundSource()}
	sub := &envRecordingSubmitter{outcome: SubmitOutcome{Category: OutcomeRetryable, Reason: "NotLeader"}}
	orch := NewOrchestrator(sub, prep, OrchestratorConfig{ExpectedSource: "snode-A"})
	orig := admissionFixture()
	if _, err := orch.Orchestrate(context.Background(), orig); err != nil {
		t.Fatalf("first attempt: %v", err)
	}

	// Each mutation reuses the same statement id but changes one signed/bound
	// field. Every one must be rejected.
	mutators := []struct {
		name   string
		mutate func(*AdmissionRecord)
	}{
		{"different SQL", func(a *AdmissionRecord) { a.SQL = "INSERT INTO events /* evil */ FORMAT Native" }},
		{"different target", func(a *AdmissionRecord) { a.TableID = "net1.other" }},
		{"different signer", func(a *AdmissionRecord) { a.Signer = "0xdifferent" }},
		{"different JWS", func(a *AdmissionRecord) { a.UserJWS = "jws-2" }},
		{"different payload bytes", func(a *AdmissionRecord) { a.Payload = []byte("different-bytes-x") }},
	}
	for _, m := range mutators {
		t.Run(m.name, func(t *testing.T) {
			adm := admissionFixture()
			m.mutate(&adm)
			res, err := orch.Orchestrate(context.Background(), adm)
			if err == nil {
				t.Fatalf("reusing statement id %s with a %s must be rejected", adm.StatementID, m.name)
			}
			if res.Ack2 {
				t.Fatal("a mismatched-envelope reuse must never ACK2")
			}
		})
	}

	// The preparer never re-ran the unsafe write, and the submitter only ever
	// saw the original SQL (mismatched envelopes were rejected before submit).
	if got := atomic.LoadInt64(&prep.prepareCount); got != 1 {
		t.Fatalf("prepare ran %d times; a mismatched reuse must not prepare, and a rejected one must not either", got)
	}
	sub.mu.Lock()
	seen := append([]string(nil), sub.seenSQLs...)
	sub.mu.Unlock()
	for _, sql := range seen {
		if sql != orig.SQL {
			t.Fatalf("submitter saw SQL %q; only the original bound envelope may be submitted", sql)
		}
	}

	// A same-envelope retry is still accepted and resumes (reuses the prepare).
	if _, err := orch.Orchestrate(context.Background(), admissionFixture()); err != nil {
		t.Fatalf("same-envelope retry must be accepted: %v", err)
	}
	if got := atomic.LoadInt64(&prep.prepareCount); got != 1 {
		t.Fatalf("prepare ran %d times; a same-envelope retry must reuse the cached prepare", got)
	}
}

// frontierProbePreparer records, per statement id, how many prepares ran and can
// block the first prepare so a test can hold the frontier open.
type frontierProbePreparer struct {
	mu       sync.Mutex
	prepares map[string]int64
	block    map[string]chan struct{} // statement id -> gate the prepare waits on
	prepared PreparedLocalResult
}

func newFrontierProbePreparer() *frontierProbePreparer {
	return &frontierProbePreparer{prepares: map[string]int64{}, block: map[string]chan struct{}{}}
}

func (p *frontierProbePreparer) prepareCountFor(id string) int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.prepares[id]
}

func (p *frontierProbePreparer) PrepareLocalStatement(_ context.Context, env StatementEnvelope, _ []byte) (PreparedLocalResult, error) {
	p.mu.Lock()
	p.prepares[env.StatementID]++
	gate := p.block[env.StatementID]
	p.mu.Unlock()
	if gate != nil {
		<-gate
	}
	res := p.prepared
	res.StatementID = env.StatementID
	return res, nil
}

func (p *frontierProbePreparer) RegisterPreparedClaim(_ context.Context, _ string) (ClaimOutcome, error) {
	return ClaimOutcome{Category: OutcomeAccepted, BoundSource: "snode-A"}, nil
}

func (p *frontierProbePreparer) AbortPreparedStatement(_ context.Context, _ string, _ []CandidatePart, _ string) error {
	return nil
}

// TestOrchestrate_DifferentStatementBlockedOnFrontier pins the fix for the
// missing serial source frontier: while one statement's intake holds the source
// frontier (not yet terminal), a different statement on the same source must not
// start its own prepare/source-write. It must proceed only after the first
// reaches a terminal stage. The blocked caller never asserts a green ACK2.
func TestOrchestrate_DifferentStatementBlockedOnFrontier(t *testing.T) {
	prep := newFrontierProbePreparer()
	prep.prepared = boundSource()
	gate := make(chan struct{})

	sub := &recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeAccepted}}
	orch := NewOrchestrator(sub, prep, OrchestratorConfig{ExpectedSource: "snode-A"})

	adm1 := admissionFixture()
	adm2 := admissionFixture()
	adm2.StatementID = fixtureStatementID(2)
	prep.block[adm1.StatementID] = gate

	// adm1 acquires the frontier and blocks inside prepare.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if _, err := orch.Orchestrate(context.Background(), adm1); err != nil {
			t.Errorf("adm1 Orchestrate: %v", err)
		}
	}()
	waitForMapCount(t, prep, adm1.StatementID, 1)

	// adm2 tries to start while adm1 holds the frontier; it must block, i.e.
	// never call prepare for adm2.
	q2started := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		close(q2started)
		if _, err := orch.Orchestrate(context.Background(), adm2); err != nil {
			t.Errorf("adm2 Orchestrate: %v", err)
		}
	}()
	<-q2started
	time.Sleep(30 * time.Millisecond) // give adm2 time to (not) start prepare
	if got := prep.prepareCountFor(adm2.StatementID); got != 0 {
		t.Fatalf("adm2 prepared %d times while adm1 held the frontier; the source write must be serialized", got)
	}

	// Release adm1: it reaches ACK2 (terminal), frees the frontier, and adm2 may
	// now prepare.
	close(gate)
	wg.Wait()
	if got := prep.prepareCountFor(adm2.StatementID); got != 1 {
		t.Fatalf("after adm1 became terminal, adm2 must prepare exactly once, got %d", got)
	}
}

func TestOrchestrate_CleanupProofFailureFencesQueuedPrepareUntilSameIDRetry(t *testing.T) {
	prep := newFrontierProbePreparer()
	prep.prepared = boundSource()
	sub := &recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeTerminalReject, Reason: "conflict"}}
	baseJournal, err := NewFileIntakeJournal(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileIntakeJournal: %v", err)
	}
	saveErr := errors.New("terminal journal save failed")
	journal := &failFirstTerminalSaveJournal{base: baseJournal, err: saveErr}
	orch := NewOrchestrator(sub, prep, OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal})
	proofErr := errors.New("inventory proof unavailable")
	var hookMu sync.Mutex
	hookCalls := 0
	orch.SetAfterExactCleanup(func(context.Context, IntakeResult) error {
		hookMu.Lock()
		defer hookMu.Unlock()
		hookCalls++
		if hookCalls == 1 {
			return proofErr
		}
		return nil
	})

	admA := admissionFixture()
	admB := admissionFixture()
	admB.StatementID = fixtureStatementID(2)
	res, err := orch.Orchestrate(context.Background(), admA)
	if !errors.Is(err, proofErr) || res.Lifecycle != LifecycleAbortPending {
		t.Fatalf("A first attempt = (%+v, %v), want AbortPending proof error", res, err)
	}

	bDone := make(chan error, 1)
	go func() {
		_, err := orch.Orchestrate(context.Background(), admB)
		bDone <- err
	}()
	time.Sleep(30 * time.Millisecond)
	if got := prep.prepareCountFor(admB.StatementID); got != 0 {
		t.Fatalf("B prepared %d times while A cleanup proof was pending", got)
	}

	res, err = orch.Orchestrate(context.Background(), admA)
	if !errors.Is(err, saveErr) || res.Lifecycle != LifecycleCleaned {
		t.Fatalf("A proof retry = (%+v, %v), want Cleaned terminal-save error", res, err)
	}
	time.Sleep(30 * time.Millisecond)
	if got := prep.prepareCountFor(admB.StatementID); got != 0 {
		t.Fatalf("B prepared %d times while A terminal journal was pending", got)
	}
	res, err = orch.Orchestrate(context.Background(), admA)
	if err != nil || res.Lifecycle != LifecycleCleaned {
		t.Fatalf("A terminal-save retry = (%+v, %v), want Cleaned", res, err)
	}
	select {
	case err := <-bDone:
		if err != nil {
			t.Fatalf("B after proof retry: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("B remained fenced after A proof retry")
	}
	if got := prep.prepareCountFor(admB.StatementID); got != 1 {
		t.Fatalf("B prepare count after proof retry = %d, want 1", got)
	}
}

func TestOrchestrate_PreCleanupProofFailureFencesAlreadyQueuedPrepare(t *testing.T) {
	prep := newFrontierProbePreparer()
	prep.prepared = boundSource()
	sub := &recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeTerminalReject, Reason: "conflict"}}
	orch := NewOrchestrator(sub, prep, OrchestratorConfig{ExpectedSource: "snode-A"})
	proofErr := errors.New("pre-cleanup inventory unavailable")
	proofCalls := 0
	orch.SetBeforeExactCleanup(func(context.Context, IntakeResult) error {
		proofCalls++
		if proofCalls == 1 {
			return proofErr
		}
		return nil
	})

	admA := admissionFixture()
	admB := admissionFixture()
	admB.StatementID = fixtureStatementID(2)
	res, err := orch.Orchestrate(context.Background(), admA)
	if !errors.Is(err, proofErr) || res.Lifecycle != LifecycleAbortPending {
		t.Fatalf("A first attempt=(%+v, %v), want AbortPending proof error", res, err)
	}
	bDone := make(chan error, 1)
	go func() {
		_, err := orch.Orchestrate(context.Background(), admB)
		bDone <- err
	}()
	time.Sleep(30 * time.Millisecond)
	if got := prep.prepareCountFor(admB.StatementID); got != 0 {
		t.Fatalf("B prepared %d times while A pre-cleanup proof was pending", got)
	}
	if res, err = orch.Orchestrate(context.Background(), admA); err != nil || res.Lifecycle != LifecycleCleaned {
		t.Fatalf("A proof retry=(%+v, %v), want Cleaned", res, err)
	}
	select {
	case err := <-bDone:
		if err != nil {
			t.Fatalf("B after proof retry: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("B remained fenced after A proof retry")
	}
	if got := prep.prepareCountFor(admB.StatementID); got != 1 {
		t.Fatalf("B prepare count=%d want 1", got)
	}
}

// TestOrchestrate_SelfRetryReentersFrontier proves a statement's own retry can
// pass through the frontier it already holds (no self-deadlock) while a stalled
// (retryable) prior outcome keeps the frontier held against other statements.
func TestOrchestrate_SelfRetryReentersFrontier(t *testing.T) {
	prep := &retryablePreparer{prepared: boundSource()}
	sub := &recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeRetryable, Reason: "NotLeader"}}
	orch := NewOrchestrator(sub, prep, OrchestratorConfig{ExpectedSource: "snode-A"})
	adm := admissionFixture()

	// First attempt: retryable submit; frontier stays held by this statement.
	if _, err := orch.Orchestrate(context.Background(), adm); err != nil {
		t.Fatalf("first: %v", err)
	}
	// Same statement retries: must re-enter its own frontier without blocking.
	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := orch.Orchestrate(context.Background(), adm); err != nil {
			t.Errorf("self-retry: %v", err)
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("self-retry deadlocked on the frontier it already holds")
	}
}

// mismatchingPreparer returns a prepared result whose StatementID does not match
// the submitted statement, while payload/source fields are otherwise correct, to
// prove the orchestrator rejects a stale/wrong-statement prepared response.
type mismatchingPreparer struct {
	preparedID  string
	prepared    PreparedLocalResult
	registerCnt int64
	abortCnt    int64
}

func (p *mismatchingPreparer) PrepareLocalStatement(_ context.Context, _ StatementEnvelope, _ []byte) (PreparedLocalResult, error) {
	res := p.prepared
	res.StatementID = p.preparedID // deliberately NOT env.StatementID
	return res, nil
}

func (p *mismatchingPreparer) RegisterPreparedClaim(_ context.Context, _ string) (ClaimOutcome, error) {
	atomic.AddInt64(&p.registerCnt, 1)
	return ClaimOutcome{Category: OutcomeAccepted, BoundSource: "snode-A"}, nil
}

func (p *mismatchingPreparer) AbortPreparedStatement(_ context.Context, _ string, _ []CandidatePart, _ string) error {
	atomic.AddInt64(&p.abortCnt, 1)
	return nil
}

// TestOrchestrate_PreparedStatementIdMismatchRejected pins the fix for the
// unverified prepared statement id: even with an accepted submit and matching
// payload/source, a prepared result whose statement id is blank or belongs to
// another statement must fail closed before the RC gate — no register, no ACK2,
// and the (wrongly) prepared candidate is aborted.
func TestOrchestrate_PreparedStatementIdMismatchRejected(t *testing.T) {
	cases := []struct {
		name       string
		preparedID string
	}{
		{"blank prepared id", ""},
		{"wrong prepared id", "other-stmt"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			prep := &mismatchingPreparer{preparedID: tc.preparedID, prepared: boundSource()}
			sub := &recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeAccepted}}
			orch := NewOrchestrator(sub, prep, OrchestratorConfig{ExpectedSource: "snode-A"})

			res, err := orch.Orchestrate(context.Background(), admissionFixture())
			if err != nil {
				t.Fatalf("Orchestrate: %v", err)
			}
			if res.Ack2 {
				t.Fatal("a prepared statement id mismatch must fail closed, no ACK2")
			}
			if got := atomic.LoadInt64(&prep.registerCnt); got != 0 {
				t.Fatalf("register ran %d times; the mismatch must be caught before the RC gate", got)
			}
			if got := atomic.LoadInt64(&prep.abortCnt); got == 0 {
				t.Fatal("the wrongly-prepared candidate must be aborted")
			}
			if res.Lifecycle != LifecycleCleaned {
				t.Fatalf("expected Cleaned after successful abort, got %q", res.Lifecycle)
			}
		})
	}
}

// TestOrchestrate_FrontierWaiterCancelDoesNotStrand proves that when a caller
// blocked on the frontier cancels its context, it exits cleanly and does not
// strand the frontier: a later statement can still acquire it once the holder
// becomes terminal.
func TestOrchestrate_FrontierWaiterCancelDoesNotStrand(t *testing.T) {
	prep := newFrontierProbePreparer()
	prep.prepared = boundSource()
	gate := make(chan struct{})

	sub := &recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeAccepted}}
	orch := NewOrchestrator(sub, prep, OrchestratorConfig{ExpectedSource: "snode-A"})

	adm1 := admissionFixture()
	adm2 := admissionFixture()
	adm2.StatementID = fixtureStatementID(2)
	adm3 := admissionFixture()
	adm3.StatementID = fixtureStatementID(3)
	prep.block[adm1.StatementID] = gate

	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); orch.Orchestrate(context.Background(), adm1) }()
	waitForMapCount(t, prep, adm1.StatementID, 1)

	// adm2 blocks on the frontier, then cancels — it must return ctx.Err and
	// not leave the frontier stranded.
	ctx2, cancel2 := context.WithCancel(context.Background())
	q2done := make(chan error, 1)
	go func() { _, err := orch.Orchestrate(ctx2, adm2); q2done <- err }()
	time.Sleep(20 * time.Millisecond)
	cancel2()
	select {
	case err := <-q2done:
		if err == nil {
			t.Fatal("cancelled adm2 must return an error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled adm2 did not return")
	}

	// Release adm1 (reaches terminal ACK2, frees the frontier). adm3 must then
	// be able to prepare — proving the cancelled waiter did not strand the gate.
	close(gate)
	wg.Wait()
	if _, err := orch.Orchestrate(context.Background(), adm3); err != nil {
		t.Fatalf("adm3 after cancel + release: %v", err)
	}
	if got := prep.prepareCountFor(adm3.StatementID); got != 1 {
		t.Fatalf("adm3 must acquire the frontier and prepare once, got %d", got)
	}
}

func waitForMapCount(t *testing.T, p *frontierProbePreparer, id string, want int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if p.prepareCountFor(id) >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("prepare for %s never reached %d", id, want)
}

func TestStatementKindCode(t *testing.T) {
	code, err := StatementKindCode(KindInsert)
	if err != nil {
		t.Fatalf("StatementKindCode(KindInsert): %v", err)
	}
	if code != StatementKindCodeInsert || code != 1 {
		t.Fatalf("StatementKindCode(KindInsert) = %d, want 1", code)
	}
	if _, err := StatementKindCode(Kind("")); err == nil {
		t.Fatal("an empty kind must not resolve to a signed code")
	}
	if _, err := StatementKindCode(Kind("UPDATE")); err == nil {
		t.Fatal("an unmodelled kind must fail closed rather than sign 0")
	}
}
