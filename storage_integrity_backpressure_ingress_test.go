package housegate

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/lthash"
	siplugin "github.com/housegate/housegate/pkg/plugins/storageintegrity"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

type fakePartsPressure struct {
	mu          sync.Mutex
	refuse      map[string]error
	allowCalls  []string
	invalidated int
	committed   int
	released    int
}

func (f *fakePartsPressure) Reserve(_ context.Context, table string, partitionIDs []string) (sicore.PartsReservation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, partitionID := range partitionIDs {
		key := table + "/" + partitionID
		f.allowCalls = append(f.allowCalls, key)
		if err := f.refuse[key]; err != nil {
			return nil, err
		}
	}
	return &fakePartsReservation{pressure: f}, nil
}

func (f *fakePartsPressure) Invalidate() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.invalidated++
}

type fakePartsReservation struct {
	pressure *fakePartsPressure
	mu       sync.Mutex
	state    string
}

func (r *fakePartsReservation) Commit() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state != "" {
		return
	}
	r.pressure.mu.Lock()
	r.pressure.committed++
	r.pressure.mu.Unlock()
	r.state = "committed"
}

func (r *fakePartsReservation) Release() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state == "released" || r.state == "finalized" {
		return
	}
	r.pressure.mu.Lock()
	r.pressure.released++
	r.pressure.mu.Unlock()
	r.state = "released"
}

func (r *fakePartsReservation) Finalize() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state == "released" || r.state == "finalized" {
		return
	}
	if r.state == "" {
		r.pressure.mu.Lock()
		r.pressure.committed++
		r.pressure.mu.Unlock()
	}
	r.state = "finalized"
}

func bpSchemas() []payloadexec.TableSchema {
	return []payloadexec.TableSchema{{
		TableID: "net1.events", PartitionBy: "region",
		Columns: []lthash.Column{{Name: "id", Type: "UInt64"}, {Name: "region", Type: "String"}},
	}}
}

func bpSchemaResolver() sicore.TableSchemaResolver {
	schema := bpSchemas()[0]
	return sicore.TableSchemaResolverFunc(func(tableID string) (payloadexec.TableSchema, bool) {
		if tableID != schema.TableID {
			return payloadexec.TableSchema{}, false
		}
		return schema, true
	})
}

func bpAdmission() siplugin.Admission {
	sql := "INSERT INTO events FORMAT CSVWithNames"
	payload := []byte("id,region\n1,eu\n2,us\n3,eu\n")
	return siplugin.Admission{
		StatementID: "0xabc:1:n1", Kind: siplugin.KindInsert, TableID: "net1.events",
		SQL: sql, SQLHash: replay.DigestString(sql), Signer: "0xabc", UserJWS: "jws",
		Payload: siplugin.CapturedPayload{Bytes: payload, Length: uint64(len(payload)), Encoding: sicore.EncodingCSVWithNames, Revision: 54465, Complete: true},
	}
}

func newBackpressureIngress(t *testing.T, pressure StorageIntegrityPartsPressure) (*StorageIntegrityIngress, *rootRecordingPayloadWriter, *rootRecordingSubmitter, *rootRecordingPreparer) {
	t.Helper()
	writer := &rootRecordingPayloadWriter{result: sicore.PayloadPutResult{PayloadRef: "payload://store/ref-1", State: sicore.PayloadStateAvailable}}
	submitter := &rootRecordingSubmitter{outcome: sicore.SubmitOutcome{Category: sicore.OutcomeAccepted}}
	preparer := &rootRecordingPreparer{source: "snode-A", claim: sicore.ClaimOutcome{Category: sicore.OutcomeAccepted, BoundSource: "snode-A"}}
	orch := sicore.NewOrchestrator(submitter, preparer, sicore.OrchestratorConfig{ExpectedSource: "snode-A"})
	ingress, err := NewStorageIntegrityIngressWithPayloadWriter(orch, nil, sicore.MaterializerCSV, writer)
	if err != nil {
		t.Fatalf("NewStorageIntegrityIngressWithPayloadWriter: %v", err)
	}
	ingress.WithPartsPressure(pressure, bpSchemaResolver())
	return ingress, writer, submitter, preparer
}

func TestIngress_BackpressureRefusesWithException252BeforePayloadPut(t *testing.T) {
	pressure := &fakePartsPressure{refuse: map[string]error{
		"net1__events/p_us": &sicore.BackpressureError{Database: "hg_unsafe", Table: "net1__events", Partition: "p_us", Parts: 2400, Limit: 2400, Kind: "soft"},
	}}
	ingress, writer, submitter, preparer := newBackpressureIngress(t, pressure)

	before := testutil.ToFloat64(storageIntegrityBackpressureTotal.WithLabelValues("net1__events"))
	err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), bpAdmission())
	var clientErr *chproto.ClientError
	if !errors.As(err, &clientErr) || clientErr.Code != chproto.CodeTooManyParts {
		t.Fatalf("err = %v, want ClientError 252", err)
	}
	if !strings.HasPrefix(clientErr.Message, "storage_integrity: back-pressure") || !strings.Contains(clientErr.Message, "p_us") {
		t.Fatalf("client message = %q", clientErr.Message)
	}
	if !errors.Is(err, sicore.ErrBackpressure) {
		t.Fatal("must unwrap to ErrBackpressure")
	}
	if writer.calls != 0 || submitter.calls != 0 || preparer.prepareCalls != 0 {
		t.Fatalf("writer/submit/prepare calls = %d/%d/%d, want 0/0/0", writer.calls, submitter.calls, preparer.prepareCalls)
	}
	if got := strings.Join(pressure.allowCalls, ","); got != "net1__events/p_eu,net1__events/p_us" {
		t.Fatalf("allow calls = %q", got)
	}
	if got := testutil.ToFloat64(storageIntegrityBackpressureTotal.WithLabelValues("net1__events")); got != before+1 {
		t.Fatalf("backpressure counter = %v, want %v", got, before+1)
	}
}

func TestIngress_PressureUnavailableFailsClosedWithException252(t *testing.T) {
	cause := errors.New("snapshot expired")
	pressure := &fakePartsPressure{refuse: map[string]error{
		"net1__events/p_eu": cause,
	}}
	ingress, writer, submitter, preparer := newBackpressureIngress(t, pressure)
	err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), bpAdmission())
	var clientErr *chproto.ClientError
	if !errors.As(err, &clientErr) || clientErr.Code != chproto.CodeTooManyParts || !errors.Is(err, sicore.ErrBackpressure) {
		t.Fatalf("err = %v, want unavailable ClientError 252 wrapping ErrBackpressure", err)
	}
	if !errors.Is(err, cause) {
		t.Fatalf("err = %v, want original pressure cause", err)
	}
	if writer.calls != 0 || submitter.calls != 0 || preparer.prepareCalls != 0 {
		t.Fatalf("unavailable pressure writer/submit/prepare = %d/%d/%d want 0/0/0", writer.calls, submitter.calls, preparer.prepareCalls)
	}
}

type countingIntakeJournal struct {
	mu      sync.Mutex
	records map[string]sicore.IntakeJournalRecord
	saves   int
}

func (j *countingIntakeJournal) LoadIntakeRecord(_ context.Context, statementID string) (sicore.IntakeJournalRecord, bool, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	rec, ok := j.records[statementID]
	return rec, ok, nil
}

func (j *countingIntakeJournal) ListIntakeRecords(context.Context) ([]sicore.IntakeJournalRecord, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	result := make([]sicore.IntakeJournalRecord, 0, len(j.records))
	for _, rec := range j.records {
		result = append(result, rec)
	}
	return result, nil
}

func (j *countingIntakeJournal) SaveIntakeRecord(_ context.Context, rec sicore.IntakeJournalRecord) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.records == nil {
		j.records = map[string]sicore.IntakeJournalRecord{}
	}
	j.records[rec.StatementID] = rec
	j.saves++
	return nil
}

func TestIngress_BackpressureRefusalHasNoDurableOrSourceSideEffects(t *testing.T) {
	pressure := &fakePartsPressure{refuse: map[string]error{
		"net1__events/p_eu": &sicore.BackpressureError{Database: "hg_unsafe", Table: "net1__events", Partition: "p_eu", Parts: 2400, Limit: 2400, Kind: "soft"},
	}}
	journal := &countingIntakeJournal{}
	writer := &rootRecordingPayloadWriter{result: sicore.PayloadPutResult{PayloadRef: "payload://store/ref-1", State: sicore.PayloadStateAvailable}}
	submitter := &rootRecordingSubmitter{outcome: sicore.SubmitOutcome{Category: sicore.OutcomeAccepted}}
	preparer := &rootRecordingPreparer{source: "snode-A", claim: sicore.ClaimOutcome{Category: sicore.OutcomeAccepted, BoundSource: "snode-A"}}
	orch := sicore.NewOrchestrator(submitter, preparer, sicore.OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal})
	ingress, err := NewStorageIntegrityIngressWithPayloadWriter(orch, nil, sicore.MaterializerCSV, writer)
	if err != nil {
		t.Fatalf("NewStorageIntegrityIngressWithPayloadWriter: %v", err)
	}
	ingress.WithPartsPressure(pressure, bpSchemaResolver())

	err = ingress.ConsumeStorageIntegrityAdmission(context.Background(), bpAdmission())
	if !errors.Is(err, sicore.ErrBackpressure) {
		t.Fatalf("err = %v, want ErrBackpressure", err)
	}
	if writer.calls != 0 || journal.saves != 0 || submitter.calls != 0 || preparer.prepareCalls != 0 {
		t.Fatalf("writer/journal/submit/prepare calls = %d/%d/%d/%d, want all zero", writer.calls, journal.saves, submitter.calls, preparer.prepareCalls)
	}
}

func TestIngress_BackpressureAllowsAndInvalidatesAfterAck2(t *testing.T) {
	pressure := &fakePartsPressure{}
	ingress, writer, _, _ := newBackpressureIngress(t, pressure)
	if err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), bpAdmission()); err != nil {
		t.Fatalf("ConsumeStorageIntegrityAdmission: %v", err)
	}
	if writer.calls != 1 {
		t.Fatalf("payload writer calls = %d want 1", writer.calls)
	}
	if got := strings.Join(pressure.allowCalls, ","); got != "net1__events/p_eu,net1__events/p_us" {
		t.Fatalf("allow calls = %q", got)
	}
	if pressure.invalidated != 1 {
		t.Fatalf("invalidate after ACK2 = %d want 1", pressure.invalidated)
	}
	if pressure.committed != 1 || pressure.released != 0 {
		t.Fatalf("reservation commit/release = %d/%d want 1/0", pressure.committed, pressure.released)
	}
}

func TestIngress_TerminalAck2ReplayBypassesPressureGate(t *testing.T) {
	pressure := &fakePartsPressure{}
	ingress, writer, _, preparer := newBackpressureIngress(t, pressure)
	if err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), bpAdmission()); err != nil {
		t.Fatalf("first admission: %v", err)
	}
	firstCalls := len(pressure.allowCalls)
	pressure.refuse = map[string]error{
		"net1__events/p_eu": &sicore.BackpressureError{Database: "hg_unsafe", Table: "net1__events", Partition: "p_eu", Kind: "soft"},
	}
	if err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), bpAdmission()); err != nil {
		t.Fatalf("terminal ACK2 replay must bypass pressure: %v", err)
	}
	if len(pressure.allowCalls) != firstCalls {
		t.Fatalf("pressure calls = %d, want unchanged %d", len(pressure.allowCalls), firstCalls)
	}
	if preparer.prepareCalls != 1 {
		t.Fatalf("prepare calls = %d want 1", preparer.prepareCalls)
	}
	if writer.calls != 2 {
		t.Fatalf("payload writer calls = %d want 2 (idempotent put remains allowed)", writer.calls)
	}
}

func TestIngress_CachedPrepareResumeBypassesPressureGate(t *testing.T) {
	pressure := &fakePartsPressure{}
	ingress, _, submitter, preparer := newBackpressureIngress(t, pressure)
	submitter.outcome = sicore.SubmitOutcome{Category: sicore.OutcomeRetryable, Reason: "not leader"}
	if err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), bpAdmission()); err == nil || !strings.Contains(err.Error(), "did not reach ACK2") {
		t.Fatalf("first admission = %v, want retryable non-ACK2", err)
	}
	firstCalls := len(pressure.allowCalls)
	pressure.refuse = map[string]error{
		"net1__events/p_eu": &sicore.BackpressureError{Database: "hg_unsafe", Table: "net1__events", Partition: "p_eu", Kind: "soft"},
	}
	submitter.outcome = sicore.SubmitOutcome{Category: sicore.OutcomeAccepted}
	if err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), bpAdmission()); err != nil {
		t.Fatalf("cached resume must bypass pressure: %v", err)
	}
	if len(pressure.allowCalls) != firstCalls {
		t.Fatalf("pressure calls = %d, want unchanged %d", len(pressure.allowCalls), firstCalls)
	}
	if preparer.prepareCalls != 1 {
		t.Fatalf("prepare calls = %d want 1", preparer.prepareCalls)
	}
}

func TestIngress_IndeterminatePrepareRetryLooksUpSourceBeforePressureGate(t *testing.T) {
	pressure := &fakePartsPressure{}
	ingress, _, submitter, preparer := newBackpressureIngress(t, pressure)
	preparer.err = errors.New("prepare response lost")
	if err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), bpAdmission()); err == nil || !strings.Contains(err.Error(), "prepare response lost") {
		t.Fatalf("first admission = %v, want indeterminate prepare error", err)
	}
	firstPressureCalls := len(pressure.allowCalls)
	if preparer.prepareCalls != 1 {
		t.Fatalf("first prepare calls = %d, want 1", preparer.prepareCalls)
	}

	preparer.err = nil
	preparer.lookupFound = true
	preparer.lookupResult = sicore.PreparedLocalResult{
		StatementID:     preparer.env.StatementID,
		SourceNode:      "snode-A",
		PayloadRef:      preparer.env.PayloadRef,
		PayloadHash:     preparer.env.PayloadHash,
		PayloadLength:   preparer.env.PayloadLength,
		PayloadEncoding: preparer.env.PayloadEncoding,
		Revision:        preparer.env.Revision,
		Lifecycle:       sicore.LifecycleUnsafeWritten,
	}
	pressure.refuse = map[string]error{
		"net1__events/p_eu": &sicore.BackpressureError{Database: "hg_unsafe", Table: "net1__events", Partition: "p_eu", Parts: 2400, Limit: 2400, Kind: "soft"},
	}
	submitter.outcome = sicore.SubmitOutcome{Category: sicore.OutcomeAccepted}
	if err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), bpAdmission()); err != nil {
		t.Fatalf("source-proven prepared retry must bypass soft pressure: %v", err)
	}
	if preparer.lookupCalls != 1 {
		t.Fatalf("prepared lookup calls = %d, want 1", preparer.lookupCalls)
	}
	if preparer.prepareCalls != 1 {
		t.Fatalf("prepare calls = %d, want no second prepare", preparer.prepareCalls)
	}
	if len(pressure.allowCalls) != firstPressureCalls {
		t.Fatalf("pressure calls = %d, want unchanged %d after source-proven resume", len(pressure.allowCalls), firstPressureCalls)
	}
}

func TestIngress_IndeterminatePrepareNoWriteProofReleasesSoftLimitSlot(t *testing.T) {
	conn := &rootPartsConn{rows: []rootPartsRow{
		{"hg_unsafe", "net1__events", "eu", "region", 1},
		{"hg_unsafe", "net1__events", "us", "region", 1},
	}}
	pressure := sicore.NewPartsPressureGuard(conn, sicore.PartsPressureConfig{
		UnsafeDatabase: "hg_unsafe", SafeDatabase: "hg_safe",
		SoftPartsPerPartition: 2, HardPartsPerPartition: 5,
	})
	ingress, _, submitter, preparer := newBackpressureIngress(t, pressure)
	preparer.err = errors.New("prepare response lost")
	if err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), bpAdmission()); err == nil || !strings.Contains(err.Error(), "prepare response lost") {
		t.Fatalf("first admission = %v, want indeterminate prepare error", err)
	}

	// The required source lookup proves the first attempt wrote nothing. Its
	// statement-addressed committed slot must be canceled before the retry asks
	// for the sole soft-limit slot again.
	preparer.err = nil
	preparer.lookupFound = false
	submitter.outcome = sicore.SubmitOutcome{Category: sicore.OutcomeAccepted}
	if err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), bpAdmission()); err != nil {
		t.Fatalf("no-write-proven retry at soft-1: %v", err)
	}
	if preparer.lookupCalls != 1 || preparer.prepareCalls != 2 {
		t.Fatalf("lookup/prepare calls = %d/%d, want 1/2", preparer.lookupCalls, preparer.prepareCalls)
	}
}

func TestIngress_TerminalCleanupInvalidatesPressure(t *testing.T) {
	pressure := &fakePartsPressure{}
	ingress, _, submitter, _ := newBackpressureIngress(t, pressure)
	submitter.outcome = sicore.SubmitOutcome{Category: sicore.OutcomeTerminalReject, Reason: "conflict"}
	if err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), bpAdmission()); err == nil || !strings.Contains(err.Error(), string(sicore.LifecycleCleaned)) {
		t.Fatalf("terminal cleanup admission = %v, want Cleaned non-ACK2", err)
	}
	if pressure.invalidated != 1 || pressure.committed != 0 || pressure.released != 1 {
		t.Fatalf("cleanup invalidate/commit/release = %d/%d/%d want 1/0/1", pressure.invalidated, pressure.committed, pressure.released)
	}
}

type failFirstCleanedSaveJournal struct {
	*countingIntakeJournal
	err    error
	failed bool
}

type failEverySaveJournal struct {
	*countingIntakeJournal
	err error
}

func (j *failEverySaveJournal) SaveIntakeRecord(context.Context, sicore.IntakeJournalRecord) error {
	return j.err
}

func TestIngress_DefinitePrePrepareFailureReleasesReservation(t *testing.T) {
	pressure := &fakePartsPressure{}
	journalErr := errors.New("initial journal save failed")
	journal := &failEverySaveJournal{countingIntakeJournal: &countingIntakeJournal{}, err: journalErr}
	writer := &rootRecordingPayloadWriter{result: sicore.PayloadPutResult{PayloadRef: "payload://store/ref-1", State: sicore.PayloadStateAvailable}}
	submitter := &rootRecordingSubmitter{outcome: sicore.SubmitOutcome{Category: sicore.OutcomeAccepted}}
	preparer := &rootRecordingPreparer{source: "snode-A", claim: sicore.ClaimOutcome{Category: sicore.OutcomeAccepted, BoundSource: "snode-A"}}
	orch := sicore.NewOrchestrator(submitter, preparer, sicore.OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal})
	ingress, err := NewStorageIntegrityIngressWithPayloadWriter(orch, nil, sicore.MaterializerCSV, writer)
	if err != nil {
		t.Fatalf("NewStorageIntegrityIngressWithPayloadWriter: %v", err)
	}
	ingress.WithPartsPressure(pressure, bpSchemaResolver())

	if err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), bpAdmission()); !errors.Is(err, journalErr) {
		t.Fatalf("admission = %v, want definite pre-prepare failure", err)
	}
	if preparer.prepareCalls != 0 || submitter.calls != 0 {
		t.Fatalf("prepare/submit calls = %d/%d, want 0/0", preparer.prepareCalls, submitter.calls)
	}
	if pressure.committed != 0 || pressure.released != 1 {
		t.Fatalf("reservation commit/release = %d/%d, want 0/1", pressure.committed, pressure.released)
	}
}

func (j *failFirstCleanedSaveJournal) SaveIntakeRecord(ctx context.Context, rec sicore.IntakeJournalRecord) error {
	if rec.IsTerminal && rec.Stage == sicore.LifecycleCleaned && !j.failed {
		j.failed = true
		return j.err
	}
	return j.countingIntakeJournal.SaveIntakeRecord(ctx, rec)
}

func TestIngress_RepeatedCleanupAndTerminalSaveFailureReleaseZeroGrowthSlot(t *testing.T) {
	conn := &rootPartsConn{rows: []rootPartsRow{
		{"hg_unsafe", "net1__events", "eu", "region", 1},
		{"hg_unsafe", "net1__events", "us", "region", 1},
	}}
	pressure := sicore.NewPartsPressureGuard(conn, sicore.PartsPressureConfig{
		UnsafeDatabase: "hg_unsafe", SafeDatabase: "hg_safe",
		SoftPartsPerPartition: 2, HardPartsPerPartition: 5,
	})
	journalErr := errors.New("cleaned journal save failed")
	journal := &failFirstCleanedSaveJournal{countingIntakeJournal: &countingIntakeJournal{}, err: journalErr}
	writer := &rootRecordingPayloadWriter{result: sicore.PayloadPutResult{PayloadRef: "payload://store/ref-1", State: sicore.PayloadStateAvailable}}
	submitter := &rootRecordingSubmitter{outcome: sicore.SubmitOutcome{Category: sicore.OutcomeTerminalReject, Reason: "conflict"}}
	preparer := &rootRecordingPreparer{source: "snode-A", claim: sicore.ClaimOutcome{Category: sicore.OutcomeAccepted, BoundSource: "snode-A"}}
	orch := sicore.NewOrchestrator(submitter, preparer, sicore.OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal})
	ingress, err := NewStorageIntegrityIngressWithPayloadWriter(orch, nil, sicore.MaterializerCSV, writer)
	if err != nil {
		t.Fatalf("NewStorageIntegrityIngressWithPayloadWriter: %v", err)
	}
	ingress.WithPartsPressure(pressure, bpSchemaResolver())

	if err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), bpAdmission()); !errors.Is(err, journalErr) {
		t.Fatalf("first cleaned admission = %v, want terminal journal error", err)
	}
	if err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), bpAdmission()); err == nil || !strings.Contains(err.Error(), string(sicore.LifecycleCleaned)) {
		t.Fatalf("repeated exact cleanup = %v, want Cleaned non-ACK2", err)
	}

	// No system.parts growth occurred: exact cleanup removed the candidate both
	// times. A different statement must still acquire the soft-1 capacity.
	submitter.outcome = sicore.SubmitOutcome{Category: sicore.OutcomeAccepted}
	next := bpAdmission()
	next.StatementID = "0xabc:2:n2"
	if err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), next); err != nil {
		t.Fatalf("new statement after repeated cleanup: %v", err)
	}
}

func TestIngress_MapsSNodeMirrorBackpressureToException252(t *testing.T) {
	pressure := &fakePartsPressure{}
	ingress, _, _, preparer := newBackpressureIngress(t, pressure)
	preparer.err = &sicore.BackpressureError{Database: "hg_unsafe", Table: "net1__events", Partition: "p_eu", Parts: 2950, Limit: 2950, Kind: "hard"}
	err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), bpAdmission())
	var clientErr *chproto.ClientError
	if !errors.As(err, &clientErr) || clientErr.Code != chproto.CodeTooManyParts || !strings.Contains(clientErr.Message, "hard limit 2950") {
		t.Fatalf("err = %v, want ClientError 252 from the SNode mirror", err)
	}
	if pressure.invalidated != 1 || pressure.released != 1 || pressure.committed != 0 {
		t.Fatalf("mirror refusal invalidate/release/commit = %d/%d/%d want 1/1/0", pressure.invalidated, pressure.released, pressure.committed)
	}
}

func TestIngress_UnknownSchemaOrFreezeViolationRefusedWithoutPut(t *testing.T) {
	ingress, writer, _, _ := newBackpressureIngress(t, &fakePartsPressure{})
	adm := bpAdmission()
	adm.TableID = "net1.unknown"
	err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), adm)
	if err == nil || !strings.Contains(err.Error(), "no pinned schema") || writer.calls != 0 {
		t.Fatalf("err = %v writer calls = %d", err, writer.calls)
	}

	ingress.schemas = sicore.TableSchemaResolverFunc(func(tableID string) (payloadexec.TableSchema, bool) {
		return payloadexec.TableSchema{
			TableID: tableID, PartitionBy: "region",
			Columns: []lthash.Column{{Name: "region", Type: "UInt64"}},
		}, true
	})
	adm = bpAdmission()
	err = ingress.ConsumeStorageIntegrityAdmission(context.Background(), adm)
	if !errors.Is(err, sicore.ErrPartitionFreeze) || writer.calls != 0 {
		t.Fatalf("freeze err = %v writer calls = %d", err, writer.calls)
	}
}

func TestIngress_ResolverTableIDMismatchFailsClosedBeforeReservationOrPut(t *testing.T) {
	pressure := &fakePartsPressure{}
	ingress, writer, _, _ := newBackpressureIngress(t, pressure)
	ingress.schemas = sicore.TableSchemaResolverFunc(func(string) (payloadexec.TableSchema, bool) {
		return payloadexec.TableSchema{
			TableID: "net1.other", PartitionBy: "region",
			Columns: []lthash.Column{{Name: "id", Type: "UInt64"}, {Name: "region", Type: "String"}},
		}, true
	})
	err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), bpAdmission())
	if err == nil || !strings.Contains(err.Error(), "schema table_id") {
		t.Fatalf("err = %v, want resolver table_id mismatch", err)
	}
	if len(pressure.allowCalls) != 0 || writer.calls != 0 {
		t.Fatalf("pressure/writer calls = %d/%d want 0/0", len(pressure.allowCalls), writer.calls)
	}
}
