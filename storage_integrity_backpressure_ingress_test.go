package housegate

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/lthash"
	siplugin "github.com/housegate/housegate/pkg/plugins/storageintegrity"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

type fakePartsPressure struct {
	mu                   sync.Mutex
	refuse               map[string]error
	allowCalls           []string
	invalidated          int
	committed            int
	released             int
	cleaned              int
	blockCleanup         bool
	blockPrepareCleanup  bool
	cleanupCtxErr        error
	prepareCleanupCtxErr error
	commitErr            error
	restored             int
	restoreBatches       int
	observedHook         func(context.Context, string, sicore.CandidatePart) error
}

type boundClaimStatusQuerier struct {
	claimCalls int
}

func (q *boundClaimStatusQuerier) QuerySubmitStatus(context.Context, string) (sicore.SubmitOutcome, error) {
	return sicore.SubmitOutcome{Category: sicore.OutcomeAccepted}, nil
}

func (q *boundClaimStatusQuerier) QueryClaimStatus(context.Context, string) (sicore.ClaimOutcome, error) {
	q.claimCalls++
	return sicore.ClaimOutcome{Category: sicore.OutcomeAccepted, BoundSource: "snode-A"}, nil
}

func (f *fakePartsPressure) ReserveStatement(_ context.Context, _ string, table string, partitionIDs []string) (sicore.PartsReservation, error) {
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

func (f *fakePartsPressure) Reserve(ctx context.Context, table string, partitionIDs []string) (sicore.PartsReservation, error) {
	return f.ReserveStatement(ctx, "test-statement", table, partitionIDs)
}

func (f *fakePartsPressure) RestoreBatch(_ context.Context, records []sicore.PartsRestoreRecord) (map[string]sicore.PartsReservation, error) {
	f.mu.Lock()
	f.restoreBatches++
	f.restored += len(records)
	f.mu.Unlock()
	restored := make(map[string]sicore.PartsReservation, len(records))
	for _, record := range records {
		if !record.Finalized {
			restored[record.StatementID] = &fakePartsReservation{pressure: f}
		}
	}
	return restored, nil
}

func (f *fakePartsPressure) SetCandidateObservedHook(hook func(context.Context, string, sicore.CandidatePart) error) {
	f.mu.Lock()
	f.observedHook = hook
	f.mu.Unlock()
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

func (r *fakePartsReservation) Commit(...sicore.CandidatePart) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state != "" {
		return nil
	}
	r.pressure.mu.Lock()
	r.pressure.committed++
	commitErr := r.pressure.commitErr
	r.pressure.mu.Unlock()
	r.state = "committed"
	return commitErr
}

func (r *fakePartsReservation) PrepareCleanupProof(ctx context.Context, _ []sicore.CandidatePart) error {
	r.pressure.mu.Lock()
	r.pressure.prepareCleanupCtxErr = ctx.Err()
	block := r.pressure.blockPrepareCleanup
	r.pressure.mu.Unlock()
	if block {
		<-ctx.Done()
		return errors.Join(sicore.ErrCleanupProofPending, sicore.ErrBackpressure, ctx.Err())
	}
	return nil
}

func (r *fakePartsReservation) Release() {
	r.release(false)
}

func (r *fakePartsReservation) ReleaseCleaned(ctx context.Context) error {
	r.pressure.mu.Lock()
	block := r.pressure.blockCleanup
	r.pressure.cleanupCtxErr = ctx.Err()
	r.pressure.mu.Unlock()
	if block {
		<-ctx.Done()
		return errors.Join(sicore.ErrCleanupProofPending, sicore.ErrBackpressure, ctx.Err())
	}
	r.release(true)
	return nil
}

func (r *fakePartsReservation) release(cleaned bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state == "released" || r.state == "finalized" {
		return
	}
	r.pressure.mu.Lock()
	r.pressure.released++
	if cleaned {
		r.pressure.cleaned++
	}
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

func bpSchemaResolver() StorageIntegrityTableSchemaResolver {
	schema := bpSchemas()[0]
	return StorageIntegrityTableSchemaResolverFunc(func(tableID string) (payloadexec.TableSchema, bool) {
		if tableID != schema.TableID {
			return payloadexec.TableSchema{}, false
		}
		return schema, true
	})
}

func bpAdmission() siplugin.Admission {
	sql := "INSERT INTO events FORMAT Native"
	payload := bpNativePayload([]uint64{1, 2, 3}, []string{"eu", "us", "eu"})
	return siplugin.Admission{
		StatementID: "0xabc:1:n1", Kind: siplugin.KindInsert, TableID: "net1.events",
		SQL: sql, SQLHash: replay.DigestString(sql), Signer: "0xabc", UserJWS: "jws",
		Payload:         siplugin.CapturedPayload{Bytes: payload, Length: uint64(len(payload)), Encoding: sicore.PayloadEncodingClickHouseNativeData, Revision: 54465, Complete: true},
		EnvelopeVersion: sicore.EnvelopeVersionV2,
		NetworkID:       "testnet-v2",
		SettingsHash:    sicore.EmptySettingsHash,
		SchemaHash:      payloadexec.TableSchemaHash("testnet-v2", bpSchemas()[0]),
		RowIDProfileID:  payloadexec.RowIDProfileID,
	}
}

func bpEUAdmission() siplugin.Admission {
	adm := bpAdmission()
	payload := bpNativePayload([]uint64{1}, []string{"eu"})
	adm.Payload.Bytes = payload
	adm.Payload.Length = uint64(len(payload))
	return adm
}

func bpNativePayload(ids []uint64, regions []string) []byte {
	if len(ids) != len(regions) || len(ids) == 0 {
		panic("bpNativePayload requires matching non-empty columns")
	}
	regionCol := new(proto.ColStr)
	for _, region := range regions {
		regionCol.Append(region)
	}
	idCol := proto.ColUInt64(ids)
	input := proto.Input{
		{Name: "id", Data: &idCol},
		{Name: "region", Data: regionCol},
	}
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ClientCodeData))
	buf.PutString("")
	if err := (proto.Block{Rows: len(ids), Columns: len(input)}).EncodeBlock(&buf, 54465, input); err != nil {
		panic(err)
	}
	return append([]byte(nil), buf.Buf...)
}

func bpPreparedCandidates() []sicore.CandidatePart {
	return []sicore.CandidatePart{
		{TableID: "net1.events", PartitionID: "p_eu", PartName: "eu_part_1"},
		{TableID: "net1.events", PartitionID: "p_us", PartName: "us_part_1"},
	}
}

func newBackpressureIngress(t *testing.T, pressure StorageIntegrityPartsPressure) (*StorageIntegrityIngress, *rootRecordingPayloadWriter, *rootRecordingSubmitter, *rootRecordingPreparer) {
	t.Helper()
	writer := &rootRecordingPayloadWriter{result: sicore.PayloadPutResult{PayloadRef: "payload://store/ref-1", State: sicore.PayloadStateAvailable}}
	submitter := &rootRecordingSubmitter{outcome: sicore.SubmitOutcome{Category: sicore.OutcomeAccepted}}
	preparer := &rootRecordingPreparer{
		source: "snode-A", claim: sicore.ClaimOutcome{Category: sicore.OutcomeAccepted, BoundSource: "snode-A"},
		candidates: bpPreparedCandidates(),
	}
	orch := sicore.NewOrchestrator(submitter, preparer, sicore.OrchestratorConfig{ExpectedSource: "snode-A"})
	ingress, err := NewStorageIntegrityIngressWithPayloadWriter(orch, nil, sicore.MaterializerNative, writer)
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

func TestIngress_RejectsMissingOrDuplicatePreparedCandidatePartitionBeforeRC(t *testing.T) {
	tests := map[string][]sicore.CandidatePart{
		"missing touched partition": {
			{TableID: "net1.events", PartitionID: "p_eu", PartName: "eu_part_1"},
		},
		"duplicate touched partition": {
			{TableID: "net1.events", PartitionID: "p_eu", PartName: "eu_part_1"},
			{TableID: "net1.events", PartitionID: "p_eu", PartName: "eu_part_2"},
			{TableID: "net1.events", PartitionID: "p_us", PartName: "us_part_1"},
		},
	}
	for name, candidates := range tests {
		t.Run(name, func(t *testing.T) {
			pressure := &fakePartsPressure{}
			ingress, _, _, preparer := newBackpressureIngress(t, pressure)
			preparer.candidates = candidates
			err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), bpAdmission())
			if err == nil || !strings.Contains(err.Error(), "candidate partition") {
				t.Fatalf("ConsumeStorageIntegrityAdmission error=%v, want candidate partition consistency failure", err)
			}
			if preparer.abortCalls != 0 {
				t.Fatalf("malformed candidate set triggered %d destructive aborts", preparer.abortCalls)
			}
		})
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

type failTouchedPartitionSaveJournal struct {
	*countingIntakeJournal
	err  error
	fail bool
}

func (j *failTouchedPartitionSaveJournal) SaveIntakeRecord(ctx context.Context, rec sicore.IntakeJournalRecord) error {
	if j.fail && rec.Admission.TouchedPartitionIDs != nil {
		return j.err
	}
	return j.countingIntakeJournal.SaveIntakeRecord(ctx, rec)
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
	ingress, err := NewStorageIntegrityIngressWithPayloadWriter(orch, nil, sicore.MaterializerNative, writer)
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

func TestIngress_TerminalPrewriteRejectWithTerminalSubmitReleasesReservation(t *testing.T) {
	pressure := &fakePartsPressure{}
	ingress, _, submitter, preparer := newBackpressureIngress(t, pressure)
	submitter.outcome = sicore.SubmitOutcome{Category: sicore.OutcomeTerminalReject, Reason: "conflicting duplicate"}
	preparer.err = fmt.Errorf("schema hash mismatch: %w", sicore.ErrPrepareTerminalReject)

	err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), bpAdmission())
	if err == nil || !strings.Contains(err.Error(), string(sicore.LifecycleCleaned)) {
		t.Fatalf("terminal pre-write reject err=%v, want terminal Cleaned non-ACK2", err)
	}
	if strings.Contains(err.Error(), "candidate partition") {
		t.Fatalf("known-unwritten empty abort incorrectly required candidates: %v", err)
	}
	if preparer.abortCalls != 1 {
		t.Fatalf("empty source abort calls=%d, want 1", preparer.abortCalls)
	}
	if pressure.committed != 0 || pressure.released != 1 || pressure.cleaned != 0 {
		t.Fatalf("reservation commit/release/cleaned=%d/%d/%d, want 0/1/0", pressure.committed, pressure.released, pressure.cleaned)
	}

	beforeAllows := len(pressure.allowCalls)
	beforePrepares := preparer.prepareCalls
	beforeAborts := preparer.abortCalls
	err = ingress.ConsumeStorageIntegrityAdmission(context.Background(), bpAdmission())
	if err == nil || !strings.Contains(err.Error(), string(sicore.LifecycleCleaned)) {
		t.Fatalf("terminal replay err=%v, want cached Cleaned non-ACK2", err)
	}
	if len(pressure.allowCalls) != beforeAllows || preparer.prepareCalls != beforePrepares || preparer.abortCalls != beforeAborts {
		t.Fatalf("terminal replay pressure/prepare/abort=%d/%d/%d, want %d/%d/%d", len(pressure.allowCalls), preparer.prepareCalls, preparer.abortCalls, beforeAllows, beforePrepares, beforeAborts)
	}
}

func TestIngress_RejectsSignedSchemaHashMismatchBeforePressure(t *testing.T) {
	pressure := &fakePartsPressure{}
	ingress, writer, submitter, preparer := newBackpressureIngress(t, pressure)
	adm := bpAdmission()
	adm.SchemaHash = replay.DigestString("different schema")

	err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), adm)
	if err == nil || !strings.Contains(err.Error(), "schema_hash") {
		t.Fatalf("schema hash mismatch err=%v", err)
	}
	if len(pressure.allowCalls) != 0 || writer.calls != 0 || submitter.calls != 0 || preparer.prepareCalls != 0 {
		t.Fatalf("mismatched schema pressure/put/submit/prepare=%d/%d/%d/%d, want all zero", len(pressure.allowCalls), writer.calls, submitter.calls, preparer.prepareCalls)
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
		CandidateParts:  bpPreparedCandidates(),
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
	if pressure.invalidated != 1 || pressure.committed != 0 || pressure.released != 1 || pressure.cleaned != 1 {
		t.Fatalf("cleanup invalidate/commit/release/exact = %d/%d/%d/%d want 1/0/1/1", pressure.invalidated, pressure.committed, pressure.released, pressure.cleaned)
	}
}

func TestIngress_CandidateBindingFailureStaysChargedAndFailsClosed(t *testing.T) {
	bindErr := errors.New("candidate maps outside reserved table")
	pressure := &fakePartsPressure{commitErr: bindErr}
	ingress, _, submitter, _ := newBackpressureIngress(t, pressure)
	submitter.outcome = sicore.SubmitOutcome{Category: sicore.OutcomeAccepted}
	adm := bpAdmission()
	err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), adm)
	if !errors.Is(err, bindErr) {
		t.Fatalf("candidate binding error=%v want %v", err, bindErr)
	}
	if pressure.committed != 1 || pressure.released != 0 {
		t.Fatalf("mismatched candidate reservation committed/released=%d/%d want 1/0", pressure.committed, pressure.released)
	}
	if ingress.pressureReservation(adm.StatementID) == nil {
		t.Fatal("mismatched candidate reservation was deleted instead of staying charged")
	}
}

func TestIngress_CleanupProofRejectsAnotherStatementsActiveCandidateBeforeAbort(t *testing.T) {
	conn := &rootPartsConn{rows: []rootPartsRow{
		{"hg_unsafe", "net1__events", "eu", "region", 1},
		{"hg_unsafe", "net1__events", "us", "region", 1},
	}}
	pressure := sicore.NewPartsPressureGuard(conn, sicore.PartsPressureConfig{
		UnsafeDatabase: "hg_unsafe", SafeDatabase: "hg_safe",
		SoftPartsPerPartition: 5, HardPartsPerPartition: 8,
	})
	ingress, _, submitter, preparer := newBackpressureIngress(t, pressure)
	shared := sicore.CandidatePart{TableID: "net1.events", PartitionID: "p_eu", PartName: "eu_part_2"}
	preparer.candidates = []sicore.CandidatePart{shared}

	owner := bpEUAdmission()
	if err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), owner); err != nil {
		t.Fatalf("owner admission: %v", err)
	}
	// The owner reached ACK2 and its exact candidate is now active. A different
	// statement returning that name must fail the pre-abort ownership proof;
	// calling AbortPreparedStatement would DROP the owner's durable part.
	conn.setRows([]rootPartsRow{
		{"hg_unsafe", "net1__events", "eu", "region", 2},
		{"hg_unsafe", "net1__events", "us", "region", 1},
	})
	submitter.outcome = sicore.SubmitOutcome{Category: sicore.OutcomeTerminalReject, Reason: "conflict"}
	other := bpEUAdmission()
	other.StatementID = "0xabc:2:n2"
	err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), other)
	if !errors.Is(err, sicore.ErrCleanupProofPending) || !errors.Is(err, sicore.ErrBackpressure) {
		t.Fatalf("conflicting cleanup error=%v, want pending back-pressure proof", err)
	}
	if preparer.abortCalls != 0 {
		t.Fatalf("source abort calls=%d, want 0 before candidate ownership proof", preparer.abortCalls)
	}
}

func TestIngress_PreparedCandidateClaimedBeforeSourceFrontierRelease(t *testing.T) {
	conn := &rootPartsConn{rows: []rootPartsRow{
		{"hg_unsafe", "net1__events", "eu", "region", 1},
		{"hg_unsafe", "net1__events", "us", "region", 1},
	}}
	pressure := sicore.NewPartsPressureGuard(conn, sicore.PartsPressureConfig{
		UnsafeDatabase: "hg_unsafe", SafeDatabase: "hg_safe",
		SoftPartsPerPartition: 5, HardPartsPerPartition: 8,
	})
	submitter := &rootRecordingSubmitter{outcome: sicore.SubmitOutcome{Category: sicore.OutcomeAccepted}}
	shared := sicore.CandidatePart{TableID: "net1.events", PartitionID: "p_eu", PartName: "eu_part_2"}
	preparer := &rootRecordingPreparer{
		source: "snode-A", claim: sicore.ClaimOutcome{Category: sicore.OutcomeAccepted, BoundSource: "snode-A"},
		candidates: []sicore.CandidatePart{shared},
	}
	orch := sicore.NewOrchestrator(submitter, preparer, sicore.OrchestratorConfig{ExpectedSource: "snode-A"})
	ingress, err := NewStorageIntegrityIngress(orch, nil, sicore.MaterializerNative)
	if err != nil {
		t.Fatalf("NewStorageIntegrityIngress: %v", err)
	}
	ingress.WithPartsPressure(pressure, bpSchemaResolver())

	ownerAdmission := bpEUAdmission()
	ownerRecord := AdmissionRecordFromPlugin(ownerAdmission)
	table, partitions, err := ingress.partsPressureTarget(ownerRecord)
	if err != nil {
		t.Fatalf("owner pressure target: %v", err)
	}
	ownerReservation, err := pressure.ReserveStatement(context.Background(), ownerRecord.StatementID, table, partitions)
	if err != nil {
		t.Fatalf("owner Reserve: %v", err)
	}
	defer ownerReservation.Release()
	ingress.setPressureReservation(ownerRecord.StatementID, ownerRecord.TableID, partitions, ownerReservation)
	ownerResult, err := orch.Orchestrate(context.Background(), ownerRecord)
	if err != nil || !ownerResult.Ack2 {
		t.Fatalf("owner Orchestrate=%+v err=%v", ownerResult, err)
	}
	// Deliberately stop in the old post-Orchestrate/pre-Commit window. The
	// source frontier has been released, but Consume has not yet had a chance to
	// run its outer reservation Commit. The owner must already be bound here.
	conn.setRows([]rootPartsRow{
		{"hg_unsafe", "net1__events", "eu", "region", 2},
		{"hg_unsafe", "net1__events", "us", "region", 1},
	})
	otherAdmission := bpEUAdmission()
	otherAdmission.StatementID = "0xabc:2:n2"
	otherRecord := AdmissionRecordFromPlugin(otherAdmission)
	otherReservation, err := pressure.ReserveStatement(context.Background(), otherRecord.StatementID, table, partitions)
	if err != nil {
		t.Fatalf("other Reserve: %v", err)
	}
	defer otherReservation.Release()
	ingress.setPressureReservation(otherRecord.StatementID, otherRecord.TableID, partitions, otherReservation)
	submitter.outcome = sicore.SubmitOutcome{Category: sicore.OutcomeTerminalReject, Reason: "conflict"}
	_, err = orch.Orchestrate(context.Background(), otherRecord)
	if !errors.Is(err, sicore.ErrCleanupProofPending) {
		t.Fatalf("conflicting cleanup error=%v, want pending proof", err)
	}
	if preparer.abortCalls != 0 {
		t.Fatalf("source abort calls=%d, want 0 after owner frontier release", preparer.abortCalls)
	}
}

func TestIngress_RecoverPendingRestoresFinalizedCandidateOwnerBeforeAbort(t *testing.T) {
	journal, err := sicore.NewFileIntakeJournal(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileIntakeJournal: %v", err)
	}
	shared := sicore.CandidatePart{TableID: "net1.events", PartitionID: "p_eu", PartName: "eu_part_2"}
	firstSubmitter := &rootRecordingSubmitter{outcome: sicore.SubmitOutcome{Category: sicore.OutcomeAccepted}}
	firstPreparer := &rootRecordingPreparer{
		source: "snode-A", claim: sicore.ClaimOutcome{Category: sicore.OutcomeAccepted, BoundSource: "snode-A"},
		candidates: []sicore.CandidatePart{shared},
	}
	first := sicore.NewOrchestrator(firstSubmitter, firstPreparer, sicore.OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal})
	owner := AdmissionRecordFromPlugin(bpEUAdmission())
	owner.TouchedPartitionIDs = []string{"p_eu"}
	if res, err := first.Orchestrate(context.Background(), owner); err != nil || !res.Ack2 {
		t.Fatalf("persist owner=(%+v, %v)", res, err)
	}
	firstSubmitter.outcome = sicore.SubmitOutcome{Category: sicore.OutcomeTerminalReject, Reason: "conflict"}
	proofErr := errors.New("stop before source abort")
	first.SetBeforeExactCleanup(func(context.Context, sicore.IntakeResult) error { return proofErr })
	other := AdmissionRecordFromPlugin(bpEUAdmission())
	other.StatementID = "0xabc:2:n2"
	if _, err := first.Orchestrate(context.Background(), other); !errors.Is(err, proofErr) {
		t.Fatalf("persist AbortPending=%v, want proof error", err)
	}
	if firstPreparer.abortCalls != 0 {
		t.Fatalf("first process abort calls=%d want 0", firstPreparer.abortCalls)
	}

	conn := &rootPartsConn{rows: []rootPartsRow{
		{"hg_unsafe", "net1__events", "eu", "region", 2},
		{"hg_unsafe", "net1__events", "us", "region", 1},
	}}
	pressure := sicore.NewPartsPressureGuard(conn, sicore.PartsPressureConfig{
		UnsafeDatabase: "hg_unsafe", SafeDatabase: "hg_safe",
		SoftPartsPerPartition: 5, HardPartsPerPartition: 8,
	})
	if _, err := pressure.Refresh(context.Background()); err != nil {
		t.Fatalf("initial pressure Refresh: %v", err)
	}
	restartedPreparer := &rootRecordingPreparer{source: "snode-A", candidates: []sicore.CandidatePart{shared}}
	restarted := sicore.NewOrchestrator(
		&rootRecordingSubmitter{}, restartedPreparer,
		sicore.OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal},
	)
	ingress, err := NewStorageIntegrityIngress(restarted, nil, sicore.MaterializerNative)
	if err != nil {
		t.Fatalf("NewStorageIntegrityIngress: %v", err)
	}
	ingress.WithPartsPressure(pressure, bpSchemaResolver())
	err = ingress.RecoverPending(context.Background())
	if err == nil {
		t.Fatal("recovery accepted a pending cleanup that reused a finalized active candidate")
	}
	if restartedPreparer.abortCalls != 0 {
		t.Fatalf("restart source abort calls=%d, want 0 before restored owner proof", restartedPreparer.abortCalls)
	}
}

func TestIngress_RecoverPendingRestoresEachJournalRecordOnlyOnce(t *testing.T) {
	journal, err := sicore.NewFileIntakeJournal(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileIntakeJournal: %v", err)
	}
	owner := AdmissionRecordFromPlugin(bpEUAdmission())
	owner.TouchedPartitionIDs = []string{"p_eu"}
	first := sicore.NewOrchestrator(
		&rootRecordingSubmitter{outcome: sicore.SubmitOutcome{Category: sicore.OutcomeAccepted}},
		&rootRecordingPreparer{
			source: "snode-A",
			claim:  sicore.ClaimOutcome{Category: sicore.OutcomeAccepted, BoundSource: "snode-A"},
			candidates: []sicore.CandidatePart{{
				TableID: owner.TableID, PartitionID: "p_eu", PartName: "eu_part_2",
			}},
		},
		sicore.OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal},
	)
	if res, err := first.Orchestrate(context.Background(), owner); err != nil || !res.Ack2 {
		t.Fatalf("persist finalized owner=(%+v, %v)", res, err)
	}

	pressure := &fakePartsPressure{}
	restarted := sicore.NewOrchestrator(
		&rootRecordingSubmitter{}, &rootRecordingPreparer{source: "snode-A"},
		sicore.OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal},
	)
	ingress, err := NewStorageIntegrityIngress(restarted, nil, sicore.MaterializerNative)
	if err != nil {
		t.Fatalf("NewStorageIntegrityIngress: %v", err)
	}
	ingress.WithPartsPressure(pressure, bpSchemaResolver())
	if err := ingress.RecoverPending(context.Background()); err != nil {
		t.Fatalf("first RecoverPending: %v", err)
	}
	if err := ingress.RecoverPending(context.Background()); err != nil {
		t.Fatalf("second RecoverPending: %v", err)
	}
	pressure.mu.Lock()
	restored := pressure.restored
	restoreBatches := pressure.restoreBatches
	pressure.mu.Unlock()
	if restored != 1 {
		t.Fatalf("durable pressure record restored %d times, want once", restored)
	}
	if restoreBatches != 1 {
		t.Fatalf("durable pressure history restored in %d batches, want one", restoreBatches)
	}
}

func TestIngress_RecoverPendingFinalizedZeroCandidateIsNoop(t *testing.T) {
	ctx := context.Background()
	journal, err := sicore.NewFileIntakeJournal(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileIntakeJournal: %v", err)
	}
	owner := AdmissionRecordFromPlugin(bpEUAdmission())
	owner.TouchedPartitionIDs = []string{}
	first := sicore.NewOrchestrator(
		&rootRecordingSubmitter{outcome: sicore.SubmitOutcome{Category: sicore.OutcomeAccepted}},
		&rootRecordingPreparer{
			source: "snode-A",
			claim:  sicore.ClaimOutcome{Category: sicore.OutcomeAccepted, BoundSource: "snode-A"},
		},
		sicore.OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal},
	)
	if res, err := first.Orchestrate(ctx, owner); err != nil || !res.Ack2 {
		t.Fatalf("persist zero-candidate owner=(%+v, %v)", res, err)
	}
	conn := &rootPartsConn{rows: []rootPartsRow{
		{"hg_unsafe", "net1__events", "eu", "region", 1},
	}}
	pressure := sicore.NewPartsPressureGuard(conn, sicore.PartsPressureConfig{
		UnsafeDatabase: "hg_unsafe", SafeDatabase: "hg_safe",
		SoftPartsPerPartition: 2, HardPartsPerPartition: 5,
	})
	restarted := sicore.NewOrchestrator(
		&rootRecordingSubmitter{}, &rootRecordingPreparer{source: "snode-A"},
		sicore.OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal},
	)
	ingress, err := NewStorageIntegrityIngress(restarted, nil, sicore.MaterializerNative)
	if err != nil {
		t.Fatalf("NewStorageIntegrityIngress: %v", err)
	}
	ingress.WithPartsPressure(pressure, bpSchemaResolver())
	if err := ingress.RecoverPending(ctx); err != nil {
		t.Fatalf("RecoverPending: %v", err)
	}
	probe, err := pressure.ReserveStatement(ctx, "zero-candidate-probe", "net1__events", []string{"p_eu"})
	if err != nil {
		t.Fatalf("zero-candidate finalized record retained pressure debt: %v", err)
	}
	probe.Release()
}

func TestIngress_LegacyFinalizedAbsentCandidateRequiresExplicitMigration(t *testing.T) {
	ctx := context.Background()
	journal, err := sicore.NewFileIntakeJournal(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileIntakeJournal: %v", err)
	}
	owner := AdmissionRecordFromPlugin(bpEUAdmission())
	owner.TouchedPartitionIDs = []string{"p_eu"}
	env, err := sicore.EnvelopeFromAdmission(owner)
	if err != nil {
		t.Fatalf("EnvelopeFromAdmission: %v", err)
	}
	candidate := sicore.CandidatePart{TableID: owner.TableID, PartitionID: "p_eu", PartName: "legacy_eu_part"}
	prepared := sicore.PreparedLocalResult{
		StatementID: owner.StatementID, SourceNode: "snode-A",
		PayloadRef: env.PayloadRef, PayloadHash: env.PayloadHash, PayloadLength: env.PayloadLength,
		PayloadEncoding: env.PayloadEncoding, Revision: env.Revision,
		CandidateParts: []sicore.CandidatePart{candidate}, Lifecycle: sicore.LifecycleUnsafeWritten,
	}
	// A zero-version record models the pre-observation journal shape. Its exact
	// candidate is now absent, so the runtime cannot distinguish delayed
	// visibility from historical cleanup without an external migration proof.
	if err := journal.SaveIntakeRecord(ctx, sicore.IntakeJournalRecord{
		StatementID: owner.StatementID, Source: "snode-A", FrontierOrdinal: 1,
		Env: env, Admission: owner, Stage: sicore.LifecycleRCBound,
		Prepared: prepared, HasPrepared: true,
		TerminalResult: sicore.IntakeResult{
			StatementID: owner.StatementID, Ack2: true, Lifecycle: sicore.LifecycleRCBound,
			Prepared: prepared,
		},
		IsTerminal: true,
	}); err != nil {
		t.Fatalf("SaveIntakeRecord: %v", err)
	}
	pressure := sicore.NewPartsPressureGuard(&rootPartsConn{}, sicore.PartsPressureConfig{
		UnsafeDatabase: "hg_unsafe", SafeDatabase: "hg_safe",
		SoftPartsPerPartition: 2, HardPartsPerPartition: 5,
	})
	restarted := sicore.NewOrchestrator(
		&rootRecordingSubmitter{}, &rootRecordingPreparer{source: "snode-A"},
		sicore.OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal},
	)
	ingress, err := NewStorageIntegrityIngress(restarted, nil, sicore.MaterializerNative)
	if err != nil {
		t.Fatalf("NewStorageIntegrityIngress: %v", err)
	}
	ingress.WithPartsPressure(pressure, bpSchemaResolver())
	err = ingress.RecoverPending(ctx)
	if !errors.Is(err, sicore.ErrIntakeJournalMigrationRequired) || !strings.Contains(err.Error(), owner.StatementID) || !strings.Contains(err.Error(), candidate.PartName) {
		t.Fatalf("RecoverPending error=%v, want named legacy migration requirement", err)
	}
}

func TestIngress_LegacyFinalizedActiveCandidateMigratesObservationAndPartitions(t *testing.T) {
	ctx := context.Background()
	journal, err := sicore.NewFileIntakeJournal(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileIntakeJournal: %v", err)
	}
	owner := AdmissionRecordFromPlugin(bpEUAdmission())
	env, err := sicore.EnvelopeFromAdmission(owner)
	if err != nil {
		t.Fatalf("EnvelopeFromAdmission: %v", err)
	}
	candidate := sicore.CandidatePart{TableID: owner.TableID, PartitionID: "p_eu", PartName: "eu_part_2"}
	prepared := sicore.PreparedLocalResult{
		StatementID: owner.StatementID, SourceNode: "snode-A",
		PayloadRef: env.PayloadRef, PayloadHash: env.PayloadHash, PayloadLength: env.PayloadLength,
		PayloadEncoding: env.PayloadEncoding, Revision: env.Revision,
		CandidateParts: []sicore.CandidatePart{candidate}, Lifecycle: sicore.LifecycleUnsafeWritten,
	}
	if err := journal.SaveIntakeRecord(ctx, sicore.IntakeJournalRecord{
		StatementID: owner.StatementID, Source: "snode-A", FrontierOrdinal: 1,
		Env: env, Admission: owner, Stage: sicore.LifecycleRCBound,
		Prepared: prepared, HasPrepared: true,
		TerminalResult: sicore.IntakeResult{
			StatementID: owner.StatementID, Ack2: true, Lifecycle: sicore.LifecycleRCBound,
			Prepared: prepared,
		},
		IsTerminal: true,
	}); err != nil {
		t.Fatalf("SaveIntakeRecord: %v", err)
	}

	visibleConn := &rootPartsConn{rows: []rootPartsRow{{"hg_unsafe", "net1__events", "eu", "region", 2}}}
	visibleGuard := sicore.NewPartsPressureGuard(visibleConn, sicore.PartsPressureConfig{
		UnsafeDatabase: "hg_unsafe", SafeDatabase: "hg_safe",
		SoftPartsPerPartition: 3, HardPartsPerPartition: 5,
	})
	visibleRestart := sicore.NewOrchestrator(
		&rootRecordingSubmitter{}, &rootRecordingPreparer{source: "snode-A"},
		sicore.OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal},
	)
	visibleIngress, err := NewStorageIntegrityIngress(visibleRestart, nil, sicore.MaterializerNative)
	if err != nil {
		t.Fatalf("NewStorageIntegrityIngress visible: %v", err)
	}
	visibleIngress.WithPartsPressure(visibleGuard, bpSchemaResolver())
	if err := visibleIngress.RecoverPending(ctx); err != nil {
		t.Fatalf("visible RecoverPending: %v", err)
	}
	persisted, ok, err := journal.LoadIntakeRecord(ctx, owner.StatementID)
	if err != nil || !ok {
		t.Fatalf("LoadIntakeRecord=(%+v, %v, %v)", persisted, ok, err)
	}
	if persisted.JournalVersion == 0 || !reflect.DeepEqual(persisted.Admission.TouchedPartitionIDs, []string{"p_eu"}) || !candidatePartIn(candidate, persisted.ObservedCandidateParts) {
		t.Fatalf("legacy migration version=%d partitions=%v observed=%+v", persisted.JournalVersion, persisted.Admission.TouchedPartitionIDs, persisted.ObservedCandidateParts)
	}

	// Once exact-active observation is durable, a later exact absence is proof
	// of historical cleanup and must retire ownership instead of requiring
	// operator migration or retaining visibility debt forever.
	absentConn := &rootPartsConn{rows: []rootPartsRow{{"hg_unsafe", "net1__events", "eu", "region", 1}}}
	absentGuard := sicore.NewPartsPressureGuard(absentConn, sicore.PartsPressureConfig{
		UnsafeDatabase: "hg_unsafe", SafeDatabase: "hg_safe",
		SoftPartsPerPartition: 2, HardPartsPerPartition: 5,
	})
	absentRestart := sicore.NewOrchestrator(
		&rootRecordingSubmitter{}, &rootRecordingPreparer{source: "snode-A"},
		sicore.OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal},
	)
	absentIngress, err := NewStorageIntegrityIngress(absentRestart, nil, sicore.MaterializerNative)
	if err != nil {
		t.Fatalf("NewStorageIntegrityIngress absent: %v", err)
	}
	absentIngress.WithPartsPressure(absentGuard, bpSchemaResolver())
	if err := absentIngress.RecoverPending(ctx); err != nil {
		t.Fatalf("absent RecoverPending: %v", err)
	}
	probe, err := absentGuard.ReserveStatement(ctx, "legacy-migration-probe", "net1__events", []string{"p_eu"})
	if err != nil {
		t.Fatalf("migrated historical candidate retained debt: %v", err)
	}
	probe.Release()
}

func TestIngress_LegacyFinalizedKnownEmptyPartitionsRejectsCandidate(t *testing.T) {
	ctx := context.Background()
	journal, err := sicore.NewFileIntakeJournal(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileIntakeJournal: %v", err)
	}
	owner := AdmissionRecordFromPlugin(bpEUAdmission())
	owner.TouchedPartitionIDs = []string{}
	env, err := sicore.EnvelopeFromAdmission(owner)
	if err != nil {
		t.Fatalf("EnvelopeFromAdmission: %v", err)
	}
	candidate := sicore.CandidatePart{TableID: owner.TableID, PartitionID: "p_eu", PartName: "eu_part_1"}
	prepared := sicore.PreparedLocalResult{
		StatementID: owner.StatementID, SourceNode: "snode-A",
		PayloadRef: env.PayloadRef, PayloadHash: env.PayloadHash, PayloadLength: env.PayloadLength,
		PayloadEncoding: env.PayloadEncoding, Revision: env.Revision,
		CandidateParts: []sicore.CandidatePart{candidate}, Lifecycle: sicore.LifecycleUnsafeWritten,
	}
	if err := journal.SaveIntakeRecord(ctx, sicore.IntakeJournalRecord{
		StatementID: owner.StatementID, Source: "snode-A", FrontierOrdinal: 1,
		Env: env, Admission: owner, Stage: sicore.LifecycleRCBound,
		Prepared: prepared, HasPrepared: true,
		TerminalResult: sicore.IntakeResult{
			StatementID: owner.StatementID, Ack2: true, Lifecycle: sicore.LifecycleRCBound,
			Prepared: prepared,
		},
		IsTerminal: true,
	}); err != nil {
		t.Fatalf("SaveIntakeRecord: %v", err)
	}
	pressure := sicore.NewPartsPressureGuard(
		&rootPartsConn{rows: []rootPartsRow{{"hg_unsafe", "net1__events", "eu", "region", 1}}},
		sicore.PartsPressureConfig{
			UnsafeDatabase: "hg_unsafe", SafeDatabase: "hg_safe",
			SoftPartsPerPartition: 3, HardPartsPerPartition: 5,
		},
	)
	restarted := sicore.NewOrchestrator(
		&rootRecordingSubmitter{}, &rootRecordingPreparer{source: "snode-A"},
		sicore.OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal},
	)
	ingress, err := NewStorageIntegrityIngress(restarted, nil, sicore.MaterializerNative)
	if err != nil {
		t.Fatalf("NewStorageIntegrityIngress: %v", err)
	}
	ingress.WithPartsPressure(pressure, bpSchemaResolver())
	err = ingress.RecoverPending(ctx)
	if err == nil || !strings.Contains(err.Error(), "candidate partition \"p_eu\" was not touched") {
		t.Fatalf("RecoverPending error=%v, want known-empty candidate mismatch", err)
	}
}

func TestIngress_LegacyFinalizedObservedCandidateMigratesWithoutCurrentPart(t *testing.T) {
	ctx := context.Background()
	journal, err := sicore.NewFileIntakeJournal(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileIntakeJournal: %v", err)
	}
	owner := AdmissionRecordFromPlugin(bpEUAdmission())
	env, err := sicore.EnvelopeFromAdmission(owner)
	if err != nil {
		t.Fatalf("EnvelopeFromAdmission: %v", err)
	}
	candidate := sicore.CandidatePart{TableID: owner.TableID, PartitionID: "p_eu", PartName: "eu_part_2"}
	prepared := sicore.PreparedLocalResult{
		StatementID: owner.StatementID, SourceNode: "snode-A",
		PayloadRef: env.PayloadRef, PayloadHash: env.PayloadHash, PayloadLength: env.PayloadLength,
		PayloadEncoding: env.PayloadEncoding, Revision: env.Revision,
		CandidateParts: []sicore.CandidatePart{candidate}, Lifecycle: sicore.LifecycleUnsafeWritten,
	}
	if err := journal.SaveIntakeRecord(ctx, sicore.IntakeJournalRecord{
		StatementID: owner.StatementID, Source: "snode-A", FrontierOrdinal: 1,
		Env: env, Admission: owner, Stage: sicore.LifecycleRCBound,
		Prepared: prepared, HasPrepared: true,
		ObservedCandidateParts: []sicore.CandidatePart{candidate},
		TerminalResult: sicore.IntakeResult{
			StatementID: owner.StatementID, Ack2: true, Lifecycle: sicore.LifecycleRCBound,
			Prepared: prepared,
		},
		IsTerminal: true,
	}); err != nil {
		t.Fatalf("SaveIntakeRecord: %v", err)
	}
	// eu_part_2 is absent, but its exact durable observation predates cleanup and
	// is sufficient migration proof. No aggregate-count inference is involved.
	conn := &rootPartsConn{rows: []rootPartsRow{{"hg_unsafe", "net1__events", "eu", "region", 1}}}
	pressure := sicore.NewPartsPressureGuard(conn, sicore.PartsPressureConfig{
		UnsafeDatabase: "hg_unsafe", SafeDatabase: "hg_safe",
		SoftPartsPerPartition: 2, HardPartsPerPartition: 5,
	})
	restarted := sicore.NewOrchestrator(
		&rootRecordingSubmitter{}, &rootRecordingPreparer{source: "snode-A"},
		sicore.OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal},
	)
	ingress, err := NewStorageIntegrityIngress(restarted, nil, sicore.MaterializerNative)
	if err != nil {
		t.Fatalf("NewStorageIntegrityIngress: %v", err)
	}
	ingress.WithPartsPressure(pressure, bpSchemaResolver())
	if err := ingress.RecoverPending(ctx); err != nil {
		t.Fatalf("RecoverPending: %v", err)
	}
	persisted, ok, err := journal.LoadIntakeRecord(ctx, owner.StatementID)
	if err != nil || !ok {
		t.Fatalf("LoadIntakeRecord=(%+v, %v, %v)", persisted, ok, err)
	}
	if persisted.JournalVersion == 0 || !reflect.DeepEqual(persisted.Admission.TouchedPartitionIDs, []string{"p_eu"}) {
		t.Fatalf("observed legacy migration version=%d partitions=%v", persisted.JournalVersion, persisted.Admission.TouchedPartitionIDs)
	}
	probe, err := pressure.ReserveStatement(ctx, "legacy-observed-probe", "net1__events", []string{"p_eu"})
	if err != nil {
		t.Fatalf("observed historical candidate retained debt: %v", err)
	}
	probe.Release()
}

func TestIngress_RestartPersistsCandidateVisibilityAndRetiresHistoricalDebt(t *testing.T) {
	ctx := context.Background()
	journal, err := sicore.NewFileIntakeJournal(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileIntakeJournal: %v", err)
	}
	owner := AdmissionRecordFromPlugin(bpEUAdmission())
	owner.TouchedPartitionIDs = []string{"p_eu"}
	candidate := sicore.CandidatePart{
		TableID: owner.TableID, PartitionID: "p_eu", PartName: "eu_part_2",
	}
	first := sicore.NewOrchestrator(
		&rootRecordingSubmitter{outcome: sicore.SubmitOutcome{Category: sicore.OutcomeAccepted}},
		&rootRecordingPreparer{
			source:     "snode-A",
			claim:      sicore.ClaimOutcome{Category: sicore.OutcomeAccepted, BoundSource: "snode-A"},
			candidates: []sicore.CandidatePart{candidate},
		},
		sicore.OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal},
	)
	if res, err := first.Orchestrate(ctx, owner); err != nil || !res.Ack2 {
		t.Fatalf("persist owner=(%+v, %v)", res, err)
	}
	persisted, ok, err := journal.LoadIntakeRecord(ctx, owner.StatementID)
	if err != nil || !ok {
		t.Fatalf("LoadIntakeRecord=(%+v, %v, %v)", persisted, ok, err)
	}
	if len(persisted.Admission.Payload) != 0 {
		t.Fatalf("terminal owner retained %d payload bytes", len(persisted.Admission.Payload))
	}

	visibleConn := &rootPartsConn{rows: []rootPartsRow{
		{"hg_unsafe", "net1__events", "eu", "region", 2},
	}}
	visibleGuard := sicore.NewPartsPressureGuard(visibleConn, sicore.PartsPressureConfig{
		UnsafeDatabase: "hg_unsafe", SafeDatabase: "hg_safe",
		SoftPartsPerPartition: 3, HardPartsPerPartition: 5,
	})
	visibleRestart := sicore.NewOrchestrator(
		&rootRecordingSubmitter{}, &rootRecordingPreparer{source: "snode-A"},
		sicore.OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal},
	)
	visibleIngress, err := NewStorageIntegrityIngress(visibleRestart, nil, sicore.MaterializerNative)
	if err != nil {
		t.Fatalf("NewStorageIntegrityIngress visible restart: %v", err)
	}
	visibleIngress.WithPartsPressure(visibleGuard, bpSchemaResolver())
	if err := visibleIngress.RecoverPending(ctx); err != nil {
		t.Fatalf("visible RecoverPending: %v", err)
	}
	if got := visibleConn.queries.Load(); got != 1 {
		t.Fatalf("visible restart inventory queries=%d, want one batch snapshot", got)
	}
	persisted, ok, err = journal.LoadIntakeRecord(ctx, owner.StatementID)
	if err != nil || !ok {
		t.Fatalf("LoadIntakeRecord after visibility=(%+v, %v, %v)", persisted, ok, err)
	}
	if !candidatePartIn(candidate, persisted.ObservedCandidateParts) {
		t.Fatalf("observed candidates=%+v, want %s", persisted.ObservedCandidateParts, candidate.PartName)
	}

	absentConn := &rootPartsConn{rows: []rootPartsRow{
		{"hg_unsafe", "net1__events", "eu", "region", 1},
	}}
	absentGuard := sicore.NewPartsPressureGuard(absentConn, sicore.PartsPressureConfig{
		UnsafeDatabase: "hg_unsafe", SafeDatabase: "hg_safe",
		SoftPartsPerPartition: 2, HardPartsPerPartition: 5,
	})
	absentRestart := sicore.NewOrchestrator(
		&rootRecordingSubmitter{}, &rootRecordingPreparer{source: "snode-A"},
		sicore.OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal},
	)
	absentIngress, err := NewStorageIntegrityIngress(absentRestart, nil, sicore.MaterializerNative)
	if err != nil {
		t.Fatalf("NewStorageIntegrityIngress absent restart: %v", err)
	}
	absentIngress.WithPartsPressure(absentGuard, bpSchemaResolver())
	if err := absentIngress.RecoverPending(ctx); err != nil {
		t.Fatalf("absent RecoverPending: %v", err)
	}
	reservation, err := absentGuard.ReserveStatement(ctx, "post-history-probe", "net1__events", []string{"p_eu"})
	if err != nil {
		t.Fatalf("historical observed+absent candidate retained debt: %v", err)
	}
	reservation.Release()
}

func TestIngress_LiveReservationPersistsCandidateVisibilityByStatement(t *testing.T) {
	ctx := context.Background()
	journal, err := sicore.NewFileIntakeJournal(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileIntakeJournal: %v", err)
	}
	conn := &rootPartsConn{rows: []rootPartsRow{
		{"hg_unsafe", "net1__events", "eu", "region", 1},
		{"hg_unsafe", "net1__events", "us", "region", 1},
	}}
	pressure := sicore.NewPartsPressureGuard(conn, sicore.PartsPressureConfig{
		UnsafeDatabase: "hg_unsafe", SafeDatabase: "hg_safe",
		SoftPartsPerPartition: 4, HardPartsPerPartition: 6,
	})
	writer := &rootRecordingPayloadWriter{result: sicore.PayloadPutResult{
		PayloadRef: "payload://store/ref-live-observation", State: sicore.PayloadStateAvailable,
	}}
	preparer := &rootRecordingPreparer{
		source: "snode-A",
		claim:  sicore.ClaimOutcome{Category: sicore.OutcomeAccepted, BoundSource: "snode-A"},
		candidates: []sicore.CandidatePart{{
			TableID: "net1.events", PartitionID: "p_eu", PartName: "eu_part_2",
		}},
	}
	orch := sicore.NewOrchestrator(
		&rootRecordingSubmitter{outcome: sicore.SubmitOutcome{Category: sicore.OutcomeAccepted}}, preparer,
		sicore.OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal},
	)
	ingress, err := NewStorageIntegrityIngressWithPayloadWriter(orch, nil, sicore.MaterializerNative, writer)
	if err != nil {
		t.Fatalf("NewStorageIntegrityIngressWithPayloadWriter: %v", err)
	}
	ingress.WithPartsPressure(pressure, bpSchemaResolver())
	adm := bpEUAdmission()
	if err := ingress.ConsumeStorageIntegrityAdmission(ctx, adm); err != nil {
		t.Fatalf("ConsumeStorageIntegrityAdmission: %v", err)
	}

	conn.setRows([]rootPartsRow{
		{"hg_unsafe", "net1__events", "eu", "region", 2},
		{"hg_unsafe", "net1__events", "us", "region", 1},
	})
	if _, err := pressure.Refresh(ctx); err != nil {
		t.Fatalf("post-write Refresh: %v", err)
	}
	persisted, ok, err := journal.LoadIntakeRecord(ctx, adm.StatementID)
	if err != nil || !ok {
		t.Fatalf("LoadIntakeRecord=(%+v, %v, %v)", persisted, ok, err)
	}
	want := sicore.CandidatePart{TableID: "net1.events", PartitionID: "p_eu", PartName: "eu_part_2"}
	if !candidatePartIn(want, persisted.ObservedCandidateParts) {
		t.Fatalf("observed candidates=%+v, want live owner %s", persisted.ObservedCandidateParts, want.PartName)
	}
}

func TestIngress_RecoverPendingRestoresReservationThroughExactCleanup(t *testing.T) {
	journal, err := sicore.NewFileIntakeJournal(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileIntakeJournal: %v", err)
	}
	shared := sicore.CandidatePart{TableID: "net1.events", PartitionID: "p_eu", PartName: "eu_part_2"}
	firstSubmitter := &rootRecordingSubmitter{outcome: sicore.SubmitOutcome{Category: sicore.OutcomeTerminalReject, Reason: "conflict"}}
	firstPreparer := &rootRecordingPreparer{source: "snode-A", candidates: []sicore.CandidatePart{shared}}
	first := sicore.NewOrchestrator(firstSubmitter, firstPreparer, sicore.OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal})
	proofErr := errors.New("crash before source abort")
	first.SetBeforeExactCleanup(func(context.Context, sicore.IntakeResult) error { return proofErr })
	admission := AdmissionRecordFromPlugin(bpEUAdmission())
	if _, err := first.Orchestrate(context.Background(), admission); !errors.Is(err, proofErr) {
		t.Fatalf("persist AbortPending=%v, want proof error", err)
	}

	conn := &rootPartsConn{rows: []rootPartsRow{
		{"hg_unsafe", "net1__events", "eu", "region", 2},
		{"hg_unsafe", "net1__events", "us", "region", 1},
	}}
	pressure := sicore.NewPartsPressureGuard(conn, sicore.PartsPressureConfig{
		UnsafeDatabase: "hg_unsafe", SafeDatabase: "hg_safe",
		SoftPartsPerPartition: 3, HardPartsPerPartition: 8,
	})
	if _, err := pressure.Refresh(context.Background()); err != nil {
		t.Fatalf("initial pressure Refresh: %v", err)
	}
	restartedPreparer := &rootRecordingPreparer{
		source: "snode-A", candidates: []sicore.CandidatePart{shared},
		abortFn: func([]sicore.CandidatePart) {
			conn.setRows([]rootPartsRow{
				{"hg_unsafe", "net1__events", "eu", "region", 1},
				{"hg_unsafe", "net1__events", "us", "region", 1},
			})
		},
	}
	restarted := sicore.NewOrchestrator(
		&rootRecordingSubmitter{}, restartedPreparer,
		sicore.OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal},
	)
	ingress, err := NewStorageIntegrityIngress(restarted, nil, sicore.MaterializerNative)
	if err != nil {
		t.Fatalf("NewStorageIntegrityIngress: %v", err)
	}
	ingress.WithPartsPressure(pressure, bpSchemaResolver())
	if err := ingress.RecoverPending(context.Background()); err != nil {
		t.Fatalf("RecoverPending: %v", err)
	}
	if restartedPreparer.abortCalls != 1 {
		t.Fatalf("restart source abort calls=%d want 1", restartedPreparer.abortCalls)
	}
	if ingress.pressureReservation(admission.StatementID) != nil {
		t.Fatal("cleaned recovery left a tracked reservation")
	}
	reservation, err := pressure.ReserveStatement(context.Background(), "post-cleanup-probe", "net1__events", []string{"p_eu", "p_us"})
	if err != nil {
		t.Fatalf("post-cleanup capacity remained charged: %v", err)
	}
	reservation.Release()
}

func TestIngress_RecoveredPendingPersistsTouchedPartitionsBeforeTerminalCompaction(t *testing.T) {
	ctx := context.Background()
	journal, err := sicore.NewFileIntakeJournal(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileIntakeJournal: %v", err)
	}
	adm := AdmissionRecordFromPlugin(bpEUAdmission())
	candidate := sicore.CandidatePart{TableID: adm.TableID, PartitionID: "p_eu", PartName: "eu_part_2"}
	first := sicore.NewOrchestrator(
		&rootRecordingSubmitter{outcome: sicore.SubmitOutcome{Category: sicore.OutcomeRetryable, Reason: "NotLeader"}},
		&rootRecordingPreparer{
			source:     "snode-A",
			claim:      sicore.ClaimOutcome{Category: sicore.OutcomeAccepted, BoundSource: "snode-A"},
			candidates: []sicore.CandidatePart{candidate},
		},
		sicore.OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal},
	)
	if res, err := first.Orchestrate(ctx, adm); err != nil || res.Ack2 || res.Submit.Category != sicore.OutcomeRetryable {
		t.Fatalf("seed pending recovery=(%+v, %v)", res, err)
	}

	conn := &rootPartsConn{rows: []rootPartsRow{{"hg_unsafe", "net1__events", "eu", "region", 2}}}
	newGuard := func() *sicore.PartsPressureGuard {
		return sicore.NewPartsPressureGuard(conn, sicore.PartsPressureConfig{
			UnsafeDatabase: "hg_unsafe", SafeDatabase: "hg_safe",
			SoftPartsPerPartition: 4, HardPartsPerPartition: 6,
		})
	}
	restarted := sicore.NewOrchestrator(
		&rootRecordingSubmitter{outcome: sicore.SubmitOutcome{Category: sicore.OutcomeAccepted}},
		&rootRecordingPreparer{source: "snode-A", claim: sicore.ClaimOutcome{Category: sicore.OutcomeAccepted, BoundSource: "snode-A"}},
		sicore.OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal},
	)
	ingress, err := NewStorageIntegrityIngress(restarted, nil, sicore.MaterializerNative)
	if err != nil {
		t.Fatalf("NewStorageIntegrityIngress: %v", err)
	}
	ingress.WithPartsPressure(newGuard(), bpSchemaResolver())
	if err := ingress.RecoverPending(ctx); err != nil {
		t.Fatalf("RecoverPending: %v", err)
	}
	persisted, ok, err := journal.LoadIntakeRecord(ctx, adm.StatementID)
	if err != nil || !ok {
		t.Fatalf("LoadIntakeRecord=(%+v, %v, %v)", persisted, ok, err)
	}
	if !persisted.IsTerminal || !persisted.TerminalResult.Ack2 || !reflect.DeepEqual(persisted.Admission.TouchedPartitionIDs, []string{"p_eu"}) {
		t.Fatalf("recovered terminal state terminal=%v ack2=%v partitions=%v", persisted.IsTerminal, persisted.TerminalResult.Ack2, persisted.Admission.TouchedPartitionIDs)
	}

	// A second restart has no payload bytes left. It must reconstruct finalized
	// ownership/debt from the durable payload-derived partition set alone.
	second := sicore.NewOrchestrator(
		&rootRecordingSubmitter{}, &rootRecordingPreparer{source: "snode-A"},
		sicore.OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal},
	)
	secondIngress, err := NewStorageIntegrityIngress(second, nil, sicore.MaterializerNative)
	if err != nil {
		t.Fatalf("NewStorageIntegrityIngress second restart: %v", err)
	}
	secondIngress.WithPartsPressure(newGuard(), bpSchemaResolver())
	if err := secondIngress.RecoverPending(ctx); err != nil {
		t.Fatalf("second RecoverPending: %v", err)
	}
}

func TestIngress_RecoveredTouchedPartitionPersistenceFailsBeforePressureRestore(t *testing.T) {
	ctx := context.Background()
	persistErr := errors.New("persist recovered touched partitions")
	journal := &failTouchedPartitionSaveJournal{
		countingIntakeJournal: &countingIntakeJournal{},
		err:                   persistErr,
	}
	adm := AdmissionRecordFromPlugin(bpEUAdmission())
	candidate := sicore.CandidatePart{TableID: adm.TableID, PartitionID: "p_eu", PartName: "eu_part_1"}
	first := sicore.NewOrchestrator(
		&rootRecordingSubmitter{outcome: sicore.SubmitOutcome{Category: sicore.OutcomeRetryable, Reason: "NotLeader"}},
		&rootRecordingPreparer{
			source:     "snode-A",
			claim:      sicore.ClaimOutcome{Category: sicore.OutcomeAccepted, BoundSource: "snode-A"},
			candidates: []sicore.CandidatePart{candidate},
		},
		sicore.OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal},
	)
	if res, err := first.Orchestrate(ctx, adm); err != nil || res.Ack2 || res.Submit.Category != sicore.OutcomeRetryable {
		t.Fatalf("seed pending recovery=(%+v, %v)", res, err)
	}

	pressure := &fakePartsPressure{}
	submitter := &rootRecordingSubmitter{outcome: sicore.SubmitOutcome{Category: sicore.OutcomeAccepted}}
	restarted := sicore.NewOrchestrator(
		submitter,
		&rootRecordingPreparer{source: "snode-A", claim: sicore.ClaimOutcome{Category: sicore.OutcomeAccepted, BoundSource: "snode-A"}},
		sicore.OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal},
	)
	ingress, err := NewStorageIntegrityIngress(restarted, nil, sicore.MaterializerNative)
	if err != nil {
		t.Fatalf("NewStorageIntegrityIngress: %v", err)
	}
	ingress.WithPartsPressure(pressure, bpSchemaResolver())
	journal.fail = true
	if err := ingress.RecoverPending(ctx); !errors.Is(err, persistErr) {
		t.Fatalf("RecoverPending error=%v, want %v", err, persistErr)
	}
	pressure.mu.Lock()
	restoreBatches := pressure.restoreBatches
	pressure.mu.Unlock()
	if restoreBatches != 0 || submitter.calls != 0 {
		t.Fatalf("failed durable bind restore/submit calls=%d/%d, want 0/0", restoreBatches, submitter.calls)
	}

	journal.fail = false
	if err := ingress.RecoverPending(ctx); err != nil {
		t.Fatalf("RecoverPending retry: %v", err)
	}
	pressure.mu.Lock()
	restoreBatches = pressure.restoreBatches
	pressure.mu.Unlock()
	if restoreBatches != 1 || submitter.calls != 1 {
		t.Fatalf("successful retry restore/submit calls=%d/%d, want 1/1", restoreBatches, submitter.calls)
	}
}

func TestIngress_LegacyTerminalTouchedPersistenceFailureRetriesDurably(t *testing.T) {
	ctx := context.Background()
	persistErr := errors.New("persist legacy terminal touched partitions")
	journal := &failTouchedPartitionSaveJournal{
		countingIntakeJournal: &countingIntakeJournal{},
		err:                   persistErr,
	}
	adm := AdmissionRecordFromPlugin(bpEUAdmission())
	env, err := sicore.EnvelopeFromAdmission(adm)
	if err != nil {
		t.Fatalf("EnvelopeFromAdmission: %v", err)
	}
	candidate := sicore.CandidatePart{TableID: adm.TableID, PartitionID: "p_eu", PartName: "eu_part_2"}
	prepared := sicore.PreparedLocalResult{
		StatementID: adm.StatementID, SourceNode: "snode-A",
		PayloadRef: env.PayloadRef, PayloadHash: env.PayloadHash, PayloadLength: env.PayloadLength,
		PayloadEncoding: env.PayloadEncoding, Revision: env.Revision,
		CandidateParts: []sicore.CandidatePart{candidate}, Lifecycle: sicore.LifecycleUnsafeWritten,
	}
	if err := journal.SaveIntakeRecord(ctx, sicore.IntakeJournalRecord{
		StatementID: adm.StatementID, Source: "snode-A", FrontierOrdinal: 1,
		Env: env, Admission: adm, Stage: sicore.LifecycleRCBound,
		Prepared: prepared, HasPrepared: true,
		TerminalResult: sicore.IntakeResult{
			StatementID: adm.StatementID, Ack2: true, Lifecycle: sicore.LifecycleRCBound,
			Prepared: prepared,
		},
		IsTerminal: true,
	}); err != nil {
		t.Fatalf("SaveIntakeRecord: %v", err)
	}

	newGuard := func() *sicore.PartsPressureGuard {
		return sicore.NewPartsPressureGuard(
			&rootPartsConn{rows: []rootPartsRow{{"hg_unsafe", "net1__events", "eu", "region", 2}}},
			sicore.PartsPressureConfig{
				UnsafeDatabase: "hg_unsafe", SafeDatabase: "hg_safe",
				SoftPartsPerPartition: 4, HardPartsPerPartition: 6,
			},
		)
	}
	orch := sicore.NewOrchestrator(
		&rootRecordingSubmitter{}, &rootRecordingPreparer{source: "snode-A"},
		sicore.OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal},
	)
	ingress, err := NewStorageIntegrityIngress(orch, nil, sicore.MaterializerNative)
	if err != nil {
		t.Fatalf("NewStorageIntegrityIngress: %v", err)
	}
	ingress.WithPartsPressure(newGuard(), bpSchemaResolver())
	journal.fail = true
	if err := ingress.RecoverPending(ctx); !errors.Is(err, persistErr) {
		t.Fatalf("first RecoverPending error=%v, want %v", err, persistErr)
	}
	journal.fail = false
	if err := ingress.RecoverPending(ctx); err != nil {
		t.Fatalf("RecoverPending retry: %v", err)
	}
	persisted, ok, err := journal.LoadIntakeRecord(ctx, adm.StatementID)
	if err != nil || !ok {
		t.Fatalf("LoadIntakeRecord=(%+v, %v, %v)", persisted, ok, err)
	}
	if persisted.JournalVersion == 0 || !reflect.DeepEqual(persisted.Admission.TouchedPartitionIDs, []string{"p_eu"}) || !candidatePartIn(candidate, persisted.ObservedCandidateParts) {
		t.Fatalf("durable migration version=%d touched=%v observed=%+v", persisted.JournalVersion, persisted.Admission.TouchedPartitionIDs, persisted.ObservedCandidateParts)
	}

	second := sicore.NewOrchestrator(
		&rootRecordingSubmitter{}, &rootRecordingPreparer{source: "snode-A"},
		sicore.OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal},
	)
	secondIngress, err := NewStorageIntegrityIngress(second, nil, sicore.MaterializerNative)
	if err != nil {
		t.Fatalf("NewStorageIntegrityIngress second: %v", err)
	}
	secondIngress.WithPartsPressure(newGuard(), bpSchemaResolver())
	if err := secondIngress.RecoverPending(ctx); err != nil {
		t.Fatalf("second restart RecoverPending: %v", err)
	}
}

func TestIngress_RecoveryRejectsMalformedPreparedBindingBeforeClaimStatusConvergence(t *testing.T) {
	ctx := context.Background()
	journal, err := sicore.NewFileIntakeJournal(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileIntakeJournal: %v", err)
	}
	adm := AdmissionRecordFromPlugin(bpAdmission())
	env, err := sicore.EnvelopeFromAdmission(adm)
	if err != nil {
		t.Fatalf("EnvelopeFromAdmission: %v", err)
	}
	prepared := sicore.PreparedLocalResult{
		StatementID: adm.StatementID, SourceNode: "snode-A",
		PayloadRef: env.PayloadRef, PayloadHash: env.PayloadHash, PayloadLength: env.PayloadLength,
		PayloadEncoding: env.PayloadEncoding, Revision: env.Revision,
		CandidateParts: []sicore.CandidatePart{{
			TableID: adm.TableID, PartitionID: "p_eu", PartName: "eu_part_1",
		}},
		Lifecycle: sicore.LifecycleUnsafeWritten,
	}
	if err := journal.SaveIntakeRecord(ctx, sicore.IntakeJournalRecord{
		StatementID: adm.StatementID, Source: "snode-A", FrontierOrdinal: 1,
		Env: env, Admission: adm, Stage: sicore.LifecycleSubmitAccepted,
		Prepared: prepared, HasPrepared: true,
		Submit: sicore.SubmitOutcome{Category: sicore.OutcomeAccepted}, HasSubmit: true,
		ClaimUnknown: true,
	}); err != nil {
		t.Fatalf("SaveIntakeRecord: %v", err)
	}
	querier := &boundClaimStatusQuerier{}
	orch := sicore.NewOrchestratorWithQuerier(
		&rootRecordingSubmitter{}, &rootRecordingPreparer{source: "snode-A"}, querier,
		sicore.OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal},
	)
	pressure := &fakePartsPressure{}
	ingress, err := NewStorageIntegrityIngress(orch, nil, sicore.MaterializerNative)
	if err != nil {
		t.Fatalf("NewStorageIntegrityIngress: %v", err)
	}
	ingress.WithPartsPressure(pressure, bpSchemaResolver())
	err = ingress.RecoverPending(ctx)
	if err == nil || !strings.Contains(err.Error(), "candidate partition \"p_us\" is missing") {
		t.Fatalf("RecoverPending error=%v, want touched/candidate binding failure", err)
	}
	if querier.claimCalls != 0 {
		t.Fatalf("malformed recovery queried claim status %d times, want 0", querier.claimCalls)
	}
	persisted, ok, loadErr := journal.LoadIntakeRecord(ctx, adm.StatementID)
	if loadErr != nil || !ok {
		t.Fatalf("LoadIntakeRecord=(%+v, %v, %v)", persisted, ok, loadErr)
	}
	if persisted.IsTerminal {
		t.Fatal("malformed recovered candidate binding reached terminal ACK2")
	}
}

func TestIngress_SchemaOnlyRecoveryRejectsMalformedPreparedBindingBeforeClaimStatusConvergence(t *testing.T) {
	ctx := context.Background()
	journal, err := sicore.NewFileIntakeJournal(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileIntakeJournal: %v", err)
	}
	adm := AdmissionRecordFromPlugin(bpAdmission())
	env, err := sicore.EnvelopeFromAdmission(adm)
	if err != nil {
		t.Fatalf("EnvelopeFromAdmission: %v", err)
	}
	prepared := sicore.PreparedLocalResult{
		StatementID: adm.StatementID, SourceNode: "snode-A",
		PayloadRef: env.PayloadRef, PayloadHash: env.PayloadHash, PayloadLength: env.PayloadLength,
		PayloadEncoding: env.PayloadEncoding, Revision: env.Revision,
		CandidateParts: []sicore.CandidatePart{{
			TableID: adm.TableID, PartitionID: "p_eu", PartName: "eu_part_1",
		}},
		Lifecycle: sicore.LifecycleUnsafeWritten,
	}
	if err := journal.SaveIntakeRecord(ctx, sicore.IntakeJournalRecord{
		StatementID: adm.StatementID, Source: "snode-A", FrontierOrdinal: 1,
		Env: env, Admission: adm, Stage: sicore.LifecycleSubmitAccepted,
		Prepared: prepared, HasPrepared: true,
		Submit: sicore.SubmitOutcome{Category: sicore.OutcomeAccepted}, HasSubmit: true,
		ClaimUnknown: true,
	}); err != nil {
		t.Fatalf("SaveIntakeRecord: %v", err)
	}
	querier := &boundClaimStatusQuerier{}
	orch := sicore.NewOrchestratorWithQuerier(
		&rootRecordingSubmitter{}, &rootRecordingPreparer{source: "snode-A"}, querier,
		sicore.OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal},
	)
	ingress, err := NewStorageIntegrityIngress(orch, nil, sicore.MaterializerNative)
	if err != nil {
		t.Fatalf("NewStorageIntegrityIngress: %v", err)
	}
	ingress.WithTableSchemas(bpSchemaResolver())
	err = ingress.RecoverPending(ctx)
	if err == nil || !strings.Contains(err.Error(), "candidate partition \"p_us\" is missing") {
		t.Fatalf("RecoverPending error=%v, want schema-only touched/candidate binding failure", err)
	}
	if querier.claimCalls != 0 {
		t.Fatalf("malformed schema-only recovery queried claim status %d times, want 0", querier.claimCalls)
	}
}

func TestIngress_CleanupProofCancellationReleasesSourceFrontier(t *testing.T) {
	pressure := &fakePartsPressure{blockCleanup: true}
	ingress, _, submitter, _ := newBackpressureIngress(t, pressure)
	ingress.cleanupProofTimeout = 20 * time.Millisecond
	submitter.outcome = sicore.SubmitOutcome{Category: sicore.OutcomeTerminalReject, Reason: "conflict"}
	if err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), bpAdmission()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked cleanup proof error=%v, want deadline", err)
	}

	pressure.mu.Lock()
	pressure.blockCleanup = false
	pressure.mu.Unlock()
	// Same-ID retry re-runs only idempotent abort+proof and closes the pending
	// proof before any new prepare can pass the unavailable inventory.
	if err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), bpAdmission()); err == nil || !strings.Contains(err.Error(), string(sicore.LifecycleCleaned)) {
		t.Fatalf("same-ID cleanup proof retry=%v, want Cleaned non-ACK2", err)
	}
	submitter.outcome = sicore.SubmitOutcome{Category: sicore.OutcomeAccepted}
	next := bpAdmission()
	next.StatementID = "0xabc:2:n2"
	if err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), next); err != nil {
		t.Fatalf("next statement after cleanup proof cancellation: %v", err)
	}
}

func TestIngress_CleanupProofUsesRuntimeContextAfterClientCancellation(t *testing.T) {
	pressure := &fakePartsPressure{}
	ingress, _, _, _ := newBackpressureIngress(t, pressure)
	reservation, err := pressure.Reserve(context.Background(), "net1__events", []string{"p_eu"})
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	ingress.setPressureReservation("stmt", "net1.events", []string{"p_eu"}, reservation)
	clientCtx, cancel := context.WithCancel(context.Background())
	cancel()
	cleanupResult := sicore.IntakeResult{
		StatementID: "stmt",
		Prepared: sicore.PreparedLocalResult{CandidateParts: []sicore.CandidatePart{{
			TableID: "net1.events", PartitionID: "p_eu", PartName: "eu_part_1",
		}}},
	}
	if err := ingress.beforeExactCleanup(clientCtx, cleanupResult); err != nil {
		t.Fatalf("beforeExactCleanup: %v", err)
	}
	if err := ingress.afterExactCleanup(clientCtx, cleanupResult); err != nil {
		t.Fatalf("afterExactCleanup: %v", err)
	}
	pressure.mu.Lock()
	defer pressure.mu.Unlock()
	if pressure.prepareCleanupCtxErr != nil || pressure.cleanupCtxErr != nil {
		t.Fatalf("proof contexts inherited client cancellation: before=%v after=%v", pressure.prepareCleanupCtxErr, pressure.cleanupCtxErr)
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
	ingress, err := NewStorageIntegrityIngressWithPayloadWriter(orch, nil, sicore.MaterializerNative, writer)
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
	preparer := &rootRecordingPreparer{
		source:     "snode-A",
		claim:      sicore.ClaimOutcome{Category: sicore.OutcomeAccepted, BoundSource: "snode-A"},
		candidates: bpPreparedCandidates(),
		abortFn:    func([]sicore.CandidatePart) { conn.setRows(nil) },
	}
	orch := sicore.NewOrchestrator(submitter, preparer, sicore.OrchestratorConfig{ExpectedSource: "snode-A", Journal: journal})
	ingress, err := NewStorageIntegrityIngressWithPayloadWriter(orch, nil, sicore.MaterializerNative, writer)
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

	freezeSchema := payloadexec.TableSchema{
		TableID: "net1.events", PartitionBy: "region",
		Columns: []lthash.Column{{Name: "region", Type: "UInt64"}},
	}
	ingress.schemas = StorageIntegrityTableSchemaResolverFunc(func(tableID string) (payloadexec.TableSchema, bool) {
		return payloadexec.TableSchema{
			TableID: tableID, PartitionBy: "region",
			Columns: []lthash.Column{{Name: "region", Type: "UInt64"}},
		}, true
	})
	adm = bpAdmission()
	adm.SchemaHash = payloadexec.TableSchemaHash(adm.NetworkID, freezeSchema)
	err = ingress.ConsumeStorageIntegrityAdmission(context.Background(), adm)
	if !errors.Is(err, sicore.ErrPartitionFreeze) || writer.calls != 0 {
		t.Fatalf("freeze err = %v writer calls = %d", err, writer.calls)
	}
}

func TestIngress_ResolverTableIDMismatchFailsClosedBeforeReservationOrPut(t *testing.T) {
	pressure := &fakePartsPressure{}
	ingress, writer, _, _ := newBackpressureIngress(t, pressure)
	ingress.schemas = StorageIntegrityTableSchemaResolverFunc(func(string) (payloadexec.TableSchema, bool) {
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
