package housegate

import (
	"context"
	"errors"
	"strings"
	"testing"

	siplugin "github.com/housegate/housegate/pkg/plugins/storageintegrity"
	"github.com/housegate/housegate/pkg/replay"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

// TestAdmissionRecordFromPlugin_MapsAllFields pins the pure Admission ->
// AdmissionRecord projection: every signed/captured field lands in the right
// core field, byte-for-byte for the payload. Green-today (no companion seam).
func TestAdmissionRecordFromPlugin_MapsAllFields(t *testing.T) {
	adm := siplugin.Admission{
		StatementID: "0xabc:1:n1",
		Kind:        siplugin.KindInsert,
		TableID:     "net1.events",
		SQL:         "INSERT INTO events FORMAT Native",
		SQLHash:     replay.DigestString("INSERT INTO events FORMAT Native"),
		Signer:      "0xabc",
		UserJWS:     "jws",
		Payload: siplugin.CapturedPayload{
			Bytes:    []byte("native-block-bytes"),
			Length:   uint64(len("native-block-bytes")),
			SHA256:   "sha256:deadbeef",
			Encoding: sicore.PayloadEncodingClickHouseNativeData,
			Revision: 54465,
			Complete: true,
		},
	}
	rec := AdmissionRecordFromPlugin(adm)
	if rec.StatementID != adm.StatementID {
		t.Fatalf("statement id: got %q want %q", rec.StatementID, adm.StatementID)
	}
	if rec.Kind != sicore.KindInsert {
		t.Fatalf("kind: got %q want %q", rec.Kind, sicore.KindInsert)
	}
	if rec.TableID != adm.TableID {
		t.Fatalf("table: got %q want %q", rec.TableID, adm.TableID)
	}
	if rec.SQL != adm.SQL {
		t.Fatalf("sql: got %q want %q", rec.SQL, adm.SQL)
	}
	if rec.SQLHash != adm.SQLHash {
		t.Fatalf("sql hash: got %q want %q", rec.SQLHash, adm.SQLHash)
	}
	if rec.Signer != adm.Signer {
		t.Fatalf("signer: got %q want %q", rec.Signer, adm.Signer)
	}
	if rec.UserJWS != adm.UserJWS {
		t.Fatalf("user jws: got %q want %q", rec.UserJWS, adm.UserJWS)
	}
	if string(rec.Payload) != string(adm.Payload.Bytes) {
		t.Fatalf("payload bytes mismatch")
	}
	if rec.PayloadLength != adm.Payload.Length {
		t.Fatalf("payload length: got %d want %d", rec.PayloadLength, adm.Payload.Length)
	}
	if want := replay.DigestBytes(adm.Payload.Bytes); rec.PayloadHash != want {
		t.Fatalf("payload hash: got %q want replay digest %q", rec.PayloadHash, want)
	}
	if rec.Revision != adm.Payload.Revision {
		t.Fatalf("revision: got %d want %d", rec.Revision, adm.Payload.Revision)
	}
	if rec.PayloadEncoding != adm.Payload.Encoding {
		t.Fatalf("payload encoding: got %q want %q", rec.PayloadEncoding, adm.Payload.Encoding)
	}
}

// TestNewStorageIntegrityIngress_RequiresOrchestrator pins that the ingress
// runtime requires an orchestrator and constructs no Verifier/Promoter — the
// struct carries only {orch, guard, matKind}, so verifier selection / quorum /
// manifest publication cannot leak into the HouseGate runtime.
func TestNewStorageIntegrityIngress_RequiresOrchestrator(t *testing.T) {
	if _, err := NewStorageIntegrityIngress(nil, nil, sicore.MaterializerNative); err == nil {
		t.Fatal("nil orchestrator must be a wiring error")
	}
	orch := sicore.NewOrchestrator(nil, nil, sicore.OrchestratorConfig{})
	ing, err := NewStorageIntegrityIngress(orch, nil, sicore.MaterializerNative)
	if err != nil {
		t.Fatalf("valid construction: %v", err)
	}
	if ing.orch != orch {
		t.Fatal("ingress must hold the given orchestrator")
	}
	if ing.matKind != sicore.MaterializerNative {
		t.Fatal("ingress must hold the selected materializer kind")
	}
}

func TestNewStorageIntegrityIngressRejectsUnspecifiedMaterializer(t *testing.T) {
	orch := sicore.NewOrchestrator(nil, nil, sicore.OrchestratorConfig{})
	if _, err := NewStorageIntegrityIngress(orch, nil, sicore.MaterializerUnspecified); err == nil {
		t.Fatal("unspecified materializer must be a wiring error")
	}
}

func TestStorageIntegrityIngressRejectsWrongMaterializerBeforePayloadPut(t *testing.T) {
	writer := &rootRecordingPayloadWriter{}
	submitter := &rootRecordingSubmitter{}
	preparer := &rootRecordingPreparer{}
	orch := sicore.NewOrchestrator(submitter, preparer, sicore.OrchestratorConfig{})
	ing, err := NewStorageIntegrityIngressWithPayloadWriter(orch, nil, sicore.MaterializerCSV, writer)
	if err != nil {
		t.Fatalf("NewStorageIntegrityIngressWithPayloadWriter: %v", err)
	}

	err = ing.ConsumeStorageIntegrityAdmission(context.Background(), storageIntegrityAdmissionForEncoding(
		sicore.PayloadEncodingClickHouseNativeData,
		54465,
	))
	if err == nil || !strings.Contains(err.Error(), "requires CSV materializer") {
		t.Fatalf("ConsumeStorageIntegrityAdmission err = %v, want CSV materializer mismatch", err)
	}
	if writer.calls != 0 || submitter.calls != 0 || preparer.prepareCalls != 0 {
		t.Fatalf("writer/submit/prepare calls = %d/%d/%d, want 0/0/0", writer.calls, submitter.calls, preparer.prepareCalls)
	}
}

func TestStorageIntegrityIngressRejectsCSVWithoutRevisionBeforePayloadPut(t *testing.T) {
	writer := &rootRecordingPayloadWriter{}
	submitter := &rootRecordingSubmitter{}
	preparer := &rootRecordingPreparer{}
	orch := sicore.NewOrchestrator(submitter, preparer, sicore.OrchestratorConfig{})
	ing, err := NewStorageIntegrityIngressWithPayloadWriter(orch, nil, sicore.MaterializerCSV, writer)
	if err != nil {
		t.Fatalf("NewStorageIntegrityIngressWithPayloadWriter: %v", err)
	}

	err = ing.ConsumeStorageIntegrityAdmission(context.Background(), storageIntegrityAdmissionForEncoding(
		sicore.EncodingCSVWithNames,
		0,
	))
	if err == nil || !strings.Contains(err.Error(), "client protocol revision") {
		t.Fatalf("ConsumeStorageIntegrityAdmission err = %v, want missing revision", err)
	}
	if writer.calls != 0 || submitter.calls != 0 || preparer.prepareCalls != 0 {
		t.Fatalf("writer/submit/prepare calls = %d/%d/%d, want 0/0/0", writer.calls, submitter.calls, preparer.prepareCalls)
	}
}

func TestStorageIntegrityIngressRejectsUnknownEncodingBeforePayloadPut(t *testing.T) {
	writer := &rootRecordingPayloadWriter{}
	orch := sicore.NewOrchestrator(&rootRecordingSubmitter{}, &rootRecordingPreparer{}, sicore.OrchestratorConfig{})
	ing, err := NewStorageIntegrityIngressWithPayloadWriter(orch, nil, sicore.MaterializerCSV, writer)
	if err != nil {
		t.Fatalf("NewStorageIntegrityIngressWithPayloadWriter: %v", err)
	}

	err = ing.ConsumeStorageIntegrityAdmission(context.Background(), storageIntegrityAdmissionForEncoding("future-encoding-v2", 54465))
	if err == nil || !strings.Contains(err.Error(), "unrecognized payload encoding") {
		t.Fatalf("ConsumeStorageIntegrityAdmission err = %v, want unknown encoding", err)
	}
	if writer.calls != 0 {
		t.Fatalf("payload writer calls = %d, want 0", writer.calls)
	}
}

func storageIntegrityAdmissionForEncoding(encoding string, revision int) siplugin.Admission {
	sql := "INSERT INTO events FORMAT CSVWithNames"
	if encoding == sicore.PayloadEncodingClickHouseNativeData {
		sql = "INSERT INTO events FORMAT Native"
	}
	payload := []byte("id,region\n1,eu\n")
	return siplugin.Admission{
		StatementID: "0xabc:1:n1",
		Kind:        siplugin.KindInsert,
		TableID:     "net1.events",
		SQL:         sql,
		SQLHash:     replay.DigestString(sql),
		Signer:      "0xabc",
		UserJWS:     "jws",
		Payload: siplugin.CapturedPayload{
			Bytes:    payload,
			Length:   uint64(len(payload)),
			Encoding: encoding,
			Revision: revision,
			Complete: true,
		},
	}
}

func TestStorageIntegrityIngress_PutsPayloadBeforeOrchestrate(t *testing.T) {
	pluginAdmission := siplugin.Admission{
		StatementID: "0xabc:1:n1",
		Kind:        siplugin.KindInsert,
		TableID:     "net1.events",
		SQL:         "INSERT INTO events FORMAT Native",
		SQLHash:     replay.DigestString("INSERT INTO events FORMAT Native"),
		Signer:      "0xabc",
		UserJWS:     "jws",
		Payload: siplugin.CapturedPayload{
			Bytes:    []byte("native-block-bytes"),
			Length:   uint64(len("native-block-bytes")),
			SHA256:   "sha256:legacy-plugin-digest",
			Encoding: sicore.PayloadEncodingClickHouseNativeData,
			Revision: 54465,
			Complete: true,
		},
	}
	writer := &rootRecordingPayloadWriter{
		result: sicore.PayloadPutResult{
			PayloadRef: "payload://store/ref-1",
			State:      sicore.PayloadStateAvailable,
		},
	}
	submitter := &rootRecordingSubmitter{
		outcome: sicore.SubmitOutcome{Category: sicore.OutcomeAccepted},
	}
	preparer := &rootRecordingPreparer{
		source: "snode-A",
		claim:  sicore.ClaimOutcome{Category: sicore.OutcomeAccepted, BoundSource: "snode-A"},
	}
	orch := sicore.NewOrchestrator(submitter, preparer, sicore.OrchestratorConfig{ExpectedSource: "snode-A"})
	ing, err := NewStorageIntegrityIngressWithPayloadWriter(orch, nil, sicore.MaterializerNative, writer)
	if err != nil {
		t.Fatalf("NewStorageIntegrityIngressWithPayloadWriter: %v", err)
	}

	if err := ing.ConsumeStorageIntegrityAdmission(context.Background(), pluginAdmission); err != nil {
		t.Fatalf("ConsumeStorageIntegrityAdmission: %v", err)
	}

	wantHash := replay.DigestBytes(pluginAdmission.Payload.Bytes)
	if writer.calls != 1 {
		t.Fatalf("payload writer calls = %d, want 1", writer.calls)
	}
	if submitter.calls != 1 {
		t.Fatalf("submitter calls = %d, want 1", submitter.calls)
	}
	if preparer.prepareCalls != 1 {
		t.Fatalf("preparer prepare calls = %d, want 1", preparer.prepareCalls)
	}
	if writer.hash != wantHash || writer.length != pluginAdmission.Payload.Length || string(writer.payload) != string(pluginAdmission.Payload.Bytes) {
		t.Fatalf("payload writer saw %q/%d/%q", writer.hash, writer.length, writer.payload)
	}
	if submitter.env.PayloadRef != writer.result.PayloadRef {
		t.Fatalf("submit payload_ref = %q want %q", submitter.env.PayloadRef, writer.result.PayloadRef)
	}
	if preparer.env.PayloadRef != writer.result.PayloadRef {
		t.Fatalf("prepare payload_ref = %q want %q", preparer.env.PayloadRef, writer.result.PayloadRef)
	}
	if submitter.env.PayloadHash != wantHash || preparer.env.PayloadHash != wantHash {
		t.Fatalf("payload hash did not stay bound to captured bytes")
	}
}

type unhealthyMergeGuard struct {
	err error
}

func (g *unhealthyMergeGuard) AssertStopMerges(context.Context) error {
	return g.err
}

func (g *unhealthyMergeGuard) CheckMergeHealth() error {
	return g.err
}

func TestStorageIntegrityIngressRejectsAdmissionWhenMergeHealthClosed(t *testing.T) {
	pluginAdmission := siplugin.Admission{
		StatementID: "0xabc:1:n1",
		Kind:        siplugin.KindInsert,
		TableID:     "net1.events",
		SQL:         "INSERT INTO events FORMAT Native",
		SQLHash:     replay.DigestString("INSERT INTO events FORMAT Native"),
		Signer:      "0xabc",
		UserJWS:     "jws",
		Payload: siplugin.CapturedPayload{
			Bytes:    []byte("native-block-bytes"),
			Length:   uint64(len("native-block-bytes")),
			Encoding: sicore.PayloadEncodingClickHouseNativeData,
			Revision: 54465,
			Complete: true,
		},
	}
	writer := &rootRecordingPayloadWriter{result: sicore.PayloadPutResult{PayloadRef: "payload://store/ref-1"}}
	submitter := &rootRecordingSubmitter{outcome: sicore.SubmitOutcome{Category: sicore.OutcomeAccepted}}
	preparer := &rootRecordingPreparer{
		source: "snode-A",
		claim:  sicore.ClaimOutcome{Category: sicore.OutcomeAccepted, BoundSource: "snode-A"},
	}
	orch := sicore.NewOrchestrator(submitter, preparer, sicore.OrchestratorConfig{ExpectedSource: "snode-A"})
	healthErr := errors.New("merge guard reconnect failed")
	ing, err := NewStorageIntegrityIngressWithPayloadWriter(
		orch,
		&unhealthyMergeGuard{err: healthErr},
		sicore.MaterializerNative,
		writer,
	)
	if err != nil {
		t.Fatalf("NewStorageIntegrityIngressWithPayloadWriter: %v", err)
	}

	err = ing.ConsumeStorageIntegrityAdmission(context.Background(), pluginAdmission)
	if err == nil || !strings.Contains(err.Error(), healthErr.Error()) {
		t.Fatalf("ConsumeStorageIntegrityAdmission err = %v, want merge health failure", err)
	}
	if writer.calls != 0 {
		t.Fatalf("payload writer calls = %d, want 0", writer.calls)
	}
	if submitter.calls != 0 || preparer.prepareCalls != 0 {
		t.Fatalf("submit/prepare calls = %d/%d, want 0/0", submitter.calls, preparer.prepareCalls)
	}
}

type rootRecordingPayloadWriter struct {
	calls   int
	payload []byte
	hash    string
	length  uint64
	result  sicore.PayloadPutResult
	err     error
}

func (w *rootRecordingPayloadWriter) PutPayload(_ context.Context, payload []byte, payloadHash string, payloadLength uint64) (sicore.PayloadPutResult, error) {
	w.calls++
	w.payload = append([]byte(nil), payload...)
	w.hash = payloadHash
	w.length = payloadLength
	return w.result, w.err
}

type rootRecordingSubmitter struct {
	calls   int
	env     sicore.StatementEnvelope
	outcome sicore.SubmitOutcome
	err     error
}

func (s *rootRecordingSubmitter) SubmitStatement(_ context.Context, env sicore.StatementEnvelope) (sicore.SubmitOutcome, error) {
	s.calls++
	s.env = env
	return s.outcome, s.err
}

type rootRecordingPreparer struct {
	prepareCalls int
	env          sicore.StatementEnvelope
	source       string
	claim        sicore.ClaimOutcome
	err          error
}

func (p *rootRecordingPreparer) PrepareLocalStatement(_ context.Context, env sicore.StatementEnvelope, _ []byte) (sicore.PreparedLocalResult, error) {
	p.prepareCalls++
	p.env = env
	if p.err != nil {
		return sicore.PreparedLocalResult{}, p.err
	}
	return sicore.PreparedLocalResult{
		StatementID:     env.StatementID,
		SourceNode:      p.source,
		PayloadRef:      env.PayloadRef,
		PayloadHash:     env.PayloadHash,
		PayloadLength:   env.PayloadLength,
		PayloadEncoding: env.PayloadEncoding,
		Revision:        env.Revision,
		Lifecycle:       sicore.LifecycleUnsafeWritten,
	}, nil
}

func (p *rootRecordingPreparer) RegisterPreparedClaim(context.Context, string) (sicore.ClaimOutcome, error) {
	return p.claim, nil
}

func (p *rootRecordingPreparer) AbortPreparedStatement(context.Context, string, []sicore.CandidatePart, string) error {
	return nil
}

func (p *rootRecordingPreparer) LookupPreparedStatement(context.Context, string) (sicore.PreparedLocalResult, bool, error) {
	return sicore.PreparedLocalResult{}, false, nil
}

type rootPreparerWithoutLookup struct{}

func (p *rootPreparerWithoutLookup) PrepareLocalStatement(_ context.Context, env sicore.StatementEnvelope, _ []byte) (sicore.PreparedLocalResult, error) {
	return sicore.PreparedLocalResult{
		StatementID:     env.StatementID,
		PayloadRef:      env.PayloadRef,
		PayloadHash:     env.PayloadHash,
		PayloadLength:   env.PayloadLength,
		PayloadEncoding: env.PayloadEncoding,
		Revision:        env.Revision,
		Lifecycle:       sicore.LifecycleUnsafeWritten,
	}, nil
}

func (p *rootPreparerWithoutLookup) RegisterPreparedClaim(context.Context, string) (sicore.ClaimOutcome, error) {
	return sicore.ClaimOutcome{}, nil
}

func (p *rootPreparerWithoutLookup) AbortPreparedStatement(context.Context, string, []sicore.CandidatePart, string) error {
	return nil
}

type rootRecordingStatusQuerier struct{}

func (rootRecordingStatusQuerier) QuerySubmitStatus(context.Context, string) (sicore.SubmitOutcome, error) {
	return sicore.SubmitOutcome{Category: sicore.OutcomeUnspecified}, nil
}

func (rootRecordingStatusQuerier) QueryClaimStatus(context.Context, string) (sicore.ClaimOutcome, error) {
	return sicore.ClaimOutcome{Category: sicore.OutcomeUnspecified}, nil
}
