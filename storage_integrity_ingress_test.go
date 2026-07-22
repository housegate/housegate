package housegate

import (
	"context"
	"testing"

	siplugin "housegate/housegate/pkg/plugins/storageintegrity"
	"housegate/housegate/pkg/replay"
	sicore "housegate/housegate/pkg/storageintegrity"
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
	if rec.PayloadHash != adm.Payload.SHA256 {
		t.Fatalf("payload hash: got %q want %q", rec.PayloadHash, adm.Payload.SHA256)
	}
	if rec.Revision != adm.Payload.Revision {
		t.Fatalf("revision: got %d want %d", rec.Revision, adm.Payload.Revision)
	}
	if rec.PayloadEncoding == "" {
		t.Fatal("payload encoding must be set so the runtime can pick a materializer")
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

func TestNewStorageIntegrityIngressFromPorts_DrivesAdmissionToAck2(t *testing.T) {
	prep := &ingressPreparer{
		prepared: sicore.PreparedLocalResult{
			SourceNode:      "snode-A",
			PayloadRef:      "sha256:deadbeef",
			PayloadHash:     "sha256:deadbeef",
			PayloadLength:   uint64(len("native-block-bytes")),
			PayloadEncoding: sicore.PayloadEncodingClickHouseNativeData,
			Revision:        54465,
			SourceClaimRoot: "root-1",
			Lifecycle:       sicore.LifecycleUnsafeWritten,
		},
		claim: sicore.ClaimOutcome{Category: sicore.OutcomeAccepted, BoundSource: "snode-A"},
	}
	ing, err := NewStorageIntegrityIngressFromPorts(StorageIntegrityIngressConfig{
		Submitter:        ingressSubmitter{outcome: sicore.SubmitOutcome{Category: sicore.OutcomeAccepted}},
		Preparer:         prep,
		ExpectedSource:   "snode-A",
		MaterializerKind: sicore.MaterializerNative,
	})
	if err != nil {
		t.Fatalf("NewStorageIntegrityIngressFromPorts: %v", err)
	}
	if err := ing.ConsumeStorageIntegrityAdmission(context.Background(), storageIntegrityAdmissionFixture()); err != nil {
		t.Fatalf("ConsumeStorageIntegrityAdmission: %v", err)
	}
	if prep.registered != storageIntegrityAdmissionFixture().StatementID {
		t.Fatalf("registered statement = %q", prep.registered)
	}
}

func TestNewStorageIntegrityIngressFromPorts_RequiresPorts(t *testing.T) {
	if _, err := NewStorageIntegrityIngressFromPorts(StorageIntegrityIngressConfig{}); err == nil {
		t.Fatal("missing submitter/preparer accepted")
	}
}

type ingressSubmitter struct {
	outcome sicore.SubmitOutcome
}

func (s ingressSubmitter) SubmitStatement(context.Context, sicore.StatementEnvelope) (sicore.SubmitOutcome, error) {
	return s.outcome, nil
}

type ingressPreparer struct {
	prepared   sicore.PreparedLocalResult
	claim      sicore.ClaimOutcome
	registered string
}

func (p *ingressPreparer) PrepareLocalStatement(_ context.Context, env sicore.StatementEnvelope, _ []byte) (sicore.PreparedLocalResult, error) {
	prepared := p.prepared
	prepared.StatementID = env.StatementID
	return prepared, nil
}

func (p *ingressPreparer) RegisterPreparedClaim(_ context.Context, statementID string) (sicore.ClaimOutcome, error) {
	p.registered = statementID
	return p.claim, nil
}

func (p *ingressPreparer) AbortPreparedStatement(context.Context, string, []sicore.CandidatePart, string) error {
	return nil
}

func storageIntegrityAdmissionFixture() siplugin.Admission {
	sql := "INSERT INTO events FORMAT Native"
	return siplugin.Admission{
		StatementID: "0xabc:1:n1",
		Kind:        siplugin.KindInsert,
		TableID:     "net1.events",
		SQL:         sql,
		SQLHash:     replay.DigestString(sql),
		Signer:      "0xabc",
		UserJWS:     "jws",
		Payload: siplugin.CapturedPayload{
			Bytes:    []byte("native-block-bytes"),
			Length:   uint64(len("native-block-bytes")),
			SHA256:   "sha256:deadbeef",
			Revision: 54465,
			Complete: true,
		},
	}
}
