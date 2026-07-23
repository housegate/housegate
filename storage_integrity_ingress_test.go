package housegate

import (
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
	if rec.PayloadHash != adm.Payload.SHA256 {
		t.Fatalf("payload hash: got %q want %q", rec.PayloadHash, adm.Payload.SHA256)
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
