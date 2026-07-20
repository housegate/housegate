package housegate

import (
	"testing"

	siplugin "housegate/housegate/pkg/plugins/storageintegrity"
	sicore "housegate/housegate/pkg/storageintegrity"
)

// TestAdmissionRecordFromPlugin_MapsAllFields pins the pure Admission ->
// AdmissionRecord projection: every signed/captured field lands in the right
// core field, byte-for-byte for the payload. Green-today (no companion seam).
func TestAdmissionRecordFromPlugin_MapsAllFields(t *testing.T) {
	adm := siplugin.Admission{
		StatementID: "q-1",
		Kind:        siplugin.KindInsert,
		TableID:     "net1.events",
		SQL:         "INSERT INTO events VALUES",
		Signer:      "0xabc",
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
	if rec.Signer != adm.Signer {
		t.Fatalf("signer: got %q want %q", rec.Signer, adm.Signer)
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

// TestAdmissionRecordFromPlugin_MapsKinds confirms UPDATE/DELETE kinds carry
// through, so a later mutation runtime sees the right core kind.
func TestAdmissionRecordFromPlugin_MapsKinds(t *testing.T) {
	cases := []struct {
		in   siplugin.Kind
		want sicore.Kind
	}{
		{siplugin.KindInsert, sicore.KindInsert},
		{siplugin.KindUpdate, sicore.KindUpdate},
		{siplugin.KindDelete, sicore.KindDelete},
	}
	for _, tc := range cases {
		rec := AdmissionRecordFromPlugin(siplugin.Admission{StatementID: "q", Kind: tc.in, TableID: "t"})
		if rec.Kind != tc.want {
			t.Fatalf("kind %q -> %q, want %q", tc.in, rec.Kind, tc.want)
		}
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
