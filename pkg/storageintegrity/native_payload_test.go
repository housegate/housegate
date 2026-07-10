package storageintegrity

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/ClickHouse/ch-go/proto"

	"housegate/housegate/pkg/replay"
	"housegate/housegate/pkg/replay/payloadexec"
)

const nativePayloadTestRevision = 54453

func TestNativePayloadClaimDrivesReplayVerifier(t *testing.T) {
	payload := encodeNativePayload(t, proto.Input{
		{Name: "id", Data: &proto.ColUInt64{1, 2}},
		{Name: "label", Data: newColStr("alpha", "beta")},
	})

	claim, err := ComputeNativePayloadClaim("tenant.events", nativePayloadTestRevision, payload)
	if err != nil {
		t.Fatalf("ComputeNativePayloadClaim: %v", err)
	}
	if claim.SourceClaimRoot == "" || claim.RowCount != 2 {
		t.Fatalf("claim = %+v", claim)
	}
	if claim.PayloadEncoding != PayloadEncodingClickHouseNativeData {
		t.Fatalf("payload encoding = %q", claim.PayloadEncoding)
	}

	snap, err := NativePayloadGenesisSnapshot("tenant.events", claim.Columns)
	if err != nil {
		t.Fatalf("NativePayloadGenesisSnapshot: %v", err)
	}
	store := fakeNativeSnapshotStore{snap.SnapshotID: snap}

	seed := make([]byte, ed25519.SeedSize)
	seed[0] = 11
	signer, err := payloadexec.NewEd25519Signer("native-replay-worker", seed)
	if err != nil {
		t.Fatalf("signer: %v", err)
	}
	verifier := &replay.Verifier{
		Snapshots: store,
		Payloads:  fakeNativePayloadStore{"payload-1": payload},
		Executor:  NativePayloadExecutor{Revision: nativePayloadTestRevision},
		Signer:    signer,
	}

	// HG-P0-01: the source claim root is the unified state root the submit path
	// computes via replay.AssembleStateRoot — base partition roots from the
	// pinned snapshot plus this statement's candidate part_row_lthash. The
	// verifier's NativePayloadExecutor.Replay derives ComputedStateRoot through
	// the identical assembly, so the two must reconcile byte-for-byte.
	wantSourceRoot, err := NativeSourceClaimRoot(snap, "tenant.events", claim.PartRowLtHash)
	if err != nil {
		t.Fatalf("NativeSourceClaimRoot: %v", err)
	}
	job := replay.ReplayJob{
		BlockSeq:           1,
		PrevSafeSnapshotID: snap.SnapshotID,
		PrevStateRoot:      snap.StateRoot,
		SchemaSnapshotID:   snap.SchemaSnapshotID,
		ExecutorProfileID:  snap.ExecutorProfileID,
		SourceClaimRoot:    wantSourceRoot,
		Statements: []replay.Statement{{
			StatementID:   "stmt-1",
			StatementSeq:  1,
			SQL:           "INSERT INTO hg_unsafe.events",
			SQLHash:       replay.DigestString("INSERT INTO hg_unsafe.events"),
			SettingsHash:  replay.DigestString("settings"),
			PayloadRef:    "payload-1",
			PayloadHash:   replay.DigestBytes(payload),
			PayloadLength: uint64(len(payload)),
			TargetTableID: "tenant.events",
		}},
	}

	att, err := verifier.Verify(context.Background(), job)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if !att.MatchSourceRoot {
		t.Fatalf("attestation did not match source root: %+v", att.Receipt)
	}
	if att.Receipt.ComputedStateRoot != wantSourceRoot {
		t.Fatalf("computed root = %s, want %s", att.Receipt.ComputedStateRoot, wantSourceRoot)
	}
	if len(att.Receipt.AffectedParts) != 1 {
		t.Fatalf("affected parts = %+v", att.Receipt.AffectedParts)
	}
}

func TestNativePayloadClaimDecodesClickHouseGoClientData(t *testing.T) {
	payloadHex := "" +
		"0200010002ffffffff0000000200010002ffffffff0002010269640655496e74" +
		"3634000100000000000000056c6162656c06537472696e67001074656e616e74" +
		"5f6532655f612d726f770200010002ffffffff000000"
	payload, err := hex.DecodeString(payloadHex)
	if err != nil {
		t.Fatalf("DecodeString: %v", err)
	}

	claim, err := ComputeNativePayloadClaim("tenant_e2e_a.events", 54460, payload)
	if err != nil {
		t.Fatalf("ComputeNativePayloadClaim: %v", err)
	}
	if claim.RowCount != 1 || claim.SourceClaimRoot == "" {
		t.Fatalf("claim = %+v", claim)
	}
	if len(claim.Columns) != 2 || claim.Columns[0].Name != "id" || claim.Columns[1].Name != "label" {
		t.Fatalf("columns = %+v", claim.Columns)
	}
}

func TestInjectNativeRowIDsLeavesEmptyDataBlockUnchanged(t *testing.T) {
	raw := encodeEmptyNativeDataPacket(t)

	rewritten, rows, err := InjectNativeRowIDs("sentio", "tenant.events", "stmt-1", nativePayloadTestRevision, raw, 0)
	if err != nil {
		t.Fatalf("InjectNativeRowIDs: %v", err)
	}
	if rows != 0 {
		t.Fatalf("rows = %d, want 0", rows)
	}
	if string(rewritten) != string(raw) {
		t.Fatalf("empty native Data packet was rewritten: got %x want %x", rewritten, raw)
	}
}

func TestStripNativeRowIDFromServerDataSample(t *testing.T) {
	raw := encodeNativePacket(t, uint64(proto.ServerCodeData), proto.Input{
		{Name: "_hg_row_id", Data: &proto.ColFixedStr{Size: 32}},
		{Name: "id", Data: &proto.ColUInt64{}},
		{Name: "label", Data: newColStr()},
	}, 0)

	rewritten, stripped, err := StripNativeRowIDFromServerData(raw, nativePayloadTestRevision)
	if err != nil {
		t.Fatalf("StripNativeRowIDFromServerData: %v", err)
	}
	if !stripped {
		t.Fatal("stripped = false, want true")
	}

	reader := proto.NewReader(bytes.NewReader(rewritten))
	code, err := reader.UVarInt()
	if err != nil {
		t.Fatalf("packet code: %v", err)
	}
	if code != uint64(proto.ServerCodeData) {
		t.Fatalf("packet code = %d, want ServerCodeData", code)
	}
	if _, err := reader.Str(); err != nil {
		t.Fatalf("block name: %v", err)
	}
	var (
		results proto.Results
		block   proto.Block
	)
	if err := block.DecodeBlock(reader, nativePayloadTestRevision, results.Auto()); err != nil {
		t.Fatalf("decode rewritten sample: %v", err)
	}
	if block.Rows != 0 || len(results) != 2 {
		t.Fatalf("block rows=%d columns=%+v, want 0 rows and id,label", block.Rows, results)
	}
	if results[0].Name != "id" || results[1].Name != "label" {
		t.Fatalf("columns = %+v, want id,label", results)
	}
}

func encodeNativePayload(t *testing.T, input proto.Input) []byte {
	t.Helper()
	return encodeNativePacket(t, uint64(proto.ClientCodeData), input, input[0].Data.Rows())
}

func encodeEmptyNativeDataPacket(t *testing.T) []byte {
	t.Helper()
	return encodeNativePacket(t, uint64(proto.ClientCodeData), nil, 0)
}

func encodeNativePacket(t *testing.T, code uint64, input proto.Input, rows int) []byte {
	t.Helper()
	var buf proto.Buffer
	buf.PutUVarInt(code)
	buf.PutString("")
	if err := (proto.Block{Rows: rows, Columns: len(input)}).EncodeBlock(&buf, nativePayloadTestRevision, input); err != nil {
		t.Fatalf("encode block: %v", err)
	}
	return buf.Buf
}

func newColStr(values ...string) *proto.ColStr {
	col := new(proto.ColStr)
	for _, value := range values {
		col.Append(value)
	}
	return col
}

type fakeNativeSnapshotStore map[string]replay.SafeSnapshotManifest

func (s fakeNativeSnapshotStore) GetSafeSnapshot(_ context.Context, snapshotID string) (replay.SafeSnapshotManifest, error) {
	return s[snapshotID], nil
}

type fakeNativePayloadStore map[string][]byte

func (s fakeNativePayloadStore) GetPayload(_ context.Context, ref string) ([]byte, error) {
	return append([]byte(nil), s[ref]...), nil
}

// TestValidateNativePayloadDecodable proves HG-P2-01: a payload with only
// supported scalar types validates, while a payload using an out-of-whitelist
// column type is rejected explicitly with ErrNativePayloadUnsupported rather
// than an opaque decode failure surfaced later at claim time.
func TestValidateNativePayloadDecodable(t *testing.T) {
	ok := encodeNativePayload(t, proto.Input{
		{Name: "id", Data: &proto.ColUInt64{1}},
		{Name: "label", Data: newColStr("alpha")},
	})
	if err := ValidateNativePayloadDecodable("tenant.events", nativePayloadTestRevision, ok); err != nil {
		t.Fatalf("supported scalar payload must validate: %v", err)
	}

	// Int128 is not on the supported column whitelist.
	bad := encodeNativePayload(t, proto.Input{
		{Name: "id", Data: &proto.ColUInt64{1}},
		{Name: "big", Data: &proto.ColInt128{proto.Int128{Low: 1}}},
	})
	err := ValidateNativePayloadDecodable("tenant.events", nativePayloadTestRevision, bad)
	if err == nil || !errors.Is(err, ErrNativePayloadUnsupported) {
		t.Fatalf("unsupported column type must be rejected with ErrNativePayloadUnsupported, got %v", err)
	}
}
