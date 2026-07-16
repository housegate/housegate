package storageintegrity

import (
	"context"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/ClickHouse/ch-go/proto"

	"housegate/housegate/pkg/lthash"
	"housegate/housegate/pkg/replay"
	"housegate/housegate/pkg/replay/payloadexec"
)

const nativePayloadTestRevision = 54453

func nativeMaterializerSchema() payloadexec.TableSchema {
	return payloadexec.TableSchema{
		TableID: "tenant.events",
		Columns: []lthash.Column{
			{Name: "id", Type: "UInt64"},
			{Name: "region", Type: "String"},
		},
		PartitionBy: "region",
	}
}

func TestDecodeNativePayloadMaterializesPinnedSchemaRows(t *testing.T) {
	schema := nativeMaterializerSchema()
	payload := encodeNativePayload(t, proto.Input{
		{Name: "region", Data: newColStr("eu", "us")},
		{Name: "id", Data: &proto.ColUInt64{1, 2}},
	})

	rows, err := DecodeNativePayload(schema, nativePayloadTestRevision, payload)
	if err != nil {
		t.Fatalf("DecodeNativePayload: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(rows))
	}
	if rows[0].RowID != nil || rows[1].RowID != nil {
		t.Fatal("DecodeNativePayload must leave RowID injection to the materializer")
	}
	if rows[0].PartitionID != "p_eu" || rows[1].PartitionID != "p_us" {
		t.Fatalf("partition ids = %q/%q, want p_eu/p_us", rows[0].PartitionID, rows[1].PartitionID)
	}
	if got, want := rows[0].Values, []any{uint64(1), "eu"}; got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("row 0 values = %#v, want %#v", got, want)
	}
	if got, want := rows[1].Values, []any{uint64(2), "us"}; got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("row 1 values = %#v, want %#v", got, want)
	}
}

func TestNativeMaterializerInjectsSharedRowIDsAcrossBlocks(t *testing.T) {
	schema := nativeMaterializerSchema()
	block1 := encodeNativePayload(t, proto.Input{
		{Name: "region", Data: newColStr("eu")},
		{Name: "id", Data: &proto.ColUInt64{1}},
	})
	block2 := encodeNativePayload(t, proto.Input{
		{Name: "region", Data: newColStr("us")},
		{Name: "id", Data: &proto.ColUInt64{2}},
	})
	payload := append(append([]byte{}, block1...), block2...)
	statement := replay.PreparedStatement{
		Statement: replay.Statement{
			StatementID:   "stmt-native-1",
			StatementSeq:  1,
			PayloadRef:    "payload-native-1",
			TargetTableID: schema.TableID,
		},
		Payload: payload,
	}

	rows, err := (NativeMaterializer{NetworkID: "network-1", Revision: nativePayloadTestRevision}).Materialize(context.Background(), schema, statement)
	if err != nil {
		t.Fatalf("Materialize: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(rows))
	}
	for ordinal := range rows {
		want := payloadexec.RowID("network-1", schema.TableID, statement.StatementID, uint64(ordinal))
		if hex.EncodeToString(rows[ordinal].RowID) != hex.EncodeToString(want) {
			t.Fatalf("row %d RowID = %x, want %x", ordinal, rows[ordinal].RowID, want)
		}
	}
}

func TestDecodeNativePayloadRejectsPinnedSchemaMismatch(t *testing.T) {
	schema := nativeMaterializerSchema()
	withInjectedRowID := encodeNativePayload(t, proto.Input{
		{Name: "_hg_row_id", Data: newColFixedStr32("row-1")},
		{Name: "region", Data: newColStr("eu")},
		{Name: "id", Data: &proto.ColUInt64{1}},
	})
	if _, err := DecodeNativePayload(schema, nativePayloadTestRevision, withInjectedRowID); err == nil {
		t.Fatal("DecodeNativePayload must reject payloads that include reserved _hg_row_id")
	}

	wrongType := encodeNativePayload(t, proto.Input{
		{Name: "region", Data: newColStr("eu")},
		{Name: "id", Data: newColStr("1")},
	})
	if _, err := DecodeNativePayload(schema, nativePayloadTestRevision, wrongType); err == nil {
		t.Fatal("DecodeNativePayload must reject unchanged column names with different Native types")
	}

	missingColumn := encodeNativePayload(t, proto.Input{{Name: "id", Data: &proto.ColUInt64{1}}})
	if _, err := DecodeNativePayload(schema, nativePayloadTestRevision, missingColumn); err == nil {
		t.Fatal("DecodeNativePayload must reject payloads missing pinned schema columns")
	}
}

func TestValidateNativePayloadDecodable(t *testing.T) {
	schema := nativeMaterializerSchema()
	ok := encodeNativePayload(t, proto.Input{
		{Name: "region", Data: newColStr("eu")},
		{Name: "id", Data: &proto.ColUInt64{1}},
	})
	if err := ValidateNativePayloadDecodable(schema, nativePayloadTestRevision, ok); err != nil {
		t.Fatalf("supported scalar payload must validate: %v", err)
	}

	bad := encodeNativePayload(t, proto.Input{
		{Name: "region", Data: newColStr("eu")},
		{Name: "id", Data: &proto.ColInt128{proto.Int128{Low: 1}}},
	})
	err := ValidateNativePayloadDecodable(schema, nativePayloadTestRevision, bad)
	if err == nil || !errors.Is(err, ErrNativePayloadUnsupported) {
		t.Fatalf("unsupported column type must be rejected with ErrNativePayloadUnsupported, got %v", err)
	}
}

func TestNativePayloadRejectsUndecodableBlock(t *testing.T) {
	err := ValidateNativePayloadDecodable(nativeMaterializerSchema(), nativePayloadTestRevision, []byte{byte(proto.ClientCodeData)})
	if err == nil || !errors.Is(err, ErrNativePayloadUnsupported) {
		t.Fatalf("undecodable native payload must be rejected with ErrNativePayloadUnsupported, got %v", err)
	}
}

func TestNativePayloadRejectsEmptyStream(t *testing.T) {
	err := ValidateNativePayloadDecodable(nativeMaterializerSchema(), nativePayloadTestRevision, nil)
	if err == nil || !errors.Is(err, ErrNativePayloadUnsupported) {
		t.Fatalf("empty native payload must be rejected with ErrNativePayloadUnsupported, got %v", err)
	}
}

func TestNativeMaterializerRejectsStatementWithoutPayload(t *testing.T) {
	schema := nativeMaterializerSchema()
	_, err := (NativeMaterializer{NetworkID: "network-1", Revision: nativePayloadTestRevision}).Materialize(
		context.Background(),
		schema,
		replay.PreparedStatement{Statement: replay.Statement{StatementID: "stmt-mutation", TargetTableID: schema.TableID}},
	)
	if err == nil {
		t.Fatal("NativeMaterializer must reject mutation/DDL-class statements without a captured payload")
	}
}

func TestNativeMaterializerRejectsTargetSchemaMismatch(t *testing.T) {
	schema := nativeMaterializerSchema()
	payload := encodeNativePayload(t, proto.Input{
		{Name: "region", Data: newColStr("eu")},
		{Name: "id", Data: &proto.ColUInt64{1}},
	})
	_, err := (NativeMaterializer{NetworkID: "network-1", Revision: nativePayloadTestRevision}).Materialize(
		context.Background(),
		schema,
		replay.PreparedStatement{
			Statement: replay.Statement{
				StatementID:   "stmt-native-1",
				StatementSeq:  1,
				PayloadRef:    "payload-native-1",
				TargetTableID: "tenant.other",
			},
			Payload: payload,
		},
	)
	if err == nil {
		t.Fatal("NativeMaterializer must reject statements whose target table does not match the pinned schema")
	}
}

func TestNativeMaterializerMatchesCSVExecutorProfile(t *testing.T) {
	networkID := "network-1"
	schema := nativeMaterializerSchema()
	csvPayload := []byte("id,region\n1,eu\n2,us\n")
	nativePayload := encodeNativePayload(t, proto.Input{
		{Name: "region", Data: newColStr("eu", "us")},
		{Name: "id", Data: &proto.ColUInt64{1, 2}},
	})

	csvRoot, csvCommitments, csvParts := applySharedPayloadExecutor(t,
		payloadexec.New(networkID, schema),
		schema.TableID,
		"stmt-shared-1",
		csvPayload,
	)
	nativeRoot, nativeCommitments, nativeParts := applySharedPayloadExecutor(t,
		payloadexec.NewWithMaterializer(networkID, NativeMaterializer{NetworkID: networkID, Revision: nativePayloadTestRevision}, schema),
		schema.TableID,
		"stmt-shared-1",
		nativePayload,
	)

	if nativeRoot != csvRoot {
		t.Fatalf("native state root = %s, want CSV root %s", nativeRoot, csvRoot)
	}
	if len(nativeCommitments) != len(csvCommitments) {
		t.Fatalf("native commitments = %+v, want %+v", nativeCommitments, csvCommitments)
	}
	for i := range csvCommitments {
		if nativeCommitments[i] != csvCommitments[i] {
			t.Fatalf("commitment %d = %+v, want %+v", i, nativeCommitments[i], csvCommitments[i])
		}
	}
	if len(nativeParts) != len(csvParts) {
		t.Fatalf("native parts = %+v, want %+v", nativeParts, csvParts)
	}
	for i := range csvParts {
		if nativeParts[i].TableID != csvParts[i].TableID ||
			nativeParts[i].PartitionID != csvParts[i].PartitionID ||
			nativeParts[i].PartRowLtHash != csvParts[i].PartRowLtHash ||
			nativeParts[i].RowCount != csvParts[i].RowCount {
			t.Fatalf("part %d = %+v, want table/partition/lthash/rows from %+v", i, nativeParts[i], csvParts[i])
		}
	}
}

func TestNativeMaterializerPopulatesDeterministicPartBytes(t *testing.T) {
	networkID := "network-1"
	schema := nativeMaterializerSchema()
	nativePayload := encodeNativePayload(t, proto.Input{
		{Name: "region", Data: newColStr("eu", "us")},
		{Name: "id", Data: &proto.ColUInt64{1, 2}},
	})

	rows, err := DecodeNativePayload(schema, nativePayloadTestRevision, nativePayload)
	if err != nil {
		t.Fatalf("DecodeNativePayload: %v", err)
	}
	if rows[0].RawBytes != 10 || rows[1].RawBytes != 10 {
		t.Fatalf("row RawBytes = %d/%d, want 10/10", rows[0].RawBytes, rows[1].RawBytes)
	}

	_, _, parts := applySharedPayloadExecutor(t,
		payloadexec.NewWithMaterializer(networkID, NativeMaterializer{NetworkID: networkID, Revision: nativePayloadTestRevision}, schema),
		schema.TableID,
		"stmt-native-bytes",
		nativePayload,
	)
	if len(parts) != 2 {
		t.Fatalf("affected parts = %+v, want 2 partition parts", parts)
	}
	for _, part := range parts {
		if part.Bytes != 10 {
			t.Fatalf("part %s bytes = %d, want 10", part.PartitionID, part.Bytes)
		}
	}
}

func TestNativePayloadDecodesClickHouseGoClientData(t *testing.T) {
	payloadHex := "" +
		"0200010002ffffffff0000000200010002ffffffff0002010269640655496e74" +
		"3634000100000000000000056c6162656c06537472696e67001074656e616e74" +
		"5f6532655f612d726f770200010002ffffffff000000"
	payload, err := hex.DecodeString(payloadHex)
	if err != nil {
		t.Fatalf("DecodeString: %v", err)
	}

	rows, err := DecodeNativePayload(payloadexec.TableSchema{
		TableID: "tenant_e2e_a.events",
		Columns: []lthash.Column{
			{Name: "id", Type: "UInt64"},
			{Name: "label", Type: "String"},
		},
	}, 54460, payload)
	if err != nil {
		t.Fatalf("DecodeNativePayload: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rows))
	}
	if got, want := rows[0].Values, []any{uint64(1), "tenant_e2e_a-row"}; got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("row values = %#v, want %#v", got, want)
	}
}

func applySharedPayloadExecutor(t *testing.T, exec *payloadexec.Executor, tableID, statementID string, payload []byte) (string, []replay.PartitionCommitment, []replay.PartManifestEntry) {
	t.Helper()
	snap, err := exec.GenesisSnapshot(0, "schema-shared", "profile-shared")
	if err != nil {
		t.Fatalf("GenesisSnapshot: %v", err)
	}
	stmt := replay.Statement{
		StatementID:   statementID,
		StatementSeq:  1,
		SQL:           "INSERT INTO tenant.events FORMAT payload",
		SQLHash:       replay.DigestString("INSERT INTO tenant.events FORMAT payload"),
		SettingsHash:  replay.DigestString("settings"),
		PayloadRef:    "payload-" + statementID,
		PayloadHash:   replay.DigestBytes(payload),
		PayloadLength: uint64(len(payload)),
		TargetTableID: tableID,
	}
	job := replay.ReplayJob{
		BlockSeq:           1,
		PrevSafeSnapshotID: snap.SnapshotID,
		PrevStateRoot:      snap.StateRoot,
		SchemaSnapshotID:   snap.SchemaSnapshotID,
		ExecutorProfileID:  snap.ExecutorProfileID,
		Statements:         []replay.Statement{stmt},
	}
	_, res, err := exec.Apply(snap, job, []replay.PreparedStatement{{Statement: stmt, Payload: payload}})
	if err != nil {
		t.Fatalf("Apply shared executor: %v", err)
	}
	return res.ComputedStateRoot, res.PartitionCommitmentsAfter, res.AffectedParts
}

func encodeNativePayload(t *testing.T, input proto.Input) []byte {
	t.Helper()
	return encodeNativePacket(t, uint64(proto.ClientCodeData), input, input[0].Data.Rows())
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

func newColFixedStr32(values ...string) *proto.ColFixedStr32 {
	col := new(proto.ColFixedStr32)
	for _, value := range values {
		var row [32]byte
		copy(row[:], value)
		col.Append(row)
	}
	return col
}
