package nativepayload

import (
	"context"
	"encoding/hex"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
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

	rows, err := Decode(schema, nativePayloadTestRevision, payload)
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

	rows, err := (Materializer{NetworkID: "network-1", Revision: nativePayloadTestRevision}).Materialize(context.Background(), schema, statement)
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
	if _, err := Decode(schema, nativePayloadTestRevision, withInjectedRowID); err == nil {
		t.Fatal("DecodeNativePayload must reject payloads that include reserved _hg_row_id")
	}

	wrongType := encodeNativePayload(t, proto.Input{
		{Name: "region", Data: newColStr("eu")},
		{Name: "id", Data: newColStr("1")},
	})
	err := func() error {
		_, err := Decode(schema, nativePayloadTestRevision, wrongType)
		return err
	}()
	if err == nil {
		t.Fatal("DecodeNativePayload must reject unchanged column names with different Native types")
	}
	// Spec Q Q-D1: the block type is compared against the wire type the
	// column-type authority declares for the schema type, and the message names
	// both so an operator can see which side moved.
	for _, want := range []string{`column "id"`, `does not match schema type "UInt64"`, `expected wire type "UInt64"`} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("type-mismatch error %q does not contain %q", err, want)
		}
	}

	missingColumn := encodeNativePayload(t, proto.Input{{Name: "id", Data: &proto.ColUInt64{1}}})
	if _, err := Decode(schema, nativePayloadTestRevision, missingColumn); err == nil {
		t.Fatal("DecodeNativePayload must reject payloads missing pinned schema columns")
	}
}

func TestValidateNativePayloadDecodable(t *testing.T) {
	schema := nativeMaterializerSchema()
	ok := encodeNativePayload(t, proto.Input{
		{Name: "region", Data: newColStr("eu")},
		{Name: "id", Data: &proto.ColUInt64{1}},
	})
	if err := ValidateDecodable(schema, nativePayloadTestRevision, ok); err != nil {
		t.Fatalf("supported scalar payload must validate: %v", err)
	}

	bad := encodeNativePayload(t, proto.Input{
		{Name: "region", Data: newColStr("eu")},
		{Name: "id", Data: &proto.ColInt128{proto.Int128{Low: 1}}},
	})
	err := ValidateDecodable(schema, nativePayloadTestRevision, bad)
	if err == nil || !errors.Is(err, ErrUnsupported) {
		t.Fatalf("unsupported column type must be rejected with ErrNativePayloadUnsupported, got %v", err)
	}
}

func TestNativePayloadRejectsUnsupportedPinnedScalarType(t *testing.T) {
	schema := payloadexec.TableSchema{
		TableID: "tenant.unsupported",
		Columns: []lthash.Column{{Name: "value", Type: "Int128"}},
	}
	payload := encodeNativePayload(t, proto.Input{
		{Name: "value", Data: &proto.ColInt128{proto.Int128{Low: 1}}},
	})
	err := ValidateDecodable(schema, nativePayloadTestRevision, payload)
	if err == nil || !errors.Is(err, ErrUnsupported) {
		t.Fatalf("matching but unsupported pinned scalar must fail closed, got %v", err)
	}
}

func TestNativePayloadRejectsUndecodableBlock(t *testing.T) {
	err := ValidateDecodable(nativeMaterializerSchema(), nativePayloadTestRevision, []byte{byte(proto.ClientCodeData)})
	if err == nil || !errors.Is(err, ErrUnsupported) {
		t.Fatalf("undecodable native payload must be rejected with ErrNativePayloadUnsupported, got %v", err)
	}
	if !errors.Is(err, io.EOF) {
		t.Fatalf("ValidateDecodable must preserve the decoder cause for errors.Is/As, got %v", err)
	}
}

func TestDecodeNativePayloadRejectsEmbeddedZeroRowDataPackets(t *testing.T) {
	schema := nativeMaterializerSchema()
	zero := encodeNativePacket(t, uint64(proto.ClientCodeData), proto.Input{
		{Name: "region", Data: newColStr()},
		{Name: "id", Data: &proto.ColUInt64{}},
	}, 0)
	valid := encodeNativePayload(t, proto.Input{
		{Name: "region", Data: newColStr("eu")},
		{Name: "id", Data: &proto.ColUInt64{1}},
	})
	for name, payload := range map[string][]byte{
		"prefix": append(append([]byte{}, zero...), valid...),
		"suffix": append(append([]byte{}, valid...), zero...),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Decode(schema, nativePayloadTestRevision, payload); err == nil {
				t.Fatal("captured Native payload must reject embedded zero-row control packets")
			}
		})
	}
}

func TestNativePayloadRejectsEmptyStream(t *testing.T) {
	err := ValidateDecodable(nativeMaterializerSchema(), nativePayloadTestRevision, nil)
	if err == nil || !errors.Is(err, ErrUnsupported) {
		t.Fatalf("empty native payload must be rejected with ErrNativePayloadUnsupported, got %v", err)
	}
}

func TestNativeMaterializerRejectsStatementWithoutPayload(t *testing.T) {
	schema := nativeMaterializerSchema()
	_, err := (Materializer{NetworkID: "network-1", Revision: nativePayloadTestRevision}).Materialize(
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
	_, err := (Materializer{NetworkID: "network-1", Revision: nativePayloadTestRevision}).Materialize(
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

func TestNativeMaterializerRejectsEmptyTargetTable(t *testing.T) {
	schema := nativeMaterializerSchema()
	payload := encodeNativePayload(t, proto.Input{
		{Name: "region", Data: newColStr("eu")},
		{Name: "id", Data: &proto.ColUInt64{1}},
	})
	_, err := (Materializer{NetworkID: "network-1", Revision: nativePayloadTestRevision}).Materialize(
		context.Background(),
		schema,
		replay.PreparedStatement{
			Statement: replay.Statement{
				StatementID:  "stmt-native-1",
				StatementSeq: 1,
				PayloadRef:   "payload-native-1",
			},
			Payload: payload,
		},
	)
	if err == nil {
		t.Fatal("NativeMaterializer must reject an empty target table")
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
		payloadexec.NewWithMaterializer(networkID, Materializer{NetworkID: networkID, Revision: nativePayloadTestRevision}, schema),
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

	rows, err := Decode(schema, nativePayloadTestRevision, nativePayload)
	if err != nil {
		t.Fatalf("DecodeNativePayload: %v", err)
	}
	if rows[0].RawBytes != 10 || rows[1].RawBytes != 10 {
		t.Fatalf("row RawBytes = %d/%d, want 10/10", rows[0].RawBytes, rows[1].RawBytes)
	}

	_, _, parts := applySharedPayloadExecutor(t,
		payloadexec.NewWithMaterializer(networkID, Materializer{NetworkID: networkID, Revision: nativePayloadTestRevision}, schema),
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

func TestNativeMaterializerAdmittedScalarGoldenMatrix(t *testing.T) {
	instant := time.Date(2026, time.July, 16, 12, 34, 56, 123000000, time.UTC)
	date := new(proto.ColDate)
	date.Append(instant)
	dateTime := &proto.ColDateTime{Location: time.UTC}
	dateTime.Append(instant)
	dateTime64 := new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli).WithLocation(time.UTC)
	dateTime64.Append(instant)

	schema := payloadexec.TableSchema{
		TableID: "tenant.scalar_matrix",
		Columns: []lthash.Column{
			{Name: "u8", Type: "UInt8"},
			{Name: "u16", Type: "UInt16"},
			{Name: "u32", Type: "UInt32"},
			{Name: "u64", Type: "UInt64"},
			{Name: "i8", Type: "Int8"},
			{Name: "i16", Type: "Int16"},
			{Name: "i32", Type: "Int32"},
			{Name: "i64", Type: "Int64"},
			{Name: "f32", Type: "Float32"},
			{Name: "f64", Type: "Float64"},
			{Name: "str", Type: "String"},
			{Name: "fixed", Type: "FixedString(32)"},
			{Name: "flag", Type: "Bool"},
			{Name: "day", Type: "Date"},
			{Name: "second", Type: "DateTime('UTC')"},
			{Name: "millisecond", Type: "DateTime64(3, 'UTC')"},
		},
	}
	payload := encodeNativePayload(t, proto.Input{
		{Name: "u8", Data: &proto.ColUInt8{8}},
		{Name: "u16", Data: &proto.ColUInt16{16}},
		{Name: "u32", Data: &proto.ColUInt32{32}},
		{Name: "u64", Data: &proto.ColUInt64{64}},
		{Name: "i8", Data: &proto.ColInt8{-8}},
		{Name: "i16", Data: &proto.ColInt16{-16}},
		{Name: "i32", Data: &proto.ColInt32{-32}},
		{Name: "i64", Data: &proto.ColInt64{-64}},
		{Name: "f32", Data: &proto.ColFloat32{1.5}},
		{Name: "f64", Data: &proto.ColFloat64{2.5}},
		{Name: "str", Data: newColStr("abc")},
		{Name: "fixed", Data: newColFixedStr32("fixed")},
		{Name: "flag", Data: &proto.ColBool{true}},
		{Name: "day", Data: date},
		{Name: "second", Data: dateTime},
		{Name: "millisecond", Data: dateTime64},
	})

	rows, err := Decode(schema, nativePayloadTestRevision, payload)
	if err != nil {
		t.Fatalf("DecodeNativePayload: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rows))
	}
	if got, want := rows[0].RawBytes, uint64(92); got != want {
		t.Fatalf("RawBytes = %d, want %d", got, want)
	}
	wantFixed := make([]byte, 32)
	copy(wantFixed, "fixed")
	wantValues := []any{
		uint8(8), uint16(16), uint32(32), uint64(64),
		int8(-8), int16(-16), int32(-32), int64(-64),
		float32(1.5), float64(2.5), "abc", wantFixed, true,
		proto.NewDate(2026, time.July, 16).Time(),
		instant.Truncate(time.Second),
		instant.Truncate(time.Millisecond),
	}
	if !reflect.DeepEqual(rows[0].Values, wantValues) {
		t.Fatalf("values = %#v, want %#v", rows[0].Values, wantValues)
	}
	if _, err := lthash.RowHash(schema.TableID, schema.Columns, rows[0].Values); err != nil {
		t.Fatalf("shared row hash rejected admitted Native scalars: %v", err)
	}

	_, _, parts := applySharedPayloadExecutor(t,
		payloadexec.NewWithMaterializer("network-1", Materializer{NetworkID: "network-1", Revision: nativePayloadTestRevision}, schema),
		schema.TableID,
		"stmt-native-scalars",
		payload,
	)
	if len(parts) != 1 || parts[0].Bytes != 92 {
		t.Fatalf("affected parts = %+v, want one 92-byte part", parts)
	}
}

func TestNativePayloadDecodesClickHouseGoClientData(t *testing.T) {
	// This fixture is the single non-empty ClientData packet captured from a
	// clickhouse-go INSERT. The leading external-tables marker and trailing
	// zero-row terminator are control packets and are deliberately excluded
	// from payload_format=clickhouse-native-data-v1.
	payloadHex := "" +
		"0200010002ffffffff0002010269640655496e74" +
		"3634000100000000000000056c6162656c06537472696e67001074656e616e74" +
		"5f6532655f612d726f77"
	payload, err := hex.DecodeString(payloadHex)
	if err != nil {
		t.Fatalf("DecodeString: %v", err)
	}

	rows, err := Decode(payloadexec.TableSchema{
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
