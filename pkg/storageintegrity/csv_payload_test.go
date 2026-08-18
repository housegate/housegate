package storageintegrity

import (
	"bytes"
	"context"
	"testing"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/lthash"
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

func encodeNativePayload(t *testing.T, input proto.Input) []byte {
	t.Helper()
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ClientCodeData))
	buf.PutString("")
	if err := (proto.Block{Rows: input[0].Data.Rows(), Columns: len(input)}).EncodeBlock(&buf, nativePayloadTestRevision, input); err != nil {
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

func TestNativeCSVPayloadMaterializerConvertsCapturedClientData(t *testing.T) {
	schema := nativeMaterializerSchema()
	nativePayload := encodeNativePayload(t, proto.Input{
		{Name: "region", Data: newColStr("eu", "us")},
		{Name: "id", Data: &proto.ColUInt64{1, 2}},
	})
	materializer := NativeCSVPayloadMaterializer{
		SchemaResolver: TableSchemaResolverFunc(func(tableID string) (payloadexec.TableSchema, bool) {
			if tableID != schema.TableID {
				return payloadexec.TableSchema{}, false
			}
			return schema, true
		}),
	}

	out, err := materializer.MaterializePayload(context.Background(), PayloadMaterializationInput{
		StatementID:     "stmt-csv-1",
		TableID:         schema.TableID,
		SQL:             "INSERT INTO tenant.events FORMAT CSVWithNames",
		PayloadEncoding: EncodingCSVWithNames,
		NativeWire:      nativePayload,
		Revision:        nativePayloadTestRevision,
	})
	if err != nil {
		t.Fatalf("MaterializePayload: %v", err)
	}
	if out.Encoding != EncodingCSVWithNames {
		t.Fatalf("encoding = %q, want %q", out.Encoding, EncodingCSVWithNames)
	}
	if bytes.Contains(out.Payload, []byte{byte(proto.ClientCodeData)}) {
		t.Fatalf("CSV payload still contains raw ClientData framing bytes: %x", out.Payload)
	}
	if got, want := string(out.Payload), "id,region\n1,eu\n2,us\n"; got != want {
		t.Fatalf("payload = %q, want %q", got, want)
	}
	rows, err := payloadexec.DecodeCSV(out.Payload, schema)
	if err != nil {
		t.Fatalf("DecodeCSV materialized payload: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(rows))
	}
}

func TestNativeCSVPayloadMaterializerRequiresCSVWithNamesEncoding(t *testing.T) {
	materializer := NativeCSVPayloadMaterializer{
		SchemaResolver: TableSchemaResolverFunc(func(string) (payloadexec.TableSchema, bool) {
			return nativeMaterializerSchema(), true
		}),
	}

	_, err := materializer.MaterializePayload(context.Background(), PayloadMaterializationInput{
		StatementID:     "stmt-native-1",
		TableID:         "tenant.events",
		PayloadEncoding: PayloadEncodingClickHouseNativeData,
		NativeWire:      []byte{byte(proto.ClientCodeData)},
		Revision:        nativePayloadTestRevision,
	})
	if err == nil {
		t.Fatal("MaterializePayload accepted non-CSVWithNames encoding")
	}
}
