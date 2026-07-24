package storageintegrity

import (
	"bytes"
	"context"
	"testing"

	"github.com/ClickHouse/ch-go/proto"

	"housegate/housegate/pkg/replay/payloadexec"
)

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
