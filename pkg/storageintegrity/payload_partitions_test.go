package storageintegrity

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

const payloadPartitionsNativeRevision = 54453

func partitionsSchema() payloadexec.TableSchema {
	return payloadexec.TableSchema{
		TableID:     "db.t",
		PartitionBy: "p",
		Columns:     []lthash.Column{{Name: "p", Type: "String"}, {Name: "v", Type: "UInt64"}},
	}
}

func TestPayloadPartitionIDs_CSVGroupsSortsAndDedups(t *testing.T) {
	got, err := PayloadPartitionIDs(partitionsSchema(), EncodingCSVWithNames, 0, []byte("p,v\nzeta,1\nalpha,2\nzeta,3\n"))
	if err != nil {
		t.Fatalf("PayloadPartitionIDs: %v", err)
	}
	if want := []string{"p_alpha", "p_zeta"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("partitions = %v want %v", got, want)
	}
}

func TestPayloadPartitionIDs_NativeUsesDecodedRows(t *testing.T) {
	payload := encodeNativePayload(t, proto.Input{
		{Name: "p", Data: newColStr("b", "a", "b")},
		{Name: "v", Data: &proto.ColUInt64{1, 2, 3}},
	})
	got, err := PayloadPartitionIDs(partitionsSchema(), PayloadEncodingClickHouseNativeData, payloadPartitionsNativeRevision, payload)
	if err != nil {
		t.Fatalf("PayloadPartitionIDs: %v", err)
	}
	if want := []string{"p_a", "p_b"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("partitions = %v want %v", got, want)
	}
}

func encodeNativePayload(t *testing.T, input proto.Input) []byte {
	t.Helper()
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ClientCodeData))
	buf.PutString("")
	rows := input[0].Data.Rows()
	if err := (proto.Block{Rows: rows, Columns: len(input)}).EncodeBlock(&buf, payloadPartitionsNativeRevision, input); err != nil {
		t.Fatalf("encode block: %v", err)
	}
	return bytes.Clone(buf.Buf)
}

func newColStr(values ...string) *proto.ColStr {
	col := new(proto.ColStr)
	for _, value := range values {
		col.Append(value)
	}
	return col
}

func TestPayloadPartitionIDs_UnpartitionedIsAll(t *testing.T) {
	schema := payloadexec.TableSchema{TableID: "db.u", Columns: []lthash.Column{{Name: "v", Type: "UInt64"}}}
	got, err := PayloadPartitionIDs(schema, EncodingCSVWithNames, 0, []byte("v\n1\n2\n"))
	if err != nil || !reflect.DeepEqual(got, []string{"all"}) {
		t.Fatalf("partitions = %v err=%v want [all]", got, err)
	}
}

func TestPayloadPartitionIDs_RejectsFreezeViolationAndUnknownEncoding(t *testing.T) {
	// Float64 is not an admitted partition key type (a bare UInt64 key is, from
	// table admission rules v3).
	bad := payloadexec.TableSchema{TableID: "db.t", PartitionBy: "n", Columns: []lthash.Column{{Name: "n", Type: "Float64"}}}
	if _, err := PayloadPartitionIDs(bad, EncodingCSVWithNames, 0, []byte("n\n1\n")); !errors.Is(err, ErrPartitionFreeze) {
		t.Fatalf("err = %v want ErrPartitionFreeze", err)
	}
	if _, err := PayloadPartitionIDs(partitionsSchema(), "future-encoding-v2", 0, []byte("x")); err == nil {
		t.Fatal("unknown encoding must error")
	}
}

// TestPayloadPartitionIDs_AdmitsRulesV3Keys: the indexer derives partitions for
// every key table admission rules v3 admits, bare non-String keys and
// partition expressions included.
func TestPayloadPartitionIDs_AdmitsRulesV3Keys(t *testing.T) {
	for _, tc := range []struct {
		key, columnType, csv string
		want                 []string
	}{
		{"n", "UInt64", "n\n1\n007\n1\n", []string{"p_1", "p_7"}},
		{"intDiv(n, 1000)", "Int64", "n\n-1500\n999\n2000\n", []string{"p_-1", "p_0", "p_2"}},
		{"toYYYYMM(n)", "DateTime('UTC')", "n\n2024-01-31 23:59:59\n2024-02-01 00:00:00\n", []string{"p_202401", "p_202402"}},
		{"toMonday(n)", "Date", "n\n1970-01-03\n2024-02-29\n", []string{"p_1970-01-01", "p_2024-02-26"}},
	} {
		schema := payloadexec.TableSchema{TableID: "db.t", PartitionBy: tc.key, Columns: []lthash.Column{{Name: "n", Type: tc.columnType}}}
		got, err := PayloadPartitionIDs(schema, EncodingCSVWithNames, 0, []byte(tc.csv))
		if err != nil || !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s: partitions = %v err=%v want %v", tc.key, got, err, tc.want)
		}
	}
}
