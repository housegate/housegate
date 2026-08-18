package storageintegrity

import (
	"errors"
	"reflect"
	"testing"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

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
	got, err := PayloadPartitionIDs(partitionsSchema(), PayloadEncodingClickHouseNativeData, nativePayloadTestRevision, payload)
	if err != nil {
		t.Fatalf("PayloadPartitionIDs: %v", err)
	}
	if want := []string{"p_a", "p_b"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("partitions = %v want %v", got, want)
	}
}

func TestPayloadPartitionIDs_UnpartitionedIsAll(t *testing.T) {
	schema := payloadexec.TableSchema{TableID: "db.u", Columns: []lthash.Column{{Name: "v", Type: "UInt64"}}}
	got, err := PayloadPartitionIDs(schema, EncodingCSVWithNames, 0, []byte("v\n1\n2\n"))
	if err != nil || !reflect.DeepEqual(got, []string{"all"}) {
		t.Fatalf("partitions = %v err=%v want [all]", got, err)
	}
}

func TestPayloadPartitionIDs_RejectsFreezeViolationAndUnknownEncoding(t *testing.T) {
	bad := payloadexec.TableSchema{TableID: "db.t", PartitionBy: "n", Columns: []lthash.Column{{Name: "n", Type: "UInt64"}}}
	if _, err := PayloadPartitionIDs(bad, EncodingCSVWithNames, 0, []byte("n\n1\n")); !errors.Is(err, ErrPartitionFreeze) {
		t.Fatalf("err = %v want ErrPartitionFreeze", err)
	}
	if _, err := PayloadPartitionIDs(partitionsSchema(), "future-encoding-v2", 0, []byte("x")); err == nil {
		t.Fatal("unknown encoding must error")
	}
}
