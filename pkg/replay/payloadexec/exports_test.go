package payloadexec

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/housegate/housegate/pkg/lthash"
)

func TestExportedHelpersDelegate(t *testing.T) {
	sch := TableSchema{
		TableID:     "db.t",
		PartitionBy: "p",
		Columns: []lthash.Column{{
			Name: "v",
			Type: "UInt64",
		}},
	}
	rid := RowID("net-1", "db.t", "acct/1/n", 0)

	exp, err := RowElementHash(sch, rid, []any{uint64(42)})
	if err != nil {
		t.Fatalf("RowElementHash: %v", err)
	}
	got, err := rowElementHash(sch, rid, []any{uint64(42)})
	if err != nil {
		t.Fatalf("rowElementHash: %v", err)
	}
	if !bytes.Equal(exp.Bytes(), got.Bytes()) {
		t.Fatal("RowElementHash must delegate to the internal derivation")
	}
	if SchemaRoot("net-1", []TableSchema{sch}) != schemaRoot("net-1", []TableSchema{sch}) {
		t.Fatal("SchemaRoot must delegate")
	}
	if TableSchemaHash("net-1", sch) != tableSchemaHash("net-1", sch) {
		t.Fatal("TableSchemaHash must delegate")
	}
}

func TestTableSchemaCanonicalJSON(t *testing.T) {
	schema := TableSchema{
		TableID:     "orders.t",
		PartitionBy: "day",
		Columns:     []lthash.Column{{Name: "id", Type: "UInt64"}},
	}
	got, err := json.Marshal(schema)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	const want = `{"table_id":"orders.t","partition_by":"day","columns":[{"name":"id","type":"UInt64"}]}`
	if string(got) != want {
		t.Fatalf("canonical schema JSON = %s, want %s", got, want)
	}
}

func TestRowIDProfileIDMatchesDomain(t *testing.T) {
	if RowIDProfileID != rowIDDomain {
		t.Fatalf("RowIDProfileID = %q, rowIDDomain = %q", RowIDProfileID, rowIDDomain)
	}
	if RowIDProfileID != "housegate-row-id-v1" {
		t.Fatalf("RowIDProfileID = %q", RowIDProfileID)
	}
	if PayloadFormatCSVWithNames != "csv-with-names-v1" {
		t.Fatalf("PayloadFormatCSVWithNames = %q", PayloadFormatCSVWithNames)
	}
}
