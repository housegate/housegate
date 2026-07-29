package integration

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/schemaregistry"
)

func TestClickHouseLoader_DerivesRealDDL(t *testing.T) {
	conn := openDirectCH(t)
	db, table := "hg_unsafe_chschema", "orders__t"

	mustExec(t, conn, "CREATE DATABASE IF NOT EXISTS "+db)
	mustExec(t, conn, "DROP TABLE IF EXISTS "+db+"."+table)
	mustExec(t, conn, `CREATE TABLE `+db+`.`+table+` (
		_hg_row_id FixedString(32),
		p String,
		v UInt64,
		note Nullable(String)
	) ENGINE = MergeTree PARTITION BY p ORDER BY tuple()`)
	t.Cleanup(func() {
		_ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+db)
	})

	got, err := schemaregistry.NewClickHouseLoader(conn).Load(
		context.Background(),
		[]schemaregistry.TableRef{{TableID: "orders.t", Database: db, Table: table}},
	)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	want := payloadexec.TableSchema{
		TableID:     "orders.t",
		PartitionBy: "p",
		Columns: []lthash.Column{
			{Name: "p", Type: "String"},
			{Name: "v", Type: "UInt64"},
			{Name: "note", Type: "Nullable(String)"},
		},
	}
	if !reflect.DeepEqual(got[0], want) {
		t.Fatalf("derived schema:\n got %+v\nwant %+v", got[0], want)
	}
	if payloadexec.SchemaRoot("net", got) != payloadexec.SchemaRoot("net", []payloadexec.TableSchema{want}) {
		t.Fatal("derived and declared schemas must produce identical roots")
	}

	_, err = schemaregistry.NewClickHouseLoader(conn).Load(
		context.Background(),
		[]schemaregistry.TableRef{{TableID: "ghost", Database: db, Table: "nope"}},
	)
	if err == nil || !strings.Contains(err.Error(), "ghost") {
		t.Fatalf("missing table: %v", err)
	}
}
