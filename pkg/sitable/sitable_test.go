package sitable

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

func testSchema(id string) payloadexec.TableSchema {
	return payloadexec.TableSchema{TableID: id, Columns: []lthash.Column{{Name: "a", Type: "UInt32"}}}
}

func TestStaticSnapshot(t *testing.T) {
	schemas := map[string]payloadexec.TableSchema{"db1.t": testSchema("db1.t")}
	st := NewStatic([]string{"db1.u", "db1.t"}, schemas, "net")
	snap := st.Current()
	if snap.Version() != StaticVersion {
		t.Fatalf("version = %d, want %d", snap.Version(), StaticVersion)
	}
	got := snap.Lookup("db1", "t")
	if got.Status != Active || got.ID != "db1.t" || got.SchemaHash != payloadexec.TableSchemaHash("net", testSchema("db1.t")) {
		t.Fatalf("Lookup(db1, t) = %+v", got)
	}
	if other := snap.Lookup("db1", "other"); other.Status != Ordinary || other.ID != "db1.other" {
		t.Fatalf("unlisted table = %+v, want Ordinary", other)
	}
	var ids []string
	for _, table := range snap.Active() {
		ids = append(ids, table.ID)
	}
	if !reflect.DeepEqual(ids, []string{"db1.t", "db1.u"}) {
		t.Fatalf("Active ids = %v, want sorted [db1.t db1.u]", ids)
	}
	if _, ok := snap.Schema("db1.other"); ok {
		t.Fatal("Schema of an Ordinary table must miss")
	}
	if table, ok := snap.Schema("db1.u"); !ok || table.SchemaHash != "" {
		t.Fatalf("a listed table without a startup schema is Active with an empty hash, got %+v ok=%v", table, ok)
	}
	if got := st.Schemas(); len(got) != 1 || got[0].TableID != "db1.t" {
		t.Fatalf("Schemas() = %+v", got)
	}
	select {
	case <-st.Changed():
		t.Fatal("Static.Changed must never fire")
	default:
	}
}

func TestFakeSwitchesVersionsAndWakes(t *testing.T) {
	f := NewFake(Pending, Table{ID: "db1.t", Status: Pending})
	first := f.Current()
	wake := f.Changed()
	if v := f.Set(Table{ID: "db1.t", Status: Active, Schema: testSchema("db1.t")}); v != 2 {
		t.Fatalf("Set returned version %d, want 2", v)
	}
	select {
	case <-wake:
	case <-time.After(time.Second):
		t.Fatal("Set must close the Changed channel handed out before it")
	}
	if first.Version() != 1 || first.Lookup("db1", "t").Status != Pending {
		t.Fatalf("an earlier snapshot must stay immutable: v=%d %+v", first.Version(), first.Lookup("db1", "t"))
	}
	second := f.Current()
	if second.Version() != 2 || second.Lookup("db1", "t").Status != Active {
		t.Fatalf("new snapshot = v%d %+v", second.Version(), second.Lookup("db1", "t"))
	}
	if unknown := second.Lookup("db2", "x"); unknown.Status != Pending {
		t.Fatalf("fallback = %v, want Pending", unknown.Status)
	}
	f.Set(Table{ID: "db1.t", Status: Gone, Schema: testSchema("db1.t")})
	if table, ok := f.Current().Schema("db1.t"); !ok || table.Status != Gone {
		t.Fatalf("Schema must answer a Gone table, got %+v ok=%v", table, ok)
	}
	if len(f.Current().Active()) != 0 {
		t.Fatal("a Gone table is not Active")
	}
}

func TestStatusNamesRoundTrip(t *testing.T) {
	for _, s := range []Status{Ordinary, Pending, Refused, Active, Gone} {
		got, ok := ParseStatus(s.String())
		if !ok || got != s {
			t.Fatalf("ParseStatus(%q) = %v, %v", s.String(), got, ok)
		}
	}
	if _, ok := ParseStatus("retiring"); ok {
		t.Fatal("unknown names must not parse")
	}
}

func TestReservedDatabasesIsACopy(t *testing.T) {
	got := ReservedDatabases()
	if strings.Join(got, ",") != "hg_safe,hg_unsafe,hg_promote" {
		t.Fatalf("ReservedDatabases() = %v", got)
	}
	got[0] = "mutated"
	if ReservedDatabases()[0] != SafeDatabase {
		t.Fatal("ReservedDatabases must return a fresh slice")
	}
}
