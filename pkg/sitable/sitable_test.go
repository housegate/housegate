package sitable

import (
	"reflect"
	"strconv"
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

// TestReservedDatabasesIsACopy pins the reserved list and its order (spec
// 2026-10-14 §14): the SNode's three databases first, then the four stage-3
// member databases, so every message that joins the list still starts with
// hg_safe.
func TestReservedDatabasesIsACopy(t *testing.T) {
	got := ReservedDatabases()
	if strings.Join(got, ",") != "hg_safe,hg_unsafe,hg_promote,hg_member_unsafe,hg_replica,hg_rstage,hg_xfer" {
		t.Fatalf("ReservedDatabases() = %v", got)
	}
	if MemberUnsafeDatabase != "hg_member_unsafe" || ReplicaDatabase != "hg_replica" ||
		ReplicaStageDatabase != "hg_rstage" || TransferDatabase != "hg_xfer" {
		t.Fatalf("stage-3 constants = %q / %q / %q / %q", MemberUnsafeDatabase, ReplicaDatabase, ReplicaStageDatabase, TransferDatabase)
	}
	got[0] = "mutated"
	if ReservedDatabases()[0] != SafeDatabase {
		t.Fatal("ReservedDatabases must return a fresh slice")
	}
}

// TestSnapshotSchemaColumnsAreNotAliased is the reviewer's reproduction: a
// caller-owned Columns slice passed into a Table must not be visible through
// the snapshot after the caller mutates it (the "in" direction).
func TestSnapshotSchemaColumnsAreNotAliased(t *testing.T) {
	cols := []lthash.Column{{Name: "a", Type: "UInt32"}}
	f := NewFake(Ordinary, Table{ID: "db1.t", Status: Active, Schema: payloadexec.TableSchema{TableID: "db1.t", Columns: cols}})
	snap := f.Current()
	cols[0].Name = "mutated"
	table, ok := snap.Schema("db1.t")
	if !ok || table.Schema.Columns[0].Name != "a" {
		t.Fatalf("Schema must not alias the caller's Columns slice, got %+v", table.Schema.Columns)
	}
}

// TestSnapshotReturnedTableColumnsAreFreshCopies covers the "out" direction:
// mutating Columns on a Table returned by Lookup/Active/Schema must not leak
// into the snapshot's internals or into another accessor for the same table.
func TestSnapshotReturnedTableColumnsAreFreshCopies(t *testing.T) {
	f := NewFake(Ordinary, Table{ID: "db1.t", Status: Active, Schema: testSchema("db1.t")})
	snap := f.Current()

	got := snap.Lookup("db1", "t")
	got.Schema.Columns[0].Name = "mutated-lookup"
	again := snap.Lookup("db1", "t")
	if again.Schema.Columns[0].Name != "a" {
		t.Fatalf("mutating a Lookup-returned Table's Columns must not affect a later Lookup, got %+v", again.Schema.Columns)
	}

	activeList := snap.Active()
	activeList[0].Schema.Columns[0].Name = "mutated-active"
	viaSchema, ok := snap.Schema("db1.t")
	if !ok || viaSchema.Schema.Columns[0].Name != "a" {
		t.Fatalf("mutating an Active()-returned Table's Columns must not affect Schema(), got %+v", viaSchema.Schema.Columns)
	}

	viaSchema.Schema.Columns[0].Name = "mutated-schema"
	viaLookup := snap.Lookup("db1", "t")
	if viaLookup.Schema.Columns[0].Name != "a" {
		t.Fatalf("mutating a Schema()-returned Table's Columns must not affect Lookup, got %+v", viaLookup.Schema.Columns)
	}
}

// TestSnapshotSchemaAnswersARefusedTableOnlyWhenItCarriesOne is clarification
// C13 (spec 2026-10-10 §10): a host reports each table of a quarantined indexer
// as Refused with the code "indexer_quarantined" and keeps its schema, so
// journal recovery and cleanup still resolve it through Schema. A table the
// arbiter refused at admission carries no schema and stays unanswered, and
// every other status behaves as before.
func TestSnapshotSchemaAnswersARefusedTableOnlyWhenItCarriesOne(t *testing.T) {
	quarantined := Table{
		ID: "db1.quarantined", Status: Refused,
		RefusedCode: "indexer_quarantined", RefusedReason: "storage-integrity indexer 1 is quarantined",
		Schema: testSchema("db1.quarantined"), SchemaHash: payloadexec.TableSchemaHash("net", testSchema("db1.quarantined")),
	}
	snap := NewFake(Pending,
		quarantined,
		Table{ID: "db1.refused", Status: Refused, RefusedCode: "SCHEMA_INVALID", RefusedReason: "bad column"},
		Table{ID: "db1.pending", Status: Pending}, // default deny carries no schema; a held Pending table with one is TestSnapshotSchemaAnswersAHeldPendingTable's
		Table{ID: "db1.ordinary", Status: Ordinary, Schema: testSchema("db1.ordinary")},
		Table{ID: "db1.active", Status: Active, Schema: testSchema("db1.active")},
		Table{ID: "db1.gone", Status: Gone, Schema: testSchema("db1.gone")},
	).Current()

	got, ok := snap.Schema("db1.quarantined")
	if !ok || !reflect.DeepEqual(got, quarantined) {
		t.Fatalf("Schema(db1.quarantined) = %+v ok=%v, want the Refused entry with its code, schema and hash", got, ok)
	}
	for _, id := range []string{"db1.active", "db1.gone"} {
		if table, ok := snap.Schema(id); !ok || table.ID != id {
			t.Fatalf("Schema(%s) = %+v ok=%v, want the entry", id, table, ok)
		}
	}
	for _, id := range []string{"db1.refused", "db1.pending", "db1.ordinary", "db1.unlisted"} {
		if table, ok := snap.Schema(id); ok {
			t.Fatalf("Schema(%s) = %+v, want a miss", id, table)
		}
	}

	// Answering Schema does not make the table usable: Lookup still reports it
	// Refused, and only the Active table is listed as Active.
	if lookup := snap.Lookup("db1", "quarantined"); lookup.Status != Refused || lookup.RefusedCode != "indexer_quarantined" {
		t.Fatalf("Lookup(db1, quarantined) = %+v, want it still Refused", lookup)
	}
	if active := snap.Active(); len(active) != 1 || active[0].ID != "db1.active" {
		t.Fatalf("Active() = %+v, want only db1.active", active)
	}

	// The answer is a fresh copy, like every other.
	got.Schema.Columns[0].Name = "mutated"
	if again, _ := snap.Schema("db1.quarantined"); again.Schema.Columns[0].Name != "a" {
		t.Fatalf("mutating a Schema()-returned Refused Table's Columns leaked into the snapshot: %+v", again.Schema.Columns)
	}
}

// TestSnapshotSchemaAnswersAHeldPendingTable is spec 2026-10-14 §14: a host
// reports an Active incarnation it holds back (not ready on its SNode, or an
// unclean owner hg_safe audit in repair mode) as Pending and keeps its schema
// and hash, so journal recovery and cleanup still resolve it. A default-deny
// Pending table carries no schema and stays unanswered, and answering Schema
// makes neither table usable.
func TestSnapshotSchemaAnswersAHeldPendingTable(t *testing.T) {
	held := Table{
		ID: "db1.held", Status: Pending,
		Schema: testSchema("db1.held"), SchemaHash: payloadexec.TableSchemaHash("net", testSchema("db1.held")),
	}
	f := NewFake(Pending, held, Table{ID: "db1.denied", Status: Pending})
	snap := f.Current()

	got, ok := snap.Schema("db1.held")
	if !ok || !reflect.DeepEqual(got, held) {
		t.Fatalf("Schema(db1.held) = %+v ok=%v, want the Pending entry with its schema and hash", got, ok)
	}
	for _, id := range []string{"db1.denied", "db1.unlisted"} {
		if table, ok := snap.Schema(id); ok {
			t.Fatalf("Schema(%s) = %+v, want a miss: a default-deny Pending table has no schema", id, table)
		}
	}
	if lookup := snap.Lookup("db1", "held"); lookup.Status != Pending || lookup.SchemaHash != held.SchemaHash {
		t.Fatalf("Lookup(db1, held) = %+v, want it still Pending with its hash", lookup)
	}
	if active := snap.Active(); len(active) != 0 {
		t.Fatalf("Active() = %+v, want none: a held table is not Active", active)
	}

	// The answer is a fresh copy, like every other.
	got.Schema.Columns[0].Name = "mutated"
	if again, _ := snap.Schema("db1.held"); again.Schema.Columns[0].Name != "a" {
		t.Fatalf("mutating a Schema()-returned Pending Table's Columns leaked into the snapshot: %+v", again.Schema.Columns)
	}

	// The Fake carries the hold across versions: Active, then held.
	f.Set(Table{ID: "db1.held", Status: Active, Schema: testSchema("db1.held"), SchemaHash: held.SchemaHash})
	f.Set(held)
	if table, ok := f.Current().Schema("db1.held"); !ok || table.Status != Pending || table.SchemaHash != held.SchemaHash {
		t.Fatalf("after the hold Schema(db1.held) = %+v ok=%v, want the held Pending entry", table, ok)
	}
	if len(f.Current().Active()) != 0 {
		t.Fatal("a held table must leave the Active set")
	}
}

// TestStaticSchemaColumnsAreNotAliased is the "in" direction for NewStatic:
// the caller's schemas map holds slices Static must not alias.
func TestStaticSchemaColumnsAreNotAliased(t *testing.T) {
	cols := []lthash.Column{{Name: "a", Type: "UInt32"}}
	schemas := map[string]payloadexec.TableSchema{"db1.t": {TableID: "db1.t", Columns: cols}}
	st := NewStatic([]string{"db1.t"}, schemas, "net")
	cols[0].Name = "mutated"
	table, ok := st.Current().Schema("db1.t")
	if !ok || table.Schema.Columns[0].Name != "a" {
		t.Fatalf("NewStatic must not alias the caller's schemas map slices, got %+v", table.Schema.Columns)
	}
}

// TestCopyTableAppendsTheIncarnationToThePhysicalName pins the name of a
// member's verified copy in hg_replica (spec 2026-10-14 §8.3, §20):
// hg_replica.<db__table>__i<incarnation_seq>, the seq in decimal.
func TestCopyTableAppendsTheIncarnationToThePhysicalName(t *testing.T) {
	for _, tc := range []struct {
		id   string
		seq  uint64
		want string
	}{
		{"db1.t", 3, "db1__t__i3"},
		{"a_b.c_d", 0, "a_b__c_d__i0"},
		{"devuser1.s3_p1", 12, "devuser1__s3_p1__i12"},
		{"db.t", 18446744073709551615, "db__t__i18446744073709551615"},
	} {
		if got := CopyTable(tc.id, tc.seq); got != tc.want {
			t.Errorf("CopyTable(%q, %d) = %q, want %q", tc.id, tc.seq, got, tc.want)
		}
		if got := CopyTable(tc.id, tc.seq); !strings.HasPrefix(got, PhysicalTable(tc.id)+"__i") {
			t.Errorf("CopyTable(%q, %d) = %q does not start with PhysicalTable + __i", tc.id, tc.seq, got)
		}
	}
}

// TestCopyTableIsInjectiveAcrossIncarnations: the digits after the last "__i"
// give back the incarnation and the text before it the physical name, so two
// incarnations of one table never share a copy, also when the table's own
// name ends in "__i<digits>" (copies are incarnation-scoped, spec 2026-10-14
// §8.3: a dropped and recreated name never adopts an old copy).
func TestCopyTableIsInjectiveAcrossIncarnations(t *testing.T) {
	for _, id := range []string{"db.t", "db.t__i7", "db__i1.x__i", "db.t__i"} {
		seen := map[string]uint64{}
		for _, seq := range []uint64{0, 1, 7, 10, 70, 71, 700, 18446744073709551615} {
			name := CopyTable(id, seq)
			cut := strings.LastIndex(name, "__i")
			if cut < 0 {
				t.Fatalf("CopyTable(%q, %d) = %q has no __i", id, seq, name)
			}
			back, err := strconv.ParseUint(name[cut+len("__i"):], 10, 64)
			if err != nil || back != seq || name[:cut] != PhysicalTable(id) {
				t.Fatalf("CopyTable(%q, %d) = %q does not split back into %q and %d", id, seq, name, PhysicalTable(id), seq)
			}
			if other, dup := seen[name]; dup {
				t.Fatalf("CopyTable(%q, %d) = CopyTable(%q, %d) = %q", id, seq, id, other, name)
			}
			seen[name] = seq
		}
	}
}

// replicaTable is another SI indexer's table that this host serves read-only
// from its member's copy of incarnation seq (spec 2026-10-14 §11): it carries
// the owner's registry schema and hash, the copy's name in hg_replica and the
// owner.
func replicaTable(id string, seq, owner uint64) Table {
	return Table{
		ID: id, Status: Replica,
		Schema: testSchema(id), SchemaHash: payloadexec.TableSchemaHash("net", testSchema(id)),
		CopyTable: CopyTable(id, seq), ReplicaOf: owner,
	}
}

// TestStatusReplicaRoundTrips pins the Replica status (spec 2026-10-14 §11):
// it is appended after Gone, so the stage-2 values keep 0..4 and Replica is 5,
// and its wire name is "replica".
func TestStatusReplicaRoundTrips(t *testing.T) {
	for want, s := range []Status{Ordinary, Pending, Refused, Active, Gone, Replica} {
		if int(s) != want {
			t.Fatalf("status %s = %d, want %d", s, s, want)
		}
	}
	if Replica.String() != "replica" {
		t.Fatalf("Replica.String() = %q, want %q", Replica.String(), "replica")
	}
	if got, ok := ParseStatus("replica"); !ok || got != Replica {
		t.Fatalf(`ParseStatus("replica") = %v, %v; want Replica, true`, got, ok)
	}
	if got := Status(6).String(); got != "unknown" {
		t.Fatalf("Status(6).String() = %q, want unknown", got)
	}
}

// TestSnapshotServedListsReplicaTablesSortedAndCloned is spec 2026-10-14 §11:
// Served lists exactly the Replica tables, sorted by id, with their copy name
// and owner. Like Active, it clones on the way in and on the way out, and a
// Replica table is never Active.
func TestSnapshotServedListsReplicaTablesSortedAndCloned(t *testing.T) {
	cols := []lthash.Column{{Name: "a", Type: "UInt32"}}
	in := replicaTable("db2.b", 3, 0)
	in.Schema.Columns = cols
	snap := NewSnapshot(4, Ordinary, []Table{
		in,
		{ID: "db1.a", Status: Active, Schema: testSchema("db1.a")},
		replicaTable("db3.c", 1, 2),
		replicaTable("db2.a", 7, 0),
		{ID: "db1.p", Status: Pending},
	})
	cols[0].Name = "mutated-in"

	served := snap.Served()
	var ids []string
	for _, table := range served {
		ids = append(ids, table.ID)
	}
	if !reflect.DeepEqual(ids, []string{"db2.a", "db2.b", "db3.c"}) {
		t.Fatalf("Served ids = %v, want sorted [db2.a db2.b db3.c]", ids)
	}
	if served[0].CopyTable != "db2__a__i7" || served[0].ReplicaOf != 0 || served[2].CopyTable != "db3__c__i1" || served[2].ReplicaOf != 2 {
		t.Fatalf("Served = %+v, want each entry's copy table and owner", served)
	}
	if served[1].Schema.Columns[0].Name != "a" {
		t.Fatalf("Served must not alias the caller's Columns slice, got %+v", served[1].Schema.Columns)
	}
	served[1].Schema.Columns[0].Name = "mutated-out"
	if again := snap.Served(); again[1].Schema.Columns[0].Name != "a" {
		t.Fatalf("mutating a Served()-returned Table's Columns leaked into the snapshot: %+v", again[1].Schema.Columns)
	}
	if lookup := snap.Lookup("db2", "b"); lookup.Status != Replica || lookup.CopyTable != "db2__b__i3" || lookup.ReplicaOf != 0 || lookup.SchemaHash == "" {
		t.Fatalf("Lookup(db2, b) = %+v, want the Replica entry with its copy table, owner and hash", lookup)
	}
	if active := snap.Active(); len(active) != 1 || active[0].ID != "db1.a" {
		t.Fatalf("Active() = %+v, want only db1.a: a Replica table is not Active", active)
	}
	if empty := NewSnapshot(1, Ordinary, []Table{{ID: "db1.a", Status: Active}}).Served(); len(empty) != 0 {
		t.Fatalf("Served() of a snapshot without Replica tables = %+v, want none", empty)
	}
}

// TestServedDatabasesAreSortedAndUnique: the replica-served databases are the
// sorted, de-duplicated databases of Served(); a nil snapshot, or one that
// serves nothing, gives nil.
func TestServedDatabasesAreSortedAndUnique(t *testing.T) {
	snap := NewSnapshot(2, Ordinary, []Table{
		replicaTable("db3.c", 1, 2),
		replicaTable("db2.b", 3, 0),
		{ID: "db1.a", Status: Active},
		replicaTable("db2.a", 7, 0),
	})
	if got := ServedDatabases(snap); !reflect.DeepEqual(got, []string{"db2", "db3"}) {
		t.Fatalf("ServedDatabases = %v, want [db2 db3]", got)
	}
	if got := ServedDatabases(nil); got != nil {
		t.Fatalf("ServedDatabases(nil) = %v, want nil", got)
	}
	if got := ServedDatabases(NewSnapshot(1, Pending, []Table{{ID: "db1.a", Status: Active}})); got != nil {
		t.Fatalf("ServedDatabases of a snapshot without Replica tables = %v, want nil", got)
	}
}

// TestSnapshotSchemaMissesAReplicaTable: a Replica entry carries the owner's
// registry schema for DESCRIBE and the rewriter's projection, but Schema
// misses it, since journal recovery and cleanup never run for a peer's table.
func TestSnapshotSchemaMissesAReplicaTable(t *testing.T) {
	snap := NewSnapshot(1, Ordinary, []Table{replicaTable("db2.a", 7, 0)})
	if table, ok := snap.Schema("db2.a"); ok {
		t.Fatalf("Schema(db2.a) = %+v, want a miss for a Replica table", table)
	}
	if lookup := snap.Lookup("db2", "a"); lookup.Schema.TableID != "db2.a" || lookup.SchemaHash == "" {
		t.Fatalf("Lookup(db2, a) = %+v, want the Replica entry to keep its schema and hash", lookup)
	}
}

// TestFakeServesReplicaTables: the Fake serves exactly the Replica tables its
// latest Set was given, and an earlier snapshot keeps what it served.
func TestFakeServesReplicaTables(t *testing.T) {
	f := NewFake(Ordinary, replicaTable("db2.a", 7, 0))
	first := f.Current()
	if served := first.Served(); len(served) != 1 || served[0].ID != "db2.a" {
		t.Fatalf("Served() = %+v, want db2.a", served)
	}
	if v := f.Set(Table{ID: "db2.a", Status: Ordinary}); v != 2 {
		t.Fatalf("Set returned version %d, want 2", v)
	}
	if served := f.Current().Served(); len(served) != 0 {
		t.Fatalf("after serving ended Served() = %+v, want none", served)
	}
	if got := ServedDatabases(f.Current()); got != nil {
		t.Fatalf("after serving ended ServedDatabases = %v, want nil", got)
	}
	if served := first.Served(); len(served) != 1 {
		t.Fatalf("an earlier snapshot must stay immutable, Served() = %+v", served)
	}
	f.Set(replicaTable("db2.a", 8, 0), replicaTable("db2.b", 2, 0))
	if got := ServedDatabases(f.Current()); !reflect.DeepEqual(got, []string{"db2"}) {
		t.Fatalf("ServedDatabases = %v, want [db2]", got)
	}
}

// TestStaticServesNoReplicaTable: the static table set is the host's own
// tables, every one Active, so it serves nothing from a copy.
func TestStaticServesNoReplicaTable(t *testing.T) {
	st := NewStatic([]string{"db1.t", "db1.u"}, nil, "net")
	if served := st.Current().Served(); len(served) != 0 {
		t.Fatalf("Static Served() = %+v, want none", served)
	}
	if got := ServedDatabases(st.Current()); got != nil {
		t.Fatalf("Static ServedDatabases = %v, want nil", got)
	}
}
