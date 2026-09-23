package payloadexec

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay"
)

func TestSchemaRootFromHashesMatchesSchemaRoot(t *testing.T) {
	a := TableSchema{TableID: "db.a", Columns: []lthash.Column{{Name: "v", Type: "UInt64"}}}
	b := TableSchema{TableID: "db.b", PartitionBy: "p", Columns: []lthash.Column{{Name: "p", Type: "String"}, {Name: "v", Type: "Int64"}}}
	c := TableSchema{TableID: "db.c", Columns: []lthash.Column{{Name: "s", Type: "String"}}}
	for _, set := range [][]TableSchema{nil, {a}, {b, a}, {c, a, b}} {
		hashes := make(map[string]string, len(set))
		for _, s := range set {
			hashes[s.TableID] = TableSchemaHash(testNetwork, s)
		}
		if got, want := SchemaRootFromHashes(hashes), SchemaRoot(testNetwork, set); got != want {
			t.Fatalf("SchemaRootFromHashes(%d tables) = %s, SchemaRoot = %s", len(set), got, want)
		}
	}
}

// A dynamic executor over the same static tables must reproduce every frozen
// vector of the static one: NewDynamic changes which inputs are admissible,
// never what an admissible input produces.
func TestDynamicExecutorReproducesTheLegacyFrozenVector(t *testing.T) {
	static := testExecutor()
	dynamic := NewDynamic(testNetwork, csvMaterializer{networkID: testNetwork}, static.tables[testTable])
	prev := genesis(t, dynamic)
	next, result := applyInsert(t, dynamic, prev, "frozen-append", "payload-frozen", "name,balance\nalice,10\nbob,20\n")
	b, err := json.Marshal(struct{ Next, Result any }{next, result})
	if err != nil {
		t.Fatal(err)
	}
	const want = "a201e4f19e20e079de4eeea63b64042859eb3abc92ac3b7479fa3aaf4d82f593" // TestLegacyAppendFrozenVector
	if got := fmt.Sprintf("%x", sha256.Sum256(b)); got != want {
		t.Fatalf("dynamic executor changed the frozen vector: got %s want %s", got, want)
	}
}

const addedTable = "db.new"

func addedSchema() TableSchema {
	return TableSchema{TableID: addedTable, Columns: []lthash.Column{{Name: "name", Type: "String"}, {Name: "balance", Type: "UInt64"}}}
}

func addedSchemaJSON(t *testing.T) string {
	t.Helper()
	b, err := json.Marshal(addedSchema())
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// twoTableDynamic is a dynamic executor configured with testTable and
// "table-2"; prev has one row in testTable so survivors carry data.
func twoTableDynamic(t *testing.T) (*Executor, replay.SafeSnapshotManifest) {
	t.Helper()
	first := testExecutor().tables[testTable]
	second := TableSchema{TableID: "table-2", Columns: []lthash.Column{{Name: "v", Type: "UInt64"}}}
	e := NewDynamic(testNetwork, csvMaterializer{networkID: testNetwork}, first, second)
	prev, _ := applyInsert(t, e, genesis(t, e), "seed-row", "payload-seed", "name,balance\nalice,10\n")
	return e, prev
}

func transitionFor(t *testing.T, e *Executor, prev replay.SafeSnapshotManifest) replay.ReplayJob {
	t.Helper()
	hashes := map[string]string{}
	for _, tm := range prev.Tables {
		hashes[tm.TableID] = tm.SchemaHash
	}
	delete(hashes, "table-2")
	hashes[addedTable] = TableSchemaHash(e.NetworkID, addedSchema())
	job := blockJob(prev)
	job.TableSetTransition = &replay.ReplayTableSetTransition{
		Adds:          []replay.ReplayTableSchema{{TableID: addedTable, SchemaJSON: addedSchemaJSON(t)}},
		Retires:       []string{"table-2"},
		NewSchemaRoot: SchemaRootFromHashes(hashes),
	}
	return job
}

func TestApplyRowsTableSetTransition(t *testing.T) {
	e, prev := twoTableDynamic(t)
	job := transitionFor(t, e, prev)
	next, result, err := e.ApplyRows(context.Background(), prev, job, nil)
	if err != nil {
		t.Fatalf("ApplyRows(transition): %v", err)
	}
	// Independent recomputation: survivors keep their partition roots, the
	// retired table disappears, the added table enters with no partitions.
	var survivor replay.TableManifest
	for _, tm := range prev.Tables {
		if tm.TableID == testTable {
			survivor = replay.TableManifest{TableID: tm.TableID, SchemaHash: tm.SchemaHash, PartitionRoots: tm.PartitionRoots}
		}
	}
	added := replay.TableManifest{TableID: addedTable, SchemaHash: TableSchemaHash(testNetwork, addedSchema())}
	_, wantRoot, err := replay.AssembleStateRoot(prev.SchemaSnapshotID, job.TableSetTransition.NewSchemaRoot, prev.ExecutorProfileID, []replay.TableManifest{survivor, added})
	if err != nil {
		t.Fatal(err)
	}
	if result.ComputedStateRoot != wantRoot || next.StateRoot != wantRoot || next.SchemaRoot != job.TableSetTransition.NewSchemaRoot {
		t.Fatalf("transition root = %s (schema %s), want %s (schema %s)", result.ComputedStateRoot, next.SchemaRoot, wantRoot, job.TableSetTransition.NewSchemaRoot)
	}
	if len(result.PartitionCommitmentsAfter) != 0 || len(result.AffectedParts) != 0 {
		t.Fatalf("a transition touches no partition: %+v", result)
	}
	if len(next.Tables) != 2 || next.Tables[0].TableID != addedTable || len(next.Tables[0].PartitionRoots) != 0 || next.Tables[1].TableID != testTable || len(next.Tables[1].ActiveParts) != 1 {
		t.Fatalf("post-transition tables = %+v", next.Tables)
	}
	// The next block can write the added table with the schema the job carries.
	insert, prepared := insertJob(next, "into-added", "payload-added", []byte("name,balance\nbob,7\n"), "")
	insert.Statements[0].TargetTableID, prepared[0].TargetTableID = addedTable, addedTable
	if _, _, err := e.Apply(next, insert, prepared); err == nil || !strings.Contains(err.Error(), "unknown target table") {
		t.Fatalf("an added table is unknown until the job carries its schema: %v", err)
	}
	insert.TableSchemas = []replay.ReplayTableSchema{{TableID: addedTable, SchemaJSON: addedSchemaJSON(t)}}
	after, _, err := e.Apply(next, insert, prepared)
	if err != nil {
		t.Fatalf("insert into added table: %v", err)
	}
	if after.SchemaRoot != next.SchemaRoot {
		t.Fatal("an ordinary block must keep the transitioned schema root")
	}
}

// Pins the transition encoding end to end, like TestLegacyAppendFrozenVector.
func TestTableSetTransitionFrozenVector(t *testing.T) {
	e, prev := twoTableDynamic(t)
	next, result, err := e.ApplyRows(context.Background(), prev, transitionFor(t, e, prev), nil)
	if err != nil {
		t.Fatal(err)
	}
	b := encodeFixture(t, struct{ Next, Result any }{next, result})
	const want = "baf42e0346ecd7e4cadc2af1d82d60bc4b331a72bf358e3e84802b1e61ac93ff"
	if got := fmt.Sprintf("%x", sha256.Sum256(b)); got != want {
		t.Fatalf("transition vector changed: got %s want %s", got, want)
	}
}

func TestApplyRowsTableSetTransitionRefusals(t *testing.T) {
	e, prev := twoTableDynamic(t)
	for _, tc := range []struct {
		name string
		want string
		edit func(*replay.ReplayJob)
	}{
		{"retire absent", "not in the previous safe snapshot", func(j *replay.ReplayJob) { j.TableSetTransition.Retires = []string{"table-9"} }},
		{"add present", "already in the previous safe snapshot", func(j *replay.ReplayJob) {
			same, err := json.Marshal(testExecutor().tables[testTable])
			if err != nil {
				t.Fatal(err)
			}
			j.TableSetTransition.Adds = []replay.ReplayTableSchema{{TableID: testTable, SchemaJSON: string(same)}}
			j.TableSetTransition.Retires = nil
		}},
		{"schema root", "new_schema_root mismatch", func(j *replay.ReplayJob) { j.TableSetTransition.NewSchemaRoot = replay.DigestString("forged") }},
		{"foreign table id", `names table "db.other"`, func(j *replay.ReplayJob) {
			j.TableSetTransition.Adds[0].SchemaJSON = `{"table_id":"db.other","columns":[{"name":"v","type":"UInt64"}]}`
		}},
		{"unadmitted type", "Decimal", func(j *replay.ReplayJob) {
			j.TableSetTransition.Adds[0].SchemaJSON = `{"table_id":"db.new","columns":[{"name":"v","type":"Decimal(10, 2)"}]}`
		}},
		{"empty change", "add or retire at least one table", func(j *replay.ReplayJob) {
			j.TableSetTransition.Adds, j.TableSetTransition.Retires = nil, nil
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			job := transitionFor(t, e, prev)
			tc.edit(&job)
			m, r, err := e.ApplyRows(context.Background(), prev, job, nil)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ApplyRows = %v, want %q", err, tc.want)
			}
			assertNoAppendResult(t, m, r, err)
		})
	}
	withRows := transitionFor(t, e, prev)
	rows := &trackedRows{rows: []Row{appendRow(testTable, "x", 0, "carol")}}
	if _, _, err := e.ApplyRows(context.Background(), prev, withRows, []StatementRows{{"x", 1, testTable, rows}}); err == nil || !strings.Contains(err.Error(), "must carry no statements") || rows.calls != 0 {
		t.Fatalf("transition with statements = %v (rows read %d)", err, rows.calls)
	}
	static := testExecutor()
	staticPrev := genesis(t, static)
	staticJob := blockJob(staticPrev)
	staticJob.TableSetTransition = &replay.ReplayTableSetTransition{Retires: []string{testTable}, NewSchemaRoot: SchemaRootFromHashes(nil)}
	if _, _, err := static.ApplyRows(context.Background(), staticPrev, staticJob, nil); err == nil || !strings.Contains(err.Error(), "NewDynamic") {
		t.Fatalf("a static executor must refuse a transition: %v", err)
	}
}

// A dynamic verifier's previous snapshot may hold a chain-origin table it has
// no schema for (not targeted by this job); the snapshot's own schema root
// still binds that table's hash.
func TestDynamicExecutorBindsUnresolvedPrevTablesThroughSchemaRoot(t *testing.T) {
	e, prev := twoTableDynamic(t)
	next, _, err := e.ApplyRows(context.Background(), prev, transitionFor(t, e, prev), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.ApplyRows(context.Background(), next, blockJob(next), nil); err != nil {
		t.Fatalf("an untargeted unresolved table must be accepted: %v", err)
	}
	forged := next
	forged.Tables = append([]replay.TableManifest(nil), next.Tables...)
	forged.Tables[0].SchemaHash = replay.DigestString("other-schema")
	forged, err = forged.Seal()
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.ApplyRows(context.Background(), forged, blockJob(forged), nil); err == nil || !strings.Contains(err.Error(), "complete schema_root mismatch") {
		t.Fatalf("a hash the schema root does not bind must be refused: %v", err)
	}
}

// recreatedTableDynamic builds a dynamic executor whose static set names
// testTable under schema S1 (via testExecutor) plus an unrelated "table-2",
// and a prev snapshot that instead commits testTable under a DIFFERENT
// schema S2 (as if testTable were retired and recreated per spec D9, with
// table-2 untouched). The static schema is stale for testTable; the returned
// snapshot's SchemaRoot is consistent with the committed (S2) hashes.
func recreatedTableDynamic(t *testing.T) (e *Executor, s2 TableSchema, prev replay.SafeSnapshotManifest) {
	t.Helper()
	s1 := testExecutor().tables[testTable]
	other := TableSchema{TableID: "table-2", Columns: []lthash.Column{{Name: "v", Type: "UInt64"}}}
	e = NewDynamic(testNetwork, csvMaterializer{networkID: testNetwork}, s1, other)
	s2 = TableSchema{TableID: testTable, Columns: []lthash.Column{
		{Name: "name", Type: "String"}, {Name: "balance", Type: "UInt64"}, {Name: "extra", Type: "String"},
	}}
	hashes := map[string]string{
		testTable: TableSchemaHash(testNetwork, s2),
		"table-2": TableSchemaHash(testNetwork, other),
	}
	prev, err := (replay.SafeSnapshotManifest{
		SafeBlockSeq:      5,
		SchemaSnapshotID:  testSchema,
		SchemaRoot:        SchemaRootFromHashes(hashes),
		ExecutorProfileID: testProfile,
		Tables: []replay.TableManifest{
			{TableID: testTable, SchemaHash: hashes[testTable]},
			{TableID: "table-2", SchemaHash: hashes["table-2"]},
		},
	}).Seal()
	if err != nil {
		t.Fatalf("seal recreated-table prev: %v", err)
	}
	return e, s2, prev
}

// A table retired and recreated under the same name with a different schema
// (spec D9) is committed in prev under its NEW schema hash, while the
// executor's static configuration still names the OLD (genesis) schema. A
// static schema is a fallback, not authoritative: it must not stall every
// block that does not target the recreated table, but a block that DOES
// target it without carrying the new schema must still refuse.
func TestApplyRowsRecreatedTableStaleStaticFallsBackWhenUntargeted(t *testing.T) {
	e, s2, prev := recreatedTableDynamic(t)

	t.Run("untargeted insert succeeds", func(t *testing.T) {
		insert, prepared := insertJob(prev, "into-other", "payload-other", []byte("v\n5\n"), "")
		insert.Statements[0].TargetTableID, prepared[0].TargetTableID = "table-2", "table-2"
		if _, _, err := e.Apply(prev, insert, prepared); err != nil {
			t.Fatalf("insert targeting an unrelated table must not stall on the stale static fallback: %v", err)
		}
	})

	t.Run("untargeted transition succeeds", func(t *testing.T) {
		const thirdTable = "table-3"
		third := TableSchema{TableID: thirdTable, Columns: []lthash.Column{{Name: "v", Type: "UInt64"}}}
		thirdJSON, err := json.Marshal(third)
		if err != nil {
			t.Fatal(err)
		}
		hashes := map[string]string{
			testTable:  TableSchemaHash(testNetwork, s2),
			"table-2":  TableSchemaHash(testNetwork, TableSchema{TableID: "table-2", Columns: []lthash.Column{{Name: "v", Type: "UInt64"}}}),
			thirdTable: TableSchemaHash(testNetwork, third),
		}
		job := blockJob(prev)
		job.TableSetTransition = &replay.ReplayTableSetTransition{
			Adds:          []replay.ReplayTableSchema{{TableID: thirdTable, SchemaJSON: string(thirdJSON)}},
			NewSchemaRoot: SchemaRootFromHashes(hashes),
		}
		if _, _, err := e.ApplyRows(context.Background(), prev, job, nil); err != nil {
			t.Fatalf("a transition block that does not target the recreated table must not stall: %v", err)
		}
	})

	t.Run("targeted insert without the new schema refuses", func(t *testing.T) {
		insert, prepared := insertJob(prev, "into-recreated-stale", "payload-stale", []byte("name,balance\nalice,10\n"), "")
		if _, _, err := e.Apply(prev, insert, prepared); err == nil || !strings.Contains(err.Error(), "schema_hash mismatch") {
			t.Fatalf("targeting the recreated table without its new schema = %v, want schema_hash mismatch", err)
		}
	})

	t.Run("targeted insert carrying the new schema succeeds", func(t *testing.T) {
		s2JSON, err := json.Marshal(s2)
		if err != nil {
			t.Fatal(err)
		}
		insert, prepared := insertJob(prev, "into-recreated-fresh", "payload-fresh", []byte("name,balance,extra\nalice,10,x\n"), "")
		insert.TableSchemas = []replay.ReplayTableSchema{{TableID: testTable, SchemaJSON: string(s2JSON)}}
		if _, _, err := e.Apply(prev, insert, prepared); err != nil {
			t.Fatalf("targeting the recreated table with its carried new schema must succeed: %v", err)
		}
	})
}

// A job-carried schema (TableSchemas or a transition add) is always
// authoritative and checked strictly: it never falls back the way an
// unrelated static schema does.
func TestApplyRowsJobCarriedSchemaMismatchRefuses(t *testing.T) {
	e, prev := twoTableDynamic(t)
	next, _, err := e.ApplyRows(context.Background(), prev, transitionFor(t, e, prev), nil)
	if err != nil {
		t.Fatal(err)
	}
	insert, prepared := insertJob(next, "into-added-wrong", "payload-wrong", []byte("name\nbob\n"), "")
	insert.Statements[0].TargetTableID, prepared[0].TargetTableID = addedTable, addedTable
	wrong := TableSchema{TableID: addedTable, Columns: []lthash.Column{{Name: "name", Type: "String"}}}
	wrongJSON, err := json.Marshal(wrong)
	if err != nil {
		t.Fatal(err)
	}
	insert.TableSchemas = []replay.ReplayTableSchema{{TableID: addedTable, SchemaJSON: string(wrongJSON)}}
	if _, _, err := e.Apply(next, insert, prepared); err == nil || !strings.Contains(err.Error(), "schema_hash mismatch") {
		t.Fatalf("a job-carried schema that disagrees with the committed hash must refuse: %v", err)
	}
}

func TestSchemaHashesForJob(t *testing.T) {
	static := testExecutor().tables[testTable]
	src := SchemaHashes{NetworkID: testNetwork, Tables: []TableSchema{static}}
	if got, ok := src.TableSchemaHash(testTable); !ok || got != TableSchemaHash(testNetwork, static) {
		t.Fatalf("static hash = %s %v", got, ok)
	}
	job := replay.ReplayJob{TableSchemas: []replay.ReplayTableSchema{{TableID: addedTable, SchemaJSON: addedSchemaJSON(t)}}}
	scoped, err := src.ForJob(job)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := scoped.TableSchemaHash(addedTable); !ok || got != TableSchemaHash(testNetwork, addedSchema()) {
		t.Fatalf("job-carried hash = %s %v", got, ok)
	}
	if _, ok := src.TableSchemaHash(addedTable); ok {
		t.Fatal("ForJob must not change the static source")
	}
	job.TableSchemas[0].SchemaJSON = "{"
	if _, err := src.ForJob(job); err == nil || !strings.Contains(err.Error(), "decode schema_json") {
		t.Fatalf("malformed carried schema = %v", err)
	}
}
