package payloadexec

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay"
	"io"
	"math"
	"reflect"
	"strings"
	"testing"
)

// Frozen against the pre-extraction executor, including physical/accounting
// and replay-log fields that the logical state-root vector does not commit.
func TestLegacyAppendFrozenVector(t *testing.T) {
	e := testExecutor()
	prev := genesis(t, e)
	next, result := applyInsert(t, e, prev, "frozen-append", "payload-frozen", "name,balance\nalice,10\nbob,20\n")
	b, err := json.Marshal(struct{ Next, Result any }{next, result})
	if err != nil {
		t.Fatal(err)
	}
	const want = "a201e4f19e20e079de4eeea63b64042859eb3abc92ac3b7479fa3aaf4d82f593"
	if got := fmt.Sprintf("%x", sha256.Sum256(b)); got != want {
		t.Fatalf("legacy append vector changed: got %s want %s", got, want)
	}
	rows := &trackedRows{rows: []Row{
		{RowID: RowID(testNetwork, testTable, "frozen-append", 0), Values: []any{"alice", uint64(10)}, PartitionID: "all", RawBytes: 7},
		{RowID: RowID(testNetwork, testTable, "frozen-append", 1), Values: []any{"bob", uint64(20)}, PartitionID: "all", RawBytes: 5},
	}}
	next, result, err = e.ApplyRows(context.Background(), prev, blockJob(prev), []StatementRows{{"frozen-append", 1, testTable, rows}})
	if err != nil {
		t.Fatal(err)
	}
	b = encodeFixture(t, struct{ Next, Result any }{next, result})
	if got := fmt.Sprintf("%x", sha256.Sum256(b)); got != want {
		t.Fatalf("direct append changed frozen vector: got %s want %s", got, want)
	}
}

type trackedRows struct {
	rows          []Row
	calls, closes int
	err, closeErr error
	onNext        func()
}

func (s *trackedRows) Next(context.Context) (Row, error) {
	s.calls++
	if s.onNext != nil {
		s.onNext()
	}
	if s.calls <= len(s.rows) {
		return s.rows[s.calls-1], nil
	}
	if s.err != nil {
		return Row{}, s.err
	}
	return Row{}, io.EOF
}
func (s *trackedRows) Close() error { s.closes++; return s.closeErr }

func appendRow(table, statement string, ordinal uint64, name string) Row {
	return Row{RowID: RowID(testNetwork, table, statement, ordinal), Values: []any{name, uint64(10)}, PartitionID: "all", RawBytes: uint64(len(name) + 2)}
}

func encodeFixture(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func assertNoAppendResult(t *testing.T, m replay.SafeSnapshotManifest, r replay.ExecutionResult, err error) {
	t.Helper()
	if err == nil || !reflect.DeepEqual(m, replay.SafeSnapshotManifest{}) || !reflect.DeepEqual(r, replay.ExecutionResult{}) {
		t.Fatalf("must refuse without partial result: %v %+v %+v", err, m, r)
	}
}

func TestApplyRowsWholeLedgerAndSequentialSelfInsert(t *testing.T) {
	baseSchema := testExecutor().tables[testTable]
	var schemas []TableSchema
	for _, id := range []string{"R", "W", "U"} {
		s := baseSchema
		s.TableID = id
		schemas = append(schemas, s)
	}
	e := New(testNetwork, schemas...)
	prev := genesis(t, e)
	// Seed with the legacy adapter. Assertions below preserve these independent
	// predecessor ledger entries byte-for-byte, rather than rebuilding them.
	var statements []replay.PreparedStatement
	for i, id := range []string{"R", "W", "U"} {
		st := preparedInsert("seed-"+id, uint64(i+1), "payload-"+id, []byte("name,balance\n"+strings.Repeat("alice,10\n", []int{2, 1, 3}[i])))
		st.TargetTableID = id
		statements = append(statements, st)
	}
	prev, _, err := e.Apply(prev, blockJob(prev, statements...), statements)
	if err != nil {
		t.Fatal(err)
	}
	original := encodeFixture(t, prev)
	source := &trackedRows{rows: []Row{appendRow("W", "copy-R", 0, "alice"), appendRow("W", "copy-R", 1, "alice")}}
	next, res, err := e.ApplyRows(context.Background(), prev, blockJob(prev), []StatementRows{{"copy-R", 4, "W", source}})
	if err != nil {
		t.Fatal(err)
	}
	if source.calls != 3 || source.closes != 0 {
		t.Fatalf("borrowed source calls=%d closes=%d", source.calls, source.closes)
	}
	if !bytes.Equal(original, encodeFixture(t, prev)) {
		t.Fatal("predecessor mutated")
	}
	if next.ParentSnapshotID != prev.SnapshotID || next.SafeBlockSeq != prev.SafeBlockSeq+1 {
		t.Fatal("broken parent link")
	}
	for i, table := range next.Tables {
		var count uint64
		for _, part := range table.ActiveParts {
			count += part.RowCount
		}
		want := map[string]uint64{"R": 2, "W": 3, "U": 3}[table.TableID]
		if count != want {
			t.Fatalf("%s rows=%d want=%d", table.TableID, count, want)
		}
		if table.TableID != "W" && !bytes.Equal(encodeFixture(t, table), encodeFixture(t, prev.Tables[i])) {
			t.Fatalf("untouched %s changed", table.TableID)
		}
	}
	if len(res.AffectedParts) != 1 || res.AffectedParts[0].PartName != "all-b2-s4" || res.AffectedParts[0].Bytes != 14 {
		t.Fatalf("wrong part: %+v", res.AffectedParts)
	}
	// The predecessor commitments include its original row IDs; the independent
	// expected accumulator adds only new statement IDs on each safe self-insert.
	prev = next
	for round, count := range []int{2, 4} {
		id := fmt.Sprintf("self-%d", round)
		rows := make([]Row, count)
		want := partitionAccum(t, prev, "R", "all")
		for i := range rows {
			rows[i] = appendRow("R", id, uint64(i), "alice")
			h, err := lthash.RowHash("R", append([]lthash.Column{{Name: rowIDColumn, Type: "FixedString(32)"}}, baseSchema.Columns...), append([]any{rows[i].RowID}, rows[i].Values...))
			if err != nil {
				t.Fatal(err)
			}
			want.AddHash(h)
		}
		s := &trackedRows{rows: rows}
		next, _, err = e.ApplyRows(context.Background(), prev, blockJob(prev), []StatementRows{{id, uint64(5 + round), "R", s}})
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(want.Bytes(), partitionAccum(t, next, "R", "all").Bytes()) {
			t.Fatal("self-insert lost original IDs or mis-hashed new IDs")
		}
		var oldR, newR replay.TableManifest
		for _, tb := range prev.Tables {
			if tb.TableID == "R" {
				oldR = tb
			}
		}
		for _, tb := range next.Tables {
			if tb.TableID == "R" {
				newR = tb
			}
		}
		var total uint64
		for _, p := range newR.ActiveParts {
			total += p.RowCount
		}
		if total != uint64(count*2) {
			t.Fatalf("self-insert count=%d", total)
		}
		for _, old := range oldR.ActiveParts {
			found := false
			for _, p := range newR.ActiveParts {
				if reflect.DeepEqual(old, p) {
					found = true
				}
			}
			if !found {
				t.Fatal("old part/IDs changed")
			}
		}
		prev = next
	}
}

func TestApplyRowsEmptyPreservesAllPartitions(t *testing.T) {
	e := testExecutor()
	prev, _ := applyInsert(t, e, genesis(t, e), "seed", "payload", "name,balance\na,10\n")
	// A declared zero-root partition is a valid ledger entry even without parts.
	prev.Tables[0].PartitionRoots = append(prev.Tables[0].PartitionRoots, replay.PartitionCommitment{TableID: testTable, PartitionID: "empty", Root: lthashHex(lthash.New())})
	prev.SnapshotID = ""
	var err error
	prev, err = prev.Seal()
	if err != nil {
		t.Fatal(err)
	}
	s := &trackedRows{}
	next, res, err := e.ApplyRows(context.Background(), prev, blockJob(prev), []StatementRows{{"zero", 2, testTable, s}})
	if err != nil {
		t.Fatal(err)
	}
	if next.DataRoot != prev.DataRoot || next.StateRoot != prev.StateRoot || !bytes.Equal(encodeFixture(t, next.Tables), encodeFixture(t, prev.Tables)) {
		t.Fatal("empty output changed data ledger")
	}
	if next.SnapshotID == prev.SnapshotID || next.ParentSnapshotID != prev.SnapshotID || len(res.AffectedParts) != 0 || len(res.PartitionCommitmentsAfter) != 0 {
		t.Fatal("empty output successor identity/result incorrect")
	}
	if s.calls != 1 || s.closes != 0 {
		t.Fatal("empty borrowed stream ownership")
	}
}

func TestApplyRowsPreflightRefusesBeforeNext(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Executor, *replay.SafeSnapshotManifest, *[]StatementRows)
	}{
		{"missing predecessor table", func(e *Executor, m *replay.SafeSnapshotManifest, b *[]StatementRows) { m.Tables = nil }},
		{"extra predecessor table", func(e *Executor, m *replay.SafeSnapshotManifest, b *[]StatementRows) {
			m.Tables = append(m.Tables, replay.TableManifest{TableID: "extra"})
		}},
		{"missing configured table", func(e *Executor, m *replay.SafeSnapshotManifest, b *[]StatementRows) { delete(e.tables, testTable) }},
		{"extra configured table", func(e *Executor, m *replay.SafeSnapshotManifest, b *[]StatementRows) {
			e.tables["extra"] = TableSchema{TableID: "extra"}
		}},
		{"duplicate predecessor", func(e *Executor, m *replay.SafeSnapshotManifest, b *[]StatementRows) {
			m.Tables = append(m.Tables, m.Tables[0])
		}},
		{"schema hash", func(e *Executor, m *replay.SafeSnapshotManifest, b *[]StatementRows) { m.Tables[0].SchemaHash = "bad" }},
		{"projection", func(e *Executor, m *replay.SafeSnapshotManifest, b *[]StatementRows) {
			s := e.tables[testTable]
			s.PartitionBy = "name"
			e.tables[testTable] = s
		}},
		{"schema root", func(e *Executor, m *replay.SafeSnapshotManifest, b *[]StatementRows) { m.SchemaRoot = "bad" }},
		{"data root", func(e *Executor, m *replay.SafeSnapshotManifest, b *[]StatementRows) { m.DataRoot = "bad" }},
		{"state root", func(e *Executor, m *replay.SafeSnapshotManifest, b *[]StatementRows) { m.StateRoot = "bad" }},
		{"manifest root", func(e *Executor, m *replay.SafeSnapshotManifest, b *[]StatementRows) { m.ManifestRoot = "bad" }},
		{"empty ID", func(e *Executor, m *replay.SafeSnapshotManifest, b *[]StatementRows) { (*b)[1].StatementID = "" }},
		{"duplicate ID", func(e *Executor, m *replay.SafeSnapshotManifest, b *[]StatementRows) {
			(*b)[1].StatementID = (*b)[0].StatementID
		}},
		{"non increasing seq", func(e *Executor, m *replay.SafeSnapshotManifest, b *[]StatementRows) { (*b)[1].StatementSeq = 1 }},
		{"zero seq", func(e *Executor, m *replay.SafeSnapshotManifest, b *[]StatementRows) { (*b)[0].StatementSeq = 0 }},
		{"unknown zero row target", func(e *Executor, m *replay.SafeSnapshotManifest, b *[]StatementRows) {
			(*b)[1].TargetTableID = "unknown"
		}},
		{"nil source", func(e *Executor, m *replay.SafeSnapshotManifest, b *[]StatementRows) { (*b)[1].Rows = nil }},
		{"typed nil source", func(e *Executor, m *replay.SafeSnapshotManifest, b *[]StatementRows) {
			var s *trackedRows
			(*b)[1].Rows = s
		}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			e := testExecutor()
			prev := genesis(t, e)
			first, second := &trackedRows{}, &trackedRows{}
			b := []StatementRows{{"one", 1, testTable, first}, {"two", 2, testTable, second}}
			tt.mutate(e, &prev, &b)
			// Reseal shape/schema mutations so rejection must check the ledger
			// contract itself, not merely catch a stale manifest digest.
			if tt.name != "data root" && tt.name != "state root" && tt.name != "manifest root" {
				var err error
				prev.SnapshotID = ""
				prev, err = prev.Seal()
				if err != nil {
					t.Fatal(err)
				}
			}
			m, r, err := e.ApplyRows(context.Background(), prev, blockJob(prev), b)
			assertNoAppendResult(t, m, r, err)
			if first.calls != 0 || second.calls != 0 || first.closes != 0 || second.closes != 0 {
				t.Fatal("preflight consumed or closed borrowed source")
			}
		})
	}
}

func TestApplyRowsPreservesUntouchedUnsupportedSchema(t *testing.T) {
	sch := testExecutor().tables[testTable]
	u := TableSchema{TableID: "U", Columns: []lthash.Column{{Name: "opaque", Type: "AggregateFunction(uniq, UInt64)"}}}
	e := NewWithMaterializer(testNetwork, nil, sch, u)
	prev := genesis(t, e)
	s := &trackedRows{rows: []Row{appendRow(testTable, "x", 0, "a")}}
	next, _, err := e.ApplyRows(context.Background(), prev, blockJob(prev), []StatementRows{{"x", 1, testTable, s}})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encodeFixture(t, prev.Tables[0]), encodeFixture(t, next.Tables[0])) {
		t.Fatal("untouched U changed")
	}
}

func TestApplyRowsInvalidLedgerBeforeNext(t *testing.T) {
	for _, name := range []string{"part hash", "root sum", "orphan", "duplicate partition", "partition table", "part table", "duplicate part"} {
		t.Run(name, func(t *testing.T) {
			e := testExecutor()
			prev, _ := applyInsert(t, e, genesis(t, e), "seed", "payload", "name,balance\na,10\n")
			tb := &prev.Tables[0]
			switch name {
			case "part hash":
				tb.ActiveParts[0].PartRowLtHash = "bad"
			case "root sum":
				tb.PartitionRoots[0].Root = lthashHex(lthash.New())
			case "orphan":
				tb.PartitionRoots = nil
			case "duplicate partition":
				tb.PartitionRoots = append(tb.PartitionRoots, tb.PartitionRoots[0])
			case "partition table":
				tb.PartitionRoots[0].TableID = "wrong"
			case "part table":
				tb.ActiveParts[0].TableID = "wrong"
			case "duplicate part":
				tb.ActiveParts = append(tb.ActiveParts, tb.ActiveParts[0])
				h := partitionAccum(t, prev, testTable, "all")
				h.AddHash(h)
				tb.PartitionRoots[0].Root = lthashHex(h)
			}
			prev.SnapshotID = ""
			var err error
			prev, err = prev.Seal()
			if err != nil {
				t.Fatal(err)
			}
			s := &trackedRows{}
			m, r, err := e.ApplyRows(context.Background(), prev, blockJob(prev), []StatementRows{{"x", 2, testTable, s}})
			assertNoAppendResult(t, m, r, err)
			if s.calls != 0 || s.closes != 0 {
				t.Fatal("invalid ledger consumed/closed source")
			}
		})
	}
}

// Reuses its row buffers on every Next, as a disk cursor may do. Deferring row
// hashing until after another Next (or collecting all rows) changes the answer.
type recyclingRows struct {
	index  int
	id     []byte
	values []any
	seen   *[]int
}

func (s *recyclingRows) Next(context.Context) (Row, error) {
	*s.seen = append(*s.seen, s.index)
	if s.index == 3 {
		return Row{}, io.EOF
	}
	copy(s.id, RowID(testNetwork, testTable, "recycle", uint64(s.index)))
	s.values[0] = fmt.Sprintf("row-%d", s.index)
	s.values[1] = uint64(s.index)
	partition := []string{"b", "a", "b"}[s.index]
	s.index++
	return Row{RowID: s.id, Values: s.values, PartitionID: partition, RawBytes: uint64(s.index)}, nil
}
func (*recyclingRows) Close() error { panic("borrowed cursor closed") }

func TestApplyRowsStreamsInOrderWithReusedBuffers(t *testing.T) {
	e := testExecutor()
	prev := genesis(t, e)
	var seen []int
	s := &recyclingRows{id: make([]byte, 32), values: make([]any, 2), seen: &seen}
	next, r, err := e.ApplyRows(context.Background(), prev, blockJob(prev), []StatementRows{{"recycle", 1, testTable, s}})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(seen, []int{0, 1, 2, 3}) {
		t.Fatalf("Next order: %v", seen)
	}
	if len(r.AffectedParts) != 2 || r.AffectedParts[0].PartName != "a-b1-s1" || r.AffectedParts[1].PartName != "b-b1-s1" || r.AffectedParts[0].Bytes != 2 || r.AffectedParts[1].Bytes != 4 {
		t.Fatal("partition grouping or accounting changed")
	}
	for _, partition := range []string{"a", "b"} {
		want := lthash.New()
		for i := 0; i < 3; i++ {
			if []string{"b", "a", "b"}[i] != partition {
				continue
			}
			h, err := lthash.RowHash(testTable, []lthash.Column{{Name: rowIDColumn, Type: "FixedString(32)"}, {Name: "name", Type: "String"}, {Name: "balance", Type: "UInt64"}}, []any{RowID(testNetwork, testTable, "recycle", uint64(i)), fmt.Sprintf("row-%d", i), uint64(i)})
			if err != nil {
				t.Fatal(err)
			}
			want.AddHash(h)
		}
		if !bytes.Equal(want.Bytes(), partitionAccum(t, next, testTable, partition).Bytes()) {
			t.Fatal("rows retained past Next or hashed incorrectly")
		}
	}
}

func TestLegacyOwnedRowsCloseOnEveryPath(t *testing.T) {
	closeErr, streamErr := errors.New("close failed"), errors.New("stream failed")
	for _, name := range []string{"success", "stream error", "preflight error", "close error", "both errors"} {
		t.Run(name, func(t *testing.T) {
			e := testExecutor()
			prev := genesis(t, e)
			a, b := &trackedRows{}, &trackedRows{}
			batches := []StatementRows{{"one", 1, testTable, a}, {"two", 2, testTable, b}}
			switch name {
			case "stream error":
				a.err = streamErr
			case "preflight error":
				batches[1].StatementID = ""
			case "close error":
				a.closeErr = closeErr
			case "both errors":
				a.err = streamErr
				a.closeErr = closeErr
				b.closeErr = closeErr
			}
			m, r, err := e.applyOwnedRows(context.Background(), prev, blockJob(prev), batches)
			if name == "success" {
				if err != nil {
					t.Fatal(err)
				}
			} else {
				assertNoAppendResult(t, m, r, err)
			}
			if a.closes != 1 || b.closes != 1 {
				t.Fatal("adapter did not close all owned sources exactly once")
			}
			if a.err != nil && !errors.Is(err, streamErr) {
				t.Fatal("lost stream error")
			}
			if a.closeErr != nil && !errors.Is(err, closeErr) {
				t.Fatal("lost close error")
			}
		})
	}
}

type recordingMaterializer struct {
	calls int
	err   error
	rows  []Row
}

func (m *recordingMaterializer) Materialize(context.Context, TableSchema, replay.PreparedStatement) ([]Row, error) {
	m.calls++
	return m.rows, m.err
}

func TestLegacyMaterializedRowsLifecycleAndErrors(t *testing.T) {
	for _, materializeErr := range []error{nil, io.EOF, errors.New("materializer unavailable")} {
		e := testExecutor()
		prev := genesis(t, e)
		m := &recordingMaterializer{err: materializeErr, rows: []Row{appendRow(testTable, "x", 0, "a")}}
		st := preparedInsert("x", 1, "payload", nil)
		s := &materializedRows{materializer: m, schema: e.tables[testTable], statement: st}
		next, r, err := e.applyOwnedRows(context.Background(), prev, blockJob(prev, st), []StatementRows{{"x", 1, testTable, s}})
		if materializeErr != nil {
			assertNoAppendResult(t, next, r, err)
			if !errors.Is(err, materializeErr) {
				t.Fatal("lost materializer error")
			}
		} else if err != nil {
			t.Fatal(err)
		}
		if m.calls != 1 || !s.closed || s.rows != nil {
			t.Fatal("materializer output not released")
		}
		if _, err := s.Next(context.Background()); err == nil {
			t.Fatal("closed source readable")
		}
	}
}

func TestApplyRowsStreamFailuresAndCancellation(t *testing.T) {
	sentinel := errors.New("stream failed")
	for _, tt := range []struct {
		name     string
		rows     []Row
		err      error
		cancelAt int
	}{
		{"partial stream", []Row{appendRow(testTable, "x", 0, "a")}, sentinel, 0},
		{"wrapped EOF", nil, fmt.Errorf("truncated: %w", io.EOF), 0},
		{"short ID", []Row{{RowID: []byte{1}, Values: []any{"a", uint64(1)}, PartitionID: "all"}}, nil, 0},
		{"empty partition", []Row{{RowID: make([]byte, 32), Values: []any{"a", uint64(1)}}}, nil, 0},
		{"bad width", []Row{{RowID: make([]byte, 32), Values: []any{"a"}, PartitionID: "all"}}, nil, 0},
		{"bad value", []Row{{RowID: make([]byte, 32), Values: []any{struct{}{}, uint64(1)}, PartitionID: "all"}}, nil, 0},
		{"byte overflow", []Row{{RowID: make([]byte, 32), Values: []any{"a", uint64(1)}, PartitionID: "all", RawBytes: math.MaxUint64}, appendRow(testTable, "x", 1, "b")}, nil, 0},
		{"cancel before next", nil, nil, -1},
		{"cancel on row", []Row{appendRow(testTable, "x", 0, "a")}, nil, 1},
		{"cancel on EOF", nil, nil, 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			e := testExecutor()
			prev := genesis(t, e)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s := &trackedRows{rows: tt.rows, err: tt.err}
			s.onNext = func() {
				if s.calls == tt.cancelAt {
					cancel()
				}
			}
			if tt.cancelAt < 0 {
				cancel()
			}
			m, r, err := e.ApplyRows(ctx, prev, blockJob(prev), []StatementRows{{"x", 1, testTable, s}})
			assertNoAppendResult(t, m, r, err)
			if tt.err != nil && !errors.Is(err, tt.err) {
				t.Fatalf("lost stream error: %v", err)
			}
			if tt.cancelAt != 0 && !errors.Is(err, context.Canceled) {
				t.Fatalf("lost cancellation: %v", err)
			}
			if s.closes != 0 {
				t.Fatal("borrowed stream closed")
			}
		})
	}
}
