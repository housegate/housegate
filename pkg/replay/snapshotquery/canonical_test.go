package snapshotquery

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"math/rand"
	"os"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

type canonicalStream struct {
	rows              [][]any
	pos, closes       int
	failure, closeErr error
	before            func(int)
}

func (s *canonicalStream) Next(ctx context.Context) ([]any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.before != nil {
		s.before(s.pos)
	}
	if s.pos == len(s.rows) {
		if s.failure != nil {
			return nil, s.failure
		}
		return nil, io.EOF
	}
	r := s.rows[s.pos]
	s.pos++
	return r, nil
}
func (s *canonicalStream) Close() error { s.closes++; return s.closeErr }
func canonicalLimits() Limits {
	return Limits{MaxSQLBytes: 1, MaxDescriptorBytes: 1, MaxRestoreBytes: 1, MaxOutputRows: 100000, MaxOutputBytes: 64 << 20, MaxSortMemoryBytes: 256 << 20, MaxSpillBytes: 1 << 30, MaxExecutionMS: 60000}
}
func canonicalSchema() payloadexec.TableSchema {
	return payloadexec.TableSchema{TableID: "tenant.copy", PartitionBy: "p", Columns: []lthash.Column{{Name: "value", Type: "UInt64"}, {Name: "p", Type: "String"}}}
}

func verifyCanonical(t *testing.T, out CanonicalOutput, schema payloadexec.TableSchema, input [][]any) {
	t.Helper()
	keys := make([][]byte, len(input))
	hashes := make([]string, len(input))
	for i, r := range input {
		n, e := NormalizeRow(schema, r)
		if e != nil {
			t.Fatal(e)
		}
		keys[i], e = lthash.EncodeRow(schema.TableID, schema.Columns, n)
		if e != nil {
			t.Fatal(e)
		}
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 })
	rows, e := out.OpenRows()
	if e != nil {
		t.Fatal(e)
	}
	defer rows.Close()
	for i, key := range keys {
		r, e := rows.Next(context.Background())
		if e != nil {
			t.Fatalf("row %d: %v", i, e)
		}
		actual, e := lthash.EncodeRow(schema.TableID, schema.Columns, r.Values)
		if e != nil || !bytes.Equal(actual, key) {
			t.Fatalf("key %d: %x != %x (%v)", i, actual, key, e)
		}
		if !bytes.Equal(r.RowID, payloadexec.RowID("network", schema.TableID, "statement", uint64(i))) {
			t.Fatalf("id %d", i)
		}
		p, e := payloadexec.PartitionIDForRow(schema, r.Values)
		if e != nil || p != r.PartitionID {
			t.Fatalf("partition %d", i)
		}
		if r.RawBytes == 0 {
			t.Fatal("missing byte count")
		}
		hashes[i] = replay.DigestBytes(key)
	}
	if _, e := rows.Next(context.Background()); e != io.EOF {
		t.Fatalf("end: %v", e)
	}
	if out.RowCount() != uint64(len(input)) {
		t.Fatal("count")
	}
	root, e := (replay.SnapshotQueryOutputCommitment{TargetTableID: schema.TableID, SchemaHash: payloadexec.TableSchemaHash("network", schema), RowCount: uint64(len(input)), RowHashes: hashes}).Hash()
	if e != nil || root != out.OutputRowsRoot() {
		t.Fatalf("root %s != %s (%v)", out.OutputRowsRoot(), root, e)
	}
}
func TestCanonicalOrderingMultiplicity(t *testing.T) {
	schema := canonicalSchema()
	original := [][]any{{uint64(256), "b"}, {uint64(1), "a"}, {uint64(0), "b"}, {uint64(65536), "a"}, {uint64(256), "b"}}
	var root string
	for _, mode := range []string{"forward", "reverse", "shuffle"} {
		t.Run(mode, func(t *testing.T) {
			rows := append([][]any(nil), original...)
			if mode == "reverse" {
				for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
					rows[i], rows[j] = rows[j], rows[i]
				}
			}
			if mode == "shuffle" {
				rand.New(rand.NewSource(7)).Shuffle(len(rows), func(i, j int) { rows[i], rows[j] = rows[j], rows[i] })
			}
			input := &canonicalStream{rows: rows}
			out, e := Canonicalize(context.Background(), "network", "statement", schema, input, canonicalLimits(), t.TempDir())
			if e != nil {
				t.Fatal(e)
			}
			defer out.Close()
			verifyCanonical(t, out, schema, original)
			if input.closes != 1 {
				t.Fatal("input close count")
			}
			if !reflect.DeepEqual(out.TouchedPartitionIDs(), []string{"p_a", "p_b"}) {
				t.Fatal(out.TouchedPartitionIDs())
			}
			if root != "" && root != out.OutputRowsRoot() {
				t.Fatal("unstable root")
			}
			root = out.OutputRowsRoot()
		})
	}
}
func TestCanonical65536Duplicates(t *testing.T) {
	rows := make([][]any, 65536)
	for i := range rows {
		rows[i] = []any{uint64(7), "same"}
	}
	rows = append(rows, []any{uint64(0), "before"}, []any{uint64(65536), "after"})
	out, e := Canonicalize(context.Background(), "network", "statement", canonicalSchema(), &canonicalStream{rows: rows}, canonicalLimits(), t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer out.Close()
	verifyCanonical(t, out, canonicalSchema(), rows)
}
func TestCanonicalEmptyAndErrors(t *testing.T) {
	for _, failure := range []error{nil, io.ErrUnexpectedEOF, context.Canceled} {
		t.Run(fmtError(failure), func(t *testing.T) {
			dir := t.TempDir()
			s := &canonicalStream{failure: failure}
			out, e := Canonicalize(context.Background(), "network", "statement", canonicalSchema(), s, canonicalLimits(), dir)
			if s.closes != 1 {
				t.Fatal("close count")
			}
			if failure == nil {
				if e != nil {
					t.Fatal(e)
				}
				verifyCanonical(t, out, canonicalSchema(), nil)
				if e = out.Close(); e != nil {
					t.Fatal(e)
				}
			} else if out != nil || !errors.Is(e, failure) {
				t.Fatalf("%v %v", out, e)
			}
			files, _ := os.ReadDir(dir)
			if len(files) != 0 {
				t.Fatal("scratch leak")
			}
		})
	}
}
func fmtError(e error) string {
	if e == nil {
		return "empty"
	}
	return e.Error()
}
func TestNormalizeCanonicalValues(t *testing.T) {
	schema := payloadexec.TableSchema{TableID: "t", Columns: []lthash.Column{{Name: "f", Type: "Float64"}, {Name: "z", Type: "Float32"}, {Name: "t", Type: "DateTime64(3)"}, {Name: "b", Type: "FixedString(32)"}}}
	b := make([]byte, 32)
	instant := time.Date(2026, 1, 1, 2, 3, 4, 123456789, time.FixedZone("x", 3600))
	n, e := NormalizeRow(schema, []any{math.Float64frombits(0x7ff0000000000001), float32(math.Copysign(0, -1)), instant, b})
	if e != nil {
		t.Fatal(e)
	}
	if math.Float64bits(n[0].(float64)) != 0x7ff8000000000000 || math.Float32bits(n[1].(float32)) != 0 || n[2].(time.Time).Location() != time.UTC || n[2].(time.Time).Nanosecond() != 123000000 {
		t.Fatal(n)
	}
	b[0] = 1
	if n[3].([]byte)[0] != 0 {
		t.Fatal("aliased input")
	}
	if _, e = NormalizeRow(schema, []any{float32(1), float32(0), instant, b}); e == nil {
		t.Fatal("coerced type")
	}
}
