package snapshotquery

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

func TestCanonicalKeyOrdersBytesNotDigest(t *testing.T) {
	schema := payloadexec.TableSchema{TableID: "tenant.copy", Columns: []lthash.Column{{Name: "value", Type: "UInt64"}}}
	rows := [][]any{{uint64(256)}, {uint64(1)}, {uint64(0)}, {uint64(65536)}}
	out, e := Canonicalize(context.Background(), "network", "statement", schema, &canonicalStream{rows: rows}, canonicalLimits(), t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer out.Close()
	verifyCanonical(t, out, schema, rows)
	reader, e := out.OpenRows()
	if e != nil {
		t.Fatal(e)
	}
	defer reader.Close()
	for _, want := range []uint64{0, 65536, 256, 1} {
		row, e := reader.Next(context.Background())
		if e != nil || row.Values[0] != want {
			t.Fatalf("got %v %v want %d", row, e, want)
		}
	}
}
func TestSortSpillChunkEquivalence(t *testing.T) {
	rows := make([][]any, 80)
	for i := range rows {
		rows[i] = []any{uint64((i * 13) % 17), []string{"a", "b"}[i%2]}
	}
	var root string
	for _, budget := range []uint64{200000, 256 << 20} {
		limits := canonicalLimits()
		limits.MaxSortMemoryBytes = budget
		// Reuse a producer-owned row slice, as a chunked database reader would.
		stream := &reusedCanonicalStream{rows: rows, chunk: []any{uint64(0), ""}}
		out, e := Canonicalize(context.Background(), "network", "statement", canonicalSchema(), stream, limits, t.TempDir())
		if e != nil {
			t.Fatal(e)
		}
		verifyCanonical(t, out, canonicalSchema(), rows)
		if root != "" && root != out.OutputRowsRoot() {
			t.Fatal("spill changed output")
		}
		root = out.OutputRowsRoot()
		if budget == 200000 && out.(*canonicalOutput).state.spill <= out.(*canonicalOutput).run.size*3 {
			t.Fatal("test did not force multiple merge writes")
		}
		if e = out.Close(); e != nil {
			t.Fatal(e)
		}
	}
}

type reusedCanonicalStream struct {
	rows  [][]any
	pos   int
	chunk []any
}

func (s *reusedCanonicalStream) Next(ctx context.Context) ([]any, error) {
	if s.pos == len(s.rows) {
		return nil, io.EOF
	}
	copy(s.chunk, s.rows[s.pos])
	s.pos++
	return s.chunk, ctx.Err()
}
func (s *reusedCanonicalStream) Close() error { return nil }

func TestSortOwnedResourceBoundaries(t *testing.T) {
	dir := t.TempDir()
	rows := [][]any{{uint64(1), "a"}}
	base, e := Canonicalize(context.Background(), "network", "statement", canonicalSchema(), &canonicalStream{rows: rows}, canonicalLimits(), dir)
	if e != nil {
		t.Fatal(e)
	}
	stats := base.(*canonicalOutput).state
	memory, spill, output := stats.peakMemory, stats.spill, stats.outputBytes
	base.Close()
	for _, tc := range []struct {
		name  string
		value uint64
		set   func(*Limits, uint64)
	}{
		{"rows", 1, func(l *Limits, n uint64) { l.MaxOutputRows = n }},
		{"output", output, func(l *Limits, n uint64) { l.MaxOutputBytes = n }},
		{"memory", memory, func(l *Limits, n uint64) { l.MaxSortMemoryBytes = n }},
		{"spill", spill, func(l *Limits, n uint64) { l.MaxSpillBytes = n }},
	} {
		for _, under := range []bool{false, true} {
			name := tc.name
			if under {
				name += "_one_over"
			}
			t.Run(name, func(t *testing.T) {
				limits := canonicalLimits()
				v := tc.value
				if under {
					v--
				}
				tc.set(&limits, v)

				stream := &canonicalStream{rows: rows}
				out, e := Canonicalize(context.Background(), "network", "statement", canonicalSchema(), stream, limits, dir)
				if stream.closes != 1 {
					t.Fatal("input close count")
				}
				if under {
					if e == nil || out != nil {
						t.Fatal("one-over accepted")
					}
				} else {
					if e != nil {
						t.Fatal(e)
					}
					verifyCanonical(t, out, canonicalSchema(), rows)
					out.Close()
				}
				entries, _ := os.ReadDir(dir)
				if len(entries) != 0 {
					t.Fatal("scratch leak")
				}
			})
		}
	}
	// The row-overflow case must also cross a nonzero admitted cap.
	limits := canonicalLimits()
	limits.MaxOutputRows = 1
	out, e := Canonicalize(context.Background(), "network", "statement", canonicalSchema(), &canonicalStream{rows: append(rows, rows...)}, limits, t.TempDir())
	if e == nil || out != nil {
		t.Fatal("row cap accepted second row")
	}
}
func TestSortLimitValidationAndOverflow(t *testing.T) {
	for i := 0; i < reflect.TypeOf(Limits{}).NumField(); i++ {
		limits := canonicalLimits()
		reflect.ValueOf(&limits).Elem().Field(i).SetUint(0)
		s := &canonicalStream{}
		out, e := Canonicalize(context.Background(), "network", "statement", canonicalSchema(), s, limits, t.TempDir())
		if e == nil || out != nil || s.closes != 1 || e.Error() != "query profile limits must be nonzero" {
			t.Fatalf("field %d: %v", i, e)
		}
	}
	if _, e := checkedAdd(math.MaxUint64, 1); e == nil {
		t.Fatal("addition overflow")
	}
	if _, e := checkedMul(math.MaxUint64, 2); e == nil {
		t.Fatal("multiplication overflow")
	}
	if e := fitInt(math.MaxUint64); e == nil {
		t.Fatal("int overflow")
	}
	limits := canonicalLimits()
	limits.MaxExecutionMS = uint64(math.MaxInt64 / int64(time.Millisecond))
	out, e := Canonicalize(context.Background(), "network", "statement", canonicalSchema(), &canonicalStream{}, limits, t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	out.Close()
	limits.MaxExecutionMS++
	if out, e = Canonicalize(context.Background(), "network", "statement", canonicalSchema(), &canonicalStream{}, limits, t.TempDir()); e == nil || out != nil {
		t.Fatal("deadline overflow")
	}
	state, e := newSortState(context.Background(), "network", "statement", canonicalSchema(), canonicalLimits(), "")
	if e != nil {
		t.Fatal(e)
	}
	state.spill = math.MaxUint64
	if e = state.chargeSpill(1); e == nil {
		t.Fatal("spill counter overflow")
	}
	state.rows = math.MaxUint64
	if e = state.add([]any{uint64(1), "a"}); e == nil {
		t.Fatal("row counter overflow")
	}
	// The three B3-unconsumed limits are validated, never spent or repurposed.
	limits = canonicalLimits()
	limits.MaxSQLBytes = 1
	limits.MaxDescriptorBytes = 1
	limits.MaxRestoreBytes = 1
	schema := payloadexec.TableSchema{TableID: "long-target", Columns: []lthash.Column{{Name: "text", Type: "String"}}}
	out, e = Canonicalize(context.Background(), "network", "statement", schema, &canonicalStream{rows: [][]any{{strings.Repeat("x", 4096)}}}, limits, t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	out.Close()
	limits.MaxSortMemoryBytes = sortBaseBytes + 4096
	if out, e = Canonicalize(context.Background(), "network", "statement", schema, &canonicalStream{rows: [][]any{{strings.Repeat("x", 65536)}}}, limits, t.TempDir()); e == nil || out != nil {
		t.Fatal("oversized single row accepted")
	}
}
func TestSortCorruptRunRefusal(t *testing.T) {
	for _, mode := range []string{"truncate", "checksum", "length", "context"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			rows := make([][]any, 20)
			for i := range rows {
				rows[i] = []any{uint64(i), "a"}
			}
			limits := canonicalLimits()
			limits.MaxSortMemoryBytes = 180000
			changed := false
			s := &canonicalStream{rows: rows}
			s.before = func(pos int) {
				if pos != len(rows) {
					return
				}
				files, e := filepath.Glob(filepath.Join(dir, "snapshot-query-*", "run-*"))
				if e != nil || len(files) == 0 {
					t.Fatal("no spilled runs")
				}
				path := files[0]
				f, e := os.OpenFile(path, os.O_RDWR, 0)
				if e != nil {
					t.Fatal(e)
				}
				defer f.Close()
				switch mode {
				case "truncate":
					e = f.Truncate(41)
				case "checksum":
					_, e = f.WriteAt([]byte{0xff}, 80)
				case "context":
					_, e = f.WriteAt([]byte{0xff}, 8)
				case "length":
					var b [8]byte
					binary.LittleEndian.PutUint64(b[:], math.MaxUint64)
					_, e = f.WriteAt(b[:], 40)
				}
				if e != nil {
					t.Fatal(e)
				}
				changed = true
			}
			out, e := Canonicalize(context.Background(), "network", "statement", canonicalSchema(), s, limits, dir)
			if !changed || e == nil || out != nil {
				t.Fatalf("corrupt run accepted: %v", e)
			}
			files, _ := os.ReadDir(dir)
			if len(files) != 0 {
				t.Fatal("scratch leaked")
			}
		})
	}
}
func TestCanonicalOwnershipAndOutputCorruption(t *testing.T) {
	dir := t.TempDir()
	sentinel := filepath.Join(dir, "caller-owned")
	if e := os.WriteFile(sentinel, []byte("preserve"), 0600); e != nil {
		t.Fatal(e)
	}
	out, e := Canonicalize(context.Background(), "network", "statement", canonicalSchema(), &canonicalStream{rows: [][]any{{uint64(1), "a"}}}, canonicalLimits(), dir)
	if e != nil {
		t.Fatal(e)
	}
	first, e := out.OpenRows()
	if e != nil {
		t.Fatal(e)
	}
	second, e := out.OpenRows()
	if e != nil {
		t.Fatal(e)
	}
	if _, e = out.OpenRows(); e == nil {
		t.Fatal("reader cap")
	}
	a, e := first.Next(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	b, e := second.Next(context.Background())
	if e != nil || !bytes.Equal(a.RowID, b.RowID) {
		t.Fatal("readers not independent")
	}
	first.Close()
	second.Close()
	parts := out.TouchedPartitionIDs()
	parts[0] = "mutated"
	if out.TouchedPartitionIDs()[0] != "p_a" {
		t.Fatal("partition metadata aliased")
	}
	c := out.(*canonicalOutput)
	f, e := os.OpenFile(filepath.Join(c.state.dir, c.run.path), os.O_RDWR, 0)
	if e != nil {
		t.Fatal(e)
	}
	_, e = f.WriteAt([]byte{0xff}, 80)
	if e != nil {
		t.Fatal(e)
	}
	f.Close()
	if _, e = out.OpenRows(); e == nil {
		t.Fatal("corrupt output opened")
	}
	if e = out.Close(); e != nil {
		t.Fatal(e)
	}
	if e = out.Close(); e != nil {
		t.Fatal(e)
	}
	if _, e = out.OpenRows(); e == nil {
		t.Fatal("opened after close")
	}
	if _, e = first.Next(context.Background()); e == nil {
		t.Fatal("reader survived close")
	}
	if _, e = os.Stat(sentinel); e != nil {
		t.Fatal("caller file removed")
	}
}
func TestCanonicalCloseFailureAndCancellation(t *testing.T) {
	closeErr := errors.New("input close failed")
	s := &canonicalStream{closeErr: closeErr}
	dir := t.TempDir()
	out, e := Canonicalize(context.Background(), "network", "statement", canonicalSchema(), s, canonicalLimits(), dir)
	if out != nil || !errors.Is(e, closeErr) || s.closes != 1 {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	rows := make([][]any, 30)
	for i := range rows {
		rows[i] = []any{uint64(i), "a"}
	}
	s = &canonicalStream{rows: rows}
	s.before = func(pos int) {
		if pos == 25 {
			cancel()
		}
	}
	limits := canonicalLimits()
	limits.MaxSortMemoryBytes = 180000
	out, e = Canonicalize(ctx, "network", "statement", canonicalSchema(), s, limits, dir)
	if out != nil || !errors.Is(e, context.Canceled) || s.closes != 1 {
		t.Fatal(e)
	}
	files, _ := os.ReadDir(dir)
	if len(files) != 0 {
		t.Fatal("cancel scratch leak")
	}
	blocking := &deadlineCanonicalStream{}
	limits.MaxExecutionMS = 1
	out, e = Canonicalize(context.Background(), "network", "statement", canonicalSchema(), blocking, limits, dir)
	if out != nil || !errors.Is(e, context.DeadlineExceeded) || blocking.closes != 1 {
		t.Fatal(e)
	}
	// A non-cooperating Next that returns after expiry is still refused.
	s = &canonicalStream{rows: [][]any{{uint64(1), "a"}}, before: func(int) { time.Sleep(3 * time.Millisecond) }}
	out, e = Canonicalize(context.Background(), "network", "statement", canonicalSchema(), s, limits, dir)
	if out != nil || !errors.Is(e, context.DeadlineExceeded) {
		t.Fatal(e)
	}
}

type deadlineCanonicalStream struct{ closes int }

func (s *deadlineCanonicalStream) Next(ctx context.Context) ([]any, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}
func (s *deadlineCanonicalStream) Close() error { s.closes++; return nil }

func TestNormalizeAllProfilesAndRoundTrip(t *testing.T) {
	values := map[string]any{"String": "hello\x00world", "FixedString(32)": make([]byte, 32), "Bool": true, "Float32": float32(-1.5), "Float64": math.NaN(), "UInt8": uint8(255), "UInt16": uint16(65535), "UInt32": uint32(math.MaxUint32), "UInt64": uint64(math.MaxUint64), "Int8": int8(-128), "Int16": int16(-32768), "Int32": int32(math.MinInt32), "Int64": int64(math.MinInt64)}
	instant := time.Date(2026, 2, 3, 4, 5, 6, 123456789, time.FixedZone("zone", -3*3600))
	for _, typ := range payloadexec.AdmittedColumnTypeVectors() {
		t.Run(typ, func(t *testing.T) {
			v, ok := values[typ]
			if !ok {
				v = instant
			}
			schema := payloadexec.TableSchema{TableID: "t", Columns: []lthash.Column{{Name: "value", Type: typ}}}
			normalized, e := NormalizeRow(schema, []any{v})
			if e != nil {
				t.Fatal(e)
			}
			_, size, _, e := rowLayout(schema, normalized)
			if e != nil {
				t.Fatal(e)
			}
			encoded := encodeTyped(normalized, size)
			decoded, e := decodeTyped(schema, encoded)
			if e != nil {
				t.Fatal(e)
			}
			a, _ := lthash.EncodeRow("t", schema.Columns, normalized)
			b, _ := lthash.EncodeRow("t", schema.Columns, decoded)
			if !bytes.Equal(a, b) {
				t.Fatal("typed roundtrip")
			}
			out, e := Canonicalize(context.Background(), "network", "statement", schema, &canonicalStream{rows: [][]any{{v}}}, canonicalLimits(), t.TempDir())
			if e != nil {
				t.Fatal(e)
			}
			defer out.Close()
			verifyCanonical(t, out, schema, [][]any{{v}})
		})
	}
}
func TestNormalizeTemporalAndFloatEquivalence(t *testing.T) {
	for _, typ := range []string{"Date", "DateTime", "DateTime64(3)", "DateTime64(9, 'UTC')"} {
		schema := payloadexec.TableSchema{TableID: "t", Columns: []lthash.Column{{Name: "t", Type: typ}}}
		utc := time.Unix(1700000000, 123456789).UTC()
		a, e := NormalizeRow(schema, []any{utc})
		if e != nil {
			t.Fatal(e)
		}
		b, e := NormalizeRow(schema, []any{utc.In(time.FixedZone("other", 5*3600))})
		if e != nil || !reflect.DeepEqual(a, b) {
			t.Fatal("time representation differs")
		}
	}
	for _, typ := range []string{"Float32", "Float64"} {
		schema := payloadexec.TableSchema{TableID: "t", Columns: []lthash.Column{{Name: "v", Type: typ}}}
		var pairs [][2]any
		if typ == "Float32" {
			pairs = [][2]any{{math.Float32frombits(0x80000000), float32(0)}, {math.Float32frombits(0xffc12345), math.Float32frombits(0x7fc00000)}}
		} else {
			pairs = [][2]any{{math.Copysign(0, -1), float64(0)}, {math.Float64frombits(0xfff8000000000001), math.NaN()}}
		}
		for _, pair := range pairs {
			a, _ := NormalizeRow(schema, []any{pair[0]})
			b, _ := NormalizeRow(schema, []any{pair[1]})
			ka, _ := lthash.EncodeRow("t", schema.Columns, a)
			kb, _ := lthash.EncodeRow("t", schema.Columns, b)
			if !bytes.Equal(ka, kb) {
				t.Fatal("noncanonical float")
			}
		}
	}
}

func TestSortContentChecksBeyondChecksums(t *testing.T) {
	for _, mode := range []string{"key", "typed", "noncanonical_zero", "out_of_order"} {
		t.Run(mode, func(t *testing.T) {
			schema := payloadexec.TableSchema{TableID: "t", Columns: []lthash.Column{{Name: "value", Type: "Float64"}}}
			state, e := newSortState(context.Background(), "network", "statement", schema, canonicalLimits(), "")
			if e != nil {
				t.Fatal(e)
			}
			state.dir = t.TempDir()
			values := []any{float64(0)}
			key, e := lthash.EncodeRow("t", schema.Columns, values)
			if e != nil {
				t.Fatal(e)
			}
			_, size, _, _ := rowLayout(schema, values)
			record := sortRecord{key: key, typed: encodeTyped(values, size)}
			switch mode {
			case "key":
				record.key = bytes.Clone(key)
				record.key[len(key)-1] ^= 1
			case "typed":
				record.typed = []byte{1}
			case "noncanonical_zero":
				record.typed = encodeTyped([]any{math.Copysign(0, -1)}, size)
			}
			writer, e := state.newWriter()
			if e != nil {
				t.Fatal(e)
			}
			if mode == "out_of_order" {
				high := []any{float64(1)}
				hk, _ := lthash.EncodeRow("t", schema.Columns, high)
				if e = writer.write(sortRecord{key: hk, typed: encodeTyped(high, size)}); e != nil {
					t.Fatal(e)
				}
			}
			if e = writer.write(record); e != nil {
				t.Fatal(e)
			}
			run, e := writer.finish()
			if e != nil {
				t.Fatal(e)
			}
			reader, e := state.openRun(run)
			if e != nil {
				t.Fatal(e)
			}
			defer reader.Close()
			if mode == "out_of_order" {
				if _, e = reader.next(); e != nil {
					t.Fatal(e)
				}
			}
			if _, e = reader.next(); e == nil {
				t.Fatal("accepted authenticated but inconsistent cache record")
			}
		})
	}
}
func TestCanonicalPostOpenMutationAndReaderRetirement(t *testing.T) {
	for _, mode := range []string{"value", "id", "partition"} {
		t.Run(mode, func(t *testing.T) {
			out, e := Canonicalize(context.Background(), "network", "statement", canonicalSchema(), &canonicalStream{rows: [][]any{{uint64(1), "a"}}}, canonicalLimits(), t.TempDir())
			if e != nil {
				t.Fatal(e)
			}
			defer out.Close()
			o := out.(*canonicalOutput)
			reader, e := out.OpenRows()
			if e != nil {
				t.Fatal(e)
			}
			state := *o.state
			state.ctx = context.Background()
			values := []any{uint64(1), "a"}
			if mode == "value" {
				values[0] = uint64(2)
			}
			key, _ := lthash.EncodeRow(state.schema.TableID, state.schema.Columns, values)
			_, size, _, _ := rowLayout(state.schema, values)
			record := sortRecord{key: key, typed: encodeTyped(values, size), id: payloadexec.RowID("network", state.schema.TableID, "statement", 0), partition: "p_a"}
			if mode == "id" {
				record.id[0] ^= 1
			}
			if mode == "partition" {
				record.partition = "p_b"
			}
			writer, e := state.newWriter()
			if e != nil {
				t.Fatal(e)
			}
			if e = writer.write(record); e != nil {
				t.Fatal(e)
			}
			run, e := writer.finish()
			if e != nil {
				t.Fatal(e)
			}
			data, e := os.ReadFile(filepath.Join(state.dir, run.path))
			if e != nil {
				t.Fatal(e)
			}
			if e = os.WriteFile(filepath.Join(state.dir, o.run.path), data, 0600); e != nil {
				t.Fatal(e)
			}
			if _, e = reader.Next(context.Background()); e == nil {
				t.Fatal("post-open mutation returned a row")
			}
			if e = out.Close(); e != nil {
				t.Fatal(e)
			}
			if !reader.(*outputRows).reader.closed {
				t.Fatal("owned reader handle leaked")
			}
			if e = reader.Close(); e != nil {
				t.Fatal(e)
			}
		})
	}
}
func TestCanonicalErrorJoinsAndImmutableFixedString(t *testing.T) {
	closeErr := errors.New("close sentinel")
	stream := &canonicalStream{failure: io.ErrUnexpectedEOF, closeErr: closeErr}
	out, e := Canonicalize(context.Background(), "network", "statement", canonicalSchema(), stream, canonicalLimits(), t.TempDir())
	if out != nil || !errors.Is(e, io.ErrUnexpectedEOF) || !errors.Is(e, closeErr) {
		t.Fatal(e)
	}
	schema := payloadexec.TableSchema{TableID: "t", Columns: []lthash.Column{{Name: "s", Type: "FixedString(32)"}}}
	b := make([]byte, 32)
	stream = &canonicalStream{rows: [][]any{{b}, {b}}}
	stream.before = func(pos int) {
		if pos == 1 {
			b[0] = 1
		}
		if pos == 2 {
			b[0] = 2
		}
	}
	out, e = Canonicalize(context.Background(), "network", "statement", schema, stream, canonicalLimits(), t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer out.Close()
	reader, e := out.OpenRows()
	if e != nil {
		t.Fatal(e)
	}
	defer reader.Close()
	for _, want := range []byte{0, 1} {
		r, e := reader.Next(context.Background())
		if e != nil || r.Values[0].([]byte)[0] != want {
			t.Fatalf("input mutation affected output: %v %v", r, e)
		}
	}
}

func TestNormalizeDateTime64Boundary(t *testing.T) {
	schema := payloadexec.TableSchema{TableID: "t", Columns: []lthash.Column{{Name: "t", Type: "DateTime64(3)"}}}
	if _, e := NormalizeRow(schema, []any{time.Unix(0, math.MinInt64)}); e == nil {
		t.Fatal("precision truncation wrapped UnixNano")
	}
	schema.Columns[0].Type = "DateTime64(9)"
	for _, n := range []int64{math.MinInt64, math.MaxInt64} {
		v, e := NormalizeRow(schema, []any{time.Unix(0, n)})
		if e != nil || v[0].(time.Time).UnixNano() != n {
			t.Fatalf("nanosecond boundary %d: %v", n, e)
		}
	}
	if _, e := NormalizeRow(schema, []any{time.Date(2500, 1, 1, 0, 0, 0, 0, time.UTC)}); e == nil {
		t.Fatal("out-of-range UnixNano accepted")
	}
}
func TestCanonicalConcurrentReadersClose(t *testing.T) {
	rows := make([][]any, 100)
	for i := range rows {
		rows[i] = []any{uint64(i), "a"}
	}
	out, e := Canonicalize(context.Background(), "network", "statement", canonicalSchema(), &canonicalStream{rows: rows}, canonicalLimits(), t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	a, e := out.OpenRows()
	if e != nil {
		t.Fatal(e)
	}
	b, e := out.OpenRows()
	if e != nil {
		t.Fatal(e)
	}
	done := make(chan error, 2)
	for _, reader := range []payloadexec.RowSource{a, b} {
		go func(reader payloadexec.RowSource) {
			for {
				_, err := reader.Next(context.Background())
				if err != nil {
					if err != io.EOF && !strings.Contains(err.Error(), "closed") {
						done <- err
						return
					}
					done <- reader.Close()
					return
				}
			}
		}(reader)
	}
	if e = out.Close(); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 2; i++ {
		if e = <-done; e != nil {
			t.Fatal(e)
		}
	}
}
