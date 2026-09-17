package snapshotquery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// Admission accounting, not a measured process RSS promise:
//   - 32 KiB fixed: 64 binary-merge slots, <=3 files/hashers and control state;
//   - schema/identity bytes x64 + 1024 per column: owned schema, validation,
//     schema-hash JSON and encoding indices (including escaping/capacity);
//   - each row reserves 1024 + 4*partitionBound bytes until Close: digest string,
//     slice capacity and all commitment JSON/hash copies, partition index/copies;
//   - row workspace = 4096 + 32*(key+typed+partitionBound) + 256*columns:
//     normalized values/boxing, encoder growth/capacity, framing/hash scratch,
//     run slice capacity and sort indices. Three maximum workspaces are reserved
//     independently of in-memory run records for bounded two-way merge/output.
//
// All arithmetic is checked before allocation. Run records also each charge a
// workspace. Charges intentionally exceed the sum of concrete live capacities.
// Directory/path scratch additionally reserves 16*(len(tempDir)+64) bytes;
// descriptors own only short basenames rather than retaining full paths.
// Runtime/allocator pools, caller-owned input/returned rows and kernel buffers are not
// attributed to this component. SQL/descriptor/restore bounds are not consumed.
const sortBaseBytes uint64 = 32768
const recordFrameBytes uint64 = 64 // four uint64 lengths + SHA-256
const fileHeaderBytes uint64 = 40  // format magic + context binding

var cacheMagic = [8]byte{'H', 'G', 'Q', 'S', 'O', 'R', 'T', 1}

type sortRecord struct {
	key, typed, id []byte
	partition      string
}

func (r sortRecord) size() uint64 {
	return recordFrameBytes + uint64(len(r.key)) + uint64(len(r.typed)) + uint64(len(r.id)) + uint64(len(r.partition))
}

type sortRun struct {
	path                   string
	count, size, maxRecord uint64
	digest                 [32]byte
}
type sortState struct {
	ctx                context.Context
	network, statement string
	schema             payloadexec.TableSchema
	limits             Limits
	dir                string
	binding            [32]byte
	// Each counter follows the admission convention above.
	base            uint64
	retained        uint64
	maxWorkspace    uint64
	runMemory       uint64
	peakMemory      uint64
	spill           uint64
	outputBytes     uint64
	projectedOutput uint64
	rows            uint64
	records         []sortRecord
	levels          [64]*sortRun
}

func newSortState(ctx context.Context, network, statement string, schema payloadexec.TableSchema, limits Limits, tempDir string) (*sortState, error) {
	base := sortBaseBytes
	directoryBytes, err := checkedMul(uint64(len(tempDir))+64, 16)
	if err != nil {
		return nil, err
	}
	base, err = checkedAdd(base, directoryBytes)
	if err != nil {
		return nil, err
	}
	for _, s := range []string{network, statement, schema.TableID, schema.PartitionBy} {
		n, e := checkedMul(uint64(len(s)), 64)
		if e != nil {
			return nil, e
		}
		base, e = checkedAdd(base, n)
		if e != nil {
			return nil, e
		}
	}
	for _, c := range schema.Columns {
		n, e := checkedMul(uint64(len(c.Name))+uint64(len(c.Type)), 64)
		if e != nil {
			return nil, e
		}
		n, e = checkedAdd(n, 1024)
		if e != nil {
			return nil, e
		}
		base, e = checkedAdd(base, n)
		if e != nil {
			return nil, e
		}
	}
	if err = fitInt(base); err != nil {
		return nil, err
	}
	if base > limits.MaxSortMemoryBytes {
		return nil, fmt.Errorf("sort memory limit exceeded: base %d", base)
	}
	seen := make(map[string]bool, len(schema.Columns))
	found := schema.PartitionBy == ""
	for _, c := range schema.Columns {
		if c.Name == "" || c.Name == "_hg_row_id" || seen[c.Name] {
			return nil, fmt.Errorf("invalid or duplicate user column %q", c.Name)
		}
		seen[c.Name] = true
		if c.Name == schema.PartitionBy {
			found = true
		}
		if _, err := payloadexec.ResolveColumnProfile(c.Type); err != nil {
			return nil, err
		}
	}
	if !found {
		return nil, fmt.Errorf("partition column is absent from schema")
	}
	schema.Columns = append([]lthash.Column(nil), schema.Columns...)
	// Bind all cache files to this call's immutable schema/ID context. Run digests
	// are kept in memory, not trusted from self-describing cache contents.
	h := sha256.New()
	writeBinding := func(value string) {
		var n [8]byte
		binary.LittleEndian.PutUint64(n[:], uint64(len(value)))
		h.Write(n[:])
		h.Write([]byte(value))
	}
	for _, value := range []string{network, statement, schema.TableID, schema.PartitionBy, payloadexec.TableSchemaHash(network, schema)} {
		writeBinding(value)
	}
	// Preserve exact declaration spelling and order in internal cache context,
	// even when the legacy semantic schema hash canonicalizes those declarations.
	for _, column := range schema.Columns {
		writeBinding(column.Name)
		writeBinding(column.Type)
	}

	state := &sortState{ctx: ctx, network: network, statement: statement, schema: schema, limits: limits, base: base, peakMemory: base}
	copy(state.binding[:], h.Sum(nil))
	return state, nil
}
func (s *sortState) memory(retained, workspace, run uint64) error {
	n, e := checkedMul(workspace, 3)
	if e != nil {
		return e
	}
	for _, v := range []uint64{s.base, retained, run} {
		n, e = checkedAdd(n, v)
		if e != nil {
			return e
		}
	}
	if e = fitInt(n); e != nil {
		return e
	}
	if n > s.limits.MaxSortMemoryBytes {
		return fmt.Errorf("sort memory limit exceeded: need %d, limit %d", n, s.limits.MaxSortMemoryBytes)
	}
	if n > s.peakMemory {
		s.peakMemory = n
	}
	return nil
}
func rowWorkspace(key, typed, partition uint64, columns int) (uint64, error) {
	n, e := checkedAdd(key, typed)
	if e != nil {
		return 0, e
	}
	n, e = checkedAdd(n, partition)
	if e != nil {
		return 0, e
	}
	n, e = checkedMul(n, 32)
	if e != nil {
		return 0, e
	}
	c, e := checkedMul(uint64(columns), 256)
	if e != nil {
		return 0, e
	}
	n, e = checkedAdd(n, c)
	if e != nil {
		return 0, e
	}
	return checkedAdd(n, 4096)
}
func (s *sortState) add(values []any) error {
	rows, e := checkedAdd(s.rows, 1)
	if e != nil {
		return e
	}
	if rows > s.limits.MaxOutputRows {
		return fmt.Errorf("output row limit exceeded")
	}
	if e = fitInt(rows); e != nil {
		return e
	}
	keySize, typedSize, partitionBound, e := rowLayout(s.schema, values)
	if e != nil {
		return e
	}
	workspace, e := rowWorkspace(keySize, typedSize, partitionBound, len(values))
	if e != nil {
		return e
	}
	maxWork := s.maxWorkspace
	if workspace > maxWork {
		maxWork = workspace
	}
	reservation, e := checkedMul(partitionBound, 4)
	if e != nil {
		return e
	}
	reservation, e = checkedAdd(reservation, 1024)
	if e != nil {
		return e
	}
	retained, e := checkedAdd(s.retained, reservation)
	if e != nil {
		return e
	}
	runMemory, e := checkedAdd(s.runMemory, workspace)
	if e != nil {
		return e
	}
	// Reserve this row's future digests before normalization/encoding. Flush the
	// existing run if needed; a single row that still cannot fit is refused.
	if e = s.memory(retained, maxWork, runMemory); e != nil {
		if len(s.records) > 0 {
			if e = s.flush(); e != nil {
				return e
			}
		}
		runMemory = workspace
		if e = s.memory(retained, maxWork, runMemory); e != nil {
			return e
		}
	}
	minimum, e := checkedAdd(keySize, typedSize)
	if e != nil {
		return e
	}
	minimum, e = checkedAdd(minimum, recordFrameBytes+32+2)
	if e != nil {
		return e
	}
	minimum, e = checkedAdd(s.projectedOutput, minimum)
	if e != nil {
		return e
	}
	if minimum > s.limits.MaxOutputBytes {
		return fmt.Errorf("output byte limit exceeded before row allocation")
	}
	normalized, e := NormalizeRow(s.schema, values)
	if e != nil {
		return e
	}
	partition, e := payloadexec.PartitionIDForRow(s.schema, normalized)
	if e != nil {
		return e
	}
	projected, e := checkedAdd(minimum, uint64(len(partition))-2)
	if e != nil {
		return e
	}
	if projected > s.limits.MaxOutputBytes {
		return fmt.Errorf("output byte limit exceeded before key encoding")
	}
	key, e := lthash.EncodeRow(s.schema.TableID, s.schema.Columns, normalized)
	if e != nil {
		return e
	}
	if uint64(len(key)) != keySize {
		return fmt.Errorf("canonical key size mismatch")
	}
	typed := encodeTyped(normalized, typedSize)
	s.projectedOutput = projected
	s.rows = rows
	s.retained = retained
	s.maxWorkspace = maxWork
	s.runMemory = runMemory
	s.records = append(s.records, sortRecord{key: key, typed: typed})
	// Keep run growth independent of the reserved digest/merge workspace. This
	// is also the documented deterministic run-size policy used by limit tests.
	if s.runMemory >= s.limits.MaxSortMemoryBytes/4 {
		return s.flush()
	}
	return s.ctx.Err()
}
func (s *sortState) chargeSpill(n uint64) error {
	v, e := checkedAdd(s.spill, n)
	if e != nil {
		return e
	}
	if v > s.limits.MaxSpillBytes {
		return fmt.Errorf("spill byte limit exceeded")
	}
	s.spill = v
	return nil
}
func (s *sortState) flush() error {
	if len(s.records) == 0 {
		return s.ctx.Err()
	}
	if e := s.ctx.Err(); e != nil {
		return e
	}
	sort.Slice(s.records, func(i, j int) bool { return bytes.Compare(s.records[i].key, s.records[j].key) < 0 })
	if e := s.ctx.Err(); e != nil {
		return e
	}
	w, e := s.newWriter()
	if e != nil {
		return e
	}
	for _, r := range s.records {
		if e = w.write(r); e != nil {
			return errors.Join(e, w.abort())
		}
	}
	run, e := w.finish()
	if e != nil {
		return e
	}
	s.records = nil
	s.runMemory = 0
	// Binary carry: at most 64 descriptors and two input files regardless of N.
	for i := range s.levels {
		if s.levels[i] == nil {
			s.levels[i] = run
			return nil
		}
		run, e = s.merge(s.levels[i], run)
		if e != nil {
			return e
		}
		s.levels[i] = nil
	}
	return fmt.Errorf("sort run count overflow")
}
func (s *sortState) finishRuns() (*sortRun, error) {
	var out *sortRun
	for i, r := range s.levels {
		if r == nil {
			continue
		}
		if out == nil {
			out = r
		} else {
			var e error
			out, e = s.merge(out, r)
			if e != nil {
				return nil, e
			}
		}
		s.levels[i] = nil
	}
	return out, s.ctx.Err()
}
func (s *sortState) merge(a, b *sortRun) (out *sortRun, err error) {
	if err = s.memory(s.retained, s.maxWorkspace, 0); err != nil {
		return nil, err
	}
	ar, err := s.openRun(a)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, ar.Close()) }()
	br, err := s.openRun(b)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, br.Close()) }()
	w, err := s.newWriter()
	if err != nil {
		return nil, err
	}
	defer func() {
		if out == nil {
			err = errors.Join(err, w.abort())
		}
	}()
	x, xe := ar.next()
	y, ye := br.next()
	for xe != io.EOF || ye != io.EOF {
		if xe != nil && xe != io.EOF {
			return nil, xe
		}
		if ye != nil && ye != io.EOF {
			return nil, ye
		}
		if ye == io.EOF || (xe == nil && bytes.Compare(x.key, y.key) <= 0) {
			if err = w.write(x); err != nil {
				return nil, err
			}
			x, xe = ar.next()
		} else {
			if err = w.write(y); err != nil {
				return nil, err
			}
			y, ye = br.next()
		}
	}
	if err = errors.Join(ar.Close(), br.Close()); err != nil {
		return nil, err
	}
	out, err = w.finish()
	if err != nil {
		return nil, err
	}
	if err = errors.Join(os.Remove(filepath.Join(s.dir, a.path)), os.Remove(filepath.Join(s.dir, b.path))); err != nil {
		return nil, err
	}
	return out, nil
}

type runWriter struct {
	state  *sortState
	file   *os.File
	hash   hash.Hash
	run    sortRun
	closed bool
}

func (s *sortState) newWriter() (*runWriter, error) {
	if e := s.ctx.Err(); e != nil {
		return nil, e
	}
	if e := s.chargeSpill(fileHeaderBytes); e != nil {
		return nil, e
	}
	f, e := os.CreateTemp(s.dir, "run-")
	if e != nil {
		return nil, e
	}
	w := &runWriter{state: s, file: f, hash: sha256.New(), run: sortRun{path: strings.Clone(filepath.Base(f.Name())), size: fileHeaderBytes}}
	if e = w.writeBytes(cacheMagic[:], s.binding[:]); e != nil {
		return nil, errors.Join(e, w.abort())
	}
	return w, nil
}
func (w *runWriter) writeBytes(chunks ...[]byte) error {
	for _, b := range chunks {
		if e := w.state.ctx.Err(); e != nil {
			return e
		}
		n, e := w.file.Write(b)
		if e != nil {
			return e
		}
		if n != len(b) {
			return io.ErrShortWrite
		}
		w.hash.Write(b)
		if e = w.state.ctx.Err(); e != nil {
			return e
		}
	}
	return nil
}
func (w *runWriter) write(r sortRecord) error {
	size := r.size()
	if e := w.state.chargeSpill(size); e != nil {
		return e
	}
	var header [32]byte
	for i, n := range []int{len(r.key), len(r.typed), len(r.id), len(r.partition)} {
		binary.LittleEndian.PutUint64(header[i*8:], uint64(n))
	}
	h := sha256.New()
	for _, b := range [][]byte{header[:], r.key, r.typed, r.id, []byte(r.partition)} {
		h.Write(b)
	}
	if e := w.writeBytes(header[:], r.key, r.typed, r.id, []byte(r.partition), h.Sum(nil)); e != nil {
		return e
	}
	w.run.size += size
	w.run.count++
	if size > w.run.maxRecord {
		w.run.maxRecord = size
	}
	return nil
}
func (w *runWriter) finish() (*sortRun, error) {
	if w.closed {
		return nil, fmt.Errorf("run writer closed")
	}
	e := w.state.ctx.Err()
	if e == nil {
		e = w.file.Sync()
	}
	e = errors.Join(e, w.state.ctx.Err(), w.abort())
	if e != nil {
		return nil, e
	}
	copy(w.run.digest[:], w.hash.Sum(nil))
	return &w.run, nil
}
func (w *runWriter) abort() error {
	if w.closed {
		return nil
	}
	w.closed = true
	return w.file.Close()
}

type runReader struct {
	state       *sortState
	file        *os.File
	run         *sortRun
	hash        hash.Hash
	count, size uint64
	previous    []byte
	closed      bool
}

func (s *sortState) openRun(run *sortRun) (*runReader, error) {
	if e := s.ctx.Err(); e != nil {
		return nil, e
	}
	f, e := os.Open(filepath.Join(s.dir, run.path))
	if e != nil {
		return nil, e
	}
	r := &runReader{state: s, file: f, run: run, hash: sha256.New()}
	var header [40]byte
	if e = r.read(header[:]); e != nil {
		return nil, errors.Join(e, r.Close())
	}
	if !bytes.Equal(header[:8], cacheMagic[:]) || !bytes.Equal(header[8:], s.binding[:]) {
		return nil, errors.Join(fmt.Errorf("cache context mismatch"), r.Close())
	}
	return r, nil
}
func (r *runReader) read(b []byte) error {
	if e := r.state.ctx.Err(); e != nil {
		return e
	}
	n, e := io.ReadFull(r.file, b)
	r.hash.Write(b[:n])
	r.size += uint64(n)
	if e == io.EOF {
		e = io.ErrUnexpectedEOF
	}
	return errors.Join(e, r.state.ctx.Err())
}
func (r *runReader) next() (sortRecord, error) {
	if r.closed {
		return sortRecord{}, fmt.Errorf("cache reader closed")
	}
	if e := r.state.ctx.Err(); e != nil {
		return sortRecord{}, e
	}
	if r.count == r.run.count {
		var b [1]byte
		n, e := r.file.Read(b[:])
		if n != 0 || e != io.EOF {
			return sortRecord{}, errors.Join(fmt.Errorf("cache trailing data or incomplete EOF"), e)
		}
		if r.size != r.run.size || !bytes.Equal(r.hash.Sum(nil), r.run.digest[:]) {
			return sortRecord{}, fmt.Errorf("cache digest or size mismatch")
		}
		if e = r.state.ctx.Err(); e != nil {
			return sortRecord{}, e
		}
		return sortRecord{}, io.EOF
	}
	var header [32]byte
	if e := r.read(header[:]); e != nil {
		return sortRecord{}, e
	}
	var lengths [4]uint64
	total := recordFrameBytes
	for i := range lengths {
		lengths[i] = binary.LittleEndian.Uint64(header[i*8:])
		var e error
		total, e = checkedAdd(total, lengths[i])
		if e != nil {
			return sortRecord{}, e
		}
	}
	if total > r.run.maxRecord || total > r.run.size || total < recordFrameBytes || fitInt(total) != nil {
		return sortRecord{}, fmt.Errorf("cache record length exceeds admitted bound")
	}
	data := make([]byte, int(total-recordFrameBytes))
	if e := r.read(data); e != nil {
		return sortRecord{}, e
	}
	var checksum [32]byte
	if e := r.read(checksum[:]); e != nil {
		return sortRecord{}, e
	}
	h := sha256.New()
	h.Write(header[:])
	h.Write(data)
	if !bytes.Equal(checksum[:], h.Sum(nil)) {
		return sortRecord{}, fmt.Errorf("cache record checksum mismatch")
	}
	fields := [4][]byte{}
	remaining := data
	for i, n := range lengths {
		fields[i] = remaining[:int(n):int(n)]
		remaining = remaining[int(n):]
	}
	record := sortRecord{key: fields[0], typed: fields[1], id: fields[2], partition: string(fields[3])}
	values, e := decodeTyped(r.state.schema, record.typed)
	if e != nil {
		return sortRecord{}, e
	}
	normalized, e := NormalizeRow(r.state.schema, values)
	if e != nil {
		return sortRecord{}, e
	}
	key, e := lthash.EncodeRow(r.state.schema.TableID, r.state.schema.Columns, normalized)
	if e != nil {
		return sortRecord{}, e
	}
	if !bytes.Equal(key, record.key) || !bytes.Equal(encodeTyped(normalized, uint64(len(record.typed))), record.typed) {
		return sortRecord{}, fmt.Errorf("cache typed values disagree with canonical key")
	}
	if r.previous != nil && bytes.Compare(r.previous, record.key) > 0 {
		return sortRecord{}, fmt.Errorf("cache row order mismatch")
	}
	r.previous = bytes.Clone(record.key)
	r.count++
	return record, nil
}
func (r *runReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	r.previous = nil
	return r.file.Close()
}
