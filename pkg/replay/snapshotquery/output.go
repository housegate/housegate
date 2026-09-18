package snapshotquery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// Two independent readers share the reserved three-row merge workspace and
// fixed control allowance. A third open is refused; Close releases the slot.
const maxOutputReaders = 2

type canonicalOutput struct {
	mu         sync.Mutex
	state      *sortState
	run        *sortRun
	root       string
	hashes     []string
	partitions []string
	readers    map[*outputRows]struct{}
	closed     bool
	closeErr   error
}

func (s *sortState) finishOutput(input *sortRun) (out *canonicalOutput, err error) {
	if err = s.memory(s.retained, s.maxWorkspace, 0); err != nil {
		return nil, err
	}
	hashes := make([]string, 0, int(s.rows))
	partitions := make(map[string]struct{})
	w, err := s.newWriter()
	if err != nil {
		return nil, err
	}
	defer func() {
		if out == nil {
			err = errors.Join(err, w.abort())
		}
	}()
	var reader *runReader
	if input != nil {
		reader, err = s.openRun(input)
		if err != nil {
			return nil, err
		}
		defer func() { err = errors.Join(err, reader.Close()) }()
	}
	for ordinal := uint64(0); ordinal < s.rows; ordinal++ {
		if reader == nil {
			return nil, fmt.Errorf("missing sorted input")
		}
		record, e := reader.next()
		if e != nil {
			return nil, e
		}
		if len(record.id) != 0 || record.partition != "" {
			return nil, fmt.Errorf("run contains final-output fields")
		}
		values, e := decodeTyped(s.schema, record.typed)
		if e != nil {
			return nil, e
		}
		record.partition, e = payloadexec.PartitionIDForRow(s.schema, values)
		if e != nil {
			return nil, e
		}
		record.id = payloadexec.RowID(s.network, s.schema.TableID, s.statement, ordinal)
		size, e := checkedAdd(s.outputBytes, record.size())
		if e != nil {
			return nil, e
		}
		if size > s.limits.MaxOutputBytes {
			return nil, fmt.Errorf("output byte limit exceeded")
		}
		s.outputBytes = size
		if e = w.write(record); e != nil {
			return nil, e
		}
		hashes = append(hashes, replay.DigestBytes(record.key))
		partitions[record.partition] = struct{}{}
	}
	if reader != nil {
		if _, e := reader.next(); e != io.EOF {
			if e == nil {
				e = fmt.Errorf("extra sorted input")
			}
			return nil, e
		}
		if err = reader.Close(); err != nil {
			return nil, err
		}
	}
	root, err := (replay.SnapshotQueryOutputCommitment{TargetTableID: s.schema.TableID, SchemaHash: payloadexec.TableSchemaHash(s.network, s.schema), RowCount: s.rows, RowHashes: hashes}).Hash()
	if err != nil {
		return nil, err
	}
	if err = s.ctx.Err(); err != nil {
		return nil, err
	}
	run, err := w.finish()
	if err != nil {
		return nil, err
	}
	if input != nil {
		if err = os.Remove(filepath.Join(s.dir, input.path)); err != nil {
			return nil, err
		}
	}
	path := filepath.Join(s.dir, "output")
	if err = os.Rename(filepath.Join(s.dir, run.path), path); err != nil {
		return nil, err
	}
	run.path = "output"
	if err = s.ctx.Err(); err != nil {
		return nil, err
	}
	dir, err := os.Open(s.dir)
	if err != nil {
		return nil, err
	}
	err = errors.Join(dir.Sync(), dir.Close(), s.ctx.Err())
	if err != nil {
		return nil, err
	}
	touched := make([]string, 0, len(partitions))
	for p := range partitions {
		touched = append(touched, p)
	}
	sort.Strings(touched)
	return &canonicalOutput{state: s, run: run, root: root, hashes: hashes, partitions: touched, readers: make(map[*outputRows]struct{})}, nil
}
func (o *canonicalOutput) RowCount() uint64              { return o.run.count }
func (o *canonicalOutput) OutputRowsRoot() string        { return o.root }
func (o *canonicalOutput) TouchedPartitionIDs() []string { return append([]string{}, o.partitions...) }
func (o *canonicalOutput) OpenRows() (payloadexec.RowSource, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return nil, fmt.Errorf("canonical output is closed")
	}
	if len(o.readers) >= maxOutputReaders {
		return nil, fmt.Errorf("canonical output reader limit exceeded")
	}
	// A bounded preflight authenticates the complete durable cache before the
	// handle is exposed. Per-row checks against retained ordered digests also
	// prevent later local mutations from returning a corrupt row.
	f, err := os.Open(filepath.Join(o.state.dir, o.run.path))
	if err != nil {
		return nil, err
	}
	info, statErr := f.Stat()
	if statErr != nil {
		return nil, errors.Join(statErr, f.Close())
	}
	if !info.Mode().IsRegular() || info.Size() < 0 || uint64(info.Size()) != o.run.size {
		return nil, errors.Join(fmt.Errorf("canonical output cache size mismatch"), f.Close())
	}
	h := sha256.New()
	var buffer [4096]byte
	n, copyErr := io.CopyBuffer(h, io.LimitReader(f, info.Size()), buffer[:])
	closeErr := f.Close()
	if err = errors.Join(copyErr, closeErr); err != nil {
		return nil, err
	}
	if uint64(n) != o.run.size || !bytes.Equal(h.Sum(nil), o.run.digest[:]) {
		return nil, fmt.Errorf("canonical output cache digest mismatch")
	}
	state := *o.state
	state.ctx = context.Background()
	r, err := state.openRun(o.run)
	if err != nil {
		return nil, err
	}
	rows := &outputRows{owner: o, reader: r}
	o.readers[rows] = struct{}{}
	return rows, nil
}
func (o *canonicalOutput) Close() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return o.closeErr
	}
	o.closed = true
	for r := range o.readers {
		o.closeErr = errors.Join(o.closeErr, r.reader.Close())
		delete(o.readers, r)
	}
	o.closeErr = errors.Join(o.closeErr, os.RemoveAll(o.state.dir))
	o.hashes = nil
	return o.closeErr
}

type outputRows struct {
	owner  *canonicalOutput
	reader *runReader
	closed bool
}

func (r *outputRows) Next(ctx context.Context) (payloadexec.Row, error) {
	o := r.owner
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed || r.closed {
		return payloadexec.Row{}, fmt.Errorf("canonical output reader is closed")
	}
	if ctx == nil {
		return payloadexec.Row{}, fmt.Errorf("context is required")
	}
	r.reader.state.ctx = ctx
	ordinal := r.reader.count
	record, err := r.reader.next()
	if err != nil {
		return payloadexec.Row{}, err
	}
	if ordinal >= uint64(len(o.hashes)) || replay.DigestBytes(record.key) != o.hashes[ordinal] {
		return payloadexec.Row{}, fmt.Errorf("canonical output row digest mismatch")
	}
	expectedID := payloadexec.RowID(o.state.network, o.state.schema.TableID, o.state.statement, ordinal)
	if !bytes.Equal(record.id, expectedID) {
		return payloadexec.Row{}, fmt.Errorf("canonical output row ID mismatch")
	}
	values, err := decodeTyped(o.state.schema, record.typed)
	if err != nil {
		return payloadexec.Row{}, err
	}
	partition, err := payloadexec.PartitionIDForRow(o.state.schema, values)
	if err != nil {
		return payloadexec.Row{}, err
	}
	if partition != record.partition {
		return payloadexec.Row{}, fmt.Errorf("canonical output partition mismatch")
	}
	if err = ctx.Err(); err != nil {
		return payloadexec.Row{}, err
	}
	return payloadexec.Row{RowID: bytes.Clone(record.id), Values: values, PartitionID: partition, RawBytes: record.size()}, nil
}
func (r *outputRows) Close() error {
	o := r.owner
	o.mu.Lock()
	defer o.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	delete(o.readers, r)
	return r.reader.Close()
}
