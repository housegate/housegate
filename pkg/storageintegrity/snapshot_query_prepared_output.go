package storageintegrity

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// SnapshotQueryPrepareRequest identifies one sequenced source attempt.
type SnapshotQueryPrepareRequest struct {
	Envelope replay.SnapshotQueryEnvelope
	Accepted replay.SnapshotQuerySubmitResult
}

// SnapshotQueryPrepared is deliberately a pre-write projection. Its empty
// claim/candidate/capacity fields cannot be mistaken for a registered claim.
type SnapshotQueryPrepared struct {
	StatementID         string   `json:"statement_id"`
	InputRoot           string   `json:"input_root"`
	BlockSeq            uint64   `json:"block_seq"`
	FencingGeneration   uint64   `json:"fencing_generation"`
	OutputRowsRoot      string   `json:"output_rows_root"`
	OutputRowCount      uint64   `json:"output_row_count"`
	TouchedPartitionIDs []string `json:"touched_partition_ids"`
	// Status is always PendingUnsubmitted here. These are explicit unavailable
	// values, not a zero-value committed claim.
	Status                string                    `json:"status"`
	CapacityReservationID string                    `json:"capacity_reservation_id"`
	Candidates            []replay.SnapshotReadPart `json:"candidates"`
	SourceClaimRoot       string                    `json:"source_claim_root"`
	Stage                 string                    `json:"stage"`
	ComputedStateRoot     string                    `json:"computed_state_root"`
	CachePath             string                    `json:"cache_path"`
	CacheDigest           string                    `json:"cache_digest"`
}

// PreparedOutputStager is an injected, default-off local journal helper. It
// has no Arbiter, pressure, candidate, or unsafe-write dependency.
type PreparedOutputStager struct {
	journal SnapshotQueryJournal
	dir     string
}

// PreparedOutput is the public, injected input boundary for Stage. The owner
// of a one-shot executor adapts its prepared handle here; storage-integrity
// deliberately does not import that executor package. Stage consumes this
// handle exactly once and always calls Close before it persists a projection.
type PreparedOutput interface {
	Job() replay.SnapshotQueryJob
	PreparedResult() replay.ExecutionResult
	OutputRows() PreparedOutputRows
	Close() error
}

// PreparedOutputRows is the narrow canonical-output capability that the
// durable cache owns. It admits no replay, publication, claim, or executor
// authority, so an adapter can be supplied at a composition boundary without
// creating a storage-integrity-to-executor dependency.
type PreparedOutputRows interface {
	RowCount() uint64
	OutputRowsRoot() string
	TouchedPartitionIDs() []string
	OpenRows() (payloadexec.RowSource, error)
}

// PreparedOutputAdapter adapts an executor-owned prepared handle at the
// composition boundary. It intentionally stores only the four capabilities
// Stage needs, so callers can bridge a concrete execution type without adding
// its package to storage-integrity's production dependency graph.
type PreparedOutputAdapter struct {
	JobValue    replay.SnapshotQueryJob
	ResultValue replay.ExecutionResult
	Rows        PreparedOutputRows
	CloseFunc   func() error
}

func (a PreparedOutputAdapter) Job() replay.SnapshotQueryJob           { return a.JobValue }
func (a PreparedOutputAdapter) PreparedResult() replay.ExecutionResult { return a.ResultValue }
func (a PreparedOutputAdapter) OutputRows() PreparedOutputRows         { return a.Rows }
func (a PreparedOutputAdapter) Close() error {
	if a.CloseFunc == nil {
		return nil
	}
	return a.CloseFunc()
}

func NewPreparedOutputStager(j SnapshotQueryJournal, dir string) (*PreparedOutputStager, error) {
	if j == nil || dir == "" {
		return nil, errors.New("storageintegrity: prepared output journal and dir are required")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	return &PreparedOutputStager{j, dir}, nil
}

// Stage consumes a PreparedOutput exactly once. It does not call Replay, so
// the SELECT cannot be repeated to recreate rows. PreparedOutput is the
// callable lifecycle boundary; concrete executor adapters belong to their
// composition owner, not this injection-only package.
func (s *PreparedOutputStager) Stage(ctx context.Context, req SnapshotQueryPrepareRequest, p PreparedOutput) (prepared SnapshotQueryPrepared, err error) {
	if s == nil || p == nil {
		return SnapshotQueryPrepared{}, errors.New("storageintegrity: prepared output stager is uninitialized")
	}
	closed := false
	defer func() {
		if closed {
			return
		}
		if closeErr := p.Close(); closeErr != nil {
			closeErr = fmt.Errorf("storageintegrity: close prepared output: %w", closeErr)
			if err == nil {
				err = closeErr
			} else {
				err = errors.Join(err, closeErr)
			}
		}
	}()
	if err := ctx.Err(); err != nil {
		return SnapshotQueryPrepared{}, err
	}
	if err := validateSnapshotQueryAccepted(req.Envelope, req.Accepted); err != nil {
		return SnapshotQueryPrepared{}, err
	}
	b := req.Envelope.Input.Binding
	job := p.Job()
	if job.Statement.StatementSeq != req.Accepted.StatementSeq || job.BlockSeq != req.Accepted.BlockSeq || job.Statement.Envelope.InputRoot != req.Envelope.InputRoot || job.Reservation.ReservationID != b.ReservationID || job.Reservation.FencingGeneration != b.FencingGeneration {
		return SnapshotQueryPrepared{}, errors.New("storageintegrity: prepared output identity mismatch")
	}
	result := p.PreparedResult()
	out := p.OutputRows()
	if result.SnapshotQuery == nil || result.SnapshotQuery.ExecutionOutcome != "applied" || out == nil || result.SnapshotQuery.OutputRowsRoot != out.OutputRowsRoot() || result.SnapshotQuery.OutputRowCount != out.RowCount() || result.ComputedStateRoot == "" {
		return SnapshotQueryPrepared{}, errors.New("storageintegrity: invalid prepared output")
	}
	path := filepath.Join(s.dir, preparedCacheName(b.StatementID, req.Envelope.InputRoot, b.ReservationID, b.FencingGeneration, req.Accepted.BlockSeq))
	digest, err := copyAndVerifyPreparedOutput(ctx, path, req, out, result.ComputedStateRoot)
	if err != nil {
		return SnapshotQueryPrepared{}, err
	}
	if err = p.Close(); err != nil {
		closed = true
		return SnapshotQueryPrepared{}, fmt.Errorf("storageintegrity: close prepared output: %w", err)
	}
	closed = true
	touched := out.TouchedPartitionIDs()
	sort.Strings(touched)
	prepared = SnapshotQueryPrepared{StatementID: b.StatementID, InputRoot: req.Envelope.InputRoot, BlockSeq: req.Accepted.BlockSeq, FencingGeneration: b.FencingGeneration, OutputRowsRoot: out.OutputRowsRoot(), OutputRowCount: out.RowCount(), TouchedPartitionIDs: touched, Status: "PendingUnsubmitted", CapacityReservationID: "", Candidates: []replay.SnapshotReadPart{}, SourceClaimRoot: "", Stage: string(SnapshotQueryStagePreparedOutput), ComputedStateRoot: result.ComputedStateRoot, CachePath: path, CacheDigest: digest}
	if err := validateSnapshotQueryPrepared(req.Envelope, req.Accepted, prepared); err != nil {
		return SnapshotQueryPrepared{}, err
	}
	rec, ok, err := s.journal.Load(ctx, b.StatementID)
	if err != nil {
		return SnapshotQueryPrepared{}, err
	}
	if !ok || rec.Stage != SnapshotQueryStageSequenced || !rec.HasSubmit || rec.Submit != req.Accepted {
		return SnapshotQueryPrepared{}, errors.New("storageintegrity: accepted query is not durably sequenced")
	}
	if err := matchSnapshotQueryIdentity(rec, req.Envelope); err != nil {
		return SnapshotQueryPrepared{}, err
	}
	rec.Stage = SnapshotQueryStagePreparedOutput
	rec.PreparedOutput = &prepared
	if err := s.journal.Save(ctx, rec); err != nil {
		return SnapshotQueryPrepared{}, err
	}
	return prepared, nil
}

func validateSnapshotQueryPrepared(env replay.SnapshotQueryEnvelope, accepted replay.SnapshotQuerySubmitResult, p SnapshotQueryPrepared) error {
	b := env.Input.Binding
	if p.StatementID != b.StatementID || p.InputRoot != env.InputRoot || p.BlockSeq != accepted.BlockSeq || p.FencingGeneration != b.FencingGeneration || p.OutputRowsRoot == "" || p.ComputedStateRoot == "" || p.CachePath == "" || p.CacheDigest == "" || p.Stage != string(SnapshotQueryStagePreparedOutput) || p.Status != "PendingUnsubmitted" || p.CapacityReservationID != "" || p.SourceClaimRoot != "" || len(p.Candidates) != 0 {
		return errors.New("storageintegrity: invalid prepared output projection")
	}
	for i, v := range p.TouchedPartitionIDs {
		if v == "" || i > 0 && p.TouchedPartitionIDs[i-1] >= v {
			return errors.New("storageintegrity: noncanonical touched partitions")
		}
	}
	return nil
}

type preparedHeader struct {
	StatementID       string `json:"statement_id"`
	InputRoot         string `json:"input_root"`
	ReservationID     string `json:"reservation_id"`
	FencingGeneration uint64 `json:"fencing_generation"`
	BlockSeq          uint64 `json:"block_seq"`
	OutputRowsRoot    string `json:"output_rows_root"`
	OutputRowCount    uint64 `json:"output_row_count"`
	ComputedStateRoot string `json:"computed_state_root"`
}
type preparedTrailer struct {
	Rows   uint64 `json:"rows"`
	Digest string `json:"digest"`
}

func preparedCacheName(a, b, c string, d, e uint64) string {
	x := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%s\x00%d\x00%d", a, b, c, d, e)))
	return hex.EncodeToString(x[:]) + ".prepared"
}

type preparedRow struct {
	Row any `json:"row"`
}

func copyAndVerifyPreparedOutput(ctx context.Context, path string, req SnapshotQueryPrepareRequest, out PreparedOutputRows, state string) (string, error) {
	rows, err := out.OpenRows()
	if err != nil {
		return "", err
	}
	defer rows.Close()
	f, err := os.CreateTemp(filepath.Dir(path), ".tmp-prepared-")
	if err != nil {
		return "", err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	w := bufio.NewWriter(f)
	h := preparedHeader{req.Envelope.Input.Binding.StatementID, req.Envelope.InputRoot, req.Envelope.Input.Binding.ReservationID, req.Envelope.Input.Binding.FencingGeneration, req.Accepted.BlockSeq, out.OutputRowsRoot(), out.RowCount(), state}
	if err = writeLine(w, h); err != nil {
		return "", err
	}
	sum := sha256.New()
	var n uint64
	for {
		if err = ctx.Err(); err != nil {
			return "", err
		}
		row, e := rows.Next(ctx)
		if e == io.EOF {
			break
		}
		if e != nil {
			return "", e
		}
		line, e := json.Marshal(preparedRow{row})
		if e != nil {
			return "", e
		}
		line = append(line, '\n')
		if _, e = w.Write(line); e != nil {
			return "", e
		}
		sum.Write(line)
		n++
	}
	if n != out.RowCount() {
		return "", errors.New("storageintegrity: output row count changed")
	}
	digest := hex.EncodeToString(sum.Sum(nil))
	if err = writeLine(w, preparedTrailer{n, digest}); err != nil {
		return "", err
	}
	if err = w.Flush(); err != nil {
		return "", err
	}
	if err = f.Sync(); err != nil {
		return "", err
	}
	if err = f.Close(); err != nil {
		return "", err
	}
	if err = os.Rename(tmp, path); err != nil {
		return "", err
	}
	if err = syncDir(filepath.Dir(path)); err != nil {
		return "", err
	}
	if err = verifyPreparedOutput(path, h, digest); err != nil {
		return "", err
	}
	return digest, nil
}
func writeLine(w *bufio.Writer, v any) error {
	b, e := json.Marshal(v)
	if e != nil {
		return e
	}
	_, e = w.Write(append(b, '\n'))
	return e
}
func verifyPreparedOutput(path string, want preparedHeader, digest string) error {
	f, e := os.Open(path)
	if e != nil {
		return e
	}
	defer f.Close()
	r := bufio.NewReader(f)
	line, e := r.ReadBytes('\n')
	if e != nil {
		return e
	}
	var h preparedHeader
	if json.Unmarshal(line, &h) != nil || h != want {
		return errors.New("storageintegrity: prepared cache header mismatch")
	}
	sum := sha256.New()
	var n uint64
	for {
		line, e = r.ReadBytes('\n')
		if e != nil {
			return errors.New("storageintegrity: prepared cache missing trailer")
		}
		var envelope struct {
			Rows   *uint64         `json:"rows"`
			Digest *string         `json:"digest"`
			Row    json.RawMessage `json:"row"`
		}
		if json.Unmarshal(line, &envelope) != nil {
			return errors.New("storageintegrity: malformed prepared cache entry")
		}
		if envelope.Rows != nil || envelope.Digest != nil {
			if envelope.Rows == nil || envelope.Digest == nil || len(envelope.Row) != 0 {
				return errors.New("storageintegrity: malformed prepared cache trailer")
			}
			return verifyPreparedTrailer(preparedTrailer{*envelope.Rows, *envelope.Digest}, n, hex.EncodeToString(sum.Sum(nil)), digest)
		}
		if len(envelope.Row) == 0 {
			return errors.New("storageintegrity: malformed prepared cache row")
		}
		sum.Write(line)
		n++
	}
}
func verifyPreparedTrailer(t preparedTrailer, n uint64, actual, want string) error {
	if t.Rows != n || t.Digest != want || actual != want {
		return errors.New("storageintegrity: prepared cache digest mismatch")
	}
	return nil
}
