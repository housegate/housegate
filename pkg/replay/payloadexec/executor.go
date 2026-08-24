// Package payloadexec is the MVP "pinned executor" for the replay verifier
// (see docs/superpowers/specs/2026-06-10-multi-replica-trust-design.md
// Appendix C.3). It is deliberately payload-local and in-process: it decodes a
// CSV INSERT payload, injects the per-row _hg_row_id described in §5.2, computes
// per-row LtHash content commitments via pkg/lthash, and folds them into the
// part -> partition -> state-root hierarchy of §5.4.
//
// The default executor is NOT a ClickHouse runner — it decodes the wire payload
// directly. How a statement's rows are produced is abstracted behind the
// Materializer seam: pkg/replay/chexec supplies a ClickHouse-backed materializer
// that executes the INSERT on a pinned ClickHouse and reads the rows back, while
// the part -> partition -> state-root assembly here is shared by both. The heavy
// storage mechanics (hardlink/reflink scratch clones, ATTACH PART, mutation
// scratch tables, cold restore) listed in Appendix C.5 remain out of scope.
// Anything that is not an admitted payload-local INSERT (mutations, INSERT
// ... SELECT, DDL) is rejected, which the verifier surfaces as a local refusal
// to attest (Appendix C.4).
package payloadexec

import (
	"context"
	"encoding/binary"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/zeebo/blake3"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay"
)

// rowIDColumn is the reserved physical row-instance identity column (§5.2).
const rowIDColumn = "_hg_row_id"

// rowIDDomain domain-separates the row-id hash (§5.2).
const rowIDDomain = "housegate-row-id-v1"

// tablePartition keys the set of partitions touched by a block.
type tablePartition struct{ table, partition string }

// TableSchema describes one verified table for the MVP executor. In production
// this is derived from the anchored DDL/schema snapshot; here it is configured.
type TableSchema struct {
	TableID string `json:"table_id"`
	// PartitionBy is an optional user column whose value selects the partition.
	// Empty means a single partition named "all".
	PartitionBy string `json:"partition_by"`
	// Columns are the user (wire) columns in declared order; the CSV payload
	// header must name exactly this set. _hg_row_id is injected by the executor
	// and must not appear here.
	Columns []lthash.Column `json:"columns"`
}

// Row is one materialized logical row: its row-instance id (_hg_row_id), the
// user-column values in schema-declared order, and its partition assignment.
type Row struct {
	RowID       []byte
	Values      []any
	PartitionID string
	RawBytes    uint64
}

// Materializer turns one payload-local INSERT statement into its ordered logical
// rows. The default (csvMaterializer) decodes the CSV wire payload in-process;
// a ClickHouse-backed implementation (pkg/replay/chexec) executes the INSERT on
// a pinned ClickHouse and reads the materialized rows back, which is what lets a
// verifier check real ClickHouse materialization (defaults, type coercion, JSON
// shredding) rather than approximating it. The materializer is the only seam
// that differs between executors; all part/partition/state-root assembly is
// shared.
type Materializer interface {
	Materialize(ctx context.Context, schema TableSchema, st replay.PreparedStatement) ([]Row, error)
}

// Executor is the payload-local replay executor. It is safe for concurrent use
// when its Materializer is.
type Executor struct {
	NetworkID    string
	tables       map[string]TableSchema
	materializer Materializer
}

// New builds an in-process executor (CSV wire-payload materializer).
func New(networkID string, tables ...TableSchema) *Executor {
	return NewWithMaterializer(networkID, csvMaterializer{networkID: networkID}, tables...)
}

// NewWithMaterializer builds an executor with a custom materializer (e.g. the
// ClickHouse-backed one in pkg/replay/chexec). networkID is still used for
// schema-root hashing in GenesisSnapshot and must match the materializer's.
func NewWithMaterializer(networkID string, m Materializer, tables ...TableSchema) *Executor {
	tbl := make(map[string]TableSchema, len(tables))
	for _, t := range tables {
		tbl[t.TableID] = t
	}
	return &Executor{NetworkID: networkID, tables: tbl, materializer: m}
}

// Replay satisfies replay.Executor.
func (e *Executor) Replay(ctx context.Context, req replay.ExecutionRequest) (replay.ExecutionResult, error) {
	_, res, err := e.ApplyContext(ctx, req.Snapshot, req.Job, req.Statements)
	return res, err
}

// GenesisSnapshot builds and seals an empty safe snapshot for the executor's
// tables, suitable as the prev-state of block 1.
func (e *Executor) GenesisSnapshot(safeBlockSeq uint64, schemaSnapshotID, executorProfileID string) (replay.SafeSnapshotManifest, error) {
	ids := e.sortedTableIDs()
	tables := make([]replay.TableManifest, 0, len(ids))
	schemas := make([]TableSchema, 0, len(ids))
	for _, id := range ids {
		sch := e.tables[id]
		schemas = append(schemas, sch)
		tables = append(tables, replay.TableManifest{
			TableID:    id,
			SchemaHash: tableSchemaHash(e.NetworkID, sch),
		})
	}
	m := replay.SafeSnapshotManifest{
		SafeBlockSeq:      safeBlockSeq,
		SchemaSnapshotID:  schemaSnapshotID,
		SchemaRoot:        schemaRoot(e.NetworkID, schemas),
		ExecutorProfileID: executorProfileID,
		Tables:            tables,
	}
	return m.Seal()
}

// Apply replays one block with a background context. See ApplyContext.
func (e *Executor) Apply(prev replay.SafeSnapshotManifest, job replay.ReplayJob, stmts []replay.PreparedStatement) (replay.SafeSnapshotManifest, replay.ExecutionResult, error) {
	return e.ApplyContext(context.Background(), prev, job, stmts)
}

// ApplyContext replays one block against the previous safe snapshot and returns
// both the sealed post-state manifest (for chaining / snapshot stores) and the
// execution result (the projection the verifier signs).
func (e *Executor) ApplyContext(ctx context.Context, prev replay.SafeSnapshotManifest, job replay.ReplayJob, stmts []replay.PreparedStatement) (replay.SafeSnapshotManifest, replay.ExecutionResult, error) {
	if e.materializer == nil {
		return replay.SafeSnapshotManifest{}, replay.ExecutionResult{}, fmt.Errorf("executor has no materializer")
	}
	if err := verifyLedger(prev); err != nil {
		return replay.SafeSnapshotManifest{}, replay.ExecutionResult{}, fmt.Errorf("prev snapshot ledger: %w", err)
	}
	// Defense-in-depth: statement_id uniqueness is safety-critical (a reused id
	// collides _hg_row_id and resurrects the duplicate-row LtHash cancellation
	// attack, §5.2). Re-enforce it here so the executor fails closed even when
	// driven directly (e.g. snapshot promotion) rather than via the Verifier.
	if err := validateBlockStatements(stmts); err != nil {
		return replay.SafeSnapshotManifest{}, replay.ExecutionResult{}, err
	}

	// parts[tableID][partitionID] = active parts (carried forward + new).
	parts := map[string]map[string][]replay.PartManifestEntry{}
	schemaHashes := map[string]string{}
	for _, tm := range prev.Tables {
		schemaHashes[tm.TableID] = tm.SchemaHash
		byPartition := map[string][]replay.PartManifestEntry{}
		for _, p := range tm.ActiveParts {
			byPartition[p.PartitionID] = append(byPartition[p.PartitionID], p)
		}
		parts[tm.TableID] = byPartition
	}

	touchedSet := map[tablePartition]struct{}{}
	var affected []replay.PartManifestEntry

	for i, st := range stmts {
		if st.PayloadRef == "" {
			return replay.SafeSnapshotManifest{}, replay.ExecutionResult{}, fmt.Errorf("statement %d (%s): MVP executor only replays payload-local INSERTs; statement has no payload (mutation/DDL class)", i, st.StatementID)
		}
		schema, ok := e.tables[st.TargetTableID]
		if !ok {
			return replay.SafeSnapshotManifest{}, replay.ExecutionResult{}, fmt.Errorf("statement %d (%s): unknown target table %q", i, st.StatementID, st.TargetTableID)
		}
		rows, err := e.materializer.Materialize(ctx, schema, st)
		if err != nil {
			return replay.SafeSnapshotManifest{}, replay.ExecutionResult{}, fmt.Errorf("statement %d (%s): %w", i, st.StatementID, err)
		}
		newParts, err := buildParts(schema, job.BlockSeq, st.StatementSeq, rows)
		if err != nil {
			return replay.SafeSnapshotManifest{}, replay.ExecutionResult{}, fmt.Errorf("statement %d (%s): %w", i, st.StatementID, err)
		}
		for _, np := range newParts {
			if parts[np.TableID] == nil {
				return replay.SafeSnapshotManifest{}, replay.ExecutionResult{}, fmt.Errorf("statement %d: table %q not in snapshot", i, np.TableID)
			}
			parts[np.TableID][np.PartitionID] = append(parts[np.TableID][np.PartitionID], np)
			touchedSet[tablePartition{np.TableID, np.PartitionID}] = struct{}{}
			affected = append(affected, np)
		}
	}

	// Rebuild table manifests, recomputing each partition root from its parts.
	tables := make([]replay.TableManifest, 0, len(parts))
	for tableID, byPartition := range parts {
		var partitionRoots []replay.PartitionCommitment
		var active []replay.PartManifestEntry
		for partitionID, entries := range byPartition {
			acc := lthash.New()
			for _, p := range entries {
				h, err := lthashFromHex(p.PartRowLtHash)
				if err != nil {
					return replay.SafeSnapshotManifest{}, replay.ExecutionResult{}, fmt.Errorf("table %s part %s: %w", tableID, p.PartName, err)
				}
				acc.AddHash(h)
				active = append(active, p)
			}
			partitionRoots = append(partitionRoots, replay.PartitionCommitment{
				TableID:     tableID,
				PartitionID: partitionID,
				Root:        lthashHex(acc),
			})
		}
		tables = append(tables, replay.TableManifest{
			TableID:        tableID,
			SchemaHash:     schemaHashes[tableID],
			PartitionRoots: partitionRoots,
			ActiveParts:    active,
		})
	}

	next, err := (replay.SafeSnapshotManifest{
		ParentSnapshotID:  prev.SnapshotID,
		SafeBlockSeq:      job.BlockSeq,
		SchemaSnapshotID:  prev.SchemaSnapshotID,
		SchemaRoot:        prev.SchemaRoot,
		ExecutorProfileID: prev.ExecutorProfileID,
		Tables:            tables,
	}).Seal()
	if err != nil {
		return replay.SafeSnapshotManifest{}, replay.ExecutionResult{}, fmt.Errorf("seal post-state manifest: %w", err)
	}

	result := replay.ExecutionResult{
		BlockSeq:                  job.BlockSeq,
		PrevSafeSnapshotID:        job.PrevSafeSnapshotID,
		PrevStateRoot:             job.PrevStateRoot,
		SchemaSnapshotID:          job.SchemaSnapshotID,
		ExecutorProfileID:         job.ExecutorProfileID,
		ComputedStateRoot:         next.StateRoot,
		PartitionCommitmentsAfter: affectedPartitionCommitments(next, touchedSet2slice(touchedSet)),
		AffectedParts:             sortedParts(affected),
		ReplayLogHash:             replayLogHash(stmts, affected),
	}
	return next, result, nil
}

// validateBlockStatements rejects duplicate statement_id and non-strictly-
// increasing statement_seq within a block.
func validateBlockStatements(stmts []replay.PreparedStatement) error {
	seen := make(map[string]struct{}, len(stmts))
	var lastSeq uint64
	for i, st := range stmts {
		if st.StatementID == "" {
			return fmt.Errorf("statement %d: statement_id is required", i)
		}
		if _, dup := seen[st.StatementID]; dup {
			return fmt.Errorf("statement %d: duplicate statement_id %q", i, st.StatementID)
		}
		seen[st.StatementID] = struct{}{}
		if st.StatementSeq <= lastSeq {
			return fmt.Errorf("statement %d: statement_seq %d must exceed previous %d", i, st.StatementSeq, lastSeq)
		}
		lastSeq = st.StatementSeq
	}
	return nil
}

// buildParts groups materialized rows into per-partition parts, computing each
// part's LtHash from the canonical row elements. One part is emitted per
// (statement, partition).
func buildParts(schema TableSchema, blockSeq, statementSeq uint64, rows []Row) ([]replay.PartManifestEntry, error) {
	type partAgg struct {
		acc      *lthash.Hash
		rowCount uint64
		bytes    uint64
	}
	byPartition := map[string]*partAgg{}
	var partitionOrder []string

	for _, r := range rows {
		h, err := rowElementHash(schema, r.RowID, r.Values)
		if err != nil {
			return nil, err
		}
		agg := byPartition[r.PartitionID]
		if agg == nil {
			agg = &partAgg{acc: lthash.New()}
			byPartition[r.PartitionID] = agg
			partitionOrder = append(partitionOrder, r.PartitionID)
		}
		agg.acc.AddHash(h)
		agg.rowCount++
		agg.bytes += r.RawBytes
	}

	sort.Strings(partitionOrder)
	out := make([]replay.PartManifestEntry, 0, len(partitionOrder))
	for _, partitionID := range partitionOrder {
		agg := byPartition[partitionID]
		partName := fmt.Sprintf("%s-b%d-s%d", partitionID, blockSeq, statementSeq)
		rowLtHash := lthashHex(agg.acc)
		out = append(out, replay.PartManifestEntry{
			TableID:       schema.TableID,
			PartitionID:   partitionID,
			PartName:      partName,
			PartPhysHash:  mvpPartPhysHash(partName, rowLtHash),
			PartRowLtHash: rowLtHash,
			RowCount:      agg.rowCount,
			Bytes:         agg.bytes,
		})
	}
	return out, nil
}

// csvMaterializer is the default in-process materializer: it decodes the CSV
// wire payload and derives _hg_row_id deterministically from (statement_id,
// global_row_ordinal) per §5.2.
type csvMaterializer struct {
	networkID string
}

func (m csvMaterializer) Materialize(_ context.Context, schema TableSchema, st replay.PreparedStatement) ([]Row, error) {
	rows, err := DecodeCSV(st.Payload, schema)
	if err != nil {
		return nil, err
	}
	for ordinal := range rows {
		rows[ordinal].RowID = RowID(m.networkID, schema.TableID, st.StatementID, uint64(ordinal))
	}
	return rows, nil
}

// DecodeCSV parses a CSVWithNames payload against the schema into logical rows
// (Values + PartitionID + RawBytes), leaving RowID for the caller to assign.
// The header must name exactly the schema's user columns (any order); each field
// is parsed to the Go type matching its declared ClickHouse width so the lthash
// encoding is width-stable.
func DecodeCSV(payload []byte, sch TableSchema) ([]Row, error) {
	r := csv.NewReader(strings.NewReader(string(payload)))
	r.FieldsPerRecord = -1
	rows, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("parse CSV payload: %w", err)
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("payload has no header row")
	}

	header := rows[0]
	if len(header) != len(sch.Columns) {
		return nil, fmt.Errorf("CSV header has %d columns, schema has %d", len(header), len(sch.Columns))
	}
	// headerIndex[columnName] = position in the CSV record.
	headerIndex := make(map[string]int, len(header))
	for i, name := range header {
		if _, dup := headerIndex[name]; dup {
			return nil, fmt.Errorf("duplicate CSV header column %q", name)
		}
		headerIndex[name] = i
	}
	colPos := make([]int, len(sch.Columns))
	for i, col := range sch.Columns {
		pos, ok := headerIndex[col.Name]
		if !ok {
			return nil, fmt.Errorf("CSV header missing schema column %q", col.Name)
		}
		colPos[i] = pos
	}
	if sch.PartitionBy != "" {
		if _, ok := headerIndex[sch.PartitionBy]; !ok {
			return nil, fmt.Errorf("partition column %q not present in payload", sch.PartitionBy)
		}
	}
	out := make([]Row, 0, len(rows)-1)
	for ln, rec := range rows[1:] {
		if len(rec) != len(header) {
			return nil, fmt.Errorf("data row %d has %d fields, want %d", ln, len(rec), len(header))
		}
		values := make([]any, len(sch.Columns))
		var raw uint64
		for i, col := range sch.Columns {
			field := rec[colPos[i]]
			raw += uint64(len(field))
			v, err := parseValue(col.Type, field)
			if err != nil {
				return nil, fmt.Errorf("data row %d column %q: %w", ln, col.Name, err)
			}
			values[i] = v
		}
		partitionID := "all"
		if sch.PartitionBy != "" {
			partitionID = "p_" + rec[headerIndex[sch.PartitionBy]]
		}
		out = append(out, Row{Values: values, PartitionID: partitionID, RawBytes: raw})
	}
	return out, nil
}

// PartitionIDForRow derives the typed executor partition id for one schema-
// ordered row. Native and other typed materializers use it; the legacy CSV
// profile intentionally preserves its original wire-text partition ids.
func PartitionIDForRow(sch TableSchema, values []any) (string, error) {
	if len(values) != len(sch.Columns) {
		return "", fmt.Errorf("row has %d values, schema has %d columns", len(values), len(sch.Columns))
	}
	if sch.PartitionBy == "" {
		return "all", nil
	}
	for i, col := range sch.Columns {
		if col.Name != sch.PartitionBy {
			continue
		}
		raw, err := partitionValueString(values[i])
		if err != nil {
			return "", fmt.Errorf("partition column %q: %w", sch.PartitionBy, err)
		}
		return "p_" + raw, nil
	}
	return "", fmt.Errorf("partition column %q not present in schema", sch.PartitionBy)
}

func partitionValueString(v any) (string, error) {
	switch x := v.(type) {
	case string:
		return x, nil
	case []byte:
		return string(x), nil
	case bool:
		return strconv.FormatBool(x), nil
	case uint8:
		return strconv.FormatUint(uint64(x), 10), nil
	case uint16:
		return strconv.FormatUint(uint64(x), 10), nil
	case uint32:
		return strconv.FormatUint(uint64(x), 10), nil
	case uint64:
		return strconv.FormatUint(x, 10), nil
	case uint:
		return strconv.FormatUint(uint64(x), 10), nil
	case int8:
		return strconv.FormatInt(int64(x), 10), nil
	case int16:
		return strconv.FormatInt(int64(x), 10), nil
	case int32:
		return strconv.FormatInt(int64(x), 10), nil
	case int64:
		return strconv.FormatInt(x, 10), nil
	case int:
		return strconv.FormatInt(int64(x), 10), nil
	case float32:
		if x == 0 {
			return "0", nil
		}
		return strconv.FormatFloat(float64(x), 'g', -1, 32), nil
	case float64:
		if x == 0 {
			return "0", nil
		}
		return strconv.FormatFloat(x, 'g', -1, 64), nil
	default:
		return "", fmt.Errorf("unsupported partition value type %T", v)
	}
}

// parseValue converts a raw CSV field to the Go type matching the declared
// ClickHouse type. Unsupported types are rejected (default-deny, §5.3).
func parseValue(typeName, raw string) (any, error) {
	columnType := classifyColumnType(typeName)
	switch columnType.kind {
	case columnTypeString:
		return raw, nil
	case columnTypeFixedString:
		return parseFixedString(columnType.fixedStringWidth, raw)
	case columnTypeBool:
		return strconv.ParseBool(raw)
	case columnTypeFloat32:
		f, err := strconv.ParseFloat(raw, 32)
		return float32(f), err
	case columnTypeFloat64:
		f, err := strconv.ParseFloat(raw, 64)
		return f, err
	case columnTypeUInt8:
		u, err := strconv.ParseUint(raw, 10, 8)
		return uint8(u), err
	case columnTypeUInt16:
		u, err := strconv.ParseUint(raw, 10, 16)
		return uint16(u), err
	case columnTypeUInt32:
		u, err := strconv.ParseUint(raw, 10, 32)
		return uint32(u), err
	case columnTypeUInt64:
		u, err := strconv.ParseUint(raw, 10, 64)
		return u, err
	case columnTypeInt8:
		n, err := strconv.ParseInt(raw, 10, 8)
		return int8(n), err
	case columnTypeInt16:
		n, err := strconv.ParseInt(raw, 10, 16)
		return int16(n), err
	case columnTypeInt32:
		n, err := strconv.ParseInt(raw, 10, 32)
		return int32(n), err
	case columnTypeInt64:
		n, err := strconv.ParseInt(raw, 10, 64)
		return n, err
	default:
		return nil, unsupportedColumnTypeError(typeName)
	}
}

// parseFixedString matches ClickHouse's physical FixedString(N): values longer
// than N are rejected (CH errors rather than truncating on insert) and shorter
// values are right-padded with NUL to exactly N bytes, so the canonical element
// holds "all N bytes, including zero padding" (§5.3). The shared type
// classifier has already proved width positive, bounded and grammar-compatible.
func parseFixedString(width int, raw string) (any, error) {
	if len(raw) > width {
		return nil, fmt.Errorf("FixedString(%d) value is %d bytes, exceeds width", width, len(raw))
	}
	b := make([]byte, width)
	copy(b, raw)
	return b, nil
}

// rowElementHash builds the canonical row element including the injected
// _hg_row_id column and returns its LtHash.
func rowElementHash(sch TableSchema, rid []byte, values []any) (*lthash.Hash, error) {
	cols := make([]lthash.Column, 0, len(sch.Columns)+1)
	cols = append(cols, lthash.Column{Name: rowIDColumn, Type: "FixedString(32)"})
	cols = append(cols, sch.Columns...)
	vals := make([]any, 0, len(values)+1)
	vals = append(vals, rid)
	vals = append(vals, values...)
	return lthash.RowHash(sch.TableID, cols, vals)
}

// RowID derives the deterministic _hg_row_id (§5.2):
// BLAKE3(domain || network_id || table_id || statement_id || global_row_ordinal).
// Fields are length-framed to avoid concatenation ambiguity. Exported so the
// ClickHouse-backed materializer injects the identical id before INSERT.
func RowID(networkID, tableID, statementID string, ordinal uint64) []byte {
	h := blake3.New()
	_, _ = h.WriteString(rowIDDomain)
	writeFramed(h, networkID)
	writeFramed(h, tableID)
	writeFramed(h, statementID)
	var ord [8]byte
	binary.LittleEndian.PutUint64(ord[:], ordinal)
	_, _ = h.Write(ord[:])
	var out [32]byte
	d := h.Digest()
	_, _ = d.Read(out[:])
	return out[:]
}

func writeFramed(h *blake3.Hasher, s string) {
	var n [4]byte
	binary.LittleEndian.PutUint32(n[:], uint32(len(s)))
	_, _ = h.Write(n[:])
	_, _ = h.WriteString(s)
}

// verifyLedger enforces §5.4: for every partition the sum of its part
// accumulators must equal the recorded partition root.
func verifyLedger(m replay.SafeSnapshotManifest) error {
	for _, t := range m.Tables {
		sums := map[string]*lthash.Hash{}
		for _, p := range t.ActiveParts {
			h, err := lthashFromHex(p.PartRowLtHash)
			if err != nil {
				return fmt.Errorf("table %s part %s: %w", t.TableID, p.PartName, err)
			}
			if sums[p.PartitionID] == nil {
				sums[p.PartitionID] = lthash.New()
			}
			sums[p.PartitionID].AddHash(h)
		}
		declared := make(map[string]struct{}, len(t.PartitionRoots))
		for _, pc := range t.PartitionRoots {
			declared[pc.PartitionID] = struct{}{}
			got := sums[pc.PartitionID]
			if got == nil {
				got = lthash.New()
			}
			if lthashHex(got) != pc.Root {
				return fmt.Errorf("table %s partition %s: parts sum != root", t.TableID, pc.PartitionID)
			}
		}
		// The invariant is bijective: a partition with active parts must also
		// declare a commitment, else an orphan part contributes data summed into
		// no partition root (§5.4 / "Registration arithmetic").
		for partitionID := range sums {
			if _, ok := declared[partitionID]; !ok {
				return fmt.Errorf("table %s partition %s: active parts present with no partition root", t.TableID, partitionID)
			}
		}
	}
	return nil
}

func (e *Executor) sortedTableIDs() []string {
	ids := make([]string, 0, len(e.tables))
	for id := range e.tables {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func touchedSet2slice(set map[tablePartition]struct{}) []tablePartition {
	out := make([]tablePartition, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Slice(out, func(a, b int) bool {
		if out[a].table != out[b].table {
			return out[a].table < out[b].table
		}
		return out[a].partition < out[b].partition
	})
	return out
}

func affectedPartitionCommitments(m replay.SafeSnapshotManifest, touched []tablePartition) []replay.PartitionCommitment {
	index := map[string]string{}
	for _, t := range m.Tables {
		for _, pc := range t.PartitionRoots {
			index[t.TableID+"\x00"+pc.PartitionID] = pc.Root
		}
	}
	out := make([]replay.PartitionCommitment, 0, len(touched))
	for _, tp := range touched {
		root := index[tp.table+"\x00"+tp.partition]
		out = append(out, replay.PartitionCommitment{TableID: tp.table, PartitionID: tp.partition, Root: root})
	}
	return out
}

func sortedParts(parts []replay.PartManifestEntry) []replay.PartManifestEntry {
	out := append([]replay.PartManifestEntry(nil), parts...)
	sort.Slice(out, func(a, b int) bool {
		if out[a].TableID != out[b].TableID {
			return out[a].TableID < out[b].TableID
		}
		if out[a].PartitionID != out[b].PartitionID {
			return out[a].PartitionID < out[b].PartitionID
		}
		return out[a].PartName < out[b].PartName
	})
	return out
}

func replayLogHash(stmts []replay.PreparedStatement, affected []replay.PartManifestEntry) string {
	var b strings.Builder
	for _, st := range stmts {
		b.WriteString(st.StatementID)
		b.WriteByte('|')
		b.WriteString(st.TargetTableID)
		b.WriteByte('\n')
	}
	for _, p := range sortedParts(affected) {
		b.WriteString(p.PartName)
		b.WriteByte('|')
		b.WriteString(p.PartRowLtHash)
		b.WriteByte('|')
		b.WriteString(strconv.FormatUint(p.RowCount, 10))
		b.WriteByte('\n')
	}
	return replay.DigestString("replay-log\x00" + b.String())
}

// mvpPartPhysHash is a deterministic placeholder for the physical part hash:
// the MVP has no materialized ClickHouse part bytes (Appendix C.5).
func mvpPartPhysHash(partName, rowLtHash string) string {
	return replay.DigestString("mvp-part-phys\x00" + partName + "\x00" + rowLtHash)
}

func lthashHex(h *lthash.Hash) string {
	return "0x" + hex.EncodeToString(h.Bytes())
}

func lthashFromHex(s string) (*lthash.Hash, error) {
	s = strings.TrimPrefix(s, "0x")
	raw, err := hex.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("decode lthash hex: %w", err)
	}
	return lthash.FromBytes(raw)
}

// tableSchemaHash and schemaRoot produce stable, opaque schema digests for the
// manifest (the verifier treats them as inputs to state_root, not as a schema
// it re-derives).
func tableSchemaHash(networkID string, t TableSchema) string {
	var b strings.Builder
	b.WriteString(networkID)
	b.WriteByte(0)
	b.WriteString(t.TableID)
	b.WriteByte(0)
	b.WriteString(t.PartitionBy)
	b.WriteByte(0)
	for _, c := range t.Columns {
		b.WriteString(c.Name)
		b.WriteByte(0)
		b.WriteString(c.Type)
		b.WriteByte(0)
	}
	return replay.DigestString("table-schema\x00" + b.String())
}

func schemaRoot(networkID string, schemas []TableSchema) string {
	sorted := append([]TableSchema(nil), schemas...)
	sort.Slice(sorted, func(a, b int) bool { return sorted[a].TableID < sorted[b].TableID })
	var b strings.Builder
	for _, s := range sorted {
		b.WriteString(tableSchemaHash(networkID, s))
		b.WriteByte(0)
	}
	return replay.DigestString("schema-root\x00" + b.String())
}
