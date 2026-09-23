package payloadexec

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"sort"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay"
)

// StatementRows supplies already materialized, validated rows for one statement.
// Rows is borrowed: the caller must keep it valid and owns Close on every path.
// No payload reference or query/publication authority is implied by this type.
type StatementRows struct {
	StatementID   string
	StatementSeq  uint64
	TargetTableID string
	Rows          RowSource
}

// validateAppendInputs checks the complete legacy projection, not query column
// eligibility or the authenticated schema object (which are caller concerns).
// A static executor requires prev to hold exactly its configured tables; a
// dynamic one (NewDynamic) accepts any table set whose SchemaRoot matches the
// hashes it commits to, and still requires every table whose schema resolves
// to match its committed hash.
func (e *Executor) validateAppendInputs(prev replay.SafeSnapshotManifest, batches []StatementRows, schemas map[string]TableSchema) error {
	if err := prev.Validate(); err != nil {
		return fmt.Errorf("prev snapshot: %w", err)
	}
	if !e.dynamic && len(prev.Tables) != len(e.tables) {
		return fmt.Errorf("prev snapshot table set does not match configured schemas")
	}
	seen := make(map[string]bool, len(prev.Tables))
	hashes := make(map[string]string, len(prev.Tables))
	for _, tm := range prev.Tables {
		if tm.TableID == "" || seen[tm.TableID] {
			return fmt.Errorf("empty or duplicate prev snapshot table %q", tm.TableID)
		}
		seen[tm.TableID] = true
		schema, ok := schemas[tm.TableID]
		switch {
		case ok || !e.dynamic:
			if !ok || schema.TableID != tm.TableID {
				return fmt.Errorf("prev snapshot table %q has no configured schema", tm.TableID)
			}
			if tm.SchemaHash != tableSchemaHash(e.NetworkID, schema) {
				return fmt.Errorf("table %q schema_hash mismatch", tm.TableID)
			}
		case tm.SchemaHash == "":
			return fmt.Errorf("prev snapshot table %q has no schema_hash", tm.TableID)
		}
		hashes[tm.TableID] = tm.SchemaHash
		partitions := map[string]bool{}
		for _, pc := range tm.PartitionRoots {
			if pc.TableID != tm.TableID || pc.PartitionID == "" || partitions[pc.PartitionID] {
				return fmt.Errorf("table %q invalid or duplicate partition %q", tm.TableID, pc.PartitionID)
			}
			partitions[pc.PartitionID] = true
		}
		parts := map[string]bool{}
		for _, p := range tm.ActiveParts {
			if p.TableID != tm.TableID || p.PartName == "" || p.PartitionID == "" || parts[p.PartName] {
				return fmt.Errorf("table %q invalid or duplicate part %q", tm.TableID, p.PartName)
			}
			parts[p.PartName] = true
		}
	}
	if prev.SchemaRoot != schemaRootFromHashes(hashes) {
		return fmt.Errorf("complete schema_root mismatch")
	}
	for i, b := range batches {
		if _, ok := schemas[b.TargetTableID]; !ok {
			return fmt.Errorf("statement %d (%s): unknown target table %q", i, b.StatementID, b.TargetTableID)
		}
		if nilRowSource(b.Rows) {
			return fmt.Errorf("statement %d (%s): row source is required", i, b.StatementID)
		}
	}
	return nil
}

func nilRowSource(s RowSource) bool {
	if s == nil {
		return true
	}
	v := reflect.ValueOf(s)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	}
	return false
}

func nonEOF(err error) error {
	if err == io.EOF {
		return nil
	}
	return err
}

// applyOwnedRows is only for the legacy adapter's own streams. In particular,
// ApplyRows must never invoke it on caller-owned streams.
func (e *Executor) applyOwnedRows(ctx context.Context, prev replay.SafeSnapshotManifest, job replay.ReplayJob, batches []StatementRows) (replay.SafeSnapshotManifest, replay.ExecutionResult, error) {
	next, result, err := e.ApplyRows(ctx, prev, job, batches)
	for i, b := range batches {
		if !nilRowSource(b.Rows) {
			if closeErr := b.Rows.Close(); closeErr != nil {
				err = errors.Join(err, fmt.Errorf("statement %d (%s): close rows: %w", i, b.StatementID, closeErr))
			}
		}
	}
	if err != nil {
		return replay.SafeSnapshotManifest{}, replay.ExecutionResult{}, err
	}
	return next, result, nil
}

// materializedRows adapts the legacy slice materializer lazily, retaining at
// most the current statement's output-sized slice. That existing materializer
// contract is unchanged; the new direct RowSource path needs no such buffer.
type materializedRows struct {
	materializer    Materializer
	schema          TableSchema
	statement       replay.PreparedStatement
	rows            []Row
	index           int
	started, closed bool
}

func (s *materializedRows) Next(ctx context.Context) (Row, error) {
	if s.closed {
		return Row{}, fmt.Errorf("materialized rows closed")
	}
	if !s.started {
		s.started = true
		if s.statement.PayloadRef == "" {
			return Row{}, fmt.Errorf("MVP executor only replays payload-local INSERTs; statement has no payload (mutation/DDL class)")
		}
		var err error
		s.rows, err = s.materializer.Materialize(ctx, s.schema, s.statement)
		if err != nil {
			// A materializer's io.EOF is a failure, not this adapter's own
			// successful end of stream. Keep the legacy error text intact.
			return Row{}, fmt.Errorf("%w", err)
		}
	}
	if s.index == len(s.rows) {
		s.rows = nil
		s.index = 0
		return Row{}, io.EOF
	}
	r := s.rows[s.index]
	s.index++
	return r, nil
}

func (s *materializedRows) Close() error {
	s.rows = nil
	s.closed = true
	return nil
}

// ApplyRows appends ordered row streams to a complete predecessor ledger. It
// consumes each stream through exact io.EOF and returns no partial result on
// failure. It never closes borrowed sources. Row IDs and RawBytes are supplied
// by the producer; they are not regenerated here.
//
// Memory is proportional to existing ledger metadata and new statement/partition
// parts, plus one row and one LtHash accumulator per current partition. It is
// not bounded independently of partition cardinality or individual row size.
func (e *Executor) ApplyRows(ctx context.Context, prev replay.SafeSnapshotManifest, job replay.ReplayJob, batches []StatementRows) (replay.SafeSnapshotManifest, replay.ExecutionResult, error) {
	if err := ctx.Err(); err != nil {
		return replay.SafeSnapshotManifest{}, replay.ExecutionResult{}, err
	}
	if err := verifyLedger(prev); err != nil {
		return replay.SafeSnapshotManifest{}, replay.ExecutionResult{}, fmt.Errorf("prev snapshot ledger: %w", err)
	}
	// Defense-in-depth: statement_id uniqueness is safety-critical (a reused id
	// collides _hg_row_id and resurrects the duplicate-row LtHash cancellation
	// attack, §5.2). Re-enforce it here so the executor fails closed even when
	// driven directly (e.g. snapshot promotion) rather than via the Verifier.
	if err := validateBlockStatements(batches); err != nil {
		return replay.SafeSnapshotManifest{}, replay.ExecutionResult{}, err
	}
	transition := job.TableSetTransition
	if transition != nil {
		if len(batches) != 0 {
			return replay.SafeSnapshotManifest{}, replay.ExecutionResult{}, fmt.Errorf("table_set_transition job must carry no statements, got %d", len(batches))
		}
		if err := transition.Validate(); err != nil {
			return replay.SafeSnapshotManifest{}, replay.ExecutionResult{}, err
		}
	}
	schemas, err := e.schemasForJob(job)
	if err != nil {
		return replay.SafeSnapshotManifest{}, replay.ExecutionResult{}, err
	}
	if err := e.validateAppendInputs(prev, batches, schemas); err != nil {
		return replay.SafeSnapshotManifest{}, replay.ExecutionResult{}, err
	}

	// parts[tableID][partitionID] = active parts (carried forward + new).
	parts := map[string]map[string][]replay.PartManifestEntry{}
	schemaHashes := map[string]string{}
	for _, tm := range prev.Tables {
		schemaHashes[tm.TableID] = tm.SchemaHash
		byPartition := map[string][]replay.PartManifestEntry{}
		for _, pc := range tm.PartitionRoots {
			byPartition[pc.PartitionID] = nil
		}
		for _, p := range tm.ActiveParts {
			byPartition[p.PartitionID] = append(byPartition[p.PartitionID], p)
		}
		parts[tm.TableID] = byPartition
	}
	nextSchemaRoot := prev.SchemaRoot
	if transition != nil {
		if nextSchemaRoot, err = e.applyTableSetTransition(*transition, schemas, parts, schemaHashes); err != nil {
			return replay.SafeSnapshotManifest{}, replay.ExecutionResult{}, err
		}
	}

	touchedSet := map[tablePartition]struct{}{}
	var affected []replay.PartManifestEntry

	for i, st := range batches {
		newParts, err := buildParts(ctx, schemas[st.TargetTableID], job.BlockSeq, st.StatementSeq, st.Rows)
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
		SchemaRoot:        nextSchemaRoot,
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
		ReplayLogHash:             replayLogHash(batches, affected),
	}
	if err := ctx.Err(); err != nil {
		return replay.SafeSnapshotManifest{}, replay.ExecutionResult{}, err
	}
	return next, result, nil
}

// applyTableSetTransition removes retired tables from, and adds empty added
// tables to, the ledger being rebuilt, and returns the resulting schema root.
// It refuses a transition that does not fit prev (a retire of an absent table,
// an add of a present one) or whose declared new_schema_root differs from the
// root this executor derives: either means the job does not describe the
// block the arbiter sealed, which is a local refusal to attest.
func (e *Executor) applyTableSetTransition(t replay.ReplayTableSetTransition, schemas map[string]TableSchema, parts map[string]map[string][]replay.PartManifestEntry, schemaHashes map[string]string) (string, error) {
	for _, id := range t.Retires {
		if parts[id] == nil {
			return "", fmt.Errorf("table_set_transition retires table %q, which is not in the previous safe snapshot", id)
		}
		delete(parts, id)
		delete(schemaHashes, id)
	}
	for _, add := range t.Adds {
		if parts[add.TableID] != nil {
			return "", fmt.Errorf("table_set_transition adds table %q, which is already in the previous safe snapshot", add.TableID)
		}
		parts[add.TableID] = map[string][]replay.PartManifestEntry{}
		schemaHashes[add.TableID] = tableSchemaHash(e.NetworkID, schemas[add.TableID])
	}
	root := schemaRootFromHashes(schemaHashes)
	if root != t.NewSchemaRoot {
		return "", fmt.Errorf("table_set_transition new_schema_root mismatch: job %s, derived %s", t.NewSchemaRoot, root)
	}
	return root, nil
}

// validateBlockStatements rejects duplicate statement_id and non-strictly-
// increasing statement_seq within a block.
func validateBlockStatements(stmts []StatementRows) error {
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
func buildParts(ctx context.Context, schema TableSchema, blockSeq, statementSeq uint64, rows RowSource) ([]replay.PartManifestEntry, error) {
	type partAgg struct {
		acc      *lthash.Hash
		rowCount uint64
		bytes    uint64
	}
	byPartition := map[string]*partAgg{}
	var partitionOrder []string

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		r, err := rows.Next(ctx)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, errors.Join(ctxErr, nonEOF(err))
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if len(r.RowID) != 32 {
			return nil, fmt.Errorf("row_id has %d bytes, want 32", len(r.RowID))
		}
		if r.PartitionID == "" {
			return nil, fmt.Errorf("row partition_id is required")
		}
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
		if agg.rowCount == math.MaxUint64 || r.RawBytes > math.MaxUint64-agg.bytes {
			return nil, fmt.Errorf("partition %q row or byte counter overflow", r.PartitionID)
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
