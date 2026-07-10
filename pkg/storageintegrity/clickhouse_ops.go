package storageintegrity

import (
	"context"
	"encoding/hex"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"housegate/housegate/pkg/lthash"
	"housegate/housegate/pkg/replay"
)

type SQLConn interface {
	Exec(ctx context.Context, query string, args ...any) error
}

type TableHash struct {
	StateRoot string
	Parts     []ByteSidePart
}

type TableHasher interface {
	HashTable(ctx context.Context, table string, partitions []string) (TableHash, error)
}

type tableIDAwareTableHasher interface {
	HashTableWithTableID(ctx context.Context, table string, partitions []string, tableID string) (TableHash, error)
}

type ActivePartReader interface {
	ReadActiveParts(ctx context.Context, table string, partitions []string) ([]replay.PartManifestEntry, error)
}

type tableIDAwareActivePartReader interface {
	ReadActivePartsWithTableID(ctx context.Context, table string, partitions []string, tableID string) ([]replay.PartManifestEntry, error)
}

type PartitionRootReader interface {
	CurrentPartitionRoot(ctx context.Context, table, partitionID string) (string, error)
}

type tableIDAwarePartitionRootReader interface {
	CurrentPartitionRootWithTableID(ctx context.Context, table, partitionID, tableID string) (string, error)
}

type PromotionSeqStore interface {
	LastPromotionSeq(ctx context.Context, table, partitionID string) (uint64, error)
	RecordPromotionSeq(ctx context.Context, table, partitionID string, seq uint64) error
}

type HashQueryConn interface {
	Query(ctx context.Context, query string, args ...any) (HashRows, error)
}

// KeyColumnProvider resolves the partition/order/primary key columns of a
// protected table so bounded mutations can reject any UPDATE that touches them
// without relying on hand-maintained config (spec §7.1, gap-34). tableID is the
// logical table identifier (e.g. "db.table"); an empty result means the table
// is unknown or has no key columns.
type KeyColumnProvider interface {
	KeyColumns(ctx context.Context, tableID string) ([]string, error)
}

// ClickHouseKeyColumnProvider derives key columns from system.columns of the
// physical safe table. Database/Table are resolved from the logical tableID via
// Resolve (typically qualifiedTable against the safe database).
type ClickHouseKeyColumnProvider struct {
	Conn HashQueryConn
	// Resolve maps a logical tableID to the physical (database, table) whose
	// schema carries the key columns. Required.
	Resolve func(tableID string) (database, table string)
}

func (p ClickHouseKeyColumnProvider) KeyColumns(ctx context.Context, tableID string) ([]string, error) {
	if p.Conn == nil || p.Resolve == nil {
		return nil, fmt.Errorf("clickhouse key-column provider is not fully configured")
	}
	database, table := p.Resolve(tableID)
	if database == "" || table == "" {
		return nil, nil
	}
	rows, err := p.Conn.Query(ctx, fmt.Sprintf(
		"SELECT name FROM system.columns WHERE database = %s AND table = %s "+
			"AND (is_in_partition_key OR is_in_sorting_key OR is_in_primary_key) ORDER BY name",
		sqlStringLiteral(database), sqlStringLiteral(table),
	))
	if err != nil {
		return nil, fmt.Errorf("query key columns for %s.%s: %w", database, table, err)
	}
	defer rows.Close()
	var cols []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan key column for %s.%s: %w", database, table, err)
		}
		if name != "" {
			cols = append(cols, name)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("key column rows for %s.%s: %w", database, table, err)
	}
	return cols, nil
}

type HashRows interface {
	Next() bool
	Scan(dest ...any) error
	Columns() []string
	ColumnTypes() []HashColumnType
	Close() error
	Err() error
}

type HashColumnType interface {
	DatabaseTypeName() string
}

type ClickHouseTableHasher struct {
	Conn    HashQueryConn
	TableID string
}

func (h ClickHouseTableHasher) HashTableWithTableID(ctx context.Context, table string, partitions []string, tableID string) (TableHash, error) {
	if tableID != "" {
		h.TableID = tableID
	}
	return h.HashTable(ctx, table, partitions)
}

func (h ClickHouseTableHasher) HashTable(ctx context.Context, table string, partitions []string) (TableHash, error) {
	if h.Conn == nil {
		return TableHash{}, fmt.Errorf("clickhouse query connection is required")
	}
	tableID := h.TableID
	if tableID == "" {
		tableID = normalizeTableID(table)
	}
	query := "SELECT _partition_id, * FROM " + table
	if len(partitions) > 0 {
		query += " WHERE _partition_id IN (" + sqlStringList(partitions) + ")"
	}
	rows, err := h.Conn.Query(ctx, query)
	if err != nil {
		return TableHash{}, fmt.Errorf("query table rows: %w", err)
	}
	defer rows.Close()

	names := rows.Columns()
	types := rows.ColumnTypes()
	if len(names) != len(types) {
		return TableHash{}, fmt.Errorf("column metadata mismatch: %d names %d types", len(names), len(types))
	}
	if len(names) == 0 || names[0] != "_partition_id" {
		return TableHash{}, fmt.Errorf("table hash query must return _partition_id as first column")
	}
	hashCols := make([]lthash.Column, 0, len(names)-1)
	for i := 1; i < len(names); i++ {
		hashCols = append(hashCols, lthash.Column{Name: names[i], Type: types[i].DatabaseTypeName()})
	}

	total := lthash.New()
	byPartition := map[string]*partitionHash{}
	for rows.Next() {
		values, err := scanHashRowValues(rows, types)
		if err != nil {
			return TableHash{}, fmt.Errorf("scan table row: %w", err)
		}
		partitionID := fmt.Sprint(values[0])
		rowValues := make([]any, 0, len(values)-1)
		for i := 1; i < len(values); i++ {
			v, err := normalizeScannedValue(types[i].DatabaseTypeName(), values[i])
			if err != nil {
				return TableHash{}, fmt.Errorf("column %s: %w", names[i], err)
			}
			rowValues = append(rowValues, v)
		}
		rowHash, err := lthash.RowHash(tableID, hashCols, rowValues)
		if err != nil {
			return TableHash{}, err
		}
		total.AddHash(rowHash)
		ph := byPartition[partitionID]
		if ph == nil {
			ph = &partitionHash{acc: lthash.New()}
			byPartition[partitionID] = ph
		}
		ph.acc.AddHash(rowHash)
		ph.rows++
	}
	if err := rows.Err(); err != nil {
		return TableHash{}, fmt.Errorf("table rows: %w", err)
	}
	ids := make([]string, 0, len(byPartition))
	for id := range byPartition {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	parts := make([]ByteSidePart, 0, len(ids))
	for _, id := range ids {
		ph := byPartition[id]
		// Raw additive accumulator (gap-14), not the BLAKE3 digest: part roots
		// must be summable for the compaction/mutation ledger equation.
		partHash := lthashAccumulatorHex(ph.acc)
		parts = append(parts, ByteSidePart{
			PartitionID:   id,
			PartName:      "hash-scan-" + unsafeIdentChars.ReplaceAllString(id, "_"),
			RowCount:      ph.rows,
			PartRowLtHash: partHash,
		})
	}
	// StateRoot stays a digest: it is a whole-table commitment that is never
	// summed with another root, so a compact 32-byte digest is correct here.
	return TableHash{StateRoot: total.Hex(), Parts: parts}, nil
}

type partitionHash struct {
	acc  *lthash.Hash
	rows uint64
}

type ClickHouseActivePartReader struct {
	Conn    HashQueryConn
	TableID string
}

func (r ClickHouseActivePartReader) ReadActivePartsWithTableID(ctx context.Context, table string, partitions []string, tableID string) ([]replay.PartManifestEntry, error) {
	if tableID != "" {
		r.TableID = tableID
	}
	return r.ReadActiveParts(ctx, table, partitions)
}

func (r ClickHouseActivePartReader) ReadActiveParts(ctx context.Context, table string, partitions []string) ([]replay.PartManifestEntry, error) {
	where := ""
	if len(partitions) > 0 {
		where = " WHERE _partition_id IN (" + sqlStringList(partitions) + ")"
	}
	return r.foldParts(ctx, table, where)
}

// ReadNamedParts recomputes the per-part row LtHash for exactly the named
// physical parts, using the SAME row fold as ReadActiveParts (`_part IN (…)`
// scoped) so the emitted PartRowLtHash is byte-identical to a full-partition
// read of the same parts. It is the cache-miss scanner for the part-LtHash
// fast path: only the parts that missed the cache are scanned. An empty
// partNames set is an error (callers must not scan the whole table by
// accident).
func (r ClickHouseActivePartReader) ReadNamedParts(ctx context.Context, table string, partNames []string) ([]replay.PartManifestEntry, error) {
	if len(partNames) == 0 {
		return nil, fmt.Errorf("ReadNamedParts requires at least one part name")
	}
	where := " WHERE _part IN (" + sqlStringList(partNames) + ")"
	return r.foldParts(ctx, table, where)
}

func (r ClickHouseActivePartReader) ReadNamedPartsWithTableID(ctx context.Context, table string, partNames []string, tableID string) ([]replay.PartManifestEntry, error) {
	if tableID != "" {
		r.TableID = tableID
	}
	return r.ReadNamedParts(ctx, table, partNames)
}

// foldParts is the shared scan+fold used by ReadActiveParts and ReadNamedParts.
// The fold (row values → lthash.RowHash(tableID, cols, values), summed per
// physical _part into a raw additive accumulator) is intentionally identical
// across both entry points so a subset scan and a full scan produce the same
// PartRowLtHash for a given part — the invariant the part-LtHash cache relies
// on. whereClause is appended verbatim after the base SELECT.
func (r ClickHouseActivePartReader) foldParts(ctx context.Context, table, whereClause string) ([]replay.PartManifestEntry, error) {
	if r.Conn == nil {
		return nil, fmt.Errorf("clickhouse query connection is required")
	}
	tableID := r.TableID
	if tableID == "" {
		tableID = normalizeTableID(table)
	}
	query := "SELECT _partition_id, _part, * FROM " + table + whereClause
	rows, err := r.Conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query active part rows: %w", err)
	}
	defer rows.Close()

	names := rows.Columns()
	types := rows.ColumnTypes()
	if len(names) != len(types) {
		return nil, fmt.Errorf("column metadata mismatch: %d names %d types", len(names), len(types))
	}
	if len(names) < 2 || names[0] != "_partition_id" || names[1] != "_part" {
		return nil, fmt.Errorf("active part query must return _partition_id and _part as first columns")
	}
	hashCols := make([]lthash.Column, 0, len(names)-2)
	for i := 2; i < len(names); i++ {
		hashCols = append(hashCols, lthash.Column{Name: names[i], Type: types[i].DatabaseTypeName()})
	}

	byPart := map[string]*activePartHash{}
	for rows.Next() {
		values, err := scanHashRowValues(rows, types)
		if err != nil {
			return nil, fmt.Errorf("scan active part row: %w", err)
		}
		partitionID := scannedString(values[0])
		partName := scannedString(values[1])
		rowValues := make([]any, 0, len(values)-2)
		for i := 2; i < len(values); i++ {
			v, err := normalizeScannedValue(types[i].DatabaseTypeName(), values[i])
			if err != nil {
				return nil, fmt.Errorf("column %s: %w", names[i], err)
			}
			rowValues = append(rowValues, v)
		}
		rowHash, err := lthash.RowHash(tableID, hashCols, rowValues)
		if err != nil {
			return nil, err
		}
		key := partitionID + "\x00" + partName
		ph := byPart[key]
		if ph == nil {
			ph = &activePartHash{partitionID: partitionID, partName: partName, acc: lthash.New()}
			byPart[key] = ph
		}
		ph.acc.AddHash(rowHash)
		ph.rows++
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("active part rows: %w", err)
	}

	keys := make([]string, 0, len(byPart))
	for key := range byPart {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]replay.PartManifestEntry, 0, len(keys))
	for _, key := range keys {
		ph := byPart[key]
		parts = append(parts, replay.PartManifestEntry{
			TableID:     tableID,
			PartitionID: ph.partitionID,
			PartName:    ph.partName,
			// Raw additive accumulator (gap-14) so part roots are summable.
			PartRowLtHash: lthashAccumulatorHex(ph.acc),
			RowCount:      ph.rows,
		})
	}
	return parts, nil
}

type activePartHash struct {
	partitionID string
	partName    string
	acc         *lthash.Hash
	rows        uint64
}

type ClickHousePromoter struct {
	Conn             SQLConn
	ActiveParts      ActivePartReader
	PartitionRoots   PartitionRootReader
	PromotionSeqs    PromotionSeqStore
	PromoteDatabase  string
	CleanupUnsafe    bool
	DropPromoteTable bool
	// StrictVerification forces a full readback of the shadow's active parts for
	// the post-root CAS. When false (default) the CAS uses the arithmetic
	// expected post root (base + sum of verified candidate part LtHashes),
	// avoiding the readback scan. Both compare against the arbiter-pinned
	// ExpectedPostRoots and fail closed before REPLACE.
	StrictVerification bool
	// RequireBaseRootCAS and RequirePromotionSeq are protected-mode switches
	// (HG-P0-04): when set, every promotion must carry a promotion sequence and a
	// verifiable per-partition base root, regardless of the per-task Require*
	// flags. A protected deployment cannot be handed a task that silently skips
	// the CAS or sequence store.
	RequireBaseRootCAS  bool
	RequirePromotionSeq bool
}

func (p ClickHousePromoter) Promote(ctx context.Context, task PromotionTask) (PromotionResult, error) {
	if p.Conn == nil {
		return PromotionResult{}, fmt.Errorf("clickhouse connection is required")
	}
	if task.SafeTable == "" {
		return PromotionResult{}, fmt.Errorf("promotion safe_table is required")
	}
	// A mutation post-state can only be published via REPLACE PARTITION (from
	// scratch) or an internal signed DROP PARTITION. ATTACH PARTITION cannot
	// remove or rewrite the pre-existing rows, so it must never express a
	// mutation post-state (spec §10).
	if task.Kind == "mutation" && !task.ReplacePartition && !task.InternalDropPartition {
		return PromotionResult{}, fmt.Errorf("mutation promotion must use replace_partition or internal_drop_partition, not attach")
	}
	if task.InternalDropPartition && len(task.PartitionIDs) == 0 {
		task.PartitionIDs = append([]string(nil), task.DropPartitionIDs...)
	}
	if err := p.checkPromotionPreconditions(ctx, task); err != nil {
		return PromotionResult{}, err
	}
	if task.InternalDropPartition {
		return p.dropSafePartitions(ctx, task)
	}
	if p.shouldUsePromoteShadow(task) {
		return p.promoteViaShadow(ctx, task)
	}
	if task.ReplacePartition {
		if task.SourceTable == "" {
			return PromotionResult{}, fmt.Errorf("replace partition promotion source_table is required")
		}
		if len(task.PartitionIDs) == 0 {
			return PromotionResult{}, fmt.Errorf("replace partition promotion requires partition_ids")
		}
		for _, partitionID := range task.PartitionIDs {
			sql := fmt.Sprintf("ALTER TABLE %s REPLACE PARTITION ID %s FROM %s", task.SafeTable, sqlStringLiteral(partitionID), task.SourceTable)
			if err := p.Conn.Exec(ctx, sql); err != nil {
				return PromotionResult{}, fmt.Errorf("replace partition %s: %w", partitionID, err)
			}
		}
		return p.promotionResult(ctx, task)
	}
	if task.UnsafeTable == "" {
		return PromotionResult{}, fmt.Errorf("insert promotion unsafe_table is required")
	}
	if len(task.PartitionIDs) == 0 {
		// Fail closed: never reconstruct the promotion set by scanning the
		// whole unsafe table. partition_ids must come from statement / part
		// registry metadata (spec §6.3).
		return PromotionResult{}, fmt.Errorf("insert promotion requires partition_ids")
	}
	for _, partitionID := range task.PartitionIDs {
		sql := fmt.Sprintf("ALTER TABLE %s ATTACH PARTITION ID %s FROM %s", task.SafeTable, sqlStringLiteral(partitionID), task.UnsafeTable)
		if err := p.Conn.Exec(ctx, sql); err != nil {
			return PromotionResult{}, fmt.Errorf("attach partition %s: %w", partitionID, err)
		}
	}
	return p.promotionResult(ctx, task)
}

func (p ClickHousePromoter) dropSafePartitions(ctx context.Context, task PromotionTask) (PromotionResult, error) {
	partitionIDs := task.DropPartitionIDs
	if len(partitionIDs) == 0 {
		partitionIDs = task.PartitionIDs
	}
	if len(partitionIDs) == 0 {
		return PromotionResult{}, fmt.Errorf("internal drop-partition promotion requires drop_partition_ids")
	}
	if len(task.PartitionIDs) == 0 {
		task.PartitionIDs = append([]string(nil), partitionIDs...)
	}
	for _, partitionID := range partitionIDs {
		if err := p.Conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s DROP PARTITION ID %s", task.SafeTable, sqlStringLiteral(partitionID))); err != nil {
			return PromotionResult{}, fmt.Errorf("drop safe partition %s: %w", partitionID, err)
		}
		if task.PromotionSeq != 0 && p.PromotionSeqs != nil {
			if err := p.PromotionSeqs.RecordPromotionSeq(ctx, task.SafeTable, partitionID, task.PromotionSeq); err != nil {
				return PromotionResult{}, fmt.Errorf("record promotion_seq for partition %s: %w", partitionID, err)
			}
		}
	}
	return p.promotionResult(ctx, task)
}

func (p ClickHousePromoter) checkPromotionPreconditions(ctx context.Context, task PromotionTask) error {
	if err := validateBatchPromotion(task); err != nil {
		return err
	}
	// HG-P0-04: protected mode forces the sequence + base-root CAS regardless of
	// the per-task Require* flags, so a mock/arbiter cannot hand a protected
	// worker a task that silently skips them.
	requirePromotionSeq := task.RequirePromotionSeq || p.RequirePromotionSeq
	requireBaseRootCAS := task.RequireBaseRootCAS || p.RequireBaseRootCAS
	if requirePromotionSeq {
		if task.PromotionSeq == 0 {
			return fmt.Errorf("promotion_seq is required")
		}
		if p.PromotionSeqs == nil {
			return fmt.Errorf("promotion_seq store is required")
		}
	}
	if task.PromotionSeq != 0 && p.PromotionSeqs != nil {
		for _, partitionID := range task.PartitionIDs {
			last, err := p.PromotionSeqs.LastPromotionSeq(ctx, task.SafeTable, partitionID)
			if err != nil {
				return fmt.Errorf("read promotion_seq watermark for partition %s: %w", partitionID, err)
			}
			if task.PromotionSeq <= last {
				return fmt.Errorf("stale promotion_seq %d for partition %s: last applied %d", task.PromotionSeq, partitionID, last)
			}
		}
	}
	if requireBaseRootCAS || task.BasePartitionRoot != "" || len(task.BasePartitionRoots) > 0 {
		if p.PartitionRoots == nil {
			return fmt.Errorf("partition root reader is required for base-root CAS")
		}
		for _, partitionID := range task.PartitionIDs {
			// HG-P0-04: CAS each partition against ITS OWN base root, never one
			// scalar base attributed to every partition.
			base, ok := promotionBasePartitionRoot(task, partitionID)
			if !ok {
				// A zero base is only acceptable for an explicit genesis-empty
				// INSERT partition; when the CAS is required we must have a real
				// base to compare, so fail closed on an unresolved partition.
				if requireBaseRootCAS && !isInsertPromotionKind(task.Kind) {
					return fmt.Errorf("no base partition root declared for partition %s under required base-root CAS", partitionID)
				}
			}
			current, err := currentPartitionRootWithTableID(ctx, p.PartitionRoots, task.SafeTable, partitionID, task.TableID)
			if err != nil {
				return fmt.Errorf("read current partition root %s: %w", partitionID, err)
			}
			if current != base {
				return fmt.Errorf("base partition root mismatch for partition %s: got %s want %s", partitionID, current, base)
			}
		}
	}
	return nil
}

func (p ClickHousePromoter) shouldUsePromoteShadow(task PromotionTask) bool {
	return firstNonEmptyString(task.PromoteDatabase, p.PromoteDatabase) != ""
}

func (p ClickHousePromoter) promoteViaShadow(ctx context.Context, task PromotionTask) (PromotionResult, error) {
	sourceTable := firstNonEmptyString(task.SourceTable, task.UnsafeTable)
	if sourceTable == "" {
		return PromotionResult{}, fmt.Errorf("promotion source table is required")
	}
	if len(task.PartitionIDs) == 0 {
		return PromotionResult{}, fmt.Errorf("promotion requires partition_ids")
	}
	promoteDB := firstNonEmptyString(task.PromoteDatabase, p.PromoteDatabase)
	if err := p.Conn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+quoteIdent(promoteDB)); err != nil {
		return PromotionResult{}, fmt.Errorf("create promote database: %w", err)
	}
	// gap-26a: one copy-on-write shadow table per partition,
	// `hg_promote.<promotion_id>__<partition_id>`, so same-table/different-
	// partition promotions never contend on a shared shadow table.
	shadowByPartition := make(map[string]string, len(task.PartitionIDs))
	for _, partitionID := range task.PartitionIDs {
		shadow := qualifiedTable(promoteDB, promoteShadowTableName(task.PromotionID, partitionID))
		shadowByPartition[partitionID] = shadow
		if err := p.Conn.Exec(ctx, "DROP TABLE IF EXISTS "+shadow); err != nil {
			return PromotionResult{}, fmt.Errorf("drop stale promote table: %w", err)
		}
		if err := p.Conn.Exec(ctx, "CREATE TABLE "+shadow+" AS "+task.SafeTable); err != nil {
			return PromotionResult{}, fmt.Errorf("create promote table: %w", err)
		}
		if shouldAttachBasePartition(task) {
			if err := p.Conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s ATTACH PARTITION ID %s FROM %s", shadow, sqlStringLiteral(partitionID), task.SafeTable)); err != nil {
				return PromotionResult{}, fmt.Errorf("attach base partition %s: %w", partitionID, err)
			}
		}
		if err := p.verifySourceCandidateParts(ctx, sourceTable, partitionID, task); err != nil {
			return PromotionResult{}, err
		}
		if err := p.attachCandidateParts(ctx, shadow, sourceTable, partitionID, task.CandidateParts); err != nil {
			return PromotionResult{}, err
		}
	}
	if task.RequirePostRootCAS {
		if p.StrictVerification && p.ActiveParts == nil {
			return PromotionResult{}, fmt.Errorf("active part reader is required for strict post-root CAS")
		}
		for _, partitionID := range task.PartitionIDs {
			want, ok := expectedPostRootForPartition(task, partitionID)
			if !ok {
				return PromotionResult{}, fmt.Errorf("post partition root CAS is required but no expected root for partition %s", partitionID)
			}
			got, err := p.postRootForCAS(ctx, task, shadowByPartition[partitionID], partitionID)
			if err != nil {
				return PromotionResult{}, err
			}
			if got != want {
				return PromotionResult{}, fmt.Errorf("post partition root mismatch for partition %s: got %s want %s", partitionID, got, want)
			}
		}
	}
	for _, partitionID := range task.PartitionIDs {
		shadow := shadowByPartition[partitionID]
		if err := p.Conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s REPLACE PARTITION ID %s FROM %s", task.SafeTable, sqlStringLiteral(partitionID), shadow)); err != nil {
			return PromotionResult{}, fmt.Errorf("replace safe partition %s: %w", partitionID, err)
		}
		if task.PromotionSeq != 0 && p.PromotionSeqs != nil {
			if err := p.PromotionSeqs.RecordPromotionSeq(ctx, task.SafeTable, partitionID, task.PromotionSeq); err != nil {
				return PromotionResult{}, fmt.Errorf("record promotion_seq for partition %s: %w", partitionID, err)
			}
		}
	}
	result, err := p.promotionResult(ctx, task)
	if err != nil {
		return PromotionResult{}, err
	}
	cleanupParts := task.CleanupUnsafeParts
	if len(cleanupParts) == 0 && (task.CleanupUnsafe || p.CleanupUnsafe) {
		cleanupParts = task.CandidateParts
	}
	if len(cleanupParts) > 0 {
		if err := p.cleanupUnsafeParts(ctx, firstNonEmptyString(task.UnsafeTable, task.SourceTable), cleanupParts); err != nil {
			return PromotionResult{}, err
		}
		result.CleanupUnsafeParts = append([]ByteSidePart(nil), cleanupParts...)
	}
	if task.PromoteDatabase != "" || p.DropPromoteTable {
		for _, partitionID := range task.PartitionIDs {
			if err := p.Conn.Exec(ctx, "DROP TABLE IF EXISTS "+shadowByPartition[partitionID]); err != nil {
				return PromotionResult{}, fmt.Errorf("drop promote table: %w", err)
			}
		}
	}
	return result, nil
}

func (p ClickHousePromoter) verifySourceCandidateParts(ctx context.Context, sourceTable, partitionID string, task PromotionTask) error {
	if !isInsertPromotionKind(task.Kind) {
		return nil
	}
	// HG-P0-02: an INSERT promotion MUST carry the source-declared candidate
	// parts and be verifiable against the physical unsafe parts before any
	// ATTACH/REPLACE. Empty candidates or a missing active-part reader mean the
	// exact-set cannot be proved, so fail closed rather than attaching whatever
	// active parts happen to be in the unsafe partition.
	if len(task.CandidateParts) == 0 {
		return fmt.Errorf("insert promotion %s has no source candidate parts; refusing to publish an unverified part set", task.PromotionID)
	}
	if p.ActiveParts == nil {
		return fmt.Errorf("insert promotion %s requires an active-part reader to verify source candidate parts", task.PromotionID)
	}
	// HG-P0-02: when the source declared partition-agnostic candidates (the "all"
	// sentinel, because the proxy cannot see ClickHouse's physical partition
	// ids), scope the scan to the source partition being promoted but match by
	// whole-set content — the candidate's total additive root binds the scanned
	// bytes regardless of which physical partition they landed in. Otherwise the
	// source named a real partition and we match that partition's candidates.
	var candidates []ByteSidePart
	if candidatePartsArePartitionAgnostic(task.CandidateParts) {
		candidates = task.CandidateParts
	} else {
		candidates = candidatePartsForPartition(task, partitionID)
		if len(candidates) == 0 {
			// The task declares real per-partition candidates but none fall in this
			// partition: it is not part of the source claim, so fail closed rather
			// than attaching unverified parts.
			return fmt.Errorf("insert promotion %s declares no candidate parts for partition %s", task.PromotionID, partitionID)
		}
	}
	entries, err := readActivePartsWithTableID(ctx, p.ActiveParts, sourceTable, []string{partitionID}, task.TableID)
	if err != nil {
		return fmt.Errorf("read source candidate parts for partition %s: %w", partitionID, err)
	}
	if err := enforceCandidatePartSet(activePartEntriesToByteSideParts(entries), candidates); err != nil {
		return fmt.Errorf("verify source candidate parts for partition %s: %w", partitionID, err)
	}
	return nil
}

func isInsertPromotionKind(kind string) bool {
	return kind == "" || strings.EqualFold(kind, "insert")
}

// attachCandidateParts installs the verified candidate partition into the
// per-partition shadow table via a byte-preserving ATTACH PARTITION ... FROM
// hardlink (spec §6.3, gap-26b). See the inline comment for why this is byte-
// exact and partition-granular rather than a per-part operation.
func (p ClickHousePromoter) attachCandidateParts(ctx context.Context, shadow, sourceTable, partitionID string, _ []ByteSidePart) error {
	// Hardlink the verified candidate partition from the unsafe source into the
	// shadow with ATTACH PARTITION ID ... FROM (spec §6.3, gap-26b): ClickHouse
	// implements this as a metadata/hardlink move that preserves the parts'
	// physical bytes — and thus the part_row_lthash the byte-side check verified —
	// exactly, unlike an INSERT ... SELECT re-materialization. It operates at
	// partition granularity (stock ClickHouse has no per-part cross-table hardlink
	// statement — MOVE PART has only TO DISK/VOLUME, not TO TABLE); this is byte-
	// exact because the frozen unsafe buffer holds exactly this partition's
	// verified candidate parts, and the post-root CAS still gates the result. The
	// source stays intact until REPLACE, so a CAS abort needs no rollback.
	if err := p.Conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s ATTACH PARTITION ID %s FROM %s",
		shadow, sqlStringLiteral(partitionID), sourceTable)); err != nil {
		return fmt.Errorf("attach candidate partition %s: %w", partitionID, err)
	}
	return nil
}

// expectedPostRootForPartition resolves the expected post-promotion root for a
// partition. Per-partition ExpectedPostRoots take precedence; otherwise the
// scalar ExpectedPostRoot is used only when the promotion covers a single
// partition (so a multi-partition promotion can never CAS every partition
// against one partition's root — spec §6.3 / gap-20).
func expectedPostRootForPartition(task PromotionTask, partitionID string) (string, bool) {
	for _, pc := range task.ExpectedPostRoots {
		if pc.PartitionID == partitionID {
			return pc.Root, true
		}
	}
	if len(task.ExpectedPostRoots) > 0 {
		// A per-partition set was provided but this partition is missing from
		// it — treat as unresolved rather than silently falling back.
		return "", false
	}
	if len(task.PartitionIDs) == 1 && task.ExpectedPostRoot != "" {
		return task.ExpectedPostRoot, true
	}
	return "", false
}

// postRootForCAS returns the observed post-promotion partition root for the CAS.
// In strict mode it reads back the shadow's active parts (a full row scan). In
// the default fast mode it computes the arithmetic post root
// base_partition_root + sum(verified candidate part LtHashes for the partition):
// ATTACH PARTITION ... FROM is byte-preserving so the candidate parts keep their
// exact accumulators, and the additive sum is merge-invariant, so this equals
// the readback root of partitionRootFromActiveParts. Either result is CAS'd
// against the arbiter-pinned ExpectedPostRoots by the caller.
func (p ClickHousePromoter) postRootForCAS(ctx context.Context, task PromotionTask, shadow, partitionID string) (string, error) {
	if !p.StrictVerification && canArithmeticPostRoot(task, partitionID) {
		base := promotionBasePartitionRootOrZero(task, partitionID)
		candidateSum, err := sumCandidatePartsForPartition(task, partitionID)
		if err != nil {
			return "", fmt.Errorf("sum candidate parts for partition %s: %w", partitionID, err)
		}
		// post = base + Σ candidate. An empty base (genesis-empty partition) is the
		// zero accumulator, so post == candidateSum.
		root, err := sumPartRowLtHashes([]string{base, candidateSum})
		if err != nil {
			return "", fmt.Errorf("compute arithmetic post root for partition %s: %w", partitionID, err)
		}
		return root, nil
	}
	// Strict mode, or a case the arithmetic path cannot resolve exactly (a
	// multi-partition promotion carrying a non-empty scalar base that cannot be
	// attributed per partition): fall back to a full readback so the CAS stays
	// correct rather than failing a legitimate promotion.
	if p.ActiveParts == nil {
		return "", fmt.Errorf("active part reader is required for post-root CAS readback")
	}
	shadowParts, err := readActivePartsWithTableID(ctx, p.ActiveParts, shadow, []string{partitionID}, task.TableID)
	if err != nil {
		return "", fmt.Errorf("read promote active parts: %w", err)
	}
	return partitionRootFromActiveParts(shadowParts, partitionID), nil
}

// canArithmeticPostRoot reports whether the arithmetic post root can be computed
// exactly for this partition: the base must be resolvable (single-partition
// promotion, or a genesis-empty multi-partition promotion with no scalar base to
// misattribute). Otherwise the caller falls back to a readback.
func canArithmeticPostRoot(task PromotionTask, partitionID string) bool {
	if len(task.PartitionIDs) == 1 {
		return true
	}
	// Multi-partition: only safe when there is no scalar base to misattribute
	// (each partition's base is the zero accumulator / genesis-empty).
	return task.BasePartitionRoot == ""
}

// promotionBasePartitionRoot resolves the base partition root for the CAS. A
// per-partition BasePartitionRoots entry wins; otherwise the scalar
// BasePartitionRoot is used only for a single-partition promotion (HG-P0-04:
// never attribute one scalar base to every partition of a multi-partition
// task). The bool reports whether an explicit base was found; a false result
// means the caller must decide whether a zero base is permitted (only an
// explicit zero-base INSERT into an empty partition) or fail closed.
func promotionBasePartitionRoot(task PromotionTask, partitionID string) (string, bool) {
	for _, pc := range task.BasePartitionRoots {
		if pc.PartitionID == partitionID {
			return pc.Root, true
		}
	}
	if len(task.BasePartitionRoots) > 0 {
		// A per-partition set was provided but this partition is missing from it:
		// unresolved, not silently zero.
		return "", false
	}
	if len(task.PartitionIDs) == 1 && task.PartitionIDs[0] == partitionID && task.BasePartitionRoot != "" {
		return task.BasePartitionRoot, true
	}
	return lthashAccumulatorHex(lthash.New()), false
}

// promotionBasePartitionRootOrZero is the arithmetic-path helper: it returns the
// resolved base root, or the zero accumulator when no explicit base was found
// (a genesis-empty partition), so post = base + Σ candidate stays correct.
func promotionBasePartitionRootOrZero(task PromotionTask, partitionID string) string {
	root, _ := promotionBasePartitionRoot(task, partitionID)
	return root
}

// sumCandidatePartsForPartition returns the additive sum of the verified
// candidate part LtHashes for the given partition. CandidateParts with an empty
// PartitionID are attributed to a single-partition promotion's sole partition
// (the arbiter may omit the partition id when it is unambiguous).
func sumCandidatePartsForPartition(task PromotionTask, partitionID string) (string, error) {
	hashes := make([]string, 0, len(task.CandidateParts))
	for _, part := range candidatePartsForPartition(task, partitionID) {
		hashes = append(hashes, part.PartRowLtHash)
	}
	return sumPartRowLtHashes(hashes)
}

func candidatePartsForPartition(task PromotionTask, partitionID string) []ByteSidePart {
	// A partition-agnostic candidate set (all "all"/empty sentinels, declared by
	// a proxy source that cannot see physical partition ids, HG-P0-02) maps to
	// whichever single real partition is being promoted — the Native MVP writes
	// one partition per statement, so the whole set is that partition's content.
	if candidatePartsArePartitionAgnostic(task.CandidateParts) {
		return append([]ByteSidePart(nil), task.CandidateParts...)
	}
	singlePartition := len(task.PartitionIDs) == 1
	out := make([]ByteSidePart, 0, len(task.CandidateParts))
	for _, part := range task.CandidateParts {
		if part.PartitionID == partitionID || (part.PartitionID == "" && singlePartition) {
			out = append(out, part)
		}
	}
	return out
}

// validateBatchPromotion enforces the restricted-batch invariant (spec §13): a
// promotion carrying more than one statement id (a batch) may only cover a
// single table + single partition, and every candidate part must belong to that
// partition. Batching is a physical optimization that coalesces several
// verified statements landing in the same partition into one REPLACE PARTITION;
// a batch that spanned partitions or tables would break the per-partition CAS
// and the statement_ids -> snapshot mapping, so it fails closed here rather than
// promoting a malformed set. Single-statement promotions are unaffected.
func validateBatchPromotion(task PromotionTask) error {
	if len(task.StatementIDs) <= 1 {
		return nil
	}
	// The batch's partition set must be exactly one partition.
	partitions := map[string]struct{}{}
	for _, pid := range task.PartitionIDs {
		if pid != "" {
			partitions[pid] = struct{}{}
		}
	}
	for _, part := range task.CandidateParts {
		if part.PartitionID != "" {
			partitions[part.PartitionID] = struct{}{}
		}
	}
	if len(partitions) > 1 {
		ids := make([]string, 0, len(partitions))
		for pid := range partitions {
			ids = append(ids, pid)
		}
		sort.Strings(ids)
		return fmt.Errorf("batched promotion (%d statements) must cover a single partition, spans %v", len(task.StatementIDs), ids)
	}
	// Every candidate part must belong to the single batched partition.
	if len(task.PartitionIDs) == 1 {
		want := task.PartitionIDs[0]
		for _, part := range task.CandidateParts {
			if part.PartitionID != "" && part.PartitionID != want {
				return fmt.Errorf("batched promotion candidate part %q is in partition %q, not the batched partition %q", part.PartName, part.PartitionID, want)
			}
		}
	}
	return nil
}

func shouldAttachBasePartition(task PromotionTask) bool {
	if task.SkipBasePartitionAttach {
		return false
	}
	if task.Kind == "mutation" {
		return false
	}
	if task.ReplacePartition && task.SourceTable != "" && task.UnsafeTable == "" {
		return false
	}
	return true
}

func (p ClickHousePromoter) cleanupUnsafeParts(ctx context.Context, unsafeTable string, parts []ByteSidePart) error {
	if unsafeTable == "" {
		return fmt.Errorf("unsafe table is required for cleanup")
	}
	for _, part := range parts {
		if part.PartName == "" {
			continue
		}
		if err := p.Conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s DROP PART %s", unsafeTable, sqlStringLiteral(part.PartName))); err != nil {
			return fmt.Errorf("drop unsafe part %s: %w", part.PartName, err)
		}
	}
	return nil
}

func (p ClickHousePromoter) promotionResult(ctx context.Context, task PromotionTask) (PromotionResult, error) {
	result := PromotionResult{
		PromotionID:        task.PromotionID,
		PromotionSeq:       task.PromotionSeq,
		LeaseID:            task.LeaseID,
		TableID:            task.TableID,
		BaseSafeSnapshotID: task.BaseSafeSnapshotID,
		BasePartitionRoot:  task.BasePartitionRoot,
		SafeTable:          task.SafeTable,
		SourceTable:        firstNonEmptyString(task.SourceTable, task.UnsafeTable),
		PartitionIDs:       append([]string(nil), task.PartitionIDs...),
		StatementIDs:       append([]string(nil), task.StatementIDs...),
	}
	if p.ActiveParts == nil {
		return result, nil
	}
	parts, err := readActivePartsWithTableID(ctx, p.ActiveParts, task.SafeTable, task.PartitionIDs, task.TableID)
	if err != nil {
		return PromotionResult{}, fmt.Errorf("read promoted active parts: %w", err)
	}
	result.ActiveParts = parts
	return result, nil
}

type ActivePartPartitionRootReader struct {
	ActiveParts ActivePartReader
}

func (r ActivePartPartitionRootReader) CurrentPartitionRoot(ctx context.Context, table, partitionID string) (string, error) {
	if r.ActiveParts == nil {
		return "", fmt.Errorf("active part reader is required")
	}
	parts, err := r.ActiveParts.ReadActiveParts(ctx, table, []string{partitionID})
	if err != nil {
		return "", err
	}
	return partitionRootFromActiveParts(parts, partitionID), nil
}

func (r ActivePartPartitionRootReader) CurrentPartitionRootWithTableID(ctx context.Context, table, partitionID, tableID string) (string, error) {
	if r.ActiveParts == nil {
		return "", fmt.Errorf("active part reader is required")
	}
	parts, err := readActivePartsWithTableID(ctx, r.ActiveParts, table, []string{partitionID}, tableID)
	if err != nil {
		return "", err
	}
	return partitionRootFromActiveParts(parts, partitionID), nil
}

func hashTableWithTableID(ctx context.Context, hasher TableHasher, table string, partitions []string, tableID string) (TableHash, error) {
	if hasher == nil {
		return TableHash{}, fmt.Errorf("table hasher is required")
	}
	if tableID != "" {
		if aware, ok := hasher.(tableIDAwareTableHasher); ok {
			return aware.HashTableWithTableID(ctx, table, partitions, tableID)
		}
	}
	return hasher.HashTable(ctx, table, partitions)
}

func readActivePartsWithTableID(ctx context.Context, reader ActivePartReader, table string, partitions []string, tableID string) ([]replay.PartManifestEntry, error) {
	if reader == nil {
		return nil, fmt.Errorf("active part reader is required")
	}
	if tableID != "" {
		if aware, ok := reader.(tableIDAwareActivePartReader); ok {
			return aware.ReadActivePartsWithTableID(ctx, table, partitions, tableID)
		}
	}
	return reader.ReadActiveParts(ctx, table, partitions)
}

func currentPartitionRootWithTableID(ctx context.Context, reader PartitionRootReader, table, partitionID, tableID string) (string, error) {
	if reader == nil {
		return "", fmt.Errorf("partition root reader is required")
	}
	if tableID != "" {
		if aware, ok := reader.(tableIDAwarePartitionRootReader); ok {
			return aware.CurrentPartitionRootWithTableID(ctx, table, partitionID, tableID)
		}
	}
	return reader.CurrentPartitionRoot(ctx, table, partitionID)
}

type MemoryPromotionSeqStore struct {
	mu   sync.Mutex
	last map[string]uint64
}

func NewMemoryPromotionSeqStore() *MemoryPromotionSeqStore {
	return &MemoryPromotionSeqStore{last: map[string]uint64{}}
}

func (s *MemoryPromotionSeqStore) LastPromotionSeq(_ context.Context, table, partitionID string) (uint64, error) {
	if s == nil {
		return 0, fmt.Errorf("promotion_seq store is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.last[table+"\x00"+partitionID], nil
}

func (s *MemoryPromotionSeqStore) RecordPromotionSeq(_ context.Context, table, partitionID string, seq uint64) error {
	if s == nil {
		return fmt.Errorf("promotion_seq store is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.last == nil {
		s.last = map[string]uint64{}
	}
	key := table + "\x00" + partitionID
	if seq <= s.last[key] {
		return fmt.Errorf("stale promotion_seq %d for partition %s: last applied %d", seq, partitionID, s.last[key])
	}
	s.last[key] = seq
	return nil
}

func isLogicalHashScanPart(partName string) bool {
	return strings.HasPrefix(partName, "hash-scan-")
}

func activePartEntriesToByteSideParts(entries []replay.PartManifestEntry) []ByteSidePart {
	out := make([]ByteSidePart, 0, len(entries))
	for _, entry := range entries {
		out = append(out, ByteSidePart{
			PartitionID:   entry.PartitionID,
			PartName:      entry.PartName,
			RowCount:      entry.RowCount,
			PartRowLtHash: entry.PartRowLtHash,
		})
	}
	return out
}

// promoteShadowTableName builds the per-partition promote shadow table name in
// the spec §6.3 form `<promotion_id>__<partition_id>` (gap-26a). Each promoted
// partition gets its own copy-on-write commit buffer, so concurrent
// same-table/different-partition promotions never share a shadow table. The
// promotion_id already encodes the promotion_seq on the arbiter side, so the
// name needs no separate seq suffix; identifier-unsafe characters are folded to
// '_'.
func promoteShadowTableName(promotionID, partitionID string) string {
	id := unsafeIdentChars.ReplaceAllString(promotionID, "_")
	pid := unsafeIdentChars.ReplaceAllString(partitionID, "_")
	return unsafeIdentChars.ReplaceAllString(id+"__"+pid, "_")
}

// lthashAccumulatorHex serializes an lthash accumulator as the raw 2048-byte
// little-endian form ("0x"+hex(Bytes())). Unlike (*lthash.Hash).Hex() (which is
// the 32-byte BLAKE3 digest, a display value that is NOT additive), this raw
// form is the arithmetic object: two accumulators serialized this way can be
// re-decoded and summed lane-wise. Every part_row_lthash / partition_root in the
// storage-integrity lane uses this form so the ledger equation
// sum(input parts) == sum(output parts) holds across compaction/mutation
// (spec §8.1, §7.3, gap-14). Kept byte-compatible with pkg/replay/payloadexec's
// lthashHex.
func lthashAccumulatorHex(acc *lthash.Hash) string {
	return "0x" + hex.EncodeToString(acc.Bytes())
}

// lthashAccumulatorFromHex is the inverse of lthashAccumulatorHex.
func lthashAccumulatorFromHex(s string) (*lthash.Hash, error) {
	raw, err := hex.DecodeString(strings.TrimPrefix(s, "0x"))
	if err != nil {
		return nil, fmt.Errorf("decode lthash accumulator hex: %w", err)
	}
	return lthash.FromBytes(raw)
}

// sumPartRowLtHashes returns the additive lattice sum of the given parts'
// raw accumulators as an lthashAccumulatorHex string. It is the additive
// partition root: the sum is invariant under how rows are grouped into parts,
// so a controlled compaction that re-packs the same rows into differently named
// parts produces an identical root (spec §8.1 ledger equation).
func sumPartRowLtHashes(hashes []string) (string, error) {
	acc := lthash.New()
	for _, h := range hashes {
		if h == "" {
			continue
		}
		part, err := lthashAccumulatorFromHex(h)
		if err != nil {
			return "", err
		}
		acc.AddHash(part)
	}
	return lthashAccumulatorHex(acc), nil
}

// subLtHashAccumulators returns the additive lattice difference post − base as
// an lthashAccumulatorHex string (gap-31): the partition_delta is the exact
// set of rows that changed, so base + delta == post holds lane-wise. Returns
// ("", false) if either operand is not a raw accumulator hex (e.g. a digest
// fallback), so the caller can keep the legacy digest delta.
func subLtHashAccumulators(post, base string) (string, bool) {
	postAcc, err := lthashAccumulatorFromHex(post)
	if err != nil {
		return "", false
	}
	baseAcc, err := lthashAccumulatorFromHex(base)
	if err != nil {
		return "", false
	}
	postAcc.SubHash(baseAcc)
	return lthashAccumulatorHex(postAcc), true
}

func partitionRootFromActiveParts(parts []replay.PartManifestEntry, partitionID string) string {
	var filtered []replay.PartManifestEntry
	for _, part := range parts {
		if part.PartitionID == partitionID {
			filtered = append(filtered, part)
		}
	}
	if len(filtered) == 1 {
		return filtered[0].PartRowLtHash
	}
	hashes := make([]string, 0, len(filtered))
	for _, p := range filtered {
		hashes = append(hashes, p.PartRowLtHash)
	}
	// Additive lattice sum of the raw part accumulators (gap-14). A malformed
	// part hash (not raw accumulator hex) falls back to the legacy digest so the
	// CAS still fails closed with a comparable-but-distinct value rather than
	// panicking.
	root, err := sumPartRowLtHashes(hashes)
	if err != nil {
		return digestManifestParts(filtered)
	}
	return root
}

type ClickHouseMutationExecutor struct {
	Conn            SQLConn
	Hasher          TableHasher
	ActiveParts     ActivePartReader
	ClaimSigner     MutationClaimSigner
	WorkerID        string
	ScratchDatabase string
	// BaseScan, when set, computes the safe base commitment via the part-LtHash
	// cache fast path instead of a full-table HashTable scan. It only affects the
	// base read; the per-partition base roots it yields are byte-identical
	// additive sums (a miss recomputes by scanning), so the mutation evidence and
	// base-root CAS are unchanged. Requires the safe table to be a qualified
	// `db`.`table`; falls back to Hasher otherwise.
	BaseScan *CachingPartScanner
	// MutationsSync is the mutations_sync value forced onto scratch mutations
	// so the read-back commitment reflects a materialized post-state. 0 means
	// the default (2); values are clamped to non-negative by ensureMutationSync.
	MutationsSync int
	// QueryTimeout, when > 0, bounds every scratch DDL/DML statement. 0 leaves
	// the caller-provided context deadline untouched.
	QueryTimeout time.Duration
}

func (e ClickHouseMutationExecutor) ExecuteMutation(ctx context.Context, task MutationTask) (MutationClaim, error) {
	before, hash, scratch, err := e.execute(ctx, task, "claim")
	if err != nil {
		return MutationClaim{}, err
	}
	evidence := buildMutationEvidence(task, before, hash)
	postStateRoot, err := mutationPostStateRoot(task, before, hash, evidence, hash.StateRoot)
	if err != nil {
		return MutationClaim{}, err
	}
	claim := MutationClaim{
		StatementID:              task.StatementID,
		WorkerID:                 e.WorkerID,
		ScratchTable:             scratch,
		BaseSafeSnapshotID:       task.BaseSafeSnapshotID,
		BasePartitionRoot:        task.BasePartitionRoot,
		BasePartitionRoots:       evidence.baseRoots,
		SchemaSnapshotID:         task.SchemaSnapshotID,
		PromotionSeq:             task.PromotionSeq,
		PostStateRoot:            postStateRoot,
		PostPartitionCommitments: evidence.postCommitments,
		PartitionDeltas:          evidence.deltas,
		RowsBefore:               evidence.rowsBefore,
		RowsAfter:                evidence.rowsAfter,
		RowsUpdated:              evidence.rowsUpdated,
		RowsDeleted:              evidence.rowsDeleted,
		Parts:                    hash.Parts,
	}
	claim.ClaimHash = digestMutationClaim(claim)
	if e.ClaimSigner != nil {
		sig, err := e.ClaimSigner.SignClaim(claim.ClaimHash)
		if err != nil {
			return MutationClaim{}, fmt.Errorf("sign mutation claim: %w", err)
		}
		claim.Signature = sig
	}
	return claim, nil
}

func (e ClickHouseMutationExecutor) ReplayMutation(ctx context.Context, task MutationTask) (MutationReplayResult, error) {
	before, hash, _, err := e.execute(ctx, task, "replay")
	if err != nil {
		return MutationReplayResult{}, err
	}
	evidence := buildMutationEvidence(task, before, hash)
	postStateRoot, err := mutationPostStateRoot(task, before, hash, evidence, hash.StateRoot)
	if err != nil {
		return MutationReplayResult{}, err
	}
	result := MutationReplayResult{
		StatementID:              task.StatementID,
		WorkerID:                 e.WorkerID,
		BaseSafeSnapshotID:       task.BaseSafeSnapshotID,
		BasePartitionRoot:        task.BasePartitionRoot,
		BasePartitionRoots:       evidence.baseRoots,
		SchemaSnapshotID:         task.SchemaSnapshotID,
		PromotionSeq:             task.PromotionSeq,
		PostStateRoot:            postStateRoot,
		PostPartitionCommitments: evidence.postCommitments,
		PartitionDeltas:          evidence.deltas,
		RowsBefore:               evidence.rowsBefore,
		RowsAfter:                evidence.rowsAfter,
		RowsUpdated:              evidence.rowsUpdated,
		RowsDeleted:              evidence.rowsDeleted,
		Parts:                    hash.Parts,
	}
	result.ClaimHash = digestMutationReplay(result)
	if e.ClaimSigner != nil {
		sig, err := e.ClaimSigner.SignClaim(result.ClaimHash)
		if err != nil {
			return MutationReplayResult{}, fmt.Errorf("sign mutation replay: %w", err)
		}
		result.Signature = sig
	}
	return result, nil
}

// verifyMutationBaseRoots checks that the local safe base commitment for each
// affected partition matches the arbiter-pinned base partition root the
// mutation was bound to (spec §7.3 step 6). It is a no-op when the task carries
// no base roots (no published safe snapshot yet / legacy single-root path).
func verifyMutationBaseRoots(task MutationTask, baseParts []ByteSidePart) error {
	if len(task.BasePartitionRoots) == 0 {
		return nil
	}
	entries := byteSidePartsToActivePartEntries(baseParts)
	for _, want := range task.BasePartitionRoots {
		got := partitionRootFromActiveParts(entries, want.PartitionID)
		if got != want.Root {
			return fmt.Errorf("mutation base partition root mismatch for partition %s: local %s want %s", want.PartitionID, got, want.Root)
		}
	}
	return nil
}

func byteSidePartsToActivePartEntries(parts []ByteSidePart) []replay.PartManifestEntry {
	out := make([]replay.PartManifestEntry, 0, len(parts))
	for _, p := range parts {
		out = append(out, replay.PartManifestEntry{
			PartitionID:   p.PartitionID,
			PartName:      p.PartName,
			PartRowLtHash: p.PartRowLtHash,
			RowCount:      p.RowCount,
		})
	}
	return out
}

func (e ClickHouseMutationExecutor) execute(ctx context.Context, task MutationTask, purpose string) (TableHash, TableHash, string, error) {
	if e.Conn == nil {
		return TableHash{}, TableHash{}, "", fmt.Errorf("clickhouse connection is required")
	}
	if e.Hasher == nil {
		return TableHash{}, TableHash{}, "", fmt.Errorf("table hasher is required")
	}
	if e.ScratchDatabase == "" {
		return TableHash{}, TableHash{}, "", fmt.Errorf("scratch database is required")
	}
	if e.WorkerID == "" {
		return TableHash{}, TableHash{}, "", fmt.Errorf("worker_id is required")
	}
	if task.StatementID == "" || task.SafeTable == "" || task.MutationSQL == "" {
		return TableHash{}, TableHash{}, "", fmt.Errorf("mutation task requires statement_id, safe_table, and mutation_sql")
	}
	before, err := e.baseTableHash(ctx, task)
	if err != nil {
		return TableHash{}, TableHash{}, "", fmt.Errorf("hash mutation base %s: %w", task.SafeTable, err)
	}
	if before.StateRoot == "" {
		before.StateRoot = digestParts("mutation-base-state", before.Parts)
	}
	// Fail closed if the local safe base does not match the arbiter-pinned base
	// partition roots: the mutation must replay from exactly the snapshot it was
	// bound to (spec §7.3 step 6). Skipped only when the task carries no base
	// roots (pre-safe-state / legacy path).
	if err := verifyMutationBaseRoots(task, before.Parts); err != nil {
		return TableHash{}, TableHash{}, "", err
	}
	scratch := qualifiedTable(e.ScratchDatabase, scratchTableName(task.SafeTable, task.StatementID, e.WorkerID, purpose))
	mutationSQL, err := rewriteAlterTableTarget(task.MutationSQL, task.SafeTable, scratch)
	if err != nil {
		return TableHash{}, TableHash{}, "", err
	}
	syncValue := e.MutationsSync
	if syncValue == 0 {
		syncValue = 2
	}
	mutationSQL = ensureMutationSync(mutationSQL, syncValue)
	if e.QueryTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, e.QueryTimeout)
		defer cancel()
	}
	// Clone the affected partitions from safe into the scratch table (spec §7.3
	// step 5). Prefer per-partition ATTACH PARTITION — snapshot-consistent, moves
	// whole parts, and only touches affected partitions (gap-28) — over a
	// whole-table INSERT ... SELECT *. When the affected partition set is unknown
	// (native MVP "all" sentinel or empty), fall back to the whole-table copy.
	sqls := []string{
		"CREATE DATABASE IF NOT EXISTS " + quoteIdent(e.ScratchDatabase),
		"DROP TABLE IF EXISTS " + scratch,
		"CREATE TABLE " + scratch + " AS " + task.SafeTable,
	}
	if clonePartitions := attachablePartitionIDs(task.PartitionIDs); len(clonePartitions) > 0 {
		for _, partitionID := range clonePartitions {
			sqls = append(sqls, fmt.Sprintf("ALTER TABLE %s ATTACH PARTITION ID %s FROM %s",
				scratch, sqlStringLiteral(partitionID), task.SafeTable))
		}
	} else {
		sqls = append(sqls, "INSERT INTO "+scratch+" SELECT * FROM "+task.SafeTable)
	}
	if task.InternalDropPartition {
		if len(task.DropPartitionIDs) == 0 {
			return TableHash{}, TableHash{}, "", fmt.Errorf("internal drop-partition mutation requires drop_partition_ids")
		}
		for _, partitionID := range task.DropPartitionIDs {
			sqls = append(sqls, fmt.Sprintf("ALTER TABLE %s DROP PARTITION ID %s", scratch, sqlStringLiteral(partitionID)))
		}
	} else {
		sqls = append(sqls, mutationSQL)
		sqls = append(sqls, "OPTIMIZE TABLE "+scratch+" FINAL")
	}
	for _, sql := range sqls {
		if err := e.Conn.Exec(ctx, sql); err != nil {
			return TableHash{}, TableHash{}, "", fmt.Errorf("exec %q: %w", sql, err)
		}
	}
	hash, err := hashTableWithTableID(ctx, e.Hasher, scratch, task.PartitionIDs, task.TableID)
	if err != nil {
		return TableHash{}, TableHash{}, "", fmt.Errorf("hash mutation scratch %s: %w", scratch, err)
	}
	if e.ActiveParts != nil {
		activeParts, err := readActivePartsWithTableID(ctx, e.ActiveParts, scratch, task.PartitionIDs, task.TableID)
		if err != nil {
			return TableHash{}, TableHash{}, "", fmt.Errorf("read mutation scratch active parts %s: %w", scratch, err)
		}
		hash.Parts = activePartEntriesToByteSideParts(activeParts)
	}
	if hash.StateRoot == "" {
		hash.StateRoot = digestParts("mutation-post-state", hash.Parts)
	}
	return before, hash, scratch, nil
}

// baseTableHash computes the safe base commitment for a mutation. It prefers the
// part-LtHash cache fast path (BaseScan) when wired and the safe table is a
// qualified `db`.`table`, falling back to the full-table Hasher otherwise. The
// fast path yields the same per-partition additive base roots as the full scan
// (only the base_partition_roots — summed — feed the CAS and evidence; the
// per-part granularity and StateRoot of the base are not submitted), so the
// mutation evidence is unchanged. StateRoot is left empty here and filled by the
// caller's digest fallback (it is not part of the submitted evidence).
func (e ClickHouseMutationExecutor) baseTableHash(ctx context.Context, task MutationTask) (TableHash, error) {
	if e.BaseScan != nil {
		if db, table, ok := splitQualifiedTable(task.SafeTable); ok {
			entries, err := e.BaseScan.ScanPartsWithTableID(ctx, db, table, task.TableID, task.PartitionIDs)
			if err != nil {
				return TableHash{}, err
			}
			return TableHash{Parts: activePartEntriesToByteSideParts(entries)}, nil
		}
	}
	return hashTableWithTableID(ctx, e.Hasher, task.SafeTable, task.PartitionIDs, task.TableID)
}

type ClickHouseRollbackExecutor struct {
	Conn SQLConn
}

// partitionRollbackParts splits declared unsafe parts into those carrying a
// physical part name (droppable via DROP PART) and those without. A rollback
// that wants exact-part cleanup must supply named parts; nameless parts cannot
// be dropped precisely and must not be widened to a partition drop.
func partitionRollbackParts(parts []ByteSidePart) (named, unnamed []ByteSidePart) {
	for _, p := range parts {
		if p.PartName == "" {
			unnamed = append(unnamed, p)
			continue
		}
		named = append(named, p)
	}
	return named, unnamed
}

func (e ClickHouseRollbackExecutor) Rollback(ctx context.Context, task RollbackTask) (RollbackResult, error) {
	if e.Conn == nil {
		return RollbackResult{}, fmt.Errorf("clickhouse connection is required")
	}
	result := RollbackResult{
		RollbackID:   task.RollbackID,
		StatementID:  task.StatementID,
		PromotionID:  task.PromotionID,
		TableID:      task.TableID,
		Reason:       task.Reason,
		UnsafeTable:  task.UnsafeTable,
		ScratchTable: task.ScratchTable,
		PromoteTable: task.PromoteTable,
		PartitionIDs: append([]string(nil), task.PartitionIDs...),
	}
	if task.UnsafeTable != "" {
		// ROLLBACK (paired with HG-P0-02): the dual unsafe buffers are NOT
		// statement-exclusive, so one partition may hold provisional parts from
		// several pending statements. A partition-wide DROP PARTITION would
		// delete co-tenant statements' bytes. An INSERT rollback must therefore
		// name the exact unsafe parts to drop; if it cannot, fail closed and
		// leave the bytes for repair/operator intervention rather than widening
		// to a partition drop or silently no-op'ing over nameless parts.
		named, unnamed := partitionRollbackParts(task.UnsafeParts)
		switch {
		case len(unnamed) > 0:
			return RollbackResult{}, fmt.Errorf("insert rollback %s declares %d unsafe part(s) without a physical part name; refusing to widen to a partition-wide drop that could delete other pending statements' bytes", task.RollbackID, len(unnamed))
		case len(named) > 0:
			for _, part := range named {
				if err := e.Conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s DROP PART %s", task.UnsafeTable, sqlStringLiteral(part.PartName))); err != nil {
					return RollbackResult{}, fmt.Errorf("drop unsafe part %s: %w", part.PartName, err)
				}
				result.CleanedUnsafeParts = append(result.CleanedUnsafeParts, part)
			}
		case task.AllowPartitionRollback:
			// Explicitly authorized coarse rollback: only for a statement-exclusive
			// buffer where dropping the whole partition cannot affect other
			// statements (e.g. a dedicated frozen buffer epoch). The control plane
			// must opt in per task; it is never the implicit default.
			for _, partitionID := range task.PartitionIDs {
				if err := e.Conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s DROP PARTITION ID %s", task.UnsafeTable, sqlStringLiteral(partitionID))); err != nil {
					return RollbackResult{}, fmt.Errorf("drop unsafe partition %s: %w", partitionID, err)
				}
				result.DroppedUnsafePartitions = append(result.DroppedUnsafePartitions, partitionID)
			}
		default:
			return RollbackResult{}, fmt.Errorf("insert rollback %s has no exact unsafe parts and partition-wide rollback is not authorized; refusing to clean up unsafe bytes without an exact part set", task.RollbackID)
		}
	}
	for _, table := range uniqueNonEmpty(append([]string{task.ScratchTable}, task.ScratchTables...)) {
		if err := e.Conn.Exec(ctx, "DROP TABLE IF EXISTS "+table); err != nil {
			return RollbackResult{}, fmt.Errorf("drop scratch table %s: %w", table, err)
		}
		result.DroppedScratchTables = append(result.DroppedScratchTables, table)
		if table == task.ScratchTable {
			result.DroppedScratch = true
		}
	}
	for _, table := range uniqueNonEmpty(append([]string{task.PromoteTable}, task.PromoteTables...)) {
		if err := e.Conn.Exec(ctx, "DROP TABLE IF EXISTS "+table); err != nil {
			return RollbackResult{}, fmt.Errorf("drop promote table %s: %w", table, err)
		}
		result.DroppedPromoteTables = append(result.DroppedPromoteTables, table)
		if table == task.PromoteTable {
			result.DroppedPromote = true
		}
	}
	return result, nil
}

type ClickHouseRepairSyncExecutor struct {
	Conn        SQLConn
	Hasher      TableHasher
	ActiveParts ActivePartReader
}

func (e ClickHouseRepairSyncExecutor) RepairSync(ctx context.Context, task RepairSyncTask) (RepairSyncResult, error) {
	if task.SafeTable == "" {
		return RepairSyncResult{}, fmt.Errorf("repair/sync safe_table is required")
	}
	if task.Manifest.SnapshotID != "" {
		if err := task.Manifest.Validate(); err != nil {
			return RepairSyncResult{}, fmt.Errorf("repair manifest: %w", err)
		}
	}
	result := RepairSyncResult{
		RepairID:     task.RepairID,
		SnapshotID:   firstNonEmptyString(task.SnapshotID, task.Manifest.SnapshotID),
		TableID:      firstNonEmptyString(task.TableID, tableIDFromManifest(task.Manifest), normalizeTableID(task.SafeTable)),
		SafeTable:    task.SafeTable,
		SourceTable:  task.SourceTable,
		PartitionIDs: append([]string(nil), task.PartitionIDs...),
		ManifestRoot: firstNonEmptyString(task.ExpectedManifestRoot, task.Manifest.ManifestRoot),
	}
	if task.SourceTable != "" && !task.VerifyOnly {
		if e.Conn == nil {
			return RepairSyncResult{}, fmt.Errorf("clickhouse connection is required for repair")
		}
		if len(task.PartitionIDs) == 0 {
			return RepairSyncResult{}, fmt.Errorf("repair/sync with source_table requires partition_ids")
		}
		for _, partitionID := range task.PartitionIDs {
			sql := fmt.Sprintf("ALTER TABLE %s REPLACE PARTITION ID %s FROM %s", task.SafeTable, sqlStringLiteral(partitionID), task.SourceTable)
			if err := e.Conn.Exec(ctx, sql); err != nil {
				return RepairSyncResult{}, fmt.Errorf("repair replace partition %s: %w", partitionID, err)
			}
		}
		result.Repaired = true
	}
	if e.Hasher == nil {
		return RepairSyncResult{}, fmt.Errorf("table hasher is required")
	}
	hash, err := hashTableWithTableID(ctx, e.Hasher, task.SafeTable, task.PartitionIDs, result.TableID)
	if err != nil {
		return RepairSyncResult{}, fmt.Errorf("hash repaired safe table: %w", err)
	}
	if hash.StateRoot == "" {
		hash.StateRoot = digestParts("repair-sync-state", hash.Parts)
	}
	result.StateRoot = hash.StateRoot
	expectedStateRoot := firstNonEmptyString(task.ExpectedStateRoot, task.Manifest.StateRoot)
	result.InSync = expectedStateRoot == "" || result.StateRoot == expectedStateRoot
	if task.Manifest.SnapshotID == "" && !task.RequireManifestMatch {
		return result, nil
	}
	if e.ActiveParts == nil {
		result.InSync = false
		result.ActivePartsMatch = false
		result.Error = "active part reader is required for manifest repair/sync"
		return result, nil
	}
	parts, err := readActivePartsWithTableID(ctx, e.ActiveParts, task.SafeTable, task.PartitionIDs, result.TableID)
	if err != nil {
		return RepairSyncResult{}, fmt.Errorf("read repaired active parts: %w", err)
	}
	result.ActiveParts = parts
	expectedParts := manifestActiveParts(task.Manifest, result.TableID, task.PartitionIDs)
	result.ActivePartsMatch = activePartsEqual(parts, expectedParts)
	result.InSync = result.InSync && result.ActivePartsMatch
	if !result.InSync && result.Error == "" {
		result.Error = "safe table does not match target manifest"
	}
	return result, nil
}

type ClickHouseCompactor struct {
	Conn             SQLConn
	ActiveParts      ActivePartReader
	PartitionRoots   PartitionRootReader
	CompactDatabase  string
	DropCompactTable bool
}

func (c ClickHouseCompactor) Compact(ctx context.Context, task CompactionTask) (CompactionResult, error) {
	if c.Conn == nil {
		return CompactionResult{}, fmt.Errorf("clickhouse connection is required")
	}
	if task.SafeTable == "" {
		return CompactionResult{}, fmt.Errorf("compaction safe_table is required")
	}
	if len(task.PartitionIDs) == 0 {
		return CompactionResult{}, fmt.Errorf("compaction requires partition_ids")
	}
	if task.RequireBaseRootCAS || task.BasePartitionRoot != "" {
		if c.PartitionRoots == nil {
			return CompactionResult{}, fmt.Errorf("partition root reader is required for compaction base-root CAS")
		}
		for _, partitionID := range task.PartitionIDs {
			current, err := currentPartitionRootWithTableID(ctx, c.PartitionRoots, task.SafeTable, partitionID, task.TableID)
			if err != nil {
				return CompactionResult{}, fmt.Errorf("read current partition root %s: %w", partitionID, err)
			}
			if current != task.BasePartitionRoot {
				return CompactionResult{}, fmt.Errorf("base partition root mismatch for partition %s: got %s want %s", partitionID, current, task.BasePartitionRoot)
			}
		}
	}
	compactDB := firstNonEmptyString(task.CompactDatabase, c.CompactDatabase)
	compactTable := task.CompactTable
	if compactTable == "" {
		if compactDB == "" {
			return CompactionResult{}, fmt.Errorf("compact database is required")
		}
		compactTable = qualifiedTable(compactDB, compactShadowTableName(task.SafeTable, task.CompactionID, task.PromotionSeq))
	}
	if compactDB != "" {
		if err := c.Conn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+quoteIdent(compactDB)); err != nil {
			return CompactionResult{}, fmt.Errorf("create compact database: %w", err)
		}
	}
	if err := c.Conn.Exec(ctx, "DROP TABLE IF EXISTS "+compactTable); err != nil {
		return CompactionResult{}, fmt.Errorf("drop stale compact table: %w", err)
	}
	if err := c.Conn.Exec(ctx, "CREATE TABLE "+compactTable+" AS "+task.SafeTable); err != nil {
		return CompactionResult{}, fmt.Errorf("create compact table: %w", err)
	}
	for _, partitionID := range task.PartitionIDs {
		if err := c.Conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s ATTACH PARTITION ID %s FROM %s", compactTable, sqlStringLiteral(partitionID), task.SafeTable)); err != nil {
			return CompactionResult{}, fmt.Errorf("attach compact partition %s: %w", partitionID, err)
		}
	}
	if err := c.Conn.Exec(ctx, "OPTIMIZE TABLE "+compactTable+" FINAL"); err != nil {
		return CompactionResult{}, fmt.Errorf("optimize compact table: %w", err)
	}
	expectedPostRoot := firstNonEmptyString(task.ExpectedPostRoot, task.BasePartitionRoot)
	if task.RequirePostRootCAS || expectedPostRoot != "" {
		if c.ActiveParts == nil {
			return CompactionResult{}, fmt.Errorf("active part reader is required for compaction post-root CAS")
		}
		compactParts, err := readActivePartsWithTableID(ctx, c.ActiveParts, compactTable, task.PartitionIDs, task.TableID)
		if err != nil {
			return CompactionResult{}, fmt.Errorf("read compact active parts: %w", err)
		}
		for _, partitionID := range task.PartitionIDs {
			got := partitionRootFromActiveParts(compactParts, partitionID)
			if got != expectedPostRoot {
				return CompactionResult{}, fmt.Errorf("compact post partition root mismatch for partition %s: got %s want %s", partitionID, got, expectedPostRoot)
			}
		}
	}
	// gap-14: controlled-compaction ledger equation. With additive partition
	// roots, re-packing the same rows into different output parts must leave the
	// per-partition Σ(part_row_lthash) unchanged. When the task pins the input
	// safe parts, verify sum(input parts) == sum(output parts) per partition so a
	// compaction that silently added/dropped/mutated rows fails closed here
	// (spec §8.1), independent of any part-name-bearing digest.
	if len(task.InputParts) > 0 {
		if c.ActiveParts == nil {
			return CompactionResult{}, fmt.Errorf("active part reader is required to verify the compaction ledger equation")
		}
		outputParts, err := readActivePartsWithTableID(ctx, c.ActiveParts, compactTable, task.PartitionIDs, task.TableID)
		if err != nil {
			return CompactionResult{}, fmt.Errorf("read compact active parts for ledger equation: %w", err)
		}
		for _, partitionID := range task.PartitionIDs {
			inputSum := partitionRootFromActiveParts(task.InputParts, partitionID)
			outputSum := partitionRootFromActiveParts(outputParts, partitionID)
			if inputSum != outputSum {
				return CompactionResult{}, fmt.Errorf("compaction ledger equation violated for partition %s: sum(input parts)=%s != sum(output parts)=%s", partitionID, inputSum, outputSum)
			}
		}
	}
	for _, partitionID := range task.PartitionIDs {
		if err := c.Conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s REPLACE PARTITION ID %s FROM %s", task.SafeTable, sqlStringLiteral(partitionID), compactTable)); err != nil {
			return CompactionResult{}, fmt.Errorf("replace compacted partition %s: %w", partitionID, err)
		}
	}
	result := CompactionResult{
		CompactionID:       task.CompactionID,
		PromotionSeq:       task.PromotionSeq,
		TableID:            task.TableID,
		SafeTable:          task.SafeTable,
		CompactTable:       compactTable,
		BaseSafeSnapshotID: task.BaseSafeSnapshotID,
		BasePartitionRoot:  task.BasePartitionRoot,
		ExpectedPostRoot:   task.ExpectedPostRoot,
		PartitionIDs:       append([]string(nil), task.PartitionIDs...),
	}
	if c.ActiveParts != nil {
		parts, err := readActivePartsWithTableID(ctx, c.ActiveParts, task.SafeTable, task.PartitionIDs, task.TableID)
		if err != nil {
			return CompactionResult{}, fmt.Errorf("read compacted active parts: %w", err)
		}
		result.ActiveParts = parts
	}
	if task.DropCompactTable || c.DropCompactTable {
		if err := c.Conn.Exec(ctx, "DROP TABLE IF EXISTS "+compactTable); err != nil {
			return CompactionResult{}, fmt.Errorf("drop compact table: %w", err)
		}
	}
	return result, nil
}

type HashingByteSideScanner struct {
	// FastScan, when set, is preferred: it reads live part metadata from
	// system.parts and reuses cached part_row_lthash for parts whose physical
	// bytes are unchanged, folding rows only for the parts that miss the cache.
	// Its output is byte-identical to ActiveParts.ReadActiveParts for the same
	// parts (live names, same additive fold), so the submitted ByteSideScanResult
	// / PartSetHash are unchanged. Requires the unsafe table to be a qualified
	// `db`.`table`; falls back to ActiveParts/Hasher otherwise.
	FastScan *CachingPartScanner
	// ActiveParts, when set, recomputes part_row_lthash per real physical part
	// (from system.parts via _part) so RCRecord candidate parts carry the actual
	// disk part names — required for hardlink ATTACH PART promotion (spec §6.2
	// byte-side check, gap-26b). Preferred over Hasher.
	ActiveParts ActivePartReader
	// Hasher is the legacy per-partition byte-side scanner. It aggregates rows by
	// _partition_id and emits synthetic `hash-scan-<partition>` part names. Kept
	// as a fallback when no ActivePartReader is wired (e.g. minimal test setups).
	Hasher   TableHasher
	WorkerID string
}

func (s HashingByteSideScanner) ScanByteSide(ctx context.Context, task ByteSideScanTask) (ByteSideScanResult, error) {
	// HG-P0-02: an INSERT byte-side scan proves the fetched hg_unsafe parts equal
	// the source's declared candidate set. A task with no candidate parts cannot
	// be checked, so the Verifier must reject it rather than attest to whatever
	// happens to be in the unsafe partition.
	if isInsertPromotionKind(task.Kind) && len(task.CandidateParts) == 0 {
		return ByteSideScanResult{}, fmt.Errorf("byte-side scan for statement %s has no candidate parts; refusing to attest an unverifiable insert", task.StatementID)
	}
	parts, err := s.scanParts(ctx, task)
	if err != nil {
		return ByteSideScanResult{}, err
	}
	if err := enforceCandidatePartSet(parts, task.CandidateParts); err != nil {
		return ByteSideScanResult{}, err
	}
	return ByteSideScanResult{
		ScanID:      task.ScanID,
		StatementID: task.StatementID,
		TableID:     task.TableID,
		UnsafeTable: task.UnsafeTable,
		WorkerID:    s.WorkerID,
		Parts:       parts,
		PartSetHash: digestParts("byte-side-part-set", parts),
	}, nil
}

// scanParts resolves the unsafe table's active parts, preferring the cached
// fast path when it is wired and the table is a qualified `db`.`table`, then
// the physical active-part reader, then the legacy per-partition hasher.
func (s HashingByteSideScanner) scanParts(ctx context.Context, task ByteSideScanTask) ([]ByteSidePart, error) {
	if s.FastScan != nil {
		if db, table, ok := splitQualifiedTable(task.UnsafeTable); ok {
			entries, err := s.FastScan.ScanPartsWithTableID(ctx, db, table, task.TableID, task.PartitionIDs)
			if err != nil {
				return nil, err
			}
			return activePartEntriesToByteSideParts(entries), nil
		}
		// Not a qualified table: fall through to the full readers.
	}
	if s.ActiveParts != nil {
		entries, err := readActivePartsWithTableID(ctx, s.ActiveParts, task.UnsafeTable, task.PartitionIDs, task.TableID)
		if err != nil {
			return nil, err
		}
		return activePartEntriesToByteSideParts(entries), nil
	}
	if s.Hasher != nil {
		hash, err := hashTableWithTableID(ctx, s.Hasher, task.UnsafeTable, task.PartitionIDs, task.TableID)
		if err != nil {
			return nil, err
		}
		return hash.Parts, nil
	}
	return nil, fmt.Errorf("byte-side scanner requires a fast-scan, active-part reader, or table hasher")
}

// enforceCandidatePartSet asserts that the physically scanned active parts
// (actual) are exactly the parts the source declared as candidates. It supports
// two binding modes:
//
//   - Name-keyed (strict): when every candidate carries a PartName it matches
//     the scanned parts one-to-one by name, comparing partition/row-count/lthash.
//     This is used when the declaring party knows the physical part names.
//   - Content-addressed: when candidates omit part names (the source SNode
//     declares a logical result-claim from the payload without a ClickHouse
//     connection, HG-P0-02), it binds per partition by the additive part_row
//     lthash sum and the row-count sum. Because a partition's additive root is
//     invariant to how rows are packed into named parts (spec §8.1), matching
//     Σ part_row_lthash and Σ row_count proves the scanned bytes contain exactly
//     the rows the source committed, without agreeing on CH-assigned names.
func enforceCandidatePartSet(actual, candidates []ByteSidePart) error {
	if len(candidates) == 0 {
		return nil
	}
	if candidatePartsCarryNames(candidates) {
		return enforceCandidatePartSetByName(actual, candidates)
	}
	return enforceCandidatePartSetByContent(actual, candidates)
}

// candidatePartsCarryNames reports whether every candidate declares a physical
// part name (the strict name-keyed binding is only sound when all do).
func candidatePartsCarryNames(candidates []ByteSidePart) bool {
	for _, c := range candidates {
		if c.PartName == "" {
			return false
		}
	}
	return true
}

func enforceCandidatePartSetByName(actual, candidates []ByteSidePart) error {
	if len(actual) != len(candidates) {
		return fmt.Errorf("candidate part set mismatch: scanned %d active parts, task declared %d candidates", len(actual), len(candidates))
	}
	byName := make(map[string]ByteSidePart, len(actual))
	for _, part := range actual {
		if part.PartName == "" {
			return fmt.Errorf("candidate part set mismatch: scanned active part with empty name")
		}
		if _, exists := byName[part.PartName]; exists {
			return fmt.Errorf("candidate part set mismatch: duplicate scanned part %q", part.PartName)
		}
		byName[part.PartName] = part
	}
	for _, want := range candidates {
		if want.PartName == "" {
			return fmt.Errorf("candidate part set mismatch: candidate has empty part name")
		}
		got, ok := byName[want.PartName]
		if !ok {
			return fmt.Errorf("candidate part set mismatch: candidate part %q is not active", want.PartName)
		}
		if want.PartitionID != "" && got.PartitionID != want.PartitionID {
			return fmt.Errorf("candidate part set mismatch: part %q partition got %q want %q", want.PartName, got.PartitionID, want.PartitionID)
		}
		if want.RowCount != 0 && got.RowCount != want.RowCount {
			return fmt.Errorf("candidate part set mismatch: part %q row count got %d want %d", want.PartName, got.RowCount, want.RowCount)
		}
		if want.PartRowLtHash != "" && got.PartRowLtHash != want.PartRowLtHash {
			return fmt.Errorf("candidate part set mismatch: part %q lthash differs", want.PartName)
		}
	}
	return nil
}

// candidatePartitionAgg is the per-partition additive aggregate used by the
// content-addressed binding.
type candidatePartitionAgg struct {
	hashes   []string
	rowCount uint64
	seen     bool
}

func aggregateCandidatePartsByPartition(parts []ByteSidePart) (map[string]*candidatePartitionAgg, error) {
	out := map[string]*candidatePartitionAgg{}
	for _, p := range parts {
		if p.PartRowLtHash == "" {
			return nil, fmt.Errorf("candidate part set mismatch: part in partition %q has empty part_row_lthash", p.PartitionID)
		}
		agg, ok := out[p.PartitionID]
		if !ok {
			agg = &candidatePartitionAgg{seen: true}
			out[p.PartitionID] = agg
		}
		agg.hashes = append(agg.hashes, p.PartRowLtHash)
		agg.rowCount += p.RowCount
	}
	return out, nil
}

func enforceCandidatePartSetByContent(actual, candidates []ByteSidePart) error {
	// A source that cannot determine ClickHouse's physical partition ids (the
	// Native MVP source declares the single "all" sentinel) commits to the TOTAL
	// content of its statement, not a per-partition breakdown. In that case bind
	// by the whole-set additive sum + row count across all scanned parts, since
	// the additive root is partition-decomposition-independent (spec §8.1). Only
	// when the source declares real per-partition ids do we match per partition.
	if candidatePartsArePartitionAgnostic(candidates) {
		return enforceCandidatePartSetTotals(actual, candidates)
	}
	wantByPartition, err := aggregateCandidatePartsByPartition(candidates)
	if err != nil {
		return err
	}
	gotByPartition, err := aggregateCandidatePartsByPartition(actual)
	if err != nil {
		return err
	}
	if len(gotByPartition) != len(wantByPartition) {
		return fmt.Errorf("candidate part set mismatch: scanned %d partitions, source declared %d", len(gotByPartition), len(wantByPartition))
	}
	for partitionID, want := range wantByPartition {
		got, ok := gotByPartition[partitionID]
		if !ok {
			return fmt.Errorf("candidate part set mismatch: source declared partition %q not present in scanned parts", partitionID)
		}
		wantRoot, err := sumPartRowLtHashes(want.hashes)
		if err != nil {
			return fmt.Errorf("candidate part set mismatch: sum declared parts for partition %q: %w", partitionID, err)
		}
		gotRoot, err := sumPartRowLtHashes(got.hashes)
		if err != nil {
			return fmt.Errorf("candidate part set mismatch: sum scanned parts for partition %q: %w", partitionID, err)
		}
		if wantRoot != gotRoot {
			return fmt.Errorf("candidate part set mismatch: partition %q additive root differs (scanned parts do not equal source candidate set)", partitionID)
		}
		// Anti-cancellation belt: 2^16 identical rows cancel per LtHash lane, so
		// pair the additive-root check with an exact row-count sum (spec §5.4).
		if want.rowCount != 0 && got.rowCount != want.rowCount {
			return fmt.Errorf("candidate part set mismatch: partition %q row count got %d want %d", partitionID, got.rowCount, want.rowCount)
		}
	}
	return nil
}

// candidatePartsArePartitionAgnostic reports whether the source declared its
// candidates without real ClickHouse partition ids — every candidate carries
// the "all" sentinel or an empty partition id. Such a claim commits to the
// total statement content, matched by whole-set sum.
func candidatePartsArePartitionAgnostic(candidates []ByteSidePart) bool {
	for _, c := range candidates {
		if c.PartitionID != "" && c.PartitionID != NativeAllPartitionID {
			return false
		}
	}
	return true
}

// enforceCandidatePartSetTotals binds the whole scanned part set to the source
// claim by the total additive part_row_lthash sum and total row count.
func enforceCandidatePartSetTotals(actual, candidates []ByteSidePart) error {
	wantHashes := make([]string, 0, len(candidates))
	var wantRows uint64
	for _, c := range candidates {
		if c.PartRowLtHash == "" {
			return fmt.Errorf("candidate part set mismatch: source candidate has empty part_row_lthash")
		}
		wantHashes = append(wantHashes, c.PartRowLtHash)
		wantRows += c.RowCount
	}
	gotHashes := make([]string, 0, len(actual))
	var gotRows uint64
	for _, a := range actual {
		if a.PartRowLtHash == "" {
			return fmt.Errorf("candidate part set mismatch: scanned part %q has empty part_row_lthash", a.PartName)
		}
		gotHashes = append(gotHashes, a.PartRowLtHash)
		gotRows += a.RowCount
	}
	if len(actual) == 0 {
		return fmt.Errorf("candidate part set mismatch: source declared candidate parts but the scan found none")
	}
	wantRoot, err := sumPartRowLtHashes(wantHashes)
	if err != nil {
		return fmt.Errorf("candidate part set mismatch: sum declared parts: %w", err)
	}
	gotRoot, err := sumPartRowLtHashes(gotHashes)
	if err != nil {
		return fmt.Errorf("candidate part set mismatch: sum scanned parts: %w", err)
	}
	if wantRoot != gotRoot {
		return fmt.Errorf("candidate part set mismatch: total additive root differs (scanned parts do not equal source candidate set)")
	}
	if wantRows != 0 && gotRows != wantRows {
		return fmt.Errorf("candidate part set mismatch: total row count got %d want %d", gotRows, wantRows)
	}
	return nil
}

type ClickHouseSafeAuditor struct {
	Hasher      TableHasher
	ActiveParts ActivePartReader
	// FastScan, when set, serves the active-set comparison (the manifest
	// active-parts check) from the part cache instead of a full row scan. It does
	// NOT affect the audit vote's StateRoot, which stays a full Hasher.HashTable
	// scan — the vote semantics are unchanged; the cache only accelerates the
	// pre-check. Requires the safe table to be a qualified `db`.`table`; falls
	// back to ActiveParts otherwise.
	FastScan *CachingPartScanner
	WorkerID string
}

func (a ClickHouseSafeAuditor) AuditSafe(ctx context.Context, task SafeAuditTask) (SafeAuditVote, error) {
	if a.Hasher == nil {
		return SafeAuditVote{}, fmt.Errorf("table hasher is required")
	}
	tableID := firstNonEmptyString(task.TableID, tableIDFromManifest(task.Manifest), normalizeTableID(task.SafeTable))
	expectedStateRoot := task.StateRoot
	if expectedStateRoot == "" {
		expectedStateRoot = task.Manifest.StateRoot
	}
	hash, err := hashTableWithTableID(ctx, a.Hasher, task.SafeTable, task.PartitionIDs, tableID)
	if err != nil {
		return SafeAuditVote{}, err
	}
	vote := SafeAuditVote{
		AuditID:    task.AuditID,
		SnapshotID: firstNonEmptyString(task.SnapshotID, task.Manifest.SnapshotID),
		WorkerID:   a.WorkerID,
		StateRoot:  hash.StateRoot,
		Match:      hash.StateRoot == expectedStateRoot,
	}
	if task.Manifest.SnapshotID == "" {
		return vote, nil
	}
	if err := task.Manifest.Validate(); err != nil {
		return SafeAuditVote{}, fmt.Errorf("safe audit manifest: %w", err)
	}
	vote.ManifestRoot = task.Manifest.ManifestRoot
	if a.FastScan == nil && a.ActiveParts == nil {
		vote.Match = false
		vote.ActivePartsMatch = false
		vote.Error = "active part reader is required for manifest audit"
		return vote, nil
	}
	parts, err := a.auditActiveParts(ctx, task)
	if err != nil {
		return SafeAuditVote{}, err
	}
	expectedParts := manifestActiveParts(task.Manifest, tableID, task.PartitionIDs)
	vote.ActiveParts = parts
	vote.ActivePartsMatch = activePartsEqual(parts, expectedParts)
	vote.Match = vote.Match && vote.ActivePartsMatch
	if !vote.ActivePartsMatch {
		vote.Error = "active parts do not match manifest"
	}
	return vote, nil
}

// auditActiveParts reads the safe table's active parts for the manifest
// active-set comparison. It prefers the part cache fast path when wired and the
// safe table is a qualified `db`.`table`, folding rows only for parts that miss
// the cache; it does NOT touch the vote StateRoot. Falls back to the full
// active-part reader otherwise.
func (a ClickHouseSafeAuditor) auditActiveParts(ctx context.Context, task SafeAuditTask) ([]replay.PartManifestEntry, error) {
	if a.FastScan != nil {
		if db, table, ok := splitQualifiedTable(task.SafeTable); ok {
			tableID := firstNonEmptyString(task.TableID, tableIDFromManifest(task.Manifest), normalizeTableID(task.SafeTable))
			parts, err := a.FastScan.ScanPartsWithTableID(ctx, db, table, tableID, task.PartitionIDs)
			if err != nil {
				return nil, fmt.Errorf("read safe active parts (fast): %w", err)
			}
			return parts, nil
		}
	}
	if a.ActiveParts == nil {
		return nil, fmt.Errorf("active part reader is required for manifest audit")
	}
	tableID := firstNonEmptyString(task.TableID, tableIDFromManifest(task.Manifest), normalizeTableID(task.SafeTable))
	parts, err := readActivePartsWithTableID(ctx, a.ActiveParts, task.SafeTable, task.PartitionIDs, tableID)
	if err != nil {
		return nil, fmt.Errorf("read safe active parts: %w", err)
	}
	return parts, nil
}

var unsafeIdentChars = regexp.MustCompile(`[^A-Za-z0-9_]`)

func scratchTableName(safeTable, statementID, workerID, purpose string) string {
	base := lastIdentifier(safeTable)
	return unsafeIdentChars.ReplaceAllString(strings.Join([]string{base, statementID, workerID, purpose}, "_"), "_")
}

func compactShadowTableName(safeTable, compactionID string, promotionSeq uint64) string {
	base := lastIdentifier(safeTable)
	id := unsafeIdentChars.ReplaceAllString(compactionID, "_")
	name := base + "_" + id
	if promotionSeq != 0 {
		name = fmt.Sprintf("%s_%d", name, promotionSeq)
	}
	return unsafeIdentChars.ReplaceAllString(name, "_")
}

type mutationEvidence struct {
	baseRoots       []replay.PartitionCommitment
	postCommitments []replay.PartitionCommitment
	deltas          []PartitionDelta
	rowsBefore      uint64
	rowsAfter       uint64
	rowsUpdated     uint64
	rowsDeleted     uint64
}

// mutationPostStateRoot binds the mutation post-state to the manifest's
// state-root formula (HG-P1-02): H(schema_snapshot_id, schema_root,
// executor_profile_id, data_root_after) via replay.AssembleStateRoot, rather
// than a bare unbound LtHash digest. data_root_after is the whole-table
// partition-root set after the mutation: the base per-partition roots with each
// touched partition replaced by its post-commitment (spec §6.3 — the manifest
// state covers unchanged partitions too, so a schema/profile swap or an
// untouched-partition divergence changes the committed root).
//
// Source (ExecuteMutation) and verifier (ReplayMutation) call this identically,
// so the claim's PostStateRoot equals the replay's by construction. When the
// task carries no schema_root/executor_profile_id (legacy tasks), it returns
// the bare post digest so behaviour is unchanged until the control plane
// supplies the schema identity.
func mutationPostStateRoot(task MutationTask, before, post TableHash, evidence mutationEvidence, bareFallback string) (string, error) {
	if task.SchemaRoot == "" || task.ExecutorProfileID == "" {
		return bareFallback, nil
	}
	tableID := firstNonEmptyString(task.TableID, normalizeTableID(task.SafeTable))
	// Start from the base whole-table partition roots (prefer the task-pinned
	// BasePartitionRoots; fall back to the roots derived from the base scan), then
	// overlay the touched partitions' post commitments.
	roots := map[string]replay.PartitionCommitment{}
	order := []string{}
	upsert := func(pc replay.PartitionCommitment) {
		key := pc.PartitionID
		if _, ok := roots[key]; !ok {
			order = append(order, key)
		}
		roots[key] = replay.PartitionCommitment{TableID: tableID, PartitionID: pc.PartitionID, Root: pc.Root}
	}
	base := task.BasePartitionRoots
	if len(base) == 0 {
		base = evidence.baseRoots
	}
	for _, pc := range base {
		upsert(pc)
	}
	for _, pc := range evidence.postCommitments {
		upsert(pc)
	}
	partitionRoots := make([]replay.PartitionCommitment, 0, len(order))
	for _, key := range order {
		partitionRoots = append(partitionRoots, roots[key])
	}
	tables := []replay.TableManifest{{TableID: tableID, PartitionRoots: partitionRoots}}
	_, stateRoot, err := replay.AssembleStateRoot(task.SchemaSnapshotID, task.SchemaRoot, task.ExecutorProfileID, tables)
	if err != nil {
		return "", fmt.Errorf("assemble mutation post state root: %w", err)
	}
	return stateRoot, nil
}

func buildMutationEvidence(task MutationTask, before, post TableHash) mutationEvidence {
	tableID := firstNonEmptyString(task.TableID, normalizeTableID(task.SafeTable))
	partitions := mutationEvidencePartitions(task, before.Parts, post.Parts)
	out := mutationEvidence{
		baseRoots:       make([]replay.PartitionCommitment, 0, len(partitions)),
		postCommitments: make([]replay.PartitionCommitment, 0, len(partitions)),
		deltas:          make([]PartitionDelta, 0, len(partitions)),
	}
	for _, partitionID := range partitions {
		rowsBefore := byteSideRows(before.Parts, partitionID)
		rowsAfter := byteSideRows(post.Parts, partitionID)
		baseRoot := mutationBaseRoot(task, before.Parts, partitionID)
		postRoot := byteSidePartitionRoot(post.Parts, partitionID)
		if postRoot == "" && len(partitions) == 1 {
			postRoot = post.StateRoot
		}
		if baseRoot != "" {
			out.baseRoots = append(out.baseRoots, replay.PartitionCommitment{
				TableID:     tableID,
				PartitionID: partitionID,
				Root:        baseRoot,
			})
		}
		if postRoot != "" {
			out.postCommitments = append(out.postCommitments, replay.PartitionCommitment{
				TableID:     tableID,
				PartitionID: partitionID,
				Root:        postRoot,
			})
		}
		delta := PartitionDelta{
			TableID:     tableID,
			PartitionID: partitionID,
			BaseRoot:    baseRoot,
			PostRoot:    postRoot,
			RowsBefore:  rowsBefore,
			RowsAfter:   rowsAfter,
		}
		switch task.MutationType {
		case MutationTypeUpdate:
			// APPROXIMATION (gap-31): ClickHouse ALTER UPDATE does not expose the
			// exact number of rows the predicate matched; system.mutations reports
			// completion, not a matched-row count. We record rows_after (the
			// partition's post-state row count) as an upper-bound proxy for
			// rows_updated. The authoritative change evidence is DeltaRoot (the
			// additive post − base commitment), which is exact; rows_updated is
			// advisory only and must not be used as a correctness gate.
			delta.RowsUpdated = rowsAfter
		case MutationTypeDelete:
			if task.InternalDropPartition {
				delta.RowsDeleted = rowsBefore
			} else if rowsBefore >= rowsAfter {
				delta.RowsDeleted = rowsBefore - rowsAfter
			}
		}
		// gap-31: the partition delta root is the additive LtHash difference
		// post − base, so base + delta == post is verifiable by lane-wise sum.
		// An empty base means no prior rows in the partition, so delta == post
		// (post − 0). Falls back to the legacy digest when a root is not a raw
		// accumulator (e.g. a single-partition post that used a state-root
		// digest), keeping the field populated and comparable.
		base := baseRoot
		if base == "" {
			base = lthashAccumulatorHex(lthash.New())
		}
		if deltaRoot, ok := subLtHashAccumulators(postRoot, base); ok {
			delta.DeltaRoot = deltaRoot
		} else {
			delta.DeltaRoot = digestMutationDelta(delta)
		}
		out.deltas = append(out.deltas, delta)
		out.rowsBefore += rowsBefore
		out.rowsAfter += rowsAfter
		out.rowsUpdated += delta.RowsUpdated
		out.rowsDeleted += delta.RowsDeleted
	}
	return out
}

func mutationEvidencePartitions(task MutationTask, before, post []ByteSidePart) []string {
	seen := map[string]struct{}{}
	var out []string
	for _, partitionID := range task.PartitionIDs {
		if partitionID == "" {
			continue
		}
		if _, ok := seen[partitionID]; ok {
			continue
		}
		seen[partitionID] = struct{}{}
		out = append(out, partitionID)
	}
	if len(out) == 0 {
		for _, part := range append(append([]ByteSidePart(nil), before...), post...) {
			if part.PartitionID == "" {
				continue
			}
			if _, ok := seen[part.PartitionID]; ok {
				continue
			}
			seen[part.PartitionID] = struct{}{}
			out = append(out, part.PartitionID)
		}
	}
	sort.Strings(out)
	return out
}

func mutationBaseRoot(task MutationTask, parts []ByteSidePart, partitionID string) string {
	for _, root := range task.BasePartitionRoots {
		if root.PartitionID == partitionID && root.Root != "" {
			return root.Root
		}
	}
	if task.BasePartitionRoot != "" {
		return task.BasePartitionRoot
	}
	return byteSidePartitionRoot(parts, partitionID)
}

func byteSideRows(parts []ByteSidePart, partitionID string) uint64 {
	var rows uint64
	for _, part := range parts {
		if part.PartitionID == partitionID {
			rows += part.RowCount
		}
	}
	return rows
}

func byteSidePartitionRoot(parts []ByteSidePart, partitionID string) string {
	var filtered []ByteSidePart
	for _, part := range parts {
		if part.PartitionID == partitionID {
			filtered = append(filtered, part)
		}
	}
	if len(filtered) == 0 {
		return ""
	}
	if len(filtered) == 1 {
		return filtered[0].PartRowLtHash
	}
	hashes := make([]string, 0, len(filtered))
	for _, p := range filtered {
		hashes = append(hashes, p.PartRowLtHash)
	}
	// Additive lattice sum (gap-14): the byte-side partition root must equal the
	// active-part partition root for the same rows, so both use Σ accumulators.
	root, err := sumPartRowLtHashes(hashes)
	if err != nil {
		return digestParts("byte-side-partition-root", sortedByteSideParts(filtered))
	}
	return root
}

func sortedByteSideParts(parts []ByteSidePart) []ByteSidePart {
	out := append([]ByteSidePart(nil), parts...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].PartitionID != out[j].PartitionID {
			return out[i].PartitionID < out[j].PartitionID
		}
		return out[i].PartName < out[j].PartName
	})
	return out
}

func digestMutationDelta(delta PartitionDelta) string {
	v := struct {
		TableID     string `json:"table_id,omitempty"`
		PartitionID string `json:"partition_id"`
		BaseRoot    string `json:"base_root,omitempty"`
		PostRoot    string `json:"post_root"`
		RowsBefore  uint64 `json:"rows_before,omitempty"`
		RowsAfter   uint64 `json:"rows_after,omitempty"`
		RowsUpdated uint64 `json:"rows_updated,omitempty"`
		RowsDeleted uint64 `json:"rows_deleted,omitempty"`
	}{
		TableID:     delta.TableID,
		PartitionID: delta.PartitionID,
		BaseRoot:    delta.BaseRoot,
		PostRoot:    delta.PostRoot,
		RowsBefore:  delta.RowsBefore,
		RowsAfter:   delta.RowsAfter,
		RowsUpdated: delta.RowsUpdated,
		RowsDeleted: delta.RowsDeleted,
	}
	return replay.DigestString("mutation-partition-delta\x00" + fmt.Sprintf("%#v", v))
}

func digestMutationClaim(claim MutationClaim) string {
	v := struct {
		StatementID              string                       `json:"statement_id"`
		WorkerID                 string                       `json:"worker_id"`
		ScratchTable             string                       `json:"scratch_table"`
		BaseSafeSnapshotID       string                       `json:"base_safe_snapshot_id,omitempty"`
		BasePartitionRoots       []replay.PartitionCommitment `json:"base_partition_roots,omitempty"`
		SchemaSnapshotID         string                       `json:"schema_snapshot_id,omitempty"`
		PromotionSeq             uint64                       `json:"promotion_seq,omitempty"`
		PostStateRoot            string                       `json:"post_state_root"`
		PostPartitionCommitments []replay.PartitionCommitment `json:"post_partition_commitments,omitempty"`
		PartitionDeltas          []PartitionDelta             `json:"partition_deltas,omitempty"`
		RowsBefore               uint64                       `json:"rows_before,omitempty"`
		RowsAfter                uint64                       `json:"rows_after,omitempty"`
		RowsUpdated              uint64                       `json:"rows_updated,omitempty"`
		RowsDeleted              uint64                       `json:"rows_deleted,omitempty"`
		Parts                    []ByteSidePart               `json:"parts,omitempty"`
	}{
		StatementID:              claim.StatementID,
		WorkerID:                 claim.WorkerID,
		ScratchTable:             claim.ScratchTable,
		BaseSafeSnapshotID:       claim.BaseSafeSnapshotID,
		BasePartitionRoots:       claim.BasePartitionRoots,
		SchemaSnapshotID:         claim.SchemaSnapshotID,
		PromotionSeq:             claim.PromotionSeq,
		PostStateRoot:            claim.PostStateRoot,
		PostPartitionCommitments: claim.PostPartitionCommitments,
		PartitionDeltas:          claim.PartitionDeltas,
		RowsBefore:               claim.RowsBefore,
		RowsAfter:                claim.RowsAfter,
		RowsUpdated:              claim.RowsUpdated,
		RowsDeleted:              claim.RowsDeleted,
		Parts:                    sortedByteSideParts(claim.Parts),
	}
	return replay.DigestString("mutation-claim\x00" + fmt.Sprintf("%#v", v))
}

func digestMutationReplay(result MutationReplayResult) string {
	claim := MutationClaim{
		StatementID:              result.StatementID,
		WorkerID:                 result.WorkerID,
		BaseSafeSnapshotID:       result.BaseSafeSnapshotID,
		BasePartitionRoot:        result.BasePartitionRoot,
		BasePartitionRoots:       result.BasePartitionRoots,
		SchemaSnapshotID:         result.SchemaSnapshotID,
		PromotionSeq:             result.PromotionSeq,
		PostStateRoot:            result.PostStateRoot,
		PostPartitionCommitments: result.PostPartitionCommitments,
		PartitionDeltas:          result.PartitionDeltas,
		RowsBefore:               result.RowsBefore,
		RowsAfter:                result.RowsAfter,
		RowsUpdated:              result.RowsUpdated,
		RowsDeleted:              result.RowsDeleted,
		Parts:                    result.Parts,
	}
	return replay.DigestString("mutation-replay\x00" + digestMutationClaim(claim))
}

func uniqueNonEmpty(values []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func lastIdentifier(path string) string {
	path = strings.TrimSpace(path)
	parts := strings.Split(path, ".")
	last := parts[len(parts)-1]
	return strings.Trim(last, "`")
}

// splitQualifiedTable splits a `db`.`table` (or db.table) identifier into its
// unquoted database and table parts. ok is false when the input is not a
// two-part qualified name, so callers that need a database (e.g. system.parts
// lookups) can fall back rather than guess.
func splitQualifiedTable(qualified string) (database, table string, ok bool) {
	qualified = strings.TrimSpace(qualified)
	parts := strings.Split(qualified, ".")
	if len(parts) != 2 {
		return "", "", false
	}
	db := strings.Trim(strings.TrimSpace(parts[0]), "`")
	tbl := strings.Trim(strings.TrimSpace(parts[1]), "`")
	if db == "" || tbl == "" {
		return "", "", false
	}
	return db, tbl, true
}

// attachablePartitionIDs returns the concrete partition ids that can be moved
// with ATTACH PARTITION ID. The "all" sentinel (native MVP where the physical
// partition set is unknown) and empty entries are dropped; an empty result tells
// the caller to fall back to a whole-table copy.
func attachablePartitionIDs(partitionIDs []string) []string {
	out := make([]string, 0, len(partitionIDs))
	for _, p := range partitionIDs {
		if p == "" || p == "all" {
			return nil
		}
		out = append(out, p)
	}
	return out
}

func normalizeTableID(path string) string {
	path = strings.TrimSpace(path)
	path = strings.ReplaceAll(path, "`", "")
	return path
}

func qualifiedTable(db, table string) string {
	return quoteIdent(db) + "." + quoteIdent(table)
}

func quoteIdent(v string) string {
	return "`" + strings.ReplaceAll(v, "`", "``") + "`"
}

func sqlStringLiteral(v string) string {
	return "'" + strings.ReplaceAll(v, "'", "''") + "'"
}

func sqlStringList(values []string) string {
	out := make([]string, 0, len(values))
	for _, v := range values {
		out = append(out, sqlStringLiteral(v))
	}
	return strings.Join(out, ",")
}

func scannedString(value any) string {
	switch v := value.(type) {
	case []byte:
		return string(v)
	case string:
		return v
	default:
		return fmt.Sprint(v)
	}
}

func tableIDFromManifest(manifest replay.SafeSnapshotManifest) string {
	if len(manifest.Tables) == 1 {
		return manifest.Tables[0].TableID
	}
	return ""
}

func manifestActiveParts(manifest replay.SafeSnapshotManifest, tableID string, partitions []string) []replay.PartManifestEntry {
	partitionSet := map[string]struct{}{}
	for _, partition := range partitions {
		partitionSet[partition] = struct{}{}
	}
	var out []replay.PartManifestEntry
	for _, table := range manifest.Tables {
		if tableID != "" && table.TableID != tableID {
			continue
		}
		for _, part := range table.ActiveParts {
			if len(partitionSet) > 0 {
				if _, ok := partitionSet[part.PartitionID]; !ok {
					continue
				}
			}
			out = append(out, part)
		}
	}
	return sortedManifestParts(out)
}

func activePartsEqual(actual, expected []replay.PartManifestEntry) bool {
	actual = sortedManifestParts(actual)
	expected = sortedManifestParts(expected)
	if len(actual) != len(expected) {
		return false
	}
	for i := range actual {
		a := actual[i]
		e := expected[i]
		if a.TableID != e.TableID ||
			a.PartitionID != e.PartitionID ||
			a.PartName != e.PartName ||
			a.PartRowLtHash != e.PartRowLtHash ||
			a.RowCount != e.RowCount {
			return false
		}
		if e.PartPhysHash != "" && a.PartPhysHash != e.PartPhysHash {
			return false
		}
		if e.Bytes != 0 && a.Bytes != e.Bytes {
			return false
		}
	}
	return true
}

func digestManifestParts(parts []replay.PartManifestEntry) string {
	type part struct {
		TableID       string `json:"table_id"`
		PartitionID   string `json:"partition_id"`
		PartName      string `json:"part_name"`
		PartRowLtHash string `json:"part_row_lthash"`
		RowCount      uint64 `json:"row_count"`
	}
	values := make([]part, 0, len(parts))
	for _, p := range sortedManifestParts(parts) {
		values = append(values, part{
			TableID:       p.TableID,
			PartitionID:   p.PartitionID,
			PartName:      p.PartName,
			PartRowLtHash: p.PartRowLtHash,
			RowCount:      p.RowCount,
		})
	}
	return replay.DigestString("manifest-active-parts\x00" + fmt.Sprint(values))
}

func sortedManifestParts(parts []replay.PartManifestEntry) []replay.PartManifestEntry {
	out := append([]replay.PartManifestEntry(nil), parts...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].TableID != out[j].TableID {
			return out[i].TableID < out[j].TableID
		}
		if out[i].PartitionID != out[j].PartitionID {
			return out[i].PartitionID < out[j].PartitionID
		}
		return out[i].PartName < out[j].PartName
	})
	return out
}

func rewriteAlterTableTarget(sql, safeTable, scratch string) (string, error) {
	safeTable = strings.TrimSpace(safeTable)
	if safeTable == "" || scratch == "" {
		return "", fmt.Errorf("safe and scratch table names are required")
	}
	re := regexp.MustCompile(`(?is)^(\s*ALTER\s+TABLE\s+)` + regexp.QuoteMeta(safeTable) + `(\s+)(.*)$`)
	match := re.FindStringSubmatch(sql)
	if len(match) != 4 {
		return "", fmt.Errorf("mutation sql %q does not target safe table %s", sql, safeTable)
	}
	return match[1] + scratch + match[2] + match[3], nil
}

// ensureMutationSync makes the ALTER ... UPDATE/DELETE wait for the mutation
// to materialize by forcing mutations_sync. It merges into an existing SETTINGS
// clause instead of skipping it, so a user-supplied SETTINGS clause can never
// leave the mutation running asynchronously (which would break the read-back
// commitment). syncValue < 0 is treated as the default of 2.
func ensureMutationSync(sql string, syncValue int) string {
	if syncValue < 0 {
		syncValue = 2
	}
	setting := fmt.Sprintf("mutations_sync = %d", syncValue)
	idx := lastSettingsIndex(sql)
	if idx < 0 {
		return sql + " SETTINGS " + setting
	}
	head := sql[:idx]
	tail := sql[idx+len(" SETTINGS "):]
	// If the caller already set mutations_sync, override it; otherwise prepend
	// ours so it takes effect regardless of the other settings.
	if mutationsSyncPattern.MatchString(tail) {
		tail = mutationsSyncPattern.ReplaceAllString(tail, setting)
		return head + " SETTINGS " + tail
	}
	return head + " SETTINGS " + setting + ", " + tail
}

var mutationsSyncPattern = regexp.MustCompile(`(?i)mutations_sync\s*=\s*\d+`)

// lastSettingsIndex returns the byte offset of the last " SETTINGS " token in
// sql (case-insensitive), or -1 if none is present.
func lastSettingsIndex(sql string) int {
	upper := strings.ToUpper(sql)
	return strings.LastIndex(upper, " SETTINGS ")
}

func scanHashRowValues(rows HashRows, types []HashColumnType) ([]any, error) {
	values := make([]any, len(types))
	dest := make([]any, len(types))
	setters := make([]func(), len(types))
	for i, typ := range types {
		dest[i], setters[i] = scanDestForType(typ.DatabaseTypeName(), func(v any) {
			values[i] = v
		})
	}
	if err := rows.Scan(dest...); err != nil {
		return nil, err
	}
	for _, set := range setters {
		set()
	}
	return values, nil
}

func scanDestForType(typeName string, set func(any)) (any, func()) {
	switch {
	case typeName == "UInt8":
		var v uint8
		return &v, func() { set(v) }
	case typeName == "UInt16":
		var v uint16
		return &v, func() { set(v) }
	case typeName == "UInt32":
		var v uint32
		return &v, func() { set(v) }
	case strings.HasPrefix(typeName, "UInt"):
		var v uint64
		return &v, func() { set(v) }
	case typeName == "Int8":
		var v int8
		return &v, func() { set(v) }
	case typeName == "Int16":
		var v int16
		return &v, func() { set(v) }
	case typeName == "Int32":
		var v int32
		return &v, func() { set(v) }
	case strings.HasPrefix(typeName, "Int"):
		var v int64
		return &v, func() { set(v) }
	case strings.HasPrefix(typeName, "FixedString"):
		var v []byte
		return &v, func() { set(append([]byte(nil), v...)) }
	case typeName == "String":
		var v string
		return &v, func() { set(v) }
	case strings.HasPrefix(typeName, "Date"):
		var v time.Time
		return &v, func() { set(v) }
	case typeName == "Bool":
		var v bool
		return &v, func() { set(v) }
	case typeName == "Float32":
		var v float32
		return &v, func() { set(v) }
	case typeName == "Float64":
		var v float64
		return &v, func() { set(v) }
	default:
		var v string
		return &v, func() { set(v) }
	}
}

func normalizeScannedValue(typeName string, value any) (any, error) {
	switch {
	case strings.HasPrefix(typeName, "UInt"):
		return normalizeUnsigned(typeName, value)
	case strings.HasPrefix(typeName, "Int"):
		return normalizeSigned(typeName, value)
	case typeName == "String" || strings.HasPrefix(typeName, "FixedString"):
		switch v := value.(type) {
		case string:
			return v, nil
		case []byte:
			return append([]byte(nil), v...), nil
		default:
			return nil, fmt.Errorf("unsupported string value type %T", value)
		}
	case typeName == "Bool":
		if v, ok := value.(bool); ok {
			return v, nil
		}
		return nil, fmt.Errorf("unsupported bool value type %T", value)
	case typeName == "Float32":
		switch v := value.(type) {
		case float32:
			return v, nil
		case float64:
			return float32(v), nil
		default:
			return nil, fmt.Errorf("unsupported Float32 value type %T", value)
		}
	case typeName == "Float64":
		switch v := value.(type) {
		case float32:
			return float64(v), nil
		case float64:
			return v, nil
		default:
			return nil, fmt.Errorf("unsupported Float64 value type %T", value)
		}
	case strings.HasPrefix(typeName, "Date"):
		if v, ok := value.(time.Time); ok {
			return v, nil
		}
		return nil, fmt.Errorf("unsupported date/time value type %T", value)
	default:
		return value, nil
	}
}

func normalizeUnsigned(typeName string, value any) (any, error) {
	var v uint64
	switch x := value.(type) {
	case uint8:
		v = uint64(x)
	case uint16:
		v = uint64(x)
	case uint32:
		v = uint64(x)
	case uint64:
		v = x
	case int:
		if x < 0 {
			return nil, fmt.Errorf("negative value for %s", typeName)
		}
		v = uint64(x)
	case int64:
		if x < 0 {
			return nil, fmt.Errorf("negative value for %s", typeName)
		}
		v = uint64(x)
	default:
		return nil, fmt.Errorf("unsupported unsigned value type %T", value)
	}
	switch typeName {
	case "UInt8":
		return uint8(v), nil
	case "UInt16":
		return uint16(v), nil
	case "UInt32":
		return uint32(v), nil
	default:
		return v, nil
	}
}

func normalizeSigned(typeName string, value any) (any, error) {
	var v int64
	switch x := value.(type) {
	case int8:
		v = int64(x)
	case int16:
		v = int64(x)
	case int32:
		v = int64(x)
	case int64:
		v = x
	case int:
		v = int64(x)
	default:
		return nil, fmt.Errorf("unsupported signed value type %T", value)
	}
	switch typeName {
	case "Int8":
		return int8(v), nil
	case "Int16":
		return int16(v), nil
	case "Int32":
		return int32(v), nil
	default:
		return v, nil
	}
}

func digestParts(domain string, parts []ByteSidePart) string {
	type part struct {
		PartitionID   string `json:"partition_id"`
		PartName      string `json:"part_name"`
		RowCount      uint64 `json:"row_count"`
		PartRowLtHash string `json:"part_row_lthash"`
	}
	values := make([]part, 0, len(parts))
	for _, p := range parts {
		values = append(values, part{
			PartitionID:   p.PartitionID,
			PartName:      p.PartName,
			RowCount:      p.RowCount,
			PartRowLtHash: p.PartRowLtHash,
		})
	}
	return replay.DigestString(domain + "\x00" + fmt.Sprint(values))
}
