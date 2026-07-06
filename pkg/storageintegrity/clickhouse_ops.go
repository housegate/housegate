package storageintegrity

import (
	"context"
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

type ActivePartReader interface {
	ReadActiveParts(ctx context.Context, table string, partitions []string) ([]replay.PartManifestEntry, error)
}

type PartitionRootReader interface {
	CurrentPartitionRoot(ctx context.Context, table, partitionID string) (string, error)
}

type PromotionSeqStore interface {
	LastPromotionSeq(ctx context.Context, table, partitionID string) (uint64, error)
	RecordPromotionSeq(ctx context.Context, table, partitionID string, seq uint64) error
}

type HashQueryConn interface {
	Query(ctx context.Context, query string, args ...any) (HashRows, error)
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
		partHash := ph.acc.Hex()
		parts = append(parts, ByteSidePart{
			PartitionID:   id,
			PartName:      "hash-scan-" + unsafeIdentChars.ReplaceAllString(id, "_"),
			RowCount:      ph.rows,
			PartRowLtHash: partHash,
		})
	}
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

func (r ClickHouseActivePartReader) ReadActiveParts(ctx context.Context, table string, partitions []string) ([]replay.PartManifestEntry, error) {
	if r.Conn == nil {
		return nil, fmt.Errorf("clickhouse query connection is required")
	}
	tableID := r.TableID
	if tableID == "" {
		tableID = normalizeTableID(table)
	}
	query := "SELECT _partition_id, _part, * FROM " + table
	if len(partitions) > 0 {
		query += " WHERE _partition_id IN (" + sqlStringList(partitions) + ")"
	}
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
			TableID:       tableID,
			PartitionID:   ph.partitionID,
			PartName:      ph.partName,
			PartRowLtHash: ph.acc.Hex(),
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
}

func (p ClickHousePromoter) Promote(ctx context.Context, task PromotionTask) (PromotionResult, error) {
	if p.Conn == nil {
		return PromotionResult{}, fmt.Errorf("clickhouse connection is required")
	}
	if task.SafeTable == "" {
		return PromotionResult{}, fmt.Errorf("promotion safe_table is required")
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
	if task.UnsafeTable != "" && len(task.PartitionIDs) > 0 {
		for _, partitionID := range task.PartitionIDs {
			sql := fmt.Sprintf("ALTER TABLE %s ATTACH PARTITION ID %s FROM %s", task.SafeTable, sqlStringLiteral(partitionID), task.UnsafeTable)
			if err := p.Conn.Exec(ctx, sql); err != nil {
				return PromotionResult{}, fmt.Errorf("attach partition %s: %w", partitionID, err)
			}
		}
		return p.promotionResult(ctx, task)
	}
	if task.UnsafeTable == "" {
		return PromotionResult{}, fmt.Errorf("insert promotion unsafe_table is required")
	}
	if err := p.Conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", task.SafeTable, task.UnsafeTable)); err != nil {
		return PromotionResult{}, fmt.Errorf("insert promotion copy: %w", err)
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
	if task.RequirePromotionSeq {
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
	if task.RequireBaseRootCAS || task.BasePartitionRoot != "" {
		if p.PartitionRoots == nil {
			return fmt.Errorf("partition root reader is required for base-root CAS")
		}
		for _, partitionID := range task.PartitionIDs {
			current, err := p.PartitionRoots.CurrentPartitionRoot(ctx, task.SafeTable, partitionID)
			if err != nil {
				return fmt.Errorf("read current partition root %s: %w", partitionID, err)
			}
			if current != task.BasePartitionRoot {
				return fmt.Errorf("base partition root mismatch for partition %s: got %s want %s", partitionID, current, task.BasePartitionRoot)
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
	shadow := qualifiedTable(promoteDB, promoteShadowTableName(task.SafeTable, task.PromotionID, task.PromotionSeq))
	if err := p.Conn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+quoteIdent(promoteDB)); err != nil {
		return PromotionResult{}, fmt.Errorf("create promote database: %w", err)
	}
	if err := p.Conn.Exec(ctx, "DROP TABLE IF EXISTS "+shadow); err != nil {
		return PromotionResult{}, fmt.Errorf("drop stale promote table: %w", err)
	}
	if err := p.Conn.Exec(ctx, "CREATE TABLE "+shadow+" AS "+task.SafeTable); err != nil {
		return PromotionResult{}, fmt.Errorf("create promote table: %w", err)
	}
	for _, partitionID := range task.PartitionIDs {
		if shouldAttachBasePartition(task) {
			if err := p.Conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s ATTACH PARTITION ID %s FROM %s", shadow, sqlStringLiteral(partitionID), task.SafeTable)); err != nil {
				return PromotionResult{}, fmt.Errorf("attach base partition %s: %w", partitionID, err)
			}
		}
		if err := p.attachCandidateParts(ctx, shadow, sourceTable, partitionID, task.CandidateParts); err != nil {
			return PromotionResult{}, err
		}
	}
	if task.RequirePostRootCAS {
		if p.ActiveParts == nil {
			return PromotionResult{}, fmt.Errorf("active part reader is required for post-root CAS")
		}
		shadowParts, err := p.ActiveParts.ReadActiveParts(ctx, shadow, task.PartitionIDs)
		if err != nil {
			return PromotionResult{}, fmt.Errorf("read promote active parts: %w", err)
		}
		for _, partitionID := range task.PartitionIDs {
			got := partitionRootFromActiveParts(shadowParts, partitionID)
			if got != task.ExpectedPostRoot {
				return PromotionResult{}, fmt.Errorf("post partition root mismatch for partition %s: got %s want %s", partitionID, got, task.ExpectedPostRoot)
			}
		}
	}
	for _, partitionID := range task.PartitionIDs {
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
		if err := p.Conn.Exec(ctx, "DROP TABLE IF EXISTS "+shadow); err != nil {
			return PromotionResult{}, fmt.Errorf("drop promote table: %w", err)
		}
	}
	return result, nil
}

func (p ClickHousePromoter) attachCandidateParts(ctx context.Context, shadow, sourceTable, partitionID string, candidates []ByteSidePart) error {
	partNames := candidatePartNames(candidates, partitionID)
	if len(partNames) == 0 {
		if err := p.Conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s ATTACH PARTITION ID %s FROM %s", shadow, sqlStringLiteral(partitionID), sourceTable)); err != nil {
			return fmt.Errorf("attach candidate partition %s: %w", partitionID, err)
		}
		return nil
	}
	sql := fmt.Sprintf("INSERT INTO %s SELECT * FROM %s WHERE _partition_id = %s AND _part IN (%s)",
		shadow, sourceTable, sqlStringLiteral(partitionID), sqlStringList(partNames))
	if err := p.Conn.Exec(ctx, sql); err != nil {
		return fmt.Errorf("attach candidate parts for partition %s: %w", partitionID, err)
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
	parts, err := p.ActiveParts.ReadActiveParts(ctx, task.SafeTable, task.PartitionIDs)
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

func candidatePartNames(candidates []ByteSidePart, partitionID string) []string {
	seen := map[string]struct{}{}
	var out []string
	for _, part := range candidates {
		if part.PartName == "" || isLogicalHashScanPart(part.PartName) {
			continue
		}
		if part.PartitionID != "" && part.PartitionID != partitionID {
			continue
		}
		if _, ok := seen[part.PartName]; ok {
			continue
		}
		seen[part.PartName] = struct{}{}
		out = append(out, part.PartName)
	}
	sort.Strings(out)
	return out
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

func promoteShadowTableName(safeTable, promotionID string, promotionSeq uint64) string {
	base := lastIdentifier(safeTable)
	id := unsafeIdentChars.ReplaceAllString(promotionID, "_")
	name := base + "_" + id
	if promotionSeq != 0 {
		name = fmt.Sprintf("%s_%d", name, promotionSeq)
	}
	return unsafeIdentChars.ReplaceAllString(name, "_")
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
	return digestManifestParts(filtered)
}

type ClickHouseMutationExecutor struct {
	Conn            SQLConn
	Hasher          TableHasher
	ActiveParts     ActivePartReader
	ClaimSigner     MutationClaimSigner
	WorkerID        string
	ScratchDatabase string
}

func (e ClickHouseMutationExecutor) ExecuteMutation(ctx context.Context, task MutationTask) (MutationClaim, error) {
	before, hash, scratch, err := e.execute(ctx, task, "claim")
	if err != nil {
		return MutationClaim{}, err
	}
	evidence := buildMutationEvidence(task, before, hash)
	claim := MutationClaim{
		StatementID:              task.StatementID,
		WorkerID:                 e.WorkerID,
		ScratchTable:             scratch,
		BaseSafeSnapshotID:       task.BaseSafeSnapshotID,
		BasePartitionRoot:        task.BasePartitionRoot,
		BasePartitionRoots:       evidence.baseRoots,
		SchemaSnapshotID:         task.SchemaSnapshotID,
		PromotionSeq:             task.PromotionSeq,
		PostStateRoot:            hash.StateRoot,
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
	result := MutationReplayResult{
		StatementID:              task.StatementID,
		WorkerID:                 e.WorkerID,
		BaseSafeSnapshotID:       task.BaseSafeSnapshotID,
		BasePartitionRoot:        task.BasePartitionRoot,
		BasePartitionRoots:       evidence.baseRoots,
		SchemaSnapshotID:         task.SchemaSnapshotID,
		PromotionSeq:             task.PromotionSeq,
		PostStateRoot:            hash.StateRoot,
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
	before, err := e.Hasher.HashTable(ctx, task.SafeTable, task.PartitionIDs)
	if err != nil {
		return TableHash{}, TableHash{}, "", fmt.Errorf("hash mutation base %s: %w", task.SafeTable, err)
	}
	if before.StateRoot == "" {
		before.StateRoot = digestParts("mutation-base-state", before.Parts)
	}
	scratch := qualifiedTable(e.ScratchDatabase, scratchTableName(task.SafeTable, task.StatementID, e.WorkerID, purpose))
	mutationSQL, err := rewriteAlterTableTarget(task.MutationSQL, task.SafeTable, scratch)
	if err != nil {
		return TableHash{}, TableHash{}, "", err
	}
	mutationSQL = ensureMutationSync(mutationSQL)
	sqls := []string{
		"CREATE DATABASE IF NOT EXISTS " + quoteIdent(e.ScratchDatabase),
		"DROP TABLE IF EXISTS " + scratch,
		"CREATE TABLE " + scratch + " AS " + task.SafeTable,
		"INSERT INTO " + scratch + " SELECT * FROM " + task.SafeTable,
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
	hash, err := e.Hasher.HashTable(ctx, scratch, task.PartitionIDs)
	if err != nil {
		return TableHash{}, TableHash{}, "", fmt.Errorf("hash mutation scratch %s: %w", scratch, err)
	}
	if e.ActiveParts != nil {
		activeParts, err := e.ActiveParts.ReadActiveParts(ctx, scratch, task.PartitionIDs)
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

type ClickHouseRollbackExecutor struct {
	Conn SQLConn
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
		if len(task.UnsafeParts) > 0 {
			for _, part := range task.UnsafeParts {
				if part.PartName == "" {
					continue
				}
				if err := e.Conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s DROP PART %s", task.UnsafeTable, sqlStringLiteral(part.PartName))); err != nil {
					return RollbackResult{}, fmt.Errorf("drop unsafe part %s: %w", part.PartName, err)
				}
				result.CleanedUnsafeParts = append(result.CleanedUnsafeParts, part)
			}
		} else {
			for _, partitionID := range task.PartitionIDs {
				if err := e.Conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s DROP PARTITION ID %s", task.UnsafeTable, sqlStringLiteral(partitionID))); err != nil {
					return RollbackResult{}, fmt.Errorf("drop unsafe partition %s: %w", partitionID, err)
				}
				result.DroppedUnsafePartitions = append(result.DroppedUnsafePartitions, partitionID)
			}
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
	hash, err := e.Hasher.HashTable(ctx, task.SafeTable, task.PartitionIDs)
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
	parts, err := e.ActiveParts.ReadActiveParts(ctx, task.SafeTable, task.PartitionIDs)
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
			current, err := c.PartitionRoots.CurrentPartitionRoot(ctx, task.SafeTable, partitionID)
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
		compactParts, err := c.ActiveParts.ReadActiveParts(ctx, compactTable, task.PartitionIDs)
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
		parts, err := c.ActiveParts.ReadActiveParts(ctx, task.SafeTable, task.PartitionIDs)
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
	Hasher   TableHasher
	WorkerID string
}

func (s HashingByteSideScanner) ScanByteSide(ctx context.Context, task ByteSideScanTask) (ByteSideScanResult, error) {
	if s.Hasher == nil {
		return ByteSideScanResult{}, fmt.Errorf("table hasher is required")
	}
	hash, err := s.Hasher.HashTable(ctx, task.UnsafeTable, task.PartitionIDs)
	if err != nil {
		return ByteSideScanResult{}, err
	}
	return ByteSideScanResult{
		ScanID:      task.ScanID,
		StatementID: task.StatementID,
		TableID:     task.TableID,
		UnsafeTable: task.UnsafeTable,
		WorkerID:    s.WorkerID,
		Parts:       hash.Parts,
		PartSetHash: digestParts("byte-side-part-set", hash.Parts),
	}, nil
}

type ClickHouseSafeAuditor struct {
	Hasher      TableHasher
	ActiveParts ActivePartReader
	WorkerID    string
}

func (a ClickHouseSafeAuditor) AuditSafe(ctx context.Context, task SafeAuditTask) (SafeAuditVote, error) {
	if a.Hasher == nil {
		return SafeAuditVote{}, fmt.Errorf("table hasher is required")
	}
	expectedStateRoot := task.StateRoot
	if expectedStateRoot == "" {
		expectedStateRoot = task.Manifest.StateRoot
	}
	hash, err := a.Hasher.HashTable(ctx, task.SafeTable, task.PartitionIDs)
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
	if a.ActiveParts == nil {
		vote.Match = false
		vote.ActivePartsMatch = false
		vote.Error = "active part reader is required for manifest audit"
		return vote, nil
	}
	parts, err := a.ActiveParts.ReadActiveParts(ctx, task.SafeTable, task.PartitionIDs)
	if err != nil {
		return SafeAuditVote{}, fmt.Errorf("read safe active parts: %w", err)
	}
	tableID := firstNonEmptyString(task.TableID, tableIDFromManifest(task.Manifest), normalizeTableID(task.SafeTable))
	expectedParts := manifestActiveParts(task.Manifest, tableID, task.PartitionIDs)
	vote.ActiveParts = parts
	vote.ActivePartsMatch = activePartsEqual(parts, expectedParts)
	vote.Match = vote.Match && vote.ActivePartsMatch
	if !vote.ActivePartsMatch {
		vote.Error = "active parts do not match manifest"
	}
	return vote, nil
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
			delta.RowsUpdated = rowsAfter
		case MutationTypeDelete:
			if task.InternalDropPartition {
				delta.RowsDeleted = rowsBefore
			} else if rowsBefore >= rowsAfter {
				delta.RowsDeleted = rowsBefore - rowsAfter
			}
		}
		delta.DeltaRoot = digestMutationDelta(delta)
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
	return digestParts("byte-side-partition-root", sortedByteSideParts(filtered))
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

func ensureMutationSync(sql string) string {
	if strings.Contains(strings.ToUpper(sql), " SETTINGS ") {
		return sql
	}
	return sql + " SETTINGS mutations_sync = 2"
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
