package storageintegrity

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/ClickHouse/ch-go/proto"

	"housegate/housegate/pkg/chproto"
	"housegate/housegate/pkg/lthash"
	"housegate/housegate/pkg/replay"
	"housegate/housegate/pkg/replay/payloadexec"
)

const (
	PayloadEncodingClickHouseNativeData = "clickhouse-native-data-v1"
	NativePayloadExecutorProfileID      = "housegate-native-data-mvp-v1"
)

var DefaultReplaySettingsHash = replay.DigestString("settings")

// NativePayloadClaim is the source-side commitment HouseGate can compute from
// the Native protocol Data packets it already captures for a payload-local
// INSERT.
type NativePayloadClaim struct {
	PayloadEncoding string          `json:"payload_encoding"`
	PayloadRevision int             `json:"payload_revision"`
	Columns         []lthash.Column `json:"columns"`
	SourceClaimRoot string          `json:"source_claim_root"`
	PartRowLtHash   string          `json:"part_row_lthash"`
	RowCount        uint64          `json:"row_count"`
	Bytes           uint64          `json:"bytes"`
}

// InjectNativeRowIDs rewrites one ClickHouse Native ClientData packet by
// prepending the protocol `_hg_row_id FixedString(32)` column. The ids use the
// same deterministic derivation as replay/payloadexec so online writes and
// replay attestations commit to identical row instances.
func InjectNativeRowIDs(networkID, tableID, statementID string, revision int, raw []byte, startingOrdinal uint64) ([]byte, uint64, error) {
	if networkID == "" {
		return nil, 0, fmt.Errorf("network_id is required")
	}
	if tableID == "" {
		return nil, 0, fmt.Errorf("table_id is required")
	}
	if statementID == "" {
		return nil, 0, fmt.Errorf("statement_id is required")
	}
	if revision == 0 {
		return nil, 0, fmt.Errorf("client protocol revision is required")
	}
	r := proto.NewReader(bytes.NewReader(raw))
	code, err := r.UVarInt()
	if err != nil {
		return nil, 0, fmt.Errorf("native packet code: %w", err)
	}
	if code != uint64(chproto.ClientDataCode) {
		return nil, 0, fmt.Errorf("packet type %d is not ClientData", code)
	}
	blockName, err := r.Str()
	if err != nil {
		return nil, 0, fmt.Errorf("native block name: %w", err)
	}

	var (
		results proto.Results
		block   proto.Block
	)
	if err := block.DecodeBlock(r, revision, results.Auto()); err != nil {
		return nil, 0, fmt.Errorf("decode native block: %w", err)
	}
	if block.Rows == 0 {
		return append([]byte(nil), raw...), 0, nil
	}
	for _, rc := range results {
		if strings.EqualFold(rc.Name, "_hg_row_id") {
			return nil, 0, fmt.Errorf("native payload already contains reserved _hg_row_id column")
		}
	}

	rowIDs := &proto.ColFixedStr{Size: 32}
	for i := 0; i < block.Rows; i++ {
		rowIDs.Append(payloadexec.RowID(networkID, tableID, statementID, startingOrdinal+uint64(i)))
	}
	input := make(proto.Input, 0, len(results)+1)
	input = append(input, proto.InputColumn{Name: "_hg_row_id", Data: rowIDs})
	for _, rc := range results {
		col := rc.Data
		if auto, ok := col.(*proto.ColAuto); ok {
			col = auto.Data
		}
		in, ok := col.(proto.ColInput)
		if !ok {
			return nil, 0, fmt.Errorf("column %q (%T) cannot be re-encoded as native input", rc.Name, col)
		}
		input = append(input, proto.InputColumn{Name: rc.Name, Data: in})
	}

	var out proto.Buffer
	out.PutUVarInt(uint64(chproto.ClientDataCode))
	out.PutString(blockName)
	if err := (proto.Block{Rows: block.Rows, Columns: len(input)}).EncodeBlock(&out, revision, input); err != nil {
		return nil, 0, fmt.Errorf("encode native block with _hg_row_id: %w", err)
	}
	return append([]byte(nil), out.Buf...), uint64(block.Rows), nil
}

// StripNativeRowIDFromServerData removes the protocol `_hg_row_id` column from
// a zero-row server Data sample packet before it is sent back to an ordinary
// ClickHouse client. Agent-side HouseGate uses this to keep client batch
// schemas stable while still injecting `_hg_row_id` into later client Data
// packets on the way upstream.
func StripNativeRowIDFromServerData(raw []byte, revision int) ([]byte, bool, error) {
	if revision == 0 {
		return nil, false, fmt.Errorf("client protocol revision is required")
	}
	r := proto.NewReader(bytes.NewReader(raw))
	code, err := r.UVarInt()
	if err != nil {
		return nil, false, fmt.Errorf("native packet code: %w", err)
	}
	if code != uint64(chproto.ServerDataCode) {
		return append([]byte(nil), raw...), false, nil
	}
	blockName, err := r.Str()
	if err != nil {
		return nil, false, fmt.Errorf("native block name: %w", err)
	}

	var (
		results proto.Results
		block   proto.Block
	)
	if err := block.DecodeBlock(r, revision, results.Auto()); err != nil {
		return nil, false, fmt.Errorf("decode native block: %w", err)
	}
	if block.Rows != 0 {
		return append([]byte(nil), raw...), false, nil
	}

	input := make(proto.Input, 0, len(results))
	stripped := false
	for _, rc := range results {
		if strings.EqualFold(rc.Name, "_hg_row_id") {
			stripped = true
			continue
		}
		col := rc.Data
		if auto, ok := col.(*proto.ColAuto); ok {
			col = auto.Data
		}
		in, ok := col.(proto.ColInput)
		if !ok {
			return nil, false, fmt.Errorf("column %q (%T) cannot be re-encoded as native input", rc.Name, col)
		}
		input = append(input, proto.InputColumn{Name: rc.Name, Data: in})
	}
	if !stripped {
		return append([]byte(nil), raw...), false, nil
	}

	var out proto.Buffer
	out.PutUVarInt(uint64(chproto.ServerDataCode))
	out.PutString(blockName)
	if err := (proto.Block{Rows: 0, Columns: len(input)}).EncodeBlock(&out, revision, input); err != nil {
		return nil, false, fmt.Errorf("encode native block without _hg_row_id: %w", err)
	}
	return append([]byte(nil), out.Buf...), true, nil
}

// ComputeNativePayloadClaim decodes one or more concatenated ClickHouse Native
// ClientData packets and returns the deterministic row LtHash commitment.
func ComputeNativePayloadClaim(tableID string, revision int, payload []byte) (NativePayloadClaim, error) {
	claim, _, err := computeNativePayloadClaim(tableID, revision, payload)
	return claim, err
}

func computeNativePayloadClaim(tableID string, revision int, payload []byte) (NativePayloadClaim, *lthash.Hash, error) {
	if tableID == "" {
		return NativePayloadClaim{}, nil, fmt.Errorf("table_id is required")
	}
	if revision == 0 {
		return NativePayloadClaim{}, nil, fmt.Errorf("client protocol revision is required")
	}
	acc := lthash.New()
	var (
		columns []lthash.Column
		rows    uint64
	)
	pr := proto.NewReader(bytes.NewReader(payload))
	for {
		code, err := pr.UVarInt()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return NativePayloadClaim{}, nil, fmt.Errorf("native packet code: %w", err)
		}
		block, err := decodeNativeDataBlock(pr, revision, code)
		if err != nil {
			return NativePayloadClaim{}, nil, err
		}
		if block.Rows == 0 {
			continue
		}
		if len(columns) == 0 {
			columns = append([]lthash.Column(nil), block.Columns...)
		} else if !sameColumns(columns, block.Columns) {
			return NativePayloadClaim{}, nil, fmt.Errorf("native payload schema changed across data blocks")
		}
		for i := 0; i < block.Rows; i++ {
			values, err := block.Row(i)
			if err != nil {
				return NativePayloadClaim{}, nil, err
			}
			h, err := lthash.RowHash(tableID, block.Columns, values)
			if err != nil {
				return NativePayloadClaim{}, nil, err
			}
			acc.AddHash(h)
			rows++
		}
	}
	dataRoot := nativeDataRootAfter(acc)
	return NativePayloadClaim{
		PayloadEncoding: PayloadEncodingClickHouseNativeData,
		PayloadRevision: revision,
		Columns:         columns,
		// SourceClaimRoot here is the data-side commitment only; callers that
		// know the schema/executor identity (submit, the replay executor) wrap
		// it via compositeStateRoot to produce the spec's R_source.
		SourceClaimRoot: dataRoot,
		PartRowLtHash:   dataRoot,
		RowCount:        rows,
		Bytes:           uint64(len(payload)),
	}, acc, nil
}

// NativePayloadGenesisSnapshot returns the empty safe snapshot used by the MVP
// Native Data replay executor for one table schema.
func NativePayloadGenesisSnapshot(tableID string, columns []lthash.Column) (replay.SafeSnapshotManifest, error) {
	if tableID == "" {
		return replay.SafeSnapshotManifest{}, fmt.Errorf("table_id is required")
	}
	schemaHash := nativeSchemaHash(tableID, columns)
	schemaRoot := replay.DigestString("native-schema-root\x00" + tableID + "\x00" + schemaHash)
	return (replay.SafeSnapshotManifest{
		SafeL3BlockSeq:      0,
		SchemaSnapshotID:  replay.DigestString("native-schema-snapshot\x00" + schemaRoot),
		SchemaRoot:        schemaRoot,
		ExecutorProfileID: NativePayloadExecutorProfileID,
		Tables: []replay.TableManifest{{
			TableID:    tableID,
			SchemaHash: schemaHash,
		}},
	}).Seal()
}

// NativePayloadExecutor is a replay.Executor over captured ClickHouse Native
// ClientData packets. It is intentionally MVP-scoped to INSERT payloads with
// scalar columns supported by pkg/lthash.
type NativePayloadExecutor struct {
	Revision int
}

func (e NativePayloadExecutor) Replay(ctx context.Context, req replay.ExecutionRequest) (replay.ExecutionResult, error) {
	if err := ctx.Err(); err != nil {
		return replay.ExecutionResult{}, err
	}
	if e.Revision == 0 {
		return replay.ExecutionResult{}, fmt.Errorf("native payload executor revision is required")
	}
	if len(req.Job.Statements) == 0 {
		return replay.ExecutionResult{}, fmt.Errorf("native payload executor requires at least one statement")
	}

	// HG-P0-01: carry the pinned prev-safe base forward per (table, partition)
	// instead of accumulating this batch from an empty accumulator. The base is
	// the additive partition root recorded in the pinned snapshot; folding this
	// block's rows on top yields partition_root_after = partition_root_before +
	// Σ candidate part_row_lthash (spec §6.2). The Native MVP has no partition
	// split yet, so every statement folds into the "all" partition of its table.
	base, err := newNativePartitionAccumulators(req.Snapshot)
	if err != nil {
		return replay.ExecutionResult{}, err
	}

	var affected []replay.PartManifestEntry
	for _, st := range req.Statements {
		claim, acc, err := computeNativePayloadClaim(st.TargetTableID, e.Revision, st.Payload)
		if err != nil {
			return replay.ExecutionResult{}, fmt.Errorf("statement %s: %w", st.StatementID, err)
		}
		base.add(st.TargetTableID, nativeAllPartitionID, acc)
		affected = append(affected, replay.PartManifestEntry{
			TableID:       st.TargetTableID,
			PartitionID:   nativeAllPartitionID,
			PartName:      fmt.Sprintf("all-b%d-s%d", req.Job.BlockSeq, st.StatementSeq),
			PartPhysHash:  replay.DigestString("native-part-phys\x00" + st.StatementID + "\x00" + claim.PartRowLtHash),
			PartRowLtHash: claim.PartRowLtHash,
			RowCount:      claim.RowCount,
			Bytes:         claim.Bytes,
		})
	}

	// HG-P0-01: derive the data/state root through the single canonical
	// assembly (replay.AssembleStateRoot → ComputeDataRoot/ComputeStateRoot,
	// domain safe-snapshot-data-v2 / safe-snapshot-state) shared by source,
	// verifier, and manifest. schema_root comes from the pinned snapshot so the
	// state root binds schema_snapshot_id, schema_root, executor_profile_id, and
	// data_root_after exactly as the spec's post_state_root formula requires.
	tables, commitments := base.tables(req.Snapshot.SchemaRoot, affected)
	dataRoot, stateRoot, err := replay.AssembleStateRoot(
		req.Job.SchemaSnapshotID,
		req.Snapshot.SchemaRoot,
		req.Job.ExecutorProfileID,
		tables,
	)
	if err != nil {
		return replay.ExecutionResult{}, fmt.Errorf("assemble native state root: %w", err)
	}
	_ = dataRoot
	return replay.ExecutionResult{
		BlockSeq:                  req.Job.BlockSeq,
		PrevSafeSnapshotID:        req.Job.PrevSafeSnapshotID,
		PrevStateRoot:             req.Job.PrevStateRoot,
		SchemaSnapshotID:          req.Job.SchemaSnapshotID,
		ExecutorProfileID:         req.Job.ExecutorProfileID,
		ComputedStateRoot:         stateRoot,
		PartitionCommitmentsAfter: commitments,
		AffectedParts:             sortedNativeParts(affected),
		ReplayLogHash:             replay.DigestString(fmt.Sprintf("native-replay-log\x00%d\x00%d", req.Job.BlockSeq, len(affected))),
	}, nil
}

// nativeAllPartitionID is the single-partition sentinel used by the Native MVP
// executor, which does not yet split rows across ClickHouse partitions.
const nativeAllPartitionID = "all"

// nativePartitionAccumulators carries per-(table, partition) LtHash
// accumulators seeded from a pinned snapshot's partition roots, so replay folds
// this block's rows on top of the existing safe state (base-carry, spec §6.2).
type nativePartitionAccumulators struct {
	// order preserves first-seen (table, partition) order for deterministic
	// TableManifest assembly independent of Go map iteration.
	order []nativePartitionKey
	accs  map[nativePartitionKey]*lthash.Hash
}

type nativePartitionKey struct {
	tableID     string
	partitionID string
}

// newNativePartitionAccumulators seeds accumulators from the pinned snapshot's
// per-partition roots (raw additive accumulator hex). An empty/absent root is
// treated as a zero accumulator (genesis base).
func newNativePartitionAccumulators(snap replay.SafeSnapshotManifest) (*nativePartitionAccumulators, error) {
	b := &nativePartitionAccumulators{accs: map[nativePartitionKey]*lthash.Hash{}}
	for _, t := range snap.Tables {
		for _, pr := range t.PartitionRoots {
			tableID := pr.TableID
			if tableID == "" {
				tableID = t.TableID
			}
			acc := lthash.New()
			if pr.Root != "" {
				parsed, err := lthashAccumulatorFromHex(pr.Root)
				if err != nil {
					return nil, fmt.Errorf("seed base partition root %s/%s: %w", tableID, pr.PartitionID, err)
				}
				acc = parsed
			}
			b.set(tableID, pr.PartitionID, acc)
		}
	}
	return b, nil
}

func (b *nativePartitionAccumulators) set(tableID, partitionID string, acc *lthash.Hash) {
	key := nativePartitionKey{tableID: tableID, partitionID: partitionID}
	if _, ok := b.accs[key]; !ok {
		b.order = append(b.order, key)
	}
	b.accs[key] = acc
}

func (b *nativePartitionAccumulators) add(tableID, partitionID string, acc *lthash.Hash) {
	key := nativePartitionKey{tableID: tableID, partitionID: partitionID}
	cur, ok := b.accs[key]
	if !ok {
		cur = lthash.New()
		b.order = append(b.order, key)
		b.accs[key] = cur
	}
	cur.AddHash(acc)
}

// tables materializes the accumulators into per-table TableManifests (with
// additive partition roots) plus the flat PartitionCommitment list the
// execution result carries. schema_hash is taken from the affected parts (all
// rows of a Native block share one schema) or left empty when unknown.
func (b *nativePartitionAccumulators) tables(schemaRoot string, affected []replay.PartManifestEntry) ([]replay.TableManifest, []replay.PartitionCommitment) {
	_ = schemaRoot
	schemaByTable := map[string]string{}
	partsByTable := map[string][]replay.PartManifestEntry{}
	for _, p := range affected {
		partsByTable[p.TableID] = append(partsByTable[p.TableID], p)
	}
	byTable := map[string]*replay.TableManifest{}
	var tableOrder []string
	var commitments []replay.PartitionCommitment
	for _, key := range b.order {
		root := lthashAccumulatorHex(b.accs[key])
		commitments = append(commitments, replay.PartitionCommitment{
			TableID:     key.tableID,
			PartitionID: key.partitionID,
			Root:        root,
		})
		tm, ok := byTable[key.tableID]
		if !ok {
			tableOrder = append(tableOrder, key.tableID)
			tm = &replay.TableManifest{
				TableID:     key.tableID,
				SchemaHash:  schemaByTable[key.tableID],
				ActiveParts: partsByTable[key.tableID],
			}
			byTable[key.tableID] = tm
		}
		tm.PartitionRoots = append(tm.PartitionRoots, replay.PartitionCommitment{
			TableID:     key.tableID,
			PartitionID: key.partitionID,
			Root:        root,
		})
	}
	tables := make([]replay.TableManifest, 0, len(tableOrder))
	for _, id := range tableOrder {
		tables = append(tables, *byTable[id])
	}
	return tables, commitments
}

type nativeDataBlock struct {
	Columns []lthash.Column
	Rows    int
	cols    []proto.ColResult
}

func decodeNativeDataBlock(pr *proto.Reader, revision int, code uint64) (*nativeDataBlock, error) {
	if code != uint64(chproto.ClientDataCode) {
		return nil, fmt.Errorf("packet type %d is not ClientData", code)
	}
	if _, err := pr.Str(); err != nil {
		return nil, fmt.Errorf("native block name: %w", err)
	}
	var (
		results proto.Results
		block   proto.Block
	)
	if err := block.DecodeBlock(pr, revision, results.Auto()); err != nil {
		return nil, fmt.Errorf("decode native block: %w", err)
	}
	out := &nativeDataBlock{Rows: block.Rows}
	for _, rc := range results {
		col := rc.Data
		typeName := ""
		if auto, ok := col.(*proto.ColAuto); ok {
			typeName = string(auto.DataType)
			col = auto.Data
		} else if col != nil {
			typeName = string(col.Type())
		}
		out.Columns = append(out.Columns, lthash.Column{Name: rc.Name, Type: typeName})
		out.cols = append(out.cols, col)
	}
	return out, nil
}

func (b *nativeDataBlock) Row(i int) ([]any, error) {
	if i < 0 || i >= b.Rows {
		return nil, fmt.Errorf("row %d out of range (rows=%d)", i, b.Rows)
	}
	values := make([]any, len(b.cols))
	for c, col := range b.cols {
		v, err := nativeColumnValue(col, i)
		if err != nil {
			return nil, fmt.Errorf("column %q (%s): %w", b.Columns[c].Name, b.Columns[c].Type, err)
		}
		values[c] = v
	}
	return values, nil
}

func nativeColumnValue(col proto.ColResult, i int) (any, error) {
	switch c := col.(type) {
	case *proto.ColUInt8:
		return (*c)[i], nil
	case *proto.ColUInt16:
		return (*c)[i], nil
	case *proto.ColUInt32:
		return (*c)[i], nil
	case *proto.ColUInt64:
		return (*c)[i], nil
	case *proto.ColInt8:
		return (*c)[i], nil
	case *proto.ColInt16:
		return (*c)[i], nil
	case *proto.ColInt32:
		return (*c)[i], nil
	case *proto.ColInt64:
		return (*c)[i], nil
	case *proto.ColFloat32:
		return (*c)[i], nil
	case *proto.ColFloat64:
		return (*c)[i], nil
	case *proto.ColStr:
		return c.Row(i), nil
	case *proto.ColFixedStr:
		return append([]byte(nil), c.Row(i)...), nil
	case *proto.ColFixedStr32:
		row := c.Row(i)
		return append([]byte(nil), row[:]...), nil
	case *proto.ColBool:
		return (*c)[i], nil
	case *proto.ColDate:
		return c.Row(i), nil
	case *proto.ColDateTime:
		return c.Row(i).UTC(), nil
	case *proto.ColDateTime64:
		return c.Row(i).UTC(), nil
	default:
		return nil, fmt.Errorf("unsupported column type %T", col)
	}
}

func sameColumns(a, b []lthash.Column) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func nativeSchemaHash(tableID string, columns []lthash.Column) string {
	var b strings.Builder
	b.WriteString(tableID)
	for _, c := range columns {
		b.WriteByte(0)
		b.WriteString(c.Name)
		b.WriteByte(':')
		b.WriteString(c.Type)
	}
	return replay.DigestString("native-table-schema\x00" + b.String())
}

// nativeDataRootAfter is the data-side commitment: the raw LtHash accumulator
// over all replayed rows. It stays additive across blocks and is the value the
// partition commitment carries.
func nativeDataRootAfter(acc *lthash.Hash) string {
	return lthashAccumulatorHex(acc)
}

// compositeStateRoot binds the data-side root to the schema/executor identity,
// matching the spec's R_source = H(schema_snapshot_id ‖ executor_profile_id ‖
// data_root_after). schema_snapshot_id already commits to schema_root in the
// native profile (NativePayloadGenesisSnapshot derives it from schema_root), so
// including it transitively binds schema_root without threading a separate
// field through the ReplayJob wire type. Source (submit) and verifier
// (NativePayloadExecutor.Replay) MUST call this identically, or the replay
// source-root check fails.
func compositeStateRoot(schemaSnapshotID, executorProfileID, dataRootAfter string) string {
	return replay.DigestString(
		"native-state-root-v1\x00" +
			schemaSnapshotID + "\x00" +
			executorProfileID + "\x00" +
			dataRootAfter,
	)
}

// NativePayloadCompositeStateRoot exposes compositeStateRoot to callers (the
// plugin's submit path) that compute the source claim root from a claim's
// PartRowLtHash (the data_root_after) plus the genesis schema/executor identity.
//
// Deprecated: use NativeSourceClaimRoot, which routes through the single
// canonical replay.AssembleStateRoot profile shared with the verifier (spec
// §6.2, HG-P0-01). This string-domain form ("native-state-root-v1") is retained
// only for the legacy golden vectors and MUST NOT be used for new source claims.
func NativePayloadCompositeStateRoot(schemaSnapshotID, executorProfileID, dataRootAfter string) string {
	return compositeStateRoot(schemaSnapshotID, executorProfileID, dataRootAfter)
}

// NativeSourceClaimRoot computes the source claim root the way the verifier's
// NativePayloadExecutor.Replay does: fold this statement's part_row_lthash onto
// the pinned snapshot's base partition root for (tableID, "all"), then derive
// the state root through replay.AssembleStateRoot. Because both sides call the
// identical canonical assembly, the source claim root equals the verifier's
// ComputedStateRoot by construction (spec §6.2 requirement that source and
// verifier share one profile).
//
// prevSnapshot is the pinned prev-safe snapshot (its SchemaRoot and the base
// partition root for this table flow into the derivation); pass the genesis
// snapshot for a table's first insert. partRowLtHash is the raw additive
// accumulator hex for the candidate part(s) of this statement.
func NativeSourceClaimRoot(prevSnapshot replay.SafeSnapshotManifest, tableID, partRowLtHash string) (string, error) {
	base, err := newNativePartitionAccumulators(prevSnapshot)
	if err != nil {
		return "", err
	}
	delta := lthash.New()
	if partRowLtHash != "" {
		parsed, err := lthashAccumulatorFromHex(partRowLtHash)
		if err != nil {
			return "", fmt.Errorf("parse candidate part_row_lthash: %w", err)
		}
		delta = parsed
	}
	base.add(tableID, nativeAllPartitionID, delta)
	tables, _ := base.tables(prevSnapshot.SchemaRoot, nil)
	_, stateRoot, err := replay.AssembleStateRoot(
		prevSnapshot.SchemaSnapshotID,
		prevSnapshot.SchemaRoot,
		prevSnapshot.ExecutorProfileID,
		tables,
	)
	if err != nil {
		return "", fmt.Errorf("assemble native source claim root: %w", err)
	}
	return stateRoot, nil
}

func sortedNativeParts(in []replay.PartManifestEntry) []replay.PartManifestEntry {
	out := append([]replay.PartManifestEntry(nil), in...)
	sort.Slice(out, func(i, j int) bool { return out[i].PartName < out[j].PartName })
	return out
}
