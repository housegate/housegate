package payloadexec

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay"
)

const (
	testNetwork = "net-1"
	testTable   = "table-1"
	testProfile = "executor-1"
	testSchema  = "schema-1"
)

func testExecutor() *Executor {
	return New(testNetwork, TableSchema{
		TableID: testTable,
		Columns: []lthash.Column{
			{Name: "name", Type: "String"},
			{Name: "balance", Type: "UInt64"},
		},
	})
}

func genesis(t *testing.T, e *Executor) replay.SafeSnapshotManifest {
	t.Helper()
	snap, err := e.GenesisSnapshot(0, testSchema, testProfile)
	if err != nil {
		t.Fatalf("GenesisSnapshot: %v", err)
	}
	if err := snap.Validate(); err != nil {
		t.Fatalf("genesis snapshot invalid: %v", err)
	}
	return snap
}

// insertJob builds a single-statement payload-local INSERT job + prepared
// statement for the given payload.
func insertJob(snap replay.SafeSnapshotManifest, statementID, payloadRef string, payload []byte, sourceRoot string) (replay.ReplayJob, []replay.PreparedStatement) {
	sql := "INSERT INTO balances FORMAT CSVWithNames"
	st := replay.Statement{
		StatementID:   statementID,
		StatementSeq:  snap.SafeBlockSeq + 1,
		SQL:           sql,
		SQLHash:       replay.DigestString(sql),
		SettingsHash:  replay.DigestString("settings"),
		PayloadRef:    payloadRef,
		PayloadHash:   replay.DigestBytes(payload),
		PayloadLength: uint64(len(payload)),
		TargetTableID: testTable,
	}
	job := replay.ReplayJob{
		BlockSeq:           snap.SafeBlockSeq + 1,
		PrevSafeSnapshotID: snap.SnapshotID,
		PrevStateRoot:      snap.StateRoot,
		SchemaSnapshotID:   snap.SchemaSnapshotID,
		ExecutorProfileID:  snap.ExecutorProfileID,
		SourceClaimRoot:    sourceRoot,
		Statements:         []replay.Statement{st},
	}
	return job, []replay.PreparedStatement{{Statement: st, Payload: payload}}
}

// applyInsert builds and applies a single-statement INSERT, failing the test on
// any executor error.
func applyInsert(t *testing.T, e *Executor, snap replay.SafeSnapshotManifest, statementID, payloadRef, payload string) (replay.SafeSnapshotManifest, replay.ExecutionResult) {
	t.Helper()
	job, prepared := insertJob(snap, statementID, payloadRef, []byte(payload), "")
	next, res, err := e.Apply(snap, job, prepared)
	if err != nil {
		t.Fatalf("Apply(%s): %v", statementID, err)
	}
	return next, res
}

func partitionAccum(t *testing.T, m replay.SafeSnapshotManifest, tableID, partitionID string) *lthash.Hash {
	t.Helper()
	for _, tab := range m.Tables {
		if tab.TableID != tableID {
			continue
		}
		for _, pc := range tab.PartitionRoots {
			if pc.PartitionID == partitionID {
				h, err := lthashFromHex(pc.Root)
				if err != nil {
					t.Fatalf("decode partition root: %v", err)
				}
				return h
			}
		}
	}
	t.Fatalf("partition %s/%s not found in manifest", tableID, partitionID)
	return nil
}

func TestExecutorComputesRootForPayloadLocalInsert(t *testing.T) {
	e := testExecutor()
	snap := genesis(t, e)
	payload := []byte("name,balance\nalice,10\n")
	job, prepared := insertJob(snap, "stmt-1", "payload-1", payload, "")

	next, res, err := e.Apply(snap, job, prepared)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if res.ComputedStateRoot == "" {
		t.Fatal("empty computed_state_root")
	}
	if res.ComputedStateRoot == snap.StateRoot {
		t.Fatal("state root did not change after insert")
	}
	if next.StateRoot != res.ComputedStateRoot {
		t.Fatalf("manifest state root %s != computed %s", next.StateRoot, res.ComputedStateRoot)
	}
	if err := next.Validate(); err != nil {
		t.Fatalf("post-state manifest invalid: %v", err)
	}
	if next.ParentSnapshotID != snap.SnapshotID {
		t.Fatalf("parent snapshot = %s, want %s", next.ParentSnapshotID, snap.SnapshotID)
	}
	if next.SafeBlockSeq != job.BlockSeq {
		t.Fatalf("safe block seq = %d, want %d", next.SafeBlockSeq, job.BlockSeq)
	}
	if len(res.AffectedParts) != 1 {
		t.Fatalf("affected parts = %d, want 1", len(res.AffectedParts))
	}
	if res.AffectedParts[0].RowCount != 1 {
		t.Fatalf("row count = %d, want 1", res.AffectedParts[0].RowCount)
	}
	if len(res.PartitionCommitmentsAfter) != 1 {
		t.Fatalf("partition commitments = %d, want 1", len(res.PartitionCommitmentsAfter))
	}
	if res.PrevSafeSnapshotID != job.PrevSafeSnapshotID || res.PrevStateRoot != job.PrevStateRoot {
		t.Fatalf("executor must echo prev identity: %#v", res)
	}
}

func TestPartitionIDForRow(t *testing.T) {
	schema := TableSchema{
		TableID: testTable,
		Columns: []lthash.Column{
			{Name: "id", Type: "UInt64"},
			{Name: "region", Type: "String"},
		},
	}
	got, err := PartitionIDForRow(schema, []any{uint64(1), "eu"})
	if err != nil {
		t.Fatalf("PartitionIDForRow without PartitionBy: %v", err)
	}
	if got != "all" {
		t.Fatalf("partition = %q, want all", got)
	}

	schema.PartitionBy = "region"
	got, err = PartitionIDForRow(schema, []any{uint64(1), "eu"})
	if err != nil {
		t.Fatalf("PartitionIDForRow with PartitionBy: %v", err)
	}
	if got != "p_eu" {
		t.Fatalf("partition = %q, want p_eu", got)
	}

	if _, err := PartitionIDForRow(schema, []any{uint64(1)}); err == nil {
		t.Fatal("PartitionIDForRow must reject values that do not match schema width")
	}

	schema.PartitionBy = "missing"
	if _, err := PartitionIDForRow(schema, []any{uint64(1), "eu"}); err == nil {
		t.Fatal("PartitionIDForRow must reject an unknown PartitionBy column")
	}
}

// TestPartitionIDForRowTypedValueGoldenMatrix pins the non-temporal partition
// renderings. Each case now carries the declared column type its value belongs
// to, because PartitionIDForRow resolves the partition column against the
// column-type authority before rendering (Spec Q Q-D1); the uint/int cases keep
// their platform-width Go values under the 64-bit declaration they would arrive
// as.
func TestPartitionIDForRowTypedValueGoldenMatrix(t *testing.T) {
	tests := []struct {
		name       string
		columnType string
		value      any
		want       string
	}{
		{name: "string", columnType: "String", value: "eu", want: "p_eu"},
		{name: "bytes", columnType: "FixedString(32)", value: []byte("eu"), want: "p_eu"},
		{name: "bool", columnType: "Bool", value: true, want: "p_true"},
		{name: "uint8", columnType: "UInt8", value: uint8(8), want: "p_8"},
		{name: "uint16", columnType: "UInt16", value: uint16(16), want: "p_16"},
		{name: "uint32", columnType: "UInt32", value: uint32(32), want: "p_32"},
		{name: "uint64", columnType: "UInt64", value: uint64(64), want: "p_64"},
		{name: "uint", columnType: "UInt64", value: uint(65), want: "p_65"},
		{name: "int8", columnType: "Int8", value: int8(-8), want: "p_-8"},
		{name: "int16", columnType: "Int16", value: int16(-16), want: "p_-16"},
		{name: "int32", columnType: "Int32", value: int32(-32), want: "p_-32"},
		{name: "int64", columnType: "Int64", value: int64(-64), want: "p_-64"},
		{name: "int", columnType: "Int64", value: int(-65), want: "p_-65"},
		{name: "float32", columnType: "Float32", value: float32(1.5), want: "p_1.5"},
		{name: "float64", columnType: "Float64", value: float64(2.5), want: "p_2.5"},
		{name: "float32 negative zero", columnType: "Float32", value: math.Float32frombits(1 << 31), want: "p_0"},
		{name: "float64 negative zero", columnType: "Float64", value: math.Float64frombits(1 << 63), want: "p_0"},
		{name: "float64 signaling nan", columnType: "Float64", value: math.Float64frombits(0x7ff0000000000001), want: "p_NaN"},
		{name: "float64 quiet nan", columnType: "Float64", value: math.Float64frombits(0x7ff8000000000002), want: "p_NaN"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := TableSchema{
				TableID:     testTable,
				Columns:     []lthash.Column{{Name: "shard", Type: tt.columnType}},
				PartitionBy: "shard",
			}
			got, err := PartitionIDForRow(schema, []any{tt.value})
			if err != nil {
				t.Fatalf("PartitionIDForRow: %v", err)
			}
			if got != tt.want {
				t.Fatalf("partition id = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestPartitionIDForRowRejectsUndefinedTypedPartitionValue keeps the renderer
// fail-closed on a value whose Go type it has no rule for. Its original form
// used a Date column, which encoded Spec Q measurement M5 as an expectation —
// a temporal partition column is now supported, and
// TestPartitionIDForRow_AcceptsTemporalPartitionColumns proves it.
func TestPartitionIDForRowRejectsUndefinedTypedPartitionValue(t *testing.T) {
	// An admitted declaration whose value arrives as an unexpected Go type.
	schema := TableSchema{
		TableID:     testTable,
		Columns:     []lthash.Column{{Name: "shard", Type: "String"}},
		PartitionBy: "shard",
	}
	if _, err := PartitionIDForRow(schema, []any{struct{ A int }{1}}); err == nil {
		t.Fatal("PartitionIDForRow must fail closed for an undefined typed partition value")
	}
	// A temporal declaration whose value is not a time.Time must not silently
	// fall through to the non-temporal renderer.
	temporal := TableSchema{
		TableID:     testTable,
		Columns:     []lthash.Column{{Name: "day", Type: "Date"}},
		PartitionBy: "day",
	}
	err := func() error {
		_, err := PartitionIDForRow(temporal, []any{"2026-07-16"})
		return err
	}()
	if err == nil {
		t.Fatal("PartitionIDForRow must fail closed when a temporal column carries a non-time value")
	}
	if !strings.Contains(err.Error(), "want time.Time") {
		t.Errorf("error %q does not explain the temporal value-type mismatch", err)
	}
}

func TestDecodeCSVPreservesLegacyPartitionWireText(t *testing.T) {
	tests := []struct {
		name       string
		columnType string
		wireValue  string
		want       string
	}{
		{name: "uint with leading zeroes", columnType: "UInt64", wireValue: "001", want: "p_001"},
		{name: "float with trailing fraction", columnType: "Float64", wireValue: "1.0", want: "p_1.0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := TableSchema{
				TableID:     testTable,
				Columns:     []lthash.Column{{Name: "shard", Type: tt.columnType}},
				PartitionBy: "shard",
			}
			rows, err := DecodeCSV([]byte("shard\n"+tt.wireValue+"\n"), schema)
			if err != nil {
				t.Fatalf("DecodeCSV: %v", err)
			}
			if got := rows[0].PartitionID; got != tt.want {
				t.Fatalf("partition id = %q, want legacy wire-derived %q", got, tt.want)
			}
		})
	}
}

func TestLegacyCSVPartitionStateRootGolden(t *testing.T) {
	const (
		networkID = "net-legacy-partition-golden"
		tableID   = "tenant.legacy_partition_golden"
	)
	schema := TableSchema{
		TableID:     tableID,
		Columns:     []lthash.Column{{Name: "shard", Type: "UInt64"}},
		PartitionBy: "shard",
	}
	exec := New(networkID, schema)
	snap, err := exec.GenesisSnapshot(0, "schema-legacy-partition-golden", "housegate-replay-mvp-v0")
	if err != nil {
		t.Fatalf("GenesisSnapshot: %v", err)
	}
	payload := []byte("shard\n001\n")
	statement := replay.Statement{
		StatementID:   "stmt-legacy-partition-golden",
		StatementSeq:  1,
		SQL:           "INSERT INTO tenant.legacy_partition_golden FORMAT CSVWithNames",
		SQLHash:       replay.DigestString("INSERT INTO tenant.legacy_partition_golden FORMAT CSVWithNames"),
		SettingsHash:  replay.DigestString("settings-legacy-partition-golden"),
		PayloadRef:    "payload-legacy-partition-golden",
		PayloadHash:   replay.DigestBytes(payload),
		PayloadLength: uint64(len(payload)),
		TargetTableID: tableID,
	}
	job := replay.ReplayJob{
		BlockSeq:           1,
		PrevSafeSnapshotID: snap.SnapshotID,
		PrevStateRoot:      snap.StateRoot,
		SchemaSnapshotID:   snap.SchemaSnapshotID,
		ExecutorProfileID:  snap.ExecutorProfileID,
		Statements:         []replay.Statement{statement},
	}
	_, result, err := exec.Apply(snap, job, []replay.PreparedStatement{{Statement: statement, Payload: payload}})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if len(result.AffectedParts) != 1 || result.AffectedParts[0].PartitionID != "p_001" {
		t.Fatalf("affected parts = %+v, want one part in p_001", result.AffectedParts)
	}
	const wantStateRoot = "0xe769a7aab847f24f11b5cea94679ac46df6c673b99b11f82d9fdc19ad48d896d"
	if result.ComputedStateRoot != wantStateRoot {
		t.Fatalf("computed state root = %q, want golden %q", result.ComputedStateRoot, wantStateRoot)
	}
}

func TestExecutorReplaySatisfiesInterface(t *testing.T) {
	var _ replay.Executor = (*Executor)(nil)

	e := testExecutor()
	snap := genesis(t, e)
	payload := []byte("name,balance\nalice,10\n")
	job, prepared := insertJob(snap, "stmt-1", "payload-1", payload, "")

	res, err := e.Replay(context.Background(), replay.ExecutionRequest{Job: job, Snapshot: snap, Statements: prepared})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if res.ComputedStateRoot == "" {
		t.Fatal("empty computed_state_root from Replay")
	}
}

func TestExecutorIsDeterministicAcrossInstances(t *testing.T) {
	payload := "name,balance\nalice,10\nbob,20\n"
	run := func() string {
		e := testExecutor()
		snap := genesis(t, e)
		_, res := applyInsert(t, e, snap, "stmt-1", "payload-1", payload)
		return res.ComputedStateRoot
	}
	a, b := run(), run()
	if a != b {
		t.Fatalf("nondeterministic computed root: %s vs %s", a, b)
	}
}

func TestExecutorDuplicateRowsDoNotCancel(t *testing.T) {
	e := testExecutor()
	snap := genesis(t, e)

	_, resOne := applyInsert(t, e, snap, "stmt-1", "p1", "name,balance\nalice,10\n")
	_, resTwo := applyInsert(t, e, snap, "stmt-1", "p2", "name,balance\nalice,10\nalice,10\n")

	if resOne.ComputedStateRoot == resTwo.ComputedStateRoot {
		t.Fatal("duplicate visible row produced identical root — _hg_row_id was not injected")
	}
	if resTwo.AffectedParts[0].RowCount != 2 {
		t.Fatalf("row count = %d, want 2", resTwo.AffectedParts[0].RowCount)
	}
	acc, err := lthashFromHex(resTwo.AffectedParts[0].PartRowLtHash)
	if err != nil {
		t.Fatalf("decode part lthash: %v", err)
	}
	if acc.IsZero() {
		t.Fatal("duplicate-row part accumulator cancelled to zero")
	}
}

// TestRowInstanceIdentityDefeatsLtHashCancellation is the §5.2 / walkthrough #4
// security property: raw-row LtHash cancels when an element is added 2^16 times,
// but the executor's per-row _hg_row_id makes every instance distinct.
func TestRowInstanceIdentityDefeatsLtHashCancellation(t *testing.T) {
	rawRow, err := lthash.RowHash(testTable,
		[]lthash.Column{{Name: "name", Type: "String"}, {Name: "balance", Type: "UInt64"}},
		[]any{"alice", uint64(10)})
	if err != nil {
		t.Fatalf("RowHash: %v", err)
	}
	cancel := lthash.New()
	for i := 0; i < 65536; i++ {
		cancel.AddHash(rawRow)
	}
	if !cancel.IsZero() {
		t.Fatal("expected raw-row LtHash to cancel to zero at 2^16 identical elements")
	}

	e := testExecutor()
	snap := genesis(t, e)
	var b strings.Builder
	b.WriteString("name,balance\n")
	for i := 0; i < 65536; i++ {
		b.WriteString("alice,10\n")
	}
	_, res := applyInsert(t, e, snap, "stmt-1", "p", b.String())
	if res.AffectedParts[0].RowCount != 65536 {
		t.Fatalf("row count = %d, want 65536", res.AffectedParts[0].RowCount)
	}
	partAcc, err := lthashFromHex(res.AffectedParts[0].PartRowLtHash)
	if err != nil {
		t.Fatalf("decode part lthash: %v", err)
	}
	if partAcc.IsZero() {
		t.Fatal("executor part accumulator cancelled despite _hg_row_id — cancellation attack succeeded")
	}
}

func TestExecutorRejectsStatementWithoutPayload(t *testing.T) {
	e := testExecutor()
	snap := genesis(t, e)
	sql := "ALTER TABLE balances DELETE WHERE balance = 0"
	st := replay.Statement{
		StatementID:   "stmt-1",
		StatementSeq:  snap.SafeBlockSeq + 1,
		SQL:           sql,
		SQLHash:       replay.DigestString(sql),
		SettingsHash:  replay.DigestString("settings"),
		TargetTableID: testTable,
	}
	job := replay.ReplayJob{
		BlockSeq:           snap.SafeBlockSeq + 1,
		PrevSafeSnapshotID: snap.SnapshotID,
		PrevStateRoot:      snap.StateRoot,
		SchemaSnapshotID:   snap.SchemaSnapshotID,
		ExecutorProfileID:  snap.ExecutorProfileID,
		SourceClaimRoot:    "x",
		Statements:         []replay.Statement{st},
	}
	if _, _, err := e.Apply(snap, job, []replay.PreparedStatement{{Statement: st}}); err == nil {
		t.Fatal("expected error for mutation-class statement (no payload) in MVP executor")
	}
}

func TestExecutorRejectsUnknownTable(t *testing.T) {
	e := testExecutor()
	snap := genesis(t, e)
	job, prepared := insertJob(snap, "stmt-1", "p", []byte("name,balance\nalice,10\n"), "")
	job.Statements[0].TargetTableID = "table-unknown"
	prepared[0].TargetTableID = "table-unknown"
	if _, _, err := e.Apply(snap, job, prepared); err == nil {
		t.Fatal("expected unknown-table error")
	}
}

func TestExecutorRejectsCSVHeaderMismatch(t *testing.T) {
	e := testExecutor()
	snap := genesis(t, e)
	job, prepared := insertJob(snap, "stmt-1", "p", []byte("name,wrongcol\nalice,10\n"), "")
	if _, _, err := e.Apply(snap, job, prepared); err == nil {
		t.Fatal("expected CSV header mismatch error")
	}
}

func TestExecutorRejectsUnparseableValue(t *testing.T) {
	e := testExecutor()
	snap := genesis(t, e)
	job, prepared := insertJob(snap, "stmt-1", "p", []byte("name,balance\nalice,not-a-number\n"), "")
	if _, _, err := e.Apply(snap, job, prepared); err == nil {
		t.Fatal("expected parse error for non-numeric UInt64")
	}
}

// preparedInsert builds a prepared INSERT statement for multi-statement blocks.
func preparedInsert(statementID string, seq uint64, payloadRef string, payload []byte) replay.PreparedStatement {
	sql := "INSERT INTO balances FORMAT CSVWithNames"
	return replay.PreparedStatement{
		Statement: replay.Statement{
			StatementID:   statementID,
			StatementSeq:  seq,
			SQL:           sql,
			SQLHash:       replay.DigestString(sql),
			SettingsHash:  replay.DigestString("settings"),
			PayloadRef:    payloadRef,
			PayloadHash:   replay.DigestBytes(payload),
			PayloadLength: uint64(len(payload)),
			TargetTableID: testTable,
		},
		Payload: payload,
	}
}

func blockJob(snap replay.SafeSnapshotManifest, prepared ...replay.PreparedStatement) replay.ReplayJob {
	stmts := make([]replay.Statement, len(prepared))
	for i, p := range prepared {
		stmts[i] = p.Statement
	}
	return replay.ReplayJob{
		BlockSeq:           snap.SafeBlockSeq + 1,
		PrevSafeSnapshotID: snap.SnapshotID,
		PrevStateRoot:      snap.StateRoot,
		SchemaSnapshotID:   snap.SchemaSnapshotID,
		ExecutorProfileID:  snap.ExecutorProfileID,
		Statements:         stmts,
	}
}

// TestExecutorMultiStatementSameBlock covers two statements writing the same
// partition within one block — a different code path from chaining two blocks.
func TestExecutorMultiStatementSameBlock(t *testing.T) {
	e := testExecutor()
	snap := genesis(t, e)
	pr1 := preparedInsert("stmt-1", 1, "p1", []byte("name,balance\nalice,10\n"))
	pr2 := preparedInsert("stmt-2", 2, "p2", []byte("name,balance\nbob,20\n"))
	job := blockJob(snap, pr1, pr2)

	next, res, err := e.Apply(snap, job, []replay.PreparedStatement{pr1, pr2})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if len(res.AffectedParts) != 2 {
		t.Fatalf("affected parts = %d, want 2", len(res.AffectedParts))
	}
	if err := verifyLedger(next); err != nil {
		t.Fatalf("post-state ledger invalid: %v", err)
	}
	if err := next.Validate(); err != nil {
		t.Fatalf("post-state invalid: %v", err)
	}
	// partition "all" == sum of both statements' part deltas.
	all := partitionAccum(t, next, testTable, "all")
	sum := lthash.New()
	for _, p := range res.AffectedParts {
		h, err := lthashFromHex(p.PartRowLtHash)
		if err != nil {
			t.Fatalf("decode part: %v", err)
		}
		sum.AddHash(h)
	}
	if !all.Equal(sum) {
		t.Fatal("partition 'all' != sum of statement part deltas")
	}
}

// TestExecutorMultiPartitionProducesDistinctCommitments exercises the
// PartitionBy split: rows must land in distinct, non-zero partition commitments
// and the root must be deterministic.
func TestExecutorMultiPartitionProducesDistinctCommitments(t *testing.T) {
	newShardExec := func() *Executor {
		return New(testNetwork, TableSchema{
			TableID: testTable,
			Columns: []lthash.Column{
				{Name: "shard", Type: "String"},
				{Name: "name", Type: "String"},
				{Name: "balance", Type: "UInt64"},
			},
			PartitionBy: "shard",
		})
	}
	payload := "shard,name,balance\na,alice,10\nb,bob,20\nc,carol,30\n"

	e := newShardExec()
	snap := genesis(t, e)
	_, res := applyInsert(t, e, snap, "stmt-1", "p", payload)

	if len(res.PartitionCommitmentsAfter) != 3 {
		t.Fatalf("partition commitments = %d, want 3", len(res.PartitionCommitmentsAfter))
	}
	zero := lthashHex(lthash.New())
	seen := map[string]bool{}
	for _, pc := range res.PartitionCommitmentsAfter {
		if pc.Root == zero {
			t.Fatalf("partition %s commitment is zero", pc.PartitionID)
		}
		if seen[pc.Root] {
			t.Fatal("partition commitments are not distinct")
		}
		seen[pc.Root] = true
	}

	e2 := newShardExec()
	snap2 := genesis(t, e2)
	_, res2 := applyInsert(t, e2, snap2, "stmt-1", "p", payload)
	if res.ComputedStateRoot != res2.ComputedStateRoot {
		t.Fatal("multi-partition root is not deterministic across instances")
	}
}

// TestExecutorHeaderOnlyPayloadIsNoOp pins the zero-row INSERT boundary: the
// block advances but the state root is unchanged.
func TestExecutorHeaderOnlyPayloadIsNoOp(t *testing.T) {
	e := testExecutor()
	snap := genesis(t, e)
	next, res := applyInsert(t, e, snap, "stmt-1", "p", "name,balance\n")

	if len(res.AffectedParts) != 0 {
		t.Fatalf("affected parts = %d, want 0", len(res.AffectedParts))
	}
	if len(res.PartitionCommitmentsAfter) != 0 {
		t.Fatalf("partition commitments = %d, want 0", len(res.PartitionCommitmentsAfter))
	}
	if res.ComputedStateRoot != snap.StateRoot {
		t.Fatalf("no-op insert changed state root: %s != %s", res.ComputedStateRoot, snap.StateRoot)
	}
	if next.SafeBlockSeq != snap.SafeBlockSeq+1 {
		t.Fatal("block did not advance on a no-op insert")
	}
	if err := next.Validate(); err != nil {
		t.Fatalf("post-state invalid: %v", err)
	}
}

func TestExecutorRejectsDuplicateStatementID(t *testing.T) {
	e := testExecutor()
	snap := genesis(t, e)
	p := []byte("name,balance\nalice,10\n")
	pr1 := preparedInsert("dup", 1, "p1", p)
	pr2 := preparedInsert("dup", 2, "p2", p)
	if _, _, err := e.Apply(snap, blockJob(snap, pr1, pr2), []replay.PreparedStatement{pr1, pr2}); err == nil {
		t.Fatal("expected duplicate statement_id rejection (cancellation-attack defense)")
	}
}

func TestExecutorRejectsNonIncreasingStatementSeq(t *testing.T) {
	e := testExecutor()
	snap := genesis(t, e)
	p := []byte("name,balance\nalice,10\n")
	pr1 := preparedInsert("a", 2, "p1", p)
	pr2 := preparedInsert("b", 2, "p2", p) // equal seq -> non-increasing
	if _, _, err := e.Apply(snap, blockJob(snap, pr1, pr2), []replay.PreparedStatement{pr1, pr2}); err == nil {
		t.Fatal("expected non-increasing statement_seq rejection")
	}
}

func TestVerifyLedgerRejectsOrphanActiveParts(t *testing.T) {
	acc := lthash.New()
	acc.Add([]byte("x"))
	m := replay.SafeSnapshotManifest{
		Tables: []replay.TableManifest{{
			TableID: testTable,
			ActiveParts: []replay.PartManifestEntry{{
				TableID: testTable, PartitionID: "all", PartName: "all-b1-s1",
				PartRowLtHash: lthashHex(acc), RowCount: 1,
			}},
			// PartitionRoots deliberately omitted: an orphan active part.
		}},
	}
	if err := verifyLedger(m); err == nil {
		t.Fatal("expected verifyLedger to reject active parts with no declared partition root")
	}
}

func TestParseFixedStringPadsAndLengthChecks(t *testing.T) {
	v, err := parseValue("FixedString(4)", "ab")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	b, ok := v.([]byte)
	if !ok {
		t.Fatalf("FixedString should decode to []byte, got %T", v)
	}
	if len(b) != 4 || b[0] != 'a' || b[1] != 'b' || b[2] != 0 || b[3] != 0 {
		t.Fatalf("FixedString not NUL-padded to width 4: %v", b)
	}
	if _, err := parseValue("FixedString(4)", "toolong"); err == nil {
		t.Fatal("expected over-length FixedString to be rejected")
	}
	if _, err := parseValue("FixedString(0)", "x"); err == nil {
		t.Fatal("expected invalid FixedString width to be rejected")
	}
}

// TestExecutorChainsBlocksAdditively proves §5.4: a partition commitment after
// block N+1 equals its accumulator after block N plus block N+1's part delta.
func TestExecutorChainsBlocksAdditively(t *testing.T) {
	e := testExecutor()
	g := genesis(t, e)

	snap1, _ := applyInsert(t, e, g, "stmt-1", "p1", "name,balance\nalice,10\n")
	if err := snap1.Validate(); err != nil {
		t.Fatalf("snap1 invalid: %v", err)
	}

	snap2, res2 := applyInsert(t, e, snap1, "stmt-2", "p2", "name,balance\nbob,20\n")

	allAfter1 := partitionAccum(t, snap1, testTable, "all")
	allAfter2 := partitionAccum(t, snap2, testTable, "all")
	partDelta, err := lthashFromHex(res2.AffectedParts[0].PartRowLtHash)
	if err != nil {
		t.Fatalf("decode delta: %v", err)
	}
	sum, err := lthash.FromBytes(allAfter1.Bytes())
	if err != nil {
		t.Fatalf("clone: %v", err)
	}
	sum.AddHash(partDelta)
	if !allAfter2.Equal(sum) {
		t.Fatal("partition commitment is not additive across blocks (prev + delta != after)")
	}
}

// TestPartitionIDForRow_AcceptsTemporalPartitionColumns proves a Date or
// DateTime column is usable as the partition column. Spec Q measurement M5:
// before this, partitionValueString had no time.Time case, so restoring the
// temporal types to the validator alone would have turned a loud startup
// refusal into a late per-statement replay failure — strictly worse than the
// state being fixed.
func TestPartitionIDForRow_AcceptsTemporalPartitionColumns(t *testing.T) {
	for _, tc := range []struct {
		typ  string
		v    time.Time
		want string
	}{
		{"Date", time.Date(2026, time.July, 16, 0, 0, 0, 0, time.UTC), "p_2026-07-16"},
		{"DateTime", time.Date(2026, time.July, 16, 12, 34, 56, 0, time.UTC), "p_2026-07-16 12:34:56"},
		{"DateTime('UTC')", time.Date(2026, time.July, 16, 12, 34, 56, 0, time.UTC), "p_2026-07-16 12:34:56"},
		{"DateTime64(0)", time.Date(2026, time.July, 16, 12, 34, 56, 0, time.UTC), "p_2026-07-16 12:34:56"},
		{"DateTime64(3, 'UTC')", time.Date(2026, time.July, 16, 12, 34, 56, 123000000, time.UTC), "p_2026-07-16 12:34:56.123"},
		{"DateTime64(9)", time.Date(2026, time.July, 16, 12, 34, 56, 123456789, time.UTC), "p_2026-07-16 12:34:56.123456789"},
	} {
		t.Run(tc.typ, func(t *testing.T) {
			schema := TableSchema{
				TableID:     "db.t",
				PartitionBy: "p",
				Columns:     []lthash.Column{{Name: "p", Type: tc.typ}, {Name: "v", Type: "UInt64"}},
			}
			got, err := PartitionIDForRow(schema, []any{tc.v, uint64(1)})
			if err != nil {
				t.Fatalf("PartitionIDForRow(%q) = %v", tc.typ, err)
			}
			if got != tc.want {
				t.Fatalf("PartitionIDForRow(%q) = %q, want %q", tc.typ, got, tc.want)
			}
		})
	}
}

// TestPartitionIDForRow_TemporalRenderingIsUTCAndInjective guards the two rules
// the rendering rests on. The partition id is not hashed into a row element — it
// is an executor-internal grouping key — but it must be stable across verifiers
// and injective across the values one column can actually take.
func TestPartitionIDForRow_TemporalRenderingIsUTCAndInjective(t *testing.T) {
	milli := TableSchema{
		TableID:     "db.t",
		PartitionBy: "p",
		Columns:     []lthash.Column{{Name: "p", Type: "DateTime64(3, 'UTC')"}},
	}
	second := TableSchema{
		TableID:     "db.t",
		PartitionBy: "p",
		Columns:     []lthash.Column{{Name: "p", Type: "DateTime('UTC')"}},
	}
	base := time.Date(2026, time.July, 16, 12, 34, 56, 123000000, time.UTC)
	next := base.Add(time.Millisecond)

	id := func(sch TableSchema, v time.Time) string {
		t.Helper()
		got, err := PartitionIDForRow(sch, []any{v})
		if err != nil {
			t.Fatalf("PartitionIDForRow: %v", err)
		}
		return got
	}

	if a, b := id(milli, base), id(milli, next); a == b {
		t.Fatalf("two instants one millisecond apart share partition id %q under DateTime64(3, 'UTC')", a)
	}
	// The same two instants under a one-second column must collide, and that is
	// correct: the column's own resolution is one second.
	if a, b := id(second, base), id(second, next); a != b {
		t.Fatalf("two instants inside one second differ under DateTime('UTC'): %q vs %q", a, b)
	}

	// A non-UTC location must not change the id: PartitionIDForRow runs on both
	// lanes, and a local-timezone rendering would put the same row in different
	// partitions on two verifiers.
	shanghai, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		t.Fatalf("load non-UTC test location: %v", err)
	}
	if a, b := id(milli, base), id(milli, base.In(shanghai)); a != b {
		t.Fatalf("partition id depends on the value's location: UTC=%q Shanghai=%q", a, b)
	}
}

// TestPartitionIDForRow_RejectsUnadmittedPartitionColumnType keeps the partition
// path inside the column-type authority: a declaration the profile does not
// admit must fail here rather than reach the value switch.
func TestPartitionIDForRow_RejectsUnadmittedPartitionColumnType(t *testing.T) {
	schema := TableSchema{
		TableID:     "db.t",
		PartitionBy: "p",
		Columns:     []lthash.Column{{Name: "p", Type: "Date32"}},
	}
	_, err := PartitionIDForRow(schema, []any{time.Now().UTC()})
	if !errors.Is(err, ErrUnsupportedColumnType) {
		t.Fatalf("PartitionIDForRow = %v, want ErrUnsupportedColumnType", err)
	}
	if !strings.Contains(err.Error(), `partition column "p"`) {
		t.Errorf("error %q does not name the partition column", err)
	}
}
