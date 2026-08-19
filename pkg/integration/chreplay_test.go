package integration

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"testing"

	"github.com/ClickHouse/ch-go/proto"
	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/chexec"
	"github.com/housegate/housegate/pkg/replay/nativepayload"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// These tests exercise the ClickHouse-backed replay executor (pkg/replay/chexec)
// end-to-end through the replay.Verifier against a real ClickHouse container:
// each statement is INSERTed into a scratch table and the materialized rows are
// read back to compute the state root (design Appendix C.3). They cover the
// honest path, the walkthrough #2 fraud path (signed value X, source claims a
// root for Y -> signed mismatch), and executor equivalence with the in-process
// executor (the read-back root must equal the wire-side root for the scalar
// whitelist).

const chReplayNetwork = "sentio-net"

func openDirectCH(t *testing.T) clickhouse.Conn {
	t.Helper()
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{chEnv.Addr},
		Auth: clickhouse.Auth{
			Database: chEnv.Database,
			Username: chEnv.User,
			Password: chEnv.Password,
		},
		Protocol: clickhouse.Native,
	})
	if err != nil {
		t.Fatalf("clickhouse.Open: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func chReplaySchema(tableID string) payloadexec.TableSchema {
	return payloadexec.TableSchema{
		TableID: tableID,
		Columns: []lthash.Column{
			{Name: "user_id", Type: "String"},
			{Name: "balance", Type: "UInt64"},
			{Name: "score", Type: "Int32"},
			{Name: "ratio", Type: "Float64"},
		},
	}
}

func chReplayJob(snap replay.SafeSnapshotManifest, tableID, statementID, payloadRef string, payload []byte, sourceClaim string) replay.ReplayJob {
	sql := "INSERT INTO " + tableID + " FORMAT CSVWithNames"
	schemaHash := ""
	for _, table := range snap.Tables {
		if table.TableID == tableID {
			schemaHash = table.SchemaHash
			break
		}
	}
	if schemaHash == "" {
		panic("chReplayJob: snapshot has no schema for " + tableID)
	}
	return replay.ReplayJob{
		BlockSeq:           snap.SafeBlockSeq + 1,
		PrevSafeSnapshotID: snap.SnapshotID,
		PrevStateRoot:      snap.StateRoot,
		SchemaSnapshotID:   snap.SchemaSnapshotID,
		ExecutorProfileID:  snap.ExecutorProfileID,
		SourceClaimRoot:    sourceClaim,
		Statements: []replay.Statement{{
			StatementID:    statementID,
			StatementSeq:   snap.SafeBlockSeq + 1,
			SQL:            sql,
			SQLHash:        replay.DigestString(sql),
			SettingsHash:   replay.DigestString("settings"),
			PayloadRef:     payloadRef,
			PayloadHash:    replay.DigestBytes(payload),
			PayloadLength:  uint64(len(payload)),
			TargetTableID:  tableID,
			PayloadFormat:  payloadexec.PayloadFormatCSVWithNames,
			ClientRevision: 54460,
			SchemaHash:     schemaHash,
		}},
	}
}

func nativeCHReplayJob(snap replay.SafeSnapshotManifest, tableID, statementID, payloadRef string, payload []byte, sourceClaim string) replay.ReplayJob {
	job := chReplayJob(snap, tableID, statementID, payloadRef, payload, sourceClaim)
	job.Statements[0].SQL = "INSERT INTO " + tableID + " FORMAT Native"
	job.Statements[0].SQLHash = replay.DigestString(job.Statements[0].SQL)
	job.Statements[0].PayloadFormat = replay.PayloadFormatClickHouseNativeData
	return job
}

type chReplaySchemaHashes map[string]string

func (s chReplaySchemaHashes) TableSchemaHash(tableID string) (string, bool) {
	hash, ok := s[tableID]
	return hash, ok
}

// execRoot runs an executor over a payload and returns the state root it
// computes — i.e. simulates a source executing the block and registering its
// claimed root. Works for both the ClickHouse-backed and in-process executors.
func execRoot(t *testing.T, exec *payloadexec.Executor, snap replay.SafeSnapshotManifest, tableID, statementID string, payload []byte) string {
	t.Helper()
	job := chReplayJob(snap, tableID, statementID, "probe", payload, "")
	prepared := []replay.PreparedStatement{{Statement: job.Statements[0], Payload: payload}}
	_, res, err := exec.ApplyContext(context.Background(), snap, job, prepared)
	if err != nil {
		t.Fatalf("probe ApplyContext: %v", err)
	}
	return res.ComputedStateRoot
}

func nativeExecRoot(t *testing.T, exec *payloadexec.Executor, snap replay.SafeSnapshotManifest, tableID, statementID string, payload []byte) string {
	t.Helper()
	job := nativeCHReplayJob(snap, tableID, statementID, "probe", payload, "")
	prepared := []replay.PreparedStatement{{Statement: job.Statements[0], Payload: payload}}
	_, res, err := exec.ApplyContext(context.Background(), snap, job, prepared)
	if err != nil {
		t.Fatalf("Native probe ApplyContext: %v", err)
	}
	return res.ComputedStateRoot
}

type chReplayRow struct {
	userID  string
	balance uint64
	score   int32
	ratio   float64
}

func nativeCHReplayPayload(t *testing.T, rows ...chReplayRow) []byte {
	t.Helper()
	users := proto.ColStr{}
	balances := proto.ColUInt64{}
	scores := proto.ColInt32{}
	ratios := proto.ColFloat64{}
	for _, row := range rows {
		users.Append(row.userID)
		balances.Append(row.balance)
		scores.Append(row.score)
		ratios.Append(row.ratio)
	}
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ClientCodeData))
	buf.PutString("")
	if err := (proto.Block{Rows: len(rows), Columns: 4}).EncodeBlock(&buf, 54460, proto.Input{
		{Name: "user_id", Data: &users},
		{Name: "balance", Data: &balances},
		{Name: "score", Data: &scores},
		{Name: "ratio", Data: &ratios},
	}); err != nil {
		t.Fatalf("encode Native replay payload: %v", err)
	}
	return append([]byte(nil), buf.Buf...)
}

func chReplaySigner(t *testing.T) *payloadexec.Ed25519Signer {
	t.Helper()
	seed := make([]byte, ed25519.SeedSize)
	seed[0] = 9
	signer, err := payloadexec.NewEd25519Signer("replica-ch", seed)
	if err != nil {
		t.Fatalf("signer: %v", err)
	}
	return signer
}

func assertCHReplaySignature(t *testing.T, signer *payloadexec.Ed25519Signer, att replay.ReplayAttestation) {
	t.Helper()
	if att.ReceiptHash == "" || att.Signature == "" {
		t.Fatal("attestation missing receipt hash / signature")
	}
	recomputed, err := att.Receipt.Hash()
	if err != nil {
		t.Fatalf("receipt hash: %v", err)
	}
	if recomputed != att.ReceiptHash {
		t.Fatalf("receipt hash %s != recomputed %s", att.ReceiptHash, recomputed)
	}
	sig, err := hex.DecodeString(att.Signature)
	if err != nil {
		t.Fatalf("signature not hex: %v", err)
	}
	if !ed25519.Verify(signer.PublicKey(), []byte(att.ReceiptHash), sig) {
		t.Fatal("attestation signature did not verify against the replica public key")
	}
}

func newCHReplayHarness(t *testing.T) (*payloadexec.Executor, replay.SafeSnapshotManifest, *payloadexec.MemSnapshotStore, *payloadexec.MemPayloadStore, *payloadexec.Ed25519Signer, *replay.Verifier, string) {
	t.Helper()
	conn := openDirectCH(t)
	tableID := uniqueTable(t)
	schema := chReplaySchema(tableID)
	exec := payloadexec.NewWithMaterializer(chReplayNetwork, chexec.NewMaterializer(chReplayNetwork, conn), schema)
	gen, err := exec.GenesisSnapshot(0, "schema-1", "ch-26.x")
	if err != nil {
		t.Fatalf("genesis: %v", err)
	}
	snapshots := payloadexec.NewMemSnapshotStore()
	snapshots.Put(gen)
	payloads := payloadexec.NewMemPayloadStore()
	signer := chReplaySigner(t)
	verifier := &replay.Verifier{
		Snapshots:    snapshots,
		Payloads:     payloads,
		Executor:     exec,
		Signer:       signer,
		SchemaHashes: chReplaySchemaHashes{tableID: payloadexec.TableSchemaHash(chReplayNetwork, schema)},
	}
	return exec, gen, snapshots, payloads, signer, verifier, tableID
}

func TestReplayCHExecutorHonestMatch(t *testing.T) {
	exec, gen, _, payloads, signer, verifier, tableID := newCHReplayHarness(t)

	payload := nativeCHReplayPayload(t,
		chReplayRow{userID: "0x123", balance: 10, score: -5, ratio: 1.5},
		chReplayRow{userID: "0xabc", balance: 250, score: 99, ratio: -0.25},
	)
	payloads.Put("p1", payload)
	claim := nativeExecRoot(t, exec, gen, tableID, "stmt-1", payload)

	att, err := verifier.Verify(context.Background(), nativeCHReplayJob(gen, tableID, "stmt-1", "p1", payload, claim))
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if !att.MatchSourceRoot {
		t.Fatal("honest ClickHouse replay should match the source root")
	}
	if att.Receipt.ComputedStateRoot != claim {
		t.Fatalf("computed %s != claim %s", att.Receipt.ComputedStateRoot, claim)
	}
	assertCHReplaySignature(t, signer, att)
}

// TestReplayCHExecutorFraudMismatch is walkthrough #2 against real ClickHouse:
// the user signs balance=10 but the source registers the root it computed for
// balance=0. The verifier re-executes the signed payload on ClickHouse, computes
// the balance=10 root, and signs a NON-matching receipt as challenge evidence.
func TestReplayCHExecutorFraudMismatch(t *testing.T) {
	exec, gen, _, payloads, signer, verifier, tableID := newCHReplayHarness(t)

	signed := nativeCHReplayPayload(t, chReplayRow{userID: "0x123", balance: 10, score: -5, ratio: 1.5})
	tampered := nativeCHReplayPayload(t, chReplayRow{userID: "0x123", balance: 0, score: -5, ratio: 1.5})
	payloads.Put("p1", signed)

	truth := nativeExecRoot(t, exec, gen, tableID, "stmt-1", signed)
	fraud := nativeExecRoot(t, exec, gen, tableID, "stmt-1", tampered)
	if truth == fraud {
		t.Fatal("test setup broken: signed and tampered payloads hash equal")
	}

	att, err := verifier.Verify(context.Background(), nativeCHReplayJob(gen, tableID, "stmt-1", "p1", signed, fraud))
	if err != nil {
		t.Fatalf("Verify must sign a mismatch, not error: %v", err)
	}
	if att.MatchSourceRoot {
		t.Fatal("fraudulent source root must not match")
	}
	if att.Receipt.ComputedStateRoot != truth {
		t.Fatalf("computed %s != recomputed truth %s", att.Receipt.ComputedStateRoot, truth)
	}
	if att.Receipt.SourceClaimRoot != fraud {
		t.Fatalf("receipt source claim %s != fraud root %s", att.Receipt.SourceClaimRoot, fraud)
	}
	assertCHReplaySignature(t, signer, att)
}

// TestReplayCHExecutorMatchesInProcessRoot anchors decoder agreement (§5.3): for
// the MVP whitelist — String, FixedString(N) (both exact-length and NUL-padded),
// and scalars, including a duplicated row that survives via distinct _hg_row_id —
// the ClickHouse read-back root must equal the in-process wire-side root. The
// read-back guards (row count, row-id presence) also fail this if ClickHouse
// drops/adds/mangles rows. It does NOT exercise the materialization-divergence
// classes (DEFAULT/MATERIALIZED columns, Enum/Decimal/JSON coercion) that are
// chexec's eventual reason to exist — those need executor support deferred to
// post-MVP (Appendix C.5).
func TestReplayCHExecutorMatchesInProcessRoot(t *testing.T) {
	conn := openDirectCH(t)
	tableID := uniqueTable(t)
	// Include FixedString(10) to exercise the []byte read-back path: row 1 fills
	// all 10 bytes, row 2 is shorter and must NUL-pad identically on both sides.
	schema := payloadexec.TableSchema{
		TableID: tableID,
		Columns: []lthash.Column{
			{Name: "user_id", Type: "String"},
			{Name: "tx_hash", Type: "FixedString(10)"},
			{Name: "balance", Type: "UInt64"},
			{Name: "score", Type: "Int32"},
			{Name: "ratio", Type: "Float64"},
		},
	}

	chE := payloadexec.NewWithMaterializer(chReplayNetwork, chexec.NewMaterializer(chReplayNetwork, conn), schema)
	inE := payloadexec.New(chReplayNetwork, schema)

	genCH, err := chE.GenesisSnapshot(0, "schema-1", "ch-26.x")
	if err != nil {
		t.Fatalf("genesis ch: %v", err)
	}
	genIn, err := inE.GenesisSnapshot(0, "schema-1", "ch-26.x")
	if err != nil {
		t.Fatalf("genesis in: %v", err)
	}
	if genCH.StateRoot != genIn.StateRoot {
		t.Fatalf("genesis roots differ: ch=%s in=%s", genCH.StateRoot, genIn.StateRoot)
	}

	payload := []byte("user_id,tx_hash,balance,score,ratio\n0x123,0xdeadbeef,10,-5,1.5\n0xabc,0xfeed,250,99,-0.25\n0x123,0xdeadbeef,10,-5,1.5\n")
	chRoot := execRoot(t, chE, genCH, tableID, "stmt-1", payload)
	inRoot := execRoot(t, inE, genIn, tableID, "stmt-1", payload)
	if chRoot != inRoot {
		t.Fatalf("ClickHouse-backed root != in-process root (executor equivalence broken):\n  ch: %s\n  in: %s", chRoot, inRoot)
	}
}

// TestReplayCHExecutorMultiPartition exercises the partition fan-out (§5.4)
// through the ClickHouse-backed path: rows spanning three partitions must yield
// three distinct partition commitments and a root equal to the in-process
// executor's. Note the partition key is derived wire-side (the scratch table is
// unpartitioned); verifying ClickHouse's own physical PARTITION BY is out of MVP
// scope.
func TestReplayCHExecutorMultiPartition(t *testing.T) {
	conn := openDirectCH(t)
	tableID := uniqueTable(t)
	schema := payloadexec.TableSchema{
		TableID: tableID,
		Columns: []lthash.Column{
			{Name: "shard", Type: "String"},
			{Name: "user_id", Type: "String"},
			{Name: "balance", Type: "UInt64"},
		},
		PartitionBy: "shard",
	}
	chE := payloadexec.NewWithMaterializer(chReplayNetwork, chexec.NewMaterializer(chReplayNetwork, conn), schema)
	inE := payloadexec.New(chReplayNetwork, schema)
	genCH, err := chE.GenesisSnapshot(0, "schema-1", "ch-26.x")
	if err != nil {
		t.Fatalf("genesis ch: %v", err)
	}
	genIn, err := inE.GenesisSnapshot(0, "schema-1", "ch-26.x")
	if err != nil {
		t.Fatalf("genesis in: %v", err)
	}

	payload := []byte("shard,user_id,balance\na,0x1,10\nb,0x2,20\nc,0x3,30\n")
	job := chReplayJob(genCH, tableID, "stmt-1", "p", payload, "")
	prepared := []replay.PreparedStatement{{Statement: job.Statements[0], Payload: payload}}
	next, res, err := chE.ApplyContext(context.Background(), genCH, job, prepared)
	if err != nil {
		t.Fatalf("ApplyContext: %v", err)
	}
	if len(res.PartitionCommitmentsAfter) != 3 {
		t.Fatalf("partition commitments = %d, want 3", len(res.PartitionCommitmentsAfter))
	}
	if err := next.Validate(); err != nil {
		t.Fatalf("post-state manifest invalid: %v", err)
	}
	inRoot := execRoot(t, inE, genIn, tableID, "stmt-1", payload)
	if res.ComputedStateRoot != inRoot {
		t.Fatalf("multi-partition ClickHouse root != in-process root:\n  ch: %s\n  in: %s", res.ComputedStateRoot, inRoot)
	}
}

// nativeReplayPayload encodes one client Data packet for the schema used by
// TestReplayCHExecutorNativePayloadMatchesInProcessRoot at revision 54460.
func nativeReplayPayload(t *testing.T) []byte {
	t.Helper()
	user := proto.ColStr{}
	user.Append("0x123")
	user.Append("0xabc")
	balance := proto.ColUInt64{10, 250}
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ClientCodeData))
	buf.PutString("")
	if err := (proto.Block{Rows: 2, Columns: 2}).EncodeBlock(&buf, 54460, proto.Input{{Name: "user_id", Data: &user}, {Name: "balance", Data: &balance}}); err != nil {
		t.Fatalf("encode: %v", err)
	}
	return append([]byte(nil), buf.Buf...)
}

// TestReplayCHExecutorNativePayloadMatchesInProcessRoot: with
// payload_format = clickhouse-native-data-v1 the ClickHouse-backed
// materializer and the in-process nativepayload materializer must produce
// the same state root (executor equivalence, envelope v2 §8).
func TestReplayCHExecutorNativePayloadMatchesInProcessRoot(t *testing.T) {
	conn := openDirectCH(t)
	tableID := uniqueTable(t)
	schema := payloadexec.TableSchema{TableID: tableID, Columns: []lthash.Column{{Name: "user_id", Type: "String"}, {Name: "balance", Type: "UInt64"}}}
	chE := payloadexec.NewWithMaterializer(chReplayNetwork, chexec.NewMaterializer(chReplayNetwork, conn), schema)
	inE := payloadexec.NewWithMaterializer(chReplayNetwork, nativepayload.Materializer{NetworkID: chReplayNetwork}, schema)
	genCH, err := chE.GenesisSnapshot(0, "schema-1", "ch-26.x")
	if err != nil {
		t.Fatal(err)
	}
	genIn, err := inE.GenesisSnapshot(0, "schema-1", "ch-26.x")
	if err != nil {
		t.Fatal(err)
	}
	payload := nativeReplayPayload(t)
	job := chReplayJob(genCH, tableID, "stmt-native-1", "probe", payload, "")
	job.Statements[0].SQL = "INSERT INTO " + tableID + " FORMAT Native"
	job.Statements[0].SQLHash = replay.DigestString(job.Statements[0].SQL)
	job.Statements[0].PayloadFormat = nativepayload.PayloadFormat
	job.Statements[0].ClientRevision = 54460
	prepared := []replay.PreparedStatement{{Statement: job.Statements[0], Payload: payload}}
	_, chRes, err := chE.ApplyContext(context.Background(), genCH, job, prepared)
	if err != nil {
		t.Fatalf("chexec native: %v", err)
	}
	_, inRes, err := inE.ApplyContext(context.Background(), genIn, job, prepared)
	if err != nil {
		t.Fatalf("in-process native: %v", err)
	}
	if chRes.ComputedStateRoot != inRes.ComputedStateRoot {
		t.Fatalf("native executor equivalence broken:\n  ch: %s\n  in: %s", chRes.ComputedStateRoot, inRes.ComputedStateRoot)
	}
}
