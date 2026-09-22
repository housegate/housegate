package integration

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	housegate "github.com/housegate/housegate"
	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/config"
	"github.com/housegate/housegate/pkg/integration/testenv"
	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/network"
	siplugin "github.com/housegate/housegate/pkg/plugins/storageintegrity"
	"github.com/housegate/housegate/pkg/registry"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/nativepayload"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/rewriter"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
	pb "github.com/housegate/rewriter-proto/gen/pb"
)

// inlineLandingConsumer exercises the admission-owner port: ingress suppresses
// ordinary row forwarding, so the test owner must land the admitted bytes. It
// deliberately does not model production ACK2, hg_unsafe, or SourcePreparer.
type inlineLandingConsumer struct {
	conn   clickhouse.Conn
	schema payloadexec.TableSchema
	mu     sync.Mutex
	seen   []siplugin.Admission
}

func (c *inlineLandingConsumer) ConsumeStorageIntegrityAdmission(ctx context.Context, adm siplugin.Admission) error {
	rows, err := nativepayload.Decode(c.schema, adm.Payload.Revision, adm.Payload.Bytes)
	if err != nil {
		return fmt.Errorf("decode admitted rows: %w", err)
	}
	batch, err := c.conn.PrepareBatch(ctx, "INSERT INTO "+c.schema.TableID)
	if err != nil {
		return fmt.Errorf("prepare admission landing: %w", err)
	}
	defer batch.Abort()
	for _, row := range rows {
		if err := batch.Append(row.Values...); err != nil {
			return fmt.Errorf("append admitted row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("land admitted rows: %w", err)
	}
	c.mu.Lock()
	c.seen = append(c.seen, adm)
	c.mu.Unlock()
	return nil
}

func (c *inlineLandingConsumer) admissions() []siplugin.Admission {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]siplugin.Admission(nil), c.seen...)
}

func startInlineValuesPair(t *testing.T, networkID string, columns []lthash.Column) (*testenv.TestProxy, *inlineLandingConsumer) {
	t.Helper()
	ffi := strings.TrimSpace(os.Getenv("POLYGLOT_SQL_FFI_PATH"))
	if ffi == "" {
		t.Skip("POLYGLOT_SQL_FFI_PATH is unset; inline VALUES requires the native materializer")
	}
	signer, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatal(err)
	}
	// Each test owns its table, schema, sequence state and connections. The
	// destination is exactly the proxied target, so row counts catch doubles.
	table := "inline_" + strings.ReplaceAll(networkID, "-", "_")
	schema := payloadexec.TableSchema{TableID: chEnv.Database + "." + table, Columns: columns}
	ch := openConn(t, chEnv.Addr)
	var defs []string
	for _, c := range columns {
		defs = append(defs, "`"+c.Name+"` "+c.Type)
	}
	if err := ch.Exec(context.Background(), "DROP TABLE IF EXISTS "+schema.TableID); err != nil {
		t.Fatal(err)
	}
	if err := ch.Exec(context.Background(), "CREATE TABLE "+schema.TableID+" ("+strings.Join(defs, ", ")+") ENGINE = MergeTree ORDER BY tuple()"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := ch.Exec(context.Background(), "DROP TABLE IF EXISTS "+schema.TableID); err != nil {
			t.Errorf("drop fixture table: %v", err)
		}
	})
	var serverVersion string
	if err := ch.QueryRow(context.Background(), "SELECT version()").Scan(&serverVersion); err != nil {
		t.Fatal(err)
	}
	t.Logf("ClickHouse server=%s target=%s FFI=%s", serverVersion, schema.TableID, ffi)
	consumer := &inlineLandingConsumer{conn: ch, schema: schema}
	schemaJSON, err := json.Marshal(schema)
	if err != nil {
		t.Fatal(err)
	}
	declared := func(_ *config.Config, opts *housegate.Options) {
		ns := opts.NetworkState.(*network.InMemoryNetworkState)
		ns.TableSchemas[chEnv.Database+"/"+table+"@1"] = network.TableSchemaInfo{
			DatabaseId: chEnv.Database, TableId: table, Version: 1,
			SchemaHash: payloadexec.TableSchemaHash(networkID, schema), SchemaJson: string(schemaJSON),
		}
	}
	rewriterOpt, mock := testenv.WithRewriterMock(t)
	mock.SetAccessedTables("INSERT INTO "+schema.TableID, []*pb.AccessedTable{{
		OriginalDatabase: chEnv.Database, OriginalTable: table, LogicalDatabase: chEnv.Database,
		PhysicalDatabase: chEnv.Database, IsStorageIntegrity: true,
	}})
	server := testenv.StartServerProxy(t, chEnv.Addr,
		rewriterOpt, authProxyConfig([]string{signer.Address()}, false),
		testenv.WithDatabasePermission(signer.Address(), chEnv.Database, registry.DbAuthWrite), declared,
		testenv.WithConfigMutator(func(cfg *config.Config) {
			cfg.Rewriter.PhysicalDatabase = chEnv.Database
			cfg.StorageIntegrity.Ingress.Enabled = true
			cfg.StorageIntegrity.Ingress.AllowedAddresses = []string{signer.Address()}
			cfg.StorageIntegrity.Ingress.NetworkID = networkID
		}),
		func(_ *config.Config, opts *housegate.Options) { opts.StorageIntegrityAdmissionConsumer = consumer },
	)
	agent := testenv.StartAgentProxy(t, authTestKey1, server.Addr, declared,
		testenv.WithConfigMutator(func(cfg *config.Config) {
			cfg.StorageIntegrity.Agent.Enabled = true
			cfg.StorageIntegrity.Agent.NetworkID = networkID
			cfg.StorageIntegrity.Agent.StateDir = t.TempDir()
			cfg.StorageIntegrity.Agent.RequireNetworkState = false
			cfg.StorageIntegrity.Agent.InlineValues.Enabled = true
			cfg.Materialize.Enabled = true
			cfg.Materialize.Engine = rewriter.EngineNative
			cfg.Materialize.NativeLibraryPath = ffi
		}),
	)
	return agent, consumer
}

func requireInlineAdmissions(t *testing.T, c *inlineLandingConsumer, want int) []siplugin.Admission {
	t.Helper()
	// ConsumeStorageIntegrityAdmission is synchronous at strict completion;
	// successful client completion must already have recorded the landing.
	seen := c.admissions()
	if len(seen) != want {
		t.Fatalf("successful admissions=%d, want %d", len(seen), want)
	}
	return seen
}

func verifyInlineSignature(t *testing.T, networkID string, schema payloadexec.TableSchema, adm siplugin.Admission) {
	t.Helper()
	signer, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatal(err)
	}
	validator := auth.NewEthValidator([]string{signer.Address()}, time.Minute, true, false, "", nil)
	want := auth.JWSStatementPayloadV2{
		NetworkID: networkID, StatementID: adm.StatementID, SQLHash: replay.DigestString(adm.SQL),
		SettingsHash: sicore.EmptySettingsHash, SchemaHash: payloadexec.TableSchemaHash(networkID, schema),
		PayloadHash: replay.DigestBytes(adm.Payload.Bytes), PayloadLength: uint64(len(adm.Payload.Bytes)),
		PayloadFormat: sicore.PayloadEncodingClickHouseNativeData, ClientRevision: uint32(adm.Payload.Revision),
		TargetTableID: schema.TableID, RowIDProfileID: payloadexec.RowIDProfileID, StatementKind: sicore.StatementKindCodeInsert,
	}
	if got, err := validator.ValidateStatementV2(adm.UserJWS, want); err != nil || got != signer.Address() {
		t.Fatalf("admitted exact SQL/payload signature: signer=%s err=%v", got, err)
	}
	if adm.Payload.SHA256 != "sha256:"+strings.TrimPrefix(want.PayloadHash, "0x") || adm.Payload.Length != want.PayloadLength {
		t.Fatal("admission hash/length do not describe captured bytes")
	}
	t.Logf("verified JWS: SQL=%q sql_hash=%s payload_revision=%d payload_hash=%s payload_length=%d raw=%x", adm.SQL, want.SQLHash, adm.Payload.Revision, want.PayloadHash, want.PayloadLength, adm.Payload.Bytes)
}

func inlineNativeRoot(t *testing.T, networkID string, schema payloadexec.TableSchema, adm siplugin.Admission) string {
	t.Helper()
	exec := payloadexec.NewWithMaterializer(networkID, nativepayload.Materializer{NetworkID: networkID}, schema)
	gen, err := exec.GenesisSnapshot(0, "schema-1", "inline-values")
	if err != nil {
		t.Fatal(err)
	}
	sql := "INSERT INTO " + schema.TableID + " FORMAT Native"
	stmt := replay.Statement{
		StatementID: "inline-probe", StatementSeq: 1, SQL: sql, SQLHash: replay.DigestString(sql),
		SettingsHash: sicore.EmptySettingsHash, PayloadRef: "probe", PayloadHash: replay.DigestBytes(adm.Payload.Bytes),
		PayloadLength: uint64(len(adm.Payload.Bytes)), TargetTableID: schema.TableID,
		PayloadFormat: replay.PayloadFormatClickHouseNativeData, ClientRevision: uint32(adm.Payload.Revision),
		SchemaHash: payloadexec.TableSchemaHash(networkID, schema),
	}
	job := replay.ReplayJob{BlockSeq: 1, PrevSafeSnapshotID: gen.SnapshotID, PrevStateRoot: gen.StateRoot,
		SchemaSnapshotID: gen.SchemaSnapshotID, ExecutorProfileID: gen.ExecutorProfileID, Statements: []replay.Statement{stmt}}
	_, result, err := exec.ApplyContext(context.Background(), gen, job, []replay.PreparedStatement{{Statement: stmt, Payload: adm.Payload.Bytes}})
	if err != nil {
		t.Fatalf("replay exact revision %d: %v", adm.Payload.Revision, err)
	}
	return result.ComputedStateRoot
}

type inlineStoredRow struct {
	ID     uint64
	Region string
}

func requireInlineStoredRows(t *testing.T, c *inlineLandingConsumer, want []inlineStoredRow) {
	t.Helper()
	rows, err := c.conn.Query(context.Background(), "SELECT id, region FROM "+c.schema.TableID+" ORDER BY id, region")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []inlineStoredRow
	for rows.Next() {
		var row inlineStoredRow
		if err := rows.Scan(&row.ID, &row.Region); err != nil {
			t.Fatal(err)
		}
		got = append(got, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	want = append([]inlineStoredRow(nil), want...)
	sort.Slice(want, func(i, j int) bool {
		if want[i].ID != want[j].ID {
			return want[i].ID < want[j].ID
		}
		return want[i].Region < want[j].Region
	})
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("physical rows=%+v, want %+v", got, want)
	}
	t.Logf("physical landing exact rows (%d): %+v", len(got), got)
}

func TestStorageIntegrity_InlineValuesSignedEndToEnd(t *testing.T) {
	const networkID = "itest-inline"
	bin := testenv.ClickHouseCLI(t)
	testenv.CLIVersion(t)
	agent, c := startInlineValuesPair(t, networkID, []lthash.Column{{Name: "id", Type: "UInt64"}, {Name: "region", Type: "String"}})
	conn := openConnNoCompression(t, agent.Addr)
	before := uint64(time.Now().Unix())
	sql := "INSERT INTO " + c.schema.TableID + " (id, region) VALUES (toUInt64(toUnixTimestamp(now())), 'eu'), (1 + 2, upper('us'))"
	if err := conn.Exec(context.Background(), sql); err != nil {
		t.Fatalf("inline INSERT: %v", err)
	}
	adm := requireInlineAdmissions(t, c, 1)[0]
	if want := "INSERT INTO " + c.schema.TableID + " (`id`, `region`) FORMAT Native"; adm.SQL != want {
		t.Fatalf("signed SQL=%q, want %q", adm.SQL, want)
	}
	verifyInlineSignature(t, networkID, c.schema, adm)
	rows, err := nativepayload.Decode(c.schema, adm.Payload.Revision, adm.Payload.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].Values[1] != "eu" || rows[1].Values[0] != uint64(3) || rows[1].Values[1] != "US" {
		t.Fatalf("decoded rows=%+v", rows)
	}
	nowID := rows[0].Values[0].(uint64)
	if nowID < before || nowID > uint64(time.Now().Unix()) {
		t.Fatalf("materialized now()=%d outside execution interval", nowID)
	}
	expected := []inlineStoredRow{{nowID, "eu"}, {3, "US"}}
	requireInlineStoredRows(t, c, expected)
	var literals []string
	for _, row := range rows {
		literals = append(literals, fmt.Sprintf("(%d,'%s')", row.Values[0].(uint64), row.Values[1].(string)))
	}
	out, err := testenv.RunCLIStdin(t, bin, agent.Addr, "", "INSERT INTO "+c.schema.TableID+" FORMAT Values", strings.Join(literals, ",")+"\n")
	if err != nil {
		t.Fatalf("CLI FORMAT Values: %v\n%s", err, out)
	}
	ref := requireInlineAdmissions(t, c, 2)[1]
	verifyInlineSignature(t, networkID, c.schema, ref)
	requireInlineStoredRows(t, c, append(expected, expected...))
	if !bytes.Equal(adm.Payload.Bytes, ref.Payload.Bytes) {
		t.Fatalf("raw payload mismatch: inline revision=%d bytes=%x; independent CLI revision=%d bytes=%x", adm.Payload.Revision, adm.Payload.Bytes, ref.Payload.Revision, ref.Payload.Bytes)
	}
	root, refRoot := inlineNativeRoot(t, networkID, c.schema, adm), inlineNativeRoot(t, networkID, c.schema, ref)
	if root != refRoot {
		t.Fatalf("replay roots inline=%s CLI=%s", root, refRoot)
	}
	t.Logf("independent CLI byte parity and in-process replay root: %s", root)
}

func TestStorageIntegrity_InlineValuesClosureRefused(t *testing.T) {
	agent, c := startInlineValuesPair(t, "itest-inline-closure", []lthash.Column{{Name: "id", Type: "UInt64"}, {Name: "region", Type: "String"}})
	conn := openConnNoCompression(t, agent.Addr)
	err := conn.Exec(context.Background(), "INSERT INTO "+c.schema.TableID+" (id, region) VALUES (1, currentDatabase())")
	if err == nil || !strings.Contains(err.Error(), sicore.InlineValuesErrorPrefix) || !strings.Contains(err.Error(), "currentDatabase") {
		t.Fatalf("closure refusal=%v", err)
	}
	requireInlineAdmissions(t, c, 0)
	requireInlineStoredRows(t, c, nil)
	t.Logf("prefixed closure refusal: %v", err)
}

func TestCLI_InlineValuesShapeByClientVersion(t *testing.T) {
	major, minor := testenv.CLIVersion(t)
	if major < 26 || major == 26 && minor < 3 {
		t.Skipf("client %d.%d streams rows client-side; inline shape requires 26.3+", major, minor)
	}
	const networkID = "itest-inline-cli"
	agent, c := startInlineValuesPair(t, networkID, []lthash.Column{{Name: "id", Type: "UInt64"}, {Name: "region", Type: "String"}})
	// Empty stdin leaves cmd.Stdin nil (the null device), never an open pipe.
	out, err := testenv.RunCLIStdin(t, testenv.ClickHouseCLI(t), agent.Addr, "", "INSERT INTO "+c.schema.TableID+" (id, region) VALUES (11, 'eu')", "")
	if err != nil {
		t.Fatalf("CLI inline INSERT: %v\n%s", err, out)
	}
	adm := requireInlineAdmissions(t, c, 1)[0]
	if want := "INSERT INTO " + c.schema.TableID + " (`id`, `region`) FORMAT Native"; adm.SQL != want {
		t.Fatalf("CLI signed SQL=%q, want %q", adm.SQL, want)
	}
	verifyInlineSignature(t, networkID, c.schema, adm)
	requireInlineStoredRows(t, c, []inlineStoredRow{{11, "eu"}})
}

// This captures the current one-row String packet independently with the real
// CLI. It does not assert provenance for the historical 33-byte measurement.
func TestStorageIntegrity_InlineValuesOneStringCapture(t *testing.T) {
	const networkID = "itest-inline-string"
	bin := testenv.ClickHouseCLI(t)
	testenv.CLIVersion(t)
	agent, c := startInlineValuesPair(t, networkID, []lthash.Column{{Name: "s", Type: "String"}})
	conn := openConnNoCompression(t, agent.Addr)
	if err := conn.Exec(context.Background(), "INSERT INTO "+c.schema.TableID+" VALUES ('hello')"); err != nil {
		t.Fatal(err)
	}
	inline := requireInlineAdmissions(t, c, 1)[0]
	out, err := testenv.RunCLIStdin(t, bin, agent.Addr, "", "INSERT INTO "+c.schema.TableID+" FORMAT Values", "('hello')\n")
	if err != nil {
		t.Fatalf("String CLI capture: %v\n%s", err, out)
	}
	ref := requireInlineAdmissions(t, c, 2)[1]
	verifyInlineSignature(t, networkID, c.schema, inline)
	verifyInlineSignature(t, networkID, c.schema, ref)
	// Captured by this test on 2026-09-23: client 26.8.1.368, server
	// 25.8.28.1, compression off, negotiated revision 54470. Preserve every
	// byte, including BlockInfo; this current capture is 28 bytes, not 33.
	fixture, err := os.ReadFile("testdata/inline_values_cli_26_8_string_54470.hex")
	if err != nil {
		t.Fatal(err)
	}
	captured, err := hex.DecodeString(strings.TrimSpace(string(fixture)))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(ref.Payload.Bytes, captured) {
		t.Fatalf("current CLI differs from recorded CLI fixture: current=%x recorded=%x", ref.Payload.Bytes, captured)
	}
	if !bytes.Equal(inline.Payload.Bytes, ref.Payload.Bytes) {
		t.Fatalf("one String row differs: inline=%x CLI=%x", inline.Payload.Bytes, ref.Payload.Bytes)
	}
	var count uint64
	if err := c.conn.QueryRow(context.Background(), "SELECT count() FROM "+c.schema.TableID+" WHERE s = 'hello'").Scan(&count); err != nil || count != 2 {
		t.Fatalf("String physical landing count=%d error=%v, want 2", count, err)
	}
	t.Logf("independent one-row String CLI capture: revision=%d length=%d raw=%x landed=%d", ref.Payload.Revision, len(ref.Payload.Bytes), ref.Payload.Bytes, count)
}

// The Go driver supplies the exact query-text boundary a 25.x CLI leaves on
// the wire. Refusal precedes payload admission; Task 5's raw relay regression
// separately pins unchanged handling of that shape's streamed Data packets.
func TestStorageIntegrity_InlineValuesTruncatedShapeRefused(t *testing.T) {
	agent, c := startInlineValuesPair(t, "itest-inline-truncated", []lthash.Column{{Name: "id", Type: "UInt64"}, {Name: "region", Type: "String"}})
	conn := openConnNoCompression(t, agent.Addr)
	err := conn.Exec(context.Background(), "INSERT INTO "+c.schema.TableID+" VALUES ")
	if err == nil || !strings.Contains(err.Error(), "storage_integrity requires streaming Native INSERT input; INSERT ... VALUES is not supported") {
		t.Fatalf("truncated VALUES changed its old refusal: %v", err)
	}
	requireInlineAdmissions(t, c, 0)
	requireInlineStoredRows(t, c, nil)
	t.Logf("unchanged truncated-VALUES query-text refusal: %v", err)
}
