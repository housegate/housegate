package integration

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/google/uuid"

	housegate "github.com/housegate/housegate"
	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/config"
	"github.com/housegate/housegate/pkg/integration/testenv"
	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/registry"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/chexec"
	"github.com/housegate/housegate/pkg/replay/nativepayload"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
	pb "github.com/housegate/rewriter-proto/gen/pb"
)

// This file is the cross-lane, cross-client and cross-version evidence for the
// column types admission rules v2 admits (Spec Q Phase 2 revision): Date32,
// UUID, Int128/256, UInt128/256, LowCardinality(String) and Nullable over
// every plain scalar. Every assertion compares row hashes, because the row
// hash is what consensus commits to; the suite runs unchanged against every
// ClickHouse server version CI selects with HOUSEGATE_CH_IMAGE and every
// client binary HOUSEGATE_CH_CLIENTS lists.

// v2ReplaySchema declares one column per v2 shape the replay lanes must agree
// on, plus a v1 key column.
func v2ReplaySchema(tableID string) payloadexec.TableSchema {
	return payloadexec.TableSchema{TableID: tableID, Columns: []lthash.Column{
		{Name: "id", Type: "UInt64"},
		{Name: "d32", Type: "Date32"},
		{Name: "u", Type: "UUID"},
		{Name: "i128", Type: "Int128"},
		{Name: "i256", Type: "Int256"},
		{Name: "u128", Type: "UInt128"},
		{Name: "u256", Type: "UInt256"},
		{Name: "lc", Type: "LowCardinality(String)"},
		{Name: "n_u64", Type: "Nullable(UInt64)"},
		{Name: "n_s", Type: "Nullable(String)"},
		{Name: "n_fs", Type: "Nullable(FixedString(32))"},
		{Name: "n_b", Type: "Nullable(Bool)"},
		{Name: "n_f64", Type: "Nullable(Float64)"},
		{Name: "n_d", Type: "Nullable(Date)"},
		{Name: "n_d32", Type: "Nullable(Date32)"},
		{Name: "n_dt", Type: "Nullable(DateTime('UTC'))"},
		{Name: "n_dt64", Type: "Nullable(DateTime64(3, 'UTC'))"},
		{Name: "n_u", Type: "Nullable(UUID)"},
		{Name: "n_i256", Type: "Nullable(Int256)"},
		{Name: "n_u128", Type: "Nullable(UInt128)"},
	}}
}

func i128(v *big.Int) proto.Int128 {
	b := twosLE(v, 16)
	return proto.Int128{Low: le64(b[0:8]), High: le64(b[8:16])}
}
func u128(v *big.Int) proto.UInt128 {
	b := twosLE(v, 16)
	return proto.UInt128{Low: le64(b[0:8]), High: le64(b[8:16])}
}
func i256(v *big.Int) proto.Int256 {
	b := twosLE(v, 32)
	return proto.Int256{Low: proto.UInt128{Low: le64(b[0:8]), High: le64(b[8:16])}, High: proto.UInt128{Low: le64(b[16:24]), High: le64(b[24:32])}}
}
func u256(v *big.Int) proto.UInt256 {
	b := twosLE(v, 32)
	return proto.UInt256{Low: proto.UInt128{Low: le64(b[0:8]), High: le64(b[8:16])}, High: proto.UInt128{Low: le64(b[16:24]), High: le64(b[24:32])}}
}
func twosLE(v *big.Int, width int) []byte {
	x := new(big.Int).Set(v)
	if x.Sign() < 0 {
		x.Add(x, new(big.Int).Lsh(big.NewInt(1), uint(8*width)))
	}
	be := x.FillBytes(make([]byte, width))
	out := make([]byte, width)
	for i := range be {
		out[width-1-i] = be[i]
	}
	return out
}
func le64(b []byte) uint64 {
	var n uint64
	for i := 7; i >= 0; i-- {
		n = n<<8 | uint64(b[i])
	}
	return n
}

// v2ReplayPayload encodes three rows: extremes, NULL in every Nullable
// column, and ordinary values. It returns the Native payload.
func v2ReplayPayload(t *testing.T) []byte {
	t.Helper()
	one := big.NewInt(1)
	minI128 := new(big.Int).Neg(new(big.Int).Lsh(one, 127))
	maxI256 := new(big.Int).Sub(new(big.Int).Lsh(one, 255), one)
	maxU128 := new(big.Int).Sub(new(big.Int).Lsh(one, 128), one)
	maxU256 := new(big.Int).Sub(new(big.Int).Lsh(one, 256), one)
	ids := [3]uuid.UUID{uuid.Nil, uuid.MustParse("61f0c404-5cb3-11e7-907b-a6006ad3dba0"), uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff")}
	days := [3]time.Time{time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2299, 12, 31, 0, 0, 0, 0, time.UTC), time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC)}
	instant := time.Date(2026, time.July, 16, 12, 34, 56, 123000000, time.UTC)
	var fixed [32]byte
	copy(fixed[:], "fixed")

	id := &proto.ColUInt64{1, 2, 3}
	d32 := new(proto.ColDate32)
	u := new(proto.ColUUID)
	ci128, ci256, cu128, cu256 := new(proto.ColInt128), new(proto.ColInt256), new(proto.ColUInt128), new(proto.ColUInt256)
	lc := proto.NewLowCardinality(new(proto.ColStr))
	for i := 0; i < 3; i++ {
		d32.Append(days[i])
		u.Append(ids[i])
	}
	ci128.AppendArr([]proto.Int128{i128(minI128), i128(big.NewInt(-1)), i128(big.NewInt(42))})
	ci256.AppendArr([]proto.Int256{i256(maxI256), i256(big.NewInt(-300)), i256(big.NewInt(0))})
	cu128.AppendArr([]proto.UInt128{u128(maxU128), u128(big.NewInt(0)), u128(big.NewInt(7))})
	cu256.AppendArr([]proto.UInt256{u256(maxU256), u256(big.NewInt(1)), u256(new(big.Int).Lsh(one, 200))})
	lc.AppendArr([]string{"eu", "", "eu"})

	nU64 := new(proto.ColUInt64).Nullable()
	nU64.AppendArr([]proto.Nullable[uint64]{proto.NewNullable[uint64](0), proto.Null[uint64](), proto.NewNullable[uint64](^uint64(0))})
	nS := new(proto.ColStr).Nullable()
	nS.AppendArr([]proto.Nullable[string]{proto.NewNullable(""), proto.Null[string](), proto.NewNullable("x")})
	nFS := new(proto.ColFixedStr32).Nullable()
	nFS.AppendArr([]proto.Nullable[[32]byte]{proto.NewNullable(fixed), proto.Null[[32]byte](), proto.NewNullable([32]byte{})})
	nB := new(proto.ColBool).Nullable()
	nB.AppendArr([]proto.Nullable[bool]{proto.NewNullable(false), proto.Null[bool](), proto.NewNullable(true)})
	nF := new(proto.ColFloat64).Nullable()
	nF.AppendArr([]proto.Nullable[float64]{proto.NewNullable(-0.0), proto.Null[float64](), proto.NewNullable(2.5)})
	nD := new(proto.ColDate).Nullable()
	nD.AppendArr([]proto.Nullable[time.Time]{proto.NewNullable(time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC)), proto.Null[time.Time](), proto.NewNullable(time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC))})
	nD32 := new(proto.ColDate32).Nullable()
	nD32.AppendArr([]proto.Nullable[time.Time]{proto.NewNullable(days[0]), proto.Null[time.Time](), proto.NewNullable(days[1])})
	nDT := (&proto.ColDateTime{Location: time.UTC}).Nullable()
	nDT.AppendArr([]proto.Nullable[time.Time]{proto.NewNullable(instant.Truncate(time.Second)), proto.Null[time.Time](), proto.NewNullable(time.Unix(0, 0).UTC())})
	nDT64 := new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli).WithLocation(time.UTC).Nullable()
	nDT64.AppendArr([]proto.Nullable[time.Time]{proto.NewNullable(instant), proto.Null[time.Time](), proto.NewNullable(instant.Add(time.Millisecond))})
	nUU := new(proto.ColUUID).Nullable()
	nUU.AppendArr([]proto.Nullable[uuid.UUID]{proto.NewNullable(ids[1]), proto.Null[uuid.UUID](), proto.NewNullable(ids[2])})
	nI256 := new(proto.ColInt256).Nullable()
	nI256.AppendArr([]proto.Nullable[proto.Int256]{proto.NewNullable(i256(big.NewInt(-1))), proto.Null[proto.Int256](), proto.NewNullable(i256(maxI256))})
	nU128 := new(proto.ColUInt128).Nullable()
	nU128.AppendArr([]proto.Nullable[proto.UInt128]{proto.NewNullable(u128(maxU128)), proto.Null[proto.UInt128](), proto.NewNullable(u128(big.NewInt(9)))})

	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ClientCodeData))
	buf.PutString("")
	input := proto.Input{
		{Name: "id", Data: id}, {Name: "d32", Data: d32}, {Name: "u", Data: u},
		{Name: "i128", Data: ci128}, {Name: "i256", Data: ci256}, {Name: "u128", Data: cu128}, {Name: "u256", Data: cu256},
		{Name: "lc", Data: lc}, {Name: "n_u64", Data: nU64}, {Name: "n_s", Data: nS}, {Name: "n_fs", Data: nFS},
		{Name: "n_b", Data: nB}, {Name: "n_f64", Data: nF}, {Name: "n_d", Data: nD}, {Name: "n_d32", Data: nD32},
		{Name: "n_dt", Data: nDT}, {Name: "n_dt64", Data: nDT64}, {Name: "n_u", Data: nUU},
		{Name: "n_i256", Data: nI256}, {Name: "n_u128", Data: nU128},
	}
	if err := (proto.Block{Rows: 3, Columns: len(input)}).EncodeBlock(&buf, 54460, input); err != nil {
		t.Fatalf("encode v2 Native payload: %v", err)
	}
	return append([]byte(nil), buf.Buf...)
}

// TestReplayCHExecutorV2TypesMatchInProcessRoot proves the ClickHouse-backed
// replay lane (DDL on the real server, insert and read back through
// clickhouse-go) and the in-process Native lane commit the same rows and the
// same state root for every v2 shape, on whatever server version runs it.
func TestReplayCHExecutorV2TypesMatchInProcessRoot(t *testing.T) {
	conn := openDirectCH(t)
	tableID := uniqueTable(t)
	schema := v2ReplaySchema(tableID)
	payload := v2ReplayPayload(t)

	inRows, err := nativepayload.Decode(schema, 54460, payload)
	if err != nil {
		t.Fatalf("Native decode: %v", err)
	}
	st := replay.PreparedStatement{Statement: replay.Statement{StatementID: "stmt-v2-1", PayloadFormat: nativepayload.PayloadFormat, ClientRevision: 54460}, Payload: payload}
	chRows, err := chexec.NewMaterializer(chReplayNetwork, conn).Materialize(context.Background(), schema, st)
	if err != nil {
		t.Fatalf("chexec Materialize: %v", err)
	}
	if len(chRows) != len(inRows) {
		t.Fatalf("chexec rows = %d, Native rows = %d", len(chRows), len(inRows))
	}
	hashes := func(rows []payloadexec.Row) []string {
		out := make([]string, len(rows))
		for i, r := range rows {
			h, err := lthash.RowHash(schema.TableID, schema.Columns, r.Values)
			if err != nil {
				t.Fatalf("row %d: %v", i, err)
			}
			out[i] = hex.EncodeToString(h.Bytes())
		}
		sort.Strings(out)
		return out
	}
	if a, b := hashes(chRows), hashes(inRows); !reflect.DeepEqual(a, b) {
		for i := range chRows {
			t.Logf("ch row %d: %#v", i, chRows[i].Values)
		}
		for i := range inRows {
			t.Logf("native row %d: %#v", i, inRows[i].Values)
		}
		t.Fatalf("row hashes differ across lanes:\n ch:     %v\n native: %v", a, b)
	}

	chE := payloadexec.NewWithMaterializer(chReplayNetwork, chexec.NewMaterializer(chReplayNetwork, conn), schema)
	inE := payloadexec.NewWithMaterializer(chReplayNetwork, nativepayload.Materializer{NetworkID: chReplayNetwork}, schema)
	genCH, err := chE.GenesisSnapshot(0, "schema-1", "ch-v2")
	if err != nil {
		t.Fatal(err)
	}
	genIn, err := inE.GenesisSnapshot(0, "schema-1", "ch-v2")
	if err != nil {
		t.Fatal(err)
	}
	job := chReplayJob(genCH, tableID, "stmt-v2-1", "probe", payload, "")
	job.Statements[0].SQL = "INSERT INTO " + tableID + " FORMAT Native"
	job.Statements[0].SQLHash = replay.DigestString(job.Statements[0].SQL)
	job.Statements[0].PayloadFormat = nativepayload.PayloadFormat
	job.Statements[0].ClientRevision = 54460
	prepared := []replay.PreparedStatement{{Statement: job.Statements[0], Payload: payload}}
	_, chRes, err := chE.ApplyContext(context.Background(), genCH, job, prepared)
	if err != nil {
		t.Fatalf("chexec apply: %v", err)
	}
	_, inRes, err := inE.ApplyContext(context.Background(), genIn, job, prepared)
	if err != nil {
		t.Fatalf("in-process apply: %v", err)
	}
	if chRes.ComputedStateRoot != inRes.ComputedStateRoot {
		t.Fatalf("v2 executor equivalence broken:\n  ch: %s\n  in: %s", chRes.ComputedStateRoot, inRes.ComputedStateRoot)
	}
}

// TestClickHouseSortsNullsLastByDefault pins the SQL ordering decision of
// admission rules v2: snapshot-query results keep ClickHouse's own NULL order
// (ASC puts NULLs last, DESC first) and housegate never re-sorts them. The
// canonical commitment order is separate (snapshotquery's byte order).
func TestClickHouseSortsNullsLastByDefault(t *testing.T) {
	conn := openDirectCH(t)
	rows, err := conn.Query(context.Background(), "SELECT v FROM VALUES('v Nullable(UInt64)', (2), (NULL), (1)) ORDER BY v ASC")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var v *uint64
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		if v == nil {
			got = append(got, "NULL")
		} else {
			got = append(got, fmt.Sprint(*v))
		}
	}
	if strings.Join(got, ",") != "1,2,NULL" {
		t.Fatalf("ORDER BY v ASC = %v, want NULLs last", got)
	}
}

// v2CLISchema is the table the client matrix writes through the signed lane.
func v2CLISchema() payloadexec.TableSchema {
	return payloadexec.TableSchema{TableID: siTenantDB + ".si_v2", Columns: []lthash.Column{
		{Name: "id", Type: "UInt64"},
		{Name: "d32", Type: "Date32"},
		{Name: "u", Type: "UUID"},
		{Name: "i128", Type: "Int128"},
		{Name: "u256", Type: "UInt256"},
		{Name: "lc", Type: "LowCardinality(String)"},
		{Name: "n_u64", Type: "Nullable(UInt64)"},
		{Name: "n_s", Type: "Nullable(String)"},
		{Name: "n_dt64", Type: "Nullable(DateTime64(3, 'UTC'))"},
	}}
}

const v2CLIDDL = "(id UInt64, d32 Date32, u UUID, i128 Int128, u256 UInt256, lc LowCardinality(String), n_u64 Nullable(UInt64), n_s Nullable(String), n_dt64 Nullable(DateTime64(3, 'UTC'))) ENGINE = MergeTree ORDER BY id"

// v2CLIExpected is the one logical row every format below spells.
func v2CLIExpected() []any {
	return []any{
		uint64(1),
		time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC),
		uuid.MustParse("61f0c404-5cb3-11e7-907b-a6006ad3dba0"),
		new(big.Int).Neg(new(big.Int).Lsh(big.NewInt(1), 127)),
		new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1)),
		"eu",
		nil,
		"x",
		nil,
	}
}

var v2CLIFormats = []struct{ format, stdin string }{
	{"CSV", "1,1900-01-01,61f0c404-5cb3-11e7-907b-a6006ad3dba0,-170141183460469231731687303715884105728,115792089237316195423570985008687907853269984665640564039457584007913129639935,eu,\\N,x,\\N\n"},
	{"CSVWithNames", "id,d32,u,i128,u256,lc,n_u64,n_s,n_dt64\n1,1900-01-01,61f0c404-5cb3-11e7-907b-a6006ad3dba0,-170141183460469231731687303715884105728,115792089237316195423570985008687907853269984665640564039457584007913129639935,eu,\\N,x,\\N\n"},
	{"TSV", "1\t1900-01-01\t61f0c404-5cb3-11e7-907b-a6006ad3dba0\t-170141183460469231731687303715884105728\t115792089237316195423570985008687907853269984665640564039457584007913129639935\teu\t\\N\tx\t\\N\n"},
	{"Values", "(1,'1900-01-01','61f0c404-5cb3-11e7-907b-a6006ad3dba0',-170141183460469231731687303715884105728,115792089237316195423570985008687907853269984665640564039457584007913129639935,'eu',NULL,'x',NULL)\n"},
	{"JSONEachRow", "{\"id\":1,\"d32\":\"1900-01-01\",\"u\":\"61f0c404-5cb3-11e7-907b-a6006ad3dba0\",\"i128\":\"-170141183460469231731687303715884105728\",\"u256\":\"115792089237316195423570985008687907853269984665640564039457584007913129639935\",\"lc\":\"eu\",\"n_u64\":null,\"n_s\":\"x\",\"n_dt64\":null}\n"},
}

// startSIAgentPairForTable is startSIAgentPairWith for a caller-declared
// table under siTenantDB.
func startSIAgentPairForTable(t *testing.T, networkID, table, ddl string, schema payloadexec.TableSchema) (*testenv.TestProxy, *capturingConsumer) {
	t.Helper()
	signer, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatal(err)
	}
	ch := openConn(t, chEnv.Addr)
	physical := chEnv.Database + "." + table
	if err := ch.Exec(context.Background(), "DROP TABLE IF EXISTS "+physical); err != nil {
		t.Fatal(err)
	}
	if err := ch.Exec(context.Background(), "CREATE TABLE "+physical+" "+ddl); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() { _ = ch.Exec(context.Background(), "DROP TABLE IF EXISTS "+physical) })
	js, err := json.Marshal(schema)
	if err != nil {
		t.Fatal(err)
	}
	declared := func(_ *config.Config, opts *housegate.Options) {
		ns := opts.NetworkState.(*network.InMemoryNetworkState)
		ns.TableSchemas[siTenantDB+"/"+table+"@1"] = network.TableSchemaInfo{
			DatabaseId: siTenantDB, TableId: table, Version: 1,
			SchemaHash: payloadexec.TableSchemaHash(networkID, schema), SchemaJson: string(js),
		}
	}
	rewriterOpt, rewriterMock := testenv.WithRewriterMock(t)
	rewriterMock.MapDatabase(siTenantDB, chEnv.Database)
	rewriterMock.SetAccessedTables("INSERT INTO "+siTenantDB+"."+table, []*pb.AccessedTable{{
		OriginalDatabase: siTenantDB, OriginalTable: table, LogicalDatabase: siTenantDB,
		PhysicalDatabase: chEnv.Database, IsStorageIntegrity: true,
	}})
	consumer := &capturingConsumer{}
	server := testenv.StartServerProxy(t, chEnv.Addr,
		rewriterOpt,
		testenv.WithExtraDatabases(siTenantDB),
		authProxyConfig([]string{signer.Address()}, false),
		testenv.WithDatabasePermission(signer.Address(), siTenantDB, registry.DbAuthWrite),
		declared,
		testenv.WithConfigMutator(func(cfg *config.Config) {
			cfg.Rewriter.PhysicalDatabase = chEnv.Database
			cfg.StorageIntegrity.Ingress.Enabled = true
			cfg.StorageIntegrity.Ingress.AllowedAddresses = []string{signer.Address()}
			cfg.StorageIntegrity.Ingress.NetworkID = networkID
		}),
		func(_ *config.Config, opts *housegate.Options) { opts.StorageIntegrityAdmissionConsumer = consumer },
	)
	agent := testenv.StartAgentProxy(t, authTestKey1, server.Addr,
		declared,
		testenv.WithConfigMutator(func(cfg *config.Config) {
			cfg.StorageIntegrity.Agent.Enabled = true
			cfg.StorageIntegrity.Agent.NetworkID = networkID
			cfg.StorageIntegrity.Agent.StateDir = t.TempDir()
			cfg.StorageIntegrity.Agent.RequireNetworkState = false
		}),
	)
	return agent, consumer
}

// v2CLIClients lists the clickhouse client binaries the matrix runs:
// HOUSEGATE_CH_CLIENTS (comma separated) or the suite's own binary.
func v2CLIClients(t *testing.T) []string {
	t.Helper()
	if env := os.Getenv("HOUSEGATE_CH_CLIENTS"); env != "" {
		return strings.Split(env, ",")
	}
	return []string{testenv.ClickHouseCLI(t)}
}

func waitV2Admissions(t *testing.T, consumer *capturingConsumer, n int) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for consumer.count() < n && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if got := consumer.count(); got != n {
		t.Fatalf("ingress admitted %d statements, want %d", got, n)
	}
}

// TestCLI_V2TypesHashIdenticallyAcrossFormatsAndClients is the client half of
// the evidence: one logical row, spelled in every client-parsed format, by
// every client binary in the matrix, and once through the clickhouse-go fork,
// must reach the signed lane as Native that decodes to exactly the expected
// values, hence to one row hash.
func TestCLI_V2TypesHashIdenticallyAcrossFormatsAndClients(t *testing.T) {
	const networkID = "itest-net"
	schema := v2CLISchema()
	agent, consumer := startSIAgentPairForTable(t, networkID, "si_v2", v2CLIDDL, schema)
	want, err := lthash.RowHash(schema.TableID, schema.Columns, v2CLIExpected())
	if err != nil {
		t.Fatal(err)
	}
	insert := "INSERT INTO " + siTenantDB + ".si_v2 FORMAT "
	n := 0
	type spelled struct{ client, format string }
	var labels []spelled
	for _, bin := range v2CLIClients(t) {
		version, _ := testenv.RunCLIStdin(t, bin, agent.Addr, "", "SELECT version()", "")
		t.Logf("client %s -> server %s", bin, version)
		for _, tc := range v2CLIFormats {
			out, err := testenv.RunCLIStdin(t, bin, agent.Addr, "", insert+tc.format, tc.stdin)
			if err != nil {
				t.Fatalf("%s FORMAT %s refused: %v\n%s", bin, tc.format, err, out)
			}
			n++
			labels = append(labels, spelled{bin, tc.format})
		}
	}
	conn := openConnNoCompression(t, agent.Addr)
	batch, err := conn.PrepareBatch(context.Background(), "INSERT INTO "+siTenantDB+".si_v2")
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.Append(v2CLIExpected()...); err != nil {
		t.Fatal(err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("clickhouse-go insert: %v", err)
	}
	n++
	labels = append(labels, spelled{"clickhouse-go", "Native"})
	waitV2Admissions(t, consumer, n)

	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	for i, adm := range consumer.seen {
		if adm.Payload.Encoding != sicore.PayloadEncodingClickHouseNativeData {
			t.Fatalf("%v: encoding %q", labels[i], adm.Payload.Encoding)
		}
		rows, err := nativepayload.Decode(schema, adm.Payload.Revision, adm.Payload.Bytes)
		if err != nil {
			t.Fatalf("%v: decode: %v", labels[i], err)
		}
		if len(rows) != 1 {
			t.Fatalf("%v: %d rows", labels[i], len(rows))
		}
		got, err := lthash.RowHash(schema.TableID, schema.Columns, rows[0].Values)
		if err != nil {
			t.Fatalf("%v: hash: %v", labels[i], err)
		}
		if !got.Equal(want) {
			t.Fatalf("%v: row hash differs from the expected row\n got values %#v", labels[i], rows[0].Values)
		}
		t.Logf("%v: %d payload bytes, sha %s", labels[i], adm.Payload.Length, adm.Payload.SHA256)
	}
}

// TestCLI_LowCardinalityLargeDictionaryDecodes measures the one LowCardinality
// wire shape the small fixtures cannot reach: a block whose distinct values
// exceed the client's low_cardinality_max_dictionary_size (8192 by default),
// which ClickHouse serializes as several key chunks inside one column. The
// payload must either decode to exactly the client's values or be refused; it
// must never decode to different values.
func TestCLI_LowCardinalityLargeDictionaryDecodes(t *testing.T) {
	const networkID = "itest-net"
	schema := payloadexec.TableSchema{TableID: siTenantDB + ".si_lc", Columns: []lthash.Column{
		{Name: "id", Type: "UInt64"}, {Name: "lc", Type: "LowCardinality(String)"},
	}}
	agent, consumer := startSIAgentPairForTable(t, networkID, "si_lc", "(id UInt64, lc LowCardinality(String)) ENGINE = MergeTree ORDER BY id", schema)
	const rows = 20000
	var csv strings.Builder
	for i := 0; i < rows; i++ {
		fmt.Fprintf(&csv, "%d,v%d\n", i, i%12000)
	}
	expected := 0
	for _, bin := range v2CLIClients(t) {
		out, err := testenv.RunCLIStdin(t, bin, agent.Addr, "", "INSERT INTO "+siTenantDB+".si_lc FORMAT CSV", csv.String())
		if err != nil {
			t.Logf("%s: large LowCardinality insert refused: %v\n%s", bin, err, out)
			continue
		}
		expected++
	}
	if expected == 0 {
		t.Fatal("every client refused the large LowCardinality insert; the measurement needs at least one")
	}
	waitV2Admissions(t, consumer, expected)
	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	for _, adm := range consumer.seen {
		decoded, err := nativepayload.Decode(schema, adm.Payload.Revision, adm.Payload.Bytes)
		if err != nil {
			t.Fatalf("admitted payload does not decode: %v", err)
		}
		if len(decoded) != rows {
			t.Fatalf("decoded %d rows, want %d", len(decoded), rows)
		}
		for _, r := range decoded {
			id := r.Values[0].(uint64)
			if r.Values[1] != fmt.Sprintf("v%d", id%12000) {
				t.Fatalf("row %d decoded lc = %q, want v%d", id, r.Values[1], id%12000)
			}
		}
	}
}

// TestStorageIntegrity_InlineValuesV2TypesHashLikeTheClient covers the one
// lane whose Native bytes housegate itself re-encodes: inline VALUES, which
// the upstream evaluates and the agent re-encodes through ch-go. The v2 row
// must decode to the same values, and hash to the same row, as the client's
// own FORMAT Values spelling of it. Wide integers are quoted: ClickHouse types
// an unquoted literal above UInt64 as Float64, and the closure gate refuses
// one (TestValuesClosure).
func TestStorageIntegrity_InlineValuesV2TypesHashLikeTheClient(t *testing.T) {
	const networkID = "itest-inline-v2"
	schema := v2CLISchema()
	agent, c := startInlineValuesPair(t, networkID, schema.Columns)
	conn := openConnNoCompression(t, agent.Addr)
	sql := "INSERT INTO " + c.schema.TableID + " (id, d32, u, i128, u256, lc, n_u64, n_s, n_dt64) VALUES " +
		"(1, '1900-01-01', '61f0c404-5cb3-11e7-907b-a6006ad3dba0', '-170141183460469231731687303715884105728', " +
		"'115792089237316195423570985008687907853269984665640564039457584007913129639935', 'eu', NULL, 'x', NULL)"
	if err := conn.Exec(context.Background(), sql); err != nil {
		t.Fatalf("inline INSERT: %v", err)
	}
	adm := requireInlineAdmissions(t, c, 1)[0]
	verifyInlineSignature(t, networkID, c.schema, adm)
	rows, err := nativepayload.Decode(c.schema, adm.Payload.Revision, adm.Payload.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("decoded %d rows", len(rows))
	}
	cols := c.schema.Columns
	got, err := lthash.RowHash(c.schema.TableID, cols, rows[0].Values)
	if err != nil {
		t.Fatal(err)
	}
	want, err := lthash.RowHash(c.schema.TableID, cols, v2CLIExpected())
	if err != nil {
		t.Fatal(err)
	}
	if !got.Equal(want) {
		t.Fatalf("inline VALUES row = %#v, want %#v", rows[0].Values, v2CLIExpected())
	}
}
