package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	housegate "github.com/housegate/housegate"
	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/config"
	"github.com/housegate/housegate/pkg/integration/testenv"
	"github.com/housegate/housegate/pkg/log"
	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/network"
	siplugin "github.com/housegate/housegate/pkg/plugins/storageintegrity"
	"github.com/housegate/housegate/pkg/registry"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/rewriter"
	"github.com/housegate/housegate/pkg/sitable"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
	pb "github.com/housegate/rewriter-proto/gen/pb"
)

// openWritesPair starts an SI ingress server with an EMPTY allowlist (spec
// 2026-10-09 D1) and an agent pinned to it. grants maps each account to its
// permission bits on siTenantDB; serverExtra appends options to the server's
// list (after the fixture's own, so they win); agentMutate, when non-nil,
// adjusts the agent config after the SI agent defaults are applied.
func openWritesPair(t *testing.T, key string, consumer siplugin.AdmissionConsumer, grants map[string]registry.DbAuth, serverExtra []testenv.ProxyOption, agentMutate func(*config.Config)) *testenv.TestProxy {
	t.Helper()
	const networkID = "itest-open-writes"
	ch := openConn(t, chEnv.Addr)
	if err := ch.Exec(context.Background(), "CREATE TABLE IF NOT EXISTS "+siEventsPhysical()+" (id UInt64, region String) ENGINE = MergeTree ORDER BY id"); err != nil {
		t.Fatal(err)
	}
	opts := []testenv.ProxyOption{
		siTenantMock(t),
		testenv.WithExtraDatabases(siTenantDB),
		authProxyConfig(nil, false), // any signer authenticates; authorization is the writer predicate
		withDeclaredSchema(t, networkID),
		testenv.WithConfigMutator(func(cfg *config.Config) {
			cfg.Rewriter.PhysicalDatabase = chEnv.Database
			cfg.StorageIntegrity.Ingress.Enabled = true
			cfg.StorageIntegrity.Ingress.AllowedAddresses = nil
			cfg.StorageIntegrity.Ingress.NetworkID = networkID
		}),
		func(_ *config.Config, opts *housegate.Options) { opts.StorageIntegrityAdmissionConsumer = consumer },
	}
	for account, bits := range grants {
		opts = append(opts, testenv.WithDatabasePermission(account, siTenantDB, bits))
	}
	server := testenv.StartServerProxy(t, chEnv.Addr, append(opts, serverExtra...)...)
	return testenv.StartAgentProxy(t, key, server.Addr,
		withDeclaredSchema(t, networkID),
		testenv.WithConfigMutator(func(cfg *config.Config) {
			cfg.StorageIntegrity.Agent.Enabled = true
			cfg.StorageIntegrity.Agent.NetworkID = networkID
			cfg.StorageIntegrity.Agent.StateDir = t.TempDir()
			cfg.StorageIntegrity.Agent.RequireNetworkState = false
			if agentMutate != nil {
				agentMutate(cfg)
			}
		}),
	)
}

func sendTwoRows(t *testing.T, addr string) error {
	t.Helper()
	conn := openConnNoCompression(t, addr)
	batch, err := conn.PrepareBatch(context.Background(), "INSERT INTO "+siTenantDB+".si_events")
	if err != nil {
		return err
	}
	_ = batch.Append(uint64(1), "eu")
	_ = batch.Append(uint64(2), "us")
	return batch.Send()
}

func waitAdmissions(t *testing.T, c *capturingConsumer, n int) []siplugin.Admission {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		c.mu.Lock()
		seen := append([]siplugin.Admission(nil), c.seen...)
		c.mu.Unlock()
		if len(seen) >= n || time.Now().After(deadline) {
			if len(seen) != n {
				t.Fatalf("consumer saw %d admissions, want %d", len(seen), n)
			}
			return seen
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// teeHandler forwards every record to two handlers: the capture and the
// handler that was the package default, so test output keeps its logs.
type teeHandler struct{ a, b slog.Handler }

func (h teeHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return h.a.Enabled(ctx, l) || h.b.Enabled(ctx, l)
}

func (h teeHandler) Handle(ctx context.Context, r slog.Record) error {
	if h.a.Enabled(ctx, r.Level) {
		_ = h.a.Handle(ctx, r.Clone())
	}
	if h.b.Enabled(ctx, r.Level) {
		return h.b.Handle(ctx, r)
	}
	return nil
}

func (h teeHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return teeHandler{h.a.WithAttrs(attrs), h.b.WithAttrs(attrs)}
}

func (h teeHandler) WithGroup(name string) slog.Handler {
	return teeHandler{h.a.WithGroup(name), h.b.WithGroup(name)}
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) lines() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return strings.Split(strings.TrimSpace(b.buf.String()), "\n")
}

// captureLogs swaps the package-default logger for one that also writes JSON
// records into a buffer, restoring it on cleanup. Every proxy session binds
// its logger from the default when the connection is accepted, so it must be
// installed before the test connects. The returned function lists the
// captured records whose message is msg.
func captureLogs(t *testing.T) func(msg string) []map[string]any {
	t.Helper()
	prev := log.Default()
	buf := &lockedBuffer{}
	capture := slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelInfo})
	log.SetDefault(log.New(teeHandler{capture, prev.Slog().Handler()}))
	t.Cleanup(func() { log.SetDefault(prev) })
	return func(msg string) []map[string]any {
		var out []map[string]any
		for _, line := range buf.lines() {
			var rec map[string]any
			if json.Unmarshal([]byte(line), &rec) == nil && rec["msg"] == msg {
				out = append(out, rec)
			}
		}
		return out
	}
}

// requireAdmittedAuditLine requires the ingress's info audit line for adm and
// its signer/owner/principal to be the ones the admission carries.
func requireAdmittedAuditLine(t *testing.T, records func(string) []map[string]any, adm siplugin.Admission) {
	t.Helper()
	for _, rec := range records("storage_integrity statement admitted") {
		if rec["statement_id"] != adm.StatementID {
			continue
		}
		if rec["signer"] != adm.Signer || rec["owner"] != adm.Owner || rec["principal"] != adm.Principal || rec["table_id"] != adm.TableID {
			t.Fatalf("audit line %v does not carry the admission's signer/owner/principal %s/%s/%s", rec, adm.Signer, adm.Owner, adm.Principal)
		}
		return
	}
	t.Fatalf("no \"storage_integrity statement admitted\" audit line for %s", adm.StatementID)
}

// §9.2 bullet 1: a writer that no allowlist names inserts, through every
// client shape the signed lane admits; Read-only and denylisted signers are
// refused with 497.
func TestOpenWrites_WriterInsertsAndOthersAre497(t *testing.T) {
	signer, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatal(err)
	}
	writerGrant := map[string]registry.DbAuth{signer.Address(): registry.DbAuthWrite}
	requireSelfWrite := func(t *testing.T, adm siplugin.Admission, records func(string) []map[string]any) {
		t.Helper()
		if adm.Signer != signer.Address() || adm.Principal != signer.Address() || adm.Owner != "" {
			t.Fatalf("admission signer/owner/principal = %s/%s/%s", adm.Signer, adm.Owner, adm.Principal)
		}
		requireAdmittedAuditLine(t, records, adm)
	}
	t.Run("writer", func(t *testing.T) {
		records := captureLogs(t)
		consumer := &capturingConsumer{}
		agent := openWritesPair(t, authTestKey1, consumer, writerGrant, nil, nil)
		if err := sendTwoRows(t, agent.Addr); err != nil {
			t.Fatalf("a non-allowlisted writer must insert: %v", err)
		}
		requireSelfWrite(t, waitAdmissions(t, consumer, 1)[0], records)
	})
	t.Run("writer CLI FORMAT CSV", func(t *testing.T) {
		bin := testenv.ClickHouseCLI(t)
		records := captureLogs(t)
		consumer := &capturingConsumer{}
		agent := openWritesPair(t, authTestKey1, consumer, writerGrant, nil, nil)
		// The SQL names the logical database and the session database stays
		// empty: the CLI copies --database into an unsigned Query setting.
		out, err := testenv.RunCLIStdin(t, bin, agent.Addr, "", "INSERT INTO "+siTenantDB+".si_events FORMAT CSV", "1,eu\n2,us\n")
		if err != nil {
			t.Fatalf("a non-allowlisted writer must insert through the CLI: %v\nout: %s", err, out)
		}
		adm := waitAdmissions(t, consumer, 1)[0]
		if adm.Payload.Encoding != sicore.PayloadEncodingClickHouseNativeData {
			t.Fatalf("CLI FORMAT CSV stored encoding %q", adm.Payload.Encoding)
		}
		requireSelfWrite(t, adm, records)
	})
	t.Run("writer inline VALUES", func(t *testing.T) {
		lib := requireNativeLib(t)
		records := captureLogs(t)
		consumer := &capturingConsumer{}
		agent := openWritesPair(t, authTestKey1, consumer, writerGrant, nil, func(cfg *config.Config) {
			cfg.StorageIntegrity.Agent.InlineValues.Enabled = true
			cfg.Materialize.Enabled = true
			cfg.Materialize.Engine = rewriter.EngineNative
			cfg.Materialize.NativeLibraryPath = lib
		})
		conn := openConnNoCompression(t, agent.Addr)
		if err := conn.Exec(context.Background(), "INSERT INTO "+siTenantDB+".si_events (id, region) VALUES (1, 'eu'), (2, 'us')"); err != nil {
			t.Fatalf("a non-allowlisted writer must insert inline VALUES: %v", err)
		}
		adm := waitAdmissions(t, consumer, 1)[0]
		if want := "INSERT INTO " + siTenantDB + ".si_events (`id`, `region`) FORMAT Native"; adm.SQL != want {
			t.Fatalf("signed SQL = %q, want %q", adm.SQL, want)
		}
		requireSelfWrite(t, adm, records)
	})
	for name, tc := range map[string]struct {
		grant  registry.DbAuth
		denied []string
		want   string
	}{
		"read-only grantee": {grant: registry.DbAuthRead, want: "is not a writer of database " + siTenantDB},
		"denylisted signer": {grant: registry.DbAuthWrite, denied: []string{signer.Address()}, want: "is not permitted to write storage-integrity tables"},
	} {
		t.Run(name, func(t *testing.T) {
			consumer := &capturingConsumer{}
			extra := []testenv.ProxyOption{testenv.WithConfigMutator(func(cfg *config.Config) {
				cfg.StorageIntegrity.Ingress.DeniedAddresses = tc.denied
			})}
			agent := openWritesPair(t, authTestKey1, consumer, map[string]registry.DbAuth{signer.Address(): tc.grant}, extra, nil)
			err := sendTwoRows(t, agent.Addr)
			if err == nil || !strings.Contains(err.Error(), "code: 497") || !strings.Contains(err.Error(), tc.want) || !strings.Contains(err.Error(), "[client_seq unspent]") {
				t.Fatalf("err = %v, want a marked 497 %q", err, tc.want)
			}
			if consumer.count() != 0 {
				t.Fatal("a refused write must not reach the consumer")
			}
		})
	}
}

// §9.2 bullet 1: an operator key writes for an owner that is a writer.
func TestOpenWrites_OperatorForAWriterOwner(t *testing.T) {
	operator, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatal(err)
	}
	const owner = "0x00000000000000000000000000000000000000c2"
	records := captureLogs(t)
	consumer := &capturingConsumer{}
	operatorRelation := func(_ *config.Config, opts *housegate.Options) {
		opts.NetworkState.(*network.InMemoryNetworkState).SetOperator(owner, network.AccountAddress(operator.Address()), true)
	}
	agent := openWritesPair(t, authTestKey1, consumer, map[string]registry.DbAuth{owner: registry.DbAuthOwner},
		[]testenv.ProxyOption{operatorRelation},
		func(cfg *config.Config) { cfg.Agent.Owner = owner })
	if err := sendTwoRows(t, agent.Addr); err != nil {
		t.Fatalf("operator for a writer owner must insert: %v", err)
	}
	adm := waitAdmissions(t, consumer, 1)[0]
	if adm.Signer != operator.Address() || adm.Owner != owner || adm.Principal != owner {
		t.Fatalf("admission signer/owner/principal = %s/%s/%s", adm.Signer, adm.Owner, adm.Principal)
	}
	requireAdmittedAuditLine(t, records, adm)
}

// refuseOnceConsumer answers the first admission with a marked 252, then
// captures.
type refuseOnceConsumer struct {
	capturingConsumer
	refuseMu sync.Mutex
	refused  string // the refused statement id
}

func (c *refuseOnceConsumer) ConsumeStorageIntegrityAdmission(ctx context.Context, adm siplugin.Admission) error {
	c.refuseMu.Lock()
	first := c.refused == ""
	if first {
		c.refused = adm.StatementID
	}
	c.refuseMu.Unlock()
	if first {
		return &chproto.ClientError{Code: chproto.CodeTooManyParts, Message: "storage_integrity: back-pressure: retry later", KeepSession: true, SeqUnspent: true}
	}
	return c.capturingConsumer.ConsumeStorageIntegrityAdmission(ctx, adm)
}

func (c *refuseOnceConsumer) refusedID() string {
	c.refuseMu.Lock()
	defer c.refuseMu.Unlock()
	return c.refused
}

// §9.2 bullet 4: a 252 back-pressure refusal is marked unspent; the agent
// recycles the seq and the retry reuses it, so no gap range opens.
func TestOpenWrites_BackpressureRecyclesTheSeq(t *testing.T) {
	signer, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatal(err)
	}
	consumer := &refuseOnceConsumer{}
	agent := openWritesPair(t, authTestKey1, consumer, map[string]registry.DbAuth{signer.Address(): registry.DbAuthWrite}, nil, nil)
	err = sendTwoRows(t, agent.Addr)
	if err == nil || !strings.Contains(err.Error(), "code: 252") || !strings.Contains(err.Error(), "[client_seq unspent]") {
		t.Fatalf("first insert err = %v, want the marked 252", err)
	}
	if err := sendTwoRows(t, agent.Addr); err != nil {
		t.Fatalf("retry: %v", err)
	}
	retried := waitAdmissions(t, &consumer.capturingConsumer, 1)[0]
	_, firstSeq, firstNonce, _ := sicore.ParseFlatStatementID(consumer.refusedID())
	_, retrySeq, retryNonce, _ := sicore.ParseFlatStatementID(retried.StatementID)
	if firstSeq != 1 || retrySeq != 1 || firstNonce == retryNonce {
		t.Fatalf("first=%s retry=%s; want seq 1 reused under a new nonce", consumer.refusedID(), retried.StatementID)
	}
}

// openConnDB opens one uncompressed connection with database as the session
// database. MaxOpenConns is 1 so every statement reuses the same TCP session:
// a test that relies on session state (such as an agent upstream switch) must
// not have a later statement land on a fresh connection.
func openConnDB(t *testing.T, addr, database string) clickhouse.Conn {
	t.Helper()
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr:         []string{addr},
		Auth:         clickhouse.Auth{Database: database, Username: chEnv.User, Password: chEnv.Password},
		Protocol:     clickhouse.Native,
		Compression:  &clickhouse.Compression{Method: clickhouse.CompressionNone},
		MaxOpenConns: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// §9.2 bullet 5: the agent starts on indexer 1, the INSERT's database lives
// on indexer 2; the session switches, the INSERT succeeds there, and a later
// unqualified SELECT runs on indexer 2 against the session database.
func TestOpenWrites_AgentSwitchesToTheHostingIndexer(t *testing.T) {
	signer, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatal(err)
	}
	const networkID = "itest-switch"
	ch := openConn(t, chEnv.Addr)
	if err := ch.Exec(context.Background(), "CREATE TABLE IF NOT EXISTS "+siEventsPhysical()+" (id UInt64, region String) ENGINE = MergeTree ORDER BY id"); err != nil {
		t.Fatal(err)
	}
	server := func(consumer siplugin.AdmissionConsumer) (*testenv.TestProxy, *testenv.RewriterMock) {
		rewriterOpt, mock := testenv.WithRewriterMock(t)
		mock.MapDatabase(siTenantDB, chEnv.Database)
		mock.SetAccessedTables("INSERT INTO "+siTenantDB+".si_events", []*pb.AccessedTable{{
			OriginalDatabase: siTenantDB, OriginalTable: "si_events", LogicalDatabase: siTenantDB, PhysicalDatabase: chEnv.Database, IsStorageIntegrity: true,
		}})
		return testenv.StartServerProxy(t, chEnv.Addr,
			rewriterOpt,
			testenv.WithExtraDatabases(siTenantDB),
			authProxyConfig(nil, false),
			testenv.WithDatabasePermission(signer.Address(), siTenantDB, registry.DbAuthWrite),
			withDeclaredSchema(t, networkID),
			testenv.WithConfigMutator(func(cfg *config.Config) {
				cfg.Rewriter.PhysicalDatabase = chEnv.Database
				cfg.StorageIntegrity.Ingress.Enabled = true
				cfg.StorageIntegrity.Ingress.NetworkID = networkID
			}),
			func(_ *config.Config, opts *housegate.Options) { opts.StorageIntegrityAdmissionConsumer = consumer },
		), mock
	}
	first, firstMock := server(&capturingConsumer{})
	hostingConsumer := &capturingConsumer{}
	hosting, hostingMock := server(hostingConsumer)
	agent := testenv.StartAgentProxyWithSelector(t, authTestKey1,
		testenv.WithPeerAt(1, first),
		testenv.WithPeerAt(2, hosting),
		testenv.WithLogicalDatabaseAt("home", 1),
		testenv.WithLogicalDatabaseAt(siTenantDB, 2),
		// The Selector's permissioned tier sees only indexer 1.
		testenv.WithDatabasePermission(signer.Address(), "home", registry.DbAuthRead),
		withDeclaredSchema(t, networkID),
		testenv.WithConfigMutator(func(cfg *config.Config) {
			cfg.StorageIntegrity.Agent.Enabled = true
			cfg.StorageIntegrity.Agent.NetworkID = networkID
			cfg.StorageIntegrity.Agent.StateDir = t.TempDir()
			cfg.StorageIntegrity.Agent.RequireNetworkState = false
		}),
	)
	conn := openConnDB(t, agent.Addr, siTenantDB)
	batch, err := conn.PrepareBatch(context.Background(), "INSERT INTO "+siTenantDB+".si_events")
	if err != nil {
		t.Fatalf("PrepareBatch: %v", err)
	}
	_ = batch.Append(uint64(7), "eu")
	if err := batch.Send(); err != nil {
		t.Fatalf("INSERT through the switched session: %v", err)
	}
	waitAdmissions(t, hostingConsumer, 1)
	if err := conn.Exec(context.Background(), "SELECT count() FROM si_events"); err != nil {
		t.Fatalf("unqualified SELECT after the switch: %v", err)
	}
	seenOn := func(m *testenv.RewriterMock, prefix string) bool {
		for _, sql := range m.SeenSQL() {
			if strings.HasPrefix(sql, prefix) {
				return true
			}
		}
		return false
	}
	if !seenOn(hostingMock, "INSERT INTO "+siTenantDB) || !seenOn(hostingMock, "SELECT count() FROM si_events") {
		t.Fatalf("hosting indexer saw %v", hostingMock.SeenSQL())
	}
	if seenOn(firstMock, "INSERT INTO "+siTenantDB) {
		t.Fatal("the INSERT must not run on indexer 1")
	}
}

// §9.2 bullet 6: on the SI host, a peer-trusted statement that reads the
// ordinary physical table of a governed table is refused by sipeerguard.
// The internal listener pre-flags every session as peer-trusted and
// non-forwarded, exactly like the measured remote() secondary query; the
// test connects to it directly instead of driving a remote() loopback.
func TestOpenWrites_PeerGuardRefusesGovernedPhysicalReads(t *testing.T) {
	lib := requireNativeLib(t)
	const phys = "phys_pg"
	seed := openConnNoDB(t, chEnv.Addr)
	ctx := context.Background()
	for _, q := range []string{"DROP DATABASE IF EXISTS " + phys, "CREATE DATABASE " + phys,
		"CREATE TABLE " + phys + ".`pgdb.t` (id UInt64) ENGINE = MergeTree ORDER BY id"} {
		if err := seed.Exec(ctx, q); err != nil {
			t.Fatalf("seed %q: %v", q, err)
		}
	}
	t.Cleanup(func() { _ = seed.Exec(ctx, "DROP DATABASE IF EXISTS "+phys) })
	schema := payloadexec.TableSchema{TableID: "pgdb.t", Columns: []lthash.Column{{Name: "id", Type: "UInt64"}}}
	state := sitable.NewFake(sitable.Ordinary, sitable.Table{ID: "pgdb.t", Status: sitable.Active, Schema: schema, SchemaHash: payloadexec.TableSchemaHash("itest-pg", schema)})
	// startServerWithInternal runs every listener (RunWith would bind only
	// the external one).
	_, internal := startServerWithInternal(t, chEnv.Addr,
		testenv.WithExtraDatabases("pgdb"),
		testenv.WithConfigMutator(func(cfg *config.Config) {
			enabled := true
			cfg.Rewriter.Engine = "native"
			cfg.Rewriter.NativeLibraryPath = lib
			cfg.Rewriter.PhysicalDatabase = phys
			// Storage integrity refuses the transport fail-open switch.
			cfg.Rewriter.FailOpenOnUnavailable = false
			cfg.StorageIntegrity.Enabled = &enabled
		}),
		func(_ *config.Config, opts *housegate.Options) { opts.StorageIntegrityTableState = state },
	)
	conn := openConnDB(t, internal, phys)
	err := conn.Exec(ctx, "SELECT count() AS `count()` FROM `"+phys+"`.`pgdb.t` AS `__table1`")
	want := "storage_integrity: table pgdb.t is governed by storage integrity and must be read through its host indexer; connect with --database pgdb or USE pgdb"
	if err == nil || !strings.Contains(err.Error(), "code: 392") || !strings.Contains(err.Error(), want) {
		t.Fatalf("err = %v, want the 392 peer-guard refusal", err)
	}
	if err := conn.Exec(ctx, "SELECT 1"); err != nil {
		t.Fatalf("a non-governed peer statement must pass: %v", err)
	}
}
