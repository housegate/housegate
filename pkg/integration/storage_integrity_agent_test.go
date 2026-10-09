package integration

import (
	"context"
	"encoding/json"
	"os"
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
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
	pb "github.com/housegate/rewriter-proto/gen/pb"
)

type capturingConsumer struct {
	mu   sync.Mutex
	seen []siplugin.Admission
}

func (c *capturingConsumer) ConsumeStorageIntegrityAdmission(_ context.Context, adm siplugin.Admission) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seen = append(c.seen, adm)
	return nil
}

func (c *capturingConsumer) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.seen)
}

// siTenantDB is the logical database the signed-lane fixtures write through.
// Tenant SQL names logical databases only: the table-reference guard refuses
// the physical database (rewriter.physical_database, chEnv.Database here;
// spec 2026-09-26 G2), so the rewriter mock maps this qualifier onto the
// physical database where the fixture's ClickHouse table lives.
const siTenantDB = "si_tenant"

// siEventsPhysical is the ClickHouse table behind siTenantDB.si_events; only
// setup and verification done directly against ClickHouse name it.
func siEventsPhysical() string { return chEnv.Database + ".si_events" }

// siTenantMock wires a rewriter mock that maps siTenantDB onto the physical
// database and reports `INSERT INTO siTenantDB.si_events` as a
// storage-integrity access, as a real engine would.
func siTenantMock(t *testing.T) testenv.ProxyOption {
	t.Helper()
	rewriterOpt, rewriterMock := testenv.WithRewriterMock(t)
	rewriterMock.MapDatabase(siTenantDB, chEnv.Database)
	rewriterMock.SetAccessedTables("INSERT INTO "+siTenantDB+".si_events", []*pb.AccessedTable{{
		OriginalDatabase:   siTenantDB,
		OriginalTable:      "si_events",
		LogicalDatabase:    siTenantDB,
		PhysicalDatabase:   chEnv.Database,
		IsStorageIntegrity: true,
	}})
	return rewriterOpt
}

func siAgentSchema() payloadexec.TableSchema {
	return payloadexec.TableSchema{
		TableID: siTenantDB + ".si_events",
		Columns: []lthash.Column{{Name: "id", Type: "UInt64"}, {Name: "region", Type: "String"}},
	}
}

func withDeclaredSchema(t *testing.T, networkID string) testenv.ProxyOption {
	t.Helper()
	schema := siAgentSchema()
	js, err := json.Marshal(schema)
	if err != nil {
		t.Fatal(err)
	}
	return func(_ *config.Config, opts *housegate.Options) {
		ns := opts.NetworkState.(*network.InMemoryNetworkState)
		ns.TableSchemas[siTenantDB+"/si_events@1"] = network.TableSchemaInfo{
			DatabaseId: siTenantDB,
			TableId:    "si_events",
			Version:    1,
			SchemaHash: payloadexec.TableSchemaHash(networkID, schema),
			SchemaJson: string(js),
		}
	}
}

// startSIAgentPair brings up the server (ingress) + agent (signer) pair the
// envelope-v2 end-to-end tests share, and returns the agent proxy plus the
// consumer that captures what the ingress admitted. Every test uses exactly
// one fixture so a drift in one cannot silently diverge from the others.
//
// The server runs with rewriter.physical_database = chEnv.Database, so the
// table-reference guard's physical-database rule is live: the clients write
// siTenantDB.si_events and the mock maps it onto the physical table. A CLI
// client leaves ClientHello.Database empty (the 25.8 client otherwise copies
// --database into Query settings, and that unsigned setting is correctly
// refused); every statement still reaches the rewriter (spec 2026-09-26 T8).
func startSIAgentPair(t *testing.T, networkID string) (*testenv.TestProxy, *capturingConsumer) {
	t.Helper()
	agentProxy, _, consumer := startSIAgentPairWith(t, networkID, siAgentPairOptions{})
	return agentProxy, consumer
}

// siAgentPairOptions varies the shared fixture for the client-lane tests
// (spec 2026-10-09 D15).
type siAgentPairOptions struct {
	// ClientLanes makes both proxies report client lanes active on the
	// network (housegate.Options.StorageIntegrityClientLanes). The agent has
	// no RPC discovery here, so that port is what it reads.
	ClientLanes bool
	// AgentStateDir replaces the agent's fresh state_dir when non-empty.
	AgentStateDir string
	// Lanes sets storage_integrity.agent.lanes.
	Lanes string
}

// startSIAgentPairWith is startSIAgentPair with options; it also returns the
// server proxy so a test can attach more agents to it.
func startSIAgentPairWith(t *testing.T, networkID string, o siAgentPairOptions) (agent, server *testenv.TestProxy, consumer *capturingConsumer) {
	t.Helper()
	signer, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatal(err)
	}
	ch := openConn(t, chEnv.Addr)
	if err := ch.Exec(context.Background(), "CREATE TABLE IF NOT EXISTS "+siEventsPhysical()+" (id UInt64, region String) ENGINE = MergeTree ORDER BY id"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	consumer = &capturingConsumer{}
	rewriterOpt := siTenantMock(t)
	opts := []testenv.ProxyOption{
		rewriterOpt,
		testenv.WithExtraDatabases(siTenantDB),
		authProxyConfig([]string{signer.Address()}, false),
		testenv.WithDatabasePermission(signer.Address(), siTenantDB, registry.DbAuthWrite),
		withDeclaredSchema(t, networkID),
		testenv.WithConfigMutator(func(cfg *config.Config) {
			cfg.Rewriter.PhysicalDatabase = chEnv.Database
			cfg.StorageIntegrity.Ingress.Enabled = true
			cfg.StorageIntegrity.Ingress.AllowedAddresses = []string{signer.Address()}
			cfg.StorageIntegrity.Ingress.NetworkID = networkID
		}),
		func(_ *config.Config, opts *housegate.Options) {
			opts.StorageIntegrityAdmissionConsumer = consumer
		},
	}
	if o.ClientLanes {
		opts = append(opts, withClientLanes())
	}
	server = testenv.StartServerProxy(t, chEnv.Addr, opts...)
	stateDir := o.AgentStateDir
	if stateDir == "" {
		stateDir = t.TempDir()
	}
	agent = startSIAgent(t, server.Addr, networkID, stateDir, o.Lanes, o.ClientLanes)
	return agent, server, consumer
}

// withClientLanes reports client lanes active on the proxy's network.
func withClientLanes() testenv.ProxyOption {
	return func(_ *config.Config, opts *housegate.Options) {
		opts.StorageIntegrityClientLanes = func() bool { return true }
	}
}

// startSIAgent starts one signing agent with authTestKey1 against serverAddr.
func startSIAgent(t *testing.T, serverAddr, networkID, stateDir, lanes string, clientLanes bool) *testenv.TestProxy {
	t.Helper()
	opts := []testenv.ProxyOption{
		withDeclaredSchema(t, networkID),
		testenv.WithConfigMutator(func(cfg *config.Config) {
			cfg.StorageIntegrity.Agent.Enabled = true
			cfg.StorageIntegrity.Agent.NetworkID = networkID
			cfg.StorageIntegrity.Agent.StateDir = stateDir
			cfg.StorageIntegrity.Agent.RequireNetworkState = false
			cfg.StorageIntegrity.Agent.Lanes = lanes
		}),
	}
	if clientLanes {
		opts = append(opts, withClientLanes())
	}
	return testenv.StartAgentProxy(t, authTestKey1, serverAddr, opts...)
}

// startSecondAgent starts another agent with the same key and the identical
// agent options against the same server, on a network with client lanes
// active.
func startSecondAgent(t *testing.T, serverAddr, networkID, stateDir, lanes string) *testenv.TestProxy {
	t.Helper()
	return startSIAgent(t, serverAddr, networkID, stateDir, lanes, true)
}

// TestStorageIntegrity_AgentSignsEnvelopeV2EndToEnd runs client -> agent
// housegate (storage_integrity.agent) -> server housegate (ingress) -> CH and
// proves the ingress stored exactly the bytes the agent signed: the v2 token
// validates against a payload_hash recomputed from the stored bytes, and those
// bytes decode (Native, at the pinned revision) into the rows the client sent.
func TestStorageIntegrity_AgentSignsEnvelopeV2EndToEnd(t *testing.T) {
	const networkID = "itest-net"
	agentProxy, consumer := startSIAgentPair(t, networkID)
	// Re-derived rather than returned: the address is a pure function of the
	// constant key, so the shared fixture keeps a two-value signature.
	signer, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatal(err)
	}

	conn := openConnNoCompression(t, agentProxy.Addr)
	batch, err := conn.PrepareBatch(context.Background(), "INSERT INTO "+siTenantDB+".si_events")
	if err != nil {
		t.Fatalf("PrepareBatch through agent: %v", err)
	}
	if err := batch.Append(uint64(1), "eu"); err != nil {
		t.Fatal(err)
	}
	if err := batch.Append(uint64(2), "us"); err != nil {
		t.Fatal(err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("batch.Send through agent -> server: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		consumer.mu.Lock()
		n := len(consumer.seen)
		consumer.mu.Unlock()
		if n == 1 || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	if len(consumer.seen) != 1 {
		t.Fatalf("consumer saw %d admissions, want 1", len(consumer.seen))
	}
	adm := consumer.seen[0]
	if adm.EnvelopeVersion != 2 || adm.NetworkID != networkID || adm.Payload.Encoding != sicore.PayloadEncodingClickHouseNativeData || adm.Payload.Revision == 0 {
		t.Fatalf("admission is not envelope v2: %+v", adm)
	}
	if account, seq, _, err := sicore.ParseFlatStatementID(adm.StatementID); err != nil || account != signer.Address() || seq != 1 {
		t.Fatalf("statement id %q: account=%s seq=%d err=%v", adm.StatementID, account, seq, err)
	}
	// The stored bytes are the signed bytes: recompute the expectation from the
	// stored payload and validate the sequenced token against it.
	validator := auth.NewEthValidator([]string{signer.Address()}, time.Minute, true, false, "", nil)
	want := auth.JWSStatementPayloadV2{
		NetworkID:      networkID,
		StatementID:    adm.StatementID,
		SQLHash:        replay.DigestString(adm.SQL),
		SettingsHash:   sicore.EmptySettingsHash,
		SchemaHash:     adm.SchemaHash,
		PayloadHash:    replay.DigestBytes(adm.Payload.Bytes),
		PayloadLength:  uint64(len(adm.Payload.Bytes)),
		PayloadFormat:  sicore.PayloadEncodingClickHouseNativeData,
		ClientRevision: uint32(adm.Payload.Revision),
		TargetTableID:  siTenantDB + ".si_events",
		RowIDProfileID: payloadexec.RowIDProfileID,
		StatementKind:  sicore.StatementKindCodeInsert,
	}
	if got, err := validator.ValidateStatementV2(adm.UserJWS, want); err != nil || got != signer.Address() {
		t.Fatalf("stored bytes do not match the signed envelope: signer=%s err=%v", got, err)
	}
	if adm.SchemaHash != payloadexec.TableSchemaHash(networkID, siAgentSchema()) {
		t.Fatalf("schema_hash %s does not match the declared schema", adm.SchemaHash)
	}
	rows, err := nativepayload.Decode(siAgentSchema(), adm.Payload.Revision, adm.Payload.Bytes)
	if err != nil {
		t.Fatalf("stored bytes are not the client's Native Data packets: %v", err)
	}
	if len(rows) != 2 || rows[0].Values[0] != uint64(1) || rows[0].Values[1] != "eu" || rows[1].Values[0] != uint64(2) || rows[1].Values[1] != "us" {
		t.Fatalf("decoded rows = %+v", rows)
	}
	if !strings.HasPrefix(adm.SQL, "INSERT INTO "+siTenantDB+".si_events") {
		t.Fatalf("signed SQL = %q", adm.SQL)
	}
}

// TestStorageIntegrity_OwnedSettingKeysEndToEnd is Spec P D3. Spec K D6 added
// SQL_x_read_mode to the enumerated owned key set so a client may still choose
// its read mode on an SI-configured deployment; the enumeration (not a
// SQL_x_ / SQL_sentio_ prefix) is what keeps every OTHER client setting off the
// signed lane, because settings_hash commits to the empty user-settings set.
// Neither direction had an end-to-end proof.
func TestStorageIntegrity_OwnedSettingKeysEndToEnd(t *testing.T) {
	const networkID = "itest-net-settings"
	agentProxy, consumer := startSIAgentPair(t, networkID)
	conn := openConnNoCompression(t, agentProxy.Addr)

	// An owned key rides through: the lane admits it and still signs the
	// empty-settings digest.
	ownedCtx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
		sicore.ReadModeSettingKey: clickhouse.CustomSetting{Value: "safe"},
	}))
	batch, err := conn.PrepareBatch(ownedCtx, "INSERT INTO "+siTenantDB+".si_events")
	if err != nil {
		t.Fatalf("PrepareBatch with %s: %v", sicore.ReadModeSettingKey, err)
	}
	if err := batch.Append(uint64(10), "eu"); err != nil {
		t.Fatal(err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("an owned setting key must not block the signed lane: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		consumer.mu.Lock()
		n := len(consumer.seen)
		consumer.mu.Unlock()
		if n == 1 || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	consumer.mu.Lock()
	seen := append([]siplugin.Admission(nil), consumer.seen...)
	consumer.mu.Unlock()
	if len(seen) != 1 {
		t.Fatalf("the ingress admitted %d statements, want 1", len(seen))
	}
	if seen[0].SettingsHash != sicore.EmptySettingsHash {
		t.Fatalf("an owned key must still hash to the empty-settings digest, got %s", seen[0].SettingsHash)
	}

	// A non-owned key is refused, naming itself.
	userCtx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
		"async_insert": 1,
	}))
	batch, err = conn.PrepareBatch(userCtx, "INSERT INTO "+siTenantDB+".si_events")
	if err == nil {
		err = batch.Append(uint64(11), "us")
		if err == nil {
			err = batch.Send()
		}
	}
	if err == nil {
		t.Fatal("a non-owned client setting must be refused on the signed lane")
	}
	if !strings.Contains(err.Error(), "async_insert") {
		t.Fatalf("the refusal must name the setting, got %v", err)
	}
	consumer.mu.Lock()
	n := len(consumer.seen)
	consumer.mu.Unlock()
	if n != 1 {
		t.Fatalf("the refused statement must not be admitted; consumer saw %d", n)
	}
}

func insertOneRow(t *testing.T, addr string, id uint64) {
	t.Helper()
	conn := openConnNoCompression(t, addr)
	batch, err := conn.PrepareBatch(context.Background(), "INSERT INTO "+siTenantDB+".si_events")
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.Append(id, "eu"); err != nil {
		t.Fatal(err)
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}
}

func admittedIDs(t *testing.T, c *capturingConsumer, want int) []sicore.StatementID {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		c.mu.Lock()
		n := len(c.seen)
		c.mu.Unlock()
		if n >= want || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.seen) != want {
		t.Fatalf("admissions = %d, want %d", len(c.seen), want)
	}
	var out []sicore.StatementID
	for _, adm := range c.seen {
		id, err := sicore.ParseStatementID(adm.StatementID)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, id)
	}
	return out
}

func TestStorageIntegrity_TwoAgentsOneKeySeparateStateDirsUseDistinctLanes(t *testing.T) {
	first, server, consumer := startSIAgentPairWith(t, "itest-net", siAgentPairOptions{ClientLanes: true})
	second := startSecondAgent(t, server.Addr, "itest-net", t.TempDir(), "auto")
	insertOneRow(t, first.Addr, 1)
	insertOneRow(t, second.Addr, 2)
	ids := admittedIDs(t, consumer, 2)
	if !ids[0].IsLaned() || !ids[1].IsLaned() || ids[0].Lane == ids[1].Lane || ids[0].Seq != 1 || ids[1].Seq != 1 {
		t.Fatalf("ids = %+v: one key, two agents must use two lanes, each starting at seq 1", ids)
	}
}

func TestStorageIntegrity_TwoAgentsSharingAStateDirUseDistinctLanes(t *testing.T) {
	dir := t.TempDir()
	first, server, consumer := startSIAgentPairWith(t, "itest-net", siAgentPairOptions{ClientLanes: true, AgentStateDir: dir})
	second := startSecondAgent(t, server.Addr, "itest-net", dir, "auto")
	insertOneRow(t, first.Addr, 1)
	insertOneRow(t, second.Addr, 2)
	ids := admittedIDs(t, consumer, 2)
	if ids[0].Lane == ids[1].Lane {
		t.Fatalf("two processes on one state dir shared lane %s", ids[0].Lane)
	}
}

func TestStorageIntegrity_LostStateDirStartsANewLane(t *testing.T) {
	dir := t.TempDir()
	first, server, consumer := startSIAgentPairWith(t, "itest-net", siAgentPairOptions{ClientLanes: true, AgentStateDir: dir})
	insertOneRow(t, first.Addr, 1)
	first.Close()
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	second := startSecondAgent(t, server.Addr, "itest-net", dir, "auto")
	insertOneRow(t, second.Addr, 2)
	ids := admittedIDs(t, consumer, 2)
	if ids[0].Lane == ids[1].Lane || ids[1].Seq != 1 {
		t.Fatalf("ids = %+v: a lost state dir must mint a new lane rather than collide", ids)
	}
}

func TestStorageIntegrity_LanesOffKeepsLegacyIDs(t *testing.T) {
	agent, _, consumer := startSIAgentPairWith(t, "itest-net", siAgentPairOptions{ClientLanes: true, Lanes: "off"})
	insertOneRow(t, agent.Addr, 1)
	if ids := admittedIDs(t, consumer, 1); ids[0].IsLaned() {
		t.Fatalf("lanes off produced %+v", ids[0])
	}
}

// Before activation the network reports client lanes disabled, so the agent
// emits a legacy id that the ingress admits (it would refuse a laned one).
func TestStorageIntegrity_LanedIDRefusedBeforeActivation(t *testing.T) {
	agent, _, consumer := startSIAgentPairWith(t, "itest-net", siAgentPairOptions{})
	insertOneRow(t, agent.Addr, 1)
	if ids := admittedIDs(t, consumer, 1); ids[0].IsLaned() {
		t.Fatal("an agent must not emit a laned id while the indexer reports client lanes disabled")
	}
}
