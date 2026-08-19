package integration

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

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

func siAgentSchema() payloadexec.TableSchema {
	return payloadexec.TableSchema{
		TableID: chEnv.Database + ".si_events",
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
		ns.TableSchemas[chEnv.Database+"/si_events@1"] = network.TableSchemaInfo{
			DatabaseId: chEnv.Database,
			TableId:    "si_events",
			Version:    1,
			SchemaHash: payloadexec.TableSchemaHash(networkID, schema),
			SchemaJson: string(js),
		}
	}
}

// TestStorageIntegrity_AgentSignsEnvelopeV2EndToEnd runs client -> agent
// housegate (storage_integrity.agent) -> server housegate (ingress) -> CH and
// proves the ingress stored exactly the bytes the agent signed: the v2 token
// validates against a payload_hash recomputed from the stored bytes, and those
// bytes decode (Native, at the pinned revision) into the rows the client sent.
func TestStorageIntegrity_AgentSignsEnvelopeV2EndToEnd(t *testing.T) {
	const networkID = "itest-net"
	signer, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatal(err)
	}
	ch := openConn(t, chEnv.Addr)
	if err := ch.Exec(context.Background(), "CREATE TABLE IF NOT EXISTS "+chEnv.Database+".si_events (id UInt64, region String) ENGINE = MergeTree ORDER BY id"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	consumer := &capturingConsumer{}
	rewriterOpt, rewriterMock := testenv.WithRewriterMock(t)
	rewriterMock.SetAccessedTables("INSERT INTO "+chEnv.Database+".si_events", []*pb.AccessedTable{{
		OriginalDatabase:   chEnv.Database,
		OriginalTable:      "si_events",
		LogicalDatabase:    chEnv.Database,
		PhysicalDatabase:   chEnv.Database,
		IsStorageIntegrity: true,
	}})
	server := testenv.StartServerProxy(t, chEnv.Addr,
		rewriterOpt,
		authProxyConfig([]string{signer.Address()}, false),
		testenv.WithDatabasePermission(signer.Address(), chEnv.Database, registry.DbAuthWrite),
		withDeclaredSchema(t, networkID),
		testenv.WithConfigMutator(func(cfg *config.Config) {
			cfg.StorageIntegrity.Ingress.Enabled = true
			cfg.StorageIntegrity.Ingress.AllowedAddresses = []string{signer.Address()}
			cfg.StorageIntegrity.Ingress.NetworkID = networkID
		}),
		func(_ *config.Config, opts *housegate.Options) {
			opts.StorageIntegrityAdmissionConsumer = consumer
		},
	)
	agentProxy := testenv.StartAgentProxy(t, authTestKey1, server.Addr,
		withDeclaredSchema(t, networkID),
		testenv.WithConfigMutator(func(cfg *config.Config) {
			cfg.StorageIntegrity.Agent.Enabled = true
			cfg.StorageIntegrity.Agent.NetworkID = networkID
			cfg.StorageIntegrity.Agent.StateDir = t.TempDir()
			cfg.StorageIntegrity.Agent.RequireNetworkState = false
		}),
	)

	conn := openConnNoCompression(t, agentProxy.Addr)
	batch, err := conn.PrepareBatch(context.Background(), "INSERT INTO "+chEnv.Database+".si_events")
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
		TargetTableID:  chEnv.Database + ".si_events",
		RowIDProfileID: payloadexec.RowIDProfileID,
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
	if !strings.HasPrefix(adm.SQL, "INSERT INTO "+chEnv.Database+".si_events") {
		t.Fatalf("signed SQL = %q", adm.SQL)
	}
}
