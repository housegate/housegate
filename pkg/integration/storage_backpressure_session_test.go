package integration

import (
	"context"
	"strings"
	"sync"
	"testing"

	housegate "github.com/housegate/housegate"
	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/config"
	"github.com/housegate/housegate/pkg/integration/testenv"
	siplugin "github.com/housegate/housegate/pkg/plugins/storageintegrity"
	"github.com/housegate/housegate/pkg/registry"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
	pb "github.com/housegate/rewriter-proto/gen/pb"
)

// throttlingConsumer refuses the first admission the way the real ingress does
// under back-pressure, then accepts later admissions.
type throttlingConsumer struct {
	mu   sync.Mutex
	seen int
}

func (c *throttlingConsumer) ConsumeStorageIntegrityAdmission(_ context.Context, _ siplugin.Admission) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seen++
	if c.seen > 1 {
		return nil
	}
	return &chproto.ClientError{
		Code:        chproto.CodeTooManyParts,
		Message:     "storage_integrity: back-pressure: hg_unsafe.db__si_events partition p_eu has 2400 active parts (soft limit 2400); retry later",
		Err:         sicore.ErrBackpressure,
		KeepSession: true,
	}
}

// Spec L D6 end-to-end acceptance: the official client receives Exception 252
// and continues to SELECT 42. ClickHouse 25.8 deliberately disconnects after
// any native INSERT Exception before its next --query, even when the server
// kept the stream framed; proxy's relay-level same-connection contract is
// therefore pinned separately by TestRelay_DeferredUpstreamBackpressure_*.
func TestStorageIntegrity_BackpressureKeepsTheClientSession(t *testing.T) {
	const networkID = "itest-net"
	bin := testenv.ClickHouseCLI(t)
	signer, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatal(err)
	}
	ch := openConn(t, chEnv.Addr)
	if err := ch.Exec(context.Background(),
		"CREATE TABLE IF NOT EXISTS "+chEnv.Database+".si_events (id UInt64, region String) ENGINE = MergeTree ORDER BY id"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	consumer := &throttlingConsumer{}
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

	out, err := testenv.RunCLIMultiqueryIgnoreError(t, bin, agentProxy.Addr, chEnv.Database,
		"INSERT INTO "+chEnv.Database+".si_events FORMAT CSVWithNames; SELECT 42", "id,region\n1,eu\n")
	if !strings.Contains(out, "252") && !strings.Contains(out, "TOO_MANY_PARTS") {
		t.Fatalf("client did not see exception 252 (exec err %v):\n%s", err, out)
	}
	if !strings.Contains(out, "back-pressure") {
		t.Fatalf("client did not see the back-pressure message:\n%s", out)
	}
	if !strings.Contains(out, "42") {
		t.Fatalf("the connection did not survive the throttle; follow-up query never ran (exec err %v):\n%s", err, out)
	}
	consumer.mu.Lock()
	seen := consumer.seen
	consumer.mu.Unlock()
	if seen != 1 {
		t.Fatalf("consumer saw %d admissions, want exactly the refused one", seen)
	}
}
