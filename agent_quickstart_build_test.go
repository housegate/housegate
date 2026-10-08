package housegate

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/config"
	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/plugin"
	"github.com/housegate/housegate/pkg/plugins/sistatement"
	"github.com/housegate/housegate/pkg/registry"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

// discoveringState is an in-memory registry that also answers discovery and
// per-table status, standing in for RpcNetworkState. Its statuses follow the
// declared YAML schemas; statusCalls counts the lookups that reach it.
type discoveringState struct {
	*network.InMemoryNetworkState
	networkID   string
	statusCalls atomic.Int32
}

func newDiscoveringState(networkID string) *discoveringState {
	return &discoveringState{InMemoryNetworkState: network.NewInMemoryNetworkState(), networkID: networkID}
}

func (d *discoveringState) StorageIntegrityInfo(context.Context, string) (registry.StorageIntegrityInfo, error) {
	return registry.StorageIntegrityInfo{Enabled: true, NetworkID: d.networkID, ServerUnixTime: time.Now().Unix()}, nil
}

func (*discoveringState) StorageIntegrityWriterCheck(context.Context, string, string) (bool, error) {
	return true, nil
}

func (d *discoveringState) StorageIntegrityTableStatus(ctx context.Context, database, table string) (registry.TableStatus, error) {
	d.statusCalls.Add(1)
	return registry.TableStatusesFromSchemas(d.InMemoryNetworkState).StorageIntegrityTableStatus(ctx, database, table)
}

// statusOnlyState answers per-table status but not discovery.
type statusOnlyState struct {
	*network.InMemoryNetworkState
	statusCalls atomic.Int32
}

func (s *statusOnlyState) StorageIntegrityTableStatus(ctx context.Context, database, table string) (registry.TableStatus, error) {
	s.statusCalls.Add(1)
	return registry.TableStatusesFromSchemas(s.InMemoryNetworkState).StorageIntegrityTableStatus(ctx, database, table)
}

// registryWithoutHosting hides the embedded state's DatabaseHosting method,
// like a host registry that predates the upstream switch.
type registryWithoutHosting struct{ *network.InMemoryNetworkState }

func (registryWithoutHosting) DatabaseHosting() {}

func agentSIConfig(t *testing.T) *config.Config {
	t.Helper()
	cfg := config.Default()
	cfg.Listen = "127.0.0.1:0"
	cfg.MetricsListen = ""
	cfg.Agent.Mode = true
	cfg.Agent.PrivateKeyHex = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	cfg.Agent.Upstream = "127.0.0.1:1"
	cfg.StorageIntegrity.Agent.Enabled = true
	cfg.StorageIntegrity.Agent.RequireNetworkState = false
	cfg.StorageIntegrity.Agent.StateDir = t.TempDir()
	return &cfg
}

func agentSignerAddress(t *testing.T, cfg *config.Config) string {
	t.Helper()
	s, err := auth.NewRelaySigner(cfg.Agent.PrivateKeyHex)
	if err != nil {
		t.Fatal(err)
	}
	return s.Address()
}

func quickstartSchema() payloadexec.TableSchema {
	return payloadexec.TableSchema{
		TableID:     "shop.orders",
		PartitionBy: "region",
		Columns: []lthash.Column{
			{Name: "id", Type: "UInt64"},
			{Name: "region", Type: "String"},
		},
	}
}

// declareOrders declares shop.orders, hashed for networkID, so a
// declared-schema status source reports it Active.
func declareOrders(t *testing.T, ns *network.InMemoryNetworkState, networkID string) {
	t.Helper()
	schema := quickstartSchema()
	js, err := json.Marshal(schema)
	if err != nil {
		t.Fatal(err)
	}
	ns.TableSchemas["shop/orders@1"] = network.TableSchemaInfo{
		DatabaseId: "shop", TableId: "orders", Version: 1,
		SchemaHash: payloadexec.TableSchemaHash(networkID, schema), SchemaJson: string(js),
	}
}

// runSIInsert drives one payload-carrying INSERT into shop.orders through
// the built agent's sistatement plugin up to signing, and returns the
// statement id it minted. Signing reserves the client_seq, so it opens the
// lazily opened counter.
func runSIInsert(t *testing.T, bs *builtServer) string {
	t.Helper()
	const rev = 54460
	chain := requireProxyServer(t, bs.listeners[0]).Hooks.(*plugin.PluginChain)
	var si *sistatement.Plugin
	for _, p := range chain.QueryPlugins {
		if p, ok := p.(*sistatement.Plugin); ok {
			si = p
		}
	}
	if si == nil {
		t.Fatal("sistatement missing from the agent chain")
	}
	client, peer := net.Pipe()
	t.Cleanup(func() { _ = peer.Close() })
	sess := chsession.New(1, client)
	t.Cleanup(func() { _ = sess.Close() })
	upClient, upPeer := net.Pipe()
	t.Cleanup(func() { _ = upPeer.Close() })
	up := chproto.NewCodec(&configuredAddressConn{Conn: upClient, upstreamAddress: "127.0.0.1:1"}, chproto.DirToUpstream)
	up.SetRevision(rev)
	if err := sess.BindUpstream(context.Background(), up); err != nil {
		t.Fatal(err)
	}
	sess.State().ClientRevision = rev
	sess.State().SetUpstreamHello(&chproto.ClientHello{ProtocolVersion: rev, User: "writer"})

	sql := "INSERT INTO shop.orders FORMAT Native"
	qctx := &plugin.QueryContext{
		Session:     sess,
		OriginalSQL: sql,
		Query:       &chproto.Query{ID: "client-1", Body: sql, Compression: proto.CompressionDisabled},
		Values:      map[string]any{},
	}
	if err := si.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if qctx.DeferredInsert == nil {
		t.Fatal("the INSERT was not claimed for signing")
	}
	id := proto.ColUInt64{1}
	region := proto.ColStr{}
	region.Append("eu")
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ClientCodeData))
	buf.PutString("")
	if err := (proto.Block{Rows: 1, Columns: 2}).EncodeBlock(&buf, rev, proto.Input{{Name: "id", Data: &id}, {Name: "region", Data: &region}}); err != nil {
		t.Fatal(err)
	}
	if err := si.OnClientDataStrict(context.Background(), qctx, buf.Buf); err != nil {
		t.Fatalf("OnClientDataStrict: %v", err)
	}
	if err := si.OnQueryInputCompleteStrict(context.Background(), qctx); err != nil {
		t.Fatalf("OnQueryInputCompleteStrict: %v", err)
	}
	return qctx.Query.ID
}

func requireSeq(t *testing.T, statementID string, want uint64) {
	t.Helper()
	if _, seq, _, err := sicore.ParseFlatStatementID(statementID); err != nil || seq != want {
		t.Fatalf("statement id %q: seq=%d err=%v, want seq %d", statementID, seq, err, want)
	}
}

// requireCounterHeldThenReleased checks that dir's counter is locked while
// the agent runs and free, with last == wantLast, after teardown.
func requireCounterHeldThenReleased(t *testing.T, bs *builtServer, dir, signer string, wantLast uint64) {
	t.Helper()
	if _, err := os.Stat(filepath.Join(dir, strings.ToLower(signer)+".seq")); err != nil {
		t.Fatalf("seq file: %v", err)
	}
	if _, err := sistatement.OpenSeqCounter(dir, signer); !errors.Is(err, sistatement.ErrSeqLocked) {
		t.Fatalf("open while the agent runs: err = %v, want ErrSeqLocked", err)
	}
	bs.teardown()
	seq, err := sistatement.OpenSeqCounter(dir, signer)
	if err != nil {
		t.Fatalf("teardown did not release the counter: %v", err)
	}
	defer seq.Close()
	if seq.Last() != wantLast {
		t.Fatalf("last = %d, want %d", seq.Last(), wantLast)
	}
}

// Spec 2026-10-09 §6.4 (D18): with a registry that answers discovery the
// agent needs no configured network id; without one the id stays required.
func TestBuildAgent_DiscoveryMakesNetworkIDOptional(t *testing.T) {
	bs, err := buildAgent(Options{Config: agentSIConfig(t), NetworkState: newDiscoveringState("itest-net")}, nil)
	if err != nil {
		t.Fatalf("buildAgent with a discovering registry and no network_id: %v", err)
	}
	bs.teardown()

	_, err = buildAgent(Options{Config: agentSIConfig(t), NetworkState: network.NewInMemoryNetworkState()}, nil)
	if err == nil || !strings.Contains(err.Error(), "network id is required") {
		t.Fatalf("err = %v, want the network-id refusal without discovery", err)
	}
}

// Without state_dir the counter lives in <base>/si/<network_id>/<signer>/,
// opened at the first SI write with the discovered network id, and teardown
// releases it (plan decision P4).
func TestBuildAgent_DefaultSeqStoreIsPerDiscoveredNetwork(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_STATE_HOME", filepath.Join(home, "xdg"))
	base, ok := config.DefaultAgentStateBase(runtime.GOOS, os.Getenv, home)
	if !ok {
		t.Skipf("no default state directory on %s", runtime.GOOS)
	}
	cfg := agentSIConfig(t)
	cfg.StorageIntegrity.Agent.StateDir = ""
	ns := newDiscoveringState("itest-net")
	declareOrders(t, ns.InMemoryNetworkState, "itest-net")
	bs, err := buildAgent(Options{Config: cfg, NetworkState: ns}, nil)
	if err != nil {
		t.Fatalf("buildAgent: %v", err)
	}
	if _, err := os.Stat(filepath.Join(base, "si")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("build must not create the state directory before the first SI write: %v", err)
	}
	requireSeq(t, runSIInsert(t, bs), 1)
	signer := agentSignerAddress(t, cfg)
	requireCounterHeldThenReleased(t, bs, config.AgentSIStateDir(base, "itest-net", signer), signer, 1)
}

// An explicit state_dir keeps <state_dir>/<signer>.seq (plan decision P4);
// the counter opens at the first SI write, and a failed build and teardown
// both release it so the same process can build the agent again.
func TestBuildAgentReleasesSeqCounterOnBuildFailureAndTeardown(t *testing.T) {
	newState := func(t *testing.T) *network.InMemoryNetworkState {
		ns := network.NewInMemoryNetworkState()
		declareOrders(t, ns, "testnet-v2")
		return ns
	}
	newCfg := func(t *testing.T) *config.Config {
		cfg := agentSIConfig(t)
		cfg.StorageIntegrity.Agent.NetworkID = "testnet-v2"
		return cfg
	}

	t.Run("build failure", func(t *testing.T) {
		cfg := newCfg(t)
		cfg.StorageIntegrity.Agent.KeeperShardID = 1 // sistatement.New refuses it
		_, err := buildAgent(Options{Config: cfg, NetworkState: newState(t)}, nil)
		if err == nil || !strings.Contains(err.Error(), "keeper_shard_id") {
			t.Fatalf("expected a sistatement.New failure, got %v", err)
		}
		seq, err := sistatement.OpenSeqCounter(cfg.StorageIntegrity.Agent.StateDir, agentSignerAddress(t, cfg))
		if err != nil {
			t.Fatalf("seq counter still locked: %v", err)
		}
		_ = seq.Close()
	})

	t.Run("successful teardown", func(t *testing.T) {
		cfg := newCfg(t)
		bs, err := buildAgent(Options{Config: cfg, NetworkState: newState(t)}, nil)
		if err != nil {
			t.Fatalf("buildAgent: %v", err)
		}
		requireSeq(t, runSIInsert(t, bs), 1)
		requireCounterHeldThenReleased(t, bs, cfg.StorageIntegrity.Agent.StateDir, agentSignerAddress(t, cfg), 1)
		// The same process can build the agent again and continues the
		// counter.
		again, err := buildAgent(Options{Config: cfg, NetworkState: newState(t)}, nil)
		if err != nil {
			t.Fatalf("rebuild after teardown: %v", err)
		}
		requireSeq(t, runSIInsert(t, again), 2)
		again.teardown()
	})
}

func TestAgentStatementOptions_Wiring(t *testing.T) {
	const signer = "0x00000000000000000000000000000000000000aa"

	t.Run("discovery, cached statuses, hosting, pre-check", func(t *testing.T) {
		cfg := agentSIConfig(t)
		cfg.Agent.Upstream = ""
		ns := newDiscoveringState("itest-net")
		declareOrders(t, ns.InMemoryNetworkState, "itest-net")
		opts, _, err := agentStatementOptions(cfg, Options{Config: cfg}, ns, signer)
		if err != nil {
			t.Fatal(err)
		}
		if opts.Discovery == nil || opts.Hosting == nil || opts.OpenSeq == nil || opts.Seq != nil || opts.Dial == nil {
			t.Fatalf("discovery=%v hosting=%v openSeq=%v seq=%v dial=%v", opts.Discovery != nil, opts.Hosting != nil, opts.OpenSeq != nil, opts.Seq != nil, opts.Dial != nil)
		}
		if !opts.WriterPrecheck || opts.PinnedUpstream || opts.SwitchTimeout != 10*time.Second {
			t.Fatalf("precheck=%v pinned=%v switch_timeout=%s", opts.WriterPrecheck, opts.PinnedUpstream, opts.SwitchTimeout)
		}
		// Spec 2026-10-09 §6.4: a burst of INSERTs costs one status lookup.
		for range 3 {
			if st, err := opts.Statuses.StorageIntegrityTableStatus(context.Background(), "shop", "orders"); err != nil || st.Status != registry.TableStatusActive {
				t.Fatalf("status = %+v, %v", st, err)
			}
		}
		if got := ns.statusCalls.Load(); got != 1 {
			t.Fatalf("status lookups = %d, want 1 behind the cache", got)
		}
	})

	t.Run("statuses without discovery are not cached", func(t *testing.T) {
		cfg := agentSIConfig(t)
		cfg.StorageIntegrity.Agent.NetworkID = "testnet-v2"
		ns := &statusOnlyState{InMemoryNetworkState: network.NewInMemoryNetworkState()}
		opts, _, err := agentStatementOptions(cfg, Options{Config: cfg}, ns, signer)
		if err != nil {
			t.Fatal(err)
		}
		if opts.Discovery != nil {
			t.Fatal("a registry without discovery must not be wired as one")
		}
		for range 2 {
			_, _ = opts.Statuses.StorageIntegrityTableStatus(context.Background(), "shop", "orders")
		}
		if got := ns.statusCalls.Load(); got != 2 {
			t.Fatalf("status lookups = %d, want 2 (no cache without discovery)", got)
		}
	})

	t.Run("driver and pinned upstream", func(t *testing.T) {
		cfg := agentSIConfig(t)
		cfg.Agent.Driver = true
		opts, _, err := agentStatementOptions(cfg, Options{Config: cfg}, newDiscoveringState("itest-net"), signer)
		if err != nil {
			t.Fatal(err)
		}
		if opts.WriterPrecheck || !opts.IsDriver || !opts.PinnedUpstream {
			t.Fatalf("driver: precheck=%v is_driver=%v pinned=%v", opts.WriterPrecheck, opts.IsDriver, opts.PinnedUpstream)
		}
	})

	t.Run("a registry without hosting disables the switch with a warning", func(t *testing.T) {
		cfg := agentSIConfig(t)
		cfg.Agent.Upstream = ""
		cfg.StorageIntegrity.Agent.NetworkID = "testnet-v2"
		logs := captureAgentBuildLogs(t)
		opts, _, err := agentStatementOptions(cfg, Options{Config: cfg}, registryWithoutHosting{network.NewInMemoryNetworkState()}, signer)
		if err != nil {
			t.Fatal(err)
		}
		if opts.Hosting != nil {
			t.Fatal("hosting wired from a registry that does not implement it")
		}
		if !strings.Contains(logs.String(), "upstream switch") || !strings.Contains(logs.String(), "level=WARN") {
			t.Fatalf("missing startup warning: %s", logs.String())
		}

		logs.Reset()
		cfg.Agent.Upstream = "127.0.0.1:1"
		if _, _, err := agentStatementOptions(cfg, Options{Config: cfg}, registryWithoutHosting{network.NewInMemoryNetworkState()}, signer); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(logs.String(), "upstream switch") {
			t.Fatalf("a pinned upstream disables the switch on purpose; no warning expected: %s", logs.String())
		}
	})

	t.Run("dial reports the dialed address", func(t *testing.T) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer ln.Close()
		go func() {
			if c, err := ln.Accept(); err == nil {
				defer c.Close()
				_, _ = c.Read(make([]byte, 1))
			}
		}()
		cfg := agentSIConfig(t)
		opts, _, err := agentStatementOptions(cfg, Options{Config: cfg}, newDiscoveringState("itest-net"), signer)
		if err != nil {
			t.Fatal(err)
		}
		codec, err := opts.Dial(context.Background(), ln.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		defer codec.Conn().(net.Conn).Close()
		named, ok := codec.Conn().(interface{ UpstreamAddress() string })
		if !ok || named.UpstreamAddress() != ln.Addr().String() {
			t.Fatalf("switch dial conn %T does not report %s; the switch would re-run on every INSERT", codec.Conn(), ln.Addr())
		}
	})
}

func TestAgentSwitchTimeout(t *testing.T) {
	for _, tc := range []struct{ dial, want time.Duration }{
		{0, 10 * time.Second},
		{5 * time.Second, 10 * time.Second},
		{30 * time.Second, 35 * time.Second},
	} {
		if got := agentSwitchTimeout(tc.dial); got != tc.want {
			t.Errorf("agentSwitchTimeout(%s) = %s, want %s", tc.dial, got, tc.want)
		}
	}
}

func TestAgentSeqOpener(t *testing.T) {
	const signer = "0x00000000000000000000000000000000000000AA"
	lower := strings.ToLower(signer)

	t.Run("explicit state dir serves every network with one counter", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "state")
		// A file in the way fails the first open; the failure is not
		// remembered, so a later INSERT retries.
		if err := os.WriteFile(dir, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		open, label, err := agentSeqOpener(dir, signer, func() (string, bool) { return "", false })
		if err != nil || label != dir {
			t.Fatalf("label=%q err=%v", label, err)
		}
		if _, err := open("net-a"); err == nil {
			t.Fatal("open through a file must fail")
		}
		if err := os.Remove(dir); err != nil {
			t.Fatal(err)
		}
		a, err := open("net-a")
		if err != nil {
			t.Fatalf("retry after the failure: %v", err)
		}
		defer a.Close()
		b, err := open("net-b")
		if err != nil || a != b {
			t.Fatalf("second network: %p %p %v; want the same counter", a, b, err)
		}
		if a.Path() != filepath.Join(dir, lower+".seq") {
			t.Fatalf("path = %s", a.Path())
		}
	})

	t.Run("default base keeps one counter per network", func(t *testing.T) {
		base := t.TempDir()
		open, label, err := agentSeqOpener("", signer, func() (string, bool) { return base, true })
		if err != nil || label != filepath.Join(base, "si") {
			t.Fatalf("label=%q err=%v", label, err)
		}
		a, err := open("net-a")
		if err != nil {
			t.Fatal(err)
		}
		defer a.Close()
		if a.Path() != filepath.Join(config.AgentSIStateDir(base, "net-a", signer), lower+".seq") {
			t.Fatalf("path = %s", a.Path())
		}
		b, err := open("net-b")
		if err != nil {
			t.Fatal(err)
		}
		defer b.Close()
		if a == b || b.Path() != filepath.Join(base, "si", "net-b", lower, lower+".seq") {
			t.Fatalf("second network path = %s", b.Path())
		}
		// A discovered network id is a single path element.
		for _, bad := range []string{"", ".", "..", "../escape", "a/b", `a\b`} {
			if _, err := open(bad); err == nil {
				t.Errorf("network id %q must be refused as a state directory name", bad)
			}
		}
		if _, err := os.Stat(filepath.Join(base, "escape")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("a refused id created a directory: %v", err)
		}
	})

	t.Run("no default base needs state_dir", func(t *testing.T) {
		_, _, err := agentSeqOpener("", signer, func() (string, bool) { return "", false })
		if err == nil || !strings.Contains(err.Error(), "state_dir is required") {
			t.Fatalf("err = %v", err)
		}
	})
}
