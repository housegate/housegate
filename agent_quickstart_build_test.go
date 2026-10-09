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

// With lanes off (the driver sidecar) an explicit state_dir opens its counter
// when the agent is built, so a misconfigured sidecar fails startup instead
// of reporting ready and refusing every SI INSERT: a second process holding
// the counter and a state directory that cannot be created both stop the
// build, and a successful build already holds the counter before any INSERT
// (final review I2). With lanes auto only the uncreatable directory stops the
// build: the legacy counter opens at its first use (preflight F1).
func TestBuildAgent_ExplicitStateDirOpensTheCounterAtBuild(t *testing.T) {
	newState := func(t *testing.T) *network.InMemoryNetworkState {
		ns := network.NewInMemoryNetworkState()
		declareOrders(t, ns, "testnet-v2")
		return ns
	}
	newCfg := func(t *testing.T) *config.Config {
		cfg := agentSIConfig(t)
		cfg.StorageIntegrity.Agent.NetworkID = "testnet-v2"
		cfg.StorageIntegrity.Agent.Lanes = "off"
		return cfg
	}

	t.Run("held by another process", func(t *testing.T) {
		cfg := newCfg(t)
		held, err := sistatement.OpenSeqCounter(cfg.StorageIntegrity.Agent.StateDir, agentSignerAddress(t, cfg))
		if err != nil {
			t.Fatal(err)
		}
		defer held.Close()
		bs, err := buildAgent(Options{Config: cfg, NetworkState: newState(t)}, nil)
		if err == nil {
			bs.teardown()
			t.Fatal("buildAgent must fail while another process holds the counter")
		}
		if !errors.Is(err, sistatement.ErrSeqLocked) || !strings.Contains(err.Error(), "storage_integrity.agent") {
			t.Fatalf("err = %v, want a storage_integrity.agent ErrSeqLocked refusal", err)
		}
	})

	t.Run("lanes auto does not lock the legacy counter at build", func(t *testing.T) {
		cfg := newCfg(t)
		cfg.StorageIntegrity.Agent.Lanes = "auto"
		held, err := sistatement.OpenSeqCounter(cfg.StorageIntegrity.Agent.StateDir, agentSignerAddress(t, cfg))
		if err != nil {
			t.Fatal(err)
		}
		defer held.Close()
		bs, err := buildAgent(Options{Config: cfg, NetworkState: newState(t)}, nil)
		if err != nil {
			t.Fatalf("a second agent sharing the state dir must start with lanes auto: %v", err)
		}
		bs.teardown()
	})

	for _, lanes := range []string{"off", "auto"} {
		t.Run("uncreatable directory with lanes "+lanes, func(t *testing.T) {
			cfg := newCfg(t)
			cfg.StorageIntegrity.Agent.Lanes = lanes
			blocker := filepath.Join(t.TempDir(), "file")
			if err := os.WriteFile(blocker, nil, 0o600); err != nil {
				t.Fatal(err)
			}
			cfg.StorageIntegrity.Agent.StateDir = filepath.Join(blocker, "state")
			bs, err := buildAgent(Options{Config: cfg, NetworkState: newState(t)}, nil)
			if err == nil {
				bs.teardown()
				t.Fatal("buildAgent must fail when the state directory cannot be created")
			}
			if !strings.Contains(err.Error(), "storage_integrity.agent") {
				t.Fatalf("err = %v, want a storage_integrity.agent refusal", err)
			}
		})
	}

	t.Run("held from build until teardown", func(t *testing.T) {
		cfg := newCfg(t)
		bs, err := buildAgent(Options{Config: cfg, NetworkState: newState(t)}, nil)
		if err != nil {
			t.Fatalf("buildAgent: %v", err)
		}
		// No INSERT ran: the build itself holds the counter's lock.
		dir, signer := cfg.StorageIntegrity.Agent.StateDir, agentSignerAddress(t, cfg)
		if _, err := sistatement.OpenSeqCounter(dir, signer); !errors.Is(err, sistatement.ErrSeqLocked) {
			bs.teardown()
			t.Fatalf("open after build: err = %v, want ErrSeqLocked", err)
		}
		bs.teardown()
		seq, err := sistatement.OpenSeqCounter(dir, signer)
		if err != nil {
			t.Fatalf("teardown did not release the counter: %v", err)
		}
		_ = seq.Close()
	})
}

// wiringOptions is agentStatementOptions for wiring assertions: it closes the
// eagerly opened counter at once, so a test may resolve the options again
// with the same state_dir.
func wiringOptions(t *testing.T, cfg *config.Config, reg registry.Registry, signer string) (sistatement.Options, string, error) {
	t.Helper()
	opts, label, err := agentStatementOptions(cfg, Options{Config: cfg}, reg, signer)
	if err == nil && opts.Seq != nil {
		if cerr := opts.Seq.Close(); cerr != nil {
			t.Fatal(cerr)
		}
	}
	return opts, label, err
}

func TestAgentStatementOptions_Wiring(t *testing.T) {
	const signer = "0x00000000000000000000000000000000000000aa"

	t.Run("discovery, cached statuses, hosting, pre-check", func(t *testing.T) {
		cfg := agentSIConfig(t)
		cfg.Agent.Upstream = ""
		ns := newDiscoveringState("itest-net")
		declareOrders(t, ns.InMemoryNetworkState, "itest-net")
		opts, _, err := wiringOptions(t, cfg, ns, signer)
		if err != nil {
			t.Fatal(err)
		}
		// agentSIConfig sets an explicit state_dir and leaves lanes auto: the
		// legacy counter opens lazily (preflight F1) and the lanes live in
		// <state_dir>/<signer>.
		if opts.Discovery == nil || opts.Hosting == nil || opts.OpenSeq == nil || opts.Seq != nil || opts.Dial == nil || opts.LaneDir == nil {
			t.Fatalf("discovery=%v hosting=%v openSeq=%v seq=%v dial=%v laneDir=%v", opts.Discovery != nil, opts.Hosting != nil, opts.OpenSeq != nil, opts.Seq != nil, opts.Dial != nil, opts.LaneDir != nil)
		}
		if dir, err := opts.LaneDir("itest-net"); err != nil || dir != filepath.Join(cfg.StorageIntegrity.Agent.StateDir, signer) {
			t.Fatalf("lane dir = %q, %v", dir, err)
		}
		if opts.Lanes != sistatement.LaneModeAuto || opts.MaxInflightPerLane != 16 || opts.ClientLanesEnabled != nil {
			t.Fatalf("lanes=%q max_inflight=%d client_lanes_port=%v; want auto, the default 16 and no host port", opts.Lanes, opts.MaxInflightPerLane, opts.ClientLanesEnabled != nil)
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

	t.Run("lanes, in-flight cap and the host client-lanes port", func(t *testing.T) {
		cfg := agentSIConfig(t)
		cfg.StorageIntegrity.Agent.NetworkID = "testnet-v2"
		cfg.StorageIntegrity.Agent.Lanes = "off"
		cfg.StorageIntegrity.Agent.MaxInflightPerLane = 3
		opts, _, err := agentStatementOptions(cfg, Options{Config: cfg, StorageIntegrityClientLanes: func() bool { return true }}, network.NewInMemoryNetworkState(), signer)
		if err != nil {
			t.Fatal(err)
		}
		if opts.Seq != nil {
			defer opts.Seq.Close()
		}
		if opts.Lanes != sistatement.LaneModeOff || opts.MaxInflightPerLane != 3 || opts.ClientLanesEnabled == nil || !opts.ClientLanesEnabled() {
			t.Fatalf("lanes=%q max_inflight=%d client_lanes_port=%v", opts.Lanes, opts.MaxInflightPerLane, opts.ClientLanesEnabled != nil)
		}
		// Lanes off is the driver sidecar: the legacy counter is opened at build.
		if opts.Seq == nil || opts.OpenSeq != nil {
			t.Fatalf("lanes off: seq=%v openSeq=%v; want the eager counter", opts.Seq != nil, opts.OpenSeq != nil)
		}
	})

	t.Run("statuses without discovery are not cached", func(t *testing.T) {
		cfg := agentSIConfig(t)
		cfg.StorageIntegrity.Agent.NetworkID = "testnet-v2"
		ns := &statusOnlyState{InMemoryNetworkState: network.NewInMemoryNetworkState()}
		opts, _, err := wiringOptions(t, cfg, ns, signer)
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
		opts, _, err := wiringOptions(t, cfg, newDiscoveringState("itest-net"), signer)
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
		opts, _, err := wiringOptions(t, cfg, registryWithoutHosting{network.NewInMemoryNetworkState()}, signer)
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
		if _, _, err := wiringOptions(t, cfg, registryWithoutHosting{network.NewInMemoryNetworkState()}, signer); err != nil {
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
		opts, _, err := wiringOptions(t, cfg, newDiscoveringState("itest-net"), signer)
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

	t.Run("explicit state dir with lanes off opens one counter for every network at build", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "state")
		// A file in the way fails the open at build.
		if err := os.WriteFile(dir, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, _, _, _, err := agentSeqOpener(dir, signer, sistatement.LaneModeOff, func() (string, bool) { return "", false }); err == nil || !strings.Contains(err.Error(), dir) {
			t.Fatalf("open through a file: err = %v, want an error naming %s", err, dir)
		}
		if err := os.Remove(dir); err != nil {
			t.Fatal(err)
		}
		seq, open, laneDir, label, err := agentSeqOpener(dir, signer, sistatement.LaneModeOff, func() (string, bool) { return "", false })
		if err != nil || label != dir || seq == nil || open != nil || laneDir == nil {
			t.Fatalf("seq=%v open=%v label=%q err=%v; want an opened counter and no lazy opener", seq != nil, open != nil, label, err)
		}
		defer seq.Close()
		if seq.Path() != filepath.Join(dir, lower+".seq") {
			t.Fatalf("path = %s", seq.Path())
		}
		if _, _, _, _, err := agentSeqOpener(dir, signer, sistatement.LaneModeOff, func() (string, bool) { return "", false }); !errors.Is(err, sistatement.ErrSeqLocked) {
			t.Fatalf("second opener: err = %v, want ErrSeqLocked", err)
		}
	})

	// Preflight F1: with lanes auto an explicit state dir is shared by several
	// agents (spec 2026-10-09 §9.2), so nothing is locked at build; the legacy
	// counter opens at its first use, once for every network.
	t.Run("explicit state dir with lanes auto opens the legacy counter lazily", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "state")
		seq, open, laneDir, label, err := agentSeqOpener(dir, signer, sistatement.LaneModeAuto, func() (string, bool) { return "", false })
		if err != nil || label != dir || seq != nil || open == nil {
			t.Fatalf("seq=%v open=%v label=%q err=%v; want no eager counter and a lazy opener", seq != nil, open != nil, label, err)
		}
		_, open2, _, _, err := agentSeqOpener(dir, signer, sistatement.LaneModeAuto, func() (string, bool) { return "", false })
		if err != nil {
			t.Fatalf("a second agent on the same state dir must start: %v", err)
		}
		for _, network := range []string{"net-a", "net-b"} {
			if got, err := laneDir(network); err != nil || got != filepath.Join(dir, lower) {
				t.Fatalf("lane dir for %s = %q, %v; want %s", network, got, err, filepath.Join(dir, lower))
			}
		}
		a, err := open("net-a")
		if err != nil {
			t.Fatal(err)
		}
		defer a.Close()
		b, err := open("net-b")
		if err != nil || a != b || a.Path() != filepath.Join(dir, lower+".seq") {
			t.Fatalf("second network: same=%v err=%v path=%s; want the one shared counter", a == b, err, a.Path())
		}
		if _, err := open2("net-a"); !errors.Is(err, sistatement.ErrSeqLocked) {
			t.Fatalf("the other agent's legacy open = %v, want ErrSeqLocked", err)
		}
	})

	t.Run("default base keeps one counter per network", func(t *testing.T) {
		base := t.TempDir()
		seq, open, laneDir, label, err := agentSeqOpener("", signer, sistatement.LaneModeAuto, func() (string, bool) { return base, true })
		if err != nil || seq != nil || label != filepath.Join(base, "si") {
			t.Fatalf("seq=%v label=%q err=%v", seq != nil, label, err)
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
		if got, err := laneDir("net-a"); err != nil || got != config.AgentSIStateDir(base, "net-a", signer) {
			t.Fatalf("lane dir = %q, %v", got, err)
		}
		// A discovered network id is a single path element.
		for _, bad := range []string{"", ".", "..", "../escape", "a/b", `a\b`} {
			if _, err := open(bad); err == nil {
				t.Errorf("network id %q must be refused as a state directory name", bad)
			}
			if _, err := laneDir(bad); err == nil {
				t.Errorf("network id %q must be refused as a lane directory name", bad)
			}
		}
		if _, err := os.Stat(filepath.Join(base, "escape")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("a refused id created a directory: %v", err)
		}
	})

	t.Run("no default base needs state_dir", func(t *testing.T) {
		_, _, _, _, err := agentSeqOpener("", signer, sistatement.LaneModeAuto, func() (string, bool) { return "", false })
		if err == nil || !strings.Contains(err.Error(), "state_dir is required") {
			t.Fatalf("err = %v", err)
		}
	})
}
