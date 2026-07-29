package testenv

import (
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"testing"

	housegate "github.com/housegate/housegate"
	"github.com/housegate/housegate/pkg/config"
	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/plugins/agent"
)

// StartAgentProxy starts an agent-mode housegate proxy pinned to a
// specific upstream address. The signer key is taken from privateKeyHex
// (must be a 32-byte hex secp256k1 key with or without 0x prefix); the
// proxy signs every outgoing query as that key.
//
// Use this when the test wants the legacy pinned-upstream agent path —
// no NetworkState lookup, no Selector. For the auto-discovery path, see
// StartAgentProxyWithSelector.
func StartAgentProxy(t *testing.T, privateKeyHex, upstream string, proxyOpts ...ProxyOption) *TestProxy {
	t.Helper()
	cfg := buildAgentConfig(t, privateKeyHex)
	cfg.Agent.Upstream = upstream
	return startProxy(t, cfg, proxyOpts...)
}

// StartAgentProxyWithSelector starts an agent-mode housegate proxy
// configured to discover its upstream per session via the Selector
// (cfg.NetworkState — actually opts.NetworkState because we always
// inject in-memory state in tests, bypassing cfg.NetworkState.Source).
//
// Callers must register at least one IndexerInfo via WithPeerAt before
// the first query, otherwise Selector returns "no bound indexers" and
// the dial fails. For the bootstrap-fallback path, register the indexer
// but do NOT grant any DatabasePermission to the signer's account.
func StartAgentProxyWithSelector(t *testing.T, privateKeyHex string, proxyOpts ...ProxyOption) *TestProxy {
	t.Helper()
	cfg := buildAgentConfig(t, privateKeyHex)
	// Config.Validate insists on either Agent.Upstream OR
	// NetworkState.Source. We inject opts.NetworkState (an in-memory
	// state) via startProxy, which makes the proxy bypass loadNetworkState
	// entirely — but Config.Validate runs BEFORE that bypass kicks in.
	// Setting a yaml-looking source satisfies the validator's IsYAMLSource
	// check; the loader is never invoked because opts.NetworkState wins.
	cfg.NetworkState.Source = filepath.Join(t.TempDir(), "unused.yaml")
	return startProxy(t, cfg, proxyOpts...)
}

func buildAgentConfig(t *testing.T, privateKeyHex string) *config.Config {
	t.Helper()
	cfgVal := config.Default()
	cfg := &cfgVal
	cfg.Listen = "127.0.0.1:0"
	cfg.MetricsListen = ""
	cfg.Agent = agent.Config{
		Mode:          true,
		PrivateKeyHex: privateKeyHex,
	}
	return cfg
}

// WithPeerAt registers `peer` as the IndexerInfo with `indexerId` on the
// agent's in-memory NetworkState. Selector consults the same state via
// the Topology interface; the agent dialer will pick this peer (it's the
// only one bound), and the receiving server proxy must have its own
// IndexerID configured to match if the call chain involves SelfIndexerID
// checks.
func WithPeerAt(indexerId uint64, peer *TestProxy) ProxyOption {
	return func(_ *config.Config, opts *housegate.Options) {
		ns, ok := opts.NetworkState.(*network.InMemoryNetworkState)
		if !ok {
			panic("WithPeerAt requires the default in-memory NetworkState")
		}
		host, port := mustSplitHostPort(peer.Addr)
		ns.IndexerInfos[indexerId] = network.IndexerInfo{
			IndexerId:           indexerId,
			IndexerUrl:          host,
			ClickhouseProxyPort: uint16(port),
		}
	}
}

// WithLogicalDatabaseAt registers a logical database hosted on a
// non-self indexer in the in-memory NetworkState. Used by tests that
// exercise forward.Plugin's OnHello peer pivot — the proxy's own
// SelfIndexerID must differ from indexerId for the pivot to trigger.
func WithLogicalDatabaseAt(database string, indexerId uint64) ProxyOption {
	return func(_ *config.Config, opts *housegate.Options) {
		ns, ok := opts.NetworkState.(*network.InMemoryNetworkState)
		if !ok {
			panic("WithLogicalDatabaseAt requires the default in-memory NetworkState")
		}
		ns.DatabaseInfos[network.Database(database)] = network.DatabaseInfo{IndexerId: indexerId}
	}
}

// WithIndexerID sets the proxy's own indexer id. forward.Plugin compares
// this against the resolved database's IndexerId to decide
// local-vs-peer.
func WithIndexerID(id uint64) ProxyOption {
	return func(cfg *config.Config, _ *housegate.Options) {
		cfg.IndexerID = id
	}
}

// WithRelayKey sets RelayPrivateKeyHex on the server-mode config. The
// proxy builds a RelaySigner from this key, which doubles as the
// PeerSigner used by the rewriter (remote() peer envelope) and by
// forward.Plugin (forward-pivot peer envelope). Pair with the receiver's
// authProxyConfig allowing the derived address so peer-relay JWS
// tokens validate.
func WithRelayKey(privateKeyHex string) ProxyOption {
	return func(cfg *config.Config, _ *housegate.Options) {
		cfg.RelayPrivateKeyHex = privateKeyHex
	}
}

// WithCredentialReplace turns on the credential injector path: the
// server proxy will dial CH using the credentials in the
// CkhManagerConfigPath YAML (which testenv's default builder already
// writes with chUser / chPassword). Required when a test connects with
// a non-CH username (e.g. a __peer__|<addr> envelope) so the proxy can
// fill in the real CH user after the envelope is stripped.
func WithCredentialReplace() ProxyOption {
	return func(cfg *config.Config, _ *housegate.Options) {
		cfg.CredentialReplaceEnabled = true
	}
}

func mustSplitHostPort(addr string) (string, int) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		panic(fmt.Sprintf("mustSplitHostPort: split %q: %v", addr, err))
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		panic(fmt.Sprintf("mustSplitHostPort: parse port from %q: %v", addr, err))
	}
	return host, port
}
