package housegate

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"housegate/housegate/pkg/auth"
	"housegate/housegate/pkg/billing"
	"housegate/housegate/pkg/chproto"
	"housegate/housegate/pkg/chsession"
	"housegate/housegate/pkg/cluster"
	"housegate/housegate/pkg/config"
	"housegate/housegate/pkg/credentials"
	"housegate/housegate/pkg/ffifetch"
	"housegate/housegate/pkg/log"
	"housegate/housegate/pkg/network"
	"housegate/housegate/pkg/plugin"
	"housegate/housegate/pkg/plugins/agent"
	authplugin "housegate/housegate/pkg/plugins/auth"
	"housegate/housegate/pkg/plugins/commitgate"
	"housegate/housegate/pkg/plugins/concurrency"
	"housegate/housegate/pkg/plugins/credential"
	"housegate/housegate/pkg/plugins/forward"
	indexingusage "housegate/housegate/pkg/plugins/indexing_usage"
	lthashplugin "housegate/housegate/pkg/plugins/lthash"
	"housegate/housegate/pkg/plugins/materialize"
	metricsplugin "housegate/housegate/pkg/plugins/metrics"
	"housegate/housegate/pkg/plugins/rewrite"
	routeplugin "housegate/housegate/pkg/plugins/route"
	"housegate/housegate/pkg/plugins/sessionstate"
	"housegate/housegate/pkg/plugins/storageintegrity"
	"housegate/housegate/pkg/plugins/usage"
	"housegate/housegate/pkg/proxy"
	"housegate/housegate/pkg/registry"
	"housegate/housegate/pkg/replicationproxy"
	"housegate/housegate/pkg/rewriter"
	"housegate/housegate/pkg/sqlmeta"

	"github.com/redis/go-redis/v9"
)

// Compile-time lock: *proxy.MetricsObserver must keep satisfying
// materialize.Observer so the wiring in buildAgent below stays valid.
var _ materialize.Observer = (*proxy.MetricsObserver)(nil)

// loadNetworkState reads cfg.NetworkState.Source and constructs the
// appropriate backend. The source field is polymorphic: a `.yaml` /
// `.yml` value loads an InMemoryNetworkState fixture (local-dev /
// integration tests, no Redis needed); an http(s) URL constructs an
// RpcNetworkState pointing at a sentio storage-node JSON-RPC endpoint
// (agent mode only — server-mode validation rejects this scheme).
//
// The Redis-backed backend is no longer built in-process; embedders
// (sentio-node) must construct it themselves and inject via
// Options.NetworkState. Specifying a Redis source through cfg returns
// an error directing operators to the injection path.
func loadNetworkState(cfg *config.Config, rf *redisFactory) (registry.Registry, error) {
	if cfg.NetworkState.IsYAMLSource() {
		yamlState, err := network.LoadNetworkStateFromYAML(cfg.NetworkState.Source)
		if err != nil {
			return nil, fmt.Errorf("failed to load network state from YAML: %w", err)
		}
		log.Infow("network state loaded from YAML", "path", cfg.NetworkState.Source)
		return yamlState, nil
	}
	if cfg.NetworkState.IsRpcSource() {
		rpcState, err := network.NewRpcNetworkState(cfg.NetworkState.Source, network.RpcOptions{
			Timeout: cfg.DialTimeout.Duration,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create RPC network state: %w", err)
		}
		log.Infow("network state loaded from RPC", "endpoint", cfg.NetworkState.Source)
		return rpcState, nil
	}
	return nil, fmt.Errorf("network_state.source %q is not a YAML or RPC source; in-process Redis is no longer supported — embedders must inject NetworkState via Options.NetworkState (e.g. sentio-node)", cfg.NetworkState.Source)
}

// resolveNativeLibraryPath returns the FFI library path for the native
// engine: an explicit path wins; otherwise, when a release tag is set, it
// is fetched (and cached) via ffifetch. Non-native engines or an explicit
// path short-circuit to (explicitPath, nil). Callers decide whether a
// fetch error is fatal (materializer) or fail-open (rewriter factory).
func resolveNativeLibraryPath(engine, explicitPath, release, sha256, baseURL string) (string, error) {
	if engine != rewriter.EngineNative || explicitPath != "" || release == "" {
		return explicitPath, nil
	}
	p, err := ffifetch.Fetch(context.Background(), ffifetch.Options{
		Tag:     release,
		SHA256:  sha256,
		BaseURL: baseURL,
	})
	if err != nil {
		return "", err
	}
	log.Infow("native library resolved from release", "tag", release, "path", p)
	return p, nil
}

// buildRewriterFactory constructs the SQL rewriter factory for the
// configured engine — dialing the external sql-rewriter gRPC service or
// loading the in-process rewriter-go engine. Returns nil (and logs a
// warning) when the backend is unavailable at startup — the relay path
// tolerates a nil factory by skipping rewriting entirely.
func buildRewriterFactory(cfg *config.Config, reg registry.Registry) rewriter.Factory {
	// Router-only deployments (no shard, no upstream) never invoke the
	// rewriter — every session gets forwarded to a peer instead.
	if cfg.Shard == nil && cfg.Upstream == "" {
		log.Info("router-only mode: SQL rewriter disabled")
		return nil
	}

	// native_library_release: resolve the FFI library from a rewriter-go
	// release before constructing the factory. Explicit NativeLibraryPath
	// wins; fetch failure keeps the warn-and-disable fail-open posture.
	nativeLibPath, err := resolveNativeLibraryPath(
		cfg.Rewriter.Engine, cfg.Rewriter.NativeLibraryPath,
		cfg.Rewriter.NativeLibraryRelease, cfg.Rewriter.NativeLibrarySHA256,
		cfg.Rewriter.NativeLibraryReleaseBaseURL,
	)
	if err != nil {
		log.Warne(err, "failed to fetch native rewriter library, rewriting disabled")
		return nil
	}

	rwConfig := rewriter.Options{
		Enabled:           true,
		ServiceAddr:       cfg.Rewriter.ServiceAddr,
		Engine:            cfg.Rewriter.Engine,
		NativeLibraryPath: nativeLibPath,
		Upstream:          cfg.Upstream,
		Listen:            cfg.Listen,
		CallbackAddr:      cfg.CallbackUrl,
		Timeout:           cfg.Rewriter.Timeout.Duration,
		PhysicalDatabase:  cfg.Rewriter.PhysicalDatabase,
		AuthEnabled:       cfg.Auth.Enabled,
		Delim:             cfg.Rewriter.Delimiter,
	}
	rwf, err := rewriter.NewSentioNetworkFactory(rwConfig, reg)
	if err != nil {
		log.Warne(err, "failed to create rewriter factory, rewriting disabled")
		return nil
	}
	log.Infow("SQL rewriter enabled",
		"engine", cfg.Rewriter.Engine,
		"service_addr", cfg.Rewriter.ServiceAddr,
		"upstream", cfg.Upstream,
		"physical_database", cfg.Rewriter.PhysicalDatabase,
	)
	return rwf
}

// buildMaterializer constructs the agent-mode Phase-1 materializer from the
// materialize config. Startup fail-fast: a returned error stops buildAgent.
func buildMaterializer(cfg *config.Config) (rewriter.Materializer, error) {
	mc := cfg.Materialize
	libPath, err := resolveNativeLibraryPath(
		mc.Engine, mc.NativeLibraryPath,
		mc.NativeLibraryRelease, mc.NativeLibrarySHA256, mc.NativeLibraryReleaseBaseURL,
	)
	if err != nil {
		return nil, fmt.Errorf("resolve native library: %w", err)
	}
	poolSize := mc.RandomPoolSize
	if poolSize <= 0 {
		poolSize = 16
	}
	return rewriter.NewMaterializer(rewriter.Options{
		Enabled:           true,
		Engine:            mc.Engine,
		ServiceAddr:       mc.ServiceAddr,
		NativeLibraryPath: libPath,
		Timeout:           mc.Timeout.Duration,
	}, poolSize, mc.ProfileID)
}

// buildClusterManager constructs the multi-replica cluster manager
// WITHOUT calling Start. Start runs at Run-time so the health-check
// loop's lifetime is bound to the run ctx. The caller is responsible
// for Close()ing the returned manager.
func buildClusterManager(cfg *config.Config) (*cluster.Manager, error) {
	if cfg.Shard != nil {
		mgr, err := cluster.NewManager(*cfg.Shard, cfg.HealthCheck, cfg.Pool, cfg.Routing)
		if err != nil {
			return nil, fmt.Errorf("failed to create cluster manager: %w", err)
		}
		log.Infow("cluster manager built", "shard", cfg.Shard.Name, "replicas", len(cfg.Shard.Replicas))
		return mgr, nil
	}
	if cfg.Upstream != "" {
		mgr, err := cluster.NewSingleReplicaManager(cfg.Upstream)
		if err != nil {
			// Single-replica wrap is best-effort: the relay's dialer
			// falls back to a direct dial if no manager is available.
			log.Warnfe(err, "failed to create single-replica cluster manager, falling back to direct dial upstream=%v", cfg.Upstream)
			return nil, nil
		}
		log.Infow("cluster manager built in single-replica mode", "upstream", cfg.Upstream)
		return mgr, nil
	}
	return nil, nil
}

// listenerRunner is the narrow lifecycle surface shared by native ClickHouse
// proxy listeners and replication-plane listeners.
type listenerRunner interface {
	Serve(ctx context.Context, ln net.Listener) error
}

type proxyServerRunner struct {
	server *proxy.Server
}

func (r *proxyServerRunner) Serve(ctx context.Context, ln net.Listener) error {
	return r.server.Serve(ctx, ln)
}

type serveFuncRunner struct {
	serve func(context.Context, net.Listener) error
}

func (r serveFuncRunner) Serve(ctx context.Context, ln net.Listener) error {
	return r.serve(ctx, ln)
}

const defaultReplicationInterserverPeerTokenTTL = time.Hour

// serverListener pairs a runner with the address it should bind
// and a human-readable label used in logs and error messages.
type serverListener struct {
	Runner     listenerRunner
	ListenAddr string
	Label      string // e.g. "external", "internal", "agent", "replication-keeper", "replication-interserver"
}

// builtServer is what each per-mode builder returns to the proxyImpl.
// preServe is run once just before any Serve call (e.g. cluster.Start).
// teardown runs after all Serve calls return; it is the reverse-order
// cleanup of every lib-built dep.
type builtServer struct {
	listeners []serverListener
	preServe  func(ctx context.Context)
	teardown  func()
	// metricsRegistry is the dedicated Prometheus registry for the downstream
	// metrics Collector (nil when collection is disabled or in agent mode).
	// Exposed to hosts via Proxy.MetricsRegistry().
	metricsRegistry *prometheus.Registry
}

// buildServer assembles the full server-mode plugin chain and a
// *proxy.Server ready to Serve. It is the canonical wiring point for
// the proxy's request-path; the cmd binary and any library host both
// reach the same chain through this function.
//
// Plugin order in the resulting chain:
//
//	HelloPlugins:  route strip → credential inject → state track → rewrite (if any)
//	QueryPlugins:  JWS validate → usage check → concurrency limit (if enabled) →
//	               rewrite (if any) → relay sign → metrics
//
// The concurrency limiter is wired in right after auth+usage so cheap
// rejections short-circuit before we acquire a permit, but ahead of
// state-tracking and rewriting so we don't spend a gRPC rewrite call
// on a query that will be quota-rejected.
//
// opts.Config must be non-nil; New validates this before calling.
func buildServer(opts Options, rf *redisFactory) (*builtServer, error) {
	cfg := opts.Config

	// Resolve every dep — opts override > built-from-config.
	var teardownStack []func()
	pushTeardown := func(f func()) { teardownStack = append(teardownStack, f) }

	var relaySigner auth.Signer
	// peerSigner is the same key as relaySigner but exposed via the
	// PeerSigner interface so the rewriter can mint peer-relay JWS
	// without depending on pkg/auth. Both signers come from the same
	// RelaySigner instance when one is built locally; an injected
	// opts.Signer that does not also satisfy auth.PeerSigner leaves
	// peer auth disabled.
	var peerSigner auth.PeerSigner
	if opts.Signer != nil {
		relaySigner = opts.Signer
		if ps, ok := opts.Signer.(auth.PeerSigner); ok {
			peerSigner = ps
		}
	} else if cfg.RelayPrivateKeyHex != "" {
		s, err := auth.NewRelaySigner(cfg.RelayPrivateKeyHex)
		if err != nil {
			return nil, fmt.Errorf("failed to create relay signer: %w", err)
		}
		relaySigner = s
		peerSigner = s
		log.Infow("relay JWS signer enabled", "address", s.Address())
	}

	var validator auth.Validator
	if opts.Validator != nil {
		validator = opts.Validator
	} else if cfg.Auth.Enabled {
		// Gate SQL_sentio_maintenance on the host-injected Options.Signer
		// only. The cfg.RelayPrivateKeyHex fallback (used by the standalone
		// binary) is operator-supplied and does NOT grant maintenance —
		// that bypass requires explicit host attestation. When opts.Signer
		// is unset, indexerAddr stays empty and every maintenance request
		// is rejected.
		var indexerAddr string
		if opts.Signer != nil {
			indexerAddr = opts.Signer.Address()
		}
		validator = auth.NewEthValidator(cfg.Auth.AllowedAddresses, cfg.Auth.MaxTokenAge.Duration, true, cfg.Auth.AllowNoAuth, indexerAddr, cfg.Auth.PlatformOperatorAddresses)
		log.Infow("Ethereum signature auth enabled",
			"allowed_addresses", len(cfg.Auth.AllowedAddresses),
			"allow_no_auth", cfg.Auth.AllowNoAuth,
			"indexer_address", indexerAddr,
			"platform_operator_addresses", len(cfg.Auth.PlatformOperatorAddresses))
	}

	var reg registry.Registry
	if opts.NetworkState != nil {
		reg = opts.NetworkState
	} else {
		var err error
		reg, err = loadNetworkState(cfg, rf)
		if err != nil {
			return nil, err
		}
	}
	log.Infow("network state loaded", "source", cfg.NetworkState.Source)
	// InMemoryNetworkState (YAML fixtures, dev mode) carries enough
	// state for the in-memory commitgate observer to mutate as DDL
	// passes through. Other Registry implementations (RPC backend,
	// sentio-node's redis adapter) are opaque to this wiring path.
	if ims, ok := reg.(*network.InMemoryNetworkState); ok {
		opts.CommitGateObservers = append(opts.CommitGateObservers, network.NewInMemoryCommitGateObserver(
			ims, []sqlmeta.StatementType{
				sqlmeta.StatementTypeCreateDatabase, sqlmeta.StatementTypeDropDatabase,
				sqlmeta.StatementTypeCreateTable, sqlmeta.StatementTypeDropTable,
				sqlmeta.StatementTypeCreateView, sqlmeta.StatementTypeCreateMaterializedView,
				sqlmeta.StatementTypeDropView,
				sqlmeta.StatementTypeGrant, sqlmeta.StatementTypeRevoke,
			}, opts.GetIndexerId))
	}
	// Permission gate is meaningful only when auth establishes a
	// per-session identity. In auth-disabled deployments (local dev,
	// some test setups) every session is anonymous and the rewriter
	// already opens up every database — wiring the observer here
	// would reject 100% of gated traffic with "requires authenticated
	// account". Stay aligned with rewriter.Options.AuthEnabled by
	// gating on the same flag.
	if cfg.Auth.Enabled {
		opts.CommitGateObservers = append(
			[]commitgate.Observer{network.NewPermissionCommitGateObserver(reg)},
			opts.CommitGateObservers...,
		)
	}
	// Surface-argument validation is universal and stateless —
	// fail-closes CREATE DATABASE on a malformed name and GRANT/REVOKE
	// on a non-Ethereum-address grantee before any registrar observer
	// mutates state. Prepended so it short-circuits ahead of the
	// in-memory registrar / permission gate.
	opts.CommitGateObservers = append(
		[]commitgate.Observer{commitgate.NewArgsValidatorObserver()},
		opts.CommitGateObservers...,
	)

	var rwFactory rewriter.Factory
	if opts.Rewriter != nil {
		rwFactory = opts.Rewriter
	} else {
		rwFactory = buildRewriterFactory(cfg, reg)
		if rwf, ok := rwFactory.(*rewriter.SentioNetworkFactory); ok && rwf != nil {
			rwf.SetGetIndexerId(opts.GetIndexerId)
			pushTeardown(func() { rwf.Close() })
		}
	}

	// Cluster: lib-built path constructs and registers Close+Start;
	// caller-injected path is used as-is (no Start, no Close).
	var clusterIface cluster.Cluster
	var libCluster *cluster.Manager
	if opts.Cluster != nil {
		clusterIface = opts.Cluster
	} else {
		m, err := buildClusterManager(cfg)
		if err != nil {
			return nil, err
		}
		if m != nil {
			libCluster = m
			clusterIface = m
			pushTeardown(func() { m.Close() })
		}
	}
	// Wire the resolved cluster into the rewriter factory for multi-replica
	// isLocal detection — but ONLY when the lib built the factory itself.
	// A caller-injected *rewriter.Factory is used as-is per the Options
	// contract; mutating it here would silently rewrite shared state.
	// Library callers who want this wiring must Set it themselves before
	// passing the factory in.
	if opts.Rewriter == nil && clusterIface != nil {
		if rwf, ok := rwFactory.(*rewriter.SentioNetworkFactory); ok && rwf != nil {
			rwf.SetClusterManager(clusterIface)
		}
	}

	var credProvider credentials.CredentialProvider
	if opts.CredProvider != nil {
		credProvider = opts.CredProvider
	} else if cfg.CredentialReplaceEnabled && cfg.CkhManagerConfigPath != "" {
		cp, err := credentials.LoadCkhManagerYAMLProvider(cfg.CkhManagerConfigPath)
		if err != nil {
			return nil, fmt.Errorf("load credential provider: %w", err)
		}
		credProvider = cp
		// Same caller-injected guard as SetClusterManager above: only wire
		// the credential provider into the rewriter factory when the lib
		// built it. Caller-injected factories are used as-is.
		if opts.Rewriter == nil {
			if rwf, ok := rwFactory.(*rewriter.SentioNetworkFactory); ok && rwf != nil {
				rwf.SetCredentialProvider(cp)
			}
		}
		log.Info("credential replacement enabled via ckh_manager_config")
	}

	// Peer validator runs at OnHello: when an inbound ClientHello
	// carries a peer-relay envelope (__peer__|<addr>), the credential
	// plugin verifies the JWS in the password against this validator.
	// We reuse the EthValidator built for the SQL-binding path — the
	// allowlist is the same set of trusted peers in both cases.
	var peerValidator auth.PeerValidator
	if pv, ok := validator.(auth.PeerValidator); ok {
		peerValidator = pv
	}

	// Wire the peer signer into the rewriter so outbound remote()
	// clauses carry a peer-relay envelope (user / password). Same
	// caller-injected guard as SetClusterManager / SetCredentialProvider:
	// only mutate when the lib built the factory.
	if opts.Rewriter == nil && peerSigner != nil {
		if rwf, ok := rwFactory.(*rewriter.SentioNetworkFactory); ok && rwf != nil {
			rwf.SetPeerSigner(peerSigner)
			if cfg.PeerTokenTTL.Duration > 0 {
				rwf.SetPeerTokenTTL(cfg.PeerTokenTTL.Duration)
			}
			log.Infow("rewriter peer-relay signer wired",
				"address", peerSigner.Address(),
				"token_ttl", cfg.PeerTokenTTL.Duration)
		}
	}

	var usageClient billing.UsageClient
	if opts.UsageClient != nil {
		usageClient = opts.UsageClient
	} else if cfg.Usage.Enabled && cfg.Usage.SentioNodeAddr != "" {
		// The in-process sentio-node usage client was moved out of
		// housegate (it pulled in an external proto dep). Embedders
		// must inject via Options.UsageClient; the standalone binary
		// gets a no-op until a host wires one up.
		log.Warnw("usage tracking requested in config but no UsageClient was injected; query billing disabled (embedders must supply Options.UsageClient)",
			"sentio_node", cfg.Usage.SentioNodeAddr)
	}

	var concurrencyRedis *redis.Client
	if cfg.ConcurrencyLimit.Enabled {
		var err error
		concurrencyRedis, err = rf.get(cfg.ConcurrencyLimit.RedisAddr)
		if err != nil {
			return nil, fmt.Errorf("concurrency limiter redis: %w", err)
		}
	}

	// ---- plugin chain (verbatim from cmd/serve.go serveServer) ----
	obs := proxy.NewMetricsObserver()
	metrics := metricsplugin.New(obs)

	queryPlugins := []plugin.QueryPlugin{
		&authplugin.Plugin{Validator: validator, Access: reg},
		&usage.Plugin{Client: usageClient},
	}
	queryCompletePlugins := []plugin.QueryCompletePlugin{}
	closePlugins := []plugin.ClosePlugin{}

	// indexing_usage is appended *after* the rewriter below so its
	// OnQuery sees qctx.StatementType / AccessedTables populated. The
	// plugin reports each INSERT directly to the injected sink — no
	// local accumulator/ticker — so sentio-node's usage.Server is the
	// single point of per-key folding. queryPlugins / chain insertion
	// happens further down right after rewritePlug.
	var iuPlugin *indexingusage.Plugin
	if cfg.IndexingUsage.Enabled {
		iuPlugin = indexingusage.New(opts.IndexingUsageReporter)
		log.Infow("indexing_usage enabled",
			"reporter_injected", opts.IndexingUsageReporter != nil,
		)
	}

	if cfg.ConcurrencyLimit.Enabled && concurrencyRedis != nil {
		lim := concurrency.NewRedisLimiter(concurrencyRedis, cfg.ConcurrencyLimit.Timeout.Duration)
		concPlugin := &concurrency.Plugin{
			Limiter:  lim,
			FailOpen: cfg.ConcurrencyLimit.FailOpen,
			Resolvers: []concurrency.DimensionResolver{
				concurrency.UserDimension(cfg.ConcurrencyLimit.PerUser),
				concurrency.NoneStakeLevelResolver(),
			},
		}
		queryPlugins = append(queryPlugins, concPlugin)
		queryCompletePlugins = append(queryCompletePlugins, concPlugin)
		closePlugins = append(closePlugins, concPlugin)
		log.Infow("concurrency limiter enabled",
			"per_user_quota", cfg.ConcurrencyLimit.PerUser,
			"timeout", cfg.ConcurrencyLimit.Timeout,
			"fail_open", cfg.ConcurrencyLimit.FailOpen,
		)
	}

	var dataPlugins []plugin.DataPlugin
	var strictDataPlugins []plugin.StrictDataPlugin
	var queryInputCompletePlugins []plugin.QueryInputCompletePlugin
	var queryAbortPlugins []plugin.QueryAbortPlugin
	if cfg.LtHash.Enabled {
		ltPlug := lthashplugin.New(lthashplugin.DefaultRegistry)
		queryPlugins = append(queryPlugins, ltPlug)
		queryCompletePlugins = append(queryCompletePlugins, ltPlug)
		closePlugins = append(closePlugins, ltPlug)
		dataPlugins = append(dataPlugins, ltPlug)
		log.Infow("lthash commitment plugin enabled (MVP)")
	}

	sessstatePlug := &sessionstate.Plugin{Config: cfg.State}

	var selfIndexerID uint64
	if opts.GetIndexerId != nil {
		selfIndexerID = opts.GetIndexerId()
	} else {
		selfIndexerID = cfg.IndexerID
	}
	// Local dialer for forward.Plugin's remote→local rebind path: same
	// cluster/upstream branches as the main session dialer below, minus
	// the route-target and router-only paths (those would dial a peer,
	// which is the opposite of what rebind-to-local needs). Router-only
	// proxies leave this nil; on those proxies no database has
	// IndexerId == self anyway, so the rebind-to-local path is
	// unreachable and forward.Plugin's nil-check is the safety net.
	var localDialer func(ctx context.Context) (*chproto.Codec, error)
	if clusterIface != nil {
		localDialer = func(ctx context.Context) (*chproto.Codec, error) {
			pc, err := clusterIface.GetConnection(ctx)
			if err != nil {
				return nil, fmt.Errorf("cluster GetConnection: %w", err)
			}
			return chproto.NewCodec(pc, chproto.DirToUpstream), nil
		}
	} else if cfg.Upstream != "" {
		upstream := cfg.Upstream
		localDialer = func(ctx context.Context) (*chproto.Codec, error) {
			return dialRaw(ctx, upstream, cfg.DialTimeout.Duration)
		}
	}

	forwardPlug := &forward.Plugin{
		Topology:         reg,
		Databases:        reg,
		SelfIndexerID:    selfIndexerID,
		PeerSigner:       peerSigner,
		PeerTokenTTL:     cfg.PeerTokenTTL.Duration,
		Fallback:         credProvider,
		LocalDialer:      localDialer,
		PhysicalDatabase: cfg.Rewriter.PhysicalDatabase,
	}
	queryPlugins = append(queryPlugins, forwardPlug)

	var rewritePlug *rewrite.Plugin
	if rwFactory != nil {
		var physicalDB string
		if rwf, ok := rwFactory.(*rewriter.SentioNetworkFactory); ok && rwf != nil {
			physicalDB = rwf.PhysicalDatabase()
		}
		rewritePlug = &rewrite.Plugin{
			Factory:          rwFactory,
			PhysicalDatabase: physicalDB,
			Observer:         obs,
		}
	}

	if rewritePlug != nil {
		queryPlugins = append(queryPlugins, rewritePlug)
	}

	var storageIntegrityIngress *storageintegrity.Plugin
	if cfg.StorageIntegrity.Ingress.Enabled {
		ingressCfg := cfg.StorageIntegrity.Ingress
		ingressValidator := auth.NewEthValidator(
			ingressCfg.AllowedAddresses,
			ingressCfg.MaxTokenAge.Duration,
			true,
			false,
			"",
			nil,
		)
		storageIntegrityIngress = storageintegrity.New(storageintegrity.Config{
			Enabled:         true,
			AuthValidator:   ingressValidator,
			Purpose:         auth.QueryPurpose,
			RequestTimeout:  ingressCfg.RequestTimeout.Duration,
			MaxPayloadBytes: ingressCfg.MaxPayloadBytes,
		})
		queryPlugins = append(queryPlugins, storageIntegrityIngress)
		strictDataPlugins = append(strictDataPlugins, storageIntegrityIngress)
		queryInputCompletePlugins = append(queryInputCompletePlugins, storageIntegrityIngress)
		queryAbortPlugins = append(queryAbortPlugins, storageIntegrityIngress)
		closePlugins = append(closePlugins, storageIntegrityIngress)
		log.Infow("storage_integrity ingress enabled",
			"allowed_addresses", len(ingressCfg.AllowedAddresses),
			"max_token_age", ingressCfg.MaxTokenAge.Duration,
			"request_timeout", ingressCfg.RequestTimeout.Duration,
			"max_payload_bytes", ingressCfg.MaxPayloadBytes,
		)
	}

	// indexing_usage runs *after* the rewriter so it can read the
	// classified StatementType + AccessedTables it populates.
	if iuPlugin != nil {
		queryPlugins = append(queryPlugins, iuPlugin)
	}

	var cgPlug *commitgate.Plugin
	if len(opts.CommitGateObservers) > 0 {
		cgPlug = commitgate.NewPlugin(opts.CommitGateObservers)
		queryPlugins = append(queryPlugins, cgPlug)
		queryCompletePlugins = append(queryCompletePlugins, cgPlug)
		log.Infow("commitgate enabled",
			"observers", len(opts.CommitGateObservers),
			"subscribed_types", cgPlug.SubscribedTypes(),
		)
	}

	queryPlugins = append(queryPlugins,
		&routeplugin.Signer{Signer: relaySigner, Observer: obs},
		metrics,
	)

	connLifecycle := []plugin.ConnLifecyclePlugin{metrics}
	exceptionPlugins := []plugin.ExceptionPlugin{}
	if rewritePlug != nil {
		connLifecycle = append(connLifecycle, rewritePlug)
		closePlugins = append(closePlugins, rewritePlug)
		exceptionPlugins = append(exceptionPlugins, rewritePlug)
	}
	if cgPlug != nil {
		exceptionPlugins = append(exceptionPlugins, cgPlug)
	}
	exceptionPlugins = append(exceptionPlugins, metrics)

	helloPlugins := []plugin.HelloPlugin{
		routeplugin.Stripper{},
		&credential.Plugin{
			Provider:         credProvider,
			PeerValidator:    peerValidator,
			GetSelfIndexerID: opts.GetIndexerId,
		},
		forwardPlug,
		sessstatePlug,
	}
	if rewritePlug != nil {
		helloPlugins = append(helloPlugins, rewritePlug)
	}

	chain := &plugin.PluginChain{
		ConnLifecyclePlugins:      connLifecycle,
		HandshakeCompletePlugins:  []plugin.HandshakeCompletePlugin{metrics},
		HelloPlugins:              helloPlugins,
		QueryPlugins:              queryPlugins,
		StrictDataPlugins:         strictDataPlugins,
		DataPlugins:               dataPlugins,
		QueryCompletePlugins:      queryCompletePlugins,
		QueryInputCompletePlugins: queryInputCompletePlugins,
		QueryAbortPlugins:         queryAbortPlugins,
		ClosePlugins:              closePlugins,
		ExceptionPlugins:          exceptionPlugins,
	}

	selfPort := selfListenPort(cfg.Listen)
	const maxRouterOnlyDialAttempts = 3
	dialer := func(ctx context.Context, sess chsession.Session) (*chproto.Codec, error) {
		// Plugin-set route target (e.g. forward.Plugin's USE pivot, or
		// routeplugin's __route__ envelope) wins over every other path.
		if target := routeplugin.RouteTarget(sess.State()); target != "" {
			return dialRaw(ctx, target, cfg.DialTimeout.Duration)
		}
		// Local replica pool — present only when cfg.Shard is set.
		if clusterIface != nil {
			pc, err := clusterIface.GetConnection(ctx)
			if err != nil {
				return nil, fmt.Errorf("cluster GetConnection: %w", err)
			}
			return chproto.NewCodec(pc, chproto.DirToUpstream), nil
		}
		// Static upstream fallback (single-replica or test fixtures).
		if cfg.Upstream != "" {
			return dialRaw(ctx, cfg.Upstream, cfg.DialTimeout.Duration)
		}
		// Router-only deployment: no shard, no upstream — forward every
		// session to a random bound peer discovered via NetworkState.
		// Three dial attempts cover the common case where the first
		// random pick is briefly unreachable.
		if reg == nil {
			return nil, fmt.Errorf("no cluster manager, no upstream, and no NetworkState — nowhere to forward")
		}
		var lastErr error
		for i := 0; i < maxRouterOnlyDialAttempts; i++ {
			target, err := pickRandomBoundProxy(reg, selfPort)
			if err != nil {
				return nil, err
			}
			codec, err := dialRaw(ctx, target, cfg.DialTimeout.Duration)
			if err == nil {
				log.Infow("router-only: bound peer", "target", target, "attempt", i+1)
				return codec, nil
			}
			lastErr = err
			log.Warnfe(err, "router-only: dial %s failed (attempt %d/%d)", target, i+1, maxRouterOnlyDialAttempts)
			obs.Error("dial", err)
		}
		return nil, fmt.Errorf("router-only: all %d dial attempts failed: %w", maxRouterOnlyDialAttempts, lastErr)
	}

	srv := proxy.NewServerWithObserver(chain, dialer, obs)
	srv.ShutdownTimeout = cfg.ShutdownTimeout.Duration

	listeners := []serverListener{
		{Runner: &proxyServerRunner{server: srv}, ListenAddr: cfg.Listen, Label: "external"},
	}

	if cfg.InternalListen != "" {
		internalSrv := proxy.NewServerWithObserver(chain, dialer, obs)
		internalSrv.ShutdownTimeout = cfg.ShutdownTimeout.Duration
		internalSrv.PreflagSession = func(st *chsession.SessionState) {
			st.IsPeerTrusted = true
			st.IsInternalPort = true
		}
		listeners = append(listeners, serverListener{
			Runner:     &proxyServerRunner{server: internalSrv},
			ListenAddr: cfg.InternalListen,
			Label:      "internal",
		})
	}

	if cfg.ReplicationProxy.Keeper.Enabled {
		keeperSrv, err := replicationproxy.NewKeeperServer(replicationproxy.KeeperOptions{
			Upstreams:   cfg.ReplicationProxy.Keeper.Upstreams,
			DialTimeout: cfg.ReplicationProxy.Keeper.DialTimeout.Duration,
		})
		if err != nil {
			return nil, fmt.Errorf("build replication keeper listener: %w", err)
		}
		listeners = append(listeners, serverListener{
			Runner:     serveFuncRunner{serve: keeperSrv.Serve},
			ListenAddr: cfg.ReplicationProxy.Keeper.Listen,
			Label:      "replication-keeper",
		})
	}

	if cfg.ReplicationProxy.Interserver.Enabled {
		interserverPeerAuth, err := buildReplicationInterserverPeerAuth(cfg, peerSigner, peerValidator)
		if err != nil {
			return nil, err
		}
		interserverRoutes, err := buildReplicationInterserverRoutes(cfg.ReplicationProxy.Interserver.Routes)
		if err != nil {
			return nil, err
		}
		interserverSrv, err := replicationproxy.NewInterserverServer(replicationproxy.InterserverOptions{
			SelfIndexerID: selfIndexerID,
			LocalUpstream: cfg.ReplicationProxy.Interserver.LocalUpstream,
			Routes:        interserverRoutes,
			PeerAuth:      interserverPeerAuth,
			DialTimeout:   cfg.ReplicationProxy.Interserver.DialTimeout.Duration,
			ReadTimeout:   cfg.ReplicationProxy.Interserver.ReadTimeout.Duration,
			WriteTimeout:  cfg.ReplicationProxy.Interserver.WriteTimeout.Duration,
		})
		if err != nil {
			return nil, fmt.Errorf("build replication interserver listener: %w", err)
		}
		listeners = append(listeners, serverListener{
			Runner:     serveFuncRunner{serve: interserverSrv.Serve},
			ListenAddr: cfg.ReplicationProxy.Interserver.Listen,
			Label:      "replication-interserver",
		})
	}

	// Downstream-metrics Collector (host + runtime + per-replica ClickHouse).
	// Started in preServe (mirroring libCluster.Start), stopped via the run ctx;
	// its poller connections are closed on teardown.
	metricsCollector, metricsRegistry, metricsCleanup := buildCollector(*cfg, credProvider, selfIndexerID)
	if metricsCleanup != nil {
		pushTeardown(metricsCleanup)
	}

	return &builtServer{
		listeners:       listeners,
		metricsRegistry: metricsRegistry,
		preServe: func(ctx context.Context) {
			if libCluster != nil {
				libCluster.Start(ctx)
				if cfg.Shard != nil {
					log.Infow("cluster manager started", "shard", libCluster.Shard().Name)
				} else {
					log.Infow("cluster manager started in single-replica mode", "upstream", cfg.Upstream)
				}
			}
			if metricsCollector != nil {
				go metricsCollector.Start(ctx)
				log.Infow("metrics collector started", "interval", cfg.Observability.Collector.Interval.Duration)
			}
		},
		teardown: func() {
			// Reverse order, mirroring runServer's defer stack.
			for i := len(teardownStack) - 1; i >= 0; i-- {
				teardownStack[i]()
			}
		},
	}, nil
}

func buildReplicationInterserverPeerAuth(cfg *config.Config, peerSigner auth.PeerSigner, peerValidator auth.PeerValidator) (*replicationproxy.InterserverPeerAuth, error) {
	ttl := cfg.PeerTokenTTL.Duration
	if ttl <= 0 {
		ttl = defaultReplicationInterserverPeerTokenTTL
	}
	peerAuth, err := replicationproxy.NewInterserverPeerAuth(replicationproxy.InterserverPeerAuthOptions{
		Signer:      peerSigner,
		Validator:   peerValidator,
		TokenTTL:    ttl,
		UserHeader:  cfg.ReplicationProxy.Interserver.PeerUserHeader,
		TokenHeader: cfg.ReplicationProxy.Interserver.PeerTokenHeader,
	})
	if err != nil {
		return nil, fmt.Errorf("build replication interserver peer auth: %w", err)
	}
	return peerAuth, nil
}

func buildReplicationInterserverRoutes(routes []config.ReplicationProxyInterserverRoute) ([]replicationproxy.InterserverRoute, error) {
	out := make([]replicationproxy.InterserverRoute, 0, len(routes))
	for _, route := range routes {
		targetIndexerID, err := strconv.ParseUint(route.Peer, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("replication interserver route peer %q must be a decimal target indexer id: %w", route.Peer, err)
		}
		out = append(out, replicationproxy.InterserverRoute{
			Peer:            route.Peer,
			TargetIndexerID: targetIndexerID,
			Upstream:        route.Upstream,
		})
	}
	return out, nil
}

func buildAgent(opts Options, rf *redisFactory) (*builtServer, error) {
	cfg := opts.Config

	var signer auth.Signer
	if opts.Signer != nil {
		signer = opts.Signer
	} else {
		s, err := auth.NewRelaySigner(cfg.Agent.PrivateKeyHex)
		if err != nil {
			return nil, fmt.Errorf("failed to create agent signer: %w", err)
		}
		signer = s
	}

	obs := proxy.NewMetricsObserver()
	metrics := metricsplugin.New(obs)

	// materialize runs first in the query chain (before the agent signer)
	// so that, when enabled, the signed and forwarded SQL are identical —
	// non-deterministic functions are already resolved to constants by
	// the time Signer's JWS binds the query body.
	queryPlugins := []plugin.QueryPlugin{}
	var materializerClose func()
	if cfg.Materialize.Enabled {
		m, err := buildMaterializer(cfg)
		if err != nil {
			return nil, fmt.Errorf("materialize: %w", err) // startup fail-fast
		}
		materializerClose = func() { _ = m.Close() }
		// Random-pool size (cfg.Materialize.RandomPoolSize) is applied
		// materializer-side in buildMaterializer, not on the plugin —
		// don't re-add a PoolSize field here.
		queryPlugins = append(queryPlugins, &materialize.Plugin{
			Materializer: m,
			Observer:     obs,
		})
		log.Infow("agent materialize enabled", "engine", cfg.Materialize.Engine)
	}
	queryPlugins = append(queryPlugins,
		&agent.Plugin{Signer: signer, Observer: obs, Owner: cfg.Agent.Owner, IsDriver: cfg.Agent.Driver},
		metrics,
	)

	chain := &plugin.PluginChain{
		ConnLifecyclePlugins:     []plugin.ConnLifecyclePlugin{metrics},
		HandshakeCompletePlugins: []plugin.HandshakeCompletePlugin{metrics},
		QueryPlugins:             queryPlugins,
		ExceptionPlugins:         []plugin.ExceptionPlugin{metrics},
	}

	// Two ways to choose an upstream:
	//   1. Explicit cfg.Agent.Upstream — pinned target, no NetworkState
	//      lookup. Useful for hermetic deployments and integration tests.
	//   2. Auto-discovery via Selector — read NetworkState (yaml or
	//      redis-backed) and pick a random permissioned peer per session.
	//      Validation in pkg/config guarantees one of the two is set.
	dialer, err := buildAgentDialer(opts, rf, signer.Address(), obs)
	if err != nil {
		return nil, err
	}

	srv := proxy.NewServerWithObserver(chain, dialer, obs)
	srv.ShutdownTimeout = cfg.ShutdownTimeout.Duration

	return &builtServer{
		listeners: []serverListener{
			{Runner: &proxyServerRunner{server: srv}, ListenAddr: cfg.Listen, Label: "agent"},
		},
		preServe: func(context.Context) {},
		teardown: func() {
			if materializerClose != nil {
				materializerClose()
			}
		},
	}, nil
}

// buildAgentDialer returns the per-session upstream dialer for agent
// mode. When cfg.Agent.Upstream is set, every dial returns that exact
// address. Otherwise, NetworkState is loaded once at build time and a
// Selector picks a random permissioned peer (or any bound peer for
// brand-new accounts) for each new session.
func buildAgentDialer(opts Options, rf *redisFactory, account string, obs *proxy.MetricsObserver) (func(context.Context, chsession.Session) (*chproto.Codec, error), error) {
	cfg := opts.Config

	if cfg.Agent.Upstream != "" {
		log.Infow("agent proxy mode: signing queries",
			"address", account, "upstream", cfg.Agent.Upstream)
		return func(ctx context.Context, _ chsession.Session) (*chproto.Codec, error) {
			return dialRaw(ctx, cfg.Agent.Upstream, cfg.DialTimeout.Duration)
		}, nil
	}

	var reg registry.Registry
	if opts.NetworkState != nil {
		reg = opts.NetworkState
	} else {
		var err error
		reg, err = loadNetworkState(cfg, rf)
		if err != nil {
			return nil, err
		}
	}
	if reg == nil {
		return nil, fmt.Errorf("agent auto-discovery: NetworkState is nil")
	}
	selector := &agent.Selector{
		Topology:  reg,
		Databases: reg,
		Access:    reg,
		Account:   account,
	}
	log.Infow("agent proxy mode: signing queries with NetworkState upstream auto-discovery",
		"address", account, "network_state_source", cfg.NetworkState.Source)

	return func(ctx context.Context, _ chsession.Session) (*chproto.Codec, error) {
		// One *rand.Rand per Pick keeps the lock-free path goroutine-safe;
		// the cost is a single syscall per session — negligible against
		// the TCP dial that follows.
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		choice, err := selector.Pick(r)
		if err != nil {
			return nil, fmt.Errorf("agent select upstream: %w", err)
		}
		if choice.IsBootstrap {
			log.Warnw("agent bootstrap fallback: no permissioned databases for account",
				"account", account, "indexer_id", choice.IndexerId, "addr", choice.Addr())
			obs.AgentBootstrapFallback()
		}
		return dialRaw(ctx, choice.Addr(), cfg.DialTimeout.Duration)
	}, nil
}

func dialRaw(ctx context.Context, addr string, timeout time.Duration) (*chproto.Codec, error) {
	d := net.Dialer{Timeout: timeout}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	return chproto.NewCodec(conn, chproto.DirToUpstream), nil
}

func pickRandomBoundProxy(topo registry.Topology, selfPort int) (string, error) {
	all := topo.AllIndexers()
	if len(all) == 0 {
		return "", fmt.Errorf("router-only: no indexer infos in network state")
	}
	addrs := make([]string, 0, len(all))
	for _, addr := range all {
		if addr.HousegatePort == 0 {
			continue
		}
		if selfPort > 0 && int(addr.HousegatePort) == selfPort && isLocalAddress(addr.Url) {
			continue
		}
		addrs = append(addrs, fmt.Sprintf("%s:%d", addr.Url, addr.HousegatePort))
	}
	if len(addrs) == 0 {
		return "", fmt.Errorf("router-only: no bound proxies (all self or unbound)")
	}
	return addrs[rand.Intn(len(addrs))], nil
}

func selfListenPort(listen string) int {
	if listen == "" {
		return 0
	}
	host := listen
	if listen[0] != ':' {
		_, p, err := net.SplitHostPort(listen)
		if err != nil {
			return 0
		}
		host = ":" + p
	}
	port, err := strconv.Atoi(host[1:])
	if err != nil {
		return 0
	}
	return port
}

func isLocalAddress(host string) bool {
	if host == "" || host == "localhost" || host == "127.0.0.1" || host == "::1" {
		return true
	}
	ifaces, err := net.InterfaceAddrs()
	if err != nil {
		return false
	}
	for _, a := range ifaces {
		if ipNet, ok := a.(*net.IPNet); ok && ipNet.IP.String() == host {
			return true
		}
	}
	return false
}
