// Package housegate is the embeddable form of the ClickHouse-proxy.
//
// Standalone operators run cmd/housegate; integrators import this
// package and call New(opts).Run(ctx). See the design spec at
// docs/superpowers/specs/2026-04-26-cmd-library-mode-design.md
// for lifecycle and ownership rules.
package housegate

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/redis/go-redis/v9"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/billing"
	"github.com/housegate/housegate/pkg/cluster"
	"github.com/housegate/housegate/pkg/config"
	"github.com/housegate/housegate/pkg/credentials"
	"github.com/housegate/housegate/pkg/log"
	"github.com/housegate/housegate/pkg/plugins/commitgate"
	"github.com/housegate/housegate/pkg/plugins/storageintegrity"
	"github.com/housegate/housegate/pkg/registry"
	"github.com/housegate/housegate/pkg/rewriter"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

// Proxy is a started, ready-to-Serve proxy. Run/RunWith blocks until
// ctx is cancelled or the listener errors; resource teardown happens
// inside before the call returns. There is no separate Close.
type Proxy interface {
	// Run binds a TCP listener on Options.Config.Listen and serves
	// until ctx is cancelled.
	Run(ctx context.Context) error

	// RunWith is identical to Run but the caller owns the listener.
	// Useful for ":0" port-binding in tests, TLS-wrap, unix sockets.
	RunWith(ctx context.Context, ln net.Listener) error

	// Addr returns the bound listener address; nil before Run/RunWith
	// has bound. After binding it is stable for the lifetime of the
	// call.
	Addr() net.Addr

	// IndexerId returns the indexer id this proxy represents — the
	// same id that keys the proxy's IndexerInfo entry in
	// NetworkState. Resolved on every call from the
	// Options.GetIndexerId getter, so a host that learns or rotates
	// the id post-startup gets the fresh value without rebuilding
	// the proxy. Returns 0 when no getter was provided.
	IndexerId() uint64

	// MetricsRegistry returns the dedicated Prometheus registry holding the
	// downstream-metrics Collector's series (ClickHouse + host + Go runtime),
	// or nil when collection is disabled or in agent mode. Hosts gather it
	// merged with the default registry to expose /metrics; the standalone
	// binary does this in startMetricsServer.
	MetricsRegistry() *prometheus.Registry
}

// Options configures a Proxy. Only Config is required. Optional
// dependency overrides: when non-nil, New uses the value verbatim and
// does NOT build one from Config — the caller owns its lifetime
// (Close it after Proxy.Run returns). When nil, the lib builds the
// dep and tears it down when Run returns.
type Options struct {
	Config *config.Config // required

	// NetworkState, if non-nil, replaces the registry.Registry that
	// housegate would otherwise construct from Config.NetworkState.Source
	// (e.g. YAML fixture or agent-mode RPC backend). Embedding hosts
	// (e.g. sentio-node) inject a redis-statemirror-backed implementation
	// directly. When set, BOTH the network_state.source validation rule
	// AND the source value itself are bypassed; the injected registry is
	// used verbatim. The operator-visible source field is irrelevant in
	// that path even if non-empty.
	NetworkState          registry.Registry
	Validator             auth.Validator
	Rewriter              rewriter.Factory
	CredProvider          credentials.CredentialProvider
	Signer                auth.Signer
	UsageClient           billing.UsageClient
	IndexingUsageReporter billing.IndexingUsageReporter
	Cluster               cluster.Cluster

	// StorageIntegrityAdmissionConsumer receives completed, input-bound
	// storage-integrity admissions when storage_integrity.ingress.enabled is
	// true. Server-mode startup fails if ingress is enabled without this
	// consumer, because otherwise completed admissions would remain pending
	// and block subsequent storage writes on persistent client sessions.
	StorageIntegrityAdmissionConsumer storageintegrity.AdmissionConsumer
	// StorageIntegrityPayloadMaterializer converts captured Native ClientData
	// into the replay payload encoding selected by the admitted INSERT. Hosts
	// that want FORMAT CSVWithNames support must provide one; without it, the
	// ingress plugin rejects CSVWithNames fail-closed. It is mandatory when the
	// built-in storage-integrity runtime is enabled because that production
	// profile admits only csv-with-names-v1.
	StorageIntegrityPayloadMaterializer sicore.PayloadMaterializer
	// StorageIntegrityRuntime supplies the real P1e runtime ports used when
	// config.storage_integrity.runtime.enabled is true. It lets HouseGate build
	// the admission consumer while the host supplies the source-side adapter.
	StorageIntegrityRuntime StorageIntegrityRuntimeOptions
	// StorageIntegrityTableSchemas optionally supplies the declared table
	// schema source used by the agent-mode statement plugin
	// (storage_integrity.agent) and the server-mode ingress
	// (storage_integrity.ingress) to resolve schema_hash. When nil, housegate
	// type-asserts the effective registry (Options.NetworkState or the
	// config-loaded one) to registry.TableSchemas; the YAML-backed in-memory
	// state and sentio-node's adapter implement it, while the agent RPC backend
	// does not.
	StorageIntegrityTableSchemas registry.TableSchemas

	// CommitGateObservers gate DDL statements (CREATE / DROP TABLE,
	// CREATE / DROP DATABASE) on host-supplied external commits.
	// Observers fire in slice order; the first non-nil error
	// short-circuits the chain and aborts the query before
	// ClickHouse is contacted. Empty / nil = no commitgate plugin
	// is wired.
	//
	// Server mode only. Agent mode ignores this field — agents
	// forward to a server-mode proxy that fires its own gate. Routed
	// (proxy-to-proxy) sessions also skip the plugin.
	//
	// Auto-wired observers: server-mode New() may PREPEND additional
	// observers ahead of whatever the host supplies, depending on
	// runtime config:
	//   - With cfg.NetworkState.Source set to an in-memory backend, an
	//     InMemoryCommitGateObserver is APPENDED to mutate the in-
	//     memory state in lock-step with passing DDL.
	//   - With cfg.Auth.Enabled = true, a PermissionCommitGateObserver
	//     is PREPENDED to enforce per-StatementType DB permission
	//     policy backed by NetworkState. Disabled in auth-off
	//     deployments because every session is anonymous and the
	//     rewriter opens up every database.
	// Hosts that need to override these defaults can replace the
	// observer set after constructing Options or pass a custom
	// NetworkState via Options.NetworkState.
	//
	// See pkg/plugins/commitgate for the Observer contract,
	// including the mandatory idempotency requirement.
	CommitGateObservers []commitgate.Observer

	// RedisClients is a pool of pre-dialed redis clients keyed by the
	// *resolved* address (post cfg.ResolveRedisAddr). New consults
	// this map before dialing; misses are dialed and added, but only
	// lib-dialed entries are closed when Run returns.
	RedisClients map[string]*redis.Client

	// GetIndexerId returns the indexer id this proxy instance
	// represents — the same id that keys the proxy's IndexerInfo
	// entry in NetworkState (IndexerInfo.IndexerId). The proxy stores
	// the getter and re-resolves on every Proxy.IndexerId() call, so
	// library hosts that learn or rotate the id post-startup (e.g.
	// via on-chain registration) can update it without rebuilding
	// the proxy.
	//
	// nil = Proxy.IndexerId() returns 0.
	GetIndexerId func() uint64

	// Logger receives the proxy's own lifecycle messages (listener
	// binding, RunWith warnings, ...). nil = pkg/log.Default() is used,
	// which falls back to slog.Default(). Library hosts inject any
	// *slog.Logger here; no proprietary interface to implement.
	Logger *slog.Logger
}

// New validates Config and resolves every synchronously-resolvable
// dependency. Errors here are fail-fast configuration / connectivity
// errors (cfg validate, NetworkState dial, credential file parse, rewriter
// factory startup, billing client connect, signer key parse,
// concurrency-limit redis dial). After New returns successfully,
// Run/RunWith only returns listener errors and ctx cancellation.
func New(opts Options) (Proxy, error) {
	if opts.Config == nil {
		return nil, errors.New("housegate.New: Options.Config is required")
	}
	// When the host injects a NetworkState, the network_state.source
	// field is operationally irrelevant: buildServer / buildForwarding
	// use opts.NetworkState verbatim and never call loadNetworkState.
	// Bypass the source-required validation rule in that case so hosts
	// (e.g. sentio-node) don't have to plumb a meaningless dummy
	// address through their config surface.
	var validateErr error
	if opts.NetworkState != nil {
		validateErr = opts.Config.ValidateExceptNetworkStateSource()
	} else {
		validateErr = opts.Config.Validate()
	}
	if validateErr != nil {
		return nil, fmt.Errorf("config validate: %w", validateErr)
	}

	// Default the indexer-id getter from a static config value when
	// the caller did not supply a dynamic one. Dynamic getters
	// (Options.GetIndexerId) take precedence so library hosts that
	// learn or rotate the id post-startup keep working unchanged.
	if opts.GetIndexerId == nil && opts.Config.IndexerID != 0 {
		id := opts.Config.IndexerID
		opts.GetIndexerId = func() uint64 { return id }
	}

	var logger *log.Logger
	if opts.Logger != nil {
		logger = log.NewFromSlog(opts.Logger)
	} else {
		logger = log.Default()
	}

	rf := newRedisFactory(opts.Config, opts.RedisClients)

	var built *builtServer
	var err error
	// Two runtime modes: agent signs+forwards client queries to a
	// remote proxy; server hosts client connections (with or without
	// a local shard — when both shard and upstream are absent, every
	// session is forwarded to a peer via NetworkState).
	switch opts.Config.Mode() {
	case config.ModeAgent:
		built, err = buildAgent(opts, rf)
	default: // ModeServer
		built, err = buildServer(opts, rf)
	}
	if err != nil {
		rf.closeAll()
		return nil, err
	}

	return &proxyImpl{
		cfg:          opts.Config,
		built:        built,
		redisFac:     rf,
		getIndexerId: opts.GetIndexerId,
		logger:       logger,
	}, nil
}

type proxyImpl struct {
	cfg          *config.Config
	built        *builtServer
	redisFac     *redisFactory
	getIndexerId func() uint64
	logger       *log.Logger

	addrMu sync.Mutex
	addr   net.Addr

	teardownOnce sync.Once
}

func (p *proxyImpl) Run(ctx context.Context) error {
	if len(p.built.listeners) == 0 {
		p.teardown()
		return fmt.Errorf("no listeners configured")
	}

	// Bind all listeners eagerly so port-binding errors surface before
	// preServe runs and before any Serve goroutine starts.
	lns := make([]net.Listener, len(p.built.listeners))
	for i, sl := range p.built.listeners {
		ln, err := net.Listen("tcp", sl.ListenAddr)
		if err != nil {
			for j := 0; j < i; j++ {
				_ = lns[j].Close()
			}
			p.teardown()
			return fmt.Errorf("listen %s (%s): %w", sl.Label, sl.ListenAddr, err)
		}
		lns[i] = ln
	}

	// Record the primary (external) listener address for Addr().
	p.addrMu.Lock()
	p.addr = lns[0].Addr()
	p.addrMu.Unlock()

	if err := p.built.preServe(ctx); err != nil {
		for _, ln := range lns {
			_ = ln.Close()
		}
		p.teardown()
		return err
	}
	defer p.teardown()

	g, gctx := errgroup.WithContext(ctx)
	for i, sl := range p.built.listeners {
		sl, ln := sl, lns[i]
		p.logger.Infow("housegate listening",
			"label", sl.Label,
			"addr", ln.Addr().String(),
			"mode", p.cfg.Mode(),
			"indexer_id", p.IndexerId(),
		)
		g.Go(func() error { return sl.Runner.Serve(gctx, ln) })
	}
	return g.Wait()
}

func (p *proxyImpl) RunWith(ctx context.Context, ln net.Listener) error {
	if len(p.built.listeners) == 0 {
		p.teardown()
		return fmt.Errorf("no listeners configured")
	}
	if len(p.built.listeners) > 1 {
		p.logger.Warnw("RunWith only starts the first listener; additional listeners (e.g. internal_listen) are ignored",
			"started", p.built.listeners[0].Label,
			"skipped_listeners", len(p.built.listeners)-1)
	}

	p.addrMu.Lock()
	p.addr = ln.Addr()
	p.addrMu.Unlock()

	if err := p.built.preServe(ctx); err != nil {
		p.teardown()
		return err
	}
	defer p.teardown()

	p.logger.Infow("housegate listening",
		"label", p.built.listeners[0].Label,
		"addr", ln.Addr().String(),
		"mode", p.cfg.Mode(),
		"indexer_id", p.IndexerId(),
	)
	return p.built.listeners[0].Runner.Serve(ctx, ln)
}

func (p *proxyImpl) Addr() net.Addr {
	p.addrMu.Lock()
	defer p.addrMu.Unlock()
	return p.addr
}

func (p *proxyImpl) IndexerId() uint64 {
	if p.getIndexerId == nil {
		return 0
	}
	return p.getIndexerId()
}

func (p *proxyImpl) MetricsRegistry() *prometheus.Registry {
	if p.built == nil {
		return nil
	}
	return p.built.metricsRegistry
}

func (p *proxyImpl) teardown() {
	p.teardownOnce.Do(func() {
		p.built.teardown()
		p.redisFac.closeAll()
	})
}
