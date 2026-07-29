// Package metrics wires a plugin.Hooks implementation that emits
// Prometheus metrics at the plugin-chain callbacks exposed by the
// Server/Relay.
//
// It is deliberately a thin adapter — all metric definitions and
// registrations live in pkg/proxy/observer.go. This plugin just
// translates chain callbacks into Observer method calls.
//
// Coverage:
//
//	clickhouse_proxy_active_connections                (OnConnect / OnDisconnect)
//	clickhouse_proxy_handshake_duration_seconds        (OnHandshakeComplete)
//	clickhouse_proxy_queries_forwarded_total           (OnQuery — semantic
//	                                                    counter: query
//	                                                    passed every
//	                                                    upstream-bound
//	                                                    plugin)
//	clickhouse_proxy_errors_total{phase=upstream_exception} (OnException)
//
// Wire-level counters (packets_total, server_packets_total,
// bytes_transferred_total, streaming_data_blocks_total) are emitted
// directly from the Relay in pkg/proxy so they reflect what actually
// came off the socket, not what plugins accepted. See
// proxy.PacketObserver.
//
// OnException is best-effort: Relay decodes upstream Exception packets
// from the buffered chunk via a first-byte heuristic. Plugins that
// require strict per-query Exception delivery should pair with an
// idempotent OnQuery / OnQueryComplete pattern.
package metrics

import (
	"context"
	"fmt"
	"time"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/plugin"
)

// Observer is the subset of *proxy.MetricsObserver this plugin depends
// on. Accepting an interface keeps the plugin unit-testable and avoids
// a pkg/plugins/metrics → pkg/proxy import cycle (pkg/proxy already
// depends on pkg/plugin, which would cycle back through pkg/plugins).
type Observer interface {
	ConnectionOpened()
	ConnectionClosed()
	QueryForwarded()
	Error(phase string, err error)
	HandshakeCompleted(duration float64)
}

// Plugin emits Prometheus metrics at the points listed in the package
// doc. Construct via New so callers cannot forget to pass an Observer.
type Plugin struct {
	obs Observer
}

// New wraps obs into a Plugin. If obs is nil, returns nil — callers
// should simply skip wiring in that case.
func New(obs Observer) *Plugin {
	if obs == nil {
		return nil
	}
	return &Plugin{obs: obs}
}

// --- ConnLifecyclePlugin ---

func (p *Plugin) OnConnect(_ context.Context, _ chsession.Session) error {
	p.obs.ConnectionOpened()
	return nil
}

func (p *Plugin) OnDisconnect(_ chsession.Session) {
	p.obs.ConnectionClosed()
}

// --- HandshakeCompletePlugin ---
//
// Fired once at the tail of Relay.handshake — the sum of the
// ClientHello decode, ServerHello round-trip, and addendum negotiation
// observed from the server side.
func (p *Plugin) OnHandshakeComplete(_ context.Context, _ chsession.Session, d time.Duration) {
	p.obs.HandshakeCompleted(d.Seconds())
}

// --- QueryPlugin ---
//
// Runs last in the OnQuery chain so an earlier plugin's rejection
// (auth, usage, concurrency, …) does not inflate queries_forwarded.
// PluginChain short-circuits on error, so by the time we're invoked
// the query is guaranteed to reach upstream.
//
// This is the "semantic" query counter — packets_total{type=Query} is
// emitted separately from the Relay for every Query packet off the
// wire (including the ones earlier plugins rejected).
func (p *Plugin) OnQuery(_ context.Context, _ *plugin.QueryContext) error {
	p.obs.QueryForwarded()
	return nil
}

// --- ExceptionPlugin ---
//
// Fired when Relay best-effort decodes an upstream Exception packet
// (see [pkg/proxy/relay.go] tryDecodeException).
// server_packets_total{type=Exception} is handled by the Relay's
// first-byte heuristic; this hook records the structured error so it
// shows up in clickhouse_proxy_errors_total{phase=upstream_exception}.
func (p *Plugin) OnException(_ context.Context, _ chsession.Session, exc *chproto.Exception) error {
	if exc != nil {
		p.obs.Error("upstream_exception", fmt.Errorf("code=%d %s: %s", exc.Code, exc.Name, exc.Message))
	}
	return nil
}

// RunOnRouted reports that metrics keep firing on routed (proxy-to-proxy)
// sessions. Operators want active-connection, handshake, and forwarded-
// query counters for routed traffic too — silence on those would make
// the routed path invisible to dashboards.
func (*Plugin) RunOnRouted() bool { return true }

var _ plugin.RouteAware = (*Plugin)(nil)
