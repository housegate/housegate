package rewriter

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// EngineNone labels a server that runs without a rewriter
// (rewriter.fail_open_on_unavailable with an unbuildable backend).
const EngineNone = "none"

// activeEngine is 1 for the engine this process rewrites with. configured is
// the operator's rewriter.engine ("grpc" for the empty value), engine the
// backend actually serving, version its release tag or "unknown".
var activeEngine = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "clickhouse_proxy_rewriter_engine",
	Help: "SQL rewriter engine selected at startup (value 1); configured is rewriter.engine, engine the backend serving.",
}, []string{"configured", "engine", "version"})

var registerActiveEngine sync.Once

// ReportActiveEngine replaces the active-engine series. Call it once the
// engine has passed its startup probes.
func ReportActiveEngine(configured, engine, version string) {
	registerActiveEngine.Do(func() { prometheus.MustRegister(activeEngine) })
	if configured == "" {
		configured = EngineGRPC
	}
	activeEngine.Reset()
	activeEngine.WithLabelValues(configured, engine, version).Set(1)
}

// Engine is the backend serving this factory: EngineGRPC or EngineNative.
// Under EngineAuto it is the engine chosen at construction.
func (f *SentioNetworkFactory) Engine() string { return f.engineName() }

// EngineVersion is the serving engine's release tag, or EngineVersionUnknown.
func (f *SentioNetworkFactory) EngineVersion() string {
	if f.engineVersion == "" {
		return EngineVersionUnknown
	}
	return f.engineVersion
}
