package proxy

import (
	"errors"
	"io"
	"net"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	activeConnections = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "clickhouse_proxy_active_connections",
		Help: "Number of currently active client connections",
	})
	packetsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "clickhouse_proxy_packets_total",
		Help: "Total count of ClickHouse protocol packets processed (client -> server)",
	}, []string{"type"})
	bytesTransferred = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "clickhouse_proxy_bytes_transferred_total",
		Help: "Total bytes transferred through the proxy",
	}, []string{"direction"})
	queriesForwarded = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "clickhouse_proxy_queries_forwarded_total",
		Help: "Total number of Query packets successfully written to upstream",
	})
	serverPacketsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "clickhouse_proxy_server_packets_total",
		Help: "Total count of ClickHouse protocol packets processed (server -> client)",
	}, []string{"type"})
	errorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "clickhouse_proxy_errors_total",
		Help: "Total count of errors encountered",
	}, []string{"type", "error"})

	rewriteDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "clickhouse_proxy_rewrite_duration_seconds",
		Help:    "Time spent on SQL rewriting via gRPC service",
		Buckets: prometheus.DefBuckets,
	})
	streamingDataBlocksTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "clickhouse_proxy_streaming_data_blocks_total",
		Help: "Total count of Data/Scalar blocks processed in streaming mode",
	}, []string{"mode"})
	handshakeDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "clickhouse_proxy_handshake_duration_seconds",
		Help:    "Time spent on Hello/ServerHello/Addendum handshake",
		Buckets: prometheus.DefBuckets,
	})

	sidecarTokensInjected = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "clickhouse_proxy_sidecar_tokens_injected_total",
		Help: "Total JWS tokens injected by sidecar proxy",
	})
	sidecarTokenErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "clickhouse_proxy_sidecar_token_errors_total",
		Help: "Total JWS token signing errors in sidecar mode",
	})
	sidecarBootstrapFallbackTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "clickhouse_proxy_sidecar_bootstrap_fallback_total",
		Help: "Total sidecar upstream picks that fell back to the bootstrap tier (account had no permissioned indexer)",
	})
)

func init() {
	prometheus.MustRegister(activeConnections)
	prometheus.MustRegister(packetsTotal)
	prometheus.MustRegister(bytesTransferred)
	prometheus.MustRegister(queriesForwarded)
	prometheus.MustRegister(serverPacketsTotal)
	prometheus.MustRegister(errorsTotal)
	prometheus.MustRegister(rewriteDuration)
	prometheus.MustRegister(streamingDataBlocksTotal)
	prometheus.MustRegister(handshakeDuration)
	prometheus.MustRegister(sidecarTokensInjected)
	prometheus.MustRegister(sidecarTokenErrors)
	prometheus.MustRegister(sidecarBootstrapFallbackTotal)
}

type MetricsObserver struct{}

func NewMetricsObserver() *MetricsObserver {
	return &MetricsObserver{}
}

func (m *MetricsObserver) ConnectionOpened() { activeConnections.Inc() }
func (m *MetricsObserver) ConnectionClosed() { activeConnections.Dec() }

func (m *MetricsObserver) ClientPacket(pktType string) {
	packetsTotal.WithLabelValues(pktType).Inc()
}

func (m *MetricsObserver) ServerPacket(pktType string) {
	serverPacketsTotal.WithLabelValues(pktType).Inc()
}

func (m *MetricsObserver) BytesTransferred(direction string, bytes float64) {
	bytesTransferred.WithLabelValues(direction).Add(bytes)
}

func (m *MetricsObserver) QueryForwarded() { queriesForwarded.Inc() }

func (m *MetricsObserver) Error(phase string, err error) {
	if err == nil {
		return
	}
	errorsTotal.WithLabelValues(phase, classifyError(err)).Inc()
}

func classifyError(err error) string {
	if err == nil {
		return "none"
	}
	if errors.Is(err, io.EOF) {
		return "eof"
	}
	if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
		return "timeout"
	}
	if errors.Is(err, net.ErrClosed) {
		return "closed"
	}
	return "other"
}

func (m *MetricsObserver) Rewritten(duration float64) {
	rewriteDuration.Observe(duration)
}

func (m *MetricsObserver) StreamingDataBlock(mode string) {
	streamingDataBlocksTotal.WithLabelValues(mode).Inc()
}

func (m *MetricsObserver) HandshakeCompleted(duration float64) {
	handshakeDuration.Observe(duration)
}

func (m *MetricsObserver) SidecarTokenInjected()      { sidecarTokensInjected.Inc() }
func (m *MetricsObserver) SidecarTokenError()         { sidecarTokenErrors.Inc() }
func (m *MetricsObserver) SidecarBootstrapFallback()  { sidecarBootstrapFallbackTotal.Inc() }
