// Package metricshttp serves housegate's Prometheus metrics — the global
// default registry's init()-registered series merged with the proxy's
// dedicated downstream-collector registry — over HTTP, plus an optional
// authenticated /debug/pprof. It is the single shared implementation used by
// both the standalone cmd/housegate binary and library hosts (e.g. sentio-node)
// that embed the proxy via housegate.New.
//
// Importing this package pulls in net/http/pprof, whose init() registers
// handlers on http.DefaultServeMux as a side effect. Handler never serves
// DefaultServeMux, so pprof endpoints are reachable only through the mux Handler
// builds (behind bearer-token auth when enabled) — provided the host process
// does not itself serve http.DefaultServeMux.
package metricshttp

import (
	"context"
	"crypto/subtle"
	"errors"
	"net/http"
	"net/http/pprof"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"housegate/housegate/pkg/config"
	"housegate/housegate/pkg/log"
)

// Start starts the HTTP server exposing /metrics (the existing
// init()-registered globals merged with the proxy's dedicated collector
// registry) and, when enabled, an authenticated /debug/pprof. It runs in the
// background and shuts down gracefully when ctx is cancelled.
func Start(ctx context.Context, addr string, reg *prometheus.Registry, pprofCfg config.PprofConfig) {
	srv := &http.Server{
		Addr:    addr,
		Handler: Handler(reg, pprofCfg),
	}
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Errorw("metrics server panic recovered", "panic", r)
			}
		}()
		log.Infow("metrics listening", "addr", addr, "pprof", pprofCfg.Enabled)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Errorw("metrics server error", "err", err)
		}
	}()
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()
}

// Handler builds the metrics/pprof HTTP handler. /metrics gathers the default
// registry (existing init() globals) merged with the proxy's dedicated
// collector registry via prometheus.Gatherers, so both old and new series
// appear in one scrape without a double-registration panic. pprof is mounted
// only when enabled, behind bearer-token auth.
func Handler(reg *prometheus.Registry, pprofCfg config.PprofConfig) http.Handler {
	mux := http.NewServeMux()

	gatherers := prometheus.Gatherers{prometheus.DefaultGatherer}
	if reg != nil {
		gatherers = append(gatherers, reg)
	}
	mux.Handle("/metrics", promhttp.HandlerFor(gatherers, promhttp.HandlerOpts{}))

	if pprofCfg.Enabled {
		pp := http.NewServeMux()
		pp.HandleFunc("/debug/pprof/", pprof.Index)
		pp.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		pp.HandleFunc("/debug/pprof/profile", pprof.Profile)
		pp.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		pp.HandleFunc("/debug/pprof/trace", pprof.Trace)
		mux.Handle("/debug/pprof/", pprofAuthMiddleware(pprofCfg.Token, pp))
	}
	return mux
}

// pprofAuthMiddleware gates next behind a bearer token compared in constant
// time. A missing/empty configured token, or any mismatch, returns 401.
func pprofAuthMiddleware(token string, next http.Handler) http.Handler {
	want := []byte(token)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got := []byte(bearerToken(r))
		// len(want)==0 cannot occur via the normal startup path — Config.Validate
		// rejects pprof.enabled with an empty token — but the guard is kept as
		// defense-in-depth so a directly-constructed middleware always rejects
		// rather than accidentally serving pprof unauthenticated.
		if len(want) == 0 || subtle.ConstantTimeCompare(got, want) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// bearerToken extracts the token from an "Authorization: Bearer <token>" header.
func bearerToken(r *http.Request) string {
	const prefix = "Bearer "
	h := r.Header.Get("Authorization")
	if strings.HasPrefix(h, prefix) {
		return strings.TrimPrefix(h, prefix)
	}
	return ""
}
