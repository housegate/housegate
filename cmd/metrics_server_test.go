package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"housegate/housegate/pkg/config"
)

func TestPprofAuthMiddleware(t *testing.T) {
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})
	h := pprofAuthMiddleware("secret", next)

	cases := []struct {
		name   string
		header string
		want   int
		passed bool
	}{
		{"no token", "", http.StatusUnauthorized, false},
		{"wrong token", "Bearer nope", http.StatusUnauthorized, false},
		{"correct token", "Bearer secret", http.StatusOK, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			called = false
			req := httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil)
			if c.header != "" {
				req.Header.Set("Authorization", c.header)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != c.want {
				t.Errorf("status = %d, want %d", rec.Code, c.want)
			}
			if called != c.passed {
				t.Errorf("next called = %v, want %v", called, c.passed)
			}
		})
	}
}

func TestBuildMetricsHandlerMergesRegistries(t *testing.T) {
	reg := prometheus.NewRegistry()
	dedicated := prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_dedicated_series_xyz"})
	reg.MustRegister(dedicated)
	dedicated.Set(2)

	h := buildMetricsHandler(reg, config.PprofConfig{})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics status = %d", rec.Code)
	}
	body := rec.Body.String()
	// A known default-registry series (init-registered in pkg/proxy/observer.go,
	// imported transitively) proves the default gatherer is merged in.
	if !strings.Contains(body, "clickhouse_proxy_active_connections") {
		t.Error("/metrics missing default-registry (init globals) series")
	}
	if !strings.Contains(body, "test_dedicated_series_xyz") {
		t.Error("/metrics missing dedicated-registry series")
	}
}

func TestBuildMetricsHandlerNilRegistryStillServesDefault(t *testing.T) {
	h := buildMetricsHandler(nil, config.PprofConfig{})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics with nil registry status = %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "clickhouse_proxy_active_connections") {
		t.Error("/metrics should still serve default registry when proxy registry is nil")
	}
}

func TestBuildMetricsHandlerPprofGating(t *testing.T) {
	// Disabled → /debug/pprof not mounted.
	hOff := buildMetricsHandler(nil, config.PprofConfig{Enabled: false})
	rec := httptest.NewRecorder()
	hOff.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil))
	if rec.Code != http.StatusNotFound {
		t.Errorf("pprof disabled: status = %d, want 404", rec.Code)
	}

	hOn := buildMetricsHandler(nil, config.PprofConfig{Enabled: true, Token: "tok"})
	// Enabled, no token → 401.
	rec = httptest.NewRecorder()
	hOn.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("pprof enabled no token: status = %d, want 401", rec.Code)
	}
	// Enabled + token → 200 (pprof.Index).
	rec = httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil)
	req.Header.Set("Authorization", "Bearer tok")
	hOn.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("pprof enabled with token: status = %d, want 200", rec.Code)
	}
}
