package metricshttp

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"housegate/housegate/pkg/config"
	// Imported for its init(), which registers the clickhouse_proxy_* series on
	// the default registry — the same path the standalone binary exercises — so
	// the merge assertions below observe real default-registry series.
	_ "housegate/housegate/pkg/proxy"
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

	h := Handler(reg, config.PprofConfig{})
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
	h := Handler(nil, config.PprofConfig{})
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
	hOff := Handler(nil, config.PprofConfig{Enabled: false})
	rec := httptest.NewRecorder()
	hOff.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil))
	if rec.Code != http.StatusNotFound {
		t.Errorf("pprof disabled: status = %d, want 404", rec.Code)
	}

	hOn := Handler(nil, config.PprofConfig{Enabled: true, Token: "tok"})
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

func TestStartMetricsServerServesAndShutsDown(t *testing.T) {
	// Grab a free loopback port, then release it for the server to bind.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	Start(ctx, addr, nil, config.PprofConfig{})

	// Poll until the background server is accepting.
	var resp *http.Response
	for i := 0; i < 100; i++ {
		resp, err = http.Get("http://" + addr + "/metrics")
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("metrics server never came up at %s: %v", addr, err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("/metrics status = %d, want 200", resp.StatusCode)
	}

	// Cancel → graceful shutdown; poll until it stops accepting.
	cancel()
	stopped := false
	for i := 0; i < 150; i++ {
		if _, err := http.Get("http://" + addr + "/metrics"); err != nil {
			stopped = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !stopped {
		t.Error("metrics server did not shut down after ctx cancel")
	}
}
