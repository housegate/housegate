package config

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestObservabilityDefaults(t *testing.T) {
	cfg := Default()
	if !cfg.Observability.Collector.Enabled {
		t.Error("collector.enabled default = false, want true")
	}
	if got := cfg.Observability.Collector.Interval.Duration; got != 15*time.Second {
		t.Errorf("collector.interval default = %v, want 15s", got)
	}
	if got := cfg.Observability.Collector.PollTimeout.Duration; got != 5*time.Second {
		t.Errorf("collector.poll_timeout default = %v, want 5s", got)
	}
	if cfg.Observability.Pprof.Enabled {
		t.Error("pprof.enabled default = true, want false")
	}
}

func TestObservabilityValidatePprofTokenRequired(t *testing.T) {
	cfg := Default()
	cfg.Listen = ":9001"
	cfg.NetworkState.Source = "localhost:6379" // satisfy server-mode source rule
	cfg.Observability.Pprof.Enabled = true
	cfg.Observability.Pprof.Token = ""
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "pprof") {
		t.Errorf("expected pprof token validation error, got %v", err)
	}
	cfg.Observability.Pprof.Token = "secret"
	if err := cfg.Validate(); err != nil && strings.Contains(err.Error(), "pprof.token") {
		t.Errorf("unexpected pprof.token error with token set: %v", err)
	}
}

func TestObservabilityValidateNonPositiveInterval(t *testing.T) {
	cfg := Default()
	cfg.Listen = ":9001"
	cfg.NetworkState.Source = "localhost:6379"
	cfg.Observability.Collector.Enabled = true
	cfg.Observability.Collector.Interval = Duration{0}
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "collector.interval") {
		t.Errorf("expected collector.interval validation error, got %v", err)
	}
}

func TestObservabilityDisabledSkipsIntervalCheck(t *testing.T) {
	cfg := Default()
	cfg.Listen = ":9001"
	cfg.NetworkState.Source = "localhost:6379"
	cfg.Observability.Collector.Enabled = false
	cfg.Observability.Collector.Interval = Duration{0}
	if err := cfg.Validate(); err != nil && strings.Contains(err.Error(), "collector.interval") {
		t.Errorf("disabled collector should not trigger interval error: %v", err)
	}
}

func TestObservabilityJSONRoundTrip(t *testing.T) {
	cfg := Default()
	cfg.Observability.Collector.Interval = Duration{30 * time.Second}
	cfg.Observability.Collector.CHAddr = "127.0.0.1:9000"
	cfg.Observability.Collector.CHUser = "collector"
	cfg.Observability.Pprof.Enabled = true
	cfg.Observability.Pprof.Token = "tok"

	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var got Config
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.Observability.Collector.Interval.Duration != 30*time.Second {
		t.Errorf("interval round-trip = %v", got.Observability.Collector.Interval.Duration)
	}
	if got.Observability.Collector.CHAddr != "127.0.0.1:9000" {
		t.Errorf("ch_addr round-trip = %q", got.Observability.Collector.CHAddr)
	}
	if got.Observability.Collector.CHUser != "collector" {
		t.Errorf("ch_user round-trip = %q", got.Observability.Collector.CHUser)
	}
	if !got.Observability.Pprof.Enabled || got.Observability.Pprof.Token != "tok" {
		t.Errorf("pprof round-trip = %+v", got.Observability.Pprof)
	}
}
