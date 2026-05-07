package cluster

import (
	"encoding/json"
	"fmt"
	"time"
)

// ShardConfig describes a single shard that this proxy instance manages.
// Each proxy instance is bound to exactly one shard containing multiple replicas.
type ShardConfig struct {
	Name     string          `json:"name" yaml:"name"`
	Replicas []ReplicaConfig `json:"replicas" yaml:"replicas"`
	Settings map[string]string `json:"settings,omitempty" yaml:"settings,omitempty"`
}

// ReplicaConfig describes a single ClickHouse replica within a shard.
type ReplicaConfig struct {
	Host     string `json:"host" yaml:"host"`
	Port     int    `json:"port" yaml:"port"`
	Weight   int    `json:"weight" yaml:"weight"`         // for weighted routing among replicas
	IsBackup bool   `json:"is_backup" yaml:"is_backup"`   // standby replica, only used when all primary replicas are down
}

// Addr returns the "host:port" address string.
func (r ReplicaConfig) Addr() string {
	return fmt.Sprintf("%s:%d", r.Host, r.Port)
}

// HealthCheckConfig controls replica health checking behavior.
type HealthCheckConfig struct {
	Interval          Duration `json:"interval" yaml:"interval"`                       // probe interval, default 5s
	Timeout           Duration `json:"timeout" yaml:"timeout"`                         // probe timeout, default 3s
	FailureThreshold  int      `json:"failure_threshold" yaml:"failure_threshold"`     // consecutive failures to mark unhealthy, default 3
	RecoveryThreshold int      `json:"recovery_threshold" yaml:"recovery_threshold"`   // consecutive successes to mark healthy, default 2
}

// DefaultHealthCheckConfig returns a HealthCheckConfig with sane defaults.
func DefaultHealthCheckConfig() HealthCheckConfig {
	return HealthCheckConfig{
		Interval:          Duration{5 * time.Second},
		Timeout:           Duration{3 * time.Second},
		FailureThreshold:  3,
		RecoveryThreshold: 2,
	}
}

// PoolConfig controls per-replica connection pool behavior.
type PoolConfig struct {
	MaxIdleConns    int      `json:"max_idle" yaml:"max_idle"`           // per-replica idle connections, default 5
	MaxOpenConns    int      `json:"max_open" yaml:"max_open"`           // per-replica max connections, default 50
	ConnMaxLifetime Duration `json:"max_lifetime" yaml:"max_lifetime"`   // max connection age, default 1h
	ConnMaxIdleTime Duration `json:"max_idle_time" yaml:"max_idle_time"` // max idle time before close, default 10m
	DialTimeout     Duration `json:"dial_timeout" yaml:"dial_timeout"`   // dial timeout, default 5s
}

// DefaultPoolConfig returns a PoolConfig with sane defaults.
func DefaultPoolConfig() PoolConfig {
	return PoolConfig{
		MaxIdleConns:    5,
		MaxOpenConns:    50,
		ConnMaxLifetime: Duration{1 * time.Hour},
		ConnMaxIdleTime: Duration{10 * time.Minute},
		DialTimeout:     Duration{5 * time.Second},
	}
}

// RoutingConfig controls replica selection strategy within a shard.
type RoutingConfig struct {
	Strategy string `json:"strategy" yaml:"strategy"` // round-robin | random | least-conn | weighted
}

// DefaultRoutingConfig returns a RoutingConfig with the default strategy.
func DefaultRoutingConfig() RoutingConfig {
	return RoutingConfig{Strategy: "round-robin"}
}

// Duration wraps time.Duration for JSON/YAML human-friendly strings (e.g. "5s").
// This mirrors the Duration type in pkg/proxy/config.go.
type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		return nil
	}
	var s string
	if err := json.Unmarshal(b, &s); err == nil {
		dur, err := time.ParseDuration(s)
		if err != nil {
			return fmt.Errorf("invalid duration %q: %w", s, err)
		}
		d.Duration = dur
		return nil
	}
	var n int64
	if err := json.Unmarshal(b, &n); err == nil {
		d.Duration = time.Duration(n)
		return nil
	}
	return fmt.Errorf("duration must be a string (e.g. \"5s\") or number of nanoseconds")
}

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.Duration.String())
}

func (d *Duration) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var s string
	if err := unmarshal(&s); err == nil {
		dur, err := time.ParseDuration(s)
		if err != nil {
			return fmt.Errorf("invalid duration %q: %w", s, err)
		}
		d.Duration = dur
		return nil
	}
	var n int64
	if err := unmarshal(&n); err == nil {
		d.Duration = time.Duration(n)
		return nil
	}
	return fmt.Errorf("duration must be a string (e.g. \"5s\") or number of nanoseconds")
}

func (d Duration) MarshalYAML() (interface{}, error) {
	return d.Duration.String(), nil
}
