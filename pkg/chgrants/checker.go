package chgrants

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/housegate/housegate/pkg/log"
)

// Mode is upstream_grants_check.mode.
type Mode string

const (
	// ModeWarn (the default, also when empty) checks at startup and every
	// RecheckInterval, logs every violation and publishes the metrics.
	ModeWarn Mode = "warn"
	// ModeEnforce is ModeWarn, except that a violation found by the startup
	// check refuses startup. A re-check never stops a running server.
	ModeEnforce Mode = "enforce"
	// ModeOff runs no check.
	ModeOff Mode = "off"
)

// RecheckInterval is how often a running server re-checks its upstream
// user's grants. A re-check only logs and updates the metrics.
const RecheckInterval = 10 * time.Minute

// Config is the operator surface (yaml `upstream_grants_check`).
type Config struct {
	// Mode is "warn" (the default, also when empty), "enforce" or "off".
	Mode string `json:"mode" yaml:"mode"`
	// ReplicaCopies declares that this server serves stage-3 replica copies,
	// so SELECT on hg_replica is expected (with storage integrity enabled).
	ReplicaCopies bool `json:"replica_copies" yaml:"replica_copies"`
}

// EffectiveMode resolves the empty default.
func (c Config) EffectiveMode() Mode {
	if c.Mode == "" {
		return ModeWarn
	}
	return Mode(c.Mode)
}

// Validate rejects an unknown mode.
func (c Config) Validate() error {
	switch c.EffectiveMode() {
	case ModeWarn, ModeEnforce, ModeOff:
		return nil
	}
	return fmt.Errorf("upstream_grants_check.mode %q is invalid (want %q, %q or %q)", c.Mode, ModeWarn, ModeEnforce, ModeOff)
}

var (
	violationsGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "clickhouse_proxy_upstream_grant_violations",
		Help: "Grants of the upstream ClickHouse user outside what HouseGate serves, from the last successful SHOW GRANTS FINAL check, by upstream address.",
	}, []string{"upstream"})
	checkErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "clickhouse_proxy_upstream_grant_check_errors_total",
		Help: "Upstream grant checks that could not read SHOW GRANTS FINAL, by upstream address.",
	}, []string{"upstream"})
)

func init() { prometheus.MustRegister(violationsGauge, checkErrors) }

// QueryFunc returns the lines of `SHOW GRANTS FINAL` run as the upstream
// user on one upstream.
type QueryFunc func(ctx context.Context) ([]string, error)

// Upstream is one ClickHouse address HouseGate forwards to.
type Upstream struct {
	Addr  string
	Query QueryFunc
}

// Checker checks every upstream against one policy.
type Checker struct {
	Mode      Mode
	Policy    Policy
	Upstreams []Upstream
	// Timeout bounds one upstream's query; zero means 10s.
	Timeout time.Duration
}

// ErrViolations is wrapped by the startup error in enforce mode.
var ErrViolations = errors.New("upstream ClickHouse user holds grants HouseGate does not serve")

// Startup runs the startup check. It returns an error only in enforce mode
// and only for violations: an upstream that cannot be queried is logged and
// counted, never fatal, so a ClickHouse that is briefly down does not keep
// HouseGate from starting.
func (c *Checker) Startup(ctx context.Context) error {
	if c == nil || c.Mode == ModeOff {
		return nil
	}
	var bad []string
	for _, u := range c.Upstreams {
		if n, ok := c.checkOne(ctx, u, "startup"); ok && n > 0 {
			bad = append(bad, fmt.Sprintf("%s: %d", u.Addr, n))
		}
	}
	if len(bad) > 0 && c.Mode == ModeEnforce {
		return fmt.Errorf("%w (%s); narrow the grants or set upstream_grants_check.mode: warn", ErrViolations, strings.Join(bad, ", "))
	}
	return nil
}

// Run re-checks every RecheckInterval until ctx is done. It never returns an
// error: a running server only logs and updates the metrics.
func (c *Checker) Run(ctx context.Context, interval time.Duration) {
	if c == nil || c.Mode == ModeOff || len(c.Upstreams) == 0 {
		return
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			for _, u := range c.Upstreams {
				c.checkOne(ctx, u, "recheck")
			}
		}
	}
}

func (c *Checker) checkOne(ctx context.Context, u Upstream, phase string) (int, bool) {
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	qctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	lines, err := u.Query(qctx)
	if err != nil {
		checkErrors.WithLabelValues(u.Addr).Inc()
		log.Warnw("upstream grants check: cannot read SHOW GRANTS FINAL", "upstream", u.Addr, "phase", phase, "err", err)
		return 0, false
	}
	violations := Check(c.Policy, lines)
	violationsGauge.WithLabelValues(u.Addr).Set(float64(len(violations)))
	if len(violations) == 0 {
		log.Infow("upstream grants check passed", "upstream", u.Addr, "phase", phase, "grant_lines", len(lines))
		return 0, true
	}
	list := make([]string, len(violations))
	for i, v := range violations {
		list[i] = v.String()
	}
	log.Warnw("upstream grants check: the upstream ClickHouse user holds grants HouseGate does not serve; every tenant, maintenance and peer session runs as this user",
		"upstream", u.Addr, "phase", phase, "mode", string(c.Mode), "violations", len(violations), "detail", list)
	return len(violations), true
}

// ShowGrants returns a QueryFunc that reads addr's grants as user: every
// line of `SHOW GRANTS FINAL`, which lists the privileges of the user's
// roles in place of the role grants, plus the role-grant lines of plain
// `SHOW GRANTS`, which FINAL drops and which alone carry WITH ADMIN OPTION.
// It opens a fresh native connection per check, so a check holds no
// connection between runs. The current user needs no privilege to read its
// own grants.
func ShowGrants(addr, user, password string) QueryFunc {
	return func(ctx context.Context) ([]string, error) {
		conn, err := clickhouse.Open(&clickhouse.Options{
			Addr:     []string{addr},
			Auth:     clickhouse.Auth{Username: user, Password: password},
			Protocol: clickhouse.Native,
		})
		if err != nil {
			return nil, fmt.Errorf("open: %w", err)
		}
		defer func() { _ = conn.Close() }()
		final, err := queryLines(ctx, conn, "SHOW GRANTS FINAL")
		if err != nil {
			return nil, err
		}
		plain, err := queryLines(ctx, conn, "SHOW GRANTS")
		if err != nil {
			return nil, err
		}
		return append(final, RoleLines(plain)...), nil
	}
}

// RoleLines keeps the role-grant lines (`GRANT <role> TO <user> …`) of plain
// SHOW GRANTS output.
func RoleLines(lines []string) []string {
	var out []string
	for _, l := range lines {
		if g, err := parseLine(strings.TrimSpace(l)); err == nil && g.role {
			out = append(out, l)
		}
	}
	return out
}

func queryLines(ctx context.Context, conn clickhouse.Conn, query string) ([]string, error) {
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var lines []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return nil, err
		}
		lines = append(lines, line)
	}
	return lines, rows.Err()
}
