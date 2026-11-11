package chgrants

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func engines(name string) bool { return name == "Memory" || name == "MergeTree" }

func devnetPolicy() Policy {
	return Policy{PhysicalDatabase: "phys", StorageIntegrity: true, ReplicaCopies: true, EngineAllowed: engines}
}

// gatewayGrants is `SHOW GRANTS FINAL` of a narrowed gateway user, measured on
// ClickHouse 26.8.36 (25.8.33 prints `ALTER` where 26.8 prints `ALTER TABLE,
// ALTER DATABASE, ALTER VIEW`; both are tested). `GRANT READ ON REMOTE`
// prints as `GRANT REMOTE ON *.*`.
var gatewayGrants = []string{
	"GRANT CREATE TEMPORARY TABLE ON *.* TO gw",
	"GRANT TABLE ENGINE ON Memory TO gw",
	"GRANT TABLE ENGINE ON MergeTree TO gw",
	"GRANT REMOTE ON *.* TO gw",
	"GRANT SELECT ON hg_safe.* TO gw",
	"GRANT SELECT ON hg_unsafe.* TO gw",
	"GRANT SELECT ON hg_replica.* TO gw",
	"GRANT SELECT, INSERT, ALTER TABLE, ALTER DATABASE, ALTER VIEW, CREATE DATABASE, CREATE TABLE, CREATE VIEW, DROP TABLE, DROP VIEW, TRUNCATE, OPTIMIZE ON phys.* TO gw",
	"GRANT SELECT, INSERT, ALTER, CREATE DATABASE, CREATE TABLE, CREATE VIEW, DROP TABLE, DROP VIEW, TRUNCATE, OPTIMIZE ON phys.* TO gw",
	"REVOKE INSERT ON phys.secret FROM gw",
	"GRANT SELECT ON system.processes TO gw",
	"GRANT SELECT(database, name) ON system.tables TO gw",
	"GRANT SELECT ON `phys`.`db1.t` TO gw",
}

func TestCheck_RoleWithoutAdminOptionPasses(t *testing.T) {
	// FINAL already lists the role's privileges; the role line itself is
	// only checked for WITH ADMIN OPTION.
	if v := Check(devnetPolicy(), []string{"GRANT pr2 TO gw", "GRANT SELECT(`a\\`b`, `c,d`) ON phys.t TO gw"}); len(v) != 0 {
		t.Fatalf("violations = %v, want none", v)
	}
}

func TestRoleLines(t *testing.T) {
	plain := []string{
		"GRANT SELECT ON phys.* TO gw",
		"GRANT pr2 TO gw",
		"GRANT pr TO gw WITH ADMIN OPTION",
		"REVOKE INSERT ON phys.secret FROM gw",
	}
	got := RoleLines(plain)
	if len(got) != 2 || got[0] != "GRANT pr2 TO gw" || got[1] != "GRANT pr TO gw WITH ADMIN OPTION" {
		t.Fatalf("RoleLines = %v", got)
	}
}

func TestCheck_NarrowedGatewayPasses(t *testing.T) {
	if v := Check(devnetPolicy(), gatewayGrants); len(v) != 0 {
		t.Fatalf("violations = %v, want none", v)
	}
}

func TestCheck_Violations(t *testing.T) {
	for _, tc := range []struct {
		name, line string
		policy     Policy
		want       []string // Violation.String() substrings, in order
	}{
		{"other database", "GRANT SELECT ON default.* TO gw", devnetPolicy(), []string{"SELECT ON default.*: database HouseGate does not serve"}},
		{"quoted other database", "GRANT SELECT ON `my-db`.* TO gw", devnetPolicy(), []string{"database HouseGate does not serve"}},
		{"external source", "GRANT URL ON *.* TO gw", devnetPolicy(), []string{"URL ON *.*: global privilege"}},
		{"file and s3", "GRANT FILE ON *.* TO gw", devnetPolicy(), []string{"FILE ON *.*: global privilege"}},
		{"sources", "GRANT SOURCES ON *.* TO gw", devnetPolicy(), []string{"SOURCES ON *.*"}},
		{"global select", "GRANT SELECT ON *.* TO gw", devnetPolicy(), []string{"SELECT ON *.*: global privilege"}},
		{"access management", "GRANT CREATE USER, ALTER USER, DROP USER ON * TO gw", devnetPolicy(),
			[]string{"CREATE USER ON *", "ALTER USER ON *", "DROP USER ON *"}},
		{"kill query and system", "GRANT KILL QUERY, SYSTEM ON *.* TO gw", devnetPolicy(), []string{"KILL QUERY", "SYSTEM ON *.*"}},
		{"grant option", "GRANT SELECT ON phys.t TO gw WITH GRANT OPTION", devnetPolicy(), []string{"WITH GRANT OPTION"}},
		{"database pattern", "GRANT SELECT ON ph*.* TO gw", devnetPolicy(), []string{"database or table pattern"}},
		{"table pattern", "GRANT SELECT ON phys.t* TO gw", devnetPolicy(), []string{"database or table pattern"}},
		{"write on system", "GRANT INSERT ON system.query_log TO gw", devnetPolicy(), []string{"non-read privilege on system"}},
		{"write on hg_safe", "GRANT INSERT ON hg_safe.* TO gw", devnetPolicy(), []string{"non-read privilege on a storage-integrity database"}},
		{"hg_promote is never served", "GRANT SELECT ON hg_promote.* TO gw", devnetPolicy(), []string{"database HouseGate does not serve"}},
		{"hg_safe without storage integrity", "GRANT SELECT ON hg_safe.* TO gw", Policy{PhysicalDatabase: "phys"}, []string{"database HouseGate does not serve"}},
		{"hg_replica without replica copies", "GRANT SELECT ON hg_replica.* TO gw",
			Policy{PhysicalDatabase: "phys", StorageIntegrity: true}, []string{"database HouseGate does not serve"}},
		{"refused engine", "GRANT TABLE ENGINE ON URL TO gw", devnetPolicy(), []string{"table engine the rewriter refuses"}},
		{"every engine", "GRANT TABLE ENGINE ON * TO gw", devnetPolicy(), []string{"table engine grant on every engine"}},
		{"empty physical database", "GRANT SELECT ON phys.* TO gw", Policy{}, []string{"database HouseGate does not serve"}},
		{"role with admin option", "GRANT r1 TO gw WITH ADMIN OPTION", devnetPolicy(), []string{"WITH ADMIN OPTION"}},
		// A quoted column name with a parenthesis or comma must not hide
		// the privilege after it (ClickHouse 25.8 output).
		{"privilege after a quoted parenthesis column", "GRANT SELECT(`(`), INSERT(x) ON hg_safe.t TO gw", devnetPolicy(),
			[]string{"INSERT ON hg_safe.t: non-read privilege on a storage-integrity database"}},
		{"privilege after a quoted comma column", "GRANT SELECT(`c,d`), INSERT ON system.t TO gw", devnetPolicy(),
			[]string{"INSERT ON system.t: non-read privilege on system"}},
		{"escaped backtick column", "GRANT SELECT(`a\\`b`), ALTER UPDATE(`)`) ON hg_unsafe.t TO gw", devnetPolicy(),
			[]string{"ALTER UPDATE ON hg_unsafe.t"}},
		{"escaped backtick database", "GRANT SELECT ON `we\\`ird`.* TO gw", devnetPolicy(), []string{"database HouseGate does not serve"}},
		{"unterminated quote", "GRANT SELECT(`a) ON phys.t TO gw", devnetPolicy(), []string{"unrecognised grant line"}},
		{"garbage", "SOMETHING ELSE", devnetPolicy(), []string{"unrecognised grant line"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := Check(tc.policy, []string{tc.line})
			if len(got) != len(tc.want) {
				t.Fatalf("violations = %v, want %d matching %v", got, len(tc.want), tc.want)
			}
			for i, w := range tc.want {
				if !strings.Contains(got[i].String(), w) {
					t.Errorf("violation %d = %q, want it to contain %q", i, got[i].String(), w)
				}
			}
		})
	}
}

// The default superuser (CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1) as ClickHouse
// 25.8 prints it: every line is a violation.
func TestCheck_Superuser(t *testing.T) {
	lines := []string{
		"GRANT CHECK, SHOW, SELECT, INSERT, ALTER, CREATE, DROP, UNDROP TABLE, TRUNCATE, OPTIMIZE, BACKUP, KILL QUERY, SYSTEM, dictGet, FILE, URL, REMOTE, S3, SOURCES ON *.* TO default WITH GRANT OPTION",
		"GRANT TABLE ENGINE ON * TO default WITH GRANT OPTION",
		"GRANT CREATE USER, ALTER USER, DROP USER, CREATE ROLE, ALTER ROLE, DROP ROLE ON * TO default WITH GRANT OPTION",
		"GRANT ALTER NAMED COLLECTION, CREATE NAMED COLLECTION, DROP NAMED COLLECTION, SHOW NAMED COLLECTIONS, NAMED COLLECTION ON * TO default WITH GRANT OPTION",
	}
	v := Check(devnetPolicy(), lines)
	if len(v) < 20 {
		t.Fatalf("violations = %d, want every privilege flagged", len(v))
	}
	for _, x := range v {
		if x.Privilege == "REMOTE" {
			t.Fatalf("REMOTE ON *.* is expected, got violation %v", x)
		}
	}
}

func TestConfig(t *testing.T) {
	for _, m := range []string{"", "warn", "enforce", "off"} {
		if err := (Config{Mode: m}).Validate(); err != nil {
			t.Errorf("mode %q: %v", m, err)
		}
	}
	if err := (Config{Mode: "observe"}).Validate(); err == nil {
		t.Error("mode observe accepted")
	}
	if (Config{}).EffectiveMode() != ModeWarn {
		t.Error("empty mode is not warn")
	}
}

func fixed(lines []string, err error) QueryFunc {
	return func(context.Context) ([]string, error) { return lines, err }
}

func TestChecker_Startup(t *testing.T) {
	bad := []string{"GRANT SELECT ON default.* TO gw"}
	down := errors.New("connection refused")
	for _, tc := range []struct {
		name    string
		mode    Mode
		query   QueryFunc
		wantErr bool
	}{
		{"warn logs only", ModeWarn, fixed(bad, nil), false},
		{"enforce refuses", ModeEnforce, fixed(bad, nil), true},
		{"enforce passes a narrowed user", ModeEnforce, fixed(gatewayGrants, nil), false},
		{"enforce does not refuse an unreachable upstream", ModeEnforce, fixed(nil, down), false},
		{"off runs nothing", ModeOff, func(context.Context) ([]string, error) { t.Fatal("queried in off mode"); return nil, nil }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addr := "ch-" + strings.ReplaceAll(tc.name, " ", "-")
			c := &Checker{Mode: tc.mode, Policy: devnetPolicy(), Upstreams: []Upstream{{Addr: addr, Query: tc.query}}}
			err := c.Startup(context.Background())
			if (err != nil) != tc.wantErr {
				t.Fatalf("Startup err = %v, want error %v", err, tc.wantErr)
			}
			if tc.wantErr && !errors.Is(err, ErrViolations) {
				t.Fatalf("err = %v, want ErrViolations", err)
			}
		})
	}
	c := &Checker{Mode: ModeWarn, Policy: devnetPolicy(), Upstreams: []Upstream{{Addr: "ch-metric", Query: fixed(bad, nil)}}}
	_ = c.Startup(context.Background())
	if got := testutil.ToFloat64(violationsGauge.WithLabelValues("ch-metric")); got != 1 {
		t.Fatalf("violations gauge = %v, want 1", got)
	}
	c = &Checker{Mode: ModeWarn, Upstreams: []Upstream{{Addr: "ch-down", Query: fixed(nil, down)}}}
	_ = c.Startup(context.Background())
	if got := testutil.ToFloat64(checkErrors.WithLabelValues("ch-down")); got != 1 {
		t.Fatalf("check errors = %v, want 1", got)
	}
}

func TestChecker_RunRechecksAndNeverFails(t *testing.T) {
	calls := make(chan struct{}, 8)
	c := &Checker{Mode: ModeEnforce, Policy: devnetPolicy(), Upstreams: []Upstream{{Addr: "ch-run", Query: func(context.Context) ([]string, error) {
		calls <- struct{}{}
		return []string{"GRANT SELECT ON default.* TO gw"}, nil
	}}}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { c.Run(ctx, 5e6); close(done) }()
	<-calls
	<-calls
	cancel()
	<-done
	if got := testutil.ToFloat64(violationsGauge.WithLabelValues("ch-run")); got != 1 {
		t.Fatalf("violations gauge = %v, want 1", got)
	}
}
