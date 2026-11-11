package integration

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/chgrants"
	"github.com/housegate/housegate/pkg/integration/testenv"
	"github.com/housegate/housegate/pkg/plugins/tablerefguard"
)

// TestUpstreamGrantsCheck_RealClickHouse runs the upstream grants check
// (housegate/housegate#341) against the CI ClickHouse server: a user narrowed
// to what a storage-integrity gateway needs passes in enforce mode, and the
// same user with one more grant on `default` (directly, or through a role,
// which SHOW GRANTS FINAL expands), a url() source, a role held WITH ADMIN
// OPTION (directly or inherited through a parent role), a non-default role
// granting `default.*`, or a write behind a quoted column name refuses
// enforce-mode startup. A role held without the admin option passes. The superuser the other tests run as is reported, never refused,
// in warn mode.
func TestUpstreamGrantsCheck_RealClickHouse(t *testing.T) {
	ctx := context.Background()
	ch := testenv.StartClickHouseWithAccessManagement(t)
	admin := openConnNoDB(t, ch.Addr)
	exec := func(q string) {
		t.Helper()
		if err := admin.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	for _, q := range []string{
		"DROP USER IF EXISTS hg341_gw", "DROP USER IF EXISTS hg341_wide", "DROP USER IF EXISTS hg341_role_user", "DROP ROLE IF EXISTS hg341_role",
	} {
		exec(q)
	}
	t.Cleanup(func() {
		for _, q := range []string{"DROP USER IF EXISTS hg341_gw", "DROP USER IF EXISTS hg341_wide", "DROP USER IF EXISTS hg341_role_user", "DROP ROLE IF EXISTS hg341_role", "DROP USER IF EXISTS hg341_admin_user", "DROP USER IF EXISTS hg341_col", "DROP USER IF EXISTS hg341_nested", "DROP USER IF EXISTS hg341_nondefault", "DROP ROLE IF EXISTS hg341_parent", "DROP ROLE IF EXISTS hg341_nd", "DROP ROLE IF EXISTS hg341_phys_role"} {
			_ = admin.Exec(ctx, q)
		}
	})
	gateway := []string{
		"GRANT SELECT, INSERT, ALTER, CREATE TABLE, CREATE VIEW, DROP TABLE, DROP VIEW, TRUNCATE, OPTIMIZE ON phys_341.* TO %s",
		"GRANT CREATE DATABASE ON phys_341.* TO %s",
		"GRANT SELECT ON hg_safe.* TO %s",
		"GRANT SELECT ON hg_unsafe.* TO %s",
		"GRANT READ ON REMOTE TO %s",
		"GRANT CREATE TEMPORARY TABLE ON *.* TO %s",
		"GRANT SELECT ON system.processes TO %s",
		"GRANT TABLE ENGINE ON MergeTree TO %s",
	}
	for _, u := range []string{"hg341_gw", "hg341_wide", "hg341_role_user", "hg341_admin_user", "hg341_col", "hg341_nested", "hg341_nondefault"} {
		exec("CREATE USER " + u + " IDENTIFIED WITH plaintext_password BY 'pw341'")
		for _, g := range gateway {
			exec(strings.ReplaceAll(g, "%s", u))
		}
	}
	exec("GRANT SELECT ON default.* TO hg341_wide")
	exec("GRANT READ ON URL TO hg341_wide")
	exec("CREATE ROLE hg341_role")
	exec("GRANT SELECT ON default.* TO hg341_role")
	exec("GRANT hg341_role TO hg341_role_user")
	// A role whose privileges are all expected, held WITH ADMIN OPTION: FINAL
	// drops the role grant, so only the plain SHOW GRANTS line shows it.
	exec("CREATE ROLE hg341_phys_role")
	exec("GRANT SELECT ON phys_341.* TO hg341_phys_role")
	exec("GRANT hg341_phys_role TO hg341_admin_user WITH ADMIN OPTION")
	exec("GRANT hg341_phys_role TO hg341_gw")
	// The admin option inherited through a parent role.
	exec("CREATE ROLE hg341_parent")
	exec("GRANT hg341_phys_role TO hg341_parent WITH ADMIN OPTION")
	exec("GRANT hg341_parent TO hg341_nested")
	// A non-default role is expanded too.
	exec("CREATE ROLE hg341_nd")
	exec("GRANT SELECT ON default.* TO hg341_nd")
	exec("GRANT hg341_nd TO hg341_nondefault")
	exec("SET DEFAULT ROLE NONE TO hg341_nondefault")
	// A write hidden behind a quoted column name with a parenthesis.
	exec("GRANT SELECT(`(`), INSERT(a) ON hg_safe.t TO hg341_col")

	policy := chgrants.Policy{PhysicalDatabase: "phys_341", StorageIntegrity: true, EngineAllowed: tablerefguard.TableEngineGrantAllowed}
	check := func(mode chgrants.Mode, user, password string) error {
		c := &chgrants.Checker{Mode: mode, Policy: policy, Upstreams: []chgrants.Upstream{{
			Addr: ch.Addr, Query: chgrants.ShowGrants(ch.Addr, user, password)}}}
		return c.Startup(ctx)
	}
	if err := check(chgrants.ModeEnforce, "hg341_gw", "pw341"); err != nil {
		t.Fatalf("narrowed gateway user refused: %v", err)
	}
	for _, u := range []string{"hg341_wide", "hg341_role_user", "hg341_admin_user", "hg341_col", "hg341_nested", "hg341_nondefault"} {
		if err := check(chgrants.ModeEnforce, u, "pw341"); !errors.Is(err, chgrants.ErrViolations) {
			t.Fatalf("%s: err = %v, want ErrViolations", u, err)
		}
		if err := check(chgrants.ModeWarn, u, "pw341"); err != nil {
			t.Fatalf("%s in warn mode: %v", u, err)
		}
	}
	lines, err := chgrants.ShowGrants(ch.Addr, "hg341_wide", "pw341")(ctx)
	if err != nil {
		t.Fatal(err)
	}
	v := chgrants.Check(policy, lines)
	var got []string
	for _, x := range v {
		got = append(got, x.String())
	}
	if len(v) != 2 || !strings.Contains(strings.Join(got, "|"), "SELECT ON default.*") || !strings.Contains(strings.Join(got, "|"), "URL ON *.*") {
		t.Fatalf("violations = %v, want SELECT ON default.* and URL ON *.*", got)
	}
	// The superuser every other integration test forwards as.
	if err := check(chgrants.ModeWarn, ch.User, ch.Password); err != nil {
		t.Fatalf("warn mode refused the superuser: %v", err)
	}
	// A wrong password is an unreadable upstream: never fatal, even in enforce.
	if err := check(chgrants.ModeEnforce, "hg341_gw", "wrong"); err != nil {
		t.Fatalf("unreadable upstream refused enforce startup: %v", err)
	}
}
