package commitgate

import (
	"context"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/sqlmeta"
)

func nameEvent(name string) *Event {
	return &Event{
		Type: sqlmeta.StatementTypeCreateDatabase,
		User: "alice",
		AccessedTables: []sqlmeta.AccessedTable{
			{OriginalDatabase: name, LogicalDatabase: name},
		},
	}
}

func granteeEvent(action sqlmeta.PrivilegeAction, names ...string) *Event {
	stmtType := sqlmeta.StatementTypeGrant
	if action == sqlmeta.PrivilegeActionRevoke {
		stmtType = sqlmeta.StatementTypeRevoke
	}
	grantees := make([]sqlmeta.PrivilegeGrantee, 0, len(names))
	for _, n := range names {
		if n == "CURRENT_USER" {
			grantees = append(grantees, sqlmeta.PrivilegeGrantee{IsCurrentUser: true})
			continue
		}
		grantees = append(grantees, sqlmeta.PrivilegeGrantee{Name: n})
	}
	return &Event{
		Type: stmtType,
		User: "alice",
		AccessedTables: []sqlmeta.AccessedTable{
			{LogicalDatabase: "foo", OriginalTable: "t"},
		},
		PrivilegesDeltas: []sqlmeta.PrivilegeDelta{{
			Action:          action,
			Scope:           sqlmeta.PrivilegeScopeTable,
			LogicalDatabase: "foo",
			OriginalTable:   "t",
			Privileges:      []string{"SELECT"},
			Grantees:        grantees,
		}},
	}
}

func TestArgsValidator_Subscription(t *testing.T) {
	o := NewArgsValidatorObserver()
	subs := o.SubscribedTypes()
	want := map[sqlmeta.StatementType]bool{
		sqlmeta.StatementTypeCreateDatabase: true,
		sqlmeta.StatementTypeGrant:          true,
		sqlmeta.StatementTypeRevoke:         true,
	}
	if len(subs) != len(want) {
		t.Fatalf("expected %d subscriptions, got %v", len(want), subs)
	}
	for _, s := range subs {
		if !want[s] {
			t.Errorf("unexpected subscription %s", s)
		}
	}
}

func TestArgsValidator_AcceptsValidDatabaseNames(t *testing.T) {
	o := NewArgsValidatorObserver()
	for _, name := range []string{
		"a",           // single letter
		"foo",         // simple
		"Foo",         // capital
		"foo123",      // letters + digits
		"foo_bar",     // single underscore
		"foo_bar_baz", // multiple segments
		"a1_b2_c3",    // alphanumeric segments
		"abcdefghijklmnopqrstuvwxyz0123456789_ABCDEFGHIJKLMNOPQR", // 55 chars
		strings.Repeat("a", 63),                                   // exactly 63 chars
	} {
		t.Run(name, func(t *testing.T) {
			if err := o.BeforeStatement(context.Background(), nameEvent(name)); err != nil {
				t.Fatalf("expected %q to pass, got %v", name, err)
			}
		})
	}
}

func TestArgsValidator_RejectsInvalidDatabaseNames(t *testing.T) {
	o := NewArgsValidatorObserver()
	cases := []struct {
		name   string
		reason string
	}{
		{"", "empty"},
		{"1foo", "leading-digit"},
		{"_foo", "leading-underscore"},
		{"foo_", "trailing-underscore"},
		{"foo__bar", "double-underscore"},
		{"foo-bar", "hyphen"},
		{"foo.bar", "dot"},
		{"foo bar", "space"},
		{"foo$", "dollar"},
		{"测试", "non-ascii"},
		{strings.Repeat("a", 64), "too-long"},
	}
	for _, tc := range cases {
		t.Run(tc.reason, func(t *testing.T) {
			err := o.BeforeStatement(context.Background(), nameEvent(tc.name))
			if err == nil {
				t.Fatalf("expected %q to be rejected (%s)", tc.name, tc.reason)
			}
			if !strings.Contains(err.Error(), "name:") {
				t.Errorf("error should be prefixed with 'name:', got %v", err)
			}
		})
	}
}

func TestArgsValidator_FallsBackToOriginalDatabase(t *testing.T) {
	o := NewArgsValidatorObserver()
	// LogicalDatabase empty (e.g. unmapped CREATE) — still validate
	// using OriginalDatabase rather than passing through.
	ev := &Event{
		Type:           sqlmeta.StatementTypeCreateDatabase,
		User:           "alice",
		AccessedTables: []sqlmeta.AccessedTable{{OriginalDatabase: "1bad"}},
	}
	if err := o.BeforeStatement(context.Background(), ev); err == nil {
		t.Fatal("expected rejection on OriginalDatabase fallback, got nil")
	}
}

func TestArgsValidator_RejectsEmptyAccessedTablesForCreateDatabase(t *testing.T) {
	o := NewArgsValidatorObserver()
	ev := &Event{Type: sqlmeta.StatementTypeCreateDatabase, User: "alice"}
	err := o.BeforeStatement(context.Background(), ev)
	if err == nil || !strings.Contains(err.Error(), "no target") {
		t.Fatalf("expected no-target rejection, got %v", err)
	}
}

func TestArgsValidator_AcceptsValidGrantees(t *testing.T) {
	o := NewArgsValidatorObserver()
	addr0 := "0x0000000000000000000000000000000000000000"
	addr1 := "0xabcdef0123456789abcdef0123456789abcdef01"
	addr2 := "0xABCDEF0123456789ABCDEF0123456789ABCDEF01"
	addrNoPrefix := "abcdef0123456789abcdef0123456789abcdef01"
	cases := []struct {
		name     string
		grantees []string
	}{
		{"full_zero", []string{addr0}},
		{"full_lower", []string{addr1}},
		{"full_upper", []string{addr2}},
		{"no_prefix", []string{addrNoPrefix}},
		{"current_user_only", []string{"CURRENT_USER"}},
		{"mix_address_and_current_user", []string{addr1, "CURRENT_USER"}},
		{"multiple_addresses", []string{addr0, addr1, addr2}},
	}
	for _, tc := range cases {
		t.Run("grant_"+tc.name, func(t *testing.T) {
			ev := granteeEvent(sqlmeta.PrivilegeActionGrant, tc.grantees...)
			if err := o.BeforeStatement(context.Background(), ev); err != nil {
				t.Fatalf("expected %v to pass, got %v", tc.grantees, err)
			}
		})
		t.Run("revoke_"+tc.name, func(t *testing.T) {
			ev := granteeEvent(sqlmeta.PrivilegeActionRevoke, tc.grantees...)
			if err := o.BeforeStatement(context.Background(), ev); err != nil {
				t.Fatalf("expected %v to pass, got %v", tc.grantees, err)
			}
		})
	}
}

func TestArgsValidator_RejectsInvalidGrantees(t *testing.T) {
	o := NewArgsValidatorObserver()
	full := "0x" + strings.Repeat("a", 40)
	cases := []struct {
		reason   string
		grantees []string
	}{
		{"short_with_prefix", []string{"0x123"}},
		{"short_no_prefix", []string{"123"}},
		{"non_hex_body", []string{"0x" + strings.Repeat("z", 40)}},
		{"empty_body", []string{"0x"}},
		{"too_long", []string{"0x" + strings.Repeat("a", 41)}},
		{"plain_username", []string{"alice"}},
		{"empty_string", []string{""}},
		{"second_grantee_invalid", []string{full, "bob"}},
	}
	for _, tc := range cases {
		t.Run("grant_"+tc.reason, func(t *testing.T) {
			ev := granteeEvent(sqlmeta.PrivilegeActionGrant, tc.grantees...)
			err := o.BeforeStatement(context.Background(), ev)
			if err == nil {
				t.Fatalf("expected rejection for grantees %v", tc.grantees)
			}
			if !strings.Contains(err.Error(), "name:") {
				t.Errorf("error should be prefixed with 'name:', got %v", err)
			}
		})
	}
}

func TestArgsValidator_RejectsEmptyDeltasForGrant(t *testing.T) {
	o := NewArgsValidatorObserver()
	ev := &Event{Type: sqlmeta.StatementTypeGrant, User: "alice"}
	err := o.BeforeStatement(context.Background(), ev)
	if err == nil || !strings.Contains(err.Error(), "no privilege deltas") {
		t.Fatalf("expected no-deltas rejection, got %v", err)
	}
}

func TestArgsValidator_RejectsEmptyGranteeList(t *testing.T) {
	o := NewArgsValidatorObserver()
	ev := &Event{
		Type: sqlmeta.StatementTypeRevoke,
		User: "alice",
		PrivilegesDeltas: []sqlmeta.PrivilegeDelta{{
			Action:     sqlmeta.PrivilegeActionRevoke,
			Privileges: []string{"SELECT"},
			// Grantees deliberately empty.
		}},
	}
	err := o.BeforeStatement(context.Background(), ev)
	if err == nil || !strings.Contains(err.Error(), "empty grantee list") {
		t.Fatalf("expected empty-grantee rejection, got %v", err)
	}
}

func TestNormalizeEthereumAddress(t *testing.T) {
	zero := strings.Repeat("0", 40)
	mixed := "ABCDef0123456789ABCDef0123456789ABCDef01"
	mixedLower := strings.ToLower(mixed)
	cases := []struct {
		in   string
		want string
	}{
		{"0x" + zero, "0x" + zero},
		{"0X" + mixed, "0x" + mixedLower},
		{"0x" + mixed, "0x" + mixedLower},
		{mixed, "0x" + mixedLower}, // no-prefix accepted
		{"0x" + strings.Repeat("F", 40), "0x" + strings.Repeat("f", 40)},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got, err := NormalizeEthereumAddress(tc.in)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("NormalizeEthereumAddress(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestNormalizeEthereumAddress_Errors(t *testing.T) {
	cases := []string{
		"",
		"0x",
		"0x123", // short
		"123",   // short without prefix
		"alice",
		"0x" + strings.Repeat("z", 40),  // non-hex body
		"0x" + strings.Repeat("a", 41),  // too long
		strings.Repeat("a", 41),         // too long without prefix
		" 0x" + strings.Repeat("a", 40), // leading whitespace
	}
	for _, in := range cases {
		t.Run(in, func(t *testing.T) {
			if _, err := NormalizeEthereumAddress(in); err == nil {
				t.Fatalf("expected error for %q, got nil", in)
			}
		})
	}
}
