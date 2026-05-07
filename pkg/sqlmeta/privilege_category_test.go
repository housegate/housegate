package sqlmeta

import "testing"

func TestCategorizePrivilege(t *testing.T) {
	cases := []struct {
		in   string
		want PrivilegeCategory
	}{
		// ALL → all bits.
		{"ALL", PrivilegeCategoryAll},
		{"all", PrivilegeCategoryAll},
		{"ALL PRIVILEGES", PrivilegeCategoryAll},

		// Read.
		{"SELECT", PrivilegeCategoryRead},
		{"select", PrivilegeCategoryRead},
		{"  SELECT  ", PrivilegeCategoryRead},
		{"dictGet", PrivilegeCategoryRead},
		{"SHOW TABLES", PrivilegeCategoryRead},
		{"SHOW DATABASES", PrivilegeCategoryRead},
		{"SHOW CREATE TABLE", PrivilegeCategoryRead},
		{"EXISTS", PrivilegeCategoryRead},

		// Write.
		{"INSERT", PrivilegeCategoryWrite},
		{"OPTIMIZE", PrivilegeCategoryWrite},
		{"TRUNCATE", PrivilegeCategoryWrite},
		{"ALTER UPDATE", PrivilegeCategoryWrite},
		{"ALTER DELETE", PrivilegeCategoryWrite},

		// Admin (schema mutation).
		{"CREATE TABLE", PrivilegeCategoryAdmin},
		{"DROP TABLE", PrivilegeCategoryAdmin},
		{"CREATE DATABASE", PrivilegeCategoryAdmin},
		{"DROP DATABASE", PrivilegeCategoryAdmin},
		{"CREATE VIEW", PrivilegeCategoryAdmin},
		{"RENAME TABLE", PrivilegeCategoryAdmin},
		{"ALTER ADD COLUMN", PrivilegeCategoryAdmin},
		{"ALTER MODIFY ORDER BY", PrivilegeCategoryAdmin},
		{"ALTER MODIFY TTL", PrivilegeCategoryAdmin},

		// Unknown / empty.
		{"", PrivilegeCategoryNone},
		{"NEVER HEARD OF IT", PrivilegeCategoryNone},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got := CategorizePrivilege(tc.in)
			if got != tc.want {
				t.Errorf("CategorizePrivilege(%q) = %s (%d); want %s (%d)",
					tc.in, got, got, tc.want, tc.want)
			}
		})
	}
}

func TestCategorizePrivileges_OR(t *testing.T) {
	got := CategorizePrivileges([]string{"SELECT", "INSERT"})
	want := PrivilegeCategoryRead | PrivilegeCategoryWrite
	if got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func TestCategorizePrivileges_EmptyAndUnknown(t *testing.T) {
	if got := CategorizePrivileges(nil); got != PrivilegeCategoryNone {
		t.Errorf("nil input: got %s, want NONE", got)
	}
	if got := CategorizePrivileges([]string{"BOGUS"}); got != PrivilegeCategoryNone {
		t.Errorf("unknown only: got %s, want NONE", got)
	}
	// Mixing unknown with known surfaces only the known category.
	got := CategorizePrivileges([]string{"BOGUS", "SELECT"})
	if got != PrivilegeCategoryRead {
		t.Errorf("mixed: got %s, want READ", got)
	}
}

func TestPrivilegeCategory_HasAndString(t *testing.T) {
	rw := PrivilegeCategoryRead | PrivilegeCategoryWrite
	if !rw.Has(PrivilegeCategoryRead) {
		t.Errorf("READ|WRITE should Has(READ)")
	}
	if rw.Has(PrivilegeCategoryAdmin) {
		t.Errorf("READ|WRITE should NOT Has(ADMIN)")
	}
	if got := rw.String(); got != "READ|WRITE" {
		t.Errorf("String() = %q, want READ|WRITE", got)
	}
	if got := PrivilegeCategoryAll.String(); got != "ALL" {
		t.Errorf("ALL String() = %q, want ALL", got)
	}
	if got := PrivilegeCategoryNone.String(); got != "NONE" {
		t.Errorf("NONE String() = %q, want NONE", got)
	}
}
