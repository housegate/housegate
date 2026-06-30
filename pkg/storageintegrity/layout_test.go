package storageintegrity

import "testing"

func TestNewTableLayoutDefaultUnsafeTableHasNoSuffix(t *testing.T) {
	layout := NewTableLayout(TableLayoutConfig{})

	if got, want := layout.UnsafeTable("realbin.t"), "`hg_unsafe`.`realbin.t`"; got != want {
		t.Fatalf("UnsafeTable() = %q, want %q", got, want)
	}
	if got, want := layout.SafeTable("realbin.t"), "`hg_safe`.`realbin.t`"; got != want {
		t.Fatalf("SafeTable() = %q, want %q", got, want)
	}
}
