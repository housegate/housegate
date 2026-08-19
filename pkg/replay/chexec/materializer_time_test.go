package chexec

import (
	"testing"
	"time"
)

func TestMaterializerTemporalScalarReadBackTypes(t *testing.T) {
	shanghai, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		t.Fatalf("load non-UTC test location: %v", err)
	}
	instant := time.Date(2026, time.July, 16, 12, 34, 56, 123000000, shanghai)
	for _, typeName := range []string{
		"Date",
		"DateTime",
		"DateTime('UTC')",
		"DateTime64(3)",
		"DateTime64(3, 'UTC')",
	} {
		t.Run(typeName, func(t *testing.T) {
			if !supportedColumnType(typeName) {
				t.Fatalf("supportedColumnType(%q) = false", typeName)
			}
			dest, err := newScanDest(typeName)
			if err != nil {
				t.Fatalf("newScanDest(%q): %v", typeName, err)
			}
			p, ok := dest.(*time.Time)
			if !ok {
				t.Fatalf("newScanDest(%q) = %T, want *time.Time", typeName, dest)
			}
			*p = instant
			got, err := derefScan(typeName, dest)
			if err != nil {
				t.Fatalf("derefScan(%q): %v", typeName, err)
			}
			want := instant.UTC()
			if typeName == "Date" {
				want = time.Date(instant.Year(), instant.Month(), instant.Day(), 0, 0, 0, 0, time.UTC)
			}
			if got != want {
				t.Fatalf("derefScan(%q) = %v, want %v", typeName, got, want)
			}
		})
	}
}
