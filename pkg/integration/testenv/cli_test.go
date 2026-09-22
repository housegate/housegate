package testenv

import (
	"reflect"
	"testing"
)

func TestBuildCLIArgs_OmitsEmptyDatabase(t *testing.T) {
	got := buildCLIArgs("127.0.0.1", "9000", "", []string{"INSERT INTO db.t FORMAT CSV", "SELECT 42"}, "--multiquery")
	want := []string{
		"client",
		"--host", "127.0.0.1",
		"--port", "9000",
		"--multiquery",
		"--query", "INSERT INTO db.t FORMAT CSV",
		"--query", "SELECT 42",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("buildCLIArgs() = %q, want %q", got, want)
	}
}

func TestBuildCLIArgs_KeepsConfiguredDatabase(t *testing.T) {
	got := buildCLIArgs("127.0.0.1", "9000", "db", []string{"SELECT 1"})
	want := []string{
		"client",
		"--host", "127.0.0.1",
		"--port", "9000",
		"--database", "db",
		"--query", "SELECT 1",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("buildCLIArgs() = %q, want %q", got, want)
	}
}

func TestParseCLIVersion(t *testing.T) {
	for _, tc := range []struct {
		output       string
		major, minor int
		ok           bool
	}{
		{"ClickHouse client version 26.8.1.368 (official build).", 26, 8, true},
		{"ClickHouse client version 25.8.16.34 (official build).", 25, 8, true},
		{"ClickHouse client version 26.3.2.3.", 26, 3, true},
		{"unreadable", 0, 0, false},
	} {
		t.Run(tc.output, func(t *testing.T) {
			major, minor, ok := parseCLIVersion(tc.output)
			if major != tc.major || minor != tc.minor || ok != tc.ok {
				t.Fatalf("parseCLIVersion = %d.%d/%v, want %d.%d/%v", major, minor, ok, tc.major, tc.minor, tc.ok)
			}
		})
	}
}
