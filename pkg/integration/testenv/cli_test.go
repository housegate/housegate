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
