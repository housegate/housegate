package testenv

import (
	"net"
	"testing"
	"time"
)

// TestStartClickHouse verifies a single container comes up and its native
// TCP port is reachable.
func TestStartClickHouse(t *testing.T) {
	env := StartClickHouse(t)
	mustDial(t, env.Addr)
}

// TestStartClickHousePair verifies both containers come up, are reachable,
// and bind to distinct addresses (so they are genuinely independent
// upstreams rather than the same container surfaced twice).
func TestStartClickHousePair(t *testing.T) {
	a, b := StartClickHousePair(t)
	if a.Addr == b.Addr {
		t.Fatalf("pair returned the same addr twice: %s", a.Addr)
	}
	mustDial(t, a.Addr)
	mustDial(t, b.Addr)
}

func mustDial(t *testing.T, addr string) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	_ = conn.Close()
}
