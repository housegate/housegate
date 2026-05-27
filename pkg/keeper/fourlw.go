package keeper

import (
	"bufio"
	"context"
	"io"
	"net"
	"strings"
	"time"
)

// ServerState mirrors ClickHouse-Keeper's (ZooKeeper-compatible)
// zk_server_state mntr field.
type ServerState string

const (
	StateUnknown    ServerState = ""
	StateLeader     ServerState = "leader"
	StateFollower   ServerState = "follower"
	StateStandalone ServerState = "standalone"
	StateObserver   ServerState = "observer"
)

// fourLetter sends a ZooKeeper four-letter-word command (ruok, mntr, srvr,
// ...) to a keeper's client port and returns the full response. Keeper
// writes its reply and then closes the connection, so we read to EOF.
//
// The proxy never participates in Raft; 4LW is the read-only channel it
// uses to observe quorum liveness and roles.
func fourLetter(ctx context.Context, addr, word string, timeout time.Duration) (string, error) {
	d := net.Dialer{Timeout: timeout}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(timeout))
	if _, err := conn.Write([]byte(word)); err != nil {
		return "", err
	}
	b, err := io.ReadAll(conn)
	if err != nil && len(b) == 0 {
		return "", err
	}
	return string(b), nil
}

// probe reports liveness and role of a keeper via ruok + mntr. A node that
// answers ruok with "imok" is considered alive even if mntr does not yet
// report a stable zk_server_state (e.g. mid-election).
func probe(ctx context.Context, addr string, timeout time.Duration) (alive bool, state ServerState) {
	resp, err := fourLetter(ctx, addr, "ruok", timeout)
	if err != nil || !strings.HasPrefix(resp, "imok") {
		return false, StateUnknown
	}
	m, err := fourLetter(ctx, addr, "mntr", timeout)
	if err != nil {
		return true, StateUnknown
	}
	return true, parseServerState(m)
}

// parseServerState extracts zk_server_state from a tab-separated mntr dump.
func parseServerState(mntr string) ServerState {
	sc := bufio.NewScanner(strings.NewReader(mntr))
	for sc.Scan() {
		if k, v, ok := strings.Cut(sc.Text(), "\t"); ok && k == "zk_server_state" {
			return ServerState(strings.TrimSpace(v))
		}
	}
	return StateUnknown
}
