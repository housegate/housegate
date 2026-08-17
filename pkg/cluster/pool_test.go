package cluster

import "testing"

func TestPooledConnUpstreamAddressUsesConfiguredReplica(t *testing.T) {
	conn := &PooledConn{replica: ReplicaConfig{Host: "clickhouse-1.local", Port: 9000}}
	if got := conn.UpstreamAddress(); got != "clickhouse-1.local:9000" {
		t.Fatalf("UpstreamAddress() = %q, want clickhouse-1.local:9000", got)
	}
}
