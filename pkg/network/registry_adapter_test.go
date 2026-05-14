package network_test

import (
	"testing"

	"sentioxyz/sentio-core/network/state"

	"housegate/housegate/pkg/network"
)

// TestRegistryAdapter_ProcessorId verifies the DatabaseMetadata seed
// method. No caller reads ProcessorId in the proxy chain today; the
// test exists so the reservation is at least behaviour-covered.
func TestRegistryAdapter_ProcessorId(t *testing.T) {
	st := network.NewInMemoryNetworkState()
	st.DatabaseInfos["owned"] = state.DatabaseInfo{
		DatabaseId:  "owned",
		ProcessorId: "proc-1",
	}
	st.DatabaseInfos["userdb"] = state.DatabaseInfo{
		DatabaseId: "userdb", // no ProcessorId — user database
	}

	adapter := network.NewRegistryAdapter(st)

	if pid, ok := adapter.ProcessorId("owned"); !ok || pid != "proc-1" {
		t.Errorf("ProcessorId(owned) = %q, %v; want (\"proc-1\", true)", pid, ok)
	}
	if pid, ok := adapter.ProcessorId("userdb"); ok || pid != "" {
		t.Errorf("ProcessorId(userdb) = %q, %v; want (\"\", false) for empty ProcessorId", pid, ok)
	}
	if pid, ok := adapter.ProcessorId("missing"); ok || pid != "" {
		t.Errorf("ProcessorId(missing) = %q, %v; want (\"\", false) for unknown database", pid, ok)
	}
}
