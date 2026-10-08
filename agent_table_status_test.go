package housegate

import (
	"context"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/registry"
)

func TestResolveAgentTableStatuses(t *testing.T) {
	rpc, err := network.NewRpcNetworkState("http://127.0.0.1:1", network.RpcOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got, fromRegistry, err := resolveAgentTableStatuses(Options{}, rpc); err != nil || !fromRegistry || got != registry.TableStatuses(rpc) {
		t.Fatalf("an RPC network state must answer statuses itself, got %T fromRegistry=%v %v", got, fromRegistry, err)
	}

	yaml := network.NewInMemoryNetworkState()
	yaml.TableSchemas["shop/orders@1"] = network.TableSchemaInfo{DatabaseId: "shop", TableId: "orders", Version: 1, SchemaHash: "0x1", SchemaJson: "{}"}
	got, fromRegistry, err := resolveAgentTableStatuses(Options{}, yaml)
	if err != nil || fromRegistry {
		t.Fatalf("declared schemas are not the registry's own statuses: fromRegistry=%v err=%v", fromRegistry, err)
	}
	if st, _ := got.StorageIntegrityTableStatus(context.Background(), "shop", "orders"); st.Status != registry.TableStatusActive {
		t.Fatalf("a declared YAML table must be active, got %+v", st)
	}
	if st, _ := got.StorageIntegrityTableStatus(context.Background(), "shop", "other"); st.Status != registry.TableStatusOrdinary {
		t.Fatalf("an undeclared YAML table must be ordinary, got %+v", st)
	}

	if _, _, err := resolveAgentTableStatuses(Options{StorageIntegrityTableSchemas: yaml}, registryOnly{network.NewInMemoryNetworkState()}); err != nil {
		t.Fatalf("host-injected schemas must satisfy the agent: %v", err)
	}
	// Injected schemas win even over a registry that answers statuses.
	if _, fromRegistry, err := resolveAgentTableStatuses(Options{StorageIntegrityTableSchemas: yaml}, rpc); err != nil || fromRegistry {
		t.Fatalf("host-injected schemas must satisfy the agent: fromRegistry=%v err=%v", fromRegistry, err)
	}
	if _, _, err := resolveAgentTableStatuses(Options{}, registryOnly{network.NewInMemoryNetworkState()}); err == nil || !strings.Contains(err.Error(), "requires a table status source") {
		t.Fatalf("err = %v, want the missing-source refusal", err)
	}
}
