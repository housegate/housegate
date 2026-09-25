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
	if got, err := resolveAgentTableStatuses(Options{}, rpc); err != nil || got != registry.TableStatuses(rpc) {
		t.Fatalf("an RPC network state must answer statuses itself, got %T %v", got, err)
	}

	yaml := network.NewInMemoryNetworkState()
	yaml.TableSchemas["shop/orders@1"] = network.TableSchemaInfo{DatabaseId: "shop", TableId: "orders", Version: 1, SchemaHash: "0x1", SchemaJson: "{}"}
	got, err := resolveAgentTableStatuses(Options{}, yaml)
	if err != nil {
		t.Fatal(err)
	}
	if st, _ := got.StorageIntegrityTableStatus(context.Background(), "shop", "orders"); st.Status != registry.TableStatusActive {
		t.Fatalf("a declared YAML table must be active, got %+v", st)
	}
	if st, _ := got.StorageIntegrityTableStatus(context.Background(), "shop", "other"); st.Status != registry.TableStatusOrdinary {
		t.Fatalf("an undeclared YAML table must be ordinary, got %+v", st)
	}

	if _, err := resolveAgentTableStatuses(Options{StorageIntegrityTableSchemas: yaml}, registryOnly{network.NewInMemoryNetworkState()}); err != nil {
		t.Fatalf("host-injected schemas must satisfy the agent: %v", err)
	}
	if _, err := resolveAgentTableStatuses(Options{}, registryOnly{network.NewInMemoryNetworkState()}); err == nil || !strings.Contains(err.Error(), "requires a table status source") {
		t.Fatalf("err = %v, want the missing-source refusal", err)
	}
}
