package network

import (
	"context"
	"testing"

	"github.com/housegate/housegate/pkg/registry"
)

func TestInMemoryNetworkState_DatabaseHosting(t *testing.T) {
	var _ registry.DatabaseHosting = (*InMemoryNetworkState)(nil)
	ns := NewInMemoryNetworkState()
	ns.IndexerInfos[2] = IndexerInfo{IndexerId: 2, IndexerUrl: "10.0.0.2", ClickhouseProxyPort: 9000}
	ns.IndexerInfos[3] = IndexerInfo{IndexerId: 3, IndexerUrl: "10.0.0.3"}
	ns.DatabaseInfos["shop"] = DatabaseInfo{DatabaseId: "shop", IndexerId: 2}
	ns.DatabaseInfos["gone"] = DatabaseInfo{DatabaseId: "gone", IndexerId: 2, PendingDelete: true}
	ns.DatabaseInfos["portless"] = DatabaseInfo{DatabaseId: "portless", IndexerId: 3}
	ns.DatabaseInfos["orphan"] = DatabaseInfo{DatabaseId: "orphan", IndexerId: 9}
	ctx := context.Background()
	if addr, id, hosted, err := ns.DatabaseHosting(ctx, "shop"); err != nil || !hosted || id != 2 || addr.Addr() != "10.0.0.2:9000" {
		t.Fatalf("shop = %+v %d %v %v", addr, id, hosted, err)
	}
	for _, db := range []string{"nope", "gone"} {
		if _, _, hosted, err := ns.DatabaseHosting(ctx, db); err != nil || hosted {
			t.Fatalf("%s: hosted=%v err=%v; want not hosted, no error", db, hosted, err)
		}
	}
	for _, db := range []string{"portless", "orphan"} {
		if _, _, hosted, err := ns.DatabaseHosting(ctx, db); err == nil || hosted {
			t.Fatalf("%s: hosted=%v err=%v; want an error", db, hosted, err)
		}
	}
}
