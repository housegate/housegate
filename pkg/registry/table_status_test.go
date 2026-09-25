package registry

import (
	"context"
	"testing"
)

type mapSchemas map[string]TableSchema

func (m mapSchemas) TableSchema(db, table string, _ uint32) (TableSchema, bool) {
	s, ok := m[db+"/"+table]
	return s, ok
}

func (m mapSchemas) LatestTableSchema(db, table string) (TableSchema, bool) {
	s, ok := m[db+"/"+table]
	return s, ok
}

func TestTableStatusesFromSchemas(t *testing.T) {
	src := TableStatusesFromSchemas(mapSchemas{"shop/orders": {DatabaseId: "shop", TableId: "orders", Version: 2, SchemaHash: "0xabc", SchemaJson: `{"table_id":"shop.orders"}`}})
	got, err := src.StorageIntegrityTableStatus(context.Background(), "shop", "orders")
	if err != nil || got.Status != TableStatusActive || got.SchemaHash != "0xabc" || got.SchemaJSON != `{"table_id":"shop.orders"}` {
		t.Fatalf("declared = %+v, %v; want active with the latest declaration", got, err)
	}
	got, err = src.StorageIntegrityTableStatus(context.Background(), "shop", "other")
	if err != nil || got.Status != TableStatusOrdinary || got.SchemaJSON != "" {
		t.Fatalf("undeclared = %+v, %v; want ordinary", got, err)
	}
}
