package registry

import (
	"context"
	"encoding/json"
	"strings"
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

// TestTableStatusReplicaFieldsAreOmittedForOtherStatuses is clarification
// K45 of Plan S3-C4 (spec 2026-10-14 §11): the four Replica fields are
// additive. Every other status encodes byte-identically to before, an older
// server's answer decodes them as absent, and a replica answer carries them,
// owner indexer 0 included.
func TestTableStatusReplicaFieldsAreOmittedForOtherStatuses(t *testing.T) {
	if TableStatusReplica != "replica" {
		t.Fatalf("TableStatusReplica = %q, want replica", TableStatusReplica)
	}
	const before = `{"status":"active","refused_code":"","refused_reason":"","schema_json":"{}","schema_hash":"0xabc","registry_version":42}`
	for _, status := range []string{TableStatusOrdinary, TableStatusPending, TableStatusRefused, TableStatusActive, TableStatusGone} {
		got, err := json.Marshal(TableStatus{Status: status, SchemaJSON: "{}", SchemaHash: "0xabc", RegistryVersion: 42})
		if err != nil {
			t.Fatal(err)
		}
		if want := strings.Replace(before, `"active"`, `"`+status+`"`, 1); string(got) != want {
			t.Fatalf("%s encodes as %s, want %s", status, got, want)
		}
	}
	var old TableStatus
	if err := json.Unmarshal([]byte(before), &old); err != nil {
		t.Fatal(err)
	}
	if old.ReplicaOf != nil || old.ServedFrom != "" || old.OwnerEntry != nil || old.FrozenSince != 0 {
		t.Fatalf("an older answer decodes as %+v, want no Replica field", old)
	}

	owner := uint64(0)
	replica := TableStatus{
		Status: TableStatusReplica, SchemaJSON: "{}", SchemaHash: "0xabc", RegistryVersion: 42,
		ReplicaOf: &owner, ServedFrom: "0x5e1f",
		OwnerEntry:  &OwnerEntry{IndexerID: 0, SafeBlockSeq: 812, AppliedPromotionSeq: 77},
		FrozenSince: 1760500000000,
	}
	got, err := json.Marshal(replica)
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"status":"replica","refused_code":"","refused_reason":"","schema_json":"{}","schema_hash":"0xabc","registry_version":42,` +
		`"replica_of":0,"served_from":"0x5e1f","owner_entry":{"indexer_id":0,"safe_block_seq":812,"applied_promotion_seq":77},"frozen_since":1760500000000}`
	if string(got) != want {
		t.Fatalf("replica encodes as\n%s\nwant\n%s", got, want)
	}
	var back TableStatus
	if err := json.Unmarshal(got, &back); err != nil {
		t.Fatal(err)
	}
	if back.ReplicaOf == nil || *back.ReplicaOf != 0 || back.ServedFrom != "0x5e1f" || back.OwnerEntry == nil ||
		*back.OwnerEntry != *replica.OwnerEntry || back.FrozenSince != replica.FrozenSince {
		t.Fatalf("round trip = %+v, want %+v", back, replica)
	}
}
