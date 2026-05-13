package registry_test

import (
	"testing"

	"housegate/housegate/pkg/registry"
)

func TestInMemoryTopology(t *testing.T) {
	m := registry.NewInMemory()
	m.Indexers[1] = registry.ProxyAddress{Url: "host-a", HousegatePort: 9000}
	m.Indexers[2] = registry.ProxyAddress{Url: "host-b", HousegatePort: 9001}

	if p, ok := m.ProxyByIndexerId(1); !ok || p.Url != "host-a" || p.HousegatePort != 9000 {
		t.Fatalf("ProxyByIndexerId(1) = %+v, %v", p, ok)
	}
	if _, ok := m.ProxyByIndexerId(99); ok {
		t.Fatal("ProxyByIndexerId(99) expected miss")
	}

	all := m.AllIndexers()
	if len(all) != 2 {
		t.Fatalf("AllIndexers len=%d, want 2", len(all))
	}
	all[3] = registry.ProxyAddress{Url: "leak"} // mutation must not leak back
	if _, leaked := m.Indexers[3]; leaked {
		t.Fatal("AllIndexers returned non-fresh map")
	}
}

func TestInMemoryDatabases(t *testing.T) {
	m := registry.NewInMemory()
	m.DatabaseInfos["db1"] = registry.Database{IndexerId: 1, Tables: []registry.Table{{Id: "t1"}}}
	m.DatabaseInfos["db2"] = registry.Database{IndexerId: 2, PendingDelete: true}

	if d, ok := m.Get("db1"); !ok || d.IndexerId != 1 || len(d.Tables) != 1 {
		t.Fatalf("Get(db1) = %+v, %v", d, ok)
	}
	if d, ok := m.Get("db2"); !ok || !d.PendingDelete {
		t.Fatalf("Get(db2) = %+v, %v", d, ok)
	}
	if _, ok := m.Get("missing"); ok {
		t.Fatal("Get(missing) expected miss")
	}
	if len(m.All()) != 2 {
		t.Fatalf("All len=%d, want 2", len(m.All()))
	}
}

func TestInMemoryAccess_HasPermission(t *testing.T) {
	m := registry.NewInMemory()
	m.DatabaseInfos["db1"] = registry.Database{}
	m.Permissions["alice"] = map[string]registry.DbAuth{
		"db1": registry.DbAuthWrite, // Write implies Read
	}
	m.Permissions["bob"] = map[string]registry.DbAuth{
		"db1": registry.DbAuthOwner, // Owner implies Admin|Write|Read
	}

	cases := []struct {
		account, db string
		action      registry.Action
		want        bool
	}{
		{"alice", "db1", registry.Read, true},
		{"alice", "db1", registry.Write, true},
		{"bob", "db1", registry.Read, true},
		{"bob", "db1", registry.Write, true},
		{"carol", "db1", registry.Read, false}, // no grant
	}
	for _, c := range cases {
		got, err := m.HasPermission(c.account, c.db, c.action)
		if err != nil {
			t.Fatalf("HasPermission(%s,%s,%d) err=%v", c.account, c.db, c.action, err)
		}
		if got != c.want {
			t.Errorf("HasPermission(%s,%s,%d) = %v, want %v", c.account, c.db, c.action, got, c.want)
		}
	}

	if _, err := m.HasPermission("alice", "ghost", registry.Read); err == nil {
		t.Fatal("HasPermission on unknown database should error")
	}
}

func TestInMemoryAccess_PermissionsFor(t *testing.T) {
	m := registry.NewInMemory()
	m.Permissions["alice"] = map[string]registry.DbAuth{"db1": registry.DbAuthRead}

	perms, ok := m.PermissionsFor("alice")
	if !ok || perms["db1"] != registry.DbAuthRead {
		t.Fatalf("PermissionsFor(alice) = %+v, %v", perms, ok)
	}
	perms["leak"] = registry.DbAuthOwner
	if _, leaked := m.Permissions["alice"]["leak"]; leaked {
		t.Fatal("PermissionsFor returned non-fresh map")
	}
	if _, ok := m.PermissionsFor("nobody"); ok {
		t.Fatal("PermissionsFor(nobody) expected (nil, false)")
	}
}

func TestInMemoryAccess_IsOperator(t *testing.T) {
	m := registry.NewInMemory()
	m.Operators["owner1"] = map[string]bool{"signer1": true}

	if !m.IsOperator("owner1", "owner1") {
		t.Error("owner is always their own operator")
	}
	if !m.IsOperator("owner1", "signer1") {
		t.Error("explicit grant should pass")
	}
	if m.IsOperator("owner1", "stranger") {
		t.Error("non-granted signer should fail")
	}
	if m.IsOperator("", "signer1") || m.IsOperator("owner1", "") {
		t.Error("empty strings should always fail")
	}
}
