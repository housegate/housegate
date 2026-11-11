package rewriter

import (
	"sort"
	"testing"

	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/registry"
)

// TestBuildDatabaseMap_FiltersPendingDelete: a logical database whose
// DatabaseInfo.PendingDelete is true is on its way out — the rewriter
// must omit it from database_map so the rewriter service stops
// rewriting against it (and SHOW DATABASES stops surfacing it). Both
// the auth-off branch (anonymous, sees everything) and the
// permission-filtered branch (authenticated account) need to honour
// PendingDelete.
func TestBuildDatabaseMap_FiltersPendingDelete(t *testing.T) {
	t.Run("anonymous_auth_off", func(t *testing.T) {
		st := network.NewInMemoryNetworkState()
		st.DatabaseInfos["live"] = network.DatabaseInfo{DatabaseId: "live"}
		st.DatabaseInfos["dying"] = network.DatabaseInfo{DatabaseId: "dying", PendingDelete: true}

		f := &SentioNetworkFactory{
			options: Options{
				PhysicalDatabase: "phys",
				AuthEnabled:      false,
			},
			registry: st,
		}
		dbMap, _, err := f.buildDatabaseMap("")
		if err != nil {
			t.Fatalf("buildDatabaseMap: %v", err)
		}
		if got, want := keys(dbMap), []string{"live"}; !equalSorted(got, want) {
			t.Errorf("dbMap keys = %v, want %v", got, want)
		}
	})

	t.Run("authenticated_account", func(t *testing.T) {
		st := network.NewInMemoryNetworkState()
		st.DatabaseInfos["live"] = network.DatabaseInfo{DatabaseId: "live"}
		st.DatabaseInfos["dying"] = network.DatabaseInfo{DatabaseId: "dying", PendingDelete: true}
		st.DatabasePermissions["alice"] = network.DatabasePermissions{
			"live":  registry.DbAuthRead,
			"dying": registry.DbAuthOwner,
		}

		f := &SentioNetworkFactory{
			options: Options{
				PhysicalDatabase: "phys",
				AuthEnabled:      true,
			},
			registry: st,
		}
		dbMap, _, err := f.buildDatabaseMap("alice")
		if err != nil {
			t.Fatalf("buildDatabaseMap: %v", err)
		}
		if got, want := keys(dbMap), []string{"live"}; !equalSorted(got, want) {
			t.Errorf("dbMap keys = %v, want %v (pending-delete db must be filtered)", got, want)
		}
	})
}

func keys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func equalSorted(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	aa := append([]string(nil), a...)
	bb := append([]string(nil), b...)
	sort.Strings(aa)
	sort.Strings(bb)
	for i := range aa {
		if aa[i] != bb[i] {
			return false
		}
	}
	return true
}

// TestBuildDatabaseMap_IncludesPublicGrants pins housegate/housegate#340:
// the registry's PermissionsFor merges the grants held by the public
// (wildcard) address, so a database granted only to it is in every
// authenticated account's database_map, like a database the account was
// granted itself. A bit the public address does not hold is not implied: the
// permission observer still checks the statement's bit (a public Read does
// not let anyone write).
func TestBuildDatabaseMap_IncludesPublicGrants(t *testing.T) {
	st := network.NewInMemoryNetworkState()
	for _, db := range []string{"own", "public", "public_dying", "private"} {
		st.DatabaseInfos[network.Database(db)] = network.DatabaseInfo{DatabaseId: db, PendingDelete: db == "public_dying"}
	}
	st.DatabasePermissions["alice"] = network.DatabasePermissions{"own": registry.DbAuthOwner}
	st.DatabasePermissions["bob"] = network.DatabasePermissions{"private": registry.DbAuthRead}
	st.DatabasePermissions[network.WildcardAddress] = network.DatabasePermissions{
		"public":       registry.DbAuthRead,
		"public_dying": registry.DbAuthRead,
	}
	f := &SentioNetworkFactory{options: Options{PhysicalDatabase: "phys", AuthEnabled: true}, registry: st}
	for _, tc := range []struct {
		account string
		want    []string
	}{
		{"alice", []string{"own", "public"}},
		// An account with no grants of its own still sees the public database.
		{"carol", []string{"public"}},
	} {
		dbMap, known, err := f.buildDatabaseMap(tc.account)
		if err != nil {
			t.Fatalf("%s: buildDatabaseMap: %v", tc.account, err)
		}
		if got := keys(dbMap); !equalSorted(got, tc.want) {
			t.Errorf("%s: dbMap keys = %v, want %v", tc.account, got, tc.want)
		}
		for db, phys := range dbMap {
			if phys != "phys" {
				t.Errorf("%s: dbMap[%s] = %q, want phys", tc.account, db, phys)
			}
		}
		if len(known) != 1 || known[0] != "phys" {
			t.Errorf("%s: known physical = %v, want [phys]", tc.account, known)
		}
	}
	// The public address grants nothing beyond its own bits: Write stays
	// with the account's grants and the contract writer predicate.
	ok, err := st.HasPermission("carol", "public", registry.Write)
	if err != nil || ok {
		t.Fatalf("HasPermission(carol, public, Write) = %v, %v; want false", ok, err)
	}
	if w, err := st.IsDatabaseWriter("public", "carol"); err != nil || w {
		t.Fatalf("IsDatabaseWriter(public, carol) = %v, %v; want false", w, err)
	}
}
