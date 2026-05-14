package rewriter

import (
	"sort"
	"testing"

	"sentioxyz/sentio-core/network/registry"

	"housegate/housegate/pkg/network"
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
			registry: network.NewRegistryAdapter(st),
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
			registry: network.NewRegistryAdapter(st),
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
