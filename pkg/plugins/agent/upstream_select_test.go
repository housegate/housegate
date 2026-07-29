package agent

import (
	"errors"
	"math/rand"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/registry"
)

// buildNS assembles an InMemoryNetworkState and returns it wrapped as a
// registry.Registry — the narrow view Selector consumes.
func buildNS(t *testing.T,
	indexers map[uint64]network.IndexerInfo,
	databases map[network.Database]network.DatabaseInfo,
	perms map[network.AccountAddress]network.DatabasePermissions,
) registry.Registry {
	t.Helper()
	ns := network.NewInMemoryNetworkState()
	for k, v := range indexers {
		ns.IndexerInfos[k] = v
	}
	for k, v := range databases {
		ns.DatabaseInfos[k] = v
	}
	for k, v := range perms {
		ns.DatabasePermissions[k] = v
	}
	return ns
}

// fixedRand returns a *rand.Rand seeded with `seed` so picks are
// deterministic across runs.
func fixedRand(seed int64) *rand.Rand { return rand.New(rand.NewSource(seed)) }

func TestSelector_Pick_PermissionedTier(t *testing.T) {
	ns := buildNS(t,
		map[uint64]network.IndexerInfo{
			1: {IndexerId: 1, IndexerUrl: "a.example.com", ClickhouseProxyPort: 9000},
			2: {IndexerId: 2, IndexerUrl: "b.example.com", ClickhouseProxyPort: 9000},
			3: {IndexerId: 3, IndexerUrl: "c.example.com", ClickhouseProxyPort: 9000},
		},
		map[network.Database]network.DatabaseInfo{
			"tenantA": {DatabaseId: "tenantA", IndexerId: 1},
			"tenantB": {DatabaseId: "tenantB", IndexerId: 2},
		},
		map[network.AccountAddress]network.DatabasePermissions{
			// alice has perms only on tenantA → indexer 1 is the only
			// permissioned target.
			"0xalice": {"tenantA": registry.DbAuthRead},
		},
	)

	sel := &Selector{Topology: ns, Databases: ns, Access: ns, Account: "0xalice"}
	chosen, err := sel.Pick(fixedRand(1))
	if err != nil {
		t.Fatalf("Pick() unexpected error: %v", err)
	}
	if chosen.IsBootstrap {
		t.Fatalf("Pick() returned bootstrap=true for permissioned account")
	}
	if got, want := chosen.IndexerId, uint64(1); got != want {
		t.Errorf("Pick() chose indexer %d, want %d", got, want)
	}
	if got, want := chosen.Addr(), "a.example.com:9000"; got != want {
		t.Errorf("Pick() Addr=%q, want %q", got, want)
	}
}

func TestSelector_Pick_BootstrapFallback_NoPerms(t *testing.T) {
	ns := buildNS(t,
		map[uint64]network.IndexerInfo{
			1: {IndexerId: 1, IndexerUrl: "a.example.com", ClickhouseProxyPort: 9000},
		},
		nil,
		// 0xnewuser has no permissions on record.
		nil,
	)

	sel := &Selector{Topology: ns, Databases: ns, Access: ns, Account: "0xnewuser"}
	chosen, err := sel.Pick(fixedRand(0))
	if err != nil {
		t.Fatalf("Pick() unexpected error: %v", err)
	}
	if !chosen.IsBootstrap {
		t.Fatalf("Pick() bootstrap=false; new accounts must take fallback path")
	}
	if got := chosen.IndexerId; got != 1 {
		t.Errorf("Pick() indexer=%d, want 1", got)
	}
}

func TestSelector_Pick_BootstrapFallback_PermsButNoBoundIndexer(t *testing.T) {
	// alice has perms on tenantC, but tenantC's indexer (id=99) is not
	// in the bound list. Selector must fall back to any other bound
	// indexer rather than erroring.
	ns := buildNS(t,
		map[uint64]network.IndexerInfo{
			7: {IndexerId: 7, IndexerUrl: "z.example.com", ClickhouseProxyPort: 9100},
		},
		map[network.Database]network.DatabaseInfo{
			"tenantC": {DatabaseId: "tenantC", IndexerId: 99},
		},
		map[network.AccountAddress]network.DatabasePermissions{
			"0xalice": {"tenantC": registry.DbAuthRead},
		},
	)

	sel := &Selector{Topology: ns, Databases: ns, Access: ns, Account: "0xalice"}
	chosen, err := sel.Pick(fixedRand(0))
	if err != nil {
		t.Fatalf("Pick() unexpected error: %v", err)
	}
	if !chosen.IsBootstrap {
		t.Fatalf("Pick() bootstrap=false; permissioned indexer was unbound")
	}
	if got := chosen.IndexerId; got != 7 {
		t.Errorf("Pick() indexer=%d, want 7 (the only bound indexer)", got)
	}
}

func TestSelector_Pick_NoBoundIndexers_Errors(t *testing.T) {
	// Two indexers in NetworkState, neither has ClickhouseProxyPort
	// set — i.e. they exist but their proxy is unbound.
	ns := buildNS(t,
		map[uint64]network.IndexerInfo{
			1: {IndexerId: 1, IndexerUrl: "a.example.com", ClickhouseProxyPort: 0},
			2: {IndexerId: 2, IndexerUrl: "b.example.com"},
		},
		nil,
		nil,
	)

	sel := &Selector{Topology: ns, Databases: ns, Access: ns, Account: "0xalice"}
	_, err := sel.Pick(fixedRand(0))
	if err == nil {
		t.Fatalf("Pick() returned nil error; want a no-bound-indexers error")
	}
	if !strings.Contains(err.Error(), "no bound indexers") {
		t.Errorf("Pick() error %q missing 'no bound indexers'", err)
	}
}

func TestSelector_Pick_EmptyState_Errors(t *testing.T) {
	reg := network.NewInMemoryNetworkState()
	sel := &Selector{Topology: reg, Databases: reg, Access: reg, Account: "0xalice"}
	_, err := sel.Pick(fixedRand(0))
	if err == nil {
		t.Fatalf("Pick() returned nil error on empty state; want error")
	}
}

func TestSelector_Pick_NilState_Errors(t *testing.T) {
	sel := &Selector{Account: "0xalice"}
	_, err := sel.Pick(fixedRand(0))
	if !errors.Is(err, ErrNoNetworkState) {
		t.Fatalf("Pick() with nil deps = %v, want ErrNoNetworkState", err)
	}
}
