package network

// registry_adapter.go bridges the legacy State interface into the
// narrow pkg/registry contracts (Topology, Databases, Access).
//
// The adapter is a pure type-translation layer: it forwards every call
// to the wrapped State and converts between this package's
// sentio-core-derived types (IndexerInfo, DatabaseInfo, AccountAddress,
// ...) and the slim, sentio-core-free types defined in pkg/registry.
// No caching; identical semantics to calling the State methods directly.

import (
	"housegate/housegate/pkg/registry"

	sentioregistry "sentioxyz/sentio-core/network/registry"
)

// RegistryAdapter wraps a State so it satisfies registry.Topology,
// registry.Databases, and registry.Access. Use NewRegistryAdapter to
// construct one; the zero value is unusable.
type RegistryAdapter struct {
	s State
}

// NewRegistryAdapter wraps s so it can be passed where any of the
// pkg/registry interfaces is expected. Callers that hold a State today
// build one adapter at construction time and hand the same instance to
// every plugin needing Topology / Databases / Access.
func NewRegistryAdapter(s State) *RegistryAdapter {
	return &RegistryAdapter{s: s}
}

// --- Topology

func (a *RegistryAdapter) ProxyByIndexerId(indexerId uint64) (registry.ProxyAddress, bool) {
	info, ok := a.s.RetrieveIndexerInfo(indexerId)
	if !ok {
		return registry.ProxyAddress{}, false
	}
	return registry.ProxyAddress{
		Url:           info.IndexerUrl,
		HousegatePort: info.ClickhouseProxyPort,
	}, true
}

func (a *RegistryAdapter) AllIndexers() map[uint64]registry.ProxyAddress {
	src := a.s.RetrieveAllIndexerInfos()
	out := make(map[uint64]registry.ProxyAddress, len(src))
	for id, info := range src {
		out[id] = registry.ProxyAddress{
			Url:           info.IndexerUrl,
			HousegatePort: info.ClickhouseProxyPort,
		}
	}
	return out
}

// --- Databases

func (a *RegistryAdapter) Get(id string) (registry.Database, bool) {
	info, ok := a.s.RetrieveDatabaseInfo(Database(id))
	if !ok {
		return registry.Database{}, false
	}
	return convertDatabase(info), true
}

func (a *RegistryAdapter) All() map[string]registry.Database {
	src := a.s.RetrieveAllDatabaseInfos()
	out := make(map[string]registry.Database, len(src))
	for db, info := range src {
		out[string(db)] = convertDatabase(info)
	}
	return out
}

func convertDatabase(info DatabaseInfo) registry.Database {
	var tables []registry.Table
	if len(info.Tables) > 0 {
		tables = make([]registry.Table, len(info.Tables))
		for i, t := range info.Tables {
			tables[i] = registry.Table{Id: t.TableId}
		}
	}
	return registry.Database{
		IndexerId:     info.IndexerId,
		PendingDelete: info.PendingDelete,
		Tables:        tables,
	}
}

// --- Access

func (a *RegistryAdapter) PermissionsFor(account string) (map[string]registry.DbAuth, bool) {
	perms, ok := a.s.RetrieveDatabasePermissions(AccountAddress(account))
	if !ok {
		return nil, false
	}
	out := make(map[string]registry.DbAuth, len(perms))
	for db, auth := range perms {
		out[string(db)] = registry.DbAuth(auth)
	}
	return out, true
}

func (a *RegistryAdapter) HasPermission(account, database string, action registry.Action) (bool, error) {
	// DbAuth/Action bit values are byte-compatible across the new and
	// sentio-core registries (Read=1, Write=2, Admin=4, Owner=8), so a
	// numeric cast preserves semantics.
	return a.s.AccountHasPermissionForDatabase(
		AccountAddress(account),
		Database(database),
		sentioregistry.Action(action),
	)
}

func (a *RegistryAdapter) IsOperator(owner, signer string) bool {
	return a.s.IsOperator(AccountAddress(owner), AccountAddress(signer))
}

// Compile-time checks.
var (
	_ registry.Topology  = (*RegistryAdapter)(nil)
	_ registry.Databases = (*RegistryAdapter)(nil)
	_ registry.Access    = (*RegistryAdapter)(nil)
)
