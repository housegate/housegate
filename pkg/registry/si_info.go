package registry

import (
	"context"
	"errors"
)

// StorageIntegrityInfo is one answer of sentio_getStorageIntegrityInfo
// (spec 2026-10-09 §6.7) from the serving indexer. SIIndexerID is null while
// the table registry is disabled; SelfIndexerID is null when the node cannot
// resolve its own id.
type StorageIntegrityInfo struct {
	Enabled                bool    `json:"enabled"`
	NetworkID              string  `json:"network_id"`
	KeeperShardID          uint32  `json:"keeper_shard_id"`
	SIIndexerID            *uint64 `json:"si_indexer_id"`
	SelfIndexerID          *uint64 `json:"self_indexer_id"`
	ClientLanesEnabled     bool    `json:"client_lanes_enabled"`
	IngressMaxPayloadBytes uint64  `json:"ingress_max_payload_bytes"`
	MaxTokenAgeSeconds     uint64  `json:"max_token_age_seconds"`
	DefaultReadMode        string  `json:"default_read_mode"`
	RegistryVersion        uint64  `json:"registry_version"`
	ServerUnixTime         int64   `json:"server_unix_time"`
}

// EffectiveDefaultReadMode maps the empty mode to "safe", as the server does.
func (i StorageIntegrityInfo) EffectiveDefaultReadMode() string {
	if i.DefaultReadMode == "" {
		return "safe"
	}
	return i.DefaultReadMode
}

// StorageIntegrityDiscovery is the agent's per-database discovery port (spec
// 2026-10-09 D18). Both calls are answered by the indexer that hosts
// database. RpcNetworkState implements it; YAML and host-injected
// registries do not, and the agent then uses its configured network id and
// skips the writer pre-check.
type StorageIntegrityDiscovery interface {
	StorageIntegrityInfo(ctx context.Context, database string) (StorageIntegrityInfo, error)
	// StorageIntegrityWriterCheck calls sentio_isDatabaseWriter (Plan A2). An
	// error, including an indexer that predates the method and
	// ErrDatabaseNotHosted, means "unknown".
	StorageIntegrityWriterCheck(ctx context.Context, database, account string) (bool, error)
}

// ErrDatabaseNotHosted is StorageIntegrityWriterCheck's answer for a database
// that resolves to no hosting indexer. Only the hosting indexer knows a
// database's writers, so the check does not fall back to the bootstrap the
// way the status and info lookups do; the agent treats it as "unknown" and
// the server decides.
var ErrDatabaseNotHosted = errors.New("database is not hosted by any known indexer")
