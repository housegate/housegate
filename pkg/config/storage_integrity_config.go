package config

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

const defaultStorageIntegrityMaxPayloadBytes uint64 = 64 << 20

// StorageIntegrityConfig owns HouseGate-local storage-integrity toggles.
type StorageIntegrityConfig struct {
	Ingress    StorageIntegrityIngressConfig    `json:"ingress"     yaml:"ingress"`
	Runtime    StorageIntegrityRuntimeConfig    `json:"runtime"     yaml:"runtime"`
	SafeMerges StorageIntegritySafeMergesConfig `json:"safe_merges" yaml:"safe_merges"`
	Agent      StorageIntegrityAgentConfig      `json:"agent"       yaml:"agent"`
}

// StorageIntegrityAgentConfig turns on the agent-mode statement plugin
// (pkg/plugins/sistatement): the agent answers the INSERT sample block from
// the network-state table schema, buffers and hashes the payload, and signs
// the envelope-v2 statement token before forwarding.
type StorageIntegrityAgentConfig struct {
	Enabled bool `json:"enabled" yaml:"enabled"`
	// NetworkID is the Arbiter genesis network id signed into every token.
	NetworkID string `json:"network_id" yaml:"network_id"`
	// KeeperShardID must be 0 in v1.
	KeeperShardID uint32 `json:"keeper_shard_id" yaml:"keeper_shard_id"`
	// StateDir holds <account>.seq, the durable client_seq counter.
	StateDir string `json:"state_dir" yaml:"state_dir"`
	// MaxPayloadBytes bounds one buffered INSERT payload (default 64 MiB).
	MaxPayloadBytes uint64 `json:"max_payload_bytes" yaml:"max_payload_bytes"`
	// RequireNetworkState (default true) makes Validate insist on
	// network_state.source; hosts that inject Options.NetworkState set false.
	RequireNetworkState bool `json:"require_network_state" yaml:"require_network_state"`
}

// StorageIntegritySafeMergesConfig governs the P1e runtime's merge guard, which
// re-asserts SYSTEM STOP MERGES on the guarded tables at startup so the
// integrity layer owns the active part inventory. AllowNativeBackgroundMerges is
// a fail-closed escape hatch: it defaults false, and enabling it is rejected in
// v1 because native background merges would mutate the guarded inventory out
// from under the integrity layer.
type StorageIntegritySafeMergesConfig struct {
	AllowNativeBackgroundMerges bool `json:"allow_native_background_merges" yaml:"allow_native_background_merges"`
}

// StorageIntegrityIngressConfig is the server-side signed admission surface.
type StorageIntegrityIngressConfig struct {
	Enabled          bool     `json:"enabled"           yaml:"enabled"`
	NetworkID        string   `json:"network_id"        yaml:"network_id"`
	AllowedAddresses []string `json:"allowed_addresses" yaml:"allowed_addresses"`
	MaxTokenAge      Duration `json:"max_token_age"     yaml:"max_token_age"`
	RequestTimeout   Duration `json:"request_timeout"  yaml:"request_timeout"`
	MaxPayloadBytes  uint64   `json:"max_payload_bytes" yaml:"max_payload_bytes"`
}

// StorageIntegrityRuntimeConfig turns on HouseGate's built-in P1e runtime
// consumer. It still depends on host-injected ports for the real companion
// topology; the YAML owns only the fail-fast protocol intent.
type StorageIntegrityRuntimeConfig struct {
	Enabled         bool                                      `json:"enabled"           yaml:"enabled"`
	ExpectedSource  string                                    `json:"expected_source"   yaml:"expected_source"`
	JournalDir      string                                    `json:"journal_dir"       yaml:"journal_dir"`
	PayloadSpoolDir string                                    `json:"payload_spool_dir" yaml:"payload_spool_dir"`
	PayloadLease    StorageIntegrityRuntimePayloadLeaseConfig `json:"payload_lease"     yaml:"payload_lease"`
	MergeGuard      StorageIntegrityRuntimeMergeGuardConfig   `json:"merge_guard"       yaml:"merge_guard"`
	Backpressure    StorageIntegrityRuntimeBackpressureConfig `json:"backpressure"      yaml:"backpressure"`
}

type StorageIntegrityRuntimePayloadLeaseConfig struct {
	RefreshInterval Duration `json:"refresh_interval" yaml:"refresh_interval"`
	RefreshBefore   Duration `json:"refresh_before"   yaml:"refresh_before"`
}

// StorageIntegrityRuntimeMergeGuardConfig is the production table set that
// HouseGate guards with table-scoped SYSTEM STOP MERGES at startup.
type StorageIntegrityRuntimeMergeGuardConfig struct {
	ReassertInterval Duration                                  `json:"reassert_interval" yaml:"reassert_interval"`
	Tables           []StorageIntegrityRuntimeMergeTableConfig `json:"tables"            yaml:"tables"`
}

// StorageIntegrityRuntimeMergeTableConfig identifies one ClickHouse table whose
// active-part inventory is owned by the storage-integrity runtime.
type StorageIntegrityRuntimeMergeTableConfig struct {
	Database string `json:"database" yaml:"database"`
	Table    string `json:"table"    yaml:"table"`
}

// StorageIntegrityRuntimeBackpressureConfig governs the ingress part-count
// throttle. The runtime polls system.parts of the co-located ClickHouse and
// refuses inserts at the soft limit; the hard limit is mirrored by the SNode.
// Both limits stay below the protocol-pinned parts_to_throw_insert setting.
type StorageIntegrityRuntimeBackpressureConfig struct {
	Enabled               bool     `json:"enabled"                  yaml:"enabled"`
	UnsafeDatabase        string   `json:"unsafe_database"          yaml:"unsafe_database"`
	SafeDatabase          string   `json:"safe_database"            yaml:"safe_database"`
	PollInterval          Duration `json:"poll_interval"            yaml:"poll_interval"`
	SoftPartsPerPartition int      `json:"soft_parts_per_partition" yaml:"soft_parts_per_partition"`
	HardPartsPerPartition int      `json:"hard_parts_per_partition" yaml:"hard_parts_per_partition"`
}

const pinnedPartsToThrowInsert = 3000

func defaultStorageIntegrityConfig() StorageIntegrityConfig {
	return StorageIntegrityConfig{
		Ingress: StorageIntegrityIngressConfig{
			Enabled:         false,
			MaxTokenAge:     Duration{Duration: time.Minute},
			RequestTimeout:  Duration{Duration: 5 * time.Second},
			MaxPayloadBytes: defaultStorageIntegrityMaxPayloadBytes,
		},
		Runtime: StorageIntegrityRuntimeConfig{
			PayloadLease: StorageIntegrityRuntimePayloadLeaseConfig{
				RefreshInterval: Duration{Duration: time.Second},
				RefreshBefore:   Duration{Duration: 30 * time.Second},
			},
			MergeGuard: StorageIntegrityRuntimeMergeGuardConfig{
				ReassertInterval: Duration{Duration: 30 * time.Second},
			},
			Backpressure: StorageIntegrityRuntimeBackpressureConfig{
				Enabled:               true,
				UnsafeDatabase:        "hg_unsafe",
				SafeDatabase:          "hg_safe",
				PollInterval:          Duration{Duration: 2 * time.Second},
				SoftPartsPerPartition: 2400,
				HardPartsPerPartition: 2950,
			},
		},
		Agent: StorageIntegrityAgentConfig{
			MaxPayloadBytes:     defaultStorageIntegrityMaxPayloadBytes,
			RequireNetworkState: true,
		},
	}
}

func (c StorageIntegrityConfig) validate(mode Mode) error {
	if c.Agent.Enabled && mode != ModeAgent {
		return errors.New("storage_integrity: storage_integrity.agent is agent mode only")
	}
	if !c.Ingress.Enabled {
		if c.Runtime.Enabled {
			return errors.New("storage_integrity: storage_integrity.runtime.enabled requires storage_integrity.ingress.enabled")
		}
		return nil
	}
	var errs []error
	if mode != ModeServer {
		errs = append(errs, errors.New("storage_integrity.ingress is server mode only"))
	}
	if c.SafeMerges.AllowNativeBackgroundMerges {
		errs = append(errs, errors.New("storage_integrity.safe_merges.allow_native_background_merges is not supported in v1: native background merges would mutate the guarded part inventory"))
	}
	if len(c.Ingress.AllowedAddresses) == 0 {
		errs = append(errs, errors.New("storage_integrity.ingress.allowed_addresses is required when storage_integrity.ingress.enabled"))
	}
	if strings.TrimSpace(c.Ingress.NetworkID) == "" {
		errs = append(errs, errors.New("storage_integrity.ingress.network_id is required when storage_integrity.ingress.enabled"))
	}
	if c.Ingress.MaxTokenAge.Duration <= 0 {
		errs = append(errs, errors.New("storage_integrity.ingress.max_token_age must be > 0 when storage_integrity.ingress.enabled"))
	}
	if c.Ingress.RequestTimeout.Duration <= 0 {
		errs = append(errs, errors.New("storage_integrity.ingress.request_timeout must be > 0 when storage_integrity.ingress.enabled"))
	}
	if c.Ingress.MaxPayloadBytes == 0 {
		errs = append(errs, errors.New("storage_integrity.ingress.max_payload_bytes must be > 0 when storage_integrity.ingress.enabled"))
	}
	if c.Runtime.Enabled {
		if strings.TrimSpace(c.Runtime.ExpectedSource) == "" {
			errs = append(errs, errors.New("storage_integrity.runtime.expected_source is required when storage_integrity.runtime.enabled"))
		}
		if strings.TrimSpace(c.Runtime.JournalDir) == "" {
			errs = append(errs, errors.New("storage_integrity.runtime.journal_dir is required when storage_integrity.runtime.enabled"))
		}
		if strings.TrimSpace(c.Runtime.PayloadSpoolDir) == "" {
			errs = append(errs, errors.New("storage_integrity.runtime.payload_spool_dir is required when storage_integrity.runtime.enabled"))
		}
		if c.Runtime.PayloadLease.RefreshInterval.Duration <= 0 {
			errs = append(errs, errors.New("storage_integrity.runtime.payload_lease.refresh_interval must be > 0 when storage_integrity.runtime.enabled"))
		}
		if c.Runtime.PayloadLease.RefreshBefore.Duration <= 0 {
			errs = append(errs, errors.New("storage_integrity.runtime.payload_lease.refresh_before must be > 0 when storage_integrity.runtime.enabled"))
		}
		if c.Runtime.MergeGuard.ReassertInterval.Duration <= 0 {
			errs = append(errs, errors.New("storage_integrity.runtime.merge_guard.reassert_interval must be > 0 when storage_integrity.runtime.enabled"))
		}
		if len(c.Runtime.MergeGuard.Tables) == 0 {
			errs = append(errs, errors.New("storage_integrity.runtime.merge_guard.tables is required when storage_integrity.runtime.enabled"))
		}
		for i, table := range c.Runtime.MergeGuard.Tables {
			if strings.TrimSpace(table.Database) == "" {
				errs = append(errs, fmt.Errorf("storage_integrity.runtime.merge_guard.tables[%d].database is required", i))
			}
			if strings.TrimSpace(table.Table) == "" {
				errs = append(errs, fmt.Errorf("storage_integrity.runtime.merge_guard.tables[%d].table is required", i))
			}
		}
		if bp := c.Runtime.Backpressure; bp.Enabled {
			if strings.TrimSpace(bp.UnsafeDatabase) == "" {
				errs = append(errs, errors.New("storage_integrity.runtime.backpressure.unsafe_database is required when backpressure is enabled"))
			}
			if bp.PollInterval.Duration <= 0 {
				errs = append(errs, errors.New("storage_integrity.runtime.backpressure.poll_interval must be > 0"))
			}
			if bp.SoftPartsPerPartition <= 0 {
				errs = append(errs, errors.New("storage_integrity.runtime.backpressure.soft_parts_per_partition must be > 0"))
			}
			if bp.HardPartsPerPartition <= bp.SoftPartsPerPartition || bp.HardPartsPerPartition >= pinnedPartsToThrowInsert {
				errs = append(errs, fmt.Errorf("storage_integrity.runtime.backpressure.hard_parts_per_partition must be > soft_parts_per_partition and < the pinned parts_to_throw_insert (%d)", pinnedPartsToThrowInsert))
			}
		}
	}
	if joined := errors.Join(errs...); joined != nil {
		return fmt.Errorf("storage_integrity: %w", joined)
	}
	return nil
}

// validateAgent checks the agent-mode statement plugin block. Called from
// Config.Validate in ModeAgent only.
func (c StorageIntegrityConfig) validateAgent(root *Config) error {
	a := c.Agent
	if !a.Enabled {
		return nil
	}
	var errs []error
	if strings.TrimSpace(a.NetworkID) == "" {
		errs = append(errs, errors.New("storage_integrity.agent.network_id is required when storage_integrity.agent.enabled"))
	}
	if a.KeeperShardID != 0 {
		errs = append(errs, fmt.Errorf("storage_integrity.agent.keeper_shard_id must be 0 in v1, got %d", a.KeeperShardID))
	}
	if strings.TrimSpace(a.StateDir) == "" {
		errs = append(errs, errors.New("storage_integrity.agent.state_dir is required when storage_integrity.agent.enabled"))
	}
	if a.MaxPayloadBytes == 0 {
		errs = append(errs, errors.New("storage_integrity.agent.max_payload_bytes must be > 0 when storage_integrity.agent.enabled"))
	}
	if root.Agent.PrivateKeyHex == "" {
		errs = append(errs, errors.New("storage_integrity.agent requires agent.private_key_hex"))
	}
	if a.RequireNetworkState &&
		!root.NetworkState.IsYAMLSource() &&
		!root.NetworkState.IsRpcSource() &&
		root.ResolveRedisAddr(root.NetworkState.Source) == "" {
		errs = append(errs, errors.New("storage_integrity.agent requires network_state.source (or set storage_integrity.agent.require_network_state: false when the host injects Options.NetworkState)"))
	}
	if joined := errors.Join(errs...); joined != nil {
		return fmt.Errorf("storage_integrity.agent: %w", joined)
	}
	return nil
}
