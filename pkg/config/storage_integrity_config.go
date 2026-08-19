package config

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/housegate/housegate/pkg/rewriter"
)

const (
	defaultStorageIntegrityMaxPayloadBytes uint64 = 64 << 20

	// Physical homes of storage-integrity tables (Spec C D2 naming freeze).
	StorageIntegrityUnsafeDatabase = "hg_unsafe"
	StorageIntegritySafeDatabase   = "hg_safe"
)

// StorageIntegrityPhysicalTable maps a logical table id "<db>.<table>" to
// the physical table name used under hg_unsafe / hg_safe / hg_promote --
// the same rule as arbiter-core's snode.CHTableName (Spec C D2). Kept here
// (one line) so housegate does not import arbiter-core.
func StorageIntegrityPhysicalTable(tableID string) string {
	return strings.ReplaceAll(tableID, ".", "__")
}

// SplitStorageIntegrityTableID validates and splits a logical table id:
// exactly one dot, non-empty database and table.
func SplitStorageIntegrityTableID(id string) (string, string, bool) {
	parts := strings.Split(id, ".")
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return "", "", false
	}
	return parts[0], parts[1], true
}

// StorageIntegrityConfig owns HouseGate-local storage-integrity toggles.
type StorageIntegrityConfig struct {
	// Tables is the explicit SI membership (Spec G D4): logical
	// "<database>.<table>" ids. Shared by the merge guard (which guards
	// hg_safe.<phys> and hg_unsafe.<phys>), the ingress, and the read
	// rewrite. Renamed from runtime.merge_guard.tables.
	Tables     []string                         `json:"tables"      yaml:"tables"`
	Read       StorageIntegrityReadConfig       `json:"read"        yaml:"read"`
	Ingress    StorageIntegrityIngressConfig    `json:"ingress"     yaml:"ingress"`
	Runtime    StorageIntegrityRuntimeConfig    `json:"runtime"     yaml:"runtime"`
	SafeMerges StorageIntegritySafeMergesConfig `json:"safe_merges" yaml:"safe_merges"`
	Agent      StorageIntegrityAgentConfig      `json:"agent"       yaml:"agent"`
}

// StorageIntegrityReadConfig is the read-surface policy (Spec G D1).
type StorageIntegrityReadConfig struct {
	// DefaultMode is "safe" (default when empty) or "unsafe_latest"; a
	// query may override it with the SQL_x_read_mode setting.
	DefaultMode string `json:"default_mode" yaml:"default_mode"`
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

// StorageIntegrityRuntimeMergeGuardConfig tunes the startup SYSTEM STOP
// MERGES guard; the guarded table set is StorageIntegrityConfig.Tables.
type StorageIntegrityRuntimeMergeGuardConfig struct {
	ReassertInterval Duration `json:"reassert_interval" yaml:"reassert_interval"`
	// LegacyTables only exists to catch the pre-Spec-G key and turn it into
	// a pointed rename error instead of a silently unguarded deployment.
	LegacyTables []StorageIntegrityRuntimeMergeTableConfig `json:"tables" yaml:"tables"`
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
	var errs []error
	if c.Agent.Enabled && mode != ModeAgent {
		errs = append(errs, errors.New("storage_integrity.agent is agent mode only"))
	}
	if len(c.Runtime.MergeGuard.LegacyTables) > 0 {
		errs = append(errs, errors.New("storage_integrity.runtime.merge_guard.tables was renamed to storage_integrity.tables (list of logical <database>.<table> ids; hg_unsafe/hg_safe physical names are derived)"))
	}
	if len(c.Tables) > 0 && mode != ModeServer {
		errs = append(errs, errors.New("storage_integrity.tables is server mode only"))
	}
	seen := make(map[string]bool, len(c.Tables))
	physicalOwners := make(map[string]string, len(c.Tables))
	for i, id := range c.Tables {
		_, _, valid := SplitStorageIntegrityTableID(id)
		if !valid {
			errs = append(errs, fmt.Errorf("storage_integrity.tables[%d] %q must be a logical <database>.<table> id", i, id))
		}
		if seen[id] {
			errs = append(errs, fmt.Errorf("storage_integrity.tables[%d] duplicates %q", i, id))
		}
		seen[id] = true
		if valid {
			physical := StorageIntegrityPhysicalTable(id)
			if previous, ok := physicalOwners[physical]; ok && previous != id {
				errs = append(errs, fmt.Errorf("storage_integrity.tables[%d] %q collides with %q at physical table %q", i, id, previous, physical))
			} else {
				physicalOwners[physical] = id
			}
		}
	}
	if c.Read.DefaultMode != "" {
		mode, err := rewriter.ParseReadMode(c.Read.DefaultMode)
		if err != nil || string(mode) != c.Read.DefaultMode {
			errs = append(errs, fmt.Errorf("storage_integrity.read.default_mode %q must be safe or unsafe_latest", c.Read.DefaultMode))
		}
	}
	if c.Read.DefaultMode != "" && len(c.Tables) == 0 {
		errs = append(errs, errors.New("storage_integrity.read.default_mode requires storage_integrity.tables"))
	}
	if !c.Ingress.Enabled {
		if c.Runtime.Enabled {
			errs = append(errs, errors.New("storage_integrity.runtime.enabled requires storage_integrity.ingress.enabled"))
		}
		return joinStorageIntegrityErrs(errs)
	}
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
		if len(c.Tables) == 0 {
			errs = append(errs, errors.New("storage_integrity.tables is required when storage_integrity.runtime.enabled"))
		}
		if bp := c.Runtime.Backpressure; bp.Enabled {
			unsafeDatabase := strings.TrimSpace(bp.UnsafeDatabase)
			safeDatabase := strings.TrimSpace(bp.SafeDatabase)
			if unsafeDatabase == "" {
				errs = append(errs, errors.New("storage_integrity.runtime.backpressure.unsafe_database is required when backpressure is enabled"))
			}
			if safeDatabase == "" {
				errs = append(errs, errors.New("storage_integrity.runtime.backpressure.safe_database is required when backpressure is enabled"))
			} else if safeDatabase == unsafeDatabase {
				errs = append(errs, errors.New("storage_integrity.runtime.backpressure.safe_database must differ from unsafe_database"))
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
	return joinStorageIntegrityErrs(errs)
}

func joinStorageIntegrityErrs(errs []error) error {
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
