package config

import (
	"errors"
	"fmt"
	"time"
)

type StorageIntegrityConfig struct {
	Enabled         bool   `json:"enabled"             yaml:"enabled"`
	DAEndpoint      string `json:"da_endpoint"         yaml:"da_endpoint"`
	ArbiterEndpoint string `json:"arbiter_endpoint"    yaml:"arbiter_endpoint"`
	// SequencerEndpoint is the legacy name kept for config compatibility.
	SequencerEndpoint string                           `json:"sequencer_endpoint"  yaml:"sequencer_endpoint"`
	UnsafeDatabase    string                           `json:"unsafe_database"     yaml:"unsafe_database"`
	SafeDatabase      string                           `json:"safe_database"       yaml:"safe_database"`
	NetworkID         string                           `json:"network_id"          yaml:"network_id"`
	InjectRowID       bool                             `json:"inject_row_id"       yaml:"inject_row_id"`
	RequireAuthToken  bool                             `json:"require_auth_token"  yaml:"require_auth_token"`
	Workers           StorageIntegrityWorkersConfig    `json:"workers"             yaml:"workers"`
	Mutations         StorageIntegrityMutationsConfig  `json:"mutations" yaml:"mutations"`
	SafeTables        StorageIntegritySafeTablesConfig `json:"safe_tables" yaml:"safe_tables"`
}

type StorageIntegrityWorkersConfig struct {
	Enabled               bool     `json:"enabled"                yaml:"enabled"`
	WorkerID              string   `json:"worker_id"              yaml:"worker_id"`
	ClickHouseAddr        string   `json:"clickhouse_addr"        yaml:"clickhouse_addr"`
	ClickHouseDatabase    string   `json:"clickhouse_database"    yaml:"clickhouse_database"`
	ClickHouseUsername    string   `json:"clickhouse_username"    yaml:"clickhouse_username"`
	ClickHousePassword    string   `json:"clickhouse_password"    yaml:"clickhouse_password"`
	MetadataDatabase      string   `json:"metadata_database"      yaml:"metadata_database"`
	PromoteDatabase       string   `json:"promote_database"       yaml:"promote_database"`
	CompactDatabase       string   `json:"compact_database"       yaml:"compact_database"`
	ClaimPrivateKeyHex    string   `json:"claim_private_key_hex"  yaml:"claim_private_key_hex"`
	ReplaySignerSeedHex   string   `json:"replay_signer_seed_hex" yaml:"replay_signer_seed_hex"`
	NativePayloadRevision int      `json:"native_payload_revision" yaml:"native_payload_revision"`
	PollInterval          Duration `json:"poll_interval"          yaml:"poll_interval"`
	ErrorBackoff          Duration `json:"error_backoff"          yaml:"error_backoff"`
	Replay                bool     `json:"replay"                 yaml:"replay"`
	UnsafeValidation      bool     `json:"unsafe_validation"      yaml:"unsafe_validation"`
	Promotion             bool     `json:"promotion"              yaml:"promotion"`
	Mutation              bool     `json:"mutation"               yaml:"mutation"`
	Rollback              bool     `json:"rollback"               yaml:"rollback"`
	RepairSync            bool     `json:"repair_sync"            yaml:"repair_sync"`
	SafeAudit             bool     `json:"safe_audit"             yaml:"safe_audit"`
	Compaction            bool     `json:"compaction"             yaml:"compaction"`
}

type StorageIntegrityMutationsConfig struct {
	Enabled                   bool     `json:"enabled"                     yaml:"enabled"`
	ScratchDatabase           string   `json:"scratch_database"            yaml:"scratch_database"`
	QueryTimeout              Duration `json:"query_timeout"               yaml:"query_timeout"`
	MutationTimeout           Duration `json:"mutation_timeout"            yaml:"mutation_timeout"`
	MaxTouchedPartitions      int      `json:"max_touched_partitions"      yaml:"max_touched_partitions"`
	MaxTouchedParts           int      `json:"max_touched_parts"           yaml:"max_touched_parts"`
	MaxTouchedBytes           int64    `json:"max_touched_bytes"           yaml:"max_touched_bytes"`
	RequirePartitionPredicate bool     `json:"require_partition_predicate" yaml:"require_partition_predicate"`
	PartitionColumns          []string `json:"partition_columns"           yaml:"partition_columns"`
	ProtectedColumns          []string `json:"protected_columns"           yaml:"protected_columns"`
	WaitMutationsSync         int      `json:"wait_mutations_sync"         yaml:"wait_mutations_sync"`
	RejectLightweightDelete   bool     `json:"reject_lightweight_delete"   yaml:"reject_lightweight_delete"`
	QuarantineMinority        bool     `json:"quarantine_minority"         yaml:"quarantine_minority"`
	MaxRebindAttempts         int      `json:"max_rebind_attempts"         yaml:"max_rebind_attempts"`
	MaxRebindDuration         Duration `json:"max_rebind_duration"         yaml:"max_rebind_duration"`
}

type StorageIntegritySafeTablesConfig struct {
	StopMerges                          bool `json:"stop_merges"                            yaml:"stop_merges"`
	EnforceNoMergeSettings              bool `json:"enforce_no_merge_settings"              yaml:"enforce_no_merge_settings"`
	VerifyPhysicalActiveMatchesManifest bool `json:"verify_physical_active_matches_manifest" yaml:"verify_physical_active_matches_manifest"`
}

func defaultStorageIntegrityConfig() StorageIntegrityConfig {
	return StorageIntegrityConfig{
		Enabled:          false,
		UnsafeDatabase:   "hg_unsafe",
		SafeDatabase:     "hg_safe",
		NetworkID:        "sentio",
		InjectRowID:      true,
		RequireAuthToken: true,
		Workers: StorageIntegrityWorkersConfig{
			MetadataDatabase:      "hg_meta",
			PromoteDatabase:       "hg_promote",
			CompactDatabase:       "hg_compact",
			NativePayloadRevision: 54460,
			PollInterval:          Duration{Duration: time.Second},
			ErrorBackoff:          Duration{Duration: 5 * time.Second},
		},
		Mutations: StorageIntegrityMutationsConfig{
			Enabled:                   true,
			ScratchDatabase:           "hg_mutation",
			MaxTouchedPartitions:      4,
			MaxTouchedParts:           128,
			MaxTouchedBytes:           1073741824,
			RequirePartitionPredicate: false,
			WaitMutationsSync:         2,
			RejectLightweightDelete:   true,
			QuarantineMinority:        true,
			MaxRebindAttempts:         3,
		},
		SafeTables: StorageIntegritySafeTablesConfig{
			StopMerges:                          true,
			EnforceNoMergeSettings:              true,
			VerifyPhysicalActiveMatchesManifest: true,
		},
	}
}

func (c StorageIntegrityConfig) ControlPlaneEndpoint() string {
	if c.ArbiterEndpoint != "" {
		return c.ArbiterEndpoint
	}
	return c.SequencerEndpoint
}

func (c StorageIntegrityConfig) validate(mode Mode) error {
	if !c.Enabled {
		return nil
	}
	var errs []error
	if mode != ModeServer {
		errs = append(errs, errors.New("storage_integrity is server mode only"))
	}
	if c.DAEndpoint == "" {
		errs = append(errs, errors.New("storage_integrity.da_endpoint is required when storage_integrity.enabled"))
	}
	if c.ControlPlaneEndpoint() == "" {
		errs = append(errs, errors.New("storage_integrity.arbiter_endpoint is required when storage_integrity.enabled"))
	}
	if c.UnsafeDatabase == "" {
		errs = append(errs, errors.New("storage_integrity.unsafe_database is required when storage_integrity.enabled"))
	}
	if c.SafeDatabase == "" {
		errs = append(errs, errors.New("storage_integrity.safe_database is required when storage_integrity.enabled"))
	}
	if c.Mutations.Enabled {
		if c.Mutations.ScratchDatabase == "" {
			errs = append(errs, errors.New("storage_integrity.mutations.scratch_database is required when mutations are enabled"))
		}
		if c.Mutations.RequirePartitionPredicate && len(c.Mutations.PartitionColumns) == 0 {
			errs = append(errs, errors.New("storage_integrity.mutations.partition_columns is required when require_partition_predicate is true"))
		}
		if c.Mutations.WaitMutationsSync < 0 {
			errs = append(errs, errors.New("storage_integrity.mutations.wait_mutations_sync must be non-negative"))
		}
		if c.Mutations.MaxTouchedPartitions < 0 || c.Mutations.MaxTouchedParts < 0 || c.Mutations.MaxTouchedBytes < 0 {
			errs = append(errs, errors.New("storage_integrity.mutations touched limits must be non-negative"))
		}
	}
	if joined := errors.Join(errs...); joined != nil {
		return fmt.Errorf("storage_integrity: %w", joined)
	}
	return nil
}
