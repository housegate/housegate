package config

import (
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

type StorageIntegrityConfig struct {
	Enabled         bool   `json:"enabled"             yaml:"enabled"`
	DAEndpoint      string `json:"da_endpoint"         yaml:"da_endpoint"`
	ArbiterEndpoint string `json:"arbiter_endpoint"    yaml:"arbiter_endpoint"`
	// SequencerEndpoint is the legacy name kept for config compatibility.
	SequencerEndpoint     string                           `json:"sequencer_endpoint"  yaml:"sequencer_endpoint"`
	UnsafeDatabase        string                           `json:"unsafe_database"     yaml:"unsafe_database"`
	UnsafeBufferDatabases []string                         `json:"unsafe_buffer_databases" yaml:"unsafe_buffer_databases"`
	SafeDatabase          string                           `json:"safe_database"       yaml:"safe_database"`
	NetworkID             string                           `json:"network_id"          yaml:"network_id"`
	// LeaderPublicKeyHex is the arbiter leader's ed25519 public key (hex). When
	// set, promotion / compaction workers verify the leader signature on every
	// publication task before executing and fail closed on mismatch (spec §9.1
	// PromotionIssued, §10, gap-25). Empty disables the check (single-node / e2e
	// flows without a leader key still run).
	LeaderPublicKeyHex    string                           `json:"leader_public_key_hex" yaml:"leader_public_key_hex"`
	InjectRowID           bool                             `json:"inject_row_id"       yaml:"inject_row_id"`
	RequireRowIDInput     bool                             `json:"require_row_id_input" yaml:"require_row_id_input"`
	RequireAuthToken      bool                             `json:"require_auth_token"  yaml:"require_auth_token"`
	Workers               StorageIntegrityWorkersConfig    `json:"workers"             yaml:"workers"`
	Mutations             StorageIntegrityMutationsConfig  `json:"mutations" yaml:"mutations"`
	SafeTables            StorageIntegritySafeTablesConfig `json:"safe_tables" yaml:"safe_tables"`
	SafeMerges            StorageIntegritySafeMergesConfig `json:"safe_merges" yaml:"safe_merges"`
	PartLtHashCache       StorageIntegrityPartLtHashCacheConfig `json:"part_lthash_cache" yaml:"part_lthash_cache"`
	Promotion             StorageIntegrityPromotionConfig  `json:"promotion" yaml:"promotion"`
}

// StorageIntegrityPromotionConfig tunes the local promotion executor. It does
// not change the arbiter command or the published manifest — only how the SNode
// verifies its own REPLACE PARTITION locally.
type StorageIntegrityPromotionConfig struct {
	// StrictVerification forces a full post-promotion readback of the shadow's
	// active parts for the post-root CAS. When false (default) the CAS uses the
	// arithmetic expected post root (base + sum of verified candidate part
	// LtHashes) — provably equal to the readback root because ATTACH PARTITION is
	// byte-preserving and the additive sum is merge-invariant — avoiding a full
	// row scan. Either way the CAS is still against the arbiter-pinned
	// ExpectedPostRoots, so a mismatch fails closed before REPLACE.
	StrictVerification bool `json:"strict_verification" yaml:"strict_verification"`
}

// StorageIntegrityPartLtHashCacheConfig controls the local, discardable
// per-part row-LtHash cache that fronts the byte-side / base-commitment /
// promotion row folds. It is a pure data-plane acceleration: a miss recomputes
// by scanning, and entries are keyed by physical part content, so the cache can
// never change the evidence submitted to the arbiter. Disabled by default.
type StorageIntegrityPartLtHashCacheConfig struct {
	Enabled    bool `json:"enabled"     yaml:"enabled"`
	MaxEntries int  `json:"max_entries" yaml:"max_entries"`
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
	// ClaimWait, when > 0, enables server-side long-poll on task claims: a claim
	// blocks on the arbiter up to this long instead of returning immediately, so
	// an idle worker picks up its next task promptly with far fewer poll round
	// trips. 0 (default) preserves the PollInterval-driven immediate-return
	// polling. The FSM/task content is unchanged — this only affects claim
	// latency.
	ClaimWait             Duration `json:"claim_wait"             yaml:"claim_wait"`
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
	// QuarantineMinority is advisory arbiter policy, not enforced by HouseGate.
	// Per spec §9.3 the quarantine decision is derived solely by the arbiter FSM
	// from recorded evidence; HouseGate only stamps its worker_id on every claim
	// (see HTTPArbiterClient.WithWorkerID) so the arbiter can refuse quarantined
	// workers. This field records the deployment's intended policy for the
	// arbiter to honor (the mock exposes /v1/mock/quarantine-minority for the
	// e2e to mirror it); HouseGate never quarantines a peer on its own.
	QuarantineMinority        bool     `json:"quarantine_minority"         yaml:"quarantine_minority"`
	MaxRebindAttempts         int      `json:"max_rebind_attempts"         yaml:"max_rebind_attempts"`
	MaxRebindDuration         Duration `json:"max_rebind_duration"         yaml:"max_rebind_duration"`
}

type StorageIntegritySafeTablesConfig struct {
	StopMerges                          bool `json:"stop_merges"                            yaml:"stop_merges"`
	EnforceNoMergeSettings              bool `json:"enforce_no_merge_settings"              yaml:"enforce_no_merge_settings"`
	VerifyPhysicalActiveMatchesManifest bool `json:"verify_physical_active_matches_manifest" yaml:"verify_physical_active_matches_manifest"`
}

// StorageIntegritySafeMergesConfig controls the controlled-compaction lane
// (spec §8.1, §14). v1 keeps safe tables STOP MERGES; controlled compaction is
// the only sanctioned way to merge safe parts, and native background merges
// must stay disabled.
type StorageIntegritySafeMergesConfig struct {
	Enabled                    bool   `json:"enabled"                       yaml:"enabled"`
	Mode                       string `json:"mode"                          yaml:"mode"`
	AllowNativeBackgroundMerges bool  `json:"allow_native_background_merges" yaml:"allow_native_background_merges"`
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
		SafeMerges: StorageIntegritySafeMergesConfig{
			Enabled:                     false,
			Mode:                        "controlled_compaction",
			AllowNativeBackgroundMerges: false,
		},
		PartLtHashCache: StorageIntegrityPartLtHashCacheConfig{
			Enabled:    false,
			MaxEntries: 1_000_000,
		},
	}
}

func (c StorageIntegrityConfig) ControlPlaneEndpoint() string {
	if c.ArbiterEndpoint != "" {
		return c.ArbiterEndpoint
	}
	return c.SequencerEndpoint
}

func (c StorageIntegrityConfig) EffectiveUnsafeDatabases() []string {
	if len(c.UnsafeBufferDatabases) > 0 {
		return append([]string(nil), c.UnsafeBufferDatabases...)
	}
	if c.UnsafeDatabase == "" {
		return nil
	}
	return []string{c.UnsafeDatabase}
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
	if len(c.UnsafeBufferDatabases) > 0 {
		if len(c.UnsafeBufferDatabases) != 2 {
			errs = append(errs, errors.New("storage_integrity.unsafe_buffer_databases must contain exactly two databases when configured"))
		}
		seen := map[string]struct{}{}
		for _, db := range c.UnsafeBufferDatabases {
			if db == "" {
				errs = append(errs, errors.New("storage_integrity.unsafe_buffer_databases cannot contain empty database names"))
				continue
			}
			if _, ok := seen[db]; ok {
				errs = append(errs, errors.New("storage_integrity.unsafe_buffer_databases cannot contain duplicates"))
			}
			seen[db] = struct{}{}
		}
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
	// v1 forbids native background merges on safe tables: they would break the
	// manifest-authoritative active set. Merges are only sanctioned via the
	// controlled-compaction lane (spec §8.1).
	if c.SafeMerges.AllowNativeBackgroundMerges {
		errs = append(errs, errors.New("storage_integrity.safe_merges.allow_native_background_merges must be false in v1"))
	}
	if c.SafeMerges.Enabled && c.SafeMerges.Mode != "" && c.SafeMerges.Mode != "controlled_compaction" {
		errs = append(errs, errors.New("storage_integrity.safe_merges.mode must be controlled_compaction"))
	}
	if c.PartLtHashCache.Enabled && c.PartLtHashCache.MaxEntries < 0 {
		errs = append(errs, errors.New("storage_integrity.part_lthash_cache.max_entries must be non-negative"))
	}
	// gap-25: a configured leader public key must be a valid 32-byte ed25519 key.
	if key := strings.TrimPrefix(strings.TrimSpace(c.LeaderPublicKeyHex), "0x"); key != "" {
		raw, err := hex.DecodeString(key)
		if err != nil {
			errs = append(errs, errors.New("storage_integrity.leader_public_key_hex is not valid hex"))
		} else if len(raw) != ed25519.PublicKeySize {
			errs = append(errs, fmt.Errorf("storage_integrity.leader_public_key_hex must be %d bytes", ed25519.PublicKeySize))
		}
	}
	if joined := errors.Join(errs...); joined != nil {
		return fmt.Errorf("storage_integrity: %w", joined)
	}
	return nil
}
