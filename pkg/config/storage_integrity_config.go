package config

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// StorageIntegrityConfig gates the HouseGate-side workers for the
// HouseKeeper storage-integrity protocol. It is disabled by default; P0 uses a
// mock payload store and external mock finality/rollback event sources.
type StorageIntegrityConfig struct {
	Enabled           bool                             `json:"enabled"            yaml:"enabled"`
	MockPayloadStore  StorageIntegrityMockPayloadStore `json:"mock_payload_store" yaml:"mock_payload_store"`
	MockFinality      StorageIntegrityMockFinality     `json:"mock_finality"      yaml:"mock_finality"`
	MockPartRegistry  StorageIntegrityMockPartRegistry `json:"mock_part_registry" yaml:"mock_part_registry"`
	HouseKeeper       StorageIntegrityHouseKeeper      `json:"housekeeper"       yaml:"housekeeper"`
	UnsafeValidation  StorageIntegrityUnsafeValidation `json:"unsafe_validation"  yaml:"unsafe_validation"`
	SafeAudit         StorageIntegritySafeAudit        `json:"safe_audit"         yaml:"safe_audit"`
	Workers           StorageIntegrityWorkersConfig    `json:"workers"           yaml:"workers"`
	UnsafeDatabase    string                           `json:"unsafe_database"   yaml:"unsafe_database"`
	SafeDatabase      string                           `json:"safe_database"     yaml:"safe_database"`
	UnsafeTableSuffix string                           `json:"unsafe_table_suffix" yaml:"unsafe_table_suffix"`
}

type StorageIntegrityMockPayloadStore struct {
	Path string `json:"path" yaml:"path"`
}

type StorageIntegrityMockFinality struct {
	Delay Duration `json:"delay" yaml:"delay"`
}

type StorageIntegrityMockPartRegistry struct {
	PartitionIDs []string `json:"partition_ids" yaml:"partition_ids"`
}

type StorageIntegrityHouseKeeper struct {
	Endpoints      []string `json:"endpoints"       yaml:"endpoints"`
	Root           string   `json:"root"            yaml:"root"`
	WorkerID       string   `json:"worker_id"       yaml:"worker_id"`
	ReplayQuorum   int      `json:"replay_quorum"   yaml:"replay_quorum"`
	SessionTimeout Duration `json:"session_timeout" yaml:"session_timeout"`
}

type StorageIntegrityUnsafeValidation struct {
	Replicas     []StorageIntegrityUnsafeReplica `json:"replicas"      yaml:"replicas"`
	QueryTimeout Duration                        `json:"query_timeout" yaml:"query_timeout"`
}

type StorageIntegrityUnsafeReplica struct {
	ReplicaID string `json:"replica_id" yaml:"replica_id"`
	Addr      string `json:"addr"       yaml:"addr"`
}

type StorageIntegritySafeAudit struct {
	Replicas   []StorageIntegritySafeAuditReplica `json:"replicas"    yaml:"replicas"`
	NetworkID  string                             `json:"network_id"  yaml:"network_id"`
	SchemaHash string                             `json:"schema_hash" yaml:"schema_hash"`
}

type StorageIntegritySafeAuditReplica struct {
	ReplicaID string `json:"replica_id" yaml:"replica_id"`
}

type StorageIntegrityWorkersConfig struct {
	PollInterval     Duration `json:"poll_interval"     yaml:"poll_interval"`
	Replay           bool     `json:"replay"            yaml:"replay"`
	UnsafeValidation bool     `json:"unsafe_validation" yaml:"unsafe_validation"`
	Promotion        bool     `json:"promotion"         yaml:"promotion"`
	Rollback         bool     `json:"rollback"          yaml:"rollback"`
	SafeAudit        bool     `json:"safe_audit"        yaml:"safe_audit"`
	Finality         bool     `json:"finality"          yaml:"finality"`
}

func defaultStorageIntegrityConfig() StorageIntegrityConfig {
	return StorageIntegrityConfig{
		Enabled:           false,
		UnsafeDatabase:    "hg_unsafe",
		SafeDatabase:      "hg_safe",
		UnsafeTableSuffix: "_a",
		HouseKeeper: StorageIntegrityHouseKeeper{
			Root:           "/housekeeper/v1/storage_integrity",
			ReplayQuorum:   2,
			SessionTimeout: Duration{Duration: 10 * time.Second},
		},
		Workers: StorageIntegrityWorkersConfig{
			PollInterval:     Duration{Duration: time.Second},
			Replay:           true,
			UnsafeValidation: true,
			Promotion:        true,
			Rollback:         true,
			SafeAudit:        true,
			Finality:         true,
		},
	}
}

func (c StorageIntegrityConfig) validate(mode Mode) error {
	if !c.Enabled {
		return nil
	}
	var errs []error
	if mode != ModeServer {
		errs = append(errs, errors.New("storage_integrity is server mode only"))
	}
	if c.MockPayloadStore.Path == "" {
		errs = append(errs, errors.New("storage_integrity.mock_payload_store.path is required when storage_integrity.enabled"))
	}
	if c.Workers.PollInterval.Duration <= 0 {
		errs = append(errs, fmt.Errorf("storage_integrity.workers.poll_interval must be > 0 (got %s)", c.Workers.PollInterval.Duration))
	}
	if c.MockFinality.Delay.Duration < 0 {
		errs = append(errs, fmt.Errorf("storage_integrity.mock_finality.delay must be >= 0 (got %s)", c.MockFinality.Delay.Duration))
	}
	for i, partitionID := range c.MockPartRegistry.PartitionIDs {
		if strings.TrimSpace(partitionID) == "" {
			errs = append(errs, fmt.Errorf("storage_integrity.mock_part_registry.partition_ids[%d] is required", i))
		}
	}
	if len(c.HouseKeeper.Endpoints) > 0 {
		if c.HouseKeeper.ReplayQuorum <= 0 {
			errs = append(errs, fmt.Errorf("storage_integrity.housekeeper.replay_quorum must be > 0 (got %d)", c.HouseKeeper.ReplayQuorum))
		}
		if c.HouseKeeper.SessionTimeout.Duration < 0 {
			errs = append(errs, fmt.Errorf("storage_integrity.housekeeper.session_timeout must be >= 0 (got %s)", c.HouseKeeper.SessionTimeout.Duration))
		}
		for i, endpoint := range c.HouseKeeper.Endpoints {
			if endpoint == "" {
				errs = append(errs, fmt.Errorf("storage_integrity.housekeeper.endpoints[%d] is required", i))
			}
		}
	}
	if c.UnsafeValidation.QueryTimeout.Duration < 0 {
		errs = append(errs, fmt.Errorf("storage_integrity.unsafe_validation.query_timeout must be >= 0 (got %s)", c.UnsafeValidation.QueryTimeout.Duration))
	}
	seenReplicas := map[string]struct{}{}
	for i, replica := range c.UnsafeValidation.Replicas {
		if replica.ReplicaID == "" {
			errs = append(errs, fmt.Errorf("storage_integrity.unsafe_validation.replicas[%d].replica_id is required", i))
		}
		if replica.Addr == "" {
			errs = append(errs, fmt.Errorf("storage_integrity.unsafe_validation.replicas[%d].addr is required", i))
		}
		if replica.ReplicaID != "" {
			if _, ok := seenReplicas[replica.ReplicaID]; ok {
				errs = append(errs, fmt.Errorf("storage_integrity.unsafe_validation.replicas[%d].replica_id %q is duplicated", i, replica.ReplicaID))
			}
			seenReplicas[replica.ReplicaID] = struct{}{}
		}
	}
	seenAuditReplicas := map[string]struct{}{}
	for i, replica := range c.SafeAudit.Replicas {
		if replica.ReplicaID == "" {
			errs = append(errs, fmt.Errorf("storage_integrity.safe_audit.replicas[%d].replica_id is required", i))
			continue
		}
		if _, ok := seenAuditReplicas[replica.ReplicaID]; ok {
			errs = append(errs, fmt.Errorf("storage_integrity.safe_audit.replicas[%d].replica_id %q is duplicated", i, replica.ReplicaID))
		}
		seenAuditReplicas[replica.ReplicaID] = struct{}{}
	}
	return errors.Join(errs...)
}
