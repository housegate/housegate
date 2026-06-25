package config

import (
	"errors"
	"fmt"
	"time"
)

// StorageIntegrityConfig gates the HouseGate-side workers for the
// HouseKeeper storage-integrity protocol. It is disabled by default and uses
// mock DA/finality adapters for the P0 implementation.
type StorageIntegrityConfig struct {
	Enabled          bool                             `json:"enabled"            yaml:"enabled"`
	MockPayloadStore StorageIntegrityMockPayloadStore `json:"mock_payload_store" yaml:"mock_payload_store"`
	MockFinality     StorageIntegrityMockFinality     `json:"mock_finality"      yaml:"mock_finality"`
	Workers          StorageIntegrityWorkersConfig    `json:"workers"           yaml:"workers"`
}

type StorageIntegrityMockPayloadStore struct {
	Path string `json:"path" yaml:"path"`
}

type StorageIntegrityMockFinality struct {
	Delay Duration `json:"delay" yaml:"delay"`
}

type StorageIntegrityWorkersConfig struct {
	PollInterval Duration `json:"poll_interval" yaml:"poll_interval"`
	Replay       bool     `json:"replay"        yaml:"replay"`
	Promotion    bool     `json:"promotion"     yaml:"promotion"`
	SafeAudit    bool     `json:"safe_audit"    yaml:"safe_audit"`
	Finality     bool     `json:"finality"      yaml:"finality"`
}

func defaultStorageIntegrityConfig() StorageIntegrityConfig {
	return StorageIntegrityConfig{
		Enabled: false,
		Workers: StorageIntegrityWorkersConfig{
			PollInterval: Duration{Duration: time.Second},
			Replay:       true,
			Promotion:    true,
			SafeAudit:    true,
			Finality:     true,
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
	return errors.Join(errs...)
}
