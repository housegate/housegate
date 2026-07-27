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
	AllowedAddresses []string `json:"allowed_addresses" yaml:"allowed_addresses"`
	MaxTokenAge      Duration `json:"max_token_age"     yaml:"max_token_age"`
	RequestTimeout   Duration `json:"request_timeout"  yaml:"request_timeout"`
	MaxPayloadBytes  uint64   `json:"max_payload_bytes" yaml:"max_payload_bytes"`
}

// StorageIntegrityRuntimeConfig turns on HouseGate's built-in P1e runtime
// consumer. It still depends on host-injected ports for the real companion
// topology; the YAML owns only the fail-fast protocol intent.
type StorageIntegrityRuntimeConfig struct {
	Enabled         bool                                    `json:"enabled"           yaml:"enabled"`
	ExpectedSource  string                                  `json:"expected_source"   yaml:"expected_source"`
	JournalDir      string                                  `json:"journal_dir"       yaml:"journal_dir"`
	PayloadSpoolDir string                                  `json:"payload_spool_dir" yaml:"payload_spool_dir"`
	MergeGuard      StorageIntegrityRuntimeMergeGuardConfig `json:"merge_guard"       yaml:"merge_guard"`
}

// StorageIntegrityRuntimeMergeGuardConfig is the production table set that
// HouseGate guards with table-scoped SYSTEM STOP MERGES at startup.
type StorageIntegrityRuntimeMergeGuardConfig struct {
	Tables []StorageIntegrityRuntimeMergeTableConfig `json:"tables" yaml:"tables"`
}

// StorageIntegrityRuntimeMergeTableConfig identifies one ClickHouse table whose
// active-part inventory is owned by the storage-integrity runtime.
type StorageIntegrityRuntimeMergeTableConfig struct {
	Database string `json:"database" yaml:"database"`
	Table    string `json:"table"    yaml:"table"`
}

func defaultStorageIntegrityConfig() StorageIntegrityConfig {
	return StorageIntegrityConfig{
		Ingress: StorageIntegrityIngressConfig{
			Enabled:         false,
			MaxTokenAge:     Duration{Duration: time.Minute},
			RequestTimeout:  Duration{Duration: 5 * time.Second},
			MaxPayloadBytes: defaultStorageIntegrityMaxPayloadBytes,
		},
	}
}

func (c StorageIntegrityConfig) validate(mode Mode) error {
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
	}
	if joined := errors.Join(errs...); joined != nil {
		return fmt.Errorf("storage_integrity: %w", joined)
	}
	return nil
}
