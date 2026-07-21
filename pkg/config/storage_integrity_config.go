package config

import (
	"errors"
	"fmt"
	"time"
)

const defaultStorageIntegrityMaxPayloadBytes uint64 = 64 << 20

// StorageIntegrityConfig owns HouseGate-local storage-integrity toggles.
type StorageIntegrityConfig struct {
	Ingress    StorageIntegrityIngressConfig    `json:"ingress"     yaml:"ingress"`
	SafeMerges StorageIntegritySafeMergesConfig `json:"safe_merges" yaml:"safe_merges"`
	Mutation   StorageIntegrityMutationConfig   `json:"mutation"    yaml:"mutation"`
}

// StorageIntegrityMutationConfig gates the P2 mutation runtime. It defaults off,
// is server-mode only, and enabling it is rejected in v1: the companion
// mutation-consensus (C2) seam does not exist, so the runtime cannot execute
// end to end. The toggle exists so the shape is versioned and the runtime can be
// turned on once C2 lands.
type StorageIntegrityMutationConfig struct {
	Enabled bool `json:"enabled" yaml:"enabled"`
}

// StorageIntegritySafeMergesConfig governs the P1e runtime's merge guard, which
// re-asserts SYSTEM STOP MERGES on the guarded tables at startup so the
// integrity layer owns the active part inventory. AllowNativeBackgroundMerges is
// a fail-closed escape hatch: it defaults false, and enabling it is rejected in
// v1 because native background merges would mutate the guarded inventory out
// from under the integrity layer.
//
// Enabled turns on P4 controlled compaction (design section 6): safe part layout
// may then change only through a manifest-selected input, the LtHash equation, a
// signed base-CAS REPLACE, and a new content-addressed manifest. It defaults off
// and enabling it is rejected in v1: the companion controlled-compaction (C4)
// seam does not exist, so the compaction runtime cannot execute end to end. Mode
// is the versioned strategy identifier (only "controlled_compaction" is defined);
// the toggle exists so the shape is versioned and the runtime can be turned on
// once C4 lands.
type StorageIntegritySafeMergesConfig struct {
	Enabled                     bool   `json:"enabled"                        yaml:"enabled"`
	Mode                        string `json:"mode"                           yaml:"mode"`
	AllowNativeBackgroundMerges bool   `json:"allow_native_background_merges" yaml:"allow_native_background_merges"`
}

// safeMergesModeControlledCompaction is the only defined safe-merges mode.
const safeMergesModeControlledCompaction = "controlled_compaction"

// StorageIntegrityIngressConfig is the server-side signed admission surface.
type StorageIntegrityIngressConfig struct {
	Enabled          bool     `json:"enabled"           yaml:"enabled"`
	AllowedAddresses []string `json:"allowed_addresses" yaml:"allowed_addresses"`
	MaxTokenAge      Duration `json:"max_token_age"     yaml:"max_token_age"`
	RequestTimeout   Duration `json:"request_timeout"  yaml:"request_timeout"`
	MaxPayloadBytes  uint64   `json:"max_payload_bytes" yaml:"max_payload_bytes"`
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
	// The mutation runtime is validated independently of ingress: a mutation
	// block enabled without ingress must still be rejected.
	if c.Mutation.Enabled {
		if mode != ModeServer {
			return fmt.Errorf("storage_integrity: %w", errors.New("storage_integrity.mutation is server mode only"))
		}
		return fmt.Errorf("storage_integrity: %w", errors.New("storage_integrity.mutation is not runnable in v1: companion mutation-consensus (C2) seam absent"))
	}
	// P4 controlled compaction is validated independently of ingress: a
	// safe_merges block enabled without ingress must still be rejected.
	if c.SafeMerges.Enabled {
		if mode != ModeServer {
			return fmt.Errorf("storage_integrity: %w", errors.New("storage_integrity.safe_merges is server mode only"))
		}
		if c.SafeMerges.Mode != "" && c.SafeMerges.Mode != safeMergesModeControlledCompaction {
			return fmt.Errorf("storage_integrity: %w", fmt.Errorf("storage_integrity.safe_merges.mode %q is not supported (only %q)", c.SafeMerges.Mode, safeMergesModeControlledCompaction))
		}
		return fmt.Errorf("storage_integrity: %w", errors.New("storage_integrity.safe_merges is not runnable in v1: companion controlled-compaction (C4) seam absent"))
	}
	if !c.Ingress.Enabled {
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
	if joined := errors.Join(errs...); joined != nil {
		return fmt.Errorf("storage_integrity: %w", joined)
	}
	return nil
}
