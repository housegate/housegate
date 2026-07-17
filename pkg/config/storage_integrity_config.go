package config

import (
	"errors"
	"fmt"
	"time"
)

const defaultStorageIntegrityMaxPayloadBytes uint64 = 64 << 20

// StorageIntegrityConfig owns HouseGate-local storage-integrity toggles.
type StorageIntegrityConfig struct {
	Ingress StorageIntegrityIngressConfig `json:"ingress" yaml:"ingress"`
}

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
	if !c.Ingress.Enabled {
		return nil
	}
	var errs []error
	if mode != ModeServer {
		errs = append(errs, errors.New("storage_integrity.ingress is server mode only"))
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
