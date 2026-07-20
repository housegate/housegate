package main

import (
	"strings"
	"testing"

	"housegate/housegate/pkg/config"
)

func TestValidateStandaloneRuntimeConfigRejectsStorageIntegrityIngress(t *testing.T) {
	cfg := config.Default()
	cfg.StorageIntegrity.Ingress.Enabled = true

	err := validateStandaloneRuntimeConfig(&cfg)
	if err == nil {
		t.Fatal("validateStandaloneRuntimeConfig returned nil, want storage-integrity ingress rejection")
	}
	if !strings.Contains(err.Error(), "standalone") {
		t.Fatalf("error = %q, want standalone context", err)
	}
	if !strings.Contains(err.Error(), "StorageIntegrityAdmissionConsumer") {
		t.Fatalf("error = %q, want consumer requirement", err)
	}
}

func TestValidateStandaloneRuntimeConfigAllowsDisabledStorageIntegrityIngress(t *testing.T) {
	cfg := config.Default()
	cfg.StorageIntegrity.Ingress.Enabled = false

	if err := validateStandaloneRuntimeConfig(&cfg); err != nil {
		t.Fatalf("validateStandaloneRuntimeConfig returned %v, want nil", err)
	}
}
