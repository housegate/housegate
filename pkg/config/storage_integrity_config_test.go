package config

import (
	"strings"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/plugins/agent"
)

func TestStorageIntegrityDisabledIsNoOp(t *testing.T) {
	cfg := Default()
	if cfg.StorageIntegrity.Ingress.Enabled {
		t.Fatal("storage_integrity.ingress.enabled defaulted true, want false")
	}
	if cfg.StorageIntegrity.Ingress.RequestTimeout.Duration <= 0 {
		t.Fatalf("storage_integrity.ingress.request_timeout = %s, want non-zero default", cfg.StorageIntegrity.Ingress.RequestTimeout)
	}
	if cfg.StorageIntegrity.Ingress.MaxTokenAge.Duration != time.Minute {
		t.Fatalf("storage_integrity.ingress.max_token_age = %s, want 1m", cfg.StorageIntegrity.Ingress.MaxTokenAge)
	}
	if cfg.StorageIntegrity.Ingress.MaxPayloadBytes <= 0 {
		t.Fatalf("storage_integrity.ingress.max_payload_bytes = %d, want positive default", cfg.StorageIntegrity.Ingress.MaxPayloadBytes)
	}
	if cfg.StorageIntegrity.Runtime.Enabled {
		t.Fatal("storage_integrity.runtime.enabled defaulted true, want false")
	}
}

func TestStorageIntegrityIngressConfig_RequiresNetworkID(t *testing.T) {
	c := Default()
	c.Listen = "127.0.0.1:0"
	c.NetworkState.Source = "ns.yaml"
	c.StorageIntegrity.Ingress.Enabled = true
	c.StorageIntegrity.Ingress.AllowedAddresses = []string{"0x0000000000000000000000000000000000000001"}
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "network_id") {
		t.Fatalf("ingress without network_id must be rejected: %v", err)
	}
	c.StorageIntegrity.Ingress.NetworkID = "testnet-v2"
	if err := c.Validate(); err != nil {
		t.Fatalf("valid ingress config rejected: %v", err)
	}
}

func TestStorageIntegrityBackpressureDefaults(t *testing.T) {
	bp := Default().StorageIntegrity.Runtime.Backpressure
	if !bp.Enabled || bp.UnsafeDatabase != "hg_unsafe" || bp.SafeDatabase != "hg_safe" {
		t.Fatalf("defaults = %+v", bp)
	}
	if bp.PollInterval.Duration != 2*time.Second || bp.SoftPartsPerPartition != 2400 || bp.HardPartsPerPartition != 2950 {
		t.Fatalf("defaults = %+v", bp)
	}
}

func TestStorageIntegrity_DatabaseNamesArePinnedWithoutRuntime(t *testing.T) {
	cfg := Default()
	cfg.StorageIntegrity.Tables = []string{"db.t"}
	cfg.StorageIntegrity.Runtime.Enabled = false
	cfg.StorageIntegrity.Runtime.Backpressure.Enabled = true
	cfg.StorageIntegrity.Runtime.Backpressure.UnsafeDatabase = "hg_unsafe_typo"
	err := cfg.StorageIntegrity.validate(ModeServer)
	if err == nil || !strings.Contains(err.Error(), "unsafe_database") {
		t.Fatalf("validate = %v, want a pinned-name rejection", err)
	}

	cfg = Default()
	cfg.StorageIntegrity.Tables = []string{"db.t"}
	cfg.StorageIntegrity.Runtime.Backpressure.Enabled = false
	cfg.StorageIntegrity.Runtime.Backpressure.SafeDatabase = "hg_safe_typo"
	if err := cfg.StorageIntegrity.validate(ModeServer); err == nil ||
		!strings.Contains(err.Error(), "safe_database") {
		t.Fatalf("validate = %v, want a pinned-name rejection even with backpressure disabled", err)
	}
}

func TestStorageIntegrityPromoteDatabaseIsPinned(t *testing.T) {
	if StorageIntegrityPromoteDatabase != "hg_promote" {
		t.Fatalf("promote database pin = %q", StorageIntegrityPromoteDatabase)
	}
}

func TestStorageIntegrityBackpressure_RefreshTimeoutAndSnapshotTTL(t *testing.T) {
	base := func() Config {
		cfg := Default()
		cfg.StorageIntegrity.Tables = []string{"db.t"}
		cfg.StorageIntegrity.Ingress.Enabled = true
		cfg.StorageIntegrity.Ingress.NetworkID = "net"
		cfg.StorageIntegrity.Ingress.AllowedAddresses = []string{"0x1"}
		cfg.StorageIntegrity.Runtime.Enabled = true
		cfg.StorageIntegrity.Runtime.ExpectedSource = "node-1"
		cfg.StorageIntegrity.Runtime.JournalDir = "/tmp/j"
		cfg.StorageIntegrity.Runtime.PayloadSpoolDir = "/tmp/p"
		return cfg
	}
	if got := Default().StorageIntegrity.Runtime.Backpressure.RefreshTimeout.Duration; got != 2*time.Second {
		t.Fatalf("default refresh_timeout = %s, want 2s", got)
	}
	if got := Default().StorageIntegrity.Runtime.Backpressure.SnapshotTTL.Duration; got != 6*time.Second {
		t.Fatalf("default snapshot_ttl = %s, want 6s", got)
	}
	cfg := base()
	cfg.StorageIntegrity.Runtime.Backpressure.RefreshTimeout = Duration{}
	if err := cfg.StorageIntegrity.validate(ModeServer); err == nil ||
		!strings.Contains(err.Error(), "refresh_timeout") {
		t.Fatalf("zero refresh_timeout error = %v", err)
	}
	cfg = base()
	cfg.StorageIntegrity.Runtime.Backpressure.SnapshotTTL = Duration{Duration: time.Second}
	if err := cfg.StorageIntegrity.validate(ModeServer); err == nil ||
		!strings.Contains(err.Error(), "snapshot_ttl") {
		t.Fatalf("snapshot_ttl below refresh_timeout error = %v", err)
	}
}

func TestConfigValidateStorageIntegrityBackpressure(t *testing.T) {
	valid := func(t *testing.T) *Config {
		cfg := storageIntegrityRuntimeConfigFixture(t)
		cfg.StorageIntegrity.Runtime.Backpressure = StorageIntegrityRuntimeBackpressureConfig{
			Enabled: true, UnsafeDatabase: "hg_unsafe", SafeDatabase: "hg_safe",
			PollInterval: Duration{Duration: 2 * time.Second}, RefreshTimeout: Duration{Duration: 2 * time.Second},
			SnapshotTTL: Duration{Duration: 6 * time.Second}, SoftPartsPerPartition: 2400, HardPartsPerPartition: 2950,
		}
		return &cfg
	}
	if err := valid(t).Validate(); err != nil {
		t.Fatalf("valid runtime config: %v", err)
	}
	cases := map[string]struct {
		mutate func(*Config)
		want   string
	}{
		"soft must be positive":   {func(c *Config) { c.StorageIntegrity.Runtime.Backpressure.SoftPartsPerPartition = 0 }, "backpressure.soft_parts_per_partition"},
		"hard must exceed soft":   {func(c *Config) { c.StorageIntegrity.Runtime.Backpressure.HardPartsPerPartition = 2400 }, "backpressure.hard_parts_per_partition"},
		"hard below pinned throw": {func(c *Config) { c.StorageIntegrity.Runtime.Backpressure.HardPartsPerPartition = 3000 }, "backpressure.hard_parts_per_partition"},
		"poll interval positive":  {func(c *Config) { c.StorageIntegrity.Runtime.Backpressure.PollInterval.Duration = 0 }, "backpressure.poll_interval"},
		"unsafe database required": {func(c *Config) {
			c.StorageIntegrity.Runtime.Backpressure.UnsafeDatabase = " "
		}, "backpressure.unsafe_database"},
		"unsafe database is protocol owned": {func(c *Config) {
			c.StorageIntegrity.Runtime.Backpressure.UnsafeDatabase = "shadow_unsafe"
		}, `backpressure.unsafe_database must be "hg_unsafe"`},
		"safe database required": {func(c *Config) {
			c.StorageIntegrity.Runtime.Backpressure.SafeDatabase = " "
		}, "backpressure.safe_database"},
		"safe database is protocol owned": {func(c *Config) {
			c.StorageIntegrity.Runtime.Backpressure.SafeDatabase = "shadow_safe"
		}, `backpressure.safe_database must be "hg_safe"`},
		"safe and unsafe databases differ": {func(c *Config) {
			c.StorageIntegrity.Runtime.Backpressure.SafeDatabase = c.StorageIntegrity.Runtime.Backpressure.UnsafeDatabase
		}, "backpressure.safe_database must differ"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := valid(t)
			tc.mutate(cfg)
			err := cfg.Validate()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate err = %v, want %q", err, tc.want)
			}
		})
	}
	t.Run("disabled skips limit validation", func(t *testing.T) {
		cfg := valid(t)
		cfg.StorageIntegrity.Runtime.Backpressure.Enabled = false
		cfg.StorageIntegrity.Runtime.Backpressure.SoftPartsPerPartition = 0
		if err := cfg.Validate(); err != nil {
			t.Fatalf("disabled backpressure must not validate limits: %v", err)
		}
	})
}

func TestConfigValidateStorageIntegrityIngress(t *testing.T) {
	base := minimalServerConfig(t)
	base.StorageIntegrity.Ingress.Enabled = true
	base.StorageIntegrity.Ingress.NetworkID = "testnet-v2"
	base.StorageIntegrity.Ingress.AllowedAddresses = []string{"0x1111111111111111111111111111111111111111"}
	base.StorageIntegrity.Ingress.MaxTokenAge.Duration = time.Minute
	base.StorageIntegrity.Ingress.RequestTimeout.Duration = 5 * time.Second
	base.StorageIntegrity.Ingress.MaxPayloadBytes = defaultStorageIntegrityMaxPayloadBytes
	if err := base.Validate(); err != nil {
		t.Fatalf("storage integrity ingress server config should validate: %v", err)
	}

	t.Run("agent mode rejected", func(t *testing.T) {
		cfg := Config{
			Listen: ":9001",
			Agent:  agentConfigForStorageIntegrityTest(),
		}
		cfg.StorageIntegrity.Ingress.Enabled = true
		cfg.StorageIntegrity.Ingress.NetworkID = "testnet-v2"
		cfg.StorageIntegrity.Ingress.AllowedAddresses = []string{"0x1111111111111111111111111111111111111111"}
		cfg.StorageIntegrity.Ingress.MaxTokenAge.Duration = time.Minute
		cfg.StorageIntegrity.Ingress.RequestTimeout.Duration = 5 * time.Second
		cfg.StorageIntegrity.Ingress.MaxPayloadBytes = defaultStorageIntegrityMaxPayloadBytes
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "storage_integrity.ingress is server mode only") {
			t.Fatalf("Validate err = %v, want server-mode rejection", err)
		}
	})

	t.Run("signer allowlist required", func(t *testing.T) {
		cfg := minimalServerConfig(t)
		cfg.StorageIntegrity.Ingress.Enabled = true
		cfg.StorageIntegrity.Ingress.NetworkID = "testnet-v2"
		cfg.StorageIntegrity.Ingress.MaxPayloadBytes = defaultStorageIntegrityMaxPayloadBytes
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "allowed_addresses") {
			t.Fatalf("Validate err = %v, want allowlist rejection", err)
		}
	})

	t.Run("positive timeout required", func(t *testing.T) {
		cfg := minimalServerConfig(t)
		cfg.StorageIntegrity.Ingress.Enabled = true
		cfg.StorageIntegrity.Ingress.NetworkID = "testnet-v2"
		cfg.StorageIntegrity.Ingress.AllowedAddresses = []string{"0x1111111111111111111111111111111111111111"}
		cfg.StorageIntegrity.Ingress.MaxTokenAge.Duration = time.Minute
		cfg.StorageIntegrity.Ingress.RequestTimeout.Duration = 0
		cfg.StorageIntegrity.Ingress.MaxPayloadBytes = defaultStorageIntegrityMaxPayloadBytes
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "request_timeout") {
			t.Fatalf("Validate err = %v, want timeout rejection", err)
		}
	})

	t.Run("positive max payload required", func(t *testing.T) {
		cfg := minimalServerConfig(t)
		cfg.StorageIntegrity.Ingress.Enabled = true
		cfg.StorageIntegrity.Ingress.NetworkID = "testnet-v2"
		cfg.StorageIntegrity.Ingress.AllowedAddresses = []string{"0x1111111111111111111111111111111111111111"}
		cfg.StorageIntegrity.Ingress.MaxTokenAge.Duration = time.Minute
		cfg.StorageIntegrity.Ingress.RequestTimeout.Duration = 5 * time.Second
		cfg.StorageIntegrity.Ingress.MaxPayloadBytes = 0
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "max_payload_bytes") {
			t.Fatalf("Validate err = %v, want max payload rejection", err)
		}
	})

	t.Run("runtime enabled requires ingress", func(t *testing.T) {
		cfg := minimalServerConfig(t)
		cfg.StorageIntegrity.Runtime.Enabled = true
		cfg.StorageIntegrity.Runtime.ExpectedSource = "snode-A"
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "storage_integrity.runtime.enabled requires storage_integrity.ingress.enabled") {
			t.Fatalf("Validate err = %v, want runtime ingress dependency rejection", err)
		}
	})

	t.Run("runtime enabled requires expected source", func(t *testing.T) {
		cfg := minimalServerConfig(t)
		cfg.StorageIntegrity.Ingress.Enabled = true
		cfg.StorageIntegrity.Ingress.NetworkID = "testnet-v2"
		cfg.StorageIntegrity.Ingress.AllowedAddresses = []string{"0x1111111111111111111111111111111111111111"}
		cfg.StorageIntegrity.Ingress.MaxTokenAge.Duration = time.Minute
		cfg.StorageIntegrity.Ingress.RequestTimeout.Duration = 5 * time.Second
		cfg.StorageIntegrity.Ingress.MaxPayloadBytes = defaultStorageIntegrityMaxPayloadBytes
		cfg.StorageIntegrity.Runtime.Enabled = true
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "storage_integrity.runtime.expected_source") {
			t.Fatalf("Validate err = %v, want expected source rejection", err)
		}
	})

	t.Run("runtime enabled requires durable runtime config", func(t *testing.T) {
		cfg := minimalServerConfig(t)
		cfg.StorageIntegrity.Ingress.Enabled = true
		cfg.StorageIntegrity.Ingress.NetworkID = "testnet-v2"
		cfg.StorageIntegrity.Ingress.AllowedAddresses = []string{"0x1111111111111111111111111111111111111111"}
		cfg.StorageIntegrity.Ingress.MaxTokenAge.Duration = time.Minute
		cfg.StorageIntegrity.Ingress.RequestTimeout.Duration = 5 * time.Second
		cfg.StorageIntegrity.Ingress.MaxPayloadBytes = defaultStorageIntegrityMaxPayloadBytes
		cfg.StorageIntegrity.Runtime.Enabled = true
		cfg.StorageIntegrity.Runtime.ExpectedSource = "snode-A"
		err := cfg.Validate()
		if err == nil {
			t.Fatal("Validate succeeded without durable runtime config")
		}
		for _, want := range []string{
			"storage_integrity.runtime.journal_dir",
			"storage_integrity.runtime.payload_spool_dir",
			"storage_integrity.tables",
		} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("Validate err = %v, missing %q", err, want)
			}
		}
	})

	t.Run("runtime enabled requires storage_integrity.tables", func(t *testing.T) {
		cfg := storageIntegrityRuntimeConfigFixture(t)
		cfg.StorageIntegrity.Tables = nil
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "storage_integrity.tables is required when storage_integrity.runtime.enabled") {
			t.Fatalf("Validate err = %v", err)
		}
	})

	t.Run("legacy merge_guard.tables key is a pointed error", func(t *testing.T) {
		cfg := storageIntegrityRuntimeConfigFixture(t)
		cfg.StorageIntegrity.Runtime.MergeGuard.LegacyTables = []StorageIntegrityRuntimeMergeTableConfig{{Database: "hg_unsafe", Table: "events"}}
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "storage_integrity.runtime.merge_guard.tables was renamed to storage_integrity.tables") {
			t.Fatalf("Validate err = %v", err)
		}
	})

	t.Run("tables entries must be logical db.table ids, unique, server-mode only", func(t *testing.T) {
		cfg := storageIntegrityRuntimeConfigFixture(t)
		cfg.StorageIntegrity.Tables = []string{
			"tenant.events",
			"noDot",
			"a.b.c",
			"tenant.events",
			"1tenant.events",
			"tenant.event-name",
			"tenant .events",
		}
		err := cfg.Validate()
		if err == nil {
			t.Fatal("Validate succeeded with malformed table ids")
		}
		for _, want := range []string{
			`storage_integrity.tables[1] "noDot" must be a logical <database>.<table> id`,
			`storage_integrity.tables[2] "a.b.c" must be a logical <database>.<table> id`,
			`storage_integrity.tables[3] duplicates "tenant.events"`,
			`storage_integrity.tables[4] "1tenant.events" must be a logical <database>.<table> id`,
			`storage_integrity.tables[5] "tenant.event-name" must be a logical <database>.<table> id`,
			`storage_integrity.tables[6] "tenant .events" must be a logical <database>.<table> id`,
		} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("Validate err = %v, missing %q", err, want)
			}
		}
	})

	t.Run("tables reject non-injective physical names without runtime", func(t *testing.T) {
		cfg := minimalServerConfig(t)
		cfg.StorageIntegrity.Tables = []string{"a.b__c", "a__b.c"}
		err := cfg.Validate()
		if err == nil {
			t.Fatal("Validate succeeded with colliding storage-integrity physical table names")
		}
		for _, want := range []string{"a.b__c", "a__b.c", "a__b__c"} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("Validate err = %v, missing %q", err, want)
			}
		}
	})

	t.Run("read.default_mode validation", func(t *testing.T) {
		cfg := minimalServerConfig(t)
		cfg.StorageIntegrity.Tables = []string{"tenant.events"}
		for _, ok := range []string{"", "safe", "unsafe_latest"} {
			cfg.StorageIntegrity.Read.DefaultMode = ok
			if err := cfg.Validate(); err != nil {
				t.Fatalf("default_mode %q: %v", ok, err)
			}
		}
		cfg.StorageIntegrity.Read.DefaultMode = "latest"
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), `storage_integrity.read.default_mode "latest" must be safe or unsafe_latest`) {
			t.Fatalf("Validate err = %v", err)
		}
		cfg.StorageIntegrity.Read.DefaultMode = "safe"
		cfg.StorageIntegrity.Tables = nil
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "storage_integrity.read.default_mode requires storage_integrity.tables") {
			t.Fatalf("Validate err = %v", err)
		}
	})

	t.Run("agent mode error does not suppress table and read diagnostics", func(t *testing.T) {
		cfg := minimalServerConfig(t)
		cfg.StorageIntegrity.Agent.Enabled = true
		cfg.StorageIntegrity.Tables = []string{"noDot", "noDot"}
		cfg.StorageIntegrity.Read.DefaultMode = "latest"
		cfg.StorageIntegrity.Runtime.MergeGuard.LegacyTables = []StorageIntegrityRuntimeMergeTableConfig{{Database: "hg_unsafe", Table: "events"}}
		err := cfg.Validate()
		if err == nil {
			t.Fatal("Validate succeeded with invalid combined storage-integrity config")
		}
		for _, want := range []string{
			"storage_integrity.agent is agent mode only",
			"storage_integrity.runtime.merge_guard.tables was renamed to storage_integrity.tables",
			`storage_integrity.tables[0] "noDot" must be a logical <database>.<table> id`,
			`storage_integrity.tables[1] duplicates "noDot"`,
			`storage_integrity.read.default_mode "latest" must be safe or unsafe_latest`,
		} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("Validate err = %v, missing %q", err, want)
			}
		}
	})

	t.Run("runtime merge guard requires positive reassert interval", func(t *testing.T) {
		cfg := storageIntegrityRuntimeConfigFixture(t)
		cfg.StorageIntegrity.Runtime.MergeGuard.ReassertInterval.Duration = 0
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "storage_integrity.runtime.merge_guard.reassert_interval") {
			t.Fatalf("Validate err = %v, want reassert interval rejection", err)
		}
	})

	t.Run("runtime payload lease requires positive refresh policy", func(t *testing.T) {
		cfg := storageIntegrityRuntimeConfigFixture(t)
		cfg.StorageIntegrity.Runtime.PayloadLease.RefreshInterval.Duration = 0
		cfg.StorageIntegrity.Runtime.PayloadLease.RefreshBefore.Duration = 0
		err := cfg.Validate()
		if err == nil {
			t.Fatal("Validate succeeded with zero payload lease refresh policy")
		}
		for _, want := range []string{
			"storage_integrity.runtime.payload_lease.refresh_interval",
			"storage_integrity.runtime.payload_lease.refresh_before",
		} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("Validate err = %v, missing %q", err, want)
			}
		}
	})

	t.Run("runtime enabled accepts complete durable config", func(t *testing.T) {
		cfg := storageIntegrityRuntimeConfigFixture(t)
		if err := cfg.Validate(); err != nil {
			t.Fatalf("storage integrity runtime config should validate: %v", err)
		}
	})
}

func storageIntegrityRuntimeConfigFixture(t *testing.T) Config {
	t.Helper()
	cfg := minimalServerConfig(t)
	cfg.StorageIntegrity.Ingress.Enabled = true
	cfg.StorageIntegrity.Ingress.NetworkID = "testnet-v2"
	cfg.StorageIntegrity.Ingress.AllowedAddresses = []string{"0x1111111111111111111111111111111111111111"}
	cfg.StorageIntegrity.Ingress.MaxTokenAge.Duration = time.Minute
	cfg.StorageIntegrity.Ingress.RequestTimeout.Duration = 5 * time.Second
	cfg.StorageIntegrity.Ingress.MaxPayloadBytes = defaultStorageIntegrityMaxPayloadBytes
	cfg.StorageIntegrity.Runtime.Enabled = true
	cfg.StorageIntegrity.Runtime.ExpectedSource = "snode-A"
	cfg.StorageIntegrity.Runtime.JournalDir = "/var/lib/housegate/storage-integrity/journal"
	cfg.StorageIntegrity.Runtime.PayloadSpoolDir = "/var/lib/housegate/storage-integrity/payload-spool"
	cfg.StorageIntegrity.Runtime.PayloadLease.RefreshInterval.Duration = time.Second
	cfg.StorageIntegrity.Runtime.PayloadLease.RefreshBefore.Duration = 30 * time.Second
	cfg.StorageIntegrity.Runtime.MergeGuard.ReassertInterval.Duration = 30 * time.Second
	cfg.StorageIntegrity.Tables = []string{"tenant.events"}
	return cfg
}

func TestStorageIntegrityPhysicalNaming(t *testing.T) {
	if got := StorageIntegrityPhysicalTable("tenant.events"); got != "tenant__events" {
		t.Fatalf("physical = %q", got)
	}
	for _, good := range []string{"tenant.events", "_tenant.event2", "T1._events"} {
		db, table, ok := SplitStorageIntegrityTableID(good)
		if !ok || db+"."+table != good {
			t.Fatalf("split %q = %q %q %v", good, db, table, ok)
		}
	}
	for _, bad := range []string{
		"", "events", ".events", "tenant.", "a.b.c",
		"1tenant.events", "tenant.event-name", "tenant .events",
	} {
		if _, _, ok := SplitStorageIntegrityTableID(bad); ok {
			t.Fatalf("%q must be rejected", bad)
		}
	}
}

func TestConfigStorageIntegritySafeMerges(t *testing.T) {
	t.Run("defaults off", func(t *testing.T) {
		cfg := Default()
		if cfg.StorageIntegrity.SafeMerges.AllowNativeBackgroundMerges {
			t.Fatal("safe_merges.allow_native_background_merges defaulted true, want false")
		}
	})

	t.Run("enabling native merges rejected", func(t *testing.T) {
		cfg := minimalServerConfig(t)
		cfg.StorageIntegrity.Ingress.Enabled = true
		cfg.StorageIntegrity.Ingress.NetworkID = "testnet-v2"
		cfg.StorageIntegrity.Ingress.AllowedAddresses = []string{"0x1111111111111111111111111111111111111111"}
		cfg.StorageIntegrity.Ingress.MaxTokenAge.Duration = time.Minute
		cfg.StorageIntegrity.Ingress.RequestTimeout.Duration = 5 * time.Second
		cfg.StorageIntegrity.Ingress.MaxPayloadBytes = defaultStorageIntegrityMaxPayloadBytes
		cfg.StorageIntegrity.SafeMerges.AllowNativeBackgroundMerges = true
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "allow_native_background_merges") {
			t.Fatalf("Validate err = %v, want native-merges rejection", err)
		}
	})
}

func agentConfigForStorageIntegrityTest() agent.Config {
	return agent.Config{
		Mode:          true,
		Upstream:      "proxy:9001",
		PrivateKeyHex: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
}
