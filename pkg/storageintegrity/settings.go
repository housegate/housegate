package storageintegrity

import (
	"fmt"
	"sort"

	"github.com/housegate/housegate/pkg/auth"
)

// SettingsHashDomain is the CanonicalDigest domain for the signed
// settings_hash. v1 commits to the empty user-settings set (spec D4).
const SettingsHashDomain = "housegate-settings-v1"

// EmptySettingsHash = replay.CanonicalDigest(SettingsHashDomain, []string{}).
// It is a constant so the Arbiter FSM and every housegate compare the same
// literal without recomputing; settings_test.go pins it to the derivation.
const EmptySettingsHash = "0x213f12b28bb47c05d226c4b86dad5b91e11d61040568a87dffe2ee87113ec006"

// EnvelopeVersionV2 is the only envelope_version the SI lane emits/admits.
const EnvelopeVersionV2 uint32 = 2

// ReadModeSettingKey is the per-query storage-integrity read-mode setting.
// It is declared here as well as in pkg/rewriter because this package must
// stay free of the rewriter's grpc/FFI dependencies; pkg/rewriter's
// read_mode_key_test.go asserts the two constants are equal.
const ReadModeSettingKey = "SQL_x_read_mode"

// housegateOwnedSettingKeys is the complete, enumerable set of ClickHouse
// query-setting keys HouseGate reserves, validates, or interprets and
// intentionally excludes from settings_hash. SQL_x_read_mode may be supplied
// by the client; HouseGate inspects and forwards it unchanged. Membership --
// not a SQL_x_ / SQL_sentio_ prefix -- prevents arbitrary unsigned settings
// from reaching ClickHouse while still hashing to the empty-settings digest
// (spec K 1f).
var housegateOwnedSettingKeys = map[string]bool{
	auth.AuthTokenSettingKey:        true,
	auth.StatementTokenSettingKey:   true,
	auth.PayerSettingKey:            true,
	auth.DriverSettingKey:           true,
	auth.MaintenanceSettingKey:      true,
	auth.PlatformOperatorSettingKey: true,
	ReadModeSettingKey:              true,
}

// HousegateOwnedSettingKeys returns the reserved, validated, or interpreted key
// set in sorted order. It exists so tests and operator diagnostics can
// enumerate it without reaching into the map.
func HousegateOwnedSettingKeys() []string {
	keys := make([]string, 0, len(housegateOwnedSettingKeys))
	for key := range housegateOwnedSettingKeys {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// IsHousegateOwnedSettingKey reports whether a ClickHouse query-setting key is
// reserved, validated, or interpreted by HouseGate and therefore excluded
// from settings_hash.
func IsHousegateOwnedSettingKey(key string) bool {
	return housegateOwnedSettingKeys[key]
}

// RejectUserSettings enforces the v1 empty-user-settings rule: every key that
// is not housegate-owned is a rejection naming the setting so writers learn
// why (e.g. async_insert / input_format_* cannot use the SI lane in v1).
func RejectUserSettings(keys []string) error {
	for _, key := range keys {
		if IsHousegateOwnedSettingKey(key) {
			continue
		}
		return fmt.Errorf("storage_integrity v1 does not admit client query setting %q on the SI lane; remove it or use a non-SI connection", key)
	}
	return nil
}
