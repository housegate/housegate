package storageintegrity

import (
	"fmt"
	"strings"
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

// IsHousegateOwnedSettingKey reports whether a ClickHouse query setting key
// is owned by housegate (auth token, statement token, payer, driver /
// maintenance / operator flags) and therefore excluded from settings_hash.
func IsHousegateOwnedSettingKey(key string) bool {
	return strings.HasPrefix(key, "SQL_x_") || strings.HasPrefix(key, "SQL_sentio_")
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
