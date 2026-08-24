package storageintegrity

import (
	"reflect"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/replay"
)

func TestEmptySettingsHashIsTheCanonicalEmptySetDigest(t *testing.T) {
	got, err := replay.CanonicalDigest(SettingsHashDomain, []string{})
	if err != nil {
		t.Fatalf("CanonicalDigest: %v", err)
	}
	if got != EmptySettingsHash {
		t.Fatalf("EmptySettingsHash = %s, canonical = %s", EmptySettingsHash, got)
	}
	if EmptySettingsHash != "0x213f12b28bb47c05d226c4b86dad5b91e11d61040568a87dffe2ee87113ec006" {
		t.Fatalf("EmptySettingsHash constant drifted: %s", EmptySettingsHash)
	}
}

func TestRejectUserSettingsStripsHousegateOwnedKeys(t *testing.T) {
	if err := RejectUserSettings([]string{"SQL_x_auth_token", "SQL_x_statement_token", "SQL_x_payer", "SQL_sentio_driver"}); err != nil {
		t.Fatalf("housegate-owned keys must be admitted: %v", err)
	}
	err := RejectUserSettings([]string{"SQL_x_auth_token", "async_insert"})
	if err == nil || !strings.Contains(err.Error(), "async_insert") {
		t.Fatalf("expected rejection naming async_insert, got %v", err)
	}
	if !IsHousegateOwnedSettingKey("SQL_sentio_maintenance") || IsHousegateOwnedSettingKey("max_threads") {
		t.Fatal("IsHousegateOwnedSettingKey prefix rules")
	}
}

func TestHousegateOwnedSettingKeysIsExactlyTheOwnedSet(t *testing.T) {
	want := []string{
		"SQL_sentio_driver",
		"SQL_sentio_maintenance",
		"SQL_sentio_platform_operator",
		"SQL_x_auth_token",
		"SQL_x_payer",
		"SQL_x_read_mode",
		"SQL_x_statement_token",
	}
	got := HousegateOwnedSettingKeys()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("HousegateOwnedSettingKeys() = %v, want %v", got, want)
	}
	for _, key := range want {
		if !IsHousegateOwnedSettingKey(key) {
			t.Fatalf("%s must be owned", key)
		}
	}
}

func TestRejectUserSettingsRejectsUnknownPrefixedKeys(t *testing.T) {
	// 1f: the prefix escape hatch let a client attach arbitrary unsigned
	// settings that still reached ClickHouse. Membership closes it.
	for _, key := range []string{
		"SQL_x_whatever",
		"SQL_x_",
		"SQL_sentio_anything",
		"sql_x_auth_token", // case-sensitive: ClickHouse settings are
		"SQL_x_auth_token_2",
		"async_insert",
		"max_threads",
	} {
		err := RejectUserSettings([]string{key})
		if err == nil {
			t.Fatalf("%q must be rejected", key)
		}
		if !strings.Contains(err.Error(), key) {
			t.Fatalf("rejection for %q must name the key, got %v", key, err)
		}
	}
}

func TestRejectUserSettingsAcceptsEveryOwnedKeyTogether(t *testing.T) {
	if err := RejectUserSettings(HousegateOwnedSettingKeys()); err != nil {
		t.Fatalf("the owned set must be admitted whole: %v", err)
	}
}

func TestOwnedSettingKeysTrackTheAuthConstants(t *testing.T) {
	// A rename in pkg/auth must not silently drop a key out of the owned set.
	for _, key := range []string{
		auth.AuthTokenSettingKey, auth.StatementTokenSettingKey, auth.PayerSettingKey,
		auth.DriverSettingKey, auth.MaintenanceSettingKey, auth.PlatformOperatorSettingKey,
	} {
		if !IsHousegateOwnedSettingKey(key) {
			t.Fatalf("pkg/auth constant %q is not in the owned set", key)
		}
	}
}
