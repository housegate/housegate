package storageintegrity

import (
	"strings"
	"testing"

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
