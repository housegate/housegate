package config

import (
	"encoding/json"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

func TestValidate_TableRefGuardMode(t *testing.T) {
	for _, mode := range []string{"", "enforce", "observe"} {
		c := minimalServerConfig(t)
		c.TableRefGuard.Mode = mode
		if err := c.Validate(); err != nil {
			t.Errorf("mode %q: %v", mode, err)
		}
	}
	c := minimalServerConfig(t)
	c.TableRefGuard.Mode = "audit"
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), `tableref_guard.mode "audit" is invalid`) {
		t.Fatalf("err = %v", err)
	}
	var fromYAML Config
	if err := yaml.Unmarshal([]byte("tableref_guard:\n  mode: observe\n"), &fromYAML); err != nil {
		t.Fatal(err)
	}
	if fromYAML.TableRefGuard.Mode != "observe" {
		t.Fatalf("yaml key tableref_guard.mode not read: %+v", fromYAML.TableRefGuard)
	}
}

func TestValidate_TableRefGuardModeIsCaseSensitiveAndNetworkStateSourceVariantChecksIt(t *testing.T) {
	c := minimalServerConfig(t)
	c.TableRefGuard.Mode = "Enforce"
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "tableref_guard.mode") {
		t.Fatalf("Validate accepted a differently-cased mode: %v", err)
	}
	if err := c.ValidateExceptNetworkStateSource(); err == nil || !strings.Contains(err.Error(), "tableref_guard.mode") {
		t.Fatalf("ValidateExceptNetworkStateSource accepted a differently-cased mode: %v", err)
	}
}

func TestConfig_TableRefGuardJSONKey(t *testing.T) {
	var fromJSON Config
	if err := json.Unmarshal([]byte(`{"tableref_guard": {"mode": "observe"}}`), &fromJSON); err != nil {
		t.Fatal(err)
	}
	if fromJSON.TableRefGuard.Mode != "observe" {
		t.Fatalf("json key tableref_guard.mode not read: %+v", fromJSON.TableRefGuard)
	}
}
