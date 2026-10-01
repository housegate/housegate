package config

import (
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
