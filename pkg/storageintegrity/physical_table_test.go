package storageintegrity

import (
	"errors"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

func TestPhysicalTableName_D2Freeze(t *testing.T) {
	for input, want := range map[string]string{"db.t": "db__t", "orders.events": "orders__events", "a.b.c": "a__b__c", "plain": "plain"} {
		if got := PhysicalTableName(input); got != want {
			t.Fatalf("PhysicalTableName(%q) = %q want %q", input, got, want)
		}
	}
}

func TestValidatePhysicalTableNamesRejectsCollision(t *testing.T) {
	err := ValidatePhysicalTableNames([]payloadexec.TableSchema{{TableID: "a.b__c"}, {TableID: "a__b.c"}})
	if !errors.Is(err, ErrPhysicalTableNameCollision) {
		t.Fatalf("err = %v, want ErrPhysicalTableNameCollision", err)
	}
	for _, want := range []string{"a.b__c", "a__b.c", "a__b__c"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q missing %q", err, want)
		}
	}
}
