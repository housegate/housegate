package storageintegrity

import "testing"

func TestPhysicalTableName_D2Freeze(t *testing.T) {
	for input, want := range map[string]string{"db.t": "db__t", "orders.events": "orders__events", "a.b.c": "a__b__c", "plain": "plain"} {
		if got := PhysicalTableName(input); got != want {
			t.Fatalf("PhysicalTableName(%q) = %q want %q", input, got, want)
		}
	}
}
