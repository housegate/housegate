package indexingusage

import "testing"

func TestMapTableTypeToSKU(t *testing.T) {
	cases := []struct {
		tableType string
		wantSKU   string
		wantOK    bool
	}{
		{"counter", "metric", true},
		{"gauge", "metric", true},
		{"event", "event", true},
		{"entity", "entity", true},
		// Case / whitespace normalization: table_type comes verbatim from
		// the on-chain TableCreated event and is not enum-validated, so a
		// casing/whitespace variant must still map (not silently drop).
		{"Counter", "metric", true},
		{"GAUGE", "metric", true},
		{" event ", "event", true},
		{"Entity", "entity", true},
		{"user", "", false},
		{"VIEW", "", false},
		{"MATERIALIZED_VIEW", "", false},
		{"", "", false},
		// Forward-compat: an unknown driver-added type drops on the
		// floor rather than over-bills. If the driver adds a new table
		// family we want a missing-SKU log line, not a guessed bucket.
		{"unknown_future_type", "", false},
	}
	for _, c := range cases {
		t.Run(c.tableType, func(t *testing.T) {
			gotSKU, gotOK := MapTableTypeToSKU(c.tableType)
			if gotSKU != c.wantSKU || gotOK != c.wantOK {
				t.Fatalf("MapTableTypeToSKU(%q) = (%q,%v), want (%q,%v)",
					c.tableType, gotSKU, gotOK, c.wantSKU, c.wantOK)
			}
		})
	}
}
