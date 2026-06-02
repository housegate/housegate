package indexingusage

import "testing"

func boolPtr(b bool) *bool { return &b }

func TestParseLogComment(t *testing.T) {
	// Real fixture pulled from devnet system.query_log on 2026-05-27
	// (sentio-node-devnet-indexer-a, x2y2 backfill).
	const fixture = `{"chain_id":"1","current_block_number":"19623000","current_block_time":"2024-04-10T04:59:23Z","processor_id":"x2y2","processor_replica":0,"watching":false}`

	cases := []struct {
		name         string
		in           string
		wantOK       bool
		wantProc     string
		wantWatching *bool // nil = key absent
	}{
		{"devnet_fixture_backfill", fixture, true, "x2y2", boolPtr(false)},
		{"watching_true", `{"processor_id":"p","watching":true}`, true, "p", boolPtr(true)},
		{"surrounding_double_quotes_stripped",
			`"` + fixture + `"`, true, "x2y2", boolPtr(false)},
		{"surrounding_single_quotes_stripped",
			`'` + `{"processor_id":"p","watching":true}` + `'`, true, "p", boolPtr(true)},
		{"empty", "", false, "", nil},
		{"whitespace_only", "   ", false, "", nil},
		{"not_json", "hello", false, "", nil},
		// Extra fields are ignored (forward-compat with future driver additions).
		{"extra_fields_ignored",
			`{"processor_id":"x","watching":true,"future_field":42}`, true, "x", boolPtr(true)},
		// Missing processor_id is still parseable; we treat the bool
		// independently. Defaults to empty processor_id (caller uses
		// the database binding as the source of truth anyway).
		{"missing_processor_id",
			`{"watching":false}`, true, "", boolPtr(false)},
		// Present-but-no-watching-key: Watching must be nil so the caller
		// can distinguish it from an explicit false (and default to
		// watching=true / NOT backfill).
		{"missing_watching_key",
			`{"processor_id":"p"}`, true, "p", nil},
		{"empty_object", `{}`, true, "", nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := ParseLogComment(c.in)
			if ok != c.wantOK {
				t.Fatalf("ok = %v, want %v", ok, c.wantOK)
			}
			if !ok {
				return
			}
			if got.ProcessorID != c.wantProc {
				t.Errorf("ProcessorID = %q, want %q", got.ProcessorID, c.wantProc)
			}
			switch {
			case got.Watching == nil && c.wantWatching == nil:
				// match
			case got.Watching == nil || c.wantWatching == nil:
				t.Errorf("Watching = %v, want %v", got.Watching, c.wantWatching)
			case *got.Watching != *c.wantWatching:
				t.Errorf("Watching = %v, want %v", *got.Watching, *c.wantWatching)
			}
		})
	}
}
