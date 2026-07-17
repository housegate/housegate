package sqlident

import "testing"

func TestNormalizeAndQuoteSQLIdentifier(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantNorm  string
		wantQuote string
	}{
		{
			name:      "bare path",
			input:     " tenant . events ",
			wantNorm:  "tenant.events",
			wantQuote: "`tenant`.`events`",
		},
		{
			name:      "quoted segment",
			input:     "`tenant`.`event.table`",
			wantNorm:  "tenant.`event.table`",
			wantQuote: "`tenant`.`event.table`",
		},
		{
			name:      "escaped backtick",
			input:     "`tenant`.`event``table`",
			wantNorm:  "tenant.`event``table`",
			wantQuote: "`tenant`.`event``table`",
		},
		{
			name:      "empty segment fails closed",
			input:     "tenant..events",
			wantNorm:  "",
			wantQuote: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NormalizePath(tt.input); got != tt.wantNorm {
				t.Fatalf("NormalizePath(%q) = %q, want %q", tt.input, got, tt.wantNorm)
			}
			if got := QuotePath(tt.input); got != tt.wantQuote {
				t.Fatalf("QuotePath(%q) = %q, want %q", tt.input, got, tt.wantQuote)
			}
		})
	}
}

func TestNormalizePathPreservesQuotedSegmentBoundaries(t *testing.T) {
	quotedDotted := NormalizePath("tenant.`event.table`")
	threeSegments := NormalizePath("tenant.event.table")
	if quotedDotted == "" || threeSegments == "" {
		t.Fatalf("normalized paths must be non-empty: quoted=%q bare=%q", quotedDotted, threeSegments)
	}
	if quotedDotted == threeSegments {
		t.Fatalf("quoted dotted table collided with three-segment path: %q", quotedDotted)
	}
	if quotedDotted != "tenant.`event.table`" {
		t.Fatalf("quoted dotted table normalized to %q, want tenant.`event.table`", quotedDotted)
	}
}

func TestSplitLastPath(t *testing.T) {
	db, table := SplitLastPath("tenant.events")
	if db != "tenant" || table != "events" {
		t.Fatalf("SplitLastPath = %q/%q, want tenant/events", db, table)
	}
	db, table = SplitLastPath("events")
	if db != "" || table != "events" {
		t.Fatalf("SplitLastPath bare = %q/%q, want empty/events", db, table)
	}
	db, table = SplitLastPath("`event.table`")
	if db != "" || table != "`event.table`" {
		t.Fatalf("SplitLastPath quoted bare = %q/%q, want empty/`event.table`", db, table)
	}
}
