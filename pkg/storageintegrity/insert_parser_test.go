package storageintegrity

import (
	"strings"
	"testing"
)

func TestLeadingKeywords(t *testing.T) {
	for sql, want := range map[string]string{
		"SET max_threads = 1":                    "SET,MAX_THREADS",
		"  -- c\n/* x */ create temporary table": "CREATE,TEMPORARY",
		"START TRANSACTION":                      "START,TRANSACTION",
		"begin":                                  "BEGIN",
		"(SELECT 1)":                             "",
	} {
		got, err := LeadingKeywords(sql, 2)
		if err != nil || strings.Join(got, ",") != want {
			t.Errorf("LeadingKeywords(%q) = %v, %v; want %q", sql, got, err, want)
		}
	}
}

func TestLeadingKeywordsLexerErrors(t *testing.T) {
	for _, sql := range []string{"/* unterminated", "#bare hash", "/* /* nested */ */ SET x = 1"} {
		if got, err := LeadingKeywords(sql, 2); err == nil {
			t.Errorf("LeadingKeywords(%q) = %v, nil; want the lexer error", sql, got)
		}
	}
}
