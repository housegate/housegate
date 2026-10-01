package sqlsurface

import (
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func TestScanSurfaces(t *testing.T) {
	s, err := Scan("SELECT 'hg_safe' AS x /* hg_unsafe */ FROM `db1`.t")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(Identifiers(s.WithLiterals), "hg_safe") {
		t.Fatalf("literal content missing from the literal surface: %q", s.WithLiterals)
	}
	if slices.Contains(Identifiers(s.OutsideLiterals), "hg_safe") {
		t.Fatalf("literal content leaked onto the executable surface: %q", s.OutsideLiterals)
	}
	for _, surface := range []string{s.OutsideLiterals, s.WithLiterals} {
		if strings.Contains(surface, "hg_unsafe") {
			t.Fatalf("comment content leaked: %q", surface)
		}
		if !slices.Contains(Identifiers(surface), "db1") {
			t.Fatalf("quoted identifier missing: %q", surface)
		}
	}
}

func TestScanTokens(t *testing.T) {
	s, err := Scan("SELECT a.b, `x.y`, 'it''s', $t$body$t$ FROM phys/* c */.t -- tail\nWHERE f (1) AND physé = 2")
	if err != nil {
		t.Fatal(err)
	}
	want := []Token{
		{TokenWord, "SELECT"}, {TokenWord, "a"}, {TokenPunct, "."}, {TokenWord, "b"}, {TokenPunct, ","},
		{TokenQuoted, "x.y"}, {TokenPunct, ","}, {TokenString, "it s"}, {TokenPunct, ","}, {TokenString, "body"},
		{TokenWord, "FROM"}, {TokenWord, "phys"}, {TokenPunct, "."}, {TokenWord, "t"},
		{TokenWord, "WHERE"}, {TokenWord, "f"}, {TokenPunct, "("}, {TokenWord, "1"}, {TokenPunct, ")"},
		{TokenWord, "AND"}, {TokenWord, "physé"}, {TokenPunct, "="}, {TokenWord, "2"},
	}
	if !reflect.DeepEqual(s.Tokens, want) {
		t.Fatalf("tokens =\n%v\nwant\n%v", s.Tokens, want)
	}
}

func TestScanAllowStringEscapes(t *testing.T) {
	s, err := ScanWith(`SELECT 'a\'b', 'c' FROM t`, Options{AllowStringEscapes: true})
	if err != nil {
		t.Fatal(err)
	}
	want := []Token{{TokenWord, "SELECT"}, {TokenString, `a\'b`}, {TokenPunct, ","}, {TokenString, "c"}, {TokenWord, "FROM"}, {TokenWord, "t"}}
	if !reflect.DeepEqual(s.Tokens, want) {
		t.Fatalf("tokens = %v, want %v", s.Tokens, want)
	}
}

func TestScanErrors(t *testing.T) {
	for _, tc := range []struct {
		sql  string
		opts Options
		want error
	}{
		{`SELECT 'a\'b'`, Options{}, ErrStringLiteralBackslash},
		{"SELECT `a\\x74`", Options{AllowStringEscapes: true}, ErrEscapedQuotedIdentifier},
		{`SELECT "a\x74"`, Options{}, ErrEscapedQuotedIdentifier},
		{"SELECT $ FROM t", Options{}, ErrStrayDollar},
	} {
		if _, err := ScanWith(tc.sql, tc.opts); !errors.Is(err, tc.want) {
			t.Errorf("ScanWith(%q) err = %v, want %v", tc.sql, err, tc.want)
		}
	}
	for _, sql := range []string{"SELECT 'x", "SELECT /* x", "SELECT `x", "SELECT $$x", `SELECT 'abc\`} {
		if _, err := ScanWith(sql, Options{AllowStringEscapes: true}); err == nil {
			t.Errorf("ScanWith(%q) accepted an unterminated span", sql)
		}
	}
}

func TestContainsIdentifierPlaceholder(t *testing.T) {
	for sql, want := range map[string]bool{
		"SELECT * FROM {p:Identifier}":     true,
		"SELECT * FROM { p : identifier }": true,
		"SELECT {v:UInt64}":                false,
		"SELECT 1":                         false,
	} {
		if got := ContainsIdentifierPlaceholder(sql); got != want {
			t.Errorf("ContainsIdentifierPlaceholder(%q) = %v, want %v", sql, got, want)
		}
	}
}
