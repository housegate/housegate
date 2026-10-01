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

func tokenTexts(tokens []Token) []string {
	texts := make([]string, len(tokens))
	for i, token := range tokens {
		texts[i] = token.Text
	}
	return texts
}

// TestScanLineCommentEndsOnlyAtLineFeed pins ClickHouse's measured rule
// (26.8, every byte 0x00-0xFF tried after "--x", "#!x", "# x" and "//x"):
// a line comment ends at \n and nowhere else. A scanner that also stopped at
// \r resumed lexing inside text ClickHouse treats as comment, so an opener
// there (/*, ', `, ", $$) could blank real SQL that ClickHouse executes.
func TestScanLineCommentEndsOnlyAtLineFeed(t *testing.T) {
	want := []string{"SELECT", "*", "FROM", "phys", ".", "t"}
	for _, marker := range []string{"--", "#!", "# ", "//"} {
		for _, opener := range []string{"/*", "'", "`", `"`, "$$"} {
			closer := opener
			if opener == "/*" {
				closer = "*/"
			}
			sql := "SELECT * " + marker + " c\r" + opener + "\nFROM phys.t " + marker + " " + closer
			s, err := Scan(sql)
			if err != nil {
				t.Fatalf("Scan(%q): %v", sql, err)
			}
			if got := tokenTexts(s.Tokens); !reflect.DeepEqual(got, want) {
				t.Fatalf("Scan(%q) tokens = %q, want %q", sql, got, want)
			}
			if !strings.Contains(s.OutsideLiterals, "phys.t") {
				t.Fatalf("Scan(%q) blanked executed SQL: %q", sql, s.OutsideLiterals)
			}
		}
	}
	for _, sql := range []string{"SELECT 1 -- c\r+ 1", "SELECT 1 # c\r+ 1", "SELECT 1 #! c\r+ 1", "SELECT 1 // c\r+ 1", "SELECT 1 -- c\x00\v\f+ 1"} {
		s, err := Scan(sql)
		if err != nil {
			t.Fatalf("Scan(%q): %v", sql, err)
		}
		if got := tokenTexts(s.Tokens); !reflect.DeepEqual(got, []string{"SELECT", "1"}) {
			t.Fatalf("Scan(%q) tokens = %q, want [SELECT 1]", sql, got)
		}
	}
}

// TestScanBareHashIsRefused pins the other half of the measured comment
// grammar: # opens a comment only when followed by a space or !. Any other
// # (including one at the end of input) is a ClickHouse syntax error, so the
// scanner refuses it rather than inventing a comment.
func TestScanBareHashIsRefused(t *testing.T) {
	for _, sql := range []string{"SELECT 1 #x\n+ 1", "SELECT 1 #\tx\n+ 1", "SELECT 1 #", "SELECT 1 ##\n"} {
		if _, err := Scan(sql); !errors.Is(err, ErrBareHash) {
			t.Errorf("Scan(%q) err = %v, want ErrBareHash", sql, err)
		}
	}
}

// TestScanDollarAfterIdentifierByteIsRefused pins the ClickHouse rule measured
// on 26.8: a $ directly after an identifier byte stays inside that word
// ("SELECT 1 AS x$$, * FROM system.one AS y$$" reads system.one; a$b is one
// identifier), so it must never open a heredoc here. Modelling which words
// keep the $ (identifiers) and which do not (numbers: 1$$x$$ starts a
// heredoc) is not worth the risk; every such $ is refused.
func TestScanDollarAfterIdentifierByteIsRefused(t *testing.T) {
	for _, sql := range []string{
		"SELECT 1 AS x$$, * FROM phys.t AS y$$",
		"SELECT 1 AS x$t$, * FROM phys.t AS y$t$",
		"SELECT 1 AS _$$, 3",
		"SELECT 1$$x$$",
		"SELECT 1 AS a$b",
	} {
		if _, err := Scan(sql); !errors.Is(err, ErrStrayDollar) {
			t.Errorf("Scan(%q) err = %v, want ErrStrayDollar", sql, err)
		}
	}
	// A heredoc after a delimiter is still a heredoc.
	for _, sql := range []string{"SELECT $$x$$", "SELECT ($$x$$)", "SELECT f($t$x$t$,$$y$$)", "SELECT 1\n$$x$$"} {
		if _, err := Scan(sql); err != nil {
			t.Errorf("Scan(%q): %v", sql, err)
		}
	}
}
