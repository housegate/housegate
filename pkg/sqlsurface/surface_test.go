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
	s, err := Scan("SELECT a.b, `x.y`, 'it''s', $t$body$t$ FROM phys/* c */.t -- tail\nWHERE f (1) AND `physé` = 2")
	if err != nil {
		t.Fatal(err)
	}
	want := []Token{
		{TokenWord, "SELECT"}, {TokenWord, "a"}, {TokenPunct, "."}, {TokenWord, "b"}, {TokenPunct, ","},
		{TokenQuoted, "x.y"}, {TokenPunct, ","}, {TokenString, "it s"}, {TokenPunct, ","}, {TokenString, "body"},
		{TokenWord, "FROM"}, {TokenWord, "phys"}, {TokenPunct, "."}, {TokenWord, "t"},
		{TokenWord, "WHERE"}, {TokenWord, "f"}, {TokenPunct, "("}, {TokenWord, "1"}, {TokenPunct, ")"},
		{TokenWord, "AND"}, {TokenQuoted, "physé"}, {TokenPunct, "="}, {TokenWord, "2"},
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

// TestScanNonASCIIOutsideQuotesIsRefused pins the rule measured on ClickHouse
// 26.8: outside quotes, literals, comments and heredoc bodies a non-ASCII
// byte is either Unicode whitespace (U+00A0, U+0085, U+200B, U+2028, U+3000,
// U+FEFF all separate tokens: "system<U+00A0>.one" reads system.one) or a
// syntax error ("SELECT 1 AS physé" fails on é). Gluing it into a word hid
// the qualifier in "phys<U+00A0>.t"; refusing it is never less safe than
// either ClickHouse outcome.
func TestScanNonASCIIOutsideQuotesIsRefused(t *testing.T) {
	for _, sql := range []string{
		"SELECT * FROM phys\u00a0.t",
		"SELECT * FROM phys\u3000.t",
		"SELECT * FROM phys\u200b.t",
		"SELECT * FROM phys\u0085.t",
		"SELECT * FROM phys\ufeff.t",
		"SELECT * FROM phys\u2028.t",
		"SELECT * FROM merge\u00a0(currentDatabase(), 't')",
		"SELECT 1 AS physé",
		"SELECT 1 \xff",
	} {
		if _, err := ScanWith(sql, Options{AllowStringEscapes: true}); !errors.Is(err, ErrNonASCII) {
			t.Errorf("ScanWith(%q) err = %v, want ErrNonASCII", sql, err)
		}
	}
	// Inside a span the bytes are content, exactly as ClickHouse reads them.
	for _, sql := range []string{
		"SELECT 'é\u00a0' AS `é`, \"é\" -- é\u00a0\nFROM t /* \u3000 */ # é\n",
		"SELECT $$é\u00a0$$, $t$é$t$",
	} {
		if _, err := Scan(sql); err != nil {
			t.Errorf("Scan(%q): %v", sql, err)
		}
	}
}

// TestScanTokenStream pins the token stream Task 4's table-reference guard
// reads. Number forms are deliberately over-split (1.e5, phys.1): ClickHouse
// lexes them as a number and tuple-element access (measured on 26.8), and a
// consumer must tolerate a qualifier-shaped sequence it does not need.
func TestScanTokenStream(t *testing.T) {
	w := func(s string) Token { return Token{TokenWord, s} }
	q := func(s string) Token { return Token{TokenQuoted, s} }
	str := func(s string) Token { return Token{TokenString, s} }
	p := func(s string) Token { return Token{TokenPunct, s} }
	for _, tc := range []struct {
		sql  string
		opts Options
		want []Token
	}{
		{"`a``b`", Options{}, []Token{q("a`b")}},
		{`"a""b"`, Options{}, []Token{q(`a"b`)}},
		{`"x"."y"`, Options{}, []Token{q("x"), p("."), q("y")}},
		{"`phys`.`t`", Options{}, []Token{q("phys"), p("."), q("t")}},
		{"phys\n.x", Options{}, []Token{w("phys"), p("."), w("x")}},
		{"phys /* c */ . x", Options{}, []Token{w("phys"), p("."), w("x")}},
		{"phys/* /* nested */ */.x", Options{}, []Token{w("phys"), p("."), w("x")}},
		{"phys -- c\r\n.x", Options{}, []Token{w("phys"), p("."), w("x")}},
		{"phys # c\n.x #! d", Options{}, []Token{w("phys"), p("."), w("x")}},
		{"phys\t\v\f\r.x", Options{}, []Token{w("phys"), p("."), w("x")}},
		{"SELECT 1.e5", Options{}, []Token{w("SELECT"), w("1"), p("."), w("e5")}},
		{"SELECT phys.1", Options{}, []Token{w("SELECT"), w("phys"), p("."), w("1")}},
		{"FROM {p:Identifier}", Options{}, []Token{w("FROM"), p("{"), w("p"), p(":"), w("Identifier"), p("}")}},
		{"'a''b'", Options{}, []Token{str("a b")}},
		{"$$a'b$$,$t$--$t$", Options{}, []Token{str("a'b"), p(","), str("--")}},
		{`'a\\', phys.t`, Options{AllowStringEscapes: true}, []Token{str(`a\\`), p(","), w("phys"), p("."), w("t")}},
		{`'a\'b', phys.t`, Options{AllowStringEscapes: true}, []Token{str(`a\'b`), p(","), w("phys"), p("."), w("t")}},
		{"a<=b", Options{}, []Token{w("a"), p("<"), p("="), w("b")}},
	} {
		s, err := ScanWith(tc.sql, tc.opts)
		if err != nil {
			t.Errorf("ScanWith(%q): %v", tc.sql, err)
			continue
		}
		if !reflect.DeepEqual(s.Tokens, tc.want) {
			t.Errorf("ScanWith(%q) tokens =\n%v\nwant\n%v", tc.sql, s.Tokens, tc.want)
		}
	}
	for _, tc := range []struct {
		sql  string
		want error
	}{
		{"SELECT $1", ErrStrayDollar},
		{"SELECT 1 AS x$$, * FROM phys.t AS y$$", ErrStrayDollar},
		{"SELECT * FROM phys .t", ErrNonASCII},
		{"SELECT 1 #x\n", ErrBareHash},
	} {
		if _, err := ScanWith(tc.sql, Options{AllowStringEscapes: true}); !errors.Is(err, tc.want) {
			t.Errorf("ScanWith(%q) err = %v, want %v", tc.sql, err, tc.want)
		}
	}
}
