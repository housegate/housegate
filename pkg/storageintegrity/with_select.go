package storageintegrity

import (
	"strings"

	"github.com/housegate/housegate/pkg/sqlsurface"
)

// WithListLeadsASelect reports whether sql, a statement starting with WITH,
// is a WITH list followed by a SELECT: ClickHouse also takes a WITH list
// before INSERT, so the leading keyword alone does not make a read. It reads
// the list's top-level shape only, [RECURSIVE] then elements separated by
// commas, each `name AS (subquery)` or `expression AS alias`, and answers
// false for anything it cannot read that way, so a statement it cannot
// classify is never failed over.
func WithListLeadsASelect(sql string) bool {
	surfaces, err := sqlsurface.ScanWith(sql, sqlsurface.Options{AllowStringEscapes: true})
	if err != nil {
		return false
	}
	toks := surfaces.Tokens
	word := func(i int, w string) bool {
		return i < len(toks) && toks[i].Kind == sqlsurface.TokenWord && strings.EqualFold(toks[i].Text, w)
	}
	punct := func(i int, p string) bool {
		return i < len(toks) && toks[i].Kind == sqlsurface.TokenPunct && toks[i].Text == p
	}
	// skipGroup returns the index just past the parenthesised group opened
	// at i, or -1 when it is not closed.
	skipGroup := func(i int) int {
		depth := 0
		for ; i < len(toks); i++ {
			switch {
			case punct(i, "("):
				depth++
			case punct(i, ")"):
				depth--
				if depth == 0 {
					return i + 1
				}
			}
		}
		return -1
	}
	i := 1 // past WITH
	if word(i, "RECURSIVE") {
		i++
	}
	for {
		// One element: everything up to its top-level AS, then a
		// parenthesised subquery or an alias name.
		start := i
		for i < len(toks) && !word(i, "AS") {
			if punct(i, ")") || punct(i, ",") {
				return false
			}
			if punct(i, "(") {
				if i = skipGroup(i); i < 0 {
					return false
				}
				continue
			}
			i++
		}
		if i == start || i >= len(toks) {
			return false
		}
		i++ // past AS
		switch {
		case punct(i, "("):
			if i = skipGroup(i); i < 0 {
				return false
			}
		case i < len(toks) && (toks[i].Kind == sqlsurface.TokenWord || toks[i].Kind == sqlsurface.TokenQuoted):
			i++
		default:
			return false
		}
		switch {
		case punct(i, ","):
			i++
		case word(i, "SELECT"):
			return true
		default:
			return false
		}
	}
}
