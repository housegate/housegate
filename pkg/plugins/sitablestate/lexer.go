package sitablestate

import "strings"

// The rewriter's classification does not say whether a CREATE carries data:
// both engines report CREATE_TABLE for a plain CREATE and for
// CREATE ... AS SELECT, and CREATE_MATERIALIZED_VIEW with or without POPULATE
// or TO (measured against rewriter-go v0.11.0, 2026-09-24). This file reads
// only the top-level clause keywords of a CREATE header; it never rewrites.

type tokenKind uint8

const (
	tokWord   tokenKind = iota // bare word, compared case-insensitively
	tokIdent                   // `quoted` or "quoted" identifier, value unquoted
	tokString                  // 'string literal'
	tokPunct                   // one punctuation byte
)

type token struct {
	kind  tokenKind
	text  string
	depth int // parenthesis depth before the token
}

func (t token) isWord(upper string) bool {
	return t.kind == tokWord && strings.EqualFold(t.text, upper)
}

// tokenize splits sql into tokens, skipping whitespace and comments. An
// unterminated quote or comment ends the scan; the caller then sees fewer
// tokens and classifies conservatively.
func tokenize(sql string) []token {
	var out []token
	depth := 0
	for i := 0; i < len(sql); {
		c := sql[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f':
			i++
		case c == '-' && i+1 < len(sql) && sql[i+1] == '-', c == '#':
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < len(sql) && sql[i+1] == '*':
			end := strings.Index(sql[i+2:], "*/")
			if end < 0 {
				return out
			}
			i += end + 4
		case c == '\'' || c == '`' || c == '"':
			value, next, ok := readQuoted(sql, i)
			if !ok {
				return out
			}
			kind := tokIdent
			if c == '\'' {
				kind = tokString
			}
			out = append(out, token{kind: kind, text: value, depth: depth})
			i = next
		case isWordByte(c):
			start := i
			for i < len(sql) && isWordByte(sql[i]) {
				i++
			}
			out = append(out, token{kind: tokWord, text: sql[start:i], depth: depth})
		default:
			out = append(out, token{kind: tokPunct, text: string(c), depth: depth})
			if c == '(' {
				depth++
			} else if c == ')' && depth > 0 {
				depth--
			}
			i++
		}
	}
	return out
}

func readQuoted(sql string, start int) (string, int, bool) {
	quote := sql[start]
	var b strings.Builder
	for i := start + 1; i < len(sql); i++ {
		switch sql[i] {
		case '\\':
			if i+1 < len(sql) {
				b.WriteByte(sql[i+1])
				i++
			}
		case quote:
			if i+1 < len(sql) && sql[i+1] == quote {
				b.WriteByte(quote)
				i++
				continue
			}
			return b.String(), i + 1, true
		default:
			b.WriteByte(sql[i])
		}
	}
	return "", len(sql), false
}

func isWordByte(c byte) bool {
	return c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c >= 0x80
}

// bodyStart returns the index of the top-level AS that introduces a SELECT
// body (AS SELECT, AS WITH, AS (SELECT ...)), or -1.
func bodyStart(toks []token) int {
	for i, t := range toks {
		if t.depth != 0 || !t.isWord("AS") || i+1 >= len(toks) {
			continue
		}
		next := toks[i+1]
		if next.isWord("SELECT") || next.isWord("WITH") {
			return i
		}
		if next.kind == tokPunct && next.text == "(" && i+2 < len(toks) && (toks[i+2].isWord("SELECT") || toks[i+2].isWord("WITH")) {
			return i
		}
	}
	return -1
}

// createTableCarriesData reports CREATE TABLE ... AS SELECT without EMPTY.
func createTableCarriesData(sql string) bool {
	toks := tokenize(sql)
	body := bodyStart(toks)
	if body < 0 {
		return false
	}
	for _, t := range toks[:body] {
		if t.depth == 0 && t.isWord("EMPTY") {
			return false
		}
	}
	return true
}

// materializedViewHeader reads a CREATE MATERIALIZED VIEW header: whether it
// populates, and its TO target (database may be empty) when present.
func materializedViewHeader(sql string) (populate bool, toDatabase, toTable string, hasTo bool) {
	toks := tokenize(sql)
	end := bodyStart(toks)
	if end < 0 {
		end = len(toks)
	}
	for i := 0; i < end; i++ {
		t := toks[i]
		if t.depth != 0 {
			continue
		}
		if t.isWord("POPULATE") {
			populate = true
			continue
		}
		if !t.isWord("TO") || i+1 >= end {
			continue
		}
		next := toks[i+1]
		if next.isWord("DISK") || next.isWord("VOLUME") || (next.kind != tokWord && next.kind != tokIdent) {
			continue
		}
		toTable = next.text
		if i+3 < end && toks[i+2].kind == tokPunct && toks[i+2].text == "." && (toks[i+3].kind == tokWord || toks[i+3].kind == tokIdent) {
			toDatabase, toTable = next.text, toks[i+3].text
		}
		hasTo = true
	}
	return populate, toDatabase, toTable, hasTo
}
