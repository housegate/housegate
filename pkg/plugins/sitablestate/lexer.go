package sitablestate

import "strings"

// The rewriter's classification does not say whether a CREATE carries data:
// both engines report CREATE_TABLE for a plain CREATE and for
// CREATE ... AS SELECT, and CREATE_MATERIALIZED_VIEW with or without POPULATE
// or TO (measured against rewriter-go v0.11.0, 2026-09-24). This file reads
// only the top-level clause keywords of a CREATE header; it never rewrites.
// Against rewriter-go v0.13.0 the forwarded CREATE TABLE body normalises
// comments and heredocs and drops EMPTY AS SELECT bodies and CLONE, but it
// also drops a refreshable view's REFRESH ... TO clause, so the plugin lexes
// the forwarded body for CREATE TABLE and the original SQL for a view's TO.
//
// It is an allow-list, not a deny-list: a CREATE TABLE counts as schema-only
// only when the header proves it, and every span the scanner cannot model with
// certainty (an unterminated quote, comment or heredoc, a stray `$`, `#` or
// `//`, unbalanced parentheses) makes the whole statement uncertain, which the
// callers treat as data-carrying or unreadable. The span model follows
// ClickHouse's lexer: nested /* */ comments, `--` comments, `#` comments only
// when followed by a space or `!`, and $tag$...$tag$ heredocs with an empty or
// [A-Za-z_][0-9A-Za-z_]* tag.

type tokenKind uint8

const (
	tokWord   tokenKind = iota // bare word, compared case-insensitively
	tokIdent                   // `quoted` or "quoted" identifier, value unquoted
	tokString                  // 'string literal' or heredoc body
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

func (t token) isPunct(p string) bool {
	return t.kind == tokPunct && t.text == p
}

func (t token) isName() bool {
	return t.kind == tokWord || t.kind == tokIdent
}

// tokenize splits sql into tokens, skipping whitespace and comments. ok is
// false when any span cannot be modelled with certainty; the caller must then
// fail closed.
func tokenize(sql string) (out []token, ok bool) {
	depth := 0
	for i := 0; i < len(sql); {
		c := sql[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v':
			i++
		case c == '-' && i+1 < len(sql) && sql[i+1] == '-',
			c == '#' && i+1 < len(sql) && (sql[i+1] == ' ' || sql[i+1] == '!'):
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
		case c == '#':
			return nil, false
		case c == '/' && i+1 < len(sql) && sql[i+1] == '/':
			return nil, false
		case c == '/' && i+1 < len(sql) && sql[i+1] == '*':
			next, closed := skipBlockComment(sql, i)
			if !closed {
				return nil, false
			}
			i = next
		case c == '$':
			body, next, closed := readHeredoc(sql, i)
			if !closed {
				return nil, false
			}
			out = append(out, token{kind: tokString, text: body, depth: depth})
			i = next
		case c == '\'' || c == '`' || c == '"':
			value, next, closed := readQuoted(sql, i)
			if !closed {
				return nil, false
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
			} else if c == ')' {
				if depth == 0 {
					return nil, false
				}
				depth--
			}
			i++
		}
	}
	if depth != 0 {
		return nil, false
	}
	return out, true
}

// skipBlockComment skips a possibly nested /* */ comment starting at start.
func skipBlockComment(sql string, start int) (int, bool) {
	level := 0
	for i := start; i+1 < len(sql); {
		switch {
		case sql[i] == '/' && sql[i+1] == '*':
			level++
			i += 2
		case sql[i] == '*' && sql[i+1] == '/':
			level--
			i += 2
			if level == 0 {
				return i, true
			}
		default:
			i++
		}
	}
	return len(sql), false
}

// readHeredoc reads a $tag$...$tag$ heredoc starting at start. A `$` that does
// not open a well-formed, terminated heredoc is reported as not closed.
func readHeredoc(sql string, start int) (string, int, bool) {
	i := start + 1
	if i < len(sql) && (sql[i] == '_' || isASCIILetter(sql[i])) {
		for i < len(sql) && (sql[i] == '_' || isASCIILetter(sql[i]) || sql[i] >= '0' && sql[i] <= '9') {
			i++
		}
	}
	if i >= len(sql) || sql[i] != '$' {
		return "", len(sql), false
	}
	delim := sql[start : i+1]
	end := strings.Index(sql[i+1:], delim)
	if end < 0 {
		return "", len(sql), false
	}
	return sql[i+1 : i+1+end], i + 1 + end + len(delim), true
}

func readQuoted(sql string, start int) (string, int, bool) {
	quote := sql[start]
	var b strings.Builder
	for i := start + 1; i < len(sql); i++ {
		switch sql[i] {
		case '\\':
			if i+1 >= len(sql) {
				return "", len(sql), false
			}
			b.WriteByte(sql[i+1])
			i++
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

func isASCIILetter(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

func isWordByte(c byte) bool {
	return c == '_' || isASCIILetter(c) || c >= '0' && c <= '9' || c >= 0x80
}

// topLevelWords returns the indexes of depth-0 occurrences of word.
func topLevelWords(toks []token, word string) []int {
	var at []int
	for i, t := range toks {
		if t.depth == 0 && t.isWord(word) {
			at = append(at, i)
		}
	}
	return at
}

// qualifiedName reads a [db.]table name at toks[i]; next is the index after it.
func qualifiedName(toks []token, i int) (database, table string, next int, ok bool) {
	if i >= len(toks) || !toks[i].isName() {
		return "", "", i, false
	}
	if i+1 < len(toks) && toks[i+1].isPunct(".") {
		if i+2 >= len(toks) || !toks[i+2].isName() {
			return "", "", i, false
		}
		return toks[i].text, toks[i+2].text, i + 3, true
	}
	return "", toks[i].text, i + 1, true
}

// schemaCopyClauses are the clauses allowed after `AS [db.]table`: none of
// them carries rows.
var schemaCopyClauses = []string{"ENGINE", "ORDER", "PARTITION", "PRIMARY", "SAMPLE", "TTL", "SETTINGS", "COMMENT"}

// createTableCarriesData reports whether a CREATE TABLE may put rows into its
// target. It returns false only when the header proves the statement
// schema-only: (a) it has no top-level AS, or (b) its single top-level AS is
// followed by a bare [db.]table schema copy and then only by the end of the
// statement or non-data clauses. Everything else, including EMPTY AS SELECT,
// CLONE AS, AS table_function(...) and any uncertain scan, carries data.
func createTableCarriesData(sql string) bool {
	toks, ok := tokenize(sql)
	if !ok || len(topLevelWords(toks, "CLONE")) > 0 {
		return true
	}
	as := topLevelWords(toks, "AS")
	switch len(as) {
	case 0:
		return false
	case 1:
	default:
		return true
	}
	i := as[0] + 1
	if i < len(toks) && (toks[i].isWord("SELECT") || toks[i].isWord("WITH") || toks[i].isWord("FROM")) {
		return true
	}
	_, _, next, ok := qualifiedName(toks, i)
	if !ok {
		return true
	}
	if next < len(toks) && toks[next].isPunct(";") {
		return next+1 != len(toks)
	}
	if next == len(toks) {
		return false
	}
	for _, clause := range schemaCopyClauses {
		if toks[next].isWord(clause) {
			return false
		}
	}
	return true
}

// mvHeader is what the gate reads from a CREATE MATERIALIZED VIEW header: the
// view's own [db.]name and its TO target, if any.
type mvHeader struct {
	viewDatabase, view  string
	toDatabase, toTable string
	hasTo               bool
}

// materializedViewHeader reads the view name and the TO target of a
// CREATE [OR REPLACE] MATERIALIZED VIEW [IF NOT EXISTS] [db.]name header (the
// part before the first top-level AS), including a refreshable view's
// APPEND TO. TTL's TO DISK / TO VOLUME are not targets, and only words after
// the view name are target keywords. ok is false when the header cannot be
// read with certainty: an uncertain scan, no top-level AS, a statement that
// does not open with that grammar, more than one target, or a TO not followed
// by a [db.]table name.
func materializedViewHeader(sql string) (h mvHeader, ok bool) {
	toks, scanned := tokenize(sql)
	if !scanned {
		return mvHeader{}, false
	}
	as := topLevelWords(toks, "AS")
	if len(as) == 0 {
		return mvHeader{}, false
	}
	header := toks[:as[0]]
	i, opened := wordsAt(header, 0, "CREATE")
	if !opened {
		return mvHeader{}, false
	}
	if next, found := wordsAt(header, i, "OR", "REPLACE"); found {
		i = next
	}
	if i, opened = wordsAt(header, i, "MATERIALIZED", "VIEW"); !opened {
		return mvHeader{}, false
	}
	if next, found := wordsAt(header, i, "IF", "NOT", "EXISTS"); found {
		i = next
	}
	db, view, next, named := qualifiedName(header, i)
	if !named {
		return mvHeader{}, false
	}
	h.viewDatabase, h.view = db, view
	rest := header[next:]
	for _, at := range topLevelWords(rest, "TO") {
		if at+1 < len(rest) && (rest[at+1].isWord("DISK") || rest[at+1].isWord("VOLUME")) {
			continue
		}
		db, table, _, named := qualifiedName(rest, at+1)
		if !named || h.hasTo {
			return mvHeader{}, false
		}
		h.toDatabase, h.toTable, h.hasTo = db, table, true
	}
	return h, true
}

// wordsAt reports whether toks[i:] opens with the given bare words and returns
// the index after them.
func wordsAt(toks []token, i int, words ...string) (int, bool) {
	for _, w := range words {
		if i >= len(toks) || !toks[i].isWord(w) {
			return i, false
		}
		i++
	}
	return i, true
}
