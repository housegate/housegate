package storageintegrity

import (
	"errors"
	"fmt"
	"strings"
)

// InlineValuesErrorPrefix starts every named refusal of the signed inline
// VALUES lane, so an operator can tell an agent-side refusal from a server
// Exception. Errors from this file already carry it; callers forward them
// unchanged rather than wrapping them again.
const InlineValuesErrorPrefix = "storage_integrity inline VALUES: "

// ErrNotInlineValues means the statement is not a 26.x-shaped inline
// INSERT ... VALUES and keeps today's behavior: FORMAT, SELECT, WITH,
// non-INSERT text, and the 25.x truncated shape whose rows text is empty.
var ErrNotInlineValues = errors.New("not an inline VALUES insert")

// InlineValuesInsert is a decoded inline INSERT ... VALUES statement. Columns
// is nil when the statement has no column list. Rows is the verbatim text
// after the VALUES keyword with surrounding whitespace and at most one
// trailing ';' removed; the evaluator sends it byte for byte, so it must never
// be normalized here.
type InlineValuesInsert struct {
	Target  InsertTarget
	Columns []string
	Rows    string
}

// ParseInlineValuesInsert decodes a complete inline
// INSERT INTO [db.]t [(cols)] VALUES <rows> statement (spec 2026-09-23 D1),
// reusing the SI lane's target scanner and identifier rules. The inline-only
// prefix parser requires VALUES to follow the target or optional column list
// directly, so a later statement or arbitrary token cannot supply the rows.
// ErrNotInlineValues means "leave this statement on the ordinary path"; every
// other error is a named refusal carrying InlineValuesErrorPrefix.
func ParseInlineValuesInsert(sql string) (InlineValuesInsert, error) {
	parsed, err := parseInsertTarget(sql)
	if err != nil {
		if errors.Is(err, ErrNotInsert) {
			return InlineValuesInsert{}, ErrNotInlineValues
		}
		return InlineValuesInsert{}, fmt.Errorf("%s%w", InlineValuesErrorPrefix, err)
	}
	keys, err := InlineInsertSettingKeys(sql)
	if err != nil {
		return InlineValuesInsert{}, fmt.Errorf("%sinspect inline SETTINGS: %w", InlineValuesErrorPrefix, err)
	}
	if len(keys) > 0 {
		return InlineValuesInsert{}, fmt.Errorf("%sINSERT ... VALUES ... SETTINGS is not supported on the signed inline lane (setting %q)", InlineValuesErrorPrefix, keys[0])
	}
	cols, end, err := parseInlineValuesPrefix(sql, parsed.end)
	if err != nil {
		return InlineValuesInsert{}, err
	}
	rows, err := trimTrailingStatement(strings.TrimSpace(sql[end:]))
	if err != nil {
		return InlineValuesInsert{}, err
	}
	if rows == "" {
		// The 25.x truncated shape: the client streams the rows instead.
		return InlineValuesInsert{}, ErrNotInlineValues
	}
	return InlineValuesInsert{Target: parsed.target, Columns: cols, Rows: rows}, nil
}

// parseInlineValuesPrefix starts immediately after a parsed INSERT target and
// consumes only an optional column list followed by VALUES. It deliberately
// does not use insertDataSourceAt: that shared classifier scans the whole SQL
// for legacy callers, while this admission boundary must reject intervening
// statements and arbitrary syntax.
func parseInlineValuesPrefix(sql string, pos int) ([]string, int, error) {
	s := storageScanner{sql: sql, pos: pos}
	if err := s.skip(); err != nil {
		return nil, 0, fmt.Errorf("%s%w", InlineValuesErrorPrefix, err)
	}
	var cols []string
	if s.take('(') {
		for {
			name, _, ok, err := s.identifier()
			if err != nil {
				return nil, 0, fmt.Errorf("%s%w", InlineValuesErrorPrefix, err)
			}
			if !ok {
				return nil, 0, fmt.Errorf("%sempty column name in INSERT column list", InlineValuesErrorPrefix)
			}
			cols = append(cols, name)
			if err := s.skip(); err != nil {
				return nil, 0, fmt.Errorf("%s%w", InlineValuesErrorPrefix, err)
			}
			switch {
			case s.take(','):
				continue
			case s.take(')'):
				goto source
			default:
				return nil, 0, fmt.Errorf("%sexpected ',' or ')' in INSERT column list", InlineValuesErrorPrefix)
			}
		}
	}

source:
	if err := s.skip(); err != nil {
		return nil, 0, fmt.Errorf("%s%w", InlineValuesErrorPrefix, err)
	}
	if s.pos >= len(sql) {
		return nil, 0, ErrNotInlineValues
	}
	if sql[s.pos] == ';' {
		return nil, 0, inlinePrefixErr("multi-statement input is not supported; ';' appears before VALUES", s.pos)
	}
	word, ok, err := s.bareWord()
	if err != nil {
		return nil, 0, fmt.Errorf("%s%w", InlineValuesErrorPrefix, err)
	}
	if !ok {
		return nil, 0, inlinePrefixErr(fmt.Sprintf("token %q is not accepted between the INSERT target and VALUES", string(sql[s.pos])), s.pos)
	}
	if strings.EqualFold(word, "VALUES") {
		return cols, s.pos, nil
	}
	if isInsertPayloadSourceKeyword(word) {
		return nil, 0, ErrNotInlineValues
	}
	return nil, 0, inlinePrefixErr(fmt.Sprintf("token %q is not accepted between the INSERT target and VALUES", word), s.pos-len(word))
}

func inlinePrefixErr(reason string, offset int) error {
	return fmt.Errorf("%s%s at SQL byte offset %d", InlineValuesErrorPrefix, reason, offset)
}

// trimTrailingStatement removes at most one terminating ';' and refuses text
// after it (D1: multi-statement input is out of scope). Single-quoted spans are
// skipped permissively so a ';' inside a literal stays data; the literal's own
// escapes are validated later by ValuesClosure.
func trimTrailingStatement(rows string) (string, error) {
	for i := 0; i < len(rows); {
		switch rows[i] {
		case '\'':
			j := i + 1
			for j < len(rows) {
				if rows[j] == '\\' {
					j += 2
					continue
				}
				if rows[j] == '\'' {
					if j+1 < len(rows) && rows[j+1] == '\'' {
						j += 2
						continue
					}
					j++
					break
				}
				j++
			}
			i = j
		case ';':
			if strings.TrimSpace(rows[i+1:]) != "" {
				return "", fmt.Errorf("%smulti-statement input is not supported; text follows the ';' at byte offset %d", InlineValuesErrorPrefix, i)
			}
			return strings.TrimSpace(rows[:i]), nil
		default:
			i++
		}
	}
	return rows, nil
}

// refusedValuesKeywords are SQL keywords refused even when a '(' follows,
// which is what stops CAST(...), IN(...) and NOT(...) from passing the
// function-call rule. Every other non-call word is refused as a bare
// identifier, so this set buys clearer errors, not coverage.
var refusedValuesKeywords = map[string]bool{
	"select": true, "from": true, "with": true, "in": true, "null": true, "default": true,
	"cast": true, "as": true, "interval": true, "and": true, "or": true, "not": true,
}

// ValuesClosure is the lexical closure gate of spec 2026-09-23 D3. It accepts
// only parenthesized row tuples separated by commas, with a closed expression
// alphabet inside each tuple, so no signed row can depend on the clock, on
// randomness, on server state, or on another table. It is a deny-list policy
// gate, not an expression evaluator: ClickHouse's VALUES table function still
// refuses anything that is not a constant expression.
//
// Token grammar (the complete accepted alphabet; every other byte is refused):
//
//	rows     := tuple (space* ',' space* tuple)*
//	tuple    := '(' token+ ')'                 ; nested parentheses balance
//	token    := space | number | string | boolean | call | punct | operator
//	space    := ' ' | '\t' | '\n' | '\r' | '\f'
//	number   := '0' ('x'|'X') hex+ | digit+ ('.' digit*)? exponent?
//	exponent := ('e'|'E') ('+'|'-')? digit+
//	string   := "'" ( "''" | escape | not("'" or '\\') )* "'"
//	escape   := '\\' ("'" | '\\' | 'n' | 't') | '\\' 'x' hex hex
//	boolean  := "true" | "false"              ; case-insensitive
//	call     := word '('                       ; '(' immediately follows word
//	word     := [A-Za-z_] [A-Za-z0-9_]*
//	punct    := '(' | ')' | ','
//	operator := '+' | '-' | '*' | '/' | '%' | '=' | '!=' | '<>' | '<' | '<=' | '>' | '>='
//
// Refused with a named error and its byte offset in rows: comments ('--', '#',
// '/*' and '//', which ClickHouse also reads as a line comment), quoted
// identifiers ('`' and '"'), heredocs ('$'), query parameters ('{'), every name
// in refusedValuesKeywords, a word not immediately followed by '(' that is
// neither true nor false, a call whose lowercased name
// IsKnownNondeterministicName or IsServerStateFunctionName accepts, and any
// other byte, including ';', '[', ']', '?', '@' and ':'.
//
// The one scanner here that models heredocs and {name:Type} parameters is
// package-private in pkg/plugins/sireserved, and a core package cannot import a
// plugin package, so this lexer recognises both itself: it refuses on the first
// '$' or '{' outside a string literal rather than delimiting their bodies.
func ValuesClosure(rows string) error {
	if strings.TrimSpace(rows) == "" {
		return fmt.Errorf("%sthe VALUES row list is empty", InlineValuesErrorPrefix)
	}

	depth := 0
	expectTuple := true
	sawTuple := false
	tupleHasValue := false
	for i := 0; i < len(rows); {
		c := rows[i]
		if isStorageSpace(c) {
			i++
			continue
		}
		switch {
		case hasPrefixAt(rows, i, "--"), hasPrefixAt(rows, i, "/*"), hasPrefixAt(rows, i, "//"), c == '#':
			return closureErr("SQL comments are not accepted", i)
		case c == '`' || c == '"':
			return closureErr("quoted identifiers are not accepted", i)
		case c == '$':
			return closureErr("heredoc string literals are not accepted", i)
		case c == '{':
			return closureErr("query parameters are not accepted", i)
		}

		if depth == 0 {
			switch {
			case c == ')':
				return closureErr("unbalanced ')'", i)
			case c == ';' || c == '[' || c == ']' || c == '?' || c == '@' || c == ':':
				return closureErr(fmt.Sprintf("token %q is not accepted", string(c)), i)
			case expectTuple:
				if c != '(' {
					return closureErr("expected a parenthesized row tuple", i)
				}
				depth = 1
				expectTuple = false
				tupleHasValue = false
				i++
				continue
			case c == ',':
				expectTuple = true
				i++
				continue
			default:
				return closureErr("expected ',' between row tuples", i)
			}
		}

		switch {
		case c == '\'':
			next, err := scanClosureString(rows, i)
			if err != nil {
				return err
			}
			tupleHasValue = true
			i = next
		case c >= '0' && c <= '9':
			next, err := scanClosureNumber(rows, i)
			if err != nil {
				return err
			}
			tupleHasValue = true
			i = next
		case isStorageIdentStart(c):
			start := i
			for i++; i < len(rows) && isStorageIdentPart(rows[i]); i++ {
			}
			word := rows[start:i]
			lower := strings.ToLower(word)
			if refusedValuesKeywords[lower] {
				return closureErr(fmt.Sprintf("keyword %q is not accepted", word), start)
			}
			if i < len(rows) && rows[i] == '(' {
				if IsKnownNondeterministicName(lower) {
					return closureErr(fmt.Sprintf("nondeterministic function %q is not accepted", word), start)
				}
				if IsServerStateFunctionName(lower) {
					return closureErr(fmt.Sprintf("server-state function %q is not accepted", word), start)
				}
				tupleHasValue = true
				continue // the '(' is consumed by the next iteration
			}
			if lower != "true" && lower != "false" {
				return closureErr(fmt.Sprintf("bare identifier %q is not accepted", word), start)
			}
			tupleHasValue = true
		case c == '(':
			depth++
			i++
		case c == ')':
			depth--
			if depth == 0 {
				if !tupleHasValue {
					return closureErr("empty row tuple is not accepted", i)
				}
				sawTuple = true
			}
			i++
		case c == ',':
			i++
		case hasPrefixAt(rows, i, "!="), hasPrefixAt(rows, i, "<>"), hasPrefixAt(rows, i, "<="), hasPrefixAt(rows, i, ">="):
			i += 2
		case c == '+', c == '-', c == '*', c == '/', c == '%', c == '=', c == '<', c == '>':
			i++
		default:
			return closureErr(fmt.Sprintf("token %q is not accepted", string(c)), i)
		}
	}

	if depth != 0 {
		return fmt.Errorf("%sunbalanced '(' in the VALUES row list", InlineValuesErrorPrefix)
	}
	if expectTuple && sawTuple {
		return fmt.Errorf("%sexpected a row tuple after ',' in the VALUES row list", InlineValuesErrorPrefix)
	}
	return nil
}

func closureErr(reason string, offset int) error {
	return fmt.Errorf("%s%s at byte offset %d of the VALUES row list", InlineValuesErrorPrefix, reason, offset)
}

// scanClosureString returns the offset just past a single-quoted literal,
// admitting only the ClickHouse escapes spec D3 names.
func scanClosureString(rows string, start int) (int, error) {
	for i := start + 1; i < len(rows); {
		switch rows[i] {
		case '\\':
			if i+1 >= len(rows) {
				return 0, closureErr("unterminated string literal", start)
			}
			switch rows[i+1] {
			case '\'', '\\', 'n', 't':
				i += 2
			case 'x':
				if i+3 >= len(rows) || !isClosureHex(rows[i+2]) || !isClosureHex(rows[i+3]) {
					return 0, closureErr(`escape \x requires two hexadecimal digits`, i)
				}
				i += 4
			default:
				return 0, closureErr(fmt.Sprintf("string escape %q is not accepted", rows[i:i+2]), i)
			}
		case '\'':
			if i+1 < len(rows) && rows[i+1] == '\'' {
				i += 2
				continue
			}
			return i + 1, nil
		default:
			i++
		}
	}
	return 0, closureErr("unterminated string literal", start)
}

// scanClosureNumber returns the offset just past a numeric literal; a leading
// sign is an operator token, not part of the number. It enforces the token's
// own mandatory digits and refuses an identifier or another decimal point
// glued to the number instead of treating it as a second token.
func scanClosureNumber(rows string, start int) (int, error) {
	if rows[start] == '0' && start+1 < len(rows) && (rows[start+1] == 'x' || rows[start+1] == 'X') {
		i := start + 2
		if i >= len(rows) || !isClosureHex(rows[i]) {
			return 0, closureErr("hexadecimal literal requires at least one digit", start)
		}
		for ; i < len(rows) && isClosureHex(rows[i]); i++ {
		}
		if i < len(rows) && (isStorageIdentPart(rows[i]) || rows[i] == '.') {
			return 0, closureErr("numeric literal is not delimited", i)
		}
		return i, nil
	}

	i := scanClosureDigits(rows, start)
	if i < len(rows) && rows[i] == '.' {
		i = scanClosureDigits(rows, i+1)
	}
	if i < len(rows) && (rows[i] == 'e' || rows[i] == 'E') {
		exponent := i
		i++
		if i < len(rows) && (rows[i] == '+' || rows[i] == '-') {
			i++
		}
		if i >= len(rows) || rows[i] < '0' || rows[i] > '9' {
			return 0, closureErr("exponent requires at least one digit", exponent)
		}
		i = scanClosureDigits(rows, i)
	}
	if i < len(rows) && (isStorageIdentPart(rows[i]) || rows[i] == '.') {
		return 0, closureErr("numeric literal is not delimited", i)
	}
	return i, nil
}

func scanClosureDigits(rows string, i int) int {
	for ; i < len(rows) && rows[i] >= '0' && rows[i] <= '9'; i++ {
	}
	return i
}

func isClosureHex(ch byte) bool {
	return (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F')
}

func hasPrefixAt(s string, i int, prefix string) bool {
	return strings.HasPrefix(s[i:], prefix)
}
