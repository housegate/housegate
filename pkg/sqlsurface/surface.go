// Package sqlsurface splits ClickHouse SQL into the lexical views the
// proxy-side guards reason on (Spec N D1): the executable text with string
// and heredoc contents blanked, the same text with those contents kept, and a
// token stream without whitespace or comments. It never parses or rewrites
// SQL, and every span it cannot model with certainty is an error.
package sqlsurface

import (
	"errors"
	"fmt"
	"strings"
)

// Errors a scan reports for spans the guards refuse. The texts are the ones
// the storage-integrity reserved-name guard has always reported.
var (
	ErrStringLiteralBackslash  = errors.New("backslash-bearing single-quoted string literal is not accepted by the storage-integrity guard")
	ErrEscapedQuotedIdentifier = errors.New("escaped quoted identifier is not accepted by the storage-integrity guard")
	ErrStrayDollar             = errors.New("stray $ is not a heredoc opener and is not accepted by the storage-integrity guard")
	// ErrBareHash reports a # that opens no comment: ClickHouse reads only
	// "# " and "#!" as comment markers and rejects any other #.
	ErrBareHash = errors.New("# that is not followed by a space or ! opens no ClickHouse comment and is not accepted by the storage-integrity guard")
)

// TokenKind classifies a Token.
type TokenKind uint8

const (
	// TokenWord is a bare word, keyword or number: a run of [A-Za-z0-9_] and
	// non-ASCII bytes.
	TokenWord TokenKind = iota
	// TokenQuoted is a backtick or double-quoted identifier, delimiters
	// removed and doubled delimiters collapsed.
	TokenQuoted
	// TokenString is a single-quoted literal or a heredoc body, raw: escapes
	// are never decoded.
	TokenString
	// TokenPunct is any other single non-space byte.
	TokenPunct
)

// Token is one lexical token outside comments and whitespace.
type Token struct {
	Kind TokenKind
	Text string
}

// Surfaces are the lexical views of one statement.
type Surfaces struct {
	// OutsideLiterals keeps executable SQL but blanks comments and string
	// literals. It is the only surface used for placeholder syntax.
	OutsideLiterals string
	// WithLiterals additionally keeps string contents because ClickHouse
	// table functions interpret some literal arguments as identifiers.
	WithLiterals string
	// Tokens is the statement's token stream; comments and whitespace are
	// dropped, so adjacency in Tokens is adjacency in ClickHouse's lexer.
	Tokens []Token
}

// Options tune a scan.
type Options struct {
	// AllowStringEscapes keeps a backslash inside a single-quoted literal,
	// raw, and lets it escape the next byte (so \' does not close the
	// literal), instead of refusing the statement. Ordinary-session guards
	// set it: tenants legitimately write 'a\nb'. Privileged-session guards
	// leave it off, because decoding escapes is where an encoded name such
	// as hg\x5Fsafe would hide.
	AllowStringEscapes bool
}

// Scan is ScanWith with default options.
func Scan(sql string) (Surfaces, error) { return ScanWith(sql, Options{}) }

// ScanWith ignores every ClickHouse comment form, retains quoted identifiers,
// models heredoc string literals, and produces the two surfaces and the token
// stream. Case order is the lexer's precedence and matches the grammar: a
// single quote and a comment marker both outrank a heredoc opener, and a
// heredoc opener outranks everything inside its own body.
func ScanWith(sql string, opts Options) (Surfaces, error) {
	var outside, withLiterals strings.Builder
	outside.Grow(len(sql))
	withLiterals.Grow(len(sql))
	var tokens []Token
	word := -1 // start offset of the bare word being read, or -1
	flushWord := func(end int) {
		if word >= 0 {
			tokens = append(tokens, Token{Kind: TokenWord, Text: sql[word:end]})
			word = -1
		}
	}

	for i := 0; i < len(sql); {
		switch {
		case sql[i] == '\'':
			flushWord(i)
			outside.WriteByte(' ')
			withLiterals.WriteByte(' ')
			next, literal, err := consumeStringLiteral(sql, i, opts.AllowStringEscapes)
			if err != nil {
				return Surfaces{}, err
			}
			withLiterals.WriteString(literal)
			withLiterals.WriteByte(' ')
			tokens = append(tokens, Token{Kind: TokenString, Text: literal})
			i = next

		case sql[i] == '#' && !hasPrefixAt(sql, i, "# ") && !hasPrefixAt(sql, i, "#!"):
			return Surfaces{}, ErrBareHash

		case hasPrefixAt(sql, i, "--") || hasPrefixAt(sql, i, "//") || sql[i] == '#':
			flushWord(i)
			outside.WriteByte(' ')
			withLiterals.WriteByte(' ')
			i = consumeLineComment(sql, i)

		case hasPrefixAt(sql, i, "/*"):
			flushWord(i)
			outside.WriteByte(' ')
			withLiterals.WriteByte(' ')
			next, err := consumeBlockComment(sql, i)
			if err != nil {
				return Surfaces{}, err
			}
			i = next

		case sql[i] == '`' || sql[i] == '"':
			flushWord(i)
			outside.WriteByte(' ')
			withLiterals.WriteByte(' ')
			next, identifier, err := consumeQuotedIdentifier(sql, i, sql[i])
			if err != nil {
				return Surfaces{}, err
			}
			outside.WriteString(identifier)
			outside.WriteByte(' ')
			withLiterals.WriteString(identifier)
			withLiterals.WriteByte(' ')
			tokens = append(tokens, Token{Kind: TokenQuoted, Text: identifier})
			i = next

		case sql[i] == '$':
			// ClickHouse heredoc: $$body$$ or $tag$body$tag$. The body is a
			// string literal, so it is blanked from OutsideLiterals and written
			// verbatim to WithLiterals -- table functions read literal arguments
			// as identifiers, so merge($$hg_safe$$, ...) must still be caught.
			// A `$` that opens no well-formed heredoc is refused: copying it
			// through is what let a comment marker inside a heredoc blank the
			// rest of a statement from both surfaces (Spec N D1).
			flushWord(i)
			outside.WriteByte(' ')
			withLiterals.WriteByte(' ')
			next, body, err := consumeHeredoc(sql, i)
			if err != nil {
				return Surfaces{}, err
			}
			withLiterals.WriteString(body)
			withLiterals.WriteByte(' ')
			tokens = append(tokens, Token{Kind: TokenString, Text: body})
			i = next

		default:
			b := sql[i]
			outside.WriteByte(b)
			withLiterals.WriteByte(b)
			switch {
			case isTokenWordByte(b):
				if word < 0 {
					word = i
				}
			case isSpace(b):
				flushWord(i)
			default:
				flushWord(i)
				tokens = append(tokens, Token{Kind: TokenPunct, Text: sql[i : i+1]})
			}
			i++
		}
	}
	flushWord(len(sql))
	return Surfaces{OutsideLiterals: outside.String(), WithLiterals: withLiterals.String(), Tokens: tokens}, nil
}

func consumeStringLiteral(sql string, start int, allowEscapes bool) (int, string, error) {
	var literal strings.Builder
	for i := start + 1; i < len(sql); {
		switch sql[i] {
		case '\\':
			if !allowEscapes {
				return 0, "", ErrStringLiteralBackslash
			}
			if i+1 >= len(sql) {
				return 0, "", fmt.Errorf("unterminated single-quoted string literal")
			}
			literal.WriteString(sql[i : i+2])
			i += 2
		case '\'':
			if i+1 < len(sql) && sql[i+1] == '\'' {
				literal.WriteByte(' ')
				i += 2
				continue
			}
			return i + 1, literal.String(), nil
		default:
			literal.WriteByte(sql[i])
			i++
		}
	}
	return 0, "", fmt.Errorf("unterminated single-quoted string literal")
}

// consumeHeredoc reads a ClickHouse heredoc string literal ($$body$$ or
// $tag$body$tag$) starting at the opening `$`, and returns the offset just
// past the closing $tag$ together with the body. Its contract matches
// consumeStringLiteral: an unterminated span is an error, never a silent
// truncation.
//
// The tag charset is [A-Za-z_][0-9A-Za-z_]* with an empty tag allowed, which
// is a deliberate strict SUBSET of the grammar's. Measured on the live v0.9.0
// polyglot grammar the engine also accepts a leading-digit tag ($1t$), a
// digits-only tag ($1$) and a non-ASCII tag, all of which this guard refuses
// through the stray-$ branch instead. Recognising fewer openers than the
// grammar only costs a false refusal, because an unrecognised `$` is refused
// rather than copied through; recognising more would let the guard blank a
// span the grammar executes, which is the shape of the bypass this closes.
//
// The closing delimiter is matched byte-exactly, so $tag$x$TAG$ is
// unterminated, and a lone `$` inside the body is content -- both measured on
// the same grammar.
//
// ClickHouse performs no escape processing inside a heredoc: merge($$hg\x5Fsafe$$,
// ...) is Success on the live engine and re-emits as merge('hg\\x5Fsafe', ...),
// so the body is the literal text hg\x5Fsafe and is not hg_safe. Heredoc bodies
// therefore do not inherit consumeStringLiteral's backslash refusal and are
// returned verbatim.
func consumeHeredoc(sql string, start int) (int, string, error) {
	tag := start + 1
	for tag < len(sql) && isHeredocTagByte(sql[tag], tag == start+1) {
		tag++
	}
	if tag >= len(sql) || sql[tag] != '$' {
		return 0, "", ErrStrayDollar
	}
	delimiter := sql[start : tag+1]
	body := sql[tag+1:]
	end := strings.Index(body, delimiter)
	if end < 0 {
		return 0, "", fmt.Errorf("unterminated heredoc string literal")
	}
	return tag + 1 + end + len(delimiter), body[:end], nil
}

// isHeredocTagByte reports whether value may appear in a heredoc tag. A digit
// is rejected in the leading position so a bare `$1` cannot be mistaken for an
// opener.
func isHeredocTagByte(value byte, leading bool) bool {
	if value == '_' || value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' {
		return true
	}
	return !leading && value >= '0' && value <= '9'
}

// ContainsIdentifierPlaceholder reports whether surface contains a ClickHouse {name:Identifier} query-parameter placeholder.
func ContainsIdentifierPlaceholder(sql string) bool {
	for offset := 0; offset < len(sql); {
		open := strings.IndexByte(sql[offset:], '{')
		if open < 0 {
			return false
		}
		open += offset
		close := strings.IndexByte(sql[open+1:], '}')
		if close < 0 {
			return false
		}
		close += open + 1
		name, parameterType, ok := strings.Cut(sql[open+1:close], ":")
		if ok && strings.TrimSpace(name) != "" && strings.EqualFold(strings.TrimSpace(parameterType), "Identifier") {
			return true
		}
		offset = close + 1
	}
	return false
}

// consumeLineComment returns the offset of the \n that ends a --, //, "# " or
// "#!" comment, or the end of input. Measured on ClickHouse 26.8 with every
// byte 0x00-0xFF after each marker, \n is the only terminator: \r, \v, \f,
// NUL and Unicode line separators are all comment text. Ending a comment
// anywhere else would resume lexing inside text ClickHouse ignores.
func consumeLineComment(sql string, start int) int {
	i := start
	for i < len(sql) && sql[i] != '\n' {
		i++
	}
	return i
}

func consumeBlockComment(sql string, start int) (int, error) {
	depth := 1
	for i := start + 2; i < len(sql); {
		switch {
		case hasPrefixAt(sql, i, "/*"):
			depth++
			i += 2
		case hasPrefixAt(sql, i, "*/"):
			depth--
			i += 2
			if depth == 0 {
				return i, nil
			}
		default:
			i++
		}
	}
	return 0, fmt.Errorf("unterminated block comment")
}

func consumeQuotedIdentifier(sql string, start int, delimiter byte) (int, string, error) {
	var identifier strings.Builder
	for i := start + 1; i < len(sql); {
		if sql[i] == '\\' {
			return 0, "", ErrEscapedQuotedIdentifier
		}
		if sql[i] != delimiter {
			identifier.WriteByte(sql[i])
			i++
			continue
		}
		if i+1 < len(sql) && sql[i+1] == delimiter {
			identifier.WriteByte(delimiter)
			i += 2
			continue
		}
		return i + 1, identifier.String(), nil
	}
	return 0, "", fmt.Errorf("unterminated quoted identifier")
}

// Identifiers returns the maximal runs of ASCII identifier bytes in sql, in order.
func Identifiers(sql string) []string {
	var result []string
	for start, i := -1, 0; i <= len(sql); i++ {
		if i < len(sql) && IsIdentifierByte(sql[i]) {
			if start < 0 {
				start = i
			}
			continue
		}
		if start >= 0 {
			result = append(result, sql[start:i])
			start = -1
		}
	}
	return result
}

// IsIdentifierByte reports whether value is an ASCII identifier byte: [A-Za-z0-9_].
func IsIdentifierByte(value byte) bool {
	return value == '_' || value >= 'a' && value <= 'z' ||
		value >= 'A' && value <= 'Z' || value >= '0' && value <= '9'
}

func hasPrefixAt(value string, offset int, prefix string) bool {
	return offset >= 0 && offset+len(prefix) <= len(value) && value[offset:offset+len(prefix)] == prefix
}

// isTokenWordByte reports a byte of a bare word in the token stream: an ASCII
// identifier byte or any byte of a multi-byte UTF-8 sequence.
func isTokenWordByte(b byte) bool { return IsIdentifierByte(b) || b >= 0x80 }

func isSpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r' || b == '\f' || b == '\v'
}
