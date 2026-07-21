package storageintegrity

import (
	"fmt"
	"strings"
)

// RequireStreamingNativeInsert accepts only INSERT forms whose row effects are
// represented by subsequent Native protocol Data packets. Inline VALUES,
// INSERT ... SELECT / WITH, and non-Native FORMAT inputs are outside P1 because
// their effects cannot be reconstructed from the captured Native payload.
func RequireStreamingNativeInsert(sql string) error {
	source, format, ok := insertDataSource(sql)
	if !ok {
		return fmt.Errorf("requires streaming Native INSERT input")
	}
	switch source {
	case "":
		return nil
	case "FORMAT":
		if format == "NATIVE" {
			return nil
		}
		if format == "" {
			return fmt.Errorf("requires streaming Native INSERT input; FORMAT without Native is not supported")
		}
		return fmt.Errorf("requires streaming Native INSERT input; FORMAT %s is not supported", format)
	case "VALUES", "SELECT", "WITH":
		return fmt.Errorf("requires streaming Native INSERT input; INSERT ... %s is not supported", source)
	default:
		return fmt.Errorf("requires streaming Native INSERT input")
	}
}

func insertDataSource(sql string) (source, format string, ok bool) {
	tok, pos, ok := nextStorageSQLToken(sql, 0)
	if !ok || tok.kind != storageSQLTokenWord || tok.text != "INSERT" {
		return "", "", false
	}

	depth := 0
	for {
		tok, next, ok := nextStorageSQLToken(sql, pos)
		if !ok {
			return "", "", true
		}
		pos = next
		switch tok.kind {
		case storageSQLTokenLParen:
			depth++
		case storageSQLTokenRParen:
			if depth > 0 {
				depth--
			}
		case storageSQLTokenWord:
			if depth != 0 {
				continue
			}
			switch tok.text {
			case "FORMAT":
				formatTok, _, formatOK := nextStorageSQLToken(sql, pos)
				if !formatOK || formatTok.kind != storageSQLTokenWord {
					return "FORMAT", "", true
				}
				return "FORMAT", formatTok.text, true
			case "VALUES", "SELECT", "WITH":
				return tok.text, "", true
			}
		}
	}
}

type storageSQLTokenKind int

const (
	storageSQLTokenOther storageSQLTokenKind = iota
	storageSQLTokenWord
	storageSQLTokenLParen
	storageSQLTokenRParen
)

type storageSQLToken struct {
	kind storageSQLTokenKind
	text string
}

func nextStorageSQLToken(sql string, pos int) (storageSQLToken, int, bool) {
	for {
		pos = skipStorageSQLSpaceAndComments(sql, pos)
		if pos >= len(sql) {
			return storageSQLToken{}, pos, false
		}
		switch sql[pos] {
		case '\'', '"', '`':
			pos = skipStorageSQLQuoted(sql, pos)
			continue
		}
		break
	}

	switch sql[pos] {
	case '(':
		return storageSQLToken{kind: storageSQLTokenLParen}, pos + 1, true
	case ')':
		return storageSQLToken{kind: storageSQLTokenRParen}, pos + 1, true
	}
	if isStorageSQLIdentStart(sql[pos]) {
		start := pos
		pos++
		for pos < len(sql) && isStorageSQLIdentPart(sql[pos]) {
			pos++
		}
		return storageSQLToken{kind: storageSQLTokenWord, text: strings.ToUpper(sql[start:pos])}, pos, true
	}
	return storageSQLToken{kind: storageSQLTokenOther}, pos + 1, true
}

func skipStorageSQLSpaceAndComments(sql string, pos int) int {
	for pos < len(sql) {
		switch {
		case sql[pos] == ' ' || sql[pos] == '\t' || sql[pos] == '\n' || sql[pos] == '\r' || sql[pos] == '\f':
			pos++
		case sql[pos] == '#':
			for pos < len(sql) && sql[pos] != '\n' {
				pos++
			}
		case sql[pos] == '-' && pos+1 < len(sql) && sql[pos+1] == '-':
			pos += 2
			for pos < len(sql) && sql[pos] != '\n' {
				pos++
			}
		case sql[pos] == '/' && pos+1 < len(sql) && sql[pos+1] == '*':
			pos += 2
			for pos+1 < len(sql) {
				if sql[pos] == '*' && sql[pos+1] == '/' {
					pos += 2
					break
				}
				pos++
			}
		default:
			return pos
		}
	}
	return pos
}

func skipStorageSQLQuoted(sql string, pos int) int {
	quote := sql[pos]
	pos++
	for pos < len(sql) {
		if quote == '\'' && sql[pos] == '\\' && pos+1 < len(sql) {
			pos += 2
			continue
		}
		if sql[pos] == quote {
			pos++
			if pos < len(sql) && sql[pos] == quote {
				pos++
				continue
			}
			return pos
		}
		pos++
	}
	return pos
}

func isStorageSQLIdentStart(b byte) bool {
	return b == '_' || (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z')
}

func isStorageSQLIdentPart(b byte) bool {
	return isStorageSQLIdentStart(b) || (b >= '0' && b <= '9')
}
