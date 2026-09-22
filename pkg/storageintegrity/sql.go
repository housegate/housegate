package storageintegrity

import (
	"fmt"
	"strings"
)

// clientParsedInsertFormats is the set of INSERT FORMATs the ClickHouse native
// TCP client parses locally, serializing the rows into Native ClientData blocks
// before they reach the wire. For every entry here the captured payload is
// byte-identical to what `FORMAT Native` produces, which is what makes them
// interchangeable for a signed statement: the SQL text differs (and is covered
// by sql_hash), while payload_hash commits to the same bytes and replay decodes
// them through the one Native decoder.
//
// This is deliberately an allowlist of MEASURED formats, not "any FORMAT". A
// client that shipped raw bytes for some format would produce a payload the
// Native decoder cannot read; intake fails closed on that (it decodes the
// payload to derive touched partitions), so the failure is a refusal at the
// door rather than anything reaching replay -- but the refusal would be
// confusing, and non-native clients are outside what these measurements cover.
// Extend it only alongside the byte-identity case in
// TestCLI_SignableInsertFormatsShareOneWirePayload.
//
// Alias pairs are listed together on purpose: ClickHouse treats TSV and
// TabSeparated (and their WithNames variants) as the same format, so admitting
// one spelling and refusing the other would reject a statement for how it was
// written rather than for what it does.
var clientParsedInsertFormats = map[string]bool{
	"NATIVE":                true,
	"VALUES":                true,
	"CSV":                   true,
	"CSVWITHNAMES":          true,
	"TSV":                   true,
	"TABSEPARATED":          true,
	"TSVWITHNAMES":          true,
	"TABSEPARATEDWITHNAMES": true,
	"JSONEACHROW":           true,
}

// InsertPayloadEncoding returns the replay payload encoding selected by an
// admitted payload-local INSERT. Server-side ingress captures ClickHouse
// native TCP ClientData packets: SQL FORMAT controls client-side parsing only,
// so every format in clientParsedInsertFormats arrives as Native blocks and is
// stored as the same wire capture.
//
// Note the version-qualified asymmetry with `INSERT ... VALUES (1)`: a 25.8
// client truncates the SQL after VALUES and streams the rows, the ordinary
// payload-local path; a 26.3+ client and the pinned clickhouse-go fork send the
// full statement text and no ClientData packet at all, so this function reports
// no payload encoding for that shape. An agent running
// storage_integrity.agent.inline_values evaluates those inline rows and
// rewrites the statement to FORMAT Native before it reaches this gate (spec
// 2026-09-23 D1). The same statement written as `FORMAT Values` with the rows
// on stdin has always been signable, because then the client streams them.
func InsertPayloadEncoding(sql string) (string, error) {
	if _, err := ParseInsertTarget(sql); err != nil {
		return "", err
	}
	source, format, ok := insertDataSource(sql)
	if !ok {
		return "", fmt.Errorf("requires streaming Native INSERT input")
	}
	switch source {
	case "":
		return PayloadEncodingClickHouseNativeData, nil
	case "FORMAT":
		if clientParsedInsertFormats[format] {
			return PayloadEncodingClickHouseNativeData, nil
		}
		if format == "" {
			return "", fmt.Errorf("requires streaming Native INSERT input; FORMAT without Native is not supported")
		}
		return "", fmt.Errorf("requires streaming Native INSERT input; FORMAT %s is not supported", format)
	case "VALUES", "SELECT", "WITH":
		return "", fmt.Errorf("requires streaming Native INSERT input; INSERT ... %s is not supported", source)
	default:
		return "", fmt.Errorf("requires streaming Native INSERT input")
	}
}

// RequireStreamingNativeInsert accepts only INSERT forms whose replay payload
// remains the captured Native protocol Data packets.
//
// InsertPayloadEncoding returns exactly one encoding today, so the check below
// is unreachable; it is kept as the guard that a second encoding must not slip
// into this lane silently, and names the encoding it actually saw rather than a
// hardcoded format that had drifted from the reason for rejecting.
func RequireStreamingNativeInsert(sql string) error {
	encoding, err := InsertPayloadEncoding(sql)
	if err != nil {
		return err
	}
	if encoding != PayloadEncodingClickHouseNativeData {
		return fmt.Errorf("requires streaming Native INSERT input; payload encoding %s is not supported", encoding)
	}
	return nil
}

func insertDataSource(sql string) (source, format string, ok bool) {
	source, format, _, ok = insertDataSourceAt(sql)
	return source, format, ok
}

// insertDataSourceAt is insertDataSource plus the byte offset just past the
// payload-source keyword, which is where an inline VALUES row list starts.
func insertDataSourceAt(sql string) (source, format string, end int, ok bool) {
	tok, pos, ok := nextStorageSQLToken(sql, 0)
	if !ok || tok.kind != storageSQLTokenWord || tok.text != "INSERT" {
		return "", "", 0, false
	}

	depth := 0
	for {
		tok, next, ok := nextStorageSQLToken(sql, pos)
		if !ok {
			return "", "", pos, true
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
				formatTok, formatEnd, formatOK := nextStorageSQLToken(sql, pos)
				if !formatOK || formatTok.kind != storageSQLTokenWord {
					return "FORMAT", "", pos, true
				}
				return "FORMAT", formatTok.text, formatEnd, true
			case "VALUES", "SELECT", "WITH":
				return tok.text, "", pos, true
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
