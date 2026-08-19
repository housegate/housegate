package storageintegrity

import (
	"errors"
	"fmt"
	"strings"

	"github.com/housegate/housegate/pkg/sqlident"
)

var (
	// ErrNotInsert means the first SQL token is not INSERT.
	ErrNotInsert = errors.New("storage_integrity: statement is not an INSERT")
	// ErrInsertIntoFunction identifies ClickHouse's table-function INSERT form,
	// which has no declared logical table identity and cannot enter the SI lane.
	ErrInsertIntoFunction = errors.New("storage_integrity: INSERT INTO FUNCTION is not supported on the SI lane")
	// ErrBackslashEscapedIdentifier rejects quoted identifiers whose decoded
	// bytes depend on ClickHouse's C-style escape table. The SI lane supports
	// doubled quote escaping and fails closed rather than risk signing a
	// different structured target from the table ClickHouse executes against.
	ErrBackslashEscapedIdentifier = errors.New("storage_integrity: backslash-escaped quoted identifiers are not supported on the SI lane")
)

// InsertTarget is the decoded, structured target of an INSERT. Database and
// Table never contain SQL quoting; ExplicitDatabase records whether the SQL
// supplied the database rather than inheriting the session database.
type InsertTarget struct {
	Database         string
	Table            string
	ExplicitDatabase bool
}

// CanonicalID returns the unambiguous logical table id used by envelopes.
// Segments containing dots, dashes, or backticks stay separately quoted.
func (t InsertTarget) CanonicalID() string {
	if t.Database == "" || t.Table == "" {
		return ""
	}
	return sqlident.NormalizePath(sqlident.Quote(t.Database) + "." + sqlident.Quote(t.Table))
}

type insertTargetParse struct {
	target InsertTarget
	end    int
}

// ParseInsertTarget parses INSERT [INTO] [TABLE] <db.table>, skipping SQL
// whitespace/comments and decoding quoted identifier segments. Table
// functions are rejected explicitly rather than being mistaken for a table
// named FUNCTION.
func ParseInsertTarget(sql string) (InsertTarget, error) {
	parsed, err := parseInsertTarget(sql)
	return parsed.target, err
}

// ResolveInsertTarget fills an unqualified target from the decoded session
// database. The returned value remains structured so registry lookup never has
// to split a canonical SQL identifier string.
func ResolveInsertTarget(sql, sessionDatabase string) (InsertTarget, error) {
	target, err := ParseInsertTarget(sql)
	if err != nil {
		return InsertTarget{}, err
	}
	if target.Database == "" {
		// SessionState stores the decoded identifier, not SQL source text. Spaces
		// can therefore be significant bytes of a quoted ClickHouse database name.
		target.Database = sessionDatabase
		if target.Database == "" {
			return InsertTarget{}, fmt.Errorf("storage_integrity: INSERT target %q must be database-qualified or the session must select a database", target.Table)
		}
	}
	if target.CanonicalID() == "" {
		return InsertTarget{}, fmt.Errorf("storage_integrity: invalid INSERT target database=%q table=%q", target.Database, target.Table)
	}
	return target, nil
}

func parseInsertTarget(sql string) (insertTargetParse, error) {
	s := storageScanner{sql: sql}
	word, ok, err := s.bareWord()
	if err != nil {
		return insertTargetParse{}, err
	}
	if !ok || !strings.EqualFold(word, "INSERT") {
		return insertTargetParse{}, ErrNotInsert
	}
	word, ok, err = s.bareWord()
	if err != nil {
		return insertTargetParse{}, err
	}
	if !ok || !strings.EqualFold(word, "INTO") {
		return insertTargetParse{}, fmt.Errorf("storage_integrity: INSERT requires INTO followed by a table target")
	}

	if err := s.skip(); err != nil {
		return insertTargetParse{}, err
	}
	first, quoted, ok, err := s.identifier()
	if err != nil {
		return insertTargetParse{}, err
	}
	if !ok {
		return insertTargetParse{}, fmt.Errorf("storage_integrity: INSERT requires a table target")
	}
	if !quoted && strings.EqualFold(first, "FUNCTION") {
		return insertTargetParse{}, ErrInsertIntoFunction
	}
	if !quoted && strings.EqualFold(first, "TABLE") {
		first, quoted, ok, err = s.identifier()
		if err != nil {
			return insertTargetParse{}, err
		}
		if !ok {
			return insertTargetParse{}, fmt.Errorf("storage_integrity: INSERT INTO TABLE requires a table target")
		}
		if !quoted && strings.EqualFold(first, "FUNCTION") {
			return insertTargetParse{}, ErrInsertIntoFunction
		}
	}

	if err := s.skip(); err != nil {
		return insertTargetParse{}, err
	}
	target := InsertTarget{Table: first}
	if s.take('.') {
		second, _, ok, err := s.identifier()
		if err != nil {
			return insertTargetParse{}, err
		}
		if !ok {
			return insertTargetParse{}, fmt.Errorf("storage_integrity: INSERT target has an empty table segment")
		}
		target.Database = first
		target.Table = second
		target.ExplicitDatabase = true
		if err := s.skip(); err != nil {
			return insertTargetParse{}, err
		}
		if s.take('.') {
			return insertTargetParse{}, fmt.Errorf("storage_integrity: INSERT target must have at most database.table segments")
		}
	}
	return insertTargetParse{target: target, end: s.pos}, nil
}

// InsertColumnList decodes the optional column list immediately following the
// INSERT target. Comments are ignored and the SQL order is preserved.
func InsertColumnList(sql string) ([]string, bool, error) {
	parsed, err := parseInsertTarget(sql)
	if err != nil {
		return nil, false, err
	}
	s := storageScanner{sql: sql, pos: parsed.end}
	if err := s.skip(); err != nil {
		return nil, false, err
	}
	if !s.take('(') {
		return nil, false, nil
	}
	var cols []string
	for {
		name, _, ok, err := s.identifier()
		if err != nil {
			return nil, true, err
		}
		if !ok {
			return nil, true, fmt.Errorf("storage_integrity: empty column name in INSERT column list")
		}
		cols = append(cols, name)
		if err := s.skip(); err != nil {
			return nil, true, err
		}
		switch {
		case s.take(','):
			continue
		case s.take(')'):
			return cols, true, nil
		default:
			return nil, true, fmt.Errorf("storage_integrity: expected ',' or ')' in INSERT column list")
		}
	}
}

// InlineInsertSettingKeys returns the names in a top-level INSERT SETTINGS
// clause. It skips comments, quoted text, and nested expressions so callers can
// apply the same empty-user-settings policy to protocol and inline settings.
func InlineInsertSettingKeys(sql string) ([]string, error) {
	parsed, err := parseInsertTarget(sql)
	if err != nil {
		return nil, err
	}
	s := storageScanner{sql: sql, pos: parsed.end}
	depth := 0
	for {
		if err := s.skip(); err != nil {
			return nil, err
		}
		if s.pos >= len(sql) {
			return nil, nil
		}
		switch sql[s.pos] {
		case '\'', '"', '`':
			if err := s.skipQuoted(sql[s.pos]); err != nil {
				return nil, err
			}
		case '(':
			depth++
			s.pos++
		case ')':
			if depth > 0 {
				depth--
			}
			s.pos++
		default:
			if isStorageIdentStart(sql[s.pos]) {
				word := s.takeBareWord()
				if depth == 0 && strings.EqualFold(word, "SETTINGS") {
					return parseInlineSettings(&s)
				}
				continue
			}
			s.pos++
		}
	}
}

func parseInlineSettings(s *storageScanner) ([]string, error) {
	var keys []string
	for {
		key, _, ok, err := s.identifier()
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("storage_integrity: malformed inline SETTINGS clause: setting name is required")
		}
		keys = append(keys, key)
		if err := s.skip(); err != nil {
			return nil, err
		}
		if !s.take('=') {
			return nil, fmt.Errorf("storage_integrity: malformed inline SETTINGS clause for %q: '=' is required", key)
		}
		haveValue := false
		depth := 0
		for {
			if err := s.skip(); err != nil {
				return nil, err
			}
			if s.pos >= len(s.sql) {
				if !haveValue {
					return nil, fmt.Errorf("storage_integrity: malformed inline SETTINGS value for %q", key)
				}
				return keys, nil
			}
			ch := s.sql[s.pos]
			switch ch {
			case '\'', '"', '`':
				if err := s.skipQuoted(ch); err != nil {
					return nil, err
				}
				haveValue = true
			case '(', '[', '{':
				depth++
				haveValue = true
				s.pos++
			case ')', ']', '}':
				if depth > 0 {
					depth--
				}
				haveValue = true
				s.pos++
			case ',':
				if depth != 0 {
					haveValue = true
					s.pos++
					continue
				}
				if !haveValue {
					return nil, fmt.Errorf("storage_integrity: malformed inline SETTINGS value for %q", key)
				}
				s.pos++
				goto nextSetting
			default:
				if isStorageIdentStart(ch) {
					word := s.takeBareWord()
					if depth == 0 && haveValue && isInsertPayloadSourceKeyword(word) {
						return keys, nil
					}
					haveValue = true
					continue
				}
				haveValue = true
				s.pos++
			}
		}
	nextSetting:
	}
}

func isInsertPayloadSourceKeyword(word string) bool {
	return strings.EqualFold(word, "FORMAT") || strings.EqualFold(word, "VALUES") || strings.EqualFold(word, "SELECT") || strings.EqualFold(word, "WITH")
}

// ParseUseDatabase returns the decoded database for a standalone USE command.
// It accepts comments and quoted ClickHouse identifiers, including doubled
// quote escapes. Parse failures are collapsed to no-match for non-SI callers;
// correctness-critical callers must use ParseUseDatabaseStrict.
func ParseUseDatabase(sql string) (string, bool) {
	db, ok, _ := ParseUseDatabaseStrict(sql)
	return db, ok
}

// ParseUseDatabaseStrict is the error-bearing form used by the SI signed lane.
// In particular, it preserves ErrBackslashEscapedIdentifier so an ambiguous
// USE cannot be forwarded while the agent silently retains a different session
// database for the next unqualified INSERT.
func ParseUseDatabaseStrict(sql string) (string, bool, error) {
	s := storageScanner{sql: sql}
	word, ok, err := s.bareWord()
	if err != nil {
		return "", false, err
	}
	if !ok || !strings.EqualFold(word, "USE") {
		return "", false, nil
	}
	db, _, ok, err := s.identifier()
	if err != nil {
		return "", false, err
	}
	if !ok || db == "" {
		return "", false, nil
	}
	if err := s.skip(); err != nil {
		return "", false, err
	}
	if s.take(';') {
		if err := s.skip(); err != nil {
			return "", false, err
		}
	}
	if s.pos != len(sql) {
		return "", false, nil
	}
	return db, true, nil
}

type storageScanner struct {
	sql string
	pos int
}

func (s *storageScanner) skip() error {
	for s.pos < len(s.sql) {
		switch {
		case isStorageSpace(s.sql[s.pos]):
			s.pos++
		case s.sql[s.pos] == '#':
			s.skipLineComment(1)
		case s.pos+1 < len(s.sql) && s.sql[s.pos] == '-' && s.sql[s.pos+1] == '-':
			s.skipLineComment(2)
		case s.pos+1 < len(s.sql) && s.sql[s.pos] == '/' && s.sql[s.pos+1] == '*':
			end := strings.Index(s.sql[s.pos+2:], "*/")
			if end < 0 {
				return fmt.Errorf("storage_integrity: unterminated SQL block comment")
			}
			s.pos += 2 + end + 2
		default:
			return nil
		}
	}
	return nil
}

func (s *storageScanner) skipLineComment(prefix int) {
	s.pos += prefix
	for s.pos < len(s.sql) && s.sql[s.pos] != '\n' {
		s.pos++
	}
}

func (s *storageScanner) bareWord() (string, bool, error) {
	if err := s.skip(); err != nil {
		return "", false, err
	}
	if s.pos >= len(s.sql) || !isStorageIdentStart(s.sql[s.pos]) {
		return "", false, nil
	}
	return s.takeBareWord(), true, nil
}

func (s *storageScanner) takeBareWord() string {
	start := s.pos
	s.pos++
	for s.pos < len(s.sql) && isStorageIdentPart(s.sql[s.pos]) {
		s.pos++
	}
	return s.sql[start:s.pos]
}

func (s *storageScanner) identifier() (value string, quoted bool, ok bool, err error) {
	if err := s.skip(); err != nil {
		return "", false, false, err
	}
	if s.pos >= len(s.sql) {
		return "", false, false, nil
	}
	if s.sql[s.pos] != '`' && s.sql[s.pos] != '"' {
		if !isStorageIdentStart(s.sql[s.pos]) {
			return "", false, false, nil
		}
		return s.takeBareWord(), false, true, nil
	}
	quote := s.sql[s.pos]
	s.pos++
	var b strings.Builder
	for s.pos < len(s.sql) {
		ch := s.sql[s.pos]
		if ch == '\\' {
			return "", true, false, fmt.Errorf("%w at SQL byte %d", ErrBackslashEscapedIdentifier, s.pos)
		}
		if ch == quote {
			if s.pos+1 < len(s.sql) && s.sql[s.pos+1] == quote {
				b.WriteByte(quote)
				s.pos += 2
				continue
			}
			s.pos++
			if b.Len() == 0 {
				return "", true, false, fmt.Errorf("storage_integrity: empty quoted identifier")
			}
			return b.String(), true, true, nil
		}
		b.WriteByte(ch)
		s.pos++
	}
	return "", true, false, fmt.Errorf("storage_integrity: unterminated quoted identifier")
}

func (s *storageScanner) skipQuoted(quote byte) error {
	if s.pos >= len(s.sql) || s.sql[s.pos] != quote {
		return fmt.Errorf("storage_integrity: internal quoted-token cursor mismatch")
	}
	s.pos++
	for s.pos < len(s.sql) {
		if s.sql[s.pos] == '\\' && s.pos+1 < len(s.sql) {
			s.pos += 2
			continue
		}
		if s.sql[s.pos] == quote {
			if s.pos+1 < len(s.sql) && s.sql[s.pos+1] == quote {
				s.pos += 2
				continue
			}
			s.pos++
			return nil
		}
		s.pos++
	}
	return fmt.Errorf("storage_integrity: unterminated quoted SQL token")
}

func (s *storageScanner) take(ch byte) bool {
	if s.pos < len(s.sql) && s.sql[s.pos] == ch {
		s.pos++
		return true
	}
	return false
}

func isStorageSpace(ch byte) bool {
	return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || ch == '\f'
}

func isStorageIdentStart(ch byte) bool {
	return ch == '_' || (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z')
}

func isStorageIdentPart(ch byte) bool {
	return isStorageIdentStart(ch) || (ch >= '0' && ch <= '9')
}
