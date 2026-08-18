package sistatement

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/sqlident"
)

const identifierPath = "(?:`(?:``|[^`])+`|[A-Za-z_][A-Za-z0-9_]*)(?:\\s*\\.\\s*(?:`(?:``|[^`])+`|[A-Za-z_][A-Za-z0-9_]*))*"

var (
	// insertTargetPattern mirrors pkg/plugins/storageintegrity's pattern so
	// agent and ingress agree on what the target path is.
	insertTargetPattern = regexp.MustCompile(`(?is)^\s*INSERT\s+INTO\s+(` + identifierPath + `)(?:\s|\(|;|$)`)
	// useRegex mirrors pkg/plugins/forward/use_regex.go (standalone USE only).
	useRegex = regexp.MustCompile("(?i)^\\s*USE\\s+(?:`([A-Za-z0-9_-]+)`|\"([A-Za-z0-9_-]+)\"|([A-Za-z0-9_-]+))\\s*;?\\s*$")
)

// insertTargetPath returns the raw target identifier path of an INSERT.
func insertTargetPath(sql string) (string, bool) {
	m := insertTargetPattern.FindStringSubmatch(sql)
	if len(m) != 2 {
		return "", false
	}
	return m[1], true
}

// resolveTargetTableID returns the logical "<db>.<table>" id: the SQL's own
// database when qualified, else sessionDB. Unqualified + no session database
// is an error (the SI lane needs an unambiguous table id to look up the
// declared schema).
func resolveTargetTableID(sql, sessionDB string) (string, error) {
	target, ok := insertTargetPath(sql)
	if !ok {
		return "", errors.New("sistatement: statement is not an INSERT")
	}
	db, table := sqlident.SplitLastPath(target)
	if table == "" {
		return "", fmt.Errorf("sistatement: invalid INSERT target %q", target)
	}
	if db == "" {
		if strings.TrimSpace(sessionDB) == "" {
			return "", fmt.Errorf("sistatement: INSERT target %q must be database-qualified (db.table) or the session must select a database", target)
		}
		db = sqlident.NormalizePath(sqlident.Quote(sessionDB))
		if db == "" {
			return "", fmt.Errorf("sistatement: invalid session database %q", sessionDB)
		}
	}
	return db + "." + table, nil
}

// insertColumnList parses the optional "(c1, c2, ...)" list that follows the
// INSERT target. explicit=false when there is no list.
func insertColumnList(sql string) ([]string, bool, error) {
	loc := insertTargetPattern.FindStringSubmatchIndex(sql)
	if loc == nil {
		return nil, false, errors.New("sistatement: statement is not an INSERT")
	}
	pos := loc[3] // end of the target path
	for pos < len(sql) && (sql[pos] == ' ' || sql[pos] == '\t' || sql[pos] == '\n' || sql[pos] == '\r') {
		pos++
	}
	if pos >= len(sql) || sql[pos] != '(' {
		return nil, false, nil
	}
	pos++
	var (
		cols    []string
		current strings.Builder
		haveTok bool
	)
	flush := func() error {
		if !haveTok {
			return errors.New("sistatement: empty column name in INSERT column list")
		}
		cols = append(cols, current.String())
		current.Reset()
		haveTok = false
		return nil
	}
	for pos < len(sql) {
		ch := sql[pos]
		switch {
		case ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r':
			pos++
		case ch == ',':
			if err := flush(); err != nil {
				return nil, true, err
			}
			pos++
		case ch == ')':
			if err := flush(); err != nil {
				return nil, true, err
			}
			return cols, true, nil
		case ch == '`' || ch == '"':
			if haveTok {
				return nil, true, fmt.Errorf("sistatement: unexpected quoted identifier at offset %d in INSERT column list", pos)
			}
			quote := ch
			pos++
			for {
				if pos >= len(sql) {
					return nil, true, errors.New("sistatement: unterminated quoted identifier in INSERT column list")
				}
				if sql[pos] == quote {
					if pos+1 < len(sql) && sql[pos+1] == quote {
						current.WriteByte(quote)
						pos += 2
						continue
					}
					pos++
					break
				}
				current.WriteByte(sql[pos])
				pos++
			}
			haveTok = true
		case ch == '_' || (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z'):
			if haveTok {
				return nil, true, fmt.Errorf("sistatement: expected ',' or ')' before %q in INSERT column list", ch)
			}
			start := pos
			for pos < len(sql) && (sql[pos] == '_' || (sql[pos] >= 'A' && sql[pos] <= 'Z') || (sql[pos] >= 'a' && sql[pos] <= 'z') || (sql[pos] >= '0' && sql[pos] <= '9')) {
				pos++
			}
			current.WriteString(sql[start:pos])
			haveTok = true
		default:
			return nil, true, fmt.Errorf("sistatement: unexpected %q in INSERT column list", ch)
		}
	}
	return nil, true, errors.New("sistatement: unterminated INSERT column list")
}

// sampleColumnsFor builds the sample block columns: schema order when the
// INSERT has no column list, else the SQL's order — which must name every
// declared column exactly once (the Native decoder requires the full column
// set, so a subset would only fail later at the ingress).
func sampleColumnsFor(schema payloadexec.TableSchema, listed []string) ([]chproto.SampleColumn, error) {
	byName := make(map[string]string, len(schema.Columns))
	for _, c := range schema.Columns {
		if c.Name == "_hg_row_id" {
			continue
		}
		byName[c.Name] = c.Type
	}
	if len(byName) == 0 {
		return nil, fmt.Errorf("sistatement: declared schema for %s has no columns", schema.TableID)
	}
	if len(listed) == 0 {
		out := make([]chproto.SampleColumn, 0, len(byName))
		for _, c := range schema.Columns {
			if c.Name == "_hg_row_id" {
				continue
			}
			out = append(out, chproto.SampleColumn{Name: c.Name, Type: c.Type})
		}
		return out, nil
	}
	seen := make(map[string]bool, len(listed))
	out := make([]chproto.SampleColumn, 0, len(listed))
	for _, name := range listed {
		typ, ok := byName[name]
		if !ok {
			return nil, fmt.Errorf("sistatement: INSERT lists unknown column %q for %s", name, schema.TableID)
		}
		if seen[name] {
			return nil, fmt.Errorf("sistatement: INSERT lists column %q twice", name)
		}
		seen[name] = true
		out = append(out, chproto.SampleColumn{Name: name, Type: typ})
	}
	for _, c := range schema.Columns {
		if c.Name != "_hg_row_id" && !seen[c.Name] {
			return nil, fmt.Errorf("sistatement: SI INSERT into %s must list every declared column (missing %q)", schema.TableID, c.Name)
		}
	}
	return out, nil
}

// matchUse returns (database, true) for a standalone USE statement.
func matchUse(sql string) (string, bool) {
	m := useRegex.FindStringSubmatch(sql)
	if m == nil {
		return "", false
	}
	for _, g := range m[1:] {
		if g != "" {
			return g, true
		}
	}
	return "", false
}
