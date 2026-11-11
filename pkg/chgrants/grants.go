// Package chgrants checks that the ClickHouse user HouseGate forwards every
// tenant session as holds only the grants HouseGate serves
// (housegate/housegate#341).
//
// Every session HouseGate forwards to ClickHouse — tenant sessions, the
// maintenance and platform-operator sessions the rewriter does not see, and
// peer-trusted remote() loopbacks — reaches ClickHouse as the one user of the
// credential provider's default credential. The rewriter engines refuse what
// they model; the grants of that user are what stops everything else (an
// external table function such as url() or file(), a statement the rewriter
// never sees, a future engine gap). This package parses the user's
// `SHOW GRANTS FINAL` and reports every grant outside the expected set.
package chgrants

import (
	"fmt"
	"strings"
)

// Policy is the grant set a HouseGate server expects its upstream user to
// hold. Everything outside it is a Violation.
type Policy struct {
	// PhysicalDatabase is rewriter.physical_database: any privilege on it
	// (database or table level) is expected. Empty expects none.
	PhysicalDatabase string
	// StorageIntegrity expects SELECT on hg_safe and hg_unsafe, the
	// databases the rewritten reads of Active tables read.
	StorageIntegrity bool
	// ReplicaCopies expects SELECT on hg_replica, which the rewritten reads
	// of stage-3 replica tables read. Only meaningful with StorageIntegrity.
	ReplicaCopies bool
	// EngineAllowed reports whether a TABLE ENGINE grant names an engine the
	// rewriter engines accept. Nil treats every engine grant as a violation.
	EngineAllowed func(engine string) bool
}

// Violation is one privilege on one target that the policy does not expect.
type Violation struct {
	Line      string // the SHOW GRANTS line it came from
	Privilege string // upper-case privilege name, column list dropped
	Target    string // the ON target as ClickHouse printed it
	Reason    string
}

func (v Violation) String() string {
	if v.Privilege == "" {
		return fmt.Sprintf("%s: %s", v.Reason, v.Line)
	}
	return fmt.Sprintf("%s ON %s: %s", v.Privilege, v.Target, v.Reason)
}

// globalAllowed are the *.* privileges a HouseGate upstream user needs:
// REMOTE for the remote() reads the rewriter emits for another indexer's
// databases (also the peer loopback's origin side), and CREATE TEMPORARY
// TABLE for GLOBAL IN and the external tables of a client's query.
var globalAllowed = map[string]bool{
	"REMOTE":                 true,
	"CREATE TEMPORARY TABLE": true,
}

// readPrivileges are the privileges allowed on the read-only databases
// (system, hg_safe, hg_unsafe, hg_replica).
var readPrivileges = map[string]bool{
	"SELECT":         true,
	"SHOW":           true,
	"SHOW DATABASES": true,
	"SHOW TABLES":    true,
	"SHOW COLUMNS":   true,
}

// Check parses grant lines — those of `SHOW GRANTS FINAL` plus the role lines
// of `SHOW GRANTS` (see ShowGrants) — and returns every violation of p, in
// line order. A role grant is checked only for WITH ADMIN OPTION: FINAL
// already lists the role's privileges. REVOKE lines only narrow what a GRANT line allows, so
// they are ignored: a GRANT the policy does not expect is a violation even
// when a REVOKE carves part of it out.
func Check(p Policy, lines []string) []Violation {
	var out []Violation
	for _, raw := range lines {
		line := strings.TrimSpace(raw)
		if line == "" {
			continue
		}
		g, err := parseLine(line)
		if err != nil {
			out = append(out, Violation{Line: line, Reason: "unrecognised grant line (" + err.Error() + ")"})
			continue
		}
		if g.revoke {
			continue
		}
		if g.role {
			// FINAL lists a role's privileges in place of the role grant, and
			// drops the grant itself; the query also reads the plain SHOW
			// GRANTS role lines (ShowGrants) for this check. A role held WITH
			// ADMIN OPTION can be granted on, like a WITH GRANT OPTION
			// privilege.
			if g.adminOption {
				out = append(out, Violation{Line: line, Reason: "WITH ADMIN OPTION lets the user grant the role on"})
			}
			continue
		}
		if g.grantOption {
			out = append(out, Violation{Line: line, Target: g.target.text, Reason: "WITH GRANT OPTION lets the user pass its privileges on"})
		}
		for _, priv := range g.privileges {
			if reason := p.violation(priv, g.target); reason != "" {
				out = append(out, Violation{Line: line, Privilege: priv, Target: g.target.text, Reason: reason})
			}
		}
	}
	return out
}

func (p Policy) violation(priv string, t target) string {
	if priv == "TABLE ENGINE" {
		switch {
		case t.kind != targetName:
			return "table engine grant on every engine"
		case p.EngineAllowed != nil && p.EngineAllowed(t.name):
			return ""
		default:
			return "table engine the rewriter refuses"
		}
	}
	switch t.kind {
	case targetGlobal:
		if globalAllowed[priv] {
			return ""
		}
		return "global privilege"
	case targetAny, targetName:
		return "privilege outside any database"
	case targetPattern:
		return "database or table pattern"
	}
	// targetDatabase
	switch db := t.database; {
	case p.PhysicalDatabase != "" && db == p.PhysicalDatabase:
		return ""
	case db == "system":
		if readPrivileges[priv] {
			return ""
		}
		return "non-read privilege on system"
	case p.StorageIntegrity && (db == "hg_safe" || db == "hg_unsafe"),
		p.StorageIntegrity && p.ReplicaCopies && db == "hg_replica":
		if readPrivileges[priv] {
			return ""
		}
		return "non-read privilege on a storage-integrity database"
	default:
		return "database HouseGate does not serve"
	}
}

type targetKind int

const (
	targetDatabase targetKind = iota // db.* or db.table
	targetGlobal                     // *.*
	targetAny                        // * (users, roles, named collections, engines)
	targetName                       // a bare name: an engine, a named collection
	targetPattern                    // a wildcard database or table (db*.*, db.t*)
)

type target struct {
	text     string
	kind     targetKind
	database string // targetDatabase
	name     string // targetName
}

type grantLine struct {
	revoke      bool
	role        bool
	adminOption bool
	grantOption bool
	privileges  []string
	target      target
}

// parseLine parses one line of ClickHouse's SHOW GRANTS output:
//
//	GRANT <privilege>[(<columns>)][, …] ON <target> TO <grantee>[ WITH GRANT OPTION]
//	REVOKE <privilege>[, …] ON <target> FROM <grantee>
//	GRANT <role>[, …] TO <grantee>[ WITH ADMIN OPTION]
//
// Measured on ClickHouse 25.8 and 26.8.
func parseLine(line string) (grantLine, error) {
	var g grantLine
	rest, ok := cutWord(line, "GRANT")
	if !ok {
		if rest, ok = cutWord(line, "REVOKE"); !ok {
			return g, fmt.Errorf("does not start with GRANT or REVOKE")
		}
		g.revoke = true
	}
	toWord := " TO "
	if g.revoke {
		toWord = " FROM "
	}
	onAt := topLevelIndex(rest, " ON ")
	if onAt < 0 {
		if topLevelIndex(rest, toWord) < 0 {
			return g, fmt.Errorf("no ON or %s", strings.TrimSpace(toWord))
		}
		g.role = true
		g.adminOption = !g.revoke && strings.HasSuffix(rest, " WITH ADMIN OPTION")
		return g, nil
	}
	privs, after := rest[:onAt], rest[onAt+len(" ON "):]
	toAt := topLevelIndex(after, toWord)
	if toAt < 0 {
		return g, fmt.Errorf("no %s", strings.TrimSpace(toWord))
	}
	targetText, grantee := after[:toAt], after[toAt+len(toWord):]
	if !g.revoke && strings.HasSuffix(grantee, " WITH GRANT OPTION") {
		g.grantOption = true
	}
	parts, err := splitTopLevel(privs)
	if err != nil {
		return g, err
	}
	for _, p := range parts {
		// A privilege name is bare words; a column list follows the first
		// '(' (column names are only inside it, so the name has no quote).
		name := strings.TrimSpace(p)
		if i := strings.IndexByte(name, '('); i >= 0 {
			name = strings.TrimSpace(name[:i])
		}
		if name == "" || strings.ContainsAny(name, "`,)") {
			return g, fmt.Errorf("malformed privilege %q", p)
		}
		g.privileges = append(g.privileges, strings.ToUpper(name))
	}
	t, err := parseTarget(strings.TrimSpace(targetText))
	if err != nil {
		return g, err
	}
	g.target = t
	return g, nil
}

func cutWord(s, word string) (string, bool) {
	if len(s) > len(word) && strings.EqualFold(s[:len(word)], word) && s[len(word)] == ' ' {
		return s[len(word)+1:], true
	}
	return s, false
}

func parseTarget(text string) (target, error) {
	t := target{text: text}
	if text == "*.*" {
		t.kind = targetGlobal
		return t, nil
	}
	if text == "*" {
		t.kind = targetAny
		return t, nil
	}
	parts, patterns, err := splitIdentifiers(text)
	if err != nil {
		return t, err
	}
	switch len(parts) {
	case 1:
		if patterns[0] {
			t.kind = targetPattern
			return t, nil
		}
		t.kind, t.name = targetName, parts[0]
	case 2:
		if patterns[0] || (patterns[1] && parts[1] != "*") {
			t.kind = targetPattern
			return t, nil
		}
		t.kind, t.database = targetDatabase, parts[0]
	default:
		return t, fmt.Errorf("target %q has %d parts", text, len(parts))
	}
	return t, nil
}

// splitIdentifiers splits a dotted target into its parts, decoding
// backtick-quoted parts. pattern[i] reports an unquoted '*' in part i.
func splitIdentifiers(s string) (parts []string, pattern []bool, err error) {
	var cur strings.Builder
	star := false
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '`':
			j := i + 1
			for ; j < len(s); j++ {
				if s[j] == '\\' && j+1 < len(s) {
					cur.WriteByte(s[j+1])
					j++
					continue
				}
				if s[j] == '`' {
					if j+1 < len(s) && s[j+1] == '`' {
						cur.WriteByte('`')
						j++
						continue
					}
					break
				}
				cur.WriteByte(s[j])
			}
			if j >= len(s) {
				return nil, nil, fmt.Errorf("unterminated quoted identifier in %q", s)
			}
			i = j
		case '.':
			parts, pattern = append(parts, cur.String()), append(pattern, star)
			cur.Reset()
			star = false
		case '*':
			star = true
			cur.WriteByte(c)
		case ' ', '\t', '"', '\'':
			return nil, nil, fmt.Errorf("unexpected %q in target %q", c, s)
		default:
			cur.WriteByte(c)
		}
	}
	parts, pattern = append(parts, cur.String()), append(pattern, star)
	for _, p := range parts {
		if p == "" {
			return nil, nil, fmt.Errorf("empty identifier in target %q", s)
		}
	}
	return parts, pattern, nil
}

// skipQuoted returns the index of the closing backtick of the quoted
// identifier opening at s[i], honouring ClickHouse's escapes (a backslash
// escapes the next byte, a doubled backtick is one backtick), or -1 when the
// quote is not closed.
func skipQuoted(s string, i int) int {
	for j := i + 1; j < len(s); j++ {
		switch {
		case s[j] == '\\':
			j++
		case s[j] == '`' && j+1 < len(s) && s[j+1] == '`':
			j++
		case s[j] == '`':
			return j
		}
	}
	return -1
}

// topLevelIndex finds sep outside parentheses and quoted identifiers.
func topLevelIndex(s, sep string) int {
	depth := 0
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c == '`':
			j := skipQuoted(s, i)
			if j < 0 {
				return -1
			}
			i = j
		case c == '(':
			depth++
		case c == ')':
			depth--
		case depth == 0 && strings.HasPrefix(s[i:], sep):
			return i
		}
	}
	return -1
}

// splitTopLevel splits a privilege list at commas outside parentheses and
// quoted identifiers: `SELECT(`(`), INSERT(x)` is two privileges.
func splitTopLevel(s string) ([]string, error) {
	var out []string
	depth, start := 0, 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '`':
			j := skipQuoted(s, i)
			if j < 0 {
				return nil, fmt.Errorf("unterminated quoted identifier in %q", s)
			}
			i = j
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				out = append(out, s[start:i])
				start = i + 1
			}
		}
	}
	if depth != 0 {
		return nil, fmt.Errorf("unbalanced parentheses in %q", s)
	}
	return append(out, s[start:]), nil
}
