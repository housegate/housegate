// Package sireserved protects protocol-owned storage-integrity names on
// privileged sessions that deliberately bypass the full SQL rewriter.
package sireserved

import (
	"context"
	"fmt"
	"strings"

	"github.com/housegate/housegate/pkg/plugin"
)

// Plugin rejects reserved SI names on maintenance and platform-operator
// sessions. It runs independently of rewrite.Plugin so forward and peer-trust
// filters cannot skip this defense-in-depth boundary.
type Plugin struct {
	ReservedDatabases   []string
	ReservedRowIDColumn string
}

// OnQuery rejects a privileged-bypass query that mentions a reserved name.
// Plain peer/forward sessions without an operator flag retain the deliberate
// Spec I D6 peer behavior.
func (p *Plugin) OnQuery(_ context.Context, qctx *plugin.QueryContext) error {
	if qctx == nil || qctx.Session == nil {
		return nil
	}
	snapshot := qctx.Session.State().Snapshot()
	if !snapshot.Maintenance && !snapshot.PlatformOperator {
		return nil
	}

	sql := qctx.OriginalSQL
	if qctx.Query != nil && qctx.Query.Body != "" {
		sql = qctx.Query.Body
	}
	surfaces, err := scanSQLSurfaces(sql)
	if err != nil {
		return fmt.Errorf("storage-integrity guard could not scan the statement: %w", err)
	}
	if containsIdentifierPlaceholder(surfaces.outsideLiterals) {
		return fmt.Errorf("storage-integrity guard refuses ClickHouse Identifier placeholders on privileged proxy-bypass sessions; use a direct ClickHouse connection for physical access")
	}
	name := reservedNamespaceViolationOnSurface(surfaces.withLiterals, p.ReservedDatabases, p.ReservedRowIDColumn)
	if name != "" {
		return fmt.Errorf("storage-integrity reserved name %q is not addressable through the proxy (the operator guard rejects any mention, including an ordinary column with that name); use a direct ClickHouse connection for physical access", name)
	}
	if carrier := objectCarrierCallable(surfaces.outsideLiterals); carrier != "" {
		return fmt.Errorf("storage-integrity object-carrier callable %q is not accepted on privileged proxy-bypass sessions; use a direct ClickHouse connection for physical access", carrier)
	}
	return nil
}

// RunOnForward keeps the guard active after a session pivots to a peer.
func (*Plugin) RunOnForward() bool { return true }

// RunOnPeerTrust keeps the guard active on peer-trusted sessions. OnQuery's
// operator-flag gate leaves ordinary peer loopbacks untouched.
func (*Plugin) RunOnPeerTrust() bool { return true }

// RejectUndecodableQuery fails closed before Relay's raw-splice fallback. An
// undecodable query cannot be scanned for reserved names; the marker methods
// above keep this policy active on the forwarded/peer paths where the rewrite
// plugin's equivalent SI policy is deliberately filtered out.
func (*Plugin) RejectUndecodableQuery() bool { return true }

// ReservedNamespaceViolation reports the first reserved name mentioned
// outside comments, including inside string literals. ClickHouse table
// functions such as merge() and remote() interpret string arguments as
// database/table identifiers, so literal contents are part of the protected
// surface. Mention is deliberately the rule: attempting to distinguish SQL
// roles here would create a partial ClickHouse parser whose gaps become
// bypasses on privileged sessions.
func ReservedNamespaceViolation(sql string, databases []string, rowIDColumn string) (string, error) {
	surfaces, err := scanSQLSurfaces(sql)
	if err != nil {
		return "", err
	}
	return reservedNamespaceViolationOnSurface(surfaces.withLiterals, databases, rowIDColumn), nil
}

func reservedNamespaceViolationOnSurface(surface string, databases []string, rowIDColumn string) string {
	names := make([]string, 0, len(databases)+1)
	for _, database := range databases {
		if database != "" {
			names = append(names, database)
		}
	}
	if rowIDColumn != "" {
		names = append(names, rowIDColumn)
	}
	for _, identifier := range identifiers(surface) {
		for _, name := range names {
			if strings.EqualFold(identifier, name) {
				return name
			}
		}
	}
	return ""
}

type sqlSurfaces struct {
	// outsideLiterals keeps executable SQL but blanks comments and string
	// literals. It is the only surface used for placeholder syntax.
	outsideLiterals string
	// withLiterals additionally keeps string contents because ClickHouse table
	// functions interpret some literal arguments as identifiers.
	withLiterals string
}

// scanSQLSurfaces ignores every ClickHouse comment form, retains quoted
// identifiers, models heredoc string literals, and produces separate views
// with/without string contents. Any backslash-bearing single-quoted string is
// refused: interpreting ClickHouse escape forms here would create an encoding
// bypass such as hg\x5Fsafe. Heredoc bodies are exempt from that refusal
// because ClickHouse applies no escape processing inside them (see
// consumeHeredoc). Case order is the lexer's precedence and matches the
// grammar: a single quote and a comment marker both outrank a heredoc opener,
// and a heredoc opener outranks everything inside its own body.
func scanSQLSurfaces(sql string) (sqlSurfaces, error) {
	var outside, withLiterals strings.Builder
	outside.Grow(len(sql))
	withLiterals.Grow(len(sql))

	for i := 0; i < len(sql); {
		switch {
		case sql[i] == '\'':
			outside.WriteByte(' ')
			withLiterals.WriteByte(' ')
			next, literal, err := consumeStringLiteral(sql, i)
			if err != nil {
				return sqlSurfaces{}, err
			}
			withLiterals.WriteString(literal)
			withLiterals.WriteByte(' ')
			i = next

		case hasPrefixAt(sql, i, "--") || hasPrefixAt(sql, i, "//") || sql[i] == '#':
			outside.WriteByte(' ')
			withLiterals.WriteByte(' ')
			i = consumeLineComment(sql, i)

		case hasPrefixAt(sql, i, "/*"):
			outside.WriteByte(' ')
			withLiterals.WriteByte(' ')
			next, err := consumeBlockComment(sql, i)
			if err != nil {
				return sqlSurfaces{}, err
			}
			i = next

		case sql[i] == '`' || sql[i] == '"':
			outside.WriteByte(' ')
			withLiterals.WriteByte(' ')
			next, identifier, err := consumeQuotedIdentifier(sql, i, sql[i])
			if err != nil {
				return sqlSurfaces{}, err
			}
			outside.WriteString(identifier)
			outside.WriteByte(' ')
			withLiterals.WriteString(identifier)
			withLiterals.WriteByte(' ')
			i = next

		case sql[i] == '$':
			// ClickHouse heredoc: $$body$$ or $tag$body$tag$. The body is a
			// string literal, so it is blanked from outsideLiterals and written
			// verbatim to withLiterals -- table functions read literal arguments
			// as identifiers, so merge($$hg_safe$$, ...) must still be caught.
			// A `$` that opens no well-formed heredoc is refused: outside a
			// heredoc opener and a quoted span it is not part of any identifier
			// or operator this guard needs to admit, and copying it through is
			// what let a comment marker inside a heredoc blank the rest of a
			// statement from both surfaces (Spec N D1).
			outside.WriteByte(' ')
			withLiterals.WriteByte(' ')
			next, body, err := consumeHeredoc(sql, i)
			if err != nil {
				return sqlSurfaces{}, err
			}
			withLiterals.WriteString(body)
			withLiterals.WriteByte(' ')
			i = next

		default:
			outside.WriteByte(sql[i])
			withLiterals.WriteByte(sql[i])
			i++
		}
	}
	return sqlSurfaces{outsideLiterals: outside.String(), withLiterals: withLiterals.String()}, nil
}

func consumeStringLiteral(sql string, start int) (int, string, error) {
	var literal strings.Builder
	for i := start + 1; i < len(sql); {
		switch sql[i] {
		case '\\':
			return 0, "", fmt.Errorf("backslash-bearing single-quoted string literal is not accepted by the storage-integrity guard")
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
		return 0, "", fmt.Errorf("stray $ is not a heredoc opener and is not accepted by the storage-integrity guard")
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

func containsIdentifierPlaceholder(sql string) bool {
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

// objectCarrierCallable returns the first callable whose arguments can carry a
// local ClickHouse database/table identity outside ordinary table syntax. The
// list mirrors rewriter-go's Spec G namespace-reference authority and adds the
// equivalent table-engine/dictionary-source callables. Arguments are
// deliberately not interpreted: ClickHouse constant-folds expressions such as
// concat('hg_', 'safe'), so any attempt to prove a carrier's target safe with a
// token scanner would be bypassable.
func objectCarrierCallable(sql string) string {
	for i := 0; i < len(sql); {
		if !isIdentifierByte(sql[i]) {
			i++
			continue
		}
		start := i
		for i < len(sql) && isIdentifierByte(sql[i]) {
			i++
		}
		name := sql[start:i]
		call := i
		for call < len(sql) && (sql[call] == ' ' || sql[call] == '\t' || sql[call] == '\r' || sql[call] == '\n') {
			call++
		}
		if call < len(sql) && sql[call] == '(' && isObjectCarrierName(name) {
			return name
		}
	}
	return ""
}

func isObjectCarrierName(name string) bool {
	lower := strings.ToLower(name)
	switch lower {
	case "remote", "remotesecure", "cluster", "clusterallreplicas",
		"merge", "loop", "dictionary",
		"timeseriesdata", "timeseriestags", "timeseriesmetrics", "timeseriesselector",
		"prometheusquery", "prometheusqueryrange",
		"distributed", "buffer", "clickhouse",
		// Foreign-connector table functions whose documented signature carries
		// an explicit (database, table) pair. ClickHouse ships its own MySQL
		// and PostgreSQL wire listeners (9004 / 9005), so these are a loopback
		// into the protected namespace; jdbc/odbc reach it through a DSN.
		// sqlite and redis are deliberately NOT here: redis()'s second
		// argument is a column name and sqlite()'s is a table inside a SQLite
		// file, so neither names a ClickHouse namespace and listing them would
		// only generate false refusals (Spec N D4 as corrected by plan
		// deviation D-2).
		"mysql", "postgresql", "mongodb", "jdbc", "odbc":
		return true
	default:
		return strings.HasPrefix(lower, "mergetree")
	}
}

func consumeLineComment(sql string, start int) int {
	i := start
	for i < len(sql) && sql[i] != '\n' && sql[i] != '\r' {
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
			return 0, "", fmt.Errorf("escaped quoted identifier is not accepted by the storage-integrity guard")
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

func identifiers(sql string) []string {
	var result []string
	for start, i := -1, 0; i <= len(sql); i++ {
		if i < len(sql) && isIdentifierByte(sql[i]) {
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

func isIdentifierByte(value byte) bool {
	return value == '_' || value >= 'a' && value <= 'z' ||
		value >= 'A' && value <= 'Z' || value >= '0' && value <= '9'
}

func hasPrefixAt(value string, offset int, prefix string) bool {
	return offset >= 0 && offset+len(prefix) <= len(value) && value[offset:offset+len(prefix)] == prefix
}

var (
	_ plugin.QueryPlugin             = (*Plugin)(nil)
	_ plugin.StrictQueryDecodePlugin = (*Plugin)(nil)
	_ plugin.ForwardAware            = (*Plugin)(nil)
	_ plugin.PeerTrustAware          = (*Plugin)(nil)
)
