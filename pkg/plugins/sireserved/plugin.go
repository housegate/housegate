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
	name, err := ReservedNamespaceViolation(sql, p.ReservedDatabases, p.ReservedRowIDColumn)
	if err != nil {
		return fmt.Errorf("storage-integrity guard could not scan the statement: %w", err)
	}
	if name != "" {
		return fmt.Errorf("storage-integrity reserved name %q is not addressable through the proxy (the operator guard rejects any mention, including an ordinary column with that name); use a direct ClickHouse connection for physical access", name)
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
// outside single-quoted string literals and comments. Mention is deliberately
// the rule: attempting to distinguish SQL roles here would create a partial
// ClickHouse parser whose gaps become bypasses on privileged sessions.
func ReservedNamespaceViolation(sql string, databases []string, rowIDColumn string) (string, error) {
	scrubbed, err := stripLiteralsAndComments(sql)
	if err != nil {
		return "", err
	}
	names := make([]string, 0, len(databases)+1)
	for _, database := range databases {
		if database != "" {
			names = append(names, database)
		}
	}
	if rowIDColumn != "" {
		names = append(names, rowIDColumn)
	}
	for _, identifier := range identifiers(scrubbed) {
		for _, name := range names {
			if strings.EqualFold(identifier, name) {
				return name, nil
			}
		}
	}
	return "", nil
}

// stripLiteralsAndComments blanks single-quoted strings and every ClickHouse
// comment form. Quoted identifiers are unquoted rather than blanked so a
// reserved name cannot hide inside backticks or double quotes.
func stripLiteralsAndComments(sql string) (string, error) {
	var out strings.Builder
	out.Grow(len(sql))

	for i := 0; i < len(sql); {
		switch {
		case sql[i] == '\'':
			out.WriteByte(' ')
			next, err := consumeStringLiteral(sql, i)
			if err != nil {
				return "", err
			}
			i = next

		case hasPrefixAt(sql, i, "--") || hasPrefixAt(sql, i, "//") || sql[i] == '#':
			out.WriteByte(' ')
			i = consumeLineComment(sql, i)

		case hasPrefixAt(sql, i, "/*"):
			out.WriteByte(' ')
			next, err := consumeBlockComment(sql, i)
			if err != nil {
				return "", err
			}
			i = next

		case sql[i] == '`' || sql[i] == '"':
			out.WriteByte(' ')
			next, identifier, err := consumeQuotedIdentifier(sql, i, sql[i])
			if err != nil {
				return "", err
			}
			out.WriteString(identifier)
			out.WriteByte(' ')
			i = next

		default:
			out.WriteByte(sql[i])
			i++
		}
	}
	return out.String(), nil
}

func consumeStringLiteral(sql string, start int) (int, error) {
	for i := start + 1; i < len(sql); {
		switch sql[i] {
		case '\\':
			// ClickHouse consumes a backslash and the following byte as one
			// escape. This ordering is what makes 'a\\' terminate correctly.
			if i+1 >= len(sql) {
				return 0, fmt.Errorf("unterminated single-quoted string literal")
			}
			i += 2
		case '\'':
			if i+1 < len(sql) && sql[i+1] == '\'' {
				i += 2
				continue
			}
			return i + 1, nil
		default:
			i++
		}
	}
	return 0, fmt.Errorf("unterminated single-quoted string literal")
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
