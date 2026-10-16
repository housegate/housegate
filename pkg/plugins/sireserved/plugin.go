// Package sireserved protects protocol-owned storage-integrity names on the
// sessions that deliberately bypass the full SQL rewriter: maintenance and
// platform-operator sessions (the operator rules) and peer-trusted sessions
// that were not forwarded from a peer, i.e. rewriter remote() loopbacks (the
// peer rules, spec 2026-10-14 §14).
package sireserved

import (
	"context"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/log"
	"github.com/housegate/housegate/pkg/plugin"
	"github.com/housegate/housegate/pkg/sqlsurface"
)

// PeerScanOptions is the scan of every peer-trusted guard, shared with
// sipeerguard so the two cannot drift. ClickHouse's remote() secondary
// queries carry backslash-bearing literals and escaped quoted identifiers
// (projection aliases such as `extract(s, '\\d+')`), which the operator
// rules' strict Scan refuses; decoding both instead compares names as
// ClickHouse reads them, so 'hg\x5Fsafe' is hg_safe. A \x escape without two
// hex digits stays a scan error.
var PeerScanOptions = sqlsurface.Options{DecodeStringEscapes: true, DecodeIdentifierEscapes: true}

// Reasons of the peer refusal counter.
const (
	peerReasonReservedName          = "reserved_name"
	peerReasonIdentifierPlaceholder = "identifier_placeholder"
	peerReasonStatementUnscannable  = "statement_unscannable"
)

var peerRefusals = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "storage_integrity_peer_reserved_name_refusals_total",
	Help: "Peer-trusted, non-forwarded statements refused because they mention a storage-integrity reserved database or the reserved row-id column, carry an Identifier placeholder, or cannot be scanned.",
}, []string{"reason"})

func init() {
	prometheus.MustRegister(peerRefusals)
	// Export every reason at zero, so "no refusal since start" is a series an
	// operator can read rather than an absent one.
	for _, reason := range []string{peerReasonReservedName, peerReasonIdentifierPlaceholder, peerReasonStatementUnscannable} {
		peerRefusals.WithLabelValues(reason)
	}
}

// Plugin rejects reserved SI names on the sessions that bypass rewrite. It
// runs independently of rewrite.Plugin so forward and peer-trust filters
// cannot skip this defense-in-depth boundary.
type Plugin struct {
	ReservedDatabases   []string
	ReservedRowIDColumn string
}

// OnQuery applies the operator rules to a maintenance or platform-operator
// session, checked first so a peer session that also carries an operator flag
// gets the stricter rules; the peer rules to a peer-trusted session that was
// not forwarded from a peer (spec 2026-10-14 §14); and nothing to any other
// session. Ordinary and forwarded-from-peer sessions keep the Spec I D6
// boundary here: tablerefguard and the engines own their client SQL.
func (p *Plugin) OnQuery(ctx context.Context, qctx *plugin.QueryContext) error {
	if qctx == nil || qctx.Session == nil {
		return nil
	}
	snapshot := qctx.Session.State().Snapshot()
	sql := qctx.OriginalSQL
	if qctx.Query != nil && qctx.Query.Body != "" {
		sql = qctx.Query.Body
	}
	switch {
	case snapshot.Maintenance || snapshot.PlatformOperator:
		return p.operatorRefusal(sql)
	case snapshot.IsPeerTrusted && !snapshot.IsForwardedFromPeer:
		return p.peerRefusal(ctx, snapshot.PeerAddress, sql)
	default:
		return nil
	}
}

// operatorRefusal is the privileged-session rule set (Spec N): a strict scan
// that refuses every backslash-bearing literal or quoted identifier, then
// every {name:Identifier} placeholder, every mention of a reserved name and
// every object-carrier callable. Its errors are plain, so the relay answers
// 403.
func (p *Plugin) operatorRefusal(sql string) error {
	surfaces, err := sqlsurface.Scan(sql)
	if err != nil {
		return fmt.Errorf("storage-integrity guard could not scan the statement: %w", err)
	}
	if sqlsurface.ContainsIdentifierPlaceholder(surfaces.OutsideLiterals) {
		return fmt.Errorf("storage-integrity guard refuses ClickHouse Identifier placeholders on privileged proxy-bypass sessions; use a direct ClickHouse connection for physical access")
	}
	name := reservedNamespaceViolationOnSurface(surfaces.WithLiterals, p.ReservedDatabases, p.ReservedRowIDColumn)
	if name != "" {
		return fmt.Errorf("storage-integrity reserved name %q is not addressable through the proxy (the operator guard rejects any mention, including an ordinary column with that name); use a direct ClickHouse connection for physical access", name)
	}
	if carrier := objectCarrierCallable(surfaces.OutsideLiterals); carrier != "" {
		return fmt.Errorf("storage-integrity object-carrier callable %q is not accepted on privileged proxy-bypass sessions; use a direct ClickHouse connection for physical access", carrier)
	}
	return nil
}

// peerRefusal is the peer rule set (spec 2026-10-14 §14) for a rewriter
// remote() loopback, whose SQL is ClickHouse's secondary query for a leg its
// origin emitted: a statement the peer scan cannot model, every
// {name:Identifier} placeholder, and every mention of a reserved database or
// of the reserved row-id column outside comments, string literals included,
// are refused with ClickHouse 392 (not retryable). Object-carrier callables
// are not refused: a legitimate leg may carry a nested remote() that
// ClickHouse pushed into it with a JOIN or an IN subquery.
func (p *Plugin) peerRefusal(ctx context.Context, peerAddress, sql string) error {
	surfaces, err := sqlsurface.ScanWith(sql, PeerScanOptions)
	if err != nil {
		return refusePeer(ctx, peerReasonStatementUnscannable, "", peerAddress, sql,
			"storage_integrity: a peer statement that cannot be checked for reserved names is refused: "+err.Error())
	}
	if identifierPlaceholderToken(surfaces.Tokens) {
		return refusePeer(ctx, peerReasonIdentifierPlaceholder, "", peerAddress, sql,
			"storage_integrity: a peer statement may not carry ClickHouse Identifier placeholders")
	}
	if name := reservedNamespaceViolationOnSurface(surfaces.WithLiterals, p.ReservedDatabases, p.ReservedRowIDColumn); name != "" {
		return refusePeer(ctx, peerReasonReservedName, name, peerAddress, sql,
			"storage_integrity: reserved name "+name+" is not addressable on a peer connection; the peer guard refuses any mention, string literals included")
	}
	return nil
}

// RunOnForward keeps the guard active after a session pivots to a peer.
func (*Plugin) RunOnForward() bool { return true }

// RunOnPeerTrust keeps the guard on peer-trusted sessions: OnQuery applies the
// peer rules to a remote() loopback and the operator rules to a peer session
// that carries an operator flag. A session forwarded from a peer reaches it
// either way and is treated as an ordinary one.
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
	surfaces, err := sqlsurface.Scan(sql)
	if err != nil {
		return "", err
	}
	return reservedNamespaceViolationOnSurface(surfaces.WithLiterals, databases, rowIDColumn), nil
}

// PeerReservedNamespaceViolation is ReservedNamespaceViolation on the peer
// scan (PeerScanOptions): string literals and quoted identifiers are decoded
// as ClickHouse reads them, so backslash-bearing spans are accepted and an
// encoded name such as 'hg\x5Fsafe' is found. It reports the first reserved
// name mentioned outside comments, string literals included, and returns a
// scan error as is. It does not check Identifier placeholders.
func PeerReservedNamespaceViolation(sql string, databases []string, rowIDColumn string) (string, error) {
	surfaces, err := sqlsurface.ScanWith(sql, PeerScanOptions)
	if err != nil {
		return "", err
	}
	return reservedNamespaceViolationOnSurface(surfaces.WithLiterals, databases, rowIDColumn), nil
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
	for _, identifier := range sqlsurface.Identifiers(surface) {
		for _, name := range names {
			if strings.EqualFold(identifier, name) {
				return name
			}
		}
	}
	return ""
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
		if !sqlsurface.IsIdentifierByte(sql[i]) {
			i++
			continue
		}
		start := i
		for i < len(sql) && sqlsurface.IsIdentifierByte(sql[i]) {
			i++
		}
		name := sql[start:i]
		call := i
		for call < len(sql) && (sql[call] == ' ' || sql[call] == '\t' || sql[call] == '\r' || sql[call] == '\n') {
			call++
		}
		if call < len(sql) && sql[call] == '(' && IsObjectCarrierName(name) {
			return name
		}
	}
	return ""
}

// IsObjectCarrierName reports whether name is a table-function or table-engine
// callable whose arguments can name a ClickHouse database or table. Shared
// with sipeerguard.
func IsObjectCarrierName(name string) bool {
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

// refusePeer counts and logs one peer refusal and returns its ClientError. The
// warn line names the peer and the statement, cut to maxLoggedStatementBytes;
// it logs no settings, but the statement text can carry a nested remote()'s
// peer-hello token, exactly as sipeerguard's refusal line can.
func refusePeer(ctx context.Context, reason, reservedName, peerAddress, sql, message string) error {
	peerRefusals.WithLabelValues(reason).Inc()
	_, logger := log.FromContext(ctx, "plugin", "sireserved")
	logger.Warnw("peer statement refused by storage-integrity reserved-name guard",
		"code", int(chproto.CodeQueryIsProhibited),
		"reason", reason,
		"reserved_name", reservedName,
		"peer_address", peerAddress,
		"statement", truncateStatement(sql))
	return &chproto.ClientError{Code: chproto.CodeQueryIsProhibited, Message: message}
}

// identifierPlaceholderToken reports whether tokens hold a ClickHouse
// {name:Identifier} query-parameter placeholder: the five tokens {, a name, :,
// Identifier (any case) and }, with any whitespace or comments between them,
// which is what ClickHouse parses as a substitution. It reads the token stream
// rather than OutsideLiterals, which keeps quoted identifiers: ClickHouse
// writes an expression's text into the projection alias of a secondary query,
// so `concat(s, '{p:Identifier}')` is an alias there, not a placeholder.
func identifierPlaceholderToken(tokens []sqlsurface.Token) bool {
	isPunct := func(i int, text string) bool {
		return i < len(tokens) && tokens[i].Kind == sqlsurface.TokenPunct && tokens[i].Text == text
	}
	isName := func(i int) bool {
		return i < len(tokens) && (tokens[i].Kind == sqlsurface.TokenWord || tokens[i].Kind == sqlsurface.TokenQuoted)
	}
	for i := range tokens {
		if isPunct(i, "{") && isName(i+1) && isPunct(i+2, ":") && isName(i+3) &&
			strings.EqualFold(tokens[i+3].Text, "Identifier") && isPunct(i+4, "}") {
			return true
		}
	}
	return false
}

// maxLoggedStatementBytes bounds the statement text a peer refusal log carries,
// as in sipeerguard (which imports this package, so it cannot be shared).
const maxLoggedStatementBytes = 512

// truncatedMarker is appended to a statement cut by truncateStatement.
const truncatedMarker = "…(truncated)"

// truncateStatement returns sql cut to at most maxLoggedStatementBytes bytes on
// a UTF-8 rune boundary, followed by truncatedMarker when anything was cut.
func truncateStatement(sql string) string {
	if len(sql) <= maxLoggedStatementBytes {
		return sql
	}
	cut := maxLoggedStatementBytes
	for cut > 0 && !utf8.RuneStart(sql[cut]) {
		cut--
	}
	return sql[:cut] + truncatedMarker
}

var (
	_ plugin.QueryPlugin             = (*Plugin)(nil)
	_ plugin.StrictQueryDecodePlugin = (*Plugin)(nil)
	_ plugin.ForwardAware            = (*Plugin)(nil)
	_ plugin.PeerTrustAware          = (*Plugin)(nil)
)
