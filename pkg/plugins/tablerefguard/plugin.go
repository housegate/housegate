// Package tablerefguard is the ordinary-session defence-in-depth guard of
// spec 2026-09-26 §9.2 (T9). It refuses the lexically decidable subset of the
// table-reference policy on the original SQL, before forward and rewrite run;
// the rewriter engines remain the authority and the startup probe proves them.
//
// The guard reasons on pkg/sqlsurface's lexical model, never on a grammar, so
// every span that model cannot represent with certainty is refused under the
// scan rule (spec deviation D7): a stray `$` or one directly after an
// identifier byte, a bare `#`, an unterminated quote, comment or heredoc, and
// any byte >= 0x80 outside a quoted identifier, string literal, comment or
// heredoc body. The last includes a leading UTF-8 byte-order mark, which
// ClickHouse accepts: a known, safe false refusal.
package tablerefguard

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/housegate/housegate/pkg/log"
	"github.com/housegate/housegate/pkg/plugin"
	"github.com/housegate/housegate/pkg/sqlsurface"
)

// Mode selects whether a rule hit refuses the statement or is only counted.
type Mode string

const (
	// ModeEnforce refuses a statement that trips a rule. The empty mode
	// enforces too.
	ModeEnforce Mode = "enforce"
	// ModeObserve logs and counts a rule hit and lets the statement through.
	ModeObserve Mode = "observe"
)

// Rule names, used in the error and as the counter label.
const (
	RuleReservedName          = "reserved_name"
	RulePhysicalDatabase      = "physical_database"
	RuleCarrierCallable       = "carrier_callable"
	RuleIdentifierPlaceholder = "identifier_placeholder"
	RuleEscapedIdentifier     = "escaped_identifier"
	// RuleScan counts a statement the lexical model cannot scan (see the
	// package documentation). It is the sixth label, beyond the spec's five.
	RuleScan = "scan"
)

var rejections = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "clickhouse_proxy_tableref_guard_rejections_total",
	Help: "Statements the table-reference guard refused, or would refuse in observe mode, by rule.",
}, []string{"rule"})

func init() { prometheus.MustRegister(rejections) }

// Plugin is the table-reference guard.
type Plugin struct {
	// PhysicalDatabase is rewriter.physical_database; empty disables G2.
	PhysicalDatabase string
	// ReservedDatabases are the storage-integrity reserved databases (G1).
	ReservedDatabases []string
	// Mode is ModeEnforce or ModeObserve; empty enforces.
	Mode Mode
}

// OnQuery applies Check to every ordinary session, driver sessions included.
// Maintenance and platform-operator sessions are sireserved's; peer-trusted
// and forwarded-from-peer sessions carry SQL the origin already checked.
func (p *Plugin) OnQuery(ctx context.Context, qctx *plugin.QueryContext) error {
	if qctx == nil || qctx.Session == nil {
		return nil
	}
	snap := qctx.Session.State().Snapshot()
	if snap.Maintenance || snap.PlatformOperator || snap.IsPeerTrusted || snap.IsForwardedFromPeer {
		return nil
	}
	sql := qctx.OriginalSQL
	if qctx.Query != nil && qctx.Query.Body != "" {
		sql = qctx.Query.Body
	}
	rule, detail := Check(sql, p.PhysicalDatabase, p.ReservedDatabases)
	if rule == "" {
		return nil
	}
	rejections.WithLabelValues(rule).Inc()
	if p.Mode == ModeObserve {
		_, logger := log.FromContext(ctx)
		logger.Warnw("table-reference guard would refuse the statement (observe mode)", "rule", rule, "detail", detail)
		return nil
	}
	return fmt.Errorf("table-reference guard: %s: %s; the rewriter applies the same policy", rule, detail)
}

// RunOnForward keeps the guard on an origin-side forwarding session: the
// client is ordinary even after its session pivots to a peer.
func (*Plugin) RunOnForward() bool { return true }

// RunOnPeerTrust lets OnQuery decide; it skips peer-trusted sessions itself.
func (*Plugin) RunOnPeerTrust() bool { return true }

// RejectUndecodableQuery fails closed: an undecodable Query cannot be scanned.
func (*Plugin) RejectUndecodableQuery() bool { return true }

// Check applies the guard's rules in order and returns the first hit; an
// empty rule means the statement passes. The order is: scan refusals (G5 is
// one of them), G1 reserved names, G3 carrier callables, G2 the physical
// database, G4 identifier placeholders. G3 precedes G2 so merge('phys', …)
// is attributed to the carrier.
func Check(sql, physicalDatabase string, reserved []string) (rule, detail string) {
	surfaces, err := sqlsurface.ScanWith(sql, sqlsurface.Options{AllowStringEscapes: true})
	if err != nil {
		return scanRefusal(err)
	}
	// G1: a reserved database anywhere but a comment, string literals included.
	for _, id := range sqlsurface.Identifiers(surfaces.WithLiterals) {
		for _, name := range reserved {
			if name != "" && strings.EqualFold(id, name) {
				return RuleReservedName, fmt.Sprintf("reserved database %s is not addressable", name)
			}
		}
	}
	// G3: a call to a §5 T5 refused table function.
	if name := refusedTableFunctionCall(surfaces.Tokens); name != "" {
		return RuleCarrierCallable, fmt.Sprintf("carrier callable %s is not accepted", name)
	}
	// G2: the physical database as a qualifier, a USE / SHOW target, or a
	// carrier or lookup argument.
	if physicalDatabase != "" && physicalDatabaseAddressed(surfaces.Tokens, physicalDatabase) {
		return RulePhysicalDatabase, fmt.Sprintf("protected database %s is not addressable", physicalDatabase)
	}
	// G4: Identifier placeholders, position-blind by design (spec §9.2).
	if sqlsurface.ContainsIdentifierPlaceholder(surfaces.OutsideLiterals) {
		return RuleIdentifierPlaceholder, "ClickHouse Identifier query parameters are not accepted"
	}
	return "", ""
}

// scanRefusal maps a pkg/sqlsurface scan error to its rule. It is the only
// place a scan error becomes a rule: G5 is the escaped-identifier sentinel,
// and every other error is the scan rule (decision D7).
func scanRefusal(err error) (rule, detail string) {
	switch {
	case errors.Is(err, sqlsurface.ErrEscapedQuotedIdentifier):
		// G5: ClickHouse decodes escapes in quoted identifiers; the guard does not.
		return RuleEscapedIdentifier, "a backslash inside a quoted identifier is not accepted"
	case errors.Is(err, sqlsurface.ErrStrayDollar):
		return RuleScan, "a $ that opens no heredoc, or that follows an identifier byte, is not accepted"
	case errors.Is(err, sqlsurface.ErrBareHash):
		return RuleScan, "a # that opens no comment is not accepted"
	case errors.Is(err, sqlsurface.ErrNonASCII):
		return RuleScan, "a non-ASCII byte outside a quoted identifier, string literal, comment or heredoc is not accepted"
	default:
		// The remaining scan errors are unterminated spans; the scanner's own
		// text names which. (ErrStringLiteralBackslash cannot occur: the guard
		// scans with AllowStringEscapes.)
		return RuleScan, fmt.Sprintf("the statement cannot be scanned: %v", err)
	}
}

func isName(t sqlsurface.Token) bool {
	return t.Kind == sqlsurface.TokenWord || t.Kind == sqlsurface.TokenQuoted
}

func isPunct(t sqlsurface.Token, p string) bool {
	return t.Kind == sqlsurface.TokenPunct && t.Text == p
}

func isWord(t sqlsurface.Token, w string) bool {
	return t.Kind == sqlsurface.TokenWord && strings.EqualFold(t.Text, w)
}

// isCall reports whether tokens[i] is a name immediately followed, in the
// token stream (so across whitespace and comments), by an opening parenthesis.
func isCall(tokens []sqlsurface.Token, i int) bool {
	return i+1 < len(tokens) && isName(tokens[i]) && isPunct(tokens[i+1], "(")
}

// refusedTableFunctionCall returns the first call to a §5 T5 refused table
// function.
func refusedTableFunctionCall(tokens []sqlsurface.Token) string {
	for i := range tokens {
		if isCall(tokens, i) && isRefusedTableFunction(tokens[i].Text) {
			return tokens[i].Text
		}
	}
	return ""
}

// isRefusedTableFunction is spec 2026-09-26 §5 T5's refused table-function
// list, matched case-insensitively as ClickHouse resolves the names.
//
// The list's "every name starting with mergeTree" deliberately excludes the
// bare name: no table function is called mergeTree, while `ENGINE =
// MergeTree()` is the allowed table engine every tenant CREATE uses, and the
// guard cannot tell an engine position from a table-function one. A
// `mergeTree(...)` table function is unknown to ClickHouse and refused by the
// engines (T5 "not recognised").
func isRefusedTableFunction(name string) bool {
	lower := strings.ToLower(name)
	if lower == "mergetree" {
		return false
	}
	if strings.HasPrefix(lower, "mergetree") {
		return true
	}
	switch lower {
	case "merge", "remote", "remotesecure", "cluster", "clusterallreplicas", "loop", "dictionary",
		"timeseriesdata", "timeseriestags", "timeseriesmetrics", "timeseriesselector",
		"prometheusquery", "prometheusqueryrange", "clickhouse",
		"mysql", "postgresql", "mongodb", "jdbc", "odbc", "executable", "fuzzquery", "fuzzjson":
		return true
	}
	return false
}

// isLookupFunction is spec 2026-09-26 T6's string-form lookup list: joinGet,
// joinGetOrNull, every dictGet* (dictGetHierarchy, dictGetChildren and
// dictGetDescendants included), dictHas, dictIsIn and hasColumnInTable.
func isLookupFunction(name string) bool {
	lower := strings.ToLower(name)
	if strings.HasPrefix(lower, "dictget") {
		return true
	}
	switch lower {
	case "joinget", "joingetornull", "dicthas", "dictisin", "hascolumnintable":
		return true
	}
	return false
}

// physicalDatabaseAddressed reports whether the token stream addresses the
// physical database, which is matched case-sensitively as ClickHouse matches
// database names, as:
//
//   - a qualifier: a bare, backtick- or double-quoted name followed by a dot
//     token. Tokens drop whitespace and comments, so `phys /* c */ .x` is a
//     qualifier, while the one quoted token `phys.x` in db1.`phys.x` is not.
//     Numbers are not modelled, so a tuple element such as phys.1 (or
//     `default .5` when the physical database is called default) is
//     over-reported as a qualifier; that only costs a false refusal;
//   - any name in a statement that starts with USE;
//   - the name after FROM or IN in a statement that starts with SHOW;
//   - inside the argument list of a carrier (G3) or lookup (T6) call, a name
//     or a string literal (a heredoc body included) equal to the physical
//     database or starting with it followed by a dot.
//
// Elsewhere the name is allowed: a column, alias, unqualified table or tenant
// string literal may share it, and DEFAULT is a keyword when the physical
// database is called default.
func physicalDatabaseAddressed(tokens []sqlsurface.Token, physical string) bool {
	named := func(t sqlsurface.Token) bool { return isName(t) && t.Text == physical }
	for i := 0; i+1 < len(tokens); i++ {
		if named(tokens[i]) && isPunct(tokens[i+1], ".") {
			return true
		}
	}
	if len(tokens) > 0 && isWord(tokens[0], "USE") {
		for _, t := range tokens[1:] {
			if named(t) {
				return true
			}
		}
	}
	if len(tokens) > 0 && isWord(tokens[0], "SHOW") {
		for i := 1; i+1 < len(tokens); i++ {
			if (isWord(tokens[i], "FROM") || isWord(tokens[i], "IN")) && named(tokens[i+1]) {
				return true
			}
		}
	}
	for i := range tokens {
		if !isCall(tokens, i) || !(isLookupFunction(tokens[i].Text) || isRefusedTableFunction(tokens[i].Text)) {
			continue
		}
		if argumentsAddress(tokens[i+1:], physical, named) {
			return true
		}
	}
	return false
}

// argumentsAddress scans the balanced parenthesised list that tokens starts
// with (tokens[0] is its opening parenthesis) for the physical database as a
// name or as a string naming it or a table in it. An unbalanced list is
// scanned to the end of the statement.
func argumentsAddress(tokens []sqlsurface.Token, physical string, named func(sqlsurface.Token) bool) bool {
	depth := 0
	for _, t := range tokens {
		switch {
		case isPunct(t, "("):
			depth++
		case isPunct(t, ")"):
			depth--
			if depth == 0 {
				return false
			}
		case named(t):
			return true
		case t.Kind == sqlsurface.TokenString && (t.Text == physical || strings.HasPrefix(t.Text, physical+".")):
			return true
		}
	}
	return false
}

var (
	_ plugin.QueryPlugin             = (*Plugin)(nil)
	_ plugin.StrictQueryDecodePlugin = (*Plugin)(nil)
	_ plugin.ForwardAware            = (*Plugin)(nil)
	_ plugin.PeerTrustAware          = (*Plugin)(nil)
)
