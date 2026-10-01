// Package tablerefguard is the ordinary-session defence-in-depth guard of
// spec 2026-09-26 §9.2 (T9). It refuses the lexically decidable subset of the
// table-reference policy on the original SQL, before forward and rewrite run;
// the rewriter engines remain the authority and the startup probe proves them.
//
// The guard reasons on pkg/sqlsurface's lexical model, never on a grammar, so
// every span that model cannot represent with certainty is refused under the
// scan rule (spec deviation D7): a stray `$` or one directly after an
// identifier byte, a bare `#`, an unterminated quote, comment or heredoc, a
// \x string escape without two hex digits, and any byte >= 0x80 outside a
// quoted identifier, string literal, comment or heredoc body. The last
// includes a leading UTF-8 byte-order mark, which ClickHouse accepts: a
// known, safe false refusal.
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

// OnQuery applies Check to every ordinary session, driver sessions included,
// and to sessions forwarded from a peer: this host owns their original client
// SQL, as it does for rewrite and the Query-packet check, and the origin may
// be a router-only server that runs no guard. Maintenance and
// platform-operator sessions are sireserved's; a peer-trusted remote()
// loopback carries SQL its origin already rewrote.
func (p *Plugin) OnQuery(ctx context.Context, qctx *plugin.QueryContext) error {
	if qctx == nil || qctx.Session == nil {
		return nil
	}
	snap := qctx.Session.State().Snapshot()
	if snap.Maintenance || snap.PlatformOperator || (snap.IsPeerTrusted && !snap.IsForwardedFromPeer) {
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

// RunOnPeerTrust lets OnQuery decide; it skips remote() loopbacks itself and
// checks sessions forwarded from a peer.
func (*Plugin) RunOnPeerTrust() bool { return true }

// RejectUndecodableQuery fails closed: an undecodable Query cannot be scanned.
func (*Plugin) RejectUndecodableQuery() bool { return true }

// Check applies the guard's rules in order and returns the first hit; an
// empty rule means the statement passes. The order is: scan refusals (G5 is
// one of them), G1 reserved names, G3 carrier callables and table engines,
// G2 the physical database, G4 identifier placeholders. G3 precedes G2 so
// merge('phys', …) is attributed to the carrier.
//
// Single-quoted literals are compared as ClickHouse decodes them
// (sqlsurface.Options.DecodeStringEscapes): '\x70hys.j' is phys.j to joinGet,
// measured on ClickHouse 26.8.1. A \x escape without two hex digits is a scan
// refusal. Heredoc bodies are raw, as in ClickHouse.
func Check(sql, physicalDatabase string, reserved []string) (rule, detail string) {
	surfaces, err := sqlsurface.ScanWith(sql, sqlsurface.Options{DecodeStringEscapes: true})
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
	tokens := surfaces.Tokens
	engine := tableEngineClause(tokens)
	// G3: a call to a table function the engines refuse under §5 T5, and an
	// ENGINE clause outside T5's engine allowlist.
	if name := refusedTableFunctionCall(tokens, engine); name != "" {
		return RuleCarrierCallable, fmt.Sprintf("table function %s is not accepted", name)
	}
	if engine.at >= 0 && !tableEngineAllowed(engine.name, engine.args) {
		return RuleCarrierCallable, fmt.Sprintf("table engine %s is not accepted", engine.name)
	}
	// G2: the physical database as a qualifier, a USE / SHOW / DATABASE
	// target, or a carrier or lookup argument.
	if physicalDatabase != "" && physicalDatabaseAddressed(tokens, engine, physicalDatabase) {
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
	case errors.Is(err, sqlsurface.ErrUndecodableStringEscape):
		return RuleScan, `a \x escape without two hex digits is not accepted`
	default:
		// The remaining scan errors are unterminated spans; the scanner's own
		// text names which. (ErrStringLiteralBackslash cannot occur: the guard
		// decodes string escapes.)
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

// wordAt reports whether tokens[i] exists and is one of words.
func wordAt(tokens []sqlsurface.Token, i int, words ...string) bool {
	if i < 0 || i >= len(tokens) {
		return false
	}
	for _, w := range words {
		if isWord(tokens[i], w) {
			return true
		}
	}
	return false
}

// engineClause locates the ENGINE clause of a CREATE / REPLACE / ATTACH
// TABLE or VIEW statement: at is the index of the engine name token, or -1.
type engineClause struct {
	at   int
	name string
	args int // 0 for no or empty parentheses, 1 for any argument
}

// tableEngineClause finds the storage engine of a table or view statement.
// ClickHouse accepts `ENGINE [=] Name[(…)]`, bare or quoted, as the first
// storage clause and only once (measured on 26.8.1: an ORDER BY, PARTITION
// BY, SETTINGS or COMMENT before ENGINE, or a second ENGINE, is a syntax
// error), so the first ENGINE keyword at parenthesis depth 0 is the clause.
// A keyword-shaped ENGINE in a name position (a table called engine) or after
// BY, KEY, a comma or an operator (a column called engine) is not, and the
// scan stops at the query body of `AS SELECT` / `AS WITH` / `AS (`. Database
// statements are excluded: their engines are not T5's (spec §5 covers
// CREATE TABLE and materialized-view engines, rewriter-go v0.16.0
// internal/engine/nodes.go:370-407).
func tableEngineClause(tokens []sqlsurface.Token) engineClause {
	none := engineClause{at: -1}
	if !tableOrViewStatement(tokens) {
		return none
	}
	depth := 0
	for i, t := range tokens {
		switch {
		case isPunct(t, "("):
			depth++
			continue
		case isPunct(t, ")"):
			depth--
			continue
		}
		if depth != 0 {
			continue
		}
		if isWord(t, "AS") && (wordAt(tokens, i+1, "SELECT", "WITH") || i+1 < len(tokens) && isPunct(tokens[i+1], "(")) {
			return none
		}
		if !isWord(t, "ENGINE") || !engineKeywordPosition(tokens, i) {
			continue
		}
		j := i + 1
		if j < len(tokens) && isPunct(tokens[j], "=") {
			j++
		}
		if j >= len(tokens) || !isName(tokens[j]) {
			return none
		}
		clause := engineClause{at: j, name: tokens[j].Text}
		if j+2 < len(tokens) && isPunct(tokens[j+1], "(") && !isPunct(tokens[j+2], ")") {
			clause.args = 1
		}
		return clause
	}
	return none
}

// tableOrViewStatement reports a CREATE / REPLACE / ATTACH statement whose
// header names a TABLE or a VIEW.
func tableOrViewStatement(tokens []sqlsurface.Token) bool {
	if !wordAt(tokens, 0, "CREATE", "REPLACE", "ATTACH") {
		return false
	}
	for i := 1; i < len(tokens); i++ {
		switch {
		case wordAt(tokens, i, "TABLE", "VIEW"):
			return true
		case wordAt(tokens, i, "OR", "REPLACE", "TEMPORARY", "MATERIALIZED", "LIVE", "WINDOW"):
			continue
		default:
			return false
		}
	}
	return false
}

// engineKeywordPosition reports whether the ENGINE word at i can be the
// keyword: it must follow the column list `)`, a name (the table, view or
// cluster name) or a string (a UUID), and that name must not itself sit in a
// keyword's object position.
func engineKeywordPosition(tokens []sqlsurface.Token, i int) bool {
	if i == 0 {
		return false
	}
	prev := tokens[i-1]
	switch {
	case isPunct(prev, ")"), prev.Kind == sqlsurface.TokenString:
		return true
	case !isName(prev):
		return false
	}
	return !wordAt(tokens, i-1, "TABLE", "VIEW", "EXISTS", "TO", "BY", "KEY", "AS")
}

// allowedTableEngines is spec 2026-09-26 §5 T5's engine allowlist in
// ClickHouse's own, case-sensitive spelling, verbatim from rewriter-go
// v0.16.0 internal/engine/allowlists.go:57-62.
var allowedTableEngines = map[string]bool{
	"MergeTree": true, "ReplacingMergeTree": true, "SummingMergeTree": true, "AggregatingMergeTree": true,
	"CollapsingMergeTree": true, "VersionedCollapsingMergeTree": true, "GraphiteMergeTree": true,
	"Memory": true, "Log": true, "TinyLog": true, "StripeLog": true, "Null": true, "Set": true, "Join": true,
	"View": true, "MaterializedView": true, "LiveView": true,
}

// tableEngineAllowed mirrors rewriter-go v0.16.0 ClassifyTableEngine
// (internal/engine/allowlists.go:78-95): an allowed engine in its exact
// spelling, or its Replicated form written without arguments. Everything
// else, a case variant included, is refused.
func tableEngineAllowed(name string, args int) bool {
	if base, ok := strings.CutPrefix(name, "Replicated"); ok && allowedTableEngines[base] {
		return args == 0
	}
	return allowedTableEngines[name]
}

// isCall reports whether tokens[i] calls a function: a name immediately
// followed, in the token stream (so across whitespace and comments), by an
// opening parenthesis, and not in a position where ClickHouse reads the
// name as a table, view, dictionary, database, index or projection followed
// by its column list or expression, nor the engine of a table statement.
// Each exempt position was measured on ClickHouse 26.8.1 to reject a table
// function (`INSERT INTO remote(…)`, `CREATE TABLE merge(…) (…)`,
// `CREATE MATERIALIZED VIEW mv TO merge(…)`, `RENAME TABLE a TO merge(…)`,
// `CREATE DICTIONARY merge(…)`, `CREATE DATABASE remote(…)`, `PROJECTION
// merge(…)` are syntax errors; `INDEX remote ('a', 'b')` names an index;
// `db1.merge(…)` is an unknown function). `INSERT INTO [TABLE] FUNCTION f(…)`,
// `DESC[RIBE] [TABLE] f(…)`, FROM / JOIN / AS and every expression position
// stay calls.
func isCall(tokens []sqlsurface.Token, i int, engine engineClause) bool {
	if i+1 >= len(tokens) || !isName(tokens[i]) || !isPunct(tokens[i+1], "(") || i == engine.at {
		return false
	}
	if i == 0 {
		return true
	}
	switch {
	case isPunct(tokens[i-1], "."):
		return false
	case wordAt(tokens, i-1, "INTO", "TO", "INDEX", "PROJECTION", "DATABASE"):
		return false
	case wordAt(tokens, i-1, "TABLE"):
		return !wordAt(tokens, i-2, "CREATE", "ATTACH", "REPLACE", "TEMPORARY", "INTO")
	case wordAt(tokens, i-1, "VIEW"):
		return !wordAt(tokens, i-2, "CREATE", "ATTACH", "REPLACE", "MATERIALIZED", "LIVE", "WINDOW")
	case wordAt(tokens, i-1, "DICTIONARY"):
		return !wordAt(tokens, i-2, "CREATE", "ATTACH", "REPLACE")
	case wordAt(tokens, i-1, "EXISTS"):
		return !(wordAt(tokens, i-2, "NOT") && wordAt(tokens, i-3, "IF"))
	}
	return true
}

// refusedTableFunctionCall returns the first call to a table function the
// engines refuse under §5 T5.
func refusedTableFunctionCall(tokens []sqlsurface.Token, engine engineClause) string {
	for i := range tokens {
		if isCall(tokens, i, engine) && isRefusedTableFunction(tokens[i].Text) {
			return tokens[i].Text
		}
	}
	return ""
}

// refusedTableFunctions is the engines' T5 "not accepted" list, verbatim from
// rewriter-go v0.16.0 internal/engine/allowlists.go:16-23 (every name
// starting with mergetree is refused too, :43).
var refusedTableFunctions = map[string]bool{
	"merge": true, "remote": true, "remotesecure": true, "cluster": true, "clusterallreplicas": true,
	"loop": true, "dictionary": true, "mergetreeindex": true, "mergetreeprojection": true,
	"timeseriesdata": true, "timeseriestags": true, "timeseriesmetrics": true, "timeseriesselector": true,
	"prometheusquery": true, "prometheusqueryrange": true, "clickhouse": true,
	"mysql": true, "postgresql": true, "mongodb": true, "jdbc": true, "odbc": true,
	"executable": true, "fuzzquery": true, "fuzzjson": true,
}

// unrecognisedTableFunctions are the ClickHouse 26.8.1 table functions
// (system.table_functions) on none of the engines' three T5 lists, which the
// engines therefore refuse as "not recognised" (allowlists.go:116). A name
// shared with an ordinary function would be a false refusal; none is
// (measured against system.functions). The engines refuse any other unknown
// name too, but the guard cannot tell a table function from a scalar call by
// position, so it lists names.
var unrecognisedTableFunctions = map[string]bool{
	"sqlstandardvalues": true, "arrowflight": true, "eval": true, "filesystem": true, "hive": true,
	"primes": true, "timeseriessamples": true, "viewexplain": true, "viewifpermitted": true, "ytsaurus": true,
	"deltalakeazure": true, "deltalakeazurecluster": true, "deltalakelocal": true, "deltalakes3": true, "deltalakes3cluster": true,
	"icebergazure": true, "icebergazurecluster": true, "iceberghdfs": true, "iceberghdfscluster": true,
	"iceberglocal": true, "iceberglocalcluster": true, "icebergs3": true, "icebergs3cluster": true,
	"paimon": true, "paimonazure": true, "paimonazurecluster": true, "paimoncluster": true, "paimonhdfs": true,
	"paimonhdfscluster": true, "paimonlocal": true, "paimons3": true, "paimons3cluster": true,
}

// isRefusedTableFunction matches case-insensitively, as the engines do. The
// mergetree prefix spares mergeTreePartInfo, an ordinary function in
// ClickHouse 26.8.1 that the engines never see as a table function.
func isRefusedTableFunction(name string) bool {
	lower := strings.ToLower(name)
	if refusedTableFunctions[lower] || unrecognisedTableFunctions[lower] {
		return true
	}
	return strings.HasPrefix(lower, "mergetree") && lower != "mergetreepartinfo"
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
//   - the object of DATABASE (CREATE, DROP, ATTACH, DETACH, ALTER, TRUNCATE,
//     EXISTS, DESCRIBE, SHOW CREATE … DATABASE [IF [NOT] EXISTS] phys,
//     SYSTEM … DATABASE [REPLICA] phys), and any name in a RENAME, EXCHANGE,
//     BACKUP or RESTORE statement that names a database (RESTORE DATABASE x
//     AS phys);
//   - the object of TABLES FROM / IN [IF EXISTS] in any statement
//     (TRUNCATE [ALL] TABLES FROM phys empties the database; SHOW
//     [TEMPORARY] TABLES FROM phys);
//   - inside the argument list of a carrier (G3) or lookup (T6) call, a name
//     or a string literal (decoded; a heredoc body raw) equal to the
//     physical database or starting with it followed by a dot.
//
// The positions were checked with clickhouse format on 26.8.1 against a
// list of database-object statements (the others name a database only as a
// qualifier, e.g. GRANT … ON phys.* or SYSTEM FLUSH DISTRIBUTED phys.t;
// CHECK / DROP ALL TABLES FROM and SHOW TABLE STATUS / MERGES FROM do not
// parse). The list is not a proof of completeness; the engines remain the
// authority.
//
// Elsewhere the name is allowed: a column, alias, unqualified table or tenant
// string literal may share it, and DEFAULT is a keyword when the physical
// database is called default.
func physicalDatabaseAddressed(tokens []sqlsurface.Token, engine engineClause, physical string) bool {
	named := func(t sqlsurface.Token) bool { return isName(t) && t.Text == physical }
	for i := 0; i+1 < len(tokens); i++ {
		if named(tokens[i]) && isPunct(tokens[i+1], ".") {
			return true
		}
	}
	anyNamed := func() bool {
		for _, t := range tokens {
			if named(t) {
				return true
			}
		}
		return false
	}
	if wordAt(tokens, 0, "USE") && anyNamed() {
		return true
	}
	if wordAt(tokens, 0, "SHOW") {
		for i := 1; i+1 < len(tokens); i++ {
			if wordAt(tokens, i, "FROM", "IN") && named(tokens[i+1]) {
				return true
			}
		}
	}
	for i := range tokens {
		if !wordAt(tokens, i, "DATABASE") {
			continue
		}
		if wordAt(tokens, 0, "RENAME", "EXCHANGE", "BACKUP", "RESTORE") && anyNamed() {
			return true
		}
		j := i + 1
		for wordAt(tokens, j, "IF", "NOT", "EXISTS", "REPLICA") {
			j++
		}
		if j < len(tokens) && named(tokens[j]) {
			return true
		}
	}
	for i := range tokens {
		if !wordAt(tokens, i, "TABLES") || !wordAt(tokens, i+1, "FROM", "IN") {
			continue
		}
		j := i + 2
		for wordAt(tokens, j, "IF", "EXISTS") {
			j++
		}
		if j < len(tokens) && named(tokens[j]) {
			return true
		}
	}
	for i := range tokens {
		if !isCall(tokens, i, engine) || !(isLookupFunction(tokens[i].Text) || isRefusedTableFunction(tokens[i].Text)) {
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
