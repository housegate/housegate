// Package sireserved protects protocol-owned storage-integrity names on
// privileged sessions that deliberately bypass the full SQL rewriter.
package sireserved

import (
	"context"
	"fmt"
	"strings"

	"github.com/housegate/housegate/pkg/plugin"
	"github.com/housegate/housegate/pkg/sqlsurface"
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
	surfaces, err := sqlsurface.Scan(sql)
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

var (
	_ plugin.QueryPlugin             = (*Plugin)(nil)
	_ plugin.StrictQueryDecodePlugin = (*Plugin)(nil)
	_ plugin.ForwardAware            = (*Plugin)(nil)
	_ plugin.PeerTrustAware          = (*Plugin)(nil)
)
