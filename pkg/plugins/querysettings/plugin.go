// Package querysettings refuses, before forwarding, the native-protocol
// Query-packet settings that the rewriter engines refuse in SQL (spec
// 2026-09-26 §9.7, R5 and §13 round 2). Query-packet settings never pass
// through the rewriter, so without this check a client could send
// enable_analyzer = 0 or enable_global_with_statement = 0 and change what a
// name the rewriter already trusted binds to.
//
// The check has no observe mode (plan D6). A refusal is an ordinary plugin
// error: the relay answers it with an Exception (code 403) and drains the
// rejected query's input, so the session stays usable for the next query.
// Every refusal is logged at warn level with the setting, connection, account
// and user, and counted in clickhouse_proxy_query_settings_rejections_total.
//
// A pre-54429 Query packet carries its settings in the old UInt64 format.
// Its decoding is not proven against a hostile client, so any old-format
// setting is refused on a governed session, whatever its name or value; only
// very old clients use that format.
//
// The plugin is wired with the table-reference guard on every server that
// forwards to ClickHouse, rewriter or not. A router-only server given a
// host-injected rewriter wires neither: its sessions are forwarded to a peer
// whose own chain runs this check (accepted gap).
package querysettings

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/housegate/housegate/pkg/log"
	"github.com/housegate/housegate/pkg/plugin"
)

// Counter labels beyond the fixed refused names, which keep the label set
// bounded: a refused name outside the fixed list can only be a _dialect
// suffix match, and an old-format setting can carry any name.
const (
	labelOtherDialect = "other_dialect"
	labelOldFormat    = "old_format"
)

// Log reasons.
const (
	reasonRefusedSetting = "refused_setting"
	reasonOldFormat      = "old_format_settings"
)

var rejections = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "clickhouse_proxy_query_settings_rejections_total",
	Help: "Queries refused for a setting in the native-protocol Query packet, by setting (fixed refused names, other_dialect, old_format).",
}, []string{"setting"})

func init() { prometheus.MustRegister(rejections) }

// rejectionLabel maps a refused setting name to its bounded counter label:
// the lowercased name when it is a fixed refused name, otherwise
// other_dialect (the only other way a name is refused).
func rejectionLabel(name string) string {
	lower := strings.ToLower(name)
	if refusedWhateverTheValue[lower] || analyzerSettings[lower] {
		return lower
	}
	return labelOtherDialect
}

// refusedWhateverTheValue mirrors rewriter-go v0.16.0
// internal/engine/settings.go:46-66 (sqlBearingSettings) and rewriter-grpc
// v0.16.0 src/handlers/table_reference.cc:1332-1354 (sqlBearingSetting):
// settings whose value is SQL evaluated against tables, the dialect switches,
// the name-binding settings and the old analyzer's name resolution. Keep the
// three lists equal; the integration suite checks them against the engine.
// allow_deprecated_syntax_for_merge_tree is deliberately absent (spec §13).
var refusedWhateverTheValue = map[string]bool{
	"additional_table_filters":            true,
	"additional_result_filter":            true,
	"parallel_replicas_custom_key":        true,
	"dialect":                             true,
	"polyglot_dialect":                    true,
	"allow_experimental_polyglot_dialect": true,
	"allow_experimental_prql_dialect":     true,
	"allow_experimental_kusto_dialect":    true,
	"enable_global_with_statement":        true,
	"compatibility":                       true,
	"implicit_table_at_top_level":         true,
	"promql_table":                        true,
	"promql_database":                     true,
	"legacy_column_name_of_tuple_literal": true,
	"profile":                             true,
}

// analyzerSettings mirrors rewriter-go internal/engine/settings.go:71-74 and
// rewriter-grpc table_reference.cc:1358-1361 (isAnalyzerSetting): the
// analyzer switch and its alias, refused unless the value keeps the new
// analyzer on.
var analyzerSettings = map[string]bool{
	"enable_analyzer":             true,
	"allow_experimental_analyzer": true,
}

// Refused reports whether a Query-packet setting is refused: a listed name, or
// any name ending in _dialect, whatever the value (names compared
// case-insensitively, a deliberate over-match like the engines', rewriter-go
// settings.go:122-125); or an analyzer switch whose value is not a closed true
// spelling (settings.go:98-100, SettingRefused). Names arrive decoded from the
// packet, so R5's escaped-spelling rule has nothing to apply to here.
func Refused(name, value string) bool {
	lower := strings.ToLower(name)
	if refusedWhateverTheValue[lower] || strings.HasSuffix(lower, "_dialect") {
		return true
	}
	return analyzerSettings[lower] && !trueSpelling(value)
}

// trueSpelling mirrors rewriter-go TrueLiteralSpelling
// (internal/engine/settings.go:87-93): 1, true, '1', 'true', any case. Every
// other spelling ClickHouse also reads as true is refused, and the value is
// compared verbatim (no trimming, no unquoting).
//
// Measured 2026-10-01 against ClickHouse 25.8.28 and 26.7.5 (raw capture of
// the Query packet): clickhouse-go v2.47 sends a non-custom value with the
// Important flag as fmt.Sprint (1, true, false) and a CustomSetting with the
// Custom flag as a quoted Field dump ('1'); clickhouse-client 26.7 sends
// --enable_analyzer=TRUE (and a SETTINGS clause it parsed) under the alias
// allow_experimental_analyzer, Important, normalised to 1 / 0, and expands
// --compatibility=21.1 into compatibility, enable_global_with_statement = 0,
// legacy_column_name_of_tuple_literal = 1 and allow_experimental_analyzer = 0.
// Every admitted spelling either sets the analyzer on or fails the query
// (code 467 for a quoted non-custom value); none turns it off.
func trueSpelling(value string) bool {
	switch strings.ToLower(value) {
	case "1", "true", "'1'", "'true'":
		return true
	}
	return false
}

// RefusedNames returns the fixed refused names, analyzer switches included,
// sorted. The _dialect suffix rule is not enumerable and is not listed.
func RefusedNames() []string {
	out := make([]string, 0, len(refusedWhateverTheValue)+len(analyzerSettings))
	for name := range refusedWhateverTheValue {
		out = append(out, name)
	}
	for name := range analyzerSettings {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// Plugin refuses a query whose Query packet carries a refused setting.
type Plugin struct{}

// OnQuery checks every Query-packet setting and names the first refused one;
// any pre-54429 (old-format) setting is refused. Maintenance and
// platform-operator sessions bypass the rewriter and therefore this check;
// every other session the chain hands it (ordinary, driver, origin-side
// forwarding, forwarded-from-peer) is checked.
func (*Plugin) OnQuery(ctx context.Context, qctx *plugin.QueryContext) error {
	if qctx == nil || qctx.Query == nil {
		return nil
	}
	if qctx.Session != nil {
		snap := qctx.Session.State().Snapshot()
		if snap.Maintenance || snap.PlatformOperator {
			return nil
		}
	}
	for _, s := range qctx.Query.Settings {
		if Refused(s.Key, s.Value) {
			return refuse(ctx, qctx, s.Key, rejectionLabel(s.Key), reasonRefusedSetting)
		}
	}
	if len(qctx.Query.OldSettings) > 0 {
		return refuse(ctx, qctx, qctx.Query.OldSettings[0].Key, labelOldFormat, reasonOldFormat)
	}
	return nil
}

// refuse counts and logs a refusal and returns the client-facing error.
func refuse(ctx context.Context, qctx *plugin.QueryContext, name, label, reason string) error {
	rejections.WithLabelValues(label).Inc()
	kv := []any{"setting", name, "reason", reason, "query_id", qctx.Query.ID}
	if qctx.Session != nil {
		snap := qctx.Session.State().Snapshot()
		kv = append(kv, "conn", qctx.Session.ID(), "account", snap.Identity.UserID, "user", snap.AuthenticatedUser)
	}
	_, logger := log.FromContext(ctx)
	logger.Warnw("query refused: native-protocol query setting is not accepted", kv...)
	return fmt.Errorf("table setting %s is not accepted (native-protocol query setting)", name)
}

// RunOnPeerTrust skips remote() loopback peer sessions, whose SQL the origin
// rewrote; the chain's IsForwardedFromPeer override still runs the check on
// forwarded-from-peer sessions, whose SQL this host's engine governs.
func (*Plugin) RunOnPeerTrust() bool { return false }

// RunOnForward checks at the origin too, before a session pivots to a peer,
// so an older receiving host cannot let a refused setting through.
func (*Plugin) RunOnForward() bool { return true }

// RejectUndecodableQuery fails closed: undecodable settings cannot be checked.
func (*Plugin) RejectUndecodableQuery() bool { return true }

var (
	_ plugin.QueryPlugin             = (*Plugin)(nil)
	_ plugin.StrictQueryDecodePlugin = (*Plugin)(nil)
	_ plugin.ForwardAware            = (*Plugin)(nil)
	_ plugin.PeerTrustAware          = (*Plugin)(nil)
)
