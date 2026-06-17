// Package indexingusage meters driver-side INSERT volume on the
// housegate wire and reports generic coordinates (which logical
// database + table received the INSERT, plus the raw log_comment) to
// the embedding host (sentio-node) via billing.IndexingUsageReporter.
//
// Before this plugin existed, the driver itself counted rows in its
// meter.Flush() loop and called sentio-node's UsageService.AsyncSave
// gRPC directly. The decentralised topology splits the write path so
// the driver now ships INSERTs through a housegate sidecar; the
// housegate / clickhouse hop is the natural choke point for metering.
//
// Division of labour: this plugin does only the wire-level *detection*
// that requires housegate-internal session state (is this a driver-
// signed INSERT, and into which logical db/table?). It carries NO
// Sentio billing-domain attribution — it does not resolve the owning
// processor, the on-chain SKU, or backfill. Those all live in the
// host's IndexingUsageReporter, which has the same NetworkState
// registry and looks them up itself. The plugin does not even read the
// registry; it just forwards the rewriter-resolved logical db/table and
// the raw log_comment. That keeps housegate free of billing semantics.
//
// Per-INSERT data emitted:
//   - LogicalDatabase + Table: the rewriter-resolved destination
//     (AccessedTables[0]). The host maps the database to a processor and
//     the table to a SKU, and drops non-processor / non-billable ones.
//   - LogComment: the raw, unparsed `log_comment` setting value (the
//     driver sets it per commit in sentioxyz/sentio PR #18293); empty
//     when absent. The host parses out the `watching` flag.
//   - Units: hard-coded to 1 per INSERT (see TODO below).
//
// TODO(row-count): swap the 1-unit-per-INSERT placeholder for actual
// row count metered off the wire. ClickHouse native INSERT puts rows
// in subsequent ClientCodeData packets (not in the Query packet's SQL
// body), so OnQuery alone cannot see them. The full row-count path
// needs chproto.Codec to surface num_rows, a new ClientDataBlockPlugin
// hook fanned out by relay, and this plugin stashing attribution in
// OnQuery + summing rows in OnClientDataBlock + reporting once in
// OnQueryComplete (which would also fix bill-before-execute).
//
// Sessions skipped:
//   - non-driver (IsDriver=false): metered via the existing query
//     usage path; double-billing would result.
//   - routed sessions (IsRouted): the originating proxy already
//     metered.
//   - peer-trusted forwarded sessions: same — RunOnPeerTrust=false.
//   - forward-pivot host side: the entry proxy did the work,
//     RunOnForward=false.
package indexingusage

import (
	"context"

	"housegate/housegate/pkg/billing"
	"housegate/housegate/pkg/log"
	"housegate/housegate/pkg/plugin"
	"housegate/housegate/pkg/sqlmeta"
)

// LogCommentSettingKey is the ClickHouse session setting the driver
// uses to ship per-commit context (a JSON object string). See
// sentioxyz/sentio PR #18293 (driver/controller/startup/startup.go::
// buildCommitCtx). The plugin forwards this value to the host unparsed;
// the host extracts the `watching` flag from it.
const LogCommentSettingKey = "log_comment"

// Plugin is the QueryPlugin that meters driver INSERTs. Construct via
// New; never set the sink directly — it is not optional.
type Plugin struct {
	sink billing.IndexingUsageReporter
}

// New returns a Plugin that reports 1 unit of generic coordinates per
// driver INSERT to sink. A nil sink disables the plugin (acts as a
// no-op QueryPlugin) — this matches the rest of pkg/plugins which fail
// open rather than rejecting queries when wiring is incomplete.
//
// housegate keeps no batching state and does not read the registry: the
// host's reporter resolves processor / SKU / backfill and folds units
// into its own accumulator. The sink is responsible for dispatching the
// report without blocking this call — see billing.IndexingUsageReporter
// ("the caller does not wait on results").
func New(sink billing.IndexingUsageReporter) *Plugin {
	return &Plugin{sink: sink}
}

// OnQuery reports 1 unit of generic coordinates for every driver INSERT
// that has a resolved destination table. Processor attribution, SKU
// mapping, and backfill interpretation are the host's job — see the
// package doc and billing.IndexingUsageEntry.
func (p *Plugin) OnQuery(ctx context.Context, qctx *plugin.QueryContext) error {
	if p == nil || p.sink == nil || qctx == nil || qctx.Session == nil {
		return nil
	}
	// Cheap, allocation-free disqualifier first: the rewriter must have
	// classified this as an INSERT (without it we can't tell SELECT from
	// INSERT, and AccessedTables is unset). Running this before the
	// SessionState snapshot below short-circuits the dominant non-INSERT
	// traffic without paying the snapshot's per-session Settings-map copy.
	if qctx.StatementType != sqlmeta.StatementTypeInsert {
		return nil
	}
	// Anything that isn't a driver-signed session is metered elsewhere
	// (query usage path). Reading IsDriver requires a state snapshot.
	snap := qctx.Session.State().Snapshot()
	if !snap.IsDriver {
		return nil
	}
	_, logger := log.FromContext(ctx)
	if len(qctx.AccessedTables) == 0 {
		logger.Debugw("indexing_usage: INSERT with no accessed tables, skip")
		return nil
	}
	// INSERT targets exactly one table at the AST level. The rewriter
	// preserves it as AccessedTables[0].
	target := qctx.AccessedTables[0]
	if target.LogicalDatabase == "" {
		logger.Debugw("indexing_usage: INSERT with empty LogicalDatabase, skip",
			"original_db", target.OriginalDatabase,
			"original_table", target.OriginalTable)
		return nil
	}
	// Raw log_comment setting value (the driver sets it per commit in
	// sentio PR #18293). Passed through unparsed — the host extracts the
	// `watching` flag. Empty when the setting is absent.
	logComment := ""
	if qctx.Query != nil {
		for _, s := range qctx.Query.Settings {
			if s.Key == LogCommentSettingKey {
				logComment = s.Value
				break
			}
		}
	}
	// 1 unit per INSERT regardless of row count — see package TODO.
	// Generic coordinates only; the host maps LogicalDatabase→processor
	// and Table→SKU (dropping non-processor / non-billable), interprets
	// LogComment, and folds into its accumulator.
	entry := billing.IndexingUsageEntry{
		LogicalDatabase: target.LogicalDatabase,
		Table:           target.OriginalTable,
		LogComment:      logComment,
		Units:           1,
	}
	p.sink.Report(ctx, []billing.IndexingUsageEntry{entry})
	logger.Infow("indexing_usage: reported INSERT",
		"database", entry.LogicalDatabase,
		"table", entry.Table)
	return nil
}

// RunOnPeerTrust marks the plugin as opt-out: peer-trusted sessions
// already had their inserts metered by the originating proxy; running
// again here would double-count.
func (p *Plugin) RunOnPeerTrust() bool { return false }

// RunOnForward marks the plugin as opt-out: a forward-pivot session is
// only billed by the entry proxy. The host proxy that the forward
// landed on has IsForwarding=false in its own session (the field is
// per-session, not per-proxy), so this only filters the entry proxy's
// own opt-out on forwarded sessions — same shape as rewrite/commitgate.
func (p *Plugin) RunOnForward() bool { return false }

// Compile-time interface assertions.
var (
	_ plugin.QueryPlugin    = (*Plugin)(nil)
	_ plugin.PeerTrustAware = (*Plugin)(nil)
	_ plugin.ForwardAware   = (*Plugin)(nil)
)
