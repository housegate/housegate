// Package indexingusage meters driver-side INSERT volume on the
// housegate wire and reports it to sentio-node via
// billing.IndexingUsageReporter.
//
// Before this plugin existed, the driver itself counted rows in its
// meter.Flush() loop and called sentio-node's UsageService.AsyncSave
// gRPC directly. The decentralised topology splits the write path so
// the driver now ships INSERTs through a housegate sidecar; the
// housegate / clickhouse hop is the natural choke point for metering.
//
// Per-INSERT attribution:
//   - processor_id: resolved from the destination logical DB via
//     registry.Databases.Get (which carries ProcessorId for processor-
//     owned DBs). log_comment's processor_id is used only as a cross-
//     check; the database-level binding is the on-chain truth.
//   - sku: rewriter-resolved AccessedTable[0] looked up against the
//     database's TableInfo.Type, then mapped via MapTableTypeToSKU.
//   - isBackfilling: !log_comment.watching (driver sets log_comment per
//     commit in sentioxyz/sentio PR #18293).
//   - units: hard-coded to 1 per INSERT (see TODO below).
//
// TODO(row-count): swap the 1-unit-per-INSERT placeholder for actual
// row count metered off the wire. ClickHouse native INSERT puts rows
// in subsequent ClientCodeData packets (not in the Query packet's SQL
// body), so OnQuery alone cannot see them. The full row-count path
// needs:
//
//   - chproto.Codec to surface num_rows on Packet.Rows (decode first
//     compressed frame's BlockInfo + num_columns + num_rows uvarint
//     prefix; uncompressed path gets it free from proto.Block.Rows).
//   - A new ClientDataBlockPlugin hook fanned out by relay's
//     clientToUpstream when pkt.Rows > 0.
//   - This plugin: stash the resolved (processor, sku, backfill)
//     attribution on SessionState.IndexingUsageContext in OnQuery, sum
//     rows in OnClientDataBlock, and Report once with Units=rows in
//     OnQueryComplete.
//
// All four pieces were prototyped and verified on devnet (txs
// 0x5bb12d32 / 0xf1c7b52f / 0xe3d90803 / 0xa9121f07 / 0x88608c68);
// reverted to the 1-unit placeholder while the row-counting work
// finds a longer-term home (concerns: cost of LZ4-decompressing every
// client Data block on the hot relay path even when this plugin is
// the only consumer; need a generic "row count observer" abstraction
// so the codec work is reusable by other future plugins).
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
	"housegate/housegate/pkg/registry"
	"housegate/housegate/pkg/sqlmeta"
)

// Plugin is the QueryPlugin that meters billable driver INSERTs.
// Construct via New; never set fields directly — Databases and the
// reporter sink are not optional.
type Plugin struct {
	dbs  registry.Databases
	sink billing.IndexingUsageReporter
}

// New returns a Plugin that resolves processor/SKU via dbs and reports
// 1 unit per INSERT directly to sink. A nil dbs or nil sink disables
// the plugin (acts as a no-op QueryPlugin) — this matches the rest of
// pkg/plugins which fail open rather than rejecting queries when wiring
// is incomplete.
//
// Housegate keeps no batching state: sentio-node's usage.Server already
// folds per-(processor,sku,backfill) units into its own Redis-backed
// accumulator, so a second accumulator here would be redundant. The
// sink (sentio-node's in-process adapter) is responsible for dispatching
// the report without blocking this call — see billing.IndexingUsageReporter
// ("the caller does not wait on results").
func New(dbs registry.Databases, sink billing.IndexingUsageReporter) *Plugin {
	return &Plugin{dbs: dbs, sink: sink}
}

// OnQuery classifies the query and, if it is a billable driver INSERT,
// reports 1 unit to the sink under the resolved
// (processor, sku, isBackfilling) attribution. See package TODO for the
// row-count upgrade path.
func (p *Plugin) OnQuery(ctx context.Context, qctx *plugin.QueryContext) error {
	if p == nil || p.dbs == nil || p.sink == nil || qctx == nil || qctx.Session == nil {
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
	logical := target.LogicalDatabase
	if logical == "" {
		logger.Debugw("indexing_usage: INSERT with empty LogicalDatabase, skip",
			"original_db", target.OriginalDatabase,
			"original_table", target.OriginalTable)
		return nil
	}
	db, ok := p.dbs.Get(logical)
	if !ok {
		logger.Debugw("indexing_usage: DB not in NetworkState, skip", "database", logical)
		return nil
	}
	if db.DbType != uint8(1) /* PROCESSOR */ {
		logger.Debugw("indexing_usage: non-processor DB, skip", "database", logical, "db_type", db.DbType)
		return nil
	}
	if db.ProcessorId == "" {
		logger.Debugw("indexing_usage: processor DB has no ProcessorId, skip", "database", logical)
		return nil
	}
	tableType, ok := tableTypeFor(db, target.OriginalTable)
	if !ok {
		logger.Debugw("indexing_usage: table not in NetworkState, skip",
			"database", logical, "table", target.OriginalTable)
		return nil
	}
	sku, ok := MapTableTypeToSKU(tableType)
	if !ok {
		logger.Debugw("indexing_usage: tableType not in SKU map, skip",
			"database", logical, "table", target.OriginalTable, "table_type", tableType)
		return nil
	}
	// log_comment carries the watching flag the driver sets in
	// buildCommitCtx (sentio PR #18293). Missing setting → default to
	// watching=true (i.e. NOT backfill) — conservative for the user:
	// undercount the backfill rate if the driver ever forgets to set
	// the marker.
	isBackfill := false
	if qctx.Query != nil {
		for _, s := range qctx.Query.Settings {
			if s.Key != LogCommentSettingKey {
				continue
			}
			if lc, ok := ParseLogComment(s.Value); ok {
				// Absent watching key (nil) leaves isBackfill=false
				// (watching=true), matching the missing-setting default;
				// only an explicit watching:false marks backfill.
				if lc.Watching != nil {
					isBackfill = !*lc.Watching
				}
				// Cross-check against db.ProcessorId. A mismatch is a
				// driver bug or a malicious session; log loudly but do
				// not reject the query — the database binding is
				// authoritative and we've already picked the right key.
				if lc.ProcessorID != "" && lc.ProcessorID != db.ProcessorId {
					logger.Warnw("indexing_usage: log_comment.processor_id mismatch",
						"log_comment_processor", lc.ProcessorID,
						"db_processor", db.ProcessorId,
						"database", logical,
					)
				}
			}
			break
		}
	}
	// 1 unit per INSERT regardless of row count — see package TODO.
	// Reported directly; sentio-node's usage accumulator does the
	// per-key folding and on-chain settling.
	entry := billing.IndexingUsageEntry{
		ProcessorID:   db.ProcessorId,
		SKU:           sku,
		IsBackfilling: isBackfill,
		Units:         1,
	}
	p.sink.Report(ctx, []billing.IndexingUsageEntry{entry})
	logger.Infow("indexing_usage: reported INSERT",
		"processor", entry.ProcessorID,
		"sku", entry.SKU,
		"is_backfilling", entry.IsBackfilling)
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

// tableTypeFor finds the Type of `table` inside db.Tables. Returns
// ok=false when the table isn't in the snapshot (newly-created table
// race or pre-TableType-aware deployment).
func tableTypeFor(db registry.Database, table string) (string, bool) {
	for _, t := range db.Tables {
		if t.Id == table {
			return t.Type, true
		}
	}
	return "", false
}

// Compile-time interface assertions.
var (
	_ plugin.QueryPlugin    = (*Plugin)(nil)
	_ plugin.PeerTrustAware = (*Plugin)(nil)
	_ plugin.ForwardAware   = (*Plugin)(nil)
)
