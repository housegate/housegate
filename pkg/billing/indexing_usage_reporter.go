package billing

import "context"

// IndexingUsageEntry is one unit of metered driver-side write traffic
// that housegate hands off to the embedding host (sentio-node) for
// usage reporting.
//
// It carries only *generic ClickHouse-proxy coordinates* — which
// logical database and table received the INSERT, plus the raw value of
// the driver's `log_comment` session setting. It contains NO Sentio
// billing concepts: housegate does not resolve which processor owns the
// database, which on-chain SKU the table maps to, or whether the write
// is backfill. All of that interpretation belongs to the host's
// IndexingUsageReporter implementation, which has the same NetworkState
// registry housegate does and looks up processor / table-type / SKU
// itself. This keeps housegate free of billing-domain attribution.
//
// Fields:
//   - LogicalDatabase: the destination logical database of the INSERT
//     (AccessedTables[0].LogicalDatabase). The host resolves this to a
//     processor (and drops non-processor / unknown databases).
//   - Table: the destination table name (AccessedTables[0].OriginalTable).
//     The host resolves this to a table type → on-chain SKU (and drops
//     non-billable tables).
//   - LogComment: the raw, unparsed value of the driver's `log_comment`
//     session setting (a JSON object string, possibly quote-wrapped, or
//     empty when absent). The host parses out whatever it needs (e.g.
//     the `watching` flag that distinguishes backfill from live).
//   - Units: row count metered for this aggregate (currently 1 per
//     INSERT; see the plugin's row-count TODO).
type IndexingUsageEntry struct {
	LogicalDatabase string
	Table           string
	LogComment      string
	Units           uint64
}

// IndexingUsageReporter is the consumer-facing surface of an
// indexing-usage sink. housegate's in-wire plugin detects driver
// INSERTs and calls Report with the generic coordinates above; the host
// implementation resolves processor / SKU / backfill and folds the
// result into its usage accumulator.
//
// housegate does not ship a production implementation; the standalone
// sentio-node host injects one that maps LogicalDatabase → processor,
// Table → SKU, interprets LogComment, and forwards to its local
// UsageService.AsyncSave. Tests supply record-and-replay stubs.
//
// Concurrency: Report must be safe to call concurrently with itself;
// implementations are expected to either dispatch fire-and-forget or
// serialize internally. The caller does not wait on results.
//
// Lifetime: the embedder owns teardown (no Close on this interface).
type IndexingUsageReporter interface {
	Report(ctx context.Context, entries []IndexingUsageEntry)
}
