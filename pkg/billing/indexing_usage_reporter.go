package billing

import "context"

// IndexingUsageEntry is one unit of metered driver-side write traffic
// that housegate hands off to the embedding host (sentio-node) for
// on-chain reporting via the existing UsageTracker.ReportIndexingUsage
// path.
//
// Semantics mirror sentio-node's IndexingUsageKey:
//   - SKU is the driver-style on-chain SKU name. When IsBackfilling is
//     true, callers append the "_backfill" suffix sentio-node's
//     usage.MapDriverSKU strips on receipt, so this struct stays a thin
//     value type without a separate flag column. Plugins that prefer a
//     boolean field over the suffix convention can flip IsBackfilling
//     and leave SKU bare — sentio-node's adapter normalises either way.
//   - Units is the row count metered on the wire for this aggregate.
//
// The struct is intentionally producer-shaped (matches what driver's
// AsyncSaveRequest previously carried), so the in-process adapter on
// sentio-node can forward it verbatim to its UsageService.AsyncSave.
type IndexingUsageEntry struct {
	ProcessorID   string
	SKU           string
	IsBackfilling bool
	Units         uint64
}

// IndexingUsageReporter is the consumer-facing surface of an
// indexing-usage sink. Housegate accumulates per-INSERT row counts
// keyed by (processor, SKU, isBackfilling) and periodically flushes
// the batch via Report.
//
// Housegate does not ship a production implementation; the standalone
// sentio-node host injects an adapter that forwards to the local
// UsageService.AsyncSave (the same path the driver used to call
// directly). Tests supply record-and-replay stubs.
//
// Concurrency: Report must be safe to call concurrently with itself;
// implementations are expected to either dispatch fire-and-forget or
// serialize internally. The caller does not wait on results.
//
// Lifetime: the embedder owns teardown (no Close on this interface).
type IndexingUsageReporter interface {
	Report(ctx context.Context, entries []IndexingUsageEntry)
}
