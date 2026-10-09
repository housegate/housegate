package sistatement

// Observer is the narrow metrics surface of the signed inline
// INSERT ... VALUES lane (spec 2026-09-23 D11): statements synthesized,
// refusals from the evaluation step (exception, timeout, transport error,
// empty result, type mismatch, row/byte limit) and refusals from the lexical
// closure gate, which are raised before anything reaches ClickHouse.
// *proxy.MetricsObserver satisfies it; a nil Observer disables metrics.
type Observer interface {
	InlineValuesSynthesized()
	InlineValuesEvaluationFailed()
	InlineValuesClosureRefused()
}

// StatusObserver counts status lookups that failed, after which the INSERT
// passed through unsigned (spec 2026-09-24 §10.3). *proxy.MetricsObserver
// satisfies it; an Observer that does not is simply not counted.
type StatusObserver interface {
	TableStatusLookupFailed()
}

// SeqObserver counts recycled and burned client_seq values (spec 2026-10-09
// §10). *proxy.MetricsObserver satisfies it; an Observer that does not is not
// counted.
type SeqObserver interface {
	SeqRecycled()
	SeqBurned(reason string)
}

// DiscoveryObserver counts agent discovery failures by step (spec 2026-10-09
// §10): "info" (sentio_getStorageIntegrityInfo failed), "network_id" (it
// answered no network id) and "precheck" (sentio_isDatabaseWriter failed, so
// the advisory pre-check was skipped). *proxy.MetricsObserver satisfies it;
// an Observer that does not is not counted.
type DiscoveryObserver interface {
	SIDiscoveryFailed(step string)
}

// SwitchObserver counts the outcomes of the agent's upstream switch to the
// indexer hosting an SI INSERT's database (spec 2026-10-09 §6.4, §10):
// "switched", "refused_state", "refused_database", "refused_revision" (also a
// differing server timezone) and "dial_failed" (dial, handshake or deadline).
// *proxy.MetricsObserver satisfies it; an Observer that does not is not
// counted.
type SwitchObserver interface {
	SIUpstreamSwitch(result string)
}

// LaneObserver is the metrics surface of client_seq lane selection (spec
// 2026-10-09 §6.5, §10). LaneRotated counts every change of client lane by
// reason: "gap_budget" (the lane was abandoned after GAP_BUDGET_EXCEEDED),
// "lost_state" (no lane file existed, so a new lane was minted) and
// "new_process" (every existing lane was held by another process or
// abandoned); a rotation and the acquire that completes it count once.
// SIInflight moves the gauge of SI statements between client_seq reservation
// and outcome. *proxy.MetricsObserver satisfies it; an Observer that does not
// is not counted.
type LaneObserver interface {
	LaneRotated(reason string)
	SIInflight(delta int)
}
