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
