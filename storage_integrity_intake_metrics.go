package housegate

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

// The ingress-side intake stages, timed around one call each, next to the
// orchestrator's (sicore.IntakeStages).
const (
	// intakeStagePayloadPut is the payload-store put (spool and remote put)
	// before the orchestrator runs.
	intakeStagePayloadPut = "payload_put"
	// intakeStageOrchestrate is one Orchestrate call: every orchestrator stage
	// plus its own bookkeeping.
	intakeStageOrchestrate = "orchestrate"
)

// intakeBuckets span a local journal fsync of a millisecond, a SubmitStatement
// held for the Arbiter's 50 ms grouping window, a cross-region payload put of a
// few hundred milliseconds, and a call that waited out a retry or a timeout.
var intakeBuckets = []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.25, 0.5, 1, 2.5, 5, 10, 30}

var storageIntegrityIntakeStageSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "storage_integrity_intake_stage_seconds",
	Help:    "Seconds one storage-integrity intake stage took on this host, by stage (payload_put, orchestrate, frontier_wait, payload_lease, prepare, submit, register_claim, journal_save) and outcome (ok, error). prepare runs concurrently with the first submit; an intake makes several journal_save calls.",
	Buckets: intakeBuckets,
}, []string{"stage", "outcome"})

var storageIntegrityIntakeSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "storage_integrity_intake_seconds",
	Help:    "Seconds one storage-integrity admission took on this host end to end, from the merge-health check through the orchestrator's answer, by outcome (ack2: the write was acknowledged; error: any refusal or failure).",
	Buckets: intakeBuckets,
}, []string{"outcome"})

func init() {
	prometheus.MustRegister(storageIntegrityIntakeStageSeconds, storageIntegrityIntakeSeconds)
	// Every series exists from the start, so a dashboard never reads an
	// absent series as no traffic.
	for _, stage := range append([]string{intakeStagePayloadPut, intakeStageOrchestrate}, sicore.IntakeStages...) {
		for _, outcome := range []string{"ok", "error"} {
			storageIntegrityIntakeStageSeconds.WithLabelValues(stage, outcome)
		}
	}
	for _, outcome := range []string{"ack2", "error"} {
		storageIntegrityIntakeSeconds.WithLabelValues(outcome)
	}
}

// intakeStageMetrics is the sicore.IntakeStageObserver the runtime hands the
// orchestrator.
type intakeStageMetrics struct{}

func (intakeStageMetrics) ObserveIntakeStage(stage string, elapsed time.Duration, err error) {
	observeIntakeStage(stage, elapsed, err)
}

func observeIntakeStage(stage string, elapsed time.Duration, err error) {
	outcome := "ok"
	if err != nil {
		outcome = "error"
	}
	storageIntegrityIntakeStageSeconds.WithLabelValues(stage, outcome).Observe(elapsed.Seconds())
}

func observeIntake(start time.Time, err error) {
	outcome := "ack2"
	if err != nil {
		outcome = "error"
	}
	storageIntegrityIntakeSeconds.WithLabelValues(outcome).Observe(time.Since(start).Seconds())
}
