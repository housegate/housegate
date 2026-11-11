package storageintegrity

import (
	"context"
	"time"
)

// The intake stages an IntakeStageObserver is told about. Each is one call the
// orchestrator makes on its ports, timed around the call alone:
//
//   - IntakeStageFrontierWait: waiting for the source frontier, i.e. for an
//     earlier statement on the same source to reach a success boundary.
//   - IntakeStagePayloadLease: EnsurePayloadLease before the submit.
//   - IntakeStagePrepare: PrepareLocalStatement (it runs concurrently with
//     the first SubmitStatement).
//   - IntakeStageSubmit: SubmitStatement to the Arbiter.
//   - IntakeStageRegisterClaim: RegisterPreparedClaim (the source's
//     RegisterResultClaim to the Arbiter).
//   - IntakeStageJournalSave: one SaveIntakeRecord; an intake makes several.
const (
	IntakeStageFrontierWait  = "frontier_wait"
	IntakeStagePayloadLease  = "payload_lease"
	IntakeStagePrepare       = "prepare"
	IntakeStageSubmit        = "submit"
	IntakeStageRegisterClaim = "register_claim"
	IntakeStageJournalSave   = "journal_save"
)

// IntakeStages lists every stage the orchestrator reports, for an observer
// that pre-creates its series.
var IntakeStages = []string{
	IntakeStageFrontierWait,
	IntakeStagePayloadLease,
	IntakeStagePrepare,
	IntakeStageSubmit,
	IntakeStageRegisterClaim,
	IntakeStageJournalSave,
}

// IntakeStageObserver receives the duration of each intake stage and the
// error the call returned (nil on success). It is called on the intake's own
// goroutines, so it must be cheap and safe for concurrent use. Process-local
// timing only; it never changes the intake's outcome.
type IntakeStageObserver interface {
	ObserveIntakeStage(stage string, elapsed time.Duration, err error)
}

// observeStage reports one stage that started at start.
func (o *Orchestrator) observeStage(stage string, start time.Time, err error) {
	if o.cfg.StageObserver != nil {
		o.cfg.StageObserver.ObserveIntakeStage(stage, time.Since(start), err)
	}
}

// timedJournal reports every SaveIntakeRecord as IntakeStageJournalSave.
// Loads and lists happen at recovery and on a first sighting, not on the
// write path, and are passed through untimed.
type timedJournal struct {
	IntakeJournal
	observer IntakeStageObserver
}

func (j timedJournal) SaveIntakeRecord(ctx context.Context, record IntakeJournalRecord) error {
	start := time.Now()
	err := j.IntakeJournal.SaveIntakeRecord(ctx, record)
	j.observer.ObserveIntakeStage(IntakeStageJournalSave, time.Since(start), err)
	return err
}
