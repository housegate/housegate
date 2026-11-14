package storageintegrity

import (
	"context"
	"fmt"
	"sort"
	"time"
)

const defaultRecoveryRetryInterval = time.Second

type recoveryStateRestoreAttempt struct {
	done chan struct{}
	err  error
}

// RecoverPending drains every durable non-terminal intake in source-frontier
// order. It is intended for preServe: callers must not expose new admissions
// until it returns. It is RestorePending followed by DrainPending.
func (o *Orchestrator) RecoverPending(ctx context.Context) error {
	return o.DrainPending(ctx)
}

// RestorePending is the half of recovery that touches neither the source nor
// the arbiter: it loads the durable journal, restores every non-terminal record
// and fences each source frontier behind its recovered holders, and runs the
// before-recovery hook. A host whose source cannot prepare until its listeners
// are open calls it before serving and DrainPending afterwards; a new
// admission for a fenced source waits at the frontier until the recovered
// statements ahead of it are terminal, so source order is unchanged. It is
// idempotent.
func (o *Orchestrator) RestorePending(ctx context.Context) error {
	if err := o.ensureJournalRecovered(ctx); err != nil {
		return err
	}
	return o.restoreRecoveryState(ctx)
}

// DrainPending restores pending records when RestorePending has not run yet,
// then drives every recovered non-terminal intake to a terminal stage in
// source-frontier order, retrying each one every RecoveryRetryInterval until
// it is terminal or ctx ends. Every attempt that leaves a statement pending is
// reported to RecoveryRetryObserver.
func (o *Orchestrator) DrainPending(ctx context.Context) error {
	if err := o.RestorePending(ctx); err != nil {
		return err
	}
	for {
		adm, ok := o.nextRecoveryAdmission()
		if !ok {
			return nil
		}
		res, err := o.Orchestrate(ctx, adm)
		if err == nil {
			if _, stillPending := o.recoveryAdmission(adm.StatementID); !stillPending {
				continue
			}
		} else if ctx.Err() != nil {
			return ctx.Err()
		}
		if observe := o.cfg.RecoveryRetryObserver; observe != nil {
			observe(adm.StatementID, res, err)
		}
		if err := waitRecoveryRetry(ctx, o.cfg.RecoveryRetryInterval); err != nil {
			return err
		}
	}
}

func (o *Orchestrator) restoreRecoveryState(ctx context.Context) error {
	o.mu.Lock()
	if o.recoveryStateRestored {
		o.mu.Unlock()
		return nil
	}
	if attempt := o.recoveryStateRestore; attempt != nil {
		done := attempt.done
		o.mu.Unlock()
		select {
		case <-done:
			if attempt.err != nil {
				return fmt.Errorf("intake: restore runtime state before recovery: %w", attempt.err)
			}
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	attempt := &recoveryStateRestoreAttempt{done: make(chan struct{})}
	o.recoveryStateRestore = attempt
	records := cloneIntakeJournalRecords(o.recoveredJournal)
	beforeRecovery := o.beforeRecovery
	o.mu.Unlock()

	sort.Slice(records, func(i, k int) bool {
		if records[i].Source != records[k].Source {
			return records[i].Source < records[k].Source
		}
		if records[i].FrontierOrdinal != records[k].FrontierOrdinal {
			return records[i].FrontierOrdinal < records[k].FrontierOrdinal
		}
		return records[i].StatementID < records[k].StatementID
	})
	if beforeRecovery != nil {
		attempt.err = beforeRecovery(ctx, records)
	}

	o.mu.Lock()
	if attempt.err == nil {
		o.recoveryStateRestored = true
		// The non-terminal intake records own the only payload bytes still
		// required for recovery. Release the full projection, especially the
		// monotonically growing terminal history, after runtime reconstruction.
		o.recoveredJournal = nil
	}
	if o.recoveryStateRestore == attempt {
		o.recoveryStateRestore = nil
	}
	close(attempt.done)
	o.mu.Unlock()
	if attempt.err != nil {
		return fmt.Errorf("intake: restore runtime state before recovery: %w", attempt.err)
	}
	return nil
}

func (o *Orchestrator) nextRecoveryAdmission() (AdmissionRecord, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	var selected *intakeRecord
	for _, rec := range o.records {
		if rec == nil || rec.isTerminal {
			continue
		}
		if selected == nil ||
			rec.source < selected.source ||
			(rec.source == selected.source && rec.frontierOrdinal < selected.frontierOrdinal) ||
			(rec.source == selected.source && rec.frontierOrdinal == selected.frontierOrdinal && rec.statementID < selected.statementID) {
			selected = rec
		}
	}
	if selected == nil {
		return AdmissionRecord{}, false
	}
	return cloneAdmissionRecord(selected.adm), true
}

func (o *Orchestrator) recoveryAdmission(statementID string) (AdmissionRecord, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	rec := o.records[statementID]
	if rec == nil || rec.isTerminal {
		return AdmissionRecord{}, false
	}
	return cloneAdmissionRecord(rec.adm), true
}

func waitRecoveryRetry(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		interval = defaultRecoveryRetryInterval
	}
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
