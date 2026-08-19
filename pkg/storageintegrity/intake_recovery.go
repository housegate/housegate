package storageintegrity

import (
	"context"
	"fmt"
	"sort"
	"time"
)

const defaultRecoveryRetryInterval = time.Second

// RecoverPending drains every durable non-terminal intake in source-frontier
// order. It is intended for preServe: callers must not expose new admissions
// until it returns.
func (o *Orchestrator) RecoverPending(ctx context.Context) error {
	if err := o.ensureJournalRecovered(ctx); err != nil {
		return err
	}
	if err := o.restoreRecoveryState(ctx); err != nil {
		return err
	}
	for {
		adm, ok := o.nextRecoveryAdmission()
		if !ok {
			return nil
		}
		if _, err := o.Orchestrate(ctx, adm); err == nil {
			if _, stillPending := o.recoveryAdmission(adm.StatementID); !stillPending {
				continue
			}
		} else if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := waitRecoveryRetry(ctx, o.cfg.RecoveryRetryInterval); err != nil {
			return err
		}
	}
}

func (o *Orchestrator) restoreRecoveryState(ctx context.Context) error {
	for {
		o.mu.Lock()
		if o.recoveryStateRestored {
			o.mu.Unlock()
			return nil
		}
		if o.recoveryStateRestoring {
			done := o.recoveryStateRestoreDone
			o.mu.Unlock()
			select {
			case <-done:
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		o.recoveryStateRestoring = true
		o.recoveryStateRestoreDone = make(chan struct{})
		done := o.recoveryStateRestoreDone
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
		var restoreErr error
		if beforeRecovery != nil {
			restoreErr = beforeRecovery(ctx, records)
		}

		o.mu.Lock()
		if restoreErr == nil {
			o.recoveryStateRestored = true
			// The non-terminal intake records own the only payload bytes still
			// required for recovery. Release the full projection, especially the
			// monotonically growing terminal history, after runtime reconstruction.
			o.recoveredJournal = nil
		}
		o.recoveryStateRestoring = false
		close(done)
		o.mu.Unlock()
		if restoreErr != nil {
			return fmt.Errorf("intake: restore runtime state before recovery: %w", restoreErr)
		}
		return nil
	}
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
