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
	records, beforeRecovery := o.recoverySnapshot()
	if beforeRecovery != nil {
		if err := beforeRecovery(ctx, records); err != nil {
			return fmt.Errorf("intake: restore runtime state before recovery: %w", err)
		}
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

func (o *Orchestrator) recoverySnapshot() ([]IntakeJournalRecord, func(context.Context, []IntakeJournalRecord) error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	records := cloneIntakeJournalRecords(o.recoveredJournal)
	sort.Slice(records, func(i, k int) bool {
		if records[i].Source != records[k].Source {
			return records[i].Source < records[k].Source
		}
		if records[i].FrontierOrdinal != records[k].FrontierOrdinal {
			return records[i].FrontierOrdinal < records[k].FrontierOrdinal
		}
		return records[i].StatementID < records[k].StatementID
	})
	return records, o.beforeRecovery
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
