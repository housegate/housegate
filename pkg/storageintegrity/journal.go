package storageintegrity

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const currentIntakeJournalVersion uint32 = 1

// IntakeJournal persists the HouseGate-local intake record. It is intentionally
// statement-scoped: the orchestrator owns concurrency/frontier bookkeeping in
// memory, while the journal owns the durable facts needed to resume one
// statement without re-running an unsafe write.
type IntakeJournal interface {
	LoadIntakeRecord(ctx context.Context, statementID string) (IntakeJournalRecord, bool, error)
	ListIntakeRecords(ctx context.Context) ([]IntakeJournalRecord, error)
	SaveIntakeRecord(ctx context.Context, record IntakeJournalRecord) error
}

// IntakeJournalRecord is the durable shape of an intakeRecord. It mirrors only
// protocol and recovery facts; active/done waiters and source-frontier queues
// are process-local and rebuilt by the orchestrator.
type IntakeJournalRecord struct {
	// JournalVersion distinguishes observation-aware finalized ownership from
	// the pre-rollout shape whose empty observed list is ambiguous. Version zero
	// is legacy and must never be silently interpreted as delayed visibility.
	JournalVersion uint32 `json:"journal_version,omitempty"`
	StatementID    string `json:"statement_id"`
	Source         string `json:"source"`
	// FrontierOrdinal is immutable for the lifetime of the statement. It
	// reconstructs same-source admission order independently of later updates.
	FrontierOrdinal uint64            `json:"frontier_ordinal"`
	Env             StatementEnvelope `json:"env"`
	Admission       AdmissionRecord   `json:"admission"`

	Stage       Lifecycle `json:"stage"`
	AbortReason string    `json:"abort_reason,omitempty"`

	Prepared    PreparedLocalResult `json:"prepared"`
	HasPrepared bool                `json:"has_prepared"`
	// ObservedCandidateParts are exact prepared names durably acknowledged by a
	// successful local system.parts inventory. They let restart distinguish a
	// delayed candidate (unobserved + absent, retain debt) from historical
	// cleanup (observed + absent, retire ownership).
	ObservedCandidateParts []CandidatePart `json:"observed_candidate_parts,omitempty"`
	Submit                 SubmitOutcome   `json:"submit"`
	HasSubmit              bool            `json:"has_submit"`

	SubmitUnknown bool `json:"submit_unknown"`
	ClaimUnknown  bool `json:"claim_unknown"`
	// PrepareKnownUnwritten is durable evidence that the previous source
	// rejection happened before any unsafe write. It authorizes a lookup-free
	// retry and is cleared durably before that retry begins.
	PrepareKnownUnwritten bool `json:"prepare_known_unwritten,omitempty"`

	TerminalResult IntakeResult `json:"terminal_result"`
	IsTerminal     bool         `json:"is_terminal"`

	UpdatedAtUnixMS int64 `json:"updated_at_unix_ms"`
}

// FileIntakeJournal stores one JSON record per statement id using atomic
// write+rename. The file name is a SHA-256 of the statement id so structured ids
// containing ':' never leak into filesystem path syntax.
type FileIntakeJournal struct {
	dir string
}

func NewFileIntakeJournal(dir string) (*FileIntakeJournal, error) {
	if dir == "" {
		return nil, errors.New("storageintegrity: intake journal dir is required")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("storageintegrity: create intake journal dir: %w", err)
	}
	return &FileIntakeJournal{dir: dir}, nil
}

func (j *FileIntakeJournal) LoadIntakeRecord(ctx context.Context, statementID string) (IntakeJournalRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return IntakeJournalRecord{}, false, err
	}
	path := j.recordPath(statementID)
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return IntakeJournalRecord{}, false, nil
	}
	if err != nil {
		return IntakeJournalRecord{}, false, fmt.Errorf("storageintegrity: read intake journal %s: %w", statementID, err)
	}
	var rec IntakeJournalRecord
	if err := json.Unmarshal(b, &rec); err != nil {
		return IntakeJournalRecord{}, false, fmt.Errorf("storageintegrity: decode intake journal %s: %w", statementID, err)
	}
	if rec.StatementID != statementID {
		return IntakeJournalRecord{}, false, fmt.Errorf("storageintegrity: journal statement id mismatch: file for %s contained %s", statementID, rec.StatementID)
	}
	if err := validateIntakeJournalVersion(rec); err != nil {
		return IntakeJournalRecord{}, false, err
	}
	return rec, true, nil
}

func (j *FileIntakeJournal) ListIntakeRecords(ctx context.Context) ([]IntakeJournalRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(j.dir)
	if err != nil {
		return nil, fmt.Errorf("storageintegrity: list intake journal dir: %w", err)
	}
	records := make([]IntakeJournalRecord, 0, len(entries))
	migrationIndexes := make([]int, 0)
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" || strings.HasPrefix(entry.Name(), ".tmp-intake-") {
			continue
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		path := filepath.Join(j.dir, entry.Name())
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("storageintegrity: read intake journal %s: %w", entry.Name(), err)
		}
		var rec IntakeJournalRecord
		if err := json.Unmarshal(b, &rec); err != nil {
			return nil, fmt.Errorf("storageintegrity: decode intake journal %s: %w", entry.Name(), err)
		}
		if rec.StatementID == "" {
			return nil, fmt.Errorf("storageintegrity: journal file %s missing statement id", entry.Name())
		}
		if err := validateIntakeJournalVersion(rec); err != nil {
			return nil, err
		}
		// Migrate terminal payload bytes while processing this one file, before
		// retaining the record in the aggregate result. This bounds first-upgrade
		// memory by one legacy payload instead of the full historical corpus.
		changed := false
		if rec.IsTerminal && len(rec.Admission.Payload) != 0 {
			rec.Admission.Payload = nil
			changed = true
		}
		if rec.IsTerminal && rec.JournalVersion == 0 &&
			legacyJournalMigrationComplete(rec.Admission.TouchedPartitionIDs, rec.Prepared.CandidateParts, rec.ObservedCandidateParts) {
			rec.JournalVersion = currentIntakeJournalVersion
			changed = true
		}
		if changed {
			migrationIndexes = append(migrationIndexes, len(records))
		}
		records = append(records, rec)
	}
	// Publish all per-file atomic rewrites with one directory sync. The retained
	// migration set contains metadata only; every large payload was already
	// dropped while its source file was the sole decoded record in memory.
	for _, idx := range migrationIndexes {
		if err := j.saveIntakeRecord(ctx, records[idx], false); err != nil {
			if syncErr := syncDir(j.dir); syncErr != nil {
				err = errors.Join(err, syncErr)
			}
			return nil, fmt.Errorf("storageintegrity: migrate terminal intake journal %s: %w", records[idx].StatementID, err)
		}
	}
	if len(migrationIndexes) != 0 {
		if err := syncDir(j.dir); err != nil {
			return nil, fmt.Errorf("storageintegrity: publish terminal intake journal migrations: %w", err)
		}
	}
	sort.Slice(records, func(i, k int) bool {
		if records[i].Source != records[k].Source {
			return records[i].Source < records[k].Source
		}
		if records[i].FrontierOrdinal != records[k].FrontierOrdinal {
			return records[i].FrontierOrdinal < records[k].FrontierOrdinal
		}
		if records[i].UpdatedAtUnixMS != records[k].UpdatedAtUnixMS {
			return records[i].UpdatedAtUnixMS < records[k].UpdatedAtUnixMS
		}
		return records[i].StatementID < records[k].StatementID
	})
	return records, nil
}

func (j *FileIntakeJournal) SaveIntakeRecord(ctx context.Context, rec IntakeJournalRecord) error {
	return j.saveIntakeRecord(ctx, rec, true)
}

func (j *FileIntakeJournal) saveIntakeRecord(ctx context.Context, rec IntakeJournalRecord, syncDirectory bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if rec.StatementID == "" {
		return errors.New("storageintegrity: journal record statement id is required")
	}
	rec.UpdatedAtUnixMS = time.Now().UnixMilli()
	b, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return fmt.Errorf("storageintegrity: encode intake journal %s: %w", rec.StatementID, err)
	}
	b = append(b, '\n')

	tmp, err := os.CreateTemp(j.dir, ".tmp-intake-*.json")
	if err != nil {
		return fmt.Errorf("storageintegrity: create temp intake journal %s: %w", rec.StatementID, err)
	}
	tmpPath := tmp.Name()
	closed := false
	defer func() {
		if !closed {
			_ = tmp.Close()
		}
		_ = os.Remove(tmpPath)
	}()
	if _, err := tmp.Write(b); err != nil {
		return fmt.Errorf("storageintegrity: write temp intake journal %s: %w", rec.StatementID, err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("storageintegrity: sync temp intake journal %s: %w", rec.StatementID, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("storageintegrity: close temp intake journal %s: %w", rec.StatementID, err)
	}
	closed = true
	if err := os.Rename(tmpPath, j.recordPath(rec.StatementID)); err != nil {
		return fmt.Errorf("storageintegrity: publish intake journal %s: %w", rec.StatementID, err)
	}
	if !syncDirectory {
		return nil
	}
	return syncDir(j.dir)
}

func (j *FileIntakeJournal) recordPath(statementID string) string {
	sum := sha256.Sum256([]byte(statementID))
	return filepath.Join(j.dir, hex.EncodeToString(sum[:])+".json")
}

func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("storageintegrity: open dir for sync %s: %w", dir, err)
	}
	defer f.Close()
	if err := f.Sync(); err != nil {
		return fmt.Errorf("storageintegrity: sync dir %s: %w", dir, err)
	}
	return nil
}

func validateIntakeJournalVersion(rec IntakeJournalRecord) error {
	if rec.JournalVersion > currentIntakeJournalVersion {
		return fmt.Errorf(
			"storageintegrity: intake journal version %d for statement %s is newer than supported version %d",
			rec.JournalVersion, rec.StatementID, currentIntakeJournalVersion,
		)
	}
	return nil
}

func intakeJournalVersionForAdmission(adm AdmissionRecord) uint32 {
	if adm.TouchedPartitionIDs == nil {
		return 0
	}
	return currentIntakeJournalVersion
}

func terminalIntakeJournalVersion(rec *intakeRecord) uint32 {
	if rec == nil {
		return 0
	}
	if rec.adm.TouchedPartitionIDs != nil || !rec.hasPrepared || len(rec.prepared.CandidateParts) == 0 {
		return currentIntakeJournalVersion
	}
	return 0
}

func journalRecordFromIntakeRecord(rec *intakeRecord) IntakeJournalRecord {
	return IntakeJournalRecord{
		JournalVersion:         rec.journalVersion,
		StatementID:            rec.statementID,
		Source:                 rec.source,
		FrontierOrdinal:        rec.frontierOrdinal,
		Env:                    rec.env,
		Admission:              cloneAdmissionRecord(rec.adm),
		Stage:                  rec.stage,
		AbortReason:            rec.abortReason,
		Prepared:               clonePreparedLocalResult(rec.prepared),
		HasPrepared:            rec.hasPrepared,
		ObservedCandidateParts: cloneCandidateParts(rec.observedCandidateParts),
		Submit:                 rec.submit,
		HasSubmit:              rec.hasSubmit,
		SubmitUnknown:          rec.submitUnknown,
		ClaimUnknown:           rec.claimUnknown,
		PrepareKnownUnwritten:  rec.prepareKnownUnwritten,
		TerminalResult:         cloneIntakeResult(rec.terminalRes),
		IsTerminal:             rec.isTerminal,
		UpdatedAtUnixMS:        time.Now().UnixMilli(),
	}
}

func intakeRecordFromJournalRecord(rec IntakeJournalRecord) *intakeRecord {
	return &intakeRecord{
		journalVersion:         rec.JournalVersion,
		statementID:            rec.StatementID,
		source:                 rec.Source,
		frontierOrdinal:        rec.FrontierOrdinal,
		env:                    rec.Env,
		adm:                    cloneAdmissionRecord(rec.Admission),
		prepared:               clonePreparedLocalResult(rec.Prepared),
		hasPrepared:            rec.HasPrepared,
		observedCandidateParts: cloneCandidateParts(rec.ObservedCandidateParts),
		submit:                 rec.Submit,
		hasSubmit:              rec.HasSubmit,
		submitUnknown:          rec.SubmitUnknown,
		claimUnknown:           rec.ClaimUnknown,
		prepareKnownUnwritten:  rec.PrepareKnownUnwritten,
		stage:                  rec.Stage,
		abortReason:            rec.AbortReason,
		terminalRes:            cloneIntakeResult(rec.TerminalResult),
		isTerminal:             rec.IsTerminal,
	}
}

func cloneAdmissionRecord(in AdmissionRecord) AdmissionRecord {
	out := in
	out.Payload = append([]byte(nil), in.Payload...)
	out.TouchedPartitionIDs = cloneStringsPreserveNil(in.TouchedPartitionIDs)
	return out
}

func cloneStringsPreserveNil(in []string) []string {
	if in == nil {
		return nil
	}
	return append([]string{}, in...)
}

func clonePreparedLocalResult(in PreparedLocalResult) PreparedLocalResult {
	out := in
	out.CandidateParts = append([]CandidatePart(nil), in.CandidateParts...)
	out.PartitionNewPartSums = append([]PartitionLtHashSum(nil), in.PartitionNewPartSums...)
	return out
}

func cloneCandidateParts(in []CandidatePart) []CandidatePart {
	return append([]CandidatePart(nil), in...)
}

func cloneIntakeResult(in IntakeResult) IntakeResult {
	out := in
	out.Prepared = clonePreparedLocalResult(in.Prepared)
	return out
}

func cloneIntakeJournalRecords(in []IntakeJournalRecord) []IntakeJournalRecord {
	out := make([]IntakeJournalRecord, len(in))
	for idx, record := range in {
		out[idx] = record
		out[idx].Admission = cloneAdmissionRecord(record.Admission)
		out[idx].Prepared = clonePreparedLocalResult(record.Prepared)
		out[idx].ObservedCandidateParts = cloneCandidateParts(record.ObservedCandidateParts)
		out[idx].TerminalResult = cloneIntakeResult(record.TerminalResult)
	}
	return out
}
