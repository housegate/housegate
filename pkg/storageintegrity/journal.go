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
	StatementID string `json:"statement_id"`
	Source      string `json:"source"`
	// FrontierOrdinal is immutable for the lifetime of the statement. It
	// reconstructs same-source admission order independently of later updates.
	FrontierOrdinal uint64            `json:"frontier_ordinal"`
	Env             StatementEnvelope `json:"env"`
	Admission       AdmissionRecord   `json:"admission"`

	Stage       Lifecycle `json:"stage"`
	AbortReason string    `json:"abort_reason,omitempty"`

	Prepared    PreparedLocalResult `json:"prepared"`
	HasPrepared bool                `json:"has_prepared"`
	Submit      SubmitOutcome       `json:"submit"`
	HasSubmit   bool                `json:"has_submit"`

	SubmitUnknown bool `json:"submit_unknown"`
	ClaimUnknown  bool `json:"claim_unknown"`

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
		records = append(records, rec)
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

func journalRecordFromIntakeRecord(rec *intakeRecord) IntakeJournalRecord {
	return IntakeJournalRecord{
		StatementID:     rec.statementID,
		Source:          rec.source,
		FrontierOrdinal: rec.frontierOrdinal,
		Env:             rec.env,
		Admission:       cloneAdmissionRecord(rec.adm),
		Stage:           rec.stage,
		AbortReason:     rec.abortReason,
		Prepared:        clonePreparedLocalResult(rec.prepared),
		HasPrepared:     rec.hasPrepared,
		Submit:          rec.submit,
		HasSubmit:       rec.hasSubmit,
		SubmitUnknown:   rec.submitUnknown,
		ClaimUnknown:    rec.claimUnknown,
		TerminalResult:  cloneIntakeResult(rec.terminalRes),
		IsTerminal:      rec.isTerminal,
		UpdatedAtUnixMS: time.Now().UnixMilli(),
	}
}

func intakeRecordFromJournalRecord(rec IntakeJournalRecord) *intakeRecord {
	return &intakeRecord{
		statementID:     rec.StatementID,
		source:          rec.Source,
		frontierOrdinal: rec.FrontierOrdinal,
		env:             rec.Env,
		adm:             cloneAdmissionRecord(rec.Admission),
		prepared:        clonePreparedLocalResult(rec.Prepared),
		hasPrepared:     rec.HasPrepared,
		submit:          rec.Submit,
		hasSubmit:       rec.HasSubmit,
		submitUnknown:   rec.SubmitUnknown,
		claimUnknown:    rec.ClaimUnknown,
		stage:           rec.Stage,
		abortReason:     rec.AbortReason,
		terminalRes:     cloneIntakeResult(rec.TerminalResult),
		isTerminal:      rec.IsTerminal,
	}
}

func cloneAdmissionRecord(in AdmissionRecord) AdmissionRecord {
	out := in
	out.Payload = append([]byte(nil), in.Payload...)
	return out
}

func clonePreparedLocalResult(in PreparedLocalResult) PreparedLocalResult {
	out := in
	out.CandidateParts = append([]CandidatePart(nil), in.CandidateParts...)
	out.PartitionNewPartSums = append([]PartitionLtHashSum(nil), in.PartitionNewPartSums...)
	return out
}

func cloneIntakeResult(in IntakeResult) IntakeResult {
	out := in
	out.Prepared = clonePreparedLocalResult(in.Prepared)
	return out
}
