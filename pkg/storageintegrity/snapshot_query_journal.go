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
	"time"

	"github.com/housegate/housegate/pkg/replay"
)

// SnapshotQueryJournalVersion is deliberately independent from the v2 payload
// intake journal. Snapshot-query records must never be decoded as payload work.
const SnapshotQueryJournalVersion uint32 = 1

// SnapshotQueryJournal persists only snapshot-query intake state.
type SnapshotQueryJournal interface {
	Load(ctx context.Context, statementID string) (SnapshotQueryJournalRecord, bool, error)
	List(ctx context.Context) ([]SnapshotQueryJournalRecord, error)
	Save(ctx context.Context, record SnapshotQueryJournalRecord) error
}

// SnapshotQueryJournalStage is a durable boundary, never an inferred status.
type SnapshotQueryJournalStage string

const (
	SnapshotQueryStageSigned           SnapshotQueryJournalStage = "Signed"
	SnapshotQueryStageSubmitIntent     SnapshotQueryJournalStage = "SubmitIntent"
	SnapshotQueryStageSubmitAuthorized SnapshotQueryJournalStage = "SubmitAuthorized"
	// SnapshotQueryStageSubmitAuthorizationUnknown means the authorization
	// durability result is indeterminate. Unlike SubmitUnknown, it never grants
	// recovery authority to issue Submit: recovery may only lookup/reconcile.
	SnapshotQueryStageSubmitAuthorizationUnknown SnapshotQueryJournalStage = "SubmitAuthorizationUnknown"
	SnapshotQueryStageSubmitUnknown              SnapshotQueryJournalStage = "SubmitUnknown"
	SnapshotQueryStageSequenced                  SnapshotQueryJournalStage = "Sequenced"
	// PreparedOutput is a pre-write boundary: the one-shot source output has
	// been copied, fsynced, reopened, and its owner closed.
	SnapshotQueryStagePreparedOutput SnapshotQueryJournalStage = "PreparedOutput"
	// SnapshotQueryStageRejected records a deterministic remote refusal. It is
	// terminal for submission: recovery must never mistake a refusal for an
	// accepted sequence or retry it.
	SnapshotQueryStageRejected      SnapshotQueryJournalStage = "Rejected"
	SnapshotQueryStageCancelPending SnapshotQueryJournalStage = "CancelPending"
	SnapshotQueryStageReleased      SnapshotQueryJournalStage = "Released"
)

// SnapshotQueryLaunchAuthorization records the exact durable right to issue a
// sequencer Submit. Intent alone deliberately contains no such right.
type SnapshotQueryLaunchAuthorization struct {
	InputRoot         string `json:"input_root"`
	OriginalJWSHash   string `json:"original_jws_hash"`
	ReservationID     string `json:"reservation_id"`
	FencingGeneration uint64 `json:"fencing_generation"`
}

// SnapshotQueryJournalRecord is a new, separately versioned on-disk shape.
// Later C4/C5 stages can append source/claim/output facts without changing the
// meaning of the pre-submit authorization boundary below.
type SnapshotQueryJournalRecord struct {
	Version         uint32                       `json:"version"`
	StatementID     string                       `json:"statement_id"`
	Envelope        replay.SnapshotQueryEnvelope `json:"envelope"`
	Source          string                       `json:"source,omitempty"`
	FrontierOrdinal uint64                       `json:"frontier_ordinal"`
	Stage           SnapshotQueryJournalStage    `json:"stage"`

	Submit                    replay.SnapshotQuerySubmitResult  `json:"submit"`
	HasSubmit                 bool                              `json:"has_submit"`
	SubmitUnknown             bool                              `json:"submit_unknown"`
	PreSubmitCancelIntent     bool                              `json:"pre_submit_cancel_intent"`
	ReleaseReconciliationDebt bool                              `json:"release_reconciliation_debt"`
	LaunchAuthorization       *SnapshotQueryLaunchAuthorization `json:"launch_authorization,omitempty"`
	PreparedOutput            *SnapshotQueryPrepared            `json:"prepared_output,omitempty"`
	UpdatedAtUnixMS           int64                             `json:"updated_at_unix_ms"`
}

// FileSnapshotQueryJournal uses the same temp-file, file-fsync, rename and
// directory-fsync publication discipline as FileIntakeJournal, but an isolated
// directory and record version so it cannot silently share the payload format.
type FileSnapshotQueryJournal struct{ dir string }

func NewFileSnapshotQueryJournal(dir string) (*FileSnapshotQueryJournal, error) {
	if dir == "" {
		return nil, errors.New("storageintegrity: snapshot query journal dir is required")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("storageintegrity: create snapshot query journal dir: %w", err)
	}
	return &FileSnapshotQueryJournal{dir: dir}, nil
}

func (j *FileSnapshotQueryJournal) Load(ctx context.Context, statementID string) (SnapshotQueryJournalRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return SnapshotQueryJournalRecord{}, false, err
	}
	b, err := os.ReadFile(j.recordPath(statementID))
	if errors.Is(err, os.ErrNotExist) {
		return SnapshotQueryJournalRecord{}, false, nil
	}
	if err != nil {
		return SnapshotQueryJournalRecord{}, false, fmt.Errorf("storageintegrity: read snapshot query journal %s: %w", statementID, err)
	}
	var rec SnapshotQueryJournalRecord
	if err := json.Unmarshal(b, &rec); err != nil {
		return SnapshotQueryJournalRecord{}, false, fmt.Errorf("storageintegrity: decode snapshot query journal %s: %w", statementID, err)
	}
	if rec.StatementID != statementID {
		return SnapshotQueryJournalRecord{}, false, fmt.Errorf("storageintegrity: snapshot query journal statement id mismatch: file for %s contained %s", statementID, rec.StatementID)
	}
	if err := validateSnapshotQueryJournalRecord(rec); err != nil {
		return SnapshotQueryJournalRecord{}, false, err
	}
	return rec, true, nil
}

func (j *FileSnapshotQueryJournal) List(ctx context.Context) ([]SnapshotQueryJournalRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(j.dir)
	if err != nil {
		return nil, fmt.Errorf("storageintegrity: list snapshot query journal: %w", err)
	}
	records := make([]SnapshotQueryJournalRecord, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !isSnapshotQueryRecordFile(entry.Name()) {
			continue
		}
		b, err := os.ReadFile(filepath.Join(j.dir, entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("storageintegrity: read snapshot query journal entry %s: %w", entry.Name(), err)
		}
		var rec SnapshotQueryJournalRecord
		if err := json.Unmarshal(b, &rec); err != nil {
			return nil, fmt.Errorf("storageintegrity: decode snapshot query journal entry %s: %w", entry.Name(), err)
		}
		if err := validateSnapshotQueryJournalRecord(rec); err != nil {
			return nil, err
		}
		records = append(records, rec)
	}
	sort.Slice(records, func(i, k int) bool { return records[i].StatementID < records[k].StatementID })
	return records, nil
}

func (j *FileSnapshotQueryJournal) Save(ctx context.Context, rec SnapshotQueryJournalRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if rec.Version == 0 {
		rec.Version = SnapshotQueryJournalVersion
	}
	if err := validateSnapshotQueryJournalRecord(rec); err != nil {
		return err
	}
	rec.UpdatedAtUnixMS = time.Now().UnixMilli()
	b, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return fmt.Errorf("storageintegrity: encode snapshot query journal %s: %w", rec.StatementID, err)
	}
	b = append(b, '\n')
	tmp, err := os.CreateTemp(j.dir, ".tmp-snapshot-query-*.json")
	if err != nil {
		return fmt.Errorf("storageintegrity: create temp snapshot query journal %s: %w", rec.StatementID, err)
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
		return fmt.Errorf("storageintegrity: write temp snapshot query journal %s: %w", rec.StatementID, err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("storageintegrity: sync temp snapshot query journal %s: %w", rec.StatementID, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("storageintegrity: close temp snapshot query journal %s: %w", rec.StatementID, err)
	}
	closed = true
	if err := os.Rename(tmpPath, j.recordPath(rec.StatementID)); err != nil {
		return fmt.Errorf("storageintegrity: publish snapshot query journal %s: %w", rec.StatementID, err)
	}
	return syncDir(j.dir)
}

func (j *FileSnapshotQueryJournal) recordPath(statementID string) string {
	sum := sha256.Sum256([]byte(statementID))
	return filepath.Join(j.dir, hex.EncodeToString(sum[:])+".json")
}

func isSnapshotQueryRecordFile(name string) bool {
	if filepath.Ext(name) != ".json" {
		return false
	}
	stem := name[:len(name)-len(".json")]
	if len(stem) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(stem)
	return err == nil
}

func validateSnapshotQueryJournalRecord(rec SnapshotQueryJournalRecord) error {
	if rec.Version != SnapshotQueryJournalVersion {
		return fmt.Errorf("storageintegrity: unsupported snapshot query journal version %d", rec.Version)
	}
	if rec.StatementID == "" || rec.StatementID != rec.Envelope.Input.Binding.StatementID {
		return errors.New("storageintegrity: snapshot query journal statement identity is required")
	}
	if rec.Stage == "" {
		return errors.New("storageintegrity: snapshot query journal stage is required")
	}
	if rec.Stage == SnapshotQueryStagePreparedOutput && rec.PreparedOutput == nil {
		return errors.New("storageintegrity: prepared output journal stage requires projection")
	}
	if rec.PreparedOutput != nil {
		if rec.Stage != SnapshotQueryStagePreparedOutput {
			return errors.New("storageintegrity: prepared output has wrong journal stage")
		}
		if err := validateSnapshotQueryPrepared(rec.Envelope, rec.Submit, *rec.PreparedOutput); err != nil {
			return err
		}
	}
	return nil
}
