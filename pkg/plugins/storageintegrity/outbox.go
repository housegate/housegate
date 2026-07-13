package storageintegrity

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	core "housegate/housegate/pkg/storageintegrity"
)

type insertOutbox interface {
	Put(context.Context, core.InsertRecord) error
	List(context.Context) ([]core.InsertRecord, error)
	Ack(context.Context, string) error
}

type FileInsertOutbox struct {
	dir string
}

func NewFileInsertOutbox(dir string) (*FileInsertOutbox, error) {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return nil, fmt.Errorf("insert outbox directory is required")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create insert outbox dir: %w", err)
	}
	return &FileInsertOutbox{dir: dir}, nil
}

func (o *FileInsertOutbox) Dir() string {
	if o == nil {
		return ""
	}
	return o.dir
}

func (o *FileInsertOutbox) Put(ctx context.Context, rec core.InsertRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if o == nil {
		return nil
	}
	if rec.StatementID == "" {
		return fmt.Errorf("insert outbox record requires statement_id")
	}
	raw, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal insert outbox record %s: %w", rec.StatementID, err)
	}
	path := o.path(rec.StatementID)
	tmp, err := os.CreateTemp(o.dir, ".pending-*.json")
	if err != nil {
		return fmt.Errorf("create insert outbox temp file: %w", err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(append(raw, '\n')); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("write insert outbox temp file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("sync insert outbox temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("close insert outbox temp file: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("commit insert outbox record: %w", err)
	}
	return nil
}

func (o *FileInsertOutbox) List(ctx context.Context) ([]core.InsertRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if o == nil {
		return nil, nil
	}
	entries, err := os.ReadDir(o.dir)
	if err != nil {
		return nil, fmt.Errorf("read insert outbox dir: %w", err)
	}
	var out []core.InsertRecord
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") || strings.HasPrefix(entry.Name(), ".pending-") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(o.dir, entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("read insert outbox record %s: %w", entry.Name(), err)
		}
		var rec core.InsertRecord
		if err := json.Unmarshal(raw, &rec); err != nil {
			return nil, fmt.Errorf("decode insert outbox record %s: %w", entry.Name(), err)
		}
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ReceivedAt.Equal(out[j].ReceivedAt) {
			return out[i].StatementID < out[j].StatementID
		}
		return out[i].ReceivedAt.Before(out[j].ReceivedAt)
	})
	return out, nil
}

func (o *FileInsertOutbox) Ack(ctx context.Context, statementID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if o == nil || strings.TrimSpace(statementID) == "" {
		return nil
	}
	if err := os.Remove(o.path(statementID)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("ack insert outbox record %s: %w", statementID, err)
	}
	return nil
}

func (o *FileInsertOutbox) path(statementID string) string {
	sum := sha256.Sum256([]byte(statementID))
	return filepath.Join(o.dir, hex.EncodeToString(sum[:])+".json")
}
