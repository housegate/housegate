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
	"time"

	"housegate/housegate/pkg/replay"
)

type PayloadSpoolState string

const (
	PayloadSpoolStatePending  PayloadSpoolState = "PENDING"
	PayloadSpoolStateUploaded PayloadSpoolState = "UPLOADED"
)

// PayloadSpoolRecord is the local durable spool metadata for one payload hash.
type PayloadSpoolRecord struct {
	PayloadHash        string            `json:"payload_hash"`
	PayloadLength      uint64            `json:"payload_length"`
	State              PayloadSpoolState `json:"state"`
	RemotePayloadRef   string            `json:"remote_payload_ref,omitempty"`
	RemoteState        PayloadState      `json:"remote_state,omitempty"`
	LeaseExpiresUnixMS uint64            `json:"lease_expires_unix_ms,omitempty"`
	UpdatedAtUnixMS    int64             `json:"updated_at_unix_ms"`
}

// FilePayloadSpool stores exact payload bytes by content hash before the remote
// PayloadStore put is attempted. It is deliberately content-addressed so a retry
// of the same statement can reuse the same local bytes without duplicating them.
type FilePayloadSpool struct {
	dir string
}

func NewFilePayloadSpool(dir string) (*FilePayloadSpool, error) {
	if dir == "" {
		return nil, errors.New("storageintegrity: payload spool dir is required")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("storageintegrity: create payload spool dir: %w", err)
	}
	return &FilePayloadSpool{dir: dir}, nil
}

func (s *FilePayloadSpool) StorePayload(ctx context.Context, payload []byte, payloadHash string, payloadLength uint64) (PayloadSpoolRecord, error) {
	if err := validatePayloadCommitment(payload, payloadHash, payloadLength); err != nil {
		return PayloadSpoolRecord{}, err
	}
	if err := ctx.Err(); err != nil {
		return PayloadSpoolRecord{}, err
	}
	if _, rec, err := s.LoadPayload(ctx, payloadHash); err == nil {
		return rec, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return PayloadSpoolRecord{}, err
	}
	if err := s.writePayloadFile(payloadHash, payload); err != nil {
		return PayloadSpoolRecord{}, err
	}
	rec := PayloadSpoolRecord{
		PayloadHash:     payloadHash,
		PayloadLength:   payloadLength,
		State:           PayloadSpoolStatePending,
		UpdatedAtUnixMS: time.Now().UnixMilli(),
	}
	if err := s.writeRecord(rec); err != nil {
		return PayloadSpoolRecord{}, err
	}
	return rec, nil
}

func (s *FilePayloadSpool) MarkUploaded(ctx context.Context, payloadHash string, put PayloadPutResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	payload, rec, err := s.LoadPayload(ctx, payloadHash)
	if err != nil {
		return err
	}
	if uint64(len(payload)) != rec.PayloadLength {
		return fmt.Errorf("storageintegrity: spooled payload length mismatch for %s", payloadHash)
	}
	if put.PayloadRef == "" {
		return fmt.Errorf("storageintegrity: remote payload ref is required to mark %s uploaded", payloadHash)
	}
	rec.State = PayloadSpoolStateUploaded
	rec.RemotePayloadRef = put.PayloadRef
	rec.RemoteState = put.State
	rec.LeaseExpiresUnixMS = put.LeaseExpiresUnixMS
	rec.UpdatedAtUnixMS = time.Now().UnixMilli()
	return s.writeRecord(rec)
}

func (s *FilePayloadSpool) LoadPayload(ctx context.Context, payloadHash string) ([]byte, PayloadSpoolRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, PayloadSpoolRecord{}, err
	}
	recBytes, err := os.ReadFile(s.recordPath(payloadHash))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, PayloadSpoolRecord{}, err
		}
		return nil, PayloadSpoolRecord{}, fmt.Errorf("storageintegrity: read payload spool metadata %s: %w", payloadHash, err)
	}
	var rec PayloadSpoolRecord
	if err := json.Unmarshal(recBytes, &rec); err != nil {
		return nil, PayloadSpoolRecord{}, fmt.Errorf("storageintegrity: decode payload spool metadata %s: %w", payloadHash, err)
	}
	if rec.PayloadHash != payloadHash {
		return nil, PayloadSpoolRecord{}, fmt.Errorf("storageintegrity: payload spool hash mismatch: requested %s, found %s", payloadHash, rec.PayloadHash)
	}
	payload, err := os.ReadFile(s.payloadPath(payloadHash))
	if err != nil {
		return nil, PayloadSpoolRecord{}, fmt.Errorf("storageintegrity: read spooled payload %s: %w", payloadHash, err)
	}
	if got := replay.DigestBytes(payload); got != payloadHash {
		return nil, PayloadSpoolRecord{}, fmt.Errorf("storageintegrity: spooled payload hash mismatch: got %s, want %s", got, payloadHash)
	}
	if uint64(len(payload)) != rec.PayloadLength {
		return nil, PayloadSpoolRecord{}, fmt.Errorf("storageintegrity: spooled payload length mismatch: got %d, want %d", len(payload), rec.PayloadLength)
	}
	return payload, rec, nil
}

func (s *FilePayloadSpool) writePayloadFile(payloadHash string, payload []byte) error {
	path := s.payloadPath(payloadHash)
	if existing, err := os.ReadFile(path); err == nil {
		if replay.DigestBytes(existing) != payloadHash {
			return fmt.Errorf("storageintegrity: existing spool payload %s has wrong digest", payloadHash)
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("storageintegrity: inspect spool payload %s: %w", payloadHash, err)
	}
	if err := atomicWriteFile(s.dir, path, payload, 0o600); err != nil {
		return fmt.Errorf("storageintegrity: write spool payload %s: %w", payloadHash, err)
	}
	return nil
}

func (s *FilePayloadSpool) writeRecord(rec PayloadSpoolRecord) error {
	b, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return fmt.Errorf("storageintegrity: encode payload spool metadata %s: %w", rec.PayloadHash, err)
	}
	b = append(b, '\n')
	if err := atomicWriteFile(s.dir, s.recordPath(rec.PayloadHash), b, 0o600); err != nil {
		return fmt.Errorf("storageintegrity: write payload spool metadata %s: %w", rec.PayloadHash, err)
	}
	return nil
}

func (s *FilePayloadSpool) payloadPath(payloadHash string) string {
	return filepath.Join(s.dir, safePayloadHash(payloadHash)+".payload")
}

func (s *FilePayloadSpool) recordPath(payloadHash string) string {
	return filepath.Join(s.dir, safePayloadHash(payloadHash)+".json")
}

func safePayloadHash(payloadHash string) string {
	sum := sha256.Sum256([]byte(payloadHash))
	return hex.EncodeToString(sum[:])
}

type SpoolingPayloadWriter struct {
	spool         *FilePayloadSpool
	remote        PayloadWriter
	now           func() time.Time
	refreshBefore time.Duration
}

func NewSpoolingPayloadWriter(spool *FilePayloadSpool, remote PayloadWriter) *SpoolingPayloadWriter {
	return NewSpoolingPayloadWriterWithLeasePolicy(spool, remote, 30*time.Second)
}

func NewSpoolingPayloadWriterWithLeasePolicy(spool *FilePayloadSpool, remote PayloadWriter, refreshBefore time.Duration) *SpoolingPayloadWriter {
	return &SpoolingPayloadWriter{
		spool:         spool,
		remote:        remote,
		now:           time.Now,
		refreshBefore: refreshBefore,
	}
}

func (w *SpoolingPayloadWriter) PutPayload(ctx context.Context, payload []byte, payloadHash string, payloadLength uint64) (PayloadPutResult, error) {
	if w == nil || w.spool == nil {
		return PayloadPutResult{}, errors.New("storageintegrity: payload spool is required")
	}
	if w.remote == nil {
		return PayloadPutResult{}, errors.New("storageintegrity: remote payload writer is required")
	}
	rec, err := w.spool.StorePayload(ctx, payload, payloadHash, payloadLength)
	if err != nil {
		return PayloadPutResult{}, err
	}
	if rec.State == PayloadSpoolStateUploaded && rec.RemotePayloadRef != "" && w.leaseReusable(rec) {
		return PayloadPutResult{
			PayloadRef:         rec.RemotePayloadRef,
			State:              rec.RemoteState,
			LeaseExpiresUnixMS: rec.LeaseExpiresUnixMS,
		}, nil
	}
	put, err := w.remote.PutPayload(ctx, payload, payloadHash, payloadLength)
	if err != nil {
		return PayloadPutResult{}, err
	}
	if rec.RemotePayloadRef != "" && put.PayloadRef != rec.RemotePayloadRef {
		return PayloadPutResult{}, fmt.Errorf(
			"storageintegrity: payload_ref changed for %s: got %q, want %q",
			payloadHash,
			put.PayloadRef,
			rec.RemotePayloadRef,
		)
	}
	if err := w.spool.MarkUploaded(ctx, payloadHash, put); err != nil {
		return PayloadPutResult{}, err
	}
	return put, nil
}

func (w *SpoolingPayloadWriter) leaseReusable(rec PayloadSpoolRecord) bool {
	if rec.LeaseExpiresUnixMS == 0 {
		return false
	}
	now := time.Now
	if w.now != nil {
		now = w.now
	}
	refreshBefore := w.refreshBefore
	if refreshBefore < 0 {
		refreshBefore = 0
	}
	return time.UnixMilli(int64(rec.LeaseExpiresUnixMS)).After(now().Add(refreshBefore))
}

func validatePayloadCommitment(payload []byte, payloadHash string, payloadLength uint64) error {
	if len(payload) == 0 || payloadLength == 0 {
		return errors.New("storageintegrity: payload bytes and length are required")
	}
	if uint64(len(payload)) != payloadLength {
		return fmt.Errorf("storageintegrity: payload length mismatch: got %d bytes, declared %d", len(payload), payloadLength)
	}
	if payloadHash == "" {
		return errors.New("storageintegrity: payload hash is required")
	}
	if got := replay.DigestBytes(payload); got != payloadHash {
		return fmt.Errorf("storageintegrity: payload hash mismatch: got %s, declared %s", got, payloadHash)
	}
	return nil
}

func atomicWriteFile(dir, path string, data []byte, perm os.FileMode) error {
	tmp, err := os.CreateTemp(dir, ".tmp-write-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	closed := false
	defer func() {
		if !closed {
			_ = tmp.Close()
		}
		_ = os.Remove(tmpPath)
	}()
	if err := tmp.Chmod(perm); err != nil {
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	closed = true
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	return syncDir(dir)
}
