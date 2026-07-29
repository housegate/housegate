package storageintegrity

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/replay"
)

type recordingPayloadWriter struct {
	calls int
	err   error
	res   PayloadPutResult
}

func (w *recordingPayloadWriter) PutPayload(_ context.Context, payload []byte, payloadHash string, payloadLength uint64) (PayloadPutResult, error) {
	w.calls++
	if payloadHash != replay.DigestBytes(payload) {
		return PayloadPutResult{}, errors.New("test writer saw mismatched payload hash")
	}
	if payloadLength != uint64(len(payload)) {
		return PayloadPutResult{}, errors.New("test writer saw mismatched payload length")
	}
	return w.res, w.err
}

func TestSpoolingPayloadWriterRetainsPayloadWhenRemotePutFails(t *testing.T) {
	ctx := context.Background()
	payload := []byte("native-block-bytes")
	payloadHash := replay.DigestBytes(payload)

	spool, err := NewFilePayloadSpool(t.TempDir())
	if err != nil {
		t.Fatalf("NewFilePayloadSpool: %v", err)
	}
	remote := &recordingPayloadWriter{err: errors.New("payload store unavailable")}
	writer := NewSpoolingPayloadWriter(spool, remote)

	_, err = writer.PutPayload(ctx, payload, payloadHash, uint64(len(payload)))
	if err == nil {
		t.Fatal("PutPayload succeeded despite remote failure")
	}
	if remote.calls != 1 {
		t.Fatalf("remote calls = %d, want 1", remote.calls)
	}

	stored, rec, err := spool.LoadPayload(ctx, payloadHash)
	if err != nil {
		t.Fatalf("LoadPayload after remote failure: %v", err)
	}
	if string(stored) != string(payload) {
		t.Fatalf("stored payload = %q, want %q", stored, payload)
	}
	if rec.State != PayloadSpoolStatePending {
		t.Fatalf("spool state = %q, want %q", rec.State, PayloadSpoolStatePending)
	}
	if rec.RemotePayloadRef != "" {
		t.Fatalf("remote ref after failed put = %q, want empty", rec.RemotePayloadRef)
	}
}

func TestSpoolingPayloadWriterRecordsRemoteRefOnSuccess(t *testing.T) {
	ctx := context.Background()
	payload := []byte("native-block-bytes")
	payloadHash := replay.DigestBytes(payload)

	spool, err := NewFilePayloadSpool(t.TempDir())
	if err != nil {
		t.Fatalf("NewFilePayloadSpool: %v", err)
	}
	remote := &recordingPayloadWriter{res: PayloadPutResult{
		PayloadRef:         "payload://store/ref-1",
		State:              PayloadStateAvailable,
		LeaseExpiresUnixMS: uint64(time.Now().Add(time.Hour).UnixMilli()),
	}}
	writer := NewSpoolingPayloadWriter(spool, remote)

	put, err := writer.PutPayload(ctx, payload, payloadHash, uint64(len(payload)))
	if err != nil {
		t.Fatalf("PutPayload: %v", err)
	}
	if put.PayloadRef != remote.res.PayloadRef {
		t.Fatalf("payload ref = %q, want %q", put.PayloadRef, remote.res.PayloadRef)
	}

	stored, rec, err := spool.LoadPayload(ctx, payloadHash)
	if err != nil {
		t.Fatalf("LoadPayload after success: %v", err)
	}
	if string(stored) != string(payload) {
		t.Fatalf("stored payload = %q, want %q", stored, payload)
	}
	if rec.State != PayloadSpoolStateUploaded {
		t.Fatalf("spool state = %q, want %q", rec.State, PayloadSpoolStateUploaded)
	}
	if rec.RemotePayloadRef != remote.res.PayloadRef {
		t.Fatalf("remote ref = %q, want %q", rec.RemotePayloadRef, remote.res.PayloadRef)
	}
	if rec.LeaseExpiresUnixMS != remote.res.LeaseExpiresUnixMS {
		t.Fatalf("lease = %d, want %d", rec.LeaseExpiresUnixMS, remote.res.LeaseExpiresUnixMS)
	}
}

func TestSpoolingPayloadWriterReusesUploadedPayloadRef(t *testing.T) {
	ctx := context.Background()
	payload := []byte("native-block-bytes")
	payloadHash := replay.DigestBytes(payload)

	spool, err := NewFilePayloadSpool(t.TempDir())
	if err != nil {
		t.Fatalf("NewFilePayloadSpool: %v", err)
	}
	remote := &recordingPayloadWriter{res: PayloadPutResult{
		PayloadRef:         "payload://store/ref-1",
		State:              PayloadStateAvailable,
		LeaseExpiresUnixMS: uint64(time.Now().Add(time.Hour).UnixMilli()),
	}}
	writer := NewSpoolingPayloadWriter(spool, remote)

	if _, err := writer.PutPayload(ctx, payload, payloadHash, uint64(len(payload))); err != nil {
		t.Fatalf("first PutPayload: %v", err)
	}
	remote.err = errors.New("remote should not be called for uploaded spool record")
	put, err := writer.PutPayload(ctx, payload, payloadHash, uint64(len(payload)))
	if err != nil {
		t.Fatalf("second PutPayload should reuse uploaded spool record: %v", err)
	}
	if remote.calls != 1 {
		t.Fatalf("remote calls = %d, want 1", remote.calls)
	}
	if put.PayloadRef != remote.res.PayloadRef {
		t.Fatalf("reused payload ref = %q, want %q", put.PayloadRef, remote.res.PayloadRef)
	}
	_, rec, err := spool.LoadPayload(ctx, payloadHash)
	if err != nil {
		t.Fatalf("LoadPayload after reuse: %v", err)
	}
	if rec.State != PayloadSpoolStateUploaded {
		t.Fatalf("spool state after reuse = %q, want %q", rec.State, PayloadSpoolStateUploaded)
	}
}

func TestSpoolingPayloadWriterRefreshesLeaseNearExpiry(t *testing.T) {
	ctx := context.Background()
	payload := []byte("native-block-bytes")
	payloadHash := replay.DigestBytes(payload)

	spool, err := NewFilePayloadSpool(t.TempDir())
	if err != nil {
		t.Fatalf("NewFilePayloadSpool: %v", err)
	}
	remote := &recordingPayloadWriter{res: PayloadPutResult{
		PayloadRef:         "payload://store/ref-1",
		State:              PayloadStateAvailable,
		LeaseExpiresUnixMS: uint64(time.Now().Add(5 * time.Second).UnixMilli()),
	}}
	writer := NewSpoolingPayloadWriter(spool, remote)

	if _, err := writer.PutPayload(ctx, payload, payloadHash, uint64(len(payload))); err != nil {
		t.Fatalf("first PutPayload: %v", err)
	}
	if _, err := writer.PutPayload(ctx, payload, payloadHash, uint64(len(payload))); err != nil {
		t.Fatalf("refresh PutPayload: %v", err)
	}
	if remote.calls != 2 {
		t.Fatalf("remote calls = %d, want 2 for near-expiry lease", remote.calls)
	}
}
