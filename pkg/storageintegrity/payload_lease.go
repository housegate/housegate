package storageintegrity

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// PayloadLeaseManager keeps a spooled payload leased until sequencing has been
// durably accepted.
type PayloadLeaseManager interface {
	EnsurePayloadLease(ctx context.Context, adm AdmissionRecord, expectedRef string) error
	ReleasePayloadLease(payloadHash string)
	Run(ctx context.Context)
}

type trackedPayloadLease struct {
	admission   AdmissionRecord
	expectedRef string
}

// PayloadLeaseSupervisor redrives idempotent PayloadStore puts from exact
// spooled bytes. SpoolingPayloadWriter decides whether the current lease is
// close enough to expiry to require a remote refresh.
type PayloadLeaseSupervisor struct {
	writer          *SpoolingPayloadWriter
	refreshInterval time.Duration

	refreshMu sync.Mutex
	mu        sync.Mutex
	tracked   map[string]trackedPayloadLease
}

func NewPayloadLeaseSupervisor(writer *SpoolingPayloadWriter, refreshInterval time.Duration) *PayloadLeaseSupervisor {
	if refreshInterval <= 0 {
		refreshInterval = time.Second
	}
	return &PayloadLeaseSupervisor{
		writer:          writer,
		refreshInterval: refreshInterval,
		tracked:         map[string]trackedPayloadLease{},
	}
}

func (s *PayloadLeaseSupervisor) EnsurePayloadLease(ctx context.Context, adm AdmissionRecord, expectedRef string) error {
	if s == nil || s.writer == nil {
		return errors.New("storageintegrity: payload lease writer is required")
	}
	if expectedRef == "" {
		return errors.New("storageintegrity: expected payload_ref is required")
	}
	put, err := s.put(ctx, adm)
	if err != nil {
		return fmt.Errorf("storageintegrity: ensure payload lease for %s: %w", adm.StatementID, err)
	}
	if put.PayloadRef != expectedRef {
		return fmt.Errorf("storageintegrity: payload_ref changed for %s: got %q, want %q", adm.StatementID, put.PayloadRef, expectedRef)
	}
	s.mu.Lock()
	if existing, ok := s.tracked[adm.PayloadHash]; ok && existing.expectedRef != expectedRef {
		s.mu.Unlock()
		return fmt.Errorf("storageintegrity: payload hash %s already tracked with payload_ref %q", adm.PayloadHash, existing.expectedRef)
	}
	s.tracked[adm.PayloadHash] = trackedPayloadLease{
		admission:   cloneAdmissionRecord(adm),
		expectedRef: expectedRef,
	}
	s.mu.Unlock()
	return nil
}

func (s *PayloadLeaseSupervisor) ReleasePayloadLease(payloadHash string) {
	if s == nil || payloadHash == "" {
		return
	}
	s.mu.Lock()
	delete(s.tracked, payloadHash)
	s.mu.Unlock()
}

func (s *PayloadLeaseSupervisor) Run(ctx context.Context) {
	if s == nil {
		return
	}
	ticker := time.NewTicker(s.refreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			_ = s.refreshTracked(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (s *PayloadLeaseSupervisor) refreshTracked(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	items := make([]trackedPayloadLease, 0, len(s.tracked))
	for _, item := range s.tracked {
		items = append(items, trackedPayloadLease{
			admission:   cloneAdmissionRecord(item.admission),
			expectedRef: item.expectedRef,
		})
	}
	s.mu.Unlock()

	var errs []error
	for _, item := range items {
		put, err := s.put(ctx, item.admission)
		if err != nil {
			errs = append(errs, fmt.Errorf("refresh %s: %w", item.admission.StatementID, err))
			continue
		}
		if put.PayloadRef != item.expectedRef {
			errs = append(errs, fmt.Errorf(
				"payload_ref changed for %s: got %q, want %q",
				item.admission.StatementID,
				put.PayloadRef,
				item.expectedRef,
			))
		}
	}
	return errors.Join(errs...)
}

func (s *PayloadLeaseSupervisor) put(ctx context.Context, adm AdmissionRecord) (PayloadPutResult, error) {
	s.refreshMu.Lock()
	defer s.refreshMu.Unlock()
	return s.writer.PutPayload(ctx, adm.Payload, adm.PayloadHash, adm.PayloadLength)
}
