package storageintegrity

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/replay"
)

// SnapshotQuerySubmitGate serializes a one-shot relay generation. It grants no
// I/O or release authority; its only purpose is deciding whether authorization
// persistence may begin.
type SnapshotQuerySubmitGate interface{ TryStart() bool }

// QuerySequencer is the C4 injection boundary. No concrete Arbiter adapter is
// supplied by this package.
type QuerySequencer interface {
	SubmitSnapshotQuery(context.Context, replay.SnapshotQueryEnvelope) (replay.SnapshotQuerySubmitResult, error)
	LookupSnapshotQuery(context.Context, string, string, string) (replay.SnapshotQueryStatus, error)
}

// SnapshotQueryEnvelopeValidator verifies the original compact JWS against the
// full recomputed envelope binding before any journal or sequencer side effect.
type SnapshotQueryEnvelopeValidator interface {
	ValidateStatementV3(string, auth.JWSStatementPayloadV3) (string, error)
}

// SnapshotQueryIntentReconciler owns the C2 conditional fence/release action.
// It is called for durable intent-only work only after the authoritative lookup.
type SnapshotQueryIntentReconciler interface {
	ReconcileSnapshotQueryIntent(context.Context, SnapshotQueryJournalRecord, replay.SnapshotQueryStatus) error
}

type SnapshotQueryIntakeOptions struct {
	Journal                SnapshotQueryJournal
	Sequencer              QuerySequencer
	Validator              SnapshotQueryEnvelopeValidator
	Reconciler             SnapshotQueryIntentReconciler
	RecoveryAttemptTimeout time.Duration
}

// SnapshotQueryIntake is intentionally inert until explicitly constructed and
// injected. Production wiring is out of scope for this core.
type SnapshotQueryIntake struct {
	opts SnapshotQueryIntakeOptions

	statementLocksMu sync.Mutex
	statementLocks   map[string]*snapshotQueryStatementLock
}

type snapshotQueryStatementLock struct {
	gate chan struct{}
	refs int
}

const defaultSnapshotQueryRecoveryAttemptTimeout = 30 * time.Second

func NewSnapshotQueryIntake(opts SnapshotQueryIntakeOptions) (*SnapshotQueryIntake, error) {
	if opts.Journal == nil || opts.Sequencer == nil || opts.Validator == nil || opts.Reconciler == nil {
		return nil, errors.New("storageintegrity: snapshot query intake requires journal, sequencer, validator and reconciler")
	}
	if opts.RecoveryAttemptTimeout <= 0 {
		opts.RecoveryAttemptTimeout = defaultSnapshotQueryRecoveryAttemptTimeout
	}
	return &SnapshotQueryIntake{opts: opts, statementLocks: make(map[string]*snapshotQueryStatementLock)}, nil
}

type SnapshotQueryIntakeResult struct {
	StatementID      string
	InputRoot        string
	BlockSeq         uint64
	AckLevel         string
	ExecutionOutcome string
	OutputRowsRoot   string
}

func (s *SnapshotQueryIntake) Submit(ctx context.Context, env replay.SnapshotQueryEnvelope, gate SnapshotQuerySubmitGate) (SnapshotQueryIntakeResult, error) {
	if gate == nil {
		return SnapshotQueryIntakeResult{}, errors.New("storageintegrity: snapshot query submit gate is required")
	}
	if err := validateSnapshotQueryEnvelope(ctx, s.opts.Validator, env); err != nil {
		return SnapshotQueryIntakeResult{}, err
	}
	statementID := env.Input.Binding.StatementID
	release, err := s.lockStatement(ctx, statementID)
	if err != nil {
		return SnapshotQueryIntakeResult{}, err
	}
	defer release()
	return s.submitLocked(ctx, env, gate)
}

func (s *SnapshotQueryIntake) submitLocked(ctx context.Context, env replay.SnapshotQueryEnvelope, gate SnapshotQuerySubmitGate) (SnapshotQueryIntakeResult, error) {
	statementID := env.Input.Binding.StatementID
	if old, found, err := s.opts.Journal.Load(ctx, statementID); err != nil {
		return SnapshotQueryIntakeResult{}, err
	} else if found {
		if err := matchSnapshotQueryIdentity(old, env); err != nil {
			return SnapshotQueryIntakeResult{}, err
		}
		serviceCtx, cancel := s.recoveryAttemptContext()
		defer cancel()
		return s.recoverRecord(serviceCtx, old)
	}
	rec := newSnapshotQueryRecord(env)
	if err := s.opts.Journal.Save(ctx, rec); err != nil {
		return SnapshotQueryIntakeResult{}, err
	}
	rec.Stage = SnapshotQueryStageSubmitIntent
	if err := s.opts.Journal.Save(ctx, rec); err != nil {
		return SnapshotQueryIntakeResult{}, err
	}
	if !gate.TryStart() {
		rec.Stage, rec.PreSubmitCancelIntent, rec.ReleaseReconciliationDebt = SnapshotQueryStageCancelPending, true, true
		if err := s.opts.Journal.Save(ctx, rec); err != nil {
			return SnapshotQueryIntakeResult{}, err
		}
		return SnapshotQueryIntakeResult{}, s.reconcileIntent(ctx, rec)
	}
	// A gate win transfers ownership from the caller/relay to intake. Do not
	// inherit a later client cancellation while persisting authorization or
	// reconciling the sequencer result.
	serviceCtx, cancel := s.recoveryAttemptContext()
	defer cancel()
	rec.Stage = SnapshotQueryStageSubmitAuthorized
	rec.LaunchAuthorization = &SnapshotQueryLaunchAuthorization{InputRoot: env.InputRoot, OriginalJWSHash: replay.DigestString(env.UserJWS), ReservationID: env.Input.Binding.ReservationID, FencingGeneration: env.Input.Binding.FencingGeneration}
	if err := s.opts.Journal.Save(serviceCtx, rec); err != nil {
		// Persistence may be uncertain. Never issue Submit in this process; only
		// reconcile the last definitely durable intent boundary.
		return SnapshotQueryIntakeResult{}, errors.Join(err, s.reconcileIntent(serviceCtx, newSnapshotQueryRecordAtIntent(env)))
	}
	return s.submitAuthorized(serviceCtx, rec)
}

// Recover never promotes intent-only work into submission. A NotFound result is
// sufficient only for C2 conditional fencing/release, never for a retry.
func (s *SnapshotQueryIntake) Recover(ctx context.Context) error {
	records, err := s.opts.Journal.List(ctx)
	if err != nil {
		return err
	}
	for _, rec := range records {
		release, err := s.lockStatement(ctx, rec.StatementID)
		if err != nil {
			return err
		}
		// List is only discovery. Reload under the statement lock so a Submit
		// that completed while Recover was waiting cannot be replayed from a
		// stale pre-sequenced record.
		rec, found, err := s.opts.Journal.Load(ctx, rec.StatementID)
		if err != nil {
			release()
			return err
		}
		if !found {
			release()
			continue
		}
		if err := validateSnapshotQueryEnvelopeHistorical(ctx, rec.Envelope); err != nil {
			release()
			return fmt.Errorf("storageintegrity: recover snapshot query %s: %w", rec.StatementID, err)
		}
		attemptCtx, cancel := s.recoveryAttemptContext()
		_, err = s.recoverRecord(attemptCtx, rec)
		cancel()
		release()
		if err != nil {
			return fmt.Errorf("storageintegrity: recover snapshot query %s: %w", rec.StatementID, err)
		}
	}
	return nil
}

func (s *SnapshotQueryIntake) recoverRecord(ctx context.Context, rec SnapshotQueryJournalRecord) (SnapshotQueryIntakeResult, error) {
	switch rec.Stage {
	case SnapshotQueryStageSigned, SnapshotQueryStageSubmitIntent, SnapshotQueryStageSubmitAuthorizationUnknown, SnapshotQueryStageCancelPending, SnapshotQueryStageReleased:
		return SnapshotQueryIntakeResult{}, s.reconcileIntent(ctx, rec)
	case SnapshotQueryStageSequenced:
		return resultFromSubmit(rec.StatementID, rec.Envelope.InputRoot, rec.Submit), nil
	case SnapshotQueryStageRejected:
		return SnapshotQueryIntakeResult{}, snapshotQueryAdmissionError(rec.Submit)
	case SnapshotQueryStageSubmitAuthorized, SnapshotQueryStageSubmitUnknown:
		if err := verifyLaunchAuthorization(rec); err != nil {
			return SnapshotQueryIntakeResult{}, err
		}
		status, err := s.lookup(ctx, rec)
		if err != nil {
			return SnapshotQueryIntakeResult{}, err
		}
		if status.Found {
			if err := validateSnapshotQueryAccepted(rec.Envelope, status.Accepted); err != nil {
				return SnapshotQueryIntakeResult{}, err
			}
			rec.Stage, rec.Submit, rec.HasSubmit, rec.SubmitUnknown = SnapshotQueryStageSequenced, status.Accepted, true, false
			if err := s.opts.Journal.Save(ctx, rec); err != nil {
				return SnapshotQueryIntakeResult{}, err
			}
			return resultFromSubmit(rec.StatementID, rec.Envelope.InputRoot, rec.Submit), nil
		}
		return s.submitAuthorized(ctx, rec)
	default:
		return SnapshotQueryIntakeResult{}, fmt.Errorf("storageintegrity: unsupported snapshot query journal stage %q", rec.Stage)
	}
}

func (s *SnapshotQueryIntake) submitAuthorized(ctx context.Context, rec SnapshotQueryJournalRecord) (SnapshotQueryIntakeResult, error) {
	result, err := s.opts.Sequencer.SubmitSnapshotQuery(ctx, rec.Envelope)
	if err != nil {
		rec.Stage, rec.SubmitUnknown = SnapshotQueryStageSubmitUnknown, true
		if saveErr := s.opts.Journal.Save(ctx, rec); saveErr != nil {
			return SnapshotQueryIntakeResult{}, errors.Join(err, saveErr)
		}
		return SnapshotQueryIntakeResult{}, err
	}
	if err := validateSnapshotQueryAccepted(rec.Envelope, result); err != nil {
		rec.Submit = result
		if isSnapshotQueryTerminalRejection(result.AdmissionCode) {
			rec.Stage, rec.HasSubmit, rec.SubmitUnknown = SnapshotQueryStageRejected, true, false
		} else {
			rec.Stage, rec.HasSubmit, rec.SubmitUnknown = SnapshotQueryStageSubmitUnknown, true, true
		}
		if saveErr := s.opts.Journal.Save(ctx, rec); saveErr != nil {
			return SnapshotQueryIntakeResult{}, errors.Join(err, saveErr)
		}
		return SnapshotQueryIntakeResult{}, err
	}
	rec.Stage, rec.Submit, rec.HasSubmit, rec.SubmitUnknown = SnapshotQueryStageSequenced, result, true, false
	if err := s.opts.Journal.Save(ctx, rec); err != nil {
		return SnapshotQueryIntakeResult{}, err
	}
	return resultFromSubmit(rec.StatementID, rec.Envelope.InputRoot, result), nil
}

func (s *SnapshotQueryIntake) recoveryAttemptContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), s.opts.RecoveryAttemptTimeout)
}

func (s *SnapshotQueryIntake) lockStatement(ctx context.Context, statementID string) (func(), error) {
	s.statementLocksMu.Lock()
	lock := s.statementLocks[statementID]
	if lock == nil {
		lock = &snapshotQueryStatementLock{gate: make(chan struct{}, 1)}
		s.statementLocks[statementID] = lock
	}
	lock.refs++
	s.statementLocksMu.Unlock()

	select {
	case lock.gate <- struct{}{}:
	case <-ctx.Done():
		s.releaseStatementLock(statementID, lock, false)
		return nil, ctx.Err()
	}
	return func() { s.releaseStatementLock(statementID, lock, true) }, nil
}

func (s *SnapshotQueryIntake) releaseStatementLock(statementID string, lock *snapshotQueryStatementLock, held bool) {
	if held {
		<-lock.gate
	}
	s.statementLocksMu.Lock()
	lock.refs--
	if lock.refs == 0 && s.statementLocks[statementID] == lock {
		delete(s.statementLocks, statementID)
	}
	s.statementLocksMu.Unlock()
}

func (s *SnapshotQueryIntake) reconcileIntent(ctx context.Context, rec SnapshotQueryJournalRecord) error {
	status, err := s.lookup(ctx, rec)
	if err != nil {
		return err
	}
	return s.opts.Reconciler.ReconcileSnapshotQueryIntent(ctx, rec, status)
}

func (s *SnapshotQueryIntake) lookup(ctx context.Context, rec SnapshotQueryJournalRecord) (replay.SnapshotQueryStatus, error) {
	return s.opts.Sequencer.LookupSnapshotQuery(ctx, rec.StatementID, rec.Envelope.InputRoot, replay.DigestString(rec.Envelope.UserJWS))
}

func newSnapshotQueryRecord(env replay.SnapshotQueryEnvelope) SnapshotQueryJournalRecord {
	return SnapshotQueryJournalRecord{Version: SnapshotQueryJournalVersion, StatementID: env.Input.Binding.StatementID, Envelope: env, Stage: SnapshotQueryStageSigned}
}
func newSnapshotQueryRecordAtIntent(env replay.SnapshotQueryEnvelope) SnapshotQueryJournalRecord {
	r := newSnapshotQueryRecord(env)
	r.Stage = SnapshotQueryStageSubmitIntent
	return r
}

func validateSnapshotQueryEnvelope(ctx context.Context, validator SnapshotQueryEnvelopeValidator, env replay.SnapshotQueryEnvelope) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if env.UserJWS == "" {
		return errors.New("storageintegrity: snapshot query original user JWS is required")
	}
	root, err := replay.SnapshotQueryInputRoot(env.Input)
	if err != nil {
		return fmt.Errorf("storageintegrity: invalid snapshot query input: %w", err)
	}
	if root != env.InputRoot {
		return errors.New("storageintegrity: snapshot query input root mismatch")
	}
	_, err = validator.ValidateStatementV3(env.UserJWS, auth.JWSStatementPayloadV3{Purpose: auth.StatementPurposeV3, Binding: env.Input.Binding, InputRoot: root})
	if err != nil {
		return fmt.Errorf("storageintegrity: validate snapshot query JWS: %w", err)
	}
	return nil
}

// validateSnapshotQueryEnvelopeHistorical deliberately does not apply ingress
// freshness or today's signer allowlist. A durable accepted/authorized record
// must remain cryptographically verifiable after the original JWS has aged out
// or policy has changed.
func validateSnapshotQueryEnvelopeHistorical(ctx context.Context, env replay.SnapshotQueryEnvelope) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	root, err := replay.SnapshotQueryInputRoot(env.Input)
	if err != nil {
		return fmt.Errorf("storageintegrity: invalid snapshot query input: %w", err)
	}
	if root != env.InputRoot {
		return errors.New("storageintegrity: snapshot query input root mismatch")
	}
	if env.UserJWS == "" {
		return errors.New("storageintegrity: snapshot query original user JWS is required")
	}
	_, err = auth.VerifyStatementV3Signature(env.UserJWS, auth.JWSStatementPayloadV3{Purpose: auth.StatementPurposeV3, Binding: env.Input.Binding, InputRoot: root})
	if err != nil {
		return fmt.Errorf("storageintegrity: verify historical snapshot query JWS: %w", err)
	}
	return nil
}

func matchSnapshotQueryIdentity(rec SnapshotQueryJournalRecord, env replay.SnapshotQueryEnvelope) error {
	if rec.Envelope.InputRoot != env.InputRoot || rec.Envelope.UserJWS != env.UserJWS {
		return errors.New("storageintegrity: snapshot query identity conflicts with durable record")
	}
	return nil
}

func verifyLaunchAuthorization(rec SnapshotQueryJournalRecord) error {
	a := rec.LaunchAuthorization
	if a == nil || a.InputRoot != rec.Envelope.InputRoot || a.OriginalJWSHash != replay.DigestString(rec.Envelope.UserJWS) || a.ReservationID != rec.Envelope.Input.Binding.ReservationID || a.FencingGeneration != rec.Envelope.Input.Binding.FencingGeneration {
		return errors.New("storageintegrity: snapshot query durable launch authorization does not bind the original envelope")
	}
	return nil
}

const snapshotQueryAdmissionAccepted uint32 = 1

func validateSnapshotQueryAccepted(env replay.SnapshotQueryEnvelope, result replay.SnapshotQuerySubmitResult) error {
	if result.AdmissionCode != snapshotQueryAdmissionAccepted {
		return snapshotQueryAdmissionError(result)
	}
	binding := env.Input.Binding
	reservation := result.Reservation
	if result.InputRoot != env.InputRoot || result.StatementSeq == 0 || result.BlockSeq == 0 || result.SourceNode == "" ||
		reservation.ReservationID != binding.ReservationID || reservation.FencingGeneration != binding.FencingGeneration ||
		reservation.ClientAccount != binding.ClientAccount || reservation.StatementID != binding.StatementID ||
		reservation.ReadSnapshot != binding.ReadSnapshot || reservation.ExecutorProfileID != binding.ExecutorProfileID ||
		reservation.QueryProfileID != binding.QueryProfileID {
		return errors.New("storageintegrity: sequencer accepted result identity mismatch")
	}
	return nil
}

func isSnapshotQueryTerminalRejection(code uint32) bool {
	return code >= 2 && code <= 8
}

func snapshotQueryAdmissionError(result replay.SnapshotQuerySubmitResult) error {
	if isSnapshotQueryTerminalRejection(result.AdmissionCode) {
		return fmt.Errorf("storageintegrity: sequencer rejected snapshot query admission code %d: %s", result.AdmissionCode, result.Message)
	}
	return fmt.Errorf("storageintegrity: sequencer returned unknown snapshot query admission code %d", result.AdmissionCode)
}

func resultFromSubmit(statementID, inputRoot string, submit replay.SnapshotQuerySubmitResult) SnapshotQueryIntakeResult {
	return SnapshotQueryIntakeResult{StatementID: statementID, InputRoot: inputRoot, BlockSeq: submit.BlockSeq, AckLevel: "sequenced"}
}
