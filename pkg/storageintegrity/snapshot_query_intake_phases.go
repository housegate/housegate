package storageintegrity

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/housegate/housegate/pkg/replay"
)

// SnapshotQueryIntakePhasePort exposes the durable C4 submission boundaries
// without coupling storageintegrity to Relay or plugin. A caller that owns a
// relay generation can map its callbacks as follows:
//
//	AgentPrepare.PersistForwardIntent -> PersistSubmitIntent
//	AgentPrepare.AuthorizeForward     -> AuthorizeForward
//	AgentPrepare.ReconcileCancel      -> CancelAndReconcile
//	AgentPrepare.PersistForwardUnknown -> PersistAuthorizationUnknownAndReconcile
//
// SubmitAfterAuthorization remains deliberately separate, and so does
// AuthorizeSubmit: a relay callback can only reach ForwardAuthorized. An
// intent or a forward gate win alone never gives this port authority to
// submit; only a host submit gate may call AuthorizeSubmit.
//
// The port is single-operation and serializes its own phase calls. It does not
// replace SnapshotQueryIntake.Submit or Recover, which remain the synchronous
// compatibility API.
type SnapshotQueryIntakePhasePort struct {
	intake *SnapshotQueryIntake
	env    replay.SnapshotQueryEnvelope

	mu       sync.Mutex
	record   SnapshotQueryJournalRecord
	prepared bool
}

// SnapshotQueryAgentPreparePhaseCallbacks is a framework-neutral adapter for
// the corresponding AgentPrepare lifecycle callbacks. The plugin layer wraps
// these no-argument phase functions in its PreparedAgentQuery callback shape;
// storageintegrity deliberately does not import plugin or Relay.
//
// SubmitAfterAuthorization is intentionally separate from the four relay
// callbacks. The generation owner invokes it only after the host submit gate
// has advanced the durable record past AuthorizeForward with AuthorizeSubmit.
type SnapshotQueryAgentPreparePhaseCallbacks struct {
	Prepare                  func(context.Context) error
	PersistForwardIntent     func(context.Context) error
	AuthorizeForward         func(context.Context) error
	ReconcileCancel          func(context.Context) error
	PersistForwardUnknown    func(context.Context) error
	SubmitAfterAuthorization func(context.Context) (SnapshotQueryIntakeResult, error)
}

// NewSnapshotQueryIntakePhasePort validates the exact signed input before a
// phase can have a durable or sequencer effect. Prepare then establishes the
// durable Signed record, so a caller may finish its detached preparation before
// it records SubmitIntent.
func (s *SnapshotQueryIntake) NewSnapshotQueryIntakePhasePort(ctx context.Context, env replay.SnapshotQueryEnvelope) (*SnapshotQueryIntakePhasePort, error) {
	if err := validateSnapshotQueryEnvelope(ctx, s.opts.Validator, env); err != nil {
		return nil, err
	}
	return &SnapshotQueryIntakePhasePort{intake: s, env: env}, nil
}

// AgentPrepareCallbacks returns the phase functions with names matching the
// relay's AgentPrepare ownership boundaries. The returned functions share this
// port's serialized operation state.
func (p *SnapshotQueryIntakePhasePort) AgentPrepareCallbacks() SnapshotQueryAgentPreparePhaseCallbacks {
	return SnapshotQueryAgentPreparePhaseCallbacks{
		Prepare:                  p.Prepare,
		PersistForwardIntent:     p.PersistSubmitIntent,
		AuthorizeForward:         p.AuthorizeForward,
		ReconcileCancel:          p.CancelAndReconcile,
		PersistForwardUnknown:    p.PersistAuthorizationUnknownAndReconcile,
		SubmitAfterAuthorization: p.SubmitAfterAuthorization,
	}
}

// Prepare records the original signed input. It is safe to repeat and never
// advances an already durable record.
func (p *SnapshotQueryIntakePhasePort) Prepare(ctx context.Context) error {
	return p.withStatementLock(ctx, func() error {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.prepareLocked(ctx)
	})
}

// PersistSubmitIntent is the AgentPrepare PersistForwardIntent counterpart. A
// durable intent is intentionally not a right to call Submit.
func (p *SnapshotQueryIntakePhasePort) PersistSubmitIntent(ctx context.Context) error {
	return p.withStatementLock(ctx, func() error {
		p.mu.Lock()
		defer p.mu.Unlock()
		if err := p.prepareLocked(ctx); err != nil {
			return err
		}
		switch p.record.Stage {
		case SnapshotQueryStageSigned:
			p.record.Stage = SnapshotQueryStageSubmitIntent
			return p.saveLocked(ctx)
		case SnapshotQueryStageSubmitIntent:
			return nil
		default:
			return fmt.Errorf("storageintegrity: cannot persist snapshot query submit intent from stage %q", p.record.Stage)
		}
	})
}

// AuthorizeForward is the AgentPrepare AuthorizeForward counterpart. It must be
// called only by the winner of the generation gate and durably records that
// gate win. It is a distinct boundary from SubmitAuthorized: forwarding a Query
// to the host never authorizes anyone to Submit, so a crash after forwarding
// leaves recovery with lookup/fence authority only.
func (p *SnapshotQueryIntakePhasePort) AuthorizeForward(ctx context.Context) error {
	return p.withStatementLock(ctx, func() error {
		p.mu.Lock()
		defer p.mu.Unlock()
		serviceCtx, cancel := p.intake.recoveryAttemptContext()
		defer cancel()
		if err := p.prepareLocked(serviceCtx); err != nil {
			return err
		}
		switch p.record.Stage {
		case SnapshotQueryStageForwardAuthorized, SnapshotQueryStageSubmitAuthorized:
			return verifyForwardAuthorization(p.record)
		case SnapshotQueryStageSubmitIntent:
			p.record.Stage = SnapshotQueryStageForwardAuthorized
			p.record.ForwardAuthorization = snapshotQueryLaunchAuthorization(p.env)
			if err := p.saveLocked(serviceCtx); err != nil {
				// The write may have reached durable storage despite its returned
				// error. Record an ambiguity which recovery can only lookup and
				// reconcile; it deliberately has no Submit retry authority.
				unknownErr := p.persistAuthorizationPersistenceUnknownAndReconcileLocked(serviceCtx)
				return errors.Join(err, unknownErr)
			}
			return nil
		default:
			return fmt.Errorf("storageintegrity: cannot authorize snapshot query forward from stage %q", p.record.Stage)
		}
	})
}

// AuthorizeSubmit is the host submit gate's boundary, not a relay callback. It
// requires a durable ForwardAuthorized record, so the forward gate win and the
// submit authorization can never be the same durable fact. Its successful
// return means the exact original envelope is durably bound to
// SubmitAuthorized; it does not itself contact the sequencer.
func (p *SnapshotQueryIntakePhasePort) AuthorizeSubmit(ctx context.Context) error {
	return p.withStatementLock(ctx, func() error {
		p.mu.Lock()
		defer p.mu.Unlock()
		serviceCtx, cancel := p.intake.recoveryAttemptContext()
		defer cancel()
		if err := p.prepareLocked(serviceCtx); err != nil {
			return err
		}
		switch p.record.Stage {
		case SnapshotQueryStageSubmitAuthorized:
			return verifyLaunchAuthorization(p.record)
		case SnapshotQueryStageForwardAuthorized:
			if err := verifyForwardAuthorization(p.record); err != nil {
				return err
			}
			p.record.Stage = SnapshotQueryStageSubmitAuthorized
			p.record.LaunchAuthorization = snapshotQueryLaunchAuthorization(p.env)
			if err := p.saveLocked(serviceCtx); err != nil {
				// The write may have reached durable storage despite its returned
				// error. Record an ambiguity which recovery can only lookup and
				// reconcile; it deliberately has no Submit retry authority.
				unknownErr := p.persistAuthorizationPersistenceUnknownAndReconcileLocked(serviceCtx)
				return errors.Join(err, unknownErr)
			}
			return nil
		default:
			return fmt.Errorf("storageintegrity: cannot authorize snapshot query submit from stage %q", p.record.Stage)
		}
	})
}

func (p *SnapshotQueryIntakePhasePort) persistAuthorizationPersistenceUnknownAndReconcileLocked(ctx context.Context) error {
	p.record.Stage = SnapshotQueryStageSubmitAuthorizationUnknown
	p.record.SubmitUnknown = false
	if err := p.saveLocked(ctx); err != nil {
		return errors.Join(err, p.intake.reconcileIntent(ctx, p.record))
	}
	return p.intake.reconcileIntent(ctx, p.record)
}

// CancelAndReconcile is the AgentPrepare ReconcileCancel counterpart. It is
// valid only before a submit authorization is durable.  Prepare deliberately
// persists Signed before the relay has a reason to persist SubmitIntent, so a
// cancellation at that boundary must be a first-class durable transition too:
// it records CancelPending directly from Signed rather than manufacturing an
// intent which could be mistaken for submission work after a restart.  The
// cancellation boundary is durable before the authoritative lookup/C2
// reconciliation begins.
func (p *SnapshotQueryIntakePhasePort) CancelAndReconcile(ctx context.Context) error {
	return p.withStatementLock(ctx, func() error {
		p.mu.Lock()
		defer p.mu.Unlock()
		if err := p.prepareLocked(ctx); err != nil {
			return err
		}
		switch p.record.Stage {
		case SnapshotQueryStageSigned, SnapshotQueryStageSubmitIntent:
			p.record.Stage = SnapshotQueryStageCancelPending
			p.record.PreSubmitCancelIntent = true
			p.record.ReleaseReconciliationDebt = true
			if err := p.saveLocked(ctx); err != nil {
				return err
			}
		case SnapshotQueryStageCancelPending, SnapshotQueryStageReleased:
			// The durable cancellation boundary already exists; reconciliation is
			// repeatable and remains service-owned.
		default:
			return fmt.Errorf("storageintegrity: cannot cancel snapshot query submit from stage %q", p.record.Stage)
		}
		return p.intake.reconcileIntent(ctx, p.record)
	})
}

// PersistAuthorizationUnknownAndReconcile is the AgentPrepare
// PersistForwardUnknown counterpart. It is used after a gate win when the
// authorization write or subsequent client delivery became indeterminate. It
// never issues Submit and leaves recovery ownership durable.
func (p *SnapshotQueryIntakePhasePort) PersistAuthorizationUnknownAndReconcile(ctx context.Context) error {
	return p.withStatementLock(ctx, func() error {
		p.mu.Lock()
		defer p.mu.Unlock()
		serviceCtx, cancel := p.intake.recoveryAttemptContext()
		defer cancel()
		if err := p.prepareLocked(serviceCtx); err != nil {
			return err
		}
		return p.persistAuthorizationUnknownAndReconcileLocked(serviceCtx)
	})
}

func (p *SnapshotQueryIntakePhasePort) persistAuthorizationUnknownAndReconcileLocked(ctx context.Context) error {
	switch p.record.Stage {
	case SnapshotQueryStageForwardAuthorized:
		// The forward authorization is durable but the client delivery became
		// indeterminate. No Submit was ever authorized, so this must not become
		// SubmitUnknown, whose recovery may retry a Submit; recovery of this
		// stage may only look up and fence.
		p.record.Stage = SnapshotQueryStageSubmitAuthorizationUnknown
		p.record.SubmitUnknown = false
		if err := p.saveLocked(ctx); err != nil {
			return errors.Join(err, p.intake.reconcileIntent(ctx, p.record))
		}
		return p.intake.reconcileIntent(ctx, p.record)
	case SnapshotQueryStageSubmitAuthorizationUnknown:
		return p.intake.reconcileIntent(ctx, p.record)
	case SnapshotQueryStageSubmitAuthorized:
		p.record.Stage = SnapshotQueryStageSubmitUnknown
		p.record.SubmitUnknown = true
		if p.record.LaunchAuthorization == nil {
			p.record.LaunchAuthorization = snapshotQueryLaunchAuthorization(p.env)
		}
		if err := p.saveLocked(ctx); err != nil {
			return errors.Join(err, p.intake.reconcileIntent(ctx, p.record))
		}
		return p.intake.reconcileIntent(ctx, p.record)
	case SnapshotQueryStageSubmitUnknown:
		return p.intake.reconcileIntent(ctx, p.record)
	default:
		return fmt.Errorf("storageintegrity: cannot persist unknown snapshot query authorization from stage %q", p.record.Stage)
	}
}

// SubmitAfterAuthorization is deliberately not an AgentPrepare callback. The
// generation owner invokes it only after AuthorizeSubmit has returned nil. It
// enforces that durable boundary again, making accidental submit-before-fsync
// impossible through this port.
func (p *SnapshotQueryIntakePhasePort) SubmitAfterAuthorization(ctx context.Context) (SnapshotQueryIntakeResult, error) {
	var result SnapshotQueryIntakeResult
	err := p.withStatementLock(ctx, func() error {
		p.mu.Lock()
		defer p.mu.Unlock()
		serviceCtx, cancel := p.intake.recoveryAttemptContext()
		defer cancel()
		if err := p.prepareLocked(serviceCtx); err != nil {
			return err
		}
		switch p.record.Stage {
		case SnapshotQueryStageSequenced:
			result = resultFromSubmit(p.record.StatementID, p.record.Envelope.InputRoot, p.record.Submit)
			return nil
		case SnapshotQueryStageSubmitAuthorized:
			if err := verifyLaunchAuthorization(p.record); err != nil {
				return err
			}
		default:
			return fmt.Errorf("storageintegrity: snapshot query submit requires durable authorization, found stage %q", p.record.Stage)
		}
		var err error
		result, err = p.intake.submitAuthorized(serviceCtx, p.record)
		if current, found, loadErr := p.intake.opts.Journal.Load(serviceCtx, p.record.StatementID); loadErr == nil && found {
			p.record = current
		} else if loadErr != nil && err == nil {
			return loadErr
		}
		return err
	})
	return result, err
}

func (p *SnapshotQueryIntakePhasePort) withStatementLock(ctx context.Context, fn func() error) error {
	release, err := p.intake.lockStatement(ctx, p.env.Input.Binding.StatementID)
	if err != nil {
		return err
	}
	defer release()
	return fn()
}

func (p *SnapshotQueryIntakePhasePort) prepareLocked(ctx context.Context) error {
	statementID := p.env.Input.Binding.StatementID
	rec, found, err := p.intake.opts.Journal.Load(ctx, statementID)
	if err != nil {
		return err
	}
	if found {
		if err := matchSnapshotQueryIdentity(rec, p.env); err != nil {
			return err
		}
		p.record, p.prepared = rec, true
		return nil
	}
	p.record = newSnapshotQueryRecord(p.env)
	if err := p.saveLocked(ctx); err != nil {
		return err
	}
	p.prepared = true
	return nil
}

func (p *SnapshotQueryIntakePhasePort) saveLocked(ctx context.Context) error {
	if err := p.intake.opts.Journal.Save(ctx, p.record); err != nil {
		return err
	}
	return nil
}

func snapshotQueryLaunchAuthorization(env replay.SnapshotQueryEnvelope) *SnapshotQueryLaunchAuthorization {
	return &SnapshotQueryLaunchAuthorization{
		InputRoot:         env.InputRoot,
		OriginalJWSHash:   replay.DigestString(env.UserJWS),
		ReservationID:     env.Input.Binding.ReservationID,
		FencingGeneration: env.Input.Binding.FencingGeneration,
	}
}
