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
//	AgentPrepare.AuthorizeForward     -> AuthorizeSubmit
//	AgentPrepare.ReconcileCancel      -> CancelAndReconcile
//	AgentPrepare.PersistForwardUnknown -> PersistAuthorizationUnknownAndReconcile
//
// SubmitAfterAuthorization remains deliberately separate. The relay owner
// calls it only after its authorization callback returned successfully; an
// intent or a gate win alone never gives this port authority to submit.
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
// callbacks. The generation owner invokes it only after AuthorizeForward has
// returned nil.
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
		AuthorizeForward:         p.AuthorizeSubmit,
		ReconcileCancel:          p.CancelAndReconcile,
		PersistForwardUnknown:    p.PersistAuthorizationUnknownAndReconcile,
		SubmitAfterAuthorization: p.SubmitAfterAuthorization,
	}
}

// Prepare records the original signed input. It is safe to repeat and never
// advances an already durable record.
func (p *SnapshotQueryIntakePhasePort) Prepare(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.prepareLocked(ctx)
}

// PersistSubmitIntent is the AgentPrepare PersistForwardIntent counterpart. A
// durable intent is intentionally not a right to call Submit.
func (p *SnapshotQueryIntakePhasePort) PersistSubmitIntent(ctx context.Context) error {
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
}

// AuthorizeSubmit is the AgentPrepare AuthorizeForward counterpart. It must be
// called only by the winner of the generation gate. Its successful return means
// the exact original envelope is durably bound to SubmitAuthorized; it does not
// itself contact the sequencer.
func (p *SnapshotQueryIntakePhasePort) AuthorizeSubmit(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.prepareLocked(ctx); err != nil {
		return err
	}
	switch p.record.Stage {
	case SnapshotQueryStageSubmitAuthorized:
		return verifyLaunchAuthorization(p.record)
	case SnapshotQueryStageSubmitIntent:
		p.record.Stage = SnapshotQueryStageSubmitAuthorized
		p.record.LaunchAuthorization = snapshotQueryLaunchAuthorization(p.env)
		if err := p.saveLocked(ctx); err != nil {
			// The write may have reached durable storage despite its returned
			// error. Record an ambiguity which recovery can only lookup and
			// reconcile; it deliberately has no Submit retry authority.
			unknownErr := p.persistAuthorizationPersistenceUnknownAndReconcileLocked(ctx)
			return errors.Join(err, unknownErr)
		}
		return nil
	default:
		return fmt.Errorf("storageintegrity: cannot authorize snapshot query submit from stage %q", p.record.Stage)
	}
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
// valid only before a submit authorization is durable. It first records the
// cancellation boundary and only then performs the authoritative lookup/C2
// reconciliation.
func (p *SnapshotQueryIntakePhasePort) CancelAndReconcile(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.prepareLocked(ctx); err != nil {
		return err
	}
	switch p.record.Stage {
	case SnapshotQueryStageSubmitIntent:
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
}

// PersistAuthorizationUnknownAndReconcile is the AgentPrepare
// PersistForwardUnknown counterpart. It is used after a gate win when the
// authorization write or subsequent client delivery became indeterminate. It
// never issues Submit and leaves recovery ownership durable.
func (p *SnapshotQueryIntakePhasePort) PersistAuthorizationUnknownAndReconcile(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.prepareLocked(ctx); err != nil {
		return err
	}
	return p.persistAuthorizationUnknownAndReconcileLocked(ctx)
}

func (p *SnapshotQueryIntakePhasePort) persistAuthorizationUnknownAndReconcileLocked(ctx context.Context) error {
	switch p.record.Stage {
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
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.prepareLocked(ctx); err != nil {
		return SnapshotQueryIntakeResult{}, err
	}
	switch p.record.Stage {
	case SnapshotQueryStageSequenced:
		return resultFromSubmit(p.record.StatementID, p.record.Envelope.InputRoot, p.record.Submit), nil
	case SnapshotQueryStageSubmitAuthorized:
		if err := verifyLaunchAuthorization(p.record); err != nil {
			return SnapshotQueryIntakeResult{}, err
		}
	default:
		return SnapshotQueryIntakeResult{}, fmt.Errorf("storageintegrity: snapshot query submit requires durable authorization, found stage %q", p.record.Stage)
	}
	result, err := p.intake.submitAuthorized(ctx, p.record)
	if current, found, loadErr := p.intake.opts.Journal.Load(ctx, p.record.StatementID); loadErr == nil && found {
		p.record = current
	} else if loadErr != nil && err == nil {
		return SnapshotQueryIntakeResult{}, loadErr
	}
	return result, err
}

func (p *SnapshotQueryIntakePhasePort) prepareLocked(ctx context.Context) error {
	if p.prepared {
		return nil
	}
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
