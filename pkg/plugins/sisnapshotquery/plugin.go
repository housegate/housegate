// Package sisnapshotquery implements the default-disabled agent snapshot-query
// preparation lane. It deliberately owns no sockets, relay state, or runtime
// wiring; callers inject every durable and remote dependency.
package sisnapshotquery

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"

	chgo "github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/plugin"
	"github.com/housegate/housegate/pkg/replay"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

// Candidate is a pure classifier result. Query is copied before it reaches a
// worker, so a classifier must never retain or mutate the live query.
type Candidate struct {
	Recognized      bool
	LogicalDatabase string
}

// Classifier is intentionally synchronous and local. Network/FFI analysis is
// performed by Analyzer in the detached Prepare worker.
type Classifier interface {
	ClassifySnapshotQuery(sql, logicalDatabase string) (Candidate, error)
}

type Grant struct {
	RequestID   string
	BlockSeq    uint64
	Reservation replay.SnapshotQueryReservation
}
type ReservationPort interface {
	AcquireSnapshotQuery(context.Context, auth.SnapshotQueryControlBinding, string) (Grant, error)
}
type Catalog struct{ ReadSet replay.SnapshotReadSet }
type CatalogPort interface {
	LoadSnapshotQueryCatalog(context.Context, replay.SnapshotQueryReservation) (Catalog, error)
}
type Analysis struct {
	SQL, TargetTableID, SchemaHash, RowIDProfileID string
	ClientRevision                                 uint32
	// ReadTableIDs is the analyzer's complete base-table closure (design D5).
	// The signed read set is exactly these pinned-catalog tables; an id
	// outside the catalog or a duplicate refuses the statement. An empty
	// closure (constant SELECT) signs an explicit empty table list.
	ReadTableIDs []string
}
type Analyzer interface {
	PrepareSnapshotQuery(context.Context, Candidate, Catalog, Grant) (Analysis, error)
}

// signedReadSet projects the pinned catalog onto the analyzer's closure. It
// never infers tables from SQL text, from the target, or from the catalog size.
func signedReadSet(catalog replay.SnapshotReadSet, closure []string) (replay.SnapshotReadSet, error) {
	byID := make(map[string]replay.SnapshotReadTable, len(catalog.Tables))
	for _, t := range catalog.Tables {
		if _, dup := byID[t.TableID]; dup {
			return replay.SnapshotReadSet{}, fmt.Errorf("sisnapshotquery: catalog table %q is duplicated", t.TableID)
		}
		byID[t.TableID] = t
	}
	out := replay.SnapshotReadSet{ReadSnapshot: catalog.ReadSnapshot, Tables: []replay.SnapshotReadTable{}}
	seen := make(map[string]bool, len(closure))
	for _, id := range closure {
		if seen[id] {
			return replay.SnapshotReadSet{}, fmt.Errorf("sisnapshotquery: read closure table %q is duplicated", id)
		}
		seen[id] = true
		t, ok := byID[id]
		if !ok {
			return replay.SnapshotReadSet{}, fmt.Errorf("sisnapshotquery: read closure table %q is not in the pinned catalog", id)
		}
		out.Tables = append(out.Tables, t)
	}
	return out, nil
}

type Sequence interface {
	NextSnapshotQueryStatementID(context.Context, string) (string, error)
}
type StatementSigner interface {
	Address() string
	SignStatementV3(auth.JWSStatementPayloadV3) (string, error)
}

// Journal owns durable request identity and the exact cancel/forward records.
// Every argument is value-owned by this package; implementations must not use
// a query id or a caller context as a substitute for Operation identity.
type Journal interface {
	BeginSnapshotQuery(context.Context, Operation) error
	ReconcileCanceledSnapshotQuery(context.Context, Operation) error
	PersistSnapshotQueryForwardIntent(context.Context, Operation) error
	AuthorizeSnapshotQueryForward(context.Context, Operation) error
	PersistSnapshotQueryForwardUnknown(context.Context, Operation) error
}

// Operation is immutable once OnQuery has installed its plan.
type Operation struct {
	RequestID     string
	RequestSeed   string
	OriginalQuery chproto.Query
	Candidate     Candidate
	Grant         Grant
	Envelope      replay.SnapshotQueryEnvelope
}

// SnapshotQueryIntakePhasePort is the narrow C4 surface the detached D1
// worker may retain after it has built the complete signed envelope.  It does
// not include SubmitAfterAuthorization: relay callbacks must never submit or
// arrange a submit as a side effect of preparing a client query.
type SnapshotQueryIntakePhasePort interface {
	Prepare(context.Context) error
	PersistSubmitIntent(context.Context) error
	// AuthorizeForward persists the relay's forward gate win. It is the only
	// authorization this bridge may reach: AuthorizeSubmit is the host submit
	// gate's boundary, so a crash after forwarding can never be recovered as
	// submit authority.
	AuthorizeForward(context.Context) error
	AuthorizeSubmit(context.Context) error
	CancelAndReconcile(context.Context) error
	PersistAuthorizationUnknownAndReconcile(context.Context) error
}

// SnapshotQueryIntakePhasePortFactory is an injection-only adapter around
// storageintegrity.SnapshotQueryIntake.NewSnapshotQueryIntakePhasePort.  The
// default runtime does not supply one, so this does not wire an intake, a
// sequencer, or any submission path into Housegate.
type SnapshotQueryIntakePhasePortFactory func(context.Context, replay.SnapshotQueryEnvelope) (SnapshotQueryIntakePhasePort, error)

type operationState struct {
	mu sync.Mutex
	// The in-flight/done pairs coordinate phase decisions without holding a
	// state lock across an external C4 or journal call.
	value       Operation
	phasePort   SnapshotQueryIntakePhasePort
	phaseIntent bool
	// A forward lease has two stages.  An admitted callback may be parked
	// between its identity check and the external port call; cancellation wins
	// that stage immediately and call start then fails.  Once beginCall has
	// linearized an actual port call, cancellation waits for it to return before
	// reconciling.  This keeps no mutex across I/O while leaving no unlocked
	// check-to-call window.
	forwardAdmission bool
	forwardIO        bool
	forwardDone      chan struct{}
	// phasePrepare* is deliberately separate from phasePort.  phasePort is
	// published only after C4 Prepare succeeds, so it is also the durable-owner
	// marker observed by cancellation.
	phasePrepareAdmission bool
	phasePrepareIO        bool
	phasePrepareDone      chan struct{}
	// cancelRequested is the linearization point shared by all four relay
	// callbacks.  Once it is set, no callback may begin a forward-side effect.
	// The two durable cancellation domains are mutually exclusive: legacy owns
	// every pre-C4 cancellation; C4 owns every successful durable Prepare.
	cancelRequested        bool
	legacyCancelInFlight   bool
	legacyCancelDone       chan struct{}
	legacyCancelReconciled bool
	phaseCancelInFlight    bool
	phaseCancelDone        chan struct{}
	phaseCancelReconciled  bool
}

type forwardLease struct{ state *operationState }

// acquireForward reserves the pre-call stage.  It serializes the three relay
// forward callbacks, so an unknown/reconciliation callback cannot overlap an
// authorization callback for the same prepared operation.
func (s *operationState) acquireForward(ctx context.Context) (*forwardLease, error) {
	for {
		s.mu.Lock()
		if s.cancelRequested {
			s.mu.Unlock()
			return nil, errSnapshotQueryCanceled
		}
		if !s.forwardAdmission && !s.forwardIO {
			s.forwardAdmission = true
			s.forwardDone = make(chan struct{})
			s.mu.Unlock()
			return &forwardLease{state: s}, nil
		}
		done := s.forwardDone
		s.mu.Unlock()
		select {
		case <-done:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// activate retains the historical pre-call checkpoint. beginCall below is the
// actual external-side-effect linearization point.
func (l *forwardLease) activate() error {
	s := l.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.forwardAdmission {
		return errors.New("sisnapshotquery: invalid forward lease")
	}
	if s.cancelRequested {
		s.forwardAdmission = false
		close(s.forwardDone)
		return errSnapshotQueryCanceled
	}
	return nil
}

// beginCall is the callback-to-port linearization point.  An admitted lease
// is still cancellable; only this final check grants the callback the right to
// begin an external call.  The call itself runs without state.mu held.
func (l *forwardLease) beginCall() error {
	s := l.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.forwardAdmission {
		return errors.New("sisnapshotquery: invalid forward lease")
	}
	if s.cancelRequested {
		s.forwardAdmission = false
		close(s.forwardDone)
		return errSnapshotQueryCanceled
	}
	s.forwardAdmission = false
	s.forwardIO = true
	return nil
}

// finish releases a started lease.  A successful unknown callback has
// already reconciled its selected durable domain, so a later cancellation is
// idempotently complete rather than issuing a second reconciliation.
func (l *forwardLease) finish(unknownReconciled bool, phaseDomain bool) {
	s := l.state
	s.mu.Lock()
	if unknownReconciled {
		if phaseDomain {
			s.phaseCancelReconciled = true
		} else {
			s.legacyCancelReconciled = true
		}
	}
	s.forwardIO = false
	close(s.forwardDone)
	s.mu.Unlock()
}

func (s *operationState) snapshot() (Operation, SnapshotQueryIntakePhasePort, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.value, s.phasePort, s.phaseIntent
}
func (s *operationState) replace(v Operation) { s.mu.Lock(); s.value = v; s.mu.Unlock() }

type phasePrepareLease struct{ state *operationState }

func (s *operationState) acquirePhasePrepare(ctx context.Context) (*phasePrepareLease, error) {
	for {
		s.mu.Lock()
		if s.cancelRequested {
			s.mu.Unlock()
			return nil, errSnapshotQueryCanceled
		}
		if !s.phasePrepareAdmission && !s.phasePrepareIO {
			s.phasePrepareAdmission = true
			s.phasePrepareDone = make(chan struct{})
			s.mu.Unlock()
			return &phasePrepareLease{state: s}, nil
		}
		done := s.phasePrepareDone
		s.mu.Unlock()
		select {
		case <-done:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (l *phasePrepareLease) beginCall() error {
	s := l.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.phasePrepareAdmission {
		return errors.New("sisnapshotquery: invalid phase prepare lease")
	}
	if s.cancelRequested {
		s.phasePrepareAdmission = false
		close(s.phasePrepareDone)
		return errSnapshotQueryCanceled
	}
	s.phasePrepareAdmission = false
	s.phasePrepareIO = true
	return nil
}

func (l *phasePrepareLease) finish(port SnapshotQueryIntakePhasePort, prepared bool) {
	s := l.state
	s.mu.Lock()
	if prepared {
		// A cancellation that races the I/O waits for this publication, and will
		// therefore select C4 exactly when durable Prepare succeeded.
		s.phasePort = port
	}
	s.phasePrepareIO = false
	close(s.phasePrepareDone)
	s.mu.Unlock()
}

type Options struct {
	Classifier      Classifier
	Reservations    ReservationPort
	Catalog         CatalogPort
	Analyzer        Analyzer
	ControlSigner   auth.SnapshotQueryControlSigner
	StatementSigner StatementSigner
	Sequence        Sequence
	Journal         Journal
	// PhasePortFactory is optional.  When injected, D1's four relay callbacks
	// delegate exactly to the C4 phase port after the worker has constructed and
	// signed the complete envelope.  It is deliberately not build/config wired.
	PhasePortFactory SnapshotQueryIntakePhasePortFactory
	// beforeForwardActivation is package-private test instrumentation for the
	// admitted-to-activated lease boundary.  Production callers cannot set it.
	beforeForwardActivation func()
	// The following are package-private deterministic test seams.  They sit
	// after admission and before beginCall, precisely where cancellation must
	// still prevent an external side effect.
	beforeForwardCall      func()
	beforePhaseFactoryCall func()
	beforePhasePrepareCall func()
	NetworkID              string
	KeeperShardID          uint32
	MaxControlBytes        uint64
}

type Plugin struct{ opts Options }

func (p *Plugin) activateForward(lease *forwardLease) error {
	if p.opts.beforeForwardActivation != nil {
		p.opts.beforeForwardActivation()
	}
	return lease.activate()
}

func New(opts Options) (*Plugin, error) {
	if opts.Classifier == nil || opts.Reservations == nil || opts.Catalog == nil || opts.Analyzer == nil || opts.ControlSigner == nil || opts.StatementSigner == nil || opts.Sequence == nil || opts.Journal == nil || strings.TrimSpace(opts.NetworkID) == "" || opts.MaxControlBytes == 0 {
		return nil, errors.New("sisnapshotquery: every port, network id, and control limit is required")
	}
	if opts.ControlSigner.Address() != opts.StatementSigner.Address() {
		return nil, errors.New("sisnapshotquery: control and statement signer identities differ")
	}
	return &Plugin{opts: opts}, nil
}

func (p *Plugin) OnQuery(_ context.Context, qctx *plugin.QueryContext) error {
	if p == nil || qctx == nil || qctx.Query == nil || qctx.Session == nil {
		return nil
	}
	if qctx.AgentPrepare != nil || qctx.DeferredInsert != nil || qctx.SuppressUpstreamExecution || qctx.AbortWithSuccess {
		return errors.New("sisnapshotquery: conflicting query ownership plan")
	}
	candidate, err := p.opts.Classifier.ClassifySnapshotQuery(qctx.Query.Body, logicalDatabase(qctx))
	if err != nil {
		return fmt.Errorf("sisnapshotquery: classify: %w", err)
	}
	if !candidate.Recognized {
		return nil
	}
	// Query parameters are a second executable input channel. The v3 envelope
	// binds only the analyzed SQL and the explicitly empty user-settings set,
	// so forwarding parameters here would make the prepared statement differ
	// from what the user signed.
	if len(qctx.Query.Parameters) != 0 {
		return errors.New("sisnapshotquery: snapshot query does not permit query parameters")
	}
	if len(qctx.Query.Settings) != 0 || len(qctx.Query.OldSettings) != 0 {
		return errors.New("sisnapshotquery: snapshot query requires an empty user settings set")
	}
	query := cloneQuery(*qctx.Query)
	// Nothing below
	// captures qctx, Session, a codec, or the live Query pointer.
	state := &operationState{value: Operation{RequestSeed: query.ID, OriginalQuery: query, Candidate: candidate}}
	qctx.AgentPrepare = &plugin.AgentPreparePlan{
		MaxControlBytes: p.opts.MaxControlBytes,
		Prepare:         func(ctx context.Context) (plugin.PreparedAgentQuery, error) { return p.prepare(ctx, state) },
		ReconcileCancel: func(ctx context.Context) error { return p.reconcileCancel(ctx, state) },
		PersistForwardIntent: func(ctx context.Context, prepared plugin.PreparedAgentQuery) error {
			return p.persistForwardIntent(ctx, state, prepared)
		},
		AuthorizeForward: func(ctx context.Context, prepared plugin.PreparedAgentQuery) error {
			op, _, _ := state.snapshot()
			if !matches(op, prepared) {
				return errors.New("sisnapshotquery: forward authorization identity mismatch")
			}
			lease, err := state.acquireForward(ctx)
			if err != nil {
				return err
			}
			if err := p.activateForward(lease); err != nil {
				return err
			}
			if p.opts.beforeForwardCall != nil {
				p.opts.beforeForwardCall()
			}
			if err := lease.beginCall(); err != nil {
				return err
			}
			op, phasePort, phaseIntent := state.snapshot()
			if phasePort == nil {
				err := p.opts.Journal.AuthorizeSnapshotQueryForward(ctx, op)
				lease.finish(false, false)
				return err
			}
			if !phaseIntent {
				lease.finish(false, true)
				return errors.New("sisnapshotquery: phase port authorization without submit intent")
			}
			err = phasePort.AuthorizeForward(ctx)
			lease.finish(false, true)
			return err
		},
		PersistForwardUnknown: func(ctx context.Context, prepared plugin.PreparedAgentQuery) error {
			op, _, _ := state.snapshot()
			if !matches(op, prepared) {
				return errors.New("sisnapshotquery: forward unknown identity mismatch")
			}
			lease, err := state.acquireForward(ctx)
			if err != nil {
				return err
			}
			if err := p.activateForward(lease); err != nil {
				return err
			}
			if p.opts.beforeForwardCall != nil {
				p.opts.beforeForwardCall()
			}
			if err := lease.beginCall(); err != nil {
				return err
			}
			op, phasePort, phaseIntent := state.snapshot()
			if phasePort == nil {
				err := p.opts.Journal.PersistSnapshotQueryForwardUnknown(ctx, op)
				// The legacy record only persists uncertainty; its corresponding
				// cancellation reconciliation is still the legacy cancel record.
				lease.finish(false, false)
				return err
			}
			if !phaseIntent {
				lease.finish(false, true)
				return errors.New("sisnapshotquery: phase port unknown authorization without submit intent")
			}
			err = phasePort.PersistAuthorizationUnknownAndReconcile(ctx)
			lease.finish(err == nil, true)
			return err
		},
	}
	return nil
}

var errSnapshotQueryCanceled = errors.New("sisnapshotquery: snapshot query cancellation is already latched")

// persistForwardIntent establishes C4 ownership only if cancellation has not
// already selected the legacy owner.  All I/O is deliberately outside the
// mutex; concurrent retries wait on the same durable attempt.
func (p *Plugin) persistForwardIntent(ctx context.Context, state *operationState, prepared plugin.PreparedAgentQuery) error {
	op, _, _ := state.snapshot()
	if !matches(op, prepared) {
		return errors.New("sisnapshotquery: forward intent identity mismatch")
	}
	lease, err := state.acquireForward(ctx)
	if err != nil {
		return err
	}
	if err := p.activateForward(lease); err != nil {
		return err
	}
	if p.opts.beforeForwardCall != nil {
		p.opts.beforeForwardCall()
	}
	if err := lease.beginCall(); err != nil {
		return err
	}
	op, phasePort, _ := state.snapshot()
	if phasePort == nil {
		err := p.opts.Journal.PersistSnapshotQueryForwardIntent(ctx, op)
		lease.finish(false, false)
		return err
	}
	state.mu.Lock()
	if state.phaseIntent {
		state.mu.Unlock()
		lease.finish(false, true)
		return nil
	}
	state.mu.Unlock()
	err = phasePort.PersistSubmitIntent(ctx)
	state.mu.Lock()
	if err == nil {
		state.phaseIntent = true
	}
	state.mu.Unlock()
	lease.finish(false, true)
	if err != nil {
		return err
	}
	if state.canceled() {
		// The durable intent won before cancellation latched; only C4 may
		// reconcile it.
		if err := p.reconcileCancel(ctx, state); err != nil {
			return err
		}
		return errSnapshotQueryCanceled
	}
	return nil
}

func (s *operationState) canceled() bool { s.mu.Lock(); defer s.mu.Unlock(); return s.cancelRequested }

// reconcileCancel latches cancellation before it selects an owner.  An
// activated forward lease is allowed to finish first; an admitted-but-not-yet
// activated lease observes the latch and never calls its external port.
func (p *Plugin) reconcileCancel(ctx context.Context, state *operationState) error {
	for {
		state.mu.Lock()
		op, phasePort := state.value, state.phasePort
		if op.RequestID == "" {
			state.mu.Unlock()
			return nil
		}
		state.cancelRequested = true
		if state.forwardIO {
			done := state.forwardDone
			state.mu.Unlock()
			select {
			case <-done:
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if state.phasePrepareIO {
			done := state.phasePrepareDone
			state.mu.Unlock()
			select {
			case <-done:
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		// A successful C4 Prepare has made a durable C4 record.  It owns every
		// later cancellation even before submit intent; falling back to legacy in
		// that state would leave the C4 record unreconciled.
		if phasePort == nil {
			if state.legacyCancelReconciled {
				state.mu.Unlock()
				return nil
			}
			if state.legacyCancelInFlight {
				done := state.legacyCancelDone
				state.mu.Unlock()
				select {
				case <-done:
					continue
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			state.legacyCancelInFlight = true
			state.legacyCancelDone = make(chan struct{})
			done := state.legacyCancelDone
			state.mu.Unlock()
			return finishLegacyCancel(ctx, state, p.opts.Journal, op, done)
		}
		if state.phaseCancelReconciled {
			state.mu.Unlock()
			return nil
		}
		if state.phaseCancelInFlight {
			done := state.phaseCancelDone
			state.mu.Unlock()
			select {
			case <-done:
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		state.phaseCancelInFlight = true
		state.phaseCancelDone = make(chan struct{})
		done := state.phaseCancelDone
		state.mu.Unlock()
		return finishPhaseCancel(ctx, state, phasePort, done)
	}
}

func finishLegacyCancel(ctx context.Context, state *operationState, journal Journal, op Operation, done chan struct{}) error {
	err := journal.ReconcileCanceledSnapshotQuery(ctx, op)
	state.mu.Lock()
	state.legacyCancelInFlight = false
	if err == nil {
		state.legacyCancelReconciled = true
	}
	close(done)
	state.mu.Unlock()
	return err
}

func finishPhaseCancel(ctx context.Context, state *operationState, phasePort SnapshotQueryIntakePhasePort, done chan struct{}) error {
	err := phasePort.CancelAndReconcile(ctx)
	state.mu.Lock()
	state.phaseCancelInFlight = false
	if err == nil {
		state.phaseCancelReconciled = true
	}
	close(done)
	state.mu.Unlock()
	return err
}

func (p *Plugin) prepare(ctx context.Context, state *operationState) (plugin.PreparedAgentQuery, error) {
	op, _, _ := state.snapshot()
	requestID, err := p.opts.Sequence.NextSnapshotQueryStatementID(ctx, op.RequestSeed)
	if err != nil {
		return plugin.PreparedAgentQuery{}, fmt.Errorf("sisnapshotquery: allocate request identity: %w", err)
	}
	if requestID == "" {
		return plugin.PreparedAgentQuery{}, errors.New("sisnapshotquery: empty request identity")
	}
	op.RequestID = requestID
	state.replace(op)
	if err := p.opts.Journal.BeginSnapshotQuery(ctx, op); err != nil {
		return plugin.PreparedAgentQuery{}, fmt.Errorf("sisnapshotquery: persist request: %w", err)
	}
	control := auth.SnapshotQueryControlBinding{Operation: auth.SnapshotQueryControlOperationAcquire, NetworkID: p.opts.NetworkID, KeeperShardID: p.opts.KeeperShardID, ClientAccount: p.opts.ControlSigner.Address(), StatementID: op.RequestID, RequestID: op.RequestID}
	token, err := p.opts.ControlSigner.SignSnapshotQueryControl(control)
	if err != nil {
		return plugin.PreparedAgentQuery{}, err
	}
	grant, err := p.opts.Reservations.AcquireSnapshotQuery(ctx, control, token)
	if err != nil {
		return plugin.PreparedAgentQuery{}, err
	}
	if grant.RequestID != op.RequestID || grant.Reservation.StatementID != op.RequestID || grant.Reservation.ClientAccount != p.opts.StatementSigner.Address() || grant.Reservation.FencingGeneration == 0 {
		return plugin.PreparedAgentQuery{}, errors.New("sisnapshotquery: reservation identity mismatch")
	}
	op.Grant = grant
	// A cancel may race catalog loading or analysis. Publish only the detached
	// grant identity now so reconciliation can conditionally release this exact
	// generation; no live query/session state is touched.
	state.replace(op)
	catalog, err := p.opts.Catalog.LoadSnapshotQueryCatalog(ctx, grant.Reservation)
	if err != nil {
		return plugin.PreparedAgentQuery{}, err
	}
	if catalog.ReadSet.ReadSnapshot != grant.Reservation.ReadSnapshot {
		return plugin.PreparedAgentQuery{}, errors.New("sisnapshotquery: catalog pin mismatch")
	}
	analysis, err := p.opts.Analyzer.PrepareSnapshotQuery(ctx, op.Candidate, catalog, grant)
	if err != nil {
		return plugin.PreparedAgentQuery{}, err
	}
	// Design D5: the statement commits the analyzer's complete relation
	// closure, never the whole pinned catalog. The catalog is only the pinned
	// source of each table's authenticated descriptor.
	readSet, err := signedReadSet(catalog.ReadSet, analysis.ReadTableIDs)
	if err != nil {
		return plugin.PreparedAgentQuery{}, err
	}
	input := replay.SnapshotQueryInput{Binding: replay.SnapshotQueryBinding{EnvelopeVersion: replay.SnapshotQueryEnvelopeVersion, InputKind: replay.SnapshotQueryInputKind, ClientAccount: p.opts.StatementSigner.Address(), StatementID: op.RequestID, StatementKind: replay.SnapshotQueryStatementKind, NetworkID: p.opts.NetworkID, KeeperShardID: p.opts.KeeperShardID, SQLHash: replay.DigestString(analysis.SQL), SettingsHash: sicore.EmptySettingsHash, TargetTableID: analysis.TargetTableID, SchemaHash: analysis.SchemaHash, RowIDProfileID: analysis.RowIDProfileID, ClientRevision: analysis.ClientRevision, ReadSnapshot: grant.Reservation.ReadSnapshot, SchemaSnapshotID: grant.Reservation.ReadSnapshot.SchemaSnapshotID, SchemaRoot: grant.Reservation.ReadSnapshot.SchemaRoot, LogicalDatabase: op.Candidate.LogicalDatabase, QueryProfileID: grant.Reservation.QueryProfileID, ExecutorProfileID: grant.Reservation.ExecutorProfileID, ReservationID: grant.Reservation.ReservationID, FencingGeneration: grant.Reservation.FencingGeneration}, SQL: analysis.SQL, ReadSet: readSet}
	input.Binding.ReadSetRoot, err = replay.SnapshotQueryReadSetRoot(input.ReadSet)
	if err != nil {
		return plugin.PreparedAgentQuery{}, err
	}
	root, err := replay.SnapshotQueryInputRoot(input)
	if err != nil {
		return plugin.PreparedAgentQuery{}, err
	}
	userJWS, err := p.opts.StatementSigner.SignStatementV3(auth.JWSStatementPayloadV3{Binding: input.Binding, InputRoot: root})
	if err != nil {
		return plugin.PreparedAgentQuery{}, err
	}
	op.Envelope = replay.SnapshotQueryEnvelope{Input: input, InputRoot: root, UserJWS: userJWS}
	state.replace(op)
	if p.opts.PhasePortFactory != nil {
		// The port sees only a fully computed and signed immutable envelope.  A
		// cancellation that wins before either C4 call remains legacy-owned and
		// prevents even factory creation.  phasePort is published only after the
		// durable Prepare call succeeds.
		factoryLease, err := state.acquirePhasePrepare(ctx)
		if err != nil {
			return plugin.PreparedAgentQuery{}, err
		}
		if p.opts.beforePhaseFactoryCall != nil {
			p.opts.beforePhaseFactoryCall()
		}
		if err := factoryLease.beginCall(); err != nil {
			return plugin.PreparedAgentQuery{}, err
		}
		phasePort, err := p.opts.PhasePortFactory(ctx, op.Envelope)
		factoryLease.finish(nil, false)
		if err != nil {
			return plugin.PreparedAgentQuery{}, fmt.Errorf("sisnapshotquery: create intake phase port: %w", err)
		}
		if phasePort == nil {
			return plugin.PreparedAgentQuery{}, errors.New("sisnapshotquery: nil intake phase port")
		}
		prepareLease, err := state.acquirePhasePrepare(ctx)
		if err != nil {
			return plugin.PreparedAgentQuery{}, err
		}
		if p.opts.beforePhasePrepareCall != nil {
			p.opts.beforePhasePrepareCall()
		}
		if err := prepareLease.beginCall(); err != nil {
			return plugin.PreparedAgentQuery{}, err
		}
		err = phasePort.Prepare(ctx)
		prepareLease.finish(phasePort, err == nil)
		if err != nil {
			return plugin.PreparedAgentQuery{}, fmt.Errorf("sisnapshotquery: prepare intake phase port: %w", err)
		}
	}
	prepared := cloneQuery(op.OriginalQuery)
	prepared.ID = op.RequestID
	prepared.Body = analysis.SQL
	prepared.Settings = append(prepared.Settings, chproto.Setting{Key: auth.StatementTokenSettingKey, Value: "'" + userJWS + "'", Custom: true})
	return plugin.PreparedAgentQuery{Query: &prepared, Claimed: true}, nil
}

func (p *Plugin) persistIntent(ctx context.Context, op Operation, prepared plugin.PreparedAgentQuery) error {
	if !matches(op, prepared) {
		return errors.New("sisnapshotquery: forward intent identity mismatch")
	}
	return p.opts.Journal.PersistSnapshotQueryForwardIntent(ctx, op)
}
func (p *Plugin) authorize(ctx context.Context, op Operation, prepared plugin.PreparedAgentQuery) error {
	if !matches(op, prepared) {
		return errors.New("sisnapshotquery: forward authorization identity mismatch")
	}
	return p.opts.Journal.AuthorizeSnapshotQueryForward(ctx, op)
}
func (p *Plugin) unknown(ctx context.Context, op Operation, prepared plugin.PreparedAgentQuery) error {
	if !matches(op, prepared) {
		return errors.New("sisnapshotquery: forward unknown identity mismatch")
	}
	return p.opts.Journal.PersistSnapshotQueryForwardUnknown(ctx, op)
}
func matches(op Operation, p plugin.PreparedAgentQuery) bool {
	if !p.Claimed || p.Query == nil || op.RequestID == "" || op.Envelope.UserJWS == "" || op.Envelope.InputRoot == "" || op.Envelope.Input.Binding.StatementID != op.RequestID || op.Envelope.Input.Binding.ReservationID != op.Grant.Reservation.ReservationID || op.Envelope.Input.Binding.FencingGeneration != op.Grant.Reservation.FencingGeneration {
		return false
	}
	// Callbacks may authorize a socket write. Recompute the canonical root
	// rather than treating the cached input_root as an opaque marker, so an
	// in-memory mutation cannot switch the signed statement it refers to.
	root, err := replay.SnapshotQueryInputRoot(op.Envelope.Input)
	if err != nil || root != op.Envelope.InputRoot {
		return false
	}
	// Every callback may authorize a socket write.  Its query must therefore be
	// byte-for-byte equivalent at the Query-object level to the only prepared
	// canonical execution input.  This rejects injected Parameters, legacy
	// settings, duplicate tokens, and any altered setting flags.
	canonical := cloneQuery(op.OriginalQuery)
	canonical.ID = op.RequestID
	canonical.Body = op.Envelope.Input.SQL
	canonical.Settings = append(canonical.Settings, chproto.Setting{Key: auth.StatementTokenSettingKey, Value: "'" + op.Envelope.UserJWS + "'", Custom: true})
	return reflect.DeepEqual(*p.Query, canonical)
}
func logicalDatabase(q *plugin.QueryContext) string {
	if s := q.Session.State(); s != nil {
		if d := s.LogicalDatabaseName(); d != "" {
			return d
		}
		return s.PhysicalDatabaseName()
	}
	return ""
}
func cloneQuery(q chproto.Query) chproto.Query {
	q.Settings = append([]chproto.Setting{}, q.Settings...)
	q.Parameters = append([]chgo.Parameter{}, q.Parameters...)
	return q
}

var _ plugin.QueryPlugin = (*Plugin)(nil)
