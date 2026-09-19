// Package sisnapshotquery implements the default-disabled agent snapshot-query
// preparation lane. It deliberately owns no sockets, relay state, or runtime
// wiring; callers inject every durable and remote dependency.
package sisnapshotquery

import (
	"context"
	"errors"
	"fmt"
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
}
type Analyzer interface {
	PrepareSnapshotQuery(context.Context, Candidate, Catalog, Grant) (Analysis, error)
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
	mu          sync.RWMutex
	value       Operation
	phasePort   SnapshotQueryIntakePhasePort
	phaseIntent bool
}

func (s *operationState) snapshot() (Operation, SnapshotQueryIntakePhasePort, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.value, s.phasePort, s.phaseIntent
}
func (s *operationState) replace(v Operation) { s.mu.Lock(); s.value = v; s.mu.Unlock() }
func (s *operationState) installPhasePort(port SnapshotQueryIntakePhasePort) {
	s.mu.Lock()
	s.phasePort = port
	s.mu.Unlock()
}
func (s *operationState) markPhaseIntent() {
	s.mu.Lock()
	s.phaseIntent = true
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
	NetworkID        string
	KeeperShardID    uint32
	MaxControlBytes  uint64
}

type Plugin struct{ opts Options }

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
		ReconcileCancel: func(ctx context.Context) error {
			op, phasePort, phaseIntent := state.snapshot()
			if op.RequestID == "" {
				return nil
			}
			// C4 cancellation starts only after its durable SubmitIntent exists.
			// Before that the D1 journal remains the exact-reservation cleanup
			// owner, including a cancel racing envelope/port construction.
			if phasePort != nil && phaseIntent {
				return phasePort.CancelAndReconcile(ctx)
			}
			return p.opts.Journal.ReconcileCanceledSnapshotQuery(ctx, op)
		},
		PersistForwardIntent: func(ctx context.Context, prepared plugin.PreparedAgentQuery) error {
			op, phasePort, _ := state.snapshot()
			if !matches(op, prepared) {
				return errors.New("sisnapshotquery: forward intent identity mismatch")
			}
			if phasePort == nil {
				return p.opts.Journal.PersistSnapshotQueryForwardIntent(ctx, op)
			}
			if err := phasePort.PersistSubmitIntent(ctx); err != nil {
				return err
			}
			state.markPhaseIntent()
			return nil
		},
		AuthorizeForward: func(ctx context.Context, prepared plugin.PreparedAgentQuery) error {
			op, phasePort, phaseIntent := state.snapshot()
			if !matches(op, prepared) {
				return errors.New("sisnapshotquery: forward authorization identity mismatch")
			}
			if phasePort == nil {
				return p.opts.Journal.AuthorizeSnapshotQueryForward(ctx, op)
			}
			if !phaseIntent {
				return errors.New("sisnapshotquery: phase port authorization without submit intent")
			}
			return phasePort.AuthorizeSubmit(ctx)
		},
		PersistForwardUnknown: func(ctx context.Context, prepared plugin.PreparedAgentQuery) error {
			op, phasePort, phaseIntent := state.snapshot()
			if !matches(op, prepared) {
				return errors.New("sisnapshotquery: forward unknown identity mismatch")
			}
			if phasePort == nil {
				return p.opts.Journal.PersistSnapshotQueryForwardUnknown(ctx, op)
			}
			if !phaseIntent {
				return errors.New("sisnapshotquery: phase port unknown authorization without submit intent")
			}
			return phasePort.PersistAuthorizationUnknownAndReconcile(ctx)
		},
	}
	return nil
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
	input := replay.SnapshotQueryInput{Binding: replay.SnapshotQueryBinding{EnvelopeVersion: replay.SnapshotQueryEnvelopeVersion, InputKind: replay.SnapshotQueryInputKind, ClientAccount: p.opts.StatementSigner.Address(), StatementID: op.RequestID, StatementKind: replay.SnapshotQueryStatementKind, NetworkID: p.opts.NetworkID, KeeperShardID: p.opts.KeeperShardID, SQLHash: replay.DigestString(analysis.SQL), SettingsHash: sicore.EmptySettingsHash, TargetTableID: analysis.TargetTableID, SchemaHash: analysis.SchemaHash, RowIDProfileID: analysis.RowIDProfileID, ClientRevision: analysis.ClientRevision, ReadSnapshot: grant.Reservation.ReadSnapshot, SchemaSnapshotID: grant.Reservation.ReadSnapshot.SchemaSnapshotID, SchemaRoot: grant.Reservation.ReadSnapshot.SchemaRoot, LogicalDatabase: op.Candidate.LogicalDatabase, QueryProfileID: grant.Reservation.QueryProfileID, ExecutorProfileID: grant.Reservation.ExecutorProfileID, ReservationID: grant.Reservation.ReservationID, FencingGeneration: grant.Reservation.FencingGeneration}, SQL: analysis.SQL, ReadSet: catalog.ReadSet}
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
		// The port sees only a fully computed and signed immutable envelope.  It
		// is retained in operation state before Prepare so a cancellation racing
		// the C4 durable setup still has an exact operation identity to reconcile.
		phasePort, err := p.opts.PhasePortFactory(ctx, op.Envelope)
		if err != nil {
			return plugin.PreparedAgentQuery{}, fmt.Errorf("sisnapshotquery: create intake phase port: %w", err)
		}
		if phasePort == nil {
			return plugin.PreparedAgentQuery{}, errors.New("sisnapshotquery: nil intake phase port")
		}
		state.installPhasePort(phasePort)
		if err := phasePort.Prepare(ctx); err != nil {
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
	if p.Query == nil || op.RequestID == "" || op.Envelope.UserJWS == "" || p.Query.ID != op.RequestID || p.Query.Body != op.Envelope.Input.SQL || op.Envelope.InputRoot == "" || op.Envelope.Input.Binding.StatementID != op.RequestID || op.Envelope.Input.Binding.ReservationID != op.Grant.Reservation.ReservationID || op.Envelope.Input.Binding.FencingGeneration != op.Grant.Reservation.FencingGeneration {
		return false
	}
	// Callbacks may authorize a socket write. Recompute the canonical root
	// rather than treating the cached input_root as an opaque marker, so an
	// in-memory mutation cannot switch the signed statement it refers to.
	root, err := replay.SnapshotQueryInputRoot(op.Envelope.Input)
	if err != nil || root != op.Envelope.InputRoot {
		return false
	}
	var token string
	for _, setting := range p.Query.Settings {
		if setting.Key == auth.StatementTokenSettingKey {
			token = setting.Value
		}
	}
	return token == "'"+op.Envelope.UserJWS+"'"
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
