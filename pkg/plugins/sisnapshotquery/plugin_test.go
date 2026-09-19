package sisnapshotquery

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/plugin"
	"github.com/housegate/housegate/pkg/replay"
)

const testKey = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

type fakeReservations struct {
	grant Grant
	got   auth.SnapshotQueryControlBinding
	token string
	calls int
}

func (f *fakeReservations) AcquireSnapshotQuery(_ context.Context, b auth.SnapshotQueryControlBinding, token string) (Grant, error) {
	f.calls++
	f.got, f.token = b, token
	return f.grant, nil
}

type fakeCatalog struct {
	value Catalog
	got   replay.SnapshotQueryReservation
	calls int
}

func (f *fakeCatalog) LoadSnapshotQueryCatalog(_ context.Context, r replay.SnapshotQueryReservation) (Catalog, error) {
	f.calls++
	f.got = r
	return f.value, nil
}

type fakeAnalyzer struct {
	value Analysis
	got   Candidate
	calls int
}

func (f *fakeAnalyzer) PrepareSnapshotQuery(_ context.Context, c Candidate, _ Catalog, _ Grant) (Analysis, error) {
	f.calls++
	f.got = c
	return f.value, nil
}

type fakeSequence struct {
	id, seed string
	calls    int
}

func (f *fakeSequence) NextSnapshotQueryStatementID(_ context.Context, seed string) (string, error) {
	f.calls++
	f.seed = seed
	return f.id, nil
}

type fakeClassifier struct {
	candidate Candidate
	calls     int
}

func (f *fakeClassifier) ClassifySnapshotQuery(_, _ string) (Candidate, error) {
	f.calls++
	return f.candidate, nil
}

type fakeJournal struct {
	calls                                        []string
	begun, canceled, intent, authorized, unknown []Operation
}

// fakePhasePort deliberately has no submission method.  It proves the bridge
// can only invoke the four C4 durable/reconciliation callbacks.
type fakePhasePort struct {
	events []string
	errs   map[string]error
}

// blockingPhasePort makes the intent/cancel hand-off deterministic.  It is
// intentionally separate from fakePhasePort because the concurrent test is
// also run under -race.
type blockingPhasePort struct {
	mu            sync.Mutex
	events        []string
	intentStarted chan struct{}
	releaseIntent chan struct{}
}

// cancelGatePhasePort lets a test hold C4 reconciliation while it attempts a
// forward authorization.  No method is called while its mutex is held.
type cancelGatePhasePort struct {
	mu            sync.Mutex
	events        []string
	cancelStarted chan struct{}
	releaseCancel chan struct{}
}

func (p *cancelGatePhasePort) record(name string) {
	p.mu.Lock()
	p.events = append(p.events, name)
	p.mu.Unlock()
}
func (p *cancelGatePhasePort) snapshotEvents() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.events...)
}
func (p *cancelGatePhasePort) Prepare(context.Context) error { p.record("prepare"); return nil }
func (p *cancelGatePhasePort) PersistSubmitIntent(context.Context) error {
	p.record("intent")
	return nil
}
func (p *cancelGatePhasePort) AuthorizeSubmit(context.Context) error {
	p.record("authorize")
	return nil
}
func (p *cancelGatePhasePort) CancelAndReconcile(context.Context) error {
	p.record("cancel")
	close(p.cancelStarted)
	<-p.releaseCancel
	return nil
}
func (p *cancelGatePhasePort) PersistAuthorizationUnknownAndReconcile(context.Context) error {
	p.record("unknown")
	return nil
}

type blockingCancelJournal struct {
	mu      sync.Mutex
	calls   int
	started chan struct{}
	release chan struct{}
}

func (j *blockingCancelJournal) BeginSnapshotQuery(context.Context, Operation) error { return nil }
func (j *blockingCancelJournal) ReconcileCanceledSnapshotQuery(context.Context, Operation) error {
	j.mu.Lock()
	j.calls++
	j.mu.Unlock()
	close(j.started)
	<-j.release
	return nil
}
func (j *blockingCancelJournal) PersistSnapshotQueryForwardIntent(context.Context, Operation) error {
	return nil
}
func (j *blockingCancelJournal) AuthorizeSnapshotQueryForward(context.Context, Operation) error {
	return nil
}
func (j *blockingCancelJournal) PersistSnapshotQueryForwardUnknown(context.Context, Operation) error {
	return nil
}
func (j *blockingCancelJournal) count() int { j.mu.Lock(); defer j.mu.Unlock(); return j.calls }

func (p *blockingPhasePort) record(name string) {
	p.mu.Lock()
	p.events = append(p.events, name)
	p.mu.Unlock()
}
func (p *blockingPhasePort) snapshotEvents() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.events...)
}
func (p *blockingPhasePort) Prepare(context.Context) error { p.record("prepare"); return nil }
func (p *blockingPhasePort) PersistSubmitIntent(context.Context) error {
	p.record("intent")
	close(p.intentStarted)
	<-p.releaseIntent
	return nil
}
func (p *blockingPhasePort) AuthorizeSubmit(context.Context) error { p.record("authorize"); return nil }
func (p *blockingPhasePort) CancelAndReconcile(context.Context) error {
	p.record("cancel")
	return nil
}
func (p *blockingPhasePort) PersistAuthorizationUnknownAndReconcile(context.Context) error {
	p.record("unknown")
	return nil
}

func (p *fakePhasePort) call(name string) error {
	p.events = append(p.events, name)
	return p.errs[name]
}
func (p *fakePhasePort) Prepare(context.Context) error { return p.call("prepare") }
func (p *fakePhasePort) PersistSubmitIntent(context.Context) error {
	return p.call("intent")
}
func (p *fakePhasePort) AuthorizeSubmit(context.Context) error { return p.call("authorize") }
func (p *fakePhasePort) CancelAndReconcile(context.Context) error {
	return p.call("cancel")
}
func (p *fakePhasePort) PersistAuthorizationUnknownAndReconcile(context.Context) error {
	return p.call("unknown")
}

func (f *fakeJournal) BeginSnapshotQuery(_ context.Context, op Operation) error {
	f.calls = append(f.calls, "begin")
	f.begun = append(f.begun, op)
	return nil
}
func (f *fakeJournal) ReconcileCanceledSnapshotQuery(_ context.Context, op Operation) error {
	f.calls = append(f.calls, "cancel")
	f.canceled = append(f.canceled, op)
	return nil
}
func (f *fakeJournal) PersistSnapshotQueryForwardIntent(_ context.Context, op Operation) error {
	f.calls = append(f.calls, "intent")
	f.intent = append(f.intent, op)
	return nil
}
func (f *fakeJournal) AuthorizeSnapshotQueryForward(_ context.Context, op Operation) error {
	f.calls = append(f.calls, "authorize")
	f.authorized = append(f.authorized, op)
	return nil
}
func (f *fakeJournal) PersistSnapshotQueryForwardUnknown(_ context.Context, op Operation) error {
	f.calls = append(f.calls, "unknown")
	f.unknown = append(f.unknown, op)
	return nil
}

type fixture struct {
	t            *testing.T
	signer       *auth.RelaySigner
	classifier   *fakeClassifier
	reservations *fakeReservations
	catalog      *fakeCatalog
	analyzer     *fakeAnalyzer
	sequence     *fakeSequence
	journal      *fakeJournal
	p            *Plugin
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	signer, err := auth.NewRelaySigner(testKey)
	if err != nil {
		t.Fatal(err)
	}
	pin := replay.SnapshotPin{NetworkID: "net", KeeperShardID: 7, SnapshotID: "snapshot", SafeBlockSeq: 8, ManifestRoot: "manifest", StateRoot: "state", SchemaSnapshotID: "schema-snapshot", SchemaRoot: "schema-root"}
	r := replay.SnapshotQueryReservation{ReservationID: "reservation", FencingGeneration: 4, ClientAccount: signer.Address(), StatementID: signer.Address() + ":1:nonce", ReadSnapshot: pin, ExecutorProfileID: "executor", QueryProfileID: "query", ActivationID: "activation"}
	f := &fixture{t: t, signer: signer, classifier: &fakeClassifier{candidate: Candidate{Recognized: true, LogicalDatabase: "logical"}}, reservations: &fakeReservations{grant: Grant{RequestID: r.StatementID, BlockSeq: 9, Reservation: r}}, catalog: &fakeCatalog{value: Catalog{ReadSet: replay.SnapshotReadSet{ReadSnapshot: pin}}}, analyzer: &fakeAnalyzer{value: Analysis{SQL: "INSERT INTO physical SELECT 1", TargetTableID: "target", SchemaHash: "schema-hash", RowIDProfileID: "row-profile", ClientRevision: 54470}}, sequence: &fakeSequence{id: r.StatementID}, journal: &fakeJournal{}}
	f.p, err = New(Options{Classifier: f.classifier, Reservations: f.reservations, Catalog: f.catalog, Analyzer: f.analyzer, ControlSigner: signer, StatementSigner: signer, Sequence: f.sequence, Journal: f.journal, NetworkID: "net", KeeperShardID: 7, MaxControlBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	return f
}
func (f *fixture) install() *plugin.AgentPreparePlan {
	f.t.Helper()
	s := chsession.New(1, nil)
	s.State().SetLogicalDatabase("logical")
	q := &plugin.QueryContext{Session: s, Query: &chproto.Query{ID: "client-id", Body: "INSERT INTO logical SELECT 1"}, Values: map[string]any{}}
	if err := f.p.OnQuery(context.Background(), q); err != nil {
		f.t.Fatal(err)
	}
	if q.AgentPrepare == nil {
		f.t.Fatal("recognized query did not install plan")
	}
	if f.sequence.calls != 0 || f.reservations.calls != 0 || len(f.journal.calls) != 0 {
		f.t.Fatal("OnQuery called worker port")
	}
	if _, ok := q.Values[plugin.SnapshotQueryAgentKey]; ok {
		f.t.Fatal("plugin installed relay-only marker")
	}
	return q.AgentPrepare
}

func TestNewFailsClosedWithoutEveryPort(t *testing.T) {
	if _, err := New(Options{}); err == nil {
		t.Fatal("New accepted missing dependencies")
	}
}

func TestPrepareBuildsDetachedSignedOperationAndForwardOrder(t *testing.T) {
	f := newFixture(t)
	plan := f.install()
	prepared, err := plan.Prepare(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Query == nil || !prepared.Claimed {
		t.Fatal("preparation did not claim detached query")
	}
	if prepared.Query.ID != f.sequence.id || prepared.Query.Body != f.analyzer.value.SQL {
		t.Fatalf("prepared=%#v", prepared.Query)
	}
	if f.reservations.got.StatementID != f.sequence.id || f.reservations.got.RequestID != f.sequence.id || f.reservations.got.Operation != auth.SnapshotQueryControlOperationAcquire {
		t.Fatalf("control=%#v", f.reservations.got)
	}
	if _, err := auth.DecodeSnapshotQueryControlPayload(f.reservations.token); err != nil {
		t.Fatal(err)
	}
	token := setting(prepared.Query, auth.StatementTokenSettingKey)
	payload, err := auth.DecodeStatementV3Payload(strings.Trim(token, "'"))
	if err != nil {
		t.Fatal(err)
	}
	if payload.InputRoot == "" || payload.Binding.ReadSetRoot == "" || payload.Binding.ReservationID != "reservation" || payload.Binding.FencingGeneration != 4 {
		t.Fatalf("binding=%#v", payload.Binding)
	}
	if got, err := auth.VerifyStatementV3Signature(strings.Trim(token, "'"), auth.JWSStatementPayloadV3{Purpose: auth.StatementPurposeV3, Binding: payload.Binding, InputRoot: payload.InputRoot}); err != nil || got != f.signer.Address() {
		t.Fatalf("verify=%q %v", got, err)
	}
	if err := plan.PersistForwardIntent(context.Background(), prepared); err != nil {
		t.Fatal(err)
	}
	if err := plan.AuthorizeForward(context.Background(), prepared); err != nil {
		t.Fatal(err)
	}
	if err := plan.PersistForwardUnknown(context.Background(), prepared); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(f.journal.calls, ","); got != "begin,intent,authorize,unknown" {
		t.Fatalf("order=%q", got)
	}
}

func TestPrepareBridgesSignedEnvelopeToC4PhasePort(t *testing.T) {
	f := newFixture(t)
	phase := &fakePhasePort{}
	var got replay.SnapshotQueryEnvelope
	f.p.opts.PhasePortFactory = func(_ context.Context, env replay.SnapshotQueryEnvelope) (SnapshotQueryIntakePhasePort, error) {
		got = env
		return phase, nil
	}
	plan := f.install()
	prepared, err := plan.Prepare(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.InputRoot == "" || got.UserJWS == "" || got.Input.Binding.StatementID != prepared.Query.ID {
		t.Fatalf("phase port received incomplete envelope: %#v", got)
	}
	if err := plan.PersistForwardIntent(context.Background(), prepared); err != nil {
		t.Fatal(err)
	}
	if err := plan.AuthorizeForward(context.Background(), prepared); err != nil {
		t.Fatal(err)
	}
	if err := plan.PersistForwardUnknown(context.Background(), prepared); err != nil {
		t.Fatal(err)
	}
	if order := strings.Join(phase.events, ","); order != "prepare,intent,authorize,unknown" {
		t.Fatalf("phase order=%q", order)
	}
	// Begin remains D1's pre-envelope reservation journal. Once C4 is present,
	// all four relay phases belong to the port, not the old forward journal.
	if order := strings.Join(f.journal.calls, ","); order != "begin" {
		t.Fatalf("journal order=%q", order)
	}
}

func TestC4PhaseBridgeCancelBeforePortAndAfterIntent(t *testing.T) {
	t.Run("before port", func(t *testing.T) {
		f := newFixture(t)
		f.p.opts.PhasePortFactory = func(context.Context, replay.SnapshotQueryEnvelope) (SnapshotQueryIntakePhasePort, error) {
			return nil, errors.New("port unavailable")
		}
		plan := f.install()
		if _, err := plan.Prepare(context.Background()); err == nil {
			t.Fatal("port construction failure prepared query")
		}
		if err := plan.ReconcileCancel(context.Background()); err != nil {
			t.Fatal(err)
		}
		if order := strings.Join(f.journal.calls, ","); order != "begin,cancel" {
			t.Fatalf("journal order=%q", order)
		}
	})
	t.Run("after port before intent", func(t *testing.T) {
		f := newFixture(t)
		phase := &fakePhasePort{}
		f.p.opts.PhasePortFactory = func(context.Context, replay.SnapshotQueryEnvelope) (SnapshotQueryIntakePhasePort, error) {
			return phase, nil
		}
		plan := f.install()
		if _, err := plan.Prepare(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := plan.ReconcileCancel(context.Background()); err != nil {
			t.Fatal(err)
		}
		if order := strings.Join(phase.events, ","); order != "prepare" {
			t.Fatalf("phase order=%q", order)
		}
		if order := strings.Join(f.journal.calls, ","); order != "begin,cancel" {
			t.Fatalf("journal order=%q", order)
		}
	})
	t.Run("after intent", func(t *testing.T) {
		f := newFixture(t)
		phase := &fakePhasePort{}
		f.p.opts.PhasePortFactory = func(context.Context, replay.SnapshotQueryEnvelope) (SnapshotQueryIntakePhasePort, error) {
			return phase, nil
		}
		plan := f.install()
		prepared, err := plan.Prepare(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if err := plan.PersistForwardIntent(context.Background(), prepared); err != nil {
			t.Fatal(err)
		}
		if err := plan.ReconcileCancel(context.Background()); err != nil {
			t.Fatal(err)
		}
		if order := strings.Join(phase.events, ","); order != "prepare,intent,cancel" {
			t.Fatalf("phase order=%q", order)
		}
		if order := strings.Join(f.journal.calls, ","); order != "begin" {
			t.Fatalf("journal order=%q", order)
		}
	})
}

func TestC4PhaseBridgeLinearizesIntentAndCancel(t *testing.T) {
	f := newFixture(t)
	phase := &blockingPhasePort{intentStarted: make(chan struct{}), releaseIntent: make(chan struct{})}
	f.p.opts.PhasePortFactory = func(context.Context, replay.SnapshotQueryEnvelope) (SnapshotQueryIntakePhasePort, error) {
		return phase, nil
	}
	plan := f.install()
	prepared, err := plan.Prepare(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	intentDone := make(chan error, 1)
	go func() { intentDone <- plan.PersistForwardIntent(context.Background(), prepared) }()
	<-phase.intentStarted
	cancelDone := make(chan error, 1)
	go func() { cancelDone <- plan.ReconcileCancel(context.Background()) }()
	// PersistSubmitIntent has entered C4 but has not returned. Releasing it
	// resolves the in-flight result that cancellation waits on before choosing
	// C4 versus the legacy journal.
	close(phase.releaseIntent)
	if err := <-intentDone; err != nil {
		t.Fatalf("intent: %v", err)
	}
	if err := <-cancelDone; err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if got := strings.Join(phase.snapshotEvents(), ","); got != "prepare,intent,cancel" {
		t.Fatalf("phase order=%q", got)
	}
	if got := strings.Join(f.journal.calls, ","); got != "begin" {
		t.Fatalf("cancel escaped to legacy journal: %q", got)
	}
}

func TestC4PhaseBridgePreIntentCancelOwnsLegacyAndRejectsLaterIntent(t *testing.T) {
	f := newFixture(t)
	phase := &fakePhasePort{}
	f.p.opts.PhasePortFactory = func(context.Context, replay.SnapshotQueryEnvelope) (SnapshotQueryIntakePhasePort, error) {
		return phase, nil
	}
	plan := f.install()
	prepared, err := plan.Prepare(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := plan.ReconcileCancel(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := plan.PersistForwardIntent(context.Background(), prepared); !errors.Is(err, errSnapshotQueryCanceled) {
		t.Fatalf("later intent err=%v", err)
	}
	if got := strings.Join(phase.events, ","); got != "prepare" {
		t.Fatalf("legacy cancel allowed C4 intent: %q", got)
	}
	if got := strings.Join(f.journal.calls, ","); got != "begin,cancel" {
		t.Fatalf("legacy ownership changed: %q", got)
	}
}

func TestC4PhaseBridgeCancelLatchRejectsAuthorizeDuringPhaseCancel(t *testing.T) {
	f := newFixture(t)
	phase := &cancelGatePhasePort{cancelStarted: make(chan struct{}), releaseCancel: make(chan struct{})}
	f.p.opts.PhasePortFactory = func(context.Context, replay.SnapshotQueryEnvelope) (SnapshotQueryIntakePhasePort, error) {
		return phase, nil
	}
	plan := f.install()
	prepared, err := plan.Prepare(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := plan.PersistForwardIntent(context.Background(), prepared); err != nil {
		t.Fatal(err)
	}
	canceled := make(chan error, 1)
	go func() { canceled <- plan.ReconcileCancel(context.Background()) }()
	<-phase.cancelStarted
	if err := plan.AuthorizeForward(context.Background(), prepared); !errors.Is(err, errSnapshotQueryCanceled) {
		t.Fatalf("authorize while cancellation is in flight: %v", err)
	}
	if got := strings.Join(phase.snapshotEvents(), ","); got != "prepare,intent,cancel" {
		t.Fatalf("authorize reached C4 while cancel was latched: %q", got)
	}
	close(phase.releaseCancel)
	if err := <-canceled; err != nil {
		t.Fatal(err)
	}
}

func TestC4PhaseBridgeLeaseGateLetsCancellationWinBeforeExternalForwardCall(t *testing.T) {
	for name, callback := range map[string]func(*plugin.AgentPreparePlan, plugin.PreparedAgentQuery) error{
		"authorize": func(plan *plugin.AgentPreparePlan, prepared plugin.PreparedAgentQuery) error {
			return plan.AuthorizeForward(context.Background(), prepared)
		},
		"unknown": func(plan *plugin.AgentPreparePlan, prepared plugin.PreparedAgentQuery) error {
			return plan.PersistForwardUnknown(context.Background(), prepared)
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			phase := &fakePhasePort{}
			f.p.opts.PhasePortFactory = func(context.Context, replay.SnapshotQueryEnvelope) (SnapshotQueryIntakePhasePort, error) {
				return phase, nil
			}
			plan := f.install()
			prepared, err := plan.Prepare(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if err := plan.PersistForwardIntent(context.Background(), prepared); err != nil {
				t.Fatal(err)
			}
			entered, release := make(chan struct{}), make(chan struct{})
			f.p.opts.beforeForwardActivation = func() { close(entered); <-release }
			result := make(chan error, 1)
			go func() { result <- callback(plan, prepared) }()
			<-entered
			// The callback has passed identity validation but has not activated its
			// I/O lease. Cancellation latches and reconciles first; releasing the
			// gate must therefore not invoke the phase forward method.
			if err := plan.ReconcileCancel(context.Background()); err != nil {
				t.Fatal(err)
			}
			close(release)
			if err := <-result; !errors.Is(err, errSnapshotQueryCanceled) {
				t.Fatalf("callback err=%v", err)
			}
			if got := strings.Join(phase.events, ","); got != "prepare,intent,cancel" {
				t.Fatalf("cancellation leaked a %s call: %q", name, got)
			}
		})
	}

	t.Run("legacy intent", func(t *testing.T) {
		f := newFixture(t)
		plan := f.install()
		prepared, err := plan.Prepare(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		entered, release := make(chan struct{}), make(chan struct{})
		f.p.opts.beforeForwardActivation = func() { close(entered); <-release }
		result := make(chan error, 1)
		go func() { result <- plan.PersistForwardIntent(context.Background(), prepared) }()
		<-entered
		if err := plan.ReconcileCancel(context.Background()); err != nil {
			t.Fatal(err)
		}
		close(release)
		if err := <-result; !errors.Is(err, errSnapshotQueryCanceled) {
			t.Fatalf("intent err=%v", err)
		}
		if got := strings.Join(f.journal.calls, ","); got != "begin,cancel" {
			t.Fatalf("cancellation leaked a legacy intent: %q", got)
		}
	})
}

func TestC4PhaseBridgeDoublePreIntentCancelHasOneLegacyOwner(t *testing.T) {
	f := newFixture(t)
	phase := &fakePhasePort{}
	f.p.opts.PhasePortFactory = func(context.Context, replay.SnapshotQueryEnvelope) (SnapshotQueryIntakePhasePort, error) {
		return phase, nil
	}
	plan := f.install()
	prepared, err := plan.Prepare(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	journal := &blockingCancelJournal{started: make(chan struct{}), release: make(chan struct{})}
	f.p.opts.Journal = journal
	first := make(chan error, 1)
	second := make(chan error, 1)
	go func() { first <- plan.ReconcileCancel(context.Background()) }()
	<-journal.started
	go func() { second <- plan.ReconcileCancel(context.Background()) }()
	if got := journal.count(); got != 1 {
		t.Fatalf("legacy cancel calls=%d", got)
	}
	close(journal.release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if err := <-second; err != nil {
		t.Fatal(err)
	}
	if err := plan.PersistForwardIntent(context.Background(), prepared); !errors.Is(err, errSnapshotQueryCanceled) {
		t.Fatalf("intent after legacy cancellation: %v", err)
	}
	if got := strings.Join(phase.events, ","); got != "prepare" {
		t.Fatalf("legacy cancellation crossed into C4: %q", got)
	}
}

func TestC4PhaseBridgeIntentCancelRaceHasNoDuplicateOrForward(t *testing.T) {
	for i := 0; i != 20; i++ {
		f := newFixture(t)
		phase := &blockingPhasePort{intentStarted: make(chan struct{}), releaseIntent: make(chan struct{})}
		f.p.opts.PhasePortFactory = func(context.Context, replay.SnapshotQueryEnvelope) (SnapshotQueryIntakePhasePort, error) {
			return phase, nil
		}
		plan := f.install()
		prepared, err := plan.Prepare(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		intent := make(chan error, 1)
		go func() { intent <- plan.PersistForwardIntent(context.Background(), prepared) }()
		<-phase.intentStarted
		one, two := make(chan error, 1), make(chan error, 1)
		go func() { one <- plan.ReconcileCancel(context.Background()) }()
		go func() { two <- plan.ReconcileCancel(context.Background()) }()
		authorized := make(chan error, 1)
		go func() { authorized <- plan.AuthorizeForward(context.Background(), prepared) }()
		close(phase.releaseIntent)
		if err := <-authorized; err == nil {
			t.Fatalf("iteration %d: authorize unexpectedly reached C4", i)
		}
		if err := <-intent; err != nil && !errors.Is(err, errSnapshotQueryCanceled) {
			t.Fatalf("iteration %d: intent=%v", i, err)
		}
		if err := <-one; err != nil {
			t.Fatalf("iteration %d: cancel one=%v", i, err)
		}
		if err := <-two; err != nil {
			t.Fatalf("iteration %d: cancel two=%v", i, err)
		}
		if got := strings.Join(phase.snapshotEvents(), ","); got != "prepare,intent,cancel" {
			t.Fatalf("iteration %d: phase events=%q", i, got)
		}
		if got := strings.Join(f.journal.calls, ","); got != "begin" {
			t.Fatalf("iteration %d: legacy journal=%q", i, got)
		}
	}
}

func TestC4PhaseBridgeRejectsCallbackTamperAndPhaseErrors(t *testing.T) {
	for name, phaseErr := range map[string]error{
		"intent":    errors.New("intent durable failure"),
		"authorize": errors.New("authorization durable failure"),
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			phase := &fakePhasePort{errs: map[string]error{name: phaseErr}}
			f.p.opts.PhasePortFactory = func(context.Context, replay.SnapshotQueryEnvelope) (SnapshotQueryIntakePhasePort, error) {
				return phase, nil
			}
			plan := f.install()
			prepared, err := plan.Prepare(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if name == "intent" {
				if err := plan.PersistForwardIntent(context.Background(), prepared); !errors.Is(err, phaseErr) {
					t.Fatalf("intent err=%v", err)
				}
			} else {
				if err := plan.PersistForwardIntent(context.Background(), prepared); err != nil {
					t.Fatal(err)
				}
				if err := plan.AuthorizeForward(context.Background(), prepared); !errors.Is(err, phaseErr) {
					t.Fatalf("authorize err=%v", err)
				}
			}
			// The bridge has no submit surface, and a phase error never falls
			// back to the legacy forward journal where a later relay write could
			// be authorized.
			if order := strings.Join(f.journal.calls, ","); order != "begin" {
				t.Fatalf("journal order=%q", order)
			}
		})
	}

	f := newFixture(t)
	phase := &fakePhasePort{}
	f.p.opts.PhasePortFactory = func(context.Context, replay.SnapshotQueryEnvelope) (SnapshotQueryIntakePhasePort, error) {
		return phase, nil
	}
	plan := f.install()
	prepared, err := plan.Prepare(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	prepared.Query.Body = "tampered"
	if err := plan.PersistForwardIntent(context.Background(), prepared); err == nil {
		t.Fatal("tampered callback reached phase port")
	}
	if order := strings.Join(phase.events, ","); order != "prepare" {
		t.Fatalf("phase order=%q", order)
	}
}

func TestC4PhaseBridgeRejectsNonCanonicalCallbackInputsWithoutC4Effects(t *testing.T) {
	callbacks := map[string]func(*plugin.AgentPreparePlan, plugin.PreparedAgentQuery) error{
		"intent": func(p *plugin.AgentPreparePlan, q plugin.PreparedAgentQuery) error {
			return p.PersistForwardIntent(context.Background(), q)
		},
		"authorize": func(p *plugin.AgentPreparePlan, q plugin.PreparedAgentQuery) error {
			return p.AuthorizeForward(context.Background(), q)
		},
		"unknown": func(p *plugin.AgentPreparePlan, q plugin.PreparedAgentQuery) error {
			return p.PersistForwardUnknown(context.Background(), q)
		},
	}
	mutations := map[string]func(*plugin.PreparedAgentQuery){
		"extra setting": func(q *plugin.PreparedAgentQuery) {
			q.Query.Settings = append(q.Query.Settings, chproto.Setting{Key: "max_threads", Value: "1"})
		},
		"legacy setting": func(q *plugin.PreparedAgentQuery) {
			q.Query.OldSettings = append(q.Query.OldSettings, chproto.OldSetting{Key: "max_threads", Value: 1})
		},
		"parameter": func(q *plugin.PreparedAgentQuery) {
			q.Query.Parameters = append(q.Query.Parameters, proto.Parameter{Key: "value", Value: "1"})
		},
		"duplicate token": func(q *plugin.PreparedAgentQuery) {
			q.Query.Settings = append(q.Query.Settings, q.Query.Settings[0])
		},
		"non-custom token": func(q *plugin.PreparedAgentQuery) { q.Query.Settings[0].Custom = false },
		"important token":  func(q *plugin.PreparedAgentQuery) { q.Query.Settings[0].Important = true },
		"obsolete token":   func(q *plugin.PreparedAgentQuery) { q.Query.Settings[0].Obsolete = true },
	}
	for callbackName, callback := range callbacks {
		for mutationName, mutate := range mutations {
			t.Run(callbackName+"/"+mutationName, func(t *testing.T) {
				f := newFixture(t)
				phase := &fakePhasePort{}
				f.p.opts.PhasePortFactory = func(context.Context, replay.SnapshotQueryEnvelope) (SnapshotQueryIntakePhasePort, error) {
					return phase, nil
				}
				plan := f.install()
				prepared, err := plan.Prepare(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				prepared.Query = ptr(cloneQuery(*prepared.Query))
				mutate(&prepared)
				if err := callback(plan, prepared); err == nil {
					t.Fatal("tampered execution input reached C4")
				}
				if got := strings.Join(phase.events, ","); got != "prepare" {
					t.Fatalf("C4 side effect=%q", got)
				}
			})
		}
	}
}

func TestOnQueryRejectsClientForgedMarkerAndSettings(t *testing.T) {
	f := newFixture(t)
	s := chsession.New(1, nil)
	q := &plugin.QueryContext{Session: s, Query: &chproto.Query{ID: "client", Body: "INSERT INTO logical SELECT 1", Settings: []chproto.Setting{{Key: auth.StatementTokenSettingKey, Value: "'forged'", Custom: true}}}, Values: map[string]any{plugin.SnapshotQueryAgentKey: true}}
	if err := f.p.OnQuery(context.Background(), q); err == nil {
		t.Fatal("forged client setting accepted")
	}
	if q.AgentPrepare != nil || f.sequence.calls != 0 {
		t.Fatal("rejected client input was claimed")
	}
}

func TestOnQueryRejectsUnsignedParametersWithoutSideEffects(t *testing.T) {
	for name, query := range map[string]*chproto.Query{
		"serialized parameters":    {ID: "client", Body: "INSERT INTO logical SELECT {value:UInt64}", Parameters: []proto.Parameter{{Key: "value", Value: "1"}}},
		"legacy parameter setting": {ID: "client", Body: "INSERT INTO logical SELECT 1", OldSettings: []chproto.OldSetting{{Key: "param_value", Value: 1}}},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			qctx := &plugin.QueryContext{Session: chsession.New(1, nil), Query: query}
			if err := f.p.OnQuery(context.Background(), qctx); err == nil {
				t.Fatal("unsigned parameter accepted")
			}
			if qctx.AgentPrepare != nil || f.sequence.calls != 0 || f.reservations.calls != 0 || len(f.journal.calls) != 0 {
				t.Fatal("parameter rejection had worker side effect")
			}
		})
	}
}

func TestPrepareFailsClosedOnIdentityAndPinMismatch(t *testing.T) {
	for _, mutate := range []func(*fixture){func(f *fixture) { f.reservations.grant.Reservation.FencingGeneration = 0 }, func(f *fixture) { f.catalog.value.ReadSet.ReadSnapshot.SnapshotID = "other" }} {
		f := newFixture(t)
		mutate(f)
		if _, err := f.install().Prepare(context.Background()); err == nil {
			t.Fatal("invalid port result prepared query")
		}
	}
}

func TestForwardCallbacksRejectAllTampering(t *testing.T) {
	f := newFixture(t)
	plan := f.install()
	prepared, err := plan.Prepare(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := plan.PersistForwardIntent(context.Background(), prepared); err != nil {
		t.Fatal(err)
	}
	base := f.journal.intent[0]
	for name, mutate := range map[string]func(*Operation, *plugin.PreparedAgentQuery){"sql": func(_ *Operation, p *plugin.PreparedAgentQuery) { p.Query.Body = "tampered" }, "token": func(_ *Operation, p *plugin.PreparedAgentQuery) { p.Query.Settings[0].Value = "'tampered'" }, "input root": func(op *Operation, _ *plugin.PreparedAgentQuery) { op.Envelope.InputRoot = "tampered" }, "reservation": func(op *Operation, _ *plugin.PreparedAgentQuery) {
		op.Envelope.Input.Binding.ReservationID = "tampered"
	}, "fence": func(op *Operation, _ *plugin.PreparedAgentQuery) { op.Envelope.Input.Binding.FencingGeneration++ }} {
		t.Run(name, func(t *testing.T) {
			op, p := base, prepared
			p.Query = ptr(cloneQuery(*prepared.Query))
			mutate(&op, &p)
			if matches(op, p) {
				t.Fatal("tamper matched")
			}
			for _, cb := range []func(context.Context, Operation, plugin.PreparedAgentQuery) error{f.p.persistIntent, f.p.authorize, f.p.unknown} {
				if err := cb(context.Background(), op, p); err == nil {
					t.Fatal("callback accepted tamper")
				}
			}
		})
	}
}

func TestCancelBeforeAndAfterPreparationUsesExactIdentity(t *testing.T) {
	f := newFixture(t)
	plan := f.install()
	if err := plan.ReconcileCancel(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(f.journal.canceled) != 0 {
		t.Fatal("pre-identity cancellation wrote record")
	}
	if _, err := plan.Prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := plan.ReconcileCancel(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(f.journal.canceled) != 1 {
		t.Fatal("missing cancellation")
	}
	a := f.journal.canceled[0]
	if a.RequestID != f.sequence.id || a.Grant.Reservation.ReservationID != f.reservations.grant.Reservation.ReservationID || a.Grant.Reservation.FencingGeneration != f.reservations.grant.Reservation.FencingGeneration {
		t.Fatal("cancellation identity changed")
	}
}
func TestOnQueryLeavesUnrecognizedQueryUnclaimed(t *testing.T) {
	f := newFixture(t)
	f.classifier.candidate.Recognized = false
	s := chsession.New(1, nil)
	q := &plugin.QueryContext{Session: s, Query: &chproto.Query{ID: "client", Body: "SELECT 1"}}
	if err := f.p.OnQuery(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	if q.AgentPrepare != nil || f.sequence.calls != 0 {
		t.Fatal("unrecognized query claimed")
	}
}
func setting(q *chproto.Query, key string) string {
	for _, s := range q.Settings {
		if s.Key == key {
			return s.Value
		}
	}
	return ""
}
func ptr(q chproto.Query) *chproto.Query { return &q }
