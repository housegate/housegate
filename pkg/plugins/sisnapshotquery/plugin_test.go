package sisnapshotquery

import (
	"context"
	"strings"
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
