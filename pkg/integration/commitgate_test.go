package integration

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	housegate "housegate/housegate"
	"housegate/housegate/pkg/auth"
	"housegate/housegate/pkg/cfgtypes"
	"housegate/housegate/pkg/chproto"
	"housegate/housegate/pkg/config"
	"housegate/housegate/pkg/integration/testenv"
	authplugin "housegate/housegate/pkg/plugins/auth"
	"housegate/housegate/pkg/plugins/commitgate"
	"housegate/housegate/pkg/registry"
	"housegate/housegate/pkg/sqlmeta"
	pb "housegate/housegate/protos"
)

// capturingObserver records every BeforeStatement event delivered to
// it. Subscribes to whatever caller passes via subscribed. Returning
// nil from BeforeStatement keeps the gate open so the proxy still
// forwards the statement to ClickHouse.
type capturingObserver struct {
	subscribed []sqlmeta.StatementType

	mu     sync.Mutex
	events []commitgate.Event
}

func (o *capturingObserver) SubscribedTypes() []sqlmeta.StatementType {
	return o.subscribed
}

func (o *capturingObserver) BeforeStatement(_ context.Context, ev *commitgate.Event) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	// Event payload is only valid for the duration of the call (per
	// the godoc); make a value-copy of the fields we care about.
	o.events = append(o.events, commitgate.Event{
		Type:         ev.Type,
		User:         ev.User,
		Owner:        ev.Owner,
		OriginalSQL:  ev.OriginalSQL,
		RewrittenSQL: ev.RewrittenSQL,
	})
	return nil
}

func (o *capturingObserver) OnStatementException(_ context.Context, _ *commitgate.Event, _ *chproto.Exception) {
}

func (o *capturingObserver) snapshot() []commitgate.Event {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]commitgate.Event, len(o.events))
	copy(out, o.events)
	return out
}

// withCommitGateObserver registers a custom observer alongside the
// observers build.go auto-wires (PermissionObserver under Auth.Enabled
// + ArgsValidator + InMemoryObserver).
func withCommitGateObserver(o commitgate.Observer) testenv.ProxyOption {
	return func(_ *config.Config, opts *housegate.Options) {
		opts.CommitGateObservers = append(opts.CommitGateObservers, o)
	}
}

// TestCommitGate_ObservesSelect verifies that a custom Observer
// registered via Options.CommitGateObservers receives BeforeStatement
// calls for the StatementTypes it subscribes to.
//
// SELECT 1 is chosen because the FROM-less path through
// PermissionObserver returns nil (allowsEmptyAccess), letting our
// downstream capturing observer fire. CREATE TABLE / DROP TABLE would
// need ev.AccessedTables populated by the rewriter mock to clear
// PermissionObserver, which the minimum-viable mock does not do.
func TestCommitGate_ObservesSelect(t *testing.T) {
	signer, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}

	obs := &capturingObserver{
		subscribed: []sqlmeta.StatementType{sqlmeta.StatementTypeSelect},
	}
	rewriterOpt, _ := testenv.WithRewriterMock(t)
	proxy := testenv.StartServerProxy(t, chEnv.Addr,
		rewriterOpt,
		testenv.WithConfigMutator(func(cfg *config.Config) {
			cfg.Auth = authplugin.Config{
				Enabled:          true,
				AllowedAddresses: []string{signer.Address()},
				MaxTokenAge:      cfgtypes.Duration{Duration: 5 * time.Minute},
				AllowNoAuth:      false,
			}
		}),
		withCommitGateObserver(obs),
	)

	conn := openSignedConn(t, proxy.Addr, signer)
	var v uint8
	if err := conn.QueryRow(context.Background(), "SELECT 1").Scan(&v); err != nil {
		t.Fatalf("signed SELECT 1: %v", err)
	}
	if v != 1 {
		t.Errorf("SELECT 1 = %d, want 1", v)
	}

	events := obs.snapshot()
	if len(events) == 0 {
		t.Fatal("custom commitgate observer never received an event")
	}
	got := events[0]
	if got.Type != sqlmeta.StatementTypeSelect {
		t.Errorf("event type = %v, want SELECT", got.Type)
	}
	if !strings.EqualFold(got.User, signer.Address()) {
		t.Errorf("event user = %q, want %q", got.User, signer.Address())
	}
	// OriginalSQL is what the client typed; RewrittenSQL is what the
	// mock returned. With the mock acting as identity transform they
	// should both contain "SELECT 1".
	if !strings.Contains(got.OriginalSQL, "SELECT 1") {
		t.Errorf("OriginalSQL = %q, want to contain SELECT 1", got.OriginalSQL)
	}
}

// commitGateDDLProxy stands up the auth-on + rewriter-mock + owner-grant
// posture every DDL-shaped commitgate test needs. The mock is wired to
// classify "CREATE TABLE " / "DROP TABLE " prefixed SQL as the right
// StatementType AND attach a single AccessedTable to the response so
// PermissionCommitGateObserver's per-table check has something to gate
// against. NetworkState grants the signer DbAuthOwner on chDatabase so
// the Write requirement for CREATE/DROP TABLE is met after promotion.
//
// Returns the proxy, the mock (for SeenSQL assertions), and the table
// name so each test can issue a unique DDL against it without colliding
// with the shared CH database.
func commitGateDDLProxy(t *testing.T, signer *auth.RelaySigner, observers ...commitgate.Observer) (*testenv.TestProxy, *testenv.RewriterMock, string) {
	t.Helper()
	rewriterOpt, mock := testenv.WithRewriterMock(t)
	table := uniqueTable(t)
	// Mock returns the same AccessedTable for both CREATE and DROP of
	// this table; PermissionObserver iterates the list and the per-DB
	// check is identical for both.
	tables := []*pb.AccessedTable{{
		OriginalDatabase: chEnv.Database,
		OriginalTable:    table,
		LogicalDatabase:  chEnv.Database,
		PhysicalDatabase: chEnv.Database,
	}}
	mock.SetAccessedTables("CREATE TABLE ", tables)
	mock.SetAccessedTables("DROP TABLE ", tables)

	opts := []testenv.ProxyOption{
		rewriterOpt,
		testenv.WithConfigMutator(func(cfg *config.Config) {
			cfg.Auth = authplugin.Config{
				Enabled:          true,
				AllowedAddresses: []string{signer.Address()},
				MaxTokenAge:      cfgtypes.Duration{Duration: 5 * time.Minute},
				AllowNoAuth:      false,
			}
		}),
		testenv.WithDatabasePermission(signer.Address(), chEnv.Database, registry.DbAuthOwner),
	}
	for _, o := range observers {
		opts = append(opts, withCommitGateObserver(o))
	}
	proxy := testenv.StartServerProxy(t, chEnv.Addr, opts...)
	return proxy, mock, table
}

// TestCommitGate_ObservesCreateTable runs CREATE TABLE through the full
// auth + rewriter + commitgate chain. The mock surfaces a synthetic
// AccessedTable so PermissionObserver clears the Write requirement; the
// downstream capturingObserver then sees a CREATE_TABLE event with the
// table name and signer identity attached.
//
// This is the canonical DDL gating path: a CREATE without an
// AccessedTable would fail with "requires a target database"; a
// CREATE on a database the signer does not own would fail with
// "no permission". Both failure modes are the *reason* this test
// exists — it pins the success path so a regression on either branch
// surfaces here as a happy-path failure.
func TestCommitGate_ObservesCreateTable(t *testing.T) {
	signer, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}
	obs := &capturingObserver{
		subscribed: []sqlmeta.StatementType{sqlmeta.StatementTypeCreateTable},
	}
	proxy, _, table := commitGateDDLProxy(t, signer, obs)

	conn := openSignedConn(t, proxy.Addr, signer)
	ddl := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (id UInt32) ENGINE = Memory", chEnv.Database, table)
	if err := conn.Exec(context.Background(), ddl); err != nil {
		t.Fatalf("CREATE TABLE through commitgate chain: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", chEnv.Database, table))
	})

	events := obs.snapshot()
	if len(events) == 0 {
		t.Fatal("commitgate observer never received CREATE_TABLE event")
	}
	got := events[0]
	if got.Type != sqlmeta.StatementTypeCreateTable {
		t.Errorf("event type = %v, want CREATE_TABLE", got.Type)
	}
	if !strings.EqualFold(got.User, signer.Address()) {
		t.Errorf("event user = %q, want %q", got.User, signer.Address())
	}
	if !strings.Contains(got.OriginalSQL, table) {
		t.Errorf("OriginalSQL = %q, want to contain %q", got.OriginalSQL, table)
	}
}

// abortObserver returns ErrAbortWithSuccess for every BeforeStatement
// call matching its subscribed types. The proxy must convert that into
// a synthetic EndOfStream — client sees a successful exec, but CH never
// receives the statement.
type abortObserver struct {
	subscribed []sqlmeta.StatementType

	mu     sync.Mutex
	fired  int
	events []commitgate.Event
}

func (o *abortObserver) SubscribedTypes() []sqlmeta.StatementType {
	return o.subscribed
}

func (o *abortObserver) BeforeStatement(_ context.Context, ev *commitgate.Event) error {
	o.mu.Lock()
	o.fired++
	o.events = append(o.events, commitgate.Event{Type: ev.Type, OriginalSQL: ev.OriginalSQL})
	o.mu.Unlock()
	return commitgate.ErrAbortWithSuccess
}

func (o *abortObserver) OnStatementException(_ context.Context, _ *commitgate.Event, _ *chproto.Exception) {
}

func (o *abortObserver) Fired() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.fired
}

// TestCommitGate_AbortWithSuccess pins the synthetic-success path: when
// BeforeStatement returns ErrAbortWithSuccess, the proxy writes an
// EndOfStream byte to the client without forwarding the SQL to CH. The
// test confirms (a) the client sees a successful exec, (b) the
// observer fired exactly once, and (c) the table the DDL would have
// created does NOT exist on CH afterward — proving the DDL did not
// reach the upstream.
//
// This is the on-chain CREATE/DROP DATABASE path's contract: those
// statements run on-chain via the observer and must NOT execute against
// ClickHouse. We test it on CREATE TABLE because the test fixtures
// already cover that path (NetworkState entry + AccessedTables wiring).
func TestCommitGate_AbortWithSuccess(t *testing.T) {
	signer, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}
	abort := &abortObserver{
		subscribed: []sqlmeta.StatementType{sqlmeta.StatementTypeCreateTable},
	}
	proxy, _, table := commitGateDDLProxy(t, signer, abort)

	conn := openSignedConn(t, proxy.Addr, signer)
	ddl := fmt.Sprintf("CREATE TABLE %s.%s (id UInt32) ENGINE = Memory", chEnv.Database, table)
	if err := conn.Exec(context.Background(), ddl); err != nil {
		t.Fatalf("expected synthetic success for aborted CREATE TABLE: %v", err)
	}
	if abort.Fired() != 1 {
		t.Errorf("abort observer fired %d times, want 1", abort.Fired())
	}

	// Verify the table never actually got created on the upstream. We
	// query the upstream directly (via a fresh proxy connection — same
	// shared chEnv) and expect zero rows from system.tables.
	var count uint64
	q := fmt.Sprintf("SELECT count() FROM system.tables WHERE database = '%s' AND name = '%s'",
		chEnv.Database, table)
	if err := openSignedConn(t, proxy.Addr, signer).QueryRow(context.Background(), q).Scan(&count); err != nil {
		t.Fatalf("verify table absence: %v", err)
	}
	if count != 0 {
		t.Errorf("synthetic-success CREATE TABLE still created the table on CH (count=%d, want 0) — the abort short-circuit is broken", count)
	}
}

// rejectObserver returns a plain error for every BeforeStatement call,
// driving the proxy's "reject" branch (synthesise an Exception to the
// client without contacting CH).
type rejectObserver struct {
	subscribed []sqlmeta.StatementType
	err        error
}

func (o *rejectObserver) SubscribedTypes() []sqlmeta.StatementType {
	return o.subscribed
}

func (o *rejectObserver) BeforeStatement(_ context.Context, _ *commitgate.Event) error {
	return o.err
}

func (o *rejectObserver) OnStatementException(_ context.Context, _ *commitgate.Event, _ *chproto.Exception) {
}

// TestCommitGate_ObserverError pins the rejection path: a non-sentinel
// error from BeforeStatement must reach the client as a ClickHouse
// Exception carrying the observer's message, and the table must NOT
// appear on the upstream. Distinct from ErrAbortWithSuccess (which
// surfaces as a clean success); this is how an observer rejects DDL
// it considers invalid.
func TestCommitGate_ObserverError(t *testing.T) {
	signer, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}
	const reason = "test-observer rejected this CREATE"
	rej := &rejectObserver{
		subscribed: []sqlmeta.StatementType{sqlmeta.StatementTypeCreateTable},
		err:        errors.New(reason),
	}
	proxy, _, table := commitGateDDLProxy(t, signer, rej)

	conn := openSignedConn(t, proxy.Addr, signer)
	ddl := fmt.Sprintf("CREATE TABLE %s.%s (id UInt32) ENGINE = Memory", chEnv.Database, table)
	err = conn.Exec(context.Background(), ddl)
	if err == nil {
		t.Fatal("expected observer rejection error, got nil")
	}
	if !strings.Contains(err.Error(), reason) {
		t.Errorf("error = %q, want to contain %q", err.Error(), reason)
	}

	// Confirm CH never saw the DDL: same upstream existence check as
	// the abort test.
	var count uint64
	q := fmt.Sprintf("SELECT count() FROM system.tables WHERE database = '%s' AND name = '%s'",
		chEnv.Database, table)
	if err := openSignedConn(t, proxy.Addr, signer).QueryRow(context.Background(), q).Scan(&count); err != nil {
		t.Fatalf("verify table absence: %v", err)
	}
	if count != 0 {
		t.Errorf("rejected CREATE TABLE still created the table on CH (count=%d, want 0)", count)
	}
}
