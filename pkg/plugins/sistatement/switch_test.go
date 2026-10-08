package sistatement

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/plugin"
)

type switchMetrics struct {
	seqMetrics
	results map[string]int
}

func (m *switchMetrics) SIUpstreamSwitch(result string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.results == nil {
		m.results = map[string]int{}
	}
	m.results[result]++
}

func addressedCodec(t *testing.T, address string) *chproto.Codec {
	t.Helper()
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	return chproto.NewCodec(namedEvaluationConn{Conn: client, address: address}, chproto.DirToUpstream)
}

// switchFixture: indexer 1 at 10.0.0.1:9000 (the session's current upstream),
// indexer 2 at 10.0.0.2:9000 hosts "shop", indexer 3 hosts "elsewhere".
func switchFixture(t *testing.T, pinned bool) (*Plugin, *fakeSession, *switchMetrics, *[]string) {
	t.Helper()
	ns := network.NewInMemoryNetworkState()
	declareSchema(t, ns, testSchema())
	ns.IndexerInfos[1] = network.IndexerInfo{IndexerId: 1, IndexerUrl: "10.0.0.1", ClickhouseProxyPort: 9000}
	ns.IndexerInfos[2] = network.IndexerInfo{IndexerId: 2, IndexerUrl: "10.0.0.2", ClickhouseProxyPort: 9000}
	ns.IndexerInfos[3] = network.IndexerInfo{IndexerId: 3, IndexerUrl: "10.0.0.3", ClickhouseProxyPort: 9000}
	ns.DatabaseInfos["shop"] = network.DatabaseInfo{DatabaseId: "shop", IndexerId: 2}
	ns.DatabaseInfos["local"] = network.DatabaseInfo{DatabaseId: "local", IndexerId: 1}
	ns.DatabaseInfos["elsewhere"] = network.DatabaseInfo{DatabaseId: "elsewhere", IndexerId: 3}
	opts, _ := inlineOptions(t, &fakeEvaluator{}, true)
	opts.Schemas = ns
	opts.InlineValues = InlineValuesOptions{}
	opts.Evaluator = nil
	opts.Hosting = ns
	opts.PinnedUpstream = pinned
	metrics := &switchMetrics{}
	opts.Observer = metrics
	var dialed []string
	opts.Dial = func(ctx context.Context, address string) (*chproto.Codec, error) {
		if _, ok := ctx.Deadline(); !ok {
			t.Errorf("dial %s without a bounded deadline", address)
		}
		dialed = append(dialed, address)
		return addressedCodec(t, address), nil
	}
	p, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	sess := newSession(1, "")
	sess.upstream = addressedCodec(t, "10.0.0.1:9000")
	sess.state.SetUpstreamHello(&chproto.ClientHello{Name: "clickhouse-client", ProtocolVersion: 54470, User: "default", Password: "pw"})
	return p, sess, metrics, &dialed
}

// assertRefusedLocally checks a refusal left the session where it was.
func assertRefusedLocally(t *testing.T, sess *fakeSession, q *plugin.QueryContext, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("the INSERT was not refused")
	}
	if !strings.Contains(err.Error(), "--database shop") {
		t.Errorf("refusal %q does not point to --database shop", err)
	}
	if len(sess.switched) != 0 || currentUpstreamAddress(sess.upstream) != "10.0.0.1:9000" || q.DeferredInsert != nil {
		t.Errorf("refused switch changed the session: switched=%d upstream=%q deferred=%v", len(sess.switched), currentUpstreamAddress(sess.upstream), q.DeferredInsert)
	}
}

func TestSwitch_InsertMovesTheSessionToTheHostingIndexer(t *testing.T) {
	p, sess, metrics, dialed := switchFixture(t, false)
	q := insertQctx(sess, "INSERT INTO shop.orders FORMAT Native")
	if err := p.OnQuery(context.Background(), q); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if len(*dialed) != 1 || (*dialed)[0] != "10.0.0.2:9000" || len(sess.switched) != 1 {
		t.Fatalf("dialed=%v switched=%d", *dialed, len(sess.switched))
	}
	hello := sess.switched[0]
	if hello.ProtocolVersion != testRevision || hello.Database != "" || hello.User != "default" || hello.Password != "pw" || hello.Name != "clickhouse-client" {
		t.Fatalf("replayed hello = %+v; want verbatim with ProtocolVersion = ClientRevision and no database", hello)
	}
	if q.DeferredInsert == nil || metrics.results["switched"] != 1 {
		t.Fatalf("deferred=%v results=%v", q.DeferredInsert, metrics.results)
	}
	// Sticky: the next INSERT is already on the hosting indexer.
	p.OnQueryAbort(context.Background(), q)
	p.OnQueryComplete(context.Background(), sess)
	if err := p.OnQuery(context.Background(), insertQctx(sess, "INSERT INTO shop.orders FORMAT Native")); err != nil || len(*dialed) != 1 {
		t.Fatalf("second INSERT re-dialed: %v %v", *dialed, err)
	}
}

func TestSwitch_NoSwitchWhenPinnedOrAlreadyThere(t *testing.T) {
	pinned, sess, _, dialed := switchFixture(t, true)
	if err := pinned.OnQuery(context.Background(), insertQctx(sess, "INSERT INTO shop.orders FORMAT Native")); err != nil || len(*dialed) != 0 {
		t.Fatalf("pinned upstream switched: %v %v", *dialed, err)
	}
	p, sess2, _, dialed2 := switchFixture(t, false)
	sess2.upstream = addressedCodec(t, "10.0.0.2:9000")
	if err := p.OnQuery(context.Background(), insertQctx(sess2, "INSERT INTO shop.orders FORMAT Native")); err != nil || len(*dialed2) != 0 {
		t.Fatalf("already on the hosting indexer but dialed %v (%v)", *dialed2, err)
	}
}

func TestSwitch_NoSwitchWithoutHostingOrDial(t *testing.T) {
	p, sess, _, dialed := switchFixture(t, false)
	p.hosting = nil
	if err := p.OnQuery(context.Background(), insertQctx(sess, "INSERT INTO shop.orders FORMAT Native")); err != nil || len(*dialed) != 0 {
		t.Fatalf("nil Hosting switched: %v %v", *dialed, err)
	}
	p2, sess2, _, _ := switchFixture(t, false)
	p2.dial = nil
	if err := p2.OnQuery(context.Background(), insertQctx(sess2, "INSERT INTO shop.orders FORMAT Native")); err != nil || len(sess2.switched) != 0 {
		t.Fatalf("nil Dial switched: %v %v", sess2.switched, err)
	}
}

func TestSwitch_UnknownOrDepartingTargetIsLeftToTheServer(t *testing.T) {
	for name, edit := range map[string]func(*network.InMemoryNetworkState){
		"unknown database": func(ns *network.InMemoryNetworkState) { delete(ns.DatabaseInfos, "shop") },
		"pending delete": func(ns *network.InMemoryNetworkState) {
			ns.DatabaseInfos["shop"] = network.DatabaseInfo{DatabaseId: "shop", IndexerId: 2, PendingDelete: true}
		},
		"unknown indexer": func(ns *network.InMemoryNetworkState) { delete(ns.IndexerInfos, 2) },
		"no housegate port": func(ns *network.InMemoryNetworkState) {
			ns.IndexerInfos[2] = network.IndexerInfo{IndexerId: 2, IndexerUrl: "10.0.0.2"}
		},
	} {
		t.Run(name, func(t *testing.T) {
			p, sess, metrics, dialed := switchFixture(t, false)
			edit(p.hosting.(*network.InMemoryNetworkState))
			q := insertQctx(sess, "INSERT INTO shop.orders FORMAT Native")
			if err := p.OnQuery(context.Background(), q); err != nil || len(*dialed) != 0 || len(metrics.results) != 0 || q.DeferredInsert == nil {
				t.Fatalf("err=%v dialed=%v results=%v deferred=%v", err, *dialed, metrics.results, q.DeferredInsert)
			}
		})
	}
}

func TestSwitch_ServerSideStateRefusesLocally(t *testing.T) {
	for _, stmt := range []string{
		"SET max_threads = 1",
		"CREATE TEMPORARY TABLE tmp (x UInt8)",
		"CREATE OR REPLACE TEMPORARY TABLE tmp (x UInt8)",
		"REPLACE TEMPORARY TABLE tmp (x UInt8) ENGINE = Memory",
		"BEGIN TRANSACTION",
		"START TRANSACTION",
		"/* c */ set max_threads = 1",
	} {
		t.Run(stmt, func(t *testing.T) {
			p, sess, metrics, dialed := switchFixture(t, false)
			state := insertQctx(sess, stmt)
			if err := p.OnQuery(context.Background(), state); err != nil {
				t.Fatal(err)
			}
			p.OnQuerySuccess(context.Background(), sess, state.Query.ID)
			p.OnQueryComplete(context.Background(), sess)
			q := insertQctx(sess, "INSERT INTO shop.orders FORMAT Native")
			err := p.OnQuery(context.Background(), q)
			want := "storage_integrity agent: INSERT into shop must run on indexer 2, but this session holds server-side state; reconnect with --database shop"
			if err == nil || err.Error() != want || len(*dialed) != 0 || q.DeferredInsert != nil || metrics.results["refused_state"] != 1 {
				t.Fatalf("err=%v dialed=%v deferred=%v results=%v", err, *dialed, q.DeferredInsert, metrics.results)
			}
			assertRefusedLocally(t, sess, q, err)
		})
	}
}

func TestSwitch_StatelessStatementsKeepTheSessionSwitchable(t *testing.T) {
	for _, stmt := range []string{"SELECT 1", "CREATE TABLE shop.t (x UInt8) ENGINE = Memory", "(SELECT 1)", "SETTINGS_TABLE_ROWS"} {
		t.Run(stmt, func(t *testing.T) {
			p, sess, _, dialed := switchFixture(t, false)
			q := insertQctx(sess, stmt)
			_ = p.OnQuery(context.Background(), q)
			p.OnQuerySuccess(context.Background(), sess, q.Query.ID)
			p.OnQueryComplete(context.Background(), sess)
			if err := p.OnQuery(context.Background(), insertQctx(sess, "INSERT INTO shop.orders FORMAT Native")); err != nil || len(*dialed) != 1 {
				t.Fatalf("%q pinned the session: %v %v", stmt, *dialed, err)
			}
		})
	}
}

func TestSwitch_FailedSetKeepsTheSessionSwitchable(t *testing.T) {
	p, sess, _, dialed := switchFixture(t, false)
	set := insertQctx(sess, "SET max_threads = 1")
	_ = p.OnQuery(context.Background(), set)
	p.OnQueryComplete(context.Background(), sess) // Exception: no success
	if err := p.OnQuery(context.Background(), insertQctx(sess, "INSERT INTO shop.orders FORMAT Native")); err != nil || len(*dialed) != 1 {
		t.Fatalf("a failed SET must not pin the session: %v %v", *dialed, err)
	}
}

func TestSwitch_OnCloseForgetsTheSessionState(t *testing.T) {
	p, sess, _, _ := switchFixture(t, false)
	set := insertQctx(sess, "SET max_threads = 1")
	_ = p.OnQuery(context.Background(), set)
	p.OnQuerySuccess(context.Background(), sess, set.Query.ID)
	_ = p.OnQuery(context.Background(), insertQctx(sess, "SET max_threads = 2"))
	p.OnClose(sess)
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.nonSwitchable) != 0 || len(p.statefulNext) != 0 {
		t.Fatalf("OnClose kept nonSwitchable=%v statefulNext=%v", p.nonSwitchable, p.statefulNext)
	}
}

func TestSwitch_SessionDatabaseRules(t *testing.T) {
	p, sess, metrics, _ := switchFixture(t, false)
	sess.state.SetLogicalDatabase("elsewhere")
	q := insertQctx(sess, "INSERT INTO shop.orders FORMAT Native")
	err := p.OnQuery(context.Background(), q)
	want := "storage_integrity agent: INSERT into shop must run on indexer 2, but the session database elsewhere lives on indexer 3; use a separate connection with --database shop or USE shop first"
	if err == nil || err.Error() != want || metrics.results["refused_database"] != 1 {
		t.Fatalf("err=%v results=%v", err, metrics.results)
	}
	assertRefusedLocally(t, sess, q, err)

	p2, sess2, _, _ := switchFixture(t, false)
	sess2.state.SetLogicalDatabase("shop") // hosted by the target: kept
	if err := p2.OnQuery(context.Background(), insertQctx(sess2, "INSERT INTO orders FORMAT Native")); err != nil || sess2.switched[0].Database != "shop" {
		t.Fatalf("err=%v hello=%+v", err, sess2.switched)
	}

	p3, sess3, _, _ := switchFixture(t, false)
	sess3.state.SetLogicalDatabase("default") // unknown to the registry: kept (plan P10)
	if err := p3.OnQuery(context.Background(), insertQctx(sess3, "INSERT INTO shop.orders FORMAT Native")); err != nil || sess3.switched[0].Database != "default" {
		t.Fatalf("err=%v hello=%+v", err, sess3.switched)
	}

	// The committed USE wins over the hello database.
	p4, sess4, metrics4, _ := switchFixture(t, false)
	sess4.state.SetLogicalDatabase("shop")
	use := insertQctx(sess4, "USE elsewhere")
	if err := p4.OnQuery(context.Background(), use); err != nil {
		t.Fatal(err)
	}
	p4.OnQuerySuccess(context.Background(), sess4, use.Query.ID)
	p4.OnQueryComplete(context.Background(), sess4)
	if err := p4.OnQuery(context.Background(), insertQctx(sess4, "INSERT INTO shop.orders FORMAT Native")); err == nil || metrics4.results["refused_database"] != 1 {
		t.Fatalf("committed USE elsewhere: err=%v results=%v", err, metrics4.results)
	}
}

func TestSwitch_RevisionAndDialFailuresRefuseLocally(t *testing.T) {
	cases := []struct {
		name      string
		switchErr error
		dialErr   error
		dialNil   bool
		result    string
		contains  string
	}{
		{name: "revision", switchErr: fmt.Errorf("%w: test", chsession.ErrUpstreamRevisionTooLow), result: "refused_revision", contains: "revision"},
		{name: "timezone", switchErr: fmt.Errorf("%w: test", chsession.ErrUpstreamTimezoneMismatch), result: "refused_revision", contains: "timezone"},
		{name: "handshake", switchErr: errors.New("switch-upstream read server-hello: EOF"), result: "dial_failed", contains: "EOF"},
		{name: "deadline", switchErr: fmt.Errorf("switch-upstream: %w", context.DeadlineExceeded), result: "dial_failed", contains: "deadline"},
		{name: "rebind denied", switchErr: fmt.Errorf("%w: test", chsession.ErrRebindDenied), result: "refused_state", contains: "rebind"},
		{name: "dial", dialErr: errors.New("connection refused"), result: "dial_failed", contains: "connection refused"},
		{name: "dial nil codec", dialNil: true, result: "dial_failed", contains: "no connection"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, sess, metrics, _ := switchFixture(t, false)
			sess.switchErr = tc.switchErr
			if tc.dialErr != nil || tc.dialNil {
				p.dial = func(context.Context, string) (*chproto.Codec, error) { return nil, tc.dialErr }
			}
			q := insertQctx(sess, "INSERT INTO shop.orders FORMAT Native")
			err := p.OnQuery(context.Background(), q)
			if err == nil || !strings.Contains(err.Error(), tc.contains) || metrics.results[tc.result] != 1 || len(metrics.results) != 1 {
				t.Fatalf("err=%v results=%v", err, metrics.results)
			}
			if tc.switchErr != nil && !errors.Is(err, tc.switchErr) {
				t.Fatalf("err %v does not wrap %v", err, tc.switchErr)
			}
			assertRefusedLocally(t, sess, q, err)
		})
	}
}

func TestSwitch_InlineEvaluatorFollowsTheNewEndpoint(t *testing.T) {
	ev := &fakeEvaluator{blocks: [][]proto.InputColumn{evaluatedBlock(1, "eu", 1.5)}}
	p, sess, _, _ := switchFixture(t, false)
	p.inline = InlineValuesOptions{Enabled: true, EvaluationTimeout: 5 * time.Second, MaxRows: 1000}
	p.evaluator = ev
	q := insertQctx(sess, "INSERT INTO shop.orders VALUES (1, 'eu', 1.5)")
	q.Values[plugin.ValuesKeyMaterialized] = "noop"
	if err := p.OnQuery(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	if ev.seen.UpstreamAddress != "10.0.0.2:9000" {
		t.Fatalf("evaluator used %q, want the switched endpoint", ev.seen.UpstreamAddress)
	}
}

func TestHoldsServerState(t *testing.T) {
	for sql, want := range map[string]bool{
		"SET max_threads = 1":                     true,
		"set role r":                              true,
		"BEGIN TRANSACTION":                       true,
		"START TRANSACTION":                       true,
		"CREATE TEMPORARY TABLE t (x UInt8)":      true,
		"CREATE OR REPLACE TEMPORARY TABLE t (x)": true,
		"REPLACE TEMPORARY TABLE t (x UInt8)":     true,
		"/* unterminated":                         true, // unreadable: refusing a later switch is safe
		"SELECT 1":                                false,
		"START MERGES":                            false,
		"CREATE TABLE t (x UInt8)":                false,
		"CREATE OR REPLACE TABLE t (x UInt8)":     false,
		"REPLACE TABLE t (x UInt8)":               false,
		"SETTINGS_TABLE_ROWS":                     false,
		"(SELECT 1)":                              false,
		"":                                        false,
	} {
		if got := holdsServerState(sql); got != want {
			t.Errorf("holdsServerState(%q) = %v, want %v", sql, got, want)
		}
	}
}
