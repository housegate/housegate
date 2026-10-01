package querysettings

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sort"
	"strings"
	"testing"

	rewritergo "github.com/housegate/rewriter-go"
	pb "github.com/housegate/rewriter-proto/gen/pb"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/log"
	"github.com/housegate/housegate/pkg/plugin"
)

func TestRefused(t *testing.T) {
	for _, tc := range []struct {
		name, value string
		want        bool
	}{
		{"additional_table_filters", "{'db1.o': 'a = 1'}", true},
		{"additional_result_filter", "a = 1", true},
		{"parallel_replicas_custom_key", "a", true},
		{"dialect", "kusto", true},
		{"polyglot_dialect", "x", true},
		{"allow_experimental_polyglot_dialect", "1", true},
		{"allow_experimental_prql_dialect", "1", true},
		{"allow_experimental_kusto_dialect", "1", true},
		{"some_future_dialect", "1", true},
		{"Some_Future_DIALECT", "0", true},
		{"enable_global_with_statement", "1", true},
		{"enable_global_with_statement", "0", true},
		{"compatibility", "21.1", true},
		{"implicit_table_at_top_level", "x", true},
		{"promql_table", "x", true},
		{"promql_database", "x", true},
		{"legacy_column_name_of_tuple_literal", "0", true},
		{"profile", "default", true},
		{"PROFILE", "default", true},
		{"enable_analyzer", "1", false},
		{"enable_analyzer", "true", false},
		{"enable_analyzer", "TRUE", false},
		{"enable_analyzer", "True", false},
		{"enable_analyzer", "'1'", false},
		{"enable_analyzer", "'true'", false},
		{"enable_analyzer", "'TRUE'", false},
		{"Enable_Analyzer", "1", false},
		{"enable_analyzer", "0", true},
		{"enable_analyzer", "false", true},
		{"enable_analyzer", "'0'", true},
		{"enable_analyzer", " 1", true},
		{"enable_analyzer", "1 ", true},
		{"enable_analyzer", "+1", true},
		{"enable_analyzer", "1.0", true},
		{"enable_analyzer", "0x1", true},
		{"enable_analyzer", "1e0", true},
		{"enable_analyzer", "yes", true},
		{"enable_analyzer", "on", true},
		{"enable_analyzer", "UInt64_1", true},
		{"enable_analyzer", "Bool_1", true},
		{"enable_analyzer", "", true},
		{"allow_experimental_analyzer", "1", false},
		{"allow_experimental_analyzer", "true", false},
		{"allow_experimental_analyzer", "0", true},
		{"max_threads", "4", false},
		{"SQL_x_auth_token", "x", false},
		{"enable_scopes_for_with_statement", "0", false},
		{"allow_deprecated_syntax_for_merge_tree", "1", false}, // spec §13: deliberately not R5
		{"", "", false},
	} {
		if got := Refused(tc.name, tc.value); got != tc.want {
			t.Errorf("Refused(%q, %q) = %v, want %v", tc.name, tc.value, got, tc.want)
		}
	}
}

// The fixed names are exactly rewriter-go v0.16.0's sqlBearingSettings and
// analyzerSettings (internal/engine/settings.go:46-74), sorted.
func TestRefusedNames(t *testing.T) {
	want := []string{
		"additional_result_filter",
		"additional_table_filters",
		"allow_experimental_analyzer",
		"allow_experimental_kusto_dialect",
		"allow_experimental_polyglot_dialect",
		"allow_experimental_prql_dialect",
		"compatibility",
		"dialect",
		"enable_analyzer",
		"enable_global_with_statement",
		"implicit_table_at_top_level",
		"legacy_column_name_of_tuple_literal",
		"parallel_replicas_custom_key",
		"polyglot_dialect",
		"profile",
		"promql_database",
		"promql_table",
	}
	got := RefusedNames()
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("RefusedNames() = %v\nwant %v", got, want)
	}
	if !sort.StringsAreSorted(got) {
		t.Fatal("RefusedNames must be sorted")
	}
	got[0] = "mutated"
	if RefusedNames()[0] != want[0] {
		t.Fatal("RefusedNames must return a fresh slice")
	}
}

func newSession(t *testing.T) chsession.Session {
	t.Helper()
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	return chsession.New(1, client)
}

func queryWith(sess chsession.Session, settings ...chproto.Setting) *plugin.QueryContext {
	return &plugin.QueryContext{Session: sess, OriginalSQL: "SELECT 1", Query: &chproto.Query{Body: "SELECT 1", Settings: settings}}
}

func TestOnQuery(t *testing.T) {
	p := &Plugin{}
	ctx := context.Background()
	err := p.OnQuery(ctx, queryWith(newSession(t), chproto.Setting{Key: "SQL_x_auth_token", Value: "'t'", Custom: true}, chproto.Setting{Key: "enable_analyzer", Value: "0", Important: true}))
	if err == nil || err.Error() != "table setting enable_analyzer is not accepted (native-protocol query setting)" {
		t.Fatalf("err = %v", err)
	}
	// The first refused setting is named, whatever follows it.
	err = p.OnQuery(ctx, queryWith(newSession(t), chproto.Setting{Key: "max_threads", Value: "2"}, chproto.Setting{Key: "profile", Value: "default"}, chproto.Setting{Key: "dialect", Value: "kusto"}))
	if err == nil || err.Error() != "table setting profile is not accepted (native-protocol query setting)" {
		t.Fatalf("err = %v", err)
	}
	for _, accepted := range []chproto.Setting{
		{Key: "enable_analyzer", Value: "1", Important: true},
		{Key: "enable_analyzer", Value: "true", Important: true},
		// A custom-flagged value arrives as a Field dump: '1' is the string 1.
		{Key: "enable_analyzer", Value: "'1'", Custom: true},
		{Key: "max_threads", Value: "4"},
	} {
		if err := p.OnQuery(ctx, queryWith(newSession(t), accepted)); err != nil {
			t.Fatalf("%+v: %v", accepted, err)
		}
	}
	if err := p.OnQuery(ctx, queryWith(newSession(t), chproto.Setting{Key: "enable_analyzer", Value: "'0'", Custom: true})); err == nil {
		t.Fatal("a custom-flagged analyzer-off setting must be refused")
	}
	if err := p.OnQuery(ctx, &plugin.QueryContext{Session: newSession(t)}); err != nil {
		t.Fatalf("no Query packet: %v", err)
	}
	if err := p.OnQuery(ctx, nil); err != nil {
		t.Fatalf("nil query context: %v", err)
	}
	// Without a session the check still applies: there is no flag to skip it.
	if err := p.OnQuery(ctx, &plugin.QueryContext{Query: &chproto.Query{Settings: []chproto.Setting{{Key: "compatibility", Value: "21.1"}}}}); err == nil {
		t.Fatal("a refused setting without a session must be refused")
	}
}

func TestPolicyMarkers(t *testing.T) {
	p := &Plugin{}
	if p.RunOnPeerTrust() {
		t.Fatal("RunOnPeerTrust must be false: remote() loopback sessions carry the origin's SQL")
	}
	if !p.RunOnForward() {
		t.Fatal("RunOnForward must be true: the origin checks before pivoting")
	}
	if !p.RejectUndecodableQuery() {
		t.Fatal("RejectUndecodableQuery must be true: undecodable settings cannot be checked")
	}
}

func TestChainSessionKinds(t *testing.T) {
	chain := &plugin.PluginChain{QueryPlugins: []plugin.QueryPlugin{&Plugin{}}}
	for _, tc := range []struct {
		name string
		set  func(*chsession.SessionState)
		want bool // refused?
		// strict: the chain fails an undecodable Query closed. The chain
		// filters only peer-trust and forwarding, so a maintenance or
		// platform-operator Query that cannot be decoded is refused too, as
		// the rewrite plugin already refuses it.
		strict bool
	}{
		{"ordinary", func(*chsession.SessionState) {}, true, true},
		{"driver", func(s *chsession.SessionState) { s.SetIsDriver(true) }, true, true},
		{"origin-side forwarding", func(s *chsession.SessionState) { s.SetForwarding(true) }, true, true},
		{"forwarded from peer", func(s *chsession.SessionState) { s.SetPeerTrustForwarded("peer:9001", true) }, true, true},
		{"peer-trusted remote() loopback", func(s *chsession.SessionState) { s.SetPeerTrust("peer:9001") }, false, false},
		{"maintenance", func(s *chsession.SessionState) { s.SetMaintenance(true) }, false, true},
		{"platform operator", func(s *chsession.SessionState) { s.SetPlatformOperator(true) }, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sess := newSession(t)
			tc.set(sess.State())
			err := chain.OnQuery(context.Background(), queryWith(sess, chproto.Setting{Key: "compatibility", Value: "21.1", Important: true}))
			if (err != nil) != tc.want {
				t.Fatalf("refused = %v, want %v (%v)", err != nil, tc.want, err)
			}
			if got := chain.RejectUndecodableQuery(sess); got != tc.strict {
				t.Fatalf("RejectUndecodableQuery = %v, want %v", got, tc.strict)
			}
		})
	}
}

// TestRefusedMatchesTheNativeEngine pins the copied lists against the pinned
// rewriter-go engine: every name housegate refuses in the Query packet is one
// the engine refuses in SQL, with the same value rule. Opt-in (needs the FFI
// library); the integration suite repeats it for every system.settings name.
func TestRefusedMatchesTheNativeEngine(t *testing.T) {
	lib := os.Getenv("POLYGLOT_SQL_FFI_PATH")
	if lib == "" {
		t.Skip("POLYGLOT_SQL_FFI_PATH not set; native engine FFI lib unavailable")
	}
	svc, err := rewritergo.NewService(lib)
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()
	args := &pb.RewriteTableDynamicArgs{
		DatabaseMap: map[string]string{"db1": "phys"}, KnownPhysicalDatabases: []string{"phys"},
		UpstreamLogicalDatabaseInContext: "db1", Delim: "_",
		ProtectedDatabases: []string{"phys", "hg_safe", "hg_unsafe", "hg_promote"},
	}
	names := append(RefusedNames(), "some_future_dialect", "Enable_Analyzer", "max_threads", "enable_scopes_for_with_statement", "allow_deprecated_syntax_for_merge_tree")
	// Values the Query packet can carry for a Bool setting, spelled as SQL
	// literals: clickhouse-go's fmt.Sprint forms and Field dumps. (+1 is left
	// out: the native engine answers `max_threads = +1` with
	// "statement is not supported", not Success; Refused covers it in
	// TestRefused.)
	values := []string{"0", "1", "true", "false", "TRUE", "'1'", "'true'", "'0'", "'false'", "1.0", "0x1", "2"}
	for _, name := range names {
		for _, value := range values {
			resp, err := svc.Rewrite(context.Background(), &pb.RewriteSQLRequest{
				Sql: fmt.Sprintf("SELECT 1 SETTINGS `%s` = %s", name, value),
				Options: []*pb.RewriteOption{{Op: pb.RewriteOp_TableNameRewrite,
					Value: &pb.RewriteOption_TableNameArgs{TableNameArgs: &pb.RewriteTableNameArgs{DynamicArgs: args}}}},
			})
			if err != nil {
				t.Fatal(err)
			}
			engineRefuses := resp.GetMessage() == "table setting "+name+" is not accepted"
			if !engineRefuses && resp.GetCode() != pb.RewriteCode_Success {
				t.Errorf("%s = %s: unexpected engine answer %s %q", name, value, resp.GetCode(), resp.GetMessage())
				continue
			}
			if got := Refused(name, value); got != engineRefuses {
				t.Errorf("%s = %s: querysettings refuses %v, engine refuses %v", name, value, got, engineRefuses)
			}
		}
	}
	if !strings.Contains(strings.Join(RefusedNames(), ","), "enable_analyzer") {
		t.Fatal("RefusedNames must list the analyzer switches")
	}
}

func oldFormatQuery(sess chsession.Session, settings ...chproto.OldSetting) *plugin.QueryContext {
	return &plugin.QueryContext{Session: sess, OriginalSQL: "SELECT 1", Query: &chproto.Query{Body: "SELECT 1", OldSettings: settings}}
}

// A pre-54429 Query packet carries its settings in the old UInt64 format,
// whose decoding is not proven against a hostile client: any old-format
// setting is refused on a governed session, whatever its name and value,
// with the ordinary refusal naming the first one.
func TestOldFormatSettingsAreRefused(t *testing.T) {
	p := &Plugin{}
	ctx := context.Background()
	for _, settings := range [][]chproto.OldSetting{
		{{Key: "enable_analyzer", Value: 0}},
		{{Key: "enable_analyzer", Value: 1}},
		{{Key: "max_threads", Value: 8}, {Key: "enable_analyzer", Value: 0}},
	} {
		err := p.OnQuery(ctx, oldFormatQuery(newSession(t), settings...))
		want := "table setting " + settings[0].Key + " is not accepted (native-protocol query setting)"
		if err == nil || err.Error() != want {
			t.Fatalf("%+v: err = %v, want %q", settings, err, want)
		}
	}
	if err := p.OnQuery(ctx, oldFormatQuery(newSession(t))); err != nil {
		t.Fatalf("a Query packet without settings: %v", err)
	}
	for _, privileged := range []func(*chsession.SessionState){
		func(s *chsession.SessionState) { s.SetMaintenance(true) },
		func(s *chsession.SessionState) { s.SetPlatformOperator(true) },
	} {
		sess := newSession(t)
		privileged(sess.State())
		if err := p.OnQuery(ctx, oldFormatQuery(sess, chproto.OldSetting{Key: "max_threads", Value: 8})); err != nil {
			t.Fatalf("maintenance / operator sessions are not governed: %v", err)
		}
	}
	chain := &plugin.PluginChain{QueryPlugins: []plugin.QueryPlugin{p}}
	loopback := newSession(t)
	loopback.State().SetPeerTrust("peer:9001")
	if err := chain.OnQuery(ctx, oldFormatQuery(loopback, chproto.OldSetting{Key: "max_threads", Value: 8})); err != nil {
		t.Fatalf("remote() loopback sessions are not governed: %v", err)
	}
	forwarded := newSession(t)
	forwarded.State().SetPeerTrustForwarded("peer:9001", true)
	if err := chain.OnQuery(ctx, oldFormatQuery(forwarded, chproto.OldSetting{Key: "max_threads", Value: 8})); err == nil {
		t.Fatal("a forwarded-from-peer session must be governed")
	}
}

func TestRejectionLabel(t *testing.T) {
	for _, tc := range []struct{ name, want string }{
		{"enable_analyzer", "enable_analyzer"},
		{"Enable_Analyzer", "enable_analyzer"},
		{"PROFILE", "profile"},
		{"allow_experimental_kusto_dialect", "allow_experimental_kusto_dialect"},
		{"some_future_dialect", labelOtherDialect},
		{"X_DIALECT", labelOtherDialect},
	} {
		if got := rejectionLabel(tc.name); got != tc.want {
			t.Errorf("rejectionLabel(%q) = %q, want %q", tc.name, got, tc.want)
		}
	}
}

// A refusal logs a structured warning naming the setting, the connection and
// the session's account and user, and increments
// clickhouse_proxy_query_settings_rejections_total under a bounded label.
func TestRejectionTelemetry(t *testing.T) {
	var buf bytes.Buffer
	ctx := log.WithContext(context.Background(), log.New(slog.NewJSONHandler(&buf, nil)))
	p := &Plugin{}

	sess := newSession(t)
	sess.State().AuthenticatedUser = "alice"
	sess.State().Identity.UserID = "0xabc"
	before := testutil.ToFloat64(rejections.WithLabelValues("compatibility"))
	qctx := queryWith(sess, chproto.Setting{Key: "compatibility", Value: "21.1", Important: true})
	qctx.Query.ID = "q-1"
	if err := p.OnQuery(ctx, qctx); err == nil {
		t.Fatal("compatibility must be refused")
	}
	if got := testutil.ToFloat64(rejections.WithLabelValues("compatibility")); got != before+1 {
		t.Fatalf("compatibility counter = %v, want %v", got, before+1)
	}
	var record map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &record); err != nil {
		t.Fatalf("log record %q: %v", buf.String(), err)
	}
	for key, want := range map[string]any{
		"level":    "WARN",
		"msg":      "query refused: native-protocol query setting is not accepted",
		"setting":  "compatibility",
		"reason":   reasonRefusedSetting,
		"conn":     float64(1),
		"account":  "0xabc",
		"user":     "alice",
		"query_id": "q-1",
	} {
		if record[key] != want {
			t.Errorf("log %s = %v, want %v (record %v)", key, record[key], want, record)
		}
	}

	buf.Reset()
	beforeDialect := testutil.ToFloat64(rejections.WithLabelValues(labelOtherDialect))
	if err := p.OnQuery(ctx, queryWith(newSession(t), chproto.Setting{Key: "some_future_dialect", Value: "x"})); err == nil {
		t.Fatal("a _dialect setting must be refused")
	}
	if got := testutil.ToFloat64(rejections.WithLabelValues(labelOtherDialect)); got != beforeDialect+1 {
		t.Fatalf("other-dialect counter = %v, want %v", got, beforeDialect+1)
	}
	if !strings.Contains(buf.String(), `"setting":"some_future_dialect"`) {
		t.Fatalf("log must name the setting: %s", buf.String())
	}

	buf.Reset()
	beforeOld := testutil.ToFloat64(rejections.WithLabelValues(labelOldFormat))
	if err := p.OnQuery(ctx, oldFormatQuery(newSession(t), chproto.OldSetting{Key: "made_up_name", Value: 1})); err == nil {
		t.Fatal("an old-format setting must be refused")
	}
	if got := testutil.ToFloat64(rejections.WithLabelValues(labelOldFormat)); got != beforeOld+1 {
		t.Fatalf("old-format counter = %v, want %v", got, beforeOld+1)
	}
	if !strings.Contains(buf.String(), `"reason":"`+reasonOldFormat+`"`) || !strings.Contains(buf.String(), `"setting":"made_up_name"`) {
		t.Fatalf("old-format log: %s", buf.String())
	}

	// An accepted query neither logs nor counts.
	buf.Reset()
	if err := p.OnQuery(ctx, queryWith(newSession(t), chproto.Setting{Key: "enable_analyzer", Value: "1"})); err != nil {
		t.Fatal(err)
	}
	if buf.Len() != 0 {
		t.Fatalf("an accepted query logged: %s", buf.String())
	}
}
