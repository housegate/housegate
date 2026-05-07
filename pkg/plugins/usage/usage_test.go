package usage

import (
	"context"
	"net"
	"strings"
	"testing"

	"housegate/housegate/pkg/billing"
	"housegate/housegate/pkg/chproto"
	"housegate/housegate/pkg/chsession"
	"housegate/housegate/pkg/plugin"
)

// newTestSession returns a Session backed by one half of a net.Pipe.
func newTestSession(t *testing.T, id int64) chsession.Session {
	t.Helper()
	client, server := net.Pipe()
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	return chsession.New(id, client)
}

// newTestQueryContext constructs a QueryContext for the given SQL/settings.
func newTestQueryContext(sess chsession.Session, sql string, settings []chproto.Setting) *plugin.QueryContext {
	return &plugin.QueryContext{
		Session:     sess,
		OriginalSQL: sql,
		Query:       &chproto.Query{Body: sql, Settings: settings},
	}
}

// fakeCheckResult records the args UsageClientPlugin would have passed to
// CheckBalance and controls the simulated outcome.
type fakeCheckResult struct {
	payer  string
	signer string
	ok     bool
	reason billing.RejectionReason
	err    error
}

// pluginFromFakeUsage mirrors Plugin.OnQuery's resolution logic but
// delegates to a capturer instead of a real *billing.Client. This keeps
// the test self-contained without introducing an extra interface seam in
// production code.
func pluginFromFakeUsage(result *fakeCheckResult, called *bool, reportCalled *bool) func(ctx context.Context, qctx *plugin.QueryContext) error {
	return func(ctx context.Context, qctx *plugin.QueryContext) error {
		signer := qctx.Session.State().Snapshot().Identity.UserID
		if signer == "" {
			return nil
		}
		payer := signer
		if qctx.Query != nil {
			for _, s := range qctx.Query.Settings {
				if s.Key == payerSettingKey && s.Value != "" {
					payer = s.Value
					break
				}
			}
		}
		*called = true
		result.payer = payer
		result.signer = signer
		if result.err != nil {
			return nil // fail-open
		}
		if !result.ok {
			code, name, msg := billing.RejectionException(result.reason, payer, signer)
			return &rejectionError{code: code, name: name, msg: msg}
		}
		*reportCalled = true
		return nil
	}
}

type rejectionError struct {
	code int32
	name string
	msg  string
}

func (e *rejectionError) Error() string { return e.name + ": " + e.msg }

func TestPlugin_PassesSignerAndPayerFromSettings(t *testing.T) {
	sess := newTestSession(t, 1)
	const signerAddr = "0xdeadbeef"
	sess.State().Identity = chsession.IdentityClaims{UserID: signerAddr}

	qctx := newTestQueryContext(sess, "SELECT 1", []chproto.Setting{
		{Key: payerSettingKey, Value: "0xpayeraddr"},
	})

	result := &fakeCheckResult{ok: true}
	var checkCalled, reportCalled bool
	fn := pluginFromFakeUsage(result, &checkCalled, &reportCalled)

	if err := fn(context.Background(), qctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !checkCalled {
		t.Fatal("expected CheckBalance to be called")
	}
	if result.signer != signerAddr {
		t.Errorf("signer: got %q, want %q", result.signer, signerAddr)
	}
	if result.payer != "0xpayeraddr" {
		t.Errorf("payer: got %q, want %q", result.payer, "0xpayeraddr")
	}
	if !reportCalled {
		t.Error("expected ReportUsage to be called on success")
	}
}

func TestPlugin_TrimsQuotedPayerSetting(t *testing.T) {
	// clickhouse-go's CustomSetting wraps Custom string values in '...';
	// the plugin must strip them so the payer reaches CheckBalance as a
	// bare hex address (otherwise HexToAddress on the receiver yields
	// the zero address and IsOperator returns false →
	// UNAUTHORIZED_SIGNER even when the operator is registered on-chain).
	for _, tt := range []struct {
		name string
		raw  string
		want string
	}{
		{"single-quoted", "'0xpayeraddr'", "0xpayeraddr"},
		{"double-quoted", "\"0xpayeraddr\"", "0xpayeraddr"},
		{"unquoted", "0xpayeraddr", "0xpayeraddr"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			sess := newTestSession(t, 1)
			sess.State().Identity = chsession.IdentityClaims{UserID: "0xsigner"}
			qctx := newTestQueryContext(sess, "SELECT 1", []chproto.Setting{
				{Key: payerSettingKey, Value: tt.raw},
			})

			fake := &fakeUsageClient{}
			p := &Plugin{Client: fake}
			if err := p.OnQuery(context.Background(), qctx); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if fake.lastPayer != tt.want {
				t.Errorf("payer: got %q, want %q", fake.lastPayer, tt.want)
			}
		})
	}
}

func TestPlugin_DefaultsPayerToSigner(t *testing.T) {
	sess := newTestSession(t, 2)
	const signerAddr = "0xsigner"
	sess.State().Identity = chsession.IdentityClaims{UserID: signerAddr}

	qctx := newTestQueryContext(sess, "SELECT 1", nil)

	result := &fakeCheckResult{ok: true}
	var checkCalled, reportCalled bool
	fn := pluginFromFakeUsage(result, &checkCalled, &reportCalled)

	if err := fn(context.Background(), qctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.payer != signerAddr {
		t.Errorf("payer: got %q, want %q", result.payer, signerAddr)
	}
}

func TestPlugin_CheckBalanceFailureReturnsError(t *testing.T) {
	sess := newTestSession(t, 3)
	sess.State().Identity = chsession.IdentityClaims{UserID: "0xsigner"}

	qctx := newTestQueryContext(sess, "SELECT 1", nil)

	result := &fakeCheckResult{
		ok:     false,
		reason: billing.RejectionInsufficientBalance,
	}
	var checkCalled, reportCalled bool
	fn := pluginFromFakeUsage(result, &checkCalled, &reportCalled)

	err := fn(context.Background(), qctx)
	if err == nil {
		t.Fatal("expected error when CheckBalance rejects, got nil")
	}
	if !strings.Contains(err.Error(), "INSUFFICIENT_BALANCE") {
		t.Errorf("error should mention rejection reason, got: %v", err)
	}
	if reportCalled {
		t.Error("ReportUsage must NOT be called when CheckBalance rejects")
	}
}

func TestPlugin_AnonymousQueriesBypass(t *testing.T) {
	sess := newTestSession(t, 4)
	// No identity set — anonymous.

	qctx := newTestQueryContext(sess, "SELECT 1", nil)

	result := &fakeCheckResult{ok: false, reason: billing.RejectionInsufficientBalance}
	var checkCalled, reportCalled bool
	fn := pluginFromFakeUsage(result, &checkCalled, &reportCalled)

	if err := fn(context.Background(), qctx); err != nil {
		t.Fatalf("anonymous query should not return error, got: %v", err)
	}
	if checkCalled {
		t.Error("CheckBalance must NOT be called for anonymous queries")
	}
}

// fakeUsageClient counts CheckBalance / ReportUsage invocations so tests
// can assert that maintenance sessions bypass billing entirely. It
// satisfies billing.UsageClient and lets tests inject the real Plugin
// rather than a duplicated resolution helper.
type fakeUsageClient struct {
	checkBalanceCalls int
	reportUsageCalls  int
	lastPayer         string
	lastSigner        string
}

func (f *fakeUsageClient) CheckBalance(_ context.Context, payer, signer string) (bool, billing.RejectionReason, error) {
	f.checkBalanceCalls++
	f.lastPayer = payer
	f.lastSigner = signer
	// Approve the query: reason is ignored when ok=true.
	return true, 0, nil
}

func (f *fakeUsageClient) ReportUsage(_ context.Context, _, _ string, _ uint64) {
	f.reportUsageCalls++
}

// TestPlugin_MaintenanceBypassesClient verifies that an indexer-signed
// maintenance session (SessionState.Maintenance == true) bypasses both
// CheckBalance and ReportUsage. Maintenance ops are issued by sentio-node
// itself (e.g. DatabaseGC table DROPs) and must not generate billing.
func TestPlugin_MaintenanceBypassesClient(t *testing.T) {
	sess := newTestSession(t, 5)
	sess.State().Identity = chsession.IdentityClaims{UserID: "0xindexer"}
	sess.State().SetMaintenance(true)

	qctx := newTestQueryContext(sess, "DROP TABLE testnet.`abc.foo`", nil)

	client := &fakeUsageClient{}
	p := &Plugin{Client: client}

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery returned error for maintenance session: %v", err)
	}
	if client.checkBalanceCalls != 0 {
		t.Errorf("CheckBalance must NOT be called for maintenance sessions, got %d calls", client.checkBalanceCalls)
	}
	if client.reportUsageCalls != 0 {
		t.Errorf("ReportUsage must NOT be called for maintenance sessions, got %d calls", client.reportUsageCalls)
	}
}

// TestPlugin_ForwardedFromPeerBypassesClient verifies that a session
// arriving on the host proxy via forward pivot (IsPeerTrusted +
// IsForwardedFromPeer) bypasses billing. The entry proxy already ran
// CheckBalance/ReportUsage on the same query; running again on the
// host would double-charge the user.
func TestPlugin_ForwardedFromPeerBypassesClient(t *testing.T) {
	sess := newTestSession(t, 6)
	sess.State().Identity = chsession.IdentityClaims{UserID: "0xsigner"}
	sess.State().SetPeerTrustForwarded("0xpeer", true)

	qctx := newTestQueryContext(sess, "SELECT 1", nil)

	client := &fakeUsageClient{}
	p := &Plugin{Client: client}

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery returned error for forwarded-from-peer session: %v", err)
	}
	if client.checkBalanceCalls != 0 {
		t.Errorf("CheckBalance must NOT be called for forwarded-from-peer sessions, got %d calls", client.checkBalanceCalls)
	}
	if client.reportUsageCalls != 0 {
		t.Errorf("ReportUsage must NOT be called for forwarded-from-peer sessions, got %d calls", client.reportUsageCalls)
	}
}

// TestPlugin_LegacyRemoteLoopbackBypassesViaSigner verifies that the
// non-forwarded peer-trust path (legacy remote() loopback) still
// bypasses billing on the host — but via the existing signer=="" short
// circuit, since the chain skips the auth plugin in that case and
// Identity.UserID stays empty.
func TestPlugin_LegacyRemoteLoopbackBypassesViaSigner(t *testing.T) {
	sess := newTestSession(t, 7)
	// IsPeerTrusted=true, IsForwardedFromPeer=false, no Identity set
	// — the chain skips auth on legacy remote() so signer is empty.
	sess.State().SetPeerTrustForwarded("0xpeer", false)

	qctx := newTestQueryContext(sess, "SELECT 1", nil)

	client := &fakeUsageClient{}
	p := &Plugin{Client: client}

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery returned error for legacy remote() peer session: %v", err)
	}
	if client.checkBalanceCalls != 0 {
		t.Errorf("CheckBalance must NOT be called when signer is empty, got %d calls", client.checkBalanceCalls)
	}
}

// Compile-time guard: fakeUsageClient must satisfy billing.UsageClient.
var _ billing.UsageClient = (*fakeUsageClient)(nil)
