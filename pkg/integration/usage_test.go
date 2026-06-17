package integration

import (
	"context"
	"strings"
	"sync"
	"testing"

	housegate "housegate/housegate"
	"housegate/housegate/pkg/auth"
	"housegate/housegate/pkg/billing"
	"housegate/housegate/pkg/config"
	"housegate/housegate/pkg/integration/testenv"
)

// recordingUsageClient implements billing.UsageClient and captures
// every CheckBalance / ReportUsage call. AllowAll=true makes
// CheckBalance unconditionally succeed; flipping it lets later tests
// exercise the rejection path.
type recordingUsageClient struct {
	mu              sync.Mutex
	checkBalance    []checkBalanceCall
	reportUsage     []reportUsageCall
	allowAll        bool
	rejectionReason billing.RejectionReason
}

type checkBalanceCall struct {
	payer  string
	signer string
}

type reportUsageCall struct {
	payer  string
	signer string
	amount uint64
}

func (c *recordingUsageClient) CheckBalance(_ context.Context, payer, signer string) (bool, billing.RejectionReason, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.checkBalance = append(c.checkBalance, checkBalanceCall{payer: payer, signer: signer})
	if c.allowAll {
		return true, 0, nil
	}
	return false, c.rejectionReason, nil
}

func (c *recordingUsageClient) ReportUsage(_ context.Context, payer, signer string, amount uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reportUsage = append(c.reportUsage, reportUsageCall{payer: payer, signer: signer, amount: amount})
}

func (c *recordingUsageClient) snapshot() ([]checkBalanceCall, []reportUsageCall) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cb := make([]checkBalanceCall, len(c.checkBalance))
	copy(cb, c.checkBalance)
	ru := make([]reportUsageCall, len(c.reportUsage))
	copy(ru, c.reportUsage)
	return cb, ru
}

// withUsageClient injects a recordingUsageClient. The usage plugin is
// wired by build.go whenever Options.UsageClient is non-nil; this
// ProxyOption is the test-side equivalent.
func withUsageClient(c billing.UsageClient) testenv.ProxyOption {
	return func(_ *config.Config, opts *housegate.Options) {
		opts.UsageClient = c
	}
}

// TestUsage_Recorded pins the billing wire path: an authenticated query
// must produce exactly one CheckBalance + one ReportUsage call, both
// with payer == signer == the lowercased Ethereum address of the
// connection's signer. Captures three regressions at once:
//
//   - usage plugin not wired into the chain (zero CheckBalance calls)
//   - billing identity drift (payer != signer when no SQL_x_payer)
//   - case-sensitivity drift in identity recovery (the auth plugin
//     lowercases the recovered address; ReportUsage must agree).
func TestUsage_Recorded(t *testing.T) {
	signer, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}
	client := &recordingUsageClient{allowAll: true}

	rewriterOpt, _ := testenv.WithRewriterMock(t)
	proxy := testenv.StartServerProxy(t, chEnv.Addr,
		rewriterOpt,
		authProxyConfig([]string{signer.Address()}, false),
		withUsageClient(client),
	)
	conn := openSignedConn(t, proxy.Addr, signer)

	var v uint8
	if err := conn.QueryRow(context.Background(), "SELECT 1").Scan(&v); err != nil {
		t.Fatalf("signed SELECT 1: %v", err)
	}

	cb, ru := client.snapshot()
	if len(cb) != 1 {
		t.Fatalf("CheckBalance calls = %d, want 1: %+v", len(cb), cb)
	}
	if len(ru) != 1 {
		t.Fatalf("ReportUsage calls = %d, want 1: %+v", len(ru), ru)
	}
	want := strings.ToLower(signer.Address())
	if cb[0].payer != want || cb[0].signer != want {
		t.Errorf("CheckBalance call = %+v, want both fields = %q", cb[0], want)
	}
	if ru[0].payer != want || ru[0].signer != want {
		t.Errorf("ReportUsage call = %+v, want both fields = %q", ru[0], want)
	}
	if ru[0].amount != 1 {
		t.Errorf("ReportUsage amount = %d, want 1", ru[0].amount)
	}
}

// TestUsage_InsufficientBalanceRejects pins the rejection path: when
// CheckBalance returns ok=false with RejectionInsufficientBalance, the
// proxy must surface that to the client as a ClickHouse exception
// carrying the INSUFFICIENT_BALANCE name; ReportUsage must NOT be called
// (the query was rejected before it ran).
func TestUsage_InsufficientBalanceRejects(t *testing.T) {
	signer, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}
	client := &recordingUsageClient{
		allowAll:        false,
		rejectionReason: billing.RejectionInsufficientBalance,
	}

	rewriterOpt, _ := testenv.WithRewriterMock(t)
	proxy := testenv.StartServerProxy(t, chEnv.Addr,
		rewriterOpt,
		authProxyConfig([]string{signer.Address()}, false),
		withUsageClient(client),
	)
	conn := openSignedConn(t, proxy.Addr, signer)

	err = conn.QueryRow(context.Background(), "SELECT 1").Scan(new(uint8))
	if err == nil {
		t.Fatal("expected INSUFFICIENT_BALANCE rejection, got nil error")
	}
	if !strings.Contains(err.Error(), "INSUFFICIENT_BALANCE") {
		t.Errorf("error = %q, want to contain INSUFFICIENT_BALANCE", err.Error())
	}

	_, ru := client.snapshot()
	if len(ru) != 0 {
		t.Errorf("ReportUsage fired %d times for rejected query, want 0: %+v", len(ru), ru)
	}
}
