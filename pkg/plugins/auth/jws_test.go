package authplugin

import (
	"context"
	"net"
	"strings"
	"testing"

	"housegate/housegate/pkg/auth"
	"housegate/housegate/pkg/chproto"
	"housegate/housegate/pkg/chsession"
	"housegate/housegate/pkg/network"
	"housegate/housegate/pkg/plugin"
)

// fakeValidator returns a fixed ValidationResult, bypassing JWS crypto
// so the test can focus on the owner-resolution branch.
type fakeValidator struct {
	res auth.ValidationResult
	err error
}

func (f *fakeValidator) ValidateQuery(_ context.Context, _ auth.QueryMeta) (auth.ValidationResult, error) {
	return f.res, f.err
}

func newQctx(t *testing.T, settings ...chproto.Setting) (*plugin.QueryContext, chsession.Session) {
	t.Helper()
	client, server := net.Pipe()
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	sess := chsession.New(1, client)
	q := &chproto.Query{Body: "SELECT 1"}
	q.Settings = append(q.Settings, settings...)
	return &plugin.QueryContext{
		Session:     sess,
		OriginalSQL: "SELECT 1",
		Query:       q,
	}, sess
}

func payerSetting(addr string) chproto.Setting {
	return chproto.Setting{
		Key:    auth.PayerSettingKey,
		Value:  "'" + addr + "'",
		Custom: true,
	}
}

const (
	testSigner = "0xa1e4252cfc8f1a14350d4b25ee2f97809a177117"
	testOwner  = "0x8bc30c01497bec83901e7d2d63502ac311370161"
)

// TestOnQuery_NoPayer leaves qctx.Owner empty when the query carries no
// SQL_x_payer setting — the legacy single-principal path.
func TestOnQuery_NoPayer(t *testing.T) {
	ns := network.NewInMemoryNetworkState()
	p := &Plugin{
		Validator: &fakeValidator{res: auth.ValidationResult{Address: testSigner}},
		Access:    network.NewRegistryAdapter(ns),
	}
	qctx, _ := newQctx(t)
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if qctx.Owner != "" {
		t.Errorf("Owner=%q, want empty", qctx.Owner)
	}
}

// TestOnQuery_PayerEqualsSigner zeros owner when the operator setting
// names the signer itself — same convention as commitgate.buildEvent.
func TestOnQuery_PayerEqualsSigner(t *testing.T) {
	ns := network.NewInMemoryNetworkState()
	p := &Plugin{
		Validator: &fakeValidator{res: auth.ValidationResult{Address: testSigner}},
		Access:    network.NewRegistryAdapter(ns),
	}
	qctx, _ := newQctx(t, payerSetting(strings.ToUpper(testSigner)))
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if qctx.Owner != "" {
		t.Errorf("Owner=%q, want empty when payer==signer", qctx.Owner)
	}
}

// TestOnQuery_OperatorAuthorized populates qctx.Owner when IsOperator
// succeeds, normalising to lowercase.
func TestOnQuery_OperatorAuthorized(t *testing.T) {
	ns := network.NewInMemoryNetworkState()
	ns.SetOperator(network.AccountAddress(testOwner), network.AccountAddress(testSigner), true)
	p := &Plugin{
		Validator: &fakeValidator{res: auth.ValidationResult{Address: testSigner}},
		Access:    network.NewRegistryAdapter(ns),
	}
	qctx, _ := newQctx(t, payerSetting("0x"+strings.ToUpper(testOwner[2:])))
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if qctx.Owner != testOwner {
		t.Errorf("Owner=%q, want %q", qctx.Owner, testOwner)
	}
}

// TestOnQuery_OperatorNotAuthorized rejects the query before any
// downstream plugin reads owner-scoped state.
func TestOnQuery_OperatorNotAuthorized(t *testing.T) {
	ns := network.NewInMemoryNetworkState() // no operator relation registered
	p := &Plugin{
		Validator: &fakeValidator{res: auth.ValidationResult{Address: testSigner}},
		Access:    network.NewRegistryAdapter(ns),
	}
	qctx, _ := newQctx(t, payerSetting(testOwner))
	err := p.OnQuery(context.Background(), qctx)
	if err == nil {
		t.Fatalf("unauthorized operator must be rejected")
	}
	if !strings.Contains(err.Error(), "not an operator of") {
		t.Errorf("error should explain operator-of failure, got %v", err)
	}
	if qctx.Owner != "" {
		t.Errorf("Owner=%q, want empty on rejection", qctx.Owner)
	}
}

// TestOnQuery_StateUnwiredFallsBack preserves backward compatibility:
// without a State handle the auth plugin cannot validate IsOperator, so
// it leaves qctx.Owner empty and lets the commitgate observer's own
// State check be the sole gate.
func TestOnQuery_StateUnwiredFallsBack(t *testing.T) {
	p := &Plugin{
		Validator: &fakeValidator{res: auth.ValidationResult{Address: testSigner}},
		Access:    nil,
	}
	qctx, _ := newQctx(t, payerSetting(testOwner))
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if qctx.Owner != "" {
		t.Errorf("Owner=%q, want empty when State is nil", qctx.Owner)
	}
}

// TestOnQuery_PayerWithoutSigner rejects: a query carrying SQL_x_payer
// must come from an authenticated principal, otherwise an attacker could
// pin qctx.Owner to any victim address.
func TestOnQuery_PayerWithoutSigner(t *testing.T) {
	ns := network.NewInMemoryNetworkState()
	p := &Plugin{
		Validator: &fakeValidator{res: auth.ValidationResult{Address: ""}},
		Access:    network.NewRegistryAdapter(ns),
	}
	qctx, _ := newQctx(t, payerSetting(testOwner))
	err := p.OnQuery(context.Background(), qctx)
	if err == nil {
		t.Fatalf("SQL_x_payer without authenticated signer must be rejected")
	}
	if !strings.Contains(err.Error(), "authenticated signer") {
		t.Errorf("error should mention authenticated signer, got %v", err)
	}
}

// TestOnQuery_MaintenanceSkipsOwner the maintenance bypass also skips
// owner resolution — those sessions short-circuit every downstream gate
// already.
func TestOnQuery_MaintenanceSkipsOwner(t *testing.T) {
	ns := network.NewInMemoryNetworkState()
	// Deliberately register no operator relation — if owner resolution
	// were to run, IsOperator would return false and OnQuery would
	// reject. Maintenance must bypass that.
	p := &Plugin{
		Validator: &fakeValidator{res: auth.ValidationResult{Address: testSigner, Maintenance: true}},
		Access:    network.NewRegistryAdapter(ns),
	}
	qctx, _ := newQctx(t, payerSetting(testOwner))
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if qctx.Owner != "" {
		t.Errorf("Owner=%q, want empty on maintenance bypass", qctx.Owner)
	}
}
