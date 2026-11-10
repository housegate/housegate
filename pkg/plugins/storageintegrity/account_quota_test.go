package storageintegrity

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/plugin"
	"github.com/housegate/housegate/pkg/registry"
	"github.com/housegate/housegate/pkg/sqlmeta"
)

type quotaClock struct{ t time.Time }

func (c *quotaClock) now() time.Time          { return c.t }
func (c *quotaClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTestQuota(t *testing.T, mode AccountQuotaMode, statementBurst, byteBurst float64) (*AccountQuota, *quotaClock) {
	t.Helper()
	clock := &quotaClock{t: time.Unix(1_700_000_000, 0)}
	q, err := NewAccountQuota(AccountQuotaConfig{
		Mode:                mode,
		StatementsPerSecond: 1,
		StatementBurst:      statementBurst,
		BytesPerSecond:      5,
		ByteBurst:           byteBurst,
		Now:                 clock.now,
	})
	if err != nil {
		t.Fatalf("NewAccountQuota: %v", err)
	}
	return q, clock
}

func TestAccountQuota_OffIsNilAndAdmitsWithoutTracking(t *testing.T) {
	for _, mode := range []AccountQuotaMode{"", AccountQuotaOff} {
		q, err := NewAccountQuota(AccountQuotaConfig{Mode: mode})
		if err != nil || q != nil {
			t.Fatalf("mode %q: quota = %v, err = %v; want nil, nil", mode, q, err)
		}
	}
	var q *AccountQuota
	if refusal, admitted := q.Admit(azSigner); refusal != nil || admitted {
		t.Fatalf("nil quota Admit = %v, %v; want nil, false", refusal, admitted)
	}
	q.ChargeBytes(azSigner, 1<<30) // must not panic
	if q.Mode() != AccountQuotaOff {
		t.Fatalf("nil quota mode = %q", q.Mode())
	}
}

func TestAccountQuota_RejectsInvalidConfig(t *testing.T) {
	valid := AccountQuotaConfig{Mode: AccountQuotaEnforce, StatementsPerSecond: 1, StatementBurst: 1, BytesPerSecond: 1, ByteBurst: 1}
	for name, mutate := range map[string]func(*AccountQuotaConfig){
		"unknown mode":        func(c *AccountQuotaConfig) { c.Mode = "strict" },
		"zero statement rate": func(c *AccountQuotaConfig) { c.StatementsPerSecond = 0 },
		"statement burst < 1": func(c *AccountQuotaConfig) { c.StatementBurst = 0.5 },
		"zero byte rate":      func(c *AccountQuotaConfig) { c.BytesPerSecond = 0 },
		"negative byte burst": func(c *AccountQuotaConfig) { c.ByteBurst = -1 },
	} {
		cfg := valid
		mutate(&cfg)
		if _, err := NewAccountQuota(cfg); err == nil {
			t.Errorf("%s: NewAccountQuota accepted %+v", name, cfg)
		}
	}
	if _, err := NewAccountQuota(valid); err != nil {
		t.Fatalf("valid config refused: %v", err)
	}
}

func TestAccountQuota_StatementBucketRefillsAtRate(t *testing.T) {
	q, clock := newTestQuota(t, AccountQuotaEnforce, 2, 100)
	for i := 0; i < 2; i++ {
		if refusal, admitted := q.Admit(azSigner); refusal != nil || !admitted {
			t.Fatalf("statement %d within burst: refusal %v admitted %v", i, refusal, admitted)
		}
	}
	refusal, admitted := q.Admit(azSigner)
	if admitted || refusal == nil || refusal.Dimension != quotaDimensionStatements || refusal.RetryAfter != time.Second {
		t.Fatalf("third statement = %+v admitted %v; want statements refusal retrying in 1s", refusal, admitted)
	}
	clock.advance(time.Second)
	if refusal, admitted := q.Admit(azSigner); refusal != nil || !admitted {
		t.Fatalf("after refill: refusal %v admitted %v", refusal, admitted)
	}
}

func TestAccountQuota_ByteDebtRefusesUntilRepaid(t *testing.T) {
	q, clock := newTestQuota(t, AccountQuotaEnforce, 100, 10)
	if refusal, _ := q.Admit(azSigner); refusal != nil {
		t.Fatalf("first statement refused: %+v", refusal)
	}
	// A statement larger than the burst is charged in full, into debt.
	q.ChargeBytes(azSigner, 25)
	refusal, admitted := q.Admit(azSigner)
	if admitted || refusal == nil || refusal.Dimension != quotaDimensionBytes || refusal.RetryAfter != 3*time.Second {
		t.Fatalf("statement in debt = %+v admitted %v; want bytes refusal retrying in 3s", refusal, admitted)
	}
	clock.advance(2 * time.Second)
	if refusal, _ := q.Admit(azSigner); refusal == nil {
		t.Fatal("statement admitted before the debt was repaid")
	}
	clock.advance(time.Second)
	if refusal, admitted := q.Admit(azSigner); refusal != nil || !admitted {
		t.Fatalf("statement after repayment: refusal %v admitted %v", refusal, admitted)
	}
}

func TestAccountQuota_KeysAreCaseInsensitiveAndIndependent(t *testing.T) {
	q, _ := newTestQuota(t, AccountQuotaEnforce, 1, 100)
	if refusal, _ := q.Admit(strings.ToUpper(azSigner[2:])); refusal != nil {
		t.Fatalf("first: %+v", refusal)
	}
	if refusal, _ := q.Admit(strings.ToUpper(azSigner[2:])); refusal == nil {
		t.Fatal("same account under another case got a fresh bucket")
	}
	if refusal, _ := q.Admit(azOwner); refusal != nil {
		t.Fatalf("another account shares the bucket: %+v", refusal)
	}
}

func TestAccountQuota_DryRunTakesNothingFromRefusedStatements(t *testing.T) {
	q, clock := newTestQuota(t, AccountQuotaDryRun, 1, 100)
	if refusal, admitted := q.Admit(azSigner); refusal != nil || !admitted {
		t.Fatalf("first: refusal %v admitted %v", refusal, admitted)
	}
	for i := 0; i < 3; i++ {
		if refusal, admitted := q.Admit(azSigner); refusal == nil || admitted {
			t.Fatalf("over budget %d: refusal %v admitted %v; want a would-refuse", i, refusal, admitted)
		}
	}
	// The would-be refusals took no tokens, exactly as enforce mode would.
	clock.advance(time.Second)
	if refusal, admitted := q.Admit(azSigner); refusal != nil || !admitted {
		t.Fatalf("after refill: refusal %v admitted %v", refusal, admitted)
	}
}

func TestAccountQuota_SweepDropsOnlyFullBuckets(t *testing.T) {
	q, clock := newTestQuota(t, AccountQuotaEnforce, 100, 1000)
	q.Admit(azSigner)
	q.Admit(azOwner)
	q.ChargeBytes(azOwner, 1000+5*int64Seconds(accountQuotaSweepInterval)+1)
	clock.advance(accountQuotaSweepInterval)
	q.Admit(azAdmin) // triggers the sweep
	q.mu.Lock()
	_, signerKept := q.buckets[azSigner]
	_, ownerKept := q.buckets[azOwner]
	q.mu.Unlock()
	if signerKept {
		t.Fatal("a refilled bucket was kept")
	}
	if !ownerKept {
		t.Fatal("a bucket still in debt was dropped, which would forgive its debt")
	}
}

func int64Seconds(d time.Duration) uint64 { return uint64(d / time.Second) }

func TestAccountQuotaExceededMessage(t *testing.T) {
	got := chproto.AccountQuotaExceededMessage(azSigner, "bytes", 1500*time.Millisecond)
	want := "storage_integrity: account " + azSigner + " exceeded its write quota (bytes); retry in 2s (retryable)"
	if got != want {
		t.Fatalf("message = %q, want %q", got, want)
	}
}

// quotaInsert drives one signed INSERT of payload through the ingress with a
// consumer and returns the OnQuery error.
func quotaInsert(t *testing.T, p *Plugin, signer *auth.RelaySigner, seq uint64, payload []byte, mutate func(*plugin.QueryContext)) error {
	t.Helper()
	sql := "INSERT INTO tenant.events FORMAT Native"
	qctx := signedQueryContext(t, 70, signer, sql, sql, sqlmeta.StatementTypeInsert)
	qctx.Query.ID = statementIDFor(signer, seq)
	qctx.AccessedTables = []sqlmeta.AccessedTable{{IsStorageIntegrity: true, OriginalDatabase: "tenant", OriginalTable: "events"}}
	withDefaultCaptureToken(t, qctx, signer, payload)
	if mutate != nil {
		mutate(qctx)
	}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		return err
	}
	if err := p.OnClientDataStrict(context.Background(), qctx, payload); err != nil {
		t.Fatalf("OnClientDataStrict: %v", err)
	}
	if err := p.OnQueryInputCompleteStrict(context.Background(), qctx); err != nil {
		t.Fatalf("OnQueryInputCompleteStrict: %v", err)
	}
	return nil
}

func requireQuotaRefusal(t *testing.T, err error, dimension string) {
	t.Helper()
	var clientErr *chproto.ClientError
	if !errors.As(err, &clientErr) || clientErr.Code != chproto.CodeQuotaExceeded {
		t.Fatalf("err = %v, want a 201 QUOTA_EXCEEDED refusal", err)
	}
	if !chproto.IsSeqUnspent(err) {
		t.Fatal("quota refusal does not mark the client_seq unspent")
	}
	if !strings.Contains(clientErr.Message, "("+dimension+")") || !strings.HasSuffix(clientErr.Message, "(retryable)") {
		t.Fatalf("message = %q, want a retryable %s refusal", clientErr.Message, dimension)
	}
}

func TestIngressAccountQuotaRefusesStatementsOverBudget(t *testing.T) {
	q, clock := newTestQuota(t, AccountQuotaEnforce, 1, 1<<20)
	consumer := &recordingConsumer{}
	p, signer := newSignedIngressWithConfig(t, Config{AdmissionConsumer: consumer, AccountQuota: q})
	payload := []byte{byte(chproto.ClientDataCode), 0, 0xab, 0xcd}
	if err := quotaInsert(t, p, signer, 1, payload, nil); err != nil {
		t.Fatalf("first INSERT: %v", err)
	}
	requireQuotaRefusal(t, quotaInsert(t, p, signer, 2, payload, nil), quotaDimensionStatements)
	// The refusal left no capture behind: once refilled, the same session writes.
	clock.advance(time.Second)
	if err := quotaInsert(t, p, signer, 2, payload, nil); err != nil {
		t.Fatalf("INSERT after refill: %v", err)
	}
	if len(consumer.admissions) != 2 {
		t.Fatalf("consumed admissions = %d, want 2", len(consumer.admissions))
	}
}

func TestIngressAccountQuotaChargesPayloadBytes(t *testing.T) {
	payload := []byte{byte(chproto.ClientDataCode), 0, 0xab, 0xcd}
	q, _ := newTestQuota(t, AccountQuotaEnforce, 100, float64(len(payload)-1))
	p, signer := newSignedIngressWithConfig(t, Config{AdmissionConsumer: &recordingConsumer{}, AccountQuota: q})
	if err := quotaInsert(t, p, signer, 1, payload, nil); err != nil {
		t.Fatalf("first INSERT: %v", err)
	}
	requireQuotaRefusal(t, quotaInsert(t, p, signer, 2, payload, nil), quotaDimensionBytes)
}

func TestIngressAccountQuotaDryRunAdmitsEverything(t *testing.T) {
	q, _ := newTestQuota(t, AccountQuotaDryRun, 1, 1)
	consumer := &recordingConsumer{}
	p, signer := newSignedIngressWithConfig(t, Config{AdmissionConsumer: consumer, AccountQuota: q})
	payload := []byte{byte(chproto.ClientDataCode), 0, 0xab, 0xcd}
	for seq := uint64(1); seq <= 3; seq++ {
		if err := quotaInsert(t, p, signer, seq, payload, nil); err != nil {
			t.Fatalf("dry_run INSERT %d: %v", seq, err)
		}
	}
	if len(consumer.admissions) != 3 {
		t.Fatalf("consumed admissions = %d, want 3", len(consumer.admissions))
	}
}

// An operator writing for an owner spends the owner's budget, not its own.
func TestIngressAccountQuotaKeysByPrincipal(t *testing.T) {
	signer, err := auth.NewRelaySigner(storageIntegrityTestKey)
	if err != nil {
		t.Fatal(err)
	}
	signerAddr := strings.ToLower(signer.Address())
	ns, _ := ingressNetworkState(t)
	ns.DatabaseInfos["tenant"] = network.DatabaseInfo{DatabaseId: "tenant"}
	ns.DatabasePermissions[azOwner] = network.DatabasePermissions{"tenant": registry.DbAuthOwner}
	ns.DatabasePermissions[network.AccountAddress(signerAddr)] = network.DatabasePermissions{"tenant": registry.DbAuthWrite}
	ns.SetOperator(azOwner, network.AccountAddress(signerAddr), true)
	q, _ := newTestQuota(t, AccountQuotaEnforce, 1, 1<<20)
	p, _ := newSignedIngressWithConfig(t, Config{TableSchemas: ns, NetworkID: "testnet-v2", Writers: ns, Operators: ns,
		AdmissionConsumer: &recordingConsumer{}, AccountQuota: q})
	payload := []byte{byte(chproto.ClientDataCode), 0, 0xab, 0xcd}
	asOwner := func(qctx *plugin.QueryContext) {
		qctx.Query.Settings = append(qctx.Query.Settings, chproto.Setting{Key: auth.PayerSettingKey, Value: "'" + azOwner + "'", Custom: true})
	}
	if err := quotaInsert(t, p, signer, 1, payload, asOwner); err != nil {
		t.Fatalf("operator INSERT for owner: %v", err)
	}
	requireQuotaRefusal(t, quotaInsert(t, p, signer, 2, payload, asOwner), quotaDimensionStatements)
	// The signer's own budget is untouched.
	if err := quotaInsert(t, p, signer, 3, payload, nil); err != nil {
		t.Fatalf("signer INSERT for itself: %v", err)
	}
}
