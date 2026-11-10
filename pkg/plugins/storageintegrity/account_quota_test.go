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
	if refusal, admitted := q.Admit(azSigner, 0); refusal != nil || admitted {
		t.Fatalf("nil quota Admit = %v, %v; want nil, false", refusal, admitted)
	}
	q.Settle(azSigner, 0, 1<<30) // must not panic
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
		if refusal, admitted := q.Admit(azSigner, 0); refusal != nil || !admitted {
			t.Fatalf("statement %d within burst: refusal %v admitted %v", i, refusal, admitted)
		}
	}
	refusal, admitted := q.Admit(azSigner, 0)
	if admitted || refusal == nil || refusal.Dimension != quotaDimensionStatements || refusal.RetryAfter != time.Second {
		t.Fatalf("third statement = %+v admitted %v; want statements refusal retrying in 1s", refusal, admitted)
	}
	clock.advance(time.Second)
	if refusal, admitted := q.Admit(azSigner, 0); refusal != nil || !admitted {
		t.Fatalf("after refill: refusal %v admitted %v", refusal, admitted)
	}
}

func TestAccountQuota_ByteDebtRefusesUntilRepaid(t *testing.T) {
	q, clock := newTestQuota(t, AccountQuotaEnforce, 100, 10)
	if refusal, _ := q.Admit(azSigner, 25); refusal != nil {
		t.Fatalf("first statement refused: %+v", refusal)
	}
	// A reservation larger than the burst is taken in full, into debt, and
	// settling it to the same length leaves the debt.
	q.Settle(azSigner, 25, 25)
	refusal, admitted := q.Admit(azSigner, 0)
	if admitted || refusal == nil || refusal.Dimension != quotaDimensionBytes || refusal.RetryAfter != 3*time.Second {
		t.Fatalf("statement in debt = %+v admitted %v; want bytes refusal retrying in 3s", refusal, admitted)
	}
	clock.advance(2 * time.Second)
	if refusal, _ := q.Admit(azSigner, 0); refusal == nil {
		t.Fatal("statement admitted before the debt was repaid")
	}
	clock.advance(time.Second)
	if refusal, admitted := q.Admit(azSigner, 0); refusal != nil || !admitted {
		t.Fatalf("statement after repayment: refusal %v admitted %v", refusal, admitted)
	}
}

func TestAccountQuota_KeysAreCaseInsensitiveAndIndependent(t *testing.T) {
	q, _ := newTestQuota(t, AccountQuotaEnforce, 1, 100)
	if refusal, _ := q.Admit(strings.ToUpper(azSigner[2:]), 0); refusal != nil {
		t.Fatalf("first: %+v", refusal)
	}
	if refusal, _ := q.Admit(strings.ToUpper(azSigner[2:]), 0); refusal == nil {
		t.Fatal("same account under another case got a fresh bucket")
	}
	if refusal, _ := q.Admit(azOwner, 0); refusal != nil {
		t.Fatalf("another account shares the bucket: %+v", refusal)
	}
}

func TestAccountQuota_DryRunTakesNothingFromRefusedStatements(t *testing.T) {
	q, clock := newTestQuota(t, AccountQuotaDryRun, 1, 100)
	if refusal, admitted := q.Admit(azSigner, 0); refusal != nil || !admitted {
		t.Fatalf("first: refusal %v admitted %v", refusal, admitted)
	}
	for i := 0; i < 3; i++ {
		if refusal, admitted := q.Admit(azSigner, 0); refusal == nil || admitted {
			t.Fatalf("over budget %d: refusal %v admitted %v; want a would-refuse", i, refusal, admitted)
		}
	}
	// The would-be refusals took no tokens, exactly as enforce mode would.
	clock.advance(time.Second)
	if refusal, admitted := q.Admit(azSigner, 0); refusal != nil || !admitted {
		t.Fatalf("after refill: refusal %v admitted %v", refusal, admitted)
	}
}

func TestAccountQuota_SweepDropsOnlyFullBuckets(t *testing.T) {
	q, clock := newTestQuota(t, AccountQuotaEnforce, 100, 1000)
	q.Admit(azSigner, 0)
	q.Admit(azOwner, 1000+5*int64Seconds(accountQuotaSweepInterval)+1) // still outstanding
	clock.advance(accountQuotaSweepInterval)
	q.Admit(azAdmin, 0) // triggers the sweep
	q.mu.Lock()
	_, signerKept := q.buckets[azSigner]
	_, ownerKept := q.buckets[azOwner]
	_, adminKept := q.buckets[azAdmin]
	q.mu.Unlock()
	if signerKept {
		t.Fatal("a refilled bucket was kept")
	}
	if !ownerKept {
		t.Fatal("a bucket with an outstanding hold was dropped")
	}
	if !adminKept {
		t.Fatal("the bucket just used was dropped")
	}
}

// Every admitted, unfinished statement holds its reservation, so concurrent
// statements of one account cannot all pass on the same balance; settling
// and releasing return what was not used.
func TestAccountQuota_ReservationsBoundConcurrentStatements(t *testing.T) {
	q, _ := newTestQuota(t, AccountQuotaEnforce, 100, 10)
	if refusal, admitted := q.Admit(azSigner, 8); refusal != nil || !admitted { // 10 -> 2
		t.Fatalf("first: refusal %v admitted %v", refusal, admitted)
	}
	if refusal, admitted := q.Admit(azSigner, 8); refusal != nil || !admitted { // 2 -> -6
		t.Fatalf("second: refusal %v admitted %v", refusal, admitted)
	}
	if refusal, admitted := q.Admit(azSigner, 8); admitted || refusal == nil || refusal.Dimension != quotaDimensionBytes {
		t.Fatalf("third while two are in flight: refusal %+v admitted %v; want a bytes refusal", refusal, admitted)
	}
	q.Settle(azSigner, 8, 4) // first completes with 4 bytes: -6 -> -2
	if refusal, _ := q.Admit(azSigner, 8); refusal == nil {
		t.Fatal("admitted while still in debt")
	}
	q.Settle(azSigner, 8, 0) // second ends unfinished: -2 -> 6
	if refusal, admitted := q.Admit(azSigner, 8); refusal != nil || !admitted {
		t.Fatalf("after release: refusal %v admitted %v", refusal, admitted)
	}
	// Releasing a hold never adds credit.
	q.Settle(azOwner, 1000, 0)
	q.mu.Lock()
	got := q.buckets[azOwner]
	q.mu.Unlock()
	if got.bytes != 10 || got.held != 0 {
		t.Fatalf("released bucket = %+v, want credit 10 and no hold", got)
	}
}

// Refill must not replace capacity an outstanding hold claims, and dropping
// an old hold after refill must not bank extra credit (second review finding).
func TestAccountQuota_RefillDoesNotReplaceHeldCapacity(t *testing.T) {
	q, clock := newTestQuota(t, AccountQuotaEnforce, 100, 10) // 5 B/s
	const maxPayload = 8
	// Two unfinished inputs hold 16 of the 10-byte burst.
	for i := 0; i < 2; i++ {
		if refusal, _ := q.Admit(azSigner, maxPayload); refusal != nil {
			t.Fatalf("hold %d: %+v", i, refusal)
		}
	}
	clock.advance(10 * time.Second) // refill would add 50; credit stays capped at 10
	used := uint64(0)
	write := func() bool {
		if refusal, _ := q.Admit(azSigner, maxPayload); refusal != nil {
			return false
		}
		q.Settle(azSigner, maxPayload, maxPayload)
		used += maxPayload
		return true
	}
	if write() {
		t.Fatal("a fresh write was admitted while refill-restored credit was still held")
	}
	// Close each old input, then write as much as admission allows.
	for i := 0; i < 2; i++ {
		q.Settle(azSigner, maxPayload, 0)
		for write() {
		}
	}
	if limit := uint64(10 + maxPayload); used > limit {
		t.Fatalf("fresh writes at one instant used %d bytes, above burst + max payload %d", used, limit)
	}
	if used == 0 {
		t.Fatal("no fresh write was admitted once the holds were released")
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

// Concurrent sessions of one principal each hold a max_payload_bytes
// reservation from OnQuery until their input completes or they end, so they
// cannot all pass admission on the same balance (review of the first cut).
func TestIngressAccountQuotaReservesPayloadForUnfinishedSessions(t *testing.T) {
	q, _ := newTestQuota(t, AccountQuotaEnforce, 100, 10)
	p, signer := newSignedIngressWithConfig(t, Config{AdmissionConsumer: &recordingConsumer{}, AccountQuota: q, MaxPayloadBytes: 8})
	sql := "INSERT INTO tenant.events FORMAT Native"
	open := func(session int64, seq uint64) (*plugin.QueryContext, error) {
		qctx := signedQueryContext(t, session, signer, sql, sql, sqlmeta.StatementTypeInsert)
		qctx.Query.ID = statementIDFor(signer, seq)
		qctx.AccessedTables = []sqlmeta.AccessedTable{{IsStorageIntegrity: true, OriginalDatabase: "tenant", OriginalTable: "events"}}
		withDefaultCaptureToken(t, qctx, signer, []byte{byte(chproto.ClientDataCode), 0, 0xab, 0xcd})
		return qctx, p.OnQuery(context.Background(), qctx)
	}
	first, err := open(80, 1) // 10 -> 2
	if err != nil {
		t.Fatalf("first session: %v", err)
	}
	second, err := open(81, 2) // 2 -> -6
	if err != nil {
		t.Fatalf("second session: %v", err)
	}
	_, err = open(82, 3)
	requireQuotaRefusal(t, err, quotaDimensionBytes)

	// The first completes with a 4-byte payload: -6 + 8 - 4 = -2, still in debt.
	payload := []byte{byte(chproto.ClientDataCode), 0, 0xab, 0xcd}
	if err := p.OnClientDataStrict(context.Background(), first, payload); err != nil {
		t.Fatal(err)
	}
	if err := p.OnQueryInputCompleteStrict(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	_, err = open(82, 3)
	requireQuotaRefusal(t, err, quotaDimensionBytes)

	// The second is aborted before its input completes: its reservation returns.
	p.OnQueryAbort(context.Background(), second)
	if _, err := open(82, 3); err != nil {
		t.Fatalf("session after the abort released its reservation: %v", err)
	}
	// Closing a session with an unfinished statement releases it the same way.
	p.OnClose(&fakeSession{id: 82})
	q.mu.Lock()
	got := q.buckets[azSignerOf(signer)].bytes
	q.mu.Unlock()
	if got != 6 {
		t.Fatalf("byte bucket after close = %v, want 6", got)
	}
}

func azSignerOf(signer *auth.RelaySigner) string { return strings.ToLower(signer.Address()) }
