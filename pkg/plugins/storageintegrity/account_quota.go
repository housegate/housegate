package storageintegrity

import (
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/housegate/housegate/pkg/chproto"
)

// AccountQuotaMode selects what the per-account write quota does with a
// statement over budget.
type AccountQuotaMode string

const (
	// AccountQuotaOff disables the quota: nothing is tracked.
	AccountQuotaOff AccountQuotaMode = "off"
	// AccountQuotaDryRun tracks every account and counts the statements the
	// quota would refuse, but admits them.
	AccountQuotaDryRun AccountQuotaMode = "dry_run"
	// AccountQuotaEnforce refuses a statement over budget with 201
	// QUOTA_EXCEEDED before its payload is read.
	AccountQuotaEnforce AccountQuotaMode = "enforce"
)

// ParseAccountQuotaMode maps a configured mode to its value; empty is off.
func ParseAccountQuotaMode(raw string) (AccountQuotaMode, error) {
	switch mode := AccountQuotaMode(strings.TrimSpace(raw)); mode {
	case "", AccountQuotaOff:
		return AccountQuotaOff, nil
	case AccountQuotaDryRun, AccountQuotaEnforce:
		return mode, nil
	default:
		return "", fmt.Errorf("account quota mode %q must be off, dry_run or enforce", raw)
	}
}

// Dimensions of a quota refusal, also the dimension label of
// storage_integrity_ingress_account_quota_total.
const (
	quotaDimensionStatements = "statements"
	quotaDimensionBytes      = "bytes"
)

// Decision labels of storage_integrity_ingress_account_quota_total.
const (
	quotaDecisionRefused     = "refused"
	quotaDecisionWouldRefuse = "would_refuse"
)

var (
	accountQuotaTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "storage_integrity_ingress_account_quota_total",
		Help: "Signed storage-integrity statements over their account's write quota, by exhausted dimension and decision (refused in enforce mode, would_refuse in dry_run).",
	}, []string{"dimension", "decision"})
	accountQuotaAccounts = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "storage_integrity_ingress_account_quota_accounts",
		Help: "Accounts whose write-quota buckets the storage-integrity ingress currently tracks.",
	})
)

func init() { prometheus.MustRegister(accountQuotaTotal, accountQuotaAccounts) }

// accountQuotaSweepInterval is how often idle, fully refilled buckets are
// dropped. Only a full bucket is dropped, so forgetting it grants nothing.
const accountQuotaSweepInterval = time.Minute

// AccountQuotaConfig is the per-account signed-write budget of one ingress:
// two token buckets per principal, one counting statements and one counting
// payload bytes. Rates are per second; a burst is the bucket's capacity.
type AccountQuotaConfig struct {
	Mode                AccountQuotaMode
	StatementsPerSecond float64
	StatementBurst      float64
	BytesPerSecond      float64
	ByteBurst           float64
	// Now is the clock; nil uses time.Now. Tests inject it.
	Now func() time.Time
}

// AccountQuota is an in-memory, node-local write budget keyed by the
// statement's principal (the validated owner, else the signer). It is a soft
// limit: counters are not shared with other ingresses and reset on restart.
//
// A statement takes one statement token when it is admitted and is charged
// its payload bytes once its input is complete. The byte bucket may go into
// debt, so a statement whose payload has already been read is never wasted;
// the account's next statement is refused until the debt is repaid. Sustained
// throughput is therefore bounded by the rates, and a burst by the bucket
// capacities plus one maximum payload.
type AccountQuota struct {
	cfg AccountQuotaConfig
	now func() time.Time

	mu        sync.Mutex
	buckets   map[string]*accountBucket
	lastSweep time.Time
}

type accountBucket struct {
	statements float64
	bytes      float64
	updated    time.Time
}

// QuotaRefusal describes a statement over its account's budget.
type QuotaRefusal struct {
	Account    string
	Dimension  string
	RetryAfter time.Duration
}

// NewAccountQuota returns nil for mode off, which every method accepts.
func NewAccountQuota(cfg AccountQuotaConfig) (*AccountQuota, error) {
	mode, err := ParseAccountQuotaMode(string(cfg.Mode))
	if err != nil {
		return nil, err
	}
	if mode == AccountQuotaOff {
		return nil, nil
	}
	cfg.Mode = mode
	switch {
	case !(cfg.StatementsPerSecond > 0) || math.IsInf(cfg.StatementsPerSecond, 0):
		return nil, fmt.Errorf("account quota statements_per_second must be a positive number, got %v", cfg.StatementsPerSecond)
	case !(cfg.StatementBurst >= 1) || math.IsInf(cfg.StatementBurst, 0):
		return nil, fmt.Errorf("account quota statement_burst must be at least 1, got %v", cfg.StatementBurst)
	case !(cfg.BytesPerSecond > 0) || math.IsInf(cfg.BytesPerSecond, 0):
		return nil, fmt.Errorf("account quota bytes_per_second must be a positive number, got %v", cfg.BytesPerSecond)
	case !(cfg.ByteBurst > 0) || math.IsInf(cfg.ByteBurst, 0):
		return nil, fmt.Errorf("account quota byte_burst must be a positive number, got %v", cfg.ByteBurst)
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &AccountQuota{cfg: cfg, now: now, buckets: map[string]*accountBucket{}, lastSweep: now()}, nil
}

// Mode reports the quota's mode; a nil quota is off.
func (q *AccountQuota) Mode() AccountQuotaMode {
	if q == nil {
		return AccountQuotaOff
	}
	return q.cfg.Mode
}

// Admit decides one statement of account. A statement within budget takes a
// statement token and returns (nil, true). A statement over budget takes
// nothing and returns its refusal with admitted false, in both modes, so a
// dry run counts what enforce mode would charge; the caller decides whether
// to refuse it. A nil quota admits everything without tracking it.
func (q *AccountQuota) Admit(account string) (refusal *QuotaRefusal, admitted bool) {
	if q == nil {
		return nil, false
	}
	account = strings.ToLower(strings.TrimSpace(account))
	q.mu.Lock()
	defer q.mu.Unlock()
	now := q.now()
	q.sweepLocked(now)
	b := q.bucketLocked(account, now)
	switch {
	case b.statements < 1:
		refusal = &QuotaRefusal{Account: account, Dimension: quotaDimensionStatements,
			RetryAfter: secondsUntil(1-b.statements, q.cfg.StatementsPerSecond)}
	case b.bytes < 0:
		refusal = &QuotaRefusal{Account: account, Dimension: quotaDimensionBytes,
			RetryAfter: secondsUntil(-b.bytes, q.cfg.BytesPerSecond)}
	default:
		b.statements--
		return nil, true
	}
	decision := quotaDecisionRefused
	if q.cfg.Mode == AccountQuotaDryRun {
		decision = quotaDecisionWouldRefuse
	}
	accountQuotaTotal.WithLabelValues(refusal.Dimension, decision).Inc()
	return refusal, false
}

// ChargeBytes debits n payload bytes from account's byte bucket, possibly into
// debt. Callers charge only statements Admit admitted.
func (q *AccountQuota) ChargeBytes(account string, n uint64) {
	if q == nil || n == 0 {
		return
	}
	account = strings.ToLower(strings.TrimSpace(account))
	q.mu.Lock()
	defer q.mu.Unlock()
	b := q.bucketLocked(account, q.now())
	b.bytes -= float64(n)
}

// bucketLocked returns account's bucket refilled to now, creating a full one.
func (q *AccountQuota) bucketLocked(account string, now time.Time) *accountBucket {
	b := q.buckets[account]
	if b == nil {
		b = &accountBucket{statements: q.cfg.StatementBurst, bytes: q.cfg.ByteBurst, updated: now}
		q.buckets[account] = b
		accountQuotaAccounts.Set(float64(len(q.buckets)))
		return b
	}
	q.refill(b, now)
	return b
}

func (q *AccountQuota) refill(b *accountBucket, now time.Time) {
	elapsed := now.Sub(b.updated).Seconds()
	if elapsed <= 0 {
		return
	}
	b.statements = math.Min(q.cfg.StatementBurst, b.statements+elapsed*q.cfg.StatementsPerSecond)
	b.bytes = math.Min(q.cfg.ByteBurst, b.bytes+elapsed*q.cfg.BytesPerSecond)
	b.updated = now
}

// sweepLocked drops buckets that have refilled completely: a new bucket starts
// full, so dropping one changes no decision and only bounds memory.
func (q *AccountQuota) sweepLocked(now time.Time) {
	if now.Sub(q.lastSweep) < accountQuotaSweepInterval {
		return
	}
	q.lastSweep = now
	for account, b := range q.buckets {
		q.refill(b, now)
		if b.statements >= q.cfg.StatementBurst && b.bytes >= q.cfg.ByteBurst {
			delete(q.buckets, account)
		}
	}
	accountQuotaAccounts.Set(float64(len(q.buckets)))
}

// secondsUntil is how long rate takes to refill deficit, at least a second.
func secondsUntil(deficit, rate float64) time.Duration {
	seconds := math.Ceil(deficit / rate)
	if seconds < 1 || math.IsNaN(seconds) {
		seconds = 1
	}
	return time.Duration(seconds) * time.Second
}

// clientError is the 201 QUOTA_EXCEEDED refusal of r. It is raised before the
// statement's payload is read or anything is submitted, so the client_seq
// coordinate is unspent and the relay keeps the session.
func (r *QuotaRefusal) clientError() error {
	return &chproto.ClientError{
		Code:       chproto.CodeQuotaExceeded,
		Message:    chproto.AccountQuotaExceededMessage(r.Account, r.Dimension, r.RetryAfter),
		SeqUnspent: true,
	}
}
