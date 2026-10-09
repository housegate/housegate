package storageintegrity

import (
	"errors"
	"fmt"
	"strings"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/registry"
)

// Result labels of storage_integrity_ingress_authz_total (spec 2026-10-09 §10).
const (
	authzAllowed         = "allowed"
	authzDeniedSigner    = "denied_signer"
	authzDeniedOwner     = "denied_owner"
	authzOperatorInvalid = "operator_invalid"
	authzNotWriter       = "not_writer"
	authzUnknownDatabase = "unknown_database"
)

var authzTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "storage_integrity_ingress_authz_total",
	Help: "Storage-integrity ingress write authorization decisions, by result.",
}, []string{"result"})

func init() { prometheus.MustRegister(authzTotal) }

// errWriterAccessRequired is the wiring refusal: without a writer predicate
// the ingress cannot decide who may write, so it refuses every write.
var errWriterAccessRequired = errors.New("storage_integrity.ingress requires a registry that implements WriterAccess (contract isDatabaseWriter)")

// writeAuthorizer is spec 2026-10-09 §6.2: open writes for every database
// writer, a static denylist, and the optional legacy allowlist. It resolves
// the owner from SQL_x_payer itself, so it depends neither on auth.enabled nor
// on plugin order.
type writeAuthorizer struct {
	denied    map[string]bool
	allowed   map[string]bool
	writers   registry.WriterAccess
	operators registry.OperatorChecker
}

type authorization struct {
	signer    string
	owner     string // validated operator relation; empty when the signer writes for itself
	principal string // owner, else signer
}

func newWriteAuthorizer(denied, allowed []string, writers registry.WriterAccess, operators registry.OperatorChecker) *writeAuthorizer {
	return &writeAuthorizer{denied: addressSet(denied), allowed: addressSet(allowed), writers: writers, operators: operators}
}

func addressSet(addresses []string) map[string]bool {
	set := make(map[string]bool, len(addresses))
	for _, address := range addresses {
		if address = strings.ToLower(strings.TrimSpace(address)); address != "" {
			set[address] = true
		}
	}
	return set
}

// normalizeOwner applies authplugin.resolveOwner's convention: ClickHouse
// wraps a Custom string setting in quotes, and addresses compare lowercase.
func normalizeOwner(payerSetting string) string {
	return strings.ToLower(strings.Trim(strings.TrimSpace(payerSetting), "\"'"))
}

// claimedPrincipal is the principal a statement asks to write as: the
// SQL_x_payer owner when it names another account, else the signer. Refusal
// logs record it; it is not validated, so only authorize decides access.
func claimedPrincipal(signer, payerSetting string) string {
	signer = strings.ToLower(strings.TrimSpace(signer))
	if owner := normalizeOwner(payerSetting); owner != "" && owner != signer {
		return owner
	}
	return signer
}

func accessDenied(result, message string, cause error) (authorization, string, error) {
	return authorization{}, result, &chproto.ClientError{Code: chproto.CodeAccessDenied, Message: message, Err: cause}
}

// authorize returns the authorization, the metric result label and the
// refusal. Order is the spec's: denylisted signer, operator relation and
// denylisted owner, allowlist, then the writer predicate on the principal. A
// wiring defect returns an empty result label so it is not counted.
func (a *writeAuthorizer) authorize(signer, payerSetting, database string) (authorization, string, error) {
	if a == nil || a.writers == nil || a.operators == nil {
		return authorization{}, "", errWriterAccessRequired
	}
	signer = strings.ToLower(strings.TrimSpace(signer))
	if a.denied[signer] {
		return accessDenied(authzDeniedSigner, fmt.Sprintf("storage_integrity: signer %s is not permitted to write storage-integrity tables", signer), nil)
	}
	granted := authorization{signer: signer, principal: signer}
	if owner := normalizeOwner(payerSetting); owner != "" && owner != signer {
		if !a.operators.IsOperator(owner, signer) {
			return accessDenied(authzOperatorInvalid, fmt.Sprintf("storage_integrity: %s is not an operator of %s", signer, owner), nil)
		}
		if a.denied[owner] {
			return accessDenied(authzDeniedOwner, fmt.Sprintf("storage_integrity: owner %s is not permitted to write storage-integrity tables", owner), nil)
		}
		granted.owner, granted.principal = owner, owner
	}
	// An allowlist miss counts as denied_signer (spec 2026-10-09 §4.5 P12).
	if len(a.allowed) > 0 && !a.allowed[signer] {
		return accessDenied(authzDeniedSigner, fmt.Sprintf("storage_integrity: signer %s is not permitted to write storage-integrity tables", signer), nil)
	}
	notWriter := fmt.Sprintf("storage_integrity: %s is not a writer of database %s", granted.principal, database)
	ok, err := a.writers.IsDatabaseWriter(database, granted.principal)
	if err != nil {
		return accessDenied(authzUnknownDatabase, notWriter, err)
	}
	if !ok {
		return accessDenied(authzNotWriter, notWriter, nil)
	}
	return granted, authzAllowed, nil
}

func countAuthz(result string) {
	if result != "" {
		authzTotal.WithLabelValues(result).Inc()
	}
}
