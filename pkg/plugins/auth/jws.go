// Package auth implements JWSValidatorPlugin, which authenticates each
// incoming Query by delegating to a Validator (typically EthValidator) and
// stores the recovered identity in SessionState for downstream plugins.
//
// A non-nil error from the validator short-circuits the OnQuery chain;
// Relay surfaces it to the client as a synthetic ClickHouse Exception.
package authplugin

import (
	"context"
	"fmt"
	"strings"

	log "sentioxyz/sentio-core/common/log"

	"housegate/housegate/pkg/auth"
	"housegate/housegate/pkg/chsession"
	"housegate/housegate/pkg/network"
	"housegate/housegate/pkg/plugin"
)

// Plugin authenticates queries using the supplied Validator.
//
// A nil Validator makes the plugin a no-op (every query passes), matching
// the legacy "auth disabled" behaviour.
//
// State, when non-nil, gates the operator-on-behalf-of-owner path: if a
// query carries a SQL_x_payer setting whose value differs from the
// recovered JWS signer, the plugin validates the operator-of-owner
// relation via State.IsOperator and (on success) writes the owner address
// into qctx.Owner so downstream plugins (rewrite, commitgate) gate
// against the owner's permissions instead of the signer's. A nil State
// disables this resolution entirely — SQL_x_payer is then ignored at
// this layer and only the legacy commitgate-side path applies.
type Plugin struct {
	Validator auth.Validator
	State     network.State
}

func (p *Plugin) OnQuery(ctx context.Context, qctx *plugin.QueryContext) error {
	if p.Validator == nil {
		return nil
	}
	sess := qctx.Session
	state := sess.State()
	snap := state.Snapshot()

	var clientAddr string
	if addr := sess.RemoteAddr(); addr != nil {
		clientAddr = addr.String()
	}

	settings := make(map[string]string, len(qctx.Query.Settings))
	for _, s := range qctx.Query.Settings {
		settings[s.Key] = s.Value
	}

	meta := auth.QueryMeta{
		ConnID:       sess.ID(),
		ClientAddr:   clientAddr,
		UpstreamAddr: "", // not exposed by Session; left empty for now
		SQL:          qctx.OriginalSQL,
		QueryPreview: snap.LastQueryID,
		Settings:     settings,
	}

	res, err := p.Validator.ValidateQuery(ctx, meta)
	if err != nil {
		return fmt.Errorf("jws validation: %w", err)
	}

	if res.Address != "" {
		state.Identity = chsession.IdentityClaims{UserID: res.Address}
		_, logger := log.FromContext(ctx)
		logger.Debugw("jws validator: query authenticated",
			"address", res.Address,
			"maintenance", res.Maintenance,
			"platform_operator", res.PlatformOperator,
		)
	}
	// Maintenance bypass: the validator only signals true after a
	// successful JWS verify + allow-list match, so this write happens
	// strictly on the success path. Downstream plugins (rewrite, usage,
	// commitgate) read state.Snapshot().Maintenance and short-circuit to
	// forward verbatim.
	if res.Maintenance {
		state.SetMaintenance(true)
	}
	// Platform-operator bypass: same shape as maintenance, gated on a
	// distinct allowlist. Downstream plugins consult
	// Snapshot().PlatformOperator the same way.
	if res.PlatformOperator {
		state.SetPlatformOperator(true)
	}

	// Operator-on-behalf-of-owner resolution. Skip on the bypass paths —
	// maintenance / platform-operator sessions short-circuit every gate
	// downstream, so resolving an effective principal there is wasted
	// work and would surface confusing log lines about non-existent
	// operator relations.
	if !res.Maintenance && !res.PlatformOperator {
		if err := p.resolveOwner(ctx, qctx, res.Address, settings); err != nil {
			return err
		}
	}
	return nil
}

// resolveOwner reads SQL_x_payer from the per-query Settings, validates
// the operator-of-owner relation via State.IsOperator, and writes the
// owner address into qctx.Owner. Returns a non-nil error iff the relation
// is required (Owner present, State wired) but does not validate — that
// rejects the query at the auth layer before any downstream plugin
// (rewriter, commitgate) reads owner-scoped state.
//
// Falls back to a no-op when:
//   - SQL_x_payer is absent.
//   - SQL_x_payer equals the signer (legacy single-key sidecar path).
//   - The signer is empty (anonymous / auth-disabled deployments).
//   - State is nil (no IsOperator backend wired). In this case the
//     legacy commitgate-side validation path remains the sole gate.
func (p *Plugin) resolveOwner(ctx context.Context, qctx *plugin.QueryContext, signer string, settings map[string]string) error {
	raw, ok := settings[auth.PayerSettingKey]
	if !ok {
		return nil
	}
	owner := strings.ToLower(strings.Trim(strings.TrimSpace(raw), "\"'"))
	if owner == "" {
		return nil
	}
	if signer == "" {
		// SQL_x_payer arrived but no signer was recovered — auth is
		// either disabled or the validator returned an anonymous
		// result. Refusing here is fail-safe: any downstream consumer
		// that trusts qctx.Owner without re-checking IsOperator would
		// otherwise gate against an arbitrary attacker-supplied
		// address.
		return fmt.Errorf("auth: SQL_x_payer requires an authenticated signer")
	}
	if owner == strings.ToLower(signer) {
		return nil
	}
	if p.State == nil {
		// No State wired — we cannot validate IsOperator at this
		// layer. Leave qctx.Owner empty so the rewriter falls back to
		// the signer's perms and let the commitgate observer's own
		// IsOperator check (if wired) gate the DDL/DCL.
		_, logger := log.FromContext(ctx)
		logger.Debugw("auth: SQL_x_payer present but State unwired; skipping owner resolution",
			"signer", signer, "owner", owner)
		return nil
	}
	if !p.State.IsOperator(network.AccountAddress(owner), network.AccountAddress(signer)) {
		return fmt.Errorf("auth: %s is not an operator of %s", signer, owner)
	}
	qctx.Owner = owner
	return nil
}

// RunOnPeerTrust opts the auth plugin out of peer-trusted sessions.
// The inner SQL arriving on a peer-trusted connection is the upstream
// rewriter's already-rewritten form — its Keccak256 hash cannot match
// any client-signed JWS, so qhash validation would false-reject every
// peer query. The originating client's identity was already validated
// at the upstream proxy and the peer-relay JWS in the ClientHello
// password authenticated this leg at OnHello time.
func (p *Plugin) RunOnPeerTrust() bool { return false }

var (
	_ plugin.QueryPlugin    = (*Plugin)(nil)
	_ plugin.PeerTrustAware = (*Plugin)(nil)
)
