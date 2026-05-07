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

	log "sentioxyz/sentio-core/common/log"

	"housegate/housegate/pkg/chsession"
	"housegate/housegate/pkg/plugin"
	"housegate/housegate/pkg/auth"
)

// Plugin authenticates queries using the supplied Validator.
//
// A nil Validator makes the plugin a no-op (every query passes), matching
// the legacy "auth disabled" behaviour.
type Plugin struct {
	Validator auth.Validator
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
