// Package usage implements UsageClientPlugin, which gates each query on a
// balance check and reports usage to sentio-node.
//
// The plugin runs after authentication: it reads the recovered Ethereum
// signer from SessionState.Identity.UserID and resolves the payer from the
// per-query SQL_x_payer setting (falling back to the signer when absent).
//
// Anonymous queries — those without an authenticated signer — bypass
// billing entirely, matching the established behaviour for AllowNoAuth
// configurations.
package usage

import (
	"context"
	"fmt"
	"strings"

	"github.com/housegate/housegate/pkg/log"

	"github.com/housegate/housegate/pkg/billing"
	"github.com/housegate/housegate/pkg/plugin"
)

// payerSettingKey is the per-query Setting that overrides the payer.
// When absent, the authenticated signer pays for their own queries.
const payerSettingKey = "SQL_x_payer"

// Plugin gates queries on a CheckBalance call and reports usage on success.
// A nil Client makes the plugin a no-op.
type Plugin struct {
	Client billing.UsageClient
}

func (p *Plugin) OnQuery(ctx context.Context, qctx *plugin.QueryContext) error {
	_, logger := log.FromContext(ctx)
	if p.Client == nil {
		logger.Debugw("usage check: skipped, no client configured")
		return nil
	}
	if qctx.Session != nil {
		snap := qctx.Session.State().Snapshot()
		// Maintenance sessions (indexer-signed) bypass billing — both
		// CheckBalance and ReportUsage are skipped. The indexer issues
		// these ops on its own schedule (e.g. DatabaseGC table DROPs);
		// charging the indexer for its own maintenance would be both
		// pointless and metric-noisy. Platform-operator sessions get
		// the same bypass — they run privileged platform workflows
		// that should not be billed against the operator wallet.
		// Driver sessions (indexer-signed indexer-driver traffic) also
		// bypass: the driver's write workload is metered separately via
		// sentio-node's IndexingUsage path (AsyncSave gRPC →
		// UsageTracker.ReportIndexingUsage on chain), so per-query
		// CheckBalance/ReportUsage here would double-count and reject
		// against the indexer's own balance.
		if snap.Maintenance || snap.PlatformOperator || snap.IsDriver {
			logger.Debugw("usage check: privileged session bypass",
				"maintenance", snap.Maintenance,
				"platform_operator", snap.PlatformOperator,
				"is_driver", snap.IsDriver,
				"signer", snap.Identity.UserID,
			)
			return nil
		}
		// Forward-pivot peer sessions (IsPeerTrusted + IsForwardedFromPeer)
		// arrive on the host proxy after the entry proxy already ran
		// auth + usage on the same query. The chain's IsForwardedFromPeer
		// override forces auth to re-run here so rewrite has a real
		// signer to bind, but billing already happened upstream — running
		// CheckBalance/ReportUsage again would double-charge the user.
		if snap.IsForwardedFromPeer {
			logger.Debugw("usage check: forwarded-from-peer bypass, billed by entry proxy",
				"peer", snap.PeerAddress,
				"signer", snap.Identity.UserID,
			)
			return nil
		}
	}
	signer := qctx.Session.State().Snapshot().Identity.UserID
	if signer == "" {
		logger.Debugw("usage check: anonymous query bypass, no signer")
		return nil
	}

	payer := signer
	if qctx.Query != nil {
		for _, s := range qctx.Query.Settings {
			if s.Key == payerSettingKey && s.Value != "" {
				// clickhouse-go's CustomSetting routes Custom-flagged string
				// values through Field::restoreFromDump, which wraps them in
				// '...'. The receiver downstream (CheckQueryBalance →
				// HexToAddress) does not strip those quotes, so a wrapped
				// payer becomes the zero address and IsOperator returns
				// false → UNAUTHORIZED_SIGNER. Trim here to match the same
				// invariant as AuthTokenSettingKey / SQL_sentio_maintenance
				// in pkg/auth/eth_validator.go.
				payer = strings.Trim(s.Value, "\"'")
				break
			}
		}
	}

	logger.Debugw("usage check: calling CheckBalance", "payer", payer, "signer", signer)
	ok, reason, err := p.Client.CheckBalance(ctx, payer, signer)
	if err != nil {
		// CheckBalance fails open internally; an error here is unexpected.
		// Log and allow the query to keep behaviour predictable.
		logger.Warnfe(err, "usage check: unexpected error, allowing query payer=%v signer=%v", payer, signer)
		return nil
	}
	if !ok {
		code, name, msg := billing.RejectionException(reason, payer, signer)
		logger.Infow("usage check: query rejected",
			"code", code,
			"name", name,
			"payer", payer,
			"signer", signer,
		)
		return fmt.Errorf("%s (code=%d): %s", name, code, msg)
	}

	// ReportUsage is internally fire-and-forget with retries.
	logger.Debugw("usage check: query allowed, reporting usage", "payer", payer, "signer", signer)
	p.Client.ReportUsage(ctx, payer, signer, 1)
	return nil
}

var _ plugin.QueryPlugin = (*Plugin)(nil)
