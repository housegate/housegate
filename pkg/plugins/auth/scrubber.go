package authplugin

import (
	"context"

	"housegate/housegate/pkg/auth"
	"housegate/housegate/pkg/chproto"
	"housegate/housegate/pkg/log"
	"housegate/housegate/pkg/plugin"
)

// InternalSettingsScrubber removes HouseGate-only query settings after
// downstream plugins have consumed them and before Relay forwards the Query to
// ClickHouse. ClickHouse does not need these credentials and may reject unknown
// settings in minimal test/prod profiles.
type InternalSettingsScrubber struct{}

func (InternalSettingsScrubber) OnQuery(ctx context.Context, qctx *plugin.QueryContext) error {
	if qctx == nil || qctx.Query == nil || len(qctx.Query.Settings) == 0 {
		return nil
	}
	settings := qctx.Query.Settings
	kept := settings[:0]
	removed := 0
	for _, setting := range settings {
		if isInternalHouseGateSetting(setting.Key) {
			removed++
			continue
		}
		kept = append(kept, setting)
	}
	clearSettings(settings[len(kept):])
	qctx.Query.Settings = kept
	if removed > 0 {
		_, logger := log.FromContext(ctx)
		logger.Debugw("scrubbed housegate internal query settings", "removed", removed, "remaining", len(kept))
	}
	return nil
}

// RunOnRouted lets the scrubber remove the caller's original credentials before
// routeplugin.Signer appends the relay token for the target HouseGate.
func (InternalSettingsScrubber) RunOnRouted() bool { return true }

func isInternalHouseGateSetting(key string) bool {
	switch key {
	case auth.AuthTokenSettingKey,
		auth.PayerSettingKey,
		auth.MaintenanceSettingKey,
		auth.PlatformOperatorSettingKey,
		auth.DriverSettingKey:
		return true
	default:
		return false
	}
}

func clearSettings(settings []chproto.Setting) {
	for i := range settings {
		settings[i] = chproto.Setting{}
	}
}

var (
	_ plugin.QueryPlugin = InternalSettingsScrubber{}
	_ plugin.RouteAware  = InternalSettingsScrubber{}
)
