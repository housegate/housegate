package sistatement

import (
	"context"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/plugin"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

// ReadModeInjector adds SQL_x_read_mode to SELECT / WITH statements that do
// not choose one (spec 2026-10-09 §6.4 -si-read-mode). Other statements and a
// client-chosen mode are left alone; the setting is owned, so the SI lane's
// empty-settings rule does not see it.
type ReadModeInjector struct {
	Mode string
}

// OnQuery implements plugin.QueryPlugin.
func (r *ReadModeInjector) OnQuery(_ context.Context, qctx *plugin.QueryContext) error {
	if r == nil || r.Mode == "" || qctx == nil || qctx.Query == nil {
		return nil
	}
	for _, setting := range qctx.Query.Settings {
		if setting.Key == sicore.ReadModeSettingKey {
			return nil
		}
	}
	words, err := sicore.LeadingKeywords(qctx.Query.Body, 1)
	if err != nil || len(words) == 0 || (words[0] != "SELECT" && words[0] != "WITH") {
		return nil
	}
	qctx.Query.Settings = append(qctx.Query.Settings, chproto.Setting{Key: sicore.ReadModeSettingKey, Value: "'" + r.Mode + "'", Custom: true})
	return nil
}

var _ plugin.QueryPlugin = (*ReadModeInjector)(nil)
