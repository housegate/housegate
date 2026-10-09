package sistatement

import (
	"context"
	"testing"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/plugin"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

func TestReadModeInjector(t *testing.T) {
	inject := &ReadModeInjector{Mode: "safe"}
	for sql, want := range map[string]bool{
		"SELECT count() FROM devuser1.t":               true,
		"  -- c\nwith x AS (SELECT 1) SELECT * FROM x": true,
		"INSERT INTO devuser1.t FORMAT CSV":            false,
		"SHOW TABLES":                                  false,
	} {
		q := &plugin.QueryContext{Query: &chproto.Query{Body: sql}}
		if err := inject.OnQuery(context.Background(), q); err != nil {
			t.Fatal(err)
		}
		got := len(q.Query.Settings) == 1 && q.Query.Settings[0].Key == sicore.ReadModeSettingKey && q.Query.Settings[0].Value == "'safe'" && q.Query.Settings[0].Custom
		if got != want {
			t.Errorf("%q: injected=%v settings=%+v, want %v", sql, got, q.Query.Settings, want)
		}
	}
	own := &plugin.QueryContext{Query: &chproto.Query{Body: "SELECT 1", Settings: []chproto.Setting{{Key: sicore.ReadModeSettingKey, Value: "'unsafe_latest'", Custom: true}}}}
	_ = inject.OnQuery(context.Background(), own)
	if len(own.Query.Settings) != 1 || own.Query.Settings[0].Value != "'unsafe_latest'" {
		t.Fatalf("a client-chosen read mode must win: %+v", own.Query.Settings)
	}
}
