package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	rewritergo "github.com/housegate/rewriter-go"
	pb "github.com/housegate/rewriter-proto/gen/pb"

	"github.com/housegate/housegate/pkg/plugins/querysettings"
)

// TestQuerySettings_RefusedInTheQueryPacket is spec 2026-09-26 §9.7: R5
// settings sent in the native Query packet are refused before forwarding,
// with the engines' value rule for the analyzer switch.
func TestQuerySettings_RefusedInTheQueryPacket(t *testing.T) {
	proxy := startTableRefProxy(t, "phys_qs", "enforce")
	conn := openConn(t, proxy.Addr)
	ctx := context.Background()
	for _, tc := range []struct {
		name     string
		settings clickhouse.Settings
		refused  string
	}{
		{"analyzer off", clickhouse.Settings{"enable_analyzer": 0}, "enable_analyzer"},
		{"experimental analyzer false", clickhouse.Settings{"allow_experimental_analyzer": false}, "allow_experimental_analyzer"},
		{"legacy tuple names", clickhouse.Settings{"legacy_column_name_of_tuple_literal": 0}, "legacy_column_name_of_tuple_literal"},
		{"global WITH off", clickhouse.Settings{"enable_global_with_statement": 0}, "enable_global_with_statement"},
		{"compatibility", clickhouse.Settings{"compatibility": "21.1"}, "compatibility"},
		{"profile", clickhouse.Settings{"profile": "default"}, "profile"},
		{"implicit table", clickhouse.Settings{"implicit_table_at_top_level": "x"}, "implicit_table_at_top_level"},
		{"dialect", clickhouse.Settings{"dialect": "kusto"}, "dialect"},
		{"analyzer on", clickhouse.Settings{"enable_analyzer": 1}, ""},
		{"analyzer true", clickhouse.Settings{"enable_analyzer": true}, ""},
		{"ordinary setting", clickhouse.Settings{"max_threads": 2}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var one uint8
			err := conn.QueryRow(clickhouse.Context(ctx, clickhouse.WithSettings(tc.settings)), "SELECT 1").Scan(&one)
			if tc.refused == "" {
				if err != nil {
					t.Fatalf("err = %v, want accepted", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), "table setting "+tc.refused+" is not accepted") {
				t.Fatalf("err = %v, want the %s refusal", err, tc.refused)
			}
		})
	}
	var one uint8
	if err := conn.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil {
		t.Fatalf("the session must survive the refusals: %v", err)
	}
}

// TestQuerySettings_MatchTheEngineForEverySetting keeps querysettings in
// lockstep with the pinned engine: for every setting ClickHouse knows (and
// every name housegate lists), housegate refuses a value in the Query packet
// exactly when the engine refuses the same assignment in SQL.
func TestQuerySettings_MatchTheEngineForEverySetting(t *testing.T) {
	lib := requireNativeLib(t)
	ctx := context.Background()
	svc, err := rewritergo.NewService(lib)
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()
	rows, err := openConnNoDB(t, chEnv.Addr).Query(ctx, "SELECT name FROM system.settings")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(names) < 500 {
		t.Fatalf("system.settings returned %d names", len(names))
	}
	names = append(names, querysettings.RefusedNames()...)
	t.Logf("comparing %d setting names (system.settings plus housegate's list) with values 0 and 1", len(names))
	args := &pb.RewriteTableDynamicArgs{
		DatabaseMap: map[string]string{"db1": "phys"}, KnownPhysicalDatabases: []string{"phys"},
		UpstreamLogicalDatabaseInContext: "db1", Delim: "_",
		ProtectedDatabases: []string{"phys", "hg_safe", "hg_unsafe", "hg_promote"},
	}
	for _, name := range names {
		for _, value := range []string{"0", "1"} {
			resp, err := svc.Rewrite(ctx, &pb.RewriteSQLRequest{
				Sql: fmt.Sprintf("SELECT 1 SETTINGS `%s` = %s", name, value),
				Options: []*pb.RewriteOption{{Op: pb.RewriteOp_TableNameRewrite,
					Value: &pb.RewriteOption_TableNameArgs{TableNameArgs: &pb.RewriteTableNameArgs{DynamicArgs: args}}}},
			})
			if err != nil {
				t.Fatal(err)
			}
			engineRefuses := resp.GetMessage() == "table setting "+name+" is not accepted"
			if !engineRefuses && resp.GetCode() != pb.RewriteCode_Success {
				t.Errorf("%s = %s: unexpected engine answer %s %q", name, value, resp.GetCode(), resp.GetMessage())
				continue
			}
			if got := querysettings.Refused(name, value); got != engineRefuses {
				t.Errorf("%s = %s: housegate refuses %v, engine refuses %v", name, value, got, engineRefuses)
			}
		}
	}
}
