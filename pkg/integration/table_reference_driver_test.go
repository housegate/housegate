package integration

import (
	"context"
	"strings"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	housegate "github.com/housegate/housegate"
	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/config"
	"github.com/housegate/housegate/pkg/integration/testenv"
	"github.com/housegate/housegate/pkg/registry"
)

// sentioDriverConnSettings is the connection-level settings template the
// Sentio indexer driver sends with every query (sentio-core
// clickhousemanager.NewConnSettingsMacro plus the driver binary's
// clickhousemanagerext.NewConnSettingsTemplate), minus the names ClickHouse
// 25.8 does not know yet.
func sentioDriverConnSettings() clickhouse.Settings {
	return clickhouse.Settings{
		"optimize_aggregation_in_order":                       1,
		"max_ast_depth":                                       50000,
		"max_partition_size_to_drop":                          uint64(536870912000),
		"max_table_size_to_drop":                              uint64(536870912000),
		"union_default_mode":                                  "ALL",
		"connect_timeout_with_failover_ms":                    120000,
		"query_cache_system_table_handling":                   "save",
		"output_format_native_write_json_as_string":           1,
		"allow_push_predicate_ast_for_distributed_subqueries": 0,
		"enable_json_type":                                    1,
		"query_cache_nondeterministic_function_handling":      "ignore",
		"max_partitions_per_insert_block":                     10240,
		"max_execution_time":                                  300,
		"max_query_size":                                      1048576,
	}
}

// TestTableReference_SentioDriverPhysicalHelloDatabase is the C1 regression
// of the 2026-10-01 final review. The deployed Sentio indexer driver
// (production charts/sentio-node: housegate_dsn
// clickhouse://default@<sidecar>/<physicalDatabase>) connects through the
// co-located agent sidecar (agent.driver: true) with the ClientHello
// database equal to rewriter.physical_database, and builds every statement
// from qualified logical names (chx.Controller.FullLogicName) plus
// system.* metadata reads that bind the physical name as a string literal.
//
// The physical database is not a logical database: the session must carry no
// logical context, so the engines rewrite qualified logical names, pass
// system.* reads and refuse unqualified names, instead of refusing every
// statement because the session context is a protected database. The guard
// (enforcing) and querysettings run on this session too.
func TestTableReference_SentioDriverPhysicalHelloDatabase(t *testing.T) {
	const (
		phys  = "phys_drv"
		logic = "p1_0" // NetworkV1: <processorID>_<replica>
	)
	indexer, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatal(err)
	}
	server := startTableRefProxy(t, phys, "enforce",
		testenv.WithExtraDatabases(logic),
		authProxyConfig([]string{indexer.Address()}, false),
		testenv.WithDatabasePermission(indexer.Address(), logic, registry.DbAuthOwner),
		func(_ *config.Config, opts *housegate.Options) { opts.Signer = indexer },
	)
	sidecar := testenv.StartAgentProxy(t, authTestKey1, server.Addr,
		testenv.WithConfigMutator(func(cfg *config.Config) { cfg.Agent.Driver = true }),
	)
	driver, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{sidecar.Addr},
		Auth: clickhouse.Auth{
			Database: phys,
			Username: chEnv.User,
			Password: chEnv.Password,
		},
		Protocol: clickhouse.Native,
		Settings: sentioDriverConnSettings(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = driver.Close() })
	ctx := context.Background()
	withSettings := func(s clickhouse.Settings) context.Context {
		return clickhouse.Context(ctx, clickhouse.WithSettings(s))
	}
	// exec runs one driver statement, which must succeed. The native engine
	// v0.16.0 refused four of these shapes as UnsupportedStatement (the
	// cluster probe's keyword column under NOT LIKE, the view and
	// materialized-view trailing COMMENT, and startsWith); rewriter-go
	// v0.17.0 answers every one Success, and the startup probe now refuses an
	// engine that does not.
	exec := func(ctx context.Context, sql string, args ...any) {
		t.Helper()
		if err := driver.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("driver %s: %v", sql, err)
		}
	}
	count := func(ctx context.Context, sql string, args ...any) uint64 {
		t.Helper()
		var n uint64
		if err := driver.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
			t.Fatalf("driver %s: %v", sql, err)
		}
		return n
	}
	prefix := logic + "."              // chx tableNamePrefix for NetworkV1
	likeArg := logic[:2] + "\\_0.%"    // chx.loadCondition escapes '_' and '%'
	full := "`" + logic + "`.`events`" // chx.FullLogicName
	agg := "`" + logic + "`.`agg`"

	// clickhousemanager helper.GetClusterStmt (chx.New → conn.GetCluster).
	// The driver swallows a failure here and would silently run without a
	// cluster, so the test requires it to succeed.
	exec(ctx, "SELECT cluster FROM ("+
		"SELECT cluster, count(*) AS rs, SUM(host_address = '127.0.0.1') AS cl "+
		"FROM system.clusters "+
		"WHERE cluster not like 'all-%' "+
		"GROUP BY cluster"+
		") WHERE cl > 0 AND rs > 1")
	if n := count(ctx, "SELECT count() FROM (SELECT cluster, count(*) AS rs FROM system.clusters GROUP BY cluster) WHERE rs > 1"); n != 0 {
		t.Fatalf("the test ClickHouse reports %d multi-replica clusters; the driver would go cluster-aware", n)
	}

	// driver/cmd/clickhouse.go Connect, then chx DDL (operator.go).
	exec(ctx, "CREATE DATABASE IF NOT EXISTS `"+logic+"`")
	exec(ctx, "CREATE TABLE "+full+" (`id` UInt64, `chain` String, `v` UInt64, "+
		"INDEX `idx_v` v TYPE minmax GRANULARITY 1) ENGINE = MergeTree() "+
		"PARTITION BY `chain` ORDER BY (`chain`,`id`) "+
		"SETTINGS enable_block_number_column=1,enable_block_offset_column=1 COMMENT 'events'")
	exec(ctx, "CREATE TABLE "+agg+" (`chain` String, `n` UInt64) ENGINE = MergeTree() ORDER BY (`chain`) COMMENT 'agg'")
	// chx.buildCreateViewSQL / buildCreateMaterializedViewSQL: parenthesised
	// bodies, a COMMENT on every column and a trailing COMMENT.
	exec(ctx, "CREATE OR REPLACE VIEW `"+logic+"`.`v_events` (`id` UInt64 COMMENT 'id', `v` UInt64 COMMENT 'v') "+
		"AS (SELECT `id`, `v` FROM "+full+") COMMENT 'view'")
	exec(ctx, "CREATE MATERIALIZED VIEW `"+logic+"`.`mv_agg` TO "+agg+" AS (SELECT `chain`, count() AS `n` FROM "+full+" GROUP BY `chain`) COMMENT 'mv'")
	var viewComment string
	if err := driver.QueryRow(ctx, "SELECT comment FROM system.tables WHERE database = ? AND name = ?", phys, prefix+"v_events").Scan(&viewComment); err != nil || viewComment != "view" {
		t.Fatalf("view comment = %q, %v; want the trailing COMMENT kept", viewComment, err)
	}
	var columnComment string
	if err := driver.QueryRow(ctx, "SELECT comment FROM system.columns WHERE database = ? AND table = ? AND name = 'v'", phys, prefix+"v_events").Scan(&columnComment); err != nil || columnComment != "v" {
		t.Fatalf("view column comment = %q, %v; want the column COMMENT kept", columnComment, err)
	}
	exec(ctx, "ALTER TABLE "+full+" ADD COLUMN `note` String DEFAULT ''")
	exec(ctx, "ALTER TABLE "+full+" COMMENT COLUMN `note` 'note'")
	exec(ctx, "ALTER TABLE "+full+" MODIFY COLUMN `note` REMOVE DEFAULT")
	exec(ctx, "ALTER TABLE "+full+" MODIFY SETTING max_partitions_to_read = -1")
	exec(ctx, "ALTER TABLE "+full+" MODIFY COMMENT 'events v2'")

	// chx metadata reads (operator.go load / loadSimple), the physical name
	// bound as a string literal: LIKE for a prefix (its '_' escaped), '=' for
	// one table (LoadOne).
	for _, q := range []string{
		"SELECT name, engine, comment FROM system.tables WHERE database = ? AND name LIKE ? AND is_temporary = 0",
		"SELECT table, name, type, default_expression, comment, compression_codec FROM system.columns WHERE database = ? AND table LIKE ? ORDER BY table, position",
		"SELECT table, name, type_full, expr, granularity FROM system.data_skipping_indices WHERE database = ? AND table LIKE ? ORDER BY table, name",
		"SELECT table, name, query FROM system.projections WHERE database = ? AND table LIKE ? ORDER BY table, name",
	} {
		exec(ctx, q, phys, likeArg)
		exec(ctx, strings.Replace(q, " LIKE ", " = ", 1), phys, prefix+"events")
	}
	var name, engineFull string
	if err := driver.QueryRow(ctx, "SELECT name, engine_full FROM system.tables WHERE database = ? AND name = ? AND is_temporary = 0", phys, prefix+"events").Scan(&name, &engineFull); err != nil {
		t.Fatalf("system.tables load: %v", err)
	}
	if name != prefix+"events" || !strings.HasPrefix(engineFull, "MergeTree") {
		t.Fatalf("system.tables = (%q, %q), want the physical table %q of %s", name, engineFull, prefix+"events", full)
	}

	// chx BatchInsert (InsertCtx) and the aggregation INSERT … SELECT
	// (InsertSelectCtx + InsertCtx).
	batch, err := driver.PrepareBatch(withSettings(clickhouse.Settings{"insert_deduplication_token": "a1"}), "INSERT INTO "+full+" (`id`, `chain`, `v`)")
	if err != nil {
		t.Fatalf("driver prepare batch: %v", err)
	}
	for i := uint64(1); i <= 4; i++ {
		if err := batch.Append(i, "1", i*10); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("driver batch send: %v", err)
	}
	exec(withSettings(clickhouse.Settings{"insert_deduplication_token": "b2", "max_partitions_per_insert_block": 0}),
		"INSERT INTO "+agg+" SELECT `chain`, count() FROM "+full+" WHERE `chain` = '1' GROUP BY `chain`")
	if n := count(ctx, "SELECT sum(`n`) FROM "+agg); n != 8 {
		t.Fatalf("agg sum = %d, want 8 (4 from the materialized view + 4 from INSERT … SELECT)", n)
	}

	// chx deleteRows: the patch-part probe (startsWith), count, lightweight DELETE (LightDeleteCtx), heavyweight ALTER … DELETE
	// (AsyncMutationCtx) and the system.mutations poll, then ListPartitions.
	exec(ctx, "SELECT count(), sum(data_uncompressed_bytes) FROM system.parts WHERE database = ? AND table = ? AND active AND startsWith(name, 'patch-')", phys, prefix+"events")
	if n := count(withSettings(clickhouse.Settings{"allow_experimental_projection_optimization": "0"}), "SELECT COUNT(*) FROM "+full+" WHERE `id` = 1"); n != 1 {
		t.Fatalf("count before delete = %d, want 1", n)
	}
	exec(withSettings(clickhouse.Settings{
		"alter_update_mode":                     "lightweight",
		"lightweight_delete_mode":               "lightweight_update",
		"enable_lightweight_delete":             "1",
		"allow_experimental_lightweight_update": "1",
	}), "DELETE FROM "+full+" WHERE `id` = 1")
	exec(withSettings(clickhouse.Settings{"mutations_sync": "0"}), "ALTER TABLE "+full+" DELETE WHERE `id` = 2")
	deadline := time.Now().Add(30 * time.Second)
	for count(ctx, "SELECT count() FROM system.mutations WHERE database = ? AND table = ? AND NOT is_done AND command LIKE ?", phys, prefix+"events", "%DELETE%WHERE%") != 0 {
		if time.Now().After(deadline) {
			t.Fatal("the delete mutation did not finish")
		}
		time.Sleep(200 * time.Millisecond)
	}
	exec(ctx, "SELECT partition, sum(rows), sum(bytes_on_disk), sum(data_compressed_bytes), sum(data_uncompressed_bytes) FROM system.parts WHERE database = ? AND table = ? AND active = 1 GROUP BY partition ORDER BY partition", phys, prefix+"events")
	if n := count(ctx, "SELECT count() FROM "+full); n != 2 {
		t.Fatalf("rows after deletes = %d, want 2", n)
	}
	if n := count(ctx, "SELECT count() FROM `"+logic+"`.`v_events` WHERE `id` IN (SELECT `id` FROM "+full+")"); n != 2 {
		t.Fatalf("view rows = %d, want 2", n)
	}
	exec(ctx, "DROP VIEW `"+logic+"`.`v_events`")

	// The physical database is no logical context: an unqualified name has
	// nothing to resolve against and is refused, and the guard still refuses a
	// physical qualifier on the driver session.
	if err := driver.Exec(ctx, "SELECT count() FROM `events`"); err == nil || !strings.Contains(err.Error(), "does not resolve through the session's logical database") {
		t.Fatalf("unqualified table on a physical-hello session: err = %v, want the engine's unqualified-name refusal", err)
	}
	want := guardRefusal("physical_database", "protected database "+phys+" is not addressable")
	if err := driver.Exec(ctx, "SELECT * FROM "+phys+".`"+prefix+"events`"); err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("physical qualifier: err = %v, want it to contain %q", err, want)
	}
	if n := count(ctx, "SELECT toUInt64(1)"); n != 1 {
		t.Fatalf("SELECT 1 = %d", n)
	}
}
