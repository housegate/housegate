package integration

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	tcclickhouse "github.com/testcontainers/testcontainers-go/modules/clickhouse"
)

const storagePromotionMVPEnv = "HOUSEGATE_RUN_STORAGE_PROMOTION_MVP"

type promotionPartProbe struct {
	Name        string
	PartitionID string
	Path        string
}

func TestStoragePromotionMVP_hardlinkUnsafePartIntoPromoteTable(t *testing.T) {
	if os.Getenv(storagePromotionMVPEnv) != "1" {
		t.Skipf("set %s=1 to run the ClickHouse storage-promotion MVP probe", storagePromotionMVPEnv)
	}

	// Given
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	conn, ctr := startPromotionProbeClickHouse(t, ctx)

	if err := conn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS hg_probe"); err != nil {
		t.Fatalf("create database: %v", err)
	}
	for _, query := range []string{
		`DROP TABLE IF EXISTS hg_probe.hg_unsafe`,
		`DROP TABLE IF EXISTS hg_probe.hg_promote`,
		`DROP TABLE IF EXISTS hg_probe.hg_safe`,
		`CREATE TABLE hg_probe.hg_unsafe (
			_hg_row_id FixedString(32),
			id UInt64,
			day Date
		) ENGINE = ReplicatedMergeTree('/housegate/storage_promotion_mvp/hg_unsafe', 'r1')
		PARTITION BY toYYYYMM(day)
		ORDER BY (id, _hg_row_id)
		SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0`,
		`CREATE TABLE hg_probe.hg_promote AS hg_probe.hg_unsafe ENGINE = MergeTree
		PARTITION BY toYYYYMM(day)
		ORDER BY (id, _hg_row_id)`,
		`CREATE TABLE hg_probe.hg_safe AS hg_probe.hg_unsafe ENGINE = MergeTree
		PARTITION BY toYYYYMM(day)
		ORDER BY (id, _hg_row_id)`,
		`SYSTEM STOP MERGES hg_probe.hg_unsafe`,
		`INSERT INTO hg_probe.hg_unsafe VALUES
			(unhex('0000000000000000000000000000000000000000000000000000000000000001'), 1, toDate('2026-06-22')),
			(unhex('0000000000000000000000000000000000000000000000000000000000000002'), 2, toDate('2026-06-22'))`,
	} {
		if err := conn.Exec(ctx, query); err != nil {
			t.Fatalf("setup query %q: %v", query, err)
		}
	}

	part := activePart(t, ctx, conn, "hg_probe", "hg_unsafe")
	promotePath := tableDataPath(t, ctx, conn, "hg_probe", "hg_promote")
	detachedPath := path.Join(promotePath, "detached", part.Name)
	t.Logf("candidate part: name=%s partition=%s path=%s", part.Name, part.PartitionID, part.Path)
	t.Logf("promote detached target: %s", detachedPath)

	// When
	execClickHouseContainer(t, ctx, ctr, fmt.Sprintf(
		`mkdir -p %q && cp -al %q/. %q/`,
		detachedPath, strings.TrimRight(part.Path, "/"), detachedPath,
	))

	attachErr := conn.Exec(ctx, fmt.Sprintf("ALTER TABLE hg_probe.hg_promote ATTACH PART '%s'", part.Name))
	replaceErr := conn.Exec(ctx, "ALTER TABLE hg_probe.hg_safe REPLACE PARTITION 202606 FROM hg_probe.hg_promote")

	// Then
	promoteRows := countRows(t, ctx, conn, "hg_probe.hg_promote")
	safeRows := countRows(t, ctx, conn, "hg_probe.hg_safe")
	detachedRows := detachedPartRows(t, ctx, conn, "hg_probe", "hg_promote")
	t.Logf("promotion outcome: attach_error=%v replace_error=%v promote_rows=%d safe_rows=%d detached_rows=%d",
		attachErr, replaceErr, promoteRows, safeRows, detachedRows)

	if attachErr == nil && replaceErr != nil {
		t.Fatalf("ATTACH PART succeeded but REPLACE PARTITION failed: %v", replaceErr)
	}
	if attachErr == nil && safeRows != 2 {
		t.Fatalf("promotion completed but safe row count = %d, want 2", safeRows)
	}
	if attachErr != nil {
		t.Logf("ATTACH PART rejected the hardlinked ReplicatedMergeTree part; detached_rows=%d", detachedRows)
	}
}

func startPromotionProbeClickHouse(t *testing.T, ctx context.Context) (clickhouse.Conn, *tcclickhouse.ClickHouseContainer) {
	t.Helper()
	configPath := writePromotionKeeperConfig(t)
	ctr, err := tcclickhouse.Run(ctx, "clickhouse/clickhouse-server:25.8",
		tcclickhouse.WithUsername("housegate_test"),
		tcclickhouse.WithPassword("housegate_test_pw"),
		tcclickhouse.WithDatabase("housegate_test"),
		tcclickhouse.WithConfigFile(configPath),
	)
	if err != nil {
		t.Fatalf("run ClickHouse container: %v", err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })

	addr, err := ctr.ConnectionHost(ctx)
	if err != nil {
		t.Fatalf("ClickHouse connection host: %v", err)
	}
	conn := openPromotionProbeConn(t, ctx, addr)
	return conn, ctr
}

func writePromotionKeeperConfig(t *testing.T) string {
	t.Helper()
	path := path.Join(t.TempDir(), "keeper.xml")
	data := `<clickhouse>
  <keeper_server>
    <tcp_port>9181</tcp_port>
    <server_id>1</server_id>
    <log_storage_path>/var/lib/clickhouse/coordination/log</log_storage_path>
    <snapshot_storage_path>/var/lib/clickhouse/coordination/snapshots</snapshot_storage_path>
    <coordination_settings>
      <operation_timeout_ms>10000</operation_timeout_ms>
      <session_timeout_ms>30000</session_timeout_ms>
    </coordination_settings>
    <raft_configuration>
      <server>
        <id>1</id>
        <hostname>127.0.0.1</hostname>
        <port>9234</port>
      </server>
    </raft_configuration>
  </keeper_server>
  <zookeeper>
    <node index="1">
      <host>127.0.0.1</host>
      <port>9181</port>
    </node>
  </zookeeper>
</clickhouse>
`
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatalf("write keeper config: %v", err)
	}
	return path
}

func openPromotionProbeConn(t *testing.T, ctx context.Context, addr string) clickhouse.Conn {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		conn, err := clickhouse.Open(&clickhouse.Options{
			Addr: []string{addr},
			Auth: clickhouse.Auth{
				Database: "housegate_test",
				Username: "housegate_test",
				Password: "housegate_test_pw",
			},
			Protocol: clickhouse.Native,
		})
		if err == nil {
			if pingErr := conn.Ping(ctx); pingErr == nil {
				t.Cleanup(func() { _ = conn.Close() })
				return conn
			} else {
				lastErr = pingErr
			}
			_ = conn.Close()
		} else {
			lastErr = err
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("open ClickHouse native connection at %s: %v", addr, lastErr)
	return nil
}

func activePart(t *testing.T, ctx context.Context, conn clickhouse.Conn, database, table string) promotionPartProbe {
	t.Helper()
	var part promotionPartProbe
	err := conn.QueryRow(ctx, fmt.Sprintf(
		"SELECT name, partition_id, path FROM system.parts WHERE database = '%s' AND table = '%s' AND active ORDER BY modification_time DESC LIMIT 1",
		database, table,
	)).Scan(&part.Name, &part.PartitionID, &part.Path)
	if err != nil {
		t.Fatalf("query active part: %v", err)
	}
	return part
}

func tableDataPath(t *testing.T, ctx context.Context, conn clickhouse.Conn, database, table string) string {
	t.Helper()
	var dataPath string
	err := conn.QueryRow(ctx, fmt.Sprintf(
		"SELECT arrayElement(data_paths, 1) FROM system.tables WHERE database = '%s' AND name = '%s'",
		database, table,
	)).Scan(&dataPath)
	if err != nil {
		t.Fatalf("query table data path: %v", err)
	}
	return strings.TrimRight(dataPath, "/")
}

func execClickHouseContainer(t *testing.T, ctx context.Context, ctr *tcclickhouse.ClickHouseContainer, command string) string {
	t.Helper()
	code, reader, err := ctr.Exec(ctx, []string{"bash", "-lc", command})
	if err != nil {
		t.Fatalf("container exec: %v", err)
	}
	out, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read container exec output: %v", err)
	}
	if code != 0 {
		t.Fatalf("container exec exit=%d output=%s", code, string(out))
	}
	return string(out)
}

func countRows(t *testing.T, ctx context.Context, conn clickhouse.Conn, table string) uint64 {
	t.Helper()
	var count uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM "+table).Scan(&count); err != nil {
		t.Fatalf("count rows from %s: %v", table, err)
	}
	return count
}

func detachedPartRows(t *testing.T, ctx context.Context, conn clickhouse.Conn, database, table string) uint64 {
	t.Helper()
	var count uint64
	err := conn.QueryRow(ctx, fmt.Sprintf(
		"SELECT count() FROM system.detached_parts WHERE database = '%s' AND table = '%s'",
		database, table,
	)).Scan(&count)
	if err != nil {
		t.Logf("query system.detached_parts failed: %v", err)
	}
	return count
}
