package integration

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	tcclickhouse "github.com/testcontainers/testcontainers-go/modules/clickhouse"

	"github.com/housegate/housegate/pkg/replay/chexec"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// policyProbeConn observes the actual parts immediately before the materializer's
// deferred DROP, after read-back, while preserving that DROP and its error.
type policyProbeConn struct {
	clickhouse.Conn
	database, table string
	disks           []string
	policy          string
}

func (c *policyProbeConn) Exec(ctx context.Context, query string, args ...any) error {
	if strings.HasPrefix(query, "DROP TABLE IF EXISTS ") {
		if err := c.Conn.QueryRow(ctx, "SELECT arraySort(groupUniqArray(disk_name)) FROM system.parts WHERE active AND database = ? AND table = ?", c.database, c.table).Scan(&c.disks); err != nil {
			return err
		}
		if len(c.disks) > 0 {
			if err := c.Conn.QueryRow(ctx, "SELECT storage_policy FROM system.tables WHERE database = ? AND name = ?", c.database, c.table).Scan(&c.policy); err != nil {
				return err
			}
		}
	}
	return c.Conn.Exec(ctx, query, args...)
}

// TestChexecMaterializerInWithStoragePolicyUsesMemberDisk proves that a separate
// member policy actually owns the replay parts, not merely the emitted SQL.
func TestChexecMaterializerInWithStoragePolicyUsesMemberDisk(t *testing.T) {
	ctx := context.Background()
	config := filepath.Join(t.TempDir(), "storage.xml")
	const xml = `<clickhouse><storage_configuration><disks><member><path>/var/lib/clickhouse/member_disk/</path></member></disks><policies><member><volumes><main><disk>member</disk></main></volumes></member></policies></storage_configuration></clickhouse>`
	if err := os.WriteFile(config, []byte(xml), 0600); err != nil {
		t.Fatal(err)
	}
	ctr, err := tcclickhouse.Run(ctx, "clickhouse/clickhouse-server:26.3.37.3", tcclickhouse.WithUsername("policy_test"), tcclickhouse.WithPassword("policy_test_local_only"), tcclickhouse.WithDatabase("policy_test"), tcclickhouse.WithConfigFile(config))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })
	addr, err := ctr.ConnectionHost(ctx)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := clickhouse.Open(&clickhouse.Options{Addr: []string{addr}, Auth: clickhouse.Auth{Database: ctr.DbName, Username: ctr.User, Password: ctr.Password}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	deadline := time.Now().Add(10 * time.Second)
	for {
		if err = conn.Ping(ctx); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	mustExec(t, conn, "CREATE DATABASE hg_rstage")
	schema := chReplaySchema(uniqueTable(t))
	st := materializerStatement(t, schema.TableID, "policy-1")
	var reference []payloadexec.Row
	for _, tc := range []struct{ name, policy, database, table string }{
		{"legacy", "default", "policy_test", "_hg_replay_scratch_1"},
		{"named", "default", "hg_rstage", "m1_replay_1"},
		{"member", "member", "hg_rstage", "m1_replay_1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			probe := &policyProbeConn{Conn: conn, database: tc.database, table: tc.table}
			var m *chexec.Materializer
			var err error
			switch tc.name {
			case "legacy":
				m = chexec.NewMaterializer(chReplayNetwork, probe)
			case "named":
				m, err = chexec.NewMaterializerIn(chReplayNetwork, probe, "hg_rstage", "m1_")
			default:
				m, err = chexec.NewMaterializerInWithStoragePolicy(chReplayNetwork, probe, "hg_rstage", "m1_", "member")
			}
			if err != nil {
				t.Fatal(err)
			}
			rows, err := m.Materialize(ctx, schema, st)
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) != 3 || !slices.Equal(probe.disks, []string{tc.policy}) || probe.policy != tc.policy {
				t.Fatalf("rows %d, disks %q, policy %q; want 3, [%s], %s", len(rows), probe.disks, probe.policy, tc.policy, tc.policy)
			}
			if reference == nil {
				reference = rows
			} else if !reflect.DeepEqual(sortedByRowID(rows), sortedByRowID(reference)) {
				t.Fatal("policy changed materialized rows")
			}
			if left := scratchTablesLike(t, conn, tc.database, tc.table); len(left) != 0 {
				t.Fatalf("scratch leaked: %v", left)
			}
		})
	}
	missing, err := chexec.NewMaterializerInWithStoragePolicy(chReplayNetwork, conn, "hg_rstage", "missing_", "missing_policy")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = missing.Materialize(ctx, schema, st); err == nil {
		t.Fatal("missing storage policy silently fell back")
	}
	if left := scratchTablesLike(t, conn, "hg_rstage", "missing_"); len(left) != 0 {
		t.Fatalf("failed CREATE leaked scratch: %v", left)
	}
}
