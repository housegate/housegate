package network

import (
	"context"
	"reflect"
	"testing"

	"sentioxyz/sentio-core/network/registry"

	"housegate/housegate/pkg/chproto"
	"housegate/housegate/pkg/plugins/commitgate"
	"housegate/housegate/pkg/sqlmeta"
)

// newEvent builds a *commitgate.Event with a single AccessedTables
// entry mirroring (database, table) — the shape commitgate.buildEvent
// produces for single-target DDL — plus a pre-allocated Values map.
// Empty (database, table) leaves AccessedTables nil so observers can
// see the empty-shape path.
func newEvent(t sqlmeta.StatementType, user, database, table string) *commitgate.Event {
	ev := &commitgate.Event{
		Type:   t,
		User:   user,
		Values: make(map[string]any),
	}
	if database != "" || table != "" {
		ev.AccessedTables = []sqlmeta.AccessedTable{{
			OriginalDatabase: database,
			OriginalTable:    table,
			LogicalDatabase:  database,
		}}
	}
	return ev
}

// snapshotState returns a deep-enough copy of the parts of
// InMemoryNetworkState that the observer mutates, for assert-equal
// comparisons after rollback.
func snapshotState(s *InMemoryNetworkState) (map[Database]DatabaseInfo, map[AccountAddress]DatabasePermissions) {
	dbs := make(map[Database]DatabaseInfo, len(s.DatabaseInfos))
	for k, v := range s.DatabaseInfos {
		// Tables is a slice; copy it.
		copied := v
		copied.Tables = append([]TableInfo(nil), v.Tables...)
		dbs[k] = copied
	}
	perms := make(map[AccountAddress]DatabasePermissions, len(s.DatabasePermissions))
	for k, v := range s.DatabasePermissions {
		inner := make(DatabasePermissions, len(v))
		for kk, vv := range v {
			inner[kk] = vv
		}
		perms[k] = inner
	}
	return dbs, perms
}

func newObserver() (*InMemoryCommitGateObserver, *InMemoryNetworkState) {
	st := NewInMemoryNetworkState()
	o := NewInMemoryCommitGateObserver(st, []sqlmeta.StatementType{
		sqlmeta.StatementTypeCreateDatabase,
		sqlmeta.StatementTypeDropDatabase,
		sqlmeta.StatementTypeCreateTable,
		sqlmeta.StatementTypeDropTable,
		sqlmeta.StatementTypeGrant,
		sqlmeta.StatementTypeRevoke,
	}, nil)
	return o, st
}

// newPrivEvent builds a *commitgate.Event populated with
// PrivilegesDeltas. The single-entry AccessedTables mirrors `db` so
// the InMemory observer's admin check (which reads
// ev.AccessedTables[0].LogicalDatabase) sees a target consistent with
// the deltas — the same mirroring commitgate.buildEvent does in
// production.
func newPrivEvent(action sqlmeta.PrivilegeAction, executor, db string, deltas []sqlmeta.PrivilegeDelta) *commitgate.Event {
	t := sqlmeta.StatementTypeGrant
	if action == sqlmeta.PrivilegeActionRevoke {
		t = sqlmeta.StatementTypeRevoke
	}
	ev := &commitgate.Event{
		Type:             t,
		User:             executor,
		Values:           make(map[string]any),
		PrivilegesDeltas: deltas,
	}
	if db != "" {
		ev.AccessedTables = []sqlmeta.AccessedTable{{
			OriginalDatabase: db,
			LogicalDatabase:  db,
		}}
	}
	return ev
}

// grantee builds a non-CURRENT_USER PrivilegeGrantee. CURRENT_USER
// grantees are constructed inline in the few tests that need them.
func grantee(name string) sqlmeta.PrivilegeGrantee {
	return sqlmeta.PrivilegeGrantee{Name: name}
}

// TestInMemoryRollback_CreateDatabase verifies that a successful
// CREATE DATABASE mutation is reverted by OnStatementException.
func TestInMemoryRollback_CreateDatabase(t *testing.T) {
	o, st := newObserver()
	beforeDBs, beforePerms := snapshotState(st)

	ev := newEvent(sqlmeta.StatementTypeCreateDatabase, "alice", "foo", "")
	if err := o.BeforeStatement(context.Background(), ev); err != nil {
		t.Fatalf("BeforeStatement: %v", err)
	}
	// Sanity: state mutated.
	if _, ok := st.DatabaseInfos["foo"]; !ok {
		t.Fatal("expected database foo present after BeforeStatement")
	}

	o.OnStatementException(context.Background(), ev, &chproto.Exception{Code: 60, Message: "boom"})

	gotDBs, gotPerms := snapshotState(st)
	if !reflect.DeepEqual(gotDBs, beforeDBs) {
		t.Errorf("DatabaseInfos not restored after rollback:\n got=%v\nwant=%v", gotDBs, beforeDBs)
	}
	if !reflect.DeepEqual(gotPerms, beforePerms) {
		t.Errorf("DatabasePermissions not restored after rollback:\n got=%v\nwant=%v", gotPerms, beforePerms)
	}
}

// TestInMemoryRollback_DropDatabase verifies a DROP DATABASE rollback
// restores the DatabaseInfo and every account's permission bit.
func TestInMemoryRollback_DropDatabase(t *testing.T) {
	o, st := newObserver()
	st.DatabaseInfos["foo"] = DatabaseInfo{DatabaseId: "foo"}
	st.DatabasePermissions["alice"] = DatabasePermissions{"foo": registry.DbAuthOwner}
	st.DatabasePermissions["bob"] = DatabasePermissions{"foo": registry.DbAuthRead}
	beforeDBs, beforePerms := snapshotState(st)

	ev := newEvent(sqlmeta.StatementTypeDropDatabase, "alice", "foo", "")
	if err := o.BeforeStatement(context.Background(), ev); err != nil {
		t.Fatalf("BeforeStatement: %v", err)
	}
	if _, ok := st.DatabaseInfos["foo"]; ok {
		t.Fatal("expected database foo deleted after DROP")
	}

	o.OnStatementException(context.Background(), ev, &chproto.Exception{Code: 60})

	gotDBs, gotPerms := snapshotState(st)
	if !reflect.DeepEqual(gotDBs, beforeDBs) {
		t.Errorf("DatabaseInfos not restored:\n got=%v\nwant=%v", gotDBs, beforeDBs)
	}
	if !reflect.DeepEqual(gotPerms, beforePerms) {
		t.Errorf("DatabasePermissions not restored:\n got=%v\nwant=%v", gotPerms, beforePerms)
	}
}

// TestInMemoryRollback_CreateTable verifies a CREATE TABLE rollback
// removes the appended TableInfo entry.
func TestInMemoryRollback_CreateTable(t *testing.T) {
	o, st := newObserver()
	st.DatabaseInfos["foo"] = DatabaseInfo{DatabaseId: "foo"}
	st.DatabasePermissions["alice"] = DatabasePermissions{"foo": registry.DbAuthOwner}
	beforeDBs, _ := snapshotState(st)

	ev := newEvent(sqlmeta.StatementTypeCreateTable, "alice", "foo", "events")
	if err := o.BeforeStatement(context.Background(), ev); err != nil {
		t.Fatalf("BeforeStatement: %v", err)
	}
	if got := st.DatabaseInfos["foo"].Tables; len(got) != 1 || got[0].TableId != "events" {
		t.Fatalf("expected events table appended; Tables=%v", got)
	}

	o.OnStatementException(context.Background(), ev, &chproto.Exception{Code: 60})

	gotDBs, _ := snapshotState(st)
	if !reflect.DeepEqual(gotDBs, beforeDBs) {
		t.Errorf("Tables not restored:\n got=%v\nwant=%v", gotDBs, beforeDBs)
	}
}

// TestInMemoryRollback_DropTable verifies a DROP TABLE rollback
// re-appends the dropped TableInfo entry.
func TestInMemoryRollback_DropTable(t *testing.T) {
	o, st := newObserver()
	st.DatabaseInfos["foo"] = DatabaseInfo{
		DatabaseId: "foo",
		Tables: []TableInfo{{TableId: "events"}, {TableId: "users"}},
	}
	st.DatabasePermissions["alice"] = DatabasePermissions{"foo": registry.DbAuthOwner}
	beforeDBs, _ := snapshotState(st)

	ev := newEvent(sqlmeta.StatementTypeDropTable, "alice", "foo", "events")
	if err := o.BeforeStatement(context.Background(), ev); err != nil {
		t.Fatalf("BeforeStatement: %v", err)
	}
	tables := st.DatabaseInfos["foo"].Tables
	if len(tables) != 1 || tables[0].TableId != "users" {
		t.Fatalf("expected only users left; Tables=%v", tables)
	}

	o.OnStatementException(context.Background(), ev, &chproto.Exception{Code: 60})

	gotDBs, _ := snapshotState(st)
	if !reflect.DeepEqual(gotDBs, beforeDBs) {
		t.Errorf("Tables not restored:\n got=%v\nwant=%v", gotDBs, beforeDBs)
	}
}

// TestInMemoryRollback_IdempotentNoOp verifies that when
// BeforeStatement was a no-op (e.g. CREATE DATABASE replayed by the
// same owner) no rollback runs and state stays unchanged.
func TestInMemoryRollback_IdempotentNoOp(t *testing.T) {
	o, st := newObserver()
	st.DatabaseInfos["foo"] = DatabaseInfo{DatabaseId: "foo"}
	st.DatabasePermissions["alice"] = DatabasePermissions{"foo": registry.DbAuthOwner}
	beforeDBs, beforePerms := snapshotState(st)

	// Replaying CREATE DATABASE foo by the same owner is a no-op.
	ev := newEvent(sqlmeta.StatementTypeCreateDatabase, "alice", "foo", "")
	if err := o.BeforeStatement(context.Background(), ev); err != nil {
		t.Fatalf("BeforeStatement: %v", err)
	}
	if _, hasRollback := ev.Values[inMemoryRollbackKey]; hasRollback {
		t.Error("expected no rollback closure for idempotent no-op")
	}

	// OnStatementException must be a no-op when no rollback was stashed.
	o.OnStatementException(context.Background(), ev, &chproto.Exception{Code: 1})

	gotDBs, gotPerms := snapshotState(st)
	if !reflect.DeepEqual(gotDBs, beforeDBs) {
		t.Errorf("DatabaseInfos changed despite idempotent no-op:\n got=%v\nwant=%v", gotDBs, beforeDBs)
	}
	if !reflect.DeepEqual(gotPerms, beforePerms) {
		t.Errorf("DatabasePermissions changed despite idempotent no-op:\n got=%v\nwant=%v", gotPerms, beforePerms)
	}
}

// TestGrant_AddsBitsByOwner verifies that the recorded owner can
// GRANT on their database and the per-grantee permission bitmap
// reflects the union of granted bits.
func TestGrant_AddsBitsByOwner(t *testing.T) {
	o, st := newObserver()
	st.DatabaseInfos["foo"] = DatabaseInfo{DatabaseId: "foo"}
	st.DatabasePermissions["alice"] = DatabasePermissions{"foo": registry.DbAuthOwner}

	deltas := []sqlmeta.PrivilegeDelta{{
		Action:          sqlmeta.PrivilegeActionGrant,
		Scope:           sqlmeta.PrivilegeScopeDatabase,
		LogicalDatabase: "foo",
		Privileges:      []string{"SELECT"},
		Category:        sqlmeta.PrivilegeCategoryRead,
		Grantees:        []sqlmeta.PrivilegeGrantee{grantee("bob"), grantee("carol")},
	}, {
		Action:          sqlmeta.PrivilegeActionGrant,
		Scope:           sqlmeta.PrivilegeScopeDatabase,
		LogicalDatabase: "foo",
		Privileges:      []string{"INSERT"},
		Category:        sqlmeta.PrivilegeCategoryWrite,
		Grantees:        []sqlmeta.PrivilegeGrantee{grantee("bob")},
	}}

	ev := newPrivEvent(sqlmeta.PrivilegeActionGrant, "alice", "foo", deltas)
	if err := o.BeforeStatement(context.Background(), ev); err != nil {
		t.Fatalf("BeforeStatement: %v", err)
	}
	if got := st.DatabasePermissions["bob"]["foo"]; got != registry.DbAuthRead|registry.DbAuthWrite {
		t.Errorf("bob: got %x, want Read|Write", got)
	}
	if got := st.DatabasePermissions["carol"]["foo"]; got != registry.DbAuthRead {
		t.Errorf("carol: got %x, want Read", got)
	}
}

// TestGrant_RejectsNonAdmin verifies that an account holding only
// Write (no Admin/Owner) cannot GRANT.
func TestGrant_RejectsNonAdmin(t *testing.T) {
	o, st := newObserver()
	st.DatabaseInfos["foo"] = DatabaseInfo{DatabaseId: "foo"}
	st.DatabasePermissions["bob"] = DatabasePermissions{"foo": registry.DbAuthWrite}

	deltas := []sqlmeta.PrivilegeDelta{{
		Action:          sqlmeta.PrivilegeActionGrant,
		Scope:           sqlmeta.PrivilegeScopeDatabase,
		LogicalDatabase: "foo",
		Privileges:      []string{"SELECT"},
		Category:        sqlmeta.PrivilegeCategoryRead,
		Grantees:        []sqlmeta.PrivilegeGrantee{grantee("carol")},
	}}

	ev := newPrivEvent(sqlmeta.PrivilegeActionGrant, "bob", "foo", deltas)
	if err := o.BeforeStatement(context.Background(), ev); err == nil {
		t.Fatal("expected admin-required error, got nil")
	}
	if _, ok := st.DatabasePermissions["carol"]; ok {
		t.Errorf("rejected GRANT must not mutate; carol got entries")
	}
}

// TestGrant_RejectsUnknownDatabase verifies a defensive failure when
// the target database is not registered.
func TestGrant_RejectsUnknownDatabase(t *testing.T) {
	o, _ := newObserver()
	deltas := []sqlmeta.PrivilegeDelta{{
		Action:          sqlmeta.PrivilegeActionGrant,
		Scope:           sqlmeta.PrivilegeScopeDatabase,
		LogicalDatabase: "ghost",
		Privileges:      []string{"SELECT"},
		Category:        sqlmeta.PrivilegeCategoryRead,
		Grantees:        []sqlmeta.PrivilegeGrantee{grantee("bob")},
	}}
	ev := newPrivEvent(sqlmeta.PrivilegeActionGrant, "alice", "ghost", deltas)
	if err := o.BeforeStatement(context.Background(), ev); err == nil {
		t.Fatal("expected unknown-database error, got nil")
	}
}

// TestGrant_AdminBitSatisfiesAdminCheck verifies that holding only
// Admin (without owner identity, without Owner bit) is enough to
// GRANT.
func TestGrant_AdminBitSatisfiesAdminCheck(t *testing.T) {
	o, st := newObserver()
	st.DatabaseInfos["foo"] = DatabaseInfo{DatabaseId: "foo"}
	st.DatabasePermissions["bob"] = DatabasePermissions{"foo": registry.DbAuthAdmin}

	deltas := []sqlmeta.PrivilegeDelta{{
		Action:          sqlmeta.PrivilegeActionGrant,
		Scope:           sqlmeta.PrivilegeScopeDatabase,
		LogicalDatabase: "foo",
		Privileges:      []string{"SELECT"},
		Category:        sqlmeta.PrivilegeCategoryRead,
		Grantees:        []sqlmeta.PrivilegeGrantee{grantee("carol")},
	}}
	ev := newPrivEvent(sqlmeta.PrivilegeActionGrant, "bob", "foo", deltas)
	if err := o.BeforeStatement(context.Background(), ev); err != nil {
		t.Errorf("admin-bit holder should be allowed to GRANT, got %v", err)
	}
}

// TestRevoke_TrimsBits verifies that a REVOKE clears only the named
// category bits and preserves the rest.
func TestRevoke_TrimsBits(t *testing.T) {
	o, st := newObserver()
	st.DatabaseInfos["foo"] = DatabaseInfo{DatabaseId: "foo"}
	st.DatabasePermissions["alice"] = DatabasePermissions{"foo": registry.DbAuthOwner}
	st.DatabasePermissions["bob"] = DatabasePermissions{"foo": registry.DbAuthRead | registry.DbAuthWrite}

	deltas := []sqlmeta.PrivilegeDelta{{
		Action:          sqlmeta.PrivilegeActionRevoke,
		Scope:           sqlmeta.PrivilegeScopeDatabase,
		LogicalDatabase: "foo",
		Privileges:      []string{"SELECT"},
		Category:        sqlmeta.PrivilegeCategoryRead,
		Grantees:        []sqlmeta.PrivilegeGrantee{grantee("bob")},
	}}
	ev := newPrivEvent(sqlmeta.PrivilegeActionRevoke, "alice", "foo", deltas)
	if err := o.BeforeStatement(context.Background(), ev); err != nil {
		t.Fatalf("BeforeStatement: %v", err)
	}
	if got := st.DatabasePermissions["bob"]["foo"]; got != registry.DbAuthWrite {
		t.Errorf("after REVOKE Read; got %x, want Write only", got)
	}
}

// TestRevoke_DeletesEmptyEntry verifies that a REVOKE that drains the
// last bit removes the (account, db) entry — and the per-account map
// itself when it becomes empty — keeping DatabasePermissions clean.
func TestRevoke_DeletesEmptyEntry(t *testing.T) {
	o, st := newObserver()
	st.DatabaseInfos["foo"] = DatabaseInfo{DatabaseId: "foo"}
	st.DatabasePermissions["alice"] = DatabasePermissions{"foo": registry.DbAuthOwner}
	st.DatabasePermissions["bob"] = DatabasePermissions{"foo": registry.DbAuthRead}

	deltas := []sqlmeta.PrivilegeDelta{{
		Action:          sqlmeta.PrivilegeActionRevoke,
		Scope:           sqlmeta.PrivilegeScopeDatabase,
		LogicalDatabase: "foo",
		Privileges:      []string{"ALL"},
		Category:        sqlmeta.PrivilegeCategoryAll,
		Grantees:        []sqlmeta.PrivilegeGrantee{grantee("bob")},
	}}
	ev := newPrivEvent(sqlmeta.PrivilegeActionRevoke, "alice", "foo", deltas)
	if err := o.BeforeStatement(context.Background(), ev); err != nil {
		t.Fatalf("BeforeStatement: %v", err)
	}
	if _, ok := st.DatabasePermissions["bob"]; ok {
		t.Errorf("bob entry should be cleaned up after REVOKE ALL")
	}
}

// TestGrant_CurrentUserResolvesToExecutor verifies the CURRENT_USER
// grantee form is bound to the executing account.
func TestGrant_CurrentUserResolvesToExecutor(t *testing.T) {
	o, st := newObserver()
	st.DatabaseInfos["foo"] = DatabaseInfo{DatabaseId: "foo"}
	st.DatabasePermissions["alice"] = DatabasePermissions{"foo": registry.DbAuthOwner}

	deltas := []sqlmeta.PrivilegeDelta{{
		Action:          sqlmeta.PrivilegeActionGrant,
		Scope:           sqlmeta.PrivilegeScopeDatabase,
		LogicalDatabase: "foo",
		Privileges:      []string{"SELECT"},
		Category:        sqlmeta.PrivilegeCategoryRead,
		Grantees:        []sqlmeta.PrivilegeGrantee{{IsCurrentUser: true}},
	}}
	ev := newPrivEvent(sqlmeta.PrivilegeActionGrant, "alice", "foo", deltas)
	if err := o.BeforeStatement(context.Background(), ev); err != nil {
		t.Fatalf("BeforeStatement: %v", err)
	}
	// The Owner bit already covers SELECT via promotion; we check the
	// Read bit was also written under the executor's address.
	if got := st.DatabasePermissions["alice"]["foo"]; got&registry.DbAuthRead == 0 {
		t.Errorf("CURRENT_USER GRANT should land under executor; perms=%x", got)
	}
}

// TestInMemoryRollback_Grant verifies that a successful GRANT reverts
// the per-grantee mutation on OnStatementException, including the
// "create new entry" path (no prior perms) which must be deleted on
// rollback.
func TestInMemoryRollback_Grant(t *testing.T) {
	o, st := newObserver()
	st.DatabaseInfos["foo"] = DatabaseInfo{DatabaseId: "foo"}
	st.DatabasePermissions["alice"] = DatabasePermissions{"foo": registry.DbAuthOwner}
	// Pre-existing perm on bob to test "restore prior bits", and no
	// perm on carol to test "delete created-from-scratch".
	st.DatabasePermissions["bob"] = DatabasePermissions{"foo": registry.DbAuthRead}
	beforeDBs, beforePerms := snapshotState(st)

	deltas := []sqlmeta.PrivilegeDelta{{
		Action:          sqlmeta.PrivilegeActionGrant,
		Scope:           sqlmeta.PrivilegeScopeDatabase,
		LogicalDatabase: "foo",
		Privileges:      []string{"INSERT"},
		Category:        sqlmeta.PrivilegeCategoryWrite,
		Grantees:        []sqlmeta.PrivilegeGrantee{grantee("bob"), grantee("carol")},
	}}
	ev := newPrivEvent(sqlmeta.PrivilegeActionGrant, "alice", "foo", deltas)
	if err := o.BeforeStatement(context.Background(), ev); err != nil {
		t.Fatalf("BeforeStatement: %v", err)
	}
	// Sanity: state mutated.
	if st.DatabasePermissions["bob"]["foo"] != registry.DbAuthRead|registry.DbAuthWrite {
		t.Fatalf("expected bob = Read|Write after GRANT")
	}
	if st.DatabasePermissions["carol"]["foo"] != registry.DbAuthWrite {
		t.Fatalf("expected carol = Write after GRANT")
	}

	o.OnStatementException(context.Background(), ev, &chproto.Exception{Code: 60, Message: "boom"})

	gotDBs, gotPerms := snapshotState(st)
	if !reflect.DeepEqual(gotDBs, beforeDBs) {
		t.Errorf("DatabaseInfos changed by GRANT rollback:\n got=%v\nwant=%v", gotDBs, beforeDBs)
	}
	if !reflect.DeepEqual(gotPerms, beforePerms) {
		t.Errorf("DatabasePermissions not restored after GRANT rollback:\n got=%v\nwant=%v", gotPerms, beforePerms)
	}
}

// TestInMemoryRollback_Revoke verifies that a successful REVOKE
// (including the "drop empty entry" case) is fully reverted.
func TestInMemoryRollback_Revoke(t *testing.T) {
	o, st := newObserver()
	st.DatabaseInfos["foo"] = DatabaseInfo{DatabaseId: "foo"}
	st.DatabasePermissions["alice"] = DatabasePermissions{"foo": registry.DbAuthOwner}
	st.DatabasePermissions["bob"] = DatabasePermissions{"foo": registry.DbAuthRead}
	beforeDBs, beforePerms := snapshotState(st)

	deltas := []sqlmeta.PrivilegeDelta{{
		Action:          sqlmeta.PrivilegeActionRevoke,
		Scope:           sqlmeta.PrivilegeScopeDatabase,
		LogicalDatabase: "foo",
		Privileges:      []string{"SELECT"},
		Category:        sqlmeta.PrivilegeCategoryRead,
		Grantees:        []sqlmeta.PrivilegeGrantee{grantee("bob")},
	}}
	ev := newPrivEvent(sqlmeta.PrivilegeActionRevoke, "alice", "foo", deltas)
	if err := o.BeforeStatement(context.Background(), ev); err != nil {
		t.Fatalf("BeforeStatement: %v", err)
	}
	// Sanity: bob's entry should be cleaned up entirely.
	if _, ok := st.DatabasePermissions["bob"]; ok {
		t.Fatalf("bob entry should be deleted by REVOKE Read")
	}

	o.OnStatementException(context.Background(), ev, &chproto.Exception{Code: 60})

	gotDBs, gotPerms := snapshotState(st)
	if !reflect.DeepEqual(gotDBs, beforeDBs) {
		t.Errorf("DatabaseInfos changed by REVOKE rollback:\n got=%v\nwant=%v", gotDBs, beforeDBs)
	}
	if !reflect.DeepEqual(gotPerms, beforePerms) {
		t.Errorf("DatabasePermissions not restored after REVOKE rollback:\n got=%v\nwant=%v", gotPerms, beforePerms)
	}
}

// TestGrant_UnknownCategorySkipped verifies that a delta carrying
// PrivilegeCategoryNone (the rewriter could not classify the
// privilege string) is skipped — neither mutates state nor causes a
// hard failure of the whole statement. This keeps the dev fixture
// resilient to new ClickHouse privileges that haven't been mapped
// yet.
func TestGrant_UnknownCategorySkipped(t *testing.T) {
	o, st := newObserver()
	st.DatabaseInfos["foo"] = DatabaseInfo{DatabaseId: "foo"}
	st.DatabasePermissions["alice"] = DatabasePermissions{"foo": registry.DbAuthOwner}
	beforePerms := map[AccountAddress]DatabasePermissions{}
	for k, v := range st.DatabasePermissions {
		inner := DatabasePermissions{}
		for kk, vv := range v {
			inner[kk] = vv
		}
		beforePerms[k] = inner
	}

	deltas := []sqlmeta.PrivilegeDelta{{
		Action:          sqlmeta.PrivilegeActionGrant,
		Scope:           sqlmeta.PrivilegeScopeDatabase,
		LogicalDatabase: "foo",
		Privileges:      []string{"INVENTED_PRIVILEGE_2099"},
		Category:        sqlmeta.PrivilegeCategoryNone,
		Grantees:        []sqlmeta.PrivilegeGrantee{grantee("bob")},
	}}
	ev := newPrivEvent(sqlmeta.PrivilegeActionGrant, "alice", "foo", deltas)
	if err := o.BeforeStatement(context.Background(), ev); err != nil {
		t.Fatalf("unknown-category delta should not abort the statement, got %v", err)
	}
	if _, hasRollback := ev.Values[inMemoryRollbackKey]; hasRollback {
		t.Errorf("no rollback closure expected for entirely-skipped GRANT")
	}
	if _, ok := st.DatabasePermissions["bob"]; ok {
		t.Errorf("unknown-category delta must not mutate; bob got entries")
	}
}
