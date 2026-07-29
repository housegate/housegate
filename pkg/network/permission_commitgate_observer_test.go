package network

import (
	"context"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/plugins/commitgate"
	"github.com/housegate/housegate/pkg/registry"
	"github.com/housegate/housegate/pkg/sqlmeta"
)

// fixturePerm builds an InMemoryNetworkState with a single database
// `db` plus an optional permission bitmap for an account. The Owner
// bit is stored in DatabasePermissions, not in DatabaseInfo.Owner.
func fixturePerm(db Database, owner AccountAddress, account AccountAddress, perm registry.DbAuth) *InMemoryNetworkState {
	st := NewInMemoryNetworkState()
	st.DatabaseInfos[db] = DatabaseInfo{
		DatabaseId: string(db),
		Tables:     []TableInfo{},
	}
	// Give the owner the Owner bit so promotion tests work.
	if owner != "" {
		st.DatabasePermissions[owner] = DatabasePermissions{db: registry.DbAuthOwner}
	}
	if account != "" && perm != 0 {
		if st.DatabasePermissions[account] == nil {
			st.DatabasePermissions[account] = DatabasePermissions{}
		}
		st.DatabasePermissions[account][db] |= perm
	}
	return st
}

func newPermObserver(st *InMemoryNetworkState) *PermissionCommitGateObserver {
	return NewPermissionCommitGateObserver(st)
}

// TestPermission_RejectsUnspecified: principle #1 — an unclassified
// statement must never be forwarded. Subscribing to Unspecified is
// what gives the observer the opportunity to reject; this test guards
// the explicit reject path inside BeforeStatement.
func TestPermission_RejectsUnspecified(t *testing.T) {
	st := fixturePerm("foo", "alice", "", 0)
	o := newPermObserver(st)

	ev := newEvent(sqlmeta.StatementTypeUnspecified, "alice", "foo", "")
	err := o.BeforeStatement(context.Background(), ev)
	if err == nil {
		t.Fatal("expected reject for Unspecified, got nil")
	}
	if !strings.Contains(err.Error(), "Unspecified") {
		t.Errorf("error should mention Unspecified, got %v", err)
	}
}

func TestPermission_SubscribedTypesMatchesPolicy(t *testing.T) {
	o := newPermObserver(NewInMemoryNetworkState())
	subs := o.SubscribedTypes()

	// Every subscribed type other than Unspecified must have a
	// policy entry; otherwise BeforeStatement returns the "no
	// policy" error and the observer hard-rejects all traffic.
	for _, t0 := range subs {
		if t0 == sqlmeta.StatementTypeUnspecified {
			continue
		}
		if _, ok := permissionPolicy[t0]; !ok {
			t.Errorf("subscribed type %s has no policy entry", t0)
		}
	}

	// And every policy entry must be in the subscribed list,
	// otherwise the policy can't fire.
	have := make(map[sqlmeta.StatementType]bool, len(subs))
	for _, t0 := range subs {
		have[t0] = true
	}
	for t0 := range permissionPolicy {
		if !have[t0] {
			t.Errorf("policy type %s is not in SubscribedTypes()", t0)
		}
	}
}

func TestPermission_RejectsAnonymous(t *testing.T) {
	st := fixturePerm("foo", "alice", "", 0)
	o := newPermObserver(st)

	ev := newEvent(sqlmeta.StatementTypeSelect, "" /* anonymous */, "foo", "t")
	err := o.BeforeStatement(context.Background(), ev)
	if err == nil || !strings.Contains(err.Error(), "authenticated") {
		t.Fatalf("expected authentication-required error, got %v", err)
	}
}

func TestPermission_RejectsUnknownDatabase(t *testing.T) {
	st := NewInMemoryNetworkState() // no DB registered
	o := newPermObserver(st)

	ev := newEvent(sqlmeta.StatementTypeSelect, "alice", "foo", "t")
	err := o.BeforeStatement(context.Background(), ev)
	if err == nil || !strings.Contains(err.Error(), "not registered") {
		t.Fatalf("expected unknown-database error, got %v", err)
	}
}

// TestPermission_AllowsSystemReads: ClickHouse's built-in `system`
// schema is never registered in NetworkState, but tools like
// clickhouse-client UNION-read system.functions / system.tables / ...
// for command-line suggestions on connect. Per-row visibility is
// enforced server-side via viewIfPermitted, so reads must succeed
// without a NetworkState entry.
func TestPermission_AllowsSystemReads(t *testing.T) {
	st := NewInMemoryNetworkState() // system not registered
	o := newPermObserver(st)

	for _, typ := range []sqlmeta.StatementType{
		sqlmeta.StatementTypeSelect,
		sqlmeta.StatementTypeShowTables,
		sqlmeta.StatementTypeShowCreateTable,
		sqlmeta.StatementTypeExistsTable,
		sqlmeta.StatementTypeUse,
	} {
		t.Run(typ.String(), func(t *testing.T) {
			ev := newEvent(typ, "alice", "system", "functions")
			if err := o.BeforeStatement(context.Background(), ev); err != nil {
				t.Fatalf("expected system read to be allowed, got %v", err)
			}
		})
	}
}

// TestPermission_AllowsMixedSystemAndTenantReads: the suggestion
// query mixes system.* with no tenant tables, but a SELECT that
// joins / unions a tenant DB with system.* must still gate on the
// tenant DB. The system entry should pass through; the tenant entry
// should be checked normally.
func TestPermission_AllowsMixedSystemAndTenantReads(t *testing.T) {
	st := fixturePerm("foo", "alice", "", 0)
	o := newPermObserver(st)

	ev := &commitgate.Event{
		Type:   sqlmeta.StatementTypeSelect,
		User:   "alice",
		Values: make(map[string]any),
		AccessedTables: []sqlmeta.AccessedTable{
			{OriginalDatabase: "system", OriginalTable: "tables", LogicalDatabase: "system"},
			{OriginalDatabase: "foo", OriginalTable: "t", LogicalDatabase: "foo"},
		},
	}
	if err := o.BeforeStatement(context.Background(), ev); err != nil {
		t.Fatalf("expected mixed read to be allowed, got %v", err)
	}
}

// TestPermission_RejectsSystemWrites: the system whitelist is
// read-only. INSERT / DDL targeting `system` must still fail-closed
// — `system` is not registered, so RetrieveDatabaseInfo misses and
// the standard "not registered" error fires. CH itself rejects
// writes to the system schema, but defence-in-depth.
func TestPermission_RejectsSystemWrites(t *testing.T) {
	st := NewInMemoryNetworkState()
	o := newPermObserver(st)

	for _, typ := range []sqlmeta.StatementType{
		sqlmeta.StatementTypeInsert,
		sqlmeta.StatementTypeAlterTable,
		sqlmeta.StatementTypeDropTable,
	} {
		t.Run(typ.String(), func(t *testing.T) {
			ev := newEvent(typ, "alice", "system", "settings")
			err := o.BeforeStatement(context.Background(), ev)
			if err == nil || !strings.Contains(err.Error(), "not registered") {
				t.Fatalf("expected system write to be rejected, got %v", err)
			}
		})
	}
}

func TestPermission_OwnerBypassesBitmap(t *testing.T) {
	// Alice holds the explicit Owner bit in DatabasePermissions.
	// Promotion (Owner→Admin→Write→Read) must grant her every
	// operation we subscribe to.
	st := fixturePerm("foo", "alice", "alice", registry.DbAuthOwner)
	o := newPermObserver(st)

	for _, tc := range []struct {
		name string
		typ  sqlmeta.StatementType
	}{
		{"select", sqlmeta.StatementTypeSelect},
		{"insert", sqlmeta.StatementTypeInsert},
		{"alter", sqlmeta.StatementTypeAlterTable},
		{"drop_db", sqlmeta.StatementTypeDropDatabase},
		{"grant", sqlmeta.StatementTypeGrant},
	} {
		t.Run(tc.name, func(t *testing.T) {
			table := "t"
			if tc.typ == sqlmeta.StatementTypeDropDatabase {
				table = ""
			}
			ev := newEvent(tc.typ, "alice", "foo", table)
			if err := o.BeforeStatement(context.Background(), ev); err != nil {
				t.Errorf("owner should be allowed for %s, got %v", tc.typ, err)
			}
		})
	}
}

func TestPermission_ReadBitGatesSelect(t *testing.T) {
	// Bob holds Read on "foo" owned by Alice.
	st := fixturePerm("foo", "alice", "bob", registry.DbAuthRead)
	o := newPermObserver(st)

	for _, tc := range []struct {
		name      string
		typ       sqlmeta.StatementType
		permitted bool
	}{
		// Read-bit-required → Bob (Read) passes.
		{"select_ok", sqlmeta.StatementTypeSelect, true},
		{"use_ok", sqlmeta.StatementTypeUse, true},
		{"show_tables_ok", sqlmeta.StatementTypeShowTables, true},

		// Write-bit-required → Bob (Read only) fails.
		{"insert_denied", sqlmeta.StatementTypeInsert, false},
		{"create_table_denied", sqlmeta.StatementTypeCreateTable, false},
		{"truncate_denied", sqlmeta.StatementTypeTruncateTable, false},
		{"create_view_denied", sqlmeta.StatementTypeCreateView, false},
		{"create_mat_view_denied", sqlmeta.StatementTypeCreateMaterializedView, false},
		{"drop_view_denied", sqlmeta.StatementTypeDropView, false},

		// Admin-bit-required → Bob (Read only) fails.
		{"alter_denied", sqlmeta.StatementTypeAlterTable, false},
		{"grant_denied", sqlmeta.StatementTypeGrant, false},

		// Owner-bit-required → Bob (Read only, not owner) fails.
		{"drop_db_denied", sqlmeta.StatementTypeDropDatabase, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			table := "t"
			if tc.typ == sqlmeta.StatementTypeDropDatabase || tc.typ == sqlmeta.StatementTypeUse {
				table = ""
			}
			ev := newEvent(tc.typ, "bob", "foo", table)
			err := o.BeforeStatement(context.Background(), ev)
			if tc.permitted && err != nil {
				t.Errorf("expected allow, got %v", err)
			}
			if !tc.permitted && err == nil {
				t.Errorf("expected deny, got nil")
			}
		})
	}
}

// TestPermission_ViewsRequireWrite verifies CREATE/DROP VIEW and
// CREATE MATERIALIZED VIEW are gated on the Write bit — symmetric with
// CREATE/DROP TABLE. An account holding exactly DbAuthWrite passes;
// the same statement from a Read-only account is rejected.
func TestPermission_ViewsRequireWrite(t *testing.T) {
	for _, typ := range []sqlmeta.StatementType{
		sqlmeta.StatementTypeCreateView,
		sqlmeta.StatementTypeCreateMaterializedView,
		sqlmeta.StatementTypeDropView,
	} {
		t.Run(typ.String()+"_write_allowed", func(t *testing.T) {
			st := fixturePerm("foo", "alice", "bob", registry.DbAuthWrite)
			o := newPermObserver(st)
			ev := newEvent(typ, "bob", "foo", "v")
			if err := o.BeforeStatement(context.Background(), ev); err != nil {
				t.Errorf("Write bit should satisfy %s, got %v", typ, err)
			}
		})
		t.Run(typ.String()+"_read_denied", func(t *testing.T) {
			st := fixturePerm("foo", "alice", "bob", registry.DbAuthRead)
			o := newPermObserver(st)
			ev := newEvent(typ, "bob", "foo", "v")
			if err := o.BeforeStatement(context.Background(), ev); err == nil {
				t.Errorf("Read-only account must NOT satisfy %s", typ)
			}
		})
	}
}

func TestPermission_WriteBitPromotesToRead(t *testing.T) {
	// Holding only DbAuthWrite must satisfy a Read check thanks to
	// the promotion chain (Write → Read).
	st := fixturePerm("foo", "alice", "bob", registry.DbAuthWrite)
	o := newPermObserver(st)

	ev := newEvent(sqlmeta.StatementTypeSelect, "bob", "foo", "t")
	if err := o.BeforeStatement(context.Background(), ev); err != nil {
		t.Errorf("Write should promote to Read for SELECT, got %v", err)
	}
}

func TestPermission_AdminBitDoesNotPromoteToWrite(t *testing.T) {
	// Admin alone does NOT satisfy Write checks (CREATE TABLE etc.)
	// — it only grants the ability to manage permissions (GRANT/REVOKE).
	st := fixturePerm("foo", "alice", "bob", registry.DbAuthAdmin)
	o := newPermObserver(st)

	if err := o.BeforeStatement(context.Background(),
		newEvent(sqlmeta.StatementTypeCreateTable, "bob", "foo", "t")); err == nil {
		t.Errorf("Admin alone must NOT satisfy Write for CREATE TABLE")
	}
	if err := o.BeforeStatement(context.Background(),
		newEvent(sqlmeta.StatementTypeDropDatabase, "bob", "foo", "")); err == nil {
		t.Errorf("Admin must NOT satisfy Owner-only DROP DATABASE")
	}
	// But Admin CAN do GRANT/REVOKE (Admin-level operations).
	if err := o.BeforeStatement(context.Background(),
		newEvent(sqlmeta.StatementTypeGrant, "bob", "foo", "")); err != nil {
		t.Errorf("Admin should satisfy Admin-level GRANT, got %v", err)
	}
}

func TestPermission_OwnerOnlyDropDatabase(t *testing.T) {
	// The Owner bit held in DatabasePermissions suffices for DROP
	// DATABASE. Both alice and bob hold the Owner bit explicitly.
	st := NewInMemoryNetworkState()
	st.DatabaseInfos["foo"] = DatabaseInfo{DatabaseId: "foo"}
	st.DatabasePermissions["alice"] = DatabasePermissions{"foo": registry.DbAuthOwner}
	st.DatabasePermissions["bob"] = DatabasePermissions{"foo": registry.DbAuthOwner}

	o := newPermObserver(st)

	if err := o.BeforeStatement(context.Background(),
		newEvent(sqlmeta.StatementTypeDropDatabase, "alice", "foo", "")); err != nil {
		t.Errorf("Owner-bit holder must be allowed to DROP DATABASE, got %v", err)
	}
	if err := o.BeforeStatement(context.Background(),
		newEvent(sqlmeta.StatementTypeDropDatabase, "bob", "foo", "")); err != nil {
		t.Errorf("explicit Owner-bit holder must be allowed to DROP DATABASE, got %v", err)
	}

	// Carol has nothing → denied.
	if err := o.BeforeStatement(context.Background(),
		newEvent(sqlmeta.StatementTypeDropDatabase, "carol", "foo", "")); err == nil {
		t.Errorf("non-owner without Owner bit must be denied")
	}
}

// TestPermission_MultiTarget_Union: principle behind the multi-target
// refactor. SELECT * FROM d1.t UNION SELECT * FROM d2.t lands two
// AccessedTables on the Event. The observer must check Read on EVERY
// entry; a deny on any one short-circuits with an account-aware
// message naming the offending DB.
func TestPermission_MultiTarget_Union(t *testing.T) {
	st := NewInMemoryNetworkState()
	st.DatabaseInfos["d1"] = DatabaseInfo{DatabaseId: "d1"}
	st.DatabaseInfos["d2"] = DatabaseInfo{DatabaseId: "d2"}
	st.DatabasePermissions["alice"] = DatabasePermissions{"d1": registry.DbAuthOwner, "d2": registry.DbAuthOwner}
	// Bob has Read on d1 only.
	st.DatabasePermissions["bob"] = DatabasePermissions{"d1": registry.DbAuthRead}
	o := newPermObserver(st)

	// Both targets — second one denies.
	ev := &commitgate.Event{
		Type: sqlmeta.StatementTypeSelect,
		User: "bob",
		AccessedTables: []sqlmeta.AccessedTable{
			{LogicalDatabase: "d1", OriginalTable: "t"},
			{LogicalDatabase: "d2", OriginalTable: "t"},
		},
	}
	err := o.BeforeStatement(context.Background(), ev)
	if err == nil {
		t.Fatal("expected deny on d2; got nil")
	}
	if !strings.Contains(err.Error(), `"d2"`) {
		t.Errorf("error should name the failing DB; got %v", err)
	}

	// Both within bob's perms now → passes.
	st.DatabasePermissions["bob"]["d2"] = registry.DbAuthRead
	if err := o.BeforeStatement(context.Background(), ev); err != nil {
		t.Errorf("expected allow when bob holds Read on both; got %v", err)
	}
}

// TestPermission_RejectsPendingDelete: a database with PendingDelete=true
// is being torn down by the network's cleanup flow. Reject every
// subscribed statement (read or write, including DROP DATABASE itself)
// so in-flight clients see a clear error instead of racing the deletion.
func TestPermission_RejectsPendingDelete(t *testing.T) {
	st := NewInMemoryNetworkState()
	st.DatabaseInfos["foo"] = DatabaseInfo{DatabaseId: "foo", PendingDelete: true}
	st.DatabasePermissions["alice"] = DatabasePermissions{"foo": registry.DbAuthOwner}
	o := newPermObserver(st)

	for _, tc := range []struct {
		name string
		typ  sqlmeta.StatementType
	}{
		{"select", sqlmeta.StatementTypeSelect},
		{"insert", sqlmeta.StatementTypeInsert},
		{"alter", sqlmeta.StatementTypeAlterTable},
		{"drop_db", sqlmeta.StatementTypeDropDatabase},
		{"grant", sqlmeta.StatementTypeGrant},
	} {
		t.Run(tc.name, func(t *testing.T) {
			table := "t"
			if tc.typ == sqlmeta.StatementTypeDropDatabase ||
				tc.typ == sqlmeta.StatementTypeGrant {
				table = ""
			}
			ev := newEvent(tc.typ, "alice", "foo", table)
			err := o.BeforeStatement(context.Background(), ev)
			if err == nil {
				t.Fatalf("expected reject for %s on pending-delete DB; got nil", tc.typ)
			}
			if !strings.Contains(err.Error(), "pending deletion") {
				t.Errorf("error should mention pending deletion; got %v", err)
			}
		})
	}
}

// TestPermission_FromlessSelect_Allowed: `SELECT 1` and friends
// surface zero AccessedTables. The observer allows them — there is
// nothing to gate. Guards the regression that previously rejected
// these queries with the misleading "rewriter did not surface
// accessed tables" message.
func TestPermission_FromlessSelect_Allowed(t *testing.T) {
	o := newPermObserver(NewInMemoryNetworkState())
	ev := &commitgate.Event{
		Type: sqlmeta.StatementTypeSelect,
		User: "bob",
		// Empty AccessedTables — FROM-less compute.
	}
	if err := o.BeforeStatement(context.Background(), ev); err != nil {
		t.Errorf("FROM-less SELECT must be allowed; got %v", err)
	}
}

// TestPermission_USE_UsesSynthesisedAccess: USE chen_test arrives at
// the observer with a single AccessedTables entry synthesised by
// commitgate.buildEvent from qctx.DatabaseRewrites. The observer
// treats it like any other Read check.
func TestPermission_USE_UsesSynthesisedAccess(t *testing.T) {
	st := fixturePerm("chen_test", "alice", "bob", registry.DbAuthRead)
	o := newPermObserver(st)
	ev := &commitgate.Event{
		Type: sqlmeta.StatementTypeUse,
		User: "bob",
		AccessedTables: []sqlmeta.AccessedTable{
			{LogicalDatabase: "chen_test", PhysicalDatabase: "testnet"},
		},
	}
	if err := o.BeforeStatement(context.Background(), ev); err != nil {
		t.Errorf("USE on a Read-permitted DB must succeed; got %v", err)
	}
}

// TestPermission_EmptyAccessedTables_FailsClosed: for any subscribed
// type other than the explicit allow-list (SELECT / SHOW DATABASES),
// missing AccessedTables means the rewriter contract drifted —
// reject with a clear message.
func TestPermission_EmptyAccessedTables_FailsClosed(t *testing.T) {
	o := newPermObserver(NewInMemoryNetworkState())
	ev := &commitgate.Event{
		Type: sqlmeta.StatementTypeCreateTable,
		User: "alice",
	}
	err := o.BeforeStatement(context.Background(), ev)
	if err == nil {
		t.Fatal("CREATE_TABLE without AccessedTables must be rejected")
	}
	if !strings.Contains(err.Error(), "no AccessedTables") {
		t.Errorf("error should call out missing AccessedTables; got %v", err)
	}
}

// TestPermission_PublicAddress_OrsIntoEffectivePerms: a permission
// granted to PublicAccountAddress (the all-zeros Ethereum address)
// must be reachable by every other authenticated account, in addition
// to whatever that account's own bitmap holds.
func TestPermission_PublicAddress_OrsIntoEffectivePerms(t *testing.T) {
	st := NewInMemoryNetworkState()
	st.DatabaseInfos["foo"] = DatabaseInfo{DatabaseId: "foo"}
	// Bob has nothing of his own; the public address holds Read.
	st.DatabasePermissions[PublicAccountAddress] = DatabasePermissions{"foo": registry.DbAuthRead}
	o := newPermObserver(st)

	// Bob can SELECT thanks to the public Read grant.
	if err := o.BeforeStatement(context.Background(),
		newEvent(sqlmeta.StatementTypeSelect, "bob", "foo", "t")); err != nil {
		t.Errorf("public Read should let bob SELECT, got %v", err)
	}

	// Bob still cannot INSERT — the public grant only covers Read.
	if err := o.BeforeStatement(context.Background(),
		newEvent(sqlmeta.StatementTypeInsert, "bob", "foo", "t")); err == nil {
		t.Errorf("public Read must NOT promote to Write for INSERT")
	}

	// Bob's own Write bit unions with the public Read — INSERT now passes.
	st.DatabasePermissions["bob"] = DatabasePermissions{"foo": registry.DbAuthWrite}
	if err := o.BeforeStatement(context.Background(),
		newEvent(sqlmeta.StatementTypeInsert, "bob", "foo", "t")); err != nil {
		t.Errorf("bob's own Write should allow INSERT, got %v", err)
	}
}

// TestPermission_PublicAddress_NoFalseAllowWithoutPublicGrant: control
// case for the public-address feature — when the public address has
// nothing on a database, accounts without their own grant remain
// denied.
func TestPermission_PublicAddress_NoFalseAllowWithoutPublicGrant(t *testing.T) {
	st := fixturePerm("foo", "alice", "", 0) // no public grants
	o := newPermObserver(st)

	if err := o.BeforeStatement(context.Background(),
		newEvent(sqlmeta.StatementTypeSelect, "bob", "foo", "t")); err == nil {
		t.Errorf("bob with no perms and no public grant must be denied")
	}
}

// TestPermission_PublicAddress_SelfLookupSkipped: when the executor IS
// the public address, the observer should not double-count public
// permissions. The single bitmap lookup must still satisfy the gate.
func TestPermission_PublicAddress_SelfLookupSkipped(t *testing.T) {
	st := NewInMemoryNetworkState()
	st.DatabaseInfos["foo"] = DatabaseInfo{DatabaseId: "foo"}
	st.DatabasePermissions[PublicAccountAddress] = DatabasePermissions{"foo": registry.DbAuthRead}
	o := newPermObserver(st)

	ev := newEvent(sqlmeta.StatementTypeSelect, string(PublicAccountAddress), "foo", "t")
	if err := o.BeforeStatement(context.Background(), ev); err != nil {
		t.Errorf("public address acting as caller should still pass its own Read, got %v", err)
	}
}

// TestPermission_Operator_Allowed: when the JWS signer is a registered
// operator of the owner (per InMemoryNetworkState.Operators, which
// production wires off the on-chain Permissions contract via the
// statemirror), the observer evaluates DDL/DCL perms against the
// OWNER's bitmap (not the signer's). Owner has Owner bit on `foo`,
// signer (operator) has nothing → DROP DATABASE succeeds.
func TestPermission_Operator_Allowed(t *testing.T) {
	owner := AccountAddress("0xa1e4252cfc8f1a14350d4b25ee2f97809a177117")
	signer := AccountAddress("0xcb4d7ec41a9098420f97ae393a38b0299f6efb64")
	st := fixturePerm("foo", owner, "", 0)
	st.SetOperator(owner, signer, true)
	o := newPermObserver(st)

	ev := newEvent(sqlmeta.StatementTypeDropDatabase, string(signer), "foo", "")
	ev.Owner = string(owner)
	if err := o.BeforeStatement(context.Background(), ev); err != nil {
		t.Errorf("operator with valid isOperator(owner, signer) should pass DROP DATABASE on owner's db; got %v", err)
	}
}

// TestPermission_Operator_NotAuthorized: signer claims owner via
// SQL_x_payer / Event.Owner but State.IsOperator returns false →
// reject with a clear error; the signer's own perms (or lack thereof)
// are irrelevant.
func TestPermission_Operator_NotAuthorized(t *testing.T) {
	owner := AccountAddress("0xa1e4252cfc8f1a14350d4b25ee2f97809a177117")
	signer := AccountAddress("0xcb4d7ec41a9098420f97ae393a38b0299f6efb64")
	st := fixturePerm("foo", owner, "", 0)
	o := newPermObserver(st) // no operator relation registered

	ev := newEvent(sqlmeta.StatementTypeDropDatabase, string(signer), "foo", "")
	ev.Owner = string(owner)
	err := o.BeforeStatement(context.Background(), ev)
	if err == nil {
		t.Fatalf("unauthorized operator must be rejected")
	}
	if !strings.Contains(err.Error(), "not an authorized operator") {
		t.Errorf("error should mention operator-of failure, got %v", err)
	}
}

// TestPermission_NoOwner_LegacyPath: when Event.Owner is empty (legacy
// single-key agent), the observer behaves exactly as before — gates
// on the signer's bitmap. Confirms the operator block is fully no-op
// for backwards compatibility.
func TestPermission_NoOwner_LegacyPath(t *testing.T) {
	owner := AccountAddress("0xa1e4252cfc8f1a14350d4b25ee2f97809a177117")
	st := fixturePerm("foo", owner, "", 0)
	o := newPermObserver(st)

	ev := newEvent(sqlmeta.StatementTypeDropDatabase, string(owner), "foo", "")
	// Owner intentionally empty.
	if err := o.BeforeStatement(context.Background(), ev); err != nil {
		t.Errorf("legacy single-key path (Owner==\"\") should pass for owner; got %v", err)
	}
}

func TestPermission_OnStatementExceptionNoOp(t *testing.T) {
	// The observer is read-only; OnStatementException is a no-op.
	// This test guards against accidental introduction of mutating
	// rollback logic (which would break the idempotency contract).
	st := fixturePerm("foo", "alice", "", 0)
	before, beforePerms := snapshotState(st)

	o := newPermObserver(st)
	ev := newEvent(sqlmeta.StatementTypeSelect, "alice", "foo", "t")
	o.OnStatementException(context.Background(), ev, &chproto.Exception{Code: 60, Message: "boom"})

	after, afterPerms := snapshotState(st)
	if len(before) != len(after) || len(beforePerms) != len(afterPerms) {
		t.Fatalf("OnStatementException must not mutate state")
	}
}
