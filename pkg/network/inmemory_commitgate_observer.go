package network

import (
	"context"
	"fmt"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/log"
	"github.com/housegate/housegate/pkg/plugins/commitgate"
	"github.com/housegate/housegate/pkg/registry"
	"github.com/housegate/housegate/pkg/sqlmeta"

	"github.com/bytedance/sonic"
)

// InMemoryCommitGateObserver mutates an InMemoryNetworkState in
// lock-step with DDL / DCL passing through commitgate. It exists for
// development and tests where a real on-chain commit isn't wired in:
// each successful BeforeStatement call updates DatabaseInfos and
// DatabasePermissions to reflect the effect the DDL/DCL will have once
// ClickHouse runs it.
//
// Subscribed types (caller-controlled via NewInMemoryCommitGateObserver):
//   - StatementTypeCreateDatabase / StatementTypeDropDatabase: tracks
//     ownership and the per-database existence record.
//   - StatementTypeCreateTable / StatementTypeDropTable: tracks the
//     table list inside DatabaseInfos.
//   - StatementTypeCreateView / StatementTypeCreateMaterializedView /
//     StatementTypeDropView: tracks the view as a DatabaseInfo.Tables
//     entry (TableType = "VIEW" / "MATERIALIZED_VIEW"), symmetric with
//     the CREATE / DROP TABLE path.
//   - StatementTypeGrant / StatementTypeRevoke: applies each
//     PrivilegeDelta to DatabasePermissions. SCOPE_TABLE collapses to
//     the per-DB bitmap (the in-memory model has no per-table
//     resolution); CURRENT_USER grantees resolve to the executing
//     account; deltas with PrivilegeCategoryNone (unknown CH privilege
//     name) are skipped with a warn.
//
// The observer is idempotent — replaying the same Event is a no-op
// when the in-memory state already reflects the change. This matches
// commitgate's contract that an Observer may be invoked more than
// once for the same logical intent (client retry, etc.).
//
// Operations that would conflict with existing state (e.g. creating
// a database that already belongs to a different owner, dropping a
// database the caller does not own) return an error and the chain
// short-circuits before ClickHouse is contacted.
//
// Rollback: when ClickHouse subsequently rejects a statement that
// passed BeforeStatement (and produced an in-memory mutation), the
// commitgate plugin fires OnStatementException. The observer reverts
// the mutation by invoking a closure stashed in ev.Values during
// BeforeStatement. Rollback is best-effort — see commitgate.Observer
// godoc for the delivery guarantees.
type InMemoryCommitGateObserver struct {
	*InMemoryNetworkState
	types        []sqlmeta.StatementType
	getIndexerId func() uint64
}

// inMemoryRollbackKey namespaces the rollback closure inside
// commitgate.Event.Values so a different observer can safely stash
// its own state under a separate key.
const inMemoryRollbackKey = "network/inmemory.rollback"

// NewInMemoryCommitGateObserver builds an observer that mutates
// inMemoryNetworkState in lock-step with the subscribed DDL types.
//
// getIndexerId, when non-nil, is invoked on every CREATE DATABASE so
// the resulting DatabaseInfo.IndexerId reflects the indexer this proxy
// represents — keeping the in-memory state byte-compatible with what a
// real on-chain producer would write. Pass nil in tests where
// IndexerId binding does not matter; the field then stays zero.
func NewInMemoryCommitGateObserver(inMemoryNetworkState *InMemoryNetworkState, types []sqlmeta.StatementType, getIndexerId func() uint64) *InMemoryCommitGateObserver {
	return &InMemoryCommitGateObserver{
		InMemoryNetworkState: inMemoryNetworkState,
		types:                types,
		getIndexerId:         getIndexerId,
	}
}

func (o *InMemoryCommitGateObserver) selfIndexerId() uint64 {
	if o.getIndexerId == nil {
		return 0
	}
	return o.getIndexerId()
}

func (o *InMemoryCommitGateObserver) SubscribedTypes() []sqlmeta.StatementType {
	return o.types
}

func (o *InMemoryCommitGateObserver) BeforeStatement(ctx context.Context, ev *commitgate.Event) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	// All subscribed types (CREATE/DROP DATABASE, CREATE/DROP TABLE,
	// CREATE/DROP VIEW, CREATE MATERIALIZED VIEW, GRANT, REVOKE) are
	// single-target by ClickHouse parser semantics, so AccessedTables[0]
	// is exhaustive. commitgate.buildEvent
	// mirrors the GRANT/REVOKE first-delta target onto AccessedTables
	// for symmetry, so this branch is uniform across DDL and DCL.
	if len(ev.AccessedTables) == 0 {
		return fmt.Errorf("inmemory: %s requires a target (rewriter surfaced no AccessedTables)", ev.Type)
	}
	target := ev.AccessedTables[0]
	db := Database(target.LogicalDatabase)
	tableID := target.OriginalTable
	if db == "" {
		return fmt.Errorf("inmemory: %s has unresolved logical database", ev.Type)
	}
	account := AccountAddress(ev.User)

	switch ev.Type {
	case sqlmeta.StatementTypeCreateDatabase:
		// Idempotent: same owner replays as a no-op; a different
		// owner is a conflict and aborts.
		if _, ok := o.DatabaseInfos[db]; ok {
			if !accountIsOwnerLocked(o.DatabasePermissions, account, db) {
				return fmt.Errorf("database %q already exists and account %s is not the owner", db, account)
			}
			return nil
		}
		o.DatabaseInfos[db] = DatabaseInfo{
			DatabaseId: string(db),
			IndexerId:  o.selfIndexerId(),
		}
		// Store ownership as an explicit permission bit.
		if o.DatabasePermissions[account] == nil {
			o.DatabasePermissions[account] = make(DatabasePermissions)
		}
		o.DatabasePermissions[account][db] |= registry.DbAuthOwner
		o.stashRollback(ev, func() {
			delete(o.DatabaseInfos, db)
			if perms, ok := o.DatabasePermissions[account]; ok {
				delete(perms, db)
				if len(perms) == 0 {
					delete(o.DatabasePermissions, account)
				}
			}
		})
		log.Infow("created database", "database", db, "owner", account)

	case sqlmeta.StatementTypeDropDatabase:
		info, ok := o.DatabaseInfos[db]
		if !ok {
			// Idempotent: dropping a non-existent database is a
			// no-op (matches DROP DATABASE IF EXISTS semantics on
			// the ClickHouse side).
			return nil
		}
		if !accountIsOwnerLocked(o.DatabasePermissions, account, db) {
			return fmt.Errorf("account %s lacks owner permission on database %q", account, db)
		}
		// Capture pre-state for rollback BEFORE deleting.
		snapshotInfo := info
		type permEntry struct {
			account AccountAddress
			auth    registry.DbAuth
		}
		var capturedPerms []permEntry
		for acct, m := range o.DatabasePermissions {
			if a, ok := m[db]; ok {
				capturedPerms = append(capturedPerms, permEntry{acct, a})
			}
		}
		delete(o.DatabaseInfos, db)
		// Sweep stale permission entries; collapse empty per-account
		// maps so DatabasePermissions stays clean.
		for acct, perms := range o.DatabasePermissions {
			delete(perms, db)
			if len(perms) == 0 {
				delete(o.DatabasePermissions, acct)
			}
		}
		o.stashRollback(ev, func() {
			o.DatabaseInfos[db] = snapshotInfo
			for _, p := range capturedPerms {
				if o.DatabasePermissions[p.account] == nil {
					o.DatabasePermissions[p.account] = make(DatabasePermissions)
				}
				o.DatabasePermissions[p.account][db] = p.auth
			}
		})
		log.Infow("dropped database", "database", db, "owner", account)

	case sqlmeta.StatementTypeCreateTable:
		if tableID == "" {
			return fmt.Errorf("inmemory: CREATE TABLE missing table name")
		}
		info, ok := o.DatabaseInfos[db]
		if !ok {
			return fmt.Errorf("database %q does not exist", db)
		}
		if !accountCanWriteLocked(o.DatabasePermissions, account, db) {
			return fmt.Errorf("account %s lacks write permission on database %q", account, db)
		}
		// Idempotent: table already present → no-op (no rollback
		// stashed, so OnStatementException does nothing).
		for _, t := range info.Tables {
			if t.TableId == tableID {
				return nil
			}
		}
		info.Tables = append(info.Tables, TableInfo{TableId: tableID})
		o.DatabaseInfos[db] = info
		o.stashRollback(ev, func() {
			info, ok := o.DatabaseInfos[db]
			if !ok {
				return
			}
			for i, t := range info.Tables {
				if t.TableId == tableID {
					info.Tables = append(info.Tables[:i], info.Tables[i+1:]...)
					o.DatabaseInfos[db] = info
					return
				}
			}
		})
		log.Infow("created table", "database", db, "table", tableID, "by", account)

	case sqlmeta.StatementTypeDropTable:
		if tableID == "" {
			return fmt.Errorf("inmemory: DROP TABLE missing table name")
		}
		info, ok := o.DatabaseInfos[db]
		if !ok {
			// Database gone → DROP TABLE is implicitly idempotent.
			return nil
		}
		if !accountCanWriteLocked(o.DatabasePermissions, account, db) {
			return fmt.Errorf("account %s lacks write permission on database %q", account, db)
		}
		var droppedTable *TableInfo
		var droppedIdx = -1
		for i, t := range info.Tables {
			if t.TableId == tableID {
				copied := t
				droppedTable = &copied
				droppedIdx = i
				break
			}
		}
		if droppedTable == nil {
			// Idempotent: table not present → no-op.
			break
		}
		info.Tables = append(info.Tables[:droppedIdx], info.Tables[droppedIdx+1:]...)
		o.DatabaseInfos[db] = info
		restored := *droppedTable
		restoreIdx := droppedIdx
		o.stashRollback(ev, func() {
			info, ok := o.DatabaseInfos[db]
			if !ok {
				return
			}
			// Re-insert at the original index so rollback preserves
			// the ordering callers may have observed via
			// RetrieveDatabaseInfo before the DROP. If the slice has
			// shrunk (concurrent unrelated mutation), append.
			if restoreIdx >= 0 && restoreIdx <= len(info.Tables) {
				info.Tables = append(info.Tables[:restoreIdx],
					append([]TableInfo{restored}, info.Tables[restoreIdx:]...)...)
			} else {
				info.Tables = append(info.Tables, restored)
			}
			o.DatabaseInfos[db] = info
		})
		log.Infow("dropped table", "database", db, "table", tableID, "by", account)

	case sqlmeta.StatementTypeCreateView, sqlmeta.StatementTypeCreateMaterializedView:
		// A view is tracked as an entry in DatabaseInfo.Tables — it
		// behaves like a table for SHOW TABLES visibility and
		// name-resolution. TableType records the kind so consumers can
		// tell views from real tables. Mirrors the CREATE TABLE path:
		// the target DB must exist and the account needs Write.
		if tableID == "" {
			return fmt.Errorf("inmemory: %s missing view name", ev.Type)
		}
		info, ok := o.DatabaseInfos[db]
		if !ok {
			return fmt.Errorf("database %q does not exist", db)
		}
		if !accountCanWriteLocked(o.DatabasePermissions, account, db) {
			return fmt.Errorf("account %s lacks write permission on database %q", account, db)
		}
		tableType := "VIEW"
		if ev.Type == sqlmeta.StatementTypeCreateMaterializedView {
			tableType = "MATERIALIZED_VIEW"
		}
		// Idempotent: an entry with this name already present → no-op
		// (no rollback stashed, so OnStatementException does nothing).
		for _, t := range info.Tables {
			if t.TableId == tableID {
				return nil
			}
		}
		info.Tables = append(info.Tables, TableInfo{TableId: tableID, TableType: tableType})
		o.DatabaseInfos[db] = info
		o.stashRollback(ev, func() {
			info, ok := o.DatabaseInfos[db]
			if !ok {
				return
			}
			for i, t := range info.Tables {
				if t.TableId == tableID {
					info.Tables = append(info.Tables[:i], info.Tables[i+1:]...)
					o.DatabaseInfos[db] = info
					return
				}
			}
		})
		log.Infow("created view", "database", db, "view", tableID, "kind", tableType, "by", account)

	case sqlmeta.StatementTypeDropView:
		// Symmetric with DROP TABLE: remove the entry by name. The
		// in-memory model does not enforce that the entry is actually a
		// view (ClickHouse does); removal is keyed on TableId alone.
		if tableID == "" {
			return fmt.Errorf("inmemory: DROP VIEW missing view name")
		}
		info, ok := o.DatabaseInfos[db]
		if !ok {
			// Database gone → DROP VIEW is implicitly idempotent.
			return nil
		}
		if !accountCanWriteLocked(o.DatabasePermissions, account, db) {
			return fmt.Errorf("account %s lacks write permission on database %q", account, db)
		}
		var droppedTable *TableInfo
		var droppedIdx = -1
		for i, t := range info.Tables {
			if t.TableId == tableID {
				copied := t
				droppedTable = &copied
				droppedIdx = i
				break
			}
		}
		if droppedTable == nil {
			// Idempotent: view not present → no-op.
			break
		}
		info.Tables = append(info.Tables[:droppedIdx], info.Tables[droppedIdx+1:]...)
		o.DatabaseInfos[db] = info
		restored := *droppedTable
		restoreIdx := droppedIdx
		o.stashRollback(ev, func() {
			info, ok := o.DatabaseInfos[db]
			if !ok {
				return
			}
			if restoreIdx >= 0 && restoreIdx <= len(info.Tables) {
				info.Tables = append(info.Tables[:restoreIdx],
					append([]TableInfo{restored}, info.Tables[restoreIdx:]...)...)
			} else {
				info.Tables = append(info.Tables, restored)
			}
			o.DatabaseInfos[db] = info
		})
		log.Infow("dropped view", "database", db, "view", tableID, "by", account)

	case sqlmeta.StatementTypeGrant, sqlmeta.StatementTypeRevoke:
		// Validate target DB exists and the executing account may
		// administer it. CH parser limits a GRANT/REVOKE to one
		// target so AccessedTables[0] (mirrored by buildEvent from
		// the first PrivilegeDelta) is the authoritative target.
		_, ok := o.DatabaseInfos[db]
		if !ok {
			return fmt.Errorf("database %q does not exist", db)
		}
		if !accountCanAdminLocked(o.DatabasePermissions, account, db) {
			return fmt.Errorf("account %s lacks admin permission on database %q", account, db)
		}
		applied := o.applyPrivilegesLocked(ev, account)
		log.Infow("applied privilege delta",
			"type", ev.Type,
			"database", db,
			"by", account,
			"deltas", len(ev.PrivilegesDeltas),
			"affected_pairs", applied,
		)
	}

	metaJSON, _ := sonic.Marshal(o.InMemoryNetworkState)
	log.Infow("updated in-memory network state", "event", ev.Type, "network_state", string(metaJSON))
	return nil
}

// OnStatementException reverts the in-memory mutation that
// BeforeStatement performed for ev. No-op when no rollback closure
// was stashed (idempotent BeforeStatement no-op path, or stash
// missing because the dispatch path was preempted).
func (o *InMemoryCommitGateObserver) OnStatementException(_ context.Context, ev *commitgate.Event, exc *chproto.Exception) {
	if ev == nil || ev.Values == nil {
		return
	}
	rollback, ok := ev.Values[inMemoryRollbackKey].(func())
	if !ok || rollback == nil {
		return
	}
	o.mu.Lock()
	rollback()
	o.mu.Unlock()
	chCode := int32(0)
	chMessage := ""
	if exc != nil {
		chCode = int32(exc.Code)
		chMessage = exc.Message
	}
	log.Infow("commitgate: rolled back in-memory state after CH exception",
		"type", ev.Type,
		"accessed_tables", ev.AccessedTables,
		"ch_code", chCode,
		"ch_message", chMessage,
	)
}

// stashRollback installs fn as the rollback closure for ev under
// inMemoryRollbackKey. Closures take no args and run with o.mu held
// by OnStatementException — they must NOT re-acquire o.mu.
func (o *InMemoryCommitGateObserver) stashRollback(ev *commitgate.Event, fn func()) {
	if ev == nil || ev.Values == nil {
		return
	}
	ev.Values[inMemoryRollbackKey] = fn
}

func accountIsOwnerLocked(perms map[AccountAddress]DatabasePermissions, account AccountAddress, db Database) bool {
	if p, ok := perms[account]; ok {
		return p[db]&registry.DbAuthOwner != 0
	}
	return false
}

// accountCanWriteLocked reports whether `account` may CREATE / DROP
// tables in `db`. The caller must already hold InMemoryNetworkState.mu.
//
// Only Owner and Write bits grant write access. Admin alone does NOT
// imply write — it only grants the ability to manage permissions.
func accountCanWriteLocked(perms map[AccountAddress]DatabasePermissions, account AccountAddress, db Database) bool {
	if p, ok := perms[account]; ok {
		const writeBits = registry.DbAuthOwner | registry.DbAuthWrite
		if p[db]&writeBits != 0 {
			return true
		}
	}
	return false
}

// accountCanAdminLocked reports whether `account` may GRANT / REVOKE
// on `db`. The caller must already hold InMemoryNetworkState.mu.
//
// "Admin" here is Owner OR Admin bit — Write is intentionally NOT
// sufficient. The Owner bit is expected to be stored explicitly in
// DatabasePermissions.
func accountCanAdminLocked(perms map[AccountAddress]DatabasePermissions, account AccountAddress, db Database) bool {
	if p, ok := perms[account]; ok {
		const adminBits = registry.DbAuthOwner | registry.DbAuthAdmin
		if p[db]&adminBits != 0 {
			return true
		}
	}
	return false
}

// categoryToDbAuth maps a sqlmeta.PrivilegeCategory bitmap (the
// rewriter's coarse classification of a GRANT/REVOKE privilege list)
// onto the registry.DbAuth bits the in-memory state stores. The two
// taxonomies are aligned by design — see sqlmeta.PrivilegeCategory
// godoc — so the mapping is bit-for-bit. PrivilegeCategoryNone maps
// to zero, which the caller treats as "skip this delta".
func categoryToDbAuth(c sqlmeta.PrivilegeCategory) registry.DbAuth {
	var bits registry.DbAuth
	if c&sqlmeta.PrivilegeCategoryRead != 0 {
		bits |= registry.DbAuthRead
	}
	if c&sqlmeta.PrivilegeCategoryWrite != 0 {
		bits |= registry.DbAuthWrite
	}
	if c&sqlmeta.PrivilegeCategoryAdmin != 0 {
		bits |= registry.DbAuthAdmin
	}
	return bits
}

// privPermKey identifies one (grantee, database) pair touched by a
// privilege application. Used to keep the snapshot map / rollback
// closure small (one entry per affected pair) even when multiple
// deltas in the same statement target the same pair.
type privPermKey struct {
	account AccountAddress
	db      Database
}

// privPermSnapshot captures the pre-mutation state of a single
// (grantee, database) entry so OnStatementException can restore it
// byte-for-byte. existed=false means "no entry was present"; rollback
// then deletes whatever we created.
type privPermSnapshot struct {
	existed bool
	auth    registry.DbAuth
}

// applyPrivilegesLocked walks ev.PrivilegesDeltas and mutates
// DatabasePermissions accordingly, capturing per-pair pre-state in
// the rollback closure stashed under inMemoryRollbackKey. Returns the
// number of distinct (grantee, db) pairs the call touched (for
// logging only). Caller must hold InMemoryNetworkState.mu.
//
// Resolution rules:
//   - g.IsCurrentUser → resolve to the executing `executor` account.
//   - Empty / unspecified action → skip.
//   - PrivilegeCategoryNone → skip with warn (unknown CH privilege).
//   - Empty grantee Name (and not CurrentUser) → skip (defensive).
//
// Map cleanliness:
//   - A REVOKE that empties a (grantee, db) entry deletes it.
//   - A GRANT that lands on a fresh entry creates the inner map
//     lazily.
//   - The rollback closure restores both shapes faithfully (recreate
//     deleted entries, drop entries that did not exist pre-mutation).
func (o *InMemoryCommitGateObserver) applyPrivilegesLocked(ev *commitgate.Event, executor AccountAddress) int {
	snapshots := make(map[privPermKey]privPermSnapshot)
	snap := func(k privPermKey) {
		if _, taken := snapshots[k]; taken {
			return // never overwrite — keep the earliest pre-mutation view
		}
		if m, ok := o.DatabasePermissions[k.account]; ok {
			if v, ok := m[k.db]; ok {
				snapshots[k] = privPermSnapshot{existed: true, auth: v}
				return
			}
		}
		snapshots[k] = privPermSnapshot{existed: false}
	}

	for _, d := range ev.PrivilegesDeltas {
		if d.LogicalDatabase == "" {
			continue
		}
		bits := categoryToDbAuth(d.Category)
		if bits == 0 {
			log.Warnw("commitgate: skipping privilege delta with unrecognised category",
				"action", d.Action,
				"privileges", d.Privileges,
				"database", d.LogicalDatabase,
			)
			continue
		}
		targetDB := Database(d.LogicalDatabase)
		for _, g := range d.Grantees {
			granteeAcct := AccountAddress(g.Name)
			if g.IsCurrentUser {
				granteeAcct = executor
			}
			if granteeAcct == "" {
				continue
			}
			k := privPermKey{account: granteeAcct, db: targetDB}
			snap(k)

			cur := registry.DbAuth(0)
			if m, ok := o.DatabasePermissions[granteeAcct]; ok {
				cur = m[targetDB]
			}
			var next registry.DbAuth
			switch d.Action {
			case sqlmeta.PrivilegeActionGrant:
				next = cur | bits
			case sqlmeta.PrivilegeActionRevoke:
				next = cur &^ bits
			default:
				continue
			}
			if next == 0 {
				if m, ok := o.DatabasePermissions[granteeAcct]; ok {
					delete(m, targetDB)
					if len(m) == 0 {
						delete(o.DatabasePermissions, granteeAcct)
					}
				}
				continue
			}
			if o.DatabasePermissions[granteeAcct] == nil {
				o.DatabasePermissions[granteeAcct] = make(DatabasePermissions)
			}
			o.DatabasePermissions[granteeAcct][targetDB] = next
		}
	}

	if len(snapshots) > 0 {
		o.stashRollback(ev, func() {
			for k, snap := range snapshots {
				if snap.existed {
					if o.DatabasePermissions[k.account] == nil {
						o.DatabasePermissions[k.account] = make(DatabasePermissions)
					}
					o.DatabasePermissions[k.account][k.db] = snap.auth
					continue
				}
				if m, ok := o.DatabasePermissions[k.account]; ok {
					delete(m, k.db)
					if len(m) == 0 {
						delete(o.DatabasePermissions, k.account)
					}
				}
			}
		})
	}

	return len(snapshots)
}

// Compile-time check the observer satisfies the commitgate contract.
var _ commitgate.Observer = (*InMemoryCommitGateObserver)(nil)
