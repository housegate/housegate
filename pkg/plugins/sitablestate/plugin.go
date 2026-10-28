// Package sitablestate enforces the storage-integrity table lifecycle on
// every query (spec 2026-09-24 §7). It runs after the rewrite plugin, reads
// the query's single table-state snapshot, the rewriter's StatementType and
// AccessedTables, and refuses what a table's status does not allow:
// Pending data access is retryable, Refused is not, a Gone table is answered
// as an unknown table, and data-carrying creation into a governed table
// (Pending or Active, and Refused too for creation with data: a CREATE TABLE
// the header lexer cannot prove schema-only, a materialized view writing TO
// it, or a materialized view named like it) is refused, as is a materialized
// view whose header cannot be read. A Replica table, another SI indexer's
// table this host serves read-only from its member's copy (spec 2026-10-14
// §11), admits reads and metadata only, and with a ReplicaReads counter the
// plugin counts every allowed query that reads one until it completes.
// Everything the matrix allows passes through untouched.
package sitablestate

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/plugin"
	"github.com/housegate/housegate/pkg/sitable"
	"github.com/housegate/housegate/pkg/sqlmeta"
)

// The three refusal classes (plan ruling R3). They alias chproto so the
// signed ingress and the runtime answer with the same codes.
const (
	CodeRetryable    = chproto.CodeTableIsBeingRestarted // 733 TABLE_IS_BEING_RESTARTED
	CodeNonRetryable = chproto.CodeQueryIsProhibited     // 392 QUERY_IS_PROHIBITED
	CodeUnknownTable = chproto.CodeUnknownTable          // 60 UNKNOWN_TABLE
)

// Plugin is the server-mode table-state QueryPlugin. It is registered only
// when storage_integrity.enabled.
type Plugin struct {
	// ReplicaReads, when set, admits and counts every allowed query that
	// accesses a Replica table, once per database, from OnQuery until
	// OnQueryComplete or OnClose (spec 2026-10-14 §11), and only while the
	// host's fence of each such database is open (ReplicaReads.Admit): the
	// host releases a database's serving lease only once none is in flight,
	// and no read starts once its lease's local deadline minus
	// serve.HostDeadlineMargin passed. build.go then also registers the plugin
	// as a QueryCompletePlugin and a ClosePlugin. Nil counts nothing.
	ReplicaReads *sitable.ReplicaReads
	// TableState is the port rewrite took the query's snapshot from. With
	// ReplicaReads, a read is admitted only once it is counted and
	// TableState.Current() still serves every Replica table it accesses from
	// the copy the query was planned against (clarification K44); without a
	// TableState no replica read is admitted.
	TableState sitable.TableState

	// replicaReadEnds maps the session, by its *chsession.SessionState, to
	// the end of its in-flight replica read. A session runs one query at a
	// time. It is not keyed by sess.ID(): the external and internal listeners
	// share this gate and number their sessions independently.
	replicaReadEnds sync.Map
}

type class uint8

const (
	classNone       class = iota
	classRead             // data read
	classWrite            // data write
	classMetadata         // DESCRIBE, SHOW, EXISTS
	classCreate           // CREATE of this name, no data
	classCreateData       // CREATE that may carry data, or any MATERIALIZED VIEW's own name
	classDrop             // DROP TABLE / DROP VIEW
	classOtherDDL         // ALTER, RENAME
	classViewTarget       // the TO target of a MATERIALIZED VIEW
	classUnreadable       // a MATERIALIZED VIEW whose header (and so its name and target) cannot be read
)

type severity uint8

const (
	allow severity = iota
	retryable
	nonRetryable
	unknownTable
)

type access struct {
	database string
	table    string
	class    class
}

// OnQuery refuses the statement when any accessed table refuses it. When
// several do, an unknown table wins over a non-retryable refusal, which wins
// over a retryable one; ties keep the first accessed table. With ReplicaReads
// set, an allowed statement that accesses a Replica table begins one
// in-flight read of each such database, which OnQueryComplete or OnClose
// ends, and is admitted only while the host's fence of each such database is
// open and the current snapshot still serves its Replica tables from the
// copies it was planned against (733 otherwise).
//
// A missing snapshot fails closed. The plugin is registered only when storage
// integrity is enabled, and rewrite, which runs first under the same chain
// filters, attaches a snapshot to every query it sees (maintenance and
// platform-operator sessions included), so nil can only mean a wiring defect.
func (p *Plugin) OnQuery(_ context.Context, qctx *plugin.QueryContext) error {
	if qctx == nil || qctx.Session == nil {
		return nil
	}
	if qctx.TableSnapshot == nil {
		return &chproto.ClientError{Code: CodeNonRetryable, Message: "storage_integrity: table state is unavailable for this query"}
	}
	state := qctx.Session.State().Snapshot()
	if state.Maintenance || state.PlatformOperator {
		return nil
	}
	sessionDB := qctx.Session.State().LogicalDatabaseName()
	var worst severity
	var worstErr error
	var served []servedAccess
	for _, a := range accessesOf(qctx, sessionDB) {
		t := qctx.TableSnapshot.Lookup(a.database, a.table)
		sev, err := decide(t, a.class)
		if sev > worst {
			worst, worstErr = sev, err
		}
		if t.Status == sitable.Replica {
			served = append(served, servedAccess{database: a.database, table: a.table, planned: t})
		}
	}
	if worstErr == nil && len(served) > 0 && p.ReplicaReads != nil {
		return p.beginReplicaRead(qctx.Session, served)
	}
	return worstErr
}

// servedAccess is one accessed table that the query's snapshot answers
// Replica, as that snapshot answers it.
type servedAccess struct {
	database, table string
	planned         sitable.Table
}

// beginReplicaRead admits the session's query as one in-flight read of each
// replica-served database it accesses, or refuses it, retryably, before
// anything reaches ClickHouse (clarification K44). Two checks guard the copy:
//   - ReplicaReads.Admit counts the read only while the host's fence of each
//     database is open. The host keeps it at its local lease deadline minus
//     serve.HostDeadlineMargin and closes it when it stops serving the
//     database, so no read starts once the member may install on the lease's
//     expiry, even while a blocked table-state refresh leaves a published
//     snapshot that still answers the database Replica.
//   - After that count, the current snapshot must still serve every such
//     table from the copy and owner the query was planned against. rewrite
//     took the query's snapshot before this count, so a query that waited
//     there while the host published a snapshot without the database, found
//     no read in flight and released the lease could otherwise read the copy
//     during its member's install.
//
// The host reads InFlight only after it closed the fence and published the
// snapshot that drops the database, so either it sees this read counted and
// waits, or one of the checks refuses it. A refused read is uncounted at
// once, and an end the session still holds is ended either way, so a lost
// complete never leaves a database counted.
func (p *Plugin) beginReplicaRead(sess chsession.Session, served []servedAccess) error {
	databases := make([]string, 0, len(served))
	for _, s := range served {
		databases = append(databases, s.database)
	}
	slices.Sort(databases)
	end, closed := p.ReplicaReads.Admit(slices.Compact(databases))
	if end == nil {
		p.endReplicaRead(sess)
		for _, s := range served {
			if s.database == closed {
				return notServed(s.planned.ID)
			}
		}
		return notServed(served[0].planned.ID)
	}
	if previous, loaded := p.replicaReadEnds.Swap(sess.State(), end); loaded {
		previous.(func())()
	}
	var current sitable.Snapshot
	if p.TableState != nil {
		current = p.TableState.Current()
	}
	for _, s := range served {
		if current == nil || !sameCopy(current.Lookup(s.database, s.table), s.planned) {
			p.endReplicaRead(sess)
			return notServed(s.planned.ID)
		}
	}
	return nil
}

// notServed refuses a read of a copy that is no longer served to the query:
// the host's fence of its database is closed, or the current snapshot no
// longer serves the copy and owner the query was planned against.
func notServed(id string) error {
	return refuse(CodeRetryable, "storage_integrity: table %s is no longer served from the copy this query was planned against; retry the query (retryable)", id)
}

// sameCopy reports whether now answers a table Replica from the same copy
// and owner as planned.
func sameCopy(now, planned sitable.Table) bool {
	return now.Status == sitable.Replica && now.ID == planned.ID && now.CopyTable == planned.CopyTable && now.ReplicaOf == planned.ReplicaOf
}

// endReplicaRead ends the session's in-flight replica read, if any.
func (p *Plugin) endReplicaRead(sess chsession.Session) {
	if sess == nil {
		return
	}
	if end, ok := p.replicaReadEnds.LoadAndDelete(sess.State()); ok {
		end.(func())()
	}
}

// OnQueryComplete ends the in-flight replica read OnQuery began for the
// session's query. Calling it again, or for a query that read no copy, does
// nothing. The relay admits the session's next query only once this query's
// terminal hooks returned, so the read it ends is this query's own.
func (p *Plugin) OnQueryComplete(_ context.Context, sess chsession.Session) {
	p.endReplicaRead(sess)
}

// OnClose ends any replica read still in flight when the session closes: a
// client that disconnects mid-query never reaches OnQueryComplete.
func (p *Plugin) OnClose(sess chsession.Session) {
	p.endReplicaRead(sess)
}

// decide is the §7.2 matrix plus the §7.3 rules for one accessed table.
func decide(t sitable.Table, c class) (severity, error) {
	if c == classUnreadable {
		// The view's name and target are unknown, so either may be governed:
		// fail closed whatever the engine reported.
		return nonRetryable, refuse(CodeNonRetryable, "storage_integrity: the materialized view header cannot be read, so its view name and target may be governed by storage integrity; create the table first, then INSERT")
	}
	switch t.Status {
	case sitable.Pending:
		switch c {
		case classRead, classWrite:
			return retryable, refuse(CodeRetryable, "storage_integrity: table %s is pending activation (retryable)", t.ID)
		case classCreateData, classViewTarget:
			return nonRetryable, dataCarrying(t.ID)
		case classOtherDDL:
			return nonRetryable, refuse(CodeNonRetryable, "storage_integrity: table %s is governed by storage integrity; ALTER and RENAME are not supported", t.ID)
		}
	case sitable.Refused:
		switch c {
		case classRead, classWrite, classOtherDDL, classViewTarget:
			return nonRetryable, &chproto.ClientError{Code: CodeNonRetryable, Message: chproto.TableRefusedMessage(t.ID, t.RefusedCode, t.RefusedReason)}
		case classCreateData:
			// For data-carrying creation a Refused name is governed too
			// (spec §7.3 rule 2), so it cannot be dropped and refilled by
			// CREATE ... AS SELECT while it is still Refused.
			return nonRetryable, dataCarrying(t.ID)
		}
	case sitable.Active:
		switch c {
		case classCreateData, classViewTarget:
			return nonRetryable, dataCarrying(t.ID)
		}
	case sitable.Gone:
		switch c {
		case classCreate, classCreateData:
			return retryable, refuse(CodeRetryable, "storage_integrity: table %s is still being purged; retry CREATE later (retryable)", t.ID)
		default:
			return unknownTable, refuse(CodeUnknownTable, "Table %s does not exist", t.ID)
		}
	case sitable.Replica:
		// Spec 2026-10-14 §11: the copy is another indexer's published data,
		// so only reads and metadata are answered here.
		switch c {
		case classWrite, classCreate, classCreateData, classDrop, classOtherDDL, classViewTarget:
			return nonRetryable, refuse(CodeNonRetryable, "storage_integrity: table %s is served read-only from a copy of indexer %d's published data; write through its owner", t.ID, t.ReplicaOf)
		}
	}
	return allow, nil
}

func dataCarrying(id string) error {
	return refuse(CodeNonRetryable, "storage_integrity: table %s is governed by storage integrity and cannot be created with data; create the table first, then INSERT", id)
}

func refuse(code int32, format string, args ...any) error {
	return &chproto.ClientError{Code: code, Message: fmt.Sprintf(format, args...)}
}

// accessesOf assigns a class to every accessed table from the rewriter's
// statement type. Unrecognised types with accessed tables are treated as data
// reads, the conservative class.
func accessesOf(qctx *plugin.QueryContext, sessionDB string) []access {
	tables := qctx.AccessedTables
	out := make([]access, 0, len(tables))
	databaseOf := func(t sqlmeta.AccessedTable) string {
		db := t.LogicalDatabase
		if db == "" {
			db = t.OriginalDatabase
		}
		if db == "" {
			db = sessionDB
		}
		return db
	}
	add := func(t sqlmeta.AccessedTable, c class) {
		out = append(out, access{database: databaseOf(t), table: t.OriginalTable, class: c})
	}
	switch qctx.StatementType {
	case sqlmeta.StatementTypeCreateDatabase, sqlmeta.StatementTypeDropDatabase,
		sqlmeta.StatementTypeGrant, sqlmeta.StatementTypeRevoke:
		return nil
	case sqlmeta.StatementTypeInsert:
		for i, t := range tables {
			add(t, pick(i == 0, classWrite, classRead))
		}
	case sqlmeta.StatementTypeUpdate, sqlmeta.StatementTypeDelete, sqlmeta.StatementTypeTruncateTable:
		for _, t := range tables {
			add(t, classWrite)
		}
	case sqlmeta.StatementTypeDescribe, sqlmeta.StatementTypeShowCreateTable, sqlmeta.StatementTypeExistsTable,
		sqlmeta.StatementTypeShowTables, sqlmeta.StatementTypeShowDatabases, sqlmeta.StatementTypeUse:
		for _, t := range tables {
			add(t, classMetadata)
		}
	case sqlmeta.StatementTypeCreateTable:
		// Lex what ClickHouse will execute: after rewrite, Query.Body is the
		// engine-normalised forwarded SQL (comments and heredocs normalised, an
		// EMPTY AS SELECT body dropped). OriginalSQL is the fallback only when
		// there is no query packet.
		sql := qctx.OriginalSQL
		if qctx.Query != nil && qctx.Query.Body != "" {
			sql = qctx.Query.Body
		}
		withData := createTableCarriesData(sql)
		for i, t := range tables {
			switch {
			case i == 0:
				add(t, pick(withData, classCreateData, classCreate))
			default:
				add(t, pick(withData, classRead, classMetadata))
			}
		}
	case sqlmeta.StatementTypeCreateView:
		for i, t := range tables {
			add(t, pick(i == 0, classCreate, classRead))
		}
	case sqlmeta.StatementTypeCreateMaterializedView:
		// The view's own name and its TO target come from the parsed header of
		// the original SQL, with logical names and the session database as the
		// default, never from AccessedTables: the engine omits a REFRESH ... TO
		// target from AccessedTables, and its ordering is not a contract. Any
		// view whose own name is governed is refused in any form, because its
		// inner storage ingests rows under that name. An unreadable header is
		// refused whatever the engine reported.
		h, readable := materializedViewHeader(qctx.OriginalSQL)
		if !readable {
			out = append(out, access{class: classUnreadable})
			for _, t := range tables {
				add(t, classRead)
			}
			return out
		}
		if h.viewDatabase == "" {
			h.viewDatabase = sessionDB
		}
		if h.toDatabase == "" {
			h.toDatabase = sessionDB
		}
		out = append(out, access{database: h.viewDatabase, table: h.view, class: classCreateData})
		for _, t := range tables {
			switch db := databaseOf(t); {
			case t.OriginalTable == h.view && db == h.viewDatabase:
				// The view itself, decided above.
			case h.hasTo && t.OriginalTable == h.toTable && db == h.toDatabase:
				// Decided once, below, as the view target.
			default:
				add(t, classRead)
			}
		}
		if h.hasTo {
			out = append(out, access{database: h.toDatabase, table: h.toTable, class: classViewTarget})
		}
	case sqlmeta.StatementTypeDropTable, sqlmeta.StatementTypeDropView:
		for _, t := range tables {
			add(t, classDrop)
		}
	case sqlmeta.StatementTypeAlterTable, sqlmeta.StatementTypeRenameTable:
		for _, t := range tables {
			add(t, classOtherDDL)
		}
	default:
		for _, t := range tables {
			add(t, classRead)
		}
	}
	return out
}

func pick(cond bool, yes, no class) class {
	if cond {
		return yes
	}
	return no
}

// RunOnPeerTrust opts out like rewrite: a peer-trusted remote() loopback runs
// SQL its origin already decided. IsForwardedFromPeer overrides this in the
// chain, so the receiving host of a forward pivot runs the plugin.
func (*Plugin) RunOnPeerTrust() bool { return false }

// RunOnForward opts out on the origin side of a forward pivot; the host that
// owns the database decides.
func (*Plugin) RunOnForward() bool { return false }

var (
	_ plugin.QueryPlugin         = (*Plugin)(nil)
	_ plugin.QueryCompletePlugin = (*Plugin)(nil)
	_ plugin.ClosePlugin         = (*Plugin)(nil)
	_ plugin.PeerTrustAware      = (*Plugin)(nil)
	_ plugin.ForwardAware        = (*Plugin)(nil)
)
