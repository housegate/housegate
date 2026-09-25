// Package sitablestate enforces the storage-integrity table lifecycle on
// every query (spec 2026-09-24 §7). It runs after the rewrite plugin, reads
// the query's single table-state snapshot, the rewriter's StatementType and
// AccessedTables, and refuses what a table's status does not allow:
// Pending data access is retryable, Refused is not, a Gone table is answered
// as an unknown table, and data-carrying creation into a governed table
// (Pending or Active, and Refused too for creation with data: a CREATE TABLE
// the header lexer cannot prove schema-only, a materialized view writing TO
// it, or a materialized view named like it) is refused, as is a materialized
// view whose header cannot be read. Everything the matrix allows passes
// through untouched.
package sitablestate

import (
	"context"
	"fmt"

	"github.com/housegate/housegate/pkg/chproto"
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
type Plugin struct{}

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
// over a retryable one; ties keep the first accessed table.
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
	for _, a := range accessesOf(qctx, sessionDB) {
		sev, err := decide(qctx.TableSnapshot.Lookup(a.database, a.table), a.class)
		if sev > worst {
			worst, worstErr = sev, err
		}
	}
	return worstErr
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
			return nonRetryable, refuse(CodeNonRetryable, "storage_integrity: table %s was refused: %s: %s", t.ID, t.RefusedCode, t.RefusedReason)
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
	_ plugin.QueryPlugin    = (*Plugin)(nil)
	_ plugin.PeerTrustAware = (*Plugin)(nil)
	_ plugin.ForwardAware   = (*Plugin)(nil)
)
