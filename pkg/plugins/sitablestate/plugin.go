// Package sitablestate enforces the storage-integrity table lifecycle on
// every query (spec 2026-09-24 §7). It runs after the rewrite plugin, reads
// the query's single table-state snapshot, the rewriter's StatementType and
// AccessedTables, and refuses what a table's status does not allow:
// Pending data access is retryable, Refused is not, a Gone table is answered
// as an unknown table, and data-carrying creation into a governed table is
// refused. Everything the matrix allows passes through untouched.
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
	classCreateData       // CREATE ... AS SELECT, or MATERIALIZED VIEW ... POPULATE
	classDrop             // DROP TABLE / DROP VIEW
	classOtherDDL         // ALTER, RENAME
	classViewTarget       // the TO target of a MATERIALIZED VIEW
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
		}
	case sitable.Active:
		switch c {
		case classCreateData, classViewTarget:
			return nonRetryable, dataCarrying(t.ID)
		}
	case sitable.Gone:
		switch c {
		case classNone:
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
	add := func(t sqlmeta.AccessedTable, c class) {
		db := t.LogicalDatabase
		if db == "" {
			db = t.OriginalDatabase
		}
		if db == "" {
			db = sessionDB
		}
		out = append(out, access{database: db, table: t.OriginalTable, class: c})
	}
	sql := qctx.OriginalSQL
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
		populate, toDB, toTable, hasTo := materializedViewHeader(sql)
		for i, t := range tables {
			switch {
			case i == 0:
				add(t, pick(populate, classCreateData, classCreate))
			case hasTo && t.OriginalTable == toTable && t.OriginalDatabase == toDB:
				add(t, classViewTarget)
			default:
				add(t, classRead)
			}
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
