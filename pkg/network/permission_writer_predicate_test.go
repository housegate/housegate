package network

import (
	"context"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/plugin"
	"github.com/housegate/housegate/pkg/plugins/commitgate"
	"github.com/housegate/housegate/pkg/registry"
	"github.com/housegate/housegate/pkg/sqlmeta"
)

const (
	predIndexer = "0x00000000000000000000000000000000000000aa"
	predWriter  = "0x00000000000000000000000000000000000000d1"
	predReader  = "0x00000000000000000000000000000000000000d2"
	predOwner   = "0x00000000000000000000000000000000000000d3"
	predNobody  = "0x00000000000000000000000000000000000000d4"
)

func predicateState() *InMemoryNetworkState {
	st := NewInMemoryNetworkState()
	st.IndexerInfos[5] = IndexerInfo{IndexerId: 5, Signer: predIndexer}
	st.DatabaseInfos["db"] = DatabaseInfo{DatabaseId: "db", IndexerId: 5}
	st.DatabasePermissions[predWriter] = DatabasePermissions{"db": registry.DbAuthWrite}
	st.DatabasePermissions[predReader] = DatabasePermissions{"db": registry.DbAuthRead}
	st.DatabasePermissions[predOwner] = DatabasePermissions{"db": registry.DbAuthOwner}
	st.DatabasePermissions[WildcardAddress] = DatabasePermissions{"db": registry.DbAuthWrite | registry.DbAuthRead}
	return st
}

func TestPermissionContractPredicate(t *testing.T) {
	st := predicateState()
	contract := NewPermissionCommitGateObserverWithWriters(st, st)
	bitmap := NewPermissionCommitGateObserver(st)
	for _, tc := range []struct {
		name         string
		typ          sqlmeta.StatementType
		user         string
		wantContract bool
		wantBitmap   bool
	}{
		{"wildcard Write no longer admits INSERT", sqlmeta.StatementTypeInsert, predNobody, false, true},
		{"wildcard Write no longer admits CREATE TABLE", sqlmeta.StatementTypeCreateTable, predNobody, false, true},
		{"wildcard Read still admits SELECT", sqlmeta.StatementTypeSelect, predNobody, true, true},
		{"indexer signer writes its own database", sqlmeta.StatementTypeInsert, predIndexer, true, true},
		{"Write grantee", sqlmeta.StatementTypeInsert, predWriter, true, true},
		{"Owner", sqlmeta.StatementTypeDropTable, predOwner, true, true},
		{"Read grantee cannot write", sqlmeta.StatementTypeInsert, predReader, false, true},
		{"Admin statements unchanged", sqlmeta.StatementTypeGrant, predWriter, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ev := newEvent(tc.typ, tc.user, "db", "t")
			if err := contract.BeforeStatement(context.Background(), ev); (err == nil) != tc.wantContract {
				t.Fatalf("contract: err = %v, want allowed=%v", err, tc.wantContract)
			}
			ev = newEvent(tc.typ, tc.user, "db", "t")
			if err := bitmap.BeforeStatement(context.Background(), ev); (err == nil) != tc.wantBitmap {
				t.Fatalf("bitmap: err = %v, want allowed=%v", err, tc.wantBitmap)
			}
		})
	}
}

func TestPermissionContractPredicate_OperatorPrincipal(t *testing.T) {
	st := predicateState()
	st.SetOperator(predOwner, predNobody, true)
	o := NewPermissionCommitGateObserverWithWriters(st, st)
	ev := newEvent(sqlmeta.StatementTypeInsert, predNobody, "db", "t")
	ev.Owner = predOwner
	if err := o.BeforeStatement(context.Background(), ev); err != nil {
		t.Fatalf("operator for an Owner must write: %v", err)
	}
}

func TestPermissionContractPredicate_RefusalNamesWrite(t *testing.T) {
	st := predicateState()
	o := NewPermissionCommitGateObserverWithWriters(st, st)
	err := o.BeforeStatement(context.Background(), newEvent(sqlmeta.StatementTypeInsert, predReader, "db", "t"))
	if err == nil || !strings.Contains(err.Error(), "lacks write on database \"db\"") {
		t.Fatalf("err = %v, want the lacks-write message", err)
	}
}

// driverSession backs commitgate.Plugin.OnQuery, which reads only State() and
// a nil Upstream(); the embedded nil Session panics on any other method.
type driverSession struct {
	chsession.Session
	state *chsession.SessionState
}

func (s driverSession) State() *chsession.SessionState { return s.state }
func (s driverSession) Upstream() *chproto.Codec       { return nil }

// A driver session (indexer-signed indexer-driver traffic) is skipped by the
// commitgate plugin before any observer runs, so the contract predicate never
// sees it: the same INSERT a non-driver nobody is refused for passes.
func TestPermissionContractPredicate_DriverSessionUnaffected(t *testing.T) {
	st := predicateState()
	gate := commitgate.NewPlugin([]commitgate.Observer{NewPermissionCommitGateObserverWithWriters(st, st)})
	newQctx := func(driver bool) *plugin.QueryContext {
		state := chsession.NewSessionState()
		state.Identity.UserID = predNobody
		state.SetIsDriver(driver)
		return &plugin.QueryContext{
			Session:       driverSession{state: state},
			Query:         &chproto.Query{ID: "qid-1"},
			StatementType: sqlmeta.StatementTypeInsert,
			AccessedTables: []sqlmeta.AccessedTable{{
				OriginalDatabase: "db", OriginalTable: "t", LogicalDatabase: "db",
			}},
		}
	}
	if err := gate.OnQuery(context.Background(), newQctx(true)); err != nil {
		t.Fatalf("driver session must bypass the permission observer: %v", err)
	}
	if err := gate.OnQuery(context.Background(), newQctx(false)); err == nil {
		t.Fatal("non-driver nobody must be refused by the contract predicate")
	}
}
