package storageintegrity

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/plugin"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/sitable"
	"github.com/housegate/housegate/pkg/sqlmeta"
)

// registrySchema differs from the network state's latest declaration
// (ingressSchema) by one column, as a registry schema that is the first
// declaration after creation can (umbrella D3).
func registrySchema() payloadexec.TableSchema {
	return payloadexec.TableSchema{TableID: "tenant.events", Columns: []lthash.Column{{Name: "id", Type: "UInt64"}}}
}

func activeSnapshot(status sitable.Status) sitable.Snapshot {
	schema := registrySchema()
	return sitable.NewSnapshot(3, sitable.Ordinary, []sitable.Table{{
		ID: "tenant.events", Status: status, Schema: schema, SchemaHash: payloadexec.TableSchemaHash("testnet-v2", schema),
	}})
}

func dropStatementToken(qctx *plugin.QueryContext) {
	kept := qctx.Query.Settings[:0]
	for _, s := range qctx.Query.Settings {
		if s.Key != auth.StatementTokenSettingKey {
			kept = append(kept, s)
		}
	}
	qctx.Query.Settings = kept
}

func snapshotQueryContext(t *testing.T, id int64, signer *auth.RelaySigner, snap sitable.Snapshot) *plugin.QueryContext {
	t.Helper()
	sql := "INSERT INTO tenant.events FORMAT Native"
	qctx := signedQueryContext(t, id, signer, sql, sql, sqlmeta.StatementTypeInsert)
	qctx.AccessedTables = []sqlmeta.AccessedTable{{IsStorageIntegrity: true, OriginalDatabase: "tenant", OriginalTable: "events", LogicalDatabase: "tenant"}}
	qctx.TableSnapshot = snap
	dropStatementToken(qctx)
	return qctx
}

// TestIngressSnapshotSchemaWinsOverTheLatestDeclaration is spec 2026-09-24
// §9.1: with a snapshot the ingress binds the registry schema and hash, not
// the network state's latest chain declaration.
func TestIngressSnapshotSchemaWinsOverTheLatestDeclaration(t *testing.T) {
	p, signer, latestHash := newV2Ingress(t)
	registryHash := payloadexec.TableSchemaHash("testnet-v2", registrySchema())
	if registryHash == latestHash {
		t.Fatal("fixture: the two schemas must hash differently")
	}
	payload := []byte{byte(chproto.ClientDataCode), 0, 0xab, 0xcd}

	qctx := snapshotQueryContext(t, 40, signer, activeSnapshot(sitable.Active))
	withStatementToken(t, qctx, signer, v2Statement(signer, qctx.Query.ID, qctx.OriginalSQL, registryHash, payload, 54453))
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if err := p.OnClientDataStrict(context.Background(), qctx, payload); err != nil {
		t.Fatal(err)
	}
	p.OnQueryInputComplete(context.Background(), qctx)
	adm, err := p.ConsumeAdmission(qctx.Session.ID())
	if err != nil {
		t.Fatalf("a token over the registry schema must be admitted: %v", err)
	}
	if adm.SchemaHash != registryHash || adm.TableSchema == nil || len(adm.TableSchema.Columns) != 1 {
		t.Fatalf("admission schema = %s %+v, want the registry schema", adm.SchemaHash, adm.TableSchema)
	}

	stale := snapshotQueryContext(t, 41, signer, activeSnapshot(sitable.Active))
	withStatementToken(t, stale, signer, v2Statement(signer, stale.Query.ID, stale.OriginalSQL, latestHash, payload, 54453))
	if err := p.OnQuery(context.Background(), stale); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if err := p.OnClientDataStrict(context.Background(), stale, payload); err != nil {
		t.Fatal(err)
	}
	p.OnQueryInputComplete(context.Background(), stale)
	if _, err := p.ConsumeAdmission(stale.Session.ID()); err == nil || !strings.Contains(err.Error(), "statement token rejected") {
		t.Fatalf("a token over the latest declaration must be rejected, got %v", err)
	}
}

func TestIngressSnapshotAdmitsOnlyActiveTables(t *testing.T) {
	p, signer, _ := newV2Ingress(t)
	for _, status := range []sitable.Status{sitable.Pending, sitable.Refused, sitable.Gone, sitable.Ordinary} {
		qctx := snapshotQueryContext(t, 42, signer, activeSnapshot(status))
		withDefaultCaptureToken(t, qctx, signer, []byte{byte(chproto.ClientDataCode), 0, 0xab, 0xcd})
		err := p.OnQuery(context.Background(), qctx)
		if err == nil || !strings.Contains(err.Error(), "is not active") {
			t.Fatalf("status %s: err = %v, want an active-only refusal", status, err)
		}
	}
}

// TestIngressStaleViewUnsignedInsertIsRetryable is spec 2026-09-24 §10.3.
func TestIngressStaleViewUnsignedInsertIsRetryable(t *testing.T) {
	p, signer, _ := newV2Ingress(t)
	qctx := snapshotQueryContext(t, 43, signer, activeSnapshot(sitable.Active))
	qctx.Query.ID = "8d7f5b0e-client-generated"
	err := p.OnQuery(context.Background(), qctx)
	var ce *chproto.ClientError
	if !errors.As(err, &ce) || ce.Code != chproto.CodeTableIsBeingRestarted ||
		ce.Message != "storage_integrity: table tenant.events requires a signed INSERT; the client's table state is stale (retryable)" {
		t.Fatalf("err = %v, want the retryable stale-view refusal", err)
	}
	if ce.KeepSession {
		t.Fatal("an OnQuery refusal already keeps the session; KeepSession must stay unset")
	}
}

// TestIngressWithoutSnapshotKeepsTheLoaderPath pins that a disabled
// deployment (no snapshot) still resolves the declared schema and names a
// missing statement id, exactly as before.
func TestIngressWithoutSnapshotKeepsTheLoaderPath(t *testing.T) {
	p, signer, _ := newV2Ingress(t)
	sql := "INSERT INTO tenant.events FORMAT Native"
	qctx := signedQueryContext(t, 44, signer, sql, sql, sqlmeta.StatementTypeInsert)
	qctx.Query.ID = ""
	if err := p.OnQuery(context.Background(), qctx); err == nil || !strings.Contains(err.Error(), "query id is required") {
		t.Fatalf("err = %v, want the unchanged statement-id error", err)
	}
}

// TestIngressEnabledWithoutSnapshotRefuses pins spec 2026-09-24 §9.1 on an
// enabled deployment: the registry's schema must win, so a query that reaches
// the ingress without a snapshot is refused, non-retryable, instead of falling
// back to the chain's latest declaration. sitablestate refuses first in the
// wired chain; this is the ingress's own fail-closed backstop, with and
// without a declared-schema loader configured.
func TestIngressEnabledWithoutSnapshotRefuses(t *testing.T) {
	payload := []byte{byte(chproto.ClientDataCode), 0, 0xab, 0xcd}
	ns, latestHash := ingressNetworkState(t)
	for _, tc := range []struct {
		name string
		cfg  Config
	}{
		{name: "with a declared-schema loader", cfg: Config{TableSchemas: ns, NetworkID: "testnet-v2", RequireTableSnapshot: true}},
		{name: "without a declared-schema loader", cfg: Config{NetworkID: "testnet-v2", RequireTableSnapshot: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, signer := newSignedIngressWithConfig(t, tc.cfg)
			sql := "INSERT INTO tenant.events FORMAT Native"
			qctx := signedQueryContext(t, 45, signer, sql, sql, sqlmeta.StatementTypeInsert)
			dropStatementToken(qctx)
			withStatementToken(t, qctx, signer, v2Statement(signer, qctx.Query.ID, qctx.OriginalSQL, latestHash, payload, 54453))
			err := p.OnQuery(context.Background(), qctx)
			var ce *chproto.ClientError
			if !errors.As(err, &ce) || ce.Code != chproto.CodeQueryIsProhibited ||
				ce.Message != "storage_integrity: table state is unavailable for this query" {
				t.Fatalf("err = %v, want the non-retryable table-state-unavailable refusal", err)
			}
			if ce.KeepSession {
				t.Fatal("an OnQuery refusal already keeps the session; KeepSession must stay unset")
			}
		})
	}
}
