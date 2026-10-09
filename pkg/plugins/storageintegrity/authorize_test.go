package storageintegrity

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/registry"
	"github.com/housegate/housegate/pkg/sqlmeta"
)

const (
	azIndexer  = "0x00000000000000000000000000000000000000aa"
	azSigner   = "0x00000000000000000000000000000000000000c1"
	azOwner    = "0x00000000000000000000000000000000000000c2"
	azReader   = "0x00000000000000000000000000000000000000c3"
	azAdmin    = "0x00000000000000000000000000000000000000c4"
	azStranger = "0x00000000000000000000000000000000000000c5"
)

func authzState() *network.InMemoryNetworkState {
	ns := network.NewInMemoryNetworkState()
	ns.IndexerInfos[3] = network.IndexerInfo{IndexerId: 3, Signer: azIndexer}
	ns.DatabaseInfos["tenant"] = network.DatabaseInfo{DatabaseId: "tenant", IndexerId: 3}
	ns.DatabasePermissions[azSigner] = network.DatabasePermissions{"tenant": registry.DbAuthWrite}
	ns.DatabasePermissions[azOwner] = network.DatabasePermissions{"tenant": registry.DbAuthOwner}
	ns.DatabasePermissions[azReader] = network.DatabasePermissions{"tenant": registry.DbAuthRead}
	ns.DatabasePermissions[azAdmin] = network.DatabasePermissions{"tenant": registry.DbAuthAdmin | registry.DbAuthRead}
	ns.DatabasePermissions[network.WildcardAddress] = network.DatabasePermissions{"tenant": registry.DbAuthWrite}
	ns.SetOperator(azOwner, azReader, true) // azReader may act for azOwner
	return ns
}

func TestWriteAuthorizer_Matrix(t *testing.T) {
	ns := authzState()
	for _, tc := range []struct {
		name          string
		denied        []string
		allowed       []string
		signer, payer string
		database      string
		wantResult    string
		wantPrincipal string
		wantMessage   string
	}{
		{name: "signer is a writer", signer: azSigner, database: "tenant", wantResult: "allowed", wantPrincipal: azSigner},
		{name: "operator for a writer owner", signer: azReader, payer: "'" + strings.ToUpper(azOwner[:2]) + azOwner[2:] + "'", database: "tenant", wantResult: "allowed", wantPrincipal: azOwner},
		{name: "payer equal to signer", signer: azSigner, payer: azSigner, database: "tenant", wantResult: "allowed", wantPrincipal: azSigner},
		{name: "indexer signer", signer: azIndexer, database: "tenant", wantResult: "allowed", wantPrincipal: azIndexer},
		{name: "invalid operator relation", signer: azSigner, payer: azOwner, database: "tenant", wantResult: "operator_invalid", wantMessage: "storage_integrity: " + azSigner + " is not an operator of " + azOwner},
		{name: "denylisted signer", denied: []string{azSigner}, signer: azSigner, database: "tenant", wantResult: "denied_signer", wantMessage: "storage_integrity: signer " + azSigner + " is not permitted to write storage-integrity tables"},
		{name: "denylisted owner", denied: []string{azOwner}, signer: azReader, payer: azOwner, database: "tenant", wantResult: "denied_owner", wantMessage: "storage_integrity: owner " + azOwner + " is not permitted to write storage-integrity tables"},
		{name: "read-only grantee", signer: azReader, database: "tenant", wantResult: "not_writer", wantMessage: "storage_integrity: " + azReader + " is not a writer of database tenant"},
		{name: "admin-only grantee", signer: azAdmin, database: "tenant", wantResult: "not_writer", wantMessage: "storage_integrity: " + azAdmin + " is not a writer of database tenant"},
		{name: "address(0) Write grant only", signer: azStranger, database: "tenant", wantResult: "not_writer", wantMessage: "storage_integrity: " + azStranger + " is not a writer of database tenant"},
		{name: "allowlist without signer", allowed: []string{azOwner}, signer: azSigner, database: "tenant", wantResult: "denied_signer", wantMessage: "storage_integrity: signer " + azSigner + " is not permitted to write storage-integrity tables"},
		{name: "unknown database", signer: azSigner, database: "missing", wantResult: "unknown_database", wantMessage: "storage_integrity: " + azSigner + " is not a writer of database missing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := newWriteAuthorizer(tc.denied, tc.allowed, ns, ns)
			got, result, err := a.authorize(tc.signer, tc.payer, tc.database)
			if result != tc.wantResult {
				t.Fatalf("result = %q, want %q (err=%v)", result, tc.wantResult, err)
			}
			if tc.wantMessage == "" {
				if err != nil || got.principal != tc.wantPrincipal {
					t.Fatalf("authorize = %+v, %v; want principal %s", got, err, tc.wantPrincipal)
				}
				return
			}
			var clientErr *chproto.ClientError
			if !errors.As(err, &clientErr) || clientErr.Code != chproto.CodeAccessDenied || clientErr.Message != tc.wantMessage || clientErr.KeepSession {
				t.Fatalf("err = %#v, want 497 %q without KeepSession", err, tc.wantMessage)
			}
		})
	}
}

func TestWriteAuthorizer_RequiresWriterAccess(t *testing.T) {
	a := newWriteAuthorizer(nil, nil, nil, nil)
	_, result, err := a.authorize(azSigner, "", "tenant")
	if err == nil || result != "" || !strings.Contains(err.Error(), "requires a registry that implements WriterAccess") {
		t.Fatalf("authorize = %q, %v; want the wiring refusal", result, err)
	}
}

// Spec 2026-10-09 §6.2: the authorizer runs again before the admission is
// handed to the consumer, so a revocation seen during a long upload stops
// the write.
func TestIngressAuthorizerRechecksBeforeAdmission(t *testing.T) {
	signer, err := auth.NewRelaySigner(storageIntegrityTestKey)
	if err != nil {
		t.Fatal(err)
	}
	ns, _ := ingressNetworkState(t)
	ns.DatabaseInfos["tenant"] = network.DatabaseInfo{DatabaseId: "tenant"}
	account := network.AccountAddress(strings.ToLower(signer.Address()))
	ns.DatabasePermissions[account] = network.DatabasePermissions{"tenant": registry.DbAuthWrite}
	p, _ := newSignedIngressWithConfig(t, Config{TableSchemas: ns, NetworkID: "testnet-v2", Writers: ns, Operators: ns})
	sql := "INSERT INTO tenant.events FORMAT Native"
	qctx := signedQueryContext(t, 61, signer, sql, sql, sqlmeta.StatementTypeInsert)
	qctx.AccessedTables = []sqlmeta.AccessedTable{{IsStorageIntegrity: true, OriginalDatabase: "tenant", OriginalTable: "events"}}
	payload := []byte{byte(chproto.ClientDataCode), 0, 0xab, 0xcd}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if err := p.OnClientDataStrict(context.Background(), qctx, payload); err != nil {
		t.Fatalf("OnClientDataStrict: %v", err)
	}
	delete(ns.DatabasePermissions, account) // revoked mid-upload
	p.OnQueryInputComplete(context.Background(), qctx)
	_, err = p.ConsumeAdmission(qctx.Session.ID())
	var clientErr *chproto.ClientError
	if !errors.As(err, &clientErr) || clientErr.Code != chproto.CodeAccessDenied || !strings.Contains(clientErr.Message, "is not a writer of database tenant") {
		t.Fatalf("ConsumeAdmission err = %v, want the 497 not-a-writer refusal", err)
	}
}

// A forwarded-from-peer session runs the ingress on the host, which resolves
// the owner from the query's own SQL_x_payer rather than trusting qctx.Owner.
func TestIngressForwardedSessionUsesHostResolvedOwner(t *testing.T) {
	signer, err := auth.NewRelaySigner(storageIntegrityTestKey)
	if err != nil {
		t.Fatal(err)
	}
	signerAddr := strings.ToLower(signer.Address())
	ns, _ := ingressNetworkState(t)
	ns.DatabaseInfos["tenant"] = network.DatabaseInfo{DatabaseId: "tenant"}
	ns.DatabasePermissions[azOwner] = network.DatabasePermissions{"tenant": registry.DbAuthOwner}
	ns.SetOperator(azOwner, network.AccountAddress(signerAddr), true)
	p, _ := newSignedIngressWithConfig(t, Config{TableSchemas: ns, NetworkID: "testnet-v2", Writers: ns, Operators: ns})
	sql := "INSERT INTO tenant.events FORMAT Native"
	qctx := signedQueryContext(t, 62, signer, sql, sql, sqlmeta.StatementTypeInsert)
	qctx.Session.State().SetPeerTrustForwarded("10.0.0.2:9000", true)
	qctx.Owner = azStranger // ignored: the ingress resolves the owner itself
	qctx.Query.Settings = append(qctx.Query.Settings, chproto.Setting{Key: auth.PayerSettingKey, Value: "'" + azOwner + "'", Custom: true})
	qctx.AccessedTables = []sqlmeta.AccessedTable{{IsStorageIntegrity: true, OriginalDatabase: "tenant", OriginalTable: "events"}}
	payload := []byte{byte(chproto.ClientDataCode), 0, 0xab, 0xcd}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if err := p.OnClientDataStrict(context.Background(), qctx, payload); err != nil {
		t.Fatal(err)
	}
	p.OnQueryInputComplete(context.Background(), qctx)
	adm, err := p.ConsumeAdmission(qctx.Session.ID())
	if err != nil {
		t.Fatalf("ConsumeAdmission: %v", err)
	}
	if adm.Signer != signerAddr || adm.Owner != azOwner || adm.Principal != azOwner {
		t.Fatalf("admission signer/owner/principal = %s/%s/%s", adm.Signer, adm.Owner, adm.Principal)
	}
}
