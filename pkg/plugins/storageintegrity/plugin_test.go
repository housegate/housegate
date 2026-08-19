package storageintegrity

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/plugin"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/sqlmeta"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

const storageIntegrityTestKey = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestIngressAcceptsSignedMaterializedInsert(t *testing.T) {
	p, signer := newSignedIngress(t)
	sql := "INSERT INTO tenant.events FORMAT Native"
	qctx := signedQueryContext(t, 10, signer, sql, sql, sqlmeta.StatementTypeInsert)
	qctx.AccessedTables = []sqlmeta.AccessedTable{{
		OriginalDatabase: "tenant",
		OriginalTable:    "events",
		LogicalDatabase:  "tenant",
		PhysicalDatabase: "tenant",
	}}
	payload := []byte{byte(chproto.ClientDataCode), 0, 1, 2, 3}
	withDefaultCaptureToken(t, qctx, signer, payload)

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if err := p.OnClientDataStrict(context.Background(), qctx, payload); err != nil {
		t.Fatalf("OnClientDataStrict: %v", err)
	}
	p.OnQueryInputComplete(context.Background(), qctx)

	admission, err := p.ConsumeAdmission(qctx.Session.ID())
	if err != nil {
		t.Fatalf("ConsumeAdmission: %v", err)
	}
	if admission.Kind != KindInsert || admission.TableID != "tenant.events" {
		t.Fatalf("admission kind/table = %s/%s, want INSERT/tenant.events", admission.Kind, admission.TableID)
	}
	if admission.SQL != sql || admission.Signer != signer.Address() {
		t.Fatalf("admission sql/signer = %q/%q, want %q/%q", admission.SQL, admission.Signer, sql, signer.Address())
	}
	if admission.SQLHash != replay.DigestString(sql) {
		t.Fatalf("admission SQLHash = %q, want replay digest", admission.SQLHash)
	}
	wantJWS := strings.Trim(querySettings(qctx)[auth.StatementTokenSettingKey], "'")
	if admission.UserJWS != wantJWS {
		t.Fatalf("admission UserJWS = %q, want captured statement token", admission.UserJWS)
	}
	wantAuth := strings.Trim(querySettings(qctx)[auth.AuthTokenSettingKey], "'")
	if admission.AuthToken != wantAuth {
		t.Fatalf("admission AuthToken = %q, want captured query token", admission.AuthToken)
	}
	if !bytes.Equal(admission.Payload.Bytes, payload) {
		t.Fatalf("payload bytes = %v, want exact client data bytes %v", admission.Payload.Bytes, payload)
	}
}

func TestIngressRejectsMalformedStatementID(t *testing.T) {
	p, signer := newSignedIngress(t)
	sql := "INSERT INTO tenant.events FORMAT Native"
	qctx := signedQueryContext(t, 12, signer, sql, sql, sqlmeta.StatementTypeInsert)
	qctx.Query.ID = "query-id"
	qctx.AccessedTables = []sqlmeta.AccessedTable{{OriginalDatabase: "tenant", OriginalTable: "events"}}

	err := p.OnQuery(context.Background(), qctx)
	if err == nil || !strings.Contains(err.Error(), "structured statement id") {
		t.Fatalf("OnQuery err = %v, want structured statement id rejection", err)
	}
}

func TestIngressRejectsStatementIDSignerMismatch(t *testing.T) {
	p, signer := newSignedIngress(t)
	sql := "INSERT INTO tenant.events FORMAT Native"
	qctx := signedQueryContext(t, 13, signer, sql, sql, sqlmeta.StatementTypeInsert)
	qctx.Query.ID = "0x0000000000000000000000000000000000000000:1:n1"
	qctx.AccessedTables = []sqlmeta.AccessedTable{{OriginalDatabase: "tenant", OriginalTable: "events"}}

	err := p.OnQuery(context.Background(), qctx)
	if err == nil || !strings.Contains(err.Error(), "does not match authenticated signer") {
		t.Fatalf("OnQuery err = %v, want signer mismatch rejection", err)
	}
}

func TestIngressCapturesExactNativeDataWithoutRowRewrite(t *testing.T) {
	p, signer := newSignedIngress(t)
	sql := "INSERT INTO tenant.events FORMAT Native"
	qctx := signedQueryContext(t, 11, signer, sql, sql, sqlmeta.StatementTypeInsert)
	qctx.AccessedTables = []sqlmeta.AccessedTable{{OriginalDatabase: "tenant", OriginalTable: "events"}}

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	beforeSQL := qctx.Query.Body
	raw := []byte{byte(chproto.ClientDataCode), 0, 0xab, 0xcd}
	if err := p.OnClientDataStrict(context.Background(), qctx, raw); err != nil {
		t.Fatalf("OnClientDataStrict: %v", err)
	}
	if qctx.Query.Body != beforeSQL {
		t.Fatalf("query body changed to %q, want no row rewrite", qctx.Query.Body)
	}
	p.OnQueryInputComplete(context.Background(), qctx)

	admission, err := p.ConsumeAdmission(qctx.Session.ID())
	if err != nil {
		t.Fatalf("ConsumeAdmission: %v", err)
	}
	sum := sha256.Sum256(raw)
	if admission.Payload.Length != uint64(len(raw)) || admission.Payload.SHA256 != "sha256:"+hex.EncodeToString(sum[:]) {
		t.Fatalf("payload metadata = len %d hash %q", admission.Payload.Length, admission.Payload.SHA256)
	}
	if !bytes.Equal(admission.Payload.Bytes, raw) {
		t.Fatalf("payload bytes = %v, want %v", admission.Payload.Bytes, raw)
	}
}

func TestIngressRejectsCompressedInsertBeforeAdmission(t *testing.T) {
	p, signer := newSignedIngress(t)
	sql := "INSERT INTO tenant.events FORMAT Native"
	qctx := signedQueryContext(t, 15, signer, sql, sql, sqlmeta.StatementTypeInsert)
	qctx.Query.Compression = proto.CompressionEnabled
	qctx.AccessedTables = []sqlmeta.AccessedTable{{OriginalDatabase: "tenant", OriginalTable: "events"}}

	err := p.OnQuery(context.Background(), qctx)
	if err == nil || !strings.Contains(err.Error(), "compressed payload") {
		t.Fatalf("OnQuery err = %v, want compressed payload rejection", err)
	}
	if limit, enforce := p.ClientDataReadLimit(qctx); enforce || limit != 0 {
		t.Fatalf("ClientDataReadLimit after rejected compressed INSERT = %d/%v, want 0/false", limit, enforce)
	}
	p.OnQueryInputComplete(context.Background(), qctx)
	if _, err := p.ConsumeAdmission(qctx.Session.ID()); err == nil {
		t.Fatal("compressed INSERT must not publish an admission")
	}
}

func TestIngressCopiesNativeDataAndDoesNotRetainRelaySlice(t *testing.T) {
	p, signer := newSignedIngress(t)
	sql := "INSERT INTO tenant.events FORMAT Native"
	qctx := signedQueryContext(t, 12, signer, sql, sql, sqlmeta.StatementTypeInsert)
	qctx.AccessedTables = []sqlmeta.AccessedTable{{OriginalDatabase: "tenant", OriginalTable: "events"}}
	raw := []byte{byte(chproto.ClientDataCode), 0, 1, 2, 3}
	withDefaultCaptureToken(t, qctx, signer, raw)
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	want := append([]byte(nil), raw...)
	if err := p.OnClientDataStrict(context.Background(), qctx, raw); err != nil {
		t.Fatalf("OnClientDataStrict: %v", err)
	}
	for i := range raw {
		raw[i] = 0xff
	}
	p.OnQueryInputComplete(context.Background(), qctx)

	admission, err := p.ConsumeAdmission(qctx.Session.ID())
	if err != nil {
		t.Fatalf("ConsumeAdmission: %v", err)
	}
	if !bytes.Equal(admission.Payload.Bytes, want) {
		t.Fatalf("payload retained relay slice; got %v want %v", admission.Payload.Bytes, want)
	}
}

func TestIngressRejectsIncompleteNativePayloadCapture(t *testing.T) {
	p, signer := newSignedIngress(t)
	sql := "INSERT INTO tenant.events FORMAT Native"
	qctx := signedQueryContext(t, 13, signer, sql, sql, sqlmeta.StatementTypeInsert)
	qctx.AccessedTables = []sqlmeta.AccessedTable{{OriginalDatabase: "tenant", OriginalTable: "events"}}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	p.OnQueryInputComplete(context.Background(), qctx)

	_, err := p.ConsumeAdmission(qctx.Session.ID())
	if err == nil || !strings.Contains(err.Error(), "incomplete payload capture") {
		t.Fatalf("ConsumeAdmission err = %v, want incomplete capture rejection", err)
	}
}

func TestIngressRejectsMissingStatementID(t *testing.T) {
	p, signer := newSignedIngress(t)
	sql := "INSERT INTO tenant.events FORMAT Native"
	qctx := signedQueryContext(t, 20, signer, sql, sql, sqlmeta.StatementTypeInsert)
	qctx.Query.ID = ""
	qctx.AccessedTables = []sqlmeta.AccessedTable{{OriginalDatabase: "tenant", OriginalTable: "events"}}

	err := p.OnQuery(context.Background(), qctx)
	if err == nil || !strings.Contains(err.Error(), "query id is required") {
		t.Fatalf("OnQuery err = %v, want missing query id rejection", err)
	}
}

func TestIngressRejectsStatementTypeSQLMismatch(t *testing.T) {
	p, signer := newSignedIngress(t)
	sql := "ALTER TABLE tenant.events UPDATE value = 2 WHERE id = 1"
	qctx := signedQueryContext(t, 21, signer, sql, sql, sqlmeta.StatementTypeInsert)
	qctx.AccessedTables = []sqlmeta.AccessedTable{{OriginalDatabase: "tenant", OriginalTable: "events"}}

	err := p.OnQuery(context.Background(), qctx)
	if err == nil || !strings.Contains(err.Error(), "statement type mismatch") {
		t.Fatalf("OnQuery err = %v, want statement type mismatch rejection", err)
	}
}

func TestIngressRejectsNonInsertWrites(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		typ  sqlmeta.StatementType
	}{
		{
			name: "update",
			sql:  "UPDATE tenant.events SET value = 2 WHERE id = 1",
			typ:  sqlmeta.StatementTypeUpdate,
		},
		{
			name: "delete",
			sql:  "DELETE FROM tenant.events WHERE id = 1",
			typ:  sqlmeta.StatementTypeDelete,
		},
		{
			name: "alter update",
			sql:  "ALTER TABLE tenant.events UPDATE value = 2 WHERE id = 1",
			typ:  sqlmeta.StatementTypeAlterTable,
		},
		{
			name: "alter delete",
			sql:  "ALTER TABLE tenant.events DELETE WHERE id = 1",
			typ:  sqlmeta.StatementTypeAlterTable,
		},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, signer := newSignedIngress(t)
			qctx := signedQueryContext(t, int64(30+i), signer, tt.sql, tt.sql, tt.typ)
			qctx.AccessedTables = []sqlmeta.AccessedTable{{OriginalDatabase: "tenant", OriginalTable: "events"}}

			err := p.OnQuery(context.Background(), qctx)
			if err == nil || !strings.Contains(err.Error(), "insert-only") {
				t.Fatalf("OnQuery err = %v, want insert-only rejection", err)
			}
		})
	}
}

func TestIngressIgnoresDescribe(t *testing.T) {
	p, signer := newSignedIngress(t)
	sql := "DESCRIBE TABLE tenant.events"
	qctx := signedQueryContext(t, 61, signer, sql, sql, sqlmeta.StatementTypeDescribe)
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("DESCRIBE must be a read-only pass-through for the ingress: %v", err)
	}
	if qctx.SuppressUpstreamExecution {
		t.Fatal("DESCRIBE must not be intercepted")
	}
}

func TestIngressRejectsTargetTableMismatch(t *testing.T) {
	p, signer := newSignedIngress(t)
	sql := "INSERT INTO tenant.other FORMAT Native"
	qctx := signedQueryContext(t, 22, signer, sql, sql, sqlmeta.StatementTypeInsert)
	qctx.AccessedTables = []sqlmeta.AccessedTable{{OriginalDatabase: "tenant", OriginalTable: "events"}}

	err := p.OnQuery(context.Background(), qctx)
	if err == nil || !strings.Contains(err.Error(), "target table mismatch") {
		t.Fatalf("OnQuery err = %v, want target table mismatch rejection", err)
	}
}

func TestIngressAcceptsQuotedTargetIdentifiers(t *testing.T) {
	ns, schemaHash := ingressNetworkStateForTable(t, "tenant", "event.table", "tenant.`event.table`")
	p, signer := newSignedIngressWithConfig(t, Config{TableSchemas: ns, NetworkID: "testnet-v2"})
	sql := "INSERT INTO `tenant`.`event.table` FORMAT Native"
	qctx := signedQueryContext(t, 60, signer, sql, sql, sqlmeta.StatementTypeInsert)
	qctx.AccessedTables = []sqlmeta.AccessedTable{{OriginalDatabase: "tenant", OriginalTable: "event.table"}}
	payload := []byte{byte(chproto.ClientDataCode), 1}
	statement := v2Statement(signer, qctx.Query.ID, sql, schemaHash, payload, 54453)
	statement.TargetTableID = "tenant.`event.table`"
	withStatementToken(t, qctx, signer, statement)
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if err := p.OnClientDataStrict(context.Background(), qctx, payload); err != nil {
		t.Fatalf("OnClientDataStrict: %v", err)
	}
	p.OnQueryInputComplete(context.Background(), qctx)
	admission, err := p.ConsumeAdmission(qctx.Session.ID())
	if err != nil {
		t.Fatalf("ConsumeAdmission: %v", err)
	}
	if admission.Kind != KindInsert || admission.TableID != "tenant.`event.table`" {
		t.Fatalf("admission = %s/%s, want INSERT/tenant.`event.table`", admission.Kind, admission.TableID)
	}
}

func TestIngressRejectsInlineInsertSources(t *testing.T) {
	tests := []string{
		"INSERT INTO tenant.events VALUES (1)",
		"INSERT INTO tenant.events SELECT * FROM tenant.source",
		"INSERT INTO tenant.events WITH 1 AS id SELECT id",
		"INSERT INTO tenant.events FORMAT CSV",
	}
	for i, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			p, signer := newSignedIngress(t)
			qctx := signedQueryContext(t, int64(62+i), signer, sql, sql, sqlmeta.StatementTypeInsert)
			qctx.AccessedTables = []sqlmeta.AccessedTable{{OriginalDatabase: "tenant", OriginalTable: "events"}}
			err := p.OnQuery(context.Background(), qctx)
			if err == nil || !strings.Contains(err.Error(), "streaming Native INSERT") {
				t.Fatalf("OnQuery err = %v, want payload-local INSERT rejection", err)
			}
		})
	}
}

func TestIngressResolvesUnqualifiedTargetAgainstSessionDatabase(t *testing.T) {
	p, signer := newSignedIngress(t)
	sql := "INSERT INTO events FORMAT Native"
	qctx := signedQueryContext(t, 63, signer, sql, sql, sqlmeta.StatementTypeInsert)
	qctx.Session.State().SetLogicalDatabase("tenant")
	qctx.AccessedTables = []sqlmeta.AccessedTable{{OriginalTable: "events", LogicalDatabase: "tenant"}}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	p.mu.Lock()
	tableID := p.active[qctx.Session.ID()].admission.TableID
	p.mu.Unlock()
	if tableID != "tenant.events" {
		t.Fatalf("target table = %q, want tenant.events", tableID)
	}
}

func TestIngressSharedParserBindsOptionalTableStructuredTargetWithoutRewriteMetadata(t *testing.T) {
	ns, schemaHash := ingressNetworkStateForTable(t, "tenant.prod", "event`log", "`tenant.prod`.`event``log`")
	p, signer := newSignedIngressWithConfig(t, Config{TableSchemas: ns, NetworkID: "testnet-v2"})
	sql := "/*lead*/ InSeRt /*a*/ InTo TABLE `tenant.prod`.`event``log` /*target*/ FORMAT Native"
	qctx := signedQueryContext(t, 630, signer, sql, sql, sqlmeta.StatementTypeInsert)
	statement := v2Statement(signer, qctx.Query.ID, sql, schemaHash, []byte{byte(chproto.ClientDataCode), 0, 0xab, 0xcd}, 54453)
	statement.TargetTableID = "`tenant.prod`.`event``log`"
	withStatementToken(t, qctx, signer, statement)
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	p.mu.Lock()
	tableID := p.active[qctx.Session.ID()].admission.TableID
	p.mu.Unlock()
	if tableID != "`tenant.prod`.`event``log`" {
		t.Fatalf("target table = %q, want structured canonical id", tableID)
	}
}

func TestIngressSharedParserRejectsBackslashEscapedTargetWithoutRewriteMetadata(t *testing.T) {
	p, signer := newSignedIngress(t)
	sql := "INSERT INTO `tenant`.`foo\\nbar` FORMAT Native"
	qctx := signedQueryContext(t, 631, signer, sql, sql, sqlmeta.StatementTypeInsert)
	if err := p.OnQuery(context.Background(), qctx); !errors.Is(err, sicore.ErrBackslashEscapedIdentifier) {
		t.Fatalf("OnQuery err = %v, want ErrBackslashEscapedIdentifier", err)
	}
	p.mu.Lock()
	active := p.active[qctx.Session.ID()]
	p.mu.Unlock()
	if active != nil {
		t.Fatalf("backslash-escaped target created admission %#v", active)
	}
}

func TestIngressRejectsInlineSettingsAndInsertIntoFunction(t *testing.T) {
	for i, tc := range []struct {
		sql, want string
	}{
		{"INSERT INTO tenant.events SeTtInGs /*c*/ AsYnC_InSeRt=1 FORMAT Native", "AsYnC_InSeRt"},
		{"INSERT INTO FUNCTION file('x', Native) FORMAT Native", "FUNCTION"},
	} {
		p, signer := newSignedIngress(t)
		qctx := signedQueryContext(t, int64(631+i), signer, tc.sql, tc.sql, sqlmeta.StatementTypeInsert)
		if err := p.OnQuery(context.Background(), qctx); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%q: err=%v, want %q rejection", tc.sql, err, tc.want)
		}
		p.mu.Lock()
		active := p.active[qctx.Session.ID()]
		p.mu.Unlock()
		if active != nil {
			t.Fatalf("%q created admission %#v", tc.sql, active)
		}
	}
}

func TestIngressRejectsAmbiguousUnqualifiedTargetMetadata(t *testing.T) {
	p, signer := newSignedIngress(t)
	sql := "INSERT INTO events FORMAT Native"
	qctx := signedQueryContext(t, 64, signer, sql, sql, sqlmeta.StatementTypeInsert)
	qctx.AccessedTables = []sqlmeta.AccessedTable{
		{OriginalDatabase: "other", OriginalTable: "events", LogicalDatabase: "other"},
		{OriginalDatabase: "tenant", OriginalTable: "events", LogicalDatabase: "tenant"},
	}
	err := p.OnQuery(context.Background(), qctx)
	if err == nil || !strings.Contains(err.Error(), "ambiguous target table") {
		t.Fatalf("OnQuery err = %v, want ambiguous target rejection", err)
	}
}

func TestIngressRejectsQHashMismatch(t *testing.T) {
	p, signer := newSignedIngress(t)
	finalSQL := "INSERT INTO tenant.events FORMAT Native"
	signedSQL := "INSERT INTO tenant.other FORMAT Native"
	qctx := signedQueryContext(t, 14, signer, signedSQL, finalSQL, sqlmeta.StatementTypeInsert)
	qctx.OriginalSQL = finalSQL
	qctx.AccessedTables = []sqlmeta.AccessedTable{{OriginalDatabase: "tenant", OriginalTable: "events"}}

	err := p.OnQuery(context.Background(), qctx)
	if err == nil || !strings.Contains(err.Error(), "query hash mismatch") {
		t.Fatalf("OnQuery err = %v, want qhash mismatch", err)
	}
}

func TestIngressValidatesSignedSQLBeforeServerRewrite(t *testing.T) {
	p, signer := newSignedIngress(t)
	signedSQL := "INSERT INTO tenant.events FORMAT Native"
	forwardSQL := "INSERT INTO physical_tenant.events FORMAT Native"
	qctx := signedQueryContext(t, 23, signer, signedSQL, forwardSQL, sqlmeta.StatementTypeInsert)
	qctx.AccessedTables = []sqlmeta.AccessedTable{{
		OriginalDatabase: "tenant",
		OriginalTable:    "events",
		LogicalDatabase:  "tenant",
		PhysicalDatabase: "physical_tenant",
	}}
	payload := []byte{byte(chproto.ClientDataCode), 1}
	withDefaultCaptureToken(t, qctx, signer, payload)

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery after server rewrite: %v", err)
	}
	if qctx.Query.Body != forwardSQL {
		t.Fatalf("forward SQL changed to %q, want %q", qctx.Query.Body, forwardSQL)
	}
	if err := p.OnClientDataStrict(context.Background(), qctx, payload); err != nil {
		t.Fatalf("OnClientDataStrict: %v", err)
	}
	p.OnQueryInputComplete(context.Background(), qctx)
	admission, err := p.ConsumeAdmission(qctx.Session.ID())
	if err != nil {
		t.Fatalf("ConsumeAdmission: %v", err)
	}
	if admission.SQL != signedSQL {
		t.Fatalf("admission SQL = %q, want signed SQL %q", admission.SQL, signedSQL)
	}
	if admission.TableID != "tenant.events" {
		t.Fatalf("table ID = %q, want tenant.events", admission.TableID)
	}
}

func TestIngressRejectsInlineSignedSQLBeforeServerRewrite(t *testing.T) {
	p, signer := newSignedIngress(t)
	signedSQL := "INSERT INTO tenant.events VALUES (1)"
	forwardSQL := "INSERT INTO tenant.events FORMAT Native"
	qctx := signedQueryContext(t, 26, signer, signedSQL, forwardSQL, sqlmeta.StatementTypeInsert)
	qctx.AccessedTables = []sqlmeta.AccessedTable{{
		OriginalDatabase: "tenant",
		OriginalTable:    "events",
		LogicalDatabase:  "tenant",
		PhysicalDatabase: "tenant",
	}}

	err := p.OnQuery(context.Background(), qctx)
	if err == nil || !strings.Contains(err.Error(), "streaming Native INSERT") {
		t.Fatalf("OnQuery err = %v, want signed inline INSERT rejection", err)
	}
}

func TestIngressRejectsPurposeMismatch(t *testing.T) {
	p, signer := newSignedIngress(t)
	sql := "INSERT INTO tenant.events FORMAT Native"
	token, err := signer.SignTokenWithPurpose(sql, "ordinary-query")
	if err != nil {
		t.Fatalf("SignTokenWithPurpose: %v", err)
	}
	qctx := &plugin.QueryContext{
		Session:     &fakeSession{id: 19, state: chsession.NewSessionState()},
		OriginalSQL: sql,
		Query: &chproto.Query{
			ID:   statementIDFor(signer, 1),
			Body: sql,
			Settings: []chproto.Setting{{
				Key:    auth.AuthTokenSettingKey,
				Value:  "'" + token + "'",
				Custom: true,
			}},
		},
		StatementType:  sqlmeta.StatementTypeInsert,
		AccessedTables: []sqlmeta.AccessedTable{{OriginalDatabase: "tenant", OriginalTable: "events"}},
	}

	err = p.OnQuery(context.Background(), qctx)
	if err == nil || !strings.Contains(err.Error(), "purpose mismatch") {
		t.Fatalf("OnQuery err = %v, want purpose mismatch", err)
	}
}

func TestIngressAuthValidationUsesRequestTimeout(t *testing.T) {
	p := New(Config{
		Enabled:        true,
		AuthValidator:  blockingPurposeValidator{},
		Purpose:        auth.QueryPurpose,
		RequestTimeout: time.Nanosecond,
	})
	signer, err := auth.NewRelaySigner(storageIntegrityTestKey)
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}
	sql := "INSERT INTO tenant.events FORMAT Native"
	qctx := signedQueryContext(t, 24, signer, sql, sql, sqlmeta.StatementTypeInsert)
	qctx.AccessedTables = []sqlmeta.AccessedTable{{OriginalDatabase: "tenant", OriginalTable: "events"}}

	err = p.OnQuery(context.Background(), qctx)
	if err == nil || !strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
		t.Fatalf("OnQuery err = %v, want validation deadline exceeded", err)
	}
}

type blockingPurposeValidator struct{}

func (blockingPurposeValidator) ValidateQuery(ctx context.Context, _ auth.QueryMeta) (auth.ValidationResult, error) {
	<-ctx.Done()
	return auth.ValidationResult{}, ctx.Err()
}

func (blockingPurposeValidator) ValidateQueryPurpose(ctx context.Context, _ auth.QueryMeta, _ string) (auth.ValidationResult, error) {
	<-ctx.Done()
	return auth.ValidationResult{}, ctx.Err()
}

func TestIngressRejectsValidatorThatDoesNotAuthenticateSigner(t *testing.T) {
	signer, err := auth.NewRelaySigner(storageIntegrityTestKey)
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}
	p := New(Config{
		Enabled:       true,
		AuthValidator: auth.NewEthValidator([]string{signer.Address()}, time.Minute, false, false, "", nil),
		Purpose:       auth.QueryPurpose,
	})
	sql := "INSERT INTO tenant.events FORMAT Native"
	qctx := signedQueryContext(t, 18, signer, sql, sql, sqlmeta.StatementTypeInsert)
	qctx.AccessedTables = []sqlmeta.AccessedTable{{OriginalDatabase: "tenant", OriginalTable: "events"}}

	err = p.OnQuery(context.Background(), qctx)
	if err == nil || !strings.Contains(err.Error(), "authenticated signer is required") {
		t.Fatalf("OnQuery err = %v, want authenticated signer rejection", err)
	}
}

func TestIngressRejectsUnmaterializedNondeterminism(t *testing.T) {
	p, signer := newSignedIngress(t)
	sql := "INSERT INTO tenant.events VALUES (now())"
	qctx := signedQueryContext(t, 15, signer, sql, sql, sqlmeta.StatementTypeInsert)
	qctx.AccessedTables = []sqlmeta.AccessedTable{{OriginalDatabase: "tenant", OriginalTable: "events"}}

	err := p.OnQuery(context.Background(), qctx)
	if err == nil || !strings.Contains(err.Error(), "unmaterialized nondeterministic function") {
		t.Fatalf("OnQuery err = %v, want nondeterminism rejection", err)
	}
}

func TestIngressRejectsMaterializerProfileNondeterminism(t *testing.T) {
	tests := []string{
		"INSERT INTO tenant.events VALUES (today())",
		"INSERT INTO tenant.events VALUES (CURRENT_DATE)",
		"INSERT INTO tenant.events VALUES (curdate())",
		"INSERT INTO tenant.events VALUES (yesterday())",
		"INSERT INTO tenant.events VALUES (UTCTimestamp())",
		"INSERT INTO tenant.events VALUES (CURRENT_TIMESTAMP)",
		"INSERT INTO tenant.events VALUES (localtimestamp)",
		"INSERT INTO tenant.events VALUES (localtime)",
		"INSERT INTO tenant.events VALUES (rand32())",
		"INSERT INTO tenant.events VALUES (randCanonical())",
	}
	for i, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			p, signer := newSignedIngress(t)
			qctx := signedQueryContext(t, int64(40+i), signer, sql, sql, sqlmeta.StatementTypeInsert)
			qctx.AccessedTables = []sqlmeta.AccessedTable{{OriginalDatabase: "tenant", OriginalTable: "events"}}

			err := p.OnQuery(context.Background(), qctx)
			if err == nil || !strings.Contains(err.Error(), "unmaterialized nondeterministic function") {
				t.Fatalf("OnQuery err = %v, want nondeterminism rejection", err)
			}
		})
	}
}

func TestIngressRejectsUnsupportedStorageIntegrityKind(t *testing.T) {
	p, signer := newSignedIngress(t)
	sql := "CREATE TABLE tenant.events (id UInt64)"
	qctx := signedQueryContext(t, 16, signer, sql, sql, sqlmeta.StatementTypeCreateTable)
	qctx.AccessedTables = []sqlmeta.AccessedTable{{OriginalDatabase: "tenant", OriginalTable: "events"}}

	err := p.OnQuery(context.Background(), qctx)
	if err == nil || !strings.Contains(err.Error(), "unsupported storage-integrity statement kind") {
		t.Fatalf("OnQuery err = %v, want unsupported kind rejection", err)
	}
}

func TestIngressRejectsWriteWhilePriorAdmissionPending(t *testing.T) {
	p, signer := newSignedIngress(t)
	sql := "INSERT INTO tenant.events FORMAT Native"
	qctx := signedQueryContext(t, 50, signer, sql, sql, sqlmeta.StatementTypeInsert)
	qctx.AccessedTables = []sqlmeta.AccessedTable{{OriginalDatabase: "tenant", OriginalTable: "events"}}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("first OnQuery: %v", err)
	}
	if err := p.OnClientDataStrict(context.Background(), qctx, []byte{byte(chproto.ClientDataCode), 1}); err != nil {
		t.Fatalf("first OnClientDataStrict: %v", err)
	}
	p.OnQueryInputComplete(context.Background(), qctx)

	nextSQL := "INSERT INTO tenant.events FORMAT Native"
	next := signedQueryContext(t, 50, signer, nextSQL, nextSQL, sqlmeta.StatementTypeInsert)
	next.AccessedTables = []sqlmeta.AccessedTable{{OriginalDatabase: "tenant", OriginalTable: "events"}}
	err := p.OnQuery(context.Background(), next)
	if err == nil || !strings.Contains(err.Error(), "pending admission") {
		t.Fatalf("second OnQuery err = %v, want pending admission rejection", err)
	}
}

func TestIngressOnCloseDropsPendingAdmission(t *testing.T) {
	p, signer := newSignedIngress(t)
	sql := "INSERT INTO tenant.events FORMAT Native"
	qctx := signedQueryContext(t, 51, signer, sql, sql, sqlmeta.StatementTypeInsert)
	qctx.AccessedTables = []sqlmeta.AccessedTable{{OriginalDatabase: "tenant", OriginalTable: "events"}}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if err := p.OnClientDataStrict(context.Background(), qctx, []byte{byte(chproto.ClientDataCode), 1}); err != nil {
		t.Fatalf("OnClientDataStrict: %v", err)
	}
	p.OnQueryInputComplete(context.Background(), qctx)
	p.OnClose(qctx.Session)

	_, err := p.ConsumeAdmission(qctx.Session.ID())
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("ConsumeAdmission err = %v, want pending admission removed on close", err)
	}
}

func TestIngressAbortsCaptureOnStrictDataError(t *testing.T) {
	p, signer := newSignedIngress(t)
	sql := "INSERT INTO tenant.events FORMAT Native"
	qctx := signedQueryContext(t, 52, signer, sql, sql, sqlmeta.StatementTypeInsert)
	qctx.AccessedTables = []sqlmeta.AccessedTable{{OriginalDatabase: "tenant", OriginalTable: "events"}}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if err := p.OnClientDataStrict(context.Background(), qctx, []byte{byte(chproto.ClientDataCode), 1}); err != nil {
		t.Fatalf("first OnClientDataStrict: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.OnClientDataStrict(ctx, qctx, []byte{byte(chproto.ClientDataCode), 2}); err == nil {
		t.Fatalf("second OnClientDataStrict err = nil, want context cancellation")
	}
	p.OnQueryComplete(context.Background(), qctx.Session)

	_, err := p.ConsumeAdmission(qctx.Session.ID())
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("ConsumeAdmission err = %v, want aborted capture to be discarded", err)
	}
}

func TestIngressQueryAbortDropsActiveCapture(t *testing.T) {
	p, signer := newSignedIngress(t)
	sql := "INSERT INTO tenant.events FORMAT Native"
	qctx := signedQueryContext(t, 53, signer, sql, sql, sqlmeta.StatementTypeInsert)
	qctx.AccessedTables = []sqlmeta.AccessedTable{{OriginalDatabase: "tenant", OriginalTable: "events"}}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if err := p.OnClientDataStrict(context.Background(), qctx, []byte{byte(chproto.ClientDataCode), 1}); err != nil {
		t.Fatalf("OnClientDataStrict: %v", err)
	}
	p.OnQueryAbort(context.Background(), qctx)
	p.OnQueryComplete(context.Background(), qctx.Session)

	_, err := p.ConsumeAdmission(qctx.Session.ID())
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("ConsumeAdmission err = %v, want aborted capture to be discarded", err)
	}
}

func TestIngressAbortOnlyDropsMatchingStatement(t *testing.T) {
	p, signer := newSignedIngress(t)
	sql := "INSERT INTO tenant.events FORMAT Native"
	active := signedQueryContext(t, 55, signer, sql, sql, sqlmeta.StatementTypeInsert)
	active.Query.ID = statementIDFor(signer, 1)
	active.AccessedTables = []sqlmeta.AccessedTable{{OriginalDatabase: "tenant", OriginalTable: "events"}}
	if err := p.OnQuery(context.Background(), active); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}

	other := signedQueryContext(t, 55, signer, sql, sql, sqlmeta.StatementTypeInsert)
	other.Query.ID = statementIDFor(signer, 2)
	p.OnQueryAbort(context.Background(), other)
	p.mu.Lock()
	got := p.active[active.Session.ID()]
	p.mu.Unlock()
	if got == nil || got.admission.StatementID != active.Query.ID {
		t.Fatalf("unrelated abort removed active admission: %#v", got)
	}
}

func TestIngressFinalizesOnClientInputComplete(t *testing.T) {
	p, signer := newSignedIngress(t)
	sql := "INSERT INTO tenant.events FORMAT Native"
	qctx := signedQueryContext(t, 56, signer, sql, sql, sqlmeta.StatementTypeInsert)
	qctx.AccessedTables = []sqlmeta.AccessedTable{{OriginalDatabase: "tenant", OriginalTable: "events"}}
	payload := []byte{byte(chproto.ClientDataCode), 1}
	withDefaultCaptureToken(t, qctx, signer, payload)
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if err := p.OnClientDataStrict(context.Background(), qctx, payload); err != nil {
		t.Fatalf("OnClientDataStrict: %v", err)
	}

	p.OnQueryComplete(context.Background(), qctx.Session)
	p.mu.Lock()
	pendingBeforeInputComplete := p.pending[qctx.Session.ID()]
	p.mu.Unlock()
	if pendingBeforeInputComplete != nil {
		t.Fatal("upstream completion heuristic published admission before client input completed")
	}

	p.OnQueryInputComplete(context.Background(), qctx)
	admission, err := p.ConsumeAdmission(qctx.Session.ID())
	if err != nil {
		t.Fatalf("ConsumeAdmission: %v", err)
	}
	if admission.Payload.Revision != qctx.Session.State().ClientRevision {
		t.Fatalf("revision = %d, want %d", admission.Payload.Revision, qctx.Session.State().ClientRevision)
	}
}

func TestIngressConsumerReceivesCompletedAdmissionAndClearsPending(t *testing.T) {
	consumer := &recordingConsumer{}
	p, signer := newSignedIngressWithConfig(t, Config{AdmissionConsumer: consumer})
	sql := "INSERT INTO tenant.events FORMAT Native"
	qctx := signedQueryContext(t, 58, signer, sql, sql, sqlmeta.StatementTypeInsert)
	qctx.AccessedTables = []sqlmeta.AccessedTable{{OriginalDatabase: "tenant", OriginalTable: "events"}}
	payload := []byte{byte(chproto.ClientDataCode), 1}
	withDefaultCaptureToken(t, qctx, signer, payload)
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if err := p.OnClientDataStrict(context.Background(), qctx, payload); err != nil {
		t.Fatalf("OnClientDataStrict: %v", err)
	}
	// The consumer runs at the strict end-of-input boundary. With a configured
	// consumer, Relay withholds non-empty payload Data from ordinary upstream;
	// success returns nil so Relay can forward the zero-row terminator.
	if err := p.OnQueryInputCompleteStrict(context.Background(), qctx); err != nil {
		t.Fatalf("OnQueryInputCompleteStrict: %v", err)
	}
	p.OnQueryInputComplete(context.Background(), qctx)

	admission := consumer.requireOne(t)
	wantStatementID := statementIDFor(signer, 1)
	if admission.StatementID != wantStatementID || admission.TableID != "tenant.events" {
		t.Fatalf("admission = %s/%s, want %s/tenant.events", admission.StatementID, admission.TableID, wantStatementID)
	}
	if _, err := p.ConsumeAdmission(qctx.Session.ID()); err == nil {
		t.Fatal("consumer-success admission remained pending")
	}

	next := signedQueryContext(t, 58, signer, sql, sql, sqlmeta.StatementTypeInsert)
	next.Query.ID = statementIDFor(signer, 2)
	next.AccessedTables = []sqlmeta.AccessedTable{{OriginalDatabase: "tenant", OriginalTable: "events"}}
	if err := p.OnQuery(context.Background(), next); err != nil {
		t.Fatalf("second OnQuery after consumer success: %v", err)
	}
}

func TestIngressWithConsumerSuppressesOrdinaryUpstreamPayloadRows(t *testing.T) {
	consumer := &recordingConsumer{}
	p, signer := newSignedIngressWithConfig(t, Config{AdmissionConsumer: consumer})
	sql := "INSERT INTO tenant.events FORMAT Native"
	qctx := signedQueryContext(t, 61, signer, sql, sql, sqlmeta.StatementTypeInsert)
	qctx.AccessedTables = []sqlmeta.AccessedTable{{OriginalDatabase: "tenant", OriginalTable: "events"}}

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if !qctx.SuppressUpstreamExecution {
		t.Fatal("storage-integrity ingress with a consumer must suppress ordinary upstream payload rows")
	}
}

// TestIngressConsumerFailureIsFailClosed pins the fail-closed boundary: a
// consumer rejection/unavailability surfaces as an error from the strict
// end-of-input hook. In the configured-consumer path, Relay has suppressed
// non-empty payload Data, so no ClickHouse INSERT rows can commit behind a
// failed admission.
func TestIngressConsumerFailureIsFailClosed(t *testing.T) {
	consumer := &recordingConsumer{err: errors.New("consumer unavailable")}
	p, signer := newSignedIngressWithConfig(t, Config{AdmissionConsumer: consumer})
	sql := "INSERT INTO tenant.events FORMAT Native"
	qctx := signedQueryContext(t, 59, signer, sql, sql, sqlmeta.StatementTypeInsert)
	qctx.AccessedTables = []sqlmeta.AccessedTable{{OriginalDatabase: "tenant", OriginalTable: "events"}}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if err := p.OnClientDataStrict(context.Background(), qctx, []byte{byte(chproto.ClientDataCode), 1}); err != nil {
		t.Fatalf("OnClientDataStrict: %v", err)
	}
	// The strict hook must return the consumer error so Relay rejects the query
	// instead of synthesizing success.
	err := p.OnQueryInputCompleteStrict(context.Background(), qctx)
	if err == nil || !strings.Contains(err.Error(), "rejected") {
		t.Fatalf("OnQueryInputCompleteStrict err = %v, want fail-closed consumer rejection", err)
	}
}

type recordingConsumer struct {
	mu         sync.Mutex
	err        error
	admissions []Admission
}

func (c *recordingConsumer) ConsumeStorageIntegrityAdmission(_ context.Context, admission Admission) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return c.err
	}
	c.admissions = append(c.admissions, admission)
	return nil
}

func (c *recordingConsumer) requireOne(t *testing.T) Admission {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.admissions) != 1 {
		t.Fatalf("consumed admissions = %d, want 1", len(c.admissions))
	}
	return c.admissions[0]
}

func TestIngressRejectsOversizedNativePayload(t *testing.T) {
	p, signer := newSignedIngressWithConfig(t, Config{MaxPayloadBytes: 3})
	sql := "INSERT INTO tenant.events FORMAT Native"
	qctx := signedQueryContext(t, 54, signer, sql, sql, sqlmeta.StatementTypeInsert)
	qctx.AccessedTables = []sqlmeta.AccessedTable{{OriginalDatabase: "tenant", OriginalTable: "events"}}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	err := p.OnClientDataStrict(context.Background(), qctx, []byte{byte(chproto.ClientDataCode), 1, 2, 3})
	if err == nil || !strings.Contains(err.Error(), "payload exceeds") {
		t.Fatalf("OnClientDataStrict err = %v, want payload size rejection", err)
	}
	p.OnQueryComplete(context.Background(), qctx.Session)
	if _, err := p.ConsumeAdmission(qctx.Session.ID()); err == nil {
		t.Fatalf("oversized rejected payload must not publish an admission")
	}
}

func TestIngressClientDataReadLimitTracksRemainingPayload(t *testing.T) {
	p, signer := newSignedIngressWithConfig(t, Config{MaxPayloadBytes: 5})
	sql := "INSERT INTO tenant.events FORMAT Native"
	qctx := signedQueryContext(t, 57, signer, sql, sql, sqlmeta.StatementTypeInsert)
	qctx.AccessedTables = []sqlmeta.AccessedTable{{OriginalDatabase: "tenant", OriginalTable: "events"}}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	limit, enforce := p.ClientDataReadLimit(qctx)
	if !enforce || limit != 5 {
		t.Fatalf("initial read limit = %d/%v, want 5/true", limit, enforce)
	}
	if err := p.OnClientDataStrict(context.Background(), qctx, []byte{2, 1}); err != nil {
		t.Fatalf("OnClientDataStrict: %v", err)
	}
	limit, enforce = p.ClientDataReadLimit(qctx)
	if !enforce || limit != 3 {
		t.Fatalf("remaining read limit = %d/%v, want 3/true", limit, enforce)
	}
}

func TestStorageIntegrityDisabledIsNoOp(t *testing.T) {
	p := New(Config{Enabled: false})
	if p.RejectUndecodableQuery() {
		t.Fatal("disabled plugin enabled strict query decoding")
	}
	sql := "INSERT INTO tenant.events VALUES (now())"
	qctx := &plugin.QueryContext{
		Session:     &fakeSession{id: 17, state: chsession.NewSessionState()},
		OriginalSQL: sql,
		Query:       &chproto.Query{Body: sql},
	}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("disabled OnQuery = %v, want no-op", err)
	}
	if err := p.OnClientDataStrict(context.Background(), qctx, []byte{1, 2, 3}); err != nil {
		t.Fatalf("disabled OnClientDataStrict = %v, want no-op", err)
	}
	if _, err := p.ConsumeAdmission(qctx.Session.ID()); err == nil {
		t.Fatal("disabled plugin produced an admission")
	}
}

func TestStorageIntegrityEnabledRequiresStrictQueryDecode(t *testing.T) {
	p, _ := newSignedIngress(t)
	if !p.RejectUndecodableQuery() {
		t.Fatal("enabled plugin did not require strict query decoding")
	}
}

func ingressSchema() payloadexec.TableSchema {
	return payloadexec.TableSchema{TableID: "tenant.events", Columns: []lthash.Column{{Name: "id", Type: "UInt64"}, {Name: "region", Type: "String"}}}
}

func ingressNetworkState(t *testing.T) (*network.InMemoryNetworkState, string) {
	return ingressNetworkStateForTable(t, "tenant", "events", "tenant.events")
}

func ingressNetworkStateForTable(t *testing.T, database, table, tableID string) (*network.InMemoryNetworkState, string) {
	t.Helper()
	ns := network.NewInMemoryNetworkState()
	schema := ingressSchema()
	schema.TableID = tableID
	js, err := json.Marshal(schema)
	if err != nil {
		t.Fatalf("marshal ingress schema: %v", err)
	}
	hash := payloadexec.TableSchemaHash("testnet-v2", schema)
	ns.TableSchemas[database+"/"+table+"@1"] = network.TableSchemaInfo{DatabaseId: database, TableId: table, Version: 1, SchemaHash: hash, SchemaJson: string(js)}
	return ns, hash
}

func newV2Ingress(t *testing.T) (*Plugin, *auth.RelaySigner, string) {
	t.Helper()
	ns, hash := ingressNetworkState(t)
	p, signer := newSignedIngressWithConfig(t, Config{TableSchemas: ns, NetworkID: "testnet-v2"})
	return p, signer, hash
}

// v2Statement builds the expected token payload the way the agent does and
// signs it; the ingress must recompute the same expectation from its capture.
func v2Statement(_ *auth.RelaySigner, statementID, sql, schemaHash string, payload []byte, revision uint32) auth.JWSStatementPayloadV2 {
	return auth.JWSStatementPayloadV2{
		NetworkID: "testnet-v2", KeeperShardID: 0, StatementID: statementID,
		SQLHash: replay.DigestString(sql), SettingsHash: sicore.EmptySettingsHash, SchemaHash: schemaHash,
		PayloadHash: replay.DigestBytes(payload), PayloadLength: uint64(len(payload)),
		PayloadFormat: sicore.PayloadEncodingClickHouseNativeData, ClientRevision: revision,
		TargetTableID: "tenant.events", RowIDProfileID: payloadexec.RowIDProfileID,
	}
}

func withStatementToken(t *testing.T, qctx *plugin.QueryContext, signer *auth.RelaySigner, payload auth.JWSStatementPayloadV2) {
	t.Helper()
	token, err := signer.SignStatementV2(payload)
	if err != nil {
		t.Fatal(err)
	}
	qctx.Query.Settings = append(qctx.Query.Settings, chproto.Setting{Key: auth.StatementTokenSettingKey, Value: "'" + token + "'", Custom: true})
}

func withDefaultCaptureToken(t *testing.T, qctx *plugin.QueryContext, signer *auth.RelaySigner, payload []byte) {
	t.Helper()
	schemaHash := payloadexec.TableSchemaHash("testnet-v2", ingressSchema())
	withStatementToken(t, qctx, signer, v2Statement(signer, qctx.Query.ID, qctx.OriginalSQL, schemaHash, payload, uint32(qctx.Session.State().ClientRevision)))
}

func TestIngressV2_AcceptsTokenBoundToItsOwnCapture(t *testing.T) {
	p, signer, schemaHash := newV2Ingress(t)
	sql := "INSERT INTO tenant.events FORMAT Native"
	qctx := signedQueryContext(t, 21, signer, sql, sql, sqlmeta.StatementTypeInsert)
	qctx.AccessedTables = []sqlmeta.AccessedTable{{OriginalDatabase: "tenant", OriginalTable: "events"}}
	payload := []byte{byte(chproto.ClientDataCode), 0, 0xab, 0xcd}
	withStatementToken(t, qctx, signer, v2Statement(signer, qctx.Query.ID, sql, schemaHash, payload, 54453))

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if err := p.OnClientDataStrict(context.Background(), qctx, payload); err != nil {
		t.Fatalf("OnClientDataStrict: %v", err)
	}
	p.OnQueryInputComplete(context.Background(), qctx)
	adm, err := p.ConsumeAdmission(qctx.Session.ID())
	if err != nil {
		t.Fatalf("ConsumeAdmission: %v", err)
	}
	if adm.EnvelopeVersion != sicore.EnvelopeVersionV2 || adm.NetworkID != "testnet-v2" || adm.SchemaHash != schemaHash || adm.SettingsHash != sicore.EmptySettingsHash || adm.RowIDProfileID != payloadexec.RowIDProfileID || adm.Payload.Encoding != sicore.PayloadEncodingClickHouseNativeData || adm.Payload.Revision != 54453 {
		t.Fatalf("admission v2 fields: %+v", adm)
	}
	if adm.UserJWS == "" || strings.Count(adm.UserJWS, ".") != 2 || adm.AuthToken == "" {
		t.Fatalf("UserJWS must be the statement token and AuthToken the query token: %+v", adm)
	}
	if adm.UserJWS == adm.AuthToken {
		t.Fatal("statement token and auth token must be different tokens")
	}
}

func TestIngressV2_RejectionMatrix(t *testing.T) {
	sql := "INSERT INTO tenant.events FORMAT Native"
	payload := []byte{byte(chproto.ClientDataCode), 0, 0xab, 0xcd}
	cases := []struct {
		name    string
		prepare func(t *testing.T, p *Plugin, signer *auth.RelaySigner, schemaHash string, qctx *plugin.QueryContext) []byte
		atQuery bool
		want    string
	}{
		{"missing statement token", func(_ *testing.T, _ *Plugin, _ *auth.RelaySigner, _ string, q *plugin.QueryContext) []byte {
			q.Query.Settings = q.Query.Settings[:1]
			return payload
		}, true, auth.StatementTokenSettingKey},
		{"user setting present", func(t *testing.T, _ *Plugin, signer *auth.RelaySigner, h string, q *plugin.QueryContext) []byte {
			withStatementToken(t, q, signer, v2Statement(signer, q.Query.ID, sql, h, payload, 54453))
			q.Query.Settings = append(q.Query.Settings, chproto.Setting{Key: "async_insert", Value: "1"})
			return payload
		}, true, "async_insert"},
		{"payload swapped after signing", func(t *testing.T, _ *Plugin, signer *auth.RelaySigner, h string, q *plugin.QueryContext) []byte {
			withStatementToken(t, q, signer, v2Statement(signer, q.Query.ID, sql, h, payload, 54453))
			return []byte{byte(chproto.ClientDataCode), 0, 0xff, 0xff}
		}, false, "payload_hash"},
		{"keeper shard differs", func(t *testing.T, _ *Plugin, signer *auth.RelaySigner, h string, q *plugin.QueryContext) []byte {
			statement := v2Statement(signer, q.Query.ID, sql, h, payload, 54453)
			statement.KeeperShardID = 1
			withStatementToken(t, q, signer, statement)
			return payload
		}, false, "keeper_shard_id"},
		{"statement id differs", func(t *testing.T, _ *Plugin, signer *auth.RelaySigner, h string, q *plugin.QueryContext) []byte {
			statement := v2Statement(signer, q.Query.ID, sql, h, payload, 54453)
			statement.StatementID = statementIDFor(signer, 2)
			withStatementToken(t, q, signer, statement)
			return payload
		}, false, "statement_id"},
		{"sql hash differs", func(t *testing.T, _ *Plugin, signer *auth.RelaySigner, h string, q *plugin.QueryContext) []byte {
			statement := v2Statement(signer, q.Query.ID, sql, h, payload, 54453)
			statement.SQLHash = replay.DigestString(sql + " ")
			withStatementToken(t, q, signer, statement)
			return payload
		}, false, "sql_hash"},
		{"settings hash differs", func(t *testing.T, _ *Plugin, signer *auth.RelaySigner, h string, q *plugin.QueryContext) []byte {
			statement := v2Statement(signer, q.Query.ID, sql, h, payload, 54453)
			statement.SettingsHash = replay.DigestString("nonempty")
			withStatementToken(t, q, signer, statement)
			return payload
		}, false, "settings_hash"},
		{"payload length differs", func(t *testing.T, _ *Plugin, signer *auth.RelaySigner, h string, q *plugin.QueryContext) []byte {
			statement := v2Statement(signer, q.Query.ID, sql, h, payload, 54453)
			statement.PayloadLength++
			withStatementToken(t, q, signer, statement)
			return payload
		}, false, "payload_length"},
		{"payload format differs", func(t *testing.T, _ *Plugin, signer *auth.RelaySigner, h string, q *plugin.QueryContext) []byte {
			statement := v2Statement(signer, q.Query.ID, sql, h, payload, 54453)
			statement.PayloadFormat = sicore.EncodingCSVWithNames
			withStatementToken(t, q, signer, statement)
			return payload
		}, false, "payload_format"},
		{"target table differs", func(t *testing.T, _ *Plugin, signer *auth.RelaySigner, h string, q *plugin.QueryContext) []byte {
			statement := v2Statement(signer, q.Query.ID, sql, h, payload, 54453)
			statement.TargetTableID = "tenant.other"
			withStatementToken(t, q, signer, statement)
			return payload
		}, false, "target_table_id"},
		{"row id profile differs", func(t *testing.T, _ *Plugin, signer *auth.RelaySigner, h string, q *plugin.QueryContext) []byte {
			statement := v2Statement(signer, q.Query.ID, sql, h, payload, 54453)
			statement.RowIDProfileID = "housegate-row-id-v0"
			withStatementToken(t, q, signer, statement)
			return payload
		}, false, "row_id_profile_id"},
		{"schema hash differs from network state", func(t *testing.T, _ *Plugin, signer *auth.RelaySigner, _ string, q *plugin.QueryContext) []byte {
			withStatementToken(t, q, signer, v2Statement(signer, q.Query.ID, sql, "0x"+strings.Repeat("ee", 32), payload, 54453))
			return payload
		}, false, "schema_hash"},
		{"client revision differs", func(t *testing.T, _ *Plugin, signer *auth.RelaySigner, h string, q *plugin.QueryContext) []byte {
			withStatementToken(t, q, signer, v2Statement(signer, q.Query.ID, sql, h, payload, 54470))
			return payload
		}, false, "client_revision"},
		{"client revision missing", func(t *testing.T, _ *Plugin, signer *auth.RelaySigner, h string, q *plugin.QueryContext) []byte {
			q.Session.State().ClientRevision = 0
			withStatementToken(t, q, signer, v2Statement(signer, q.Query.ID, sql, h, payload, 0))
			return payload
		}, false, "client protocol revision"},
		{"network id differs", func(t *testing.T, _ *Plugin, signer *auth.RelaySigner, h string, q *plugin.QueryContext) []byte {
			statement := v2Statement(signer, q.Query.ID, sql, h, payload, 54453)
			statement.NetworkID = "other"
			withStatementToken(t, q, signer, statement)
			return payload
		}, false, "network_id"},
		{"statement token signed by other key", func(t *testing.T, _ *Plugin, _ *auth.RelaySigner, h string, q *plugin.QueryContext) []byte {
			other, err := auth.NewRelaySigner("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
			if err != nil {
				t.Fatal(err)
			}
			withStatementToken(t, q, other, v2Statement(other, q.Query.ID, sql, h, payload, 54453))
			return payload
		}, false, "allowlist"},
		{"legacy query token in statement slot", func(t *testing.T, _ *Plugin, signer *auth.RelaySigner, _ string, q *plugin.QueryContext) []byte {
			legacy, err := signer.SignToken(sql)
			if err != nil {
				t.Fatal(err)
			}
			q.Query.Settings = append(q.Query.Settings, chproto.Setting{Key: auth.StatementTokenSettingKey, Value: "'" + legacy + "'", Custom: true})
			return payload
		}, false, "purpose"},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, signer, schemaHash := newV2Ingress(t)
			qctx := signedQueryContext(t, int64(30+i), signer, sql, sql, sqlmeta.StatementTypeInsert)
			qctx.AccessedTables = []sqlmeta.AccessedTable{{OriginalDatabase: "tenant", OriginalTable: "events"}}
			captured := tc.prepare(t, p, signer, schemaHash, qctx)
			err := p.OnQuery(context.Background(), qctx)
			if tc.atQuery {
				if err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("OnQuery err = %v, want containing %q", err, tc.want)
				}
				return
			}
			if err != nil {
				t.Fatalf("OnQuery: %v", err)
			}
			if err := p.OnClientDataStrict(context.Background(), qctx, captured); err != nil {
				t.Fatalf("OnClientDataStrict: %v", err)
			}
			p.OnQueryInputComplete(context.Background(), qctx)
			_, err = p.ConsumeAdmission(qctx.Session.ID())
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ConsumeAdmission err = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestIngressV2_UndeclaredTableFailsClosedAtQuery(t *testing.T) {
	p, signer := newSignedIngressWithConfig(t, Config{TableSchemas: network.NewInMemoryNetworkState(), NetworkID: "testnet-v2"})
	sql := "INSERT INTO tenant.events FORMAT Native"
	qctx := signedQueryContext(t, 40, signer, sql, sql, sqlmeta.StatementTypeInsert)
	qctx.AccessedTables = []sqlmeta.AccessedTable{{OriginalDatabase: "tenant", OriginalTable: "events"}}
	withStatementToken(t, qctx, signer, v2Statement(signer, qctx.Query.ID, sql, "0x00", []byte{2, 0}, 54453))
	if err := p.OnQuery(context.Background(), qctx); err == nil || !strings.Contains(err.Error(), "schema") {
		t.Fatalf("undeclared table must fail closed at OnQuery: %v", err)
	}
}

func TestIngressV2_RequiresTableSchemasAndNetworkID(t *testing.T) {
	ns, _ := ingressNetworkState(t)
	cases := []struct {
		name string
		cfg  Config
		want string
	}{
		{name: "missing table schemas", cfg: Config{NetworkID: "testnet-v2"}, want: "TableSchemas"},
		{name: "missing network id", cfg: Config{TableSchemas: ns}, want: "network_id"},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, signer := newSignedIngressWithoutV2Config(t, tc.cfg)
			sql := "INSERT INTO tenant.events FORMAT Native"
			qctx := signedQueryContext(t, int64(41+i), signer, sql, sql, sqlmeta.StatementTypeInsert)
			qctx.AccessedTables = []sqlmeta.AccessedTable{{OriginalDatabase: "tenant", OriginalTable: "events"}}
			if err := p.OnQuery(context.Background(), qctx); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("OnQuery err = %v, want missing %s rejection", err, tc.want)
			}
		})
	}
}

type legacyOnlyPurposeValidator struct {
	validator *auth.EthValidator
}

func (v legacyOnlyPurposeValidator) ValidateQuery(ctx context.Context, meta auth.QueryMeta) (auth.ValidationResult, error) {
	return v.validator.ValidateQuery(ctx, meta)
}

func (v legacyOnlyPurposeValidator) ValidateQueryPurpose(ctx context.Context, meta auth.QueryMeta, purpose string) (auth.ValidationResult, error) {
	return v.validator.ValidateQueryPurpose(ctx, meta, purpose)
}

func TestIngressV2_RequiresStatementValidatorV2(t *testing.T) {
	ns, _ := ingressNetworkState(t)
	signer, err := auth.NewRelaySigner(storageIntegrityTestKey)
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}
	p := New(Config{
		Enabled:       true,
		AuthValidator: legacyOnlyPurposeValidator{validator: auth.NewEthValidator([]string{signer.Address()}, time.Minute, true, false, "", nil)},
		Purpose:       auth.QueryPurpose,
		TableSchemas:  ns,
		NetworkID:     "testnet-v2",
	})
	sql := "INSERT INTO tenant.events FORMAT Native"
	qctx := signedQueryContext(t, 43, signer, sql, sql, sqlmeta.StatementTypeInsert)
	qctx.AccessedTables = []sqlmeta.AccessedTable{{OriginalDatabase: "tenant", OriginalTable: "events"}}
	payload := []byte{byte(chproto.ClientDataCode), 0, 0xab, 0xcd}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if err := p.OnClientDataStrict(context.Background(), qctx, payload); err != nil {
		t.Fatalf("OnClientDataStrict: %v", err)
	}
	p.OnQueryInputComplete(context.Background(), qctx)
	if _, err := p.ConsumeAdmission(qctx.Session.ID()); err == nil || !strings.Contains(err.Error(), "does not support envelope v2") {
		t.Fatalf("ConsumeAdmission err = %v, want StatementValidatorV2 rejection", err)
	}
}

func newSignedIngress(t *testing.T) (*Plugin, *auth.RelaySigner) {
	return newSignedIngressWithConfig(t, Config{})
}

func newSignedIngressWithConfig(t *testing.T, cfg Config) (*Plugin, *auth.RelaySigner) {
	if cfg.TableSchemas == nil && strings.TrimSpace(cfg.NetworkID) == "" {
		ns, _ := ingressNetworkState(t)
		cfg.TableSchemas = ns
		cfg.NetworkID = "testnet-v2"
	}
	return newSignedIngressWithoutV2Config(t, cfg)
}

func newSignedIngressWithoutV2Config(t *testing.T, cfg Config) (*Plugin, *auth.RelaySigner) {
	t.Helper()
	signer, err := auth.NewRelaySigner(storageIntegrityTestKey)
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}
	validator := auth.NewEthValidator([]string{signer.Address()}, time.Minute, true, false, "", nil)
	cfg.Enabled = true
	cfg.AuthValidator = validator
	cfg.Purpose = auth.QueryPurpose
	return New(cfg), signer
}

func signedQueryContext(t *testing.T, sessionID int64, signer *auth.RelaySigner, signedSQL, finalSQL string, typ sqlmeta.StatementType) *plugin.QueryContext {
	t.Helper()
	token, err := signer.SignToken(signedSQL)
	if err != nil {
		t.Fatalf("SignTokenWithPurpose: %v", err)
	}
	state := chsession.NewSessionState()
	state.ClientRevision = 54453
	sess := &fakeSession{id: sessionID, state: state}
	qctx := &plugin.QueryContext{
		Session:     sess,
		OriginalSQL: signedSQL,
		Query: &chproto.Query{
			ID:   statementIDFor(signer, 1),
			Body: finalSQL,
			Settings: []chproto.Setting{{
				Key:    auth.AuthTokenSettingKey,
				Value:  "'" + token + "'",
				Custom: true,
			}},
		},
		StatementType: typ,
	}
	defaultPayload := []byte{byte(chproto.ClientDataCode), 0, 0xab, 0xcd}
	defaultSchemaHash := payloadexec.TableSchemaHash("testnet-v2", ingressSchema())
	withStatementToken(t, qctx, signer, v2Statement(signer, qctx.Query.ID, signedSQL, defaultSchemaHash, defaultPayload, uint32(state.ClientRevision)))
	return qctx
}

func statementIDFor(signer *auth.RelaySigner, seq uint64) string {
	return strings.ToLower(signer.Address()) + ":" + strconv.FormatUint(seq, 10) + ":n" + strconv.FormatUint(seq, 10)
}

type fakeSession struct {
	id    int64
	state *chsession.SessionState
}

func (s *fakeSession) ID() int64                                          { return s.id }
func (s *fakeSession) State() *chsession.SessionState                     { return s.state }
func (s *fakeSession) Client() *chproto.Codec                             { return nil }
func (s *fakeSession) Upstream() *chproto.Codec                           { return nil }
func (s *fakeSession) RemoteAddr() net.Addr                               { return nil }
func (s *fakeSession) Close() error                                       { return nil }
func (s *fakeSession) BindUpstream(context.Context, *chproto.Codec) error { return nil }
func (s *fakeSession) RebindUpstream(context.Context, *chproto.Codec, bool) error {
	return nil
}
func (s *fakeSession) RebindToPeer(context.Context, *chproto.Codec, *chproto.ClientHello) error {
	return nil
}
func (s *fakeSession) RebindToLocal(context.Context, *chproto.Codec, *chproto.ClientHello) error {
	return nil
}
