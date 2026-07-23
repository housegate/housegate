package storageintegrity

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"housegate/housegate/pkg/auth"
	"housegate/housegate/pkg/chproto"
	"housegate/housegate/pkg/chsession"
	"housegate/housegate/pkg/plugin"
	"housegate/housegate/pkg/replay"
	"housegate/housegate/pkg/sqlmeta"
	sicore "housegate/housegate/pkg/storageintegrity"
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

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	payload := []byte{byte(chproto.ClientDataCode), 0, 1, 2, 3}
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
	wantJWS := strings.Trim(qctx.Query.Settings[0].Value, "'")
	if admission.UserJWS != wantJWS {
		t.Fatalf("admission UserJWS = %q, want captured token", admission.UserJWS)
	}
	if !bytes.Equal(admission.Payload.Bytes, payload) {
		t.Fatalf("payload bytes = %v, want exact client data bytes %v", admission.Payload.Bytes, payload)
	}
}

func TestIngressAcceptsCSVWithNamesPayload(t *testing.T) {
	p, signer := newSignedIngress(t)
	sql := "INSERT INTO tenant.events FORMAT CSVWithNames"
	qctx := signedQueryContext(t, 16, signer, sql, sql, sqlmeta.StatementTypeInsert)
	qctx.AccessedTables = []sqlmeta.AccessedTable{{
		OriginalDatabase: "tenant",
		OriginalTable:    "events",
		LogicalDatabase:  "tenant",
		PhysicalDatabase: "tenant",
	}}

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	payload := []byte("p,v\nwest,7\n")
	if err := p.OnClientDataStrict(context.Background(), qctx, payload); err != nil {
		t.Fatalf("OnClientDataStrict: %v", err)
	}
	p.OnQueryInputComplete(context.Background(), qctx)

	admission, err := p.ConsumeAdmission(qctx.Session.ID())
	if err != nil {
		t.Fatalf("ConsumeAdmission: %v", err)
	}
	if admission.Payload.Encoding != sicore.EncodingCSVWithNames {
		t.Fatalf("payload encoding = %q, want %q", admission.Payload.Encoding, sicore.EncodingCSVWithNames)
	}
	if !bytes.Equal(admission.Payload.Bytes, payload) {
		t.Fatalf("payload bytes = %q, want exact CSV bytes %q", admission.Payload.Bytes, payload)
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
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	raw := []byte{byte(chproto.ClientDataCode), 0, 1, 2, 3}
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
	p, signer := newSignedIngress(t)
	sql := "INSERT INTO `tenant`.`event.table` FORMAT Native"
	qctx := signedQueryContext(t, 60, signer, sql, sql, sqlmeta.StatementTypeInsert)
	qctx.AccessedTables = []sqlmeta.AccessedTable{{OriginalDatabase: "tenant", OriginalTable: "event.table"}}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if err := p.OnClientDataStrict(context.Background(), qctx, []byte{byte(chproto.ClientDataCode), 1}); err != nil {
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
			if err == nil || !strings.Contains(err.Error(), "CSVWithNames INSERT") {
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

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery after server rewrite: %v", err)
	}
	if qctx.Query.Body != forwardSQL {
		t.Fatalf("forward SQL changed to %q, want %q", qctx.Query.Body, forwardSQL)
	}
	if err := p.OnClientDataStrict(context.Background(), qctx, []byte{byte(chproto.ClientDataCode), 1}); err != nil {
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
	if err == nil || !strings.Contains(err.Error(), "CSVWithNames INSERT") {
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
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if err := p.OnClientDataStrict(context.Background(), qctx, []byte{byte(chproto.ClientDataCode), 1}); err != nil {
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
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if err := p.OnClientDataStrict(context.Background(), qctx, []byte{byte(chproto.ClientDataCode), 1}); err != nil {
		t.Fatalf("OnClientDataStrict: %v", err)
	}
	// The consumer runs at the pre-splice strict end-of-input boundary; success
	// returns nil so the terminating block is allowed to reach the upstream.
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

// TestIngressConsumerFailureIsFailClosed pins the fix for the fail-open hole: a
// consumer rejection/unavailability now surfaces as an error from the pre-splice
// strict end-of-input hook, so Relay rejects the query and the terminating empty
// Data block is never forwarded — the upstream INSERT cannot commit and return
// success behind a failed admission.
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
	// The pre-splice strict hook must return the consumer error so Relay rejects
	// the query before the terminating block reaches ClickHouse.
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

func newSignedIngress(t *testing.T) (*Plugin, *auth.RelaySigner) {
	return newSignedIngressWithConfig(t, Config{})
}

func newSignedIngressWithConfig(t *testing.T, cfg Config) (*Plugin, *auth.RelaySigner) {
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
	return &plugin.QueryContext{
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
