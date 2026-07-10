package storageintegrity

import (
	"bytes"
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"housegate/housegate/pkg/auth"
	"housegate/housegate/pkg/chproto"
	"housegate/housegate/pkg/chsession"
	"housegate/housegate/pkg/lthash"
	"housegate/housegate/pkg/plugin"
	"housegate/housegate/pkg/replay"
	"housegate/housegate/pkg/replay/payloadexec"
	"housegate/housegate/pkg/sqlmeta"
	core "housegate/housegate/pkg/storageintegrity"
)

func TestPluginCapturesInsertPayloadAndSubmitsExternalDAAndSequencer(t *testing.T) {
	da := &fakeDA{}
	seq := &fakeSequencer{}
	p := New(Config{
		UnsafeDatabase: "hg_unsafe",
		SafeDatabase:   "hg_safe",
		DA:             da,
		Sequencer:      seq,
	})
	const revision = 54453
	state := chsession.NewSessionState()
	state.ClientRevision = revision
	sess := &fakeSession{id: 7, state: state}
	qctx := &plugin.QueryContext{
		Session:       sess,
		OriginalSQL:   "INSERT INTO accounts.events VALUES",
		Query:         &chproto.Query{Body: "INSERT INTO accounts.events VALUES"},
		StatementType: sqlmeta.StatementTypeInsert,
	}

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if qctx.Query.Body != "INSERT INTO `hg_unsafe`.`events` VALUES" {
		t.Fatalf("rewritten query = %q", qctx.Query.Body)
	}
	raw := encodeStorageNativePayload(t, revision, proto.Input{
		{Name: "id", Data: &proto.ColUInt64{1}},
		{Name: "label", Data: storageColStr("alpha")},
	})
	if err := p.OnClientData(context.Background(), qctx, raw); err != nil {
		t.Fatalf("OnClientData: %v", err)
	}

	p.OnQueryComplete(context.Background(), sess)

	if !bytes.Equal(da.payload, raw) {
		t.Fatalf("payload = %q, want captured native payload", da.payload)
	}
	if seq.rec.StatementID == "" || seq.rec.TableID != "accounts.events" {
		t.Fatalf("sequencer record = %+v", seq.rec)
	}
	if seq.rec.UnsafeTable != "`hg_unsafe`.`events`" || seq.rec.SafeTable != "`hg_safe`.`events`" {
		t.Fatalf("sequencer tables = unsafe %q safe %q", seq.rec.UnsafeTable, seq.rec.SafeTable)
	}
	if seq.rec.Payload.Ref == "" {
		t.Fatalf("payload commitment = %+v", seq.rec.Payload)
	}
	if seq.rec.SourceClaimRoot == "" {
		t.Fatalf("source claim root should be populated")
	}
}

func TestPluginUsesQueryIDAsInsertStatementID(t *testing.T) {
	da := &fakeDA{}
	seq := &fakeSequencer{}
	p := New(Config{
		UnsafeDatabase: "hg_unsafe",
		SafeDatabase:   "hg_safe",
		DA:             da,
		Sequencer:      seq,
	})
	const revision = 54453
	state := chsession.NewSessionState()
	state.ClientRevision = revision
	sess := &fakeSession{id: 77, state: state}
	qctx := &plugin.QueryContext{
		Session:       sess,
		OriginalSQL:   "INSERT INTO tenant_a.events",
		Query:         &chproto.Query{ID: "query-statement-id", Body: "INSERT INTO tenant_a.events"},
		StatementType: sqlmeta.StatementTypeInsert,
	}

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	raw := encodeStorageNativePayload(t, revision, proto.Input{
		{Name: "id", Data: &proto.ColUInt64{1}},
		{Name: "label", Data: storageColStr("alpha")},
	})
	if err := p.OnClientData(context.Background(), qctx, raw); err != nil {
		t.Fatalf("OnClientData: %v", err)
	}
	p.OnQueryComplete(context.Background(), sess)

	if seq.rec.StatementID != "query-statement-id" {
		t.Fatalf("statement_id = %q, want query-statement-id", seq.rec.StatementID)
	}
}

func TestPluginCapturesInsertWhenRewriterLeavesStatementTypeUnset(t *testing.T) {
	da := &fakeDA{}
	seq := &fakeSequencer{}
	p := New(Config{
		UnsafeDatabase: "hg_unsafe",
		SafeDatabase:   "hg_safe",
		DA:             da,
		Sequencer:      seq,
	})
	const revision = 54453
	state := chsession.NewSessionState()
	state.SetLogicalDatabase("tenant_a")
	state.ClientRevision = revision
	sess := &fakeSession{id: 10, state: state}
	qctx := &plugin.QueryContext{
		Session:     sess,
		OriginalSQL: "INSERT INTO events FORMAT Native",
		Query:       &chproto.Query{Body: "INSERT INTO events FORMAT Native"},
	}

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if qctx.Query.Body != "INSERT INTO `hg_unsafe`.`events` FORMAT Native" {
		t.Fatalf("rewritten query = %q", qctx.Query.Body)
	}
	raw := encodeStorageNativePayload(t, revision, proto.Input{
		{Name: "id", Data: &proto.ColUInt64{1}},
		{Name: "label", Data: storageColStr("alpha")},
	})
	if err := p.OnClientData(context.Background(), qctx, raw); err != nil {
		t.Fatalf("OnClientData: %v", err)
	}
	p.OnQueryComplete(context.Background(), sess)

	if seq.rec.TableID != "tenant_a.events" {
		t.Fatalf("table id = %q, want tenant_a.events", seq.rec.TableID)
	}
	if seq.rec.UnsafeTable != "`hg_unsafe`.`events`" || seq.rec.SafeTable != "`hg_safe`.`events`" {
		t.Fatalf("sequencer tables = unsafe %q safe %q", seq.rec.UnsafeTable, seq.rec.SafeTable)
	}
	if !bytes.Equal(da.payload, raw) {
		t.Fatalf("payload = %q, want captured native payload", da.payload)
	}
}

func TestPluginUsesRewriterOutputWithoutSecondRewrite(t *testing.T) {
	da := &fakeDA{}
	seq := &fakeSequencer{}
	p := New(Config{
		UnsafeDatabase: "hg_unsafe",
		SafeDatabase:   "hg_safe",
		DA:             da,
		Sequencer:      seq,
	})
	const revision = 54453
	state := chsession.NewSessionState()
	state.ClientRevision = revision
	sess := &fakeSession{id: 8, state: state}
	qctx := &plugin.QueryContext{
		Session:       sess,
		OriginalSQL:   "INSERT INTO tenant_a.events FORMAT Native",
		RewrittenSQL:  "INSERT INTO hg_unsafe.events FORMAT Native",
		Query:         &chproto.Query{Body: "INSERT INTO hg_unsafe.events FORMAT Native"},
		StatementType: sqlmeta.StatementTypeInsert,
		AccessedTables: []sqlmeta.AccessedTable{{
			OriginalDatabase: "tenant_a",
			OriginalTable:    "events",
			LogicalDatabase:  "tenant_a",
			PhysicalDatabase: "hg_unsafe",
		}},
		TableRewrites: map[string]string{
			"tenant_a.events": "hg_unsafe.events",
		},
	}

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if qctx.Query.Body != "INSERT INTO hg_unsafe.events FORMAT Native" {
		t.Fatalf("storage_integrity rewrote over rewriter output: %q", qctx.Query.Body)
	}
	raw := encodeStorageNativePayload(t, revision, proto.Input{
		{Name: "id", Data: &proto.ColUInt64{1}},
		{Name: "label", Data: storageColStr("alpha")},
	})
	if err := p.OnClientData(context.Background(), qctx, raw); err != nil {
		t.Fatalf("OnClientData: %v", err)
	}

	p.OnQueryComplete(context.Background(), sess)

	if seq.rec.TableID != "tenant_a.events" {
		t.Fatalf("table id = %q, want tenant_a.events", seq.rec.TableID)
	}
	if seq.rec.UnsafeSQL != "INSERT INTO hg_unsafe.events FORMAT Native" {
		t.Fatalf("unsafe sql = %q", seq.rec.UnsafeSQL)
	}
	if seq.rec.UnsafeTable != "`hg_unsafe`.`events`" {
		t.Fatalf("unsafe table = %q", seq.rec.UnsafeTable)
	}
	if seq.rec.SafeTable != "`hg_safe`.`events`" {
		t.Fatalf("safe table = %q", seq.rec.SafeTable)
	}
	if !bytes.Equal(da.payload, raw) {
		t.Fatalf("payload = %q, want captured native payload", da.payload)
	}
}

func TestPluginRewritesInsertToActiveUnsafeBuffer(t *testing.T) {
	da := &fakeDA{}
	seq := &fakeSequencer{activeBuffer: core.UnsafeBufferInfo{
		TableID:        "tenant_a.events",
		UnsafeBufferID: 1,
		Epoch:          42,
		Database:       "hg_unsafe_1",
	}}
	p := New(Config{
		UnsafeDatabase:        "hg_unsafe",
		UnsafeBufferDatabases: []string{"hg_unsafe_0", "hg_unsafe_1"},
		SafeDatabase:          "hg_safe",
		DA:                    da,
		Sequencer:             seq,
		UnsafeBufferResolver:  seq,
	})
	const revision = 54453
	state := chsession.NewSessionState()
	state.ClientRevision = revision
	sess := &fakeSession{id: 88, state: state}
	qctx := &plugin.QueryContext{
		Session:       sess,
		OriginalSQL:   "INSERT INTO tenant_a.events FORMAT Native",
		RewrittenSQL:  "INSERT INTO hg_unsafe.events FORMAT Native",
		Query:         &chproto.Query{Body: "INSERT INTO hg_unsafe.events FORMAT Native"},
		StatementType: sqlmeta.StatementTypeInsert,
		AccessedTables: []sqlmeta.AccessedTable{{
			OriginalDatabase: "tenant_a",
			OriginalTable:    "events",
			LogicalDatabase:  "tenant_a",
			PhysicalDatabase: "hg_unsafe",
		}},
		TableRewrites: map[string]string{
			"tenant_a.events": "hg_unsafe.events",
		},
	}

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if qctx.Query.Body != "INSERT INTO `hg_unsafe_1`.`events` FORMAT Native" {
		t.Fatalf("rewritten query = %q", qctx.Query.Body)
	}
	raw := encodeStorageNativePayload(t, revision, proto.Input{
		{Name: "id", Data: &proto.ColUInt64{1}},
		{Name: "label", Data: storageColStr("alpha")},
	})
	if err := p.OnClientData(context.Background(), qctx, raw); err != nil {
		t.Fatalf("OnClientData: %v", err)
	}
	p.OnQueryComplete(context.Background(), sess)

	if seq.activeReq.TableID != "tenant_a.events" || seq.activeReq.TableName != "events" {
		t.Fatalf("active buffer request = %+v", seq.activeReq)
	}
	if seq.rec.UnsafeTable != "`hg_unsafe_1`.`events`" || seq.rec.UnsafeSQL != "INSERT INTO `hg_unsafe_1`.`events` FORMAT Native" {
		t.Fatalf("insert unsafe target sql/table = %q / %q", seq.rec.UnsafeSQL, seq.rec.UnsafeTable)
	}
	if seq.rec.UnsafeBufferID != 1 || seq.rec.UnsafeBufferEpoch != 42 || seq.rec.UnsafeBufferDatabase != "hg_unsafe_1" {
		t.Fatalf("insert unsafe buffer metadata = id %d epoch %d db %q", seq.rec.UnsafeBufferID, seq.rec.UnsafeBufferEpoch, seq.rec.UnsafeBufferDatabase)
	}
}

func TestPluginSubmitsInsertOnCloseFallback(t *testing.T) {
	da := &fakeDA{}
	seq := &fakeSequencer{}
	p := New(Config{
		UnsafeDatabase: "hg_unsafe",
		SafeDatabase:   "hg_safe",
		DA:             da,
		Sequencer:      seq,
		InjectRowID:    false,
	})
	const revision = 54453
	state := chsession.NewSessionState()
	state.ClientRevision = revision
	sess := &fakeSession{id: 91, state: state}
	qctx := &plugin.QueryContext{
		Session:       sess,
		OriginalSQL:   "INSERT INTO tenant_a.events FORMAT Native",
		Query:         &chproto.Query{ID: "stmt-close-fallback", Body: "INSERT INTO tenant_a.events FORMAT Native"},
		StatementType: sqlmeta.StatementTypeInsert,
	}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	raw := encodeStorageNativePayload(t, revision, proto.Input{
		{Name: "id", Data: &proto.ColUInt64{1}},
		{Name: "label", Data: storageColStr("alpha")},
	})
	if err := p.OnClientData(context.Background(), qctx, raw); err != nil {
		t.Fatalf("OnClientData: %v", err)
	}

	p.OnClose(sess)

	if seq.rec.StatementID != "stmt-close-fallback" || seq.rec.Payload.Hash == "" {
		t.Fatalf("insert record after OnClose = %+v", seq.rec)
	}
	p.OnQueryComplete(context.Background(), sess)
	if seq.rec.StatementID != "stmt-close-fallback" {
		t.Fatalf("OnQueryComplete after OnClose submitted unexpected record: %+v", seq.rec)
	}
}

func TestPluginSubmitsNativePayloadReplayMetadata(t *testing.T) {
	const revision = 54453
	da := &fakeDA{}
	seq := &fakeSequencer{}
	p := New(Config{
		UnsafeDatabase: "hg_unsafe",
		SafeDatabase:   "hg_safe",
		DA:             da,
		Sequencer:      seq,
	})
	state := chsession.NewSessionState()
	state.ClientRevision = revision
	sess := &fakeSession{id: 9, state: state}
	qctx := &plugin.QueryContext{
		Session:       sess,
		OriginalSQL:   "INSERT INTO tenant_a.events",
		RewrittenSQL:  "INSERT INTO hg_unsafe.events",
		Query:         &chproto.Query{Body: "INSERT INTO hg_unsafe.events"},
		StatementType: sqlmeta.StatementTypeInsert,
		AccessedTables: []sqlmeta.AccessedTable{{
			OriginalDatabase: "tenant_a",
			OriginalTable:    "events",
			LogicalDatabase:  "tenant_a",
			PhysicalDatabase: "hg_unsafe",
		}},
		TableRewrites: map[string]string{
			"tenant_a.events": "hg_unsafe.events",
		},
	}
	raw := encodeStorageNativePayload(t, revision, proto.Input{
		{Name: "id", Data: &proto.ColUInt64{1}},
		{Name: "label", Data: storageColStr("alpha")},
	})
	wantClaim, err := core.ComputeNativePayloadClaim("tenant_a.events", revision, raw)
	if err != nil {
		t.Fatalf("ComputeNativePayloadClaim: %v", err)
	}
	wantSnap, err := core.NativePayloadGenesisSnapshot("tenant_a.events", wantClaim.Columns)
	if err != nil {
		t.Fatalf("NativePayloadGenesisSnapshot: %v", err)
	}
	// HG-P0-01: no SnapshotReader is wired here, so the submit path pins the
	// genesis snapshot as base and derives the source root through the unified
	// replay.AssembleStateRoot assembly (base + candidate part_row_lthash).
	wantSourceClaimRoot, err := core.NativeSourceClaimRoot(wantSnap, "tenant_a.events", wantClaim.PartRowLtHash)
	if err != nil {
		t.Fatalf("NativeSourceClaimRoot: %v", err)
	}

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if err := p.OnClientData(context.Background(), qctx, raw); err != nil {
		t.Fatalf("OnClientData: %v", err)
	}
	p.OnQueryComplete(context.Background(), sess)

	if seq.rec.SourceClaimRoot != wantSourceClaimRoot {
		t.Fatalf("source claim root = %s, want %s", seq.rec.SourceClaimRoot, wantSourceClaimRoot)
	}
	if seq.rec.PayloadEncoding != core.PayloadEncodingClickHouseNativeData {
		t.Fatalf("payload encoding = %q", seq.rec.PayloadEncoding)
	}
	if seq.rec.PayloadRevision != revision {
		t.Fatalf("payload revision = %d, want %d", seq.rec.PayloadRevision, revision)
	}
	if seq.rec.PrevSafeSnapshotID == "" || seq.rec.PrevStateRoot == "" ||
		seq.rec.SchemaSnapshotID == "" || seq.rec.ExecutorProfileID == "" {
		t.Fatalf("replay metadata missing from insert record: %+v", seq.rec)
	}
	if seq.rec.SettingsHash != core.DefaultReplaySettingsHash {
		t.Fatalf("settings hash = %q, want %q", seq.rec.SettingsHash, core.DefaultReplaySettingsHash)
	}
	// HG-P0-02: the source result-claim must declare the candidate part(s) with
	// the content-addressed part_row_lthash and row count from the payload.
	if len(seq.rec.CandidateParts) != 1 {
		t.Fatalf("candidate parts = %+v, want exactly 1 declared", seq.rec.CandidateParts)
	}
	cp := seq.rec.CandidateParts[0]
	if cp.PartRowLtHash != wantClaim.PartRowLtHash || cp.RowCount != wantClaim.RowCount || cp.PartitionID != core.NativeAllPartitionID {
		t.Fatalf("candidate part = %+v, want {partition %q, lthash %s, rows %d}", cp, core.NativeAllPartitionID, wantClaim.PartRowLtHash, wantClaim.RowCount)
	}
}

func TestPluginRewriteClientDataInjectsRowIDAndCapturesMaterializedPayload(t *testing.T) {
	const revision = 54453
	da := &fakeDA{}
	seq := &fakeSequencer{}
	p := New(Config{
		UnsafeDatabase: "hg_unsafe",
		SafeDatabase:   "hg_safe",
		DA:             da,
		Sequencer:      seq,
		NetworkID:      "sentio-testnet",
		InjectRowID:    true,
	})
	state := chsession.NewSessionState()
	state.ClientRevision = revision
	sess := &fakeSession{id: 17, state: state}
	qctx := &plugin.QueryContext{
		Session:       sess,
		OriginalSQL:   "INSERT INTO tenant_a.events",
		RewrittenSQL:  "INSERT INTO hg_unsafe.events",
		Query:         &chproto.Query{Body: "INSERT INTO hg_unsafe.events"},
		StatementType: sqlmeta.StatementTypeInsert,
		AccessedTables: []sqlmeta.AccessedTable{{
			OriginalDatabase: "tenant_a",
			OriginalTable:    "events",
			LogicalDatabase:  "tenant_a",
			PhysicalDatabase: "hg_unsafe",
		}},
		TableRewrites: map[string]string{
			"tenant_a.events": "hg_unsafe.events",
		},
	}
	raw := encodeStorageNativePayload(t, revision, proto.Input{
		{Name: "id", Data: &proto.ColUInt64{1, 2}},
		{Name: "label", Data: storageColStr("alpha", "beta")},
	})

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	rewritten, err := p.RewriteClientData(context.Background(), qctx, raw)
	if err != nil {
		t.Fatalf("RewriteClientData: %v", err)
	}
	if bytes.Equal(rewritten, raw) {
		t.Fatal("RewriteClientData returned the original packet; want _hg_row_id injection")
	}
	p.OnQueryComplete(context.Background(), sess)

	if !bytes.Equal(da.payload, rewritten) {
		t.Fatalf("DA payload was not the materialized payload")
	}
	rows := decodeStorageNativeRows(t, revision, da.payload)
	if len(rows.columns) != 3 || rows.columns[0].Name != "_hg_row_id" || rows.columns[0].Type != "FixedString(32)" {
		t.Fatalf("columns = %+v, want _hg_row_id FixedString(32) prepended", rows.columns)
	}
	for i := 0; i < rows.rowCount; i++ {
		want := payloadexec.RowID("sentio-testnet", "tenant_a.events", seq.rec.StatementID, uint64(i))
		if got := rows.rowIDs[i]; !bytes.Equal(got, want) {
			t.Fatalf("row %d _hg_row_id = %x, want %x", i, got, want)
		}
	}
	if seq.rec.PayloadEncoding != core.PayloadEncodingClickHouseNativeData {
		t.Fatalf("payload encoding = %q", seq.rec.PayloadEncoding)
	}
	if seq.rec.SourceClaimRoot == "" {
		t.Fatal("source claim root missing")
	}
}

func TestPluginRequiresAndRecordsJWSForStorageIntegrityInsert(t *testing.T) {
	const privateKey = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	signer, err := auth.NewRelaySigner(privateKey)
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}
	sql := "INSERT INTO events FORMAT Native"
	token, err := signer.SignToken(sql)
	if err != nil {
		t.Fatalf("SignToken: %v", err)
	}
	validator := auth.NewEthValidator([]string{signer.Address()}, time.Hour, true, false, "", nil)
	da := &fakeDA{}
	seq := &fakeSequencer{}
	p := New(Config{
		UnsafeDatabase:    "hg_unsafe",
		SafeDatabase:      "hg_safe",
		DA:                da,
		Sequencer:         seq,
		RequireAuthToken:  true,
		AuthValidator:     validator,
		RequireRowIDInput: false,
	})
	state := chsession.NewSessionState()
	state.ClientRevision = 54453
	sess := &fakeSession{id: 18, state: state}
	qctx := &plugin.QueryContext{
		Session:     sess,
		OriginalSQL: sql,
		Query: &chproto.Query{
			Body: sql,
			Settings: []chproto.Setting{{
				Key:    auth.AuthTokenSettingKey,
				Value:  "'" + token + "'",
				Custom: true,
			}},
		},
		StatementType: sqlmeta.StatementTypeInsert,
	}

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	raw := encodeStorageNativePayload(t, 54453, proto.Input{
		{Name: "id", Data: &proto.ColUInt64{1}},
		{Name: "label", Data: storageColStr("alpha")},
	})
	if err := p.OnClientData(context.Background(), qctx, raw); err != nil {
		t.Fatalf("OnClientData: %v", err)
	}
	p.OnQueryComplete(context.Background(), sess)

	if seq.rec.UserJWS == "" {
		t.Fatalf("insert record did not preserve user JWS: %+v", seq.rec)
	}
	if seq.rec.AuthenticatedSigner != signer.Address() {
		t.Fatalf("authenticated signer = %q, want %q", seq.rec.AuthenticatedSigner, signer.Address())
	}

	badToken, err := signer.SignToken("SELECT 1")
	if err != nil {
		t.Fatalf("SignToken bad: %v", err)
	}
	qctx = &plugin.QueryContext{
		Session:     sess,
		OriginalSQL: sql,
		Query: &chproto.Query{
			Body: sql,
			Settings: []chproto.Setting{{
				Key:    auth.AuthTokenSettingKey,
				Value:  "'" + badToken + "'",
				Custom: true,
			}},
		},
		StatementType: sqlmeta.StatementTypeInsert,
	}
	if err := p.OnQuery(context.Background(), qctx); err == nil {
		t.Fatal("OnQuery accepted a JWS signed for different SQL, want rejection")
	}
}

func TestPluginRejectsUnmaterializedNondeterminismForStorageIntegrityWrites(t *testing.T) {
	p := New(Config{
		UnsafeDatabase: "hg_unsafe",
		SafeDatabase:   "hg_safe",
		DA:             &fakeDA{},
		Sequencer:      &fakeSequencer{},
	})
	sess := &fakeSession{id: 19, state: chsession.NewSessionState()}

	insert := &plugin.QueryContext{
		Session:       sess,
		OriginalSQL:   "INSERT INTO events VALUES (now())",
		Query:         &chproto.Query{Body: "INSERT INTO events VALUES (now())"},
		StatementType: sqlmeta.StatementTypeInsert,
	}
	err := p.OnQuery(context.Background(), insert)
	if err == nil || !strings.Contains(err.Error(), "unmaterialized nondeterministic function") {
		t.Fatalf("insert OnQuery err = %v, want nondeterminism rejection", err)
	}

	mutation := &plugin.QueryContext{
		Session:     sess,
		OriginalSQL: "ALTER TABLE events UPDATE ts = now() WHERE day = '2026-07-03'",
		Query:       &chproto.Query{Body: "ALTER TABLE events UPDATE ts = now() WHERE day = '2026-07-03'"},
	}
	err = p.OnQuery(context.Background(), mutation)
	if err == nil || !strings.Contains(err.Error(), "unmaterialized nondeterministic function") {
		t.Fatalf("mutation OnQuery err = %v, want nondeterminism rejection", err)
	}

	literalOnly := &plugin.QueryContext{
		Session:     sess,
		OriginalSQL: "ALTER TABLE events UPDATE label = 'now() text' WHERE day = '2026-07-03'",
		Query:       &chproto.Query{Body: "ALTER TABLE events UPDATE label = 'now() text' WHERE day = '2026-07-03'"},
	}
	if err := p.OnQuery(context.Background(), literalOnly); err != nil {
		t.Fatalf("literal-only mutation rejected: %v", err)
	}
}

func TestPluginSubmitsBoundedUpdateMutationAndAbortsWithSuccess(t *testing.T) {
	seq := &fakeSequencer{}
	p := New(Config{
		UnsafeDatabase: "hg_unsafe",
		SafeDatabase:   "hg_safe",
		Sequencer:      seq,
	})
	state := chsession.NewSessionState()
	state.SetLogicalDatabase("tenant_a")
	sess := &fakeSession{id: 11, state: state}
	qctx := &plugin.QueryContext{
		Session:     sess,
		OriginalSQL: "ALTER TABLE events UPDATE label = concat(label, '-mut') WHERE id = 1",
		Query:       &chproto.Query{Body: "ALTER TABLE events UPDATE label = concat(label, '-mut') WHERE id = 1"},
	}

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if !qctx.AbortWithSuccess {
		t.Fatal("bounded mutation should abort upstream forwarding with synthetic success")
	}
	if seq.mut.StatementID == "" || seq.mut.TableID != "tenant_a.events" {
		t.Fatalf("mutation record = %+v", seq.mut)
	}
	if seq.mut.MutationType != core.MutationTypeUpdate {
		t.Fatalf("mutation type = %q, want %q", seq.mut.MutationType, core.MutationTypeUpdate)
	}
	if seq.mut.SafeTable != "`hg_safe`.`events`" {
		t.Fatalf("safe table = %q", seq.mut.SafeTable)
	}
	if seq.mut.MutationSQL != "ALTER TABLE `hg_safe`.`events` UPDATE label = concat(label, '-mut') WHERE id = 1" {
		t.Fatalf("mutation sql = %q", seq.mut.MutationSQL)
	}
}

// A plain `DELETE FROM ... WHERE` is a normalizable bounded mutation (spec
// §7.1) and must be accepted even when reject_lightweight_delete is set: it is
// rewritten into a heavyweight `ALTER TABLE ... DELETE`, not a lightweight mask.
func TestPluginAcceptsNormalizableDeleteFromWhenLightweightRejected(t *testing.T) {
	seq := &fakeSequencer{}
	p := New(Config{
		UnsafeDatabase:          "hg_unsafe",
		SafeDatabase:            "hg_safe",
		Sequencer:               seq,
		RejectLightweightDelete: true,
		PartitionColumns:        []string{"day"},
	})
	state := chsession.NewSessionState()
	state.SetLogicalDatabase("tenant_a")
	sess := &fakeSession{id: 21, state: state}
	qctx := &plugin.QueryContext{
		Session:     sess,
		OriginalSQL: "DELETE FROM events WHERE day = '2026-07-03'",
		Query:       &chproto.Query{Body: "DELETE FROM events WHERE day = '2026-07-03'"},
	}

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery err = %v, want normalizable DELETE FROM to be accepted", err)
	}
	if seq.mut.StatementID == "" {
		t.Fatal("expected a mutation record for normalizable DELETE FROM")
	}
	if seq.mut.MutationType != core.MutationTypeDelete {
		t.Fatalf("mutation type = %q, want delete", seq.mut.MutationType)
	}
}

// An explicit ClickHouse lightweight-delete mask request (via query setting) is
// the "lightweight DELETE mask" the spec rejects when configured.
func TestPluginRejectsLightweightDeleteMaskSettingWhenConfigured(t *testing.T) {
	p := New(Config{
		UnsafeDatabase:          "hg_unsafe",
		SafeDatabase:            "hg_safe",
		Sequencer:               &fakeSequencer{},
		RejectLightweightDelete: true,
		PartitionColumns:        []string{"day"},
	})
	state := chsession.NewSessionState()
	state.SetLogicalDatabase("tenant_a")
	sess := &fakeSession{id: 22, state: state}
	qctx := &plugin.QueryContext{
		Session:     sess,
		OriginalSQL: "DELETE FROM events WHERE day = '2026-07-03'",
		Query: &chproto.Query{
			Body:     "DELETE FROM events WHERE day = '2026-07-03'",
			Settings: []chproto.Setting{{Key: "lightweight_deletes_sync", Value: "2"}},
		},
	}

	err := p.OnQuery(context.Background(), qctx)
	if err == nil || !strings.Contains(err.Error(), "lightweight DELETE") {
		t.Fatalf("OnQuery err = %v, want lightweight DELETE rejection", err)
	}
}

func TestPluginRejectsMutationTouchedPartitionAndManifestCostLimits(t *testing.T) {
	seq := &fakeSequencer{}
	p := New(Config{
		UnsafeDatabase:       "hg_unsafe",
		SafeDatabase:         "hg_safe",
		Sequencer:            seq,
		SnapshotReader:       seq,
		PartitionColumns:     []string{"day"},
		MaxTouchedPartitions: 1,
	})
	state := chsession.NewSessionState()
	state.SetLogicalDatabase("tenant_a")
	sess := &fakeSession{id: 22, state: state}
	qctx := &plugin.QueryContext{
		Session:     sess,
		OriginalSQL: "ALTER TABLE events UPDATE label = 'x' WHERE day IN ('2026-07-03', '2026-08-03')",
		Query:       &chproto.Query{Body: "ALTER TABLE events UPDATE label = 'x' WHERE day IN ('2026-07-03', '2026-08-03')"},
	}
	err := p.OnQuery(context.Background(), qctx)
	if err == nil || !strings.Contains(err.Error(), "touched partitions") {
		t.Fatalf("partition limit err = %v, want touched partitions rejection", err)
	}

	manifest := sealedPluginTestManifest(t, []replay.PartManifestEntry{
		{TableID: "tenant_a.events", PartitionID: "202607", PartName: "p1", PartRowLtHash: "root-1", RowCount: 10, Bytes: 600},
		{TableID: "tenant_a.events", PartitionID: "202607", PartName: "p2", PartRowLtHash: "root-2", RowCount: 5, Bytes: 500},
	})
	seq.watermark = core.SafeWatermark{SnapshotID: manifest.SnapshotID, SafeL3BlockSeq: manifest.SafeL3BlockSeq, StateRoot: manifest.StateRoot, ManifestRoot: manifest.ManifestRoot}
	seq.manifests = map[string]replay.SafeSnapshotManifest{manifest.SnapshotID: manifest}
	p = New(Config{
		UnsafeDatabase:   "hg_unsafe",
		SafeDatabase:     "hg_safe",
		Sequencer:        seq,
		SnapshotReader:   seq,
		PartitionColumns: []string{"day"},
		MaxTouchedParts:  1,
		MaxTouchedBytes:  1000,
	})
	qctx = &plugin.QueryContext{
		Session:     sess,
		OriginalSQL: "ALTER TABLE events UPDATE label = 'x' WHERE day = '2026-07-03'",
		Query:       &chproto.Query{Body: "ALTER TABLE events UPDATE label = 'x' WHERE day = '2026-07-03'"},
	}
	err = p.OnQuery(context.Background(), qctx)
	if err == nil || !strings.Contains(err.Error(), "touched parts") {
		t.Fatalf("parts limit err = %v, want touched parts rejection", err)
	}

	p = New(Config{
		UnsafeDatabase:   "hg_unsafe",
		SafeDatabase:     "hg_safe",
		Sequencer:        seq,
		SnapshotReader:   seq,
		PartitionColumns: []string{"day"},
		MaxTouchedParts:  3,
		MaxTouchedBytes:  1000,
	})
	err = p.OnQuery(context.Background(), qctx)
	if err == nil || !strings.Contains(err.Error(), "touched bytes") {
		t.Fatalf("bytes limit err = %v, want touched bytes rejection", err)
	}
}

func TestPluginRejectsUnboundedMutation(t *testing.T) {
	seq := &fakeSequencer{}
	p := New(Config{
		SafeDatabase: "hg_safe",
		Sequencer:    seq,
	})
	sess := &fakeSession{id: 12, state: chsession.NewSessionState()}
	qctx := &plugin.QueryContext{
		Session:     sess,
		OriginalSQL: "ALTER TABLE events DELETE",
		Query:       &chproto.Query{Body: "ALTER TABLE events DELETE"},
	}

	err := p.OnQuery(context.Background(), qctx)
	if err == nil {
		t.Fatal("OnQuery succeeded for unbounded mutation, want rejection")
	}
	if seq.mut.StatementID != "" {
		t.Fatalf("unexpected mutation record after rejection: %+v", seq.mut)
	}
}

func TestPluginNormalizesUpdateAndDeleteMutationSyntax(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantType string
		wantSQL  string
	}{
		{
			name:     "update set",
			sql:      "UPDATE events SET label = 'b' WHERE day = '2026-07-03'",
			wantType: core.MutationTypeUpdate,
			wantSQL:  "ALTER TABLE `hg_safe`.`events` UPDATE label = 'b' WHERE day = '2026-07-03'",
		},
		{
			name:     "delete from",
			sql:      "DELETE FROM events WHERE day = '2026-07-03'",
			wantType: core.MutationTypeDelete,
			wantSQL:  "ALTER TABLE `hg_safe`.`events` DELETE WHERE day = '2026-07-03'",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			seq := &fakeSequencer{}
			p := New(Config{
				SafeDatabase: "hg_safe",
				Sequencer:    seq,
			})
			state := chsession.NewSessionState()
			state.SetLogicalDatabase("tenant_a")
			sess := &fakeSession{id: 13, state: state}
			qctx := &plugin.QueryContext{
				Session:     sess,
				OriginalSQL: tt.sql,
				Query:       &chproto.Query{Body: tt.sql},
			}

			if err := p.OnQuery(context.Background(), qctx); err != nil {
				t.Fatalf("OnQuery: %v", err)
			}
			if seq.mut.MutationType != tt.wantType {
				t.Fatalf("mutation type = %q, want %q", seq.mut.MutationType, tt.wantType)
			}
			if seq.mut.MutationSQL != tt.wantSQL {
				t.Fatalf("mutation sql = %q, want %q", seq.mut.MutationSQL, tt.wantSQL)
			}
			if seq.mut.TableID != "tenant_a.events" {
				t.Fatalf("table id = %q, want tenant_a.events", seq.mut.TableID)
			}
		})
	}
}

func TestPluginRejectsUnsafeMutationAdmissionShapes(t *testing.T) {
	tests := []string{
		"TRUNCATE TABLE events",
		"ALTER TABLE events DROP PARTITION '202607'",
		"ALTER TABLE events UPDATE label = (SELECT max(label) FROM other) WHERE day = '2026-07-03'",
		"ALTER TABLE events UPDATE label = dictGet('d', 'v', id) WHERE day = '2026-07-03'",
		"ALTER TABLE events UPDATE label = remote('host', db, table) WHERE day = '2026-07-03'",
		"UPDATE events SET label = 'b' FROM other WHERE events.id = other.id AND day = '2026-07-03'",
	}
	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			seq := &fakeSequencer{}
			p := New(Config{
				SafeDatabase: "hg_safe",
				Sequencer:    seq,
			})
			sess := &fakeSession{id: 14, state: chsession.NewSessionState()}
			qctx := &plugin.QueryContext{
				Session:     sess,
				OriginalSQL: sql,
				Query:       &chproto.Query{Body: sql},
			}

			if err := p.OnQuery(context.Background(), qctx); err == nil {
				t.Fatalf("OnQuery succeeded for unsafe mutation %q, want rejection", sql)
			}
			if seq.mut.StatementID != "" {
				t.Fatalf("unexpected mutation record after rejection: %+v", seq.mut)
			}
		})
	}
}

func TestPluginRejectsProtocolColumnDDL(t *testing.T) {
	tests := []string{
		"ALTER TABLE events DROP COLUMN _hg_row_id",
		"ALTER TABLE events RENAME COLUMN _hg_row_id TO row_id",
		"ALTER TABLE events MODIFY COLUMN _hg_row_id String",
		"ALTER TABLE events ALTER COLUMN _hg_row_id DEFAULT 'x'",
		"ALTER TABLE events CLEAR COLUMN _hg_row_id IN PARTITION '202607'",
		"ALTER TABLE events MODIFY ORDER BY (id, _hg_row_id)",
		"ALTER TABLE events DROP COLUMN `_hg_row_id`",
		"ALTER TABLE events MODIFY ORDER BY (id, `_hg_row_id`)",
	}
	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			p := New(Config{
				SafeDatabase: "hg_safe",
				Sequencer:    &fakeSequencer{},
			})
			sess := &fakeSession{id: 141, state: chsession.NewSessionState()}
			qctx := &plugin.QueryContext{
				Session:     sess,
				OriginalSQL: sql,
				Query:       &chproto.Query{Body: sql},
			}

			err := p.OnQuery(context.Background(), qctx)
			if err == nil || !strings.Contains(err.Error(), "protocol columns") {
				t.Fatalf("OnQuery err = %v, want protocol column DDL rejection", err)
			}
		})
	}
}

// fakeKeyColumnProvider returns a fixed key-column set for gap-34 tests.
type fakeKeyColumnProvider struct {
	cols map[string][]string
	err  error
}

func (f fakeKeyColumnProvider) KeyColumns(_ context.Context, tableID string) ([]string, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.cols[tableID], nil
}

func TestPluginAutoDerivesKeyColumnProtection(t *testing.T) {
	// "id" is the sorting key (auto-derived), not in the manual ProtectedColumns.
	provider := fakeKeyColumnProvider{cols: map[string][]string{"events": {"id"}}}

	// An UPDATE that modifies the auto-derived key column "id" is rejected even
	// though it was never listed in ProtectedColumns.
	seq := &fakeSequencer{}
	p := New(Config{
		SafeDatabase:      "hg_safe",
		Sequencer:         seq,
		KeyColumnProvider: provider,
	})
	sess := &fakeSession{id: 210, state: chsession.NewSessionState()}
	sql := "ALTER TABLE events UPDATE id = 2 WHERE day = '2026-07-03'"
	qctx := &plugin.QueryContext{Session: sess, OriginalSQL: sql, Query: &chproto.Query{Body: sql}}
	err := p.OnQuery(context.Background(), qctx)
	if err == nil || !strings.Contains(err.Error(), "protected column") {
		t.Fatalf("OnQuery err = %v, want auto-derived key-column rejection", err)
	}
	if seq.mut.StatementID != "" {
		t.Fatalf("unexpected mutation record after rejection: %+v", seq.mut)
	}

	// An UPDATE on a non-key column is still admitted.
	seq2 := &fakeSequencer{}
	p2 := New(Config{SafeDatabase: "hg_safe", Sequencer: seq2, KeyColumnProvider: provider})
	sess2 := &fakeSession{id: 211, state: chsession.NewSessionState()}
	okSQL := "ALTER TABLE events UPDATE label = 'b' WHERE day = '2026-07-03'"
	qctx2 := &plugin.QueryContext{Session: sess2, OriginalSQL: okSQL, Query: &chproto.Query{Body: okSQL}}
	if err := p2.OnQuery(context.Background(), qctx2); err != nil {
		t.Fatalf("OnQuery err = %v, want non-key-column UPDATE admitted", err)
	}
	if seq2.mut.StatementID == "" {
		t.Fatal("non-key-column UPDATE was not admitted")
	}
}

func TestPluginKeyColumnProviderErrorFailsClosed(t *testing.T) {
	seq := &fakeSequencer{}
	p := New(Config{
		SafeDatabase:      "hg_safe",
		Sequencer:         seq,
		KeyColumnProvider: fakeKeyColumnProvider{err: errors.New("schema unavailable")},
	})
	sess := &fakeSession{id: 212, state: chsession.NewSessionState()}
	sql := "ALTER TABLE events UPDATE label = 'b' WHERE day = '2026-07-03'"
	qctx := &plugin.QueryContext{Session: sess, OriginalSQL: sql, Query: &chproto.Query{Body: sql}}
	if err := p.OnQuery(context.Background(), qctx); err == nil {
		t.Fatal("OnQuery succeeded despite key-column provider error, want fail-closed")
	}
	if seq.mut.StatementID != "" {
		t.Fatalf("unexpected mutation record after provider error: %+v", seq.mut)
	}
}

func TestPluginRequiresConfiguredPartitionPredicate(t *testing.T) {
	seq := &fakeSequencer{}
	p := New(Config{
		SafeDatabase:              "hg_safe",
		Sequencer:                 seq,
		RequirePartitionPredicate: true,
		PartitionColumns:          []string{"day"},
	})
	sess := &fakeSession{id: 15, state: chsession.NewSessionState()}
	qctx := &plugin.QueryContext{
		Session:     sess,
		OriginalSQL: "ALTER TABLE events UPDATE label = 'b' WHERE id = 1",
		Query:       &chproto.Query{Body: "ALTER TABLE events UPDATE label = 'b' WHERE id = 1"},
	}

	if err := p.OnQuery(context.Background(), qctx); err == nil {
		t.Fatal("OnQuery succeeded without configured partition predicate, want rejection")
	}
	if seq.mut.StatementID != "" {
		t.Fatalf("unexpected mutation record after rejection: %+v", seq.mut)
	}

	qctx = &plugin.QueryContext{
		Session:     sess,
		OriginalSQL: "ALTER TABLE events UPDATE label = 'b' WHERE day = '2026-07-03'",
		Query:       &chproto.Query{Body: "ALTER TABLE events UPDATE label = 'b' WHERE day = '2026-07-03'"},
	}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery with partition predicate: %v", err)
	}
	if seq.mut.StatementID == "" {
		t.Fatal("mutation with partition predicate did not submit")
	}
	if len(seq.mut.PartitionIDs) != 1 || seq.mut.PartitionIDs[0] != "202607" {
		t.Fatalf("partition ids = %v, want [202607]", seq.mut.PartitionIDs)
	}
}

func TestPluginRejectsMutationOfProtectedColumns(t *testing.T) {
	seq := &fakeSequencer{}
	p := New(Config{
		SafeDatabase:     "hg_safe",
		Sequencer:        seq,
		ProtectedColumns: []string{"day", "id"},
	})
	sess := &fakeSession{id: 16, state: chsession.NewSessionState()}
	qctx := &plugin.QueryContext{
		Session:     sess,
		OriginalSQL: "ALTER TABLE events UPDATE day = '2026-07-04' WHERE day = '2026-07-03'",
		Query:       &chproto.Query{Body: "ALTER TABLE events UPDATE day = '2026-07-04' WHERE day = '2026-07-03'"},
	}

	if err := p.OnQuery(context.Background(), qctx); err == nil {
		t.Fatal("OnQuery succeeded while modifying protected column, want rejection")
	}
	if seq.mut.StatementID != "" {
		t.Fatalf("unexpected mutation record after rejection: %+v", seq.mut)
	}
}

func TestPluginStatementIDsAreUniqueAcrossInstances(t *testing.T) {
	a := New(Config{})
	b := New(Config{})

	aID := a.newStatementID()
	bID := b.newStatementID()
	if aID == bID {
		t.Fatalf("two plugin instances generated the same statement id %q", aID)
	}
}

func TestPluginRejectsSafeReadWhenNodeIsNotInActiveReadSet(t *testing.T) {
	gate := &fakeReadGate{decision: core.SafeReadDecision{
		Active:       false,
		Reason:       "node quarantined",
		SnapshotID:   "snap-2",
		SafeL3BlockSeq: 10,
	}}
	p := New(Config{
		SafeDatabase: "hg_safe",
		ReadGate:     gate,
		NodeID:       "node-a",
	})
	sess := &fakeSession{id: 88, state: chsession.NewSessionState()}
	qctx := &plugin.QueryContext{
		Session:       sess,
		OriginalSQL:   "SELECT * FROM `hg_safe`.`events`",
		Query:         &chproto.Query{Body: "SELECT * FROM `hg_safe`.`events`"},
		StatementType: sqlmeta.StatementTypeSelect,
		AccessedTables: []sqlmeta.AccessedTable{{
			OriginalDatabase: "tenant_a",
			OriginalTable:    "events",
			PhysicalDatabase: "hg_safe",
		}},
	}

	err := p.OnQuery(context.Background(), qctx)
	if err == nil || !strings.Contains(err.Error(), "safe read gated") {
		t.Fatalf("OnQuery err = %v, want safe read gated", err)
	}
	if gate.req.NodeID != "node-a" || len(gate.req.TableIDs) != 1 || gate.req.TableIDs[0] != "tenant_a.events" {
		t.Fatalf("read gate request = %+v", gate.req)
	}
}

// TestPluginSafeReadBindsCurrentSnapshot proves HG-P2-02: gateSafeRead binds
// the request to the current safe watermark snapshot, so the read-set cache
// (keyed on snapshot/node/tables) can hit instead of always bypassing on an
// empty SnapshotID.
func TestPluginSafeReadBindsCurrentSnapshot(t *testing.T) {
	gate := &fakeReadGate{decision: core.SafeReadDecision{Active: true}}
	seq := &fakeSequencer{}
	seq.watermark = core.SafeWatermark{SnapshotID: "snap-current"}
	p := New(Config{
		SafeDatabase:   "hg_safe",
		ReadGate:       gate,
		SnapshotReader: seq,
		NodeID:         "node-a",
	})
	sess := &fakeSession{id: 89, state: chsession.NewSessionState()}
	qctx := &plugin.QueryContext{
		Session:       sess,
		OriginalSQL:   "SELECT * FROM `hg_safe`.`events`",
		Query:         &chproto.Query{Body: "SELECT * FROM `hg_safe`.`events`"},
		StatementType: sqlmeta.StatementTypeSelect,
		AccessedTables: []sqlmeta.AccessedTable{{
			OriginalDatabase: "tenant_a", OriginalTable: "events", PhysicalDatabase: "hg_safe",
		}},
	}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if gate.req.SnapshotID != "snap-current" {
		t.Fatalf("read gate request SnapshotID = %q, want the current watermark snapshot", gate.req.SnapshotID)
	}
}

type fakeDA struct {
	payload []byte
}

func (f *fakeDA) PutPayload(_ context.Context, req core.PutPayloadRequest) (core.PayloadCommitment, error) {
	f.payload = append([]byte(nil), req.Payload...)
	return core.PayloadCommitment{Ref: "mockda://" + req.TableID + "/" + req.StatementID + "/hash", Hash: "0xhash", Length: uint64(len(req.Payload))}, nil
}

type fakeSequencer struct {
	rec          core.InsertRecord
	mut          core.MutationRecord
	watermark    core.SafeWatermark
	manifests    map[string]replay.SafeSnapshotManifest
	activeBuffer core.UnsafeBufferInfo
	activeReq    core.ActiveUnsafeBufferRequest
}

func (f *fakeSequencer) SubmitInsert(_ context.Context, rec core.InsertRecord) error {
	f.rec = rec
	return nil
}

func (f *fakeSequencer) SubmitMutation(_ context.Context, rec core.MutationRecord) error {
	f.mut = rec
	return nil
}

func (f *fakeSequencer) GetActiveUnsafeBuffer(_ context.Context, req core.ActiveUnsafeBufferRequest) (core.UnsafeBufferInfo, error) {
	f.activeReq = req
	if f.activeBuffer.TableID == "" {
		f.activeBuffer.TableID = req.TableID
	}
	return f.activeBuffer, nil
}

func (f *fakeSequencer) GetSafeWatermark(context.Context) (core.SafeWatermark, error) {
	return f.watermark, nil
}

func (f *fakeSequencer) GetSafeSnapshot(_ context.Context, snapshotID string) (replay.SafeSnapshotManifest, bool, error) {
	manifest, ok := f.manifests[snapshotID]
	return manifest, ok, nil
}

func sealedPluginTestManifest(t *testing.T, parts []replay.PartManifestEntry) replay.SafeSnapshotManifest {
	t.Helper()
	roots := make([]replay.PartitionCommitment, 0, len(parts))
	for _, part := range parts {
		roots = append(roots, replay.PartitionCommitment{
			TableID:     part.TableID,
			PartitionID: part.PartitionID,
			Root:        part.PartRowLtHash,
		})
	}
	manifest, err := (replay.SafeSnapshotManifest{
		SafeL3BlockSeq:      11,
		SchemaSnapshotID:  "schema",
		SchemaRoot:        "schema-root",
		ExecutorProfileID: "exec",
		Tables: []replay.TableManifest{{
			TableID:        "tenant_a.events",
			SchemaHash:     "schema-hash",
			PartitionRoots: roots,
			ActiveParts:    parts,
		}},
	}).Seal()
	if err != nil {
		t.Fatalf("seal manifest: %v", err)
	}
	return manifest
}

type fakeReadGate struct {
	req      core.SafeReadRequest
	decision core.SafeReadDecision
}

func (f *fakeReadGate) CheckSafeRead(_ context.Context, req core.SafeReadRequest) (core.SafeReadDecision, error) {
	f.req = req
	return f.decision, nil
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

func encodeStorageNativePayload(t *testing.T, revision int, input proto.Input) []byte {
	t.Helper()
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ClientCodeData))
	buf.PutString("")
	block := proto.Block{Rows: input[0].Data.Rows(), Columns: len(input)}
	if err := block.EncodeBlock(&buf, revision, input); err != nil {
		t.Fatalf("encode block: %v", err)
	}
	return buf.Buf
}

func storageColStr(values ...string) *proto.ColStr {
	col := new(proto.ColStr)
	for _, value := range values {
		col.Append(value)
	}
	return col
}

type decodedStorageRows struct {
	columns  []lthash.Column
	rowIDs   [][]byte
	rowCount int
}

func decodeStorageNativeRows(t *testing.T, revision int, payload []byte) decodedStorageRows {
	t.Helper()
	r := proto.NewReader(bytes.NewReader(payload))
	code, err := r.UVarInt()
	if err != nil {
		t.Fatalf("packet code: %v", err)
	}
	if code != uint64(chproto.ClientDataCode) {
		t.Fatalf("packet code = %d, want ClientData", code)
	}
	if _, err := r.Str(); err != nil {
		t.Fatalf("block name: %v", err)
	}
	var (
		results proto.Results
		block   proto.Block
	)
	if err := block.DecodeBlock(r, revision, results.Auto()); err != nil {
		t.Fatalf("DecodeBlock: %v", err)
	}
	out := decodedStorageRows{rowCount: block.Rows}
	for _, rc := range results {
		col := rc.Data
		typeName := ""
		if auto, ok := col.(*proto.ColAuto); ok {
			typeName = string(auto.DataType)
			col = auto.Data
		} else if col != nil {
			typeName = string(col.Type())
		}
		out.columns = append(out.columns, lthash.Column{Name: rc.Name, Type: typeName})
		if rc.Name == "_hg_row_id" {
			switch fixed := col.(type) {
			case *proto.ColFixedStr:
				for i := 0; i < block.Rows; i++ {
					out.rowIDs = append(out.rowIDs, append([]byte(nil), fixed.Row(i)...))
				}
			case *proto.ColFixedStr32:
				for i := 0; i < block.Rows; i++ {
					row := fixed.Row(i)
					out.rowIDs = append(out.rowIDs, append([]byte(nil), row[:]...))
				}
			default:
				t.Fatalf("_hg_row_id column type = %T, want FixedString(32)", col)
			}
		}
	}
	return out
}

// TestPluginSealsSafeDatabaseDDL proves HG-P0-05: every user write/DDL that
// targets hg_safe is rejected at ingress, while a SELECT on hg_safe and an
// INSERT into the unsafe database are not.
func TestPluginSealsSafeDatabaseDDL(t *testing.T) {
	newPlugin := func() *Plugin {
		return New(Config{
			UnsafeDatabase: "hg_unsafe",
			SafeDatabase:   "hg_safe",
			DA:             &fakeDA{},
			Sequencer:      &fakeSequencer{},
			ReadGate:       &fakeReadGate{decision: core.SafeReadDecision{Active: true}},
		})
	}
	safeTable := sqlmeta.AccessedTable{
		OriginalDatabase: "hg_safe", OriginalTable: "events",
		LogicalDatabase: "hg_safe", PhysicalDatabase: "hg_safe",
	}
	rejected := []string{
		"DROP TABLE hg_safe.events",
		"RENAME TABLE hg_safe.events TO hg_safe.events_old",
		"ALTER TABLE hg_safe.events DETACH PARTITION '202607'",
		"ALTER TABLE hg_safe.events ATTACH PARTITION '202607'",
		"ALTER TABLE hg_safe.events MOVE PARTITION '202607' TO TABLE hg_safe.other",
		"ALTER TABLE hg_safe.events REPLACE PARTITION '202607' FROM hg_safe.other",
		"ALTER TABLE hg_safe.events ADD COLUMN extra String",
		"ALTER TABLE hg_safe.events MODIFY COLUMN label String",
		"ALTER TABLE hg_safe.events DROP COLUMN label",
		"INSERT INTO hg_safe.events VALUES (1)",
	}
	for _, sql := range rejected {
		t.Run("reject/"+sql, func(t *testing.T) {
			p := newPlugin()
			state := chsession.NewSessionState()
			sess := &fakeSession{id: 1, state: state}
			qctx := &plugin.QueryContext{
				Session:        sess,
				OriginalSQL:    sql,
				Query:          &chproto.Query{Body: sql},
				AccessedTables: []sqlmeta.AccessedTable{safeTable},
			}
			err := p.OnQuery(context.Background(), qctx)
			if err == nil || !strings.Contains(err.Error(), "safe database") {
				// TRUNCATE / DROP PARTITION are caught by the older forbidden-write
				// rule with a different message; those are covered elsewhere. Here we
				// assert the seal rejects the broader DDL set.
				t.Fatalf("expected safe-database ingress rejection for %q, got %v", sql, err)
			}
		})
	}

	t.Run("allow/select-on-safe", func(t *testing.T) {
		p := newPlugin()
		state := chsession.NewSessionState()
		sess := &fakeSession{id: 2, state: state}
		sql := "SELECT * FROM hg_safe.events"
		qctx := &plugin.QueryContext{
			Session:        sess,
			OriginalSQL:    sql,
			Query:          &chproto.Query{Body: sql},
			StatementType:  sqlmeta.StatementTypeSelect,
			AccessedTables: []sqlmeta.AccessedTable{safeTable},
		}
		if err := p.OnQuery(context.Background(), qctx); err != nil {
			t.Fatalf("SELECT on safe database must be allowed (read-gated), got %v", err)
		}
	})

	t.Run("allow/insert-into-unsafe", func(t *testing.T) {
		p := newPlugin()
		state := chsession.NewSessionState()
		state.ClientRevision = 54453
		sess := &fakeSession{id: 3, state: state}
		sql := "INSERT INTO tenant_a.events FORMAT Native"
		qctx := &plugin.QueryContext{
			Session:       sess,
			OriginalSQL:   sql,
			RewrittenSQL:  "INSERT INTO hg_unsafe.events FORMAT Native",
			Query:         &chproto.Query{Body: "INSERT INTO hg_unsafe.events FORMAT Native"},
			StatementType: sqlmeta.StatementTypeInsert,
			AccessedTables: []sqlmeta.AccessedTable{{
				OriginalDatabase: "tenant_a", OriginalTable: "events",
				LogicalDatabase: "tenant_a", PhysicalDatabase: "hg_unsafe",
			}},
			TableRewrites: map[string]string{"tenant_a.events": "hg_unsafe.events"},
		}
		if err := p.OnQuery(context.Background(), qctx); err != nil {
			t.Fatalf("INSERT into unsafe database must not be sealed, got %v", err)
		}
	})
}
