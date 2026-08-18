// Package storageintegrity implements the server-side storage-integrity ingress
// admission gate. It owns signature/kind/target validation and exact
// payload-byte capture; unsafe writes and Arbiter/SNode calls belong to later
// stages.
package storageintegrity

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/plugin"
	"github.com/housegate/housegate/pkg/registry"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/schemaregistry"
	"github.com/housegate/housegate/pkg/sqlident"
	"github.com/housegate/housegate/pkg/sqlmeta"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

const (
	DefaultMaxPayloadBytes uint64 = 64 << 20
	DefaultRequestTimeout         = 5 * time.Second
)

type Kind string

const (
	KindInsert Kind = "INSERT"
	KindUpdate Kind = "UPDATE"
	KindDelete Kind = "DELETE"
)

type Config struct {
	Enabled             bool
	AuthValidator       auth.Validator
	Purpose             string
	RequestTimeout      time.Duration
	MaxPayloadBytes     uint64
	AdmissionConsumer   AdmissionConsumer
	PayloadMaterializer sicore.PayloadMaterializer

	// TableSchemas resolves the declared network-state schema for the target
	// table so the ingress can compute its own schema_hash expectation
	// (fail closed when unresolvable). NetworkID is the genesis network id
	// every statement token must carry.
	TableSchemas registry.TableSchemas
	NetworkID    string
}

type AdmissionConsumer interface {
	ConsumeStorageIntegrityAdmission(ctx context.Context, admission Admission) error
}

type Plugin struct {
	enabled             bool
	authValidator       auth.Validator
	purpose             string
	requestTimeout      time.Duration
	maxPayload          uint64
	admissionConsumer   AdmissionConsumer
	payloadMaterializer sicore.PayloadMaterializer
	schemaLoader        *schemaregistry.NetworkStateLoader
	networkID           string

	mu      sync.Mutex
	active  map[int64]*admissionState
	pending map[int64]*admissionState
}

type Admission struct {
	StatementID string
	Kind        Kind
	TableID     string
	SQL         string
	SQLHash     string
	Signer      string
	UserJWS     string
	// AuthToken is the SQL_x_auth_token query JWS (audit only). UserJWS is
	// the SQL_x_statement_token envelope-v2 token that is sequenced.
	AuthToken       string
	EnvelopeVersion uint32
	NetworkID       string
	KeeperShardID   uint32
	SettingsHash    string
	SchemaHash      string
	RowIDProfileID  string
	Payload         CapturedPayload
}

type CapturedPayload struct {
	Bytes    []byte
	Length   uint64
	SHA256   string
	Encoding string
	Revision int
	Complete bool
}

type admissionState struct {
	admission       Admission
	payload         bytes.Buffer
	payloadEncoding string
	revision        int
	statementToken  string
	schemaHash      string
	complete        bool
}

type purposeValidator interface {
	ValidateQueryPurpose(context.Context, auth.QueryMeta, string) (auth.ValidationResult, error)
}

func New(cfg Config) *Plugin {
	purpose := cfg.Purpose
	if purpose == "" {
		purpose = auth.QueryPurpose
	}
	maxPayload := cfg.MaxPayloadBytes
	if maxPayload == 0 {
		maxPayload = DefaultMaxPayloadBytes
	}
	requestTimeout := cfg.RequestTimeout
	if requestTimeout == 0 {
		requestTimeout = DefaultRequestTimeout
	}
	p := &Plugin{
		enabled:             cfg.Enabled,
		authValidator:       cfg.AuthValidator,
		purpose:             purpose,
		requestTimeout:      requestTimeout,
		maxPayload:          maxPayload,
		admissionConsumer:   cfg.AdmissionConsumer,
		payloadMaterializer: cfg.PayloadMaterializer,
		networkID:           cfg.NetworkID,
		active:              map[int64]*admissionState{},
		pending:             map[int64]*admissionState{},
	}
	if cfg.TableSchemas != nil {
		p.schemaLoader = schemaregistry.NewNetworkStateLoader(cfg.TableSchemas, cfg.NetworkID)
	}
	return p
}

func (p *Plugin) OnQuery(ctx context.Context, qctx *plugin.QueryContext) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p == nil || !p.enabled || qctx == nil || qctx.Query == nil || qctx.Session == nil {
		return nil
	}
	forwardSQL := qctx.Query.Body
	signedSQL := qctx.OriginalSQL
	if signedSQL == "" {
		signedSQL = forwardSQL
	}
	kind, storageWrite, err := classifyStorageIntegrityKind(qctx.StatementType, forwardSQL)
	if err != nil {
		return err
	}
	if !storageWrite {
		return nil
	}
	if kind != KindInsert {
		return fmt.Errorf("storage_integrity insert-only ingress rejects %s; mutation integrity protocol is not available", kind)
	}
	if kind == KindInsert && qctx.Query.Compression == proto.CompressionEnabled {
		return fmt.Errorf("storage_integrity rejects compressed payloads; retry INSERT with ClickHouse query compression disabled")
	}
	if err := rejectInlineUserSettings(signedSQL); err != nil {
		return err
	}
	if forwardSQL != signedSQL {
		if err := rejectInlineUserSettings(forwardSQL); err != nil {
			return fmt.Errorf("storage_integrity rewritten SQL: %w", err)
		}
	}
	stmtID, err := statementID(qctx)
	if err != nil {
		return err
	}
	if fn, ok := containsUnmaterializedNondeterminism(signedSQL); ok {
		return fmt.Errorf("storage_integrity rejects unmaterialized nondeterministic function %s", fn)
	}
	if forwardSQL != signedSQL {
		if fn, ok := containsUnmaterializedNondeterminism(forwardSQL); ok {
			return fmt.Errorf("storage_integrity rejects rewritten nondeterministic function %s", fn)
		}
	}
	payloadEncoding, err := requirePayloadLocalInsert(signedSQL)
	if err != nil {
		return err
	}
	if payloadEncoding == sicore.EncodingCSVWithNames && p.payloadMaterializer == nil {
		return fmt.Errorf("storage_integrity FORMAT CSVWithNames requires a payload materializer")
	}
	if forwardSQL != signedSQL {
		forwardEncoding, err := requirePayloadLocalInsert(forwardSQL)
		if err != nil {
			return err
		}
		if forwardEncoding != payloadEncoding {
			return fmt.Errorf("storage_integrity rewritten INSERT payload encoding mismatch: signed %s forwarded %s", payloadEncoding, forwardEncoding)
		}
	}
	target, err := resolveTargetTable(qctx, signedSQL)
	if err != nil {
		return err
	}
	tableID := target.id
	userJWS, err := queryAuthToken(qctx)
	if err != nil {
		return err
	}
	signer, err := p.authenticate(ctx, qctx, signedSQL)
	if err != nil {
		return err
	}
	if err := requireStatementIDSigner(stmtID, signer); err != nil {
		return err
	}
	if p.schemaLoader == nil || strings.TrimSpace(p.networkID) == "" {
		return errors.New("storage_integrity ingress requires a network-state TableSchemas source and network_id to verify envelope v2 statements")
	}
	statementToken, err := statementTokenFromSettings(querySettings(qctx))
	if err != nil {
		return err
	}
	if err := sicore.RejectUserSettings(settingKeys(qctx)); err != nil {
		return err
	}
	schemaHash, err := p.resolveSchemaHash(ctx, target)
	if err != nil {
		return err
	}
	state := &admissionState{
		admission: Admission{
			StatementID: stmtID,
			Kind:        kind,
			TableID:     tableID,
			SQL:         signedSQL,
			SQLHash:     replay.DigestString(signedSQL),
			Signer:      signer,
			UserJWS:     statementToken,
			AuthToken:   userJWS,
		},
		payloadEncoding: payloadEncoding,
		statementToken:  statementToken,
		schemaHash:      schemaHash,
	}
	if qctx.Session.State() != nil {
		state.revision = qctx.Session.State().ClientRevision
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	id := qctx.Session.ID()
	if existing := p.active[id]; existing != nil {
		return fmt.Errorf("storage_integrity pending admission %s for session %d has not completed", existing.admission.StatementID, id)
	}
	if existing := p.pending[id]; existing != nil {
		return fmt.Errorf("storage_integrity pending admission %s for session %d has not been consumed", existing.admission.StatementID, id)
	}
	p.active[id] = state
	if p.admissionConsumer != nil {
		// A configured consumer means storage-integrity owns this INSERT's
		// write through the staged path. Relay may still forward the Query so
		// ClickHouse can return the Native sample block, but it must capture and
		// withhold non-empty client Data packets; forwarding those rows to
		// ordinary upstream would double-write beside the staged unsafe write.
		qctx.SuppressUpstreamExecution = true
	}
	return nil
}

func (p *Plugin) OnClientDataStrict(ctx context.Context, qctx *plugin.QueryContext, raw []byte) error {
	if p == nil || !p.enabled || qctx == nil || qctx.Session == nil || len(raw) == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		p.abortQuery(qctx)
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	id := qctx.Session.ID()
	state := p.active[id]
	if state == nil || state.admission.Kind != KindInsert {
		return nil
	}
	nextLen := uint64(state.payload.Len()) + uint64(len(raw))
	if p.maxPayload > 0 && nextLen > p.maxPayload {
		delete(p.active, id)
		return fmt.Errorf("storage_integrity payload exceeds max_payload_bytes (%d > %d)", nextLen, p.maxPayload)
	}
	_, _ = state.payload.Write(raw)
	return nil
}

func (p *Plugin) ClientDataReadLimit(qctx *plugin.QueryContext) (uint64, bool) {
	if p == nil || !p.enabled || qctx == nil || qctx.Session == nil {
		return 0, false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	state := p.active[qctx.Session.ID()]
	if state == nil || state.admission.Kind != KindInsert {
		return 0, false
	}
	used := uint64(state.payload.Len())
	if used >= p.maxPayload {
		return 0, true
	}
	return p.maxPayload - used, true
}

func (p *Plugin) OnClientData(context.Context, *plugin.QueryContext, []byte) error {
	return nil
}

func (p *Plugin) OnQueryComplete(_ context.Context, sess chsession.Session) {
	// Admission readiness is driven by OnQueryInputComplete. The upstream
	// completion hook is best-effort and must not publish correctness state.
}

// OnQueryInputCompleteStrict is the correctness-critical end-of-input boundary.
// When an admission consumer is configured, OnQuery marks the query with
// SuppressUpstreamExecution, so Relay preserves Native sample-block negotiation
// but withholds non-empty client Data from ordinary upstream. The completed
// admission is driven through the consumer HERE; success lets Relay forward the
// terminating empty Data block so upstream finishes with zero rows, while
// rejection/unavailability returns an error before that commit boundary.
func (p *Plugin) OnQueryInputCompleteStrict(ctx context.Context, qctx *plugin.QueryContext) error {
	if p == nil || !p.enabled || qctx == nil || qctx.Session == nil || qctx.Query == nil {
		return nil
	}
	if p.admissionConsumer == nil {
		// No consumer: nothing correctness-critical to enforce here; the
		// post-splice OnQueryInputComplete records the admission as pending.
		return nil
	}
	p.mu.Lock()
	id := qctx.Session.ID()
	state := p.active[id]
	if state == nil || state.admission.StatementID != strings.TrimSpace(qctx.Query.ID) {
		p.mu.Unlock()
		return nil
	}
	state.complete = true
	delete(p.active, id)
	p.mu.Unlock()

	admission, err := p.admissionFromState(ctx, state)
	if err != nil {
		return fmt.Errorf("storage_integrity admission incomplete for %s: %w", state.admission.StatementID, err)
	}
	if err := p.admissionConsumer.ConsumeStorageIntegrityAdmission(ctx, admission); err != nil {
		return fmt.Errorf("storage_integrity admission rejected for %s: %w", state.admission.StatementID, err)
	}
	return nil
}

func (p *Plugin) OnQueryInputComplete(ctx context.Context, qctx *plugin.QueryContext) {
	if p == nil || !p.enabled || qctx == nil || qctx.Session == nil || qctx.Query == nil {
		return
	}
	// When a consumer is configured, OnQueryInputCompleteStrict already consumed
	// (and cleared) the admission before Relay forwarded the terminating block,
	// so a consumed admission is no longer in p.active. This completion observer
	// only handles the no-consumer case: record the completed admission as
	// pending for a later ConsumeAdmission caller.
	if p.admissionConsumer != nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	id := qctx.Session.ID()
	state := p.active[id]
	if state == nil || state.admission.StatementID != strings.TrimSpace(qctx.Query.ID) {
		return
	}
	state.complete = true
	delete(p.active, id)
	if p.pending[id] == nil {
		p.pending[id] = state
	}
}

func (p *Plugin) OnQueryAbort(_ context.Context, qctx *plugin.QueryContext) {
	if p == nil || !p.enabled || qctx == nil || qctx.Session == nil || qctx.Query == nil {
		return
	}
	p.abortQuery(qctx)
}

func (p *Plugin) OnClose(sess chsession.Session) {
	if p == nil || sess == nil {
		return
	}
	p.mu.Lock()
	delete(p.active, sess.ID())
	delete(p.pending, sess.ID())
	p.mu.Unlock()
}

func (p *Plugin) abortQuery(qctx *plugin.QueryContext) {
	sessionID := qctx.Session.ID()
	statementID := strings.TrimSpace(qctx.Query.ID)
	p.mu.Lock()
	if state := p.active[sessionID]; state != nil && state.admission.StatementID == statementID {
		delete(p.active, sessionID)
	}
	if state := p.pending[sessionID]; state != nil && state.admission.StatementID == statementID {
		delete(p.pending, sessionID)
	}
	p.mu.Unlock()
}

func (p *Plugin) ConsumeAdmission(sessionID int64) (Admission, error) {
	if p == nil {
		return Admission{}, errors.New("storage_integrity admission plugin is nil")
	}
	p.mu.Lock()
	state := p.pending[sessionID]
	if state == nil {
		p.mu.Unlock()
		return Admission{}, fmt.Errorf("storage_integrity admission for session %d not found", sessionID)
	}
	delete(p.pending, sessionID)
	p.mu.Unlock()

	return p.admissionFromState(context.Background(), state)
}

func (p *Plugin) admissionFromState(ctx context.Context, state *admissionState) (Admission, error) {
	admission := state.admission
	if admission.Kind == KindInsert && state.payload.Len() == 0 {
		return Admission{}, fmt.Errorf("storage_integrity incomplete payload capture for statement %s", admission.StatementID)
	}
	payload := append([]byte(nil), state.payload.Bytes()...)
	payloadEncoding := state.payloadEncoding
	if payloadEncoding == sicore.EncodingCSVWithNames {
		if p == nil || p.payloadMaterializer == nil {
			return Admission{}, fmt.Errorf("storage_integrity payload materializer is required for %s", payloadEncoding)
		}
		out, err := p.payloadMaterializer.MaterializePayload(ctx, sicore.PayloadMaterializationInput{
			StatementID:     admission.StatementID,
			TableID:         admission.TableID,
			SQL:             admission.SQL,
			PayloadEncoding: payloadEncoding,
			NativeWire:      payload,
			Revision:        state.revision,
		})
		if err != nil {
			return Admission{}, err
		}
		if out.Encoding != payloadEncoding {
			return Admission{}, fmt.Errorf("storage_integrity payload materializer returned encoding %q, want %q", out.Encoding, payloadEncoding)
		}
		if len(out.Payload) == 0 {
			return Admission{}, fmt.Errorf("storage_integrity payload materializer returned empty payload")
		}
		payload = append([]byte(nil), out.Payload...)
	}
	if state.revision <= 0 || uint64(state.revision) > uint64(^uint32(0)) {
		return Admission{}, fmt.Errorf("storage_integrity admission %s has invalid client protocol revision %d", admission.StatementID, state.revision)
	}
	want := auth.JWSStatementPayloadV2{
		NetworkID:      p.networkID,
		KeeperShardID:  0,
		StatementID:    admission.StatementID,
		SQLHash:        admission.SQLHash,
		SettingsHash:   sicore.EmptySettingsHash,
		SchemaHash:     state.schemaHash,
		PayloadHash:    replay.DigestBytes(payload),
		PayloadLength:  uint64(len(payload)),
		PayloadFormat:  sicore.PayloadEncodingClickHouseNativeData,
		ClientRevision: uint32(state.revision),
		TargetTableID:  admission.TableID,
		RowIDProfileID: payloadexec.RowIDProfileID,
	}
	validator, ok := p.authValidator.(auth.StatementValidatorV2)
	if !ok {
		return Admission{}, errors.New("storage_integrity auth validator does not support envelope v2 statement tokens")
	}
	recovered, err := validator.ValidateStatementV2(state.statementToken, want)
	if err != nil {
		return Admission{}, fmt.Errorf("storage_integrity statement token rejected for %s: %w", admission.StatementID, err)
	}
	if !strings.EqualFold(recovered, admission.Signer) {
		return Admission{}, fmt.Errorf("storage_integrity statement token signer %s does not match query signer %s", recovered, admission.Signer)
	}
	if err := requireStatementIDSigner(admission.StatementID, recovered); err != nil {
		return Admission{}, err
	}
	admission.EnvelopeVersion = sicore.EnvelopeVersionV2
	admission.NetworkID = p.networkID
	admission.KeeperShardID = 0
	admission.SettingsHash = sicore.EmptySettingsHash
	admission.SchemaHash = state.schemaHash
	admission.RowIDProfileID = payloadexec.RowIDProfileID
	if p != nil && p.maxPayload > 0 && uint64(len(payload)) > p.maxPayload {
		return Admission{}, fmt.Errorf("storage_integrity payload exceeds max_payload_bytes after materialization (%d > %d)", len(payload), p.maxPayload)
	}
	sum := sha256.Sum256(payload)
	admission.Payload = CapturedPayload{
		Bytes:    payload,
		Length:   uint64(len(payload)),
		SHA256:   "sha256:" + hex.EncodeToString(sum[:]),
		Encoding: payloadEncoding,
		Revision: state.revision,
		Complete: state.complete,
	}
	if !admission.Payload.Complete {
		return Admission{}, fmt.Errorf("storage_integrity admission %s is not complete", admission.StatementID)
	}
	return admission, nil
}

func (p *Plugin) authenticate(ctx context.Context, qctx *plugin.QueryContext, sql string) (string, error) {
	if p.authValidator == nil {
		return "", errors.New("storage_integrity auth validator is required")
	}
	if p.requestTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.requestTimeout)
		defer cancel()
	}
	settings := querySettings(qctx)
	if _, err := authTokenFromSettings(settings); err != nil {
		return "", err
	}
	meta := auth.QueryMeta{
		ConnID:   qctx.Session.ID(),
		SQL:      sql,
		Settings: settings,
	}
	var (
		res auth.ValidationResult
		err error
	)
	if p.purpose != "" {
		pv, ok := p.authValidator.(purposeValidator)
		if !ok {
			return "", errors.New("storage_integrity auth validator does not support purpose validation")
		}
		res, err = pv.ValidateQueryPurpose(ctx, meta, p.purpose)
	} else {
		res, err = p.authValidator.ValidateQuery(ctx, meta)
	}
	if err != nil {
		return "", fmt.Errorf("storage_integrity JWS validation: %w", err)
	}
	if res.Address != "" {
		return strings.ToLower(res.Address), nil
	}
	return "", errors.New("storage_integrity authenticated signer is required")
}

func queryAuthToken(qctx *plugin.QueryContext) (string, error) {
	return authTokenFromSettings(querySettings(qctx))
}

func authTokenFromSettings(settings map[string]string) (string, error) {
	token := strings.Trim(strings.TrimSpace(settings[auth.AuthTokenSettingKey]), "\"'")
	if token == "" {
		return "", fmt.Errorf("storage_integrity requires %s", auth.AuthTokenSettingKey)
	}
	return token, nil
}

func statementTokenFromSettings(settings map[string]string) (string, error) {
	token := strings.Trim(strings.TrimSpace(settings[auth.StatementTokenSettingKey]), "\"'")
	if token == "" {
		return "", fmt.Errorf("storage_integrity requires %s (envelope v2 statement token)", auth.StatementTokenSettingKey)
	}
	return token, nil
}

func settingKeys(qctx *plugin.QueryContext) []string {
	if qctx == nil || qctx.Query == nil {
		return nil
	}
	keys := make([]string, 0, len(qctx.Query.Settings))
	for _, setting := range qctx.Query.Settings {
		keys = append(keys, setting.Key)
	}
	return keys
}

func querySettings(qctx *plugin.QueryContext) map[string]string {
	settings := map[string]string{}
	if qctx == nil || qctx.Query == nil {
		return settings
	}
	for _, setting := range qctx.Query.Settings {
		settings[setting.Key] = setting.Value
	}
	return settings
}

func statementID(qctx *plugin.QueryContext) (string, error) {
	if qctx != nil && qctx.Query != nil {
		if id := strings.TrimSpace(qctx.Query.ID); id != "" {
			if _, err := parseFlatStatementID(id); err != nil {
				return "", err
			}
			return id, nil
		}
	}
	return "", errors.New("storage_integrity query id is required")
}

type flatStatementID struct {
	ClientAccount string
	ClientSeq     uint64
	ClientNonce   string
}

func parseFlatStatementID(id string) (flatStatementID, error) {
	parts := strings.Split(id, ":")
	if len(parts) != 3 {
		return flatStatementID{}, fmt.Errorf("storage_integrity requires structured statement id <client_account>:<client_seq>:<client_nonce>")
	}
	account, seqText, nonce := parts[0], parts[1], parts[2]
	if account == "" || seqText == "" || nonce == "" {
		return flatStatementID{}, fmt.Errorf("storage_integrity requires structured statement id <client_account>:<client_seq>:<client_nonce>")
	}
	if account != strings.ToLower(account) || !strings.HasPrefix(account, "0x") || !isLowerHex(account[2:]) {
		return flatStatementID{}, fmt.Errorf("storage_integrity requires structured statement id with lowercase 0x client_account")
	}
	if len(seqText) > 1 && seqText[0] == '0' {
		return flatStatementID{}, fmt.Errorf("storage_integrity requires canonical decimal client_seq")
	}
	if !isDecimalDigits(seqText) {
		return flatStatementID{}, fmt.Errorf("storage_integrity requires canonical decimal client_seq")
	}
	seq, err := strconv.ParseUint(seqText, 10, 64)
	if err != nil || seq == 0 {
		return flatStatementID{}, fmt.Errorf("storage_integrity requires non-zero decimal client_seq")
	}
	if strings.TrimSpace(nonce) != nonce {
		return flatStatementID{}, fmt.Errorf("storage_integrity requires structured statement id with non-empty client_nonce")
	}
	return flatStatementID{ClientAccount: account, ClientSeq: seq, ClientNonce: nonce}, nil
}

func requireStatementIDSigner(id, signer string) error {
	stmt, err := parseFlatStatementID(id)
	if err != nil {
		return err
	}
	if stmt.ClientAccount != strings.ToLower(signer) {
		return fmt.Errorf("storage_integrity statement id client_account %s does not match authenticated signer %s", stmt.ClientAccount, strings.ToLower(signer))
	}
	return nil
}

func isLowerHex(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if !((s[i] >= '0' && s[i] <= '9') || (s[i] >= 'a' && s[i] <= 'f')) {
			return false
		}
	}
	return true
}

func isDecimalDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func classifyStorageIntegrityKind(typ sqlmeta.StatementType, sql string) (Kind, bool, error) {
	textKind, textWrite, textUnsupported := storageIntegrityKindFromSQL(sql)
	switch typ {
	case sqlmeta.StatementTypeInsert:
		if !textWrite || textKind != KindInsert {
			return "", false, fmt.Errorf("storage_integrity statement type mismatch: %s classified as %s", firstKeyword(sql), typ)
		}
		return KindInsert, true, nil
	case sqlmeta.StatementTypeUpdate:
		if !textWrite || textKind != KindUpdate {
			return "", false, fmt.Errorf("storage_integrity statement type mismatch: %s classified as %s", firstKeyword(sql), typ)
		}
		return KindUpdate, true, nil
	case sqlmeta.StatementTypeDelete:
		if !textWrite || textKind != KindDelete {
			return "", false, fmt.Errorf("storage_integrity statement type mismatch: %s classified as %s", firstKeyword(sql), typ)
		}
		return KindDelete, true, nil
	case sqlmeta.StatementTypeAlterTable:
		if textKind != KindUpdate && textKind != KindDelete {
			return "", false, fmt.Errorf("unsupported storage-integrity statement kind %s", firstKeyword(sql))
		}
		return textKind, true, nil
	case sqlmeta.StatementTypeSelect, sqlmeta.StatementTypeUse, sqlmeta.StatementTypeShowTables,
		sqlmeta.StatementTypeShowCreateTable, sqlmeta.StatementTypeExistsTable,
		sqlmeta.StatementTypeShowDatabases, sqlmeta.StatementTypeUnspecified:
		if textWrite || textUnsupported {
			return "", false, fmt.Errorf("storage_integrity statement type mismatch: %s classified as %s", firstKeyword(sql), typ)
		}
		return "", false, nil
	default:
		return "", false, fmt.Errorf("unsupported storage-integrity statement kind %s", typ)
	}
}

func storageIntegrityKindFromSQL(sql string) (Kind, bool, bool) {
	trimmed := strings.TrimSpace(sql)
	if _, err := sicore.ParseInsertTarget(sql); err == nil || !errors.Is(err, sicore.ErrNotInsert) {
		return KindInsert, true, false
	}
	switch {
	case updateTargetPattern.MatchString(trimmed), alterUpdateTargetPattern.MatchString(trimmed):
		return KindUpdate, true, false
	case deleteTargetPattern.MatchString(trimmed), alterDeleteTargetPattern.MatchString(trimmed):
		return KindDelete, true, false
	case readLikePattern.MatchString(trimmed):
		return "", false, false
	case unsupportedWritePattern.MatchString(trimmed):
		return "", false, true
	default:
		return "", false, false
	}
}

func rejectInlineUserSettings(sql string) error {
	keys, err := sicore.InlineInsertSettingKeys(sql)
	if err != nil {
		return fmt.Errorf("storage_integrity inspect inline SETTINGS: %w", err)
	}
	return sicore.RejectUserSettings(keys)
}

func requirePayloadLocalInsert(sql string) (string, error) {
	encoding, err := sicore.InsertPayloadEncoding(sql)
	if err != nil {
		return "", fmt.Errorf("storage_integrity %w", err)
	}
	return encoding, nil
}

type resolvedTableTarget struct {
	id       string
	database string
	table    string
}

func targetTableID(qctx *plugin.QueryContext, sql string) (string, error) {
	target, err := resolveTargetTable(qctx, sql)
	return target.id, err
}

func resolveTargetTable(qctx *plugin.QueryContext, sql string) (resolvedTableTarget, error) {
	sessionDB := ""
	if qctx.Session != nil && qctx.Session.State() != nil {
		sessionDB = qctx.Session.State().LogicalDatabaseName()
	}
	target, err := sicore.ParseInsertTarget(sql)
	if err != nil {
		return resolvedTableTarget{}, fmt.Errorf("storage_integrity target table: %w", err)
	}
	if target.Database == "" && sessionDB != "" {
		target.Database = sessionDB
	}
	canonicalTable := sqlident.NormalizePath(sqlident.Quote(target.Table))
	resolvedSQLTarget := canonicalTable
	if target.Database != "" {
		resolvedSQLTarget = target.CanonicalID()
	}
	matches := make(map[string]resolvedTableTarget)
	for _, table := range qctx.AccessedTables {
		if strings.TrimSpace(table.OriginalTable) == "" {
			continue
		}
		metadataTable, err := normalizeTablePath(sqlident.Quote(table.OriginalTable))
		if err != nil {
			return resolvedTableTarget{}, err
		}
		if metadataTable != canonicalTable {
			continue
		}
		referenceDB := firstNonEmpty(table.OriginalDatabase, table.LogicalDatabase, sessionDB)
		metadataTarget, err := normalizeStructuredTablePath(referenceDB, table.OriginalTable)
		if err != nil {
			return resolvedTableTarget{}, err
		}
		if target.Database != "" {
			if metadataTarget != resolvedSQLTarget {
				continue
			}
		}
		logicalDB := firstNonEmpty(table.LogicalDatabase, table.OriginalDatabase, sessionDB)
		logicalID, err := normalizeStructuredTablePath(logicalDB, table.OriginalTable)
		if err != nil {
			return resolvedTableTarget{}, err
		}
		if logicalID == "" {
			continue
		}
		matches[logicalID] = resolvedTableTarget{id: logicalID, database: logicalDB, table: table.OriginalTable}
	}
	if len(matches) == 1 {
		for _, resolved := range matches {
			return resolved, nil
		}
	}
	if len(matches) > 1 {
		return resolvedTableTarget{}, fmt.Errorf("storage_integrity ambiguous target table %s in rewriter metadata", resolvedSQLTarget)
	}
	if len(qctx.AccessedTables) > 0 {
		return resolvedTableTarget{}, fmt.Errorf("storage_integrity target table mismatch: SQL target %s does not match rewriter metadata", resolvedSQLTarget)
	}
	return resolvedTableTarget{id: resolvedSQLTarget, database: target.Database, table: target.Table}, nil
}

// resolveSchemaHash computes this ingress's own expectation from the declared,
// hash-verified network-state schema. The structured target is carried from SQL
// parsing/rewriter metadata so quoted dots are never split from a canonical id.
func (p *Plugin) resolveSchemaHash(ctx context.Context, target resolvedTableTarget) (string, error) {
	if target.id == "" || target.database == "" || target.table == "" {
		return "", fmt.Errorf("storage_integrity target table id %q must be db.table for schema resolution", target.id)
	}
	schemas, err := p.schemaLoader.Load(ctx, []schemaregistry.TableRef{{
		TableID:         target.id,
		Database:        target.database,
		Table:           target.table,
		LogicalDatabase: target.database,
		LogicalTable:    target.table,
	}})
	if err != nil {
		return "", fmt.Errorf("storage_integrity cannot resolve declared schema for %s: %w", target.id, err)
	}
	if len(schemas) != 1 {
		return "", fmt.Errorf("storage_integrity schema source returned %d schemas for %s, want 1", len(schemas), target.id)
	}
	return payloadexec.TableSchemaHash(p.networkID, schemas[0]), nil
}

func normalizeStructuredTablePath(db, table string) (string, error) {
	if db == "" {
		return normalizeTablePath(sqlident.Quote(table))
	}
	return normalizeTablePath(sqlident.Quote(db) + "." + sqlident.Quote(table))
}

func normalizeTablePath(path string) (string, error) {
	normalized := sqlident.NormalizePath(path)
	if normalized == "" {
		return "", fmt.Errorf("storage_integrity target table path %q is invalid", path)
	}
	return normalized, nil
}

func containsUnmaterializedNondeterminism(sql string) (string, bool) {
	stripped := stripSQLLiteralsAndComments(sql)
	for _, loc := range functionPattern.FindAllStringSubmatchIndex(stripped, -1) {
		if len(loc) < 4 {
			continue
		}
		name := stripped[loc[2]:loc[3]]
		if isKnownNondeterministicName(strings.ToLower(name)) {
			return name, true
		}
	}
	for _, loc := range identifierTokenPattern.FindAllStringSubmatchIndex(stripped, -1) {
		if len(loc) < 4 {
			continue
		}
		name := stripped[loc[2]:loc[3]]
		if isKnownNondeterministicName(strings.ToLower(name)) {
			return name, true
		}
	}
	return "", false
}

func isKnownNondeterministicName(name string) bool {
	switch name {
	case "any",
		"anylast",
		"blocknumber",
		"blocksize",
		"curdate",
		"current_date",
		"current_timestamp",
		"datetimetouuidv7",
		"fuzzbits",
		"fuzzquery",
		"generaterandomstructure",
		"generateserialid",
		"generatesnowflakeid",
		"generateuuidv4",
		"generateuuidv7",
		"localtime",
		"localtimestamp",
		"now",
		"now64",
		"nowinblock",
		"nowinblock64",
		"obfuscatequery",
		"quantile",
		"quantiles",
		"rand",
		"rand32",
		"rand64",
		"randbernoulli",
		"randbinomial",
		"randcanonical",
		"randchisquared",
		"randconstant",
		"randexponential",
		"randfisherf",
		"randlognormal",
		"randnegativebinomial",
		"randnormal",
		"randpoisson",
		"randstudentt",
		"randuniform",
		"random",
		"randomfixedstring",
		"randomprintableascii",
		"randomstring",
		"randomstringutf8",
		"rownumberinallblocks",
		"rownumberinblock",
		"runningaccumulate",
		"runningconcurrency",
		"runningdifference",
		"runningdifferencestartingwithfirstvalue",
		"today",
		"utc_timestamp",
		"utctimestamp",
		"uuidv4",
		"yesterday":
		return true
	default:
		return false
	}
}

func stripSQLLiteralsAndComments(sql string) string {
	out := []byte(sql)
	for i := 0; i < len(out); {
		switch {
		case out[i] == '\'':
			i = blankQuoted(out, i, '\'')
		case out[i] == '"' || out[i] == '`':
			i = blankQuoted(out, i, out[i])
		case out[i] == '-' && i+1 < len(out) && out[i+1] == '-':
			for i < len(out) && out[i] != '\n' {
				out[i] = ' '
				i++
			}
		case out[i] == '/' && i+1 < len(out) && out[i+1] == '*':
			out[i], out[i+1] = ' ', ' '
			i += 2
			for i+1 < len(out) {
				if out[i] == '*' && out[i+1] == '/' {
					out[i], out[i+1] = ' ', ' '
					i += 2
					break
				}
				out[i] = ' '
				i++
			}
		default:
			i++
		}
	}
	return string(out)
}

func blankQuoted(out []byte, i int, quote byte) int {
	out[i] = ' '
	i++
	for i < len(out) {
		if quote == '\'' && out[i] == '\\' && i+1 < len(out) {
			out[i], out[i+1] = ' ', ' '
			i += 2
			continue
		}
		if out[i] == quote {
			out[i] = ' '
			i++
			if i < len(out) && out[i] == quote {
				out[i] = ' '
				i++
				continue
			}
			return i
		}
		out[i] = ' '
		i++
	}
	return i
}

func firstKeyword(sql string) string {
	fields := strings.Fields(sql)
	if len(fields) == 0 {
		return "UNKNOWN"
	}
	return strings.ToUpper(fields[0])
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

const identifierPath = "(?:`(?:``|[^`])+`|[A-Za-z_][A-Za-z0-9_]*)(?:\\s*\\.\\s*(?:`(?:``|[^`])+`|[A-Za-z_][A-Za-z0-9_]*))*"

var (
	updateTargetPattern      = regexp.MustCompile(`(?is)^\s*UPDATE\s+(` + identifierPath + `)\s+SET\b`)
	deleteTargetPattern      = regexp.MustCompile(`(?is)^\s*DELETE\s+FROM\s+(` + identifierPath + `)(?:\s|;|$)`)
	alterUpdateTargetPattern = regexp.MustCompile(`(?is)^\s*ALTER\s+TABLE\s+(` + identifierPath + `)\s+UPDATE\b`)
	alterDeleteTargetPattern = regexp.MustCompile(`(?is)^\s*ALTER\s+TABLE\s+(` + identifierPath + `)\s+DELETE\b`)
	readLikePattern          = regexp.MustCompile(`(?is)^\s*(SELECT|SHOW|EXISTS|DESCRIBE|DESC|USE)\b`)
	unsupportedWritePattern  = regexp.MustCompile(`(?is)^\s*(CREATE|DROP|ALTER|RENAME|TRUNCATE|GRANT|REVOKE|ATTACH|DETACH|OPTIMIZE)\b`)
	functionPattern          = regexp.MustCompile(`(?is)([A-Za-z_][A-Za-z0-9_]*)\s*\(`)
	identifierTokenPattern   = regexp.MustCompile(`(?is)\b([A-Za-z_][A-Za-z0-9_]*)\b`)
)

func (p *Plugin) RunOnPeerTrust() bool { return false }

func (p *Plugin) RunOnForward() bool { return false }

func (p *Plugin) RejectUndecodableQuery() bool { return p != nil && p.enabled }

var (
	_ plugin.QueryPlugin              = (*Plugin)(nil)
	_ plugin.StrictQueryDecodePlugin  = (*Plugin)(nil)
	_ plugin.StrictDataPlugin         = (*Plugin)(nil)
	_ plugin.StrictDataLimitPlugin    = (*Plugin)(nil)
	_ plugin.DataPlugin               = (*Plugin)(nil)
	_ plugin.QueryInputCompletePlugin = (*Plugin)(nil)
	_ plugin.QueryAbortPlugin         = (*Plugin)(nil)
	_ plugin.QueryCompletePlugin      = (*Plugin)(nil)
	_ plugin.ClosePlugin              = (*Plugin)(nil)
	_ plugin.PeerTrustAware           = (*Plugin)(nil)
	_ plugin.ForwardAware             = (*Plugin)(nil)
)
