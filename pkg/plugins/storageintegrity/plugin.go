// Package storageintegrity implements the server-side storage-integrity ingress
// admission gate. It owns signature/kind/target validation and exact Native
// Data capture; unsafe writes and Arbiter/SNode calls belong to later stages.
package storageintegrity

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"

	"housegate/housegate/pkg/auth"
	"housegate/housegate/pkg/chsession"
	"housegate/housegate/pkg/plugin"
	"housegate/housegate/pkg/sqlident"
	"housegate/housegate/pkg/sqlmeta"
)

const DefaultMaxPayloadBytes uint64 = 64 << 20

type Kind string

const (
	KindInsert Kind = "INSERT"
	KindUpdate Kind = "UPDATE"
	KindDelete Kind = "DELETE"
)

type Config struct {
	Enabled         bool
	AuthValidator   auth.Validator
	Purpose         string
	MaxPayloadBytes uint64
}

type Plugin struct {
	enabled       bool
	authValidator auth.Validator
	purpose       string
	maxPayload    uint64

	mu      sync.Mutex
	active  map[int64]*admissionState
	pending map[int64]*admissionState
}

type Admission struct {
	StatementID string
	Kind        Kind
	TableID     string
	SQL         string
	Signer      string
	Payload     CapturedPayload
}

type CapturedPayload struct {
	Bytes    []byte
	Length   uint64
	SHA256   string
	Revision int
	Complete bool
}

type admissionState struct {
	admission Admission
	payload   bytes.Buffer
	revision  int
	complete  bool
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
	return &Plugin{
		enabled:       cfg.Enabled,
		authValidator: cfg.AuthValidator,
		purpose:       purpose,
		maxPayload:    maxPayload,
		active:        map[int64]*admissionState{},
		pending:       map[int64]*admissionState{},
	}
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
	tableID, err := targetTableID(qctx, signedSQL)
	if err != nil {
		return err
	}
	signer, err := p.authenticate(ctx, qctx, signedSQL)
	if err != nil {
		return err
	}
	state := &admissionState{admission: Admission{
		StatementID: stmtID,
		Kind:        kind,
		TableID:     tableID,
		SQL:         signedSQL,
		Signer:      signer,
	}}
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
		return fmt.Errorf("storage_integrity native payload exceeds max_payload_bytes (%d > %d)", nextLen, p.maxPayload)
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

func (p *Plugin) OnQueryInputComplete(_ context.Context, qctx *plugin.QueryContext) {
	if p == nil || !p.enabled || qctx == nil || qctx.Session == nil || qctx.Query == nil {
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

	admission := state.admission
	if admission.Kind == KindInsert && state.payload.Len() == 0 {
		return Admission{}, fmt.Errorf("storage_integrity incomplete native payload capture for statement %s", admission.StatementID)
	}
	payload := append([]byte(nil), state.payload.Bytes()...)
	sum := sha256.Sum256(payload)
	admission.Payload = CapturedPayload{
		Bytes:    payload,
		Length:   uint64(len(payload)),
		SHA256:   "sha256:" + hex.EncodeToString(sum[:]),
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
	settings := querySettings(qctx)
	if token := strings.Trim(strings.TrimSpace(settings[auth.AuthTokenSettingKey]), "\"'"); token == "" {
		return "", fmt.Errorf("storage_integrity requires %s", auth.AuthTokenSettingKey)
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
			return id, nil
		}
	}
	return "", errors.New("storage_integrity query id is required")
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
	switch {
	case insertTargetPattern.MatchString(trimmed):
		return KindInsert, true, false
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

func targetTableID(qctx *plugin.QueryContext, sql string) (string, error) {
	sqlTarget := parseTarget(sql)
	if sqlTarget == "" {
		return "", errors.New("storage_integrity target table is required")
	}
	sqlDB, sqlTable := sqlident.SplitLastPath(sqlTarget)
	if sqlTable == "" {
		return "", fmt.Errorf("storage_integrity target table path %q is invalid", sqlTarget)
	}
	sessionDB := ""
	if qctx.Session != nil && qctx.Session.State() != nil {
		sessionDB = qctx.Session.State().LogicalDatabaseName()
	}
	resolvedSQLTarget, err := normalizeSQLTargetTablePath(qctx, sqlTarget, sessionDB)
	if err != nil {
		return "", err
	}
	matches := make(map[string]struct{})
	for _, table := range qctx.AccessedTables {
		if strings.TrimSpace(table.OriginalTable) == "" {
			continue
		}
		metadataTable, err := normalizeTablePath(sqlident.Quote(table.OriginalTable))
		if err != nil {
			return "", err
		}
		if metadataTable != sqlTable {
			continue
		}
		referenceDB := firstNonEmpty(table.OriginalDatabase, table.LogicalDatabase, sessionDB)
		metadataTarget, err := normalizeStructuredTablePath(referenceDB, table.OriginalTable)
		if err != nil {
			return "", err
		}
		if sqlDB != "" || sessionDB != "" {
			if metadataTarget != resolvedSQLTarget {
				continue
			}
		}
		logicalDB := firstNonEmpty(table.LogicalDatabase, table.OriginalDatabase, sessionDB)
		logicalID, err := normalizeStructuredTablePath(logicalDB, table.OriginalTable)
		if err != nil {
			return "", err
		}
		if logicalID == "" {
			continue
		}
		matches[logicalID] = struct{}{}
	}
	if len(matches) == 1 {
		for tableID := range matches {
			return tableID, nil
		}
	}
	if len(matches) > 1 {
		return "", fmt.Errorf("storage_integrity ambiguous target table %s in rewriter metadata", sqlTarget)
	}
	if len(qctx.AccessedTables) > 0 {
		return "", fmt.Errorf("storage_integrity target table mismatch: SQL target %s does not match rewriter metadata", sqlTarget)
	}
	return resolvedSQLTarget, nil
}

func normalizeSQLTargetTablePath(qctx *plugin.QueryContext, target, fallbackDB string) (string, error) {
	db, table := sqlident.SplitLastPath(target)
	if table == "" {
		return "", fmt.Errorf("storage_integrity target table path %q is invalid", target)
	}
	if db != "" {
		return normalizeTablePath(target)
	}
	db = fallbackDB
	if db == "" && qctx != nil && qctx.Session != nil && qctx.Session.State() != nil {
		db = qctx.Session.State().LogicalDatabaseName()
	}
	if db != "" {
		target = sqlident.Quote(db) + "." + table
	} else {
		target = table
	}
	return normalizeTablePath(target)
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

func parseTarget(sql string) string {
	patterns := []*regexp.Regexp{
		insertTargetPattern,
		updateTargetPattern,
		deleteTargetPattern,
		alterUpdateTargetPattern,
		alterDeleteTargetPattern,
	}
	for _, pattern := range patterns {
		match := pattern.FindStringSubmatch(sql)
		if len(match) == 2 {
			return match[1]
		}
	}
	return ""
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
	insertTargetPattern      = regexp.MustCompile(`(?is)^\s*INSERT\s+INTO\s+(` + identifierPath + `)(?:\s|\(|;|$)`)
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
