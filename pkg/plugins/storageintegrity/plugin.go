package storageintegrity

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"housegate/housegate/pkg/auth"
	"housegate/housegate/pkg/chsession"
	"housegate/housegate/pkg/log"
	"housegate/housegate/pkg/plugin"
	"housegate/housegate/pkg/replay"
	"housegate/housegate/pkg/sqlident"
	"housegate/housegate/pkg/sqlmeta"
	core "housegate/housegate/pkg/storageintegrity"
)

type Config struct {
	UnsafeDatabase            string
	SafeDatabase              string
	DA                        core.PayloadStore
	Sequencer                 core.SequencerIngress
	RequirePartitionPredicate bool
	PartitionColumns          []string
	ProtectedColumns          []string
	RejectLightweightDelete   bool
	MaxTouchedPartitions      int
	MaxTouchedParts           int
	MaxTouchedBytes           int64
	NetworkID                 string
	InjectRowID               bool
	RequireRowIDInput         bool
	RequireAuthToken          bool
	AuthValidator             auth.Validator
	SnapshotReader            core.SnapshotReader
	ReadGate                  core.ReadSetGate
	NodeID                    string
}

type Plugin struct {
	unsafeDB                  string
	safeDB                    string
	da                        core.PayloadStore
	sequencer                 core.SequencerIngress
	requirePartitionPredicate bool
	partitionColumns          []string
	protectedColumns          []string
	rejectLightweightDelete   bool
	maxTouchedPartitions      int
	maxTouchedParts           int
	maxTouchedBytes           int64
	networkID                 string
	injectRowID               bool
	requireRowIDInput         bool
	requireAuthToken          bool
	authValidator             auth.Validator
	snapshotReader            core.SnapshotReader
	readGate                  core.ReadSetGate
	nodeID                    string

	instanceID string
	nextStmt   atomic.Uint64

	mu     sync.Mutex
	active map[int64]*insertCapture
	now    func() time.Time
}

type insertCapture struct {
	tableID     string
	statementID string
	originalSQL string
	unsafeSQL   string
	unsafeTable string
	safeTable   string
	revision    int
	payload     bytes.Buffer
	userJWS     string
	authSigner  string
	nextOrdinal uint64
}

type insertTarget struct {
	tableID     string
	unsafeSQL   string
	unsafeTable string
	safeTable   string
}

type mutationTarget struct {
	tableID      string
	mutationType string
	mutationSQL  string
	safeTable    string
	partitionIDs []string
}

func New(cfg Config) *Plugin {
	unsafeDB := cfg.UnsafeDatabase
	if unsafeDB == "" {
		unsafeDB = "hg_unsafe"
	}
	safeDB := cfg.SafeDatabase
	if safeDB == "" {
		safeDB = "hg_safe"
	}
	networkID := cfg.NetworkID
	if networkID == "" {
		networkID = "sentio"
	}
	return &Plugin{
		unsafeDB:                  unsafeDB,
		safeDB:                    safeDB,
		da:                        cfg.DA,
		sequencer:                 cfg.Sequencer,
		requirePartitionPredicate: cfg.RequirePartitionPredicate,
		partitionColumns:          normalizeColumns(cfg.PartitionColumns),
		protectedColumns:          normalizeColumns(cfg.ProtectedColumns),
		rejectLightweightDelete:   cfg.RejectLightweightDelete,
		maxTouchedPartitions:      cfg.MaxTouchedPartitions,
		maxTouchedParts:           cfg.MaxTouchedParts,
		maxTouchedBytes:           cfg.MaxTouchedBytes,
		networkID:                 networkID,
		injectRowID:               cfg.InjectRowID,
		requireRowIDInput:         cfg.RequireRowIDInput,
		requireAuthToken:          cfg.RequireAuthToken,
		authValidator:             cfg.AuthValidator,
		snapshotReader:            cfg.SnapshotReader,
		readGate:                  cfg.ReadGate,
		nodeID:                    cfg.NodeID,
		instanceID:                newPluginInstanceID(),
		active:                    map[int64]*insertCapture{},
		now:                       time.Now,
	}
}

func (p *Plugin) OnQuery(ctx context.Context, qctx *plugin.QueryContext) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p == nil || qctx == nil || qctx.Query == nil || qctx.Session == nil {
		return nil
	}
	originalSQL := firstNonEmpty(qctx.OriginalSQL, qctx.Query.Body)
	if isForbiddenStorageIntegrityWrite(originalSQL) {
		return fmt.Errorf("storage_integrity rejects direct safe-state publication or destructive table operation")
	}
	if p.isSafeRead(qctx, originalSQL) {
		return p.gateSafeRead(ctx, qctx)
	}
	if isStorageIntegrityWriteSQL(qctx, originalSQL) {
		if fn, ok := containsUnmaterializedNondeterminism(originalSQL); ok {
			return fmt.Errorf("storage_integrity rejects unmaterialized nondeterministic function %s", fn)
		}
	}
	if isMutationSQL(originalSQL) {
		return p.handleMutation(ctx, qctx, originalSQL)
	}
	if qctx.StatementType != sqlmeta.StatementTypeInsert && !isInsertSQL(originalSQL) {
		return nil
	}
	target, err := p.resolveInsertTarget(qctx)
	if err != nil {
		return err
	}
	userJWS, signer, err := p.authenticateQuery(ctx, qctx, originalSQL)
	if err != nil {
		return err
	}
	stmtID := p.statementIDForQuery(qctx)
	cap := &insertCapture{
		tableID:     target.tableID,
		statementID: stmtID,
		originalSQL: originalSQL,
		unsafeSQL:   target.unsafeSQL,
		unsafeTable: target.unsafeTable,
		safeTable:   target.safeTable,
		revision:    qctx.Session.State().ClientRevision,
		userJWS:     userJWS,
		authSigner:  signer,
	}
	qctx.Query.Body = target.unsafeSQL
	qctx.RewrittenSQL = target.unsafeSQL
	p.mu.Lock()
	p.active[qctx.Session.ID()] = cap
	p.mu.Unlock()
	return nil
}

func (p *Plugin) isSafeRead(qctx *plugin.QueryContext, sql string) bool {
	if p == nil || !isSelectSQL(sql) {
		return false
	}
	for _, table := range qctx.AccessedTables {
		if table.PhysicalDatabase == p.safeDB || table.LogicalDatabase == p.safeDB || table.OriginalDatabase == p.safeDB {
			return true
		}
	}
	return strings.Contains(sql, "`"+p.safeDB+"`") || strings.Contains(sql, p.safeDB+".")
}

func (p *Plugin) gateSafeRead(ctx context.Context, qctx *plugin.QueryContext) error {
	if p.readGate == nil {
		return nil
	}
	req := core.SafeReadRequest{
		NodeID:   p.nodeID,
		TableIDs: p.safeReadTableIDs(qctx),
	}
	if req.NodeID == "" {
		req.NodeID = p.instanceID
	}
	decision, err := p.readGate.CheckSafeRead(ctx, req)
	if err != nil {
		return fmt.Errorf("storage_integrity safe read gate: %w", err)
	}
	if !decision.Active {
		reason := decision.Reason
		if reason == "" {
			reason = "node is not in active read set"
		}
		return fmt.Errorf("storage_integrity safe read gated: node_id=%s snapshot_id=%s safe_block_seq=%d reason=%s",
			req.NodeID, decision.SnapshotID, decision.SafeBlockSeq, reason)
	}
	return nil
}

func (p *Plugin) safeReadTableIDs(qctx *plugin.QueryContext) []string {
	if qctx == nil {
		return nil
	}
	seen := map[string]struct{}{}
	var out []string
	for _, table := range qctx.AccessedTables {
		if table.PhysicalDatabase != p.safeDB && table.LogicalDatabase != p.safeDB && table.OriginalDatabase != p.safeDB {
			continue
		}
		db := firstNonEmpty(table.LogicalDatabase, table.OriginalDatabase)
		if db == p.safeDB {
			db = ""
		}
		tableID := table.OriginalTable
		if db != "" {
			tableID = db + "." + table.OriginalTable
		}
		if tableID == "" {
			continue
		}
		if _, ok := seen[tableID]; ok {
			continue
		}
		seen[tableID] = struct{}{}
		out = append(out, tableID)
	}
	sort.Strings(out)
	return out
}

func (p *Plugin) statementIDForQuery(qctx *plugin.QueryContext) string {
	if qctx != nil && qctx.Query != nil {
		if id := strings.TrimSpace(qctx.Query.ID); id != "" {
			return id
		}
	}
	return p.newStatementID()
}

func (p *Plugin) handleMutation(ctx context.Context, qctx *plugin.QueryContext, originalSQL string) error {
	if p.sequencer == nil {
		return fmt.Errorf("sequencer client is required")
	}
	userJWS, signer, err := p.authenticateQuery(ctx, qctx, originalSQL)
	if err != nil {
		return err
	}
	target, err := p.resolveMutationTarget(qctx, originalSQL)
	if err != nil {
		return err
	}
	if err := p.validateMutationTouchedLimits(ctx, target.tableID, target.partitionIDs); err != nil {
		return err
	}
	rec := core.MutationRecord{
		TableID:             target.tableID,
		StatementID:         p.statementIDForQuery(qctx),
		MutationType:        target.mutationType,
		OriginalSQL:         originalSQL,
		MutationSQL:         target.mutationSQL,
		SafeTable:           target.safeTable,
		PartitionIDs:        target.partitionIDs,
		UserJWS:             userJWS,
		AuthenticatedSigner: signer,
		ExecutionMode:       "parallel_local_replay",
		ReceivedAt:          p.now().UTC(),
	}
	if err := p.sequencer.SubmitMutation(ctx, rec); err != nil {
		return fmt.Errorf("submit sequencer mutation: %w", err)
	}
	qctx.AbortWithSuccess = true
	qctx.Query.Body = "SELECT 1"
	qctx.RewrittenSQL = qctx.Query.Body
	return nil
}

func (p *Plugin) authenticateQuery(ctx context.Context, qctx *plugin.QueryContext, sql string) (userJWS, signer string, err error) {
	settings := querySettings(qctx)
	userJWS = strings.Trim(settings[auth.AuthTokenSettingKey], "\"'")
	if p.requireAuthToken && userJWS == "" {
		return "", "", fmt.Errorf("storage_integrity requires %s", auth.AuthTokenSettingKey)
	}
	if p.authValidator != nil {
		res, err := p.authValidator.ValidateQuery(ctx, auth.QueryMeta{
			ConnID:   qctx.Session.ID(),
			SQL:      sql,
			Settings: settings,
		})
		if err != nil {
			return "", "", fmt.Errorf("storage_integrity JWS validation: %w", err)
		}
		signer = res.Address
	}
	if signer == "" && qctx != nil && qctx.Session != nil && qctx.Session.State() != nil {
		signer = qctx.Session.State().Snapshot().Identity.UserID
	}
	return userJWS, signer, nil
}

func querySettings(qctx *plugin.QueryContext) map[string]string {
	out := map[string]string{}
	if qctx == nil || qctx.Query == nil {
		return out
	}
	for _, s := range qctx.Query.Settings {
		out[s.Key] = s.Value
	}
	return out
}

func (p *Plugin) OnClientData(ctx context.Context, qctx *plugin.QueryContext, raw []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p == nil || qctx == nil || qctx.Session == nil || len(raw) == 0 {
		return nil
	}
	if p.injectRowID {
		return nil
	}
	p.mu.Lock()
	cap := p.active[qctx.Session.ID()]
	if cap != nil {
		_, _ = cap.payload.Write(raw)
	}
	p.mu.Unlock()
	return nil
}

func (p *Plugin) RewriteClientData(ctx context.Context, qctx *plugin.QueryContext, raw []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if p == nil || qctx == nil || qctx.Session == nil || len(raw) == 0 {
		return raw, nil
	}
	p.mu.Lock()
	cap := p.active[qctx.Session.ID()]
	if cap == nil {
		p.mu.Unlock()
		return raw, nil
	}
	if !p.injectRowID {
		p.mu.Unlock()
		return raw, nil
	}
	rewritten, rows, err := core.InjectNativeRowIDs(p.networkID, cap.tableID, cap.statementID, cap.revision, raw, cap.nextOrdinal)
	if err != nil {
		p.mu.Unlock()
		return nil, err
	}
	cap.nextOrdinal += rows
	_, _ = cap.payload.Write(rewritten)
	p.mu.Unlock()
	return rewritten, nil
}

func (p *Plugin) OnQueryComplete(ctx context.Context, sess chsession.Session) {
	if p == nil || sess == nil {
		return
	}
	p.mu.Lock()
	cap := p.active[sess.ID()]
	delete(p.active, sess.ID())
	p.mu.Unlock()
	if cap == nil {
		return
	}
	if err := p.submit(ctx, cap); err != nil {
		log.Warnw("storage_integrity insert submit failed", "statement_id", cap.statementID, "table_id", cap.tableID, "err", err)
	}
}

func (p *Plugin) submit(ctx context.Context, cap *insertCapture) error {
	if p.da == nil {
		return fmt.Errorf("DA client is required")
	}
	if p.sequencer == nil {
		return fmt.Errorf("sequencer client is required")
	}
	payload := append([]byte(nil), cap.payload.Bytes()...)
	commit, err := p.da.PutPayload(ctx, core.PutPayloadRequest{
		TableID:     cap.tableID,
		StatementID: cap.statementID,
		Payload:     payload,
	})
	if err != nil {
		return fmt.Errorf("put DA payload: %w", err)
	}
	rec := core.InsertRecord{
		TableID:             cap.tableID,
		StatementID:         cap.statementID,
		OriginalSQL:         cap.originalSQL,
		UnsafeSQL:           cap.unsafeSQL,
		UnsafeTable:         cap.unsafeTable,
		SafeTable:           cap.safeTable,
		UserJWS:             cap.userJWS,
		AuthenticatedSigner: cap.authSigner,
		Payload:             commit,
		SourceClaimRoot:     replay.DigestBytes(payload),
		ReceivedAt:          p.now().UTC(),
	}
	if claim, err := core.ComputeNativePayloadClaim(cap.tableID, cap.revision, payload); err == nil && claim.RowCount > 0 {
		rec.SourceClaimRoot = claim.SourceClaimRoot
		rec.PayloadEncoding = claim.PayloadEncoding
		rec.PayloadRevision = claim.PayloadRevision
		rec.SettingsHash = core.DefaultReplaySettingsHash
		if snap, err := core.NativePayloadGenesisSnapshot(cap.tableID, claim.Columns); err == nil {
			rec.PrevSafeSnapshotID = snap.SnapshotID
			rec.PrevStateRoot = snap.StateRoot
			rec.SchemaSnapshotID = snap.SchemaSnapshotID
			rec.ExecutorProfileID = snap.ExecutorProfileID
		}
	}
	if err := p.sequencer.SubmitInsert(ctx, rec); err != nil {
		return fmt.Errorf("submit sequencer insert: %w", err)
	}
	return nil
}

const identifierPathPattern = `(?:` + "`[^`]+`" + `|[A-Za-z_][A-Za-z0-9_]*)(?:\.(?:` + "`[^`]+`" + `|[A-Za-z_][A-Za-z0-9_]*))?`

var (
	insertIntoPattern     = regexp.MustCompile(`(?is)^\s*INSERT\s+INTO\s+(` + identifierPathPattern + `)(.*)$`)
	alterMutationPrefix   = regexp.MustCompile(`(?is)^\s*ALTER\s+TABLE\s+` + identifierPathPattern + `\s+(UPDATE|DELETE)\b`)
	alterMutationPattern  = regexp.MustCompile(`(?is)^\s*ALTER\s+TABLE\s+(` + identifierPathPattern + `)\s+(UPDATE|DELETE)\s+(.+?)\s*$`)
	updateMutationPrefix  = regexp.MustCompile(`(?is)^\s*UPDATE\s+` + identifierPathPattern + `\s+SET\b`)
	updateMutationPattern = regexp.MustCompile(`(?is)^\s*UPDATE\s+(` + identifierPathPattern + `)\s+SET\s+(.+?)\s+WHERE\s+(.+?)\s*$`)
	deleteMutationPrefix  = regexp.MustCompile(`(?is)^\s*DELETE\s+FROM\s+` + identifierPathPattern + `\s+WHERE\b`)
	deleteMutationPattern = regexp.MustCompile(`(?is)^\s*DELETE\s+FROM\s+(` + identifierPathPattern + `)\s+WHERE\s+(.+?)\s*$`)
	truncatePattern       = regexp.MustCompile(`(?is)^\s*TRUNCATE\s+TABLE\b`)
	dropPartitionPattern  = regexp.MustCompile(`(?is)^\s*ALTER\s+TABLE\s+` + identifierPathPattern + `\s+DROP\s+PARTITION\b`)
	wherePattern          = regexp.MustCompile(`(?is)\s+WHERE\s+`)
)

func (p *Plugin) resolveInsertTarget(qctx *plugin.QueryContext) (insertTarget, error) {
	if target, ok := p.targetFromRewriter(qctx); ok {
		return target, nil
	}
	return p.rewriteInsert(qctx, qctx.Query.Body)
}

func (p *Plugin) targetFromRewriter(qctx *plugin.QueryContext) (insertTarget, bool) {
	if qctx == nil || qctx.Query == nil || len(qctx.AccessedTables) == 0 {
		return insertTarget{}, false
	}
	for _, table := range qctx.AccessedTables {
		originalTable := normalizeIdentifierPath(table.OriginalTable)
		if originalTable == "" {
			continue
		}
		logicalDB := firstNonEmpty(table.LogicalDatabase, table.OriginalDatabase)
		tableID := originalTable
		if logicalDB != "" {
			tableID = normalizeIdentifierPath(logicalDB) + "." + originalTable
		}
		rewritten := firstNonEmpty(qctx.TableRewrites[tableID], qctx.TableRewrites[table.OriginalDatabase+"."+table.OriginalTable])
		if rewritten == "" {
			rewritten = parseInsertTargetPath(qctx.Query.Body)
		}
		if rewritten == "" {
			return insertTarget{}, false
		}
		unsafeTable := quoteIdentifierPath(rewritten)
		return insertTarget{
			tableID:     tableID,
			unsafeSQL:   qctx.Query.Body,
			unsafeTable: unsafeTable,
			safeTable:   qualifiedTable(p.safeDB, originalTable),
		}, true
	}
	return insertTarget{}, false
}

func (p *Plugin) rewriteInsert(qctx *plugin.QueryContext, sql string) (insertTarget, error) {
	match := insertIntoPattern.FindStringSubmatch(sql)
	if len(match) != 3 {
		return insertTarget{}, fmt.Errorf("storage_integrity only supports INSERT INTO <table> in phase 1")
	}
	targetPath := normalizeIdentifierPath(match[1])
	if targetPath == "" {
		return insertTarget{}, fmt.Errorf("insert target table is required")
	}
	logicalDB, tableName := splitInsertTargetPath(targetPath)
	if logicalDB == "" {
		logicalDB = sessionLogicalDatabase(qctx)
	}
	tableID := tableName
	if logicalDB != "" && logicalDB != p.unsafeDB && logicalDB != p.safeDB {
		tableID = logicalDB + "." + tableName
	}
	unsafeTable := qualifiedTable(p.unsafeDB, tableName)
	return insertTarget{
		tableID:     tableID,
		unsafeSQL:   "INSERT INTO " + unsafeTable + match[2],
		unsafeTable: unsafeTable,
		safeTable:   qualifiedTable(p.safeDB, tableName),
	}, nil
}

func (p *Plugin) resolveMutationTarget(qctx *plugin.QueryContext, sql string) (mutationTarget, error) {
	match := alterMutationPattern.FindStringSubmatch(sql)
	var targetPath, op, beforeWhere, whereClause string
	lightweightDelete := false
	switch {
	case len(match) == 4:
		targetPath = normalizeIdentifierPath(match[1])
		op = strings.ToUpper(strings.TrimSpace(match[2]))
		var ok bool
		beforeWhere, whereClause, ok = splitMutationWhere(match[3])
		if !ok || whereClause == "" {
			return mutationTarget{}, fmt.Errorf("bounded mutation requires WHERE")
		}
	case updateMutationPattern.MatchString(sql):
		match = updateMutationPattern.FindStringSubmatch(sql)
		targetPath = normalizeIdentifierPath(match[1])
		op = "UPDATE"
		beforeWhere = strings.TrimSpace(match[2])
		whereClause = strings.TrimSpace(match[3])
	case deleteMutationPattern.MatchString(sql):
		match = deleteMutationPattern.FindStringSubmatch(sql)
		targetPath = normalizeIdentifierPath(match[1])
		op = "DELETE"
		whereClause = strings.TrimSpace(match[2])
		lightweightDelete = true
	default:
		return mutationTarget{}, fmt.Errorf("storage_integrity only supports bounded UPDATE/DELETE mutations")
	}
	if targetPath == "" {
		return mutationTarget{}, fmt.Errorf("mutation target table is required")
	}
	logicalDB, tableName := splitInsertTargetPath(targetPath)
	if logicalDB == "" {
		logicalDB = sessionLogicalDatabase(qctx)
	}
	tableID := tableName
	if logicalDB != "" && logicalDB != p.unsafeDB && logicalDB != p.safeDB {
		tableID = logicalDB + "." + tableName
	}
	if whereClause == "" {
		return mutationTarget{}, fmt.Errorf("bounded mutation requires WHERE")
	}
	if err := p.validateMutationAdmission(op, beforeWhere, whereClause, lightweightDelete); err != nil {
		return mutationTarget{}, err
	}
	partitionIDs, err := p.extractMutationPartitionIDs(whereClause)
	if err != nil {
		return mutationTarget{}, err
	}
	if len(partitionIDs) == 0 {
		if p.requirePartitionPredicate && len(p.partitionColumns) > 0 {
			return mutationTarget{}, fmt.Errorf("bounded mutation requires extractable partition predicate on one of %v", p.partitionColumns)
		}
		partitionIDs = []string{"all"}
	}
	var mutationType, mutationTail string
	switch op {
	case "UPDATE":
		if beforeWhere == "" {
			return mutationTarget{}, fmt.Errorf("UPDATE mutation requires assignments before WHERE")
		}
		mutationType = core.MutationTypeUpdate
		mutationTail = "UPDATE " + beforeWhere + " WHERE " + whereClause
	case "DELETE":
		if strings.TrimSpace(beforeWhere) != "" {
			return mutationTarget{}, fmt.Errorf("DELETE mutation only supports DELETE WHERE ...")
		}
		mutationType = core.MutationTypeDelete
		mutationTail = "DELETE WHERE " + whereClause
	default:
		return mutationTarget{}, fmt.Errorf("unsupported mutation type %q", op)
	}
	safeTable := qualifiedTable(p.safeDB, tableName)
	return mutationTarget{
		tableID:      tableID,
		mutationType: mutationType,
		mutationSQL:  "ALTER TABLE " + safeTable + " " + mutationTail,
		safeTable:    safeTable,
		partitionIDs: partitionIDs,
	}, nil
}

func parseInsertTargetPath(sql string) string {
	match := insertIntoPattern.FindStringSubmatch(sql)
	if len(match) != 3 {
		return ""
	}
	return normalizeIdentifierPath(match[1])
}

func isInsertSQL(sql string) bool {
	return insertIntoPattern.MatchString(sql)
}

func isSelectSQL(sql string) bool {
	return regexp.MustCompile(`(?is)^\s*SELECT\b`).MatchString(sql)
}

func isMutationSQL(sql string) bool {
	return alterMutationPrefix.MatchString(sql) || updateMutationPrefix.MatchString(sql) || deleteMutationPrefix.MatchString(sql)
}

func isStorageIntegrityWriteSQL(qctx *plugin.QueryContext, sql string) bool {
	if isMutationSQL(sql) || isInsertSQL(sql) {
		return true
	}
	return qctx != nil && qctx.StatementType == sqlmeta.StatementTypeInsert
}

func splitMutationWhere(rest string) (beforeWhere, whereClause string, ok bool) {
	loc := wherePattern.FindStringIndex(rest)
	if loc == nil {
		return "", "", false
	}
	return strings.TrimSpace(rest[:loc[0]]), strings.TrimSpace(rest[loc[1]:]), true
}

func containsProtocolColumn(value string) bool {
	return strings.Contains(strings.ToLower(value), "_hg_")
}

func isForbiddenStorageIntegrityWrite(sql string) bool {
	return truncatePattern.MatchString(sql) || dropPartitionPattern.MatchString(sql)
}

func (p *Plugin) validateMutationAdmission(op, assignments, whereClause string, lightweightDelete bool) error {
	if lightweightDelete && p.rejectLightweightDelete {
		return fmt.Errorf("storage_integrity rejects lightweight DELETE")
	}
	checkSurface := assignments + " " + whereClause
	if containsProtocolColumn(assignments) {
		return fmt.Errorf("UPDATE mutation cannot modify protocol columns")
	}
	if strings.EqualFold(op, "UPDATE") {
		for _, col := range p.protectedColumns {
			if assignmentModifiesColumn(assignments, col) {
				return fmt.Errorf("UPDATE mutation cannot modify protected column %q", col)
			}
		}
	}
	if hasUnsupportedMutationExpression(checkSurface) {
		return fmt.Errorf("mutation contains unsupported subquery, join, dictionary, remote, or table-function expression")
	}
	if p.requirePartitionPredicate && len(p.partitionColumns) > 0 && !whereContainsAnyColumn(whereClause, p.partitionColumns) {
		return fmt.Errorf("bounded mutation requires partition predicate on one of %v", p.partitionColumns)
	}
	return nil
}

func (p *Plugin) validateMutationTouchedLimits(ctx context.Context, tableID string, partitionIDs []string) error {
	if p.maxTouchedPartitions <= 0 && p.maxTouchedParts <= 0 && p.maxTouchedBytes <= 0 {
		return nil
	}
	if p.maxTouchedPartitions > 0 && !partitionSetIncludesAll(partitionIDs) && len(partitionIDs) > p.maxTouchedPartitions {
		return fmt.Errorf("mutation touched partitions %d exceeds limit %d", len(partitionIDs), p.maxTouchedPartitions)
	}
	if p.maxTouchedParts <= 0 && p.maxTouchedBytes <= 0 && !(p.maxTouchedPartitions > 0 && partitionSetIncludesAll(partitionIDs)) {
		return nil
	}
	if p.snapshotReader == nil {
		return fmt.Errorf("mutation touched-set limits require safe snapshot reader")
	}
	watermark, err := p.snapshotReader.GetSafeWatermark(ctx)
	if err != nil {
		return fmt.Errorf("get safe watermark for mutation touched-set limits: %w", err)
	}
	if watermark.SnapshotID == "" {
		return fmt.Errorf("safe watermark snapshot_id is required for mutation touched-set limits")
	}
	manifest, ok, err := p.snapshotReader.GetSafeSnapshot(ctx, watermark.SnapshotID)
	if err != nil {
		return fmt.Errorf("get safe snapshot for mutation touched-set limits: %w", err)
	}
	if !ok {
		return fmt.Errorf("safe snapshot %s not found for mutation touched-set limits", watermark.SnapshotID)
	}
	if err := manifest.Validate(); err != nil {
		return fmt.Errorf("validate safe snapshot for mutation touched-set limits: %w", err)
	}
	partitions, parts, bytes := manifestTouchedCost(manifest, tableID, partitionIDs)
	if p.maxTouchedPartitions > 0 && partitions > p.maxTouchedPartitions {
		return fmt.Errorf("mutation touched partitions %d exceeds limit %d", partitions, p.maxTouchedPartitions)
	}
	if p.maxTouchedParts > 0 && parts > p.maxTouchedParts {
		return fmt.Errorf("mutation touched parts %d exceeds limit %d", parts, p.maxTouchedParts)
	}
	if p.maxTouchedBytes > 0 && bytes > p.maxTouchedBytes {
		return fmt.Errorf("mutation touched bytes %d exceeds limit %d", bytes, p.maxTouchedBytes)
	}
	return nil
}

func manifestTouchedCost(manifest replay.SafeSnapshotManifest, tableID string, partitionIDs []string) (partitions, parts int, bytes int64) {
	partSet := stringSet(partitionIDs)
	includeAll := len(partSet) == 0 || partitionSetIncludesAll(partitionIDs)
	seenPartitions := map[string]struct{}{}
	for _, table := range manifest.Tables {
		if table.TableID != tableID {
			continue
		}
		for _, part := range table.ActiveParts {
			if !includeAll {
				if _, ok := partSet[part.PartitionID]; !ok {
					continue
				}
			}
			if _, ok := seenPartitions[part.PartitionID]; !ok {
				seenPartitions[part.PartitionID] = struct{}{}
				partitions++
			}
			parts++
			bytes += int64(part.Bytes)
		}
	}
	return partitions, parts, bytes
}

func stringSet(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		out[value] = struct{}{}
	}
	return out
}

func partitionSetIncludesAll(partitionIDs []string) bool {
	for _, partitionID := range partitionIDs {
		if partitionID == "all" {
			return true
		}
	}
	return false
}

var nondeterministicFunctionPattern = regexp.MustCompile(`(?is)([A-Za-z_][A-Za-z0-9_]*)\s*\(`)

func containsUnmaterializedNondeterminism(sql string) (string, bool) {
	stripped := stripSQLLiteralsAndComments(sql)
	for _, loc := range nondeterministicFunctionPattern.FindAllStringSubmatchIndex(stripped, -1) {
		if len(loc) < 4 {
			continue
		}
		if loc[2] > 0 && isIdentifierByte(stripped[loc[2]-1]) {
			continue
		}
		name := stripped[loc[2]:loc[3]]
		fn := strings.ToLower(name)
		switch fn {
		case "now", "now64", "rand", "rand64", "random", "generateuuidv4", "generateuuidv7", "uuidv4":
			return name, true
		}
	}
	return "", false
}

func isIdentifierByte(b byte) bool {
	return (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z') || (b >= '0' && b <= '9') || b == '_'
}

func stripSQLLiteralsAndComments(sql string) string {
	out := []byte(sql)
	for i := 0; i < len(out); {
		switch {
		case out[i] == '\'':
			i = blankSQLSingleQuoted(out, i)
		case out[i] == '"' || out[i] == '`':
			i = blankSQLQuoted(out, i, out[i])
		case out[i] == '-' && i+1 < len(out) && out[i+1] == '-':
			for i < len(out) && out[i] != '\n' {
				out[i] = ' '
				i++
			}
		case out[i] == '/' && i+1 < len(out) && out[i+1] == '*':
			out[i] = ' '
			out[i+1] = ' '
			i += 2
			for i+1 < len(out) {
				if out[i] == '*' && out[i+1] == '/' {
					out[i] = ' '
					out[i+1] = ' '
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

func blankSQLSingleQuoted(out []byte, i int) int {
	out[i] = ' '
	i++
	for i < len(out) {
		if out[i] == '\\' && i+1 < len(out) {
			out[i] = ' '
			out[i+1] = ' '
			i += 2
			continue
		}
		if out[i] == '\'' {
			out[i] = ' '
			i++
			if i < len(out) && out[i] == '\'' {
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

func blankSQLQuoted(out []byte, i int, quote byte) int {
	out[i] = ' '
	i++
	for i < len(out) {
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

func hasUnsupportedMutationExpression(value string) bool {
	upper := strings.ToUpper(value)
	unsupported := []string{
		" SELECT ",
		"(SELECT ",
		" JOIN ",
		" FROM ",
		"DICTGET",
		"REMOTE(",
		"REMOTESECURE(",
		"CLUSTER(",
		"S3(",
		"URL(",
		"MYSQL(",
		"POSTGRESQL(",
		"ODBC(",
		"JDBC(",
	}
	for _, marker := range unsupported {
		if strings.Contains(upper, marker) {
			return true
		}
	}
	return false
}

func assignmentModifiesColumn(assignments, column string) bool {
	pattern := regexp.MustCompile(`(?is)(^|,)\s*` + regexp.QuoteMeta(column) + `\s*=`)
	return pattern.MatchString(assignments)
}

func whereContainsAnyColumn(where string, columns []string) bool {
	for _, col := range columns {
		pattern := regexp.MustCompile(`(?is)(^|[^A-Za-z0-9_])` + columnReferencePattern(col) + `([^A-Za-z0-9_]|$)`)
		if pattern.MatchString(where) {
			return true
		}
	}
	return false
}

func (p *Plugin) extractMutationPartitionIDs(whereClause string) ([]string, error) {
	if len(p.partitionColumns) == 0 {
		return nil, nil
	}
	seen := map[string]struct{}{}
	for _, col := range p.partitionColumns {
		for _, id := range extractDatePartitionIDsForColumn(whereClause, col) {
			seen[id] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}
	sort.Strings(out)
	return out, nil
}

func extractDatePartitionIDsForColumn(whereClause, column string) []string {
	col := columnReferencePattern(column)
	dateLiteral := `'([0-9]{4})-([0-9]{2})-[0-9]{2}'`
	eq := regexp.MustCompile(`(?is)(^|[^A-Za-z0-9_])` + col + `\s*=\s*(?:toDate\s*\(\s*)?` + dateLiteral)
	in := regexp.MustCompile(`(?is)(^|[^A-Za-z0-9_])` + col + `\s+IN\s*\(([^)]*)\)`)
	ids := make([]string, 0, 1)
	for _, match := range eq.FindAllStringSubmatch(whereClause, -1) {
		if len(match) >= 4 {
			ids = append(ids, match[2]+match[3])
		}
	}
	for _, match := range in.FindAllStringSubmatch(whereClause, -1) {
		if len(match) < 3 {
			continue
		}
		for _, literal := range regexp.MustCompile(dateLiteral).FindAllStringSubmatch(match[2], -1) {
			if len(literal) >= 3 {
				ids = append(ids, literal[1]+literal[2])
			}
		}
	}
	return ids
}

func columnReferencePattern(column string) string {
	parts := strings.Split(normalizeIdentifierPath(column), ".")
	for i, part := range parts {
		parts[i] = `(?:` + regexp.QuoteMeta(part) + `|` + "`" + regexp.QuoteMeta(part) + "`" + `)`
	}
	return strings.Join(parts, `\s*\.\s*`)
}

func normalizeColumns(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = normalizeIdentifierPath(value)
		if value == "" {
			continue
		}
		out = append(out, value)
	}
	return out
}

func splitInsertTargetPath(path string) (db, table string) {
	return sqlident.SplitLastPath(path)
}

func sessionLogicalDatabase(qctx *plugin.QueryContext) string {
	if qctx == nil || qctx.Session == nil || qctx.Session.State() == nil {
		return ""
	}
	return normalizeIdentifierPath(qctx.Session.State().LogicalDatabaseName())
}

func (p *Plugin) newStatementID() string {
	n := p.nextStmt.Add(1)
	return fmt.Sprintf("stmt-%s-%d", p.instanceID, n)
}

var nextPluginInstance atomic.Uint64

func newPluginInstanceID() string {
	return fmt.Sprintf("%d-%d-%d", os.Getpid(), time.Now().UnixNano(), nextPluginInstance.Add(1))
}

func normalizeIdentifierPath(value string) string {
	return sqlident.NormalizePath(value)
}

func qualifiedTable(database, tableID string) string {
	return quoteIdent(database) + "." + quoteIdent(tableID)
}

func quoteIdentifierPath(value string) string {
	return sqlident.QuotePath(value)
}

func quoteIdent(value string) string {
	return sqlident.Quote(value)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

var _ plugin.QueryPlugin = (*Plugin)(nil)
var _ plugin.DataPlugin = (*Plugin)(nil)
var _ plugin.QueryCompletePlugin = (*Plugin)(nil)
