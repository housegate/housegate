package snapshotquery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/rewriter"
	pb "github.com/housegate/rewriter-proto/gen/pb"
	"google.golang.org/protobuf/proto"
)

// ExecutorOptions are trusted constructor dependencies. They are borrowed and
// must remain valid/unmodified; no invocation selects endpoints or authority.
// Matching measured read/analyzer engines and retained historical logical-name
// provenance remain the creator's responsibility, not facts proven by injection.
type ExecutorOptions struct {
	NetworkID         string
	ExecutorProfileID string
	QueryProfileID    string
	Snapshots         SnapshotReadStore
	Analyzer          rewriter.SnapshotQueryAnalyzer
	Profiles          ProfileRegistry
	Appender          *payloadexec.Executor
	HistoricalPolicy  HistoricalPolicy
	LogicalNames      SnapshotLogicalNames
	Uses              QueryUseAdmission
	TempDir           string
}

// Executor freezes one exact profile pair AND one retained pin/O/name mapping.
// It neither selects historical engines nor implements production authority,
// snapshot restoration, current-use registry, or unsafe writes.
type Executor struct {
	networkID    string
	profile      Profile
	snapshots    SnapshotReadStore
	analyzer     rewriter.SnapshotQueryAnalyzer
	appender     *payloadexec.Executor
	policy       HistoricalPolicy
	uses         QueryUseAdmission
	pin          replay.SnapshotPin
	schemaDigest string
	names        map[string]LogicalTableName
	tempDir      string
}

func NewExecutor(ctx context.Context, opts ExecutorOptions) (*Executor, error) {
	return newExecutor(ctx, opts, rewriter.ProbeSnapshotQuery)
}

type snapshotQueryProbe func(context.Context, rewriter.SnapshotQueryAnalyzer, string, []*pb.SnapshotQueryCatalogTable) error

func newExecutor(ctx context.Context, opts ExecutorOptions, probe snapshotQueryProbe) (*Executor, error) {
	if ctx == nil {
		return nil, fmt.Errorf("context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(opts.NetworkID) == "" || isNil(opts.Snapshots) || isNil(opts.Analyzer) || isNil(opts.Profiles) || opts.Appender == nil || isNil(opts.HistoricalPolicy) || isNil(opts.Uses) || probe == nil {
		return nil, fmt.Errorf("snapshot query dependencies are required")
	}
	if opts.Appender.NetworkID != opts.NetworkID || opts.LogicalNames.Pin.NetworkID != opts.NetworkID {
		return nil, fmt.Errorf("executor network mismatch")
	}
	p, err := freezeProfile(opts.Profiles, opts.ExecutorProfileID, opts.QueryProfileID)
	if err != nil {
		return nil, err
	}
	names, err := freezeLogicalNames(opts.LogicalNames)
	if err != nil {
		return nil, err
	}
	if err = probe(ctx, opts.Analyzer, p.QueryProfileID, snapshotQueryProbeCatalog()); err != nil {
		return nil, fmt.Errorf("query capability probe: %w", err)
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	return &Executor{networkID: opts.NetworkID, profile: p, snapshots: opts.Snapshots, analyzer: opts.Analyzer, appender: opts.Appender, policy: opts.HistoricalPolicy, uses: opts.Uses, pin: opts.LogicalNames.Pin, schemaDigest: opts.LogicalNames.SchemaArtifactDigest, names: names, tempDir: opts.TempDir}, nil
}

// Fresh synthetic capability data only, never a snapshot or execution catalog.
func snapshotQueryProbeCatalog() []*pb.SnapshotQueryCatalogTable {
	out := make([]*pb.SnapshotQueryCatalogTable, 0, 2)
	for _, name := range []string{"copy", "events"} {
		s := payloadexec.TableSchema{TableID: "probe-" + name, Columns: []lthash.Column{{Name: "value", Type: "Int64"}}}
		out = append(out, &pb.SnapshotQueryCatalogTable{Database: "tenant", Table: name, TableId: s.TableID, SchemaHash: payloadexec.TableSchemaHash("probe-only", s), Columns: []*pb.SnapshotQueryColumn{{Name: "value", Type: "Int64", Generation: pb.SnapshotQueryColumnGeneration_SNAPSHOT_QUERY_COLUMN_GENERATION_ORDINARY}}})
	}
	return out
}

// PreparedQueryExecution owns only the complete independent output. Job and
// Manifest accessors copy the authenticated invocation and complete post-state
// for C5 input/read/statement-to-output binding. B3 alone has no input-root or
// stream-provenance proof; C5 must durably copy/fsync/reopen before unsafe use.
type PreparedQueryExecution struct {
	Result        replay.ExecutionResult
	Output        CanonicalOutput
	job           replay.SnapshotQueryJob
	manifest      replay.SafeSnapshotManifest
	statementRoot string
	ownedOutput   CanonicalOutput
	once          sync.Once
	closeErr      error
}

func (p *PreparedQueryExecution) Job() replay.SnapshotQueryJob { return cloneJob(p.job) }
func (p *PreparedQueryExecution) Manifest() replay.SafeSnapshotManifest {
	return cloneManifest(p.manifest)
}
func (p *PreparedQueryExecution) StatementRoot() string { return p.statementRoot }
func (p *PreparedQueryExecution) Close() error {
	if p == nil {
		return nil
	}
	p.once.Do(func() {
		if p.ownedOutput != nil {
			p.closeErr = p.ownedOutput.Close()
		}
	})
	return p.closeErr
}

func (e *Executor) Replay(ctx context.Context, req replay.ExecutionRequest) (replay.ExecutionResult, error) {
	p, err := e.Prepare(ctx, req)
	if err != nil {
		return replay.ExecutionResult{}, err
	}
	return replayPrepared(p)
}

// Shared return boundary keeps a required output-close error from escaping as a
// successful result. The prepared handle closes its retained owner exactly once.
func replayPrepared(p *PreparedQueryExecution) (replay.ExecutionResult, error) {
	if err := p.Close(); err != nil {
		return replay.ExecutionResult{}, err
	}
	return p.Result, nil
}

func (e *Executor) Prepare(ctx context.Context, req replay.ExecutionRequest) (prepared *PreparedQueryExecution, err error) {
	if e == nil || e.appender == nil || isNil(e.policy) || isNil(e.uses) {
		return nil, fmt.Errorf("uninitialized snapshot query executor")
	}
	if ctx == nil {
		return nil, fmt.Errorf("context is required")
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if req.SnapshotQuery == nil || strings.TrimSpace(req.SnapshotQueryReferenceID) == "" || !reflect.DeepEqual(req.Job, replay.ReplayJob{}) || !reflect.DeepEqual(req.Snapshot, replay.SafeSnapshotManifest{}) || req.Statements != nil {
		return nil, fmt.Errorf("exactly one explicit snapshot query and reference, without legacy fields, is required")
	}
	job := cloneJob(*req.SnapshotQuery)
	reference := req.SnapshotQueryReferenceID
	if err = validateJob(job); err != nil {
		return nil, err
	}
	in := job.Statement.Envelope.Input
	b := in.Binding
	if job.ExecutorProfileID != e.profile.ExecutorProfileID || job.QueryProfileID != e.profile.QueryProfileID || b.ReadSnapshot != e.pin || b.NetworkID != e.networkID || b.RowIDProfileID != payloadexec.RowIDProfileID {
		return nil, fmt.Errorf("unsupported exact query pair, pin or row ID profile")
	}
	limits := e.profile.Record.Limits
	if uint64(len(in.SQL)) > limits.MaxSQLBytes {
		return nil, fmt.Errorf("SQL exceeds query profile limit")
	}
	descriptor, err := json.Marshal(in.ReadSet)
	if err != nil {
		return nil, err
	}
	if uint64(len(descriptor)) > limits.MaxDescriptorBytes {
		return nil, fmt.Errorf("read descriptor exceeds query profile limit")
	}
	var restoreBytes uint64
	for _, table := range in.ReadSet.Tables {
		for _, part := range table.ActiveParts {
			restoreBytes, err = checkedAdd(restoreBytes, part.Bytes)
			if err != nil {
				return nil, err
			}
		}
	}
	if restoreBytes > limits.MaxRestoreBytes {
		return nil, fmt.Errorf("restore exceeds query profile limit")
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(limits.MaxExecutionMS)*time.Millisecond)
	defer cancel()
	decision, err := e.policy.VerifyReservation(ctx, cloneJob(job))
	if err != nil {
		return nil, fmt.Errorf("historical policy: %w", err)
	}
	if err = decision.CheckJob(job); err != nil {
		return nil, err
	}
	if decision.SchemaArtifactDigest() != e.schemaDigest {
		return nil, fmt.Errorf("logical-name scope outer O mismatch")
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	lease, err := e.uses.AcquireQueryUse(ctx, reference, cloneJob(job))
	if err != nil {
		if !isNil(lease) {
			err = errors.Join(err, lease.Close())
		}
		return nil, fmt.Errorf("current query use: %w", err)
	}
	if isNil(lease) {
		return nil, fmt.Errorf("current query use returned no lease")
	}
	var read ReadSnapshot
	var stream *ownedStream
	var output CanonicalOutput
	var cursor payloadexec.RowSource
	artifactStarted := false
	defer func() {
		err = errors.Join(err, closeInvocation(lease, artifactStarted, read, stream, cursor))
		if err == nil {
			err = ctx.Err()
		}
		if err != nil {
			if !isNil(output) {
				err = errors.Join(err, output.Close())
			}
			prepared = nil
		}
	}()
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	artifactStarted = true
	read, err = e.snapshots.Open(ctx, b.ReadSnapshot, cloneJob(job).Statement.Envelope.Input.ReadSet, reference)
	if err != nil {
		return nil, fmt.Errorf("open pinned snapshot: %w", err)
	}
	if isNil(read) {
		return nil, fmt.Errorf("snapshot store returned no read handle")
	}
	manifest := cloneManifest(read.Manifest())
	artifact := read.SchemaArtifact()
	artifact.Artifact = artifact.Artifact.Clone()
	if err = replay.ValidateSnapshotQueryManifest(in, manifest); err != nil {
		return nil, err
	}
	validated, err := ValidateAuthenticatedSchemaProfile(manifest, decision.Reservation().ReadSnapshot, artifact, decision.SchemaArtifactDigest())
	if err != nil {
		return nil, err
	}
	schemas := validated.Schemas()
	if !equalSchemas(schemas, cloneSchemas(read.Schemas())) {
		return nil, fmt.Errorf("returned legacy schema projection mismatch")
	}
	// Genesis projects the appender's COMPLETE configuration; ApplyRows with no
	// batches is a pure ledger preflight, not a query execution or synthetic rows.
	configured, err := e.appender.GenesisSnapshot(manifest.SafeBlockSeq, manifest.SchemaSnapshotID, manifest.ExecutorProfileID)
	if err != nil {
		return nil, err
	}
	if configured.SchemaRoot != manifest.SchemaRoot || len(configured.Tables) != len(manifest.Tables) {
		return nil, fmt.Errorf("appender complete schema configuration mismatch")
	}
	legacy := replay.ReplayJob{BlockSeq: job.BlockSeq, PrevSafeSnapshotID: job.PrevSafeSnapshotID, PrevStateRoot: job.PrevStateRoot, SchemaSnapshotID: job.SchemaSnapshotID, ExecutorProfileID: job.ExecutorProfileID}
	if _, _, err = e.appender.ApplyRows(ctx, manifest, legacy, nil); err != nil {
		return nil, fmt.Errorf("appender ledger: %w", err)
	}
	catalog, target, err := e.catalog(in, validated.SchemaArtifact().Artifact, schemas)
	if err != nil {
		return nil, err
	}
	analysis := &pb.AnalyzeSnapshotQueryRequest{ContractVersion: 1, QueryProfileId: job.QueryProfileID, Sql: in.SQL, LogicalDatabase: b.LogicalDatabase, Catalog: catalog}
	if uint64(proto.Size(analysis)-len(in.SQL)) > limits.MaxDescriptorBytes {
		return nil, fmt.Errorf("analysis descriptor exceeds profile limit")
	}
	analyzed, err := e.analyzer.AnalyzeSnapshotQuery(ctx, proto.Clone(analysis).(*pb.AnalyzeSnapshotQueryRequest))
	if err != nil {
		return nil, err
	}
	if analyzed == nil {
		return nil, fmt.Errorf("empty analysis response")
	}
	analyzed = proto.Clone(analyzed).(*pb.AnalyzeSnapshotQueryResponse)
	reads := sortedReadIDs(in.ReadSet)
	if analyzed.ContractVersion != 1 || analyzed.QueryProfileId != job.QueryProfileID || analyzed.Code != pb.SnapshotQueryCode_SUCCESS || analyzed.Message != "" || analyzed.SqlAfterMaterialization != in.SQL {
		return nil, fmt.Errorf("analysis changed signed SQL or failed exact profile acknowledgement")
	}
	if err = checkOutputIdentity(analyzed.TargetTableId, analyzed.TargetColumns, analyzed.ReadTableIds, target, reads); err != nil {
		return nil, err
	}
	if uint64(proto.Size(analyzed)-len(analyzed.SqlAfterMaterialization)) > limits.MaxDescriptorBytes {
		return nil, fmt.Errorf("analysis response exceeds profile limit")
	}
	bindings, err := scratchBindings(append([]Relation{}, read.Relations()...), reads)
	if err != nil {
		return nil, err
	}
	prepareReq := &pb.PrepareSnapshotQueryRequest{Analysis: analysis, Bindings: bindings}
	if uint64(proto.Size(prepareReq)-len(in.SQL)) > limits.MaxDescriptorBytes {
		return nil, fmt.Errorf("prepare descriptor exceeds profile limit")
	}
	selected, err := e.analyzer.PrepareSnapshotQuery(ctx, proto.Clone(prepareReq).(*pb.PrepareSnapshotQueryRequest))
	if err != nil {
		return nil, err
	}
	if selected == nil {
		return nil, fmt.Errorf("empty preparation response")
	}
	selected = proto.Clone(selected).(*pb.PrepareSnapshotQueryResponse)
	if selected.ContractVersion != 1 || selected.QueryProfileId != job.QueryProfileID || selected.Code != pb.SnapshotQueryCode_SUCCESS || selected.Message != "" || strings.TrimSpace(selected.SelectSql) == "" || uint64(len(selected.SelectSql)) > limits.MaxSQLBytes || uint64(proto.Size(selected)-len(selected.SelectSql)) > limits.MaxDescriptorBytes {
		return nil, fmt.Errorf("invalid exact-profile preparation response")
	}
	if err = checkOutputIdentity(selected.TargetTableId, selected.TargetColumns, selected.ReadTableIds, target, reads); err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	raw, queryErr := read.QueryRows(ctx, selected.SelectSql, cloneSchemas([]payloadexec.TableSchema{target})[0])
	if !isNil(raw) {
		stream = &ownedStream{RowStream: raw}
	}
	if queryErr != nil {
		return nil, queryErr
	}
	if stream == nil {
		return nil, fmt.Errorf("query returned no row stream")
	}
	output, err = Canonicalize(ctx, e.networkID, b.StatementID, target, stream, limits, e.tempDir)
	if err != nil {
		return nil, err
	}
	cursor, err = output.OpenRows()
	if err != nil {
		return nil, err
	}
	next, result, err := e.appender.ApplyRows(ctx, manifest, legacy, []payloadexec.StatementRows{{StatementID: b.StatementID, StatementSeq: job.Statement.StatementSeq, TargetTableID: b.TargetTableID, Rows: cursor}})
	if err != nil {
		return nil, err
	}
	result.SnapshotQuery = &replay.SnapshotQueryEvidence{ExecutionOutcome: "applied", OutputRowCount: output.RowCount(), OutputRowsRoot: output.OutputRowsRoot()}
	statementRoot, err := replay.SnapshotQueryStatementRoot(job.Statement)
	if err != nil {
		return nil, err
	}
	return &PreparedQueryExecution{Result: result, Output: output, ownedOutput: output, job: job, manifest: next, statementRoot: statementRoot}, nil
}

// closeInvocation joins every owned temporary handle before releasing a lease.
// A failed Close or unowned failed Open leaves the lifecycle owner's tracked use
// live/uncertain; recovery belongs to that trusted owner, not this executor.
func closeInvocation(lease QueryUseLease, artifactStarted bool, read ReadSnapshot, stream *ownedStream, cursor payloadexec.RowSource) (err error) {
	quiescent := !artifactStarted || !isNil(read)
	if !isNil(cursor) {
		if e := cursor.Close(); e != nil {
			quiescent = false
			err = errors.Join(err, e)
		}
	}
	if stream != nil {
		if e := stream.Close(); e != nil {
			quiescent = false
			err = errors.Join(err, e)
		}
	}
	if !isNil(read) {
		if e := read.Close(); e != nil {
			quiescent = false
			err = errors.Join(err, e)
		}
	}
	if quiescent {
		err = errors.Join(err, lease.Close())
	}
	return err
}

// Canonicalize owns Close of its input. Retain its exact Close result so cleanup
// neither closes it twice nor releases the lease after uncertain quiescence.
type ownedStream struct {
	RowStream
	once     sync.Once
	closeErr error
}

func (s *ownedStream) Close() error {
	s.once.Do(func() { s.closeErr = s.RowStream.Close() })
	return s.closeErr
}
func isNil(v any) bool {
	if v == nil {
		return true
	}
	r := reflect.ValueOf(v)
	switch r.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return r.IsNil()
	}
	return false
}
func sortedReadIDs(r replay.SnapshotReadSet) []string {
	out := make([]string, 0, len(r.Tables))
	for _, t := range r.Tables {
		out = append(out, t.TableID)
	}
	sort.Strings(out)
	return out
}
func checkOutputIdentity(id string, columns, reads []string, target payloadexec.TableSchema, wantReads []string) error {
	if id != target.TableID || len(columns) != len(target.Columns) || len(reads) != len(wantReads) {
		return fmt.Errorf("analysis/preparation target or complete read closure mismatch")
	}
	for i, c := range target.Columns {
		if columns[i] != c.Name {
			return fmt.Errorf("target columns differ from authenticated declaration order")
		}
	}
	for i, id := range wantReads {
		if reads[i] != id {
			return fmt.Errorf("analysis/preparation read closure mismatch")
		}
	}
	return nil
}
func scratchBindings(relations []Relation, reads []string) ([]*pb.SnapshotScratchBinding, error) {
	if len(relations) != len(reads) {
		return nil, fmt.Errorf("restored relation set mismatch")
	}
	byID := map[string]Relation{}
	names := map[[2]string]bool{}
	for _, r := range relations {
		key := [2]string{r.Database, r.Table}
		if !validIdentifier(r.Database) || !validIdentifier(r.Table) || names[key] {
			return nil, fmt.Errorf("invalid or duplicate scratch relation")
		}
		if _, ok := byID[r.TableID]; ok {
			return nil, fmt.Errorf("duplicate restored table")
		}
		names[key] = true
		byID[r.TableID] = r
	}
	out := make([]*pb.SnapshotScratchBinding, 0, len(reads))
	for _, id := range reads {
		r, ok := byID[id]
		if !ok {
			return nil, fmt.Errorf("missing restored read table")
		}
		out = append(out, &pb.SnapshotScratchBinding{TableId: id, ScratchDatabase: r.Database, ScratchTable: r.Table})
	}
	return out, nil
}
func (e *Executor) catalog(in replay.SnapshotQueryInput, a replay.SnapshotQuerySchemaArtifactV1, schemas []payloadexec.TableSchema) ([]*pb.SnapshotQueryCatalogTable, payloadexec.TableSchema, error) {
	if len(a.Tables) != len(e.names) {
		return nil, payloadexec.TableSchema{}, fmt.Errorf("logical names must cover complete O")
	}
	touched := map[string]bool{in.Binding.TargetTableID: true}
	for _, r := range in.ReadSet.Tables {
		n, ok := e.names[r.TableID]
		if !ok || n.Database != r.Database || n.Table != r.Table {
			return nil, payloadexec.TableSchema{}, fmt.Errorf("signed read logical names mismatch")
		}
		touched[r.TableID] = true
	}
	catalog := make([]*pb.SnapshotQueryCatalogTable, 0, len(a.Tables))
	var target payloadexec.TableSchema
	for _, table := range a.Tables {
		n, ok := e.names[table.TableID]
		if !ok {
			return nil, target, fmt.Errorf("complete O table has no logical name")
		}
		if touched[table.TableID] {
			if err := ValidateInitialTableColumnProfile(table); err != nil {
				return nil, target, err
			}
			for _, col := range table.Columns {
				if _, err := payloadexec.ResolveColumnProfile(col.Type); err != nil {
					return nil, target, fmt.Errorf("touched table %q column profile: %w", table.TableID, err)
				}
			}

		}
		c := &pb.SnapshotQueryCatalogTable{Database: n.Database, Table: n.Table, TableId: table.TableID, SchemaHash: table.SchemaHash}
		for _, col := range table.Columns {
			c.Columns = append(c.Columns, &pb.SnapshotQueryColumn{Name: col.Name, Type: col.Type, Generation: pb.SnapshotQueryColumnGeneration(col.Generation), DefaultExpression: col.DefaultExpression})
		}
		catalog = append(catalog, c)
	}
	for _, s := range schemas {
		if s.TableID == in.Binding.TargetTableID {
			target = s
		}
	}
	if target.TableID == "" {
		return nil, target, fmt.Errorf("missing target schema")
	}
	return catalog, target, nil
}
func equalSchemas(a, b []payloadexec.TableSchema) bool {
	if len(a) != len(b) {
		return false
	}
	a = cloneSchemas(a)
	b = cloneSchemas(b)
	sort.Slice(a, func(i, j int) bool { return a[i].TableID < a[j].TableID })
	sort.Slice(b, func(i, j int) bool { return b[i].TableID < b[j].TableID })
	return reflect.DeepEqual(a, b)
}
func cloneJob(j replay.SnapshotQueryJob) replay.SnapshotQueryJob {
	r := &j.Statement.Envelope.Input.ReadSet
	r.Tables = append([]replay.SnapshotReadTable{}, r.Tables...)
	for i := range r.Tables {
		r.Tables[i].PartitionRoots = append([]replay.PartitionCommitment{}, r.Tables[i].PartitionRoots...)
		r.Tables[i].ActiveParts = append([]replay.SnapshotReadPart{}, r.Tables[i].ActiveParts...)
	}
	if j.SourceClaim != nil {
		c := *j.SourceClaim
		c.PartitionDeltas = append([]replay.PartitionCommitment{}, c.PartitionDeltas...)
		c.PartitionCommitmentsAfter = append([]replay.PartitionCommitment{}, c.PartitionCommitmentsAfter...)
		c.CandidateParts = append([]replay.SnapshotReadPart{}, c.CandidateParts...)
		j.SourceClaim = &c
	}
	return j
}
func cloneManifest(m replay.SafeSnapshotManifest) replay.SafeSnapshotManifest {
	m.Tables = append([]replay.TableManifest{}, m.Tables...)
	for i := range m.Tables {
		t := &m.Tables[i]
		t.PartitionRoots = append([]replay.PartitionCommitment{}, t.PartitionRoots...)
		t.ActiveParts = append([]replay.PartManifestEntry{}, t.ActiveParts...)
		for j := range t.ActiveParts {
			t.ActiveParts[j].StorageRefs = append([]string{}, t.ActiveParts[j].StorageRefs...)
		}
	}
	return m
}
