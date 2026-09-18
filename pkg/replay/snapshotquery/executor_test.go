package snapshotquery

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/housegate/housegate/pkg/auth"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/rewriter"
	pb "github.com/housegate/rewriter-proto/gen/pb"
)

// All ports in this file are NON-AUTHORITATIVE deterministic scaffolding.
// Private probe success proves plumbing only, never engine/authority/admission.
type fixtureRegistry struct {
	profile Profile
	calls   int
}

func (r *fixtureRegistry) Lookup(e, q string) (Profile, bool) {
	r.calls++
	return r.profile, e == r.profile.ExecutorProfileID && q == r.profile.QueryProfileID
}

type fixturePolicy struct {
	decision HistoricalDecision
	err      error
	calls    int
	hook     func(replay.SnapshotQueryJob)
}

func (p *fixturePolicy) VerifyReservation(_ context.Context, j replay.SnapshotQueryJob) (HistoricalDecision, error) {
	p.calls++
	if p.hook != nil {
		p.hook(j)
	}
	return p.decision, p.err
}

type fixtureLease struct {
	events *[]string
	closes int
	err    error
}

func (l *fixtureLease) Close() error {
	l.closes++
	*l.events = append(*l.events, "lease-close")
	return l.err
}

type fixtureUses struct {
	lease    *fixtureLease
	calls    int
	ref      string
	err      error
	hook     func(replay.SnapshotQueryJob)
	nilLease bool
}

func (u *fixtureUses) AcquireQueryUse(_ context.Context, ref string, j replay.SnapshotQueryJob) (QueryUseLease, error) {
	u.calls++
	u.ref = ref
	*u.lease.events = append(*u.lease.events, "acquire")
	if u.hook != nil {
		u.hook(j)
	}
	if u.nilLease {
		return nil, u.err
	}
	if u.err != nil {
		return nil, u.err
	}
	return u.lease, nil
}

type fixtureRead struct {
	manifest           replay.SafeSnapshotManifest
	artifact           replay.AuthenticatedSnapshotQuerySchemaV1
	schemas            []payloadexec.TableSchema
	relations          []Relation
	rows               *fixtureStream
	events             *[]string
	queries, closes    int
	closeErr, queryErr error
	queryHook          func(string, payloadexec.TableSchema)
}

func (r *fixtureRead) Manifest() replay.SafeSnapshotManifest                     { return r.manifest }
func (r *fixtureRead) SchemaArtifact() replay.AuthenticatedSnapshotQuerySchemaV1 { return r.artifact }
func (r *fixtureRead) Schemas() []payloadexec.TableSchema                        { return r.schemas }
func (r *fixtureRead) Relations() []Relation                                     { return r.relations }
func (r *fixtureRead) QueryRows(_ context.Context, sql string, s payloadexec.TableSchema) (RowStream, error) {
	r.queries++
	*r.events = append(*r.events, "query")
	if r.queryHook != nil {
		r.queryHook(sql, s)
	}
	if r.queryErr != nil {
		return nil, r.queryErr
	}
	return r.rows, nil
}
func (r *fixtureRead) Close() error {
	r.closes++
	*r.events = append(*r.events, "read-close")
	return r.closeErr
}

type fixtureStore struct {
	read  *fixtureRead
	calls int
	ref   string
	pin   replay.SnapshotPin
	hook  func(replay.SnapshotReadSet)
	err   error
}

func (s *fixtureStore) Open(_ context.Context, p replay.SnapshotPin, r replay.SnapshotReadSet, ref string) (ReadSnapshot, error) {
	s.calls++
	s.ref = ref
	s.pin = p
	*s.read.events = append(*s.read.events, "open")
	if s.hook != nil {
		s.hook(r)
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.read, nil
}

type fixtureStream struct {
	rows             [][]any
	n, closes        int
	endErr, closeErr error
	events           *[]string
	hook             func()
}

func (r *fixtureStream) Next(context.Context) ([]any, error) {
	if r.hook != nil {
		r.hook()
	}
	if r.n < len(r.rows) {
		v := r.rows[r.n]
		r.n++
		return v, nil
	}
	if r.endErr != nil {
		return nil, r.endErr
	}
	return nil, io.EOF
}
func (r *fixtureStream) Close() error {
	r.closes++
	*r.events = append(*r.events, "stream-close")
	return r.closeErr
}

type fixtureAnalyzer struct {
	analyses, prepares     int
	analyzeHook            func(*pb.AnalyzeSnapshotQueryRequest)
	prepareHook            func(*pb.PrepareSnapshotQueryRequest)
	analyzeErr, prepareErr error
	changeAnalysis         func(*pb.AnalyzeSnapshotQueryResponse)
	changePrepare          func(*pb.PrepareSnapshotQueryResponse)
}

func (a *fixtureAnalyzer) AnalyzeSnapshotQuery(_ context.Context, r *pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error) {
	a.analyses++
	if a.analyzeHook != nil {
		a.analyzeHook(r)
	}
	if a.analyzeErr != nil {
		return nil, a.analyzeErr
	}
	out := &pb.AnalyzeSnapshotQueryResponse{ContractVersion: 1, QueryProfileId: r.QueryProfileId, Code: pb.SnapshotQueryCode_SUCCESS, SqlAfterMaterialization: r.Sql, TargetTableId: "opaque-W", TargetColumns: []string{"value"}, ReadTableIds: []string{"R.with.dots"}}
	if a.changeAnalysis != nil {
		a.changeAnalysis(out)
	}
	return out, nil
}
func (a *fixtureAnalyzer) ClassifySnapshotQuery(ctx context.Context, r *pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error) {
	return a.AnalyzeSnapshotQuery(ctx, r)
}
func (a *fixtureAnalyzer) PrepareSnapshotQuery(_ context.Context, r *pb.PrepareSnapshotQueryRequest) (*pb.PrepareSnapshotQueryResponse, error) {
	a.prepares++
	if a.prepareHook != nil {
		a.prepareHook(r)
	}
	if a.prepareErr != nil {
		return nil, a.prepareErr
	}
	out := &pb.PrepareSnapshotQueryResponse{ContractVersion: 1, QueryProfileId: r.Analysis.QueryProfileId, Code: pb.SnapshotQueryCode_SUCCESS, SelectSql: "SELECT value FROM scratch.restored_r", TargetTableId: "opaque-W", TargetColumns: []string{"value"}, ReadTableIds: []string{"R.with.dots"}}
	if a.changePrepare != nil {
		a.changePrepare(out)
	}
	return out, nil
}
func (*fixtureAnalyzer) Close() error { return nil }

type executionFixture struct {
	job      replay.SnapshotQueryJob
	records  HistoricalDecisionRecords
	opts     ExecutorOptions
	policy   *fixturePolicy
	uses     *fixtureUses
	store    *fixtureStore
	read     *fixtureRead
	analyzer *fixtureAnalyzer
	registry *fixtureRegistry
	events   []string
}

func newExecutionFixture(t *testing.T) *executionFixture {
	t.Helper()
	f := &executionFixture{}
	f.job, f.records = historicalFixture(t)
	schemas := []payloadexec.TableSchema{{TableID: "R.with.dots", Columns: []lthash.Column{{Name: "value", Type: "Int64"}}}, {TableID: "opaque-W", Columns: []lthash.Column{{Name: "value", Type: "Int64"}}}, {TableID: "untouched-U", Columns: []lthash.Column{{Name: "value", Type: "Int64"}}}}
	app := payloadexec.New("network-fixture-1", schemas...)
	m, err := app.GenesisSnapshot(12, "schema-fixture-1", "executor-fixture-1")
	if err != nil {
		t.Fatal(err)
	}
	p := replay.SnapshotPin{NetworkID: app.NetworkID, KeeperShardID: 7, SnapshotID: m.SnapshotID, SafeBlockSeq: m.SafeBlockSeq, ManifestRoot: m.ManifestRoot, StateRoot: m.StateRoot, SchemaSnapshotID: m.SchemaSnapshotID, SchemaRoot: m.SchemaRoot}
	a := replay.SnapshotQuerySchemaArtifactV1{Kind: replay.SnapshotQuerySchemaArtifactKindV1, Version: 1, NetworkID: p.NetworkID, KeeperShardID: p.KeeperShardID, SnapshotID: p.SnapshotID, ManifestRoot: p.ManifestRoot, SchemaSnapshotID: p.SchemaSnapshotID, SchemaRoot: p.SchemaRoot}
	for _, s := range schemas {
		a.Tables = append(a.Tables, replay.SnapshotQueryTableSchemaV1{TableID: s.TableID, SchemaHash: payloadexec.TableSchemaHash(p.NetworkID, s), Columns: []replay.SnapshotQuerySchemaColumnV1{{Name: "value", Type: "Int64", Generation: replay.SnapshotQueryColumnGenerationOrdinary}}})
	}
	a.Tables[2].Columns[0].Generation = replay.SnapshotQueryColumnGenerationDefault
	a.Tables[2].Columns[0].DefaultExpression = "now()"
	_, _, o, digest := signProfile(t, m, p, a)
	record := replay.QueryProfileRecord{Version: 1, ClickHouseBuildDigest: replay.DigestString("ch"), Platform: "fixture/platform", NativeAnalyzerBuildDigest: replay.DigestString("native"), GRPCAnalyzerBuildDigest: replay.DigestString("grpc"), TZDataDigest: replay.DigestString("tz"), Settings: []replay.ProfileSetting{{Name: "max_threads", Value: "1"}}, ScalarOperators: []string{"column", "literal"}, ColumnProfileID: "fixture-column", OutputOrderID: "fixture-order", Limits: replay.QueryLimits{MaxSQLBytes: 65536, MaxDescriptorBytes: 65536, MaxOutputRows: 100, MaxOutputBytes: 1 << 20, MaxRestoreBytes: 1 << 20, MaxSortMemoryBytes: 1 << 20, MaxSpillBytes: 4 << 20, MaxExecutionMS: 10000}}
	q, err := record.Hash()
	if err != nil {
		t.Fatal(err)
	}
	b := &f.job.Statement.Envelope.Input.Binding
	b.ReadSnapshot = p
	b.SchemaRoot = p.SchemaRoot
	b.SchemaHash = a.Tables[1].SchemaHash
	b.TargetTableID = "opaque-W"
	b.QueryProfileID = q
	b.RowIDProfileID = payloadexec.RowIDProfileID
	f.job.QueryProfileID = q
	f.job.PrevSafeSnapshotID = p.SnapshotID
	f.job.PrevStateRoot = p.StateRoot
	f.job.Reservation.ReadSnapshot = p
	f.job.Reservation.QueryProfileID = q
	f.job.Statement.Envelope.Input.ReadSet = replay.SnapshotReadSet{ReadSnapshot: p, Tables: []replay.SnapshotReadTable{{Database: "tenant", Table: "events", TableID: "R.with.dots", SchemaHash: a.Tables[0].SchemaHash}}}
	f.job.Statement.Envelope.Input.SQL = "INSERT INTO tenant.copy SELECT value FROM tenant.events"
	b.SQLHash = replay.DigestString(f.job.Statement.Envelope.Input.SQL)
	b.ReadSetRoot, err = replay.SnapshotQueryReadSetRoot(f.job.Statement.Envelope.Input.ReadSet)
	if err != nil {
		t.Fatal(err)
	}
	f.job.Statement.Envelope.InputRoot, err = replay.SnapshotQueryInputRoot(f.job.Statement.Envelope.Input)
	if err != nil {
		t.Fatal(err)
	}
	f.records.Reservation = f.job.Reservation
	f.records.Activation.QueryProfileID = q
	f.records.StatementRoot, err = replay.SnapshotQueryStatementRoot(f.job.Statement)
	if err != nil {
		t.Fatal(err)
	}
	f.records.Artifacts = replay.SnapshotArtifactSet{SchemaArtifactDigest: digest}
	root, err := f.records.Artifacts.Hash()
	if err != nil {
		t.Fatal(err)
	}
	f.records.Ready.SnapshotID = p.SnapshotID
	f.records.Ready.ManifestRoot = p.ManifestRoot
	f.records.Ready.SchemaRoot = p.SchemaRoot
	f.records.Ready.ArtifactSetRoot = root
	decision, err := NewHistoricalDecisionFromVerifiedRecords(f.job, f.records)
	if err != nil {
		t.Fatal(err)
	}
	f.policy = &fixturePolicy{decision: decision}
	f.uses = &fixtureUses{lease: &fixtureLease{events: &f.events}}
	f.read = &fixtureRead{manifest: m, artifact: o, schemas: schemas, relations: []Relation{{TableID: "R.with.dots", Database: "scratch", Table: "restored_r"}}, events: &f.events}
	f.read.rows = &fixtureStream{rows: [][]any{{int64(3)}, {int64(1)}, {int64(3)}}, events: &f.events}
	f.store = &fixtureStore{read: f.read}
	f.analyzer = &fixtureAnalyzer{}
	f.registry = &fixtureRegistry{profile: Profile{ExecutorProfileID: m.ExecutorProfileID, QueryProfileID: q, Record: record}}
	f.opts = ExecutorOptions{NetworkID: p.NetworkID, ExecutorProfileID: m.ExecutorProfileID, QueryProfileID: q, Snapshots: f.store, Analyzer: f.analyzer, Profiles: f.registry, Appender: app, HistoricalPolicy: f.policy, Uses: f.uses, TempDir: t.TempDir(), LogicalNames: SnapshotLogicalNames{Pin: p, SchemaArtifactDigest: digest, Tables: []LogicalTableName{{TableID: "R.with.dots", Database: "tenant", Table: "events"}, {TableID: "opaque-W", Database: "tenant", Table: "copy"}, {TableID: "untouched-U", Database: "strange.db", Table: "a.b"}}}}
	return f
}
func fixtureProbe(context.Context, rewriter.SnapshotQueryAnalyzer, string, []*pb.SnapshotQueryCatalogTable) error {
	return nil
}
func (f *executionFixture) executor(t *testing.T) *Executor {
	t.Helper()
	e, err := newExecutor(context.Background(), f.opts, fixtureProbe)
	if err != nil {
		t.Fatal(err)
	}
	return e
}
func (f *executionFixture) request() replay.ExecutionRequest {
	return replay.ExecutionRequest{SnapshotQuery: &f.job, SnapshotQueryReferenceID: "fixture-existing-replay-reference"}
}

func TestPrepareTransfersCompleteOutput(t *testing.T) {
	f := newExecutionFixture(t)
	e := f.executor(t)
	f.analyzer.analyzeHook = func(r *pb.AnalyzeSnapshotQueryRequest) {
		if r.Materialize || r.Sql != f.job.Statement.Envelope.Input.SQL || len(r.Catalog) != 3 {
			t.Fatal("analysis input mismatch")
		}
		if r.Catalog[2].Columns[0].DefaultExpression != "now()" {
			t.Fatal("untouched metadata dropped")
		}
	}
	p, err := e.Prepare(context.Background(), f.request())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if p.Result.SnapshotQuery.OutputRowCount != 3 || p.Result.SnapshotQuery.OutputRowsRoot != p.Output.OutputRowsRoot() || p.Result.ComputedStateRoot != p.Manifest().StateRoot {
		t.Fatal("unbound result")
	}
	if p.Job().Statement.Envelope.InputRoot != f.job.Statement.Envelope.InputRoot || p.StatementRoot() != f.records.StatementRoot {
		t.Fatal("lost exact job")
	}
	if f.read.queries != 1 || f.read.rows.closes != 1 || f.read.closes != 1 || f.uses.lease.closes != 1 || f.store.ref != f.uses.ref {
		t.Fatalf("ownership: %+v", f.events)
	}
	if !reflect.DeepEqual(f.events, []string{"acquire", "open", "query", "stream-close", "read-close", "lease-close"}) {
		t.Fatal(f.events)
	}
	rows, err := p.Output.OpenRows()
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	n := 0
	for {
		_, err = rows.Next(context.Background())
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		n++
	}
	if n != 3 {
		t.Fatal(n)
	}
	if len(p.Manifest().Tables) != 3 || p.Manifest().ParentSnapshotID != f.job.PrevSafeSnapshotID {
		t.Fatal("incomplete post-state")
	}
}

func TestPrepareRefusesBeforeOpen(t *testing.T) {
	for _, name := range []string{"empty", "mixed", "empty-payload-variant", "reference", "policy", "zero-decision", "assignment", "fence", "input-root", "pair", "pin", "use", "nil-lease"} {
		t.Run(name, func(t *testing.T) {
			f := newExecutionFixture(t)
			e := f.executor(t)
			r := f.request()
			switch name {
			case "empty":
				r = replay.ExecutionRequest{}
			case "mixed":
				r.Job.BlockSeq = 1
			case "empty-payload-variant":
				r.Statements = []replay.PreparedStatement{}
			case "reference":
				r.SnapshotQueryReferenceID = ""
			case "policy":
				f.policy.err = io.ErrUnexpectedEOF
			case "zero-decision":
				f.policy.decision = HistoricalDecision{}
			case "assignment":
				f.job.Statement.StatementSeq++
			case "fence":
				f.job.Reservation.FencingGeneration++
			case "input-root":
				f.job.Statement.Envelope.InputRoot = replay.DigestString("wrong")
			case "pair":
				f.job.ExecutorProfileID = "other"
			case "pin":
				f.job.PrevSafeSnapshotID = "other"
			case "use":
				f.uses.err = io.ErrClosedPipe
			case "nil-lease":
				f.uses.nilLease = true
			}
			p, err := e.Prepare(context.Background(), r)
			if p != nil || err == nil || f.store.calls != 0 || f.analyzer.analyses != 0 {
				t.Fatalf("accepted: %v %v", p, err)
			}
		})
	}
}

func TestPrepareCleanupAndNoPartialOutput(t *testing.T) {
	sentinel := errors.New("fixture failure")
	for _, stage := range []string{"open", "analysis", "prepare", "query", "stream", "stream-close", "read-close", "lease-close", "cancel"} {
		t.Run(stage, func(t *testing.T) {
			f := newExecutionFixture(t)
			e := f.executor(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch stage {
			case "open":
				f.store.err = sentinel
			case "analysis":
				f.analyzer.analyzeErr = sentinel
			case "prepare":
				f.analyzer.prepareErr = sentinel
			case "query":
				f.read.queryErr = sentinel
			case "stream":
				f.read.rows.endErr = sentinel
			case "stream-close":
				f.read.rows.closeErr = sentinel
			case "read-close":
				f.read.closeErr = sentinel
			case "lease-close":
				f.uses.lease.err = sentinel
			case "cancel":
				f.read.rows.hook = cancel
				sentinel = context.Canceled
			}
			p, err := e.Prepare(ctx, f.request())
			if p != nil || !errors.Is(err, sentinel) {
				t.Fatalf("partial/lost cause: %v %v", p, err)
			}
			wantLease := 1
			if stage == "stream-close" || stage == "read-close" || stage == "open" {
				wantLease = 0
			}
			if f.uses.lease.closes != wantLease {
				t.Fatalf("lease close=%d events=%v", f.uses.lease.closes, f.events)
			}
		})
	}
}

func (f *executionFixture) rebind(t *testing.T) {
	t.Helper()
	var err error
	in := &f.job.Statement.Envelope.Input
	in.Binding.SQLHash = replay.DigestString(in.SQL)
	in.Binding.ReadSetRoot, err = replay.SnapshotQueryReadSetRoot(in.ReadSet)
	if err != nil {
		t.Fatal(err)
	}
	f.job.Statement.Envelope.InputRoot, err = replay.SnapshotQueryInputRoot(*in)
	if err != nil {
		t.Fatal(err)
	}
	f.records.Reservation = f.job.Reservation
	f.records.BlockSeq = f.job.BlockSeq
	f.records.StatementRoot, err = replay.SnapshotQueryStatementRoot(f.job.Statement)
	if err != nil {
		t.Fatal(err)
	}
	f.policy.decision, err = NewHistoricalDecisionFromVerifiedRecords(f.job, f.records)
	if err != nil {
		t.Fatal(err)
	}
}
func (f *executionFixture) commitArtifactFixture(t *testing.T) {
	t.Helper()
	_, _, o, d := signProfile(t, f.read.manifest, f.opts.LogicalNames.Pin, f.read.artifact.Artifact)
	f.read.artifact = o
	f.records.Artifacts.SchemaArtifactDigest = d
	var err error
	f.records.Ready.ArtifactSetRoot, err = f.records.Artifacts.Hash()
	if err != nil {
		t.Fatal(err)
	}
	f.opts.LogicalNames.SchemaArtifactDigest = d
	f.rebind(t)
}
func TestPrepareRejectsUncommittedOuterSchema(t *testing.T) {
	for _, name := range []string{"equal-projection-generation", "different-valid-certificate", "dropped-metadata"} {
		t.Run(name, func(t *testing.T) {
			f := newExecutionFixture(t)
			e := f.executor(t)
			changed := f.read.artifact.Artifact.Clone()
			if name == "equal-projection-generation" {
				changed.Tables[2].Columns[0].DefaultExpression = "now64()"
			}
			if name == "dropped-metadata" {
				changed.Tables[2].Columns[0].Generation = replay.SnapshotQueryColumnGenerationUnspecified
				changed.Tables[2].Columns[0].DefaultExpression = ""
			}
			_, _, f.read.artifact, _ = signProfile(t, f.read.manifest, f.opts.LogicalNames.Pin, changed)
			if name == "different-valid-certificate" {
				digest, err := replay.SnapshotQuerySchemaArtifactDigestV1(changed)
				if err != nil {
					t.Fatal(err)
				}
				signer, err := auth.NewSnapshotSchemaCertificateSigner(strings.Repeat("b", 64))
				if err != nil {
					t.Fatal(err)
				}
				f.read.artifact.AuthorityJWS, err = signer.SignSnapshotSchemaCertificateV1(digest)
				if err != nil {
					t.Fatal(err)
				}
			}
			p, err := e.Prepare(context.Background(), f.request())
			if p != nil || err == nil || f.analyzer.analyses != 0 {
				t.Fatal("O2 substituted for committed O1", err)
			}
			if f.read.closes != 1 || f.uses.lease.closes != 1 {
				t.Fatal("metadata refusal leaked quiescent handles")
			}
		})
	}
}

func TestPrepareProjectionAndEligibilityBeforeAnalysis(t *testing.T) {
	for _, name := range []string{"projection-missing", "projection-column", "appender-missing", "appender-network", "target-generated", "read-generated", "manifest-drop"} {
		t.Run(name, func(t *testing.T) {
			f := newExecutionFixture(t)
			switch name {
			case "projection-missing":
				f.read.schemas = f.read.schemas[:2]
			case "projection-column":
				f.read.schemas = cloneSchemas(f.read.schemas)
				f.read.schemas[0].Columns[0].Type = "UInt64"
			case "appender-missing":
				f.opts.Appender = payloadexec.New(f.opts.NetworkID, f.read.schemas[0])
			case "target-generated":
				f.read.artifact.Artifact.Tables[1].Columns[0].Generation = replay.SnapshotQueryColumnGenerationDefault
				f.read.artifact.Artifact.Tables[1].Columns[0].DefaultExpression = "1"
				f.commitArtifactFixture(t)
			case "read-generated":
				f.read.artifact.Artifact.Tables[0].Columns[0].Generation = replay.SnapshotQueryColumnGenerationDefault
				f.read.artifact.Artifact.Tables[0].Columns[0].DefaultExpression = "1"
				f.commitArtifactFixture(t)
			case "manifest-drop":
				f.read.manifest.Tables = f.read.manifest.Tables[:2]
			}
			e := f.executor(t)
			if name == "appender-network" {
				f.opts.Appender.NetworkID = "mutated"
			}
			p, err := e.Prepare(context.Background(), f.request())
			if p != nil || err == nil || f.analyzer.analyses != 0 {
				t.Fatal("invalid projection/eligibility reached analyzer", err)
			}
		})
	}
}

func TestPrepareClosureAndRestoredRelations(t *testing.T) {
	for _, name := range []string{"analysis-closure", "analysis-columns", "analysis-target", "analysis-profile", "analysis-sql", "prepare-closure", "prepare-columns", "prepare-profile", "prepare-empty", "relations-missing", "relations-extra", "relations-wrong", "relations-empty"} {
		t.Run(name, func(t *testing.T) {
			f := newExecutionFixture(t)
			e := f.executor(t)
			switch name {
			case "analysis-closure":
				f.analyzer.changeAnalysis = func(r *pb.AnalyzeSnapshotQueryResponse) { r.ReadTableIds = nil }
			case "analysis-columns":
				f.analyzer.changeAnalysis = func(r *pb.AnalyzeSnapshotQueryResponse) { r.TargetColumns = []string{"wrong"} }
			case "analysis-target":
				f.analyzer.changeAnalysis = func(r *pb.AnalyzeSnapshotQueryResponse) { r.TargetTableId = "R.with.dots" }
			case "analysis-profile":
				f.analyzer.changeAnalysis = func(r *pb.AnalyzeSnapshotQueryResponse) { r.QueryProfileId = "wrong" }
			case "analysis-sql":
				f.analyzer.changeAnalysis = func(r *pb.AnalyzeSnapshotQueryResponse) { r.SqlAfterMaterialization += " " }
			case "prepare-closure":
				f.analyzer.changePrepare = func(r *pb.PrepareSnapshotQueryResponse) { r.ReadTableIds = nil }
			case "prepare-columns":
				f.analyzer.changePrepare = func(r *pb.PrepareSnapshotQueryResponse) { r.TargetColumns = []string{"wrong"} }
			case "prepare-profile":
				f.analyzer.changePrepare = func(r *pb.PrepareSnapshotQueryResponse) { r.QueryProfileId = "wrong" }
			case "prepare-empty":
				f.analyzer.changePrepare = func(r *pb.PrepareSnapshotQueryResponse) { r.SelectSql = "" }
			case "relations-missing":
				f.read.relations = nil
			case "relations-extra":
				f.read.relations = append(f.read.relations, Relation{TableID: "opaque-W", Database: "scratch", Table: "w"})
			case "relations-wrong":
				f.read.relations[0].TableID = "untouched-U"
			case "relations-empty":
				f.read.relations[0].Database = ""
			}
			p, err := e.Prepare(context.Background(), f.request())
			if p != nil || err == nil || f.read.queries != 0 {
				t.Fatal("invalid closure/restoration queried", err)
			}
		})
	}
}

func TestPrepareCopiesInvocationAcrossDependencies(t *testing.T) {
	f := newExecutionFixture(t)
	f.job.SourceClaim = &replay.SnapshotQueryClaim{CandidateParts: []replay.SnapshotReadPart{{TableID: "claim-fixture"}}}
	original := cloneJob(f.job)
	e := f.executor(t)
	f.policy.hook = func(j replay.SnapshotQueryJob) {
		j.Statement.Envelope.Input.ReadSet.Tables[0].Table = "mutated"
		j.SourceClaim.CandidateParts[0].TableID = "mutated"
		f.job.Statement.Envelope.Input.ReadSet.Tables[0].Database = "caller-changed-after-copy"
	}
	f.uses.hook = func(j replay.SnapshotQueryJob) {
		if j.Statement.Envelope.Input.ReadSet.Tables[0].Table != "events" || j.SourceClaim.CandidateParts[0].TableID != "claim-fixture" {
			t.Fatal("policy mutated owned job")
		}
		j.Statement.Envelope.Input.ReadSet.Tables[0].SchemaHash = "mutated"
	}
	f.store.hook = func(r replay.SnapshotReadSet) {
		if r.Tables[0].Database != "tenant" {
			t.Fatal("caller mutated owned job")
		}
		r.Tables[0].TableID = "mutated"
	}
	f.analyzer.analyzeHook = func(r *pb.AnalyzeSnapshotQueryRequest) { r.Catalog[0].Columns[0].Type = "mutated" }
	f.analyzer.prepareHook = func(r *pb.PrepareSnapshotQueryRequest) {
		if r.Analysis.Catalog[0].Columns[0].Type != "Int64" || r.Bindings[0].TableId != "R.with.dots" {
			t.Fatal("dependency mutated preparation")
		}
		r.Bindings[0].ScratchTable = "mutated"
	}
	p, err := e.Prepare(context.Background(), f.request())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if !reflect.DeepEqual(p.Job(), original) {
		t.Fatal("prepared job does not bind original owned invocation")
	}
	j := p.Job()
	j.Statement.Envelope.Input.ReadSet.Tables[0].Table = "mutated"
	j.SourceClaim.CandidateParts[0].TableID = "mutated"
	m := p.Manifest()
	m.Tables[0].TableID = "mutated"
	if p.Job().Statement.Envelope.Input.ReadSet.Tables[0].Table != "events" || p.Manifest().Tables[0].TableID == "mutated" {
		t.Fatal("mutable evidence accessor")
	}
}

func TestPrepareZeroRowsAndAssignedGlobalSequence(t *testing.T) {
	f := newExecutionFixture(t)
	f.read.rows.rows = nil
	p, err := f.executor(t).Prepare(context.Background(), f.request())
	if err != nil {
		t.Fatal(err)
	}
	if p.Output.RowCount() != 0 || p.Manifest().DataRoot != f.read.manifest.DataRoot || p.Manifest().SafeBlockSeq != f.job.BlockSeq {
		t.Fatal("zero rows lost complete predecessor")
	}
	p.Close()
	f = newExecutionFixture(t)
	p, err = f.executor(t).Prepare(context.Background(), f.request())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if len(p.Result.AffectedParts) != 1 || !strings.Contains(p.Result.AffectedParts[0].PartName, "42") {
		t.Fatal("assigned statement sequence lost", p.Result.AffectedParts)
	}
	rows, err := p.Output.OpenRows()
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for i := uint64(0); i < 3; i++ {
		r, err := rows.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(r.RowID, payloadexec.RowID(f.opts.NetworkID, "opaque-W", f.job.Statement.Envelope.Input.Binding.StatementID, i)) {
			t.Fatal("global canonical row ID mismatch")
		}
	}
}

func TestPrepareCurrentUseGateAndOwnerSelectors(t *testing.T) {
	// Owner classes/principals are opaque to B4. This fixture's exact-match policy
	// is NOT a production registry parser or proof of atomic closing/race behavior.
	for _, ref := range []string{"reservation", "publication", "challenge", "unknown", "spent", "wrong-principal", "wrong-verifier-role", "closing"} {
		t.Run(ref, func(t *testing.T) {
			f := newExecutionFixture(t)
			e := f.executor(t)
			f.uses.err = errors.New("fixture current-use denial")
			r := f.request()
			r.SnapshotQueryReferenceID = ref
			p, err := e.Prepare(context.Background(), r)
			if p != nil || err == nil || f.store.calls != 0 || f.uses.ref != ref || f.policy.calls != 1 {
				t.Fatal("current-use denial bypassed")
			}
		})
	}
	f := newExecutionFixture(t)
	e := f.executor(t)
	f.policy.err = context.DeadlineExceeded
	_, err := e.Prepare(context.Background(), f.request())
	if !errors.Is(err, context.DeadlineExceeded) || f.uses.calls != 0 {
		t.Fatal("historical failure reached admission", err)
	}
}

type fixtureCursor struct {
	closes int
	err    error
	events *[]string
}

func (*fixtureCursor) Next(context.Context) (payloadexec.Row, error) {
	return payloadexec.Row{}, io.EOF
}
func (r *fixtureCursor) Close() error {
	r.closes++
	*r.events = append(*r.events, "cursor-close")
	return r.err
}

type fixtureOutput struct {
	closes int
	err    error
}

func (*fixtureOutput) RowCount() uint64              { return 0 }
func (*fixtureOutput) OutputRowsRoot() string        { return "fixture-root" }
func (*fixtureOutput) TouchedPartitionIDs() []string { return nil }
func (*fixtureOutput) OpenRows() (payloadexec.RowSource, error) {
	return nil, errors.New("fixture does not supply rows")
}
func (o *fixtureOutput) Close() error { o.closes++; return o.err }

func TestInvocationCleanupRetainsUncertainLease(t *testing.T) {
	cause := errors.New("cursor could not prove quiescence")
	f := newExecutionFixture(t)
	cursor := &fixtureCursor{err: cause, events: &f.events}
	stream := &ownedStream{RowStream: f.read.rows}
	err := closeInvocation(f.uses.lease, true, f.read, stream, cursor)
	if !errors.Is(err, cause) || cursor.closes != 1 || f.read.rows.closes != 1 || f.read.closes != 1 || f.uses.lease.closes != 0 {
		t.Fatal("uncertain cursor released lease", err, f.events)
	}
	f = newExecutionFixture(t)
	if err = closeInvocation(f.uses.lease, false, nil, nil, nil); err != nil || f.uses.lease.closes != 1 {
		t.Fatal("no-IO lease was not released")
	}
	// Preserve every cause, even when several owners fail to establish quiescence.
	f = newExecutionFixture(t)
	streamErr := errors.New("stream-close")
	readErr := errors.New("read-close")
	f.read.rows.closeErr = streamErr
	f.read.closeErr = readErr
	err = closeInvocation(f.uses.lease, true, f.read, &ownedStream{RowStream: f.read.rows}, &fixtureCursor{err: cause, events: &f.events})
	if !errors.Is(err, cause) || !errors.Is(err, streamErr) || !errors.Is(err, readErr) || f.uses.lease.closes != 0 {
		t.Fatal("cleanup lost failure or released uncertainty", err)
	}
}

func TestReplayOutputCloseBoundary(t *testing.T) {
	for _, failure := range []error{nil, errors.New("output close failed")} {
		o := &fixtureOutput{err: failure}
		p := &PreparedQueryExecution{Result: replay.ExecutionResult{BlockSeq: 7}, Output: o, ownedOutput: o}
		result, err := replayPrepared(p)
		if failure != nil {
			if !errors.Is(err, failure) || !reflect.DeepEqual(result, replay.ExecutionResult{}) {
				t.Fatal("close failure returned successful result", result, err)
			}
		} else if err != nil || result.BlockSeq != 7 {
			t.Fatal(result, err)
		}
		if err = p.Close(); !errors.Is(err, failure) || o.closes != 1 {
			t.Fatal("output owner not closed exactly once", err, o.closes)
		}
	}
	// Real Replay runs one SELECT, closes all owned resources and removes B3 output.
	f := newExecutionFixture(t)
	result, err := f.executor(t).Replay(context.Background(), f.request())
	if err != nil || result.SnapshotQuery.OutputRowCount != 3 {
		t.Fatal(result, err)
	}
	entries, err := os.ReadDir(f.opts.TempDir)
	if err != nil || len(entries) != 0 || f.read.queries != 1 || f.read.rows.closes != 1 || f.read.closes != 1 || f.uses.lease.closes != 1 {
		t.Fatal("Replay leaked output/handles", entries, err)
	}
}

func TestPrepareUsesFrozenProfileLimits(t *testing.T) {
	for _, limit := range []string{"sql", "descriptor", "rows", "output-bytes", "sort-memory", "spill", "restore"} {
		t.Run(limit, func(t *testing.T) {
			f := newExecutionFixture(t)
			r := &f.registry.profile.Record
			switch limit {
			case "sql":
				r.Limits.MaxSQLBytes = 1
			case "descriptor":
				r.Limits.MaxDescriptorBytes = 1
			case "rows":
				r.Limits.MaxOutputRows = 1
			case "output-bytes":
				r.Limits.MaxOutputBytes = 1
			case "sort-memory":
				r.Limits.MaxSortMemoryBytes = 1
			case "spill":
				r.Limits.MaxSpillBytes = 1
			case "restore":
				r.Limits.MaxRestoreBytes = 1
				f.job.Statement.Envelope.Input.ReadSet.Tables[0].PartitionRoots = []replay.PartitionCommitment{{TableID: "R.with.dots", PartitionID: "all", Root: strings.Repeat("a", 64)}}
				f.job.Statement.Envelope.Input.ReadSet.Tables[0].ActiveParts = []replay.SnapshotReadPart{{TableID: "R.with.dots", PartitionID: "all", PartName: "fixture-part", PartPhysHash: replay.DigestString("physical"), PartRowLtHash: strings.Repeat("a", 64), RowCount: 1, Bytes: 2}}
			}
			q, err := r.Hash()
			if err != nil {
				t.Fatal(err)
			}
			f.registry.profile.QueryProfileID = q
			f.opts.QueryProfileID = q
			f.job.QueryProfileID = q
			f.job.Reservation.QueryProfileID = q
			f.job.Statement.Envelope.Input.Binding.QueryProfileID = q
			f.records.Activation.QueryProfileID = q
			f.rebind(t)
			p, err := f.executor(t).Prepare(context.Background(), f.request())
			if p != nil || err == nil {
				t.Fatal("profile bound not enforced", limit)
			}
			if (limit == "sql" || limit == "descriptor" || limit == "restore") && f.store.calls != 0 {
				t.Fatal("preflight resource bound opened snapshot")
			}
		})
	}
}

func TestPrepareConstantSelectHasNoReadRelations(t *testing.T) {
	f := newExecutionFixture(t)
	f.job.Statement.Envelope.Input.ReadSet.Tables = nil
	f.job.Statement.Envelope.Input.SQL = "INSERT INTO tenant.copy SELECT 7"
	f.read.relations = nil
	f.read.rows.rows = [][]any{{int64(7)}}
	f.rebind(t)
	f.analyzer.changeAnalysis = func(r *pb.AnalyzeSnapshotQueryResponse) { r.ReadTableIds = nil }
	f.analyzer.changePrepare = func(r *pb.PrepareSnapshotQueryResponse) { r.ReadTableIds = nil; r.SelectSql = "SELECT 7" }
	p, err := f.executor(t).Prepare(context.Background(), f.request())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if p.Output.RowCount() != 1 || f.read.queries != 1 {
		t.Fatal("constant SELECT was not executed once")
	}
}

// This strict fixture checks exact supplied job/claim/reference values. It is
// not a real registry, role authenticator, reference grammar or closing-race test.
type boundFixtureUses struct {
	expected  replay.SnapshotQueryJob
	reference string
	lease     QueryUseLease
	calls     int
}

func (u *boundFixtureUses) AcquireQueryUse(_ context.Context, ref string, j replay.SnapshotQueryJob) (QueryUseLease, error) {
	u.calls++
	if ref != u.reference || !reflect.DeepEqual(j, u.expected) {
		return nil, errors.New("fixture exact job/claim/reference mismatch")
	}
	return u.lease, nil
}
func TestPrepareSuppliesExactCurrentUseJobAndClaim(t *testing.T) {
	for _, change := range []string{"none", "claim", "claim-root", "reference"} {
		t.Run(change, func(t *testing.T) {
			f := newExecutionFixture(t)
			f.job.SourceClaim = &replay.SnapshotQueryClaim{SourceNode: "fixed-source", StatementID: f.job.Reservation.StatementID, OutputRowsRoot: replay.DigestString("claim")}
			f.job.SourceClaimRoot = replay.DigestString("claim-root")
			u := &boundFixtureUses{expected: cloneJob(f.job), reference: "fixed-verifier-replay-acquisition", lease: f.uses.lease}
			f.opts.Uses = u
			req := f.request()
			req.SnapshotQueryReferenceID = u.reference
			switch change {
			case "claim":
				f.job.SourceClaim.SourceNode = "other-source"
			case "claim-root":
				f.job.SourceClaimRoot = replay.DigestString("other")
			case "reference":
				req.SnapshotQueryReferenceID = "source-accepted-acquisition"
			}
			p, err := f.executor(t).Prepare(context.Background(), req)
			if change == "none" {
				if err != nil {
					t.Fatal(err)
				}
				p.Close()
			} else if err == nil || p != nil || f.store.calls != 0 {
				t.Fatal("changed current-use binding opened snapshot", err)
			}
			if u.calls != 1 {
				t.Fatal("current-use port did not check exact invocation")
			}
		})
	}
}

func TestPrepareRefusesUnsupportedTouchedTypeButCarriesUntouchedType(t *testing.T) {
	for _, which := range []int{0, 1, 2} {
		t.Run(fmt.Sprint(which), func(t *testing.T) {
			f := newExecutionFixture(t)
			f.read.schemas = cloneSchemas(f.read.schemas)
			f.read.schemas[which].Columns[0].Type = "Array(Int64)"
			f.opts.Appender = payloadexec.New(f.opts.NetworkID, f.read.schemas...)
			m, err := f.opts.Appender.GenesisSnapshot(12, f.job.SchemaSnapshotID, f.job.ExecutorProfileID)
			if err != nil {
				t.Fatal(err)
			}
			p := f.opts.LogicalNames.Pin
			p.SnapshotID = m.SnapshotID
			p.ManifestRoot = m.ManifestRoot
			p.StateRoot = m.StateRoot
			p.SchemaRoot = m.SchemaRoot
			f.opts.LogicalNames.Pin = p
			f.read.manifest = m
			a := &f.read.artifact.Artifact
			a.SnapshotID = p.SnapshotID
			a.ManifestRoot = p.ManifestRoot
			a.SchemaRoot = p.SchemaRoot
			a.Tables[which].Columns[0].Type = "Array(Int64)"
			a.Tables[which].SchemaHash = payloadexec.TableSchemaHash(f.opts.NetworkID, f.read.schemas[which])
			f.job.PrevSafeSnapshotID = p.SnapshotID
			f.job.PrevStateRoot = p.StateRoot
			f.job.Reservation.ReadSnapshot = p
			in := &f.job.Statement.Envelope.Input
			in.Binding.ReadSnapshot = p
			in.Binding.SchemaRoot = p.SchemaRoot
			in.Binding.SchemaHash = a.Tables[1].SchemaHash
			in.ReadSet.ReadSnapshot = p
			in.ReadSet.Tables[0].SchemaHash = a.Tables[0].SchemaHash
			f.records.Ready.SnapshotID = p.SnapshotID
			f.records.Ready.ManifestRoot = p.ManifestRoot
			f.records.Ready.SchemaRoot = p.SchemaRoot
			f.commitArtifactFixture(t)
			prepared, err := f.executor(t).Prepare(context.Background(), f.request())
			if which == 2 {
				if err != nil {
					t.Fatal(err)
				}
				defer prepared.Close()
				if len(prepared.Manifest().Tables) != 3 {
					t.Fatal("untouched unsupported U dropped")
				}
			} else if err == nil || prepared != nil || f.analyzer.analyses != 0 {
				t.Fatal("unsupported touched type analyzed", err)
			}
		})
	}
}

func TestPrepareResultMatchesSharedAppendAtExactAssignment(t *testing.T) {
	f := newExecutionFixture(t)
	p, err := f.executor(t).Prepare(context.Background(), f.request())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	rows, err := p.Output.OpenRows()
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	j := replay.ReplayJob{BlockSeq: f.job.BlockSeq, PrevSafeSnapshotID: f.job.PrevSafeSnapshotID, PrevStateRoot: f.job.PrevStateRoot, SchemaSnapshotID: f.job.SchemaSnapshotID, ExecutorProfileID: f.job.ExecutorProfileID}
	expected, res, err := f.opts.Appender.ApplyRows(context.Background(), cloneManifest(f.read.manifest), j, []payloadexec.StatementRows{{StatementID: f.job.Reservation.StatementID, StatementSeq: f.job.Statement.StatementSeq, TargetTableID: "opaque-W", Rows: rows}})
	if err != nil {
		t.Fatal(err)
	}
	res.SnapshotQuery = p.Result.SnapshotQuery
	if !reflect.DeepEqual(p.Result, res) || !reflect.DeepEqual(p.Manifest(), cloneManifest(expected)) {
		t.Fatal("prepared result diverges from complete shared state assembly")
	}
	// Pure getters preserve complete parts and storage-hint ownership.
	got := p.Manifest()
	for i := range got.Tables {
		for k := range got.Tables[i].ActiveParts {
			got.Tables[i].ActiveParts[k].PartName = "mutated"
		}
	}
	if p.Manifest().ManifestRoot != expected.ManifestRoot {
		t.Fatal("mutable complete manifest")
	}
}

type failedAcquisition struct {
	lease QueryUseLease
	err   error
}

func (u failedAcquisition) AcquireQueryUse(context.Context, string, replay.SnapshotQueryJob) (QueryUseLease, error) {
	return u.lease, u.err
}

type partialStore struct {
	read ReadSnapshot
	err  error
}

func (s partialStore) Open(context.Context, replay.SnapshotPin, replay.SnapshotReadSet, string) (ReadSnapshot, error) {
	return s.read, s.err
}
func TestPrepareAcquisitionAndOpenErrorOwnership(t *testing.T) {
	first := errors.New("acquisition failed")
	last := errors.New("lease close failed")
	f := newExecutionFixture(t)
	f.uses.lease.err = last
	f.opts.Uses = failedAcquisition{lease: f.uses.lease, err: first}
	p, err := f.executor(t).Prepare(context.Background(), f.request())
	if p != nil || !errors.Is(err, first) || !errors.Is(err, last) || f.uses.lease.closes != 1 || f.store.calls != 0 {
		t.Fatal("failed acquisition leaked returned lease", err)
	}
	f = newExecutionFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.uses.hook = func(replay.SnapshotQueryJob) { cancel() }
	p, err = f.executor(t).Prepare(ctx, f.request())
	if p != nil || !errors.Is(err, context.Canceled) || f.store.calls != 0 || f.uses.lease.closes != 1 {
		t.Fatal("canceled admission started IO or leaked lease", err)
	}
	f = newExecutionFixture(t)
	f.opts.Snapshots = partialStore{read: f.read, err: first}
	p, err = f.executor(t).Prepare(context.Background(), f.request())
	if p != nil || !errors.Is(err, first) || f.read.closes != 1 || f.uses.lease.closes != 1 {
		t.Fatal("partial Open handle was not joined", err)
	}
	f = newExecutionFixture(t)
	f.opts.Snapshots = partialStore{}
	p, err = f.executor(t).Prepare(context.Background(), f.request())
	if p != nil || err == nil || f.uses.lease.closes != 0 {
		t.Fatal("nil Open handle claimed quiescence", err)
	}
}
