package rewriter

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	rewritergo "github.com/housegate/rewriter-go"
	pb "github.com/housegate/rewriter-proto/gen/pb"
	"google.golang.org/protobuf/proto"
)

const (
	snapshotQueryContractVersion    = 1
	snapshotQueryMaxSQLBytes        = 65536
	snapshotQueryMaxDescriptorBytes = 65536
	snapshotQueryDefaultTimeout     = 5 * time.Second
)

var ErrSnapshotQueryAnalyzerClosed = errors.New("snapshot query analyzer is closed")

// SnapshotQueryError reports a fail-closed snapshot-query refusal. Code is the
// acknowledged application code when one was available. Local input validation
// uses INVALID_INPUT; transport and acknowledgement failures use UNSPECIFIED.
type SnapshotQueryError struct {
	Operation string
	Code      pb.SnapshotQueryCode
	Message   string
	Cause     error
	// acknowledged is set only after validating a backend rejection and its empty outputs.
	acknowledged bool
}

func (e *SnapshotQueryError) Error() string {
	message := e.Message
	if message == "" {
		message = "snapshot query request rejected"
	}
	if e.Operation == "" {
		return message
	}
	return e.Operation + ": " + message
}

func (e *SnapshotQueryError) Unwrap() error { return e.Cause }

// SnapshotQueryAnalyzer is the fail-closed snapshot-query analysis and scratch
// preparation surface. It owns a backend independent of the ordinary rewriter.
type SnapshotQueryAnalyzer interface {
	AnalyzeSnapshotQuery(context.Context, *pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error)
	ClassifySnapshotQuery(context.Context, *pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error)
	PrepareSnapshotQuery(context.Context, *pb.PrepareSnapshotQueryRequest) (*pb.PrepareSnapshotQueryResponse, error)
	Close() error
}

type snapshotQueryAnalyzer struct {
	mu      sync.RWMutex
	backend backend
	// timeout bounds every RPC. Native FFI observes the deadline only after
	// returning; the read lock deliberately preserves its handle until then.
	timeout  time.Duration
	closed   bool
	closeErr error
}

func newSnapshotQueryAnalyzer(be backend) *snapshotQueryAnalyzer {
	return newSnapshotQueryAnalyzerWithTimeout(be, 0)
}

func newSnapshotQueryAnalyzerWithTimeout(be backend, timeout time.Duration) *snapshotQueryAnalyzer {
	if timeout == 0 {
		timeout = snapshotQueryDefaultTimeout
	}
	return &snapshotQueryAnalyzer{backend: be, timeout: timeout}
}

// NewSnapshotQueryAnalyzer constructs a separate snapshot-query backend. Native
// mode requires explicit measured library and profile paths; it never falls back
// to the ordinary native loader's environment/default search.
func NewSnapshotQueryAnalyzer(opts Options) (SnapshotQueryAnalyzer, error) {
	var (
		be  backend
		err error
	)
	switch opts.Engine {
	case "", EngineGRPC:
		be, err = newGRPCBackend(opts)
	case EngineNative:
		if strings.TrimSpace(opts.NativeLibraryPath) == "" || strings.TrimSpace(opts.SnapshotQueryProfilePath) == "" {
			return nil, fmt.Errorf("snapshot query native analyzer requires explicit native_library_path and snapshot_query_profile_path")
		}
		var svc *rewritergo.Service
		svc, err = rewritergo.NewServiceWithSnapshotQueryProfiles(opts.NativeLibraryPath, opts.SnapshotQueryProfilePath)
		be = svc
	default:
		return nil, fmt.Errorf("unknown rewriter engine %q (want %q or %q)", opts.Engine, EngineGRPC, EngineNative)
	}
	if err != nil {
		return nil, fmt.Errorf("create snapshot query analyzer: %w", err)
	}
	return newSnapshotQueryAnalyzerWithTimeout(be, opts.Timeout), nil
}

func (a *snapshotQueryAnalyzer) AnalyzeSnapshotQuery(ctx context.Context, req *pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error) {
	if err := validateSnapshotAnalyzeRequest(req); err != nil {
		return nil, snapshotInputError("analyze snapshot query", err)
	}
	resp, err := a.callAnalyze(ctx, req)
	if err != nil {
		return nil, err
	}
	if err := validateAnalyzeResponse(req, resp, false); err != nil {
		return nil, err
	}
	return resp, nil
}

func (a *snapshotQueryAnalyzer) ClassifySnapshotQuery(ctx context.Context, req *pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error) {
	if err := validateSnapshotAnalyzeRequest(req); err != nil {
		return nil, snapshotInputError("classify snapshot query", err)
	}
	resp, err := a.callAnalyze(ctx, req)
	if err != nil {
		return nil, err
	}
	if err := validateAnalyzeResponse(req, resp, true); err != nil {
		return nil, err
	}
	return resp, nil
}

func (a *snapshotQueryAnalyzer) callAnalyze(ctx context.Context, req *pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.closed {
		return nil, &SnapshotQueryError{Operation: "analyze snapshot query", Cause: ErrSnapshotQueryAnalyzerClosed, Message: ErrSnapshotQueryAnalyzerClosed.Error()}
	}
	callCtx, cancel := context.WithTimeout(ctx, a.timeout)
	defer cancel()
	resp, err := a.backend.AnalyzeSnapshotQuery(callCtx, req)
	if err != nil {
		return nil, &SnapshotQueryError{Operation: "analyze snapshot query", Cause: err, Message: "backend call failed"}
	}
	return resp, nil
}

func (a *snapshotQueryAnalyzer) PrepareSnapshotQuery(ctx context.Context, req *pb.PrepareSnapshotQueryRequest) (*pb.PrepareSnapshotQueryResponse, error) {
	if err := validateSnapshotPrepareRequest(req); err != nil {
		return nil, snapshotInputError("prepare snapshot query", err)
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.closed {
		return nil, &SnapshotQueryError{Operation: "prepare snapshot query", Cause: ErrSnapshotQueryAnalyzerClosed, Message: ErrSnapshotQueryAnalyzerClosed.Error()}
	}
	callCtx, cancel := context.WithTimeout(ctx, a.timeout)
	defer cancel()
	resp, err := a.backend.PrepareSnapshotQuery(callCtx, req)
	if err != nil {
		return nil, &SnapshotQueryError{Operation: "prepare snapshot query", Cause: err, Message: "backend call failed"}
	}
	if err := validatePrepareResponse(req, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (a *snapshotQueryAnalyzer) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return a.closeErr
	}
	a.closed = true
	a.closeErr = a.backend.Close()
	return a.closeErr
}

func snapshotInputError(operation string, err error) error {
	return &SnapshotQueryError{Operation: operation, Code: pb.SnapshotQueryCode_INVALID_INPUT, Cause: err, Message: err.Error()}
}

func validateSnapshotAnalyzeRequest(req *pb.AnalyzeSnapshotQueryRequest) error {
	if req == nil {
		return fmt.Errorf("request is required")
	}
	if req.GetContractVersion() != snapshotQueryContractVersion {
		return fmt.Errorf("contract version must be %d", snapshotQueryContractVersion)
	}
	if !snapshotQueryDigest(req.GetQueryProfileId()) {
		return fmt.Errorf("query profile id is invalid")
	}
	if len(req.GetSql()) > snapshotQueryMaxSQLBytes {
		return fmt.Errorf("SQL exceeds the wrapper limit")
	}
	if proto.Size(req)-len(req.GetSql()) > snapshotQueryMaxDescriptorBytes {
		return fmt.Errorf("descriptor exceeds the wrapper limit")
	}
	if len(req.GetCatalog()) == 0 {
		return fmt.Errorf("catalog is required")
	}
	seenTables := make(map[string]struct{}, len(req.GetCatalog()))
	for _, table := range req.GetCatalog() {
		if table == nil || strings.TrimSpace(table.GetDatabase()) == "" || strings.TrimSpace(table.GetTable()) == "" || strings.TrimSpace(table.GetTableId()) == "" || !snapshotQueryDigest(table.GetSchemaHash()) || len(table.GetColumns()) == 0 {
			return fmt.Errorf("catalog table metadata is incomplete")
		}
		if _, duplicate := seenTables[table.GetTableId()]; duplicate {
			return fmt.Errorf("catalog table identity is duplicated")
		}
		seenTables[table.GetTableId()] = struct{}{}
		// Only the exact-Q backend knows which tables the AST touches; generation
		// eligibility must not exclude untouched tables from the complete catalog.
		seenColumns := make(map[string]struct{}, len(table.GetColumns()))
		for _, column := range table.GetColumns() {
			if column == nil || strings.TrimSpace(column.GetName()) == "" || strings.TrimSpace(column.GetType()) == "" {
				return fmt.Errorf("catalog column metadata is incomplete")
			}
			if _, duplicate := seenColumns[column.GetName()]; duplicate {
				return fmt.Errorf("catalog column is duplicated")
			}
			seenColumns[column.GetName()] = struct{}{}
		}
	}
	return nil
}

func validateSnapshotPrepareRequest(req *pb.PrepareSnapshotQueryRequest) error {
	if req == nil || req.GetAnalysis() == nil {
		return fmt.Errorf("analysis request is required")
	}
	if err := validateSnapshotAnalyzeRequest(req.GetAnalysis()); err != nil {
		return err
	}
	if req.GetAnalysis().GetMaterialize() {
		return fmt.Errorf("preparation requires nonmaterializing analysis")
	}
	if proto.Size(req)-len(req.GetAnalysis().GetSql()) > snapshotQueryMaxDescriptorBytes {
		return fmt.Errorf("descriptor exceeds the wrapper limit")
	}
	seenTables := make(map[string]struct{}, len(req.GetBindings()))
	seenScratch := make(map[string]struct{}, len(req.GetBindings()))
	for _, binding := range req.GetBindings() {
		if binding == nil || strings.TrimSpace(binding.GetTableId()) == "" || strings.TrimSpace(binding.GetScratchDatabase()) == "" || strings.TrimSpace(binding.GetScratchTable()) == "" {
			return fmt.Errorf("scratch binding is incomplete")
		}
		if _, duplicate := seenTables[binding.GetTableId()]; duplicate {
			return fmt.Errorf("scratch table identity is duplicated")
		}
		key := binding.GetScratchDatabase() + "\x00" + binding.GetScratchTable()
		if _, duplicate := seenScratch[key]; duplicate {
			return fmt.Errorf("scratch location is duplicated")
		}
		seenTables[binding.GetTableId()] = struct{}{}
		seenScratch[key] = struct{}{}
	}
	return nil
}

func validateAnalyzeResponse(req *pb.AnalyzeSnapshotQueryRequest, resp *pb.AnalyzeSnapshotQueryResponse, allowOrdinary bool) error {
	if resp == nil || resp.GetContractVersion() != snapshotQueryContractVersion || resp.GetQueryProfileId() != req.GetQueryProfileId() {
		return &SnapshotQueryError{Operation: "analyze snapshot query", Message: "snapshot query analysis did not acknowledge the reserved profile"}
	}
	if len(resp.GetSqlAfterMaterialization()) > snapshotQueryMaxSQLBytes || proto.Size(resp)-len(resp.GetSqlAfterMaterialization()) > snapshotQueryMaxDescriptorBytes {
		return &SnapshotQueryError{Operation: "analyze snapshot query", Message: "snapshot query analysis response exceeds the wrapper limit"}
	}
	if resp.GetCode() == pb.SnapshotQueryCode_NOT_SNAPSHOT_QUERY && allowOrdinary {
		if resp.GetMessage() != "" || resp.GetSqlAfterMaterialization() != "" || resp.GetTargetTableId() != "" || len(resp.GetTargetColumns()) != 0 || len(resp.GetReadTableIds()) != 0 {
			return &SnapshotQueryError{Operation: "analyze snapshot query", Code: resp.GetCode(), Message: "ordinary classification returned executable output"}
		}
		return nil
	}
	if resp.GetCode() != pb.SnapshotQueryCode_SUCCESS {
		if resp.GetSqlAfterMaterialization() != "" || resp.GetTargetTableId() != "" || len(resp.GetTargetColumns()) != 0 || len(resp.GetReadTableIds()) != 0 {
			return &SnapshotQueryError{Operation: "analyze snapshot query", Message: "rejected analysis returned executable output"}
		}
		return &SnapshotQueryError{Operation: "analyze snapshot query", acknowledged: true, Code: resp.GetCode(), Message: responseMessage(resp.GetMessage(), "snapshot query analysis did not acknowledge the reserved profile")}
	}
	if err := validateSnapshotOutputIdentity(req.GetCatalog(), resp.GetTargetTableId(), resp.GetTargetColumns(), resp.GetReadTableIds()); err != nil {
		return snapshotInputError("analyze snapshot query response", err)
	}
	if resp.GetMessage() != "" || strings.TrimSpace(resp.GetSqlAfterMaterialization()) == "" {
		return &SnapshotQueryError{Operation: "analyze snapshot query", Code: resp.GetCode(), Message: "successful analysis response is structurally invalid"}
	}
	return nil
}

func validatePrepareResponse(req *pb.PrepareSnapshotQueryRequest, resp *pb.PrepareSnapshotQueryResponse) error {
	profileID := req.GetAnalysis().GetQueryProfileId()
	if resp == nil || resp.GetContractVersion() != snapshotQueryContractVersion || resp.GetQueryProfileId() != profileID {
		return &SnapshotQueryError{Operation: "prepare snapshot query", Message: "snapshot query preparation did not acknowledge the reserved profile"}
	}
	if len(resp.GetSelectSql()) > snapshotQueryMaxSQLBytes || proto.Size(resp)-len(resp.GetSelectSql()) > snapshotQueryMaxDescriptorBytes {
		return &SnapshotQueryError{Operation: "prepare snapshot query", Message: "snapshot query preparation response exceeds the wrapper limit"}
	}
	if resp.GetCode() != pb.SnapshotQueryCode_SUCCESS {
		return &SnapshotQueryError{Operation: "prepare snapshot query", Code: resp.GetCode(), Message: responseMessage(resp.GetMessage(), "snapshot query preparation did not acknowledge the reserved profile")}
	}
	if err := validateSnapshotOutputIdentity(req.GetAnalysis().GetCatalog(), resp.GetTargetTableId(), resp.GetTargetColumns(), resp.GetReadTableIds()); err != nil {
		return snapshotInputError("prepare snapshot query response", err)
	}
	var target *pb.SnapshotQueryCatalogTable
	for _, table := range req.GetAnalysis().GetCatalog() {
		if table.GetTableId() == resp.GetTargetTableId() {
			target = table
			break
		}
	}
	if target == nil || len(resp.GetTargetColumns()) != len(target.GetColumns()) {
		return &SnapshotQueryError{Operation: "prepare snapshot query", Code: resp.GetCode(), Message: "preparation target columns do not match authenticated schema order"}
	}
	for i, column := range target.GetColumns() {
		if resp.GetTargetColumns()[i] != column.GetName() {
			return &SnapshotQueryError{Operation: "prepare snapshot query", Code: resp.GetCode(), Message: "preparation target columns do not match authenticated schema order"}
		}
	}
	if resp.GetMessage() != "" || strings.TrimSpace(resp.GetSelectSql()) == "" {
		return &SnapshotQueryError{Operation: "prepare snapshot query", Code: resp.GetCode(), Message: "successful preparation response is structurally invalid"}
	}
	if len(resp.GetReadTableIds()) != len(req.GetBindings()) {
		return &SnapshotQueryError{Operation: "prepare snapshot query", Code: resp.GetCode(), Message: "preparation response does not match scratch bindings"}
	}
	for i, tableID := range resp.GetReadTableIds() {
		if tableID != req.GetBindings()[i].GetTableId() {
			return &SnapshotQueryError{Operation: "prepare snapshot query", Code: resp.GetCode(), Message: "preparation response does not match scratch bindings"}
		}
	}
	return nil
}

func validateSnapshotOutputIdentity(catalog []*pb.SnapshotQueryCatalogTable, targetID string, targetColumns, readIDs []string) error {
	tables := make(map[string]*pb.SnapshotQueryCatalogTable, len(catalog))
	for _, table := range catalog {
		tables[table.GetTableId()] = table
	}
	target := tables[targetID]
	if target == nil || len(targetColumns) == 0 {
		return fmt.Errorf("response target identity is absent from the catalog")
	}
	columns := make(map[string]struct{}, len(target.GetColumns()))
	for _, column := range target.GetColumns() {
		columns[column.GetName()] = struct{}{}
	}
	seenColumns := make(map[string]struct{}, len(targetColumns))
	for _, column := range targetColumns {
		if _, ok := columns[column]; !ok {
			return fmt.Errorf("response target column is absent from the catalog")
		}
		if _, duplicate := seenColumns[column]; duplicate {
			return fmt.Errorf("response target column is duplicated")
		}
		seenColumns[column] = struct{}{}
	}
	previous := ""
	for _, tableID := range readIDs {
		if _, ok := tables[tableID]; !ok || previous >= tableID {
			return fmt.Errorf("response read identity is invalid")
		}
		previous = tableID
	}
	return nil
}

func snapshotQueryDigest(value string) bool {
	if len(value) != 66 || !strings.HasPrefix(value, "0x") {
		return false
	}
	for _, c := range value[2:] {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func responseMessage(message, fallback string) string {
	if message != "" {
		return message
	}
	return fallback
}

// ProbeSnapshotQuery checks exact-Q behavior without creating reservations or
// mutating unsafe data. The supplied catalog remains caller-authenticated data;
// this probe does not claim it is published.
func ProbeSnapshotQuery(ctx context.Context, analyzer SnapshotQueryAnalyzer, profileID string, catalog []*pb.SnapshotQueryCatalogTable) error {
	if analyzer == nil {
		return fmt.Errorf("snapshot query capability probe: analyzer is required")
	}
	request := func(sql string) *pb.AnalyzeSnapshotQueryRequest {
		return &pb.AnalyzeSnapshotQueryRequest{ContractVersion: 1, QueryProfileId: profileID, Sql: sql, LogicalDatabase: "tenant", Catalog: proto.Clone(&pb.AnalyzeSnapshotQueryRequest{Catalog: catalog}).(*pb.AnalyzeSnapshotQueryRequest).Catalog}
	}
	if _, err := analyzer.AnalyzeSnapshotQuery(ctx, request("INSERT INTO tenant.copy SELECT 7")); err != nil {
		return fmt.Errorf("snapshot query capability probe: positive analysis: %w", err)
	}
	for _, probe := range []struct {
		name string
		sql  string
		code pb.SnapshotQueryCode
	}{
		{"hidden source", "INSERT INTO tenant.copy SELECT value FROM tenant.events WHERE value IN (SELECT value FROM ordinary.secret)", pb.SnapshotQueryCode_UNSUPPORTED},
		{"nested limit", "INSERT INTO tenant.copy SELECT value FROM (SELECT value FROM tenant.events LIMIT 1)", pb.SnapshotQueryCode_UNSUPPORTED},
		{"residual volatility", "INSERT INTO tenant.copy SELECT rand()", pb.SnapshotQueryCode_MATERIALIZATION_FAILED},
	} {
		if err := expectSnapshotProbeRejection(ctx, analyzer, request(probe.sql), probe.code); err != nil {
			return fmt.Errorf("snapshot query capability probe: %s: %w", probe.name, err)
		}
	}
	missing := request("INSERT INTO tenant.copy SELECT 7")
	missing.Catalog[0].Columns[0].Generation = pb.SnapshotQueryColumnGeneration_SNAPSHOT_QUERY_COLUMN_GENERATION_UNSPECIFIED
	if err := expectSnapshotProbeRejection(ctx, analyzer, missing, pb.SnapshotQueryCode_UNSUPPORTED); err != nil {
		return fmt.Errorf("snapshot query capability probe: missing generation: %w", err)
	}
	defaultTarget := request("INSERT INTO tenant.copy SELECT 7")
	defaultTarget.Catalog[0].Columns[0].Generation = pb.SnapshotQueryColumnGeneration_SNAPSHOT_QUERY_COLUMN_GENERATION_DEFAULT
	defaultTarget.Catalog[0].Columns[0].DefaultExpression = "7"
	if err := expectSnapshotProbeRejection(ctx, analyzer, defaultTarget, pb.SnapshotQueryCode_UNSUPPORTED); err != nil {
		return fmt.Errorf("snapshot query capability probe: default target: %w", err)
	}
	ordinary := request("SELECT value FROM tenant.events")
	classification, err := analyzer.ClassifySnapshotQuery(ctx, ordinary)
	if err != nil {
		return fmt.Errorf("snapshot query capability probe: ordinary classification: %w", err)
	}
	if classification.GetCode() != pb.SnapshotQueryCode_NOT_SNAPSHOT_QUERY {
		return fmt.Errorf("snapshot query capability probe: ordinary classification code=%s, want %s", classification.GetCode(), pb.SnapshotQueryCode_NOT_SNAPSHOT_QUERY)
	}
	final, err := analyzer.AnalyzeSnapshotQuery(ctx, ordinary)
	if final != nil {
		return fmt.Errorf("snapshot query capability probe: final ordinary analysis returned a payload")
	}
	var typed *SnapshotQueryError
	if !errors.As(err, &typed) || !typed.acknowledged || typed.Code != pb.SnapshotQueryCode_NOT_SNAPSHOT_QUERY {
		return fmt.Errorf("snapshot query capability probe: final ordinary analysis got %w, want typed %s refusal", err, pb.SnapshotQueryCode_NOT_SNAPSHOT_QUERY)
	}
	return nil
}

func expectSnapshotProbeRejection(ctx context.Context, analyzer SnapshotQueryAnalyzer, req *pb.AnalyzeSnapshotQueryRequest, code pb.SnapshotQueryCode) error {
	_, err := analyzer.AnalyzeSnapshotQuery(ctx, req)
	if err == nil {
		return errors.New("unexpected success")
	}
	var typed *SnapshotQueryError
	if !errors.As(err, &typed) || !typed.acknowledged || typed.Code != code {
		return fmt.Errorf("got %w, want code %s", err, code)
	}
	return nil
}
