package storageintegrity

import (
	"context"
	"fmt"
	"strings"
	"sync"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// ArbiterStatementSubmitter adapts the HouseGate intake StatementSubmitter port
// to arbiter-proto's ArbiterIngress.SubmitStatement RPC.
type ArbiterStatementSubmitter struct {
	client pb.ArbiterIngressClient
}

// NewArbiterStatementSubmitter constructs a StatementSubmitter over an
// arbiter-proto client. The caller owns the underlying gRPC connection.
func NewArbiterStatementSubmitter(client pb.ArbiterIngressClient) *ArbiterStatementSubmitter {
	return &ArbiterStatementSubmitter{client: client}
}

// SubmitStatement sends env to ArbiterIngress.SubmitStatement and maps the
// application-level admission code into the existing intake outcome taxonomy.
func (s *ArbiterStatementSubmitter) SubmitStatement(ctx context.Context, env StatementEnvelope) (SubmitOutcome, error) {
	if s == nil || s.client == nil {
		return SubmitOutcome{}, fmt.Errorf("storageintegrity: arbiter ingress client is required")
	}
	req, err := ArbiterStatementEnvelopeToProto(env)
	if err != nil {
		return SubmitOutcome{}, err
	}
	ack, err := s.client.SubmitStatement(ctx, req)
	if err != nil {
		return submitOutcomeFromRPCError(err), nil
	}
	return SubmitOutcomeFromSequencedAck(ack), nil
}

// ArbiterIntakeStatusQuerier adapts ArbiterIngress.GetStatementStatus to the
// HouseGate intake convergence port.
type ArbiterIntakeStatusQuerier struct {
	client pb.ArbiterIngressClient
}

// NewArbiterIntakeStatusQuerier constructs a read-only intake status querier.
// The caller owns the underlying gRPC connection.
func NewArbiterIntakeStatusQuerier(client pb.ArbiterIngressClient) *ArbiterIntakeStatusQuerier {
	return &ArbiterIntakeStatusQuerier{client: client}
}

// QuerySubmitStatus reports whether the original SubmitStatement landed.
// found=true is sufficient even when later FSM processing rejected the
// statement; that later status does not undo the accepted submit.
func (q *ArbiterIntakeStatusQuerier) QuerySubmitStatus(ctx context.Context, statementID string) (SubmitOutcome, error) {
	status, err := q.statementStatus(ctx, statementID)
	if err != nil {
		return SubmitOutcome{}, err
	}
	if !status.GetFound() {
		return SubmitOutcome{Category: OutcomeUnspecified, Reason: "arbiter has no statement record"}, nil
	}
	return SubmitOutcome{Category: OutcomeExactIdempotent, Reason: firstNonEmpty(status.GetStatus(), "arbiter statement found")}, nil
}

// QueryClaimStatus reports a landed claim only when Arbiter exposes both the
// rc_bound bit and the selected source. A found-but-unbound statement remains
// resend-safe; the statement's later FSM status is not a claim rejection log.
func (q *ArbiterIntakeStatusQuerier) QueryClaimStatus(ctx context.Context, statementID string) (ClaimOutcome, error) {
	status, err := q.statementStatus(ctx, statementID)
	if err != nil {
		return ClaimOutcome{}, err
	}
	if !status.GetFound() || !status.GetRcBound() {
		return ClaimOutcome{Category: OutcomeUnspecified, Reason: "arbiter has no bound result claim"}, nil
	}
	source := strings.TrimSpace(status.GetBoundSource())
	if source == "" {
		return ClaimOutcome{}, fmt.Errorf("storageintegrity: arbiter status for %s is rc_bound with empty bound_source", statementID)
	}
	return ClaimOutcome{
		Category:    OutcomeExactIdempotent,
		BoundSource: source,
		Reason:      firstNonEmpty(status.GetStatus(), "arbiter result claim bound"),
	}, nil
}

func (q *ArbiterIntakeStatusQuerier) statementStatus(ctx context.Context, statementID string) (*pb.StatementStatus, error) {
	if q == nil || q.client == nil {
		return nil, fmt.Errorf("storageintegrity: arbiter ingress client is required")
	}
	statementID = strings.TrimSpace(statementID)
	if statementID == "" {
		return nil, fmt.Errorf("storageintegrity: statement id is required for arbiter status query")
	}
	res, err := q.client.GetStatementStatus(ctx, &pb.GetStatementStatusRequest{StatementId: statementID})
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, fmt.Errorf("storageintegrity: arbiter returned nil StatementStatus for %s", statementID)
	}
	rawBoundSource := res.GetBoundSource()
	boundSource := strings.TrimSpace(rawBoundSource)
	if rawBoundSource != boundSource {
		return nil, fmt.Errorf("storageintegrity: arbiter status for %s bound_source has surrounding whitespace", statementID)
	}
	if !res.GetFound() {
		if res.GetStatementSeq() != 0 || strings.TrimSpace(res.GetStatus()) != "" || res.GetRcBound() || boundSource != "" {
			return nil, fmt.Errorf("storageintegrity: arbiter status for %s is not found with statement data set", statementID)
		}
		return res, nil
	}
	if res.GetStatementSeq() == 0 {
		return nil, fmt.Errorf("storageintegrity: arbiter status for %s is found with zero statement_seq", statementID)
	}
	if !res.GetRcBound() && boundSource != "" {
		return nil, fmt.Errorf("storageintegrity: arbiter status for %s has rc_bound=false with non-empty bound_source", statementID)
	}
	return res, nil
}

// ArbiterPayloadStoreWriter adapts the HouseGate ingest PayloadWriter port to
// arbiter-proto's PayloadStore put RPCs. It validates HouseGate's local
// payload commitment before any RPC, fetches deployment limits once, then uses
// the unary inline put or chunked client stream according to those limits.
type ArbiterPayloadStoreWriter struct {
	client pb.PayloadStoreClient

	mu     sync.Mutex
	limits *pb.StoreLimits
}

// NewArbiterPayloadStoreWriter constructs a PayloadWriter over an arbiter-proto
// PayloadStore client. The caller owns the underlying gRPC connection.
func NewArbiterPayloadStoreWriter(client pb.PayloadStoreClient) *ArbiterPayloadStoreWriter {
	return &ArbiterPayloadStoreWriter{client: client}
}

// PutPayload stores payload bytes durably in DA before SubmitStatement can
// carry the returned opaque payload_ref. The payload hash and length are the
// commitment the store and later verifiers must check against the same bytes.
func (w *ArbiterPayloadStoreWriter) PutPayload(ctx context.Context, payload []byte, payloadHash string, payloadLength uint64) (PayloadPutResult, error) {
	if w == nil || w.client == nil {
		return PayloadPutResult{}, fmt.Errorf("storageintegrity: payload store client is required")
	}
	if err := ctx.Err(); err != nil {
		return PayloadPutResult{}, err
	}
	if len(payload) == 0 || payloadLength == 0 {
		return PayloadPutResult{}, fmt.Errorf("storageintegrity: payload bytes and length are required")
	}
	if uint64(len(payload)) != payloadLength {
		return PayloadPutResult{}, fmt.Errorf("storageintegrity: payload length mismatch: got %d bytes, declared %d", len(payload), payloadLength)
	}
	if payloadHash == "" {
		return PayloadPutResult{}, fmt.Errorf("storageintegrity: payload hash is required")
	}
	if got := replay.DigestBytes(payload); got != payloadHash {
		return PayloadPutResult{}, fmt.Errorf("storageintegrity: payload hash mismatch: got %s, declared %s", got, payloadHash)
	}

	limits, err := w.storeLimits(ctx)
	if err != nil {
		return PayloadPutResult{}, err
	}
	if payloadLength > limits.GetMaxPayloadBytes() {
		return PayloadPutResult{}, fmt.Errorf("storageintegrity: payload length %d exceeds store max_payload_bytes %d", payloadLength, limits.GetMaxPayloadBytes())
	}

	header := &pb.PutPayloadHeader{
		PayloadHash:   payloadHash,
		PayloadLength: payloadLength,
	}
	if payloadLength <= limits.GetMaxInlineBytes() {
		res, err := w.client.PutPayloadInline(ctx, &pb.PutPayloadInlineRequest{
			Header:  header,
			Payload: payload,
		})
		if err != nil {
			return PayloadPutResult{}, err
		}
		return PayloadPutResultFromProto(res)
	}

	stream, err := w.client.PutPayload(ctx)
	if err != nil {
		return PayloadPutResult{}, err
	}
	if err := stream.Send(&pb.PutPayloadFrame{
		Frame: &pb.PutPayloadFrame_Header{Header: header},
	}); err != nil {
		return PayloadPutResult{}, err
	}
	maxChunk := int(limits.GetMaxChunkBytes())
	for offset := 0; offset < len(payload); offset += maxChunk {
		end := offset + maxChunk
		if end > len(payload) {
			end = len(payload)
		}
		if err := stream.Send(&pb.PutPayloadFrame{
			Frame: &pb.PutPayloadFrame_Chunk{Chunk: payload[offset:end]},
		}); err != nil {
			return PayloadPutResult{}, err
		}
	}
	res, err := stream.CloseAndRecv()
	if err != nil {
		return PayloadPutResult{}, err
	}
	return PayloadPutResultFromProto(res)
}

func (w *ArbiterPayloadStoreWriter) storeLimits(ctx context.Context) (*pb.StoreLimits, error) {
	w.mu.Lock()
	if w.limits != nil {
		limits := w.limits
		w.mu.Unlock()
		return limits, nil
	}
	w.mu.Unlock()

	limits, err := w.client.GetStoreLimits(ctx, &pb.GetStoreLimitsRequest{})
	if err != nil {
		return nil, err
	}
	if err := validateStoreLimits(limits); err != nil {
		return nil, err
	}

	w.mu.Lock()
	if w.limits == nil {
		w.limits = limits
	}
	limits = w.limits
	w.mu.Unlock()
	return limits, nil
}

func validateStoreLimits(limits *pb.StoreLimits) error {
	if limits == nil {
		return fmt.Errorf("storageintegrity: payload store returned nil limits")
	}
	if limits.GetMaxPayloadBytes() == 0 {
		return fmt.Errorf("storageintegrity: payload store max_payload_bytes must be non-zero")
	}
	if limits.GetMaxChunkBytes() == 0 {
		return fmt.Errorf("storageintegrity: payload store max_chunk_bytes must be non-zero")
	}
	if limits.GetMaxInlineBytes() > limits.GetMaxPayloadBytes() {
		return fmt.Errorf("storageintegrity: payload store max_inline_bytes %d exceeds max_payload_bytes %d", limits.GetMaxInlineBytes(), limits.GetMaxPayloadBytes())
	}
	return nil
}

// PayloadPutResultFromProto maps DA put result codes into HouseGate's
// payload-ref result. Application-level put failures are returned as errors so
// ingress fails closed before unsafe write or SubmitStatement.
func PayloadPutResultFromProto(res *pb.PutPayloadResult) (PayloadPutResult, error) {
	if res == nil {
		return PayloadPutResult{}, fmt.Errorf("storageintegrity: payload store returned nil PutPayloadResult")
	}
	if res.GetCode() != pb.PutCode_PUT_CODE_OK {
		return PayloadPutResult{}, fmt.Errorf("storageintegrity: payload store put rejected: %s: %s", res.GetCode().String(), res.GetMessage())
	}
	if res.GetPayloadRef() == "" {
		return PayloadPutResult{}, fmt.Errorf("storageintegrity: payload store accepted without payload_ref")
	}
	state, err := payloadStateFromProto(res.GetState())
	if err != nil {
		return PayloadPutResult{}, err
	}
	return PayloadPutResult{
		PayloadRef:         res.GetPayloadRef(),
		State:              state,
		LeaseExpiresUnixMS: res.GetLeaseExpiresUnixMs(),
		Deduplicated:       res.GetDeduplicated(),
	}, nil
}

func payloadStateFromProto(state pb.PayloadState) (PayloadState, error) {
	switch state {
	case pb.PayloadState_PAYLOAD_STATE_PENDING:
		return PayloadStatePending, nil
	case pb.PayloadState_PAYLOAD_STATE_AVAILABLE:
		return PayloadStateAvailable, nil
	default:
		return "", fmt.Errorf("storageintegrity: payload store accepted with unsupported state %s", state.String())
	}
}

// ArbiterStatementEnvelopeToProto converts the HouseGate-core envelope to the
// frozen arbiter-proto StatementEnvelopeV2 wire shape.
func ArbiterStatementEnvelopeToProto(env StatementEnvelope) (*pb.StatementEnvelopeV2, error) {
	id, err := parseFlatStatementID(env.StatementID)
	if err != nil {
		return nil, fmt.Errorf("storageintegrity: invalid statement id %q: %w", env.StatementID, err)
	}
	if env.Signer != "" && id.ClientAccount != strings.ToLower(env.Signer) {
		return nil, fmt.Errorf("storageintegrity: statement id account %s does not match signer %s", id.ClientAccount, strings.ToLower(env.Signer))
	}
	if env.StatementKind != KindInsert {
		return nil, fmt.Errorf("storageintegrity: unsupported statement kind %q for arbiter SubmitStatement", env.StatementKind)
	}
	if env.SQL == "" || env.SQLHash == "" {
		return nil, fmt.Errorf("storageintegrity: SQL and SQLHash are required for arbiter SubmitStatement")
	}
	if env.TargetTableID == "" {
		return nil, fmt.Errorf("storageintegrity: target table id is required for arbiter SubmitStatement")
	}
	if env.PayloadRef == "" || env.PayloadHash == "" || env.PayloadLength == 0 {
		return nil, fmt.Errorf("storageintegrity: payload ref/hash/length are required for arbiter SubmitStatement")
	}
	if env.UserJWS == "" {
		return nil, fmt.Errorf("storageintegrity: user JWS is required for arbiter SubmitStatement")
	}
	if env.EnvelopeVersion != EnvelopeVersionV2 {
		return nil, fmt.Errorf("storageintegrity: envelope %s has envelope_version %d, want %d", env.StatementID, env.EnvelopeVersion, EnvelopeVersionV2)
	}
	if strings.TrimSpace(env.NetworkID) == "" || env.SettingsHash == "" || env.SchemaHash == "" || env.RowIDProfileID == "" || env.PayloadEncoding == "" || env.Revision <= 0 || uint64(env.Revision) > uint64(^uint32(0)) {
		return nil, fmt.Errorf("storageintegrity: envelope %s is missing v2 fields (version=%d network=%q settings=%q schema=%q profile=%q format=%q revision=%d)", env.StatementID, env.EnvelopeVersion, env.NetworkID, env.SettingsHash, env.SchemaHash, env.RowIDProfileID, env.PayloadEncoding, env.Revision)
	}
	if env.KeeperShardID != 0 {
		return nil, fmt.Errorf("storageintegrity: envelope %s keeper_shard_id %d, want 0 in v1", env.StatementID, env.KeeperShardID)
	}
	if env.SettingsHash != EmptySettingsHash {
		return nil, fmt.Errorf("storageintegrity: envelope %s settings_hash %q, want empty-settings digest", env.StatementID, env.SettingsHash)
	}
	if env.RowIDProfileID != payloadexec.RowIDProfileID {
		return nil, fmt.Errorf("storageintegrity: envelope %s row_id_profile_id %q, want %q", env.StatementID, env.RowIDProfileID, payloadexec.RowIDProfileID)
	}
	if env.PayloadEncoding != PayloadEncodingClickHouseNativeData {
		return nil, fmt.Errorf("storageintegrity: envelope %s payload format %q, want %q", env.StatementID, env.PayloadEncoding, PayloadEncodingClickHouseNativeData)
	}
	return &pb.StatementEnvelopeV2{
		StatementId: &pb.StatementID{
			ClientAccount: id.ClientAccount,
			ClientSeq:     id.ClientSeq,
			ClientNonce:   id.ClientNonce,
		},
		StatementKind:   pb.StatementKind_STATEMENT_KIND_INSERT,
		Sql:             env.SQL,
		SqlHash:         env.SQLHash,
		SettingsHash:    env.SettingsHash,
		PayloadRef:      env.PayloadRef,
		PayloadHash:     env.PayloadHash,
		PayloadLength:   env.PayloadLength,
		TargetTableId:   env.TargetTableID,
		UserJws:         env.UserJWS,
		EnvelopeVersion: env.EnvelopeVersion,
		NetworkId:       env.NetworkID,
		KeeperShardId:   env.KeeperShardID,
		PayloadFormat:   env.PayloadEncoding,
		ClientRevision:  uint32(env.Revision),
		SchemaHash:      env.SchemaHash,
		RowIdProfileId:  env.RowIDProfileID,
	}, nil
}

// SubmitOutcomeFromSequencedAck maps Arbiter's application-level admission
// result into the existing staged-intake outcome categories.
func SubmitOutcomeFromSequencedAck(ack *pb.SequencedAck) SubmitOutcome {
	if ack == nil {
		return SubmitOutcome{Category: OutcomeUnknown, Reason: "arbiter returned nil SequencedAck"}
	}
	reason := ack.GetMessage()
	switch ack.GetCode() {
	case pb.AdmissionCode_ADMISSION_CODE_ACCEPTED:
		if ack.GetStatementSeq() == 0 {
			return SubmitOutcome{Category: OutcomeUnknown, Reason: "arbiter accepted without statement_seq"}
		}
		return SubmitOutcome{Category: OutcomeAccepted, Reason: reason}
	case pb.AdmissionCode_ADMISSION_CODE_DUPLICATE_CLIENT_SEQ,
		pb.AdmissionCode_ADMISSION_CODE_SCHEMA_NOT_ALLOWED,
		pb.AdmissionCode_ADMISSION_CODE_KIND_NOT_ADMITTED,
		pb.AdmissionCode_ADMISSION_CODE_INVALID_SIGNATURE,
		pb.AdmissionCode_ADMISSION_CODE_INVALID_PROOF,
		pb.AdmissionCode_ADMISSION_CODE_MALFORMED,
		pb.AdmissionCode_ADMISSION_CODE_GAP_BUDGET_EXCEEDED:
		return SubmitOutcome{Category: OutcomeTerminalReject, Reason: firstNonEmpty(reason, ack.GetCode().String())}
	default:
		return SubmitOutcome{Category: OutcomeUnknown, Reason: firstNonEmpty(reason, ack.GetCode().String())}
	}
}

func submitOutcomeFromRPCError(err error) SubmitOutcome {
	st, ok := status.FromError(err)
	if !ok {
		return SubmitOutcome{Category: OutcomeUnknown, Reason: err.Error()}
	}
	reason := firstNonEmpty(st.Message(), st.Code().String())
	switch st.Code() {
	case codes.FailedPrecondition, codes.Unavailable, codes.ResourceExhausted, codes.Aborted:
		return SubmitOutcome{Category: OutcomeRetryable, Reason: reason}
	case codes.DeadlineExceeded, codes.Canceled, codes.Unknown:
		return SubmitOutcome{Category: OutcomeUnknown, Reason: reason}
	case codes.InvalidArgument, codes.PermissionDenied, codes.Unauthenticated, codes.OutOfRange:
		return SubmitOutcome{Category: OutcomeTerminalReject, Reason: reason}
	default:
		return SubmitOutcome{Category: OutcomeUnknown, Reason: reason}
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
