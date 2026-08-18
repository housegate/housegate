package storageintegrity

import (
	"bytes"
	"context"
	"strings"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

func TestArbiterStatementEnvelopeToProtoMapsFields(t *testing.T) {
	env := arbiterProtoEnvelopeFixture()

	got, err := ArbiterStatementEnvelopeToProto(env)
	if err != nil {
		t.Fatalf("ArbiterStatementEnvelopeToProto: %v", err)
	}
	if got.GetStatementId().GetClientAccount() != "0xabc" {
		t.Fatalf("client_account = %q", got.GetStatementId().GetClientAccount())
	}
	if got.GetStatementId().GetClientSeq() != 7 {
		t.Fatalf("client_seq = %d", got.GetStatementId().GetClientSeq())
	}
	if got.GetStatementId().GetClientNonce() != "nonce-7" {
		t.Fatalf("client_nonce = %q", got.GetStatementId().GetClientNonce())
	}
	if got.GetStatementKind() != pb.StatementKind_STATEMENT_KIND_INSERT {
		t.Fatalf("statement_kind = %v", got.GetStatementKind())
	}
	if got.GetSql() != env.SQL || got.GetSqlHash() != env.SQLHash {
		t.Fatalf("sql/hash = %q/%q", got.GetSql(), got.GetSqlHash())
	}
	if got.GetPayloadRef() != env.PayloadRef || got.GetPayloadHash() != env.PayloadHash || got.GetPayloadLength() != env.PayloadLength {
		t.Fatalf("payload identity = %q/%q/%d", got.GetPayloadRef(), got.GetPayloadHash(), got.GetPayloadLength())
	}
	if got.GetTargetTableId() != env.TargetTableID || got.GetUserJws() != env.UserJWS {
		t.Fatalf("target/jws = %q/%q", got.GetTargetTableId(), got.GetUserJws())
	}
	if got.GetSettingsHash() != env.SettingsHash {
		t.Fatalf("settings_hash = %q, want %q", got.GetSettingsHash(), env.SettingsHash)
	}
}

func TestArbiterStatementEnvelopeToProto_FillsEveryV2Field(t *testing.T) {
	adm := validNativeAdmissionV2(t)
	env, err := EnvelopeFromAdmission(adm)
	if err != nil {
		t.Fatal(err)
	}
	msg, err := ArbiterStatementEnvelopeToProto(env)
	if err != nil {
		t.Fatalf("ArbiterStatementEnvelopeToProto: %v", err)
	}
	if msg.GetEnvelopeVersion() != EnvelopeVersionV2 || msg.GetNetworkId() != "testnet-v2" || msg.GetKeeperShardId() != 0 ||
		msg.GetPayloadFormat() != PayloadEncodingClickHouseNativeData || msg.GetClientRevision() != 54460 ||
		msg.GetSchemaHash() != adm.SchemaHash || msg.GetRowIdProfileId() != payloadexec.RowIDProfileID ||
		msg.GetSettingsHash() != EmptySettingsHash {
		t.Fatalf("proto v2 fields: %+v", msg)
	}
	if msg.GetSettingsHash() == "" {
		t.Fatal("settings_hash must no longer be empty")
	}
}

func TestArbiterStatementEnvelopeToProto_RejectsV2Violations(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*StatementEnvelope)
		want   string
	}{
		{"wrong envelope version", func(e *StatementEnvelope) { e.EnvelopeVersion = 1 }, "envelope_version"},
		{"blank network id", func(e *StatementEnvelope) { e.NetworkID = " \t" }, "missing v2 fields"},
		{"non-zero shard", func(e *StatementEnvelope) { e.KeeperShardID = 1 }, "keeper_shard_id"},
		{"wrong settings hash", func(e *StatementEnvelope) { e.SettingsHash = replay.DigestString("x") }, "settings_hash"},
		{"missing schema hash", func(e *StatementEnvelope) { e.SchemaHash = "" }, "missing v2 fields"},
		{"wrong row profile", func(e *StatementEnvelope) { e.RowIDProfileID = "housegate-row-id-v0" }, "row_id_profile_id"},
		{"wrong payload format", func(e *StatementEnvelope) { e.PayloadEncoding = EncodingCSVWithNames }, "payload format"},
		{"negative revision", func(e *StatementEnvelope) { e.Revision = -1 }, "missing v2 fields"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := arbiterProtoEnvelopeFixture()
			tc.mutate(&env)
			_, err := ArbiterStatementEnvelopeToProto(env)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestArbiterStatementSubmitterCallsProtoClient(t *testing.T) {
	client := &recordingArbiterIngressClient{
		ack: &pb.SequencedAck{
			Code:         pb.AdmissionCode_ADMISSION_CODE_ACCEPTED,
			StatementSeq: 42,
		},
	}
	submitter := NewArbiterStatementSubmitter(client)

	out, err := submitter.SubmitStatement(context.Background(), arbiterProtoEnvelopeFixture())
	if err != nil {
		t.Fatalf("SubmitStatement: %v", err)
	}
	if out.Category != OutcomeAccepted {
		t.Fatalf("outcome = %v, want accepted", out.Category)
	}
	if client.calls != 1 {
		t.Fatalf("client calls = %d, want 1", client.calls)
	}
	if client.last.GetStatementId().GetClientSeq() != 7 {
		t.Fatalf("submitted client_seq = %d", client.last.GetStatementId().GetClientSeq())
	}
}

func TestArbiterStatementSubmitterMapsAdmissionRejects(t *testing.T) {
	client := &recordingArbiterIngressClient{
		ack: &pb.SequencedAck{
			Code:    pb.AdmissionCode_ADMISSION_CODE_DUPLICATE_CLIENT_SEQ,
			Message: "conflicting client_seq",
		},
	}
	submitter := NewArbiterStatementSubmitter(client)

	out, err := submitter.SubmitStatement(context.Background(), arbiterProtoEnvelopeFixture())
	if err != nil {
		t.Fatalf("SubmitStatement: %v", err)
	}
	if out.Category != OutcomeTerminalReject {
		t.Fatalf("outcome = %v, want terminal reject", out.Category)
	}
	if out.Reason != "conflicting client_seq" {
		t.Fatalf("reason = %q", out.Reason)
	}
}

func TestArbiterStatementSubmitterMapsNotLeaderAsRetryable(t *testing.T) {
	client := &recordingArbiterIngressClient{
		err: status.Error(codes.FailedPrecondition, "not leader"),
	}
	submitter := NewArbiterStatementSubmitter(client)

	out, err := submitter.SubmitStatement(context.Background(), arbiterProtoEnvelopeFixture())
	if err != nil {
		t.Fatalf("SubmitStatement: %v", err)
	}
	if out.Category != OutcomeRetryable {
		t.Fatalf("outcome = %v, want retryable", out.Category)
	}
}

func TestArbiterIntakeStatusQuerierMapsSubmitStatus(t *testing.T) {
	tests := []struct {
		name   string
		status *pb.StatementStatus
		want   OutcomeCategory
	}{
		{
			name:   "not found is resend safe",
			status: &pb.StatementStatus{Found: false},
			want:   OutcomeUnspecified,
		},
		{
			name: "found proves submit landed even after later rejection",
			status: &pb.StatementStatus{
				Found:        true,
				StatementSeq: 42,
				Status:       "Rejected",
			},
			want: OutcomeExactIdempotent,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := &recordingArbiterIngressClient{statementStatus: tc.status}
			querier := NewArbiterIntakeStatusQuerier(client)

			got, err := querier.QuerySubmitStatus(context.Background(), "0xabc:7:nonce-7")
			if err != nil {
				t.Fatalf("QuerySubmitStatus: %v", err)
			}
			if got.Category != tc.want {
				t.Fatalf("category = %v, want %v", got.Category, tc.want)
			}
			if client.statusCalls != 1 || client.statusReq.GetStatementId() != "0xabc:7:nonce-7" {
				t.Fatalf("status calls/request = %d/%q", client.statusCalls, client.statusReq.GetStatementId())
			}
		})
	}
}

func TestArbiterIntakeStatusQuerierMapsClaimStatus(t *testing.T) {
	tests := []struct {
		name        string
		status      *pb.StatementStatus
		want        OutcomeCategory
		wantSource  string
		wantErrText string
	}{
		{
			name:   "not found is resend safe",
			status: &pb.StatementStatus{Found: false},
			want:   OutcomeUnspecified,
		},
		{
			name: "found but unbound is resend safe",
			status: &pb.StatementStatus{
				Found:        true,
				StatementSeq: 42,
				Status:       "Sequenced",
				RcBound:      false,
			},
			want: OutcomeUnspecified,
		},
		{
			name: "bound claim converges forward",
			status: &pb.StatementStatus{
				Found:        true,
				StatementSeq: 42,
				Status:       "Sequenced",
				RcBound:      true,
				BoundSource:  "snode-A",
			},
			want:       OutcomeExactIdempotent,
			wantSource: "snode-A",
		},
		{
			name: "bound claim without source is malformed",
			status: &pb.StatementStatus{
				Found:        true,
				StatementSeq: 42,
				RcBound:      true,
			},
			wantErrText: "empty bound_source",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := &recordingArbiterIngressClient{statementStatus: tc.status}
			querier := NewArbiterIntakeStatusQuerier(client)

			got, err := querier.QueryClaimStatus(context.Background(), "0xabc:7:nonce-7")
			if tc.wantErrText != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrText) {
					t.Fatalf("QueryClaimStatus err = %v, want %q", err, tc.wantErrText)
				}
				return
			}
			if err != nil {
				t.Fatalf("QueryClaimStatus: %v", err)
			}
			if got.Category != tc.want || got.BoundSource != tc.wantSource {
				t.Fatalf("claim = %v/%q, want %v/%q", got.Category, got.BoundSource, tc.want, tc.wantSource)
			}
		})
	}
}

func TestArbiterIntakeStatusQuerierRejectsIndeterminateProbe(t *testing.T) {
	tests := []struct {
		name   string
		client *recordingArbiterIngressClient
		want   string
	}{
		{
			name:   "nil response",
			client: &recordingArbiterIngressClient{},
			want:   "nil StatementStatus",
		},
		{
			name: "rpc error",
			client: &recordingArbiterIngressClient{
				statusErr: status.Error(codes.Unavailable, "probe unavailable"),
			},
			want: "probe unavailable",
		},
		{
			name: "not found cannot be rc bound",
			client: &recordingArbiterIngressClient{
				statementStatus: &pb.StatementStatus{Found: false, RcBound: true, BoundSource: "snode-A"},
			},
			want: "not found with statement data",
		},
		{
			name: "not found cannot have sequence",
			client: &recordingArbiterIngressClient{
				statementStatus: &pb.StatementStatus{Found: false, StatementSeq: 42},
			},
			want: "not found with statement data",
		},
		{
			name: "found requires statement sequence",
			client: &recordingArbiterIngressClient{
				statementStatus: &pb.StatementStatus{Found: true, Status: "Sequenced"},
			},
			want: "zero statement_seq",
		},
		{
			name: "unbound claim cannot have source",
			client: &recordingArbiterIngressClient{
				statementStatus: &pb.StatementStatus{Found: true, StatementSeq: 42, BoundSource: "snode-A"},
			},
			want: "rc_bound=false with non-empty bound_source",
		},
		{
			name: "bound source is not normalized",
			client: &recordingArbiterIngressClient{
				statementStatus: &pb.StatementStatus{Found: true, StatementSeq: 42, RcBound: true, BoundSource: " snode-A "},
			},
			want: "bound_source has surrounding whitespace",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			querier := NewArbiterIntakeStatusQuerier(tc.client)
			if _, err := querier.QuerySubmitStatus(context.Background(), "0xabc:7:nonce-7"); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("QuerySubmitStatus err = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestArbiterPayloadStoreWriterUsesInlinePutWithinLimit(t *testing.T) {
	payload := []byte("native-block")
	hash := replay.DigestBytes(payload)
	client := &recordingPayloadStoreClient{
		limits: &pb.StoreLimits{
			MaxInlineBytes:  32,
			MaxChunkBytes:   8,
			MaxPayloadBytes: 64,
			IngestLeaseMs:   60000,
		},
		putResult: &pb.PutPayloadResult{
			Code:               pb.PutCode_PUT_CODE_OK,
			PayloadRef:         "payload://inline/ref",
			State:              pb.PayloadState_PAYLOAD_STATE_AVAILABLE,
			LeaseExpiresUnixMs: 1234,
			Deduplicated:       true,
		},
	}
	writer := NewArbiterPayloadStoreWriter(client)

	got, err := writer.PutPayload(context.Background(), payload, hash, uint64(len(payload)))
	if err != nil {
		t.Fatalf("PutPayload: %v", err)
	}
	if got.PayloadRef != "payload://inline/ref" {
		t.Fatalf("payload ref = %q", got.PayloadRef)
	}
	if got.State != PayloadStateAvailable {
		t.Fatalf("state = %q", got.State)
	}
	if !got.Deduplicated {
		t.Fatal("deduplicated flag was not carried")
	}
	if client.limitsCalls != 1 {
		t.Fatalf("GetStoreLimits calls = %d, want 1", client.limitsCalls)
	}
	if client.inlineReq == nil {
		t.Fatal("PutPayloadInline was not called")
	}
	if gotHeader := client.inlineReq.GetHeader(); gotHeader.GetPayloadHash() != hash || gotHeader.GetPayloadLength() != uint64(len(payload)) {
		t.Fatalf("inline header = %q/%d", gotHeader.GetPayloadHash(), gotHeader.GetPayloadLength())
	}
	if !bytes.Equal(client.inlineReq.GetPayload(), payload) {
		t.Fatal("inline payload bytes mismatch")
	}
	if client.stream != nil {
		t.Fatal("chunked stream should not be used for inline payload")
	}
}

func TestArbiterPayloadStoreWriterUsesChunkedPutAboveInlineLimit(t *testing.T) {
	payload := []byte("0123456789")
	hash := replay.DigestBytes(payload)
	stream := &recordingPutPayloadStream{
		result: &pb.PutPayloadResult{
			Code:       pb.PutCode_PUT_CODE_OK,
			PayloadRef: "payload://chunked/ref",
			State:      pb.PayloadState_PAYLOAD_STATE_PENDING,
		},
	}
	client := &recordingPayloadStoreClient{
		limits: &pb.StoreLimits{
			MaxInlineBytes:  4,
			MaxChunkBytes:   3,
			MaxPayloadBytes: 64,
			IngestLeaseMs:   60000,
		},
		stream: stream,
	}
	writer := NewArbiterPayloadStoreWriter(client)

	got, err := writer.PutPayload(context.Background(), payload, hash, uint64(len(payload)))
	if err != nil {
		t.Fatalf("PutPayload: %v", err)
	}
	if got.PayloadRef != "payload://chunked/ref" {
		t.Fatalf("payload ref = %q", got.PayloadRef)
	}
	if got.State != PayloadStatePending {
		t.Fatalf("state = %q", got.State)
	}
	if client.inlineReq != nil {
		t.Fatal("PutPayloadInline should not be called for chunked payload")
	}
	if len(stream.frames) != 5 {
		t.Fatalf("frame count = %d, want header + 4 chunks", len(stream.frames))
	}
	if gotHeader := stream.frames[0].GetHeader(); gotHeader.GetPayloadHash() != hash || gotHeader.GetPayloadLength() != uint64(len(payload)) {
		t.Fatalf("stream header = %q/%d", gotHeader.GetPayloadHash(), gotHeader.GetPayloadLength())
	}
	wantChunks := [][]byte{[]byte("012"), []byte("345"), []byte("678"), []byte("9")}
	for i, want := range wantChunks {
		if !bytes.Equal(stream.frames[i+1].GetChunk(), want) {
			t.Fatalf("chunk %d = %q want %q", i, stream.frames[i+1].GetChunk(), want)
		}
	}
	if !stream.closed {
		t.Fatal("stream was not closed")
	}
}

func TestArbiterPayloadStoreWriterRejectsCommitmentMismatch(t *testing.T) {
	client := &recordingPayloadStoreClient{
		limits: &pb.StoreLimits{MaxInlineBytes: 32, MaxChunkBytes: 8, MaxPayloadBytes: 64},
	}
	writer := NewArbiterPayloadStoreWriter(client)

	_, err := writer.PutPayload(context.Background(), []byte("native-block"), replay.DigestString("different"), uint64(len("native-block")))
	if err == nil {
		t.Fatal("PutPayload accepted a mismatched payload hash")
	}
	if !strings.Contains(err.Error(), "payload hash mismatch") {
		t.Fatalf("error = %v", err)
	}
	if client.limitsCalls != 0 || client.inlineReq != nil || client.stream != nil {
		t.Fatal("writer should reject local commitment mismatch before any RPC")
	}
}

func TestArbiterPayloadStoreWriterRejectsPutFailureCode(t *testing.T) {
	payload := []byte("native-block")
	client := &recordingPayloadStoreClient{
		limits: &pb.StoreLimits{MaxInlineBytes: 32, MaxChunkBytes: 8, MaxPayloadBytes: 64},
		putResult: &pb.PutPayloadResult{
			Code:    pb.PutCode_PUT_CODE_COMMITMENT_MISMATCH,
			Message: "store computed a different digest",
		},
	}
	writer := NewArbiterPayloadStoreWriter(client)

	_, err := writer.PutPayload(context.Background(), payload, replay.DigestBytes(payload), uint64(len(payload)))
	if err == nil {
		t.Fatal("PutPayload accepted a non-OK PutPayloadResult")
	}
	if !strings.Contains(err.Error(), "COMMITMENT_MISMATCH") {
		t.Fatalf("error = %v", err)
	}
}

func arbiterProtoEnvelopeFixture() StatementEnvelope {
	sql := "INSERT INTO events FORMAT Native"
	return StatementEnvelope{
		StatementID:     "0xabc:7:nonce-7",
		StatementKind:   KindInsert,
		SQL:             sql,
		SQLHash:         replay.DigestString(sql),
		TargetTableID:   "net1.events",
		PayloadRef:      "payload://sha256-deadbeef",
		PayloadHash:     "0xdeadbeef",
		PayloadLength:   17,
		PayloadEncoding: PayloadEncodingClickHouseNativeData,
		Revision:        54465,
		Signer:          "0xabc",
		UserJWS:         "jws",
		EnvelopeVersion: EnvelopeVersionV2,
		NetworkID:       "testnet-v2",
		KeeperShardID:   0,
		SettingsHash:    EmptySettingsHash,
		SchemaHash:      "0x" + strings.Repeat("44", 32),
		RowIDProfileID:  payloadexec.RowIDProfileID,
	}
}

type recordingArbiterIngressClient struct {
	calls           int
	last            *pb.StatementEnvelopeV2
	ack             *pb.SequencedAck
	err             error
	statusCalls     int
	statusReq       *pb.GetStatementStatusRequest
	statementStatus *pb.StatementStatus
	statusErr       error
}

func (c *recordingArbiterIngressClient) SubmitStatement(_ context.Context, in *pb.StatementEnvelopeV2, _ ...grpc.CallOption) (*pb.SequencedAck, error) {
	c.calls++
	c.last = in
	return c.ack, c.err
}

func (c *recordingArbiterIngressClient) GetStatementStatus(_ context.Context, in *pb.GetStatementStatusRequest, _ ...grpc.CallOption) (*pb.StatementStatus, error) {
	c.statusCalls++
	c.statusReq = in
	return c.statementStatus, c.statusErr
}

type recordingPayloadStoreClient struct {
	limitsCalls int
	inlineReq   *pb.PutPayloadInlineRequest
	stream      *recordingPutPayloadStream

	limits    *pb.StoreLimits
	putResult *pb.PutPayloadResult
	limitsErr error
	inlineErr error
	streamErr error
}

func (c *recordingPayloadStoreClient) GetStoreLimits(context.Context, *pb.GetStoreLimitsRequest, ...grpc.CallOption) (*pb.StoreLimits, error) {
	c.limitsCalls++
	return c.limits, c.limitsErr
}

func (c *recordingPayloadStoreClient) PutPayloadInline(_ context.Context, in *pb.PutPayloadInlineRequest, _ ...grpc.CallOption) (*pb.PutPayloadResult, error) {
	c.inlineReq = in
	return c.putResult, c.inlineErr
}

func (c *recordingPayloadStoreClient) PutPayload(context.Context, ...grpc.CallOption) (grpc.ClientStreamingClient[pb.PutPayloadFrame, pb.PutPayloadResult], error) {
	if c.stream == nil {
		c.stream = &recordingPutPayloadStream{result: c.putResult}
	}
	return c.stream, c.streamErr
}

func (c *recordingPayloadStoreClient) FetchPayloads(context.Context, *pb.FetchPayloadsRequest, ...grpc.CallOption) (grpc.ServerStreamingClient[pb.FetchFrame], error) {
	return nil, nil
}

func (c *recordingPayloadStoreClient) StatPayloads(context.Context, *pb.StatPayloadsRequest, ...grpc.CallOption) (*pb.StatPayloadsResult, error) {
	return nil, nil
}

type recordingPutPayloadStream struct {
	grpc.ClientStream

	frames   []*pb.PutPayloadFrame
	result   *pb.PutPayloadResult
	sendErr  error
	closeErr error
	closed   bool
}

func (s *recordingPutPayloadStream) Send(frame *pb.PutPayloadFrame) error {
	if s.sendErr != nil {
		return s.sendErr
	}
	s.frames = append(s.frames, frame)
	return nil
}

func (s *recordingPutPayloadStream) CloseAndRecv() (*pb.PutPayloadResult, error) {
	s.closed = true
	return s.result, s.closeErr
}
