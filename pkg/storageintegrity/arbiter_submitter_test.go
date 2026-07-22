package storageintegrity

import (
	"context"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestArbiterIngressSubmitterMapsAcceptedAck(t *testing.T) {
	client := &fakeArbiterIngressClient{ack: &pb.SequencedAck{Code: pb.AdmissionCode_ADMISSION_CODE_ACCEPTED}}
	submitter, err := NewArbiterIngressSubmitter(client)
	if err != nil {
		t.Fatalf("NewArbiterIngressSubmitter: %v", err)
	}

	out, err := submitter.SubmitStatement(context.Background(), admissionEnvelopeFixture())
	if err != nil {
		t.Fatalf("SubmitStatement: %v", err)
	}
	if out.Category != OutcomeAccepted || out.Reason != "" {
		t.Fatalf("outcome = %+v", out)
	}
	got := client.got
	if got.GetStatementId().GetClientAccount() != fixtureSigner || got.GetStatementId().GetClientSeq() != 1 ||
		got.GetStatementId().GetClientNonce() != "n1" || got.GetStatementKind() != pb.StatementKind_STATEMENT_KIND_INSERT ||
		got.GetSqlHash() != replayDigestFixtureSQL() || got.GetPayloadRef() != "sha256:deadbeef" {
		t.Fatalf("submitted envelope = %+v", got)
	}
}

func TestArbiterIngressSubmitterClassifiesRejectAndTransportOutcomes(t *testing.T) {
	tests := []struct {
		name string
		ack  *pb.SequencedAck
		err  error
		want OutcomeCategory
	}{
		{
			name: "duplicate client seq is terminal",
			ack:  &pb.SequencedAck{Code: pb.AdmissionCode_ADMISSION_CODE_DUPLICATE_CLIENT_SEQ, Message: "conflict"},
			want: OutcomeTerminalReject,
		},
		{
			name: "not leader is retryable",
			err:  status.Error(codes.Unavailable, "not leader"),
			want: OutcomeRetryable,
		},
		{
			name: "deadline is unknown",
			err:  status.Error(codes.DeadlineExceeded, "timeout"),
			want: OutcomeUnknown,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			submitter, err := NewArbiterIngressSubmitter(&fakeArbiterIngressClient{ack: tt.ack, err: tt.err})
			if err != nil {
				t.Fatalf("NewArbiterIngressSubmitter: %v", err)
			}
			out, err := submitter.SubmitStatement(context.Background(), admissionEnvelopeFixture())
			if err != nil {
				t.Fatalf("SubmitStatement: %v", err)
			}
			if out.Category != tt.want || out.Reason == "" {
				t.Fatalf("outcome = %+v, want category %v with reason", out, tt.want)
			}
		})
	}
}

func TestNewArbiterIngressSubmitterRejectsNilClient(t *testing.T) {
	if _, err := NewArbiterIngressSubmitter(nil); err == nil {
		t.Fatal("nil client accepted")
	}
}

type fakeArbiterIngressClient struct {
	got *pb.StatementEnvelopeV2
	ack *pb.SequencedAck
	err error
}

func (f *fakeArbiterIngressClient) SubmitStatement(_ context.Context, in *pb.StatementEnvelopeV2, _ ...grpc.CallOption) (*pb.SequencedAck, error) {
	f.got = in
	return f.ack, f.err
}

func admissionEnvelopeFixture() StatementEnvelope {
	env, err := EnvelopeFromAdmission(admissionFixture())
	if err != nil {
		panic(err)
	}
	return env
}

func replayDigestFixtureSQL() string {
	return admissionFixture().SQLHash
}
