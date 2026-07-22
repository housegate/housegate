package storageintegrity

import (
	"context"
	"fmt"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ArbiterIngressClient is the subset of arbiter-proto's ArbiterIngress client
// required by the storage-integrity intake submitter.
type ArbiterIngressClient interface {
	SubmitStatement(ctx context.Context, in *pb.StatementEnvelopeV2, opts ...grpc.CallOption) (*pb.SequencedAck, error)
}

// ArbiterIngressSubmitter adapts the ArbiterIngress.SubmitStatement RPC to the
// HouseGate intake StatementSubmitter port.
type ArbiterIngressSubmitter struct {
	client ArbiterIngressClient
}

func NewArbiterIngressSubmitter(client ArbiterIngressClient) (*ArbiterIngressSubmitter, error) {
	if client == nil {
		return nil, fmt.Errorf("storage_integrity arbiter submitter: client is required")
	}
	return &ArbiterIngressSubmitter{client: client}, nil
}

func (s *ArbiterIngressSubmitter) SubmitStatement(ctx context.Context, env StatementEnvelope) (SubmitOutcome, error) {
	req, err := statementEnvelopeToPB(env)
	if err != nil {
		return SubmitOutcome{}, err
	}
	ack, err := s.client.SubmitStatement(ctx, req)
	if err != nil {
		return submitOutcomeFromRPCError(err), nil
	}
	return submitOutcomeFromAck(ack), nil
}

func statementEnvelopeToPB(env StatementEnvelope) (*pb.StatementEnvelopeV2, error) {
	id, err := parseFlatStatementID(env.StatementID)
	if err != nil {
		return nil, fmt.Errorf("storage_integrity arbiter submitter: invalid statement id %s: %w", env.StatementID, err)
	}
	kind, err := statementKindToPB(env.StatementKind)
	if err != nil {
		return nil, err
	}
	return &pb.StatementEnvelopeV2{
		StatementId: &pb.StatementID{
			ClientAccount: id.ClientAccount,
			ClientSeq:     id.ClientSeq,
			ClientNonce:   id.ClientNonce,
		},
		StatementKind: kind,
		Sql:           env.SQL,
		SqlHash:       env.SQLHash,
		SettingsHash:  "",
		PayloadRef:    env.PayloadRef,
		PayloadHash:   env.PayloadHash,
		PayloadLength: env.PayloadLength,
		TargetTableId: env.TargetTableID,
		UserJws:       env.UserJWS,
	}, nil
}

func statementKindToPB(kind Kind) (pb.StatementKind, error) {
	switch kind {
	case KindInsert:
		return pb.StatementKind_STATEMENT_KIND_INSERT, nil
	default:
		return pb.StatementKind_STATEMENT_KIND_UNSPECIFIED, fmt.Errorf("storage_integrity arbiter submitter: unsupported statement kind %q", kind)
	}
}

func submitOutcomeFromAck(ack *pb.SequencedAck) SubmitOutcome {
	if ack == nil {
		return SubmitOutcome{Category: OutcomeUnknown, Reason: "nil SubmitStatement ack"}
	}
	reason := ack.GetMessage()
	switch ack.GetCode() {
	case pb.AdmissionCode_ADMISSION_CODE_ACCEPTED:
		return SubmitOutcome{Category: OutcomeAccepted}
	case pb.AdmissionCode_ADMISSION_CODE_DUPLICATE_CLIENT_SEQ,
		pb.AdmissionCode_ADMISSION_CODE_SCHEMA_NOT_ALLOWED,
		pb.AdmissionCode_ADMISSION_CODE_KIND_NOT_ADMITTED,
		pb.AdmissionCode_ADMISSION_CODE_INVALID_SIGNATURE,
		pb.AdmissionCode_ADMISSION_CODE_INVALID_PROOF,
		pb.AdmissionCode_ADMISSION_CODE_MALFORMED,
		pb.AdmissionCode_ADMISSION_CODE_GAP_BUDGET_EXCEEDED:
		if reason == "" {
			reason = ack.GetCode().String()
		}
		return SubmitOutcome{Category: OutcomeTerminalReject, Reason: reason}
	default:
		if reason == "" {
			reason = ack.GetCode().String()
		}
		return SubmitOutcome{Category: OutcomeUnknown, Reason: reason}
	}
}

func submitOutcomeFromRPCError(err error) SubmitOutcome {
	code := status.Code(err)
	reason := err.Error()
	switch code {
	case codes.Unavailable, codes.ResourceExhausted, codes.Aborted:
		return SubmitOutcome{Category: OutcomeRetryable, Reason: reason}
	case codes.DeadlineExceeded, codes.Canceled:
		return SubmitOutcome{Category: OutcomeUnknown, Reason: reason}
	case codes.InvalidArgument, codes.PermissionDenied, codes.Unauthenticated, codes.FailedPrecondition, codes.OutOfRange:
		return SubmitOutcome{Category: OutcomeTerminalReject, Reason: reason}
	default:
		return SubmitOutcome{Category: OutcomeUnknown, Reason: reason}
	}
}
