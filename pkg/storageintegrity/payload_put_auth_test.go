package storageintegrity

import (
	"context"
	"errors"
	"io"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/housegate/housegate/pkg/replay"
)

type ctxKey struct{}

func testPutAuthorizer(calls *[]string) PayloadPutAuthorizer {
	return func(ctx context.Context, payloadHash string, payloadLength uint64) (context.Context, error) {
		*calls = append(*calls, payloadHash)
		return metadata.AppendToOutgoingContext(context.WithValue(ctx, ctxKey{}, payloadLength), "sentio-da-put-auth", "token-"+payloadHash), nil
	}
}

func putToken(ctx context.Context) string {
	md, _ := metadata.FromOutgoingContext(ctx)
	if values := md.Get("sentio-da-put-auth"); len(values) == 1 {
		return values[0]
	}
	return ""
}

func TestAuthorizedPayloadStoreWriterAuthorizesInlineAndStreamPuts(t *testing.T) {
	ok := &pb.PutPayloadResult{Code: pb.PutCode_PUT_CODE_OK, PayloadRef: "payload://ref", State: pb.PayloadState_PAYLOAD_STATE_AVAILABLE}
	var calls []string
	client := &recordingPayloadStoreClient{
		limits:    &pb.StoreLimits{MaxInlineBytes: 4, MaxChunkBytes: 3, MaxPayloadBytes: 64},
		putResult: ok,
	}
	writer := NewAuthorizedArbiterPayloadStoreWriter(client, testPutAuthorizer(&calls))

	small, large := []byte("abc"), []byte("0123456789")
	for _, payload := range [][]byte{small, large} {
		if _, err := writer.PutPayload(context.Background(), payload, replay.DigestBytes(payload), uint64(len(payload))); err != nil {
			t.Fatalf("PutPayload: %v", err)
		}
	}
	if len(calls) != 2 || calls[0] != replay.DigestBytes(small) || calls[1] != replay.DigestBytes(large) {
		t.Fatalf("authorizer calls = %v", calls)
	}
	if got := putToken(client.inlineCtx); got != "token-"+replay.DigestBytes(small) {
		t.Fatalf("inline token = %q", got)
	}
	if got := putToken(client.streamCtx); got != "token-"+replay.DigestBytes(large) {
		t.Fatalf("stream token = %q", got)
	}
}

func TestPayloadStoreWriterWithoutAuthorizerSendsNoToken(t *testing.T) {
	client := &recordingPayloadStoreClient{
		limits:    &pb.StoreLimits{MaxInlineBytes: 32, MaxChunkBytes: 8, MaxPayloadBytes: 64},
		putResult: &pb.PutPayloadResult{Code: pb.PutCode_PUT_CODE_OK, PayloadRef: "payload://ref", State: pb.PayloadState_PAYLOAD_STATE_AVAILABLE},
	}
	payload := []byte("abc")
	if _, err := NewAuthorizedArbiterPayloadStoreWriter(client, nil).PutPayload(context.Background(), payload, replay.DigestBytes(payload), 3); err != nil {
		t.Fatal(err)
	}
	if got := putToken(client.inlineCtx); got != "" {
		t.Fatalf("token without authorizer = %q", got)
	}
}

func TestPayloadStoreWriterAuthorizerFailureStopsThePut(t *testing.T) {
	client := &recordingPayloadStoreClient{
		limits: &pb.StoreLimits{MaxInlineBytes: 32, MaxChunkBytes: 8, MaxPayloadBytes: 64},
	}
	failing := func(context.Context, string, uint64) (context.Context, error) { return nil, errors.New("key unavailable") }
	payload := []byte("abc")
	if _, err := NewAuthorizedArbiterPayloadStoreWriter(client, failing).PutPayload(context.Background(), payload, replay.DigestBytes(payload), 3); err == nil {
		t.Fatal("put succeeded without authorisation")
	}
	if client.inlineReq != nil || client.stream != nil {
		t.Fatal("put reached the store")
	}
}

func TestPayloadStoreRefusalIsBackpressure(t *testing.T) {
	exhausted := status.Error(codes.ResourceExhausted, "write quota exceeded for indexer 7 (bytes); retry after 2s")
	for name, client := range map[string]*recordingPayloadStoreClient{
		"inline": {
			limits:    &pb.StoreLimits{MaxInlineBytes: 32, MaxChunkBytes: 8, MaxPayloadBytes: 64},
			inlineErr: exhausted,
		},
		// The store refuses the stream after the header: Send sees io.EOF and
		// the refusal arrives from CloseAndRecv.
		"stream": {
			limits: &pb.StoreLimits{MaxInlineBytes: 1, MaxChunkBytes: 8, MaxPayloadBytes: 64},
			stream: &recordingPutPayloadStream{sendErr: io.EOF, closeErr: exhausted},
		},
	} {
		payload := []byte("abc")
		_, err := NewArbiterPayloadStoreWriter(client).PutPayload(context.Background(), payload, replay.DigestBytes(payload), 3)
		if !errors.Is(err, ErrBackpressure) || status.Code(err) != codes.ResourceExhausted {
			t.Fatalf("%s: err = %v, want back-pressure", name, err)
		}
	}
	for name, err := range map[string]error{
		"permission denied": status.Error(codes.PermissionDenied, "not allowlisted"),
		"message too large": status.Error(codes.ResourceExhausted, "grpc: received message larger than max (5 vs. 4)"),
	} {
		client := &recordingPayloadStoreClient{
			limits:    &pb.StoreLimits{MaxInlineBytes: 32, MaxChunkBytes: 8, MaxPayloadBytes: 64},
			inlineErr: err,
		}
		payload := []byte("abc")
		if _, got := NewArbiterPayloadStoreWriter(client).PutPayload(context.Background(), payload, replay.DigestBytes(payload), 3); errors.Is(got, ErrBackpressure) || got == nil {
			t.Fatalf("%s: err = %v, want a plain error", name, got)
		}
	}
}
