package credential

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"housegate/housegate/pkg/auth"
	"housegate/housegate/pkg/chproto"
	"housegate/housegate/pkg/chsession"
	"housegate/housegate/pkg/network"
	"housegate/housegate/pkg/peer"
)

// signerKeyHex is a fixed secp256k1 private key for tests.
const signerKeyHex = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"

// TestOnHello_PeerEnvelope_IndexerIDZero verifies that an inbound peer-relay
// JWS whose audience is the literal "0" (because the receiving proxy's
// IndexerId == 0) is accepted. Pre-fix, the validator short-circuited id==0
// to expectedAud="" and produced "audience mismatch: got \"0\", expected \"\"".
func TestOnHello_PeerEnvelope_IndexerIDZero(t *testing.T) {
	signer, err := auth.NewRelaySigner(signerKeyHex)
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}
	validator := auth.NewEthValidator([]string{signer.Address()}, time.Hour, true, false, "", nil)

	target := network.IndexerInfo{IndexerId: 0, IndexerUrl: "self", ClickhouseProxyPort: 9000}
	user, password, err := peer.SignPeerHello(signer, time.Minute, target, nil)
	if err != nil {
		t.Fatalf("SignPeerHello: %v", err)
	}

	p := &Plugin{
		PeerValidator:    validator,
		GetSelfIndexerID: func() uint64 { return 0 },
	}

	hello := &chproto.ClientHello{User: user, Password: password}
	sess := newTestSession(t)
	if err := p.OnHello(context.Background(), sess, hello); err != nil {
		t.Fatalf("OnHello rejected peer envelope with IndexerId=0: %v", err)
	}
	if !sess.State().IsPeerTrusted {
		t.Errorf("expected IsPeerTrusted=true after successful peer validation")
	}
	if hello.User != "" {
		t.Errorf("expected hello.User cleared after peer envelope handling, got %q", hello.User)
	}
}

// TestOnHello_PeerEnvelope_AudienceMismatch ensures non-matching ids still fail
// — i.e. removing the id==0 short-circuit didn't widen audience verification.
func TestOnHello_PeerEnvelope_AudienceMismatch(t *testing.T) {
	signer, _ := auth.NewRelaySigner(signerKeyHex)
	validator := auth.NewEthValidator([]string{signer.Address()}, time.Hour, true, false, "", nil)

	target := network.IndexerInfo{IndexerId: 7}
	user, password, _ := peer.SignPeerHello(signer, time.Minute, target, nil)

	p := &Plugin{
		PeerValidator:    validator,
		GetSelfIndexerID: func() uint64 { return 9 }, // signer aimed at 7, we are 9
	}

	hello := &chproto.ClientHello{User: user, Password: password}
	err := p.OnHello(context.Background(), newTestSession(t), hello)
	if err == nil {
		t.Fatal("expected audience mismatch, got nil")
	}
	if !strings.Contains(err.Error(), "audience mismatch") {
		t.Errorf("expected audience-mismatch error, got %v", err)
	}
}

func newTestSession(t *testing.T) chsession.Session {
	t.Helper()
	c1, c2 := net.Pipe()
	t.Cleanup(func() {
		_ = c1.Close()
		_ = c2.Close()
	})
	return chsession.New(1, c1)
}
