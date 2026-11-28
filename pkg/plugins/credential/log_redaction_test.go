package credential

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/log"
	"github.com/housegate/housegate/pkg/peer"
)

type staticProvider struct{ user, password string }

func (s staticProvider) GetDefaultCredential() (string, string, error) {
	return s.user, s.password, nil
}

func (s staticProvider) GetCredentialForIndexer(uint64) (string, string, error) {
	return s.user, s.password, nil
}

// debugLogContext binds a debug-level text logger writing to buf, so a test
// sees every line OnHello emits at any level.
func debugLogContext(buf *bytes.Buffer) context.Context {
	h := slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	return log.WithContext(context.Background(), log.New(h))
}

// TestOnHello_NeverLogsPasswords pins that neither the client's password nor
// the provider's upstream password reaches the log, even at debug level.
func TestOnHello_NeverLogsPasswords(t *testing.T) {
	const (
		clientPassword   = "client-password-value"
		upstreamPassword = "upstream-password-value"
	)
	var buf bytes.Buffer
	p := &Plugin{Provider: staticProvider{user: "gateway", password: upstreamPassword}}
	hello := &chproto.ClientHello{User: "someone", Password: clientPassword}
	if err := p.OnHello(debugLogContext(&buf), newTestSession(t), hello); err != nil {
		t.Fatalf("OnHello: %v", err)
	}

	out := buf.String()
	// The capture works: the user names are still logged.
	if !strings.Contains(out, "someone") || !strings.Contains(out, "gateway") {
		t.Fatalf("expected the debug lines to name the users, got:\n%s", out)
	}
	for _, secret := range []string{clientPassword, upstreamPassword} {
		if strings.Contains(out, secret) {
			t.Fatalf("log contains a password %q:\n%s", secret, out)
		}
	}
}

// TestOnHello_NeverLogsPeerRelayToken pins that the peer-relay JWS carried in
// the password of a __peer__ envelope, a bearer token, is not logged.
func TestOnHello_NeverLogsPeerRelayToken(t *testing.T) {
	signer, err := auth.NewRelaySigner(signerKeyHex)
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}
	validator := auth.NewEthValidator([]string{signer.Address()}, time.Hour, true, false, "", nil)
	user, token, err := peer.SignPeerHello(signer, time.Minute, 5, nil)
	if err != nil {
		t.Fatalf("SignPeerHello: %v", err)
	}

	var buf bytes.Buffer
	p := &Plugin{
		Provider:         staticProvider{user: "gateway", password: "upstream-password-value"},
		PeerValidator:    validator,
		GetSelfIndexerID: func() uint64 { return 5 },
	}
	hello := &chproto.ClientHello{User: user, Password: token}
	if err := p.OnHello(debugLogContext(&buf), newTestSession(t), hello); err != nil {
		t.Fatalf("OnHello: %v", err)
	}
	if out := buf.String(); strings.Contains(out, token) {
		t.Fatalf("log contains the peer-relay token:\n%s", out)
	}
}
