package agent

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"testing"

	"golang.org/x/crypto/sha3"

	"housegate/housegate/pkg/chproto"
	"housegate/housegate/pkg/chsession"
	"housegate/housegate/pkg/plugin"
	"housegate/housegate/pkg/auth"
)

func newTestSession(t *testing.T, id int64) chsession.Session {
	t.Helper()
	client, server := net.Pipe()
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	return chsession.New(id, client)
}

func newTestQueryContext(sess chsession.Session, sql string) *plugin.QueryContext {
	return &plugin.QueryContext{
		Session:     sess,
		OriginalSQL: sql,
		Query:       &chproto.Query{Body: sql},
	}
}

// findAuthToken returns the value of the auth-token setting injected by the
// plugin, or "" if absent.
func findAuthToken(settings []chproto.Setting) string {
	for _, s := range settings {
		if s.Key == auth.AuthTokenSettingKey {
			return s.Value
		}
	}
	return ""
}

// findSetting returns the value of a setting by key, or "" if absent.
func findSetting(settings []chproto.Setting, key string) string {
	for _, s := range settings {
		if s.Key == key {
			return s.Value
		}
	}
	return ""
}

func TestPlugin_InjectsTokenIntoSettings(t *testing.T) {
	signer, err := auth.NewRelaySigner("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatalf("create signer: %v", err)
	}

	sess := newTestSession(t, 5)
	qctx := newTestQueryContext(sess, "SELECT 1")

	p := &Plugin{Signer: signer}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	token := findAuthToken(qctx.Query.Settings)
	if token == "" {
		t.Fatalf("expected %q setting to be injected", auth.AuthTokenSettingKey)
	}
	if parts := strings.Split(token, "."); len(parts) != 3 {
		t.Errorf("expected JWS compact token (3 parts), got %d: %q", len(parts), token)
	}
}

func TestPlugin_InjectsOwnerAsPayerSetting(t *testing.T) {
	signer, err := auth.NewRelaySigner("cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")
	if err != nil {
		t.Fatalf("create signer: %v", err)
	}

	const owner = "0x1111111111111111111111111111111111111111"
	sess := newTestSession(t, 7)
	qctx := newTestQueryContext(sess, "SELECT 1")

	p := &Plugin{Signer: signer, Owner: owner}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := findSetting(qctx.Query.Settings, auth.PayerSettingKey)
	want := "'" + owner + "'"
	if got != want {
		t.Errorf("payer setting: got %q, want %q", got, want)
	}
}

func TestPlugin_OmitsPayerSettingWhenOwnerUnset(t *testing.T) {
	signer, err := auth.NewRelaySigner("dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd")
	if err != nil {
		t.Fatalf("create signer: %v", err)
	}

	sess := newTestSession(t, 8)
	qctx := newTestQueryContext(sess, "SELECT 1")

	p := &Plugin{Signer: signer}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if v := findSetting(qctx.Query.Settings, auth.PayerSettingKey); v != "" {
		t.Errorf("expected no %q setting, got %q", auth.PayerSettingKey, v)
	}
}

func TestPlugin_InjectsDriverSettingWhenIsDriverTrue(t *testing.T) {
	signer, err := auth.NewRelaySigner("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
	if err != nil {
		t.Fatalf("create signer: %v", err)
	}

	sess := newTestSession(t, 9)
	qctx := newTestQueryContext(sess, "SELECT 1")

	p := &Plugin{Signer: signer, IsDriver: true}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := findSetting(qctx.Query.Settings, auth.DriverSettingKey)
	if got != "'1'" {
		t.Errorf("driver setting: got %q, want %q", got, "'1'")
	}
}

func TestPlugin_OmitsDriverSettingWhenIsDriverFalse(t *testing.T) {
	signer, err := auth.NewRelaySigner("1111111111111111111111111111111111111111111111111111111111111111")
	if err != nil {
		t.Fatalf("create signer: %v", err)
	}

	sess := newTestSession(t, 10)
	qctx := newTestQueryContext(sess, "SELECT 1")

	p := &Plugin{Signer: signer} // IsDriver defaults to false
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if v := findSetting(qctx.Query.Settings, auth.DriverSettingKey); v != "" {
		t.Errorf("expected no %q setting when IsDriver=false, got %q", auth.DriverSettingKey, v)
	}
}

func TestPlugin_NilSignerIsNoop(t *testing.T) {
	sess := newTestSession(t, 6)
	qctx := newTestQueryContext(sess, "SELECT 1")

	p := &Plugin{Signer: nil}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("nil signer should be no-op, got: %v", err)
	}
	if len(qctx.Query.Settings) != 0 {
		t.Errorf("expected no settings injected, got: %v", qctx.Query.Settings)
	}
}

// TestPlugin_SignsBodyNotOriginalSQL verifies the token's qhash is derived
// from Query.Body (which may differ from OriginalSQL after a rewrite).
func TestPlugin_SignsBodyNotOriginalSQL(t *testing.T) {
	signer, err := auth.NewRelaySigner("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	if err != nil {
		t.Fatalf("create signer: %v", err)
	}

	sess := newTestSession(t, 7)
	qctx := &plugin.QueryContext{
		Session:     sess,
		OriginalSQL: "SELECT 1",
		Query:       &chproto.Query{Body: "SELECT 2"},
	}

	p := &Plugin{Signer: signer}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	token := findAuthToken(qctx.Query.Settings)
	if token == "" {
		t.Fatal("token not injected")
	}

	qhash, err := decodeJWSPayloadQHash(token)
	if err != nil {
		t.Fatalf("decode token payload: %v", err)
	}
	expected := keccak256Hex([]byte("SELECT 2"))
	if qhash != expected {
		t.Errorf("qhash mismatch: token bound to wrong SQL. got=%s, want=%s", qhash, expected)
	}
}

// decodeJWSPayloadQHash parses a JWS compact token (header.payload.sig) and
// returns the qhash from its payload. We do not verify the signature here —
// the agent signer plugin only delegates signing to RelaySigner, which has
// its own coverage in pkg/proxy.
func decodeJWSPayloadQHash(token string) (string, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return "", errors.New("invalid JWS compact token")
	}
	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", err
	}
	var payload struct {
		QueryHash string `json:"qhash"`
	}
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		return "", err
	}
	return payload.QueryHash, nil
}

func keccak256Hex(data []byte) string {
	h := sha3.NewLegacyKeccak256()
	h.Write(data)
	return "0x" + hex.EncodeToString(h.Sum(nil))
}
