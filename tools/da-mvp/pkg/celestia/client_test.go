package celestia

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSubmit_SendsBlobAndReturnsHeight(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer testtoken" {
			t.Errorf("missing auth: %q", got)
		}
		body, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(body), `"method":"blob.Submit"`) {
			t.Errorf("expected blob.Submit, got %s", body)
		}
		w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":12345}`))
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "testtoken")
	ns := []byte("hgmv\x00\x00\x00\x00\x00\x01")
	height, err := c.Submit(context.Background(), ns, []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if height != 12345 {
		t.Errorf("got height %d want 12345", height)
	}
}

func TestSubmit_ErrorBubblesUp(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"out of gas"}}`))
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "")
	_, err := c.Submit(context.Background(), []byte("0123456789"), []byte("x"))
	if err == nil || !strings.Contains(err.Error(), "out of gas") {
		t.Errorf("expected out-of-gas error, got %v", err)
	}
}

func TestGet_DecodesBase64Data(t *testing.T) {
	want := []byte("blob payload")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]any{
			"jsonrpc": "2.0", "id": 1,
			"result": map[string]any{
				"namespace":     base64.StdEncoding.EncodeToString([]byte("hgmv\x00\x00\x00\x00\x00\x01")),
				"data":          base64.StdEncoding.EncodeToString(want),
				"share_version": 0,
				"commitment":    base64.StdEncoding.EncodeToString([]byte("dummycommit")),
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "")
	got, err := c.Get(context.Background(), 100, []byte("hgmv\x00\x00\x00\x00\x00\x01"), []byte("dummycommit"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Errorf("got %q want %q", got, want)
	}
}

func TestCall_Non2xxStatus_ReturnsHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte("invalid token"))
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "bad")
	_, err := c.Submit(context.Background(), []byte("0123456789"), []byte("x"))
	if err == nil || !strings.Contains(err.Error(), "http 401") {
		t.Errorf("expected 'http 401' error, got %v", err)
	}
}

func TestCall_NoBearerWhenTokenEmpty(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "" {
			t.Errorf("Authorization header should be unset when token is empty, got %q", got)
		}
		w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":1}`))
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "")
	if _, err := c.Submit(context.Background(), []byte("0123456789"), []byte("x")); err != nil {
		t.Fatal(err)
	}
}

func TestCall_OversizeResponse_Capped(t *testing.T) {
	// Server streams more than 4 MiB; LimitReader should cap the read
	// and json.Unmarshal will produce a decode error from the truncated
	// JSON. The point of this test is to verify we don't OOM by reading
	// the full unbounded stream.
	huge := strings.Repeat("x", 6*1024*1024)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"` + huge + `"}`))
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "")
	_, err := c.Submit(context.Background(), []byte("0123456789"), []byte("x"))
	// We expect *some* error (decode failure on truncated JSON) — not a
	// silent success and not an OOM.
	if err == nil {
		t.Error("expected error on oversize response, got nil")
	}
}
