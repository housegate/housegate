package storageintegrity

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestArbiterClientClaimWaitSendsWaitParam verifies WithClaimWait appends a
// `wait=<ms>` query param to a claim RPC, and that the default (no claim wait)
// sends none — the additive contract the mock long-poll relies on.
func TestArbiterClientClaimWaitSendsWaitParam(t *testing.T) {
	var gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/arbiter/promotions/claim" {
			gotQuery = r.URL.RawQuery
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": false})
	}))
	defer srv.Close()

	// With claim wait: the request carries wait=1500 (ms).
	client := NewHTTPArbiterClient(srv.URL).WithClaimWait(1500 * time.Millisecond)
	if _, _, err := client.ClaimPromotion(context.Background()); err != nil {
		t.Fatalf("ClaimPromotion: %v", err)
	}
	if gotQuery != "wait=1500" {
		t.Fatalf("claim query = %q, want wait=1500", gotQuery)
	}

	// Without claim wait: no wait param (historical immediate-return behavior).
	gotQuery = "sentinel"
	plain := NewHTTPArbiterClient(srv.URL)
	if _, _, err := plain.ClaimPromotion(context.Background()); err != nil {
		t.Fatalf("ClaimPromotion (no wait): %v", err)
	}
	if gotQuery != "" {
		t.Fatalf("default claim query = %q, want empty (no wait param)", gotQuery)
	}
}

// TestArbiterClientClaimWaitRaisesTimeout verifies enabling long-poll raises the
// HTTP client timeout above the wait window so a blocked claim is not aborted.
func TestArbiterClientClaimWaitRaisesTimeout(t *testing.T) {
	client := NewHTTPArbiterClient("http://127.0.0.1:1").WithClaimWait(30 * time.Second)
	if client.client.Timeout <= 30*time.Second {
		t.Fatalf("client timeout = %s, want > 30s wait window", client.client.Timeout)
	}
	// Disabling resets the wait (timeout is left raised, which is harmless).
	client.WithClaimWait(0)
	if client.claimWait != 0 {
		t.Fatalf("WithClaimWait(0) must disable long-poll, claimWait=%s", client.claimWait)
	}
}

// TestArbiterClientSubmitDoesNotCarryWait verifies the wait param is scoped to
// claim RPCs only (a submit/result POST must not carry it).
func TestArbiterClientSubmitDoesNotCarryWait(t *testing.T) {
	var submitQuery = "sentinel"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/arbiter/byte-side-scans" {
			submitQuery = r.URL.RawQuery
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
	}))
	defer srv.Close()

	client := NewHTTPArbiterClient(srv.URL).WithClaimWait(2 * time.Second)
	if err := client.SubmitByteSideScan(context.Background(), ByteSideScanResult{ScanID: "s1"}); err != nil {
		t.Fatalf("SubmitByteSideScan: %v", err)
	}
	if submitQuery != "" {
		t.Fatalf("submit query = %q, want empty (wait is claim-only)", submitQuery)
	}
}
