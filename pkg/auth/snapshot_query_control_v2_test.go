package auth

import (
	"encoding/base64"
	"strings"
	"testing"
	"time"
)

func snapshotQueryControlV2Fixture(account string) SnapshotQueryControlBindingV2 {
	return SnapshotQueryControlBindingV2{
		SnapshotQueryControlBinding: snapshotQueryControlFixture(account),
		TargetTableID:               "tbl-0123456789abcdef",
	}
}

func TestSnapshotQueryControlV2FixedCanonicalVector(t *testing.T) {
	signer, err := NewRelaySigner(statementV2TestKey)
	if err != nil {
		t.Fatal(err)
	}
	binding := snapshotQueryControlV2Fixture(signer.Address())
	token, err := signer.signSnapshotQueryControlV2At(binding, 1789550000)
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("token = %q", token)
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"purpose":"housegate-snapshot-query-control-v2","iat":1789550000,"operation":"release","network_id":"testnet-v2","keeper_shard_id":7,"client_account":"0x8fd379246834eac74b8419ffda202cf8051f7a03","statement_id":"0x8fd379246834eac74b8419ffda202cf8051f7a03:42:n1","request_id":"request-42","reservation_id":"reservation-42","fencing_generation":9,"target_table_id":"tbl-0123456789abcdef"}`
	if string(raw) != want {
		t.Fatalf("canonical payload = %s\nwant = %s", raw, want)
	}
	// The signature is deterministic (RFC 6979), so the same binding and iat
	// always give the same token; the payload above is the pinned contract.
	again, err := signer.signSnapshotQueryControlV2At(binding, 1789550000)
	if err != nil || again != token {
		t.Fatalf("re-signed token differs: %v", err)
	}
	got, err := DecodeSnapshotQueryControlPayloadV2(token)
	if err != nil || got.Purpose != SnapshotQueryControlPurposeV2 || got.Iat != 1789550000 || got.SnapshotQueryControlBindingV2 != binding {
		t.Fatalf("decode = %+v, %v", got, err)
	}
	account, err := VerifySnapshotQueryControlSignatureV2(token, binding)
	if err != nil || account != signer.Address() {
		t.Fatalf("verify = %q, %v", account, err)
	}
}

func TestSnapshotQueryControlV2BindsEveryField(t *testing.T) {
	signer, _ := NewRelaySigner(statementV2TestKey)
	binding := snapshotQueryControlV2Fixture(signer.Address())
	token, err := signer.SignSnapshotQueryControlV2(binding)
	if err != nil {
		t.Fatal(err)
	}
	mutations := map[string]func(*SnapshotQueryControlBindingV2){
		"operation":          func(p *SnapshotQueryControlBindingV2) { p.Operation = SnapshotQueryControlOperationAcquire },
		"network_id":         func(p *SnapshotQueryControlBindingV2) { p.NetworkID = "other" },
		"keeper_shard_id":    func(p *SnapshotQueryControlBindingV2) { p.KeeperShardID++ },
		"client_account":     func(p *SnapshotQueryControlBindingV2) { p.ClientAccount = "0x0000000000000000000000000000000000000001" },
		"statement_id":       func(p *SnapshotQueryControlBindingV2) { p.StatementID += "x" },
		"request_id":         func(p *SnapshotQueryControlBindingV2) { p.RequestID += "x" },
		"reservation_id":     func(p *SnapshotQueryControlBindingV2) { p.ReservationID += "x" },
		"fencing_generation": func(p *SnapshotQueryControlBindingV2) { p.FencingGeneration++ },
		"target_table_id":    func(p *SnapshotQueryControlBindingV2) { p.TargetTableID += "x" },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			want := binding
			mutate(&want)
			if _, err := VerifySnapshotQueryControlSignatureV2(token, want); err == nil || !strings.Contains(err.Error(), name) {
				t.Fatalf("VerifySnapshotQueryControlSignatureV2 = %v, want %s refusal", err, name)
			}
		})
	}
	bad := binding
	bad.Operation = "submit"
	badToken, err := signer.SignSnapshotQueryControlV2(bad)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := VerifySnapshotQueryControlSignatureV2(badToken, bad); err == nil || !strings.Contains(err.Error(), "operation") {
		t.Fatalf("unknown operation accepted: %v", err)
	}
}

// TestSnapshotQueryControlVersionsNeverCross: a v1 token is never a v2 token
// and the reverse, even when every shared field matches, so a sequencer that
// accepts exactly one version per network cannot be handed the other.
func TestSnapshotQueryControlVersionsNeverCross(t *testing.T) {
	signer, _ := NewRelaySigner(statementV2TestKey)
	v2 := snapshotQueryControlV2Fixture(signer.Address())
	v1 := v2.SnapshotQueryControlBinding
	v1Token, err := signer.SignSnapshotQueryControl(v1)
	if err != nil {
		t.Fatal(err)
	}
	v2Token, err := signer.SignSnapshotQueryControlV2(v2)
	if err != nil {
		t.Fatal(err)
	}
	empty := v2
	empty.TargetTableID = ""
	for name, check := range map[string]func() error{
		"v1 token as v2":              func() error { _, err := VerifySnapshotQueryControlSignatureV2(v1Token, v2); return err },
		"v1 token as v2 empty target": func() error { _, err := VerifySnapshotQueryControlSignatureV2(v1Token, empty); return err },
		"v2 token as v1":              func() error { _, err := VerifySnapshotQueryControlSignature(v2Token, v1); return err },
		"v2 token decoded as v1":      func() error { _, err := DecodeSnapshotQueryControlPayload(v2Token); return err },
		"v1 token decoded as v2":      func() error { _, err := DecodeSnapshotQueryControlPayloadV2(v1Token); return err },
	} {
		if err := check(); err == nil {
			t.Fatalf("%s: accepted", name)
		}
	}
}

func TestSnapshotQueryControlV2ValidatesFreshnessAndAllowlist(t *testing.T) {
	signer, _ := NewRelaySigner(statementV2TestKey)
	binding := snapshotQueryControlV2Fixture(signer.Address())
	validator := NewEthValidator([]string{signer.Address()}, time.Minute, false, true, "", nil)
	token, err := signer.SignSnapshotQueryControlV2(binding)
	if err != nil {
		t.Fatal(err)
	}
	if account, err := validator.ValidateSnapshotQueryControlV2(token, binding); err != nil || account != signer.Address() {
		t.Fatalf("validate = %q, %v", account, err)
	}
	expired, err := signer.signSnapshotQueryControlV2At(binding, time.Now().Add(-2*time.Minute).Unix())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := validator.ValidateSnapshotQueryControlV2(expired, binding); err == nil || !strings.Contains(err.Error(), "expired") {
		t.Fatalf("expired token accepted: %v", err)
	}
	other := NewEthValidator([]string{"0x0000000000000000000000000000000000000001"}, time.Minute, false, true, "", nil)
	if _, err := other.ValidateSnapshotQueryControlV2(token, binding); err == nil || !strings.Contains(err.Error(), "allowlist") {
		t.Fatalf("signer outside the allowlist accepted: %v", err)
	}
}

func TestSnapshotQueryControlV2TreatsEmptyTargetAsExactValue(t *testing.T) {
	signer, _ := NewRelaySigner(statementV2TestKey)
	binding := snapshotQueryControlV2Fixture(signer.Address())
	binding.TargetTableID = ""
	token, err := signer.SignSnapshotQueryControlV2(binding)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := VerifySnapshotQueryControlSignatureV2(token, binding); err != nil {
		t.Fatalf("empty target exact binding refused: %v", err)
	}
	binding.TargetTableID = "tbl"
	if _, err := VerifySnapshotQueryControlSignatureV2(token, binding); err == nil || !strings.Contains(err.Error(), "target_table_id") {
		t.Fatalf("empty target behaved as wildcard: %v", err)
	}
}
