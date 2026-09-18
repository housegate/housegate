package auth

import (
	"encoding/base64"
	"strings"
	"testing"
	"time"
)

func snapshotQueryControlFixture(account string) SnapshotQueryControlBinding {
	return SnapshotQueryControlBinding{
		Operation:         SnapshotQueryControlOperationRelease,
		NetworkID:         "testnet-v2",
		KeeperShardID:     7,
		ClientAccount:     account,
		StatementID:       account + ":42:n1",
		RequestID:         "request-42",
		ReservationID:     "reservation-42",
		FencingGeneration: 9,
	}
}

func TestSnapshotQueryControlFixedCanonicalVector(t *testing.T) {
	signer, err := NewRelaySigner(statementV2TestKey)
	if err != nil {
		t.Fatal(err)
	}
	binding := snapshotQueryControlFixture(signer.Address())
	token, err := signer.signSnapshotQueryControlAt(binding, 1789550000)
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
	const want = `{"purpose":"housegate-snapshot-query-control-v1","iat":1789550000,"operation":"release","network_id":"testnet-v2","keeper_shard_id":7,"client_account":"0x8fd379246834eac74b8419ffda202cf8051f7a03","statement_id":"0x8fd379246834eac74b8419ffda202cf8051f7a03:42:n1","request_id":"request-42","reservation_id":"reservation-42","fencing_generation":9}`
	if string(raw) != want {
		t.Fatalf("canonical payload = %s\\nwant = %s", raw, want)
	}
	got, err := DecodeSnapshotQueryControlPayload(token)
	if err != nil || got.Purpose != SnapshotQueryControlPurposeV1 || got.Iat != 1789550000 || got.SnapshotQueryControlBinding != binding {
		t.Fatalf("decode = %+v, %v", got, err)
	}
	account, err := VerifySnapshotQueryControlSignature(token, binding)
	if err != nil || account != signer.Address() {
		t.Fatalf("verify = %q, %v", account, err)
	}
}

func TestSnapshotQueryControlBindsEveryFieldAndOperation(t *testing.T) {
	signer, _ := NewRelaySigner(statementV2TestKey)
	binding := snapshotQueryControlFixture(signer.Address())
	token, err := signer.SignSnapshotQueryControl(binding)
	if err != nil {
		t.Fatal(err)
	}
	mutations := map[string]func(*SnapshotQueryControlBinding){
		"operation":          func(p *SnapshotQueryControlBinding) { p.Operation = SnapshotQueryControlOperationAcquire },
		"network_id":         func(p *SnapshotQueryControlBinding) { p.NetworkID = "other" },
		"keeper_shard_id":    func(p *SnapshotQueryControlBinding) { p.KeeperShardID++ },
		"client_account":     func(p *SnapshotQueryControlBinding) { p.ClientAccount = "0x0000000000000000000000000000000000000001" },
		"statement_id":       func(p *SnapshotQueryControlBinding) { p.StatementID += "x" },
		"request_id":         func(p *SnapshotQueryControlBinding) { p.RequestID += "x" },
		"reservation_id":     func(p *SnapshotQueryControlBinding) { p.ReservationID += "x" },
		"fencing_generation": func(p *SnapshotQueryControlBinding) { p.FencingGeneration++ },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			want := binding
			mutate(&want)
			if _, err := VerifySnapshotQueryControlSignature(token, want); err == nil || !strings.Contains(err.Error(), name) {
				t.Fatalf("VerifySnapshotQueryControlSignature = %v, want %s refusal", err, name)
			}
		})
	}
	for _, operation := range []string{"", "submit", "Acquire"} {
		t.Run("unknown operation "+operation, func(t *testing.T) {
			bad := binding
			bad.Operation = operation
			badToken, err := signer.SignSnapshotQueryControl(bad)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := VerifySnapshotQueryControlSignature(badToken, bad); err == nil || !strings.Contains(err.Error(), "operation") {
				t.Fatalf("unknown operation accepted: %v", err)
			}
		})
	}
}

func TestSnapshotQueryControlValidatesSignerAccountAndFreshness(t *testing.T) {
	signer, _ := NewRelaySigner(statementV2TestKey)
	binding := snapshotQueryControlFixture(signer.Address())
	validator := NewEthValidator([]string{signer.Address()}, time.Minute, false, true, "", nil)
	token, err := signer.SignSnapshotQueryControl(binding)
	if err != nil {
		t.Fatal(err)
	}
	if account, err := validator.ValidateSnapshotQueryControl(token, binding); err != nil || account != signer.Address() {
		t.Fatalf("validate = %q, %v", account, err)
	}

	wrongAccount := binding
	wrongAccount.ClientAccount = "0x0000000000000000000000000000000000000001"
	wrongToken, err := signer.SignSnapshotQueryControl(wrongAccount)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := VerifySnapshotQueryControlSignature(wrongToken, wrongAccount); err == nil || !strings.Contains(err.Error(), "client_account") {
		t.Fatalf("wrong account accepted: %v", err)
	}

	expired, err := signer.signSnapshotQueryControlAt(binding, time.Now().Add(-2*time.Minute).Unix())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := validator.ValidateSnapshotQueryControl(expired, binding); err == nil || !strings.Contains(err.Error(), "expired") {
		t.Fatalf("expired token accepted: %v", err)
	}
}

func TestSnapshotQueryControlAllowsZeroFenceOnlyAsExactValue(t *testing.T) {
	signer, _ := NewRelaySigner(statementV2TestKey)
	for _, operation := range []string{SnapshotQueryControlOperationAcquire, SnapshotQueryControlOperationLookup, SnapshotQueryControlOperationRelease} {
		t.Run(operation, func(t *testing.T) {
			binding := snapshotQueryControlFixture(signer.Address())
			binding.Operation, binding.ReservationID, binding.FencingGeneration = operation, "", 0
			token, err := signer.SignSnapshotQueryControl(binding)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := VerifySnapshotQueryControlSignature(token, binding); err != nil {
				t.Fatalf("zero fence exact binding refused: %v", err)
			}
			binding.FencingGeneration = 1
			if _, err := VerifySnapshotQueryControlSignature(token, binding); err == nil || !strings.Contains(err.Error(), "fencing_generation") {
				t.Fatalf("zero fence behaved as wildcard: %v", err)
			}
		})
	}
}
