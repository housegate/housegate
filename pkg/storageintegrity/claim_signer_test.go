package storageintegrity

import "testing"

const mutationClaimSignerTestKey = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestSecp256k1MutationClaimSignerSignsVerifiableClaimHash(t *testing.T) {
	signer, err := NewSecp256k1MutationClaimSigner(mutationClaimSignerTestKey, "worker-a")
	if err != nil {
		t.Fatalf("NewSecp256k1MutationClaimSigner: %v", err)
	}

	token, err := signer.SignClaim("0x1234")
	if err != nil {
		t.Fatalf("SignClaim: %v", err)
	}

	got, err := VerifyMutationClaimSignature(token, "0x1234")
	if err != nil {
		t.Fatalf("VerifyMutationClaimSignature: %v", err)
	}
	if got.Signer != signer.Address() || got.WorkerID != "worker-a" || got.ClaimHash != "0x1234" {
		t.Fatalf("verified claim = %+v, signer address %s", got, signer.Address())
	}
}

func TestVerifyMutationClaimSignatureRejectsDifferentClaimHash(t *testing.T) {
	signer, err := NewSecp256k1MutationClaimSigner(mutationClaimSignerTestKey, "worker-a")
	if err != nil {
		t.Fatalf("NewSecp256k1MutationClaimSigner: %v", err)
	}
	token, err := signer.SignClaim("0x1234")
	if err != nil {
		t.Fatalf("SignClaim: %v", err)
	}

	if _, err := VerifyMutationClaimSignature(token, "0xbeef"); err == nil {
		t.Fatalf("VerifyMutationClaimSignature accepted a mismatched claim hash")
	}
}
