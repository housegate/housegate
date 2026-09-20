package snapshotquery

import (
	"fmt"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/replay"
)

// VerifyEnvelope performs the v3 envelope check shared by every caller that
// must authenticate a signed snapshot-query statement: validate the complete
// input, recompute the input root and match it against the envelope's own
// InputRoot, verify the "housegate-statement-v3" signature over
// purpose/binding/root, and confirm the recovered signer matches
// Input.Binding.ClientAccount. It returns the lowercase recovered account on
// success.
//
// VerifyEnvelope is envelope-internal: it authenticates only the envelope's
// own bytes and signature. It deliberately does not bind the envelope to a
// job's reservation, block assignment or historical policy — those remain
// the caller's responsibility (see Verifier.Verify and validateJob).
func VerifyEnvelope(envelope replay.SnapshotQueryEnvelope) (account string, err error) {
	account, _, err = verifyEnvelope(envelope, auth.VerifyStatementV3Signature)
	return account, err
}

// verifyEnvelope is the shared implementation behind VerifyEnvelope and
// Verifier.Verify's injectable-signature-verifier test seam (see
// newVerifier/statementV3SignatureVerifier in verifier.go). It additionally
// returns the recomputed input root so a caller that also needs it for a
// receipt is not forced to recompute it a second time.
func verifyEnvelope(envelope replay.SnapshotQueryEnvelope, verify statementV3SignatureVerifier) (string, string, error) {
	in := envelope.Input
	if err := replay.ValidateSnapshotQueryInput(in); err != nil {
		return "", "", fmt.Errorf("validate complete input: %w", err)
	}
	root, err := replay.SnapshotQueryInputRoot(in)
	if err != nil {
		return "", "", fmt.Errorf("recompute input root: %w", err)
	}
	if root != envelope.InputRoot {
		return "", "", fmt.Errorf("input_root mismatch")
	}
	want := auth.JWSStatementPayloadV3{Purpose: auth.StatementPurposeV3, Binding: in.Binding, InputRoot: root}
	account, err := verify(envelope.UserJWS, want)
	if err != nil {
		return "", "", fmt.Errorf("verify statement signature: %w", err)
	}
	if account != in.Binding.ClientAccount {
		return "", "", fmt.Errorf("client_account does not match signature")
	}
	return account, root, nil
}
