package snapshotquery

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// All historical/read/use ports remain non-authoritative test fixtures. The
// public verifier uses real A2 crypto and the receipt signer uses real ed25519.
type consumerSigner struct {
	inner         replay.Signer
	calls         int
	hook          func(string)
	id, signature string
	err           error
	override      bool
}

func assertNoConsumerAttestation(t *testing.T, got replay.SnapshotQueryAttestation, err error) {
	t.Helper()
	if err == nil || !reflect.DeepEqual(got, replay.SnapshotQueryAttestation{}) {
		t.Fatalf("expected zero attestation, got %+v, %v", got, err)
	}
}

func TestConsumerSignatureAndRootsRefuseBeforeHistory(t *testing.T) {
	for name, change := range map[string]func(*testing.T, *executionFixture){
		"signature": func(t *testing.T, f *executionFixture) {
			p := strings.Split(f.job.Statement.Envelope.UserJWS, ".")
			sig, _ := base64.RawURLEncoding.DecodeString(p[2])
			sig[0] ^= 1
			p[2] = base64.RawURLEncoding.EncodeToString(sig)
			f.job.Statement.Envelope.UserJWS = strings.Join(p, ".")
		},
		"purpose": func(t *testing.T, f *executionFixture) {
			p := strings.Split(f.job.Statement.Envelope.UserJWS, ".")
			raw, _ := base64.RawURLEncoding.DecodeString(p[1])
			raw = bytes.Replace(raw, []byte(auth.StatementPurposeV3), []byte("housegate-statement-v2"), 1)
			p[1] = base64.RawURLEncoding.EncodeToString(raw)
			f.job.Statement.Envelope.UserJWS = strings.Join(p, ".")
		},
		"other-recovered-account": func(t *testing.T, f *executionFixture) {
			s, err := auth.NewRelaySigner("0000000000000000000000000000000000000000000000000000000000000002")
			if err != nil {
				t.Fatal(err)
			}
			f.job.Statement.Envelope.UserJWS, err = s.SignStatementV3(auth.JWSStatementPayloadV3{Iat: 1, Binding: f.job.Statement.Envelope.Input.Binding, InputRoot: f.job.Statement.Envelope.InputRoot})
			if err != nil {
				t.Fatal(err)
			}
		},
		"sql": func(t *testing.T, f *executionFixture) { f.job.Statement.Envelope.Input.SQL += " " },
		"read-descriptor": func(t *testing.T, f *executionFixture) {
			f.job.Statement.Envelope.Input.ReadSet.Tables[0].Table = "changed"
		},
		"input-root": func(t *testing.T, f *executionFixture) {
			f.job.Statement.Envelope.InputRoot = replay.DigestString("other")
		},
		"pin": func(t *testing.T, f *executionFixture) {
			f.job.Statement.Envelope.Input.Binding.ReadSnapshot.StateRoot = replay.DigestString("other")
		},
		"original-token-whitespace": func(t *testing.T, f *executionFixture) {
			f.job.Statement.Envelope.UserJWS = " " + f.job.Statement.Envelope.UserJWS
		},
	} {
		t.Run(name, func(t *testing.T) {
			f, result, signer, _ := consumerSetup(t)
			calls := 0
			v := consumerVerifier(t, f, consumerExecutor(func(context.Context, replay.ExecutionRequest) (replay.ExecutionResult, error) {
				calls++
				return result, nil
			}), signer)
			change(t, f)
			got, err := v.Verify(context.Background(), VerifyRequest{Job: f.job, ReferenceID: "ref"})
			assertNoConsumerAttestation(t, got, err)
			if f.policy.calls != 0 || calls != 0 || signer.calls != 0 {
				t.Fatal("invalid signature/input escaped", f.policy.calls, calls, signer.calls)
			}
		})
	}
}

func TestConsumerHistoryAndClaimBeforeRoute(t *testing.T) {
	for name, change := range map[string]func(*executionFixture){
		"history-error":    func(f *executionFixture) { f.policy.err = errors.New("history unavailable") },
		"zero-decision":    func(f *executionFixture) { f.policy.decision = HistoricalDecision{} },
		"wrong-assignment": func(f *executionFixture) { f.policy.decision.blockSeq++ },
		"wrong-activation": func(f *executionFixture) { f.policy.decision.activation.ActivationID = "other" },
		"wrong-pin": func(f *executionFixture) {
			f.policy.decision.reservation.ReadSnapshot.ManifestRoot = replay.DigestString("other")
		},
		"missing-ready":     func(f *executionFixture) { f.policy.decision.ready = replay.SnapshotArtifactReady{} },
		"nil-claim":         func(f *executionFixture) { f.job.SourceClaim = nil },
		"claim-hash":        func(f *executionFixture) { f.job.SourceClaimRoot = replay.DigestString("other") },
		"claim-source":      func(f *executionFixture) { f.job.SourceClaim.SourceNode = " " },
		"claim-statement":   func(f *executionFixture) { f.job.SourceClaim.StatementID = "other" },
		"claim-sequence":    func(f *executionFixture) { f.job.SourceClaim.StatementSeq++ },
		"claim-block":       func(f *executionFixture) { f.job.SourceClaim.BlockSeq++ },
		"claim-input":       func(f *executionFixture) { f.job.SourceClaim.InputRoot = replay.DigestString("other") },
		"claim-reservation": func(f *executionFixture) { f.job.SourceClaim.ReservationID = "other" },
		"claim-fence":       func(f *executionFixture) { f.job.SourceClaim.FencingGeneration++ },
		"claim-abort":       func(f *executionFixture) { f.job.SourceClaim.ExecutionOutcome = "aborted" },
		"claim-output":      func(f *executionFixture) { f.job.SourceClaim.OutputRowsRoot = "bad" },
		"claim-state":       func(f *executionFixture) { f.job.SourceClaim.ComputedStateRoot = "bad" },
		"claim-partition":   func(f *executionFixture) { f.job.SourceClaim.PartitionCommitmentsAfter[0].Root = "bad" },
		"claim-duplicate-partition": func(f *executionFixture) {
			f.job.SourceClaim.PartitionCommitmentsAfter = append(f.job.SourceClaim.PartitionCommitmentsAfter, f.job.SourceClaim.PartitionCommitmentsAfter[0])
		},
		"claim-delta": func(f *executionFixture) {
			f.job.SourceClaim.PartitionDeltas = []replay.PartitionCommitment{{TableID: "x", PartitionID: "p", Root: "bad"}}
		},
		"claim-part":   func(f *executionFixture) { f.job.SourceClaim.CandidateParts[0].PartPhysHash = "bad" },
		"claim-lthash": func(f *executionFixture) { f.job.SourceClaim.CandidateParts[0].PartRowLtHash = "bad" },
		"claim-duplicate-part": func(f *executionFixture) {
			f.job.SourceClaim.CandidateParts = append(f.job.SourceClaim.CandidateParts, f.job.SourceClaim.CandidateParts[0])
		},
	} {
		t.Run(name, func(t *testing.T) {
			f, result, signer, _ := consumerSetup(t)
			calls := 0
			// Only an unrelated route exists: earlier validation must win over lookup.
			d, err := NewCompositeExecutor(nil, []QueryRoute{{QueryProfileKey{"other", replay.DigestString("other")}, consumerExecutor(func(context.Context, replay.ExecutionRequest) (replay.ExecutionResult, error) {
				calls++
				return result, nil
			})}})
			if err != nil {
				t.Fatal(err)
			}
			v, err := NewVerifier(VerifierOptions{d, signer, f.policy})
			if err != nil {
				t.Fatal(err)
			}
			change(f)
			if f.job.SourceClaim != nil && name != "claim-hash" {
				if hash, e := f.job.SourceClaim.Hash(); e == nil {
					f.job.SourceClaimRoot = hash
				}
			}
			got, err := v.Verify(context.Background(), VerifyRequest{Job: f.job, ReferenceID: "ref"})
			assertNoConsumerAttestation(t, got, err)
			if strings.Contains(err.Error(), "route unavailable") || calls != 0 || signer.calls != 0 || f.policy.calls != 1 {
				t.Fatal("validation ordering", err, calls, signer.calls, f.policy.calls)
			}
		})
	}
}

func TestConsumerExactVerificationOrderAndMissingRoute(t *testing.T) {
	f, result, signer, _ := consumerSetup(t)
	var events []string
	ex := consumerExecutor(func(context.Context, replay.ExecutionRequest) (replay.ExecutionResult, error) {
		events = append(events, "execute")
		return result, nil
	})
	d, _ := NewCompositeExecutor(nil, []QueryRoute{{QueryProfileKey{f.job.ExecutorProfileID, f.job.QueryProfileID}, ex}})
	v, err := newVerifier(VerifierOptions{d, signer, f.policy}, func(token string, want auth.JWSStatementPayloadV3) (string, error) {
		events = append(events, "signature")
		return auth.VerifyStatementV3Signature(token, want)
	})
	if err != nil {
		t.Fatal(err)
	}
	f.policy.hook = func(replay.SnapshotQueryJob) { events = append(events, "history") }
	signer.hook = func(string) { events = append(events, "sign") }
	if _, err = v.Verify(context.Background(), VerifyRequest{f.job, "ref"}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(events, []string{"signature", "history", "execute", "sign"}) {
		t.Fatal(events)
	}
	// A different immutable dispatcher cannot infer a route from profile data.
	missing, _ := NewCompositeExecutor(nil, []QueryRoute{{QueryProfileKey{"other", replay.DigestString("other")}, ex}})
	v, err = NewVerifier(VerifierOptions{missing, signer, f.policy})
	if err != nil {
		t.Fatal(err)
	}
	events = nil
	got, err := v.Verify(context.Background(), VerifyRequest{f.job, "ref"})
	assertNoConsumerAttestation(t, got, err)
	if !strings.Contains(err.Error(), "route unavailable") || !reflect.DeepEqual(events, []string{"history"}) {
		t.Fatal(err, events)
	}
}

func TestConsumerAppliedMismatchConjunction(t *testing.T) {
	for name, change := range map[string]func(*replay.ExecutionResult){
		"match": func(*replay.ExecutionResult) {},
		"state": func(r *replay.ExecutionResult) { r.ComputedStateRoot = replay.DigestString("different full state") },
		"count": func(r *replay.ExecutionResult) { r.SnapshotQuery.OutputRowCount++ },
		"output": func(r *replay.ExecutionResult) {
			r.SnapshotQuery.OutputRowsRoot = replay.DigestString("different output")
		},
		"partition-root": func(r *replay.ExecutionResult) {
			r.PartitionCommitmentsAfter[0].Root = "0x" + strings.Repeat("0", 2*lthash.Size)
		},
		"partition-identity": func(r *replay.ExecutionResult) { r.PartitionCommitmentsAfter[0].PartitionID = "other" },
		"partition-count":    func(r *replay.ExecutionResult) { r.PartitionCommitmentsAfter = nil },
	} {
		t.Run(name, func(t *testing.T) {
			f, result, signer, _ := consumerSetup(t)
			change(&result)
			v := consumerVerifier(t, f, consumerExecutor(func(context.Context, replay.ExecutionRequest) (replay.ExecutionResult, error) { return result, nil }), signer)
			got, err := v.Verify(context.Background(), VerifyRequest{f.job, "ref"})
			if err != nil {
				t.Fatal(err)
			}
			if got.Receipt.MatchSourceRoot != (name == "match") || got.Receipt.ComputedStateRoot != result.ComputedStateRoot || signer.calls != 1 || got.Receipt.AbortRecordRoot != "" {
				t.Fatal(got, signer.calls)
			}
		})
	}
}

func TestConsumerPartitionOrderAndPhysicalPartIndependence(t *testing.T) {
	f, result, signer, _ := consumerSetup(t)
	p := result.PartitionCommitmentsAfter[0]
	p.PartitionID = "second"
	result.PartitionCommitmentsAfter = append(result.PartitionCommitmentsAfter, p)
	consumerClaim(t, &f.job, result)
	f.job.SourceClaim.PartitionCommitmentsAfter[0], f.job.SourceClaim.PartitionCommitmentsAfter[1] = f.job.SourceClaim.PartitionCommitmentsAfter[1], f.job.SourceClaim.PartitionCommitmentsAfter[0]
	f.job.SourceClaim.CandidateParts[0].PartName = "different-physical-layout"
	f.job.SourceClaimRoot, _ = f.job.SourceClaim.Hash()
	result.AffectedParts[0].StorageRefs = []string{"untrusted-hint"}
	v := consumerVerifier(t, f, consumerExecutor(func(context.Context, replay.ExecutionRequest) (replay.ExecutionResult, error) { return result, nil }), signer)
	got, err := v.Verify(context.Background(), VerifyRequest{f.job, "ref"})
	if err != nil || !got.Receipt.MatchSourceRoot {
		t.Fatal(got, err)
	}
	got.Receipt.AffectedParts[0].StorageRefs = []string{"different-hint"}
	hash, err := got.Receipt.Hash()
	if err != nil || hash != got.ReceiptHash {
		t.Fatal("hints entered receipt hash", err)
	}
}

func TestConsumerMalformedResultRefuses(t *testing.T) {
	for name, change := range map[string]func(*replay.ExecutionResult){
		"block": func(r *replay.ExecutionResult) { r.BlockSeq++ }, "previous": func(r *replay.ExecutionResult) { r.PrevSafeSnapshotID = replay.DigestString("other") },
		"previous-state": func(r *replay.ExecutionResult) { r.PrevStateRoot = replay.DigestString("other") }, "schema": func(r *replay.ExecutionResult) { r.SchemaSnapshotID = "other" },
		"executor": func(r *replay.ExecutionResult) { r.ExecutorProfileID = "other" }, "nil-evidence": func(r *replay.ExecutionResult) { r.SnapshotQuery = nil },
		"abort": func(r *replay.ExecutionResult) { r.SnapshotQuery.ExecutionOutcome = "aborted" }, "output-root": func(r *replay.ExecutionResult) { r.SnapshotQuery.OutputRowsRoot = "bad" },
		"state-root": func(r *replay.ExecutionResult) { r.ComputedStateRoot = "bad" }, "log": func(r *replay.ExecutionResult) { r.ReplayLogHash = "bad" },
		"partition-root": func(r *replay.ExecutionResult) { r.PartitionCommitmentsAfter[0].Root = "bad" },
		"partition-duplicate": func(r *replay.ExecutionResult) {
			r.PartitionCommitmentsAfter = append(r.PartitionCommitmentsAfter, r.PartitionCommitmentsAfter[0])
		},
		"part-digest":    func(r *replay.ExecutionResult) { r.AffectedParts[0].PartPhysHash = "bad" },
		"part-lthash":    func(r *replay.ExecutionResult) { r.AffectedParts[0].PartRowLtHash = "bad" },
		"part-identity":  func(r *replay.ExecutionResult) { r.AffectedParts[0].TableID = "" },
		"part-duplicate": func(r *replay.ExecutionResult) { r.AffectedParts = append(r.AffectedParts, r.AffectedParts[0]) },
	} {
		t.Run(name, func(t *testing.T) {
			f, result, signer, _ := consumerSetup(t)
			change(&result)
			v := consumerVerifier(t, f, consumerExecutor(func(context.Context, replay.ExecutionRequest) (replay.ExecutionResult, error) { return result, nil }), signer)
			got, err := v.Verify(context.Background(), VerifyRequest{f.job, "ref"})
			assertNoConsumerAttestation(t, got, err)
			if signer.calls != 0 {
				t.Fatal("malformed result signed")
			}
		})
	}
}

func TestConsumerErrorsCancellationAndSignerRefusals(t *testing.T) {
	for _, stage := range []string{"nil-context", "canceled-entry", "blank-reference", "history-cancel", "result-error", "execute-cancel", "signer-error", "blank-id", "blank-signature", "signer-cancel"} {
		t.Run(stage, func(t *testing.T) {
			f, result, signer, _ := consumerSetup(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			ex := consumerExecutor(func(context.Context, replay.ExecutionRequest) (replay.ExecutionResult, error) {
				calls++
				if stage == "execute-cancel" {
					cancel()
				}
				if stage == "result-error" {
					return result, errors.New("execution failed with partial result")
				}
				return result, nil
			})
			v := consumerVerifier(t, f, ex, signer)
			req := VerifyRequest{f.job, "ref"}
			switch stage {
			case "nil-context":
				ctx = nil
			case "canceled-entry":
				cancel()
			case "blank-reference":
				req.ReferenceID = " \n\t"
			case "history-cancel":
				f.policy.hook = func(replay.SnapshotQueryJob) { cancel() }
			case "signer-error":
				signer.override = true
				signer.err = errors.New("sign failed")
			case "blank-id":
				signer.override = true
				signer.id = " \t"
				signer.signature = "sig"
			case "blank-signature":
				signer.override = true
				signer.id = "replica"
				signer.signature = " \n"
			case "signer-cancel":
				signer.hook = func(string) { cancel() }
			}
			got, err := v.Verify(ctx, req)
			assertNoConsumerAttestation(t, got, err)
			expectSign := strings.HasPrefix(stage, "signer-") || strings.HasPrefix(stage, "blank-") && stage != "blank-reference"
			if signer.calls != 0 && !expectSign || expectSign && signer.calls != 1 || calls > 1 {
				t.Fatal(stage, calls, signer.calls)
			}
		})
	}
}

func TestConsumerB4FailuresKeepOwnership(t *testing.T) {
	for _, stage := range []string{"admission", "open-no-handle", "schema-object", "analysis", "prepare", "stream", "stream-close", "read-close", "lease-close", "output-limit"} {
		t.Run(stage, func(t *testing.T) {
			f, _, signer, _ := consumerSetup(t)
			cause := errors.New("fixture " + stage)
			switch stage {
			case "admission":
				f.uses.err = cause
			case "open-no-handle":
				f.store.err = cause
			case "schema-object":
				f.read.artifact.AuthorityJWS = "invalid-original-certificate"
			case "analysis":
				f.analyzer.analyzeErr = cause
			case "prepare":
				f.analyzer.prepareErr = cause
			case "stream":
				f.read.rows.endErr = cause
			case "stream-close":
				f.read.rows.closeErr = cause
			case "read-close":
				f.read.closeErr = cause
			case "lease-close":
				f.uses.lease.err = cause
			case "output-limit":
				f.read.rows.rows = make([][]any, 101)
				for i := range f.read.rows.rows {
					f.read.rows.rows[i] = []any{int64(i)}
				}
			}
			v := consumerVerifier(t, f, f.executor(t), signer)
			got, err := v.Verify(context.Background(), VerifyRequest{f.job, "live-reference"})
			assertNoConsumerAttestation(t, got, err)
			if signer.calls != 0 || f.uses.calls != 1 || f.read.queries > 1 {
				t.Fatal("retried or signed", f.events)
			}
			uncertain := stage == "open-no-handle" || stage == "stream-close" || stage == "read-close" || stage == "admission"
			if uncertain && f.uses.lease.closes != 0 || !uncertain && f.uses.lease.closes != 1 {
				t.Fatal("incorrect lease closure", stage, f.events)
			}
		})
	}
}

// This test-owned wrapper retains the failed output owner for recovery. It
// exercises B4's real replayPrepared boundary without altering production seams.
type consumerOutputFailure struct {
	CanonicalOutput
	calls   int
	failure error
}

func (o *consumerOutputFailure) Close() error {
	o.calls++
	_ = o.CanonicalOutput.Close()
	return o.failure
}

func TestConsumerTransferredOutputAndCursorCloseFailures(t *testing.T) {
	for _, stage := range []string{"output", "cursor"} {
		t.Run(stage, func(t *testing.T) {
			f, _, signer, _ := consumerSetup(t)
			cause := errors.New("unsettled " + stage)
			var output *consumerOutputFailure
			var prepared *PreparedQueryExecution
			ex := consumerExecutor(func(ctx context.Context, r replay.ExecutionRequest) (replay.ExecutionResult, error) {
				if stage == "cursor" {
					return replay.ExecutionResult{}, closeInvocation(f.uses.lease, true, f.read, &ownedStream{RowStream: f.read.rows}, &fixtureCursor{err: cause, events: &f.events})
				}
				var err error
				prepared, err = f.executor(t).Prepare(ctx, r)
				if err != nil {
					return replay.ExecutionResult{}, err
				}
				output = &consumerOutputFailure{CanonicalOutput: prepared.ownedOutput, failure: cause}
				prepared.ownedOutput = output
				return replayPrepared(prepared)
			})
			got, err := consumerVerifier(t, f, ex, signer).Verify(context.Background(), VerifyRequest{f.job, "ref"})
			assertNoConsumerAttestation(t, got, err)
			if !errors.Is(err, cause) || signer.calls != 0 {
				t.Fatal(err, signer.calls)
			}
			if stage == "output" {
				if f.uses.lease.closes != 1 || output.calls != 1 || !errors.Is(prepared.Close(), cause) || output.calls != 1 {
					t.Fatal("output failure resurrected lease or dropped retained owner", f.events)
				}
			} else if f.uses.lease.closes != 0 {
				t.Fatal("uncertain cursor released lease")
			}
		})
	}
}

func TestConsumerZeroRowsAndUntouchedFullState(t *testing.T) {
	for _, variant := range []string{"zero", "alter-U", "drop-U"} {
		t.Run(variant, func(t *testing.T) {
			source := newExecutionFixture(t)
			signConsumerJob(t, source)
			verifier := newExecutionFixture(t)
			signConsumerJob(t, verifier)
			if variant == "zero" {
				source.read.rows.rows = nil
				verifier.read.rows.rows = nil
			}
			prepared, err := source.executor(t).Prepare(context.Background(), source.request())
			if err != nil {
				t.Fatal(err)
			}
			sourceResult := prepared.Result
			post := prepared.Manifest()
			if err = prepared.Close(); err != nil {
				t.Fatal(err)
			}
			if variant != "zero" {
				for i := range post.Tables {
					if post.Tables[i].TableID == "untouched-U" {
						if variant == "drop-U" {
							post.Tables = append(post.Tables[:i], post.Tables[i+1:]...)
						} else {
							h := lthash.New()
							h.Add([]byte("source changed untouched U"))
							post.Tables[i].PartitionRoots = []replay.PartitionCommitment{{TableID: "untouched-U", PartitionID: "p", Root: "0x" + hex.EncodeToString(h.Bytes())}}
						}
						break
					}
				}
				// Source evidence comes from the actual complete post-manifest, altered in
				// the fraud case. The verifier runs B4/ApplyRows over its untouched S.
				_, sourceResult.ComputedStateRoot, err = replay.AssembleStateRoot(post.SchemaSnapshotID, post.SchemaRoot, post.ExecutorProfileID, post.Tables)
				if err != nil {
					t.Fatal(err)
				}
			}
			consumerClaim(t, &verifier.job, sourceResult)
			signerBase, err := payloadexec.NewEd25519Signer("replica", bytes.Repeat([]byte{9}, 32))
			if err != nil {
				t.Fatal(err)
			}
			signer := &consumerSigner{inner: signerBase}
			got, err := consumerVerifier(t, verifier, verifier.executor(t), signer).Verify(context.Background(), VerifyRequest{verifier.job, "independent-verifier-reference"})
			if err != nil {
				t.Fatal(err)
			}
			if got.Receipt.MatchSourceRoot != (variant == "zero") || got.Receipt.OutputRowCount != sourceResult.SnapshotQuery.OutputRowCount || got.Receipt.OutputRowsRoot != sourceResult.SnapshotQuery.OutputRowsRoot {
				t.Fatal("full state/output comparison", variant, got)
			}
			if variant == "zero" && (got.Receipt.OutputRowCount != 0 || got.Receipt.ComputedStateRoot != verifier.job.PrevStateRoot) {
				t.Fatal("zero output changed data state")
			}
			if verifier.read.queries != 1 || source.read.queries != 1 || signer.calls != 1 {
				t.Fatal("not independent single execution")
			}
		})
	}
}

func TestConsumerCopiesBeforeDependenciesAndSigning(t *testing.T) {
	f, result, signer, _ := consumerSetup(t)
	original := cloneJob(f.job)
	result.AffectedParts[0].StorageRefs = []string{"original-hint"}
	expectedResult := cloneExecutionResult(result)
	f.policy.hook = func(j replay.SnapshotQueryJob) {
		j.Statement.Envelope.Input.ReadSet.Tables[0].Table = "policy changed its copy"
		j.SourceClaim.CandidateParts[0].PartName = "policy changed its copy"
		f.job.Statement.Envelope.Input.ReadSet.Tables[0].Table = "caller changed after transfer"
		f.job.SourceClaim.PartitionCommitmentsAfter[0].Root = "caller changed after transfer"
	}
	ex := consumerExecutor(func(_ context.Context, r replay.ExecutionRequest) (replay.ExecutionResult, error) {
		if !reflect.DeepEqual(*r.SnapshotQuery, original) {
			t.Fatal("policy/caller mutation reached executor")
		}
		r.SnapshotQuery.Statement.Envelope.Input.ReadSet.Tables[0].Table = "executor copy"
		r.SnapshotQuery.SourceClaim.OutputRowsRoot = "executor copy"
		return result, nil
	})
	signer.hook = func(string) {
		result.SnapshotQuery.OutputRowCount++
		result.PartitionCommitmentsAfter[0].Root = "signer changed external result"
		result.AffectedParts[0].StorageRefs[0] = "signer changed external result"
	}
	v := consumerVerifier(t, f, ex, signer)
	got, err := v.Verify(context.Background(), VerifyRequest{f.job, "ref"})
	if err != nil {
		t.Fatal(err)
	}
	if got.Receipt.OutputRowCount != expectedResult.SnapshotQuery.OutputRowCount || got.Receipt.PartitionCommitmentsAfter[0] != expectedResult.PartitionCommitmentsAfter[0] || got.Receipt.AffectedParts[0].StorageRefs[0] != "original-hint" || !got.Receipt.MatchSourceRoot {
		t.Fatal("dependency mutation reached signed evidence")
	}
	got.Receipt.PartitionCommitmentsAfter[0].Root = "caller changed receipt"
	got.Receipt.AffectedParts[0].StorageRefs[0] = "caller changed receipt"
	f.job = cloneJob(original)
	result = cloneExecutionResult(expectedResult)
	f.policy.hook = nil
	signer.hook = nil
	again, err := v.Verify(context.Background(), VerifyRequest{f.job, "second-ref"})
	if err != nil || !again.Receipt.MatchSourceRoot || again.Receipt.PartitionCommitmentsAfter[0].Root == "caller changed receipt" || again.Receipt.AffectedParts[0].StorageRefs[0] != "original-hint" {
		t.Fatal("receipt alias escaped", err)
	}
}

func (s *consumerSigner) SignReplayReceipt(ctx context.Context, hash string) (string, string, error) {
	s.calls++
	if s.hook != nil {
		s.hook(hash)
	}
	if s.override {
		return s.id, s.signature, s.err
	}
	return s.inner.SignReplayReceipt(ctx, hash)
}
func signConsumerJob(t *testing.T, f *executionFixture) {
	t.Helper()
	signer, err := auth.NewRelaySigner("0000000000000000000000000000000000000000000000000000000000000001")
	if err != nil {
		t.Fatal(err)
	}
	b := &f.job.Statement.Envelope.Input.Binding
	b.ClientAccount = signer.Address()
	b.StatementID = signer.Address() + ":1:consumer"
	f.job.Reservation.ClientAccount = b.ClientAccount
	f.job.Reservation.StatementID = b.StatementID
	f.job.Statement.Envelope.InputRoot, err = replay.SnapshotQueryInputRoot(f.job.Statement.Envelope.Input)
	if err != nil {
		t.Fatal(err)
	}
	f.job.Statement.Envelope.UserJWS, err = signer.SignStatementV3(auth.JWSStatementPayloadV3{Iat: 1, Binding: *b, InputRoot: f.job.Statement.Envelope.InputRoot})
	if err != nil {
		t.Fatal(err)
	}
	f.records.Reservation = f.job.Reservation
	f.records.StatementRoot, err = replay.SnapshotQueryStatementRoot(f.job.Statement)
	if err != nil {
		t.Fatal(err)
	}
	f.policy.decision, err = NewHistoricalDecisionFromVerifiedRecords(f.job, f.records)
	if err != nil {
		t.Fatal(err)
	}
}
func consumerClaim(t *testing.T, j *replay.SnapshotQueryJob, result replay.ExecutionResult) {
	t.Helper()
	b := j.Statement.Envelope.Input.Binding
	j.SourceClaim = &replay.SnapshotQueryClaim{SourceNode: "source-fixture", StatementID: b.StatementID, StatementSeq: j.Statement.StatementSeq, BlockSeq: j.BlockSeq, InputRoot: j.Statement.Envelope.InputRoot, ReservationID: b.ReservationID, FencingGeneration: b.FencingGeneration, ExecutionOutcome: "applied", OutputRowCount: result.SnapshotQuery.OutputRowCount, OutputRowsRoot: result.SnapshotQuery.OutputRowsRoot, ComputedStateRoot: result.ComputedStateRoot, PartitionCommitmentsAfter: append([]replay.PartitionCommitment{}, result.PartitionCommitmentsAfter...)}
	for _, p := range result.AffectedParts {
		j.SourceClaim.CandidateParts = append(j.SourceClaim.CandidateParts, replay.SnapshotReadPart{TableID: p.TableID, PartitionID: p.PartitionID, PartName: p.PartName, PartPhysHash: p.PartPhysHash, PartRowLtHash: p.PartRowLtHash, RowCount: p.RowCount, Bytes: p.Bytes})
	}
	var err error
	j.SourceClaimRoot, err = j.SourceClaim.Hash()
	if err != nil {
		t.Fatal(err)
	}
}
func consumerSetup(t *testing.T) (*executionFixture, replay.ExecutionResult, *consumerSigner, ed25519.PublicKey) {
	t.Helper()
	f := newExecutionFixture(t)
	signConsumerJob(t, f)
	result, err := f.executor(t).Replay(context.Background(), f.request())
	if err != nil {
		t.Fatal(err)
	}
	consumerClaim(t, &f.job, result)
	f.policy.calls = 0
	f.read.rows.n = 0
	f.read.rows.closes = 0
	f.read.closes = 0
	f.read.queries = 0
	f.store.calls = 0
	f.uses.calls = 0
	f.uses.lease.closes = 0
	f.events = nil
	signer, err := payloadexec.NewEd25519Signer("receipt-replica", bytes.Repeat([]byte{7}, ed25519.SeedSize))
	if err != nil {
		t.Fatal(err)
	}
	return f, result, &consumerSigner{inner: signer}, signer.PublicKey()
}
func consumerVerifier(t *testing.T, f *executionFixture, ex replay.Executor, signer replay.Signer) *Verifier {
	t.Helper()
	d, err := NewCompositeExecutor(nil, []QueryRoute{{QueryProfileKey{f.job.ExecutorProfileID, f.job.QueryProfileID}, ex}})
	if err != nil {
		t.Fatal(err)
	}
	v, err := NewVerifier(VerifierOptions{Dispatcher: d, Signer: signer, HistoricalPolicy: f.policy})
	if err != nil {
		t.Fatal(err)
	}
	return v
}
func TestConsumerRealCryptoAndB4Handoff(t *testing.T) {
	f, expected, signer, pub := consumerSetup(t)
	v := consumerVerifier(t, f, f.executor(t), signer)
	for _, reference := range []string{" first-exact-reference ", "second-reference"} {
		f.read.rows.n = 0
		got, err := v.Verify(context.Background(), VerifyRequest{Job: f.job, ReferenceID: reference})
		if err != nil {
			t.Fatal(err)
		}
		hash, err := got.Receipt.Hash()
		if err != nil {
			t.Fatal(err)
		}
		sig, err := hex.DecodeString(got.Signature)
		if err != nil {
			t.Fatal(err)
		}
		if !ed25519.Verify(pub, []byte(hash), sig) || hash != got.ReceiptHash || !got.Receipt.MatchSourceRoot || got.Receipt.ComputedStateRoot != expected.ComputedStateRoot || got.Receipt.SourceClaimRoot == got.Receipt.ComputedStateRoot {
			t.Fatal("invalid attestation", got)
		}
		if f.uses.ref != reference || f.store.ref != reference {
			t.Fatal("reference changed")
		}
		if got.Receipt.AbortRecordRoot != "" || got.Receipt.ExecutionOutcome != "applied" || got.Receipt.StatementRoot != f.records.StatementRoot {
			t.Fatal("receipt identity")
		}
	}
	if f.policy.calls != 4 || f.store.calls != 2 || f.uses.calls != 2 || f.uses.lease.closes != 2 || signer.calls != 2 {
		t.Fatal("ordering/count", f.events)
	}
	raw, err := json.Marshal(VerifyRequest{Job: f.job, ReferenceID: "secret-reference"})
	if err != nil || string(raw) != "{}" {
		t.Fatal(string(raw), err)
	}
}

func TestConsumerConstructor(t *testing.T) {
	f, result, signer, _ := consumerSetup(t)
	ex := consumerExecutor(func(context.Context, replay.ExecutionRequest) (replay.ExecutionResult, error) { return result, nil })
	d, _ := NewCompositeExecutor(nil, []QueryRoute{{QueryProfileKey{f.job.ExecutorProfileID, f.job.QueryProfileID}, ex}})
	good := VerifierOptions{Dispatcher: d, Signer: signer, HistoricalPolicy: f.policy}
	for _, change := range []func(*VerifierOptions){func(o *VerifierOptions) { o.Dispatcher = nil }, func(o *VerifierOptions) { o.Dispatcher = &CompositeExecutor{} }, func(o *VerifierOptions) { o.Signer = (*consumerSigner)(nil) }, func(o *VerifierOptions) { o.HistoricalPolicy = (*fixturePolicy)(nil) }, func(o *VerifierOptions) { o.Dispatcher, _ = NewCompositeExecutor(ex, nil) }} {
		o := good
		change(&o)
		if _, err := NewVerifier(o); err == nil {
			t.Fatal("invalid constructor")
		}
	}
	var v Verifier
	got, err := v.Verify(context.Background(), VerifyRequest{Job: f.job, ReferenceID: "ref"})
	if err == nil || !reflect.DeepEqual(got, replay.SnapshotQueryAttestation{}) {
		t.Fatal(got, err)
	}
}

func TestConsumerFreezesDispatcherValueAtConstruction(t *testing.T) {
	f, result, signer, _ := consumerSetup(t)
	ex := consumerExecutor(func(context.Context, replay.ExecutionRequest) (replay.ExecutionResult, error) { return result, nil })
	d, err := NewCompositeExecutor(nil, []QueryRoute{{QueryProfileKey{f.job.ExecutorProfileID, f.job.QueryProfileID}, ex}})
	if err != nil {
		t.Fatal(err)
	}
	v, err := NewVerifier(VerifierOptions{d, signer, f.policy})
	if err != nil {
		t.Fatal(err)
	}
	// The caller can replace its exported struct value even though its fields
	// are private. The verifier must retain its original immutable route table.
	*d = CompositeExecutor{}
	got, err := v.Verify(context.Background(), VerifyRequest{f.job, "ref"})
	if err != nil || !got.Receipt.MatchSourceRoot {
		t.Fatal("dispatcher replacement changed verifier routes", err)
	}
}
