# REPLAY COMPATIBILITY GUIDE

## OVERVIEW
`pkg/replay`, `pkg/replay/payloadexec`, `pkg/replay/chexec` and
`pkg/replay/nativepayload` are the **canonical implementation** of the
replay-verifier core, not compatibility shims. Housegate is the source of
truth: it owns the `Verifier` / `Executor` seam, the order-canonicalized
data/state/manifest roots, the `_hg_row_id` derivation, the per-row LtHash
commitments and the ClickHouse-backed materializer.

`github.com/sentioxyz/arbiter-core/replay` does not exist. Housegate does not
depend on arbiter-core; its only `sentioxyz` module requirement is
`github.com/sentioxyz/arbiter-proto`. The dependency runs the other way:
arbiter-core imports `github.com/housegate/housegate/pkg/replay` in 63 places
across 48 files.

The wire form is mirrored in `arbiter-proto`'s `proto/replay.proto`. That
mirror is under a **field-name freeze** enforced by arbiter-core's
`conformance/replay_wire_test.go`, which compares the proto descriptor's field
names against the Go structs' `json` tags. Renaming or dropping a `json` tag in
this package breaks that conformance test in another repo.

## CONVENTIONS
- Behaviour changes land **here**, with their tests, and only then propagate.
- Adding, removing or renaming a field on a type mirrored in `replay.proto`
  requires the matching arbiter-proto change plus an arbiter-core conformance
  run; do not land a one-sided rename.
- `payloadexec.TableSchema` and `lthash.Column` `json` tags are the
  network-state content contract (`table_id` / `partition_by` / `columns`,
  `name` / `type`). `TableSchemaHash` hashes the decoded semantic fields, not
  the JSON bytes.
- Root digests are domain-versioned (`safe-snapshot-data-v2`); changing what
  enters a root requires a new domain string, never a silent redefinition.
- A source-root mismatch is **signed, not errored** — it is non-repudiable
  challenge evidence. Only a pre-receipt failure is a local refusal to attest.
- This is a verifier core: no proxy plugins, no ClickHouse daemon, no
  network I/O beyond the injected `Executor` / stores.

## COMMANDS
```bash
bazel test //pkg/replay:replay_test
bazel test //pkg/replay/payloadexec:payloadexec_test
bazel test //pkg/replay/nativepayload:nativepayload_test
bazel test //pkg/replay/chexec:chexec_test
bazel test //pkg/integration:integration_test --test_filter='TestCHReplay|Test.*Replay' --test_output=errors
```
