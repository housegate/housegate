# User-Run Agents Writing SI Tables — Plan A1: housegate, Phase A

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship release step A1 of the user-agent SI-writes design: an SI ingress that admits any on-chain database writer (with a static denylist) instead of a static signer allowlist, an ordinary write path that uses the same contract predicate, a host-side guard against peer reads of governed ordinary physical tables, and an agent that a user starts with only `-agent-key` — discovering the network, choosing its state directory, switching the session to the hosting indexer, and recycling provably unspent `client_seq` values on the legacy lane.

**Architecture:** One shared statement-id grammar in `pkg/storageintegrity` replaces the three copies (laned form parsed but refused everywhere until Plan B). `registry.WriterAccess` is the host-implemented `isDatabaseWriter` predicate; the in-memory network state implements it for tests and YAML servers; the SI ingress resolves the owner itself and authorizes in `OnQuery` and again before admission, and `PermissionCommitGateObserver` uses the same predicate for every Write-bit statement under `auth.writer_predicate: contract`. Every refusal that provably leaves the coordinate unspent carries a typed flag that `exceptionForPluginError` renders once as ` [client_seq unspent]`. The new `sipeerguard` query plugin refuses peer-trusted reads that resolve to the ordinary physical table of a non-Ordinary table. On the agent, `sistatement` reserves `client_seq` in the strict input-complete hook from a flock-protected counter with a durable free list, routes status/info lookups to the hosting indexer through `RpcNetworkState`, and moves the session with the neutral `Session.SwitchUpstream` before claiming a statement.

**Tech Stack:** Go 1.26.3, Bazel 9.1.0 + Bzlmod (`bazel test //...`, `bazel mod tidy && bazel run //:gazelle`), ch-go / clickhouse-go Sentio forks, Prometheus client, `golang.org/x/sys/unix`-free `syscall.Flock` behind `//go:build linux || darwin`, ClickHouse 25.8 docker-bound integration suite, rewriter-go native engine via the polyglot FFI library (`pkg/ffifetch`).

**Spec:** [docs/superpowers/specs/2026-10-09-user-agent-si-writes-design.md](../specs/2026-10-09-user-agent-si-writes-design.md) — release step A1 (§8.1): §6.1, §6.2, §6.3, §6.4, §6.5 (legacy lane only), §6.6, §6.9 (housegate half), §9.1 and §9.2 for those parts, §10. Phase B (lanes, arbiter-proto/arbiter-core/arbiter changes, `LANE_BUDGET_EXCEEDED`, lane vectors) and release L1 (D20 client compression) are out of scope.

**Working copy:** execution creates its own worktree with Worktrunk per the URWT convention at `~/src/remotesrc/claude/housegate/user-agent-si-writes-a1`, never with `git worktree add`, never with `--yes`/`--clobber`. Run the URWT skill's shell-neutral command unchanged with the trailing arguments below (branch `feat/user-agent-si-writes-a1`, base `origin/main`) and take the worktree path from the JSON `path`:

```sh
bash -c '
repo=$1 runtime=$2 name=$3 branch=${4:-urwt/$2/$3} base=${5:-origin/main}
case $name in ""|[._-]*|*[!A-Za-z0-9._-]*) echo "urwt: invalid task name: $name" >&2; exit 2;; esac
if [[ $branch == pr:* || $branch == https://* ]] ||
   git -C "$repo" show-ref --quiet --verify "refs/heads/$branch" ||
   git -C "$repo" show-ref --quiet --verify "refs/remotes/origin/$branch"; then
  create=()
else
  create=(--create --base "$base")
fi
command wt -C "$repo" \
  --config-set "worktree-path = \"~/src/remotesrc/$runtime/{{ repo }}/$name\"" \
  switch "${create[@]}" "$branch" --no-cd --format=json
' urwt /Users/uranuswch/Dev/housegate/housegate claude user-agent-si-writes-a1 feat/user-agent-si-writes-a1
```

Re-read the worktree's `CLAUDE.md`. Every later command runs with that path as its working directory (the agent's Bash tool is zsh; the `bash`/`sh` blocks in this plan are agent-run, not commands handed to the user). Branch from `origin/main` (`010b4de` at plan time; re-check after `git fetch -q origin`). Bazel is the ground truth; after adding files or imports run `bazel mod tidy && bazel run //:gazelle`. Fetch the FFI library once for native-engine tests:

```bash
FFI="$(bazel run //cmd:housegate -- fetch-rewriter-lib --tag v0.17.0 | tail -n 1)"
echo "$FFI"   # e.g. $HOME/Library/Caches/housegate/rewriter-ffi/v0.17.0/libpolyglot_sql_ffi.dylib
```

The integration suite needs docker and a `clickhouse` client in `tests/bin/` (CI installs one; locally symlink an installed `clickhouse` binary into `tests/bin/clickhouse` for the run and remove the symlink afterwards).

## Plan decisions (ratified)

The spec is binding; where it is silent or contradicts the code at `010b4de`, this plan decides as follows. All of P1–P13 were accepted in the 2026-10-09 cross-plan review and are recorded in the spec's §4.5 "Ratified deviations" (with the normative sections updated); the `flock` build tags follow the same review (`linux || darwin`, see Global Constraints).

- **P1 (§6.1) Laned ids in A1.** `ParseStatementID` accepts both grammars so the lane rules are tested now, but every A1 consumer (ingress, intake, arbiter conversion, agent SDK path) refuses a laned id with `ErrClientLanesNotEnabled` ("storage_integrity: client lanes are not enabled on this network"). The ingress `lanesEnabled` option, `StatementID.ClientLane` proto mapping and `LANE_BUDGET_EXCEEDED` outcome arrive with Plan B (B4), because arbiter-proto has no `client_lane` field or code 9 yet.
- **P2 (§6.4 writer pre-check) — coordinated with Plan A2.** `sentio_getDatabaseInfoByAccount` cannot tell a Read-only grantee from a writer (it returns readable databases, already merged with address(0) grants). Plan A2 adds the storage RPC `sentio_isDatabaseWriter(database, account) -> bool` on every indexer; the agent's advisory pre-check calls it on the hosting indexer's storage RPC through `RpcNetworkState.StorageIntegrityWriterCheck`. A `false` answer refuses locally and session-preserving; an RPC error, an unsupported method or a non-RPC registry is "unknown" and falls through to the server's authoritative 497. The pre-check is skipped for driver sessions.
- **P3 (§6.5 legacy free list).** The legacy `<signer>.seq` keeps its single-integer format (an old binary must still read it). Its free list lives beside it in `<signer>.seq.free` (`{"version":1,"free":[…]}`, same tmp + fsync + rename + dir fsync discipline), and its lock is `<signer>.seq.lock`.
- **P4 (§6.4 default state directory with the legacy lane).** An explicit `storage_integrity.agent.state_dir` keeps `<state_dir>/<signer>.seq` exactly as today. Without one, the legacy counter lives in the SI subtree: `<base>/si/<network_id>/<signer>/<signer>.seq`, opened at the first SI write for that network.
- **P5 (§6.4 "Explicit YAML keeps winning").** The quickstart defaults (`-network` preset, `-si auto`, `-si-inline-values auto`, implicit native materializer, `-listen 127.0.0.1:9000`, implied agent mode from `-agent-key`) apply only when no config file was loaded. With a config file only explicitly passed flags or set env vars change it.
- **P6 (§6.4 `-si auto` vs `on`).** `auto` enables the SI plugin only when the agent's network state is an RPC source (discovery possible); `on` enables it regardless (a YAML or host-injected status source then needs `network_id`); `off` disables it.
- **P7 (§6.4 discovery failure with a configured network id).** Spec step 4 refuses when the info lookup fails. With an explicitly configured `network_id` the agent instead warns, counts `clickhouse_proxy_agent_si_discovery_failures_total{step="info"}` and signs for the configured id, so an existing configured agent keeps writing against a sentio-node that predates `sentio_getStorageIntegrityInfo` (the driver sidecar itself stays on housegate v0.15.0 in Phase A, per Plan A2). A discovered id that differs from the configured one still refuses. Without a configured id the refusal is exactly the spec's.
- **P8 (§6.5 in-flight cap, §10 lane metrics).** `max_inflight_per_lane`, `clickhouse_proxy_agent_si_inflight` and `clickhouse_proxy_agent_si_lane_rotations_total` are lane machinery and ship with Plan B. `clickhouse_proxy_agent_si_seq_burned_total` emits `unknown_outcome` and `free_list_overflow` here; `crash` cannot be observed at runtime and is not emitted.
- **P9 (§6.3 table split).** HouseGate configures no extras, so a physical table identifier is `<logical>.<table>`. The guard looks up both the first-`.` and the last-`.` split of every candidate and refuses if either is non-Ordinary. For form 3 it skips the first argument of the carrier call (the address or cluster name), because `'10.0.0.1:9000'` would otherwise look up `10` under the Pending default and refuse every IP-addressed `remote()`.
- **P10 (§6.4 step 2).** A session database `cur` that the registry does not know (for example `default`) is treated like an empty one: the target's `forward` plugin keeps an unknown database local, so replaying it does not pivot the session.
- **P11 (§6.9).** `OnStatementSequenced` fires whenever the intake's submit outcome is `OutcomeAccepted`, including an idempotent ACK2 replay of the same statement id; consumers dedupe on `StatementID` (documented on the interface).
- **P13 — constraints from the finished Plan A2 (sentio-node/production).** (a) `registry.WriterAccess` is exactly `IsDatabaseWriter(database, account string) (bool, error)` in `pkg/registry`; sentio-node implements it on the `RedisNetworkState` it injects (and `FromStatecore`), and HouseGate detects it only by type assertion on the configured or host-injected registry, refusing startup when the ingress is enabled, or `auth.enabled` with `auth.writer_predicate: contract`, and it is absent. (b) `registry.ProxyAddress.StorageRPCPort` (`uint16`) keeps that name; sentio-node fills it from `StorageNodeRpcPort`. (c) `sentio_getStorageIntegrityInfo` answers `enabled` (SI runtime present and ingress enabled), nullable `si_indexer_id` (null while the registry is disabled) and nullable `self_indexer_id`, `default_read_mode` where `""` means `safe`, `keeper_shard_id` always 0, `client_lanes_enabled` false in Phase A; the A1 client parses nullable ids as `*uint64`. (d) The driver sidecar stays on v0.15.0 in Phase A and the chart sets `HOUSEGATE_SI_LANES=off` as an env var; A1 reads that env var and no new CLI flag may be required by the sidecar. (e) `Options.StorageIntegrityWriteMeter` stays optional; sentio-node leaves it nil.
- **P12 (§10 labels).** An allowlist miss in the authorizer counts as `denied_signer`; the spec's label set has no separate value and the `EthValidator` refuses that signer first anyway.

## Global Constraints

- Bazel 9.1.0 is the ground truth: `bazel test //...`; after adding files or imports run `bazel mod tidy && bazel run //:gazelle`. A failing test that also fails on a clean `origin/main` build is not a regression; compare before chasing it.
- Go module path `github.com/housegate/housegate`; every internal import uses the full path.
- Logging through `github.com/housegate/housegate/pkg/log` using the structured `*w` forms (`log.Infow`, `logger.Warnw` from `log.FromContext`); no bracketed prefixes in structured messages; never `fmt.Println` or stdlib `log`.
- English identifiers, comments and operator messages. Markdown is never hard-wrapped (one paragraph per line).
- Errors are wrapped with `fmt.Errorf("context: %w", err)`; startup config errors aggregate with `errors.Join`.
- Integration targets are tagged `manual` and listed explicitly in `.github/workflows/ci.yml`; this plan adds tests to the existing `//pkg/integration:integration_test` target only.
- Statement id grammar (spec §5.1), verbatim: account lowercase `0x` + hex (any length, as today), lane exactly 16 lowercase hex (`^[0-9a-f]{16}$`), seq canonical non-zero decimal, nonce non-empty without `:` and without surrounding whitespace; legacy `<account>:<seq>:<nonce>`, laned `<account>:<lane>:<seq>:<nonce>`, disjoint by segment count.
- Error codes and texts (spec §6.2, §6.3, §6.6), verbatim: 497 `storage_integrity: signer 0x… is not permitted to write storage-integrity tables` / `storage_integrity: owner 0x… is not permitted to write storage-integrity tables` / `storage_integrity: 0x… is not an operator of 0x…` / `storage_integrity: 0x… is not a writer of database <db>`; 392 `storage_integrity: table <db>.<t> is governed by storage integrity and must be read through its host indexer; connect with --database <db> or USE <db>`; marker suffix ` [client_seq unspent]`; coded terminal reject `storage_integrity: statement <id> rejected by the arbiter: <ADMISSION_CODE_…>`; startup `storage_integrity.ingress requires a registry that implements WriterAccess (contract isDatabaseWriter)`.
- Config keys (spec §6.2, §6.4): `storage_integrity.ingress.denied_addresses`, optional `storage_integrity.ingress.allowed_addresses`, `auth.writer_predicate: contract | bitmap` (default `contract`); flags/env `-agent-key`/`HOUSEGATE_AGENT_KEY`, `-agent-owner`/`HOUSEGATE_AGENT_OWNER`, `-network`/`HOUSEGATE_NETWORK` (default `devnet2` = `http://64.38.144.158:32003`), `-state`/`HOUSEGATE_NETWORK_STATE_SOURCE`, `-si`/`HOUSEGATE_SI` (`auto`/`on`/`off`), `-si-state-dir`/`HOUSEGATE_SI_STATE_DIR`, `-si-lanes`/`HOUSEGATE_SI_LANES` (`auto`/`off`), `-si-read-mode`/`HOUSEGATE_SI_READ_MODE`, `-si-inline-values`/`HOUSEGATE_SI_INLINE_VALUES`, `-listen` default `127.0.0.1:9000` in agent mode.
- Metric names (spec §10), verbatim: `storage_integrity_ingress_authz_total{result="allowed|denied_signer|denied_owner|operator_invalid|not_writer|unknown_database"}`, `storage_integrity_peer_guard_refusals_total`, `clickhouse_proxy_agent_si_seq_recycled_total`, `clickhouse_proxy_agent_si_seq_burned_total{reason}`, `clickhouse_proxy_agent_si_upstream_switches_total{result="switched|refused_state|refused_database|refused_revision|dial_failed"}`, `clickhouse_proxy_agent_si_discovery_failures_total{step}`. The lane id is never a metric label.
- Platforms: linux/amd64 and darwin/arm64 only; the flock lives behind `//go:build linux || darwin` with a refusing stub (`//go:build !(linux || darwin)`) elsewhere (`pkg/secretsload/anonfile_*.go` precedent). Not `//go:build unix`: `syscall.Flock` does not exist on every `unix` GOOS (aix, solaris, illumos), so a `unix` tag would not compile there; Plan B's lane pool uses the same tags and reuses this lock (spec D15 as ratified in §4.5).
- Every commit message ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Never push, merge or release without the user's explicit approval.

## Review Focus

1. **An operator key writing for an owner whose `SQL_x_payer` arrives in a different casing or quoted** (`'0xABC…'`): the ingress must normalize exactly like `authplugin.resolveOwner` (trim quotes, lowercase) before `IsOperator`, the denylist and `IsDatabaseWriter` (Task 4).
2. **A grant revoked while a long INSERT uploads**: the `admissionFromState` re-check refuses with 497 and the marker, and nothing reaches the consumer (Task 4, Task 7).
3. **A server Exception that already ends with ` [client_seq unspent]` crossing a second housegate** (agent → server → server via forward): the suffix must be appended once, and the session-preserving matchers must still recognise 252/733/392 with the suffix (Task 6).
4. **The legacy counter opened by two agents on one machine with the same key** (the default state dir): the second refuses SI writes with a lock error instead of colliding on `DUPLICATE_CLIENT_SEQ`; a crash between free-list removal and use only burns (Task 11).
5. **A session that ran `SET max_threads = 1` and then INSERTs into a database hosted elsewhere**: refused locally and session-preserving, never switched; the same session's next SELECT still works on the old upstream (Task 15).

---
## File Structure

| Path | Status | Responsibility |
|---|---|---|
| `pkg/storageintegrity/statementid.go` (+ `_test.go`) | Create | The one statement-id grammar: `StatementID`, `ParseStatementID`, `ParseLegacyStatementID`, `Flat`, `Subject`, `ErrClientLanesNotEnabled` (Task 1). |
| `pkg/storageintegrity/intake.go`, `arbiter_proto.go`, `pkg/plugins/storageintegrity/plugin.go` | Modify | Drop the local parsers; use the shared grammar (Task 1). |
| `pkg/registry/access.go` | Modify | `WriterAccess`, `OperatorChecker` (Task 2). |
| `pkg/network/inmemory.go`, `inmemory_writer_test.go` | Modify / Create | In-memory `IsDatabaseWriter` with the contract semantics (Task 2). |
| `pkg/config/storage_integrity_config.go`, `config.go`, `pkg/plugins/auth/config.go` | Modify | `denied_addresses`, optional `allowed_addresses`, `auth.writer_predicate` (Task 3). |
| `pkg/plugins/storageintegrity/authorize.go` (+ `_test.go`), `metrics.go` | Create | `writeAuthorizer`, 497 refusals, `storage_integrity_ingress_authz_total` (Task 4). |
| `pkg/chproto/client_error.go` | Modify | `CodeAccessDenied`, `ClientError.SeqUnspent`, `MarkSeqUnspent`, suffix helpers (Task 4, Task 6). |
| `pkg/network/permission_commitgate_observer.go` | Modify | Contract writer predicate for Write-bit statements (Task 5). |
| `pkg/proxy/relay.go`, `relay_unspent_test.go` | Modify / Create | Render the marker once; mark token-bearing pre-submission refusals (Task 6). |
| `storage_integrity_ingress.go`, `pkg/storageintegrity/arbiter_proto.go`, `intake.go` | Modify | Mark pre-`Orchestrate` and coded terminal refusals; name the code; carry `StatementSeq` (Task 7). |
| `pkg/storageintegrity/write_meter.go`, `proxy.go`, `storage_integrity_ingress.go` | Create / Modify | `WriteMeter`, `SIWriteEvent`, `Options.StorageIntegrityWriteMeter` (Task 8). |
| `pkg/plugins/sipeerguard/plugin.go` (+ `_test.go`), `pkg/plugins/sireserved/plugin.go` | Create / Modify | Host guard against peer reads of governed ordinary physical tables; export the carrier list (Task 9). |
| `pkg/chsession/session.go`, `errors.go`, `state_test.go` and every `chsession.Session` fake | Modify | `Session.SwitchUpstream` (Task 10). |
| `pkg/plugins/sistatement/seq.go`, `seq_lock_unix.go`, `seq_lock_other.go`, `seq_dir.go`, `seq_test.go`, `seq_lock_test.go` | Modify / Create | Durable dir creation, flock, free list, `Reserve`/`Release`/`Close` (Task 11). |
| `pkg/plugins/sistatement/plugin.go`, `observer.go`, `plugin_test.go`, `inline_values_test.go`, `table_status_test.go`, `late_reservation_test.go` | Modify / Create | Reservation in the strict hook, release on the marker, burn accounting (Task 12). |
| `pkg/registry/topology.go`, `table_status.go`, `si_info.go`, `pkg/network/rpc.go`, `rpc_routing_test.go` | Modify / Create | Storage RPC port, routed status, `sentio_getStorageIntegrityInfo`, 5 s status cache (Task 13). |
| `pkg/plugins/sistatement/discovery.go` (+ `_test.go`) | Create | Network-id resolution, clock-skew warning, lazy seq store, writer pre-check (Task 14). |
| `pkg/plugins/sistatement/switch.go` (+ `_test.go`), `pkg/storageintegrity/insert_parser.go`, `pkg/proxy/relay_switch_test.go` | Create / Modify | Upstream switch with the §6.4 guards; leading-keyword helper (Task 15). |
| `pkg/config/agent_quickstart.go`, `agent_state_dir.go` (+ tests), `cmd/main.go`, `build.go` | Create / Modify | Flags/env, presets, defaults, agent wiring (Task 16). |
| `pkg/ffifetch/ffifetch.go`, `ffifetch_pin_test.go` (root), `pkg/plugins/materialize/config.go`, `pkg/plugins/sistatement/read_mode.go`, `build.go` | Modify / Create | Default native materializer, read-mode injector, messages (Task 17). |
| `pkg/integration/storage_integrity_open_writes_test.go` | Create | §9.2 Phase-A scenarios (Task 18). |
| `CLAUDE.md`, `README.md`, `configs/*.yaml`, `docs/agent-inline-values.md` | Modify | Operator and contributor docs (Task 19). |

---
### Task 1: one statement-id grammar (legacy-only behaviour)

**Files:**
- Create: `pkg/storageintegrity/statementid.go`, `pkg/storageintegrity/statementid_test.go`
- Modify: `pkg/storageintegrity/intake.go:195-306` (delete `flatStatementID`, `parseFlatStatementID`; move `isLowerHex` / `isDecimalDigits` into `statementid.go`; `EnvelopeFromAdmission` at `:180` and `ParseFlatStatementID` at `:279-289` use the shared grammar)
- Modify: `pkg/storageintegrity/arbiter_proto.go:300-307`
- Modify: `pkg/plugins/storageintegrity/plugin.go:690-765` (`statementID`, `requireStatementIDSigner`; delete `flatStatementID`, `parseFlatStatementID`, `isLowerHex`, `isDecimalDigits`)
- Test: `pkg/plugins/storageintegrity/plugin_test.go` (one new case)

**Interfaces:**
- Consumes: nothing new.
- Produces (package `github.com/housegate/housegate/pkg/storageintegrity`, imported as `sicore` elsewhere):
  - `var ErrClientLanesNotEnabled error` — text `storage_integrity: client lanes are not enabled on this network`.
  - `type StatementID struct { Account, Lane string; Seq uint64; Nonce string }`
  - `func ParseStatementID(flat string) (StatementID, error)` — both grammars.
  - `func ParseLegacyStatementID(flat string) (StatementID, error)` — refuses a laned id with an error that `errors.Is(err, ErrClientLanesNotEnabled)`.
  - `func (StatementID) Flat() string`, `func (StatementID) Subject() string`, `func (StatementID) IsLaned() bool`.
  - `func ParseFlatStatementID(id string) (account string, seq uint64, nonce string, err error)` keeps its signature (legacy wrapper for SDK callers).

- [ ] **Step 1: Write the failing grammar test**

```go
// pkg/storageintegrity/statementid_test.go
package storageintegrity

import (
	"errors"
	"strings"
	"testing"
)

const testAccount = "0x00000000000000000000000000000000000000a1"

func TestParseStatementID_RoundTrips(t *testing.T) {
	for _, flat := range []string{
		testAccount + ":42:9f1c",
		testAccount + ":5e1f0a2b7c9d3e4f:42:9f1c",
		"0xabc:1:n1", // any lowercase hex length, as today (spec §5.1)
		testAccount + ":18446744073709551615:n",
	} {
		id, err := ParseStatementID(flat)
		if err != nil {
			t.Fatalf("ParseStatementID(%q): %v", flat, err)
		}
		if got := id.Flat(); got != flat {
			t.Fatalf("Flat() = %q, want %q", got, flat)
		}
	}
}

func TestParseStatementID_Subject(t *testing.T) {
	legacy, err := ParseStatementID(testAccount + ":42:n")
	if err != nil || legacy.Subject() != testAccount || legacy.IsLaned() {
		t.Fatalf("legacy subject = %q laned=%v err=%v", legacy.Subject(), legacy.IsLaned(), err)
	}
	laned, err := ParseStatementID(testAccount + ":5e1f0a2b7c9d3e4f:42:n")
	if err != nil || laned.Subject() != testAccount+":5e1f0a2b7c9d3e4f" || !laned.IsLaned() || laned.Seq != 42 {
		t.Fatalf("laned = %+v subject=%q err=%v", laned, laned.Subject(), err)
	}
}

func TestParseStatementID_RejectsMalformed(t *testing.T) {
	for name, flat := range map[string]string{
		"two segments":        testAccount + ":42",
		"five segments":       testAccount + ":5e1f0a2b7c9d3e4f:42:n:x",
		"lane 15 hex":         testAccount + ":5e1f0a2b7c9d3e4:42:n",
		"lane 17 hex":         testAccount + ":5e1f0a2b7c9d3e4f0:42:n",
		"lane uppercase":      testAccount + ":5E1F0A2B7C9D3E4F:42:n",
		"lane not hex":        testAccount + ":5e1f0a2b7c9d3e4g:42:n",
		"uppercase account":   "0x00000000000000000000000000000000000000A1:42:n",
		"account without 0x":  "00000000000000000000000000000000000000a1:42:n",
		"empty account hex":   "0x:42:n",
		"leading-zero seq":    testAccount + ":042:n",
		"zero seq":            testAccount + ":0:n",
		"signed seq":          testAccount + ":+4:n",
		"seq overflow":        testAccount + ":18446744073709551616:n",
		"empty nonce":         testAccount + ":42:",
		"nonce whitespace":    testAccount + ":42: n",
		"laned leading zero":  testAccount + ":5e1f0a2b7c9d3e4f:01:n",
		"laned empty nonce":   testAccount + ":5e1f0a2b7c9d3e4f:1:",
		"empty":               "",
	} {
		if _, err := ParseStatementID(flat); err == nil {
			t.Errorf("%s: ParseStatementID(%q) accepted a malformed id", name, flat)
		}
	}
}

func TestParseLegacyStatementID_RefusesLanes(t *testing.T) {
	_, err := ParseLegacyStatementID(testAccount + ":5e1f0a2b7c9d3e4f:42:n")
	if !errors.Is(err, ErrClientLanesNotEnabled) {
		t.Fatalf("err = %v, want ErrClientLanesNotEnabled", err)
	}
	if !strings.Contains(err.Error(), "client lanes are not enabled on this network") {
		t.Fatalf("message = %q", err)
	}
	if _, err := ParseLegacyStatementID(testAccount + ":42:n"); err != nil {
		t.Fatalf("legacy id refused: %v", err)
	}
}

func TestParseFlatStatementID_IsLegacyOnly(t *testing.T) {
	account, seq, nonce, err := ParseFlatStatementID(testAccount + ":7:abc")
	if err != nil || account != testAccount || seq != 7 || nonce != "abc" {
		t.Fatalf("got %s/%d/%s err=%v", account, seq, nonce, err)
	}
	if _, _, _, err := ParseFlatStatementID(testAccount + ":5e1f0a2b7c9d3e4f:7:abc"); !errors.Is(err, ErrClientLanesNotEnabled) {
		t.Fatalf("laned id err = %v, want ErrClientLanesNotEnabled", err)
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `bazel run //:gazelle && bazel test //pkg/storageintegrity:storageintegrity_test --test_filter='TestParseStatementID|TestParseLegacyStatementID|TestParseFlatStatementID_IsLegacyOnly' --test_output=errors`
Expected: FAIL to compile (`undefined: ParseStatementID`).

- [ ] **Step 3: Implement the grammar**

```go
// pkg/storageintegrity/statementid.go
package storageintegrity

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// ErrClientLanesNotEnabled refuses a laned statement id on a network whose
// arbiter has not activated client lanes (spec 2026-10-09 §6.1, D13). In
// release A1 every laned id is refused with it; Plan B gates it on the
// table-registry snapshot instead.
var ErrClientLanesNotEnabled = errors.New("storage_integrity: client lanes are not enabled on this network")

// StatementID is the structured statement identity of spec 2026-10-09 §5.1.
// Lane is empty for the legacy default lane. The nonce is entropy only; the
// uniqueness key is (Account, Lane, Seq).
type StatementID struct {
	Account string
	Lane    string
	Seq     uint64
	Nonce   string
}

const statementIDShape = "<client_account>:<client_seq>:<client_nonce> or <client_account>:<lane>:<client_seq>:<client_nonce>"

// ParseStatementID parses either flat form. The forms are disjoint by segment
// count because a nonce cannot contain ':'. Every rule is spec §5.1's; this is
// the only parser, shared by the ingress, the intake, the arbiter conversion
// and the agent.
func ParseStatementID(flat string) (StatementID, error) {
	parts := strings.Split(flat, ":")
	var (
		id      StatementID
		seqText string
	)
	switch len(parts) {
	case 3:
		id.Account, seqText, id.Nonce = parts[0], parts[1], parts[2]
	case 4:
		id.Account, id.Lane, seqText, id.Nonce = parts[0], parts[1], parts[2], parts[3]
		if !isLane(id.Lane) {
			return StatementID{}, fmt.Errorf("requires a 16-character lowercase hex lane, got %q", id.Lane)
		}
	default:
		return StatementID{}, fmt.Errorf("requires %s", statementIDShape)
	}
	if id.Account == "" || seqText == "" || id.Nonce == "" {
		return StatementID{}, fmt.Errorf("requires %s", statementIDShape)
	}
	if id.Account != strings.ToLower(id.Account) || !strings.HasPrefix(id.Account, "0x") || !isLowerHex(id.Account[2:]) {
		return StatementID{}, errors.New("requires lowercase 0x client_account")
	}
	if (len(seqText) > 1 && seqText[0] == '0') || !isDecimalDigits(seqText) {
		return StatementID{}, errors.New("requires canonical decimal client_seq")
	}
	seq, err := strconv.ParseUint(seqText, 10, 64)
	if err != nil || seq == 0 {
		return StatementID{}, errors.New("requires non-zero decimal client_seq")
	}
	if strings.TrimSpace(id.Nonce) != id.Nonce {
		return StatementID{}, errors.New("requires non-empty client_nonce")
	}
	id.Seq = seq
	return id, nil
}

// ParseLegacyStatementID is ParseStatementID for components that do not
// accept lanes yet: a laned id fails with ErrClientLanesNotEnabled.
func ParseLegacyStatementID(flat string) (StatementID, error) {
	id, err := ParseStatementID(flat)
	if err != nil {
		return StatementID{}, err
	}
	if id.IsLaned() {
		return StatementID{}, fmt.Errorf("statement id %s: %w", flat, ErrClientLanesNotEnabled)
	}
	return id, nil
}

// Flat renders the canonical flat form: three segments for the legacy lane,
// four for a laned id. It is the string the JWS, the ClickHouse query id and
// _hg_row_id bind.
func (id StatementID) Flat() string {
	seq := strconv.FormatUint(id.Seq, 10)
	if id.Lane == "" {
		return id.Account + ":" + seq + ":" + id.Nonce
	}
	return id.Account + ":" + id.Lane + ":" + seq + ":" + id.Nonce
}

// Subject is the accumulator key of spec D12: the account for the legacy
// lane, account + ":" + lane otherwise.
func (id StatementID) Subject() string {
	if id.Lane == "" {
		return id.Account
	}
	return id.Account + ":" + id.Lane
}

// IsLaned reports whether the id carries a lane segment.
func (id StatementID) IsLaned() bool { return id.Lane != "" }

func isLane(s string) bool { return len(s) == 16 && isLowerHex(s) }

func isLowerHex(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if !((s[i] >= '0' && s[i] <= '9') || (s[i] >= 'a' && s[i] <= 'f')) {
			return false
		}
	}
	return true
}

func isDecimalDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}
```

In `pkg/storageintegrity/intake.go` delete `type flatStatementID`, `func parseFlatStatementID`, `func isLowerHex` and `func isDecimalDigits` (now in `statementid.go`), and replace `ParseFlatStatementID` with:

```go
// ParseFlatStatementID validates the legacy flat "<lowercase 0x account>:<seq>:<nonce>"
// form and returns its parts. Kept for SDK callers; a laned id is refused
// with ErrClientLanesNotEnabled.
func ParseFlatStatementID(id string) (account string, seq uint64, nonce string, err error) {
	parsed, err := ParseLegacyStatementID(id)
	if err != nil {
		return "", 0, "", err
	}
	return parsed.Account, parsed.Seq, parsed.Nonce, nil
}
```

At `intake.go:180` replace `parseFlatStatementID(adm.StatementID)` with `ParseLegacyStatementID(adm.StatementID)` and `stmtID.ClientAccount` with `stmtID.Account` at `:187`. In `arbiter_proto.go:301` do the same (`ParseLegacyStatementID(env.StatementID)`; `id.Account`, `id.Seq`, `id.Nonce` in the `pb.StatementID` literal).

In `pkg/plugins/storageintegrity/plugin.go` replace `statementID` and `requireStatementIDSigner` and delete the four local helpers:

```go
func statementID(qctx *plugin.QueryContext) (string, error) {
	if qctx != nil && qctx.Query != nil {
		if id := strings.TrimSpace(qctx.Query.ID); id != "" {
			if _, err := parseIngressStatementID(id); err != nil {
				return "", err
			}
			return id, nil
		}
	}
	return "", errors.New("storage_integrity query id is required")
}

// parseIngressStatementID applies the shared grammar. Release A1 refuses a
// laned id locally (spec 2026-10-09 §6.1) instead of letting the arbiter
// answer MALFORMED.
func parseIngressStatementID(id string) (sicore.StatementID, error) {
	parsed, err := sicore.ParseLegacyStatementID(id)
	if errors.Is(err, sicore.ErrClientLanesNotEnabled) {
		return sicore.StatementID{}, sicore.ErrClientLanesNotEnabled
	}
	if err != nil {
		return sicore.StatementID{}, fmt.Errorf("storage_integrity requires structured statement id: %w", err)
	}
	return parsed, nil
}

func requireStatementIDSigner(id, signer string) error {
	stmt, err := parseIngressStatementID(id)
	if err != nil {
		return err
	}
	if stmt.Account != strings.ToLower(signer) {
		return fmt.Errorf("storage_integrity statement id client_account %s does not match authenticated signer %s", stmt.Account, strings.ToLower(signer))
	}
	return nil
}
```

Remove the now-unused `strconv` import from `plugin.go` only if `bazel build //pkg/plugins/storageintegrity` reports it unused.

- [ ] **Step 4: Add the ingress case for a laned id**

Append to `pkg/plugins/storageintegrity/plugin_test.go`:

```go
// Spec 2026-10-09 §6.1: before lane activation the ingress itself refuses a
// laned id with the stable message, instead of forwarding it to the arbiter.
func TestIngressRefusesLanedStatementIDBeforeActivation(t *testing.T) {
	p, signer := newSignedIngress(t)
	sql := "INSERT INTO tenant.events FORMAT Native"
	qctx := signedQueryContext(t, 14, signer, sql, sql, sqlmeta.StatementTypeInsert)
	qctx.Query.ID = strings.ToLower(signer.Address()) + ":5e1f0a2b7c9d3e4f:1:n1"
	qctx.AccessedTables = []sqlmeta.AccessedTable{{IsStorageIntegrity: true, OriginalDatabase: "tenant", OriginalTable: "events"}}

	err := p.OnQuery(context.Background(), qctx)
	if !errors.Is(err, sicore.ErrClientLanesNotEnabled) {
		t.Fatalf("OnQuery err = %v, want ErrClientLanesNotEnabled", err)
	}
}
```

- [ ] **Step 5: Run the packages**

Run: `bazel test //pkg/storageintegrity:storageintegrity_test //pkg/plugins/storageintegrity:storageintegrity_test //pkg/plugins/sistatement:sistatement_test --test_output=errors`
Expected: PASS (the existing `TestIngressRejectsMalformedStatementID` still sees "structured statement id"; the agent tests use `ParseFlatStatementID` unchanged).

- [ ] **Step 6: Commit**

```bash
git add pkg/storageintegrity/statementid.go pkg/storageintegrity/statementid_test.go pkg/storageintegrity/intake.go pkg/storageintegrity/arbiter_proto.go pkg/storageintegrity/BUILD.bazel pkg/plugins/storageintegrity/plugin.go pkg/plugins/storageintegrity/plugin_test.go
git commit -m "feat(storageintegrity): one statement-id grammar; refuse laned ids before activation (spec 2026-10-09 §6.1)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---
### Task 2: `registry.WriterAccess` and the in-memory implementation

**Files:**
- Modify: `pkg/registry/access.go`
- Modify: `pkg/network/inmemory.go` (new method next to `IsOperator`, compile-time assertion at the end)
- Create: `pkg/network/inmemory_writer_test.go`

**Interfaces:**
- Consumes: nothing new.
- Produces:
  - `registry.WriterAccess interface { IsDatabaseWriter(database, account string) (bool, error) }`
  - `registry.OperatorChecker interface { IsOperator(owner, signer string) bool }` (the narrow half of `registry.Access` the ingress needs; `registry.Access` already satisfies it)
  - `func (s *network.InMemoryNetworkState) IsDatabaseWriter(database, account string) (bool, error)` — contract semantics: unknown or pending-delete database → `(false, error)`; `account == lowercase(IndexerInfo(db.IndexerId).Signer)` (non-empty, non-zero) → true; the account's own stored bits (no wildcard union, no promotion) contain Owner `0x08` or Write `0x02` → true; Admin alone, Read and wildcard grants → false.

- [ ] **Step 1: Write the failing test**

```go
// pkg/network/inmemory_writer_test.go
package network

import (
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/registry"
)

const (
	writerIndexerSigner = "0x00000000000000000000000000000000000000aa"
	writerOwner         = "0x00000000000000000000000000000000000000b1"
	writerWrite         = "0x00000000000000000000000000000000000000b2"
	writerAdminOnly     = "0x00000000000000000000000000000000000000b3"
	writerReadOnly      = "0x00000000000000000000000000000000000000b4"
	writerNobody        = "0x00000000000000000000000000000000000000b5"
)

func writerFixture() *InMemoryNetworkState {
	s := NewInMemoryNetworkState()
	s.IndexerInfos[7] = IndexerInfo{IndexerId: 7, Signer: strings.ToUpper(writerIndexerSigner[:2]) + writerIndexerSigner[2:]}
	s.DatabaseInfos["db1"] = DatabaseInfo{DatabaseId: "db1", IndexerId: 7}
	s.DatabaseInfos["gone"] = DatabaseInfo{DatabaseId: "gone", IndexerId: 7, PendingDelete: true}
	s.DatabasePermissions[writerOwner] = DatabasePermissions{"db1": registry.DbAuthOwner}
	s.DatabasePermissions[writerWrite] = DatabasePermissions{"db1": registry.DbAuthWrite}
	s.DatabasePermissions[writerAdminOnly] = DatabasePermissions{"db1": registry.DbAuthAdmin | registry.DbAuthRead}
	s.DatabasePermissions[writerReadOnly] = DatabasePermissions{"db1": registry.DbAuthRead}
	// A wildcard Write grant: the contract does not union address(0).
	s.DatabasePermissions[WildcardAddress] = DatabasePermissions{"db1": registry.DbAuthWrite | registry.DbAuthOwner}
	return s
}

func TestInMemoryIsDatabaseWriter_ContractSemantics(t *testing.T) {
	s := writerFixture()
	for _, tc := range []struct {
		account string
		want    bool
	}{
		{writerIndexerSigner, true},   // indexer signer of the hosting indexer (stored mixed-case)
		{writerOwner, true},           // Owner bit
		{writerWrite, true},           // Write bit
		{writerAdminOnly, false},      // Admin alone is not a writer (Databases.sol code, not NatSpec)
		{writerReadOnly, false},       // Read only
		{writerNobody, false},         // only the wildcard grant applies to them
		{string(WildcardAddress), false},
		{"", false},
	} {
		got, err := s.IsDatabaseWriter("db1", tc.account)
		if err != nil || got != tc.want {
			t.Errorf("IsDatabaseWriter(db1, %q) = %v, %v; want %v, nil", tc.account, got, err, tc.want)
		}
	}
}

func TestInMemoryIsDatabaseWriter_UnknownAndPendingDeleteDatabases(t *testing.T) {
	s := writerFixture()
	for _, db := range []string{"nope", "gone"} {
		if ok, err := s.IsDatabaseWriter(db, writerOwner); err == nil || ok {
			t.Errorf("IsDatabaseWriter(%s) = %v, %v; want false and an error", db, ok, err)
		}
	}
}

func TestInMemoryIsDatabaseWriter_AccountCaseInsensitive(t *testing.T) {
	s := writerFixture()
	mixed := "0x" + strings.ToUpper(writerWrite[2:])
	if ok, err := s.IsDatabaseWriter("db1", mixed); err != nil || !ok {
		t.Fatalf("mixed-case writer = %v, %v; want true", ok, err)
	}
}

func TestInMemoryIsDatabaseWriter_IndexerWithoutSignerIsNotAWriter(t *testing.T) {
	s := NewInMemoryNetworkState()
	s.IndexerInfos[1] = IndexerInfo{IndexerId: 1}
	s.DatabaseInfos["db1"] = DatabaseInfo{DatabaseId: "db1", IndexerId: 1}
	if ok, err := s.IsDatabaseWriter("db1", ""); err != nil || ok {
		t.Fatalf("empty signer matched the empty account: %v, %v", ok, err)
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `bazel test //pkg/network:network_test --test_filter='TestInMemoryIsDatabaseWriter' --test_output=errors`
Expected: FAIL to compile (`s.IsDatabaseWriter undefined`).

- [ ] **Step 3: Implement**

Append to `pkg/registry/access.go`:

```go
// WriterAccess answers the contract's Databases.isDatabaseWriter exactly:
// indexer signer of the database's indexer, or the account's own Owner (0x08)
// or Write (0x02) bit. Admin alone and address(0) grants do not count. An
// unknown or pending-delete database is an error, which callers refuse.
// Implemented by the host over raw chain state (sentio-node FromStatecore) and
// by the in-memory network state; RpcNetworkState does not implement it.
type WriterAccess interface {
	IsDatabaseWriter(database, account string) (bool, error)
}

// OperatorChecker is the operator half of Access, for consumers that need
// only IsOperator (the storage-integrity ingress authorizer).
type OperatorChecker interface {
	IsOperator(owner, signer string) bool
}
```

Add to `pkg/network/inmemory.go` after `IsOperator`:

```go
// IsDatabaseWriter implements registry.WriterAccess with the contract's
// semantics (spec 2026-10-09 D2): the hosting indexer's signer, or the
// account's own Owner or Write bit. Unlike HasPermission it neither unions the
// wildcard address's grants nor promotes Admin.
func (s *InMemoryNetworkState) IsDatabaseWriter(database, account string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	info, ok := s.DatabaseInfos[Database(database)]
	if !ok || info.PendingDelete {
		return false, fmt.Errorf("database not found: %s", database)
	}
	account = strings.ToLower(strings.TrimSpace(account))
	if account == "" || AccountAddress(account) == WildcardAddress {
		return false, nil
	}
	if indexer, ok := s.IndexerInfos[info.IndexerId]; ok {
		signer := strings.ToLower(strings.TrimSpace(indexer.Signer))
		if signer != "" && AccountAddress(signer) != WildcardAddress && signer == account {
			return true, nil
		}
	}
	bits := s.DatabasePermissions[AccountAddress(account)][Database(database)]
	return bits&(registry.DbAuthOwner|registry.DbAuthWrite) != 0, nil
}
```

and extend the compile-time assertions at the end of the file:

```go
var _ registry.WriterAccess = (*InMemoryNetworkState)(nil)
```

- [ ] **Step 4: Run the tests**

Run: `bazel test //pkg/network:network_test //pkg/registry:registry_test --test_output=errors`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/registry/access.go pkg/network/inmemory.go pkg/network/inmemory_writer_test.go pkg/network/BUILD.bazel
git commit -m "feat(registry): WriterAccess (contract isDatabaseWriter) with an in-memory implementation

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---
### Task 3: ingress and auth configuration (`denied_addresses`, optional `allowed_addresses`, `auth.writer_predicate`)

**Files:**
- Modify: `pkg/config/storage_integrity_config.go:130-137` (struct), `:296-298` (drop the non-empty allowlist rule), plus new validation
- Modify: `pkg/plugins/auth/config.go` (field, constants, `EffectiveWriterPredicate`)
- Modify: `pkg/config/config.go` (`Validate`: writer predicate value)
- Modify: `pkg/config/storage_integrity_config_test.go:198-209` (the "signer allowlist required" subtest inverts)
- Test: `pkg/config/storage_integrity_config_test.go`, `pkg/config/config_test.go`

**Interfaces:**
- Consumes: nothing new.
- Produces:
  - `config.StorageIntegrityIngressConfig.DeniedAddresses []string` (yaml/json `denied_addresses`), validated as `^0x[0-9a-f]{40}$` and free of duplicates.
  - `authplugin.Config.WriterPredicate string` (yaml/json `writer_predicate`), constants `authplugin.WriterPredicateContract = "contract"`, `authplugin.WriterPredicateBitmap = "bitmap"`, and `func (c authplugin.Config) EffectiveWriterPredicate() string` (empty → `contract`).

- [ ] **Step 1: Write the failing tests**

In `pkg/config/storage_integrity_config_test.go` replace the `"signer allowlist required"` subtest with:

```go
	t.Run("empty allowlist is open (spec 2026-10-09 D1)", func(t *testing.T) {
		cfg := minimalServerConfig(t)
		cfg.StorageIntegrity.Ingress.Enabled = true
		cfg.StorageIntegrity.Ingress.NetworkID = "testnet-v2"
		cfg.StorageIntegrity.Ingress.MaxPayloadBytes = defaultStorageIntegrityMaxPayloadBytes
		if err := cfg.Validate(); err != nil {
			t.Fatalf("an empty allowed_addresses must validate: %v", err)
		}
	})

	t.Run("network_id still required", func(t *testing.T) {
		cfg := minimalServerConfig(t)
		cfg.StorageIntegrity.Ingress.Enabled = true
		cfg.StorageIntegrity.Ingress.MaxPayloadBytes = defaultStorageIntegrityMaxPayloadBytes
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "storage_integrity.ingress.network_id is required") {
			t.Fatalf("Validate err = %v, want network_id rejection", err)
		}
	})

	t.Run("denied_addresses validated", func(t *testing.T) {
		for name, tc := range map[string]struct {
			denied []string
			want   string
		}{
			"uppercase":  {[]string{"0xABCDEF0000000000000000000000000000000001"}, "lowercase"},
			"short":      {[]string{"0x1234"}, "lowercase"},
			"no 0x":      {[]string{"1111111111111111111111111111111111111111"}, "lowercase"},
			"duplicate":  {[]string{"0x1111111111111111111111111111111111111111", "0x1111111111111111111111111111111111111111"}, "duplicate"},
		} {
			t.Run(name, func(t *testing.T) {
				cfg := minimalServerConfig(t)
				cfg.StorageIntegrity.Ingress.Enabled = true
				cfg.StorageIntegrity.Ingress.NetworkID = "testnet-v2"
				cfg.StorageIntegrity.Ingress.MaxPayloadBytes = defaultStorageIntegrityMaxPayloadBytes
				cfg.StorageIntegrity.Ingress.DeniedAddresses = tc.denied
				err := cfg.Validate()
				if err == nil || !strings.Contains(err.Error(), "storage_integrity.ingress.denied_addresses") || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("Validate err = %v, want a denied_addresses %q rejection", err, tc.want)
				}
			})
		}
		cfg := minimalServerConfig(t)
		cfg.StorageIntegrity.Ingress.Enabled = true
		cfg.StorageIntegrity.Ingress.NetworkID = "testnet-v2"
		cfg.StorageIntegrity.Ingress.MaxPayloadBytes = defaultStorageIntegrityMaxPayloadBytes
		cfg.StorageIntegrity.Ingress.DeniedAddresses = []string{"0x1111111111111111111111111111111111111111"}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("a valid denylist must validate: %v", err)
		}
	})
```

Append to `pkg/config/config_test.go`:

```go
func TestConfigValidateWriterPredicate(t *testing.T) {
	for _, value := range []string{"", "contract", "bitmap"} {
		cfg := minimalServerConfig(t)
		cfg.Auth.WriterPredicate = value
		if err := cfg.Validate(); err != nil {
			t.Fatalf("writer_predicate %q must validate: %v", value, err)
		}
	}
	cfg := minimalServerConfig(t)
	cfg.Auth.WriterPredicate = "Contract"
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "auth.writer_predicate") {
		t.Fatalf("Validate err = %v, want auth.writer_predicate rejection", err)
	}
	if got := (Config{}).Auth.EffectiveWriterPredicate(); got != "contract" {
		t.Fatalf("default writer predicate = %q, want contract", got)
	}
}
```

If `minimalServerConfig` lives only in `storage_integrity_config_test.go`, it is in the same package and usable from `config_test.go`.

- [ ] **Step 2: Run them to verify they fail**

Run: `bazel test //pkg/config:config_test --test_filter='TestConfigValidateStorageIntegrityIngress|TestConfigValidateWriterPredicate' --test_output=errors`
Expected: FAIL to compile (`DeniedAddresses`, `WriterPredicate` undefined).

- [ ] **Step 3: Implement**

`pkg/plugins/auth/config.go`, add to `Config` and below it:

```go
	// WriterPredicate selects how the ordinary write path answers the Write
	// bit (spec 2026-10-09 R3): "contract" (default, also empty) uses the
	// host's registry.WriterAccess — the contract's isDatabaseWriter, with no
	// address(0) wildcard and with the indexer signer counted; "bitmap" keeps
	// the stored bitmap with the address(0) union. "bitmap" is an escape hatch
	// for a network that still relies on a wildcard Write grant.
	WriterPredicate string `json:"writer_predicate" yaml:"writer_predicate"`
}

// Writer predicate values of auth.writer_predicate.
const (
	WriterPredicateContract = "contract"
	WriterPredicateBitmap   = "bitmap"
)

// EffectiveWriterPredicate returns the configured predicate; empty means
// contract.
func (c Config) EffectiveWriterPredicate() string {
	if c.WriterPredicate == "" {
		return WriterPredicateContract
	}
	return c.WriterPredicate
}
```

(The struct's closing brace moves below the new field.)

`pkg/config/config.go`, in `Validate` next to the `TableRefGuard` check:

```go
	switch c.Auth.WriterPredicate {
	case "", authplugin.WriterPredicateContract, authplugin.WriterPredicateBitmap:
	default:
		errs = append(errs, fmt.Errorf("auth.writer_predicate %q is invalid (want %q or %q)",
			c.Auth.WriterPredicate, authplugin.WriterPredicateContract, authplugin.WriterPredicateBitmap))
	}
```

`pkg/config/storage_integrity_config.go`, struct:

```go
// StorageIntegrityIngressConfig is the server-side signed admission surface.
// Writes are open to every database writer (spec 2026-10-09 D1/D2):
// AllowedAddresses is optional (empty admits any signer the writer predicate
// admits) and DeniedAddresses refuses a statement whose signer or resolved
// owner is listed. Both are static; a change needs a restart.
type StorageIntegrityIngressConfig struct {
	Enabled          bool     `json:"enabled"           yaml:"enabled"`
	NetworkID        string   `json:"network_id"        yaml:"network_id"`
	AllowedAddresses []string `json:"allowed_addresses" yaml:"allowed_addresses"`
	DeniedAddresses  []string `json:"denied_addresses"  yaml:"denied_addresses"`
	MaxTokenAge      Duration `json:"max_token_age"     yaml:"max_token_age"`
	RequestTimeout   Duration `json:"request_timeout"   yaml:"request_timeout"`
	MaxPayloadBytes  uint64   `json:"max_payload_bytes" yaml:"max_payload_bytes"`
}
```

In `validate`, delete the `len(c.Ingress.AllowedAddresses) == 0` block and add in its place:

```go
	errs = append(errs, validateDeniedAddresses(c.Ingress.DeniedAddresses)...)
```

and at the end of the file:

```go
var lowercaseAddressPattern = regexp.MustCompile(`^0x[0-9a-f]{40}$`)

// validateDeniedAddresses requires canonical lowercase addresses: the
// ingress compares them byte-for-byte with recovered signers and normalized
// owners, so a checksum-cased entry would silently never match.
func validateDeniedAddresses(addresses []string) []error {
	var errs []error
	seen := make(map[string]bool, len(addresses))
	for _, address := range addresses {
		if !lowercaseAddressPattern.MatchString(address) {
			errs = append(errs, fmt.Errorf("storage_integrity.ingress.denied_addresses entry %q must be a lowercase 0x-prefixed 20-byte hex address", address))
			continue
		}
		if seen[address] {
			errs = append(errs, fmt.Errorf("storage_integrity.ingress.denied_addresses lists %s twice (duplicate)", address))
		}
		seen[address] = true
	}
	return errs
}
```

Add `"regexp"` to the file's imports.

- [ ] **Step 4: Run the package**

Run: `bazel test //pkg/config:config_test --test_output=errors`
Expected: PASS (`TestSampleConfigs*` still pass: no sample config sets these keys yet).

- [ ] **Step 5: Commit**

```bash
git add pkg/config pkg/plugins/auth/config.go
git commit -m "feat(config): open SI ingress with denied_addresses; auth.writer_predicate (spec 2026-10-09 D1, R3)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---
### Task 4: SI ingress write authorization (`writeAuthorizer`, 497, startup refusal)

**Files:**
- Create: `pkg/plugins/storageintegrity/authorize.go`, `pkg/plugins/storageintegrity/authorize_test.go`
- Modify: `pkg/plugins/storageintegrity/plugin.go` (`Config`, `Plugin`, `Admission`, `admissionState`, `New`, `OnQuery` after `requireStatementIDSigner` at `:323`, `admissionFromState` after the signer checks at `:584`)
- Modify: `pkg/plugins/storageintegrity/plugin_test.go` (`newSignedIngressWithoutV2Config` at `:1357`, `TestIngressV2_RequiresStatementValidatorV2` at `:1315`)
- Modify: `pkg/chproto/client_error.go` (`CodeAccessDenied`)
- Modify: `build.go:854-934` (ingress block: WriterAccess resolution at the top, new `Config` fields, log field)
- Test: `build_test.go` (startup refusal)

**Interfaces:**
- Consumes: `registry.WriterAccess`, `registry.OperatorChecker` (Task 2); `config.StorageIntegrityIngressConfig.DeniedAddresses` (Task 3); `sicore` grammar (Task 1).
- Produces:
  - `chproto.CodeAccessDenied int32 = 497`
  - `storageintegrity.Config` fields `Writers registry.WriterAccess`, `Operators registry.OperatorChecker`, `DeniedAddresses []string`, `AllowedAddresses []string`.
  - `storageintegrity.Admission` fields `Owner string` (validated operator relation, empty otherwise) and `Principal string` (owner, else signer).
  - Metric `storage_integrity_ingress_authz_total{result}`.
  - `func (a *writeAuthorizer) authorize(signer, payerSetting, database string) (authorization, string, error)` — the string is the metric result label (empty for a wiring defect).

- [ ] **Step 1: Write the failing authorizer test**

```go
// pkg/plugins/storageintegrity/authorize_test.go
package storageintegrity

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/registry"
	"github.com/housegate/housegate/pkg/sqlmeta"
)

const (
	azIndexer  = "0x00000000000000000000000000000000000000aa"
	azSigner   = "0x00000000000000000000000000000000000000c1"
	azOwner    = "0x00000000000000000000000000000000000000c2"
	azReader   = "0x00000000000000000000000000000000000000c3"
	azAdmin    = "0x00000000000000000000000000000000000000c4"
	azStranger = "0x00000000000000000000000000000000000000c5"
)

func authzState() *network.InMemoryNetworkState {
	ns := network.NewInMemoryNetworkState()
	ns.IndexerInfos[3] = network.IndexerInfo{IndexerId: 3, Signer: azIndexer}
	ns.DatabaseInfos["tenant"] = network.DatabaseInfo{DatabaseId: "tenant", IndexerId: 3}
	ns.DatabasePermissions[azSigner] = network.DatabasePermissions{"tenant": registry.DbAuthWrite}
	ns.DatabasePermissions[azOwner] = network.DatabasePermissions{"tenant": registry.DbAuthOwner}
	ns.DatabasePermissions[azReader] = network.DatabasePermissions{"tenant": registry.DbAuthRead}
	ns.DatabasePermissions[azAdmin] = network.DatabasePermissions{"tenant": registry.DbAuthAdmin | registry.DbAuthRead}
	ns.DatabasePermissions[network.WildcardAddress] = network.DatabasePermissions{"tenant": registry.DbAuthWrite}
	ns.SetOperator(azOwner, azReader, true) // azReader may act for azOwner
	return ns
}

func TestWriteAuthorizer_Matrix(t *testing.T) {
	ns := authzState()
	for _, tc := range []struct {
		name          string
		denied        []string
		allowed       []string
		signer, payer string
		database      string
		wantResult    string
		wantPrincipal string
		wantMessage   string
	}{
		{name: "signer is a writer", signer: azSigner, database: "tenant", wantResult: "allowed", wantPrincipal: azSigner},
		{name: "operator for a writer owner", signer: azReader, payer: "'" + strings.ToUpper(azOwner[:2]) + azOwner[2:] + "'", database: "tenant", wantResult: "allowed", wantPrincipal: azOwner},
		{name: "payer equal to signer", signer: azSigner, payer: azSigner, database: "tenant", wantResult: "allowed", wantPrincipal: azSigner},
		{name: "indexer signer", signer: azIndexer, database: "tenant", wantResult: "allowed", wantPrincipal: azIndexer},
		{name: "invalid operator relation", signer: azSigner, payer: azOwner, database: "tenant", wantResult: "operator_invalid", wantMessage: "storage_integrity: " + azSigner + " is not an operator of " + azOwner},
		{name: "denylisted signer", denied: []string{azSigner}, signer: azSigner, database: "tenant", wantResult: "denied_signer", wantMessage: "storage_integrity: signer " + azSigner + " is not permitted to write storage-integrity tables"},
		{name: "denylisted owner", denied: []string{azOwner}, signer: azReader, payer: azOwner, database: "tenant", wantResult: "denied_owner", wantMessage: "storage_integrity: owner " + azOwner + " is not permitted to write storage-integrity tables"},
		{name: "read-only grantee", signer: azReader, database: "tenant", wantResult: "not_writer", wantMessage: "storage_integrity: " + azReader + " is not a writer of database tenant"},
		{name: "admin-only grantee", signer: azAdmin, database: "tenant", wantResult: "not_writer", wantMessage: "storage_integrity: " + azAdmin + " is not a writer of database tenant"},
		{name: "address(0) Write grant only", signer: azStranger, database: "tenant", wantResult: "not_writer", wantMessage: "storage_integrity: " + azStranger + " is not a writer of database tenant"},
		{name: "allowlist without signer", allowed: []string{azOwner}, signer: azSigner, database: "tenant", wantResult: "denied_signer", wantMessage: "storage_integrity: signer " + azSigner + " is not permitted to write storage-integrity tables"},
		{name: "unknown database", signer: azSigner, database: "missing", wantResult: "unknown_database", wantMessage: "storage_integrity: " + azSigner + " is not a writer of database missing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := newWriteAuthorizer(tc.denied, tc.allowed, ns, ns)
			got, result, err := a.authorize(tc.signer, tc.payer, tc.database)
			if result != tc.wantResult {
				t.Fatalf("result = %q, want %q (err=%v)", result, tc.wantResult, err)
			}
			if tc.wantMessage == "" {
				if err != nil || got.principal != tc.wantPrincipal {
					t.Fatalf("authorize = %+v, %v; want principal %s", got, err, tc.wantPrincipal)
				}
				return
			}
			var clientErr *chproto.ClientError
			if !errors.As(err, &clientErr) || clientErr.Code != chproto.CodeAccessDenied || clientErr.Message != tc.wantMessage || clientErr.KeepSession {
				t.Fatalf("err = %#v, want 497 %q without KeepSession", err, tc.wantMessage)
			}
		})
	}
}

func TestWriteAuthorizer_RequiresWriterAccess(t *testing.T) {
	a := newWriteAuthorizer(nil, nil, nil, nil)
	_, result, err := a.authorize(azSigner, "", "tenant")
	if err == nil || result != "" || !strings.Contains(err.Error(), "requires a registry that implements WriterAccess") {
		t.Fatalf("authorize = %q, %v; want the wiring refusal", result, err)
	}
}

// Spec 2026-10-09 §6.2: the authorizer runs again before the admission is
// handed to the consumer, so a revocation seen during a long upload stops
// the write.
func TestIngressAuthorizerRechecksBeforeAdmission(t *testing.T) {
	signer, err := auth.NewRelaySigner(storageIntegrityTestKey)
	if err != nil {
		t.Fatal(err)
	}
	ns, _ := ingressNetworkState(t)
	ns.DatabaseInfos["tenant"] = network.DatabaseInfo{DatabaseId: "tenant"}
	account := network.AccountAddress(strings.ToLower(signer.Address()))
	ns.DatabasePermissions[account] = network.DatabasePermissions{"tenant": registry.DbAuthWrite}
	p, _ := newSignedIngressWithConfig(t, Config{TableSchemas: ns, NetworkID: "testnet-v2", Writers: ns, Operators: ns})
	sql := "INSERT INTO tenant.events FORMAT Native"
	qctx := signedQueryContext(t, 61, signer, sql, sql, sqlmeta.StatementTypeInsert)
	qctx.AccessedTables = []sqlmeta.AccessedTable{{IsStorageIntegrity: true, OriginalDatabase: "tenant", OriginalTable: "events"}}
	payload := []byte{byte(chproto.ClientDataCode), 0, 0xab, 0xcd}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if err := p.OnClientDataStrict(context.Background(), qctx, payload); err != nil {
		t.Fatalf("OnClientDataStrict: %v", err)
	}
	delete(ns.DatabasePermissions, account) // revoked mid-upload
	p.OnQueryInputComplete(context.Background(), qctx)
	_, err = p.ConsumeAdmission(qctx.Session.ID())
	var clientErr *chproto.ClientError
	if !errors.As(err, &clientErr) || clientErr.Code != chproto.CodeAccessDenied || !strings.Contains(clientErr.Message, "is not a writer of database tenant") {
		t.Fatalf("ConsumeAdmission err = %v, want the 497 not-a-writer refusal", err)
	}
}

// A forwarded-from-peer session runs the ingress on the host, which resolves
// the owner from the query's own SQL_x_payer rather than trusting qctx.Owner.
func TestIngressForwardedSessionUsesHostResolvedOwner(t *testing.T) {
	signer, err := auth.NewRelaySigner(storageIntegrityTestKey)
	if err != nil {
		t.Fatal(err)
	}
	signerAddr := strings.ToLower(signer.Address())
	ns, _ := ingressNetworkState(t)
	ns.DatabaseInfos["tenant"] = network.DatabaseInfo{DatabaseId: "tenant"}
	ns.DatabasePermissions[azOwner] = network.DatabasePermissions{"tenant": registry.DbAuthOwner}
	ns.SetOperator(azOwner, network.AccountAddress(signerAddr), true)
	p, _ := newSignedIngressWithConfig(t, Config{TableSchemas: ns, NetworkID: "testnet-v2", Writers: ns, Operators: ns})
	sql := "INSERT INTO tenant.events FORMAT Native"
	qctx := signedQueryContext(t, 62, signer, sql, sql, sqlmeta.StatementTypeInsert)
	qctx.Session.State().SetPeerTrustForwarded("10.0.0.2:9000", true)
	qctx.Owner = azStranger // ignored: the ingress resolves the owner itself
	qctx.Query.Settings = append(qctx.Query.Settings, chproto.Setting{Key: auth.PayerSettingKey, Value: "'" + azOwner + "'", Custom: true})
	qctx.AccessedTables = []sqlmeta.AccessedTable{{IsStorageIntegrity: true, OriginalDatabase: "tenant", OriginalTable: "events"}}
	payload := []byte{byte(chproto.ClientDataCode), 0, 0xab, 0xcd}
	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if err := p.OnClientDataStrict(context.Background(), qctx, payload); err != nil {
		t.Fatal(err)
	}
	p.OnQueryInputComplete(context.Background(), qctx)
	adm, err := p.ConsumeAdmission(qctx.Session.ID())
	if err != nil {
		t.Fatalf("ConsumeAdmission: %v", err)
	}
	if adm.Signer != signerAddr || adm.Owner != azOwner || adm.Principal != azOwner {
		t.Fatalf("admission signer/owner/principal = %s/%s/%s", adm.Signer, adm.Owner, adm.Principal)
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `bazel test //pkg/plugins/storageintegrity:storageintegrity_test --test_filter='TestWriteAuthorizer|TestIngressAuthorizerRechecks|TestIngressForwardedSessionUsesHostResolvedOwner' --test_output=errors`
Expected: FAIL to compile (`newWriteAuthorizer`, `Config.Writers`, `chproto.CodeAccessDenied` undefined).

- [ ] **Step 3: Implement the authorizer**

Add to `pkg/chproto/client_error.go` inside the existing code block:

```go
	// CodeAccessDenied is ACCESS_DENIED: the storage-integrity ingress refuses
	// a write by a denylisted signer or owner, an invalid operator relation, or
	// a principal that is not a writer of the database (spec 2026-10-09 R6).
	CodeAccessDenied int32 = 497
```

```go
// pkg/plugins/storageintegrity/authorize.go
package storageintegrity

import (
	"errors"
	"fmt"
	"strings"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/registry"
)

// Result labels of storage_integrity_ingress_authz_total (spec 2026-10-09 §10).
const (
	authzAllowed         = "allowed"
	authzDeniedSigner    = "denied_signer"
	authzDeniedOwner     = "denied_owner"
	authzOperatorInvalid = "operator_invalid"
	authzNotWriter       = "not_writer"
	authzUnknownDatabase = "unknown_database"
)

var authzTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "storage_integrity_ingress_authz_total",
	Help: "Storage-integrity ingress write authorization decisions, by result.",
}, []string{"result"})

func init() { prometheus.MustRegister(authzTotal) }

// errWriterAccessRequired is the wiring refusal: without a writer predicate
// the ingress cannot decide who may write, so it refuses every write.
var errWriterAccessRequired = errors.New("storage_integrity.ingress requires a registry that implements WriterAccess (contract isDatabaseWriter)")

// writeAuthorizer is spec 2026-10-09 §6.2: open writes for every database
// writer, a static denylist, and the optional legacy allowlist. It resolves
// the owner from SQL_x_payer itself, so it depends neither on auth.enabled nor
// on plugin order.
type writeAuthorizer struct {
	denied    map[string]bool
	allowed   map[string]bool
	writers   registry.WriterAccess
	operators registry.OperatorChecker
}

type authorization struct {
	signer    string
	owner     string // validated operator relation; empty when the signer writes for itself
	principal string // owner, else signer
}

func newWriteAuthorizer(denied, allowed []string, writers registry.WriterAccess, operators registry.OperatorChecker) *writeAuthorizer {
	return &writeAuthorizer{denied: addressSet(denied), allowed: addressSet(allowed), writers: writers, operators: operators}
}

func addressSet(addresses []string) map[string]bool {
	set := make(map[string]bool, len(addresses))
	for _, address := range addresses {
		if address = strings.ToLower(strings.TrimSpace(address)); address != "" {
			set[address] = true
		}
	}
	return set
}

// normalizeOwner applies authplugin.resolveOwner's convention: ClickHouse
// wraps a Custom string setting in quotes, and addresses compare lowercase.
func normalizeOwner(payerSetting string) string {
	return strings.ToLower(strings.Trim(strings.TrimSpace(payerSetting), "\"'"))
}

func accessDenied(result, message string, cause error) (authorization, string, error) {
	return authorization{}, result, &chproto.ClientError{Code: chproto.CodeAccessDenied, Message: message, Err: cause}
}

// authorize returns the authorization, the metric result label and the
// refusal. Order is the spec's: denylisted signer, operator relation and
// denylisted owner, allowlist, then the writer predicate on the principal.
func (a *writeAuthorizer) authorize(signer, payerSetting, database string) (authorization, string, error) {
	if a == nil || a.writers == nil || a.operators == nil {
		return authorization{}, "", errWriterAccessRequired
	}
	signer = strings.ToLower(strings.TrimSpace(signer))
	if a.denied[signer] {
		return accessDenied(authzDeniedSigner, fmt.Sprintf("storage_integrity: signer %s is not permitted to write storage-integrity tables", signer), nil)
	}
	auth := authorization{signer: signer, principal: signer}
	if owner := normalizeOwner(payerSetting); owner != "" && owner != signer {
		if !a.operators.IsOperator(owner, signer) {
			return accessDenied(authzOperatorInvalid, fmt.Sprintf("storage_integrity: %s is not an operator of %s", signer, owner), nil)
		}
		if a.denied[owner] {
			return accessDenied(authzDeniedOwner, fmt.Sprintf("storage_integrity: owner %s is not permitted to write storage-integrity tables", owner), nil)
		}
		auth.owner, auth.principal = owner, owner
	}
	if len(a.allowed) > 0 && !a.allowed[signer] {
		return accessDenied(authzDeniedSigner, fmt.Sprintf("storage_integrity: signer %s is not permitted to write storage-integrity tables", signer), nil)
	}
	notWriter := fmt.Sprintf("storage_integrity: %s is not a writer of database %s", auth.principal, database)
	ok, err := a.writers.IsDatabaseWriter(database, auth.principal)
	if err != nil {
		return accessDenied(authzUnknownDatabase, notWriter, err)
	}
	if !ok {
		return accessDenied(authzNotWriter, notWriter, nil)
	}
	return auth, authzAllowed, nil
}

func countAuthz(result string) {
	if result != "" {
		authzTotal.WithLabelValues(result).Inc()
	}
}
```

- [ ] **Step 4: Wire it into the plugin**

In `pkg/plugins/storageintegrity/plugin.go`:

`Config` — add after `RequireTableSnapshot`:

```go
	// Writers, Operators, DeniedAddresses and AllowedAddresses authorize every
	// signed write (spec 2026-10-09 §6.2). Writers and Operators are required;
	// a plugin built without them refuses every storage-integrity write.
	Writers          registry.WriterAccess
	Operators        registry.OperatorChecker
	DeniedAddresses  []string
	AllowedAddresses []string
```

`Plugin` — add `authorizer *writeAuthorizer`; `New` sets `authorizer: newWriteAuthorizer(cfg.DeniedAddresses, cfg.AllowedAddresses, cfg.Writers, cfg.Operators)`.

`Admission` — add after `Signer`:

```go
	// Owner is the SQL_x_payer account the signer validly operates for, empty
	// when the signer writes for itself; Principal is Owner, else Signer. Both
	// are authorization and audit data only: the signed statement binds the
	// signer (spec 2026-10-09 D3).
	Owner     string
	Principal string
```

`admissionState` — add `payer string` and `database string`.

In `OnQuery`, right after `if err := requireStatementIDSigner(stmtID, signer); err != nil { return err }`:

```go
	payer := querySettings(qctx)[auth.PayerSettingKey]
	authz, result, err := p.authorizer.authorize(signer, payer, target.database)
	countAuthz(result)
	if err != nil {
		_, logger := log.FromContext(ctx)
		logger.Warnw("storage_integrity write refused",
			"statement_id", stmtID, "signer", signer, "owner", normalizeOwner(payer),
			"table_id", target.id, "result", result, "err", err)
		return err
	}
```

and set `payer: payer, database: target.database` plus `admission.Owner: authz.owner, admission.Principal: authz.principal` in the `admissionState` literal. Add the `github.com/housegate/housegate/pkg/log` import.

In `admissionFromState`, rename the unused `_ context.Context` parameter to `ctx`, and after `if err := requireStatementIDSigner(admission.StatementID, recovered); err != nil { return Admission{}, err }` add:

```go
	// Spec 2026-10-09 §6.2: re-check before the admission reaches the consumer,
	// so a revocation the registry saw during the upload stops the write.
	authz, result, err := p.authorizer.authorize(admission.Signer, state.payer, state.database)
	if err != nil {
		countAuthz(result)
		_, logger := log.FromContext(ctx)
		logger.Warnw("storage_integrity write refused at admission",
			"statement_id", admission.StatementID, "signer", admission.Signer,
			"owner", normalizeOwner(state.payer), "table_id", admission.TableID, "result", result, "err", err)
		return Admission{}, err
	}
	admission.Owner, admission.Principal = authz.owner, authz.principal
```

and immediately before the final `return admission, nil`:

```go
	_, logger := log.FromContext(ctx)
	logger.Infow("storage_integrity statement admitted",
		"statement_id", admission.StatementID, "signer", admission.Signer,
		"owner", admission.Owner, "principal", admission.Principal, "table_id", admission.TableID)
```

Test fixtures in `pkg/plugins/storageintegrity/plugin_test.go`:

```go
// allowAllWriters admits every account as a writer of every database. The
// authorizer's own tests use the in-memory network state instead.
type allowAllWriters struct{}

func (allowAllWriters) IsDatabaseWriter(string, string) (bool, error) { return true, nil }
func (allowAllWriters) IsOperator(owner, signer string) bool         { return owner == signer }
```

In `newSignedIngressWithoutV2Config`, before `return New(cfg), signer`:

```go
	if cfg.Writers == nil {
		cfg.Writers = allowAllWriters{}
	}
	if cfg.Operators == nil {
		cfg.Operators = allowAllWriters{}
	}
```

and in `TestIngressV2_RequiresStatementValidatorV2` add `Writers: allowAllWriters{}, Operators: allowAllWriters{},` to its `Config` literal.

- [ ] **Step 5: Run the plugin package**

Run: `bazel run //:gazelle && bazel test //pkg/plugins/storageintegrity:storageintegrity_test //pkg/chproto:chproto_test --test_output=errors`
Expected: PASS.

- [ ] **Step 6: Write the failing startup test and grant the root fixtures**

The root ingress tests build their registry with `buildTestStorageIntegrityNetworkState()` (`build_test.go:2057`), which registers no database and no grant, so the authorizer would refuse their signed INSERTs. Make the fixture a real writer setup and add the startup test in `build_test.go`:

```go
// buildTestStorageIntegrityKey is the key every root SI ingress test signs with.
const buildTestStorageIntegrityKey = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func buildTestStorageIntegrityNetworkState() *network.InMemoryNetworkState {
	ns := network.NewInMemoryNetworkState()
	schema := buildTestStorageIntegritySchema()
	ns.TableSchemas["tenant/events@1"] = network.TableSchemaInfo{
		DatabaseId: "tenant",
		TableId:    "events",
		Version:    1,
		SchemaHash: payloadexec.TableSchemaHash("testnet-v2", schema),
		SchemaJson: `{"table_id":"tenant.events","columns":[{"name":"id","type":"UInt64"},{"name":"region","type":"String"}]}`,
	}
	// Spec 2026-10-09 D2: the ingress admits only database writers.
	ns.DatabaseInfos["tenant"] = network.DatabaseInfo{DatabaseId: "tenant"}
	signer, err := auth.NewRelaySigner(buildTestStorageIntegrityKey)
	if err != nil {
		panic(err)
	}
	ns.DatabasePermissions[network.AccountAddress(strings.ToLower(signer.Address()))] = network.DatabasePermissions{"tenant": registry.DbAuthWrite}
	return ns
}

// registryWithoutWriters hides InMemoryNetworkState's WriterAccess.
type registryWithoutWriters struct{ registry.Registry }

func TestBuildServer_StorageIntegrityIngressRequiresWriterAccess(t *testing.T) {
	cfg := minimalRouterOnlyCfg(t)
	cfg.StorageIntegrity.Ingress.Enabled = true
	cfg.StorageIntegrity.Ingress.NetworkID = "testnet-v2"
	cfg.StorageIntegrity.Ingress.MaxTokenAge.Duration = time.Minute
	cfg.StorageIntegrity.Ingress.RequestTimeout.Duration = 50 * time.Millisecond
	cfg.StorageIntegrity.Ingress.MaxPayloadBytes = 7
	_, err := buildServer(Options{
		Config:                            cfg,
		NetworkState:                      registryWithoutWriters{Registry: buildTestStorageIntegrityNetworkState()},
		StorageIntegrityAdmissionConsumer: &recordingAdmissionConsumer{},
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "storage_integrity.ingress requires a registry that implements WriterAccess (contract isDatabaseWriter)") {
		t.Fatalf("buildServer err = %v, want the WriterAccess startup refusal", err)
	}
}
```

Replace the literal key in the existing root SI tests (`auth.NewRelaySigner("aaaa…")` in the ingress tests around `build_test.go:903-1070`) with `buildTestStorageIntegrityKey`.

Run: `bazel test //:housegate_test --test_filter='TestBuildServer_StorageIntegrityIngress' --test_output=errors`
Expected: `TestBuildServer_StorageIntegrityIngressRequiresWriterAccess` FAILS (`buildServer` succeeds); the existing ingress wiring tests still pass.

- [ ] **Step 7: Wire `buildServer`**

At the top of the `if cfg.StorageIntegrity.Ingress.Enabled {` block in `build.go` (before the runtime consumer is built, so a refusal tears nothing down):

```go
		// Spec 2026-10-09 D17: writes are open to every database writer, so the
		// ingress cannot start without the host's contract predicate — whether
		// or not an allowlist is configured.
		ingressWriters, ok := reg.(registry.WriterAccess)
		if !ok || isNilInterface(ingressWriters) {
			return nil, errors.New("storage_integrity.ingress requires a registry that implements WriterAccess (contract isDatabaseWriter)")
		}
```

and in the `storageintegrity.Config` literal:

```go
			Writers:          ingressWriters,
			Operators:        reg,
			DeniedAddresses:  ingressCfg.DeniedAddresses,
			AllowedAddresses: ingressCfg.AllowedAddresses,
```

Add `"denied_addresses", len(ingressCfg.DeniedAddresses),` to the `storage_integrity ingress enabled` log line. The `ingressValidator` keeps `ingressCfg.AllowedAddresses` (an empty list admits any valid signature, `pkg/auth/eth_validator.go:192-193`).

- [ ] **Step 8: Run the root suite**

Run: `bazel test //:housegate_test //pkg/plugins/storageintegrity:storageintegrity_test --test_output=errors`
Expected: PASS. A root SI test that injects a registry other than the fixture above now fails at startup with the WriterAccess refusal or at OnQuery with 497; give that test's registry the fixture's grant (or an `IsDatabaseWriter` method returning true when it is a deliberate non-network fake) — test wiring only, never a production fallback.

- [ ] **Step 9: Commit**

```bash
git add pkg/plugins/storageintegrity pkg/chproto/client_error.go build.go build_test.go
git commit -m "feat(storageintegrity): authorize SI writes by the contract writer predicate with a denylist (spec 2026-10-09 §6.2)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---
### Task 5: ordinary write path uses the contract writer predicate (`auth.writer_predicate`)

**Files:**
- Modify: `pkg/network/permission_commitgate_observer.go` (`PermissionCommitGateObserver` field, new constructor, `checkAccess`)
- Create: `pkg/network/permission_writer_predicate_test.go`
- Modify: `build.go:463-475` (observer construction and startup refusal)
- Test: `build_test.go`

**Interfaces:**
- Consumes: `registry.WriterAccess` and `(*InMemoryNetworkState).IsDatabaseWriter` (Task 2); `authplugin.Config.EffectiveWriterPredicate()` (Task 3).
- Produces: `func network.NewPermissionCommitGateObserverWithWriters(reg registry.Registry, writers registry.WriterAccess) *PermissionCommitGateObserver` — `writers == nil` keeps the bitmap behaviour (what `NewPermissionCommitGateObserver(reg)` still builds); non-nil answers every Write-bit policy entry with `IsDatabaseWriter(db, principal)`.

- [ ] **Step 1: Write the failing test**

```go
// pkg/network/permission_writer_predicate_test.go
package network

import (
	"context"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/registry"
	"github.com/housegate/housegate/pkg/sqlmeta"
)

const (
	predIndexer = "0x00000000000000000000000000000000000000aa"
	predWriter  = "0x00000000000000000000000000000000000000d1"
	predReader  = "0x00000000000000000000000000000000000000d2"
	predOwner   = "0x00000000000000000000000000000000000000d3"
	predNobody  = "0x00000000000000000000000000000000000000d4"
)

func predicateState() *InMemoryNetworkState {
	st := NewInMemoryNetworkState()
	st.IndexerInfos[5] = IndexerInfo{IndexerId: 5, Signer: predIndexer}
	st.DatabaseInfos["db"] = DatabaseInfo{DatabaseId: "db", IndexerId: 5}
	st.DatabasePermissions[predWriter] = DatabasePermissions{"db": registry.DbAuthWrite}
	st.DatabasePermissions[predReader] = DatabasePermissions{"db": registry.DbAuthRead}
	st.DatabasePermissions[predOwner] = DatabasePermissions{"db": registry.DbAuthOwner}
	st.DatabasePermissions[WildcardAddress] = DatabasePermissions{"db": registry.DbAuthWrite | registry.DbAuthRead}
	return st
}

func TestPermissionContractPredicate(t *testing.T) {
	st := predicateState()
	contract := NewPermissionCommitGateObserverWithWriters(st, st)
	bitmap := NewPermissionCommitGateObserver(st)
	for _, tc := range []struct {
		name         string
		typ          sqlmeta.StatementType
		user         string
		wantContract bool
		wantBitmap   bool
	}{
		{"wildcard Write no longer admits INSERT", sqlmeta.StatementTypeInsert, predNobody, false, true},
		{"wildcard Write no longer admits CREATE TABLE", sqlmeta.StatementTypeCreateTable, predNobody, false, true},
		{"wildcard Read still admits SELECT", sqlmeta.StatementTypeSelect, predNobody, true, true},
		{"indexer signer writes its own database", sqlmeta.StatementTypeInsert, predIndexer, true, true},
		{"Write grantee", sqlmeta.StatementTypeInsert, predWriter, true, true},
		{"Owner", sqlmeta.StatementTypeDropTable, predOwner, true, true},
		{"Read grantee cannot write", sqlmeta.StatementTypeInsert, predReader, false, true},
		{"Admin statements unchanged", sqlmeta.StatementTypeGrant, predWriter, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ev := newEvent(tc.typ, tc.user, "db", "t")
			if err := contract.BeforeStatement(context.Background(), ev); (err == nil) != tc.wantContract {
				t.Fatalf("contract: err = %v, want allowed=%v", err, tc.wantContract)
			}
			ev = newEvent(tc.typ, tc.user, "db", "t")
			if err := bitmap.BeforeStatement(context.Background(), ev); (err == nil) != tc.wantBitmap {
				t.Fatalf("bitmap: err = %v, want allowed=%v", err, tc.wantBitmap)
			}
		})
	}
}

func TestPermissionContractPredicate_OperatorPrincipal(t *testing.T) {
	st := predicateState()
	st.SetOperator(predOwner, predNobody, true)
	o := NewPermissionCommitGateObserverWithWriters(st, st)
	ev := newEvent(sqlmeta.StatementTypeInsert, predNobody, "db", "t")
	ev.Owner = predOwner
	if err := o.BeforeStatement(context.Background(), ev); err != nil {
		t.Fatalf("operator for an Owner must write: %v", err)
	}
}

func TestPermissionContractPredicate_RefusalNamesWrite(t *testing.T) {
	st := predicateState()
	o := NewPermissionCommitGateObserverWithWriters(st, st)
	err := o.BeforeStatement(context.Background(), newEvent(sqlmeta.StatementTypeInsert, predReader, "db", "t"))
	if err == nil || !strings.Contains(err.Error(), "lacks write on database \"db\"") {
		t.Fatalf("err = %v, want the lacks-write message", err)
	}
}
```

Every bitmap-column `true` for an account without its own Write bit comes from the address(0) `Write|Read` grant in the fixture: that union is exactly what the contract predicate drops.

- [ ] **Step 2: Run it to verify it fails**

Run: `bazel test //pkg/network:network_test --test_filter='TestPermissionContractPredicate' --test_output=errors`
Expected: FAIL to compile (`NewPermissionCommitGateObserverWithWriters` undefined).

- [ ] **Step 3: Implement**

In `pkg/network/permission_commitgate_observer.go`:

```go
type PermissionCommitGateObserver struct {
	reg registry.Registry
	// writers, when set, answers every Write-bit policy entry with the
	// contract's isDatabaseWriter (spec 2026-10-09 R3, auth.writer_predicate:
	// contract): no address(0) union, and the indexer signer counts. Nil keeps
	// the stored-bitmap check with the wildcard union (writer_predicate: bitmap).
	writers registry.WriterAccess
}

// NewPermissionCommitGateObserverWithWriters builds the observer with the
// contract writer predicate. writers == nil is the bitmap behaviour.
func NewPermissionCommitGateObserverWithWriters(reg registry.Registry, writers registry.WriterAccess) *PermissionCommitGateObserver {
	return &PermissionCommitGateObserver{reg: reg, writers: writers}
}
```

In `checkAccess`, right after the `PendingDelete` refusal and before the bitmap is assembled:

```go
	if required == registry.DbAuthWrite && o.writers != nil {
		ok, err := o.writers.IsDatabaseWriter(string(db), string(account))
		if err != nil {
			return fmt.Errorf("permission: database %q: %w", db, err)
		}
		if !ok {
			return fmt.Errorf("permission: account %s lacks %s on database %q for %s",
				account, prettyAuthBit(required), db, stmtType)
		}
		return nil
	}
```

Read, Admin and Owner checks keep the bitmap path. Update the type comment's "Bit semantics" paragraph with one sentence: "With writers set (auth.writer_predicate: contract) the Write bit is the contract's isDatabaseWriter instead."

- [ ] **Step 4: Run the package**

Run: `bazel test //pkg/network:network_test --test_output=errors`
Expected: PASS.

- [ ] **Step 5: Write the failing build test**

Append to `build_test.go`:

```go
func TestBuildServer_WriterPredicateRequiresWriterAccess(t *testing.T) {
	cfg := withoutRewriter(minimalServerCfg(t))
	cfg.Auth.Enabled = true
	_, err := buildServer(Options{Config: cfg, NetworkState: registryWithoutWriters{Registry: network.NewInMemoryNetworkState()}}, nil)
	if err == nil || !strings.Contains(err.Error(), "auth.writer_predicate: bitmap") {
		t.Fatalf("buildServer err = %v, want the writer-predicate startup refusal naming the escape hatch", err)
	}
	cfg.Auth.WriterPredicate = "bitmap"
	bs, err := buildServer(Options{Config: cfg, NetworkState: registryWithoutWriters{Registry: network.NewInMemoryNetworkState()}}, nil)
	if err != nil {
		t.Fatalf("bitmap escape hatch must start without WriterAccess: %v", err)
	}
	bs.teardown()
}
```

(`registryWithoutWriters` is from Task 4.) Run: `bazel test //:housegate_test --test_filter='TestBuildServer_WriterPredicateRequiresWriterAccess' --test_output=errors`. Expected: FAIL (startup succeeds with the contract default).

- [ ] **Step 6: Wire `buildServer`**

Replace the `if cfg.Auth.Enabled { ... NewPermissionCommitGateObserver(reg) ... }` block at `build.go:470-475` with:

```go
	if cfg.Auth.Enabled {
		permissionObserver := network.NewPermissionCommitGateObserver(reg)
		if cfg.Auth.EffectiveWriterPredicate() == authplugin.WriterPredicateContract {
			// Spec 2026-10-09 R3: ordinary writes use the contract predicate, the
			// same one the SI ingress uses, so INSERT agrees with CREATE/DROP.
			writers, ok := reg.(registry.WriterAccess)
			if !ok || isNilInterface(writers) {
				return nil, errors.New("auth.writer_predicate: contract (the default) requires a registry that implements WriterAccess (contract isDatabaseWriter); set auth.writer_predicate: bitmap to keep the stored-bitmap check")
			}
			permissionObserver = network.NewPermissionCommitGateObserverWithWriters(reg, writers)
		}
		log.Infow("permission gate enabled", "writer_predicate", cfg.Auth.EffectiveWriterPredicate())
		opts.CommitGateObservers = append(
			[]commitgate.Observer{permissionObserver},
			opts.CommitGateObservers...,
		)
	}
```

`authplugin` is already imported in `build.go` as the auth plugin package alias; if the file imports it as `authplugin "github.com/housegate/housegate/pkg/plugins/auth"`, use that alias.

- [ ] **Step 7: Run root and integration-compile checks**

Run: `bazel test //:housegate_test //pkg/network:network_test --test_output=errors && bazel build //pkg/integration:integration_test`
Expected: PASS / build OK. Root tests that enable `auth` with a fake non-network registry fail at startup; give those fakes `IsDatabaseWriter` (test wiring only) or set `cfg.Auth.WriterPredicate = "bitmap"` where the test is about the bitmap.

- [ ] **Step 8: Commit**

```bash
git add pkg/network/permission_commitgate_observer.go pkg/network/permission_writer_predicate_test.go build.go build_test.go
git commit -m "feat(network): auth.writer_predicate contract (default) aligns ordinary writes with isDatabaseWriter (spec 2026-10-09 R3)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---
### Task 6: the unspent marker — typed flag, one renderer, Relay marking

**Files:**
- Modify: `pkg/chproto/client_error.go` (`ClientError.SeqUnspent`, `SeqUnspentSuffix`, `MarkSeqUnspent`, `IsSeqUnspent`, `HasSeqUnspentSuffix`, `TrimSeqUnspentSuffix`; `isTableRefusalMessage` strips the suffix)
- Test: `pkg/chproto/client_error_test.go`
- Modify: `pkg/proxy/relay.go` (`exceptionForPluginError` `:2167-2180`, `isSessionPreservingIngressException` `:2194-2210`, marking at the data-limit gate `:935-943`, the OnQuery chain `:1002-1009`, the SynthesizedInsert plan conflict `:1010-1017`, the DeferredInsert/Suppress conflict `:1173-1182`, the strict data hook `:1299-1305`)
- Create: `pkg/proxy/relay_unspent_test.go`

**Interfaces:**
- Consumes: `auth.StatementTokenSettingKey` (pinned by a test only).
- Produces:
  - `chproto.SeqUnspentSuffix = " [client_seq unspent]"`
  - `chproto.ClientError.SeqUnspent bool`
  - `func chproto.MarkSeqUnspent(err error) error` (idempotent; preserves `errors.As` to any wrapped `*ClientError`, so `KeepsSession` keeps working)
  - `func chproto.IsSeqUnspent(err error) bool`
  - `func chproto.HasSeqUnspentSuffix(message string) bool`, `func chproto.TrimSeqUnspentSuffix(message string) string`
  - Relay: every refusal of a Query that carries `SQL_x_statement_token`, raised by the OnQuery chain, `OnClientDataStrict`, the strict data-limit gate or Relay's own pre-forward plan checks, renders with the suffix.

- [ ] **Step 1: Write the failing chproto tests**

Append to `pkg/chproto/client_error_test.go`:

```go
func TestMarkSeqUnspent(t *testing.T) {
	plain := errors.New("refused")
	if IsSeqUnspent(plain) {
		t.Fatal("unmarked error reported unspent")
	}
	marked := MarkSeqUnspent(plain)
	if !IsSeqUnspent(marked) || marked.Error() != "refused" || !errors.Is(marked, plain) {
		t.Fatalf("marked = %v (unspent=%v)", marked, IsSeqUnspent(marked))
	}
	if MarkSeqUnspent(marked) != marked {
		t.Fatal("MarkSeqUnspent must be idempotent")
	}
	if MarkSeqUnspent(nil) != nil {
		t.Fatal("MarkSeqUnspent(nil) must stay nil")
	}
	keep := &ClientError{Code: CodeTooManyParts, Message: "storage_integrity: back-pressure: retry later", KeepSession: true}
	wrapped := fmt.Errorf("strict: %w", MarkSeqUnspent(keep))
	if !IsSeqUnspent(wrapped) || !KeepsSession(wrapped) {
		t.Fatal("marking must not hide the wrapped ClientError")
	}
	if !IsSeqUnspent(&ClientError{Code: 497, Message: "x", SeqUnspent: true}) {
		t.Fatal("ClientError.SeqUnspent must count as marked")
	}
}

func TestSeqUnspentSuffixHelpers(t *testing.T) {
	msg := TableActivatingMessage("db1.t") + SeqUnspentSuffix
	if !HasSeqUnspentSuffix(msg) || TrimSeqUnspentSuffix(msg) != TableActivatingMessage("db1.t") {
		t.Fatalf("suffix helpers on %q", msg)
	}
	if !IsTableActivatingMessage(msg) {
		t.Fatal("IsTableActivatingMessage must accept the marker suffix")
	}
	if !IsTableNoLongerAcceptsWritesMessage(TableNoLongerAcceptsWritesMessage("db1.t") + SeqUnspentSuffix) {
		t.Fatal("IsTableNoLongerAcceptsWritesMessage must accept the marker suffix")
	}
	if HasSeqUnspentSuffix("storage_integrity: refused") {
		t.Fatal("unsuffixed message reported the marker")
	}
}
```

(Add `"errors"` / `"fmt"` imports if the file lacks them.)

- [ ] **Step 2: Run them to verify they fail**

Run: `bazel test //pkg/chproto:chproto_test --test_filter='TestMarkSeqUnspent|TestSeqUnspentSuffixHelpers' --test_output=errors`
Expected: FAIL to compile (`MarkSeqUnspent` undefined).

- [ ] **Step 3: Implement the primitives**

In `pkg/chproto/client_error.go` add the field to `ClientError`:

```go
	// SeqUnspent marks a refusal that provably leaves the statement's
	// client_seq coordinate unspent (spec 2026-10-09 §6.6). Relay renders it
	// as SeqUnspentSuffix; the agent returns the seq to its free list.
	SeqUnspent bool
```

and append:

```go
// SeqUnspentSuffix is appended, once, to the client message of every refusal
// that provably left the client_seq coordinate unspent (spec 2026-10-09 D16).
// The agent matches it to recycle the seq; nothing else may emit it.
const SeqUnspentSuffix = " [client_seq unspent]"

type seqUnspentError struct{ err error }

func (e *seqUnspentError) Error() string { return e.err.Error() }
func (e *seqUnspentError) Unwrap() error { return e.err }

// MarkSeqUnspent flags err as a provably-unspent refusal. It wraps without
// changing the text, so the flag survives further %w wrapping and every
// errors.As on the wrapped chain still works.
func MarkSeqUnspent(err error) error {
	if err == nil || IsSeqUnspent(err) {
		return err
	}
	return &seqUnspentError{err: err}
}

// IsSeqUnspent reports whether err carries the unspent flag, either through
// MarkSeqUnspent or a ClientError with SeqUnspent set.
func IsSeqUnspent(err error) bool {
	var marked *seqUnspentError
	if errors.As(err, &marked) {
		return true
	}
	var clientErr *ClientError
	return errors.As(err, &clientErr) && clientErr.SeqUnspent
}

// HasSeqUnspentSuffix reports whether a rendered Exception message carries
// the marker.
func HasSeqUnspentSuffix(message string) bool {
	return strings.HasSuffix(strings.TrimSpace(message), SeqUnspentSuffix)
}

// TrimSeqUnspentSuffix removes one trailing marker; message matchers compare
// the text before it.
func TrimSeqUnspentSuffix(message string) string {
	return strings.TrimSuffix(strings.TrimSpace(message), SeqUnspentSuffix)
}
```

and make `isTableRefusalMessage` start with `message = TrimSeqUnspentSuffix(message)`.

- [ ] **Step 4: Run chproto**

Run: `bazel test //pkg/chproto:chproto_test --test_output=errors`
Expected: PASS.

- [ ] **Step 5: Write the failing Relay tests**

```go
// pkg/proxy/relay_unspent_test.go
package proxy

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/plugin"
)

func TestStatementTokenSettingMatchesAuth(t *testing.T) {
	if statementTokenSetting != auth.StatementTokenSettingKey {
		t.Fatalf("relay statement-token key %q drifted from auth %q", statementTokenSetting, auth.StatementTokenSettingKey)
	}
}

func TestExceptionForPluginError_RendersUnspentMarkerOnce(t *testing.T) {
	for name, tc := range map[string]struct {
		err  error
		want string
	}{
		"client error": {fmt.Errorf("strict: %w", chproto.MarkSeqUnspent(&chproto.ClientError{Code: 497, Message: "storage_integrity: 0xa is not a writer of database d"})), "storage_integrity: 0xa is not a writer of database d [client_seq unspent]"},
		"field":        {&chproto.ClientError{Code: 252, Message: "storage_integrity: back-pressure: retry later", SeqUnspent: true}, "storage_integrity: back-pressure: retry later [client_seq unspent]"},
		"plain":        {chproto.MarkSeqUnspent(errors.New("storage_integrity rejects compressed payloads")), "storage_integrity rejects compressed payloads [client_seq unspent]"},
		"already":      {chproto.MarkSeqUnspent(errors.New("x [client_seq unspent]")), "x [client_seq unspent]"},
		"unmarked":     {errors.New("jws invalid"), "jws invalid"},
	} {
		if got := exceptionForPluginError(tc.err).Message; got != tc.want {
			t.Errorf("%s: message = %q, want %q", name, got, tc.want)
		}
	}
}

func TestSessionPreservingIngressException_AcceptsMarker(t *testing.T) {
	for _, exc := range []*chproto.Exception{
		{Code: proto.Error(chproto.CodeTooManyParts), Message: "storage_integrity: back-pressure: retry later" + chproto.SeqUnspentSuffix},
		{Code: proto.Error(chproto.CodeTableIsBeingRestarted), Message: chproto.TableActivatingMessage("db1.t") + chproto.SeqUnspentSuffix},
		{Code: proto.Error(chproto.CodeQueryIsProhibited), Message: chproto.TableNoLongerAcceptsWritesMessage("db1.t") + chproto.SeqUnspentSuffix},
	} {
		if !isSessionPreservingIngressException(exc) {
			t.Errorf("marked %d %q must stay session-preserving", exc.Code, exc.Message)
		}
	}
}

type onQueryRejectHooks struct {
	plugin.NoopHooks
	err error
}

func (h *onQueryRejectHooks) OnQuery(context.Context, *plugin.QueryContext) error { return h.err }

func encodeQueryWithSettings(t *testing.T, id, sql string, settings []proto.Setting) []byte {
	t.Helper()
	var qb proto.Buffer
	(&proto.Query{
		ID: id, Body: sql, Settings: settings,
		Info: proto.ClientInfo{
			ProtocolVersion: deferredTestRev, Major: 24, Minor: 1,
			Interface: proto.InterfaceTCP,
			Query:     proto.ClientQueryInitial,
		},
	}).EncodeAware(&qb, deferredTestRev)
	return append([]byte(nil), qb.Buf...)
}

// Spec 2026-10-09 §6.6 (1): nothing in the OnQuery chain can reach the
// arbiter, so a token-bearing refusal there is marked; a Query without the
// token is not.
func TestRelay_OnQueryRefusalOfSignedQueryCarriesMarker(t *testing.T) {
	for name, tc := range map[string]struct {
		settings []proto.Setting
		want     string
	}{
		"signed":   {[]proto.Setting{{Key: auth.StatementTokenSettingKey, Value: "'tok'", Custom: true}}, "storage_integrity: refused [client_seq unspent]"},
		"unsigned": {nil, "storage_integrity: refused"},
	} {
		t.Run(name, func(t *testing.T) {
			h := newDeferredHarness(t, &onQueryRejectHooks{err: errors.New("storage_integrity: refused")})
			writeAllConn(t, h.clientProxy, encodeQueryWithSettings(t, "q1", "INSERT INTO db.t FORMAT Native", tc.settings))
			exc := readServerException(t, h.clientProxy)
			if exc.Message != tc.want {
				t.Fatalf("message = %q, want %q", exc.Message, tc.want)
			}
			if strings.Count(exc.Message, "[client_seq unspent]") > 1 {
				t.Fatalf("marker rendered twice: %q", exc.Message)
			}
		})
	}
}

type strictDataRejectHooks struct {
	plugin.NoopHooks
}

func (strictDataRejectHooks) OnClientDataStrict(context.Context, *plugin.QueryContext, []byte) error {
	return errors.New("storage_integrity payload exceeds max_payload_bytes")
}

func TestRelay_StrictDataRefusalOfSignedQueryCarriesMarker(t *testing.T) {
	h := newDeferredHarness(t, strictDataRejectHooks{})
	upstreamDone := make(chan error, 1)
	go func() {
		codec := chproto.NewCodec(h.upstreamProxy, chproto.DirFromClient)
		codec.SetRevision(deferredTestRev)
		_, err := codec.ReadPacket(uint64(chproto.ClientQueryCode))
		upstreamDone <- err
	}()
	writeAllConn(t, h.clientProxy, encodeQueryWithSettings(t, "q1", "INSERT INTO db.t FORMAT Native",
		[]proto.Setting{{Key: auth.StatementTokenSettingKey, Value: "'tok'", Custom: true}}))
	writeAllConn(t, h.clientProxy, encodeNonEmptyClientDataPacket(t, deferredTestRev))
	exc := readServerException(t, h.clientProxy)
	if !chproto.HasSeqUnspentSuffix(exc.Message) {
		t.Fatalf("message = %q, want the unspent marker", exc.Message)
	}
	<-upstreamDone
}
```

Run: `bazel run //:gazelle && bazel test //pkg/proxy:proxy_test --test_filter='TestStatementTokenSettingMatchesAuth|TestExceptionForPluginError_RendersUnspentMarkerOnce|TestSessionPreservingIngressException_AcceptsMarker|TestRelay_OnQueryRefusalOfSignedQueryCarriesMarker|TestRelay_StrictDataRefusalOfSignedQueryCarriesMarker' --test_output=errors`
Expected: FAIL to compile (`statementTokenSetting` undefined).

- [ ] **Step 6: Implement in Relay**

In `pkg/proxy/relay.go`:

```go
// statementTokenSetting mirrors auth.StatementTokenSettingKey without the
// import; TestStatementTokenSettingMatchesAuth keeps them equal.
const statementTokenSetting = "SQL_x_statement_token"

// markUnspentIfSigned flags a pre-submission refusal of a signed SI statement
// as provably unspent (spec 2026-10-09 §6.6 (1)): submission to the arbiter
// happens only inside OnQueryInputCompleteStrict, so no refusal raised before
// it can have spent the coordinate.
func markUnspentIfSigned(q *chproto.Query, err error) error {
	if err == nil || q == nil {
		return err
	}
	for _, setting := range q.Settings {
		if setting.Key == statementTokenSetting {
			return chproto.MarkSeqUnspent(err)
		}
	}
	return err
}
```

Replace `exceptionForPluginError` with:

```go
// exceptionForPluginError maps a plugin error to the synthetic Exception the
// client sees. ClientError selects an explicit code and message; all other
// plugin rejections keep the generic 403 behavior. A refusal flagged unspent
// gets chproto.SeqUnspentSuffix exactly once — the only place it is rendered.
func exceptionForPluginError(pluginErr error) *chproto.Exception {
	exception := &chproto.Exception{Code: 403, Name: "DB::Exception", Message: pluginErr.Error()} // 403 = AUTHENTICATION_FAILED; generic plugin-reject
	var clientErr *chproto.ClientError
	if errors.As(pluginErr, &clientErr) {
		exception.Code = proto.Error(clientErr.Code)
		exception.Message = clientErr.Message
	}
	if chproto.IsSeqUnspent(pluginErr) && !chproto.HasSeqUnspentSuffix(exception.Message) {
		exception.Message += chproto.SeqUnspentSuffix
	}
	return exception
}
```

In `isSessionPreservingIngressException` change the message line to `message := chproto.TrimSeqUnspentSuffix(exc.Message)`.

Mark the five sites (the error value written to the client and returned keeps its text; only the flag is added):

```go
		// data-limit gate (top of clientToUpstream)
		if errors.Is(decErr, chproto.ErrPacketTooLarge) {
			err := fmt.Errorf("client Data packet exceeds remaining payload limit %d: %w", limit, decErr)
			if limitQctx != nil {
				err = markUnspentIfSigned(limitQctx.Query, err)
			}
			r.writeExceptionToClient(ctx, err)
			...

			// OnQuery chain
			if err := r.hooks.OnQuery(ctx, qctx); err != nil {
				err = markUnspentIfSigned(qctx.Query, err)
				r.writeExceptionToClient(ctx, err)
				...

			// SynthesizedInsert plan conflict right after OnQuery
				err := markUnspentIfSigned(qctx.Query, fmt.Errorf("query %q: SynthesizedInsert conflicts with another ownership plan", q.ID))

			// DeferredInsert + SuppressUpstreamExecution conflict
					err := markUnspentIfSigned(qctx.Query, fmt.Errorf("query %q: DeferredInsert and SuppressUpstreamExecution are mutually exclusive", q.ID))

			// strict data hook
				if err := r.hooks.OnClientDataStrict(ctx, curQctx, pkt.Raw); err != nil {
					err = markUnspentIfSigned(curQctx.Query, err)
					r.writeExceptionToClient(ctx, err)
					...
```

`OnQueryInputCompleteStrict` refusals are not marked here: they may follow a submission, so the ingress marks its own (Task 7).

- [ ] **Step 7: Run the proxy package**

Run: `bazel test //pkg/proxy:proxy_test --test_output=errors`
Expected: PASS (`TestExceptionForPluginError_DefaultsTo403` and `_HonorsClientError` unchanged).

- [ ] **Step 8: Commit**

```bash
git add pkg/chproto/client_error.go pkg/chproto/client_error_test.go pkg/proxy/relay.go pkg/proxy/relay_unspent_test.go pkg/proxy/BUILD.bazel
git commit -m "feat(proxy): render the client_seq unspent marker once; mark signed pre-submission refusals (spec 2026-10-09 §6.6)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---
### Task 7: the ingress and the intake runtime mark their own unspent refusals; coded rejects name their code

**Files:**
- Modify: `pkg/plugins/storageintegrity/plugin.go` (`OnQueryInputCompleteStrict` `:458-464`)
- Modify: `storage_integrity_ingress.go` (`ConsumeStorageIntegrityAdmission` `:531-699`)
- Modify: `pkg/storageintegrity/intake.go` (`SubmitOutcome` `:385-392`), `pkg/storageintegrity/arbiter_proto.go` (`AdmissionCodeDuplicateClientSeq`, `SubmitOutcomeFromSequencedAck`)
- Create: `storage_integrity_ingress_unspent_test.go`
- Test: `pkg/storageintegrity/admission_code_test.go`, `pkg/plugins/storageintegrity/plugin_test.go`

**Interfaces:**
- Consumes: `chproto.MarkSeqUnspent`, `chproto.IsSeqUnspent`, `ClientError.SeqUnspent` (Task 6); the `newBackpressureIngress` / `bpAdmission` fixtures in `storage_integrity_table_state_runtime_test.go` and its siblings.
- Produces:
  - `sicore.AdmissionCodeDuplicateClientSeq = pb.AdmissionCode_ADMISSION_CODE_DUPLICATE_CLIENT_SEQ.String()`
  - `sicore.SubmitOutcome.StatementSeq uint64` (`json:",omitempty"`), set from `SequencedAck.statement_seq` on `ACCEPTED` (Task 8 reads it).
  - A coded terminal reject other than `DUPLICATE_CLIENT_SEQ` returns `storage_integrity: statement <id> rejected by the arbiter: <ADMISSION_CODE_…>`, marked unspent.

- [ ] **Step 1: Write the failing tests**

Append to `pkg/storageintegrity/admission_code_test.go`:

```go
func TestSubmitOutcomeCarriesTheStatementSeq(t *testing.T) {
	got := SubmitOutcomeFromSequencedAck(&pb.SequencedAck{Code: pb.AdmissionCode_ADMISSION_CODE_ACCEPTED, StatementSeq: 77})
	if got.Category != OutcomeAccepted || got.StatementSeq != 77 {
		t.Fatalf("outcome = %+v, want accepted with statement_seq 77", got)
	}
	if AdmissionCodeDuplicateClientSeq != "ADMISSION_CODE_DUPLICATE_CLIENT_SEQ" {
		t.Fatalf("AdmissionCodeDuplicateClientSeq = %q", AdmissionCodeDuplicateClientSeq)
	}
}
```

Create `storage_integrity_ingress_unspent_test.go`:

```go
package housegate

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/chproto"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

// Spec 2026-10-09 §6.6 (2): every ConsumeStorageIntegrityAdmission refusal
// before Orchestrate is provably unspent.
func TestConsumeAdmission_PreOrchestrateRefusalsAreUnspent(t *testing.T) {
	orch := sicore.NewOrchestrator(&rootRecordingSubmitter{}, &rootRecordingPreparer{}, sicore.OrchestratorConfig{})
	ing, err := NewStorageIntegrityIngressWithPayloadWriter(orch, nil, sicore.MaterializerNative, &rootRecordingPayloadWriter{})
	if err != nil {
		t.Fatal(err)
	}
	err = ing.ConsumeStorageIntegrityAdmission(context.Background(), storageIntegrityAdmissionForEncoding(sicore.EncodingCSVWithNames, 54465))
	if err == nil || !chproto.IsSeqUnspent(err) {
		t.Fatalf("materializer mismatch err = %v, want marked unspent", err)
	}

	guarded, err := NewStorageIntegrityIngressWithPayloadWriter(orch, &unhealthyMergeGuard{err: errors.New("reconnect failed")}, sicore.MaterializerNative, &rootRecordingPayloadWriter{})
	if err != nil {
		t.Fatal(err)
	}
	err = guarded.ConsumeStorageIntegrityAdmission(context.Background(), storageIntegrityAdmissionForEncoding(sicore.PayloadEncodingClickHouseNativeData, 54465))
	if err == nil || !chproto.IsSeqUnspent(err) {
		t.Fatalf("merge-health err = %v, want marked unspent", err)
	}

	failingWriter := &rootRecordingPayloadWriter{err: errors.New("store down")}
	stored, err := NewStorageIntegrityIngressWithPayloadWriter(orch, nil, sicore.MaterializerNative, failingWriter)
	if err != nil {
		t.Fatal(err)
	}
	err = stored.ConsumeStorageIntegrityAdmission(context.Background(), storageIntegrityAdmissionForEncoding(sicore.PayloadEncodingClickHouseNativeData, 54465))
	if err == nil || !chproto.IsSeqUnspent(err) {
		t.Fatalf("PutPayload err = %v, want marked unspent", err)
	}
}

func TestConsumeAdmission_PostOrchestrateMarking(t *testing.T) {
	for name, tc := range map[string]struct {
		outcome    sicore.SubmitOutcome
		submitErr  error
		wantMarked bool
		wantText   string
	}{
		"gap budget": {outcome: sicore.SubmitOutcome{Category: sicore.OutcomeTerminalReject, Reason: "64 open ranges", AdmissionCode: "ADMISSION_CODE_GAP_BUDGET_EXCEEDED"}, wantMarked: true, wantText: "rejected by the arbiter: ADMISSION_CODE_GAP_BUDGET_EXCEEDED"},
		"malformed":  {outcome: sicore.SubmitOutcome{Category: sicore.OutcomeTerminalReject, Reason: "bad", AdmissionCode: "ADMISSION_CODE_MALFORMED"}, wantMarked: true, wantText: "rejected by the arbiter: ADMISSION_CODE_MALFORMED"},
		"schema":     {outcome: sicore.SubmitOutcome{Category: sicore.OutcomeTerminalReject, Reason: "retired", AdmissionCode: sicore.AdmissionCodeSchemaNotAllowed}, wantMarked: true, wantText: "no longer accepts writes"},
		"duplicate":  {outcome: sicore.SubmitOutcome{Category: sicore.OutcomeTerminalReject, Reason: "dup", AdmissionCode: sicore.AdmissionCodeDuplicateClientSeq}},
		"no code":    {outcome: sicore.SubmitOutcome{Category: sicore.OutcomeTerminalReject, Reason: "permission denied"}},
		"retryable":  {outcome: sicore.SubmitOutcome{Category: sicore.OutcomeRetryable, Reason: "fence"}},
		"unknown":    {outcome: sicore.SubmitOutcome{Category: sicore.OutcomeUnknown, Reason: "deadline"}},
	} {
		t.Run(name, func(t *testing.T) {
			ingress, _, submitter, _ := newBackpressureIngress(t, &fakePartsPressure{})
			submitter.outcome = tc.outcome
			submitter.err = tc.submitErr
			adm := bpAdmission()
			err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), adm)
			if err == nil {
				t.Fatal("a non-ACK2 outcome must refuse")
			}
			if chproto.IsSeqUnspent(err) != tc.wantMarked {
				t.Fatalf("err = %v, marked = %v, want %v", err, chproto.IsSeqUnspent(err), tc.wantMarked)
			}
			if tc.wantText != "" && !strings.Contains(err.Error(), tc.wantText) {
				t.Fatalf("err = %v, want %q", err, tc.wantText)
			}
			if name == "gap budget" && err.Error() != "storage_integrity: statement "+adm.StatementID+" rejected by the arbiter: ADMISSION_CODE_GAP_BUDGET_EXCEEDED" {
				t.Fatalf("coded reject text = %q", err)
			}
		})
	}
}
```

`newBackpressureIngress` (`storage_integrity_backpressure_ingress_test.go:249`) returns `(ingress, writer, submitter, preparer)`; `rootRecordingSubmitter` and `rootRecordingPayloadWriter` already carry `outcome`/`err` and `err` fields.

Append to `pkg/plugins/storageintegrity/plugin_test.go`:

```go
type failingConsumer struct{ err error }

func (c failingConsumer) ConsumeStorageIntegrityAdmission(context.Context, Admission) error { return c.err }

// Spec 2026-10-09 §6.6 (2): an admissionFromState failure (here a statement
// token that does not bind the captured payload) is marked by the ingress;
// a consumer error is passed through unchanged, because the consumer marks
// only what it can prove.
func TestIngressStrictHookMarksAdmissionFailuresOnly(t *testing.T) {
	for name, tc := range map[string]struct {
		payload    []byte
		consumer   error
		wantMarked bool
	}{
		"token mismatch": {payload: []byte{byte(chproto.ClientDataCode), 0, 0xff}, wantMarked: true},
		"consumer error": {payload: []byte{byte(chproto.ClientDataCode), 0, 0xab, 0xcd}, consumer: errors.New("orchestrate: transport")},
	} {
		t.Run(name, func(t *testing.T) {
			ns, _ := ingressNetworkState(t)
			p, signer := newSignedIngressWithConfig(t, Config{TableSchemas: ns, NetworkID: "testnet-v2", AdmissionConsumer: failingConsumer{err: tc.consumer}})
			sql := "INSERT INTO tenant.events FORMAT Native"
			qctx := signedQueryContext(t, 63, signer, sql, sql, sqlmeta.StatementTypeInsert) // token binds {0xab, 0xcd}
			qctx.AccessedTables = []sqlmeta.AccessedTable{{IsStorageIntegrity: true, OriginalDatabase: "tenant", OriginalTable: "events"}}
			if err := p.OnQuery(context.Background(), qctx); err != nil {
				t.Fatal(err)
			}
			if err := p.OnClientDataStrict(context.Background(), qctx, tc.payload); err != nil {
				t.Fatal(err)
			}
			err := p.OnQueryInputCompleteStrict(context.Background(), qctx)
			if err == nil || chproto.IsSeqUnspent(err) != tc.wantMarked {
				t.Fatalf("err = %v marked=%v, want marked=%v", err, chproto.IsSeqUnspent(err), tc.wantMarked)
			}
		})
	}
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `bazel run //:gazelle && bazel test //:housegate_test //pkg/storageintegrity:storageintegrity_test //pkg/plugins/storageintegrity:storageintegrity_test --test_filter='TestConsumeAdmission_|TestSubmitOutcomeCarriesTheStatementSeq|TestIngressStrictHookMarksAdmissionFailuresOnly' --test_output=errors`
Expected: FAIL (`StatementSeq`, `AdmissionCodeDuplicateClientSeq` undefined; nothing marked).

- [ ] **Step 3: Implement**

`pkg/storageintegrity/intake.go`, `SubmitOutcome`:

```go
	// StatementSeq is the arbiter's statement_seq on an accepted submission
	// (the WriteMeter event carries it); zero otherwise and on status-path
	// outcomes. omitempty keeps older journal records byte-identical.
	StatementSeq uint64 `json:",omitempty"`
```

`pkg/storageintegrity/arbiter_proto.go`:

```go
// AdmissionCodeDuplicateClientSeq is the arbiter's refusal of a coordinate
// that is already spent; it is the one coded terminal reject that is never
// marked unspent (spec 2026-10-09 §6.6).
var AdmissionCodeDuplicateClientSeq = pb.AdmissionCode_ADMISSION_CODE_DUPLICATE_CLIENT_SEQ.String()
```

and in `SubmitOutcomeFromSequencedAck`'s `ACCEPTED` branch return `SubmitOutcome{Category: OutcomeAccepted, Reason: reason, StatementSeq: ack.GetStatementSeq()}`.

`pkg/plugins/storageintegrity/plugin.go`, `OnQueryInputCompleteStrict`:

```go
	admission, err := p.admissionFromState(ctx, state)
	if err != nil {
		// Nothing has been submitted yet: every admissionFromState refusal
		// (JWS, signer, hashes, the authorizer re-check) leaves the coordinate
		// unspent (spec 2026-10-09 §6.6 (2)).
		return chproto.MarkSeqUnspent(fmt.Errorf("storage_integrity admission incomplete for %s: %w", state.admission.StatementID, err))
	}
```

The consumer error stays `fmt.Errorf("storage_integrity admission rejected for %s: %w", …)` without marking.

`storage_integrity_ingress.go`, `ConsumeStorageIntegrityAdmission` — mark every return before `i.orch.Orchestrate`:

```go
	if i.guard != nil {
		if err := i.guard.CheckMergeHealth(adm.TableID); err != nil {
			if errors.Is(err, errStorageIntegrityMergeGuardNotAsserted) {
				return &chproto.ClientError{Code: chproto.CodeTableIsBeingRestarted,
					Message: chproto.TableActivatingMessage(adm.TableID), Err: err, KeepSession: true, SeqUnspent: true}
			}
			return chproto.MarkSeqUnspent(fmt.Errorf("storage_integrity ingress: merge health: %w", err))
		}
	}
	rec := AdmissionRecordFromPlugin(adm)
	actualMaterializer, err := sicore.SelectMaterializerKind(rec.PayloadEncoding)
	if err != nil {
		return chproto.MarkSeqUnspent(fmt.Errorf("storage_integrity ingress: %w", err))
	}
	if actualMaterializer != i.matKind {
		return chproto.MarkSeqUnspent(fmt.Errorf(
			"storage_integrity ingress: runtime requires %s materializer, payload encoding %q selects %s",
			storageIntegrityMaterializerName(i.matKind), rec.PayloadEncoding, storageIntegrityMaterializerName(actualMaterializer)))
	}
	if i.requireAdmissionSchema && adm.TableSchema == nil {
		return &chproto.ClientError{Code: chproto.CodeQueryIsProhibited,
			Message: "storage_integrity: table state is unavailable for this query", SeqUnspent: true}
	}
```

and likewise `return chproto.MarkSeqUnspent(err)` for the `partsPressureTarget` and `reservePartsPressure` errors, and `chproto.MarkSeqUnspent(fmt.Errorf(…))` for the preflight, `PutPayload` and empty-`payload_ref` returns. After `Orchestrate`, the `!res.Ack2` block becomes:

```go
	if !res.Ack2 {
		if res.Submit.AdmissionCode == sicore.AdmissionCodeSchemaNotAllowed {
			return &chproto.ClientError{Code: chproto.CodeQueryIsProhibited,
				Message:     chproto.TableNoLongerAcceptsWritesMessage(rec.TableID),
				Err:         fmt.Errorf("arbiter %s: %s", res.Submit.AdmissionCode, res.Submit.Reason),
				KeepSession: true,
				SeqUnspent:  true}
		}
		// Spec 2026-10-09 §6.6: a coded terminal reject other than
		// DUPLICATE_CLIENT_SEQ left the coordinate unspent; name the code so the
		// agent can act on it. A code-less terminal reject (a gRPC status) or
		// any other outcome stays unmarked.
		if res.Submit.Category == sicore.OutcomeTerminalReject && res.Submit.AdmissionCode != "" &&
			res.Submit.AdmissionCode != sicore.AdmissionCodeDuplicateClientSeq {
			log.Warnw("storage_integrity statement rejected by the arbiter",
				"statement_id", rec.StatementID, "code", res.Submit.AdmissionCode, "reason", res.Submit.Reason)
			return chproto.MarkSeqUnspent(fmt.Errorf("storage_integrity: statement %s rejected by the arbiter: %s", rec.StatementID, res.Submit.AdmissionCode))
		}
		return fmt.Errorf("storage_integrity ingress: statement %s did not reach ACK2 (lifecycle %s, reason %q)", rec.StatementID, res.Lifecycle, res.Reason)
	}
```

`storage_integrity_ingress.go` does not import `pkg/log` yet; add `"github.com/housegate/housegate/pkg/log"` for the warn line. The plain `Orchestrate` error path (`return fmt.Errorf("storage_integrity ingress: orchestrate %s: %w", …)` and the back-pressure conversion after `Orchestrate`) stays unmarked: a prepare, RC or submit may have happened.

- [ ] **Step 4: Run the suites**

Run: `bazel test //:housegate_test //pkg/storageintegrity:storageintegrity_test //pkg/plugins/storageintegrity:storageintegrity_test --test_output=errors`
Expected: PASS, including `TestSchemaNotAllowedReachesTheClientAsNoLongerAcceptsWrites` and `TestSubmitOutcomeJournalShapeIsUnchangedWithoutACode`.

- [ ] **Step 5: Commit**

```bash
git add storage_integrity_ingress.go storage_integrity_ingress_unspent_test.go storage_integrity_ingress_test.go pkg/storageintegrity/intake.go pkg/storageintegrity/arbiter_proto.go pkg/storageintegrity/admission_code_test.go pkg/plugins/storageintegrity/plugin.go pkg/plugins/storageintegrity/plugin_test.go BUILD.bazel
git commit -m "feat(storageintegrity): mark unspent intake refusals and name coded arbiter rejects (spec 2026-10-09 §6.6)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---
### Task 8: the `WriteMeter` extension point (`OnStatementSequenced`)

**Files:**
- Create: `pkg/storageintegrity/write_meter.go`
- Modify: `storage_integrity_ingress.go` (field, `SetWriteMeter`, call after `Orchestrate`)
- Modify: `proxy.go` (`Options.StorageIntegrityWriteMeter`), `build.go:861-868` (wire it into the built-in runtime)
- Create: `storage_integrity_write_meter_test.go`

**Interfaces:**
- Consumes: `Admission.Owner`, `Admission.Principal` (Task 4); `SubmitOutcome.StatementSeq` (Task 7).
- Produces:
  - `type sicore.SIWriteEvent struct { StatementID, Signer, Owner, Principal, TableID string; Rows, PayloadBytes, StatementSeq uint64 }` (`Rows` is filled only by the host's `OnStatementSafe`; zero here).
  - `type sicore.WriteMeter interface { OnStatementSequenced(ctx context.Context, ev SIWriteEvent) }`
  - `func (i *StorageIntegrityIngress) SetWriteMeter(m sicore.WriteMeter)`
  - `housegate.Options.StorageIntegrityWriteMeter sicore.WriteMeter`

- [ ] **Step 1: Write the failing test**

```go
// storage_integrity_write_meter_test.go
package housegate

import (
	"context"
	"testing"
	"time"

	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

type recordingMeter struct{ events chan sicore.SIWriteEvent }

func (m recordingMeter) OnStatementSequenced(_ context.Context, ev sicore.SIWriteEvent) { m.events <- ev }

type blockingMeter struct{ release chan struct{} }

func (m blockingMeter) OnStatementSequenced(context.Context, sicore.SIWriteEvent) { <-m.release }

func TestWriteMeter_SequencedEventCarriesTheOwner(t *testing.T) {
	ingress, _, submitter, _ := newBackpressureIngress(t, &fakePartsPressure{})
	submitter.outcome = sicore.SubmitOutcome{Category: sicore.OutcomeAccepted, StatementSeq: 9}
	meter := recordingMeter{events: make(chan sicore.SIWriteEvent, 1)}
	ingress.SetWriteMeter(meter)
	adm := bpAdmission()
	adm.Owner, adm.Principal = "0x00000000000000000000000000000000000000c2", "0x00000000000000000000000000000000000000c2"
	if err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), adm); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	select {
	case ev := <-meter.events:
		want := sicore.SIWriteEvent{
			StatementID: adm.StatementID, Signer: adm.Signer, Owner: adm.Owner, Principal: adm.Principal,
			TableID: adm.TableID, PayloadBytes: uint64(len(adm.Payload.Bytes)), StatementSeq: 9,
		}
		if ev != want {
			t.Fatalf("event = %+v, want %+v", ev, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no OnStatementSequenced event")
	}
}

func TestWriteMeter_NotCalledWithoutAcceptedSubmission(t *testing.T) {
	ingress, _, submitter, _ := newBackpressureIngress(t, &fakePartsPressure{})
	submitter.outcome = sicore.SubmitOutcome{Category: sicore.OutcomeTerminalReject, AdmissionCode: "ADMISSION_CODE_MALFORMED"}
	meter := recordingMeter{events: make(chan sicore.SIWriteEvent, 1)}
	ingress.SetWriteMeter(meter)
	_ = ingress.ConsumeStorageIntegrityAdmission(context.Background(), bpAdmission())
	select {
	case ev := <-meter.events:
		t.Fatalf("unexpected event %+v for a rejected statement", ev)
	case <-time.After(200 * time.Millisecond):
	}
}

// The meter is best-effort and asynchronous: a stuck meter never blocks or
// fails the write (spec 2026-10-09 §6.9).
func TestWriteMeter_NeverBlocksTheWrite(t *testing.T) {
	ingress, _, _, _ := newBackpressureIngress(t, &fakePartsPressure{})
	meter := blockingMeter{release: make(chan struct{})}
	defer close(meter.release)
	ingress.SetWriteMeter(meter)
	done := make(chan error, 1)
	go func() { done <- ingress.ConsumeStorageIntegrityAdmission(context.Background(), bpAdmission()) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Consume: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a blocked meter blocked the write")
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `bazel test //:housegate_test --test_filter='TestWriteMeter_' --test_output=errors`
Expected: FAIL to compile (`SetWriteMeter`, `sicore.SIWriteEvent` undefined).

- [ ] **Step 3: Implement**

```go
// pkg/storageintegrity/write_meter.go
package storageintegrity

import "context"

// SIWriteEvent describes one storage-integrity write for billing (spec
// 2026-10-09 §6.9). Signer is the accountable writer bound into the
// statement; Owner is the validated operator relation (empty when the signer
// writes for itself) and Principal is Owner, else Signer. Rows is filled only
// by the host's promotion-time OnStatementSafe event.
type SIWriteEvent struct {
	StatementID  string
	Signer       string
	Owner        string
	Principal    string
	TableID      string
	Rows         uint64
	PayloadBytes uint64
	StatementSeq uint64
}

// WriteMeter is the billing extension point; nothing in HouseGate bills
// through it yet (spec D4, R11). OnStatementSequenced is called after the
// arbiter accepted a submission, asynchronously and best-effort: it never
// blocks or fails the write, and an idempotent ACK2 replay of the same
// statement may call it again, so implementations dedupe on StatementID.
type WriteMeter interface {
	OnStatementSequenced(ctx context.Context, ev SIWriteEvent)
}
```

In `storage_integrity_ingress.go` add the field `meter sicore.WriteMeter` to `StorageIntegrityIngress` and:

```go
// SetWriteMeter installs the optional billing hook (spec 2026-10-09 §6.9).
func (i *StorageIntegrityIngress) SetWriteMeter(m sicore.WriteMeter) {
	i.meter = m
}

func (i *StorageIntegrityIngress) meterSequenced(ctx context.Context, adm siplugin.Admission, statementSeq uint64) {
	meter := i.meter
	if meter == nil {
		return
	}
	ev := sicore.SIWriteEvent{
		StatementID:  adm.StatementID,
		Signer:       adm.Signer,
		Owner:        adm.Owner,
		Principal:    adm.Principal,
		TableID:      adm.TableID,
		PayloadBytes: uint64(len(adm.Payload.Bytes)),
		StatementSeq: statementSeq,
	}
	ctx = context.WithoutCancel(ctx)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Warnw("storage_integrity write meter panicked", "statement_id", ev.StatementID, "panic", r)
			}
		}()
		meter.OnStatementSequenced(ctx, ev)
	}()
}
```

and, in `ConsumeStorageIntegrityAdmission`, immediately after `res, err := i.orch.Orchestrate(ctx, rec)`:

```go
	if res.Submit.Category == sicore.OutcomeAccepted {
		// Sequenced means spent; metering follows the arbiter, not ACK2.
		i.meterSequenced(ctx, adm, res.Submit.StatementSeq)
	}
```

In `proxy.go` add to `Options` after `StorageIntegrityTableState`:

```go
	// StorageIntegrityWriteMeter optionally receives an OnStatementSequenced
	// event for every storage-integrity statement the built-in runtime got
	// accepted by the arbiter (spec 2026-10-09 §6.9). Best-effort and
	// asynchronous; nil disables it.
	StorageIntegrityWriteMeter sicore.WriteMeter
```

`proxy.go` imports only the plugin package (`github.com/housegate/housegate/pkg/plugins/storageintegrity`, unaliased); add `sicore "github.com/housegate/housegate/pkg/storageintegrity"`. The field stays optional: sentio-node leaves it nil in Phase A (Plan A2). In `build.go` right after `consumer, guard, err := buildStorageIntegrityRuntimeConsumer(...)` succeeds:

```go
			if !isNilInterface(opts.StorageIntegrityWriteMeter) {
				consumer.SetWriteMeter(opts.StorageIntegrityWriteMeter)
			}
```

- [ ] **Step 4: Run the root suite**

Run: `bazel run //:gazelle && bazel test //:housegate_test --test_output=errors`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/storageintegrity/write_meter.go storage_integrity_ingress.go storage_integrity_write_meter_test.go proxy.go build.go BUILD.bazel pkg/storageintegrity/BUILD.bazel
git commit -m "feat(storageintegrity): WriteMeter.OnStatementSequenced billing extension point (spec 2026-10-09 §6.9)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---
### Task 9: `sipeerguard` — refuse peer reads of governed ordinary physical tables

**Files:**
- Modify: `pkg/plugins/sireserved/plugin.go:132` (export `isObjectCarrierName` as `IsObjectCarrierName`; update its one caller)
- Create: `pkg/plugins/sipeerguard/plugin.go`, `pkg/plugins/sipeerguard/plugin_test.go`
- Modify: `build.go` (register after the `sireserved` block at `:681-687`)
- Test: `build_test.go`

**Interfaces:**
- Consumes: `sqlsurface.ScanWith`, `sqlsurface.Token*`; `sitable.TableState.Current().Lookup`; `chsession.SessionStateSnapshot.IsPeerTrusted/IsForwardedFromPeer`.
- Produces: `sireserved.IsObjectCarrierName(name string) bool`; `type sipeerguard.Plugin struct { PhysicalDatabase string; TableState sitable.TableState }` implementing `plugin.QueryPlugin`, `plugin.PeerTrustAware` (true), `plugin.StrictQueryDecodePlugin` (true); metric `storage_integrity_peer_guard_refusals_total`.

- [ ] **Step 1: Write the failing test**

```go
// pkg/plugins/sipeerguard/plugin_test.go
package sipeerguard

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/plugin"
	"github.com/housegate/housegate/pkg/sitable"
)

func guardSession(t *testing.T, peer, forwarded bool) chsession.Session {
	t.Helper()
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	sess := chsession.New(1, client)
	if peer {
		sess.State().SetPeerTrustForwarded("10.0.0.9:9000", forwarded)
	}
	return sess
}

func guardQuery(sess chsession.Session, sql string) *plugin.QueryContext {
	return &plugin.QueryContext{Session: sess, OriginalSQL: sql, Query: &chproto.Query{Body: sql}}
}

func newGuard(status sitable.Status) *Plugin {
	return &Plugin{
		PhysicalDatabase: "devnet2",
		// Unrecorded tables default to Ordinary here so each case controls the
		// one table it names.
		TableState: sitable.NewFake(sitable.Ordinary, sitable.Table{ID: "devnet101.swap_new106", Status: status}),
	}
}

var governedForms = map[string]string{
	"measured secondary query":   "SELECT count() AS `count()` FROM `devnet2`.`devnet101.swap_new106` AS `__table1`",
	"bare physical database":     "SELECT `__table1`.`value` AS `value` FROM devnet2.`devnet101.swap_new106` AS `__table1`",
	"double-quoted":              `SELECT * FROM "devnet2"."devnet101.swap_new106"`,
	"standalone quoted":          "SELECT * FROM `devnet101.swap_new106`",
	"remote carrier literal":     "SELECT count() FROM remote('localhost:33001', 'devnet2', 'devnet101.swap_new106')",
	"comment before qualifier":   "SELECT 1 /* x */ FROM `devnet2` /* y */ . `devnet101.swap_new106`",
	"heredoc does not hide it":   "SELECT $$x$$, * FROM `devnet2`.`devnet101.swap_new106`",
	"IN subquery":                "SELECT 1 WHERE 1 IN (SELECT value FROM `devnet2`.`devnet101.swap_new106`)",
}

func TestGuardRefusesGovernedTablesOnPeerSessions(t *testing.T) {
	for _, status := range []sitable.Status{sitable.Pending, sitable.Refused, sitable.Active, sitable.Gone} {
		for name, sql := range governedForms {
			t.Run(status.String()+"/"+name, func(t *testing.T) {
				err := newGuard(status).OnQuery(context.Background(), guardQuery(guardSession(t, true, false), sql))
				var clientErr *chproto.ClientError
				if !errors.As(err, &clientErr) || clientErr.Code != chproto.CodeQueryIsProhibited {
					t.Fatalf("err = %v, want 392", err)
				}
				want := "storage_integrity: table devnet101.swap_new106 is governed by storage integrity and must be read through its host indexer; connect with --database devnet101 or USE devnet101"
				if clientErr.Message != want {
					t.Fatalf("message = %q, want %q", clientErr.Message, want)
				}
			})
		}
	}
}

func TestGuardPassesOrdinaryForwardedAndNonPeerSessions(t *testing.T) {
	sql := governedForms["measured secondary query"]
	if err := newGuard(sitable.Ordinary).OnQuery(context.Background(), guardQuery(guardSession(t, true, false), sql)); err != nil {
		t.Fatalf("Ordinary table refused: %v", err)
	}
	if err := newGuard(sitable.Active).OnQuery(context.Background(), guardQuery(guardSession(t, true, true), sql)); err != nil {
		t.Fatalf("forwarded-from-peer session refused (its host runs the full chain): %v", err)
	}
	if err := newGuard(sitable.Active).OnQuery(context.Background(), guardQuery(guardSession(t, false, false), sql)); err != nil {
		t.Fatalf("non-peer session refused: %v", err)
	}
}

func TestGuardIgnoresNonCandidates(t *testing.T) {
	g := newGuard(sitable.Active)
	sess := guardSession(t, true, false)
	for _, sql := range []string{
		"SELECT * FROM `other`.`devnet101.swap_new106`",                    // a different database qualifier
		"SELECT 'devnet101.swap_new106'",                                    // a literal outside a carrier
		"SELECT * FROM remote('10.0.0.1:9000', 'devnet2', 'plain')",         // address argument skipped; no dot in the table
		"-- `devnet2`.`devnet101.swap_new106`\nSELECT 1",                    // inside a comment
	} {
		if err := g.OnQuery(context.Background(), guardQuery(sess, sql)); err != nil {
			t.Fatalf("%q refused: %v", sql, err)
		}
	}
}

func TestGuardRefusesUndecodableQuotedIdentifier(t *testing.T) {
	err := newGuard(sitable.Ordinary).OnQuery(context.Background(), guardQuery(guardSession(t, true, false), "SELECT * FROM `devnet2`.`devnet101\\x2eswap_new106`"))
	var clientErr *chproto.ClientError
	if !errors.As(err, &clientErr) || clientErr.Code != chproto.CodeQueryIsProhibited || !strings.Contains(clientErr.Message, "storage_integrity") {
		t.Fatalf("err = %v, want a 392 refusal of an undecodable identifier", err)
	}
}

func TestGuardMarkers(t *testing.T) {
	p := &Plugin{}
	if !p.RunOnPeerTrust() || !p.RejectUndecodableQuery() {
		t.Fatal("the guard must run on peer-trusted sessions and refuse undecodable queries")
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `bazel run //:gazelle && bazel test //pkg/plugins/sipeerguard:sipeerguard_test --test_output=errors`
Expected: FAIL to compile (package has no `Plugin`).

- [ ] **Step 3: Implement**

In `pkg/plugins/sireserved/plugin.go` rename `isObjectCarrierName` to `IsObjectCarrierName` (doc: "IsObjectCarrierName reports whether name is a table-function or table-engine callable whose arguments can name a ClickHouse database or table. Shared with sipeerguard.") and update `objectCarrierCallable`.

```go
// pkg/plugins/sipeerguard/plugin.go

// Package sipeerguard refuses, on the storage-integrity host, a peer-trusted
// statement that reads the ordinary physical table of a governed table (spec
// 2026-10-09 §6.3, D19). Another indexer's rewriter turns a read of an SI
// table into a remote() whose secondary query names that ordinary table; it
// arrives peer-trusted and non-forwarded, bypasses rewrite by design, and
// would silently read 0 rows instead of the hg_safe/hg_unsafe data.
package sipeerguard

import (
	"context"
	"fmt"
	"strings"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/plugin"
	"github.com/housegate/housegate/pkg/plugins/sireserved"
	"github.com/housegate/housegate/pkg/sitable"
	"github.com/housegate/housegate/pkg/sqlsurface"
)

var refusals = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "storage_integrity_peer_guard_refusals_total",
	Help: "Peer-trusted statements refused because they name the ordinary physical table of a governed storage-integrity table.",
})

func init() { prometheus.MustRegister(refusals) }

// Plugin is the host guard. PhysicalDatabase is rewriter.physical_database;
// TableState is the host's storage-integrity table state.
type Plugin struct {
	PhysicalDatabase string
	TableState       sitable.TableState
}

// OnQuery acts only on peer-trusted, non-forwarded sessions.
func (p *Plugin) OnQuery(_ context.Context, qctx *plugin.QueryContext) error {
	if p == nil || p.TableState == nil || p.PhysicalDatabase == "" || qctx == nil || qctx.Session == nil || qctx.Query == nil {
		return nil
	}
	snap := qctx.Session.State().Snapshot()
	if !snap.IsPeerTrusted || snap.IsForwardedFromPeer {
		return nil
	}
	surfaces, err := sqlsurface.ScanWith(qctx.Query.Body, sqlsurface.Options{DecodeStringEscapes: true})
	if err != nil {
		refusals.Inc()
		return &chproto.ClientError{Code: chproto.CodeQueryIsProhibited,
			Message: "storage_integrity: a peer statement that cannot be checked for governed tables is refused: " + err.Error()}
	}
	tables := p.TableState.Current()
	for _, candidate := range candidates(surfaces.Tokens, p.PhysicalDatabase) {
		for _, split := range splits(candidate) {
			if table := tables.Lookup(split[0], split[1]); table.Status != sitable.Ordinary {
				refusals.Inc()
				return &chproto.ClientError{Code: chproto.CodeQueryIsProhibited, Message: fmt.Sprintf(
					"storage_integrity: table %s.%s is governed by storage integrity and must be read through its host indexer; connect with --database %s or USE %s",
					split[0], split[1], split[0], split[0])}
			}
		}
	}
	return nil
}

// candidates returns every table text the statement may read as an ordinary
// physical table: (1) <physical>.<quoted> in any quoting of the database;
// (2) a standalone quoted identifier containing '.'; (3) a string literal
// containing '.' among a carrier's arguments, skipping the first (an address
// or cluster name, plan decision P9).
func candidates(tokens []sqlsurface.Token, physical string) []string {
	isPunct := func(i int, text string) bool {
		return i >= 0 && i < len(tokens) && tokens[i].Kind == sqlsurface.TokenPunct && tokens[i].Text == text
	}
	var out []string
	for i, tok := range tokens {
		if (tok.Kind == sqlsurface.TokenWord || tok.Kind == sqlsurface.TokenQuoted) && tok.Text == physical &&
			isPunct(i+1, ".") && i+2 < len(tokens) && tokens[i+2].Kind == sqlsurface.TokenQuoted {
			out = append(out, tokens[i+2].Text)
		}
		if tok.Kind == sqlsurface.TokenQuoted && strings.Contains(tok.Text, ".") && !isPunct(i-1, ".") && !isPunct(i+1, ".") {
			out = append(out, tok.Text)
		}
		if tok.Kind == sqlsurface.TokenWord && sireserved.IsObjectCarrierName(tok.Text) && isPunct(i+1, "(") {
			out = append(out, carrierLiterals(tokens, i+1)...)
		}
	}
	return out
}

func carrierLiterals(tokens []sqlsurface.Token, open int) []string {
	var out []string
	depth, argument := 0, 0
	for i := open; i < len(tokens); i++ {
		tok := tokens[i]
		if tok.Kind == sqlsurface.TokenPunct {
			switch tok.Text {
			case "(":
				depth++
			case ")":
				depth--
				if depth == 0 {
					return out
				}
			case ",":
				if depth == 1 {
					argument++
				}
			}
			continue
		}
		if tok.Kind == sqlsurface.TokenString && argument > 0 && strings.Contains(tok.Text, ".") {
			out = append(out, tok.Text)
		}
	}
	return out
}

// splits returns the (database, table) readings of a physical table text: at
// the first and at the last '.', deduplicated (plan decision P9).
func splits(text string) [][2]string {
	var out [][2]string
	add := func(i int) {
		if i <= 0 || i >= len(text)-1 {
			return
		}
		pair := [2]string{text[:i], text[i+1:]}
		for _, seen := range out {
			if seen == pair {
				return
			}
		}
		out = append(out, pair)
	}
	add(strings.IndexByte(text, '.'))
	add(strings.LastIndexByte(text, '.'))
	return out
}

// RunOnPeerTrust keeps the guard on peer-trusted sessions: they are its job.
func (*Plugin) RunOnPeerTrust() bool { return true }

// RejectUndecodableQuery refuses a Query Relay cannot decode instead of
// raw-splicing it past the guard.
func (*Plugin) RejectUndecodableQuery() bool { return true }

var (
	_ plugin.QueryPlugin             = (*Plugin)(nil)
	_ plugin.PeerTrustAware          = (*Plugin)(nil)
	_ plugin.StrictQueryDecodePlugin = (*Plugin)(nil)
)
```

The `StrictQueryDecodePlugin` chain filter applies peer filters too, so `RejectUndecodableQuery` on this plugin fires only where `sireserved`'s already does or on a server whose session reaches this plugin; both are fail-closed.

- [ ] **Step 4: Run the plugin tests**

Run: `bazel run //:gazelle && bazel test //pkg/plugins/sipeerguard:sipeerguard_test //pkg/plugins/sireserved:sireserved_test --test_output=errors`
Expected: PASS.

- [ ] **Step 5: Wire `buildServer` and test the wiring**

In `build.go`, right after the `if siOptions.Enabled { queryPlugins = append(queryPlugins, &sireserved.Plugin{…}) … }` block:

```go
	// Spec 2026-10-09 §6.3: on the SI host, a peer-trusted read that names the
	// ordinary physical table of a governed table fails loudly instead of
	// reading the empty table. Without a physical database no such table can
	// exist.
	if siOptions.Enabled {
		if physical := cfg.Rewriter.PhysicalDatabase; physical != "" {
			queryPlugins = append(queryPlugins, &sipeerguard.Plugin{PhysicalDatabase: physical, TableState: siState})
			log.Infow("storage-integrity peer guard enabled", "physical_database", physical)
		} else {
			log.Info("storage-integrity peer guard off: rewriter.physical_database is empty, so no ordinary physical SI table can exist")
		}
	}
```

Append to `build_test.go` (same helpers as `TestBuildServer_StorageIntegrityReservedGuardWiring`):

```go
func TestBuildServer_StorageIntegrityPeerGuardWiring(t *testing.T) {
	peerGuards := func(bs *builtServer) (count, guardIndex, reservedIndex int) {
		guardIndex, reservedIndex = -1, -1
		for i, candidate := range requireExternalChain(t, bs).QueryPlugins {
			switch candidate.(type) {
			case *sipeerguard.Plugin:
				count++
				guardIndex = i
			case *sireserved.Plugin:
				reservedIndex = i
			}
		}
		return count, guardIndex, reservedIndex
	}
	for name, tc := range map[string]struct {
		tables   []string
		physical string
		want     int
	}{
		"SI with physical database": {tables: []string{"tenant.events"}, physical: "phys", want: 1},
		"SI without physical":       {tables: []string{"tenant.events"}, physical: "", want: 0},
		"no SI":                     {physical: "phys", want: 0},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := minimalServerCfg(t)
			cfg.StorageIntegrity.Tables = tc.tables
			cfg.Rewriter.PhysicalDatabase = tc.physical
			var factory rewriter.Factory = stubRewriterFactory{}
			if len(tc.tables) > 0 {
				factory = siProbeStubRewriterFactory{}
			}
			bs, err := buildServer(Options{Config: cfg, NetworkState: network.NewInMemoryNetworkState(), Rewriter: factory}, nil)
			if err != nil {
				t.Fatalf("buildServer: %v", err)
			}
			defer bs.teardown()
			count, guardIndex, reservedIndex := peerGuards(bs)
			if count != tc.want {
				t.Fatalf("peer guards = %d, want %d", count, tc.want)
			}
			if tc.want == 1 && guardIndex < reservedIndex {
				t.Fatalf("peer guard index %d before reserved guard %d", guardIndex, reservedIndex)
			}
		})
	}
}
```

Run: `bazel test //:housegate_test --test_filter='TestBuildServer_StorageIntegrity(PeerGuard|ReservedGuard)Wiring' --test_output=errors`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add pkg/plugins/sipeerguard pkg/plugins/sireserved/plugin.go build.go build_test.go BUILD.bazel
git commit -m "feat(sipeerguard): refuse peer-trusted reads of governed ordinary physical tables (spec 2026-10-09 §6.3)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---
### Task 10: `Session.SwitchUpstream` — a neutral upstream switch

**Files:**
- Modify: `pkg/chsession/session.go` (interface doc + method; `handshakeNewUpstream` returns the upstream hello instead of storing it; `RebindToPeer` / `RebindToLocal` store it themselves)
- Modify: `pkg/chsession/errors.go` (`ErrUpstreamRevisionTooLow`)
- Test: `pkg/chsession/state_test.go`
- Modify (add a `SwitchUpstream` stub to every `chsession.Session` fake): `build_test.go:2092`, `pkg/proxy/commitgate_e2e_test.go:76`, `pkg/plugins/commitgate/plugin_test.go:87`, `pkg/plugins/sessionstate/tracker_test.go:93`, `pkg/plugins/storageintegrity/plugin_test.go:1424`, `pkg/plugins/sistatement/plugin_test.go:50`, `pkg/plugin/chain_test.go:39`

**Interfaces:**
- Consumes: nothing new.
- Produces: `Session.SwitchUpstream(ctx context.Context, newUp *chproto.Codec, hello *chproto.ClientHello) error` — handshake (ClientHello → ServerHello → non-chunked addendum), refuse with `ErrUpstreamRevisionTooLow` when the negotiated revision is below `SessionState.ClientRevision`, otherwise store the hello and swap. It writes no peer/forward fields and replays nothing. On any error it closes `newUp` and keeps the old upstream; it takes ownership of `newUp` in every case.

- [ ] **Step 1: Write the failing tests**

Append to `pkg/chsession/state_test.go`:

```go
// switchServer answers one ClientHello with a ServerHello at rev and then
// drains everything else; it reports the hello it saw.
func switchServer(t *testing.T, conn net.Conn, rev int, hellos chan<- *chproto.ClientHello) {
	t.Helper()
	go func() {
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		codec := chproto.NewCodec(conn, chproto.DirFromClient)
		pkt, err := codec.ReadPacket(uint64(chproto.ClientHelloCode))
		if err != nil {
			hellos <- nil
			return
		}
		hellos <- pkt.Decoded.(*chproto.ClientHello)
		srv := &proto.ServerHello{Name: "switch-target", Major: 24, Minor: 1, Revision: rev}
		var buf proto.Buffer
		srv.EncodeAware(&buf, rev)
		if _, err := conn.Write(buf.Buf); err != nil {
			return
		}
		_, _ = io.Copy(io.Discard, conn)
	}()
}

func boundSession(t *testing.T) (Session, *chproto.Codec) {
	t.Helper()
	clientConn, _ := net.Pipe()
	t.Cleanup(func() { clientConn.Close() })
	sess := New(1, clientConn)
	placeholder, _ := net.Pipe()
	t.Cleanup(func() { placeholder.Close() })
	old := chproto.NewCodec(placeholder, chproto.DirToUpstream)
	if err := sess.BindUpstream(context.Background(), old); err != nil {
		t.Fatal(err)
	}
	return sess, old
}

func TestSession_SwitchUpstreamReplaysHelloVerbatim(t *testing.T) {
	const rev = chproto.RevisionMinAddendum
	sess, _ := boundSession(t)
	sess.State().ClientRevision = rev
	sess.State().SetForwarding(false)
	serverConn, clientConn := net.Pipe()
	hellos := make(chan *chproto.ClientHello, 1)
	switchServer(t, serverConn, rev, hellos)
	newUp := chproto.NewCodec(clientConn, chproto.DirToUpstream)
	hello := &chproto.ClientHello{Name: "clickhouse-client", Major: 26, Minor: 3, ProtocolVersion: rev, User: "default", Password: "pw", Database: "devuser1"}

	if err := sess.SwitchUpstream(context.Background(), newUp, hello); err != nil {
		t.Fatalf("SwitchUpstream: %v", err)
	}
	got := <-hellos
	if got == nil || got.User != "default" || got.Password != "pw" || got.Database != "devuser1" || got.Name != "clickhouse-client" {
		t.Fatalf("upstream saw hello %+v, want the verbatim replay", got)
	}
	if sess.Upstream() != newUp || newUp.Revision() != rev {
		t.Fatalf("upstream not swapped (rev=%d)", newUp.Revision())
	}
	if stored := sess.State().UpstreamHello(); stored == nil || stored.Database != "devuser1" {
		t.Fatalf("stored upstream hello = %+v", stored)
	}
	snap := sess.State().Snapshot()
	if snap.IsForwarding || snap.IsPeerTrusted || snap.RouteTarget != "" || len(sess.State().PeerServerHelloRaw) != 0 {
		t.Fatalf("SwitchUpstream must not write peer/forward state: %+v", snap)
	}
}

func TestSession_SwitchUpstreamRefusesALowerRevision(t *testing.T) {
	const clientRev = chproto.RevisionMinAddendum
	sess, old := boundSession(t)
	sess.State().ClientRevision = clientRev
	sess.State().SetUpstreamHello(&chproto.ClientHello{Database: "before"})
	serverConn, clientConn := net.Pipe()
	hellos := make(chan *chproto.ClientHello, 1)
	switchServer(t, serverConn, clientRev-1, hellos)
	newUp := chproto.NewCodec(clientConn, chproto.DirToUpstream)

	err := sess.SwitchUpstream(context.Background(), newUp, &chproto.ClientHello{ProtocolVersion: clientRev, Database: "after"})
	if !errors.Is(err, ErrUpstreamRevisionTooLow) {
		t.Fatalf("err = %v, want ErrUpstreamRevisionTooLow", err)
	}
	if sess.Upstream() != old {
		t.Fatal("the old upstream must stay bound after a refusal")
	}
	if stored := sess.State().UpstreamHello(); stored == nil || stored.Database != "before" {
		t.Fatalf("stored hello changed to %+v on refusal", stored)
	}
	if _, err := clientConn.Write([]byte{0}); err == nil {
		t.Fatal("the refused connection must be closed")
	}
}

func TestSession_SwitchUpstreamClosesNewConnOnHandshakeFailure(t *testing.T) {
	sess, old := boundSession(t)
	serverConn, clientConn := net.Pipe()
	serverConn.Close() // the handshake write fails
	err := sess.SwitchUpstream(context.Background(), chproto.NewCodec(clientConn, chproto.DirToUpstream), &chproto.ClientHello{ProtocolVersion: chproto.RevisionMinAddendum})
	if err == nil || sess.Upstream() != old {
		t.Fatalf("err = %v upstream swapped = %v", err, sess.Upstream() != old)
	}
}
```

Add `"errors"` to the test imports if it is missing.

- [ ] **Step 2: Run them to verify they fail**

Run: `bazel test //pkg/chsession:chsession_test --test_filter='TestSession_SwitchUpstream' --test_output=errors`
Expected: FAIL to compile (`sess.SwitchUpstream undefined`, `ErrUpstreamRevisionTooLow undefined`).

- [ ] **Step 3: Implement**

`pkg/chsession/errors.go`:

```go
	// ErrUpstreamRevisionTooLow refuses an upstream switch whose negotiated
	// revision is below the client leg's: Relay re-frames packets between the
	// legs and cannot down-convert them.
	ErrUpstreamRevisionTooLow = errors.New("chsession: upstream revision below the client revision")
```

`pkg/chsession/session.go` — add to the `Session` interface after `RebindToLocal`:

```go
	// SwitchUpstream moves the session to another server for the agent's
	// storage-integrity upstream switch (spec 2026-10-09 §6.4, D19). hello is
	// replayed as given — the caller sets ProtocolVersion and Database — then
	// the new leg must negotiate a revision at least SessionState.ClientRevision.
	// It writes no peer or forward state and replays nothing. It owns newUp in
	// every case: on error newUp is closed and the old upstream stays bound.
	// Call it only from OnQuery, where no query is active.
	SwitchUpstream(ctx context.Context, newUp *chproto.Codec, hello *chproto.ClientHello) error
```

Change `handshakeNewUpstream` to return `(rev int, serverHelloRaw []byte, upstreamHello *chproto.ClientHello, err error)` and delete its `s.state.SetUpstreamHello(upstreamHello)` line; in `RebindToPeer` and `RebindToLocal` call `s.state.SetUpstreamHello(upstreamHello)` right after a successful handshake (same point as before, so their behaviour is unchanged). Then:

```go
// SwitchUpstream implements Session.SwitchUpstream.
func (s *sessionImpl) SwitchUpstream(_ context.Context, newUp *chproto.Codec, hello *chproto.ClientHello) error {
	closeNew := func() {
		if newUp == nil {
			return
		}
		if closer, ok := newUp.Conn().(interface{ Close() error }); ok {
			_ = closer.Close()
		}
	}
	if hello == nil {
		closeNew()
		return fmt.Errorf("%w: switch-upstream: nil hello", ErrRebindDenied)
	}
	rev, _, upstreamHello, err := s.handshakeNewUpstream(newUp, hello, "switch-upstream")
	if err != nil {
		closeNew()
		return err
	}
	if clientRev := s.state.Snapshot().ClientRevision; rev < clientRev {
		closeNew()
		return fmt.Errorf("%w: switch-upstream negotiated %d, client leg uses %d", ErrUpstreamRevisionTooLow, rev, clientRev)
	}
	s.state.SetUpstreamHello(upstreamHello)
	s.swapAndCloseOld(newUp)
	return nil
}
```

Add to each test fake listed under **Files**:

```go
func (s *fakeSession) SwitchUpstream(context.Context, *chproto.Codec, *chproto.ClientHello) error {
	return nil
}
```

(with that file's receiver type name — `buildTestSession`, `commitgateTestSession`, `stateOnlySession`, `fakeSession`). The `sistatement` fake gains a recording implementation in Task 15; a no-op is correct until then.

- [ ] **Step 4: Run every package with a fake**

Run: `bazel test //pkg/chsession:chsession_test //pkg/proxy:proxy_test //pkg/plugin:plugin_test //pkg/plugins/... //:housegate_test --test_output=errors`
Expected: PASS (`TestSession_RebindToLocal` / `RebindToPeer` unchanged).

- [ ] **Step 5: Commit**

```bash
git add pkg/chsession build_test.go pkg/proxy/commitgate_e2e_test.go pkg/plugins/commitgate/plugin_test.go pkg/plugins/sessionstate/tracker_test.go pkg/plugins/storageintegrity/plugin_test.go pkg/plugins/sistatement/plugin_test.go pkg/plugin/chain_test.go
git commit -m "feat(chsession): SwitchUpstream with a revision guard for the agent's SI upstream switch (spec 2026-10-09 D19)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---
### Task 11: the legacy `client_seq` store — durable directory, flock, free list

**Files:**
- Modify: `pkg/plugins/sistatement/seq.go`
- Create: `pkg/plugins/sistatement/seq_dir.go` (durable `mkdir -p`, shared durable write), `pkg/plugins/sistatement/seq_lock.go` (portable refusal), `pkg/plugins/sistatement/seq_lock_unix.go` (`//go:build linux || darwin`), `pkg/plugins/sistatement/seq_lock_other.go` (`//go:build !(linux || darwin)`)
- Create: `pkg/plugins/sistatement/seq_freelist_test.go`
- Modify: `pkg/plugins/sistatement/seq_test.go` (`TestSeqCounter_StartsAtOneAndPersistsAcrossReopen`, `TestSeqCounter_AdvanceToReservesSuppliedSequenceDurably` close before reopening; `TestSeqCounter_RequiresAccountAndDir`'s missing-dir case now expects creation)
- Modify: `pkg/plugins/sistatement/plugin_test.go` (`newTestPlugin` closes its counter in `t.Cleanup`; `TestPlugin_SeqSurvivesRestart` closes the first plugin's counter before the second opens)

**Interfaces:**
- Consumes: nothing new.
- Produces (package `sistatement`):
  - `OpenSeqCounter(stateDir, account string) (*SeqCounter, error)` — now creates `stateDir` (mode 0700, every new ancestor fsynced into its parent) and takes `flock(LOCK_EX|LOCK_NB)` on `<stateDir>/<account>.seq.lock` for the counter's lifetime; loads `<account>.seq` and `<account>.seq.free`.
  - `ErrSeqLocked` (another process holds the lock), `ErrSeqLockUnsupported` (any platform other than linux/darwin).
  - `(*SeqCounter) Reserve() (uint64, error)` — smallest free entry (durably removed) else `last+1` (durably persisted). `Next()` is now an alias of `Reserve`.
  - `(*SeqCounter) Release(seq uint64) (overflow bool, err error)` — durably adds an issued, not-yet-free seq to the free list; the list is capped at `MaxFreeSeqs = 64` and an overflow drops the largest entry (returned as `overflow = true`; that seq stays burned).
  - `(*SeqCounter) Close() error` — releases the lock; idempotent.
  - `ReserveSupplied` / `AdvanceTo` / `Last` / `Path` unchanged.

- [ ] **Step 1: Write the failing tests**

```go
// pkg/plugins/sistatement/seq_freelist_test.go
package sistatement

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const freeAccount = "0x00000000000000000000000000000000000000e1"

func openFree(t *testing.T, dir string) *SeqCounter {
	t.Helper()
	c, err := OpenSeqCounter(dir, freeAccount)
	if err != nil {
		t.Fatalf("OpenSeqCounter: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func reserveN(t *testing.T, c *SeqCounter, n int) []uint64 {
	t.Helper()
	out := make([]uint64, 0, n)
	for i := 0; i < n; i++ {
		seq, err := c.Reserve()
		if err != nil {
			t.Fatalf("Reserve: %v", err)
		}
		out = append(out, seq)
	}
	return out
}

func TestSeqCounter_CreatesItsDirectoryDurably(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "si", "devnet2", freeAccount)
	c := openFree(t, dir)
	info, err := os.Stat(dir)
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
		t.Fatalf("state dir = %v, %v; want a 0700 directory", info, err)
	}
	if seq, err := c.Reserve(); err != nil || seq != 1 {
		t.Fatalf("first Reserve = %d, %v", seq, err)
	}
}

func TestSeqCounter_ReleaseReusesTheSmallestFirst(t *testing.T) {
	c := openFree(t, t.TempDir())
	reserveN(t, c, 5) // 1..5
	for _, seq := range []uint64{4, 2} {
		if overflow, err := c.Release(seq); err != nil || overflow {
			t.Fatalf("Release(%d) = %v, %v", seq, overflow, err)
		}
	}
	if got := reserveN(t, c, 3); got[0] != 2 || got[1] != 4 || got[2] != 6 {
		t.Fatalf("reuse order = %v, want [2 4 6]", got)
	}
	if c.Last() != 6 {
		t.Fatalf("Last = %d, want 6", c.Last())
	}
}

func TestSeqCounter_ReleaseRefusesUnissuedAndDuplicateSeqs(t *testing.T) {
	c := openFree(t, t.TempDir())
	reserveN(t, c, 2)
	if _, err := c.Release(3); err == nil {
		t.Fatal("releasing a never-issued seq must fail")
	}
	if _, err := c.Release(0); err == nil {
		t.Fatal("releasing seq 0 must fail")
	}
	if _, err := c.Release(1); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Release(1); err == nil {
		t.Fatal("releasing a seq twice must fail")
	}
}

func TestSeqCounter_FreeListSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	c, err := OpenSeqCounter(dir, freeAccount)
	if err != nil {
		t.Fatal(err)
	}
	reserveN(t, c, 3)
	if _, err := c.Release(2); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openFree(t, dir)
	if got := reserveN(t, reopened, 2); got[0] != 2 || got[1] != 4 {
		t.Fatalf("after restart = %v, want [2 4]", got)
	}
	// The legacy .seq keeps its single-integer format for older binaries.
	b, err := os.ReadFile(filepath.Join(dir, freeAccount+".seq"))
	if err != nil || string(b) != "4\n" {
		t.Fatalf(".seq = %q, %v", b, err)
	}
}

func TestSeqCounter_FreeListCapDropsTheLargest(t *testing.T) {
	c := openFree(t, t.TempDir())
	reserveN(t, c, MaxFreeSeqs+1)
	for seq := uint64(1); seq <= MaxFreeSeqs; seq++ {
		if overflow, err := c.Release(seq); err != nil || overflow {
			t.Fatalf("Release(%d) = %v, %v", seq, overflow, err)
		}
	}
	overflow, err := c.Release(MaxFreeSeqs + 1)
	if err != nil || !overflow {
		t.Fatalf("Release past the cap = %v, %v; want overflow", overflow, err)
	}
	got := reserveN(t, c, MaxFreeSeqs+1)
	if got[0] != 1 || got[MaxFreeSeqs-1] != MaxFreeSeqs || got[MaxFreeSeqs] != MaxFreeSeqs+2 {
		t.Fatalf("after overflow the largest (%d) must stay burned: %v", MaxFreeSeqs+1, got)
	}
}

func TestSeqCounter_LockRefusesASecondOpener(t *testing.T) {
	dir := t.TempDir()
	first := openFree(t, dir)
	// flock locks belong to an open file description, so a second open in the
	// same process behaves like a second process.
	if _, err := OpenSeqCounter(dir, freeAccount); !errors.Is(err, ErrSeqLocked) {
		t.Fatalf("second opener err = %v, want ErrSeqLocked", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second := openFree(t, dir)
	if _, err := second.Reserve(); err != nil {
		t.Fatalf("after Close a new opener must work: %v", err)
	}
}

func TestSeqLockUnsupportedStubRefuses(t *testing.T) {
	_, err := lockUnsupported("/tmp/x.lock")
	if !errors.Is(err, ErrSeqLockUnsupported) || !strings.Contains(err.Error(), "/tmp/x.lock") {
		t.Fatalf("stub err = %v", err)
	}
}

// A crash after the free entry is durably removed but before the statement
// is sent only burns it: the reopened counter neither reissues it nor fails.
func TestSeqCounter_FreeRemovalIsDurableBeforeUse(t *testing.T) {
	dir := t.TempDir()
	c, err := OpenSeqCounter(dir, freeAccount)
	if err != nil {
		t.Fatal(err)
	}
	reserveN(t, c, 2)
	if _, err := c.Release(1); err != nil {
		t.Fatal(err)
	}
	if seq, _ := c.Reserve(); seq != 1 {
		t.Fatalf("Reserve = %d, want the freed 1", seq)
	}
	_ = c.Close() // "crash" before using seq 1
	reopened := openFree(t, dir)
	if seq, _ := reopened.Reserve(); seq != 3 {
		t.Fatalf("after crash Reserve = %d, want 3 (1 burned, never reissued)", seq)
	}
}
```

In `seq_test.go`: in `TestSeqCounter_StartsAtOneAndPersistsAcrossReopen` and `TestSeqCounter_AdvanceToReservesSuppliedSequenceDurably` call `c.Close()` before `OpenSeqCounter` reopens the same directory, and replace the missing-directory assertions in `TestSeqCounter_RequiresAccountAndDir` with:

```go
	missing := filepath.Join(t.TempDir(), "not-created")
	created, err := OpenSeqCounter(missing, "0xabc")
	if err != nil {
		t.Fatalf("missing state dir must be created (spec 2026-10-09 §6.4): %v", err)
	}
	_ = created.Close()
	if info, err := os.Stat(missing); err != nil || !info.IsDir() {
		t.Fatalf("state dir not created: %v", err)
	}
```

`TestSeqCounter_StateDirPermissionErrorPreservesSequence` keeps every assertion except `"must already exist"` (still absent) — the permission diagnostic must survive the new creation path.

- [ ] **Step 2: Run them to verify they fail**

Run: `bazel run //:gazelle && bazel test //pkg/plugins/sistatement:sistatement_test --test_filter='TestSeqCounter|TestSeqLock' --test_output=errors`
Expected: FAIL to compile (`Reserve`, `Release`, `MaxFreeSeqs`, `ErrSeqLocked`, `lockUnsupported` undefined).

- [ ] **Step 3: Implement the lock**

```go
// pkg/plugins/sistatement/seq_lock.go
package sistatement

import (
	"errors"
	"fmt"
)

// ErrSeqLocked means another agent process holds the client_seq lock for the
// same state directory and account (spec 2026-10-09 D15): two processes must
// never draw from one counter.
var ErrSeqLocked = errors.New("sistatement: another housegate agent holds the client_seq lock")

// ErrSeqLockUnsupported refuses a client_seq store on a platform without
// flock (spec 2026-10-09 D15, U9).
var ErrSeqLockUnsupported = errors.New("sistatement: client_seq locking is not supported on this platform")

func lockUnsupported(path string) (func() error, error) {
	return nil, fmt.Errorf("%w: %s", ErrSeqLockUnsupported, path)
}
```

```go
// pkg/plugins/sistatement/seq_lock_unix.go
//go:build linux || darwin

package sistatement

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

// lockFile takes an exclusive, non-blocking flock on path for the caller's
// lifetime. The kernel releases it if the process dies, so a crashed agent
// never wedges its successor.
func lockFile(path string) (func() error, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("sistatement: open lock %s: %w", path, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("%w: %s", ErrSeqLocked, path)
		}
		return nil, fmt.Errorf("sistatement: lock %s: %w", path, err)
	}
	return func() error {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		return f.Close()
	}, nil
}
```

```go
// pkg/plugins/sistatement/seq_lock_other.go
//go:build !(linux || darwin)

package sistatement

func lockFile(path string) (func() error, error) { return lockUnsupported(path) }
```

- [ ] **Step 4: Implement the durable directory and writes**

```go
// pkg/plugins/sistatement/seq_dir.go
package sistatement

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// mkdirAllDurable creates dir and its missing ancestors with perm and fsyncs
// every new entry into its parent, so a crash cannot lose a directory a
// durable seq file was already renamed into (spec 2026-10-09 §6.4).
func mkdirAllDurable(dir string, perm fs.FileMode, openDir func(string) (seqDir, error)) error {
	dir = filepath.Clean(dir)
	var missing []string
	for p := dir; ; {
		info, err := os.Stat(p)
		if err == nil {
			if !info.IsDir() {
				return fmt.Errorf("sistatement: state dir %s is not a directory", p)
			}
			break
		}
		if errors.Is(err, fs.ErrPermission) {
			return fmt.Errorf("sistatement: cannot access state dir %s; check ownership and search permissions on parent directories for the agent user: %w", p, err)
		}
		if !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("sistatement: stat state dir %s: %w", p, err)
		}
		missing = append(missing, p)
		parent := filepath.Dir(p)
		if parent == p {
			break
		}
		p = parent
	}
	for i := len(missing) - 1; i >= 0; i-- {
		if err := os.Mkdir(missing[i], perm); err != nil && !errors.Is(err, fs.ErrExist) {
			return fmt.Errorf("sistatement: create state dir %s: %w", missing[i], err)
		}
		if err := syncDir(openDir, filepath.Dir(missing[i])); err != nil {
			return err
		}
	}
	return nil
}

func syncDir(openDir func(string) (seqDir, error), path string) error {
	dir, err := openDir(path)
	if err != nil {
		return fmt.Errorf("sistatement: open state dir %s for fsync: %w", path, err)
	}
	var errs []error
	if err := dir.Sync(); err != nil {
		errs = append(errs, fmt.Errorf("fsync state dir %s: %w", path, err))
	}
	if err := dir.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close state dir %s: %w", path, err))
	}
	return errors.Join(errs...)
}

// writeDurable replaces path with data: temp file, fsync, rename, directory
// fsync — the discipline the seq file has always used.
func writeDurable(openDir func(string) (seqDir, error), path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("sistatement: open %s: %w", tmp, err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return fmt.Errorf("sistatement: write %s: %w", tmp, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("sistatement: fsync %s: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("sistatement: close %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("sistatement: rename %s: %w", tmp, err)
	}
	return syncDir(openDir, filepath.Dir(path))
}
```

- [ ] **Step 5: Rework `seq.go`**

Replace the type comment's last two lines ("One process per (state_dir, account) — sharing a key across agents is out of scope.") with "One process per (state_dir, account) is enforced by an exclusive flock on <account>.seq.lock (spec 2026-10-09 D15).", and change the struct and functions:

```go
// MaxFreeSeqs caps the durable free list (spec 2026-10-09 §6.5).
const MaxFreeSeqs = 64

type SeqCounter struct {
	path     string
	freePath string
	openDir  func(string) (seqDir, error)
	unlock   func() error
	mu       sync.Mutex
	last     uint64
	free     []uint64 // ascending, each <= last
}

type freeListFile struct {
	Version int      `json:"version"`
	Free    []uint64 `json:"free"`
}

// OpenSeqCounter creates stateDir durably when missing (mode 0700), locks
// <account>.seq.lock, and loads <account>.seq (0 when absent) and the free
// list <account>.seq.free (empty when absent). Close releases the lock.
func OpenSeqCounter(stateDir, account string) (*SeqCounter, error) {
	stateDir = strings.TrimSpace(stateDir)
	account = strings.ToLower(strings.TrimSpace(account))
	if stateDir == "" {
		return nil, errors.New("sistatement: state dir is required")
	}
	if account == "" {
		return nil, errors.New("sistatement: account is required")
	}
	if err := mkdirAllDurable(stateDir, 0o700, openSeqDir); err != nil {
		return nil, err
	}
	base := filepath.Join(stateDir, account+".seq")
	unlock, err := lockFile(base + ".lock")
	if err != nil {
		return nil, err
	}
	c := &SeqCounter{path: base, freePath: base + ".free", openDir: openSeqDir, unlock: unlock}
	if err := c.load(); err != nil {
		_ = unlock()
		return nil, err
	}
	return c, nil
}

func (c *SeqCounter) load() error {
	b, err := os.ReadFile(c.path)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		c.last = 0
	case err != nil:
		return fmt.Errorf("sistatement: read %s: %w", c.path, err)
	default:
		last, perr := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
		if perr != nil {
			return fmt.Errorf("sistatement: corrupt seq file %s: %w", c.path, perr)
		}
		c.last = last
	}
	fb, err := os.ReadFile(c.freePath)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("sistatement: read %s: %w", c.freePath, err)
	}
	var file freeListFile
	if err := json.Unmarshal(fb, &file); err != nil || file.Version != 1 {
		return fmt.Errorf("sistatement: corrupt free list %s (version %d): %v", c.freePath, file.Version, err)
	}
	for i, seq := range file.Free {
		if seq == 0 || seq > c.last || (i > 0 && seq <= file.Free[i-1]) {
			return fmt.Errorf("sistatement: corrupt free list %s: entry %d is not an ascending issued seq", c.freePath, seq)
		}
	}
	c.free = file.Free
	return nil
}

// Close releases the lock. It is idempotent.
func (c *SeqCounter) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.unlock == nil {
		return nil
	}
	err := c.unlock()
	c.unlock = nil
	return err
}

func (c *SeqCounter) persistFreeLocked(free []uint64) error {
	data, err := json.Marshal(freeListFile{Version: 1, Free: free})
	if err != nil {
		return err
	}
	if err := writeDurable(c.openDir, c.freePath, data); err != nil {
		return fmt.Errorf("sistatement: persist free list: %w", err)
	}
	c.free = free
	return nil
}

// Reserve returns the smallest free seq, removed durably first, else issues
// last+1 durably. Ascending reuse is always a sequential append or a
// start-edge fill at the arbiter, which never opens a gap range.
func (c *SeqCounter) Reserve() (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.free) > 0 {
		seq := c.free[0]
		if err := c.persistFreeLocked(append([]uint64(nil), c.free[1:]...)); err != nil {
			return 0, err
		}
		return seq, nil
	}
	if c.last == ^uint64(0) {
		return 0, ErrClientSeqExhausted
	}
	next := c.last + 1
	if err := c.persistLocked(next); err != nil {
		return 0, err
	}
	return next, nil
}

// Next is Reserve; kept for existing callers.
func (c *SeqCounter) Next() (uint64, error) { return c.Reserve() }

// Release returns a provably unspent seq to the free list (spec 2026-10-09
// D16). The list is capped at MaxFreeSeqs; past it the largest entry is
// dropped and stays burned (overflow = true).
func (c *SeqCounter) Release(seq uint64) (overflow bool, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if seq == 0 || seq > c.last {
		return false, fmt.Errorf("sistatement: release of never-issued client_seq %d (last %d)", seq, c.last)
	}
	idx := sort.Search(len(c.free), func(i int) bool { return c.free[i] >= seq })
	if idx < len(c.free) && c.free[idx] == seq {
		return false, fmt.Errorf("sistatement: client_seq %d is already free", seq)
	}
	next := make([]uint64, 0, len(c.free)+1)
	next = append(next, c.free[:idx]...)
	next = append(next, seq)
	next = append(next, c.free[idx:]...)
	if len(next) > MaxFreeSeqs {
		next = next[:MaxFreeSeqs]
		overflow = true
	}
	return overflow, c.persistFreeLocked(next)
}
```

`persistLocked` now delegates the file write to `writeDurable(c.openDir, c.path, []byte(strconv.FormatUint(next, 10)+"\n"))` and keeps its contract of updating `c.last` only after success (the existing `TestSeqCounter_DirectoryDurabilityFailures*` tests pin this). Imports gain `encoding/json` and `sort`.

`plugin_test.go` — in `newTestPlugin` add `t.Cleanup(func() { _ = seq.Close() })` after opening; in `TestPlugin_SeqSurvivesRestart` close the first counter before `newTestPlugin` reopens the directory (keep a handle: change `newTestPlugin` to also return nothing new — instead call `p.seq.Close()` on the first plugin, which is in-package).

- [ ] **Step 6: Run the package**

Run: `bazel test //pkg/plugins/sistatement:sistatement_test --test_output=errors && GOOS=freebsd GOARCH=amd64 go vet ./pkg/plugins/sistatement/`
Expected: PASS; the freebsd vet compiles the refusing stub (`seq_lock_other.go`), proving a platform outside the two release targets builds and refuses instead of failing to compile (the same check Plan B Task 15 runs for the lane pool).

- [ ] **Step 7: Commit**

```bash
git add pkg/plugins/sistatement
git commit -m "feat(sistatement): lock the legacy client_seq counter, create its directory durably, add a durable free list (spec 2026-10-09 §6.5)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---
### Task 12: late `client_seq` reservation and recycling on the agent

**Files:**
- Modify: `pkg/plugins/sistatement/plugin.go` (`pendingStatement`, `OnQuery` reservation sites `:248-268`, `statementIDFor` → `reserveStatementID`, `OnClientDataStrict` / `ClientDataReadLimit` matching, `OnQueryInputCompleteStrict`, `OnQuerySuccess`, `OnQueryComplete`, `OnClose`, new `OnException`, interface assertions)
- Modify: `pkg/plugins/sistatement/observer.go` (`SeqObserver`)
- Modify: `pkg/proxy/observer.go` (two metrics + methods)
- Modify: `build.go` (`buildAgentWithBuilders`: `siPlug` joins the exception chain)
- Create: `pkg/plugins/sistatement/late_reservation_test.go`, `pkg/proxy/relay_late_id_test.go`
- Modify: `pkg/plugins/sistatement/plugin_test.go`, `inline_values_test.go`, `table_status_test.go` (tests that read the statement id or `seq.Last()` right after `OnQuery`)

**Interfaces:**
- Consumes: `(*SeqCounter).Reserve/Release` (Task 11); `chproto.HasSeqUnspentSuffix` (Task 6).
- Produces:
  - `OnQuery` claims the statement but leaves `qctx.Query.ID` untouched; `OnQueryInputCompleteStrict` reserves the seq, writes the final statement id into `qctx.Query.ID` and signs it. Agent-local failures after `Reserve` (signing) release directly.
  - `(*Plugin).OnException(ctx, sess, exc) error` — releases the outstanding seq when the upstream Exception ends with ` [client_seq unspent]`.
  - `sistatement.SeqObserver interface { SeqRecycled(); SeqBurned(reason string) }`; `*proxy.MetricsObserver` implements it with `clickhouse_proxy_agent_si_seq_recycled_total` and `clickhouse_proxy_agent_si_seq_burned_total{reason="unknown_outcome|free_list_overflow"}`.

- [ ] **Step 1: Write the failing plugin tests**

```go
// pkg/plugins/sistatement/late_reservation_test.go
package sistatement

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/plugin"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

type seqMetrics struct {
	mu       sync.Mutex
	recycled int
	burned   map[string]int
}

func (m *seqMetrics) InlineValuesSynthesized()     {}
func (m *seqMetrics) InlineValuesEvaluationFailed() {}
func (m *seqMetrics) InlineValuesClosureRefused()   {}
func (m *seqMetrics) SeqRecycled()                  { m.mu.Lock(); m.recycled++; m.mu.Unlock() }
func (m *seqMetrics) SeqBurned(reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.burned == nil {
		m.burned = map[string]int{}
	}
	m.burned[reason]++
}

func lateFixture(t *testing.T) (*Plugin, *SeqCounter, *seqMetrics) {
	t.Helper()
	ns := network.NewInMemoryNetworkState()
	declareSchema(t, ns, testSchema())
	signer, err := auth.NewRelaySigner(testKey)
	if err != nil {
		t.Fatal(err)
	}
	seq, err := OpenSeqCounter(t.TempDir(), signer.Address())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = seq.Close() })
	metrics := &seqMetrics{}
	p, err := New(Options{Signer: signer, Schemas: ns, NetworkID: testNetworkID, Seq: seq, MaxPayloadBytes: 1 << 20, Observer: metrics})
	if err != nil {
		t.Fatal(err)
	}
	return p, seq, metrics
}

// signDeferred drives one claimed INSERT through the strict hook and returns
// its statement id.
func signDeferred(t *testing.T, p *Plugin, qctx *plugin.QueryContext) string {
	t.Helper()
	if err := p.OnClientDataStrict(context.Background(), qctx, encodeRows(t)); err != nil {
		t.Fatalf("OnClientDataStrict: %v", err)
	}
	if err := p.OnQueryInputCompleteStrict(context.Background(), qctx); err != nil {
		t.Fatalf("OnQueryInputCompleteStrict: %v", err)
	}
	return qctx.Query.ID
}

func seqOf(t *testing.T, statementID string) uint64 {
	t.Helper()
	_, seq, _, err := sicore.ParseFlatStatementID(statementID)
	if err != nil {
		t.Fatalf("statement id %q: %v", statementID, err)
	}
	return seq
}

const lateSQL = "INSERT INTO shop.orders FORMAT Native"

func TestLateReservation_OnQueryReservesNothing(t *testing.T) {
	p, seq, _ := lateFixture(t)
	q := insertQctx(newSession(1, ""), lateSQL)
	if err := p.OnQuery(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	if q.DeferredInsert == nil || q.Query.ID != "client-uuid-1" || seq.Last() != 0 {
		t.Fatalf("after OnQuery deferred=%v id=%q last=%d; want claimed, client id kept, nothing reserved", q.DeferredInsert, q.Query.ID, seq.Last())
	}
	if id := signDeferred(t, p, q); seqOf(t, id) != 1 || seq.Last() != 1 {
		t.Fatalf("strict hook id = %q last=%d, want seq 1", id, seq.Last())
	}
}

func TestLateReservation_CancelAndPayloadLimitReserveNothing(t *testing.T) {
	p, seq, _ := lateFixture(t)
	sess := newSession(1, "")
	canceled := insertQctx(sess, lateSQL)
	if err := p.OnQuery(context.Background(), canceled); err != nil {
		t.Fatal(err)
	}
	p.OnQueryAbort(context.Background(), canceled)
	p.OnQueryComplete(context.Background(), sess)

	p.maxPayload = 4
	limited := insertQctx(sess, lateSQL)
	if err := p.OnQuery(context.Background(), limited); err != nil {
		t.Fatal(err)
	}
	if err := p.OnClientDataStrict(context.Background(), limited, encodeRows(t)); err == nil {
		t.Fatal("over-limit payload must be refused")
	}
	p.OnQueryAbort(context.Background(), limited)
	p.OnQueryComplete(context.Background(), sess)
	if seq.Last() != 0 {
		t.Fatalf("cancel / payload-limit consumed client_seq (last=%d)", seq.Last())
	}
}

func TestLateReservation_UnspentExceptionRecyclesTheSeq(t *testing.T) {
	p, seq, metrics := lateFixture(t)
	sess := newSession(1, "")
	q := insertQctx(sess, lateSQL)
	if err := p.OnQuery(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	first := signDeferred(t, p, q)
	if err := p.OnException(context.Background(), sess, &chproto.Exception{Code: 252, Message: "storage_integrity: back-pressure: retry later" + chproto.SeqUnspentSuffix}); err != nil {
		t.Fatal(err)
	}
	p.OnQueryComplete(context.Background(), sess)

	next := insertQctx(sess, lateSQL)
	if err := p.OnQuery(context.Background(), next); err != nil {
		t.Fatal(err)
	}
	second := signDeferred(t, p, next)
	if seqOf(t, first) != 1 || seqOf(t, second) != 1 || seq.Last() != 1 || second == first {
		t.Fatalf("first=%q second=%q last=%d; want seq 1 reused with a new nonce", first, second, seq.Last())
	}
	if metrics.recycled != 1 || len(metrics.burned) != 0 {
		t.Fatalf("recycled=%d burned=%v", metrics.recycled, metrics.burned)
	}
}

func TestLateReservation_OtherOutcomesBurn(t *testing.T) {
	p, seq, metrics := lateFixture(t)
	sess := newSession(1, "")

	plain := insertQctx(sess, lateSQL)
	_ = p.OnQuery(context.Background(), plain)
	signDeferred(t, p, plain)
	_ = p.OnException(context.Background(), sess, &chproto.Exception{Code: 403, Message: "storage_integrity ingress: orchestrate failed"})
	p.OnQueryComplete(context.Background(), sess)

	ok := insertQctx(sess, lateSQL)
	_ = p.OnQuery(context.Background(), ok)
	id := signDeferred(t, p, ok)
	p.OnQuerySuccess(context.Background(), sess, id)
	p.OnQueryComplete(context.Background(), sess)

	if seq.Last() != 2 || metrics.burned["unknown_outcome"] != 1 || metrics.recycled != 0 {
		t.Fatalf("last=%d burned=%v recycled=%d; want the unmarked failure burned and the success not counted", seq.Last(), metrics.burned, metrics.recycled)
	}
}

type failingStatementSigner struct{ *auth.RelaySigner }

func (failingStatementSigner) SignStatementV2(auth.JWSStatementPayloadV2) (string, error) {
	return "", errors.New("hsm unavailable")
}

func TestLateReservation_SigningFailureReleasesDirectly(t *testing.T) {
	p, seq, metrics := lateFixture(t)
	relay, _ := auth.NewRelaySigner(testKey)
	p.signer = failingStatementSigner{RelaySigner: relay}
	q := insertQctx(newSession(1, ""), lateSQL)
	if err := p.OnQuery(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	if err := p.OnClientDataStrict(context.Background(), q, encodeRows(t)); err != nil {
		t.Fatal(err)
	}
	err := p.OnQueryInputCompleteStrict(context.Background(), q)
	if err == nil || !strings.Contains(err.Error(), "hsm unavailable") {
		t.Fatalf("err = %v", err)
	}
	if got, _ := seq.Reserve(); got != 1 || metrics.recycled != 1 {
		t.Fatalf("next Reserve = %d recycled=%d; want the released seq 1", got, metrics.recycled)
	}
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `bazel test //pkg/plugins/sistatement:sistatement_test --test_filter='TestLateReservation' --test_output=errors`
Expected: FAIL to compile (`p.OnException` undefined, `seqMetrics` lacks nothing yet but `SeqObserver` is unused) or FAIL on `seq.Last() != 0`.

- [ ] **Step 3: Implement**

`pkg/plugins/sistatement/observer.go`:

```go
// SeqObserver counts recycled and burned client_seq values (spec 2026-10-09
// §10). *proxy.MetricsObserver satisfies it; an Observer that does not is not
// counted.
type SeqObserver interface {
	SeqRecycled()
	SeqBurned(reason string)
}
```

`pkg/plugins/sistatement/plugin.go`:

```go
type pendingStatement struct {
	queryID        string // the client's query id until the strict hook replaces it
	tableID        string
	schemaHash     string
	clientRevision uint32
	payload        bytes.Buffer
}

// reservedSeq is the statement whose client_seq was reserved and whose
// outcome is not known yet; at most one per session.
type reservedSeq struct {
	statementID string
	seq         uint64
	resolved    bool // released as unspent, or sequenced (success)
}
```

Add `reserved map[int64]*reservedSeq` to `Plugin` (initialised in `New`). In `OnQuery` delete both `statementIDFor` calls and the `qctx.Query.ID = statementID` assignment; the pending entry becomes `&pendingStatement{queryID: qctx.Query.ID, tableID: tableID, schemaHash: schemaHash, clientRevision: uint32(revision)}`, and the two debug logs use `"query_id", qctx.Query.ID` instead of `statement_id`. In `OnClientDataStrict`, `ClientDataReadLimit` and `OnQueryAbort` compare `st.queryID` instead of `st.statementID`; their messages name `st.queryID`.

Replace `statementIDFor` with:

```go
// reserveStatementID durably reserves a client_seq at the strict input
// boundary (spec 2026-10-09 D16 (a)): an SDK-supplied flat id for this
// agent's own account keeps its seq; otherwise the smallest free seq or the
// next one is used with a fresh nonce.
func (p *Plugin) reserveStatementID(queryID string) (string, uint64, error) {
	if canonical, seq, ok := ownSuppliedStatementID(queryID, p.account); ok {
		if err := p.seq.ReserveSupplied(seq); err != nil {
			return "", 0, fmt.Errorf("storage_integrity agent: reserve supplied client_seq: %w", err)
		}
		return canonical, seq, nil
	}
	seq, err := p.seq.Reserve()
	if err != nil {
		return "", 0, fmt.Errorf("storage_integrity agent: issue client_seq: %w", err)
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		p.releaseSeq(seq)
		return "", 0, fmt.Errorf("storage_integrity agent: nonce: %w", err)
	}
	return p.account + ":" + strconv.FormatUint(seq, 10) + ":" + hex.EncodeToString(nonce[:]), seq, nil
}

// releaseSeq returns a provably unspent seq to the free list.
func (p *Plugin) releaseSeq(seq uint64) {
	overflow, err := p.seq.Release(seq)
	if err != nil {
		log.Warnw("sistatement: could not release client_seq; it stays burned", "client_seq", seq, "err", err)
		p.observeSeq(func(o SeqObserver) { o.SeqBurned("unknown_outcome") })
		return
	}
	if overflow {
		p.observeSeq(func(o SeqObserver) { o.SeqBurned("free_list_overflow") })
		return
	}
	p.observeSeq(func(o SeqObserver) { o.SeqRecycled() })
}

func (p *Plugin) observeSeq(fn func(SeqObserver)) {
	if o, ok := p.observer.(SeqObserver); ok && o != nil {
		fn(o)
	}
}
```

In `OnQueryInputCompleteStrict` match `st.queryID != qctx.Query.ID`, keep every payload check (they run before the reservation, so they consume nothing), then before signing:

```go
	statementID, seq, err := p.reserveStatementID(st.queryID)
	if err != nil {
		return err
	}
	token, err := p.signer.SignStatementV2(auth.JWSStatementPayloadV2{
		NetworkID:      p.networkID,
		KeeperShardID:  p.keeperShardID,
		StatementID:    statementID,
		SQLHash:        replay.DigestString(qctx.Query.Body),
		SettingsHash:   sicore.EmptySettingsHash,
		SchemaHash:     st.schemaHash,
		PayloadHash:    replay.DigestBytes(payload),
		PayloadLength:  uint64(len(payload)),
		PayloadFormat:  sicore.PayloadEncodingClickHouseNativeData,
		ClientRevision: st.clientRevision,
		TargetTableID:  st.tableID,
		RowIDProfileID: payloadexec.RowIDProfileID,
		StatementKind:  sicore.StatementKindCodeInsert,
	})
	if err != nil {
		// Nothing has left the agent: the seq is provably unspent.
		p.releaseSeq(seq)
		return fmt.Errorf("storage_integrity agent: sign statement %s: %w", statementID, err)
	}
	qctx.Query.ID = statementID
	qctx.Query.Settings = append(qctx.Query.Settings, chproto.Setting{Key: auth.StatementTokenSettingKey, Value: "'" + token + "'", Custom: true})
	p.mu.Lock()
	p.reserved[qctx.Session.ID()] = &reservedSeq{statementID: statementID, seq: seq}
	p.mu.Unlock()
```

(`forwardSignedInsert` reads `q.ID` only after this hook, so the id written here is the one Relay records and forwards — spec §3.1.) The `"sistatement: statement token signed"` info log keeps `statement_id`.

New and changed terminal hooks:

```go
// OnException recycles the outstanding seq when the server proved it unspent
// (spec 2026-10-09 D16 (b)). Any other Exception leaves it to OnQueryComplete,
// which counts it burned.
func (p *Plugin) OnException(_ context.Context, sess chsession.Session, exc *chproto.Exception) error {
	if p == nil || sess == nil || exc == nil || !chproto.HasSeqUnspentSuffix(exc.Message) {
		return nil
	}
	p.mu.Lock()
	r := p.reserved[sess.ID()]
	if r == nil || r.resolved {
		p.mu.Unlock()
		return nil
	}
	r.resolved = true
	seq := r.seq
	p.mu.Unlock()
	p.releaseSeq(seq)
	return nil
}
```

In `OnQuerySuccess`, before the USE handling: `if r := p.reserved[sess.ID()]; r != nil && r.statementID == queryID { r.resolved = true }`. In `OnQueryComplete` and `OnClose` (under the lock): `if r := p.reserved[sess.ID()]; r != nil { if !r.resolved { burn = true }; delete(p.reserved, sess.ID()) }`, and after unlocking `if burn { p.observeSeq(func(o SeqObserver) { o.SeqBurned("unknown_outcome") }) }`. Add `_ plugin.ExceptionPlugin = (*Plugin)(nil)` to the assertions.

`pkg/proxy/observer.go`:

```go
	agentSISeqRecycledTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "clickhouse_proxy_agent_si_seq_recycled_total",
		Help: "Agent-mode client_seq values returned to the free list after a provably-unspent refusal",
	})
	agentSISeqBurnedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "clickhouse_proxy_agent_si_seq_burned_total",
		Help: "Agent-mode client_seq values that may have been spent or could not be recycled, by reason",
	}, []string{"reason"})
```

registered in `init()`, plus `func (m *MetricsObserver) SeqRecycled() { agentSISeqRecycledTotal.Inc() }` and `func (m *MetricsObserver) SeqBurned(reason string) { agentSISeqBurnedTotal.WithLabelValues(reason).Inc() }`.

`build.go` (`buildAgentWithBuilders`): declare `exceptionPlugins := []plugin.ExceptionPlugin{metrics}` before the SI block, `exceptionPlugins = append([]plugin.ExceptionPlugin{siPlug}, exceptionPlugins...)` inside it, and use `ExceptionPlugins: exceptionPlugins` in the chain literal.

- [ ] **Step 4: Update the tests that read the id right after `OnQuery`**

Every case below now drives the strict hook through `signDeferred` (deferred lane) or keeps its `OnQueryInputCompleteStrict` call (inline lane) before it reads `qctx.Query.ID` or `seq.Last()`:

- `plugin_test.go`: `TestPlugin_HappyPathSignsStatementTokenAfterPayload` (move the statement-id assertions after the strict hook; the second INSERT signs before asserting seq 2), `TestPlugin_SeqSurvivesRestart` (sign `q` before closing the counter), `TestPlugin_ClientSuppliedStatementIDIsKeptOnlyForOwnAccount`, `TestPlugin_ClientSuppliedMaxSequenceIsRejectedWithoutAdvancing` (the terminal seq is now refused by the strict hook: assert `OnQueryInputCompleteStrict` errors with `ErrClientSeqExhausted` and the next generated id is seq 1).
- `TestPlugin_ClientSuppliedSequenceCannotBeSignedTwice`: both statements are claimed by `OnQuery`; the second statement's strict hook fails with `ErrClientSeqReused` and leaves its query id unchanged.
- `TestPlugin_ConcurrentClientSuppliedSequenceAdmitsExactlyOne`: run the two strict hooks concurrently; exactly one succeeds.
- `table_status_test.go` `TestPlugin_SignsActiveTablesOnly`: an Active table is claimed with `seq.Last() == 0`; assert `seq.Last() == 1` after `signDeferred`.
- `inline_values_test.go`: `TestPlugin_InlineValuesInstallsSynthesizedPlan`, `TestPlugin_InlineValuesColumnIdentityAndPendingSequence`, `TestPlugin_InlineValuesStrictHookRejectsInvalidPayloadAccounting` (a strict-hook payload refusal now reserves nothing: `seq.Last()` stays 0) and `TestPlugin_InlineValuesStrictHookHashesPlanPayload` read the id after `OnQueryInputCompleteStrict`.

- [ ] **Step 5: Run the package**

Run: `bazel test //pkg/plugins/sistatement:sistatement_test --test_output=errors`
Expected: PASS.

- [ ] **Step 6: Pin the Relay side of the contract**

```go
// pkg/proxy/relay_late_id_test.go
package proxy

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/plugin"
)

const lateStatementID = "0x00000000000000000000000000000000000000a1:1:late"

type lateIDHooks struct {
	deferredInsertHooks
	mu        sync.Mutex
	successID string
}

func (h *lateIDHooks) OnQueryInputCompleteStrict(_ context.Context, qctx *plugin.QueryContext) error {
	qctx.Query.ID = lateStatementID
	return nil
}

func (h *lateIDHooks) OnQuerySuccess(_ context.Context, _ chsession.Session, queryID string) {
	h.mu.Lock()
	h.successID = queryID
	h.mu.Unlock()
}

// Spec 2026-10-09 §3.1 / §9.1: the id the strict hook writes is the id Relay
// records as active and the upstream receives — no retagging exists.
func TestRelay_DeferredInsertForwardsTheIDTheStrictHookWrote(t *testing.T) {
	hooks := &lateIDHooks{deferredInsertHooks: deferredInsertHooks{inputDone: make(chan struct{}, 1)}}
	h := newDeferredHarness(t, hooks)
	nonEmpty := encodeNonEmptyClientDataPacket(t, deferredTestRev)
	sample := encodeServerSampleDataPacket(t, deferredTestRev)
	empty := encodeEmptyClientData(t)

	upDone := make(chan error, 1)
	go func() {
		codec := chproto.NewCodec(h.upstreamProxy, chproto.DirFromClient)
		codec.SetRevision(deferredTestRev)
		codec.SetCompression(proto.CompressionDisabled)
		pkt, err := codec.ReadPacket(uint64(chproto.ClientQueryCode))
		if err != nil {
			upDone <- err
			return
		}
		if q, ok := pkt.Decoded.(*chproto.Query); !ok || q.ID != lateStatementID {
			upDone <- errors.New("upstream did not receive the strict hook's statement id")
			return
		}
		if _, err := codec.ReadPacket(); err != nil { // external-tables marker
			upDone <- err
			return
		}
		if _, err := h.upstreamProxy.Write(sample); err != nil {
			upDone <- err
			return
		}
		for i := 0; i < 2; i++ { // payload + terminator
			if _, err := codec.ReadPacket(); err != nil {
				upDone <- err
				return
			}
		}
		select {
		case <-hooks.inputDone:
		case <-time.After(time.Second):
			upDone <- errors.New("no input completion")
			return
		}
		_, err = h.upstreamProxy.Write([]byte{byte(chproto.ServerEndOfStreamCode)})
		upDone <- err
	}()

	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "client-id", "INSERT INTO t FORMAT Native"))
	if got := readExact(t, h.clientProxy, len(sample)); !bytes.Equal(got, sample) {
		t.Fatalf("sample = %x", got)
	}
	writeAllConn(t, h.clientProxy, empty)
	writeAllConn(t, h.clientProxy, nonEmpty)
	writeAllConn(t, h.clientProxy, empty)
	if got := readExact(t, h.clientProxy, 1); got[0] != byte(chproto.ServerEndOfStreamCode) {
		t.Fatalf("terminal = %d", got[0])
	}
	if err := <-upDone; err != nil {
		t.Fatal(err)
	}
	hooks.mu.Lock()
	defer hooks.mu.Unlock()
	if hooks.successID != lateStatementID {
		t.Fatalf("OnQuerySuccess id = %q, want the strict hook's id", hooks.successID)
	}
	for _, err := range h.close(t) {
		if err != nil && !errors.Is(err, io.EOF) {
			t.Logf("relay loop returned: %v", err)
		}
	}
}
```

Run: `bazel test //pkg/proxy:proxy_test --test_filter='TestRelay_DeferredInsertForwardsTheIDTheStrictHookWrote' --test_output=errors`
Expected: PASS (it pins existing Relay behaviour; if it fails, stop and report — the spec's "no Relay change" premise would be wrong).

- [ ] **Step 7: Run the agent build tests and commit**

Run: `bazel test //:housegate_test //pkg/proxy:proxy_test //pkg/plugins/sistatement:sistatement_test --test_output=errors`
Expected: PASS.

```bash
git add pkg/plugins/sistatement pkg/proxy/observer.go pkg/proxy/relay_late_id_test.go build.go
git commit -m "feat(sistatement): reserve client_seq at the strict input boundary and recycle provably unspent seqs (spec 2026-10-09 D16)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---
### Task 13: discovery client — storage RPC port, routed status, SI info, writer check, status cache

**Files:**
- Modify: `pkg/registry/topology.go` (`ProxyAddress.StorageRPCPort`, `ProxyAddress.Addr`)
- Create: `pkg/registry/si_info.go`, `pkg/registry/status_cache.go`, `pkg/registry/status_cache_test.go`
- Modify: `pkg/network/rpc.go` (`callAt`, routing, `StorageIntegrityInfo`, `StorageIntegrityWriterCheck`, ports in `AllIndexers` / `ProxyByIndexerId`)
- Modify: `pkg/network/inmemory.go` (fill `StorageRPCPort` in `ProxyByIndexerId` / `AllIndexers`)
- Create: `pkg/network/rpc_routing_test.go`
- Modify: `pkg/network/rpc_table_status_test.go:28` (call list now starts with `sentio_getDatabaseInfoById`)

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces (`pkg/registry`):
  - `ProxyAddress.StorageRPCPort uint16` (name fixed by Plan A2), `func (a ProxyAddress) Addr() string` (`"<url>:<housegate port>"`, the `agent.Choice.Addr` format).
  - `type StorageIntegrityInfo struct { Enabled bool; NetworkID string; KeeperShardID uint32; SIIndexerID, SelfIndexerID *uint64; ClientLanesEnabled bool; IngressMaxPayloadBytes, MaxTokenAgeSeconds uint64; DefaultReadMode string; RegistryVersion uint64; ServerUnixTime int64 }` with the JSON names of spec §6.7 and `func (StorageIntegrityInfo) EffectiveDefaultReadMode() string` (`""` → `"safe"`).
  - `type StorageIntegrityDiscovery interface { StorageIntegrityInfo(ctx context.Context, database string) (StorageIntegrityInfo, error); StorageIntegrityWriterCheck(ctx context.Context, database, account string) (bool, error) }` — both answered by the indexer that hosts `database`.
  - `func NewCachedTableStatuses(src TableStatuses, ttl time.Duration, now func() time.Time) TableStatuses` — caches successful answers per `(database, table)` for `ttl`; errors are never cached.
- Produces (`pkg/network`): `*RpcNetworkState` implements `registry.StorageIntegrityDiscovery`; `StorageIntegrityTableStatus` asks the hosting indexer's storage RPC (`IndexerInfo.indexerUrl` + `storageNodeRpcPort`, bootstrap scheme) and falls back to the bootstrap endpoint only when the database is unknown.

- [ ] **Step 1: Write the failing tests**

```go
// pkg/network/rpc_routing_test.go
package network_test

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/registry"
)

// hostingFake starts a second JSON-RPC server (the indexer hosting the
// database) and returns its storage port.
func hostingFake(t *testing.T, methods map[string]rpcMethod) (*fakeRpcServer, uint16) {
	t.Helper()
	fake := &fakeRpcServer{methods: methods}
	srv := httptest.NewServer(http.HandlerFunc(fake.handler))
	t.Cleanup(srv.Close)
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, portText, _ := net.SplitHostPort(u.Host)
	port, _ := strconv.Atoi(portText)
	return fake, uint16(port)
}

func bootstrapFor(t *testing.T, hostingPort uint16, extra map[string]rpcMethod) (*network.RpcNetworkState, *fakeRpcServer) {
	t.Helper()
	methods := map[string]rpcMethod{
		"sentio_getDatabaseInfoById": func(params []interface{}) (interface{}, *rpcErrEnvelope) {
			if params[0] == "devuser1" {
				return network.DatabaseInfo{DatabaseId: "devuser1", IndexerId: 2}, nil
			}
			return nil, nil
		},
		"sentio_getIndexerInfoById": func([]interface{}) (interface{}, *rpcErrEnvelope) {
			return network.IndexerInfo{IndexerId: 2, IndexerUrl: "127.0.0.1", ClickhouseProxyPort: 33001, StorageNodeRpcPort: hostingPort}, nil
		},
	}
	for name, method := range extra {
		methods[name] = method
	}
	return newFakeRpc(t, methods)
}

func TestRpcNetworkState_StatusRoutesToTheHostingIndexer(t *testing.T) {
	hosting, port := hostingFake(t, map[string]rpcMethod{
		"sentio_getStorageIntegrityTableStatus": func([]interface{}) (interface{}, *rpcErrEnvelope) {
			return map[string]any{"status": "active"}, nil
		},
	})
	rpc, bootstrap := bootstrapFor(t, port, map[string]rpcMethod{
		"sentio_getStorageIntegrityTableStatus": func([]interface{}) (interface{}, *rpcErrEnvelope) {
			return map[string]any{"status": "ordinary"}, nil
		},
	})
	got, err := rpc.StorageIntegrityTableStatus(context.Background(), "devuser1", "t")
	if err != nil || got.Status != registry.TableStatusActive {
		t.Fatalf("status = %+v, %v; want the hosting indexer's active", got, err)
	}
	if strings.Join(hosting.calls, ",") != "sentio_getStorageIntegrityTableStatus" {
		t.Fatalf("hosting calls = %v", hosting.calls)
	}
	if strings.Contains(strings.Join(bootstrap.calls, ","), "sentio_getStorageIntegrityTableStatus") {
		t.Fatalf("bootstrap answered a routed status: %v", bootstrap.calls)
	}
	// An unknown database falls back to the bootstrap.
	got, err = rpc.StorageIntegrityTableStatus(context.Background(), "nope", "t")
	if err != nil || got.Status != registry.TableStatusOrdinary {
		t.Fatalf("unknown database = %+v, %v; want the bootstrap's answer", got, err)
	}
}

func TestRpcNetworkState_StorageIntegrityInfoShape(t *testing.T) {
	_, port := hostingFake(t, map[string]rpcMethod{
		"sentio_getStorageIntegrityInfo": func(params []interface{}) (interface{}, *rpcErrEnvelope) {
			if len(params) != 0 {
				return nil, &rpcErrEnvelope{Code: -32602, Message: "no params expected"}
			}
			return map[string]any{
				"enabled": true, "network_id": "devnet2-si", "keeper_shard_id": 0,
				"si_indexer_id": nil, "self_indexer_id": 2, "client_lanes_enabled": false,
				"ingress_max_payload_bytes": 67108864, "max_token_age_seconds": 60,
				"default_read_mode": "", "registry_version": 7, "server_unix_time": 1760000000,
			}, nil
		},
	})
	rpc, _ := bootstrapFor(t, port, nil)
	info, err := rpc.StorageIntegrityInfo(context.Background(), "devuser1")
	if err != nil {
		t.Fatal(err)
	}
	if !info.Enabled || info.NetworkID != "devnet2-si" || info.SIIndexerID != nil || info.SelfIndexerID == nil || *info.SelfIndexerID != 2 ||
		info.ClientLanesEnabled || info.IngressMaxPayloadBytes != 64<<20 || info.MaxTokenAgeSeconds != 60 ||
		info.EffectiveDefaultReadMode() != "safe" || info.RegistryVersion != 7 || info.ServerUnixTime != 1760000000 {
		t.Fatalf("info = %+v", info)
	}
}

func TestRpcNetworkState_WriterCheck(t *testing.T) {
	var gotParams []interface{}
	_, port := hostingFake(t, map[string]rpcMethod{
		"sentio_isDatabaseWriter": func(params []interface{}) (interface{}, *rpcErrEnvelope) {
			gotParams = params
			return params[1] == "0x00000000000000000000000000000000000000b1", nil
		},
	})
	rpc, _ := bootstrapFor(t, port, nil)
	ok, err := rpc.StorageIntegrityWriterCheck(context.Background(), "devuser1", "0x00000000000000000000000000000000000000b1")
	if err != nil || !ok || len(gotParams) != 2 || gotParams[0] != "devuser1" {
		t.Fatalf("writer check = %v, %v params=%v", ok, err, gotParams)
	}
	if ok, err := rpc.StorageIntegrityWriterCheck(context.Background(), "devuser1", "0x00000000000000000000000000000000000000b2"); err != nil || ok {
		t.Fatalf("non-writer = %v, %v", ok, err)
	}
}

func TestRpcNetworkState_WriterCheckUnsupportedIsAnError(t *testing.T) {
	_, port := hostingFake(t, nil) // method not found
	rpc, _ := bootstrapFor(t, port, nil)
	if _, err := rpc.StorageIntegrityWriterCheck(context.Background(), "devuser1", "0xb1"); err == nil {
		t.Fatal("an indexer without sentio_isDatabaseWriter must answer an error (the caller treats it as unknown)")
	}
}

func TestRpcNetworkState_ProxyAddressCarriesStorageRPCPort(t *testing.T) {
	rpc, _ := bootstrapFor(t, 32003, nil)
	addr, ok := rpc.ProxyByIndexerId(2)
	if !ok || addr.StorageRPCPort != 32003 || addr.Addr() != "127.0.0.1:33001" {
		t.Fatalf("addr = %+v ok=%v", addr, ok)
	}
}
```

```go
// pkg/registry/status_cache_test.go
package registry

import (
	"context"
	"errors"
	"testing"
	"time"
)

type countingStatuses struct {
	calls int
	err   error
}

func (c *countingStatuses) StorageIntegrityTableStatus(context.Context, string, string) (TableStatus, error) {
	c.calls++
	return TableStatus{Status: TableStatusActive}, c.err
}

func TestCachedTableStatuses(t *testing.T) {
	now := time.Unix(100, 0)
	src := &countingStatuses{}
	cached := NewCachedTableStatuses(src, 5*time.Second, func() time.Time { return now })
	for i := 0; i < 3; i++ {
		if _, err := cached.StorageIntegrityTableStatus(context.Background(), "db", "t"); err != nil {
			t.Fatal(err)
		}
	}
	if src.calls != 1 {
		t.Fatalf("a burst cost %d lookups, want 1", src.calls)
	}
	_, _ = cached.StorageIntegrityTableStatus(context.Background(), "db", "other")
	now = now.Add(5 * time.Second)
	_, _ = cached.StorageIntegrityTableStatus(context.Background(), "db", "t")
	if src.calls != 3 {
		t.Fatalf("calls = %d, want a miss per table and a refresh after the TTL", src.calls)
	}
	src.err = errors.New("down")
	now = now.Add(time.Minute)
	if _, err := cached.StorageIntegrityTableStatus(context.Background(), "db", "t"); err == nil {
		t.Fatal("error not propagated")
	}
	if _, err := cached.StorageIntegrityTableStatus(context.Background(), "db", "t"); err == nil || src.calls != 5 {
		t.Fatalf("errors must not be cached (calls=%d)", src.calls)
	}
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `bazel run //:gazelle && bazel test //pkg/network:network_test //pkg/registry:registry_test --test_filter='TestRpcNetworkState_|TestCachedTableStatuses' --test_output=errors`
Expected: FAIL to compile (`StorageRPCPort`, `StorageIntegrityInfo`, `NewCachedTableStatuses` undefined).

- [ ] **Step 3: Implement the registry types**

`pkg/registry/topology.go`:

```go
type ProxyAddress struct {
	Url           string
	HousegatePort uint16
	// StorageRPCPort is the indexer's storage-node JSON-RPC port
	// (IndexerInfo.storageNodeRpcPort); zero when not advertised. The agent
	// asks the hosting indexer for SI table status and info there.
	StorageRPCPort uint16
}

// Addr is the housegate dialing target "<url>:<port>".
func (a ProxyAddress) Addr() string { return fmt.Sprintf("%s:%d", a.Url, a.HousegatePort) }
```

(add `import "fmt"`).

```go
// pkg/registry/si_info.go
package registry

import "context"

// StorageIntegrityInfo is one answer of sentio_getStorageIntegrityInfo
// (spec 2026-10-09 §6.7) from the serving indexer. SIIndexerID is null while
// the table registry is disabled; SelfIndexerID is null when the node cannot
// resolve its own id.
type StorageIntegrityInfo struct {
	Enabled                bool    `json:"enabled"`
	NetworkID              string  `json:"network_id"`
	KeeperShardID          uint32  `json:"keeper_shard_id"`
	SIIndexerID            *uint64 `json:"si_indexer_id"`
	SelfIndexerID          *uint64 `json:"self_indexer_id"`
	ClientLanesEnabled     bool    `json:"client_lanes_enabled"`
	IngressMaxPayloadBytes uint64  `json:"ingress_max_payload_bytes"`
	MaxTokenAgeSeconds     uint64  `json:"max_token_age_seconds"`
	DefaultReadMode        string  `json:"default_read_mode"`
	RegistryVersion        uint64  `json:"registry_version"`
	ServerUnixTime         int64   `json:"server_unix_time"`
}

// EffectiveDefaultReadMode maps the empty mode to "safe", as the server does.
func (i StorageIntegrityInfo) EffectiveDefaultReadMode() string {
	if i.DefaultReadMode == "" {
		return "safe"
	}
	return i.DefaultReadMode
}

// StorageIntegrityDiscovery is the agent's per-database discovery port (spec
// 2026-10-09 D18). Both calls are answered by the indexer that hosts
// database. RpcNetworkState implements it; YAML and host-injected
// registries do not, and the agent then uses its configured network id and
// skips the writer pre-check.
type StorageIntegrityDiscovery interface {
	StorageIntegrityInfo(ctx context.Context, database string) (StorageIntegrityInfo, error)
	// StorageIntegrityWriterCheck calls sentio_isDatabaseWriter (Plan A2). An
	// error, including an indexer that predates the method, means "unknown".
	StorageIntegrityWriterCheck(ctx context.Context, database, account string) (bool, error)
}
```

```go
// pkg/registry/status_cache.go
package registry

import (
	"context"
	"sync"
	"time"
)

// NewCachedTableStatuses caches successful status answers per table for ttl
// (spec 2026-10-09 §6.4: a burst of INSERTs costs one lookup). Errors are
// never cached, so a failed lookup is retried on the next INSERT.
func NewCachedTableStatuses(src TableStatuses, ttl time.Duration, now func() time.Time) TableStatuses {
	if now == nil {
		now = time.Now
	}
	return &cachedTableStatuses{src: src, ttl: ttl, now: now, entries: map[[2]string]cachedStatus{}}
}

type cachedStatus struct {
	status  TableStatus
	expires time.Time
}

type cachedTableStatuses struct {
	src     TableStatuses
	ttl     time.Duration
	now     func() time.Time
	mu      sync.Mutex
	entries map[[2]string]cachedStatus
}

func (c *cachedTableStatuses) StorageIntegrityTableStatus(ctx context.Context, database, table string) (TableStatus, error) {
	key := [2]string{database, table}
	c.mu.Lock()
	if entry, ok := c.entries[key]; ok && c.now().Before(entry.expires) {
		c.mu.Unlock()
		return entry.status, nil
	}
	c.mu.Unlock()
	status, err := c.src.StorageIntegrityTableStatus(ctx, database, table)
	if err != nil {
		return TableStatus{}, err
	}
	c.mu.Lock()
	c.entries[key] = cachedStatus{status: status, expires: c.now().Add(c.ttl)}
	c.mu.Unlock()
	return status, nil
}
```

- [ ] **Step 4: Implement routing in `RpcNetworkState`**

Rename `call` to `callAt(ctx, endpoint, method, params, result)` (posting to `endpoint` instead of `r.endpoint`) and keep `call` as `return r.callAt(ctx, r.endpoint, method, params, result)`. In `AllIndexers` and `ProxyByIndexerId` set `StorageRPCPort: info.StorageNodeRpcPort` (and in `InMemoryNetworkState`'s two converters). Then:

```go
// storageEndpointFor returns the storage RPC endpoint of the indexer hosting
// database, falling back to the bootstrap endpoint only when the database is
// unknown (spec 2026-10-09 §6.4 step 2).
func (r *RpcNetworkState) storageEndpointFor(ctx context.Context, database string) (string, error) {
	var db DatabaseInfo
	ok, err := r.call(ctx, "sentio_getDatabaseInfoById", []interface{}{database}, &db)
	if err != nil {
		// Not "unknown": the bootstrap may not host the database, so its
		// answer could be wrong. The caller treats the error as a failed
		// lookup (status: unsigned pass-through, the server answers 733).
		return "", fmt.Errorf("rpc: database %s lookup: %w", database, err)
	}
	if !ok {
		return r.endpoint, nil
	}
	var info IndexerInfo
	ok, err = r.call(ctx, "sentio_getIndexerInfoById", []interface{}{db.IndexerId}, &info)
	if err != nil {
		return "", fmt.Errorf("rpc: indexer %d hosting %s: %w", db.IndexerId, database, err)
	}
	if !ok || info.IndexerUrl == "" || info.StorageNodeRpcPort == 0 {
		return "", fmt.Errorf("rpc: indexer %d hosting %s advertises no storage RPC endpoint", db.IndexerId, database)
	}
	scheme := "http"
	if u, err := url.Parse(r.endpoint); err == nil && u.Scheme != "" {
		scheme = u.Scheme
	}
	host := info.IndexerUrl
	if u, err := url.Parse(host); err == nil && u.Host != "" {
		host = u.Hostname() // tolerate a full URL in indexerUrl
	}
	return scheme + "://" + net.JoinHostPort(host, strconv.Itoa(int(info.StorageNodeRpcPort))), nil
}
```

`StorageIntegrityTableStatus` resolves `endpoint, err := r.storageEndpointFor(ctx, database)` (returning a wrapped error) and calls `r.callAt(ctx, endpoint, …)`; everything else in it is unchanged. Add:

```go
// StorageIntegrityInfo calls sentio_getStorageIntegrityInfo (no params) on the
// indexer hosting database (spec 2026-10-09 §6.7).
func (r *RpcNetworkState) StorageIntegrityInfo(ctx context.Context, database string) (registry.StorageIntegrityInfo, error) {
	endpoint, err := r.storageEndpointFor(ctx, database)
	if err != nil {
		return registry.StorageIntegrityInfo{}, err
	}
	var info registry.StorageIntegrityInfo
	ok, err := r.callAt(ctx, endpoint, "sentio_getStorageIntegrityInfo", nil, &info)
	if err != nil {
		return registry.StorageIntegrityInfo{}, fmt.Errorf("rpc: getStorageIntegrityInfo at %s: %w", endpoint, err)
	}
	if !ok {
		return registry.StorageIntegrityInfo{}, fmt.Errorf("rpc: getStorageIntegrityInfo at %s returned null", endpoint)
	}
	return info, nil
}

// StorageIntegrityWriterCheck calls sentio_isDatabaseWriter(database,
// account) on the hosting indexer (Plan A2). Errors mean "unknown".
func (r *RpcNetworkState) StorageIntegrityWriterCheck(ctx context.Context, database, account string) (bool, error) {
	endpoint, err := r.storageEndpointFor(ctx, database)
	if err != nil {
		return false, err
	}
	var writer bool
	ok, err := r.callAt(ctx, endpoint, "sentio_isDatabaseWriter", []interface{}{database, account}, &writer)
	if err != nil {
		return false, fmt.Errorf("rpc: isDatabaseWriter at %s: %w", endpoint, err)
	}
	if !ok {
		return false, fmt.Errorf("rpc: isDatabaseWriter at %s returned null", endpoint)
	}
	return writer, nil
}

var _ registry.StorageIntegrityDiscovery = (*RpcNetworkState)(nil)
```

Imports gain `net`, `net/url`, `strconv`. Update the file's header comment list of methods. In `rpc_table_status_test.go:28` the expected call list becomes `"sentio_getDatabaseInfoById,sentio_getStorageIntegrityTableStatus"` (the unknown-database fallback).

- [ ] **Step 5: Run the packages**

Run: `bazel test //pkg/network:network_test //pkg/registry:registry_test //pkg/plugins/agent:agent_test --test_output=errors`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add pkg/registry pkg/network
git commit -m "feat(network): route SI status to the hosting indexer; sentio_getStorageIntegrityInfo and sentio_isDatabaseWriter clients; status cache (spec 2026-10-09 D18)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---
### Task 14: agent discovery — network id per hosting indexer, lazy seq store, writer pre-check

**Files:**
- Create: `pkg/plugins/sistatement/discovery.go`, `pkg/plugins/sistatement/discovery_test.go`
- Modify: `pkg/plugins/sistatement/plugin.go` (`Options`, `Plugin`, `New`, `OnQuery` network-id resolution between status and hash check, pre-check before the claim, `pendingStatement.networkID`, strict hook uses `st.networkID` and `p.seqFor`, `reservedSeq.counter`, new `Close`)
- Modify: `pkg/plugins/sistatement/observer.go` (`DiscoveryObserver`), `pkg/proxy/observer.go` (metric)

**Interfaces:**
- Consumes: `registry.StorageIntegrityDiscovery`, `registry.StorageIntegrityInfo` (Task 13); `SeqCounter` (Task 11); reservation flow (Task 12).
- Produces (`sistatement.Options` additions):
  - `Discovery registry.StorageIntegrityDiscovery` — nil keeps today's behaviour: `NetworkID` required, no pre-check.
  - `OpenSeq func(networkID string) (*SeqCounter, error)` — opens the counter for a network at its first SI write (plan decision P4); `Seq`, when set, serves every network (tests, explicit state dir).
  - `WriterPrecheck bool` — the advisory pre-check (build sets it false for drivers).
  - `Now func() time.Time` — clock for the skew warning (default `time.Now`).
  - `(*Plugin).Close() error` — closes every opened counter (releases the locks).
  - `sistatement.DiscoveryObserver interface { SIDiscoveryFailed(step string) }`; metric `clickhouse_proxy_agent_si_discovery_failures_total{step="info|network_id|precheck"}`.

- [ ] **Step 1: Write the failing tests**

```go
// pkg/plugins/sistatement/discovery_test.go
package sistatement

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/registry"
)

type fakeDiscovery struct {
	info      registry.StorageIntegrityInfo
	infoErr   error
	writer    bool
	writerErr error
	mu        sync.Mutex
	infoCalls int
	checked   []string
}

func (f *fakeDiscovery) StorageIntegrityInfo(context.Context, string) (registry.StorageIntegrityInfo, error) {
	f.mu.Lock()
	f.infoCalls++
	f.mu.Unlock()
	return f.info, f.infoErr
}

func (f *fakeDiscovery) StorageIntegrityWriterCheck(_ context.Context, database, account string) (bool, error) {
	f.mu.Lock()
	f.checked = append(f.checked, database+"/"+account)
	f.mu.Unlock()
	return f.writer, f.writerErr
}

type discoveryMetrics struct {
	seqMetrics
	failed map[string]int
}

func (m *discoveryMetrics) SIDiscoveryFailed(step string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failed == nil {
		m.failed = map[string]int{}
	}
	m.failed[step]++
}

func discoveryPlugin(t *testing.T, d *fakeDiscovery, configured string, precheck bool) (*Plugin, *discoveryMetrics, *[]string) {
	t.Helper()
	ns := network.NewInMemoryNetworkState()
	declareSchema(t, ns, testSchema())
	signer, err := auth.NewRelaySigner(testKey)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	var opened []string
	metrics := &discoveryMetrics{}
	p, err := New(Options{
		Signer: signer, Schemas: ns, NetworkID: configured, MaxPayloadBytes: 1 << 20,
		Discovery: d, WriterPrecheck: precheck, Observer: metrics,
		Now: func() time.Time { return time.Unix(1_760_000_000, 0) },
		OpenSeq: func(networkID string) (*SeqCounter, error) {
			opened = append(opened, networkID)
			return OpenSeqCounter(dir+"/"+networkID, signer.Address())
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p, metrics, &opened
}

func goodInfo() registry.StorageIntegrityInfo {
	return registry.StorageIntegrityInfo{Enabled: true, NetworkID: testNetworkID, ServerUnixTime: 1_760_000_000}
}

func TestDiscovery_NetworkIDComesFromTheHostingIndexer(t *testing.T) {
	d := &fakeDiscovery{info: goodInfo(), writer: true}
	p, _, opened := discoveryPlugin(t, d, "", true)
	q := insertQctx(newSession(1, ""), lateSQL)
	if err := p.OnQuery(context.Background(), q); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	id := signDeferred(t, p, q)
	if seqOf(t, id) != 1 || len(*opened) != 1 || (*opened)[0] != testNetworkID {
		t.Fatalf("id=%q opened=%v; want the counter for the discovered network", id, *opened)
	}
	q2 := insertQctx(newSession(2, ""), lateSQL)
	_ = p.OnQuery(context.Background(), q2)
	signDeferred(t, p, q2)
	if d.infoCalls != 1 || len(*opened) != 1 {
		t.Fatalf("info calls=%d opened=%v; info and counter are cached for the process", d.infoCalls, *opened)
	}
}

func TestDiscovery_InfoFailureRefusesWithoutAConfiguredID(t *testing.T) {
	d := &fakeDiscovery{infoErr: errors.New("method not found"), writer: true}
	p, metrics, _ := discoveryPlugin(t, d, "", true)
	err := p.OnQuery(context.Background(), insertQctx(newSession(1, ""), lateSQL))
	if err == nil || !strings.Contains(err.Error(), "storage_integrity agent: cannot discover network id") {
		t.Fatalf("err = %v", err)
	}
	if metrics.failed["info"] != 1 {
		t.Fatalf("failures = %v", metrics.failed)
	}
}

func TestDiscovery_InfoFailureFallsBackToTheConfiguredID(t *testing.T) {
	d := &fakeDiscovery{infoErr: errors.New("method not found"), writer: true}
	p, metrics, opened := discoveryPlugin(t, d, testNetworkID, true)
	q := insertQctx(newSession(1, ""), lateSQL)
	if err := p.OnQuery(context.Background(), q); err != nil {
		t.Fatalf("configured network id must keep signing (plan P7): %v", err)
	}
	signDeferred(t, p, q)
	if (*opened)[0] != testNetworkID || metrics.failed["info"] != 1 {
		t.Fatalf("opened=%v failures=%v", *opened, metrics.failed)
	}
}

func TestDiscovery_MismatchedNetworkRefuses(t *testing.T) {
	d := &fakeDiscovery{info: goodInfo(), writer: true}
	p, _, _ := discoveryPlugin(t, d, "some-other-net", true)
	err := p.OnQuery(context.Background(), insertQctx(newSession(1, ""), lateSQL))
	if err == nil || !strings.Contains(err.Error(), "refusing to sign for the wrong network") {
		t.Fatalf("err = %v", err)
	}
}

func TestDiscovery_ClockSkewIsReportedOnce(t *testing.T) {
	info := goodInfo()
	info.ServerUnixTime = 1_760_000_000 + 30
	p, _, _ := discoveryPlugin(t, &fakeDiscovery{info: info, writer: true}, "", true)
	if !p.checkSkew("shop", info) || p.checkSkew("shop", info) {
		t.Fatal("a 30 s skew must be reported once per database")
	}
	ok := goodInfo()
	if p.checkSkew("other", ok) {
		t.Fatal("no skew must not be reported")
	}
}

func TestDiscovery_WriterPrecheck(t *testing.T) {
	refuse := &fakeDiscovery{info: goodInfo(), writer: false}
	p, _, _ := discoveryPlugin(t, refuse, "", true)
	q := insertQctx(newSession(1, ""), lateSQL)
	err := p.OnQuery(context.Background(), q)
	if err == nil || !strings.Contains(err.Error(), "is not a writer of database shop") || q.DeferredInsert != nil {
		t.Fatalf("err = %v deferred=%v; want a local refusal before the claim", err, q.DeferredInsert)
	}
	if refuse.checked[0] != "shop/"+p.account {
		t.Fatalf("checked %v, want the signer as principal", refuse.checked)
	}

	unknown := &fakeDiscovery{info: goodInfo(), writerErr: errors.New("method not found")}
	p2, metrics, _ := discoveryPlugin(t, unknown, "", true)
	q2 := insertQctx(newSession(1, ""), lateSQL)
	if err := p2.OnQuery(context.Background(), q2); err != nil || q2.DeferredInsert == nil {
		t.Fatalf("an unknown pre-check must fall through to the server: err=%v", err)
	}
	if metrics.failed["precheck"] != 1 {
		t.Fatalf("failures = %v", metrics.failed)
	}

	driver, _, _ := discoveryPlugin(t, &fakeDiscovery{info: goodInfo(), writer: false}, "", false)
	q3 := insertQctx(newSession(1, ""), lateSQL)
	if err := driver.OnQuery(context.Background(), q3); err != nil {
		t.Fatalf("pre-check disabled (driver) must not refuse: %v", err)
	}
}

func TestDiscovery_OwnerIsThePrecheckPrincipal(t *testing.T) {
	d := &fakeDiscovery{info: goodInfo(), writer: true}
	p, _, _ := discoveryPlugin(t, d, "", true)
	p.owner = "0x00000000000000000000000000000000000000c2"
	_ = p.OnQuery(context.Background(), insertQctx(newSession(1, ""), lateSQL))
	if d.checked[0] != "shop/0x00000000000000000000000000000000000000c2" {
		t.Fatalf("checked %v, want the owner", d.checked)
	}
}
```

(`testSchema()` declares `shop.orders`; `lateSQL`, `signDeferred`, `seqOf`, `seqMetrics` come from Task 12's test file.)

- [ ] **Step 2: Run them to verify they fail**

Run: `bazel test //pkg/plugins/sistatement:sistatement_test --test_filter='TestDiscovery_' --test_output=errors`
Expected: FAIL to compile (`Options.Discovery`, `p.checkSkew`, `p.Close` undefined).

- [ ] **Step 3: Implement**

`observer.go`:

```go
// DiscoveryObserver counts agent discovery failures by step (spec 2026-10-09
// §10): "info", "network_id", "precheck".
type DiscoveryObserver interface {
	SIDiscoveryFailed(step string)
}
```

```go
// pkg/plugins/sistatement/discovery.go
package sistatement

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/housegate/housegate/pkg/log"
	"github.com/housegate/housegate/pkg/registry"
)

// maxClockSkew is the ingress's future-token tolerance (pkg/auth
// eth_validator.go): a larger skew makes every statement token look issued
// in the future.
const maxClockSkew = 5 * time.Second

func (p *Plugin) observeDiscovery(step string) {
	if o, ok := p.observer.(DiscoveryObserver); ok && o != nil {
		o.SIDiscoveryFailed(step)
	}
}

// siInfo returns the hosting indexer's SI info for database, cached for the
// process lifetime (spec 2026-10-09 §6.4 step 3).
func (p *Plugin) siInfo(ctx context.Context, database string) (registry.StorageIntegrityInfo, error) {
	p.mu.Lock()
	info, ok := p.infos[database]
	p.mu.Unlock()
	if ok {
		return info, nil
	}
	info, err := p.discovery.StorageIntegrityInfo(ctx, database)
	if err != nil {
		return registry.StorageIntegrityInfo{}, err
	}
	p.mu.Lock()
	p.infos[database] = info
	p.mu.Unlock()
	p.checkSkew(database, info)
	return info, nil
}

// resolveNetworkID is spec 2026-10-09 §6.4 steps 3-4 with plan decision P7.
func (p *Plugin) resolveNetworkID(ctx context.Context, database string) (string, error) {
	if p.discovery == nil {
		return p.networkID, nil
	}
	_, logger := log.FromContext(ctx)
	info, err := p.siInfo(ctx, database)
	step := "info"
	if err == nil && strings.TrimSpace(info.NetworkID) == "" {
		step, err = "network_id", fmt.Errorf("the indexer reported no network_id")
	}
	if err != nil {
		p.observeDiscovery(step)
		if p.networkID != "" {
			logger.Warnw("sistatement: network discovery failed; signing for the configured storage_integrity.agent.network_id",
				"database", database, "network_id", p.networkID, "err", err)
			return p.networkID, nil
		}
		return "", fmt.Errorf("storage_integrity agent: cannot discover network id from the indexer hosting %s: %w", database, err)
	}
	if p.networkID != "" && info.NetworkID != p.networkID {
		return "", fmt.Errorf("storage_integrity agent: the indexer hosting %s reports network %s but storage_integrity.agent.network_id is %s; refusing to sign for the wrong network", database, info.NetworkID, p.networkID)
	}
	return info.NetworkID, nil
}

// checkSkew warns once per database when the local clock is more than
// maxClockSkew away from the indexer's. It reports whether it warned.
func (p *Plugin) checkSkew(database string, info registry.StorageIntegrityInfo) bool {
	if info.ServerUnixTime == 0 {
		return false
	}
	skew := p.now().Sub(time.Unix(info.ServerUnixTime, 0))
	if skew < 0 {
		skew = -skew
	}
	if skew <= maxClockSkew {
		return false
	}
	p.mu.Lock()
	warned := p.skewWarned[database]
	p.skewWarned[database] = true
	p.mu.Unlock()
	if warned {
		return false
	}
	log.Warnw("sistatement: local clock differs from the indexer's; the ingress refuses statement tokens issued in the future",
		"database", database, "skew", skew, "tolerance", maxClockSkew)
	return true
}

// precheckWriter is the advisory pre-check of spec 2026-10-09 §6.4 through
// sentio_isDatabaseWriter (plan decision P2). Only a definite "no" refuses;
// an error is unknown and the server decides.
func (p *Plugin) precheckWriter(ctx context.Context, database string) error {
	if p.discovery == nil || !p.writerPrecheck {
		return nil
	}
	principal := p.account
	if p.owner != "" {
		principal = strings.ToLower(p.owner)
	}
	ok, err := p.discovery.StorageIntegrityWriterCheck(ctx, database, principal)
	if err != nil {
		p.observeDiscovery("precheck")
		_, logger := log.FromContext(ctx)
		logger.Debugw("sistatement: writer pre-check unavailable; the server decides", "database", database, "principal", principal, "err", err)
		return nil
	}
	if !ok {
		return fmt.Errorf("storage_integrity agent: %s is not a writer of database %s (it needs Owner or Write on the database); the server would refuse this INSERT", principal, database)
	}
	return nil
}

// seqFor opens, once, the client_seq counter for a network (plan decision P4).
func (p *Plugin) seqFor(networkID string) (*SeqCounter, error) {
	if p.seq != nil {
		return p.seq, nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.seqs[networkID]; ok {
		return c, nil
	}
	c, err := p.openSeq(networkID)
	if err != nil {
		return nil, fmt.Errorf("storage_integrity agent: open client_seq store for network %s: %w", networkID, err)
	}
	p.seqs[networkID] = c
	return c, nil
}

// Close releases every counter this plugin opened.
func (p *Plugin) Close() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	var errs []error
	for _, c := range p.seqs {
		errs = append(errs, c.Close())
	}
	if p.seq != nil {
		errs = append(errs, p.seq.Close())
	}
	return errors.Join(errs...)
}
```

(add `"errors"` to the imports).

`plugin.go` changes:

- `Options`: add `Discovery registry.StorageIntegrityDiscovery`, `OpenSeq func(networkID string) (*SeqCounter, error)`, `WriterPrecheck bool`, `Now func() time.Time` with doc comments from **Interfaces**. `New` validation: `NetworkID` is required only when `Discovery == nil`; one of `Seq` / `OpenSeq` is required (`"seq counter or opener is required"`).
- `Plugin`: add `discovery registry.StorageIntegrityDiscovery`, `openSeq func(string) (*SeqCounter, error)`, `seqs map[string]*SeqCounter`, `infos map[string]registry.StorageIntegrityInfo`, `skewWarned map[string]bool`, `writerPrecheck bool`, `now func() time.Time` (default `time.Now`); initialise the maps in `New`.
- `activeTarget` becomes two steps: `activeStatus(ctx, target) (registry.TableStatus, bool)` (the status lookup and its pass-through rules, unchanged) and `verifySchema(target, status, networkID) (payloadexec.TableSchema, string, error)` (the decode, table-id and hash checks, using `networkID` instead of `p.networkID`).
- In `OnQuery`, after `activeStatus` reports Active: `networkID, err := p.resolveNetworkID(ctx, target.Database)`; return the error; `schema, schemaHash, err = p.verifySchema(target, status, networkID)`. After the settings checks and before `insertColumnList`: `if err := p.precheckWriter(ctx, target.Database); err != nil { return err }`. The pending entry records `networkID`.
- In `OnQueryInputCompleteStrict`: `counter, err := p.seqFor(st.networkID)`; `reserveStatementID` takes the counter (`p.reserveStatementID(counter, st.queryID)`), the token's `NetworkID` is `st.networkID`, and `reservedSeq` stores `counter` so `OnException` releases on the right counter (`releaseSeq(counter, seq)`).

`pkg/proxy/observer.go`: `agentSIDiscoveryFailuresTotal = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "clickhouse_proxy_agent_si_discovery_failures_total", Help: "Agent-mode storage-integrity discovery failures by step"}, []string{"step"})`, registered in `init()`, and `func (m *MetricsObserver) SIDiscoveryFailed(step string) { agentSIDiscoveryFailuresTotal.WithLabelValues(step).Inc() }`.

`build.go`: register `pushTeardown`-style cleanup for the agent: extend the agent `teardown` closure with `if siPlugClose != nil { siPlugClose() }`, where `siPlugClose = func() { _ = siPlug.Close() }` is set after `sistatement.New` succeeds and also run by the existing `buildSucceeded` defer on failure. (Wiring of `Discovery` / `OpenSeq` comes in Task 16; until then build keeps passing `Seq`.)

- [ ] **Step 4: Run the package and the root build**

Run: `bazel test //pkg/plugins/sistatement:sistatement_test //:housegate_test //pkg/proxy:proxy_test --test_output=errors`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/plugins/sistatement pkg/proxy/observer.go build.go
git commit -m "feat(sistatement): discover the network id at the hosting indexer, open the seq store lazily, pre-check writers (spec 2026-10-09 D18)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---
### Task 15: the agent's upstream switch to the hosting indexer

**Files:**
- Modify: `pkg/storageintegrity/insert_parser.go` (`LeadingKeywords`), test in `pkg/storageintegrity/insert_parser_test.go`
- Create: `pkg/plugins/sistatement/switch.go`, `pkg/plugins/sistatement/switch_test.go`
- Modify: `pkg/plugins/sistatement/plugin.go` (`Options`, `Plugin`, `OnQuery` stateful tracking and switch call, `OnQuerySuccess` / `OnQueryComplete` / `OnClose`), `pkg/plugins/sistatement/observer.go` (`SwitchObserver`), `pkg/plugins/sistatement/plugin_test.go` (`fakeSession.SwitchUpstream` records)
- Modify: `pkg/proxy/observer.go` (metric)
- Create: `pkg/proxy/relay_switch_test.go`

**Interfaces:**
- Consumes: `Session.SwitchUpstream`, `chsession.ErrUpstreamRevisionTooLow` (Task 10); `registry.ProxyAddress.Addr` (Task 13); `resolveNetworkID`, `precheckWriter` (Task 14).
- Produces:
  - `func sicore.LeadingKeywords(sql string, n int) ([]string, error)` — up to `n` leading bare words, upper-cased, comments and whitespace skipped by the `ParseUseDatabaseStrict` lexer.
  - `sistatement.Options` additions: `Hosting HostingResolver` (`interface { Get(string) (registry.Database, bool); ProxyByIndexerId(uint64) (registry.ProxyAddress, bool) }`), `Dial func(ctx context.Context, address string) (*chproto.Codec, error)`, `PinnedUpstream bool`. Nil `Hosting` or `Dial`, or `PinnedUpstream`, disables the switch.
  - `sistatement.SwitchObserver interface { SIUpstreamSwitch(result string) }`; metric `clickhouse_proxy_agent_si_upstream_switches_total{result="switched|refused_state|refused_database|refused_revision|dial_failed"}`.

- [ ] **Step 1: Write the failing tests**

Append to `pkg/storageintegrity/insert_parser_test.go`:

```go
func TestLeadingKeywords(t *testing.T) {
	for sql, want := range map[string]string{
		"SET max_threads = 1":                    "SET,MAX_THREADS",
		"  -- c\n/* x */ create temporary table": "CREATE,TEMPORARY",
		"START TRANSACTION":                      "START,TRANSACTION",
		"begin":                                  "BEGIN",
		"(SELECT 1)":                             "",
	} {
		got, err := LeadingKeywords(sql, 2)
		if err != nil || strings.Join(got, ",") != want {
			t.Errorf("LeadingKeywords(%q) = %v, %v; want %q", sql, got, err, want)
		}
	}
}
```

Replace the `RebindToLocal` neighbour in `pkg/plugins/sistatement/plugin_test.go`'s `fakeSession` with a recording switch (add `switched []*chproto.ClientHello`, `switchErr error` fields):

```go
func (s *fakeSession) SwitchUpstream(_ context.Context, newUp *chproto.Codec, hello *chproto.ClientHello) error {
	if s.switchErr != nil {
		return s.switchErr
	}
	cloned := *hello
	s.switched = append(s.switched, &cloned)
	// Production negotiates the revision in the handshake; the fake fixes it.
	newUp.SetRevision(54470)
	s.upstream = newUp
	return nil
}
```

```go
// pkg/plugins/sistatement/switch_test.go
package sistatement

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/plugin"
)

type switchMetrics struct {
	seqMetrics
	results map[string]int
}

func (m *switchMetrics) SIUpstreamSwitch(result string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.results == nil {
		m.results = map[string]int{}
	}
	m.results[result]++
}

func addressedCodec(t *testing.T, address string) *chproto.Codec {
	t.Helper()
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	return chproto.NewCodec(namedEvaluationConn{Conn: client, address: address}, chproto.DirToUpstream)
}

// switchFixture: indexer 1 at 10.0.0.1:9000 (the session's current upstream),
// indexer 2 at 10.0.0.2:9000 hosts "shop", indexer 3 hosts "elsewhere".
func switchFixture(t *testing.T, pinned bool) (*Plugin, *fakeSession, *switchMetrics, *[]string) {
	t.Helper()
	ns := network.NewInMemoryNetworkState()
	declareSchema(t, ns, testSchema())
	ns.IndexerInfos[1] = network.IndexerInfo{IndexerId: 1, IndexerUrl: "10.0.0.1", ClickhouseProxyPort: 9000}
	ns.IndexerInfos[2] = network.IndexerInfo{IndexerId: 2, IndexerUrl: "10.0.0.2", ClickhouseProxyPort: 9000}
	ns.IndexerInfos[3] = network.IndexerInfo{IndexerId: 3, IndexerUrl: "10.0.0.3", ClickhouseProxyPort: 9000}
	ns.DatabaseInfos["shop"] = network.DatabaseInfo{DatabaseId: "shop", IndexerId: 2}
	ns.DatabaseInfos["local"] = network.DatabaseInfo{DatabaseId: "local", IndexerId: 1}
	ns.DatabaseInfos["elsewhere"] = network.DatabaseInfo{DatabaseId: "elsewhere", IndexerId: 3}
	opts, _ := inlineOptions(t, &fakeEvaluator{}, true)
	opts.Schemas = ns
	opts.InlineValues = InlineValuesOptions{}
	opts.Evaluator = nil
	opts.Hosting = ns
	opts.PinnedUpstream = pinned
	metrics := &switchMetrics{}
	opts.Observer = metrics
	var dialed []string
	opts.Dial = func(_ context.Context, address string) (*chproto.Codec, error) {
		dialed = append(dialed, address)
		return addressedCodec(t, address), nil
	}
	p, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	sess := newSession(1, "")
	sess.upstream = addressedCodec(t, "10.0.0.1:9000")
	sess.state.SetUpstreamHello(&chproto.ClientHello{Name: "clickhouse-client", ProtocolVersion: 54470, User: "default", Password: "pw"})
	return p, sess, metrics, &dialed
}

func TestSwitch_InsertMovesTheSessionToTheHostingIndexer(t *testing.T) {
	p, sess, metrics, dialed := switchFixture(t, false)
	q := insertQctx(sess, "INSERT INTO shop.orders FORMAT Native")
	if err := p.OnQuery(context.Background(), q); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if len(*dialed) != 1 || (*dialed)[0] != "10.0.0.2:9000" || len(sess.switched) != 1 {
		t.Fatalf("dialed=%v switched=%d", *dialed, len(sess.switched))
	}
	hello := sess.switched[0]
	if hello.ProtocolVersion != testRevision || hello.Database != "" || hello.User != "default" || hello.Password != "pw" || hello.Name != "clickhouse-client" {
		t.Fatalf("replayed hello = %+v; want verbatim with ProtocolVersion = ClientRevision and no database", hello)
	}
	if q.DeferredInsert == nil || metrics.results["switched"] != 1 {
		t.Fatalf("deferred=%v results=%v", q.DeferredInsert, metrics.results)
	}
	// Sticky: the next INSERT is already on the hosting indexer.
	p.OnQueryAbort(context.Background(), q)
	p.OnQueryComplete(context.Background(), sess)
	if err := p.OnQuery(context.Background(), insertQctx(sess, "INSERT INTO shop.orders FORMAT Native")); err != nil || len(*dialed) != 1 {
		t.Fatalf("second INSERT re-dialed: %v %v", *dialed, err)
	}
}

func TestSwitch_NoSwitchWhenPinnedOrAlreadyThere(t *testing.T) {
	pinned, sess, _, dialed := switchFixture(t, true)
	if err := pinned.OnQuery(context.Background(), insertQctx(sess, "INSERT INTO shop.orders FORMAT Native")); err != nil || len(*dialed) != 0 {
		t.Fatalf("pinned upstream switched: %v %v", *dialed, err)
	}
	p, sess2, _, dialed2 := switchFixture(t, false)
	sess2.upstream = addressedCodec(t, "10.0.0.2:9000")
	if err := p.OnQuery(context.Background(), insertQctx(sess2, "INSERT INTO shop.orders FORMAT Native")); err != nil || len(*dialed2) != 0 {
		t.Fatalf("already on the hosting indexer but dialed %v (%v)", *dialed2, err)
	}
}

func TestSwitch_ServerSideStateRefusesLocally(t *testing.T) {
	for _, stmt := range []string{"SET max_threads = 1", "CREATE TEMPORARY TABLE tmp (x UInt8)", "BEGIN TRANSACTION", "START TRANSACTION"} {
		t.Run(stmt, func(t *testing.T) {
			p, sess, metrics, dialed := switchFixture(t, false)
			state := insertQctx(sess, stmt)
			if err := p.OnQuery(context.Background(), state); err != nil {
				t.Fatal(err)
			}
			p.OnQuerySuccess(context.Background(), sess, state.Query.ID)
			p.OnQueryComplete(context.Background(), sess)
			q := insertQctx(sess, "INSERT INTO shop.orders FORMAT Native")
			err := p.OnQuery(context.Background(), q)
			want := "storage_integrity agent: INSERT into shop must run on indexer 2, but this session holds server-side state; reconnect with --database shop"
			if err == nil || err.Error() != want || len(*dialed) != 0 || q.DeferredInsert != nil || metrics.results["refused_state"] != 1 {
				t.Fatalf("err=%v dialed=%v deferred=%v results=%v", err, *dialed, q.DeferredInsert, metrics.results)
			}
		})
	}
}

func TestSwitch_FailedSetKeepsTheSessionSwitchable(t *testing.T) {
	p, sess, _, dialed := switchFixture(t, false)
	set := insertQctx(sess, "SET max_threads = 1")
	_ = p.OnQuery(context.Background(), set)
	p.OnQueryComplete(context.Background(), sess) // Exception: no success
	if err := p.OnQuery(context.Background(), insertQctx(sess, "INSERT INTO shop.orders FORMAT Native")); err != nil || len(*dialed) != 1 {
		t.Fatalf("a failed SET must not pin the session: %v %v", *dialed, err)
	}
}

func TestSwitch_SessionDatabaseRules(t *testing.T) {
	p, sess, metrics, _ := switchFixture(t, false)
	sess.state.SetLogicalDatabase("elsewhere")
	err := p.OnQuery(context.Background(), insertQctx(sess, "INSERT INTO shop.orders FORMAT Native"))
	want := "storage_integrity agent: INSERT into shop must run on indexer 2, but the session database elsewhere lives on indexer 3; use a separate connection with --database shop or USE shop first"
	if err == nil || err.Error() != want || metrics.results["refused_database"] != 1 {
		t.Fatalf("err=%v results=%v", err, metrics.results)
	}

	p2, sess2, _, _ := switchFixture(t, false)
	sess2.state.SetLogicalDatabase("shop") // hosted by the target: kept
	if err := p2.OnQuery(context.Background(), insertQctx(sess2, "INSERT INTO orders FORMAT Native")); err != nil || sess2.switched[0].Database != "shop" {
		t.Fatalf("err=%v hello=%+v", err, sess2.switched)
	}

	p3, sess3, _, _ := switchFixture(t, false)
	sess3.state.SetLogicalDatabase("default") // unknown to the registry: kept (plan P10)
	if err := p3.OnQuery(context.Background(), insertQctx(sess3, "INSERT INTO shop.orders FORMAT Native")); err != nil || sess3.switched[0].Database != "default" {
		t.Fatalf("err=%v hello=%+v", err, sess3.switched)
	}
}

func TestSwitch_RevisionAndDialFailuresRefuseLocally(t *testing.T) {
	p, sess, metrics, _ := switchFixture(t, false)
	sess.switchErr = fmt.Errorf("%w: test", chsession.ErrUpstreamRevisionTooLow)
	if err := p.OnQuery(context.Background(), insertQctx(sess, "INSERT INTO shop.orders FORMAT Native")); err == nil || metrics.results["refused_revision"] != 1 {
		t.Fatalf("err=%v results=%v", err, metrics.results)
	}

	p2, sess2, metrics2, _ := switchFixture(t, false)
	p2.dial = func(context.Context, string) (*chproto.Codec, error) { return nil, errors.New("connection refused") }
	err := p2.OnQuery(context.Background(), insertQctx(sess2, "INSERT INTO shop.orders FORMAT Native"))
	if err == nil || !strings.Contains(err.Error(), "connection refused") || metrics2.results["dial_failed"] != 1 {
		t.Fatalf("err=%v results=%v", err, metrics2.results)
	}
}

func TestSwitch_InlineEvaluatorFollowsTheNewEndpoint(t *testing.T) {
	ev := &fakeEvaluator{blocks: [][]proto.InputColumn{evaluatedBlock(1, "eu", 1.5)}}
	p, sess, _, _ := switchFixture(t, false)
	p.inline = InlineValuesOptions{Enabled: true, EvaluationTimeout: 5 * time.Second, MaxRows: 1000}
	p.evaluator = ev
	q := insertQctx(sess, "INSERT INTO shop.orders VALUES (1, 'eu', 1.5)")
	q.Values[plugin.ValuesKeyMaterialized] = "noop"
	if err := p.OnQuery(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	if ev.seen.UpstreamAddress != "10.0.0.2:9000" {
		t.Fatalf("evaluator used %q, want the switched endpoint", ev.seen.UpstreamAddress)
	}
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `bazel test //pkg/storageintegrity:storageintegrity_test //pkg/plugins/sistatement:sistatement_test --test_filter='TestLeadingKeywords|TestSwitch_' --test_output=errors`
Expected: FAIL to compile (`LeadingKeywords`, `Options.Hosting`, `p.dial` undefined).

- [ ] **Step 3: Implement**

`pkg/storageintegrity/insert_parser.go`:

```go
// LeadingKeywords returns up to n leading bare words of sql, upper-cased,
// skipping comments and whitespace with the lexer ParseUseDatabaseStrict
// uses. It stops at the first token that is not a bare word.
func LeadingKeywords(sql string, n int) ([]string, error) {
	s := storageScanner{sql: sql}
	var out []string
	for len(out) < n {
		word, ok, err := s.bareWord()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		out = append(out, strings.ToUpper(word))
	}
	return out, nil
}
```

`observer.go`:

```go
// SwitchObserver counts upstream-switch outcomes (spec 2026-10-09 §10).
type SwitchObserver interface {
	SIUpstreamSwitch(result string)
}
```

```go
// pkg/plugins/sistatement/switch.go
package sistatement

import (
	"context"
	"errors"
	"fmt"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/log"
	"github.com/housegate/housegate/pkg/plugin"
	"github.com/housegate/housegate/pkg/registry"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

// HostingResolver answers which indexer hosts a database and where its
// housegate listens. registry.Registry satisfies it.
type HostingResolver interface {
	Get(id string) (registry.Database, bool)
	ProxyByIndexerId(indexerId uint64) (registry.ProxyAddress, bool)
}

// holdsServerState reports whether sql changes server-side session state the
// agent cannot replay on another server (spec 2026-10-09 §6.4 step 1). An
// unreadable statement counts as stateful: refusing a later switch is safe.
func holdsServerState(sql string) bool {
	words, err := sicore.LeadingKeywords(sql, 2)
	if err != nil {
		return true
	}
	if len(words) == 0 {
		return false
	}
	switch words[0] {
	case "SET", "BEGIN":
		return true
	case "CREATE":
		return len(words) > 1 && words[1] == "TEMPORARY"
	case "START":
		return len(words) > 1 && words[1] == "TRANSACTION"
	}
	return false
}

func (p *Plugin) observeSwitch(result string) {
	if o, ok := p.observer.(SwitchObserver); ok && o != nil {
		o.SIUpstreamSwitch(result)
	}
}

func currentUpstreamAddress(up *chproto.Codec) string {
	if up == nil {
		return ""
	}
	if named, ok := up.Conn().(interface{ UpstreamAddress() string }); ok {
		return named.UpstreamAddress()
	}
	return ""
}

// maybeSwitch moves the session to the indexer hosting database before the
// statement is claimed (spec 2026-10-09 §6.4, D19). Every refusal is local and
// session-preserving: OnQuery errors end only the query.
func (p *Plugin) maybeSwitch(ctx context.Context, qctx *plugin.QueryContext, database string) error {
	if p.hosting == nil || p.dial == nil || p.pinnedUpstream {
		return nil
	}
	db, ok := p.hosting.Get(database)
	if !ok {
		return nil // unknown database: the server decides
	}
	target, ok := p.hosting.ProxyByIndexerId(db.IndexerId)
	if !ok || target.HousegatePort == 0 {
		return nil
	}
	targetAddr := target.Addr()
	sess := qctx.Session
	if currentUpstreamAddress(sess.Upstream()) == targetAddr {
		return nil
	}
	p.mu.Lock()
	stateful := p.nonSwitchable[sess.ID()]
	p.mu.Unlock()
	if stateful {
		p.observeSwitch("refused_state")
		return fmt.Errorf("storage_integrity agent: INSERT into %s must run on indexer %d, but this session holds server-side state; reconnect with --database %s", database, db.IndexerId, database)
	}
	cur := p.sessionDatabase(sess)
	if cur != "" {
		if curInfo, ok := p.hosting.Get(cur); ok && curInfo.IndexerId != db.IndexerId {
			p.observeSwitch("refused_database")
			return fmt.Errorf("storage_integrity agent: INSERT into %s must run on indexer %d, but the session database %s lives on indexer %d; use a separate connection with --database %s or USE %s first", database, db.IndexerId, cur, curInfo.IndexerId, database, database)
		}
	}
	hello := sess.State().UpstreamHello()
	if hello == nil {
		p.observeSwitch("dial_failed")
		return fmt.Errorf("storage_integrity agent: INSERT into %s must run on indexer %d, but this session has no replayable hello", database, db.IndexerId)
	}
	hello.ProtocolVersion = sess.State().Snapshot().ClientRevision
	hello.Database = cur
	newUp, err := p.dial(ctx, targetAddr)
	if err != nil {
		p.observeSwitch("dial_failed")
		return fmt.Errorf("storage_integrity agent: INSERT into %s must run on indexer %d (%s): %w", database, db.IndexerId, targetAddr, err)
	}
	if err := sess.SwitchUpstream(ctx, newUp, hello); err != nil {
		result := "dial_failed"
		if errors.Is(err, chsession.ErrUpstreamRevisionTooLow) {
			result = "refused_revision"
		}
		p.observeSwitch(result)
		return fmt.Errorf("storage_integrity agent: INSERT into %s must run on indexer %d (%s): %w", database, db.IndexerId, targetAddr, err)
	}
	p.observeSwitch("switched")
	_, logger := log.FromContext(ctx)
	logger.Infow("sistatement: session switched to the hosting indexer", "database", database, "indexer_id", db.IndexerId, "upstream", targetAddr)
	return nil
}
```

`plugin.go`:

- `Options`: `Hosting HostingResolver`, `Dial func(ctx context.Context, address string) (*chproto.Codec, error)`, `PinnedUpstream bool`; `Plugin`: `hosting`, `dial`, `pinnedUpstream`, `nonSwitchable map[int64]bool`, `statefulNext map[int64]string` (initialised in `New`).
- `OnQuery`, right after the `USE` branch: `if holdsServerState(sql) { p.mu.Lock(); p.statefulNext[sessID] = qctx.Query.ID; p.mu.Unlock() }`.
- `OnQuery`, after `precheckWriter` and before `insertColumnList`: `if err := p.maybeSwitch(ctx, qctx, target.Database); err != nil { return err }`. The existing revision and inline checks read the (possibly new) upstream afterwards.
- `OnQuerySuccess` (under the lock): `if id, ok := p.statefulNext[sess.ID()]; ok && id == queryID { p.nonSwitchable[sess.ID()] = true }`; `OnQueryComplete` deletes `statefulNext[sess.ID()]`; `OnClose` deletes both maps' entries.

`pkg/proxy/observer.go`: `clickhouse_proxy_agent_si_upstream_switches_total` counter vector with label `result`, registered in `init()`, and `func (m *MetricsObserver) SIUpstreamSwitch(result string)`.

- [ ] **Step 4: Write the Relay test that the read loop follows the switch**

```go
// pkg/proxy/relay_switch_test.go
package proxy

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/plugin"
)

type switchingHooks struct {
	plugin.NoopHooks
	newUp *chproto.Codec
}

func (h *switchingHooks) OnQuery(ctx context.Context, qctx *plugin.QueryContext) error {
	return qctx.Session.SwitchUpstream(ctx, h.newUp, &chproto.ClientHello{Name: "client", ProtocolVersion: deferredTestRev})
}

// Spec 2026-10-09 §6.4 step 6: the switch runs inside OnQuery; Relay forwards
// the query on the new upstream and upstreamToClient resumes on it once its
// blocked read on the closed old connection fails.
func TestRelay_SwitchUpstreamInOnQueryServesTheQueryOnTheNewUpstream(t *testing.T) {
	newServer, newClient := net.Pipe()
	hooks := &switchingHooks{newUp: chproto.NewCodec(newClient, chproto.DirToUpstream)}
	h := newDeferredHarness(t, hooks)
	served := make(chan string, 1)
	go func() {
		defer newServer.Close()
		codec := chproto.NewCodec(newServer, chproto.DirFromClient)
		if _, err := codec.ReadPacket(uint64(chproto.ClientHelloCode)); err != nil {
			served <- "hello: " + err.Error()
			return
		}
		var buf proto.Buffer
		(&proto.ServerHello{Name: "target", Major: 24, Minor: 1, Revision: deferredTestRev}).EncodeAware(&buf, deferredTestRev)
		_, _ = newServer.Write(buf.Buf)
		codec.SetRevision(deferredTestRev)
		pkt, err := codec.ReadPacket(uint64(chproto.ClientQueryCode))
		if err != nil {
			served <- "query: " + err.Error()
			return
		}
		served <- pkt.Decoded.(*chproto.Query).Body
		_, _ = codec.ReadPacket() // the client's empty Data
		_, _ = newServer.Write([]byte{byte(chproto.ServerEndOfStreamCode)})
	}()

	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "q1", "SELECT 1"))
	writeAllConn(t, h.clientProxy, encodeEmptyClientData(t))
	select {
	case body := <-served:
		if body != "SELECT 1" {
			t.Fatalf("new upstream got %q", body)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the query never reached the new upstream")
	}
	if got := readExact(t, h.clientProxy, 1); got[0] != byte(chproto.ServerEndOfStreamCode) {
		t.Fatalf("client got %d, want the new upstream's EndOfStream", got[0])
	}
}
```

- [ ] **Step 5: Run the packages**

Run: `bazel run //:gazelle && bazel test //pkg/storageintegrity:storageintegrity_test //pkg/plugins/sistatement:sistatement_test //pkg/proxy:proxy_test --test_output=errors`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add pkg/storageintegrity/insert_parser.go pkg/storageintegrity/insert_parser_test.go pkg/plugins/sistatement pkg/proxy/observer.go pkg/proxy/relay_switch_test.go
git commit -m "feat(sistatement): switch the session to the hosting indexer before claiming an SI INSERT (spec 2026-10-09 D19)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---
### Task 16: `-agent-key` quickstart — flags, env, network preset, default state directory, agent wiring

**Files:**
- Create: `pkg/config/agent_state_dir.go`, `pkg/config/agent_quickstart.go`, `pkg/config/agent_quickstart_test.go`
- Modify: `pkg/config/storage_integrity_config.go` (`StorageIntegrityAgentConfig.Lanes`, `.ReadMode`; `validateAgent` relaxations), `pkg/config/storage_integrity_agent_config_test.go` (state-dir case)
- Modify: `cmd/main.go` (six flags, env, `ApplyAgentQuickstart` before `Validate`)
- Modify: `build.go` (`buildAgentWithBuilders` SI block: status cache, discovery, hosting, dial, pin, pre-check, seq opener, state-dir startup check)
- Create: `agent_quickstart_build_test.go`

**Interfaces:**
- Consumes: `sistatement.Options.Discovery/OpenSeq/WriterPrecheck/Hosting/Dial/PinnedUpstream` (Tasks 14, 15); `registry.StorageIntegrityDiscovery`, `registry.NewCachedTableStatuses` (Task 13).
- Produces (`pkg/config`):
  - `const DefaultAgentNetwork = "devnet2"`, `const DefaultAgentListen = "127.0.0.1:9000"`, `var AgentNetworkPresets = map[string]string{"devnet2": "http://64.38.144.158:32003"}`.
  - `func DefaultAgentStateBase(goos string, getenv func(string) string, home string) (string, bool)` — linux `$XDG_STATE_HOME/housegate` else `~/.local/state/housegate`; darwin `~/Library/Application Support/housegate`; otherwise none.
  - `func AgentSIStateDir(base, networkID, signer string) string` — `<base>/si/<network_id>/<lowercase signer>`.
  - `type AgentQuickstart struct { ConfigFileLoaded, AgentModeSet, ListenSet bool; Network, SI, SIStateDir, SILanes, SIReadMode, SIInlineValues string }` and `func ApplyAgentQuickstart(cfg *Config, q AgentQuickstart) error`.
  - `StorageIntegrityAgentConfig.Lanes string` (`lanes`: `""`/`auto`/`off`; both mean the legacy lane until Plan B) and `.ReadMode string` (`read_mode`: `""`/`safe`/`unsafe_latest`; used by Task 17).

- [ ] **Step 1: Write the failing config tests**

```go
// pkg/config/agent_quickstart_test.go
package config

import (
	"path/filepath"
	"strings"
	"testing"
)

func quickstartBase() *Config {
	c := Default()
	c.Agent.PrivateKeyHex = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	return &c
}

func TestApplyAgentQuickstart_KeyOnly(t *testing.T) {
	cfg := quickstartBase()
	if err := ApplyAgentQuickstart(cfg, AgentQuickstart{}); err != nil {
		t.Fatal(err)
	}
	if !cfg.Agent.Mode || cfg.NetworkState.Source != "http://64.38.144.158:32003" || cfg.Listen != "127.0.0.1:9000" {
		t.Fatalf("mode=%v source=%q listen=%q", cfg.Agent.Mode, cfg.NetworkState.Source, cfg.Listen)
	}
	if !cfg.StorageIntegrity.Agent.Enabled || cfg.StorageIntegrity.Agent.StateDir != "" || cfg.StorageIntegrity.Agent.NetworkID != "" {
		t.Fatalf("SI agent = %+v; want enabled with a discovered network and the default state dir", cfg.StorageIntegrity.Agent)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("an -agent-key-only config must validate: %v", err)
	}
}

func TestApplyAgentQuickstart_ExplicitAgentFalseWins(t *testing.T) {
	cfg := quickstartBase()
	if err := ApplyAgentQuickstart(cfg, AgentQuickstart{AgentModeSet: true}); err != nil || cfg.Agent.Mode {
		t.Fatalf("-agent=false must keep server mode: mode=%v err=%v", cfg.Agent.Mode, err)
	}
}

func TestApplyAgentQuickstart_ConfigFileKeepsItsValues(t *testing.T) {
	cfg := quickstartBase()
	cfg.Agent.Mode = true
	cfg.Agent.Upstream = "10.0.0.8:9001"
	cfg.Listen = ":9001"
	if err := ApplyAgentQuickstart(cfg, AgentQuickstart{ConfigFileLoaded: true, SILanes: "off"}); err != nil {
		t.Fatal(err)
	}
	if cfg.Listen != ":9001" || cfg.NetworkState.Source != "" || cfg.StorageIntegrity.Agent.Enabled {
		t.Fatalf("a config file must not receive quickstart defaults: %+v", cfg)
	}
	if cfg.StorageIntegrity.Agent.Lanes != "off" {
		t.Fatalf("an explicitly set HOUSEGATE_SI_LANES must still apply: %q", cfg.StorageIntegrity.Agent.Lanes)
	}
}

func TestApplyAgentQuickstart_SourceAndUpstreamOverridesWin(t *testing.T) {
	cfg := quickstartBase()
	cfg.NetworkState.Source = "http://node:10003"
	if err := ApplyAgentQuickstart(cfg, AgentQuickstart{}); err != nil || cfg.NetworkState.Source != "http://node:10003" {
		t.Fatalf("-state must win over the preset: %q %v", cfg.NetworkState.Source, err)
	}
	pinned := quickstartBase()
	pinned.Agent.Upstream = "10.0.0.8:9001"
	if err := ApplyAgentQuickstart(pinned, AgentQuickstart{}); err != nil {
		t.Fatal(err)
	}
	if pinned.NetworkState.Source != "" || pinned.StorageIntegrity.Agent.Enabled {
		t.Fatalf("a pinned upstream needs no preset and -si auto cannot discover: %+v", pinned.StorageIntegrity.Agent)
	}
}

func TestApplyAgentQuickstart_Values(t *testing.T) {
	for name, tc := range map[string]struct {
		q       AgentQuickstart
		wantErr string
		check   func(*Config) bool
	}{
		"si off":          {q: AgentQuickstart{SI: "off"}, check: func(c *Config) bool { return !c.StorageIntegrity.Agent.Enabled }},
		"si on":           {q: AgentQuickstart{SI: "on"}, check: func(c *Config) bool { return c.StorageIntegrity.Agent.Enabled }},
		"state dir":       {q: AgentQuickstart{SIStateDir: "/var/lib/hg"}, check: func(c *Config) bool { return c.StorageIntegrity.Agent.StateDir == "/var/lib/hg" }},
		"read mode":       {q: AgentQuickstart{SIReadMode: "safe"}, check: func(c *Config) bool { return c.StorageIntegrity.Agent.ReadMode == "safe" }},
		"inline off":      {q: AgentQuickstart{SIInlineValues: "off"}, check: func(c *Config) bool { return !c.StorageIntegrity.Agent.InlineValues.Enabled }},
		"unknown network": {q: AgentQuickstart{Network: "mainnet-9"}, wantErr: `unknown -network "mainnet-9"`},
		"bad si":          {q: AgentQuickstart{SI: "yes"}, wantErr: "-si"},
		"bad lanes":       {q: AgentQuickstart{SILanes: "on"}, wantErr: "-si-lanes"},
		"bad read mode":   {q: AgentQuickstart{SIReadMode: "fast"}, wantErr: "-si-read-mode"},
		"bad inline":      {q: AgentQuickstart{SIInlineValues: "maybe"}, wantErr: "-si-inline-values"},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := quickstartBase()
			err := ApplyAgentQuickstart(cfg, tc.q)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil || !tc.check(cfg) {
				t.Fatalf("err=%v cfg=%+v", err, cfg.StorageIntegrity.Agent)
			}
		})
	}
}

func TestDefaultAgentStateBase(t *testing.T) {
	env := func(m map[string]string) func(string) string { return func(k string) string { return m[k] } }
	for _, tc := range []struct {
		goos, xdg, home, want string
		ok                    bool
	}{
		{"linux", "/xdg", "/home/u", "/xdg/housegate", true},
		{"linux", "", "/home/u", "/home/u/.local/state/housegate", true},
		{"linux", "relative", "/home/u", "/home/u/.local/state/housegate", true},
		{"darwin", "", "/Users/u", "/Users/u/Library/Application Support/housegate", true},
		{"windows", "", `C:\Users\u`, "", false},
		{"linux", "", "", "", false},
	} {
		got, ok := DefaultAgentStateBase(tc.goos, env(map[string]string{"XDG_STATE_HOME": tc.xdg}), tc.home)
		if got != tc.want || ok != tc.ok {
			t.Errorf("%s xdg=%q home=%q = %q,%v; want %q,%v", tc.goos, tc.xdg, tc.home, got, ok, tc.want, tc.ok)
		}
	}
	if got := AgentSIStateDir("/b", "devnet2", "0xAB"); got != filepath.Join("/b", "si", "devnet2", "0xab") {
		t.Fatalf("AgentSIStateDir = %q", got)
	}
}
```

In `pkg/config/storage_integrity_agent_config_test.go` change the `"missing state_dir"` case to expect success on the release platforms and add the network-id relaxation:

```go
		{"missing state_dir uses the per-OS default", func(c *Config) { c.StorageIntegrity.Agent.StateDir = "" }, ""},
		{"rpc source discovers the network id", func(c *Config) {
			c.StorageIntegrity.Agent.NetworkID = ""
			c.NetworkState.Source = "http://node:10003"
		}, ""},
		{"bad lanes", func(c *Config) { c.StorageIntegrity.Agent.Lanes = "on" }, "storage_integrity.agent.lanes"},
		{"bad read_mode", func(c *Config) { c.StorageIntegrity.Agent.ReadMode = "fast" }, "storage_integrity.agent.read_mode"},
```

(the test binary runs on linux/darwin only — Global Constraints).

- [ ] **Step 2: Run them to verify they fail**

Run: `bazel test //pkg/config:config_test --test_output=errors`
Expected: FAIL to compile (`ApplyAgentQuickstart`, `DefaultAgentStateBase`, `Lanes`, `ReadMode` undefined).

- [ ] **Step 3: Implement the config side**

```go
// pkg/config/agent_state_dir.go
package config

import (
	"path/filepath"
	"strings"
)

// DefaultAgentStateBase is the per-OS base of the agent's state (spec
// 2026-10-09 §6.4). Other platforms have none: they need an explicit
// storage_integrity.agent.state_dir, and the lane lock refuses them anyway.
func DefaultAgentStateBase(goos string, getenv func(string) string, home string) (string, bool) {
	switch goos {
	case "linux":
		if xdg := getenv("XDG_STATE_HOME"); xdg != "" && filepath.IsAbs(xdg) {
			return filepath.Join(xdg, "housegate"), true
		}
		if home == "" {
			return "", false
		}
		return filepath.Join(home, ".local", "state", "housegate"), true
	case "darwin":
		if home == "" {
			return "", false
		}
		return filepath.Join(home, "Library", "Application Support", "housegate"), true
	default:
		return "", false
	}
}

// AgentSIStateDir is the SI subtree <base>/si/<network_id>/<signer>.
func AgentSIStateDir(base, networkID, signer string) string {
	return filepath.Join(base, "si", networkID, strings.ToLower(signer))
}
```

```go
// pkg/config/agent_quickstart.go
package config

import (
	"fmt"
	"sort"
	"strings"
)

// DefaultAgentNetwork is the preset an -agent-key-only agent joins (spec
// 2026-10-09 R2); DefaultAgentListen keeps clickhouse-client uncompressed.
const (
	DefaultAgentNetwork = "devnet2"
	DefaultAgentListen  = "127.0.0.1:9000"
)

// AgentNetworkPresets maps -network names to their bootstrap storage RPC.
var AgentNetworkPresets = map[string]string{
	"devnet2": "http://64.38.144.158:32003",
}

// AgentQuickstart carries the agent-UX flags and env values of spec
// 2026-10-09 §6.4 (flag over env; empty = not given) and what the binary
// knows about its config file.
type AgentQuickstart struct {
	ConfigFileLoaded bool // -config / HOUSEGATE_CONFIG / ./config.json was read
	AgentModeSet     bool // -agent or HOUSEGATE_AGENT was given, either value
	ListenSet        bool // -listen or HOUSEGATE_LISTEN was given
	Network          string
	SI               string
	SIStateDir       string
	SILanes          string
	SIReadMode       string
	SIInlineValues   string
}

// ApplyAgentQuickstart applies the agent defaults. Without a config file an
// agent key implies agent mode and every quickstart default applies; with a
// config file only the values the operator passed change it (plan decision
// P5).
func ApplyAgentQuickstart(cfg *Config, q AgentQuickstart) error {
	if !q.ConfigFileLoaded && !q.AgentModeSet && cfg.Agent.PrivateKeyHex != "" {
		cfg.Agent.Mode = true
	}
	if cfg.Mode() != ModeAgent {
		return nil
	}
	quick := !q.ConfigFileLoaded
	if quick || q.Network != "" {
		name := q.Network
		if name == "" {
			name = DefaultAgentNetwork
		}
		source, ok := AgentNetworkPresets[name]
		if !ok {
			names := make([]string, 0, len(AgentNetworkPresets))
			for n := range AgentNetworkPresets {
				names = append(names, n)
			}
			sort.Strings(names)
			return fmt.Errorf("unknown -network %q (known: %s)", name, strings.Join(names, ", "))
		}
		if cfg.Agent.Upstream == "" && cfg.NetworkState.Source == "" {
			cfg.NetworkState.Source = source
		}
	}
	if quick && !q.ListenSet {
		cfg.Listen = DefaultAgentListen
	}
	si := q.SI
	if si == "" && quick {
		si = "auto"
	}
	switch si {
	case "":
	case "off":
		cfg.StorageIntegrity.Agent.Enabled = false
	case "on":
		cfg.StorageIntegrity.Agent.Enabled = true
	case "auto":
		// Discovery needs an RPC network state (plan decision P6).
		cfg.StorageIntegrity.Agent.Enabled = cfg.NetworkState.IsRpcSource()
	default:
		return fmt.Errorf("-si %q is invalid (want auto, on or off)", si)
	}
	if q.SIStateDir != "" {
		cfg.StorageIntegrity.Agent.StateDir = q.SIStateDir
	}
	switch q.SILanes {
	case "":
	case "auto", "off":
		cfg.StorageIntegrity.Agent.Lanes = q.SILanes
	default:
		return fmt.Errorf("-si-lanes %q is invalid (want auto or off)", q.SILanes)
	}
	switch q.SIReadMode {
	case "":
	case "safe", "unsafe_latest":
		cfg.StorageIntegrity.Agent.ReadMode = q.SIReadMode
	default:
		return fmt.Errorf("-si-read-mode %q is invalid (want safe or unsafe_latest)", q.SIReadMode)
	}
	inline := q.SIInlineValues
	if inline == "" && quick {
		inline = "auto"
	}
	switch inline {
	case "", "auto":
		// Task 17 resolves auto against the platform's native engine.
	case "on":
		cfg.StorageIntegrity.Agent.InlineValues.Enabled = true
	case "off":
		cfg.StorageIntegrity.Agent.InlineValues.Enabled = false
	default:
		return fmt.Errorf("-si-inline-values %q is invalid (want auto, on or off)", inline)
	}
	return nil
}
```

`StorageIntegrityAgentConfig` gains:

```go
	// Lanes selects client_seq lanes: "auto" (default) uses lanes when the
	// hosting indexer reports them enabled, "off" keeps legacy lane-less ids
	// (the driver sidecar, HOUSEGATE_SI_LANES=off). No indexer reports lanes
	// before Plan B, so both use the legacy lane today.
	Lanes string `json:"lanes" yaml:"lanes"`
	// ReadMode, when set, injects SQL_x_read_mode on SELECTs ("safe" or
	// "unsafe_latest"); empty keeps the server default.
	ReadMode string `json:"read_mode" yaml:"read_mode"`
```

and the comment on `NetworkID` / `StateDir` says they are optional (discovered / per-OS default). In `validateAgent` replace the two required-field rules with:

```go
	discoverable := root.NetworkState.IsRpcSource() || !a.RequireNetworkState
	if strings.TrimSpace(a.NetworkID) == "" && !discoverable {
		errs = append(errs, errors.New("storage_integrity.agent.network_id is required unless network_state.source is an RPC URL (the agent then discovers it from the hosting indexer)"))
	}
	if strings.TrimSpace(a.StateDir) == "" {
		home, _ := os.UserHomeDir()
		if _, ok := DefaultAgentStateBase(runtime.GOOS, os.Getenv, home); !ok {
			errs = append(errs, fmt.Errorf("storage_integrity.agent.state_dir is required on %s (no default state directory)", runtime.GOOS))
		}
	}
	switch a.Lanes {
	case "", "auto", "off":
	default:
		errs = append(errs, fmt.Errorf("storage_integrity.agent.lanes %q is invalid (want auto or off)", a.Lanes))
	}
	switch a.ReadMode {
	case "", "safe", "unsafe_latest":
	default:
		errs = append(errs, fmt.Errorf("storage_integrity.agent.read_mode %q is invalid (want safe or unsafe_latest)", a.ReadMode))
	}
```

(imports `os`, `runtime`).

- [ ] **Step 4: Run the config tests**

Run: `bazel test //pkg/config:config_test --test_output=errors`
Expected: PASS.

- [ ] **Step 5: Wire the flags in `cmd/main.go`**

After the existing `agentDriver` flag:

```go
	networkName := flag.String("network", "", `agent network preset (default "devnet2" without -config; also HOUSEGATE_NETWORK)`)
	siMode := flag.String("si", "", "storage-integrity signing: auto|on|off (default auto without -config; also HOUSEGATE_SI)")
	siStateDir := flag.String("si-state-dir", "", "storage-integrity agent state directory (default per OS; also HOUSEGATE_SI_STATE_DIR)")
	siLanes := flag.String("si-lanes", "", "client_seq lanes: auto|off (also HOUSEGATE_SI_LANES)")
	siReadMode := flag.String("si-read-mode", "", "inject SQL_x_read_mode on SELECTs: safe|unsafe_latest (also HOUSEGATE_SI_READ_MODE)")
	siInlineValues := flag.String("si-inline-values", "", "signed inline INSERT ... VALUES: auto|on|off (default auto without -config; also HOUSEGATE_SI_INLINE_VALUES)")
```

and, after every existing `explicitFlags[...]` override and before `cfg.Validate()`:

```go
	flagOrEnv := func(name, value, env string) string {
		if explicitFlags[name] {
			return value
		}
		return os.Getenv(env)
	}
	_, envListen := os.LookupEnv("HOUSEGATE_LISTEN")
	_, envAgent := os.LookupEnv("HOUSEGATE_AGENT")
	if err := config.ApplyAgentQuickstart(&cfg, config.AgentQuickstart{
		ConfigFileLoaded: *configPath != "" || fileExists("config.json"),
		AgentModeSet:     explicitFlags["agent"] || envAgent,
		ListenSet:        explicitFlags["listen"] || envListen,
		Network:          flagOrEnv("network", *networkName, "HOUSEGATE_NETWORK"),
		SI:               flagOrEnv("si", *siMode, "HOUSEGATE_SI"),
		SIStateDir:       flagOrEnv("si-state-dir", *siStateDir, "HOUSEGATE_SI_STATE_DIR"),
		SILanes:          flagOrEnv("si-lanes", *siLanes, "HOUSEGATE_SI_LANES"),
		SIReadMode:       flagOrEnv("si-read-mode", *siReadMode, "HOUSEGATE_SI_READ_MODE"),
		SIInlineValues:   flagOrEnv("si-inline-values", *siInlineValues, "HOUSEGATE_SI_INLINE_VALUES"),
	}); err != nil {
		log.Fatale(err, "agent options")
	}
```

with a small helper at the end of the file:

```go
func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
```

The driver sidecar keeps working unchanged: it passes a config file, sets no new flag, and its `HOUSEGATE_SI_LANES=off` (Plan A2 chart) only sets `lanes: off`.

- [ ] **Step 6: Write the failing agent-build test**

```go
// agent_quickstart_build_test.go
package housegate

import (
	"context"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/config"
	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/registry"
)

// discoveringState is an in-memory registry that also answers discovery,
// standing in for RpcNetworkState.
type discoveringState struct{ *network.InMemoryNetworkState }

func (discoveringState) StorageIntegrityInfo(context.Context, string) (registry.StorageIntegrityInfo, error) {
	return registry.StorageIntegrityInfo{Enabled: true, NetworkID: "itest-net"}, nil
}
func (discoveringState) StorageIntegrityWriterCheck(context.Context, string, string) (bool, error) {
	return true, nil
}
func (discoveringState) StorageIntegrityTableStatus(context.Context, string, string) (registry.TableStatus, error) {
	return registry.TableStatus{Status: registry.TableStatusOrdinary}, nil
}

func agentSIConfig(t *testing.T) *config.Config {
	t.Helper()
	cfg := config.Default()
	cfg.Listen = "127.0.0.1:0"
	cfg.Agent.Mode = true
	cfg.Agent.PrivateKeyHex = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	cfg.Agent.Upstream = "127.0.0.1:1"
	cfg.StorageIntegrity.Agent.Enabled = true
	cfg.StorageIntegrity.Agent.RequireNetworkState = false
	cfg.StorageIntegrity.Agent.StateDir = t.TempDir()
	return &cfg
}

func TestBuildAgent_DiscoveryMakesNetworkIDOptional(t *testing.T) {
	cfg := agentSIConfig(t)
	bs, err := buildAgent(Options{Config: cfg, NetworkState: discoveringState{network.NewInMemoryNetworkState()}}, nil)
	if err != nil {
		t.Fatalf("buildAgent with a discovering registry and no network_id: %v", err)
	}
	bs.teardown()

	_, err = buildAgent(Options{Config: agentSIConfig(t), NetworkState: network.NewInMemoryNetworkState()}, nil)
	if err == nil || !strings.Contains(err.Error(), "network id is required") {
		t.Fatalf("err = %v, want the network-id refusal without discovery", err)
	}
}
```

Run: `bazel test //:housegate_test --test_filter='TestBuildAgent_DiscoveryMakesNetworkIDOptional' --test_output=errors`
Expected: FAIL (build opens a counter eagerly and requires `network_id`).

- [ ] **Step 7: Wire `buildAgentWithBuilders`**

Inside `if cfg.StorageIntegrity.Agent.Enabled {`:

```go
		statuses, err := resolveAgentTableStatuses(opts, reg)
		if err != nil {
			return nil, err
		}
		var discovery registry.StorageIntegrityDiscovery
		if d, ok := reg.(registry.StorageIntegrityDiscovery); ok && !isNilInterface(d) {
			discovery = d
			if statuses == registry.TableStatuses(reg) {
				// Spec 2026-10-09 §6.4: a burst of INSERTs costs one lookup.
				statuses = registry.NewCachedTableStatuses(statuses, 5*time.Second, nil)
			}
		}
		agentCfg := cfg.StorageIntegrity.Agent
		openSeq, stateLabel, err := agentSeqOpener(agentCfg.StateDir, signer.Address())
		if err != nil {
			return nil, fmt.Errorf("storage_integrity.agent: %w", err)
		}
```

and in `sistatement.Options` replace `Seq: seq` with:

```go
			OpenSeq:        openSeq,
			Discovery:      discovery,
			WriterPrecheck: !cfg.Agent.Driver,
			PinnedUpstream: cfg.Agent.Upstream != "",
			Dial: func(ctx context.Context, address string) (*chproto.Codec, error) {
				return dialRaw(ctx, address, cfg.DialTimeout.Duration)
			},
```

plus `Hosting: reg` when `reg != nil` (assign after the literal: `if reg != nil { siOpts.Hosting = reg }`, building the options in a variable `siOpts`). Delete the eager `sistatement.OpenSeqCounter` call; the startup log replaces `"seq_last", seq.Last()` with `"state_dir", stateLabel, "discovery", discovery != nil, "lanes", agentLanesLabel(agentCfg.Lanes)`. Add:

```go
// agentSeqOpener returns the legacy client_seq opener (plan decision P4): an
// explicit state_dir keeps <state_dir>/<signer>.seq for every network; the
// default lives in <base>/si/<network_id>/<signer>/, opened at the first SI
// write for that network.
func agentSeqOpener(stateDir, signer string) (func(string) (*sistatement.SeqCounter, error), string, error) {
	if strings.TrimSpace(stateDir) != "" {
		// One counter for every network: open it once and hand out the same
		// instance (its flock admits a single opener).
		var (
			once    sync.Once
			counter *sistatement.SeqCounter
			openErr error
		)
		return func(string) (*sistatement.SeqCounter, error) {
			once.Do(func() { counter, openErr = sistatement.OpenSeqCounter(stateDir, signer) })
			return counter, openErr
		}, stateDir, nil
	}
	home, _ := os.UserHomeDir()
	base, ok := config.DefaultAgentStateBase(runtime.GOOS, os.Getenv, home)
	if !ok {
		return nil, "", fmt.Errorf("state_dir is required on %s (no default state directory)", runtime.GOOS)
	}
	return func(networkID string) (*sistatement.SeqCounter, error) {
		return sistatement.OpenSeqCounter(config.AgentSIStateDir(base, networkID, signer), signer)
	}, filepath.Join(base, "si"), nil
}

func agentLanesLabel(lanes string) string {
	if lanes == "off" {
		return "off"
	}
	return "auto (legacy until the hosting indexer reports client lanes)"
}
```

(`os`, `runtime`, `sync`, `path/filepath` imports in `build.go` if missing.) `sistatement.Plugin.Close` may then close the shared counter once per network id it was handed out for; `SeqCounter.Close` is idempotent.

- [ ] **Step 8: Run the suites**

Run: `bazel test //:housegate_test //pkg/config:config_test //cmd:cmd_test //pkg/plugins/sistatement:sistatement_test --test_output=errors`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add pkg/config cmd/main.go build.go agent_quickstart_build_test.go BUILD.bazel
git commit -m "feat(agent): -agent-key quickstart with network presets, per-OS state dir, discovery and upstream-switch wiring (spec 2026-10-09 §6.4)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---
### Task 17: native materializer default for inline `VALUES`, read-mode injection, actionable messages

**Files:**
- Modify: `pkg/ffifetch/ffifetch.go` (`DefaultRelease`, `Supported`), `pkg/ffifetch/ffifetch_test.go`
- Create: `ffifetch_release_pin_test.go` (root package; reads `go.mod`), add `data = ["go.mod"]` to the root `go_test` in `BUILD.bazel`
- Modify: `pkg/plugins/materialize/config.go` (`Optional`)
- Modify: `pkg/config/agent_quickstart.go` (`AgentQuickstart.GOOS/GOARCH`, the `auto` / `on` inline branches), `pkg/config/agent_quickstart_test.go`
- Create: `pkg/plugins/sistatement/read_mode.go`, `pkg/plugins/sistatement/read_mode_test.go`
- Modify: `pkg/plugins/sistatement/plugin.go` (compression and settings messages, `:203-216`), `pkg/plugins/sistatement/plugin_test.go` (`TestPlugin_Rejections` expectations)
- Modify: `build.go` (`buildAgentWithBuilders`: optional materializer fallback; register `ReadModeInjector`)
- Test: `build_inline_values_test.go`

**Interfaces:**
- Consumes: `config.ApplyAgentQuickstart` (Task 16); `sicore.LeadingKeywords` (Task 15).
- Produces:
  - `const ffifetch.DefaultRelease = "v0.17.0"` (equal to the `github.com/housegate/rewriter-go` require in `go.mod`, pinned by a test) and `func ffifetch.Supported(goos, goarch string) bool`.
  - `materialize.Config.Optional bool` (`json:"-" yaml:"-"`): set only by the quickstart; a materializer that cannot be built then disables materialization and inline `VALUES` with a warning instead of failing startup.
  - `AgentQuickstart.GOOS`, `AgentQuickstart.GOARCH` (empty = `runtime.GOOS` / `runtime.GOARCH`).
  - `type sistatement.ReadModeInjector struct { Mode string }` (a `plugin.QueryPlugin`).

- [ ] **Step 1: Write the failing tests**

Append to `pkg/ffifetch/ffifetch_test.go`:

```go
func TestSupported(t *testing.T) {
	for platform, want := range map[[2]string]bool{
		{"linux", "amd64"}: true, {"darwin", "arm64"}: true,
		{"linux", "arm64"}: false, {"windows", "amd64"}: false,
	} {
		if got := Supported(platform[0], platform[1]); got != want {
			t.Errorf("Supported(%s/%s) = %v, want %v", platform[0], platform[1], got, want)
		}
	}
}
```

```go
// ffifetch_release_pin_test.go
package housegate

import (
	"os"
	"regexp"
	"testing"

	"github.com/housegate/housegate/pkg/ffifetch"
)

// The implicit native materializer fetches ffifetch.DefaultRelease; it must
// be the rewriter-go version the binary's Go binding was built against.
func TestFFIDefaultReleaseMatchesGoMod(t *testing.T) {
	raw, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`(?m)^\s*github\.com/housegate/rewriter-go (v\S+)`).FindSubmatch(raw)
	if m == nil {
		t.Fatal("go.mod has no rewriter-go require")
	}
	if string(m[1]) != ffifetch.DefaultRelease {
		t.Fatalf("ffifetch.DefaultRelease = %s, go.mod requires rewriter-go %s; bump them together", ffifetch.DefaultRelease, m[1])
	}
}
```

Append to `pkg/config/agent_quickstart_test.go`:

```go
func TestApplyAgentQuickstart_InlineValuesAuto(t *testing.T) {
	supported := quickstartBase()
	if err := ApplyAgentQuickstart(supported, AgentQuickstart{GOOS: "linux", GOARCH: "amd64"}); err != nil {
		t.Fatal(err)
	}
	m := supported.Materialize
	if !supported.StorageIntegrity.Agent.InlineValues.Enabled || !m.Enabled || m.Engine != "native" || m.NativeLibraryRelease != "v0.17.0" || !m.Optional {
		t.Fatalf("inline=%v materialize=%+v; want the optional native default", supported.StorageIntegrity.Agent.InlineValues.Enabled, m)
	}
	if err := supported.Validate(); err != nil {
		t.Fatalf("auto inline values must validate: %v", err)
	}

	unsupported := quickstartBase()
	if err := ApplyAgentQuickstart(unsupported, AgentQuickstart{GOOS: "linux", GOARCH: "arm64"}); err != nil {
		t.Fatal(err)
	}
	if unsupported.StorageIntegrity.Agent.InlineValues.Enabled || unsupported.Materialize.Enabled {
		t.Fatal("without a prebuilt engine inline values stay off")
	}

	explicit := quickstartBase()
	explicit.Materialize.Enabled = true
	explicit.Materialize.Engine = "grpc"
	explicit.Materialize.ServiceAddr = "127.0.0.1:50051"
	if err := ApplyAgentQuickstart(explicit, AgentQuickstart{GOOS: "linux", GOARCH: "amd64"}); err != nil {
		t.Fatal(err)
	}
	if explicit.Materialize.Engine != "grpc" || explicit.Materialize.Optional {
		t.Fatalf("an explicit materializer must be kept and stay fail-fast: %+v", explicit.Materialize)
	}

	forced := quickstartBase()
	if err := ApplyAgentQuickstart(forced, AgentQuickstart{GOOS: "linux", GOARCH: "arm64", SIInlineValues: "on"}); err != nil {
		t.Fatal(err)
	}
	if !forced.StorageIntegrity.Agent.InlineValues.Enabled || !forced.Materialize.Enabled || forced.Materialize.Optional {
		t.Fatalf("-si-inline-values on must enable a fail-fast materializer: %+v", forced.Materialize)
	}
}
```

```go
// pkg/plugins/sistatement/read_mode_test.go
package sistatement

import (
	"context"
	"testing"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/plugin"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

func TestReadModeInjector(t *testing.T) {
	inject := &ReadModeInjector{Mode: "safe"}
	for sql, want := range map[string]bool{
		"SELECT count() FROM devuser1.t":           true,
		"  -- c\nwith x AS (SELECT 1) SELECT * FROM x": true,
		"INSERT INTO devuser1.t FORMAT CSV":        false,
		"SHOW TABLES":                              false,
	} {
		q := &plugin.QueryContext{Query: &chproto.Query{Body: sql}}
		if err := inject.OnQuery(context.Background(), q); err != nil {
			t.Fatal(err)
		}
		got := len(q.Query.Settings) == 1 && q.Query.Settings[0].Key == sicore.ReadModeSettingKey && q.Query.Settings[0].Value == "'safe'" && q.Query.Settings[0].Custom
		if got != want {
			t.Errorf("%q: injected=%v settings=%+v, want %v", sql, got, q.Query.Settings, want)
		}
	}
	own := &plugin.QueryContext{Query: &chproto.Query{Body: "SELECT 1", Settings: []chproto.Setting{{Key: sicore.ReadModeSettingKey, Value: "'unsafe_latest'", Custom: true}}}}
	_ = inject.OnQuery(context.Background(), own)
	if len(own.Query.Settings) != 1 || own.Query.Settings[0].Value != "'unsafe_latest'" {
		t.Fatalf("a client-chosen read mode must win: %+v", own.Query.Settings)
	}
}
```

In `TestPlugin_Rejections` (`pkg/plugins/sistatement/plugin_test.go:365`) extend the compression case's expected substring to `"connect to 127.0.0.1 or pass --compression 0"` and the client-setting case's to `"~/.clickhouse-client/config.xml"`.

- [ ] **Step 2: Run them to verify they fail**

Run: `bazel run //:gazelle && bazel test //pkg/ffifetch:ffifetch_test //pkg/config:config_test //pkg/plugins/sistatement:sistatement_test //:housegate_test --test_filter='TestSupported|TestFFIDefaultReleaseMatchesGoMod|TestApplyAgentQuickstart_InlineValuesAuto|TestReadModeInjector|TestPlugin_Rejections' --test_output=errors`
Expected: FAIL to compile (`Supported`, `DefaultRelease`, `GOOS`, `Optional`, `ReadModeInjector` undefined).

- [ ] **Step 3: Implement**

`pkg/ffifetch/ffifetch.go`:

```go
// DefaultRelease is the rewriter-go release whose FFI library matches the Go
// binding this binary was built with (the go.mod require; a root test pins
// them together). The agent's implicit native materializer fetches it.
const DefaultRelease = "v0.17.0"

// Supported reports whether a prebuilt FFI library exists for the platform.
func Supported(goos, goarch string) bool {
	_, _, err := assetNameFor(goos, goarch)
	return err == nil
}
```

`pkg/plugins/materialize/config.go`, in `Config`:

```go
	// Optional is set only by the agent quickstart's implicit native
	// materializer (spec 2026-10-09 §6.4): if it cannot be built, the agent
	// disables materialization and inline VALUES with a warning. An explicit
	// materialize.enabled keeps the startup fail-fast. Never read from files.
	Optional bool `json:"-" yaml:"-"`
```

`pkg/config/agent_quickstart.go`: add `GOOS, GOARCH string` to `AgentQuickstart` (doc: "empty means the running platform") and replace the inline switch with:

```go
	goos, goarch := q.GOOS, q.GOARCH
	if goos == "" {
		goos = runtime.GOOS
	}
	if goarch == "" {
		goarch = runtime.GOARCH
	}
	switch inline {
	case "":
	case "auto":
		if cfg.StorageIntegrity.Agent.Enabled && ffifetch.Supported(goos, goarch) {
			cfg.StorageIntegrity.Agent.InlineValues.Enabled = true
			enableNativeMaterializer(cfg, true)
		}
	case "on":
		cfg.StorageIntegrity.Agent.InlineValues.Enabled = true
		enableNativeMaterializer(cfg, false)
	case "off":
		cfg.StorageIntegrity.Agent.InlineValues.Enabled = false
	default:
		return fmt.Errorf("-si-inline-values %q is invalid (want auto, on or off)", inline)
	}
	return nil
}

// enableNativeMaterializer turns on the in-process native materializer at the
// release matching this binary, unless the operator configured one.
func enableNativeMaterializer(cfg *Config, optional bool) {
	if cfg.Materialize.Enabled {
		return
	}
	cfg.Materialize.Enabled = true
	cfg.Materialize.Engine = rewriter.EngineNative
	if cfg.Materialize.NativeLibraryPath == "" && cfg.Materialize.NativeLibraryRelease == "" {
		cfg.Materialize.NativeLibraryRelease = ffifetch.DefaultRelease
	}
	cfg.Materialize.Optional = optional
}
```

(imports `runtime`, `github.com/housegate/housegate/pkg/ffifetch`; `rewriter` is already imported by `config.go` in this package.)

```go
// pkg/plugins/sistatement/read_mode.go
package sistatement

import (
	"context"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/plugin"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

// ReadModeInjector adds SQL_x_read_mode to SELECT / WITH statements that do
// not choose one (spec 2026-10-09 §6.4 -si-read-mode). Other statements and a
// client-chosen mode are left alone; the setting is owned, so the SI lane's
// empty-settings rule does not see it.
type ReadModeInjector struct {
	Mode string
}

func (r *ReadModeInjector) OnQuery(_ context.Context, qctx *plugin.QueryContext) error {
	if r == nil || r.Mode == "" || qctx == nil || qctx.Query == nil {
		return nil
	}
	for _, setting := range qctx.Query.Settings {
		if setting.Key == sicore.ReadModeSettingKey {
			return nil
		}
	}
	words, err := sicore.LeadingKeywords(qctx.Query.Body, 1)
	if err != nil || len(words) == 0 || (words[0] != "SELECT" && words[0] != "WITH") {
		return nil
	}
	qctx.Query.Settings = append(qctx.Query.Settings, chproto.Setting{Key: sicore.ReadModeSettingKey, Value: "'" + r.Mode + "'", Custom: true})
	return nil
}

var _ plugin.QueryPlugin = (*ReadModeInjector)(nil)
```

`pkg/plugins/sistatement/plugin.go`: the compression refusal becomes

```go
		return errors.New("storage_integrity agent rejects compressed INSERT payloads; retry with ClickHouse query compression disabled; clickhouse-client compresses by default only for non-local hosts — connect to 127.0.0.1 or pass --compression 0")
```

and the settings refusal wraps the core error:

```go
	if err := sicore.RejectUserSettings(keys); err != nil {
		return fmt.Errorf("%w (clickhouse-client also sends settings from ~/.clickhouse-client/config.xml)", err)
	}
```

`build.go` (`buildAgentWithBuilders`), replace the materializer block with:

```go
	inlineEnabled := cfg.StorageIntegrity.Agent.InlineValues.Enabled
	if cfg.Materialize.Enabled {
		m, err := materializerBuilder(cfg)
		switch {
		case err != nil && cfg.Materialize.Optional:
			// Spec 2026-10-09 §6.4: the implicit native default degrades to
			// streaming FORMAT … only; inline VALUES then gets the server's
			// actionable refusal.
			log.Warnw("agent: native engine unavailable; inline INSERT ... VALUES is disabled (streaming FORMAT inserts keep working)",
				"release", cfg.Materialize.NativeLibraryRelease, "err", err)
			inlineEnabled = false
		case err != nil:
			return nil, fmt.Errorf("materialize: %w", err) // startup fail-fast
		default:
			materializerClose = func() { _ = m.Close() }
			queryPlugins = append(queryPlugins, &materialize.Plugin{Materializer: m, Observer: obs})
			log.Infow("agent materialize enabled", "engine", cfg.Materialize.Engine, "implicit", cfg.Materialize.Optional)
		}
	}
	if mode := cfg.StorageIntegrity.Agent.ReadMode; mode != "" {
		queryPlugins = append(queryPlugins, &sistatement.ReadModeInjector{Mode: mode})
		log.Infow("agent read mode injected on SELECTs", "read_mode", mode)
	}
```

and the inline block uses `inlineEnabled` in place of `inlineCfg.Enabled` (both the evaluator construction and `sistatement.InlineValuesOptions.Enabled`). The startup prerequisite check at the top of the function stays on the configured values.

Add to `build_inline_values_test.go` (it already has a `materializerBuilder` stub pattern):

```go
func TestBuildAgent_OptionalMaterializerFailureDisablesInlineValues(t *testing.T) {
	cfg := agentSIConfig(t) // from agent_quickstart_build_test.go
	cfg.StorageIntegrity.Agent.NetworkID = "itest-net"
	cfg.Materialize.Enabled, cfg.Materialize.Engine, cfg.Materialize.Optional = true, "native", true
	cfg.StorageIntegrity.Agent.InlineValues.Enabled = true
	failing := func(*config.Config) (rewriter.Materializer, error) { return nil, errors.New("fetch failed") }
	bs, err := buildAgentWithMaterializerBuilder(Options{Config: cfg, NetworkState: network.NewInMemoryNetworkState()}, nil, failing)
	if err != nil {
		t.Fatalf("an optional materializer must not fail startup: %v", err)
	}
	bs.teardown()

	cfg.Materialize.Optional = false
	if _, err := buildAgentWithMaterializerBuilder(Options{Config: cfg, NetworkState: network.NewInMemoryNetworkState()}, nil, failing); err == nil {
		t.Fatal("an explicit materializer must stay fail-fast")
	}
}
```

In `BUILD.bazel` add `data = ["go.mod"],` to the root `go_test(name = "housegate_test", …)` (gazelle preserves it).

- [ ] **Step 4: Run the suites**

Run: `bazel test //pkg/ffifetch:ffifetch_test //pkg/config:config_test //pkg/plugins/sistatement:sistatement_test //:housegate_test --test_output=errors`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/ffifetch pkg/plugins/materialize/config.go pkg/config pkg/plugins/sistatement build.go build_inline_values_test.go ffifetch_release_pin_test.go BUILD.bazel
git commit -m "feat(agent): implicit native materializer for inline VALUES, -si-read-mode injection, actionable SI refusals (spec 2026-10-09 §6.4)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---
### Task 18: integration tests for the Phase-A scenarios (§9.2)

**Files:**
- Create: `pkg/integration/storage_integrity_open_writes_test.go` (part of the existing `//pkg/integration:integration_test` target, already listed in `.github/workflows/ci.yml` — no CI change)

**Interfaces:**
- Consumes: every earlier task; existing fixtures `siTenantDB`, `siTenantMock`, `withDeclaredSchema`, `siEventsPhysical`, `capturingConsumer` (`storage_integrity_agent_test.go`), `authProxyConfig` (`auth_test.go`), `statusRegistry` and `requireNativeLib` (`storage_integrity_table_state_test.go`, `table_reference_hardening_test.go`), `openConn` (`smoke_test.go`), `openConnNoDB` (`database_routing_test.go`); testenv `StartServerProxy`, `StartAgentProxy`, `StartAgentProxyWithSelector`, `WithPeerAt`, `WithLogicalDatabaseAt`, `WithDatabasePermission`, `WithExtraDatabases`, `WithConfigMutator`.
- Produces: tests only. Lane scenarios (two agents per key, lost state dir) and the compression scenario belong to Plan B / L1.

- [ ] **Step 1: Write the tests**

```go
// pkg/integration/storage_integrity_open_writes_test.go
package integration

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	housegate "github.com/housegate/housegate"
	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/config"
	"github.com/housegate/housegate/pkg/integration/testenv"
	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/registry"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/sitable"
	siplugin "github.com/housegate/housegate/pkg/plugins/storageintegrity"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
	pb "github.com/housegate/rewriter-proto/gen/pb"
)

// openWritesPair starts an SI ingress server with an EMPTY allowlist (spec
// 2026-10-09 D1) and an agent pinned to it. grant is the signer's (or
// owner's) permission on siTenantDB; serverMutate/agentMutate adjust the
// two configs.
func openWritesPair(t *testing.T, key string, consumer siplugin.AdmissionConsumer, grants map[string]registry.DbAuth, serverExtra []testenv.ProxyOption, agentMutate func(*config.Config)) *testenv.TestProxy {
	t.Helper()
	const networkID = "itest-open-writes"
	ch := openConn(t, chEnv.Addr)
	if err := ch.Exec(context.Background(), "CREATE TABLE IF NOT EXISTS "+siEventsPhysical()+" (id UInt64, region String) ENGINE = MergeTree ORDER BY id"); err != nil {
		t.Fatal(err)
	}
	opts := []testenv.ProxyOption{
		siTenantMock(t),
		testenv.WithExtraDatabases(siTenantDB),
		authProxyConfig(nil, false), // any signer authenticates; authorization is the writer predicate
		withDeclaredSchema(t, networkID),
		testenv.WithConfigMutator(func(cfg *config.Config) {
			cfg.Rewriter.PhysicalDatabase = chEnv.Database
			cfg.StorageIntegrity.Ingress.Enabled = true
			cfg.StorageIntegrity.Ingress.AllowedAddresses = nil
			cfg.StorageIntegrity.Ingress.NetworkID = networkID
		}),
		func(_ *config.Config, opts *housegate.Options) { opts.StorageIntegrityAdmissionConsumer = consumer },
	}
	for account, bits := range grants {
		opts = append(opts, testenv.WithDatabasePermission(account, siTenantDB, bits))
	}
	server := testenv.StartServerProxy(t, chEnv.Addr, append(opts, serverExtra...)...)
	return testenv.StartAgentProxy(t, key, server.Addr,
		withDeclaredSchema(t, networkID),
		testenv.WithConfigMutator(func(cfg *config.Config) {
			cfg.StorageIntegrity.Agent.Enabled = true
			cfg.StorageIntegrity.Agent.NetworkID = networkID
			cfg.StorageIntegrity.Agent.StateDir = t.TempDir()
			cfg.StorageIntegrity.Agent.RequireNetworkState = false
			if agentMutate != nil {
				agentMutate(cfg)
			}
		}),
	)
}

func sendTwoRows(t *testing.T, addr string) error {
	t.Helper()
	conn := openConnNoCompression(t, addr)
	batch, err := conn.PrepareBatch(context.Background(), "INSERT INTO "+siTenantDB+".si_events")
	if err != nil {
		return err
	}
	_ = batch.Append(uint64(1), "eu")
	_ = batch.Append(uint64(2), "us")
	return batch.Send()
}

func waitAdmissions(t *testing.T, c *capturingConsumer, n int) []siplugin.Admission {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		c.mu.Lock()
		seen := append([]siplugin.Admission(nil), c.seen...)
		c.mu.Unlock()
		if len(seen) >= n || time.Now().After(deadline) {
			if len(seen) != n {
				t.Fatalf("consumer saw %d admissions, want %d", len(seen), n)
			}
			return seen
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// §9.2 bullet 1: a writer that no allowlist names inserts; Read-only and
// denylisted signers are refused with 497.
func TestOpenWrites_WriterInsertsAndOthersAre497(t *testing.T) {
	signer, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("writer", func(t *testing.T) {
		consumer := &capturingConsumer{}
		agent := openWritesPair(t, authTestKey1, consumer, map[string]registry.DbAuth{signer.Address(): registry.DbAuthWrite}, nil, nil)
		if err := sendTwoRows(t, agent.Addr); err != nil {
			t.Fatalf("a non-allowlisted writer must insert: %v", err)
		}
		adm := waitAdmissions(t, consumer, 1)[0]
		if adm.Signer != signer.Address() || adm.Principal != signer.Address() || adm.Owner != "" {
			t.Fatalf("admission signer/owner/principal = %s/%s/%s", adm.Signer, adm.Owner, adm.Principal)
		}
	})
	for name, tc := range map[string]struct {
		grant  registry.DbAuth
		denied []string
		want   string
	}{
		"read-only grantee": {grant: registry.DbAuthRead, want: "is not a writer of database " + siTenantDB},
		"denylisted signer": {grant: registry.DbAuthWrite, denied: []string{signer.Address()}, want: "is not permitted to write storage-integrity tables"},
	} {
		t.Run(name, func(t *testing.T) {
			consumer := &capturingConsumer{}
			extra := []testenv.ProxyOption{testenv.WithConfigMutator(func(cfg *config.Config) {
				cfg.StorageIntegrity.Ingress.DeniedAddresses = tc.denied
			})}
			agent := openWritesPair(t, authTestKey1, consumer, map[string]registry.DbAuth{signer.Address(): tc.grant}, extra, nil)
			err := sendTwoRows(t, agent.Addr)
			if err == nil || !strings.Contains(err.Error(), "code: 497") || !strings.Contains(err.Error(), tc.want) || !strings.Contains(err.Error(), "[client_seq unspent]") {
				t.Fatalf("err = %v, want a marked 497 %q", err, tc.want)
			}
			if consumer.count() != 0 {
				t.Fatal("a refused write must not reach the consumer")
			}
		})
	}
}

// §9.2 bullet 1: an operator key writes for an owner that is a writer.
func TestOpenWrites_OperatorForAWriterOwner(t *testing.T) {
	operator, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatal(err)
	}
	const owner = "0x00000000000000000000000000000000000000c2"
	consumer := &capturingConsumer{}
	operatorRelation := func(_ *config.Config, opts *housegate.Options) {
		opts.NetworkState.(*network.InMemoryNetworkState).SetOperator(owner, network.AccountAddress(operator.Address()), true)
	}
	agent := openWritesPair(t, authTestKey1, consumer, map[string]registry.DbAuth{owner: registry.DbAuthOwner},
		[]testenv.ProxyOption{operatorRelation},
		func(cfg *config.Config) { cfg.Agent.Owner = owner })
	if err := sendTwoRows(t, agent.Addr); err != nil {
		t.Fatalf("operator for a writer owner must insert: %v", err)
	}
	adm := waitAdmissions(t, consumer, 1)[0]
	if adm.Signer != operator.Address() || adm.Owner != owner || adm.Principal != owner {
		t.Fatalf("admission signer/owner/principal = %s/%s/%s", adm.Signer, adm.Owner, adm.Principal)
	}
}

// refuseOnceConsumer answers the first admission with a marked 252, then
// captures.
type refuseOnceConsumer struct {
	capturingConsumer
	refuseMu sync.Mutex
	refused  string // the refused statement id
}

func (c *refuseOnceConsumer) ConsumeStorageIntegrityAdmission(ctx context.Context, adm siplugin.Admission) error {
	c.refuseMu.Lock()
	first := c.refused == ""
	if first {
		c.refused = adm.StatementID
	}
	c.refuseMu.Unlock()
	if first {
		return &chproto.ClientError{Code: chproto.CodeTooManyParts, Message: "storage_integrity: back-pressure: retry later", KeepSession: true, SeqUnspent: true}
	}
	return c.capturingConsumer.ConsumeStorageIntegrityAdmission(ctx, adm)
}

func (c *refuseOnceConsumer) refusedID() string {
	c.refuseMu.Lock()
	defer c.refuseMu.Unlock()
	return c.refused
}

// §9.2 bullet 4: a 252 back-pressure refusal is marked unspent; the agent
// recycles the seq and the retry reuses it, so no gap range opens.
func TestOpenWrites_BackpressureRecyclesTheSeq(t *testing.T) {
	signer, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatal(err)
	}
	consumer := &refuseOnceConsumer{}
	agent := openWritesPair(t, authTestKey1, consumer, map[string]registry.DbAuth{signer.Address(): registry.DbAuthWrite}, nil, nil)
	err = sendTwoRows(t, agent.Addr)
	if err == nil || !strings.Contains(err.Error(), "code: 252") || !strings.Contains(err.Error(), "[client_seq unspent]") {
		t.Fatalf("first insert err = %v, want the marked 252", err)
	}
	if err := sendTwoRows(t, agent.Addr); err != nil {
		t.Fatalf("retry: %v", err)
	}
	retried := waitAdmissions(t, &consumer.capturingConsumer, 1)[0]
	_, firstSeq, firstNonce, _ := sicore.ParseFlatStatementID(consumer.refusedID())
	_, retrySeq, retryNonce, _ := sicore.ParseFlatStatementID(retried.StatementID)
	if firstSeq != 1 || retrySeq != 1 || firstNonce == retryNonce {
		t.Fatalf("first=%s retry=%s; want seq 1 reused under a new nonce", consumer.refusedID(), retried.StatementID)
	}
}

func openConnDB(t *testing.T, addr, database string) clickhouse.Conn {
	t.Helper()
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr:        []string{addr},
		Auth:        clickhouse.Auth{Database: database, Username: chEnv.User, Password: chEnv.Password},
		Protocol:    clickhouse.Native,
		Compression: &clickhouse.Compression{Method: clickhouse.CompressionNone},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// §9.2 bullet 5: the agent starts on indexer 1, the INSERT's database lives
// on indexer 2; the session switches, the INSERT succeeds there, and a later
// unqualified SELECT runs on indexer 2 against the session database.
func TestOpenWrites_AgentSwitchesToTheHostingIndexer(t *testing.T) {
	signer, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatal(err)
	}
	const networkID = "itest-switch"
	ch := openConn(t, chEnv.Addr)
	if err := ch.Exec(context.Background(), "CREATE TABLE IF NOT EXISTS "+siEventsPhysical()+" (id UInt64, region String) ENGINE = MergeTree ORDER BY id"); err != nil {
		t.Fatal(err)
	}
	server := func(consumer siplugin.AdmissionConsumer) (*testenv.TestProxy, *testenv.RewriterMock) {
		rewriterOpt, mock := testenv.WithRewriterMock(t)
		mock.MapDatabase(siTenantDB, chEnv.Database)
		mock.SetAccessedTables("INSERT INTO "+siTenantDB+".si_events", []*pb.AccessedTable{{
			OriginalDatabase: siTenantDB, OriginalTable: "si_events", LogicalDatabase: siTenantDB, PhysicalDatabase: chEnv.Database, IsStorageIntegrity: true,
		}})
		return testenv.StartServerProxy(t, chEnv.Addr,
			rewriterOpt,
			testenv.WithExtraDatabases(siTenantDB),
			authProxyConfig(nil, false),
			testenv.WithDatabasePermission(signer.Address(), siTenantDB, registry.DbAuthWrite),
			withDeclaredSchema(t, networkID),
			testenv.WithConfigMutator(func(cfg *config.Config) {
				cfg.Rewriter.PhysicalDatabase = chEnv.Database
				cfg.StorageIntegrity.Ingress.Enabled = true
				cfg.StorageIntegrity.Ingress.NetworkID = networkID
			}),
			func(_ *config.Config, opts *housegate.Options) { opts.StorageIntegrityAdmissionConsumer = consumer },
		), mock
	}
	first, firstMock := server(&capturingConsumer{})
	hostingConsumer := &capturingConsumer{}
	hosting, hostingMock := server(hostingConsumer)
	agent := testenv.StartAgentProxyWithSelector(t, authTestKey1,
		testenv.WithPeerAt(1, first),
		testenv.WithPeerAt(2, hosting),
		testenv.WithLogicalDatabaseAt("home", 1),
		testenv.WithLogicalDatabaseAt(siTenantDB, 2),
		// The Selector's permissioned tier sees only indexer 1.
		testenv.WithDatabasePermission(signer.Address(), "home", registry.DbAuthRead),
		withDeclaredSchema(t, networkID),
		testenv.WithConfigMutator(func(cfg *config.Config) {
			cfg.StorageIntegrity.Agent.Enabled = true
			cfg.StorageIntegrity.Agent.NetworkID = networkID
			cfg.StorageIntegrity.Agent.StateDir = t.TempDir()
			cfg.StorageIntegrity.Agent.RequireNetworkState = false
		}),
	)
	conn := openConnDB(t, agent.Addr, siTenantDB)
	batch, err := conn.PrepareBatch(context.Background(), "INSERT INTO "+siTenantDB+".si_events")
	if err != nil {
		t.Fatalf("PrepareBatch: %v", err)
	}
	_ = batch.Append(uint64(7), "eu")
	if err := batch.Send(); err != nil {
		t.Fatalf("INSERT through the switched session: %v", err)
	}
	waitAdmissions(t, hostingConsumer, 1)
	if err := conn.Exec(context.Background(), "SELECT count() FROM si_events"); err != nil {
		t.Fatalf("unqualified SELECT after the switch: %v", err)
	}
	seenOn := func(m *testenv.RewriterMock, prefix string) bool {
		for _, sql := range m.SeenSQL() {
			if strings.HasPrefix(sql, prefix) {
				return true
			}
		}
		return false
	}
	if !seenOn(hostingMock, "INSERT INTO "+siTenantDB) || !seenOn(hostingMock, "SELECT count() FROM si_events") {
		t.Fatalf("hosting indexer saw %v", hostingMock.SeenSQL())
	}
	if seenOn(firstMock, "INSERT INTO "+siTenantDB) {
		t.Fatal("the INSERT must not run on indexer 1")
	}
}

// §9.2 bullet 6: on the SI host, a peer-trusted statement that reads the
// ordinary physical table of a governed table is refused by sipeerguard.
// The internal listener pre-flags every session as peer-trusted and
// non-forwarded, exactly like the measured remote() secondary query.
func TestOpenWrites_PeerGuardRefusesGovernedPhysicalReads(t *testing.T) {
	lib := requireNativeLib(t)
	const phys = "phys_pg"
	seed := openConnNoDB(t, chEnv.Addr)
	ctx := context.Background()
	for _, q := range []string{"DROP DATABASE IF EXISTS " + phys, "CREATE DATABASE " + phys,
		"CREATE TABLE " + phys + ".`pgdb.t` (id UInt64) ENGINE = MergeTree ORDER BY id"} {
		if err := seed.Exec(ctx, q); err != nil {
			t.Fatalf("seed %q: %v", q, err)
		}
	}
	t.Cleanup(func() { _ = seed.Exec(ctx, "DROP DATABASE IF EXISTS "+phys) })
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	internal := ln.Addr().String()
	_ = ln.Close()
	schema := payloadexec.TableSchema{TableID: "pgdb.t", Columns: []lthash.Column{{Name: "id", Type: "UInt64"}}}
	state := sitable.NewFake(sitable.Ordinary, sitable.Table{ID: "pgdb.t", Status: sitable.Active, Schema: schema, SchemaHash: payloadexec.TableSchemaHash("itest-pg", schema)})
	testenv.StartServerProxy(t, chEnv.Addr,
		testenv.WithExtraDatabases("pgdb"),
		testenv.WithConfigMutator(func(cfg *config.Config) {
			enabled := true
			cfg.InternalListen = internal
			cfg.Rewriter.Engine = "native"
			cfg.Rewriter.NativeLibraryPath = lib
			cfg.Rewriter.PhysicalDatabase = phys
			cfg.StorageIntegrity.Enabled = &enabled
		}),
		func(_ *config.Config, opts *housegate.Options) { opts.StorageIntegrityTableState = state },
	)
	conn := openConnDB(t, internal, phys)
	err = conn.Exec(ctx, "SELECT count() AS `count()` FROM `"+phys+"`.`pgdb.t` AS `__table1`")
	want := "storage_integrity: table pgdb.t is governed by storage integrity and must be read through its host indexer; connect with --database pgdb or USE pgdb"
	if err == nil || !strings.Contains(err.Error(), "code: 392") || !strings.Contains(err.Error(), want) {
		t.Fatalf("err = %v, want the 392 peer-guard refusal", err)
	}
	if err := conn.Exec(ctx, "SELECT 1"); err != nil {
		t.Fatalf("a non-governed peer statement must pass: %v", err)
	}
}
```

Add a `count()` method to `capturingConsumer` in `storage_integrity_agent_test.go`:

```go
func (c *capturingConsumer) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.seen)
}
```

- [ ] **Step 2: Run them**

Run: `bazel run //:gazelle && bazel test //pkg/integration:integration_test --test_filter='TestOpenWrites_' --test_env=POLYGLOT_SQL_FFI_PATH="$FFI" --test_output=errors`
Expected: PASS. If `TestOpenWrites_PeerGuardRefusesGovernedPhysicalReads` fails because the internal listener's session cannot authenticate to ClickHouse, add `testenv.WithCredentialReplace()` (it fills the real ClickHouse credentials after peer trust), as `internal_port_envelope_test.go` does.

- [ ] **Step 3: Run the whole integration suite against `origin/main`'s failing set**

Run: `bazel test //pkg/integration:integration_test //pkg/integration/testenv:testenv_test --test_env=POLYGLOT_SQL_FFI_PATH="$FFI" --test_output=errors 2>&1 | tail -40`
Expected: PASS, or failures identical to a clean `origin/main` run (CLAUDE.md baseline rule). Existing SI integration fixtures keep their `AllowedAddresses` and `WithDatabasePermission(..., DbAuthWrite/Owner)` grants, so the authorizer admits them; a fixture that signs with an account that has no grant on its database now gets 497 — give it the grant (that is the new contract, not a regression).

- [ ] **Step 4: Commit**

```bash
git add pkg/integration/storage_integrity_open_writes_test.go pkg/integration/storage_integrity_agent_test.go pkg/integration/BUILD.bazel
git commit -m "test(integration): open SI writes, 497 refusals, seq recycling, upstream switch, peer guard (spec 2026-10-09 §9.2)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---
### Task 19: documentation — CLAUDE.md, README, sample configs, inline-values guide

**Files:**
- Modify: `CLAUDE.md` (Key Modules: `pkg/chsession`, the `pkg/plugins/` list entries for `sistatement`, `storageintegrity`, `commitgate`, a new `sipeerguard` entry; the pipeline paragraph; a new "Plan decisions" pointer)
- Modify: `README.md` (`### auth`, `### storage_integrity`, `### 1.2 Agent Mode` and its flag table, a refusal-code list)
- Modify: `configs/local.server.yaml`, `configs/local.server-mock-remote.yaml` (commented ingress example), `configs/local.agent-rpc.yaml` (header comment)
- Modify: `docs/agent-inline-values.md` (`## Configuration`, `## Errors, sequence allocation and metrics`)

**Interfaces:**
- Consumes: the behaviour shipped by Tasks 1–18 and the plan decisions P1–P13 in this plan's header.
- Produces: documentation only. Markdown is never hard-wrapped.

- [ ] **Step 1: CLAUDE.md**

Make these edits (one paragraph per line, no wrapping):

1. `pkg/chsession` bullet — append: "`SwitchUpstream` is the neutral agent-side switch for storage-integrity INSERTs (spec 2026-10-09 D19): it replays the stored hello with the caller's `ProtocolVersion`/`Database`, refuses an upstream whose negotiated revision is below `ClientRevision` (`ErrUpstreamRevisionTooLow`), writes no peer/forward state, replays nothing, and owns the new codec (closed on every error)."
2. In the `sistatement` entry replace "recomputing `TableSchemaHash` with `storage_integrity.agent.network_id`" with "recomputing `TableSchemaHash` with the network id the hosting indexer reports through `sentio_getStorageIntegrityInfo` (or the configured `storage_integrity.agent.network_id`, which must match it)", and replace "persists durable `client_seq` in `<state_dir>/<account>.seq`" with "reserves `client_seq` only in `OnQueryInputCompleteStrict` (so client cancels, payload-limit and inline-evaluation refusals consume nothing) from `<state_dir>/<account>.seq` — or, without `state_dir`, `<per-OS base>/si/<network_id>/<signer>/<signer>.seq` — under an exclusive `flock` on `<account>.seq.lock` (`//go:build linux || darwin`; other platforms refuse), and returns a seq to the durable free list `<account>.seq.free` (cap 64, smallest reused first) when the upstream Exception ends with ` [client_seq unspent]`; before claiming it routes status and info lookups to the hosting indexer (`RpcNetworkState`, 5 s status cache), runs the advisory `sentio_isDatabaseWriter` pre-check (skipped for drivers; an RPC error falls through to the server) and switches the session to the hosting indexer with `Session.SwitchUpstream` unless `agent.upstream` is pinned, the session ran `SET`/`CREATE TEMPORARY`/`BEGIN`/`START TRANSACTION`, or its database lives on a third indexer (all refused locally, session-preserving)".
3. In the `storageintegrity` entry append: "Writes are authorized by `writeAuthorizer` (spec 2026-10-09 §6.2): denylisted signer, operator relation + denylisted owner (owner resolved from `SQL_x_payer` by the ingress itself), the optional legacy `allowed_addresses`, then the host's `registry.WriterAccess.IsDatabaseWriter(database, principal)` — the contract's `isDatabaseWriter`, no address(0) union, indexer signer counted — in `OnQuery` and again in `admissionFromState`; refusals are 497 `ACCESS_DENIED` and count in `storage_integrity_ingress_authz_total{result}`. `buildServer` refuses to start the ingress on a registry without `WriterAccess`. Every refusal that provably leaves the coordinate unspent (every pre-submission refusal of a token-bearing Query, every `admissionFromState` and pre-`Orchestrate` intake refusal, and coded arbiter terminal rejects other than `DUPLICATE_CLIENT_SEQ`, which name their code) carries `chproto.MarkSeqUnspent` / `ClientError.SeqUnspent`, rendered once by `exceptionForPluginError` as ` [client_seq unspent]`. `Options.StorageIntegrityWriteMeter` (optional) receives `OnStatementSequenced` asynchronously after every `OutcomeAccepted` submission."
4. In the `commitgate` entry append: "`network.PermissionCommitGateObserver` answers every Write-bit statement with `registry.WriterAccess` under `auth.writer_predicate: contract` (the default; startup refuses a registry without it and names `bitmap` as the escape hatch); `bitmap` keeps the stored bitmap with the address(0) union."
5. Add to the plugin list: "`sipeerguard` (server-mode, registered when `storage_integrity.enabled` and `rewriter.physical_database` is set; on peer-trusted, non-forwarded sessions it refuses with 392 any statement whose `<physical>.<quoted>` qualifier, standalone dotted quoted identifier, or carrier string literal (first carrier argument skipped) resolves — split at the first and the last `.` — to a table whose `sitable` status is not Ordinary, so a non-SI indexer's `remote()` read of an SI table fails loudly instead of reading the empty ordinary physical table; counter `storage_integrity_peer_guard_refusals_total`)".
6. Add a bullet after "Signed INSERT ... SELECT lane": "**User-run agents (spec 2026-10-09, release A1).** `-agent-key` alone implies agent mode when no config file is loaded and `-agent=false` is not given; the quickstart defaults (`-network devnet2` → `http://64.38.144.158:32003`, `-si auto`, `-si-inline-values auto` with an optional native materializer at `ffifetch.DefaultRelease`, listen `127.0.0.1:9000`) apply only without a config file (`config.ApplyAgentQuickstart`). Laned statement ids parse but are refused everywhere until Plan B (`sicore.ErrClientLanesNotEnabled`). Plan and its decisions: `docs/superpowers/plans/2026-10-09-user-agent-si-writes-a1-housegate.md`."

- [ ] **Step 2: README.md**

- `### auth` table: add a row `| auth.writer_predicate | string | No | contract | How INSERT/CREATE/DROP and the other Write-bit statements are authorized: contract uses the host's isDatabaseWriter (no address(0) Write wildcard; the indexer signer counts); bitmap keeps the stored bitmap with the address(0) union. Startup refuses contract on a registry without WriterAccess. |`
- `### storage_integrity`: after the dynamic-table-state paragraph add a paragraph: "**Who may write.** The signed ingress admits every writer of the target database: the indexer signer, or an account holding Owner or Write on it (Admin alone and address(0) grants do not count). `storage_integrity.ingress.denied_addresses` (lowercase addresses; restart to change) refuses a signer or a resolved owner; `allowed_addresses` is optional and, when set, still limits signers. Refusals are code 497 (`ACCESS_DENIED`): `storage_integrity: signer 0x… is not permitted to write storage-integrity tables`, `storage_integrity: owner 0x… is not permitted to write storage-integrity tables`, `storage_integrity: 0x… is not an operator of 0x…`, `storage_integrity: 0x… is not a writer of database <db>`. A refusal that provably did not spend the statement's `client_seq` ends with ` [client_seq unspent]`; the agent then reuses that seq. A peer-trusted read that names the ordinary physical table of a governed table is refused with 392 `… must be read through its host indexer; connect with --database <db> or USE <db>`." Add `denied_addresses: []` to the YAML example's `ingress:` block.
- `### 1.2 Agent Mode`: add a first subsection "Quick start (user agent)" with:

```bash
export HOUSEGATE_AGENT_KEY=0xYOUR_PRIVATE_KEY
housegate                                # agent mode, devnet2, 127.0.0.1:9000
clickhouse-client --host 127.0.0.1 --port 9000
```

and the sentence: "With only a key and no config file the agent joins `devnet2`, discovers the network id and table status from the indexer hosting each database, keeps its `client_seq` state under `~/.local/state/housegate` (Linux, or `$XDG_STATE_HOME/housegate`) or `~/Library/Application Support/housegate` (macOS), fetches the native engine for inline `VALUES` on linux/amd64 and darwin/arm64, and moves the session to the hosting indexer for SI INSERTs. Reads are not moved: connect with `--database <db>` to read an SI table. Billing: each INSERT is one query unit (two for inline `VALUES`), charged also when refused." Extend the flag table with `-network`, `-si`, `-si-state-dir`, `-si-lanes`, `-si-read-mode`, `-si-inline-values` (values and env names exactly as in Global Constraints) and change `-listen`'s default to "`127.0.0.1:9000` in agent mode without a config file, else `:9001`".

- [ ] **Step 3: Sample configs and the inline-values guide**

In `configs/local.server.yaml` and `configs/local.server-mock-remote.yaml`, below the commented `storage_integrity:` block add:

```yaml
#   ingress:
#     enabled: true
#     network_id: devnet2-si
#     allowed_addresses: []        # optional; empty admits every database writer
#     denied_addresses: []         # lowercase signer/owner addresses refused on the SI lane
# auth:
#   writer_predicate: contract     # contract (default) | bitmap (address(0) Write wildcard escape hatch)
```

In `configs/local.agent-rpc.yaml` add to the header comment: "Without a config file, `HOUSEGATE_AGENT_KEY=… housegate` gives the same agent against the devnet2 preset; see README §1.2."

In `docs/agent-inline-values.md`:
- `## Configuration`: add "Without a config file the agent enables this lane automatically on linux/amd64 and darwin/arm64 (`-si-inline-values auto`) with an implicit native materializer at the rewriter-go release matching the binary; if that library cannot be fetched the agent starts without inline `VALUES` and logs a warning. An explicit `materialize.enabled: true` keeps the startup fail-fast."
- `## Errors, sequence allocation and metrics`: replace the sentence "They run before a statement id is minted, so they consume no `client_seq`." with "They run before a statement id is minted, so they consume no `client_seq`; since release A1 no refusal before the strict input-complete hook consumes one (the seq is reserved there), and a server refusal marked ` [client_seq unspent]` returns the seq to the agent's free list."

- [ ] **Step 4: Verify the docs build-adjacent tests**

Run: `bazel test //pkg/config:config_test --test_filter='TestSampleConfig' --test_output=errors && git diff --stat`
Expected: PASS (commented YAML is ignored by the sample-config test); only documentation and config files in the diff.

- [ ] **Step 5: Commit**

```bash
git add CLAUDE.md README.md configs docs/agent-inline-values.md
git commit -m "docs: open SI writes, writer predicate, unspent marker, peer guard and the -agent-key quickstart (spec 2026-10-09 A1)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 6: Whole-branch verification**

Run:

```bash
bazel mod tidy && bazel run //:gazelle && git status --short
bazel test //... 2>&1 | tail -20
bazel test //pkg/integration:integration_test //pkg/integration/testenv:testenv_test --test_env=POLYGLOT_SQL_FFI_PATH="$FFI" --test_output=errors 2>&1 | tail -40
```

Expected: no gazelle drift; all unit targets pass; the integration suite passes or fails only where a clean `origin/main` build fails too.

- [ ] **Step 7: PR and merge (requires explicit user confirmation)**

Ask the user before each remote action. After approval: `git push -u origin feat/user-agent-si-writes-a1`, then `gh pr create --repo housegate/housegate --base main --title "feat: open SI writes, writer predicate, peer guard and the -agent-key quickstart (spec 2026-10-09 A1)" --body-file <scratchpad>/a1-pr-body.md`, where the body (written to the session's scratchpad directory first) lists the plan decisions P1–P13, the Phase-A scope, the verification of Step 6, and ends with `🤖 Generated with [Claude Code](https://claude.com/claude-code)`. Wait for CI (`Build`, `Integration (ClickHouse)`) and review; the user merges.

- [ ] **Step 8: Release `<A1_TAG>` (requires explicit user confirmation)**

Run the release workflow: `gh workflow run release.yml --repo housegate/housegate --ref main` (default `bump=auto`; never push a tag by hand). Record the produced tag as `<A1_TAG>` and its commit as `<A1_COMMIT>` (`git -C /Users/uranuswch/Dev/housegate/housegate fetch -q --tags && git -C /Users/uranuswch/Dev/housegate/housegate rev-parse "<A1_TAG>^{commit}"`). These are the only release placeholders and are unknown until the workflow runs; Plan A2 Task 4 and Plan A2 Task 11 (the user's `brew upgrade` agent) consume them, and Plan B's Plan A contract names them. Verify by content: `git -C /Users/uranuswch/Dev/housegate/housegate show "<A1_TAG>:pkg/registry/access.go" | grep -c 'type WriterAccess interface'` prints `1`. If only the Homebrew job fails, re-run that job alone (re-running the workflow cuts another tag).

---
## Self-review

**Spec coverage (release step A1).**

| Spec | Task |
|---|---|
| §6.1 shared grammar, legacy wrapper, local laned-id refusal | 1 (P1: lanes refused until Plan B; `ClientLane` proto mapping and `LANE_BUDGET_EXCEEDED` belong to B4 because arbiter-proto has neither yet) |
| §6.2 `WriterAccess`, in-memory implementation | 2 |
| §6.2 `denied_addresses`, optional `allowed_addresses`, `network_id` still required | 3 |
| §6.2 `writeAuthorizer`, 497 texts, `OnQuery` + `admissionFromState` re-check, forwarded sessions, startup refusal, audit log | 4 |
| §6.2 / R3 `auth.writer_predicate` (default `contract`, `bitmap` escape, startup refusal) | 3, 5 |
| §6.3 `sipeerguard` (three candidate forms, quoting variants, comments/heredocs, undecodable escapes, Ordinary/forwarded/non-peer pass, wiring condition) | 9 (P9) |
| §6.4 flags/env, presets, `-agent-key` implies agent mode, listen default, default state dir | 16 (P5, P6) |
| §6.4 discovery: storage RPC port, routed status with bootstrap fallback, 5 s cache, `sentio_getStorageIntegrityInfo`, failure modes, clock-skew warning | 13, 14 (P7, P13c) |
| §6.4 writer pre-check | 13, 14 (P2: `sentio_isDatabaseWriter` from Plan A2) |
| §6.4 `SwitchUpstream` and the switch rules (pinned upstream, server-side state, session database, revision guard, `dialRaw`, sticky, Relay resumes) | 10, 15, 16 (P10) |
| §6.4 materializer default, compression and settings messages, `-si-read-mode` | 17 |
| §6.5 flock + stub, durable directory, late reservation, free list and recycle on the legacy lane, burn accounting | 11, 12 (P3, P4, P8) |
| §6.6 typed flag, single renderer, Relay marking, ingress/intake marking, coded rejects named, matchers strip the suffix | 6, 7 |
| §6.9 `WriteMeter.OnStatementSequenced` | 7 (`StatementSeq`), 8 (P11, P13e) |
| §10 metrics for these parts | 4, 9, 12, 14, 15 (P8 defers lane metrics) |
| §9.1 / §9.2 tests for these parts | each task; 18 for §9.2 |
| Docs | 19 |

Deliberately not in A1 (spec §8.1): every Phase B item (B1–B6: laned ids end to end, `client_lane`, `ClientLanes` params, `LANE_BUDGET_EXCEEDED`, lane store under `<si_dir>/lanes/`, rotation, SDK lane rule R9, lane vectors and `auth.SharedStatementLaneVectorsSHA256`, in-flight cap), L1 (D20 compression termination), and the sentio-node / production / arbiter work (A2, A3, C1).

**Placeholder scan.** No "TBD", "similar to Task N" or code-free code steps. Two steps tell the implementer to adapt test fixtures whose shape the plan cannot see byte for byte (Task 4 Step 8, Task 5 Step 7: root tests that inject a non-network registry); each names the exact remedy and forbids a production fallback.

**Type consistency.** `sicore.StatementID{Account, Lane, Seq, Nonce}` and `ParseLegacyStatementID` (Task 1) are what Tasks 4 and 12 call. `registry.WriterAccess` / `OperatorChecker` (Task 2) are the types of `storageintegrity.Config.Writers/Operators` (Task 4) and `NewPermissionCommitGateObserverWithWriters` (Task 5). `chproto.MarkSeqUnspent`, `IsSeqUnspent`, `HasSeqUnspentSuffix`, `TrimSeqUnspentSuffix`, `SeqUnspentSuffix`, `ClientError.SeqUnspent` (Task 6) are used unchanged in Tasks 7, 12, 18. `SeqCounter.Reserve/Release/Close`, `MaxFreeSeqs`, `ErrSeqLocked` (Task 11) feed Task 12; Task 14 changes `reserveStatementID` and `releaseSeq` to take the counter, as its step says. `registry.StorageIntegrityDiscovery` (Task 13) is `sistatement.Options.Discovery` (Task 14) and is detected in `buildAgentWithBuilders` (Task 16). `sistatement.HostingResolver`, `Options.Dial`, `Options.PinnedUpstream` (Task 15) are set in Task 16. `materialize.Config.Optional` and `ffifetch.DefaultRelease/Supported` (Task 17) are consumed by `ApplyAgentQuickstart` in the same task.

**Review Focus coverage.** (1) payer casing and quoting: Task 4 matrix "operator for a writer owner" sends `'0X…'`; (2) revocation mid-upload: Task 4 `TestIngressAuthorizerRechecksBeforeAdmission`, marked by Task 7; (3) suffix once and matchers: Task 6 `already` case and `TestSessionPreservingIngressException_AcceptsMarker`; (4) two openers of the legacy counter and crash after free-list removal: Task 11 `TestSeqCounter_LockRefusesASecondOpener`, `TestSeqCounter_FreeRemovalIsDurableBeforeUse`; (5) `SET` then an INSERT elsewhere: Task 15 `TestSwitch_ServerSideStateRefusesLocally` (the refusal is an `OnQuery` error, which Relay answers without closing the session).
