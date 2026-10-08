# User-run agents writing SI tables — Plan A2: sentio-node, production chart and contract comment (Phase A + C1)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make sentio-node host the Phase-A open SI write lane (contract writer predicate on the registry HouseGate actually receives, `sentio_getStorageIntegrityInfo`, a writer pre-check RPC, HouseGate A1), ship the production chart guard and values that let indexer-a run with an empty ingress allowlist, fix the `isDatabaseWriter` NatSpec, and roll Phase A out on devnet2 with the §9.4 Phase-A end-to-end checks.

**Architecture:** sentio-node implements housegate's new `registry.WriterAccess` on both registry adapters (`FromStatecore` and the production-injected `RedisNetworkState`) through one shared function that reproduces `Databases.isDatabaseWriter` byte for byte over mirrored chain rows; the storage JSON-RPC gains `sentio_isDatabaseWriter` (the same predicate, for the agent's advisory pre-check) and `sentio_getStorageIntegrityInfo` (network id, limits, read mode, registry version, `client_lanes_enabled: false`). Then sentio-node bumps housegate to the A1 release, whose ingress authorizer and `auth.writer_predicate: contract` default refuse to start without `WriterAccess`. The production chart replaces the "allowlist must not be empty" guards with a `writerAuthorization` assertion pinned to reviewed images, renders `denied_addresses`, and the devnet2 overlay opens indexer-a in two steps (soak with the allowlist kept, then empty).

**Tech Stack:** Go 1.26 / Bazel 9.1.0 + Bzlmod (sentio-node), go-ethereum JSON-RPC, sentio-core statecore/statemirror, housegate (embedded, A1 release), Helm 3 templates + Python `unittest` render tests (production), Foundry (compute-network-contracts), kubectl/helmfile on `sentio-sea`.

**Spec:** `/Users/uranuswch/src/remotesrc/claude/housegate/spec-user-agent-si-writes/docs/superpowers/specs/2026-10-09-user-agent-si-writes-design.md` (binding; this plan implements its §6.7 Phase-A parts, §6.9 sentio-node side, §6.10, §6.12, §8.2 step 1 and the Phase-A steps of §9.4). Plan A1 (housegate) is a separate plan; this plan consumes its release.

## Global Constraints

- Phase A changes no consensus state: `client_lanes_enabled` is always `false`, no laned statement id is produced or accepted, no arbiter, voter or verifier image moves, and arbiter-core stays at v0.10.1 (B5 bumps it).
- Writer predicate is the contract's, exactly: "indexer signer, or the account's own Owner (0x08) or Write (0x02) bit. Admin alone and address(0) grants do not count" (spec §6.2, D2). No `expandAuth`, no wildcard merge.
- `HasPermission` and `PermissionsFor` stay unchanged (spec §6.7).
- Unknown or pending-delete database → `(false, error)` (spec §6.7).
- The denylist applies to the SI ingress lane only and is static configuration (restart to change) (D1, R7).
- sentio-node: every change lands through a PR, never a direct push to main; conventional commits with a scope (`feat(storage-integrity): …`, `chore(deps): …`); Bazel 9.1.0 is the test ground truth; after adding files or imports run `bazel run //:gazelle`.
- sentio-node pins housegate three times: `go.mod` `require`, `MODULE.bazel` `bazel_dep(name = "housegate", version = …)` and `git_override(module_name = "housegate", commit = …)`. All three move together; the `git_override` commit must equal `git -C /Users/uranuswch/Dev/housegate/housegate rev-parse <A1_TAG>^{commit}`, and `bazel mod show_repo housegate` must report it (housegate `.claude/skills/upgrade-dependency/SKILL.md`, Step 3).
- Never predict a release tag: `<A1_TAG>` / `<A1_COMMIT>` are set to the tag and dereferenced commit that Plan A1 Task 19 Step 8 records when the housegate release workflow cuts A1 (Tasks 1–3 may start before it, since they need only the interface shape; Task 4 and everything after it wait for that release); `<A2_COMMIT>` / `<A2_DIGEST>` to the sentio-node main-CI image built from the merged A2 PR; `<TEST_WRITER_1>` / `<TEST_WRITER_2>` / `<READER_KEY_ADDR>` to the public addresses generated in Task 10/12. These are the only placeholders in this plan and each is genuinely unknown until then. `<scratchpad>` is the executing session's scratchpad directory (temporary files, PR bodies), never a repository path.
- production: shared chart changes (`charts/`) go through a PR; single-service values (the devnet2 indexer-a overlays and their test mirrors) go straight to `main` after the chart PR merges. Render tests: `python3 -B charts/storage-integrity/tests/test_render.py` (this file also tests the sentio-node chart).
- Never print secrets: private keys, the docker registry password in `charts/sentio-node/values.yaml`, Secret contents. Keys generated for tests live in files with mode 0600 and are never echoed.
- Worktrees only through URWT/Worktrunk under `~/src/remotesrc/claude/<repo>/<task>`; never `git worktree add`; `command wt … --no-cd --format=json`.
- Commit trailer on every commit: `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. PR bodies end with `🤖 Generated with [Claude Code](https://claude.com/claude-code)`.
- Markdown is never hard-wrapped (one paragraph per line).
- Commands given to the user for their own terminal are fish syntax. Repository runbook text keeps the runbook's bash convention.
- Every live step (push to a shared branch, PR creation, `helmfile sync`, pod deletion, on-chain action, funds) requires explicit user confirmation in chat before it runs. Moving funds (funding a test key's Billing balance) is done by the user, never by Claude.

## Review Focus

- Mixed-case (EIP-55 checksummed) account strings: agents and `SQL_x_payer` may carry checksum case while the mirror keys permission rows and indexer signers by lowercase; `IsDatabaseWriter` must lowercase before every comparison and lookup (Task 1 table rows "checksummed owner" and "checksummed indexer signer").
- A database that is being dropped (`pendingDelete`) or no longer exists: the writer check must refuse with an error, never answer from a stale permission row (Task 1 rows "pending delete", "unknown database"; Task 2 error path).
- A corrupt or undecodable Redis mirror row: the production registry must return an error (the ingress refuses), not a silent `false` that looks like "not a writer" or a silent `true` (Task 1 `TestRedisNetworkStateIsDatabaseWriterRefusesUndecodableRows`).
- `sentio_getStorageIntegrityInfo` on a node whose table registry is disabled, before the follower's first answer, or on a node with no SI runtime: `si_indexer_id` must be `null` (not `0`, which is a valid id), `enabled` false without the runtime, and `self_indexer_id` must come from the resolved on-chain id (0 is valid) (Task 3 tests).
- An operator setting `writerAuthorization: true` with a sentio-node image that predates D17 (for example by rolling back only the image): that image silently ignores `denied_addresses` and admits any signature with an empty allowlist; the render test must refuse any devnet2 overlay whose image is not in the reviewed list, and rollback order must revert the allowlist before the image (Task 7 `test_writer_authorization_needs_a_reviewed_image`, Task 8 rollback text).

---

## File Structure

sentio-node (`/Users/uranuswch/src/remotesrc/claude/sentio-node/user-agent-si-writes-a2`):

| File | Responsibility |
|---|---|
| `standalone/networkstate/writer.go` (create) | `databaseWriter(indexerSigner, rawPermission, account)`: the contract predicate over one mirrored row, shared by both adapters |
| `standalone/networkstate/networkstate.go` (modify) | `FromStatecore.IsDatabaseWriter`; Task 4 adds the `registry.WriterAccess` assertion |
| `standalone/networkstate/redis.go` (modify) | `NewMirrored`, `RedisNetworkState.IsDatabaseWriter` and its three raw mirror reads, nil-safe `Close`; Task 4 assertion |
| `standalone/networkstate/writer_test.go` (create) | One conformance table run against both adapters over one mirrored fixture |
| `standalone/networkstate/convert.go` (modify, Task 4) | Project `StorageNodeRpcPort` into A1's `ProxyAddress.StorageRPCPort` |
| `standalone/networkstate/networkstate_test.go` (modify, Task 4) | Assert the new port |
| `rpc/storage_service.go` (modify) | Two optional sources on `StorageNodeService` (`writers`, `info`) |
| `rpc/database_writer.go` + `_test.go` (create) | `sentio_isDatabaseWriter` |
| `rpc/storage_integrity_info.go` + `_test.go` (create) | `StorageIntegrityInfo`, `sentio_getStorageIntegrityInfo` |
| `rpc/storage_rpc_call_test.go` (create) | Shared JSON-RPC test helper |
| `standalone/storage_integrity_info.go` (create) | Ingress settings for the info method and the runtime's `StorageIntegrityInfo` |
| `standalone/storage_integrity_table_state.go` (modify) | Carry network id and ingress settings; serve the info option |
| `standalone/standalone.go` (modify) | Pass the ingress settings into the table-state runtime |
| `standalone/storage_integrity_table_state_test.go` (modify) | Info tests; option count 2 |
| `go.mod`, `go.sum`, `MODULE.bazel` (modify, Task 4) | housegate `<A1_TAG>` |
| `config/config_test.go` (modify, Task 4) | `denied_addresses` passthrough |
| `standalone/storage_integrity_schemas_test.go` (modify, Task 4) | Open-ingress startup over the production registry type; refusal without `WriterAccess` |
| `storageintegrityadapter/write_meter.go` (create, Task 5) | `SafeWriteMeter` over HouseGate's `sicore.SIWriteEvent` (defined, not emitted) |
| `BUILD.bazel` files | Regenerated by gazelle |

production (`/Users/uranuswch/src/remotesrc/claude/production/si-open-writes-chart`, then `…/si-open-writes-devnet2`):

| File | Responsibility |
|---|---|
| `charts/sentio-node/templates/node.yaml` | `writerAuthorization` guard, `deniedAddresses` guards and rendering, sidecar `HOUSEGATE_SI_LANES` env and guard, comment block |
| `charts/sentio-node/values.yaml` | `deniedAddresses: []`, `writerAuthorization: false`, `housegateSidecar.storageIntegrity.lanes: "off"` |
| `charts/storage-integrity/tests/test_render.py` | `WRITER_AUTHORIZATION_IMAGES`, `DEVNET2_INGRESS_ALLOWED`, `WriterAuthorizationTest`, devnet2 image pin test |
| `docs/storage-integrity-devnet2-dynamic-table-set.md` | Stage 4 (4a/4b) runbook text |
| `k8s-sea/sentio-network-devnet2/sentio-node-indexer-a-storage-integrity-si-v2.yaml` (live) | `writerAuthorization`, `allowedAddresses`, `deniedAddresses` |
| `k8s-sea/sentio-network-devnet2/sentio-node-indexer-a-storage-integrity-si-v2-source-image.yaml` + `docs/examples/storage-integrity-fresh-source-image.yaml` (live) | sentio-node A2 image |

compute-network-contracts (`/Users/uranuswch/src/remotesrc/claude/compute-network-contracts/databases-writer-natspec`):

| File | Responsibility |
|---|---|
| `src/Databases.sol:373-387` | NatSpec of `isDatabaseWriter` |
| `test/Databases.t.sol` | Characterization test pinning "Admin alone and address(0) grants are not writers" |

Nothing is needed for storage RPC port exposure in production: the storage RPC (`node.ports.storage_rpc`, 32003/33003) and HouseGate ports are already publicly reachable on devnet2 (spec §3.4, R2). The sentio-node side of that item is the `StorageRPCPort` projection in Task 4.

---

## Part 1 — sentio-node

### Task 1: Contract writer predicate on both registry adapters

**Files:**
- Create: `standalone/networkstate/writer.go`
- Modify: `standalone/networkstate/networkstate.go` (add method after `IsOperator`)
- Modify: `standalone/networkstate/redis.go` (constructor, method, `Close`)
- Test: `standalone/networkstate/writer_test.go`

**Interfaces:**
- Consumes: sentio-core `statecore.State` (`GetDatabase`, `GetIndexerInfo`, `GetAccountDatabasePermissions`), `statemirror.Mirror.Get`, `statecore.NewStateMirrored`, `statemirror.NewFileMirror`.
- Produces: `func (a *FromStatecore) IsDatabaseWriter(database, account string) (bool, error)`; `func (r *RedisNetworkState) IsDatabaseWriter(database, account string) (bool, error)`; `func NewMirrored(mirror statemirror.Mirror) *RedisNetworkState`. Both methods match housegate A1's `registry.WriterAccess` shape (`IsDatabaseWriter(database, account string) (bool, error)`); Task 4 adds the compile-time assertion once A1 is in the module graph. Error text for a missing or pending-delete database: `database not found: <db>`.

- [ ] **Step 1: Create the sentio-node worktree**

Run (shell-neutral wrapper from the URWT skill):

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
' urwt /Users/uranuswch/Dev/sentio_xyz/sentio-node claude user-agent-si-writes-a2
```

Expected: one JSON object whose `path` is `/Users/uranuswch/src/remotesrc/claude/sentio-node/user-agent-si-writes-a2` and `action` is `created`. Every later sentio-node command runs with that path as its working directory. Re-read `CLAUDE.md` there.

- [ ] **Step 2: Write the failing conformance test**

Create `standalone/networkstate/writer_test.go`:

```go
package networkstate_test

import (
	"context"
	"testing"

	"compute-network-node/standalone/networkstate"

	"github.com/housegate/housegate/pkg/registry"
	"sentioxyz/sentio-core/common/statemirror"
	statecore "sentioxyz/sentio-core/network/state"

	"github.com/stretchr/testify/require"
)

// Addresses are stored lowercase, as the syncer's event handlers store them.
const (
	hostSigner    = "0xabcdef0000000000000000000000000000000001"
	otherSigner   = "0xabcdef0000000000000000000000000000000002"
	zeroAddress   = "0x0000000000000000000000000000000000000000"
	acctOwner     = "0xa000000000000000000000000000000000000001"
	acctWriter    = "0xa000000000000000000000000000000000000002"
	acctReadWrite = "0xa000000000000000000000000000000000000003"
	acctAdmin     = "0xa000000000000000000000000000000000000004"
	acctAdminRead = "0xa000000000000000000000000000000000000005"
	acctReader    = "0xa000000000000000000000000000000000000006"
	acctStranger  = "0xa000000000000000000000000000000000000007"
	acctGarbage   = "0xa000000000000000000000000000000000000008"
)

type databaseWriters interface {
	IsDatabaseWriter(database, account string) (bool, error)
}

// writerFixture models Databases.isDatabaseWriter's inputs: each database's
// indexer, that indexer's signer, and raw permission rows (decimal strings).
// address(0) holds Write|Owner on "tenant": a wildcard grant HasPermission
// unions in and the contract does not.
func writerFixture() *statecore.PlainState {
	return &statecore.PlainState{
		IndexerInfos: map[uint64]statecore.IndexerInfo{
			0: {IndexerId: 0, Signer: hostSigner},
			1: {IndexerId: 1, Signer: otherSigner},
			2: {IndexerId: 2, Signer: zeroAddress},
		},
		Databases: map[string]statecore.DatabaseInfo{
			"tenant":   {DatabaseId: "tenant", IndexerId: 0},
			"zerosig":  {DatabaseId: "zerosig", IndexerId: 2},
			"orphan":   {DatabaseId: "orphan", IndexerId: 9},
			"dropping": {DatabaseId: "dropping", IndexerId: 0, PendingDelete: true},
		},
		DatabasePermissions: map[string]map[string]string{
			acctOwner:     {"tenant": "8", "dropping": "8"},
			acctWriter:    {"tenant": "2", "orphan": "2"},
			acctReadWrite: {"tenant": "3"},
			acctAdmin:     {"tenant": "4"},
			acctAdminRead: {"tenant": "5"},
			acctReader:    {"tenant": "1"},
			acctGarbage:   {"tenant": "not-a-number"},
			zeroAddress:   {"tenant": "10"},
		},
	}
}

// writerImplementations returns both registry adapters over one fixture: the
// in-memory statecore adapter and the Redis-statemirror adapter standalone.Run
// injects into HouseGate, reading the mirror the statecore wrote.
func writerImplementations(t *testing.T) map[string]databaseWriters {
	t.Helper()
	mirror, err := statemirror.NewFileMirror(t.TempDir())
	require.NoError(t, err)
	state, err := statecore.NewStateMirrored(context.Background(), writerFixture(), mirror)
	require.NoError(t, err)
	return map[string]databaseWriters{
		"FromStatecore":     networkstate.New(state),
		"RedisNetworkState": networkstate.NewMirrored(mirror),
	}
}

func TestIsDatabaseWriterMatchesTheContract(t *testing.T) {
	cases := []struct {
		name     string
		database string
		account  string
		want     bool
		wantErr  string
	}{
		{name: "owner bit", database: "tenant", account: acctOwner, want: true},
		{name: "write bit", database: "tenant", account: acctWriter, want: true},
		{name: "read and write bits", database: "tenant", account: acctReadWrite, want: true},
		{name: "admin alone is not a writer", database: "tenant", account: acctAdmin},
		{name: "admin and read is not a writer", database: "tenant", account: acctAdminRead},
		{name: "read only", database: "tenant", account: acctReader},
		{name: "address(0) write and owner grants are not unioned", database: "tenant", account: acctStranger},
		{name: "signer of the hosting indexer", database: "tenant", account: hostSigner, want: true},
		{name: "checksummed indexer signer", database: "tenant", account: "0xABCDEF0000000000000000000000000000000001", want: true},
		{name: "checksummed owner", database: "tenant", account: "0xA000000000000000000000000000000000000001", want: true},
		{name: "signer of another indexer", database: "tenant", account: otherSigner},
		{name: "a zero indexer signer never matches", database: "zerosig", account: zeroAddress},
		{name: "unregistered hosting indexer falls back to bits", database: "orphan", account: acctWriter, want: true},
		{name: "unparsable permission row", database: "tenant", account: acctGarbage},
		{name: "unknown database", database: "nosuch", account: acctOwner, wantErr: "database not found: nosuch"},
		{name: "pending delete", database: "dropping", account: acctOwner, wantErr: "database not found: dropping"},
	}
	for implName, impl := range writerImplementations(t) {
		for _, tc := range cases {
			t.Run(implName+"/"+tc.name, func(t *testing.T) {
				got, err := impl.IsDatabaseWriter(tc.database, tc.account)
				if tc.wantErr != "" {
					require.ErrorContains(t, err, tc.wantErr)
					require.False(t, got)
					return
				}
				require.NoError(t, err)
				require.Equal(t, tc.want, got)
			})
		}
	}
}

// TestIsDatabaseWriterIgnoresTheWildcardThatHasPermissionHonours pins the one
// intended divergence (spec 2026-10-09 D2, §6.7): HasPermission keeps the
// address(0) union for reads and the ordinary path, the writer predicate does not.
func TestIsDatabaseWriterIgnoresTheWildcardThatHasPermissionHonours(t *testing.T) {
	mirror, err := statemirror.NewFileMirror(t.TempDir())
	require.NoError(t, err)
	state, err := statecore.NewStateMirrored(context.Background(), writerFixture(), mirror)
	require.NoError(t, err)
	adapter := networkstate.New(state)

	viaBitmap, err := adapter.HasPermission(acctStranger, "tenant", registry.Write)
	require.NoError(t, err)
	require.True(t, viaBitmap, "HasPermission unions address(0) grants")

	writer, err := adapter.IsDatabaseWriter("tenant", acctStranger)
	require.NoError(t, err)
	require.False(t, writer, "the contract does not union address(0) grants")
}

func TestRedisNetworkStateIsDatabaseWriterRefusesUndecodableRows(t *testing.T) {
	ctx := context.Background()
	for name, seed := range map[string]map[statemirror.OnChainKey]map[string]string{
		"database row": {
			statemirror.MappingDatabases: {"tenant": "{not json"},
		},
		"indexer row": {
			statemirror.MappingDatabases:    {"tenant": `{"databaseId":"tenant","indexerId":0}`},
			statemirror.MappingIndexerInfos: {"0": "{not json"},
		},
		"permission row": {
			statemirror.MappingDatabases:           {"tenant": `{"databaseId":"tenant","indexerId":0}`},
			statemirror.MappingIndexerInfos:        {"0": `{"indexerId":0,"signer":"` + hostSigner + `"}`},
			statemirror.MappingDatabasePermissions: {acctOwner: "{not json"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			mirror, err := statemirror.NewFileMirror(t.TempDir())
			require.NoError(t, err)
			for key, rows := range seed {
				require.NoError(t, mirror.Upsert(ctx, key, func(context.Context, statemirror.OnChainKey) (map[string]string, error) {
					return rows, nil
				}))
			}
			got, err := networkstate.NewMirrored(mirror).IsDatabaseWriter("tenant", acctOwner)
			require.ErrorContains(t, err, "decode", "a corrupt mirror row must refuse, not answer")
			require.False(t, got)
		})
	}
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `bazel test //standalone/networkstate:networkstate_test --test_output=errors`
Expected: build FAILS with `IsDatabaseWriter undefined` / `undefined: networkstate.NewMirrored` (gazelle may first be needed for the new test file; if Bazel reports the file is not in `srcs`, run `bazel run //:gazelle` and rerun).

- [ ] **Step 4: Implement the shared predicate**

Create `standalone/networkstate/writer.go`:

```go
package networkstate

import (
	"strconv"
	"strings"

	sentioregistry "sentioxyz/sentio-core/network/registry"
)

// zeroAddress is the contract's address(0). It is never a database's indexer
// signer, and its permission row is not unioned into anyone's writer check.
const zeroAddress = string(sentioregistry.WildcardAddress)

// databaseWriter is Databases.isDatabaseWriter
// (compute-network-contracts src/Databases.sol) over one mirrored row: account
// is the non-zero signer of the database's indexer, or its own permission row
// carries the Owner (0x08) or Write (0x02) bit. Admin (0x04) alone and grants
// held by address(0) do not count, and no hierarchy expansion applies, so an
// INSERT is admitted exactly when the contract would admit the same account's
// createTable / deleteTable (housegate spec 2026-10-09 D2, D17).
//
// account must already be lowercase. rawPermission is the decimal string the
// syncer's event handlers store; an empty or unparsable value grants nothing.
func databaseWriter(indexerSigner, rawPermission, account string) bool {
	signer := strings.ToLower(indexerSigner)
	if signer != "" && signer != zeroAddress && signer == account {
		return true
	}
	bits, err := strconv.ParseInt(rawPermission, 10, 64)
	if err != nil {
		return false
	}
	return sentioregistry.DbAuth(bits)&(sentioregistry.DbAuthOwner|sentioregistry.DbAuthWrite) != 0
}
```

- [ ] **Step 5: Implement `FromStatecore.IsDatabaseWriter`**

In `standalone/networkstate/networkstate.go`, after `IsOperator`, add:

```go
// --- registry.WriterAccess

// IsDatabaseWriter answers the contract's Databases.isDatabaseWriter over the
// in-memory chain mirror (see databaseWriter). An unknown or pending-delete
// database is an error, matching HasPermission's contract that a check against
// a missing database is a caller bug.
func (a *FromStatecore) IsDatabaseWriter(database, account string) (bool, error) {
	info, ok := a.s.GetDatabase(database)
	if !ok || info.PendingDelete {
		return false, fmt.Errorf("database not found: %s", database)
	}
	var signer string
	if indexer, ok := a.s.GetIndexerInfo(info.IndexerId); ok {
		signer = indexer.Signer
	}
	key := strings.ToLower(account)
	return databaseWriter(signer, a.s.GetAccountDatabasePermissions(key)[database], key), nil
}
```

- [ ] **Step 6: Implement the Redis adapter side**

In `standalone/networkstate/redis.go` replace `NewRedis` and `Close`, and add the method and its readers. Add `"strconv"` to the imports.

```go
// NewRedis wraps an existing redis client as a registry.Registry
// backed by the sentio-core statemirror. The client is retained for
// Close to tear down.
func NewRedis(client *redis.Client) (*RedisNetworkState, error) {
	state := NewMirrored(statemirror.NewRedisMirror(client))
	state.redisClient = client
	return state, nil
}

// NewMirrored returns the registry over an existing statemirror mirror. NewRedis
// uses it with the Redis mirror; tests use a file mirror. The value owns no
// Redis client, so its Close is a no-op.
func NewMirrored(mirror statemirror.Mirror) *RedisNetworkState {
	return &RedisNetworkState{
		dbRegistry: sentioregistry.NewDbRegistry(mirror),
		mirror:     mirror,
	}
}
```

```go
// --- registry.WriterAccess

// IsDatabaseWriter answers the contract's Databases.isDatabaseWriter from the
// same statemirror rows the rest of this registry reads (see databaseWriter).
// Any read or decode failure is an error so the ingress refuses rather than
// guessing.
func (r *RedisNetworkState) IsDatabaseWriter(database, account string) (bool, error) {
	ctx := context.Background()
	info, err := r.liveDatabase(ctx, database)
	if err != nil {
		return false, err
	}
	signer, err := r.indexerSigner(ctx, info.IndexerId)
	if err != nil {
		return false, err
	}
	key := strings.ToLower(account)
	perms, err := r.rawPermissions(ctx, key)
	if err != nil {
		return false, err
	}
	return databaseWriter(signer, perms[database], key), nil
}

func (r *RedisNetworkState) liveDatabase(ctx context.Context, database string) (statecore.DatabaseInfo, error) {
	value, ok, err := r.mirror.Get(ctx, statemirror.MappingDatabases, database)
	if err != nil {
		return statecore.DatabaseInfo{}, fmt.Errorf("read database %q: %w", database, err)
	}
	if !ok {
		return statecore.DatabaseInfo{}, fmt.Errorf("database not found: %s", database)
	}
	var info statecore.DatabaseInfo
	if err := json.Unmarshal([]byte(value), &info); err != nil {
		return statecore.DatabaseInfo{}, fmt.Errorf("decode database %q: %w", database, err)
	}
	if info.PendingDelete {
		return statecore.DatabaseInfo{}, fmt.Errorf("database not found: %s", database)
	}
	return info, nil
}

// indexerSigner returns "" for an unregistered indexer: the contract's
// getSigner answers address(0) there, which grants nothing.
func (r *RedisNetworkState) indexerSigner(ctx context.Context, indexerID uint64) (string, error) {
	field := strconv.FormatUint(indexerID, 10)
	value, ok, err := r.mirror.Get(ctx, statemirror.MappingIndexerInfos, field)
	if err != nil {
		return "", fmt.Errorf("read indexer %s: %w", field, err)
	}
	if !ok {
		return "", nil
	}
	var info statecore.IndexerInfo
	if err := json.Unmarshal([]byte(value), &info); err != nil {
		return "", fmt.Errorf("decode indexer %s: %w", field, err)
	}
	return info.Signer, nil
}

// rawPermissions reads the account's own permission row only: no address(0)
// merge and no hierarchy expansion (that is HasPermission's job).
func (r *RedisNetworkState) rawPermissions(ctx context.Context, account string) (map[string]string, error) {
	value, ok, err := r.mirror.Get(ctx, statemirror.MappingDatabasePermissions, account)
	if err != nil {
		return nil, fmt.Errorf("read permissions of %s: %w", account, err)
	}
	if !ok {
		return nil, nil
	}
	var perms map[string]string
	if err := json.Unmarshal([]byte(value), &perms); err != nil {
		return nil, fmt.Errorf("decode permissions of %s: %w", account, err)
	}
	return perms, nil
}
```

```go
// Close tears down the underlying Redis connection; a registry built by
// NewMirrored owns none.
func (r *RedisNetworkState) Close() error {
	if r.redisClient == nil {
		return nil
	}
	return r.redisClient.Close()
}
```

- [ ] **Step 7: Regenerate BUILD files and run the tests**

Run: `bazel run //:gazelle && bazel test //standalone/networkstate:networkstate_test --test_output=errors`
Expected: `//standalone/networkstate:networkstate_test PASSED`. If `TestIsDatabaseWriterMatchesTheContract/RedisNetworkState/...` fails on the `orphan` or `zerosig` rows while `FromStatecore` passes, the mirror field encoding differs from `strconv.FormatUint`; compare with `statecore.stringKeyMap` (`fmt.Sprintf("%d", k)`), which it must equal.

- [ ] **Step 8: Commit**

```bash
git add standalone/networkstate
git commit -m "feat(storage-integrity): contract isDatabaseWriter on both registry adapters

Implements Databases.isDatabaseWriter (indexer signer, own Owner or Write
bit; Admin alone and address(0) grants do not count) on FromStatecore and on
RedisNetworkState, the registry standalone.Run injects into HouseGate.
housegate spec 2026-10-09 D2/D17/§6.7. NewMirrored builds the Redis adapter
over any statemirror mirror.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 2: `sentio_isDatabaseWriter` for the agent's writer pre-check

The spec's agent pre-check (§6.4) names `sentio_getDatabaseInfoByAccount`, but that method returns the databases an account can *read* (bits merged with address(0) and expanded, `rpc/storage_service.go` `readableDatabases`), with no permission bits: a Read-only grantee is indistinguishable from a writer. This task adds the exact predicate as its own method; Plan A1's pre-check calls it (Plan A1 decision P2, Task 13 `RpcNetworkState.StorageIntegrityWriterCheck`, ratified in spec §4.5).

**Files:**
- Modify: `rpc/storage_service.go:22-27` (struct fields)
- Create: `rpc/database_writer.go`
- Create: `rpc/storage_rpc_call_test.go`
- Test: `rpc/database_writer_test.go`

**Interfaces:**
- Consumes: Task 1 `networkstate.New(statecore.State).IsDatabaseWriter`.
- Produces: JSON-RPC `sentio_isDatabaseWriter`, params `[database, account]`, result `bool`; errors `database and account are required`, `account "<x>" is not an address`, `database not found: <db>`, `database writer check is not available on this node`. Go: `type DatabaseWriterSource interface { IsDatabaseWriter(database, account string) (bool, error) }`, `func WithDatabaseWriters(source DatabaseWriterSource) StorageNodeOption`, test helper `callStorageRPC(t, url, method string, params ...any) (json.RawMessage, string)`.

- [ ] **Step 1: Write the shared test helper**

Create `rpc/storage_rpc_call_test.go`:

```go
package rpc

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

// callStorageRPC posts one JSON-RPC request and returns the raw result, or the
// error message when the server answered with an error object.
func callStorageRPC(t *testing.T, url, method string, params ...any) (json.RawMessage, string) {
	t.Helper()
	if params == nil {
		params = []any{}
	}
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	require.NoError(t, err)
	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	var envelope struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&envelope))
	if envelope.Error != nil {
		return nil, envelope.Error.Message
	}
	return envelope.Result, ""
}
```

- [ ] **Step 2: Write the failing test**

Create `rpc/database_writer_test.go`:

```go
package rpc

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

type fixedWriters map[string]bool

func (f fixedWriters) IsDatabaseWriter(database, account string) (bool, error) {
	if database == "nosuch" {
		return false, fmt.Errorf("database not found: %s", database)
	}
	return f[database+"/"+account], nil
}

func TestIsDatabaseWriterRPC(t *testing.T) {
	const writer = "0xa000000000000000000000000000000000000002"
	const reader = "0xa000000000000000000000000000000000000006"
	url := startStorageRPC(t, WithDatabaseWriters(fixedWriters{"tenant/" + writer: true}))

	raw, errMsg := callStorageRPC(t, url, "sentio_isDatabaseWriter", "tenant", writer)
	require.Empty(t, errMsg)
	require.JSONEq(t, "true", string(raw))

	raw, errMsg = callStorageRPC(t, url, "sentio_isDatabaseWriter", "tenant", reader)
	require.Empty(t, errMsg)
	require.JSONEq(t, "false", string(raw))

	for _, tc := range []struct {
		database, account, want string
	}{
		{"nosuch", writer, "database not found: nosuch"},
		{"", writer, "database and account are required"},
		{"tenant", "", "database and account are required"},
		{"tenant", "alice", `account "alice" is not an address`},
	} {
		_, errMsg = callStorageRPC(t, url, "sentio_isDatabaseWriter", tc.database, tc.account)
		require.Contains(t, errMsg, tc.want, tc)
	}
}

func TestIsDatabaseWriterRPCWithoutStateRefuses(t *testing.T) {
	_, errMsg := callStorageRPC(t, startStorageRPC(t), "sentio_isDatabaseWriter", "tenant", "0xa000000000000000000000000000000000000002")
	require.Contains(t, errMsg, "database writer check is not available on this node")
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `bazel run //:gazelle && bazel test //rpc:rpc_test --test_filter='TestIsDatabaseWriterRPC' --test_output=errors`
Expected: build FAILS with `undefined: WithDatabaseWriters`.

- [ ] **Step 4: Implement**

In `rpc/storage_service.go`, extend the struct:

```go
type StorageNodeService struct {
	syncer      *syncer.Syncer
	env         *common.NodeEnv
	decoder     *RelayErrorDecoder // eagerly constructed in NewStorageNodeService
	tableStatus StorageIntegrityTableStatusSource
	writers     DatabaseWriterSource
}
```

(Task 3 adds the `info` field.)

Create `rpc/database_writer.go`:

```go
package rpc

import (
	"context"
	"errors"
	"fmt"

	"compute-network-node/standalone/networkstate"

	gethcommon "github.com/ethereum/go-ethereum/common"
)

// DatabaseWriterSource answers the contract's Databases.isDatabaseWriter over
// this node's chain state. It is the predicate the embedded HouseGate's SI
// ingress authorizer applies (housegate spec 2026-10-09 D17).
type DatabaseWriterSource interface {
	IsDatabaseWriter(database, account string) (bool, error)
}

// WithDatabaseWriters serves sentio_isDatabaseWriter from source instead of
// the syncer's in-memory chain state.
func WithDatabaseWriters(source DatabaseWriterSource) StorageNodeOption {
	return func(s *StorageNodeService) { s.writers = source }
}

// IsDatabaseWriter reports whether account may write database by the
// contract's rule: the indexer signer of the database's indexer, or the
// account's own Owner or Write bit. A HouseGate agent calls it on the hosting
// indexer before claiming an SI INSERT (advisory pre-check, spec §6.4); the
// ingress authorizer stays authoritative.
//
// JSON-RPC method: sentio_isDatabaseWriter
//
//	params: [database, account]
//	result: bool
func (s *StorageNodeService) IsDatabaseWriter(_ context.Context, database, account string) (bool, error) {
	if database == "" || account == "" {
		return false, errors.New("database and account are required")
	}
	if !gethcommon.IsHexAddress(account) {
		return false, fmt.Errorf("account %q is not an address", account)
	}
	writers := s.writers
	if writers == nil {
		if s.syncer == nil {
			return false, errors.New("database writer check is not available on this node")
		}
		writers = networkstate.New(s.syncer.GetState())
	}
	return writers.IsDatabaseWriter(database, account)
}
```

- [ ] **Step 5: Run the tests**

Run: `bazel run //:gazelle && bazel test //rpc:rpc_test --test_output=errors`
Expected: `//rpc:rpc_test PASSED` (gazelle adds `//standalone/networkstate` to the `rpc` library deps).

- [ ] **Step 6: Commit**

```bash
git add rpc
git commit -m "feat(storage-integrity): sentio_isDatabaseWriter storage RPC

Serves the contract's isDatabaseWriter predicate for the HouseGate agent's
advisory writer pre-check (housegate spec 2026-10-09 §6.4). The existing
sentio_getDatabaseInfoByAccount returns readable databases without bits and
cannot tell a Read-only grantee from a writer.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 3: `sentio_getStorageIntegrityInfo`

**Files:**
- Modify: `rpc/storage_service.go` (add `info` field)
- Create: `rpc/storage_integrity_info.go`
- Test: `rpc/storage_integrity_info_test.go`
- Create: `standalone/storage_integrity_info.go`
- Modify: `standalone/storage_integrity_table_state.go:31-72,126-131`
- Modify: `standalone/standalone.go` (the `storageIntegrityTableStateDeps{…}` literal, ~line 383)
- Test: `standalone/storage_integrity_table_state_test.go` (new test; `require.Len(t, options, 1)` → `2`)

**Interfaces:**
- Consumes: `dataplane.RegistryView.View() (wire.TableRegistrySnapshot, bool)`; `snapshot.Params.SIIndexerID uint64`, `snapshot.Version uint64`; `common.IndexerRuntime.ResolvedIndexerID() (uint64, bool)`; housegate `config.StorageIntegrityConfig` (`Ingress.Enabled`, `Ingress.MaxPayloadBytes uint64`, `Ingress.MaxTokenAge config.Duration`, `Read.DefaultMode string`); `rewriter.ReadModeSafe`.
- Produces: `rpc.StorageIntegrityInfo` (JSON keys exactly `enabled, network_id, keeper_shard_id, si_indexer_id, self_indexer_id, client_lanes_enabled, ingress_max_payload_bytes, max_token_age_seconds, default_read_mode, registry_version, server_unix_time`), `rpc.StorageIntegrityInfoSource`, `rpc.WithStorageIntegrityInfo`; `(*storageIntegrityTableStateRuntime).StorageIntegrityInfo() rpc.StorageIntegrityInfo`; `storageIntegrityTableStateRuntime.storageRPCOptions()` now returns the status and info options (2). Semantics: `enabled` = this node runs the SI runtime **and** its HouseGate ingress is enabled; `si_indexer_id`/`registry_version` from the registry follower, `null`/`0` while the registry is disabled; `self_indexer_id` from the node's resolved on-chain id, `null` when unresolved (observer); `client_lanes_enabled` is `false` in Phase A; `default_read_mode` `""` normalizes to `safe`; `keeper_shard_id` is 0 (the SNode is built with `KeeperShardID: 0`).

- [ ] **Step 1: Write the failing RPC tests**

Create `rpc/storage_integrity_info_test.go`:

```go
package rpc

import (
	"encoding/json"
	"math/big"
	"testing"
	"time"

	"compute-network-node/common"

	"github.com/stretchr/testify/require"
)

type fixedStorageIntegrityInfo StorageIntegrityInfo

func (f fixedStorageIntegrityInfo) StorageIntegrityInfo() StorageIntegrityInfo {
	return StorageIntegrityInfo(f)
}

func getInfo(t *testing.T, url string) (map[string]any, int64, int64, int64) {
	t.Helper()
	before := time.Now().Unix()
	raw, errMsg := callStorageRPC(t, url, "sentio_getStorageIntegrityInfo")
	after := time.Now().Unix()
	require.Empty(t, errMsg)
	var got map[string]any
	require.NoError(t, json.Unmarshal(raw, &got))
	serverTime, ok := got["server_unix_time"].(float64)
	require.True(t, ok, string(raw))
	delete(got, "server_unix_time")
	return got, before, int64(serverTime), after
}

// TestStorageIntegrityInfoWireShape pins the exact JSON object of housegate
// spec 2026-10-09 §6.7: every field present, nothing else; the handler, not
// the source, owns self_indexer_id and server_unix_time.
func TestStorageIntegrityInfoWireShape(t *testing.T) {
	siIndexer, wrongSelf := uint64(0), uint64(99)
	env := &common.NodeEnv{IndexerRuntime: &common.IndexerRuntime{IndexerId: big.NewInt(0)}}
	server, err := NewStorageNodeRPCServer(nil, env, WithStorageIntegrityInfo(fixedStorageIntegrityInfo{
		Enabled: true, NetworkID: "devnet2", SIIndexerID: &siIndexer, SelfIndexerID: &wrongSelf,
		IngressMaxPayloadBytes: 67108864, MaxTokenAgeSeconds: 60, DefaultReadMode: "unsafe_latest",
		RegistryVersion: 42, ServerUnixTime: 1,
	}))
	require.NoError(t, err)
	url := serveForTest(t, server)

	got, before, serverTime, after := getInfo(t, url)
	require.GreaterOrEqual(t, serverTime, before)
	require.LessOrEqual(t, serverTime, after)
	require.Equal(t, map[string]any{
		"enabled": true, "network_id": "devnet2", "keeper_shard_id": float64(0),
		"si_indexer_id": float64(0), "self_indexer_id": float64(0),
		"client_lanes_enabled": false, "ingress_max_payload_bytes": float64(67108864),
		"max_token_age_seconds": float64(60), "default_read_mode": "unsafe_latest",
		"registry_version": float64(42),
	}, got)
}

func TestStorageIntegrityInfoWithoutStorageIntegrity(t *testing.T) {
	got, _, _, _ := getInfo(t, startStorageRPC(t))
	require.Equal(t, map[string]any{
		"enabled": false, "network_id": "", "keeper_shard_id": float64(0),
		"si_indexer_id": nil, "self_indexer_id": nil,
		"client_lanes_enabled": false, "ingress_max_payload_bytes": float64(0),
		"max_token_age_seconds": float64(0), "default_read_mode": "",
		"registry_version": float64(0),
	}, got, "a node without the SI runtime answers enabled:false; an unresolved id is null, not 0")
}
```

In `rpc/storage_integrity_status_test.go`, split `startStorageRPC` so the info test can pass an env:

```go
func startStorageRPC(t *testing.T, opts ...StorageNodeOption) string {
	t.Helper()
	server, err := NewStorageNodeRPCServer(nil, nil, opts...)
	require.NoError(t, err)
	return serveForTest(t, server)
}

func serveForTest(t *testing.T, server *http.Server) string {
	t.Helper()
	httpServer := httptest.NewServer(server.Handler)
	t.Cleanup(httpServer.Close)
	return httpServer.URL
}
```

- [ ] **Step 2: Run the RPC tests to verify they fail**

Run: `bazel run //:gazelle && bazel test //rpc:rpc_test --test_filter='TestStorageIntegrityInfo' --test_output=errors`
Expected: build FAILS with `undefined: StorageIntegrityInfo`.

- [ ] **Step 3: Implement the RPC side**

Add `info StorageIntegrityInfoSource` as the last field of `StorageNodeService` in `rpc/storage_service.go`. Create `rpc/storage_integrity_info.go`:

```go
package rpc

import (
	"context"
	"time"
)

// StorageIntegrityInfo is what a HouseGate agent needs before it signs an SI
// INSERT for a database this node hosts (housegate spec 2026-10-09 §6.4, §6.7).
// Pointers render as JSON null: indexer id 0 is valid, so "unknown" must not
// look like 0.
type StorageIntegrityInfo struct {
	Enabled                bool    `json:"enabled"`
	NetworkID              string  `json:"network_id"`
	KeeperShardID          uint32  `json:"keeper_shard_id"`
	SIIndexerID            *uint64 `json:"si_indexer_id"`
	SelfIndexerID          *uint64 `json:"self_indexer_id"`
	ClientLanesEnabled     bool    `json:"client_lanes_enabled"`
	IngressMaxPayloadBytes uint64  `json:"ingress_max_payload_bytes"`
	MaxTokenAgeSeconds     int64   `json:"max_token_age_seconds"`
	DefaultReadMode        string  `json:"default_read_mode"`
	RegistryVersion        uint64  `json:"registry_version"`
	ServerUnixTime         int64   `json:"server_unix_time"`
}

// StorageIntegrityInfoSource supplies the storage-integrity part of the answer.
type StorageIntegrityInfoSource interface {
	StorageIntegrityInfo() StorageIntegrityInfo
}

// WithStorageIntegrityInfo serves sentio_getStorageIntegrityInfo from source.
// Without it the method answers enabled:false.
func WithStorageIntegrityInfo(source StorageIntegrityInfoSource) StorageNodeOption {
	return func(s *StorageNodeService) { s.info = source }
}

// GetStorageIntegrityInfo answers sentio_getStorageIntegrityInfo. The node's
// own indexer id and clock always come from this handler: the agent warns
// when its clock is more than 5 s from server_unix_time, because the ingress
// refuses tokens issued in the future.
//
// JSON-RPC method: sentio_getStorageIntegrityInfo
//
//	params: []
//	result: StorageIntegrityInfo
func (s *StorageNodeService) GetStorageIntegrityInfo(context.Context) (StorageIntegrityInfo, error) {
	var info StorageIntegrityInfo
	if s.info != nil {
		info = s.info.StorageIntegrityInfo()
	}
	info.SelfIndexerID = nil
	if s.env != nil {
		if id, ok := s.env.IndexerRuntime.ResolvedIndexerID(); ok {
			info.SelfIndexerID = &id
		}
	}
	info.ServerUnixTime = time.Now().Unix()
	return info, nil
}
```

- [ ] **Step 4: Run the RPC tests**

Run: `bazel test //rpc:rpc_test --test_output=errors`
Expected: `//rpc:rpc_test PASSED`.

- [ ] **Step 5: Write the failing runtime test**

Append to `standalone/storage_integrity_table_state_test.go` (and change `require.Len(t, options, 1)` in `TestStorageIntegrityTableStateServesTheStatusMethod` to `require.Len(t, options, 2)`):

```go
func TestStorageIntegrityTableStateServesTheInfoMethod(t *testing.T) {
	siIndexer := uint64(7)
	ingress := housegateConfig.Default().StorageIntegrity
	ingress.Ingress.Enabled = true
	ingress.Ingress.MaxPayloadBytes = 1 << 20
	ingress.Ingress.MaxTokenAge = housegateConfig.Duration{Duration: 90 * time.Second}
	ingress.Read.DefaultMode = ""
	for name, tc := range map[string]struct {
		follower       *scriptedRegistryFollower
		ingressEnabled bool
		want           rpc.StorageIntegrityInfo
	}{
		"registry disabled": {
			follower:       newScriptedRegistryFollower(wire.TableRegistrySnapshot{}, false, true),
			ingressEnabled: true,
			want: rpc.StorageIntegrityInfo{Enabled: true, NetworkID: "net", IngressMaxPayloadBytes: 1 << 20,
				MaxTokenAgeSeconds: 90, DefaultReadMode: "safe"},
		},
		"registry enabled": {
			follower:       newScriptedRegistryFollower(activeTenantRegistry(t), true, true),
			ingressEnabled: true,
			want: rpc.StorageIntegrityInfo{Enabled: true, NetworkID: "net", SIIndexerID: &siIndexer,
				IngressMaxPayloadBytes: 1 << 20, MaxTokenAgeSeconds: 90, DefaultReadMode: "safe", RegistryVersion: 5},
		},
		"ingress disabled": {
			follower: newScriptedRegistryFollower(activeTenantRegistry(t), true, true),
			want: rpc.StorageIntegrityInfo{NetworkID: "net", SIIndexerID: &siIndexer,
				IngressMaxPayloadBytes: 1 << 20, MaxTokenAgeSeconds: 90, DefaultReadMode: "safe", RegistryVersion: 5},
		},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := ingress
			cfg.Ingress.Enabled = tc.ingressEnabled
			deps := testTableStateDeps(tc.follower)
			deps.ingress = storageIntegrityIngressInfoFrom(cfg)
			runtime, err := newStorageIntegrityTableStateRuntime(deps)
			require.NoError(t, err)

			service := rpc.NewStorageNodeService(nil, nil, runtime.storageRPCOptions()...)
			got, err := service.GetStorageIntegrityInfo(t.Context())
			require.NoError(t, err)
			tc.want.ServerUnixTime = got.ServerUnixTime
			require.Equal(t, tc.want, got)
			require.False(t, got.ClientLanesEnabled, "Phase A never advertises lanes")
		})
	}
}
```

Add `housegateConfig "github.com/housegate/housegate/pkg/config"` to that file's imports.

- [ ] **Step 6: Run it to verify it fails**

Run: `bazel run //:gazelle && bazel test //standalone:standalone_test --test_filter='TestStorageIntegrityTableStateServes' --test_output=errors`
Expected: build FAILS with `deps.ingress undefined` / `undefined: storageIntegrityIngressInfoFrom`.

- [ ] **Step 7: Implement the runtime side**

Create `standalone/storage_integrity_info.go`:

```go
package standalone

import (
	"time"

	"compute-network-node/rpc"

	housegateConfig "github.com/housegate/housegate/pkg/config"
	"github.com/housegate/housegate/pkg/rewriter"
)

// storageIntegrityIngressInfo is the part of sentio_getStorageIntegrityInfo
// that comes from the embedded HouseGate's configuration.
type storageIntegrityIngressInfo struct {
	enabled         bool
	maxPayloadBytes uint64
	maxTokenAge     time.Duration
	defaultReadMode string
}

func storageIntegrityIngressInfoFrom(cfg housegateConfig.StorageIntegrityConfig) storageIntegrityIngressInfo {
	mode := cfg.Read.DefaultMode
	if mode == "" {
		mode = string(rewriter.ReadModeSafe) // HouseGate's default when unset
	}
	return storageIntegrityIngressInfo{
		enabled:         cfg.Ingress.Enabled,
		maxPayloadBytes: cfg.Ingress.MaxPayloadBytes,
		maxTokenAge:     cfg.Ingress.MaxTokenAge.Duration,
		defaultReadMode: mode,
	}
}

var _ rpc.StorageIntegrityInfoSource = (*storageIntegrityTableStateRuntime)(nil)

// StorageIntegrityInfo answers sentio_getStorageIntegrityInfo for a node that
// runs the storage-integrity runtime (housegate spec 2026-10-09 §6.7). Enabled
// means this node admits signed SI writes: the runtime exists and the ingress
// is on.
func (r *storageIntegrityTableStateRuntime) StorageIntegrityInfo() rpc.StorageIntegrityInfo {
	info := rpc.StorageIntegrityInfo{
		Enabled:   r.ingress.enabled,
		NetworkID: r.networkID,
		// The SNode is built with KeeperShardID 0 (v1).
		KeeperShardID: 0,
		// Phase A: lanes are activated by a consensus parameter this
		// arbiter-core does not carry yet; Phase B reads it from the registry
		// snapshot (client_lanes != nil).
		ClientLanesEnabled:     false,
		IngressMaxPayloadBytes: r.ingress.maxPayloadBytes,
		MaxTokenAgeSeconds:     int64(r.ingress.maxTokenAge / time.Second),
		DefaultReadMode:        r.ingress.defaultReadMode,
	}
	if snapshot, enabled := r.follower.View(); enabled {
		id := snapshot.Params.SIIndexerID
		info.SIIndexerID = &id
		info.RegistryVersion = snapshot.Version
	}
	return info
}
```

In `standalone/storage_integrity_table_state.go`:

```go
type storageIntegrityTableStateDeps struct {
	follower  storageIntegrityRegistryFollower
	readiness func(tableID string) bool
	chain     tablestate.DatabaseInfoReader
	genesis   []payloadexec.TableSchema
	networkID string
	logger    *slog.Logger
	// ingress is what sentio_getStorageIntegrityInfo reports from HouseGate's
	// storage_integrity config.
	ingress storageIntegrityIngressInfo
	// … existing reconciler, env, declarations, physicalDatabase fields unchanged
}

type storageIntegrityTableStateRuntime struct {
	readyTimeout time.Duration
	follower     storageIntegrityRegistryFollower
	state        *tablestate.State
	metrics      *tablestate.Metrics
	reconciler   storageIntegrityReconcilerStats
	compensator  *database_registry.SchemaCompensator
	networkID    string
	ingress      storageIntegrityIngressInfo
}
```

In `newStorageIntegrityTableStateRuntime`, set `networkID: deps.networkID, ingress: deps.ingress` in the `runtime := &storageIntegrityTableStateRuntime{…}` literal. Replace `storageRPCOptions`:

```go
// storageRPCOptions serve sentio_getStorageIntegrityTableStatus from the table
// state and sentio_getStorageIntegrityInfo from this runtime; without a runtime
// the methods answer "ordinary" and enabled:false, which is right only for a
// node without storage integrity.
func (r *storageIntegrityTableStateRuntime) storageRPCOptions() []rpc.StorageNodeOption {
	if r == nil {
		return nil
	}
	return []rpc.StorageNodeOption{
		rpc.WithStorageIntegrityTableStatus(r.state),
		rpc.WithStorageIntegrityInfo(r),
	}
}
```

In `standalone/standalone.go`, in the `newStorageIntegrityTableStateRuntime(storageIntegrityTableStateDeps{…})` literal, add after `networkID: si.SNode.NetworkID,`:

```go
				ingress:          storageIntegrityIngressInfoFrom(cfg.Housegate.StorageIntegrity),
```

- [ ] **Step 8: Run the tests**

Run: `bazel run //:gazelle && bazel test //rpc:rpc_test //standalone:standalone_test --test_output=errors`
Expected: both PASSED (the `_ch_test` files self-skip without `SENTIO_SI_CH_E2E=1`).

- [ ] **Step 9: Commit**

```bash
git add rpc standalone
git commit -m "feat(storage-integrity): sentio_getStorageIntegrityInfo storage RPC

Answers network id, keeper shard, SI indexer and registry version from the
table-registry follower, the node's own indexer id, ingress payload and token
limits, the default read mode and the server clock; client_lanes_enabled is
false in Phase A (housegate spec 2026-10-09 §6.4, §6.7).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 4: Bump housegate to the A1 release

**Files:**
- Modify: `go.mod`, `go.sum`, `MODULE.bazel:16-19,29-34`
- Modify: `standalone/networkstate/networkstate.go` (assertions), `standalone/networkstate/redis.go` (assertions), `standalone/networkstate/convert.go:23-28`
- Test: `standalone/networkstate/networkstate_test.go`, `config/config_test.go`, `standalone/storage_integrity_schemas_test.go`

**Interfaces:**
- Consumes (A1, names fixed by spec §6.2/§6.4): `registry.WriterAccess` with `IsDatabaseWriter(database, account string) (bool, error)`; `config.StorageIntegrityIngressConfig.DeniedAddresses []string` (`yaml:"denied_addresses"`); `registry.ProxyAddress.StorageRPCPort uint16`; A1 startup refusal text containing `requires a registry that implements WriterAccess`; `auth.writer_predicate` default `contract`.
- Produces: sentio-node built against `<A1_TAG>`; compile-time proof that both adapters implement `registry.WriterAccess`.

- [ ] **Step 1: Resolve and verify the A1 release by content**

Run:

```bash
git -C /Users/uranuswch/Dev/housegate/housegate fetch -q --tags origin
gh release view <A1_TAG> --repo housegate/housegate --json tagName,publishedAt,body --jq '.tagName, .publishedAt'
git -C /Users/uranuswch/Dev/housegate/housegate rev-parse '<A1_TAG>^{commit}'
git -C /Users/uranuswch/Dev/housegate/housegate show '<A1_TAG>:pkg/registry/access.go' | grep -n 'type WriterAccess interface' -A2
git -C /Users/uranuswch/Dev/housegate/housegate show '<A1_TAG>:pkg/config/storage_integrity_config.go' | grep -n 'DeniedAddresses'
git -C /Users/uranuswch/Dev/housegate/housegate show '<A1_TAG>:pkg/registry/topology.go' | grep -n 'StorageRPCPort'
git -C /Users/uranuswch/Dev/housegate/housegate grep -n 'requires a registry that implements WriterAccess' '<A1_TAG>' -- build.go
```

Expected: the tag exists; the commit hash is printed (record it as `<A1_COMMIT>`); each grep prints one or more lines. If a name differs, stop: the spec fixes these names, so the difference is an A1 defect to raise with the A1 owner. Also read the release body for a raised rewriter engine floor; if A1 raises it above rewriter-grpc v0.17.0, record the new minimum, because Task 11's values commit must then move the in-pod rewriter pin in the same commit (stage 2c precedent).

- [ ] **Step 2: Write the failing passthrough and startup tests**

Append to `config/config_test.go`:

```go
// TestDecodeConfigYAMLPassesTheIngressDenylistToHousegate pins housegate spec
// 2026-10-09 §6.7: denied_addresses needs no sentio-node code because the
// embedded HouseGate config is decoded in place, and an empty allowlist is
// accepted.
func TestDecodeConfigYAMLPassesTheIngressDenylistToHousegate(t *testing.T) {
	cfg, err := decodeConfigYAML([]byte(`
housegate:
  storage_integrity:
    ingress:
      enabled: true
      allowed_addresses: []
      denied_addresses:
        - "0x00000000000000000000000000000000000000aa"
`))
	require.NoError(t, err)
	ingress := cfg.Housegate.StorageIntegrity.Ingress
	require.Empty(t, ingress.AllowedAddresses)
	require.Equal(t, []string{"0x00000000000000000000000000000000000000aa"}, ingress.DeniedAddresses)
}
```

Append to `standalone/storage_integrity_schemas_test.go` (add imports `"compute-network-node/standalone/networkstate"`, `"github.com/housegate/housegate/pkg/registry"`, `"sentioxyz/sentio-core/common/statemirror"`):

```go
// openIngressOptions boots the embedded HouseGate the way the devnet2 overlay
// configures indexer-a after the soak (housegate spec 2026-10-09 §8.2): auth
// on, so the default auth.writer_predicate (contract) is active; an empty
// ingress allowlist; a denylist; and the registry standalone.Run injects.
func openIngressOptions(t *testing.T, reg registry.Registry) housegate.Options {
	t.Helper()
	state, err := tablestate.New(tablestate.Config{
		Registry:  tablestate.FromFollower(disabledRegistryFollower{}),
		Readiness: tablestate.ReadinessFunc(func(string) bool { return true }),
		Chain:     tablestate.DatabaseInfoChain(emptyDatabaseInfos{}),
		Genesis:   []payloadexec.TableSchema{{TableID: "orders.t"}},
		NetworkID: "testnet",
	})
	require.NoError(t, err)
	require.NoError(t, state.Refresh(t.Context()))

	enabled := true
	cfg := housegateConfig.Default()
	cfg.Listen = "127.0.0.1:0"
	cfg.Auth.Enabled = true
	cfg.StorageIntegrity.Enabled = &enabled
	cfg.StorageIntegrity.Read.DefaultMode = "unsafe_latest"
	cfg.StorageIntegrity.Ingress.Enabled = true
	cfg.StorageIntegrity.Ingress.NetworkID = "testnet"
	cfg.StorageIntegrity.Ingress.AllowedAddresses = nil
	cfg.StorageIntegrity.Ingress.DeniedAddresses = []string{"0x00000000000000000000000000000000000000aa"}
	cfg.StorageIntegrity.Ingress.MaxTokenAge = housegateConfig.Duration{Duration: time.Minute}
	cfg.StorageIntegrity.Ingress.RequestTimeout = housegateConfig.Duration{Duration: time.Second}
	cfg.StorageIntegrity.Ingress.MaxPayloadBytes = 1 << 20
	cfg.StorageIntegrity.Runtime.Enabled = true
	cfg.StorageIntegrity.Runtime.ExpectedSource = "snode-1"
	cfg.StorageIntegrity.Runtime.JournalDir = filepath.Join(t.TempDir(), "journal")
	cfg.StorageIntegrity.Runtime.PayloadSpoolDir = filepath.Join(t.TempDir(), "payload-spool")
	cfg.StorageIntegrity.Runtime.Backpressure.Enabled = false

	return housegate.Options{
		Config:                     &cfg,
		NetworkState:               reg,
		Rewriter:                   storageIntegrityCapableRewriterFactory{},
		StorageIntegrityReadState:  noPromotedParts{},
		StorageIntegrityTableState: state,
		StorageIntegrityRuntime: housegate.StorageIntegrityRuntimeOptions{
			StatementSubmitter: storageIntegrityRuntimePorts{},
			SourcePreparer:     storageIntegrityRuntimePorts{},
			StatusQuerier:      storageIntegrityRuntimePorts{},
			PayloadWriter:      storageIntegrityRuntimePorts{},
			MergeGuard:         storageIntegrityRuntimePorts{},
		},
	}
}

func TestHousegateNewAcceptsAnOpenIngressOverTheProductionRegistry(t *testing.T) {
	mirror, err := statemirror.NewFileMirror(t.TempDir())
	require.NoError(t, err)
	proxy, err := housegate.New(openIngressOptions(t, networkstate.NewMirrored(mirror)))
	require.NoError(t, err, "the registry standalone.Run injects must satisfy registry.WriterAccess")
	require.NotNil(t, proxy)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_ = proxy.RunWith(ctx, listener)
}

// registryWithoutWriterAccess hides IsDatabaseWriter: embedding the interface
// promotes only registry.Registry's methods.
type registryWithoutWriterAccess struct{ registry.Registry }

func TestHousegateNewRefusesAnIngressRegistryWithoutWriterAccess(t *testing.T) {
	mirror, err := statemirror.NewFileMirror(t.TempDir())
	require.NoError(t, err)
	opts := openIngressOptions(t, registryWithoutWriterAccess{networkstate.NewMirrored(mirror)})
	opts.Config.Auth.Enabled = false // isolate the ingress refusal from the ordinary-path one
	_, err = housegate.New(opts)
	require.ErrorContains(t, err, "requires a registry that implements WriterAccess")
}
```

Append to `TestNetworkState_ProxyByIndexerId` in `standalone/networkstate/networkstate_test.go`:

```go
	require.Equal(t, uint16(9101), addr.StorageRPCPort)
```

and in `newFixtureState` give indexer 1 `StorageNodeRpcPort: 9101`.

- [ ] **Step 3: Run the tests to verify they fail**

Run: `bazel run //:gazelle && bazel test //config:config_test //standalone/networkstate:networkstate_test //standalone:standalone_test --test_output=errors`
Expected: build FAILS with `ingress.DeniedAddresses undefined` and `addr.StorageRPCPort undefined` (housegate v0.16.1 has neither).

- [ ] **Step 4: Bump the three pins**

Run:

```bash
go get github.com/housegate/housegate@<A1_TAG> && go mod tidy
```

Edit `MODULE.bazel`:

```
bazel_dep(
    name = "housegate",
    version = "<A1_TAG without the leading v>",
)
```

```
git_override(
    module_name = "housegate",
    # Resolved Housegate <A1_TAG>; source is pinned by the commit below.
    commit = "<A1_COMMIT>",
    remote = "https://github.com/housegate/housegate",
)
```

Then `bazel mod tidy && bazel run //:gazelle`. Do not move arbiter-core: it stays at v0.10.1 and must build against `<A1_TAG>` as it is. If `bazel build //...` later fails inside `@arbiter_core`, stop and report; an arbiter-core release is out of this plan's scope.

- [ ] **Step 5: Add the compile-time assertions and the port projection**

At the bottom of `standalone/networkstate/networkstate.go`:

```go
var _ registry.WriterAccess = (*FromStatecore)(nil)
```

At the bottom of `standalone/networkstate/redis.go`:

```go
// HouseGate refuses to start an SI ingress, or the ordinary path under the
// default auth.writer_predicate: contract, without registry.WriterAccess on the
// injected registry (housegate spec 2026-10-09 D17, R3).
var _ registry.WriterAccess = (*RedisNetworkState)(nil)
```

In `standalone/networkstate/convert.go`:

```go
func proxyAddressFromCore(s statecore.IndexerInfo) registry.ProxyAddress {
	return registry.ProxyAddress{
		Url:            s.IndexerUrl,
		HousegatePort:  s.ClickhouseProxyPort,
		StorageRPCPort: s.StorageNodeRpcPort,
	}
}
```

- [ ] **Step 6: Run the full suite and prove Bazel compiles A1**

Run:

```bash
bazel build //... && bazel test //... --test_output=errors
bazel mod show_repo housegate | grep -E 'commit|remote'
```

Expected: build succeeds; every test target PASSED (or matches the failing set of a clean `origin/main` build: measure that before calling anything a regression, per the main-baseline rule); `show_repo` prints `commit = "<A1_COMMIT>"`. Also confirm the new sources are what Bazel compiled: `find "$(bazel info output_base)/external" -maxdepth 5 -type d -name sipeerguard -path '*housegate*'` prints one directory (A1's `pkg/plugins/sipeerguard`).

- [ ] **Step 7: Commit**

```bash
git add go.mod go.sum MODULE.bazel config standalone
git commit -m "chore(deps): bump housegate to <A1_TAG> (open SI writes, Phase A)

housegate <A1_TAG> (<A1_COMMIT>): SI ingress authorized by the contract
writer predicate with a static denylist, auth.writer_predicate defaulting to
contract, sipeerguard, and the agent UX of spec 2026-10-09 Phase A.
MODULE.bazel bazel_dep and git_override move with go.mod; arbiter-core stays
at v0.10.1. Both registry adapters now assert registry.WriterAccess, without
which HouseGate refuses to start on every indexer (auth is enabled there).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 5: Define the safe-write metering event (no emitter)

Spec R11 defines both metering events now and wires neither to billing. HouseGate A1 owns `OnStatementSequenced` (`housegate.Options`, nil = no-op; sentio-node passes nothing). sentio-node owns `OnStatementSafe`, but the emission point does not exist: arbiter-core's SNode (v0.10.1 and v0.11.0) promotes per `(table, partition)` with `PromoteSafePartition.CandidateParts`, has no per-statement promotion callback, and `snode.Deps` exposes no observer. This task fixes the event contract only; the emitter lands with an arbiter-core hook (follow-up, not Phase A).

**Files:**
- Create: `storageintegrityadapter/write_meter.go`

**Interfaces:**
- Consumes: housegate `<A1_TAG>` (Task 4) `sicore.SIWriteEvent{StatementID, Signer, Owner, Principal, TableID string; Rows, PayloadBytes, StatementSeq uint64}` from `github.com/housegate/housegate/pkg/storageintegrity` (Plan A1 Task 8; its `Rows` field exists for this event).
- Produces: `storageintegrityadapter.SafeWriteMeter` with `OnStatementSafe(ctx context.Context, event sicore.SIWriteEvent)` — the same event type as HouseGate's `OnStatementSequenced` (spec §6.9: both events carry `SIWriteEvent`).

- [ ] **Step 1: Write the type**

```go
package storageintegrityadapter

import (
	"context"

	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

// SafeWriteMeter receives the storage-integrity write event for a statement
// whose rows reached hg_safe (housegate spec 2026-10-09 §6.9, R10, R11),
// best-effort: an implementation must not block or fail promotion. The event
// is HouseGate's sicore.SIWriteEvent, the type its OnStatementSequenced
// carries, with Rows filled here; it carries the owner, which billing will
// persist once it exists, and the signer is the accountable writer (D3). It
// is not connected to billing in this phase.
//
// Nothing emits it yet: arbiter-core's SNode promotes per (table, partition)
// and exposes no per-statement promotion callback. The emitter lands with
// that hook.
type SafeWriteMeter interface {
	OnStatementSafe(ctx context.Context, event sicore.SIWriteEvent)
}
```

- [ ] **Step 2: Build**

Run: `bazel run //:gazelle && bazel build //storageintegrityadapter:storageintegrityadapter`
Expected: `Build completed successfully`.

- [ ] **Step 3: Commit**

```bash
git add storageintegrityadapter/write_meter.go
git commit -m "feat(storage-integrity): define the safe-write metering event

R11 of housegate spec 2026-10-09: the OnStatementSafe event contract is
fixed now and wired to nothing; arbiter-core's SNode exposes no
per-statement promotion callback to emit it from yet.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 6: sentio-node PR and image

**Files:** none (integration step).

**Interfaces:**
- Produces: merged sentio-node PR; image `ghcr.io/sentioxyz/sentio-node:sha-<A2_COMMIT>@sha256:<A2_DIGEST>` from main CI `docker-push`.

- [ ] **Step 1: Final local gate**

Run: `bazel test //... --test_output=errors && git log --oneline origin/main..HEAD`
Expected: PASSED; five commits from Tasks 1–5.

- [ ] **Step 2: Push and open the PR (requires explicit user confirmation)**

Ask the user before running:

```bash
git push -u origin HEAD
gh pr create --repo sentioxyz/sentio-node --base main --title "feat(storage-integrity): open SI writes host side (housegate <A1_TAG>)" --body-file <scratchpad>/a2-pr-body.md
```

The body (written to the scratchpad first) lists: what each commit does; the rollout requirement that this image must run before any values file sets `writerAuthorization: true` and that HouseGate `<A1_TAG>` refuses to start on a registry without `registry.WriterAccess` (both adapters now carry it); the new storage RPC methods `sentio_isDatabaseWriter` and `sentio_getStorageIntegrityInfo`; that the SI sidecar image is unchanged; that `OnStatementSafe` is defined but not emitted; verification (`bazel test //...`, `bazel mod show_repo housegate`); and ends with `🤖 Generated with [Claude Code](https://claude.com/claude-code)`.

- [ ] **Step 3: After the user merges, resolve the image**

Run:

```bash
git -C /Users/uranuswch/Dev/sentio_xyz/sentio-node fetch -q origin && git -C /Users/uranuswch/Dev/sentio_xyz/sentio-node rev-parse origin/main
gh run list --repo sentioxyz/sentio-node --branch main --limit 3
docker buildx imagetools inspect ghcr.io/sentioxyz/sentio-node:sha-<A2_COMMIT> | sed -n 's/^Digest: *//p'
```

Expected: the merge commit (record as `<A2_COMMIT>`), a successful CI run that includes `docker-push`, and the index digest (record as `<A2_DIGEST>`).

---

## Part 2 — production chart, render tests and runbook

### Task 7: Chart guard, values and render tests (chart PR)

**Files:**
- Modify: `charts/sentio-node/templates/node.yaml:18-20` (guard), `:48-67` (comment block and second guard), `:107-127` (sidecar guards), `:259-267` (ingress rendering), `:521-526` (sidecar env)
- Modify: `charts/sentio-node/values.yaml:141-145` (ingress), `:231-258` (sidecar SI)
- Test: `charts/storage-integrity/tests/test_render.py`

**Interfaces:**
- Consumes: Task 6 `<A2_COMMIT>`, `<A2_DIGEST>`.
- Produces: values keys `housegate.storageIntegrity.ingress.writerAuthorization` (bool, default false), `housegate.storageIntegrity.ingress.deniedAddresses` (list, default []), `housegateSidecar.storageIntegrity.lanes` (`"off"`|`"auto"`, default `"off"`); rendered `denied_addresses` under `housegate.storage_integrity.ingress`; sidecar env `HOUSEGATE_SI_LANES`; Python constants `WRITER_AUTHORIZATION_IMAGES`, `DEVNET2_INGRESS_ALLOWED`.

- [ ] **Step 1: Create the production chart worktree**

Run the URWT command of Task 1 Step 1 with trailing arguments `urwt /Users/uranuswch/Dev/sentio_xyz/production claude si-open-writes-chart`.
Expected: `path` `/Users/uranuswch/src/remotesrc/claude/production/si-open-writes-chart`, `action` `created`. Re-read that worktree's `CLAUDE.md`.

- [ ] **Step 2: Write the failing render tests**

In `charts/storage-integrity/tests/test_render.py`, after `FRESH_SOURCE_IMAGE`:

```python
# sentio-node images whose embedded HouseGate authorizes signed SI writes by the
# contract writer predicate and enforces ingress.denied_addresses (housegate spec
# 2026-10-09 D17; sentio-node PR "open SI writes host side"). A values file may set
# housegate.storageIntegrity.ingress.writerAuthorization only with one of these:
# an older image admits any valid signature when the allowlist is empty and
# silently ignores the denylist.
WRITER_AUTHORIZATION_IMAGES = (
    "ghcr.io/sentioxyz/sentio-node:sha-<A2_COMMIT>@sha256:<A2_DIGEST>",
)
# indexer-a's SI ingress allowlist; stage 4 of
# docs/storage-integrity-devnet2-dynamic-table-set.md changes it.
DEVNET2_INGRESS_ALLOWED = [INDEXER_A_SIGNER]
```

(Place `DEVNET2_INGRESS_ALLOWED` after `INDEXER_A_SIGNER` is defined.) In `ActualFreshNetworkTest.test_indexer_a_binds_to_the_fresh_control_plane`, replace the allowlist assertion with:

```python
        self.assertEqual(
            addresses(host["housegate"]["storage_integrity"]["ingress"]["allowed_addresses"]),
            addresses(DEVNET2_INGRESS_ALLOWED),
        )
```

Add to `ActualFreshNetworkTest`:

```python
    def test_writer_authorization_needs_a_reviewed_image(self):
        ingress = yaml.safe_load(
            (DEVNET2 / "sentio-node-indexer-a-storage-integrity-si-v2.yaml").read_text()
        )["housegate"]["storageIntegrity"]["ingress"]
        if ingress.get("writerAuthorization") is True:
            self.assertIn(FRESH_SOURCE_IMAGE, WRITER_AUTHORIZATION_IMAGES)
        else:
            self.assertTrue(ingress["allowedAddresses"], "an open allowlist needs writerAuthorization")
        for image in WRITER_AUTHORIZATION_IMAGES:
            self.assertRegex(image, r"^ghcr\.io/sentioxyz/sentio-node:sha-[0-9a-f]{40}@sha256:[0-9a-f]{64}$")
```

Add a class after `FreshSourceBindingTest`:

```python
class WriterAuthorizationTest(unittest.TestCase):
    """housegate spec 2026-10-09 §6.10: an open SI ingress needs an image with writer authorization."""

    @classmethod
    def setUpClass(cls):
        cls.image_values = yaml.safe_load(SOURCE_IMAGE_VALUES.read_text())
        # Reuse the binding test's registry-shaped source values; source_values
        # reads only cls.image_values from its class.
        cls.base = FreshSourceBindingTest.source_values.__func__(
            cls, documents(render()), RENDER_NAMESPACE, registry=True,
        )

    def render_with(self, overrides):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory) / "base.yaml"
            extra = Path(directory) / "extra.yaml"
            base.write_text(yaml.safe_dump(self.base))
            extra.write_text(yaml.safe_dump(overrides))
            return subprocess.run([
                "helm", "template", "fresh-source", str(ROOT / "charts/sentio-node"),
                "--namespace", RENDER_NAMESPACE, "-f", str(base), "-f", str(extra),
            ], capture_output=True, text=True, check=False)

    @staticmethod
    def ingress(docs):
        host = yaml.safe_load(next(
            doc for doc in docs.values() if "standalone.yaml" in doc.get("data", {})
        )["data"]["standalone.yaml"])
        return host["housegate"]["storage_integrity"]["ingress"]

    @staticmethod
    def overrides(**ingress):
        return {"housegate": {"storageIntegrity": {"ingress": ingress}}}

    def test_open_allowlist_requires_writer_authorization(self):
        result = self.render_with(self.overrides(allowedAddresses=[]))
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("may be empty only with ingress.writerAuthorization: true", result.stderr)
        docs = documents(self.render_with(self.overrides(allowedAddresses=[], writerAuthorization=True)))
        self.assertEqual(self.ingress(docs)["allowed_addresses"], [])
        self.assertEqual(self.ingress(docs)["denied_addresses"], [])
        self.assertNotIn("writer_authorization", self.ingress(docs), "a chart-only assertion, not HouseGate config")

    def test_denied_addresses_require_writer_authorization_and_render(self):
        denied = ["0x" + "d" * 40]
        result = self.render_with(self.overrides(deniedAddresses=denied))
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("deniedAddresses requires ingress.writerAuthorization: true", result.stderr)
        docs = documents(self.render_with(self.overrides(deniedAddresses=denied, writerAuthorization=True)))
        self.assertEqual(addresses(self.ingress(docs)["denied_addresses"]), denied)
        for bad in ("0x" + "D" * 40, "0x1234", "d" * 40):
            with self.subTest(bad=bad):
                result = self.render_with(self.overrides(deniedAddresses=[bad], writerAuthorization=True))
                self.assertNotEqual(result.returncode, 0, result.stdout)
                self.assertIn("must be a lowercase 0x-prefixed 40-hex address", result.stderr)

    def test_closed_allowlist_still_renders_without_writer_authorization(self):
        docs = documents(self.render_with({}))
        self.assertTrue(self.ingress(docs)["allowed_addresses"])
        self.assertEqual(self.ingress(docs)["denied_addresses"], [])

    def test_sidecar_stays_on_legacy_lanes(self):
        docs = documents(self.render_with({}))
        sidecar = next(
            container
            for doc in docs.values() if doc.get("kind") == "StatefulSet"
            for container in doc["spec"]["template"]["spec"]["containers"]
            if container["name"] == "housegate-sidecar"
        )
        self.assertIn({"name": "HOUSEGATE_SI_LANES", "value": "off"}, sidecar["env"])
        for lanes in ("sometimes", False):
            with self.subTest(lanes=lanes):
                result = self.render_with({"housegateSidecar": {"storageIntegrity": {"lanes": lanes}}})
                self.assertNotEqual(result.returncode, 0, result.stdout)
                self.assertIn("housegateSidecar.storageIntegrity.lanes must be off or auto", result.stderr)
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `python3 -B charts/storage-integrity/tests/test_render.py WriterAuthorizationTest ActualFreshNetworkTest`
Expected: FAIL: `test_open_allowlist_requires_writer_authorization` (stderr says `allowedAddresses is required`), `KeyError: 'denied_addresses'`, the sidecar env assertion; `test_writer_authorization_needs_a_reviewed_image` passes (no overlay sets it yet).

- [ ] **Step 4: Implement the chart**

In `charts/sentio-node/templates/node.yaml`, replace lines 18-20 (the first `allowedAddresses is required` guard) with:

```
{{- $ingress := .Values.housegate.storageIntegrity.ingress -}}
{{- if and $ingress.enabled (empty $ingress.allowedAddresses) (not $ingress.writerAuthorization) -}}
{{- fail "housegate.storageIntegrity.ingress.allowedAddresses may be empty only with ingress.writerAuthorization: true: an image without writer authorization admits any valid signature when the list is empty" -}}
{{- end -}}
{{- if and (not (empty $ingress.deniedAddresses)) (not $ingress.writerAuthorization) -}}
{{- fail "housegate.storageIntegrity.ingress.deniedAddresses requires ingress.writerAuthorization: true: an image without writer authorization ignores the denylist" -}}
{{- end -}}
{{- range $ingress.deniedAddresses -}}
{{- if not (regexMatch "^0x[0-9a-f]{40}$" (toString .)) -}}
{{- fail (printf "housegate.storageIntegrity.ingress.deniedAddresses entry %v must be a lowercase 0x-prefixed 40-hex address" .) -}}
{{- end -}}
{{- end -}}
```

Replace the comment block and second guard at lines 48-67 (from `{{/*` "The ingress allowlist and the SNode authority set…" through the second `allowedAddresses must not be empty` `{{- end -}}`) with:

```
{{/*
  The ingress allowlist and the SNode authority set are two different trust
  roles and are deliberately NOT tied together: ingress.allowedAddresses says
  which signers may SUBMIT a signed statement, while snode.authorityAddresses
  says whose signature the SNode will accept to PROMOTE parts into hg_safe
  (arbiter-core snode/promote.go, AuthorizePromotion). Granting write access
  to a client must never make it a promotion authority.

  HouseGate reads an EMPTY allowlist as "admit any valid signature". Before
  housegate spec 2026-10-09 (D17) that was a silent opening of the signed lane;
  from that release the ingress also requires the owner or signer to be a
  writer of the target database by the contract's isDatabaseWriter, and honours
  ingress.deniedAddresses. The chart cannot inspect the image, so
  ingress.writerAuthorization is the operator's assertion that node.image
  carries D17; only with it may allowedAddresses be empty or deniedAddresses be
  set. charts/storage-integrity/tests/test_render.py pins the reviewed images
  for which a values file may set it.
*/}}
```

After the two `inlineValues` guards (around line 127) add:

```
{{- if and $isIndexer .Values.housegateSidecar.storageIntegrity.enabled (not (has (toString .Values.housegateSidecar.storageIntegrity.lanes) (list "off" "auto"))) -}}
{{- fail "housegateSidecar.storageIntegrity.lanes must be off or auto (quote \"off\": unquoted YAML off is the boolean false)" -}}
{{- end -}}
```

In the `ingress:` rendering block, after the `allowed_addresses` lines:

```
          denied_addresses:
            {{- toYaml (default (list) .Values.housegate.storageIntegrity.ingress.deniedAddresses) | nindent 12 }}
```

In the `housegate-sidecar` container `env:` list, after the `HOUSEGATE_AGENT_KEY` entry:

```
            {{- if .Values.housegateSidecar.storageIntegrity.enabled }}
            # Read by HouseGate's agent as the -si-lanes default; older images
            # ignore an unknown environment variable, where a new flag would
            # stop them. The driver sidecar stays on legacy ids (spec R8).
            - name: HOUSEGATE_SI_LANES
              value: {{ .Values.housegateSidecar.storageIntegrity.lanes | quote }}
            {{- end }}
```

In `charts/sentio-node/values.yaml`, under `housegate.storageIntegrity.ingress` after `allowedAddresses: []`:

```yaml
      # -- Signers or owners refused on the signed SI lane, as lowercase 0x
      # addresses (HouseGate storage_integrity.ingress.denied_addresses; a
      # change needs a restart). Requires writerAuthorization.
      deniedAddresses: []
      # -- Asserts that node.image carries HouseGate's contract writer
      # authorization (housegate spec 2026-10-09 D17): the ingress admits a
      # signed INSERT only when its owner or signer is a writer of the target
      # database, and honours deniedAddresses. Only with it may
      # allowedAddresses be empty. Chart-only; not rendered into the config.
      writerAuthorization: false
```

Under `housegateSidecar.storageIntegrity`, after `maxPayloadBytes`:

```yaml
    # -- client_seq lane mode of the sidecar agent (HOUSEGATE_SI_LANES, "off"
    # or "auto"). The indexer-driver sidecar stays on legacy lane-less
    # statement ids. Keep it quoted: unquoted YAML off is a boolean.
    lanes: "off"
```

- [ ] **Step 5: Run the full render suite**

Run: `python3 -B charts/storage-integrity/tests/test_render.py`
Expected: last line `OK`. Then confirm the devnet2 indexer-a render differs only by the new keys. Never use `git stash` for the baseline (the stash stack is shared across worktrees); render an exported `origin/main` tree instead:

```bash
mkdir -p <scratchpad>/prod-main
git archive origin/main | tar -x -C <scratchpad>/prod-main
helmfile -f <scratchpad>/prod-main/k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.helmfile.yaml template --selector name=sentio-node-devnet2-indexer-a > <scratchpad>/before.yaml
helmfile -f k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.helmfile.yaml template --selector name=sentio-node-devnet2-indexer-a > <scratchpad>/after.yaml
diff <scratchpad>/before.yaml <scratchpad>/after.yaml
```

Expected diff: `denied_addresses: []`, the `HOUSEGATE_SI_LANES` env entry, and the `checksum/config` annotation; nothing else.

- [ ] **Step 6: Commit**

```bash
git add charts/sentio-node charts/storage-integrity/tests/test_render.py
git commit -m "feat(sentio-node): writerAuthorization guard and SI ingress denylist

housegate spec 2026-10-09 §6.10. An empty ingress allowlist, or any
deniedAddresses, now requires ingress.writerAuthorization: true, the
operator's assertion that node.image carries the contract writer
authorization; test_render.py pins the reviewed images. denied_addresses is
rendered for HouseGate, validated as lowercase addresses, and the sidecar
gets HOUSEGATE_SI_LANES=off (an env var, which older images ignore).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 8: Runbook stage 4 (same chart PR)

**Files:**
- Modify: `docs/storage-integrity-devnet2-dynamic-table-set.md` (stage table near the top; new section before "Operating rules"; rename "## 6. Operating rules" to "## 7. Operating rules"; two new operating rules)

**Interfaces:**
- Consumes: Task 7 values keys; Task 2/3 RPC methods.
- Produces: runbook §6 (Stage 4a/4b) that Tasks 10–12 execute.

- [ ] **Step 1: Check section references before renumbering**

Run: `grep -n '§6\b\|§6\.' docs/storage-integrity-devnet2-dynamic-table-set.md`
Expected: no output (no existing reference to §6, so renaming "Operating rules" to §7 breaks nothing).

- [ ] **Step 2: Add the stage table row**

After the `| 3 | Registry, auditors, watcher | … |` row:

```markdown
| 4a | indexer-a: sentio-node `<A2_COMMIT>` (HouseGate `<A1_TAG>`: contract writer authorization, ingress denylist, `sipeerguard`); `writerAuthorization: true` with the allowlist kept (indexer-a signer and one test writer) for a soak day | Revert the stage 4a commit and sync (§6.5) |
| 4b | indexer-a: `allowedAddresses: []` (open signed writes for every database writer) | Revert the stage 4b commit and sync (§6.5) |
```

- [ ] **Step 3: Add the section**

Insert before the operating rules (renamed to `## 7. Operating rules`):

````markdown
## 6. Stage 4: open storage-integrity writes

Design: housegate `docs/superpowers/specs/2026-10-09-user-agent-si-writes-design.md`, Phase A (§6.2, §6.10, §8.2). indexer-a moves to sentio-node `<A2_COMMIT>` with HouseGate `<A1_TAG>`. Its SI ingress then admits a signed INSERT only when the effective principal (the `SQL_x_payer` owner validated through the operator relation, else the signer) is a writer of the target database by the contract's `isDatabaseWriter`: the indexer signer, or the account's own Owner or Write bit; Admin alone and address(0) grants do not count. `deniedAddresses` refuses a signer or owner with 497. `sipeerguard` refuses a peer-trusted read of a governed table's ordinary physical table with 392 instead of answering from the empty table. The storage RPC answers `sentio_getStorageIntegrityInfo` (`client_lanes_enabled: false` until Phase B) and `sentio_isDatabaseWriter`. No consensus parameter, voter or verifier changes; the agent sidecar stays on v0.15.0. The ordinary write check moves to the contract predicate (`auth.writer_predicate: contract`); devnet2 has no address(0) grant (spec Appendix B U4), so that changes no outcome here.

### 6.1 Preconditions

- Stage 3b is done and `arbiter_safe_watermark_block_seq` advances on every voter.
- The chart PR that adds `writerAuthorization` is merged, and `WRITER_AUTHORIZATION_IMAGES` in `test_render.py` contains `ghcr.io/sentioxyz/sentio-node:sha-<A2_COMMIT>@sha256:<A2_DIGEST>`.
- The pin resolves: `docker buildx imagetools inspect ghcr.io/sentioxyz/sentio-node:sha-<A2_COMMIT> | sed -n 's/^Digest: *//p'` prints `sha256:<A2_DIGEST>`.
- HouseGate `<A1_TAG>`'s release notes raise no rewriter engine floor above rewriter-grpc v0.17.0; otherwise the in-pod rewriter moves in the same commit, as in stage 2c.
- Record the running driver tasks (§3.1 command).

### 6.2 Stage 4a values commit

One commit, pushed to `main` after `python3 -B charts/storage-integrity/tests/test_render.py` passes:

- `sentio-node-indexer-a-storage-integrity-si-v2-source-image.yaml`, its mirror `docs/examples/storage-integrity-fresh-source-image.yaml` and `FRESH_SOURCE_IMAGE` in `test_render.py`: `sha-<A2_COMMIT>@sha256:<A2_DIGEST>`, with the header comments naming housegate `<A1_TAG>`.
- `sentio-node-indexer-a-storage-integrity-si-v2.yaml`, `housegate.storageIntegrity.ingress`: `writerAuthorization: true`, `allowedAddresses` = indexer-a's signer plus the soak's test writer, `deniedAddresses: []`; `DEVNET2_INGRESS_ALLOWED` in `test_render.py` lists the same two addresses.

```bash
stage4a_commit="$(git rev-parse HEAD)"
python3 -B charts/storage-integrity/tests/test_render.py
git push origin HEAD:main
helmfile -f k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.helmfile.yaml sync --selector name=sentio-node-devnet2-indexer-a
si_kubectl rollout status statefulset/sentio-node-devnet2-indexer-a --timeout=15m
```

### 6.3 Verify

```bash
si_kubectl get pod sentio-node-devnet2-indexer-a-0 -o jsonpath='{range .spec.containers[*]}{.name}{"\t"}{.image}{"\n"}{end}'
si_kubectl logs sentio-node-devnet2-indexer-a-0 -c sentio-node | grep -E 'storage_integrity ingress enabled|rewriter table-reference policy verified'
si_rpc '{"jsonrpc":"2.0","id":1,"method":"sentio_getStorageIntegrityInfo","params":[]}' | jq -c '.result | del(.server_unix_time)'
si_rpc '{"jsonrpc":"2.0","id":2,"method":"sentio_isDatabaseWriter","params":["devnet101","0x22a67f498e669cbde18a0eecfff74a72c48331f4"]}' | jq -c .result
si_kubectl logs sentio-node-devnet2-indexer-a-0 -c sentio-node --since=15m | grep -cE 'is not a writer of database|is not permitted to write storage-integrity tables|is not an operator of' || true
```

- `sentio-node` runs `…:sha-<A2_COMMIT>@sha256:<A2_DIGEST>`; `housegate-sidecar` and `housegate-rewriter` are unchanged.
- The info answer is `{"enabled":true,"network_id":"devnet2","keeper_shard_id":0,"si_indexer_id":0,"self_indexer_id":0,"client_lanes_enabled":false,"ingress_max_payload_bytes":67108864,"max_token_age_seconds":60,"default_read_mode":"unsafe_latest","registry_version":<n>}` with `<n>` equal to `sentio_node_storage_integrity_registry_version`.
- indexer-a's signer is a writer of `devnet101` (`true`): it is the signer of the hosting indexer.
- No authorization refusals from driver traffic (count `0`), and the running driver task count equals §6.1's after a few minutes.
- Repeat §3.4's signed INSERT through the sidecar with a new value and its safe-watermark check.

Leave stage 4a for at least 24 hours, during which the spec §9.4 Phase-A checks run with the test writer.

### 6.4 Stage 4b values commit

After the soak: `allowedAddresses: []` in `sentio-node-indexer-a-storage-integrity-si-v2.yaml` and `DEVNET2_INGRESS_ALLOWED = []` in `test_render.py`, one commit, then the §6.2 test, push, sync and rollout commands with `stage4b_commit="$(git rev-parse HEAD)"`, and §6.3's checks again. Then run the spec §9.4 Phase-A checks with a key that was never allowlisted.

### 6.5 Rollback

Revert in reverse order; the authorizer persists nothing.

```bash
git show --stat "$stage4b_commit"   # must be the stage 4b commit
git revert --no-edit "$stage4b_commit"
python3 -B charts/storage-integrity/tests/test_render.py
git push origin HEAD:main
helmfile -f k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.helmfile.yaml sync --selector name=sentio-node-devnet2-indexer-a
si_kubectl rollout status statefulset/sentio-node-devnet2-indexer-a --timeout=15m
```

For stage 4a use `stage4a_commit` in the same block, after 4b is reverted: it restores sentio-node `sha-597a67564dffce3e818333f30bfda59991b8dffe@sha256:02de7b26…` and removes `writerAuthorization`. Never revert 4a alone while 4b is live: the chart refuses an empty allowlist without `writerAuthorization`, and an older image would admit any valid signature.
````

Append to the renamed `## 7. Operating rules`:

```markdown
- Set `ingress.writerAuthorization: true` only with a sentio-node image listed in `WRITER_AUTHORIZATION_IMAGES`; an image without writer authorization ignores `deniedAddresses` and admits any valid signature with an empty allowlist.
- A `deniedAddresses` change takes effect only when indexer-a restarts (sync and rollout); it applies to the signed SI lane only, not to ordinary queries, CREATE or DROP.
```

- [ ] **Step 4: Check the document**

Run: `grep -n '^## ' docs/storage-integrity-devnet2-dynamic-table-set.md && python3 -B charts/storage-integrity/tests/test_render.py`
Expected: sections 0–7 in order with "## 6. Stage 4: open storage-integrity writes" and "## 7. Operating rules"; render tests `OK`.

- [ ] **Step 5: Commit, push and open the chart PR (push and PR require explicit user confirmation)**

```bash
git add docs/storage-integrity-devnet2-dynamic-table-set.md
git commit -m "docs(storage-integrity): devnet2 stage 4, open SI writes

Runbook for housegate spec 2026-10-09 Phase A on indexer-a: stage 4a
(writer authorization, allowlist kept for a soak day) and 4b (empty
allowlist), verification and reverse-order rollback.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git push -u origin HEAD
gh pr create --repo sentioxyz/production --base main --title "feat(sentio-node): writerAuthorization guard, SI ingress denylist, devnet2 stage 4 runbook" --body-file <scratchpad>/a3-chart-pr-body.md
```

The PR body states that devnet2 renders unchanged except `denied_addresses: []`, the sidecar `HOUSEGATE_SI_LANES` env and the config checksum (the Step 5 diff of Task 7), names the A2 image added to `WRITER_AUTHORIZATION_IMAGES`, and ends with `🤖 Generated with [Claude Code](https://claude.com/claude-code)`.

---

## Part 3 — compute-network-contracts (C1)

### Task 9: `isDatabaseWriter` NatSpec

**Files:**
- Modify: `src/Databases.sol:373-378`
- Test: `test/Databases.t.sol` (new test after `test_isDatabaseWriter_promotion`)

**Interfaces:**
- Produces: corrected NatSpec; a characterization test. No bytecode change other than the metadata hash.

- [ ] **Step 1: Create the worktree and capture the current bytecode**

Run the URWT command of Task 1 Step 1 with `urwt /Users/uranuswch/Dev/sentio_xyz/compute-network-contracts claude databases-writer-natspec`, then in that path:

```bash
git submodule update --init --recursive
forge build
forge inspect Databases deployedBytecode > <scratchpad>/databases-before.hex
```

Expected: build succeeds; the file holds one `0x…` hex line.

- [ ] **Step 2: Write the characterization test**

Add to `test/Databases.t.sol` after `test_isDatabaseWriter_promotion`:

```solidity
    function test_isDatabaseWriter_adminAloneAndWildcardGrantsAreNotWriters() public {
        _registerIndexer(INDEXER_SIGNER, "ix0");
        vm.prank(INDEXER_SIGNER);
        databases.createUserDatabase(DATABASE_ID, USER, "");

        // Admin (0x04) gates GRANT/REVOKE only; it does not confer write.
        vm.prank(INDEXER_SIGNER);
        databases.grantPermission(DATABASE_ID, USER, STRANGER, PERM_ADMIN);
        assertFalse(databases.isDatabaseWriter(DATABASE_ID, STRANGER));

        // A grant held by address(0) is not unioned into another account.
        vm.prank(INDEXER_SIGNER);
        databases.grantPermission(DATABASE_ID, USER, address(0), PERM_WRITE);
        assertFalse(databases.isDatabaseWriter(DATABASE_ID, STRANGER));
    }
```

- [ ] **Step 3: Run it**

Run: `forge test --mt test_isDatabaseWriter -vv`
Expected: `test_isDatabaseWriter_promotion` and `test_isDatabaseWriter_adminAloneAndWildcardGrantsAreNotWriters` PASS. This is a characterization test: it passes before the comment fix because the code is already correct and the NatSpec is what is wrong.

- [ ] **Step 4: Fix the NatSpec**

Replace the two `@dev` lines above `isDatabaseWriter`:

```solidity
    /// @notice Return true if the account may write to the database.
    /// @dev    The non-zero signer of the database's indexer always has implicit write
    ///         access; otherwise only the account's own Owner (0x08) or Write (0x02) bit
    ///         grants it. Admin (0x04) alone is not a writer, and a grant held by
    ///         address(0) is not unioned into other accounts. Off-chain writers
    ///         (HouseGate's storage-integrity ingress) apply exactly this predicate.
```

- [ ] **Step 5: Prove the executable code is unchanged**

Run:

```bash
forge fmt --check && forge build && forge test
forge inspect Databases deployedBytecode > <scratchpad>/databases-after.hex
python3 -c 'import sys
def code(path):
    b = bytes.fromhex(open(path).read().strip().removeprefix("0x"))
    return b[: -(int.from_bytes(b[-2:], "big") + 2)]
print("executable code identical" if code(sys.argv[1]) == code(sys.argv[2]) else "EXECUTABLE CODE DIFFERS")' <scratchpad>/databases-before.hex <scratchpad>/databases-after.hex
```

Expected: fmt clean, all tests pass, `executable code identical`. The full bytecode differs in its trailing CBOR metadata hash (it covers the source), so `scripts/diff_deployed.sh` will report `Databases` as STALE; do not upgrade the deployed implementation for this change.

- [ ] **Step 6: Commit, push and open the PR (push and PR require explicit user confirmation)**

```bash
git add src/Databases.sol test/Databases.t.sol
git commit -m "docs(databases): isDatabaseWriter NatSpec: Admin alone is not a writer

The code grants write to the indexer signer and to the Owner or Write bit;
the NatSpec also listed Admin (0x04). Comment only: the executable bytecode
is unchanged (only the metadata hash moves, so diff_deployed.sh reports
Databases as stale; no upgrade is needed). Adds a test pinning that Admin
alone and address(0) grants are not writers. housegate spec 2026-10-09 R4/C1.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git push -u origin HEAD
gh pr create --repo sentioxyz/compute-network-contracts --base main --title "docs(databases): isDatabaseWriter NatSpec: Admin alone is not a writer" --body-file <scratchpad>/c1-pr-body.md
```

The body repeats the bytecode evidence and the "no upgrade" note and ends with `🤖 Generated with [Claude Code](https://claude.com/claude-code)`.

---

## Part 4 — devnet2 Phase A rollout and E2E (live)

Every step in this part requires explicit user confirmation before it runs. Commands are fish, for the user's terminal (Claude may run a read-only one only after the user agrees). Open one fish session and define:

```fish
function si_kubectl
    kubectl --context sentio-sea -n sentio-network-devnet2 $argv
end
function si_rpc --argument-names port body
    curl -s -H 'Content-Type: application/json' -d $body http://64.38.144.158:$port
end
function si_status --argument-names db table
    si_rpc 32003 '{"jsonrpc":"2.0","id":1,"method":"sentio_getStorageIntegrityTableStatus","params":["'$db'","'$table'"]}' | jq -c '.result | {status, registry_version}'
end
function si_safe_seq
    si_kubectl exec storage-integrity-si-v2-arbiter-0 -c arbiter -- wget -qO- http://127.0.0.1:9090/metrics | string match -r '^arbiter_safe_watermark_block_seq .*'
end
function si_wait_status --argument-names db table want
    for i in (seq 180)
        set -l st (si_rpc 32003 '{"jsonrpc":"2.0","id":1,"method":"sentio_getStorageIntegrityTableStatus","params":["'$db'","'$table'"]}' | jq -r .result.status)
        echo "$i $st"
        test "$st" = "$want"; and return 0
        sleep 10
    end
    return 1
end
set -g hg_a (si_rpc 32003 '{"jsonrpc":"2.0","id":1,"method":"sentio_getIndexerInfoById","params":[0]}' | jq -r '.result | "\(.indexerUrl | sub("^https?://";"") | sub("/.*$";"") | sub(":[0-9]+$";"")):\(.clickhouseProxyPort)"')
set -g hg_b (si_rpc 32003 '{"jsonrpc":"2.0","id":1,"method":"sentio_getIndexerInfoById","params":[1]}' | jq -r '.result | "\(.indexerUrl | sub("^https?://";"") | sub("/.*$";"") | sub(":[0-9]+$";"")):\(.clickhouseProxyPort)"')
echo "indexer-a housegate $hg_a, indexer-b housegate $hg_b"
```

Expected: `$hg_a` ends in `:32001` and `$hg_b` in `:33001` (spec §3.4).

### Task 10: Gates, test writer and stage 4a

**Files:**
- Modify (production worktree `si-open-writes-devnet2`): `k8s-sea/sentio-network-devnet2/sentio-node-indexer-a-storage-integrity-si-v2.yaml`, `k8s-sea/sentio-network-devnet2/sentio-node-indexer-a-storage-integrity-si-v2-source-image.yaml`, `docs/examples/storage-integrity-fresh-source-image.yaml`, `charts/storage-integrity/tests/test_render.py` (`FRESH_SOURCE_IMAGE`, `DEVNET2_INGRESS_ALLOWED`)

**Interfaces:**
- Consumes: Task 6 image, merged Task 7/8 chart PR, runbook §6.
- Produces: indexer-a on stage 4a; `<TEST_WRITER_1>` and its key file `~/.config/housegate-e2e/devuser1.key`.

- [ ] **Step 1: Read-only gates (requires explicit user confirmation to run against devnet2)**

```fish
docker buildx imagetools inspect ghcr.io/sentioxyz/sentio-node:sha-<A2_COMMIT> | string match -r '^Digest: .*'
si_rpc 32003 '{"jsonrpc":"2.0","id":1,"method":"sentio_getStorageIntegrityInfo","params":[]}'
for i in 0 1 2; si_kubectl exec storage-integrity-si-v2-arbiter-$i -c arbiter -- wget -qO- http://127.0.0.1:9090/metrics | string match -r '^arbiter_safe_watermark_block_seq .*'; end
si_kubectl exec sentio-node-devnet2-indexer-a-0 -c sentio-node -- wget -qO- http://sentio-node-devnet2-indexer-a-dind:2375/tasks | jq '[.[] | select(.Status.State=="running")] | length'
```

Expected: the digest equals `sha256:<A2_DIGEST>`; the info call on today's image answers a JSON-RPC "method not found" error (the method arrives with A2); the safe watermark is equal on all three voters; record the running driver task count.

- [ ] **Step 2: Generate the soak test writer (user's machine; nothing is printed but the address)**

```fish
mkdir -p ~/.config/housegate-e2e; and chmod 700 ~/.config/housegate-e2e
set -l w (cast wallet new --json)
echo $w | jq -r '.[0].private_key' > ~/.config/housegate-e2e/devuser1.key
chmod 600 ~/.config/housegate-e2e/devuser1.key
set -gx TEST_WRITER_1 (echo $w | jq -r '.[0].address' | string lower)
set -e w
echo $TEST_WRITER_1
```

Record the printed address as `<TEST_WRITER_1>`. The user funds `<TEST_WRITER_1>`'s devnet2 Billing balance themselves for one database and two table creations (the contract charges `chargeUserDatabaseCreation` / `chargeUserTableCreation` to the payer); Claude does not move funds.

- [ ] **Step 3: Prepare the stage 4a values commit**

Create the production worktree with the URWT command and `urwt /Users/uranuswch/Dev/sentio_xyz/production claude si-open-writes-devnet2` (base `origin/main`, which includes the merged chart PR). In it:

`sentio-node-indexer-a-storage-integrity-si-v2-source-image.yaml` and `docs/examples/storage-integrity-fresh-source-image.yaml` (keep them identical):

```yaml
node:
  image:
    repository: ghcr.io/sentioxyz/sentio-node
    tag: "sha-<A2_COMMIT>@sha256:<A2_DIGEST>"
    pullPolicy: IfNotPresent
```

with the header comment's first lines changed to name `sentio-node <A2_COMMIT short>: housegate <A1_TAG> (contract writer authorization, SI ingress denylist, sipeerguard), rewriter-proto/rewriter-go as resolved by that build, arbiter-core v0.10.1 (unchanged)` and the stage-2c rollback target `sha-597a67564dffce3e818333f30bfda59991b8dffe@sha256:02de7b26d9ffbb500d3910d57224b878d41551bd8af7f3c15133f4c654933400`.

`sentio-node-indexer-a-storage-integrity-si-v2.yaml`, `housegate.storageIntegrity.ingress`:

```yaml
    ingress:
      enabled: true
      networkID: devnet2
      # sentio-node <A2_COMMIT short> carries HouseGate's contract writer
      # authorization (housegate spec 2026-10-09 D17); test_render.py pins it.
      writerAuthorization: true
      allowedAddresses:
        # indexer-a's signer: the co-located agent sidecar signs with it.
        - "0x22a67f498e669cbde18a0eecfff74a72c48331f4"
        # Stage 4a soak test writer; removed with the whole list in stage 4b.
        - "<TEST_WRITER_1>"
      deniedAddresses: []
      maxTokenAge: 1m
      requestTimeout: 30s
      maxPayloadBytes: 67108864
```

`test_render.py`: `FRESH_SOURCE_IMAGE = ("ghcr.io/sentioxyz/sentio-node:sha-<A2_COMMIT>" "@sha256:<A2_DIGEST>")` and `DEVNET2_INGRESS_ALLOWED = [INDEXER_A_SIGNER, "<TEST_WRITER_1>"]`. If Task 4 Step 1 recorded a raised rewriter floor, move `rewriter.image.tag` in the same file now.

Run: `python3 -B charts/storage-integrity/tests/test_render.py`
Expected: `OK`.

```bash
git add -A k8s-sea/sentio-network-devnet2 docs/examples charts/storage-integrity/tests/test_render.py
git commit -m "chore(devnet2): stage 4a, indexer-a open SI write authorization (soak)

sentio-node <A2_COMMIT short> (housegate <A1_TAG>) with
ingress.writerAuthorization: true; the allowlist keeps indexer-a's signer and
adds one soak test writer. Runbook §6.2.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 4: Push and sync (requires explicit user confirmation)**

```fish
set -g stage4a_commit (git rev-parse HEAD)
git push origin HEAD:main
helmfile -f k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.helmfile.yaml sync --selector name=sentio-node-devnet2-indexer-a
si_kubectl rollout status statefulset/sentio-node-devnet2-indexer-a --timeout=15m
```

Expected: rollout completes.

- [ ] **Step 5: Verify (runbook §6.3)**

```fish
si_kubectl get pod sentio-node-devnet2-indexer-a-0 -o jsonpath='{range .spec.containers[*]}{.name}{"\t"}{.image}{"\n"}{end}'
si_rpc 32003 '{"jsonrpc":"2.0","id":1,"method":"sentio_getStorageIntegrityInfo","params":[]}' | jq -c '.result | del(.server_unix_time)'
si_rpc 32003 '{"jsonrpc":"2.0","id":2,"method":"sentio_isDatabaseWriter","params":["devnet101","0x22a67f498e669cbde18a0eecfff74a72c48331f4"]}' | jq -c .result
si_kubectl logs sentio-node-devnet2-indexer-a-0 -c sentio-node --since=15m | grep -cE 'is not a writer of database|is not permitted to write storage-integrity tables|is not an operator of'
si_kubectl delete pod si-a2-acceptance --ignore-not-found
set -l base_safe (si_safe_seq | string split ' ')[2]
si_kubectl run si-a2-acceptance -i --restart=Never \
  --image=us-west1-docker.pkg.dev/sentio-352722/sentio/clickhouse-server:25.8-lts-decimal512-2026-03-27@sha256:89839a322a59489fa14ca12e9d00f53d860b08c5486808f652a9484d7c247797 \
  --overrides='{"apiVersion":"v1","spec":{"imagePullSecrets":[{"name":"google-image-secret"}]}}' \
  --command -- sh -ec 'clickhouse-local --query "SELECT toInt64(601) AS value FORMAT Native" > /tmp/row.native
clickhouse-client --compression=0 --host sentio-node-devnet2-indexer-a --port 9011 --query "INSERT INTO devnet101.swap_new2 (value) FORMAT Native" < /tmp/row.native'
si_kubectl delete pod si-a2-acceptance
for i in (seq 90); set -l s (si_safe_seq | string split ' ')[2]; test $s -gt $base_safe; and break; sleep 10; end; si_safe_seq
```

Expected: images as in runbook §6.3; info `enabled:true`, `network_id:"devnet2"`, `si_indexer_id:0`, `self_indexer_id:0`, `client_lanes_enabled:false`, `default_read_mode:"unsafe_latest"`; writer check `true`; refusal count `0`; the sidecar INSERT succeeds and the safe watermark passes `base_safe`; the driver task count equals Step 1's after a few minutes. Any failure: roll back with runbook §6.5 (stage 4a block) before continuing.

### Task 11: Soak E2E with the allowlisted test writer (spec §9.4 steps 1–3)

**Files:** none.

**Interfaces:**
- Consumes: Task 10's indexer-a, `<TEST_WRITER_1>`; HouseGate `<A1_TAG>` agent on the user's machine (`brew upgrade housegate/housegate/housegate`; `brew list --versions housegate` shows `<A1_TAG>` without the `v`); `clickhouse client` 26.3+ for inline VALUES.
- Produces: soak evidence (statement ids, counts, log lines) recorded in the session notes.

- [ ] **Step 1: Create the database and table on indexer-a (requires explicit user confirmation: on-chain transactions charged to the test writer)**

`devuser1` must be unused: `si_rpc 32003 '{"jsonrpc":"2.0","id":1,"method":"sentio_getDatabaseInfoById","params":["devuser1"]}' | jq -c .result` prints `null`; otherwise pick `devuser1x` and use it throughout. A database is created on the indexer the session lands on, so pin this agent to indexer-a. Run only one agent per key at a time (the legacy `client_seq` counter is per signer and state directory).

```fish
housegate -agent-key (cat ~/.config/housegate-e2e/devuser1.key) -agent-upstream $hg_a -listen 127.0.0.1:9000 2> ~/.config/housegate-e2e/agent-pinned-a.log &
sleep 3
clickhouse client --host 127.0.0.1 --port 9000 --query 'CREATE DATABASE devuser1'
clickhouse client --host 127.0.0.1 --port 9000 --query 'CREATE TABLE devuser1.t (k String, v Int64) ENGINE = MergeTree ORDER BY k'
si_status devuser1 t
si_wait_status devuser1 t active
kill %1
```

Expected: both DDL statements succeed; the status moves from `pending` to `active` (an INSERT before that is refused with the retryable 733 "pending activation").

- [ ] **Step 2: Signed INSERTs, streaming and inline (spec §9.4 step 2)**

```fish
housegate -agent-key (cat ~/.config/housegate-e2e/devuser1.key) -listen 127.0.0.1:9000 2> ~/.config/housegate-e2e/agent.log &
sleep 3
printf 'k1,1\nk2,2\n' | clickhouse client --host 127.0.0.1 --port 9000 --query 'INSERT INTO devuser1.t FORMAT CSV'
clickhouse client --host 127.0.0.1 --port 9000 --query "INSERT INTO devuser1.t VALUES ('k3', 3)"
si_kubectl logs sentio-node-devnet2-indexer-a-0 -c sentio-node --since=5m | grep -i $TEST_WRITER_1 | tail -5
```

Expected: both INSERTs succeed (no `--compression` flag: the agent listens on loopback, so the client does not compress); indexer-a's log has an info line per admitted statement naming signer, owner and principal (`$TEST_WRITER_1` for all three) and the statement id (R10).

- [ ] **Step 3: Reads (spec §9.4 step 3)**

```fish
clickhouse client --host 127.0.0.1 --port 9000 --database devuser1 --query 'SELECT count() FROM devuser1.t'
kill %1
housegate -agent-key (cat ~/.config/housegate-e2e/devuser1.key) -listen 127.0.0.1:9000 -si-read-mode safe 2> ~/.config/housegate-e2e/agent-safe.log &
sleep 3
for i in (seq 90); set -l n (clickhouse client --host 127.0.0.1 --port 9000 --database devuser1 --query 'SELECT count() FROM devuser1.t'); echo $n; test "$n" = 3; and break; sleep 10; end
kill %1
```

Expected: `3` immediately (`unsafe_latest`, the server default, D6); the safe read reaches `3` after promotion.

- [ ] **Step 4: Soak watch**

Over the 24 hours, run Task 10 Step 5's refusal count and driver task count every few hours and once at the end. Expected: refusal count `0`, driver task count stable, safe watermark advancing. Any refusal from the sidecar or the driver: roll back stage 4a (runbook §6.5) and stop.

### Task 12: Stage 4b and the full Phase-A E2E with never-allowlisted keys (spec §9.4 steps 1–4, 7–9)

**Files:**
- Modify (production worktree `si-open-writes-devnet2`, rebased on `origin/main`): `k8s-sea/sentio-network-devnet2/sentio-node-indexer-a-storage-integrity-si-v2.yaml`, `charts/storage-integrity/tests/test_render.py`

**Interfaces:**
- Consumes: Task 11 soak result; runbook §6.4/§6.5.
- Produces: indexer-a on stage 4b; E2E record including the live `sipeerguard` secondary-query text (spec §6.3 open item).

- [ ] **Step 1: Stage 4b values commit, push and sync (push and sync require explicit user confirmation)**

In the overlay set `allowedAddresses: []` (keep `writerAuthorization: true`, `deniedAddresses: []`) and replace the soak comment with `# Stage 4b: open to every database writer (contract isDatabaseWriter); the denylist is deniedAddresses.`; in `test_render.py` set `DEVNET2_INGRESS_ALLOWED = []`. Run `python3 -B charts/storage-integrity/tests/test_render.py` (expected `OK`), commit with message `chore(devnet2): stage 4b, empty SI ingress allowlist on indexer-a` plus the trailer, then:

```fish
set -g stage4b_commit (git rev-parse HEAD)
git push origin HEAD:main
helmfile -f k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.helmfile.yaml sync --selector name=sentio-node-devnet2-indexer-a
si_kubectl rollout status statefulset/sentio-node-devnet2-indexer-a --timeout=15m
```

Then repeat Task 10 Step 5. Expected: as there, with the sidecar INSERT still accepted (indexer-a's signer is the hosting indexer's signer, so the contract predicate admits it).

- [ ] **Step 2: A never-allowlisted writer with databases on both indexers (requires explicit user confirmation: on-chain, charged)**

Generate `devuser2.key` exactly as Task 10 Step 2 (variable `TEST_WRITER_2`, record `<TEST_WRITER_2>`); the user funds it. A new account's agent picks among indexers hosting databases it holds permissions on, so to exercise the session switch (spec §9.4 step 2) it also needs a database on indexer-b:

```fish
housegate -agent-key (cat ~/.config/housegate-e2e/devuser2.key) -agent-upstream $hg_a -listen 127.0.0.1:9000 2> ~/.config/housegate-e2e/u2-pinned-a.log &
sleep 3
clickhouse client --host 127.0.0.1 --port 9000 --query 'CREATE DATABASE devuser2'
clickhouse client --host 127.0.0.1 --port 9000 --query 'CREATE TABLE devuser2.t (k String, v Int64) ENGINE = MergeTree ORDER BY k'
kill %1
housegate -agent-key (cat ~/.config/housegate-e2e/devuser2.key) -agent-upstream $hg_b -listen 127.0.0.1:9000 2> ~/.config/housegate-e2e/u2-pinned-b.log &
sleep 3
clickhouse client --host 127.0.0.1 --port 9000 --query 'CREATE DATABASE devuser2b'
kill %1
si_wait_status devuser2 t active
si_rpc 32003 '{"jsonrpc":"2.0","id":1,"method":"sentio_isDatabaseWriter","params":["devuser2","'$TEST_WRITER_2'"]}' | jq -c .result
```

Expected: `active`; writer check `true` (Owner bit).

- [ ] **Step 3: INSERTs and the session switch (spec §9.4 step 2)**

```fish
housegate -agent-key (cat ~/.config/housegate-e2e/devuser2.key) -listen 127.0.0.1:9000 2> ~/.config/housegate-e2e/u2.log &
sleep 3
printf 'k1,1\nk2,2\n' | clickhouse client --host 127.0.0.1 --port 9000 --query 'INSERT INTO devuser2.t FORMAT CSV'
for i in (seq 10); clickhouse client --host 127.0.0.1 --port 9000 --multiquery --query "SELECT 1; INSERT INTO devuser2.t VALUES ('s$i', $i)"; end
grep -iE 'switch|upstream' ~/.config/housegate-e2e/u2.log | tail -20
```

Expected: every INSERT succeeds; the agent log shows at least one session that started on `$hg_b` and switched to `$hg_a` for its INSERT (about half of ten sessions start on indexer-b). If none started on b, repeat the loop. Record the log lines.

- [ ] **Step 4: Cross-indexer read is refused, `--database` works (spec §9.4 step 4)**

```fish
kill %1
housegate -agent-key (cat ~/.config/housegate-e2e/devuser2.key) -agent-upstream $hg_b -listen 127.0.0.1:9000 2> ~/.config/housegate-e2e/u2-read-b.log &
sleep 3
clickhouse client --host 127.0.0.1 --port 9000 --query 'SELECT count() FROM devuser2.t'
clickhouse client --host 127.0.0.1 --port 9000 --query 'SELECT * FROM devuser2.t'
clickhouse client --host 127.0.0.1 --port 9000 --query 'SELECT a.k FROM devuser2.t AS a JOIN devuser2.t AS b ON a.k = b.k'
clickhouse client --host 127.0.0.1 --port 9000 --query 'SELECT count() FROM devuser2.t WHERE k IN (SELECT k FROM devuser2.t)'
si_kubectl logs sentio-node-devnet2-indexer-a-0 -c sentio-node --since=5m | grep -F 'devuser2.t' | tail -20
clickhouse client --host 127.0.0.1 --port 9000 --database devuser2 --query 'SELECT count() FROM devuser2.t'
kill %1
```

Expected: each of the four reads fails with code 392 and `storage_integrity: table devuser2.t is governed by storage integrity and must be read through its host indexer; connect with --database devuser2 or USE devuser2`, never a `0` count; indexer-a's log holds the peer-trusted secondary query text for each shape (record them verbatim: this is spec §6.3's live confirmation); the `--database devuser2` read returns the inserted row count. If the `--database` read fails on indexer-b's current image, record the error as a follow-up for indexer-b's upgrade; it does not block Phase A.

- [ ] **Step 5: Denylist (spec §9.4 step 7; values commits, pushes and syncs require explicit user confirmation)**

Commit `deniedAddresses: ["<TEST_WRITER_2>"]` in the overlay (`test_render.py` passes unchanged), push, sync and wait for rollout as in Step 1. Then:

```fish
housegate -agent-key (cat ~/.config/housegate-e2e/devuser2.key) -listen 127.0.0.1:9000 2> ~/.config/housegate-e2e/u2-denied.log &
sleep 3
clickhouse client --host 127.0.0.1 --port 9000 --query "INSERT INTO devuser2.t VALUES ('denied', 1)"
clickhouse client --host 127.0.0.1 --port 9000 --database devuser2 --query 'SELECT count() FROM devuser2.t'
kill %1
```

Expected: the INSERT fails with code 497 `storage_integrity: signer 0x… is not permitted to write storage-integrity tables` ending in ` [client_seq unspent]`; the read still works (the denylist covers the signed lane only, R7). Then `git revert --no-edit` that commit, push, sync, roll out, and repeat the INSERT: it succeeds, reusing the recycled seq (the agent log names it).

- [ ] **Step 6: Read-only grantee is refused locally and by the server (spec §9.4 step 8; GRANT is on-chain and requires explicit user confirmation)**

Generate `reader.key` as in Task 10 Step 2 (variable `READER_KEY_ADDR`, record `<READER_KEY_ADDR>`); the user funds it lightly in case usage metering needs a balance.

```fish
housegate -agent-key (cat ~/.config/housegate-e2e/devuser2.key) -agent-upstream $hg_a -listen 127.0.0.1:9000 2> ~/.config/housegate-e2e/u2-grant.log &
sleep 3
clickhouse client --host 127.0.0.1 --port 9000 --database devuser2 --query "GRANT SELECT ON devuser2.* TO `$READER_KEY_ADDR`"
kill %1
si_rpc 32003 '{"jsonrpc":"2.0","id":1,"method":"sentio_isDatabaseWriter","params":["devuser2","'$READER_KEY_ADDR'"]}' | jq -c .result
housegate -agent-key (cat ~/.config/housegate-e2e/reader.key) -listen 127.0.0.1:9000 2> ~/.config/housegate-e2e/reader.log &
sleep 3
clickhouse client --host 127.0.0.1 --port 9000 --query "INSERT INTO devuser2.t VALUES ('reader', 1)"
clickhouse client --host 127.0.0.1 --port 9000 --database devuser2 --query 'SELECT count() FROM devuser2.t'
kill %1
```

Expected: writer check `false`; the A1 agent refuses the INSERT locally (its pre-check calls `sentio_isDatabaseWriter`) and the session stays usable, so the following SELECT in a new session answers.

Server path, with an agent that has no pre-check (HouseGate v0.16.1, built from the local clone; nothing is downloaded):

```fish
set -g old_hg (mktemp -d)
git -C ~/Dev/housegate/housegate archive v0.16.1 | tar -x -C $old_hg
pushd $old_hg; go build -o housegate-v0.16.1 ./cmd; popd
mkdir -p $old_hg/state; chmod 700 $old_hg/state
printf '%s\n' 'listen: 127.0.0.1:9100' 'agent:' '  mode: true' "  upstream: $hg_a" 'network_state:' '  source: http://64.38.144.158:32003' 'storage_integrity:' '  agent:' '    enabled: true' '    network_id: devnet2' '    keeper_shard_id: 0' "    state_dir: $old_hg/state" > $old_hg/agent.yaml
env HOUSEGATE_AGENT_KEY=(cat ~/.config/housegate-e2e/reader.key) $old_hg/housegate-v0.16.1 -config $old_hg/agent.yaml 2> $old_hg/agent.log &
sleep 3
clickhouse client --host 127.0.0.1 --port 9100 --query "INSERT INTO devuser2.t VALUES ('reader', 1)"
kill %1
```

Expected: code 497 `storage_integrity: 0x<reader> is not a writer of database devuser2` with ` [client_seq unspent]`. If v0.16.1 rejects the YAML, read `$old_hg/agent.log` and `$old_hg/housegate-v0.16.1 -h` for the exact key names and correct the file; the expected server answer does not change.

- [ ] **Step 7: DROP → retire → purge → re-CREATE (spec §9.4 step 9; on-chain, requires explicit user confirmation)**

```fish
housegate -agent-key (cat ~/.config/housegate-e2e/devuser2.key) -agent-upstream $hg_a -listen 127.0.0.1:9000 2> ~/.config/housegate-e2e/u2-drop.log &
sleep 3
clickhouse client --host 127.0.0.1 --port 9000 --database devuser2 --query 'DROP TABLE devuser2.t'
clickhouse client --host 127.0.0.1 --port 9000 --database devuser2 --query 'SELECT count() FROM devuser2.t'
si_status devuser2 t
for i in (seq 180); set -l st (si_rpc 32003 '{"jsonrpc":"2.0","id":1,"method":"sentio_getStorageIntegrityTableStatus","params":["devuser2","t"]}' | jq -r .result.status); echo $st; test "$st" != gone; and break; sleep 20; end
clickhouse client --host 127.0.0.1 --port 9000 --database devuser2 --query 'CREATE TABLE devuser2.t (k String, v Int64) ENGINE = MergeTree ORDER BY k'
si_wait_status devuser2 t active
clickhouse client --host 127.0.0.1 --port 9000 --query "INSERT INTO devuser2.t VALUES ('again', 1)"
clickhouse client --host 127.0.0.1 --port 9000 --database devuser2 --query 'SELECT count() FROM devuser2.t'
kill %1
```

Expected: DROP succeeds; the read answers code 60 `UNKNOWN_TABLE`; the status is `gone`, then leaves `gone` after retire and purge (a purged name reads as unrecorded); the same-name CREATE succeeds (retryable refusals until purge completes are expected); the table becomes `active`; the INSERT succeeds and the count is `1`.

- [ ] **Step 8: Close out**

Record in the session notes: both stage commits, the A2 image, every statement id observed, the §6.3 secondary-query texts, and the outcome of each step. Leave stage 4b in place. Rollback at any point: runbook §6.5 (4b first, then 4a).

---

## Spec coverage

| Spec item | Task |
|---|---|
| §6.7 `WriterAccess` on `FromStatecore`, exact semantics, unknown/pending-delete errors | 1 (plus `RedisNetworkState`, which production injects), 4 (assertion) |
| §6.7 `sentio_getStorageIntegrityInfo`, `client_lanes_enabled: false` | 3 |
| §6.4 writer pre-check source | 2 (`sentio_isDatabaseWriter`) |
| §6.4 `StorageRPCPort` / storage RPC exposure | 4 (projection); production needs nothing (already public) |
| §6.7 `denied_addresses` passthrough (verify embedding) | 4 (decode test, open-ingress startup test) |
| §6.7 / §8.1 A2 housegate bump | 4, 6 |
| §6.9 / R11 `OnStatementSafe` | 5 (defined; emitter blocked on arbiter-core) |
| §6.10 chart guard, values, `deniedAddresses`, `test_render.py` image pin, sidecar lanes | 7 |
| §6.10 runbook text | 8 (Phase-A stage; the lane-activation stage is Phase B) |
| §6.10 devnet2 overlay with soak | 10, 12 |
| §6.12 / R4 C1 | 9 |
| §8.2 step 1, §9.4 Phase-A steps 1, 2, 3, 4, 7, 8, 9 | 11, 12 |
| §6.7 adapter parser, `lanesEnabled`, arbiter-core bump; §9.4 steps 5, 6, 10 | Phase B (out of scope) |
