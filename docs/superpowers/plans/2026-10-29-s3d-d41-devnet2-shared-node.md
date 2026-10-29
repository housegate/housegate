# Plan S3-D decision D41: the devnet2 shared-node exception to D39 (user decision, 2026-10-29)

Plan S3-D (`2026-10-15-multi-source-si-s3d-p4-settle-rebuild-hardening.md`) requires decision D39 and clarification K55. Under them the three source ClickHouses run on pairwise distinct nodes, and no verifier ClickHouse runs on a source's node. The hardening runbook moves a verifier off a conflict (§5.0), never a source. It stops, and the plan is amended, when two sources share a node.

Task 53 Step 3 read devnet2's placement, read-only, at 2026-10-29T03:42:40Z. It found 15 eligible nodes (`sentio.xyz/general=true`: sea-dg-1..9, sea-g-1..4, sea-sol-1..2) and the following placement. In each row the pod's node equals its local-path data volume's node.

| SI ClickHouse | node |
|---|---|
| network installation `source` (indexer-a's source) | sea-dg-7 |
| network installation `verifier-1`, `verifier-2`, `verifier-3` | sea-dg-7 |
| `storage-integrity-si-v2-indexer-b` `source` | sea-dg-7 (volume there since 2026-10-11) |
| `storage-integrity-si-v2-indexer-c` `source` | sea-dg-3 (pinned since Plan S3-B Task 19) |

So two sources share sea-dg-7, which is D39's stop. The plan's synthetic fixture had assumed indexer-b on sea-dg-8.

On 2026-10-29 the user decided in chat that devnet2 is a development network and nothing moves: 「不用迁移了，开发过程中用同一台机器没问题」, 「保持原样吧，继续开发」.

## Decision D41

**D41: the devnet2 shared-node exception.** On devnet2 the SI ClickHouses stay where the Task 53 read found them:

- The network installation's source and verifier-1..3 and indexer-b's source share sea-dg-7.
- indexer-c's source stays on sea-dg-3.

No source and no verifier ClickHouse moves. The hardening runbook's §5.0 (moving a verifier) does not run on devnet2, and no source-move procedure exists.

D39 and K55 stay in force for everything D41 does not record. Any other sharing still stops the runbook, and so does a recorded ClickHouse found on another node. D41 is lifted only by a later amendment, for example when devnet2 has dedicated nodes. It is never widened by an edit.

**Recorded form (production, Plan S3-D Task 53).** `charts/storage-integrity/tests/test_render.py` holds `DEVNET2_SHARED_NODE_EXCEPTION`, which is never edited afterwards:

```python
DEVNET2_SHARED_NODE_EXCEPTION = {"sea-dg-7": frozenset({
    ("storage-integrity-si-v2", "source"), ("storage-integrity-si-v2", "verifier-1"),
    ("storage-integrity-si-v2", "verifier-2"), ("storage-integrity-si-v2", "verifier-3"),
    ("storage-integrity-si-v2-indexer-b", "source")})}
```

The tests enforce it as follows:

- `test_si_clickhouses_run_on_distinct_nodes` exempts from D39 only a recorded ClickHouse that sits on its recorded node, which is also its read node. It still refuses every unrecorded sharing.
- `test_devnet2_node_pins_follow_the_rollout` refuses any pin of a recorded ClickHouse other than its read node.
- `DEVNET2_CLICKHOUSE_NODES` keeps the read.

**Node pins.** Every pin is the ClickHouse's read node, where its local-path volume already is, so each pin restarts the pod in place:

- indexer-b's source is pinned to sea-dg-7 at its interserver step 1 (hardening runbook §5.1).
- The network CHI's `source` and `verifier-1..3` are pinned to sea-dg-7 at its interserver step 1 (§5.3).
- indexer-c keeps its sea-dg-3 pin (§5.2).

**Failure domains (K55).** `DEVNET2_FAILURE_DOMAINS` is recorded from the actual placement, so indexer-a and indexer-b share `node:sea-dg-7` and indexer-c has `node:sea-dg-3`. Task 55 records the value with the real indexer ids; it is not predicted here.

The `hardening run` record states that, by the user's decision (D41), devnet2's failure domains are not independent. One loss of sea-dg-7 takes indexer-a's and indexer-b's sources and every verifier ClickHouse at once. That is acceptable for development only.

Plans S3-E4 (`stage3.indexer_failure_domains`) and S3-F (`failure_domain`) copy the value verbatim, as K55 says. They must not count devnet2's domains as independent. S3-F's follow-up "move the verifiers onto nodes of their own" covers the sources as well.

**Budget.** The hardening makes its original 18 acceptance promotions. With no §5.0 move there is no "plus 3 per MOVE" term.

## The Plan S3-D text this amends

Wherever Plan S3-D says the three sources run on pairwise distinct nodes, that no verifier runs on a source's node, that a conflict moves a verifier, or that the runbook stops on two sources sharing a node, it now reads "except as decision D41 records for devnet2". This applies to the Goal, the Task 53/54/55 architecture bullets, K55, Review Focus 5, the spec coverage map and the follow-ups. In particular:

**Task 53 (production; executed with this amendment):**
- `DEVNET2_SHARED_NODE_EXCEPTION` is added, and the two placement tests are taught it as stated above.
- The preflight tool `preflight-p4.py` still prints its STOP for the raw read. That is expected, because it judges the read without D41.
- The Step 24 `rollout-check.py` skips a §5.0 move for a recorded verifier, and adds the out-of-order case "a recorded verifier moved by §5.0" (refused).

**Task 54 (production; hardening runbook `docs/storage-integrity-devnet2-stage3-p4-hardening.md`):**
- `si_p4_py placement` reads `DEVNET2_SHARED_NODE_EXCEPTION`. A recorded ClickHouse on its recorded node is neither a source conflict nor a `MOVE`; the helper prints one `verdict SHARE D41 <node>: <clusters> (devnet2 shared-node exception, user decision; failure domains not independent)` line instead.
- Any unrecorded sharing still prints `MOVE` or `STOP` exactly as before.
- §2.4 expects `verdict placement ok` with the D41 `SHARE` line.
- §5.0 says that on devnet2 it has nothing to run, since every verifier sharing is D41's.
- §5.1 and §5.3 pin to the read nodes (sea-dg-7). §5.2 is unchanged.
- §9's `failure-domains.txt` carries the D41 `SHARE` line before the `DEVNET2_FAILURE_DOMAINS=` line.
- §11's end record copies it (its existing `^verdict SHARE` grep).
- §12.2 has no move to undo.
- The budget is 18.

**Task 54 Step 7 (the helper self-test `selftest.bash`, local).**
- Section 7's expected-move count leaves out the keys `DEVNET2_SHARED_NODE_EXCEPTION` records. On the recorded placement it expects 0 `MOVE` lines, `verdict placement ok` and one `verdict SHARE D41 sea-dg-7: ` line.
- Its negative fixture moves an unrecorded source (indexer-c's) onto another source's node and still expects `verdict STOP two sources share a node`. The old fixture moved indexer-b onto indexer-a's node, which D41 accepts.

**Task 54 (owner-rebuild runbook):** no change. The rebuild target indexer-c stays on its own node, sea-dg-3.

**Task 55 (devnet2 live):**
- The order of contract §37.4 loses the verifier-move windows: census → budget → interserver secret → staging proof → step 1 (indexer-b, indexer-c, then the network CHI, every pin on its read node) → step 2 → policies → Keeper allowlist → failure domains → exit probe.
- The budget needs the original 18 acceptances and no MOVE term.
- `p4-edit.py` writes pins equal to the read node and refuses a pin of a recorded ClickHouse elsewhere.
- `DEVNET2_FAILURE_DOMAINS` is recorded from the actual placement, as `failure-domains.txt` prints it.
- The `hardening run` record states: "D41: devnet2's failure domains are not independent (indexer-a and indexer-b share node:sea-dg-7 with every verifier ClickHouse), by the user's decision of 2026-10-29".

**Task 55's final domain gate (`p4_domains_check`, Step 20).**
- The D39 condition `len(set(sources)) != 3 or set(sources) & set(verifiers) or (len(eligible) >= 6 and len(set(verifiers)) != 3)` is judged over the rows that D41 does not excuse. A row is excused when its `(installation, cluster)` is in `DEVNET2_SHARED_NODE_EXCEPTION[node]` and its node is that `node`. The excused sources must sit only on their recorded node, and no unexcused source may share it.
- A `verdict SHARE D41 <node>: …` line is accepted for each exception node, also with six or more eligible nodes. Every other `SHARE` line keeps the existing rule.
- The record check (`DEVNET2_FAILURE_DOMAINS` equal to the source pins at HEAD) is unchanged. It is what makes `0=node:sea-dg-7,1=node:sea-dg-7,<INDEXER_C_ID>=node:sea-dg-3` pass.
- Its expected output and the `hardening run` record carry the D41 statement above.

**Downstream consumers (Plans S3-E4 and S3-F; amend before they run).** Both reject the shared value today:
- S3-E4's `si_p5_py cmd_params` refuses repeated `failure_domain` values (`len({e["failure_domain"] for e in entries}) != len(entries)`).
- S3-F's `daily_copies` requires three distinct source nodes (`source nodes not three distinct nodes`).

Before either runs on devnet2, its plan is amended with the same bounded rule:
- A repeated domain is accepted only for exactly the indexers whose sources D41 records on one node (devnet2: indexer-a and indexer-b on `node:sea-dg-7`). Any other repetition is still refused.
- Copies and `min_safe_copies` are counted per distinct failure domain, never per indexer, so the shared pair counts as one domain and no independent-copy count is inflated.
- The P5 activation record and the P6 daily-copies report state the D41 non-independence.

The alternative is that the user gives devnet2 dedicated nodes first and lifts D41 by a later amendment. Then the hardening's pins move by that amendment's own procedure, which D41 does not provide. Until one of the two is done, P5 activation and P6 on devnet2 are blocked on this point.

No Appendix C row changes. `s3-ledger.py` is unaffected.
