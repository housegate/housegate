# Plan S3-F decision D46: a single-member group's rebalance releases at once on devnet2 (user decision, 2026-11-03)

Decision D45 (`2026-11-02-s3e4-s3f-d45-devnet2-d41-d44.md`) accepts single-member groups on devnet2 at k = 2. Under Plan S3-D's decision D41, indexer-a and indexer-b share `node:sea-dg-7`, so PlaceGroup gives owners a and b `[member-c]`, and owner c `[member-a]` or `[member-b]` per database.

Plan S3-E4's final review raised finding I2, which D45 does not cover: what a `RebalanceGroup` does to such a group. Plan S3-F's runbook §15.4 expected the removed member's release to be "eventually true", that is, held until other copies reflect the owner's entry. That expectation does not hold for a group of one. The review triaged it as a gate for S3-F step 10 and later. This document records the finding, the user's decision and the plan text it changes.

## The finding (I2)

The code is arbiter `v0.16.0` (`c63b1649`), which is `ARBITER_TAG_P5`, the arbiter Plan S3-F runs. The line numbers below were checked at that tag.

**The removed member's E is frozen without it.** For every removed member, `RebalanceGroup` freezes the group's copy-floor set E as it is before the command, with that member left out (`fsm/rebalance_group.go:71-77`):

```go
	// E of each removed member, the member itself excluded, as it is before
	// this command changes anything (K74).
	frozen := make(map[string][]string, len(c.Remove))
	frozenDomains := make(map[string]uint32, len(c.Remove))
	for _, id := range c.Remove {
		frozen[id], frozenDomains[id] = f.copyFloorSetLocked(g, false, id)
	}
```

`copyFloorSetLocked` (`fsm/stage3_groups.go:417-439`) counts only members of class `caught_up` or `behind`, outside the owner's failure domain. A member added by the same command is `MemberBootstrapping` (`fsm/rebalance_group.go:97-104`). So in a one-member group the frozen E of the removed member is empty, and `FrozenEDomains` is 0. This holds for a pure removal and for a replacement alike.

**The release is recorded in the same Apply.** The command ends in `groupsChangedLocked` (`fsm/rebalance_group.go:116`). That function evaluates every Leaving release first (`fsm/stage3_groups.go:502-508`). Outside strict mode, the release condition is (`fsm/floor.go:414-426`):

```go
	} else {
		required = min(need, m.FrozenEDomains)
		...
	}
	if required == 0 {
		return true
	}
```

Here `need` is `floorNeed(k)` = k − 1 (`fsm/floor.go:64-69`). With `FrozenEDomains` = 0, `required` is 0 and the release holds at once, at k = 1 and at k = 2 alike. `releaseLeavingMembersLocked` marks it `Released` in the rebalance's own Apply. This is the "probe 2" the review ran.

**Before the `checkpoints` switch nothing is released.** `releaseLeavingMembersLocked` returns false while the copy floor is inactive (`fsm/floor.go:462`). The floor is active exactly when the `checkpoints` switch is on (`fsm/floor.go:58-60`). Every `ConsensusParamsUpdate` evaluates the releases at its end (`fsm/consensus_updates.go:229-232`), the `checkpoints` switch-on included. So:
- a member removed during steps 5–7 stays Leaving, unreleased;
- step 8's switch-on Apply then releases it, at once, by the rule above.

**What the member then does.** The member keeps its `hg_member_unsafe` replicas only while it is a current member, or Leaving and not released. Once released, it drops them when the table is quiet (arbiter-core `v0.21.0`, `member/consensus/unsafe_reconciler.go:146-152` and `:253-271`).

**What k = 2 then shows.** The group's E now holds no counting member, because the new member is still Bootstrapping, or there is none. So |E| = 0 < k − 1, and:
- `arbiter_under_replicated_groups` counts the group (`metrics/metrics.go:343`);
- the owner's `arbiter_safe_copies_min` is 1;
- every cleanup that Apply allows for it is recorded as `domains_below_k` (`fsm/floor.go:30`, and the README's "Copy floor" paragraph at that tag), with the hint-only `under_replicated` event.

This lasts until a replacement member is caught up with a current checkpoint. Cleanups are not held, because min(k − 1, |E|) = 0 asks for no reflecting copy. So the held-cleanup age of K87 (a) is not what reveals this. `safe_copies` of K87 (b) is.

**This is the spec's rule.** It is spec §6.4's non-strict release rule, min(k − 1, frozen |E|). Strict mode would instead ask for k − 1 domains of the group as E is now. The P5 code is correct. P5 runs with every switch off and k = 1, so this is not a P5 blocker.

**No add-then-remove sequence exists on devnet2.** `RebalanceGroup` refuses a result in which two members share a failure domain, or a member sits in the owner's (`fsm/rebalance_group.go:176-188`):
- For owners a and b, only member-c is eligible: member-a and member-b share `node:sea-dg-7` with the owner. So a second member can never be added first.
- For owner c, member-a and member-b are both eligible but share `node:sea-dg-7`. Its group can only swap them in one command (`--add` one, `--remove` the other). That swap releases at once too.

D45 already records that no `RebalanceGroup` can add the shared-domain peer (S3-F §15.3).

## The user's decision (2026-11-03)

The main session put the finding to the user in chat:
- on devnet2 every group has one member (D45);
- after k = 2, a `RebalanceGroup` that replaces or removes that member freezes an empty floor set, so the removed member is Released in the same Apply and the database falls at once to the owner's single copy, with `under_replicated` raised;
- this is spec §6.4's non-strict rule, but it contradicts S3-F runbook §15.4's "release_recorded eventually true";
- for owners a and b only member-c is eligible, so add-then-remove is impossible.

The user chose 「接受并写进计划」: accept it and write it into the plan as D46.

## Decision D46

**D46: a single-member group's rebalance on devnet2.**

- **Accepted as specified.** On devnet2, from the `checkpoints` switch on, a `RebalanceGroup` that removes or replaces a group's one member releases that member in its own Apply.
  - At k = 1 that is the expected release: the owner's copy is all that k = 1 asks.
  - At k = 2 the database falls at once to the owner's single copy. `under_replicated` is raised (`arbiter_under_replicated_groups`, the owner's `arbiter_safe_copies_min` = 1, `domains_below_k` cleanups) until a replacement member is caught up with a current checkpoint.
  - Before the `checkpoints` switch, the removed member stays Leaving with its replicas, until step 8's switch-on releases it.
- **Each such rebalance is confirmed on its own.** The confirmation request states the consequence for that group at the current k, in the words of runbook §15.4.
- **During the exit soak:**
  - its `domains_below_k` cleanups are under-replicated events that the day's review must explain (§13.1);
  - a daily check that still finds `safe_copies` 1 is a K87 (b) breach, and the 7 days restart.
- **No add-then-remove repair on devnet2.** Owner c's group may swap member-a and member-b in one command, with the same immediate release.
- **No change elsewhere.** No arbiter, arbiter-core, sentio-node or housegate code change. No spec change. Strict mode stays off. A RebalanceGroup is never automatic, and a release is never forced.
- **Bounds.** D46 is limited to devnet2 under D41 and D45. It lapses with them when D41 is lifted, for example by dedicated nodes: with two members per group, the frozen E of a removed member is no longer empty.

## What changes in Plan S3-F

All changes are in `2026-10-15-multi-source-si-s3f-p6-switch-on.md`. Line numbers are those of this commit.

- **Header** (line 11): the D46 amendment paragraph, after D45's.
- **Review Focus 3** (line 87): the Keeper-abort forward fix's release is not evaluated before the `checkpoints` switch. During step 6 its first evaluation is step 8's switch-on, and the member keeps its replicas until then.
- **Review Focus 5** (line 89): a `group rebalance` of a group's one member is no remedy for a member that is down. It releases at once, and at k = 2 it drops the database to the owner's single copy. In the soak, that is a reviewed under-replicated event, or a K87 (b) breach if a daily check still sees it. Each such rebalance has its own confirmation.
- **Follow-ups** (line 155): a rebalance that holds a single-member group's copy until its replacement is ready (strict mode for the window, or a hold-until-ready option, which needs new code and a user/spec decision), and S3-E4 Task 76's review M3 (no test of the one-member held state before Lagging).
- **Runbook §15.3** (line 6070): on devnet2 a placement fix replaces a group's one member, and its release is immediate at step 8's first evaluation.
- **Runbook §15.4** (lines 6072–6106):
  - The prose states:
    - the checkpoints-switch gate;
    - the immediate release at k = 1 and at k = 2;
    - the k = 2 under-replication and its readings;
    - that no add-then-remove exists;
    - owner c's one-command swap;
    - that strict mode stays off.
  - The confirmation before block `15.4-remove` names the consequence for each case:
    - before the switch: the member stays Leaving with its replicas;
    - at k = 1: released, and the owner's copy suffices;
    - at k = 2: released, the database is on the owner's single copy, `under_replicated` is raised, and in the soak a daily check that still sees it restarts the 7 days.
  - Block `15.4-leaving` adds one read-only line. `si_voter_series` of `arbiter_under_replicated_groups|arbiter_safe_copies_min|arbiter_under_replicated_total` is written into `forward-under.txt`.
  - The Expected names:
    - `release_recorded` false before the switch, and true in the rebalance's own Apply after it;
    - at k = 1: `arbiter_under_replicated_groups` 0;
    - at k = 2: `arbiter_under_replicated_groups` ≥ 1, the owner's `arbiter_safe_copies_min` 1, and `domains_below_k` rising until the replacement is caught up.
- **Runbook §16.2** (line 6152): the playbook's "generation-checked RebalanceGroup if needed" carries the same consequence and §15.4's confirmation.
- **Task 87 abort text** (line 10039): the k1/k2 sentence now states the checkpoints gate and the immediate release, in place of "at k2/strict it can need other reflecting copies".
- **Appendix B** (line 13242): a D46 entry with the code facts above.

Checked and left unchanged:
- §15.6's "A pressure escape or |E| below k−1 is an under_replicated event to investigate", which already covers this;
- §16.1's restart text, since a restart is no rebalance;
- §15.3's "No RebalanceGroup can add the shared-domain peer" (D45);
- Task 88's stop list, which authorises no rebalance;
- Appendix A's command rows.

No other statement in the plan assumes that a rebalance keeps the removed member until a replacement is ready, or that add-then-remove is available on devnet2.

## Verification

- Every runbook block still passes Task 84's local checker: 17 sections, 114 Bash blocks, 3 fish blocks, 9 ordered held updates. That covers `bash -n` under bash 5 and `/bin/bash`, `fish -n`, and `shellcheck -S warning` over the shared session.
- Task 84's local self-test (67 checks) and the restart self-test pass unchanged. No `si_p6_py` source or fixture changed.
- No marker was added or removed.

## Follow-ups

- **S3-E4 Task 76 M3.** No test covers a one-member group's held state before Lagging, which is the shape every devnet2 group has. Add one with any work on this rule.
- **If devnet2 ever needs the copy kept through a rebalance**, that is a fresh user and spec decision: strict mode for the window, or a hold-until-ready option on `RebalanceGroup` (new arbiter code and its own release).
- **The pre-switch window.** A Keeper-abort forward repair during step 6 removes no replica before step 8's `checkpoints` switch-on, because no release is evaluated until then. If load must actually leave the shared Keeper during step 6, the runbook has no repair that achieves it. That is a question for the user, recorded with this decision but not decided by it.
- **The pins.** The pins of Appendices A and B that D45 marked as pre-D45 are re-derived after this decision; the re-derivation is recorded with them.
