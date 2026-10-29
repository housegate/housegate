# Plan S3-D decision D42: a one-hour census on devnet2 (user decision, 2026-10-29)

Plan S3-D (`2026-10-15-multi-source-si-s3d-p4-settle-rebuild-hardening.md`) and its hardening runbook (production `docs/storage-integrity-devnet2-stage3-p4-hardening.md` §2) gate every policy enforce on a 7-day census, read-only (K57):

- ClickHouse `query_log` client addresses over the window.
- The interserver replicas Keeper records.
- Keeper `cons` sampled once per UTC day, at least seven times on seven distinct UTC days (Task 55 Step 5, `p4_census_window`).

Task 55 started the census on devnet2 at `2026-10-29 09:24:56` UTC and took its first Keeper sample at `09:31:35`. On 2026-10-29 the user decided in chat that devnet2 is a development network and the window is shortened: 「把7天改成1h吧」.

## Decision D42

**D42: the devnet2 census window is at least one hour.** It replaces "7 days" wherever Plan S3-D, K57 or the hardening runbook names the census window, for devnet2 only:

- **Window.** The census window is complete when at least 1 hour has passed since `census-start`.
- **Keeper samples.** The window also needs at least 6 Keeper `cons` samples (runbook §2.3's sample block, unchanged), taken at least 10 minutes apart. That replaces "once per UTC day, at least seven times on seven distinct UTC days". Every sample lies inside the window: never before `census-start`, never in the future.
- **Start.** A census already started keeps its `census-start` and its samples when they meet this rule. Task 55's census of 2026-10-29 keeps `09:24:56` and its first sample.
- **What does not change.** `query_log` is still read over the whole window from `census-start` (§2.1). The interserver replicas are still read at its end (§2.2). The census check, `p4_census_gate`, the D55-1 and D55-2 rules, the placement, the probes and every enforce gate are unchanged.

D42 is lifted only by a later amendment. It never widens to another network.

### Risk, and why no census-check change

A one-hour window does not see clients that connect less often than hourly. On devnet2 these are the three backup CronJobs (`sentio-node-devnet2-indexer-{a,b,c}-si-backup`, daily at 03:17, 03:47 and 04:17 UTC), a restore, and any operator's occasional reads.

They are not left unchecked:

- The observe documents already admit the reviewed flows, backup and restore included (`SI_NETWORK_FLOWS`, `KEEPER_INGRESS_CLIENTS`, Task 53).
- The probe matrix proves them positively from every pod before each enforce (§7.2, §8.2). The backup rows run from a throwaway pod that carries the CronJob's pod labels (`si_net_probe_backup`).
- After an enforce, a dropped flow or a failing job is handled by the plan's existing rollback: §7.3's watch and §12.3 for an SI policy, §8.3's abort criterion and §12.4 for the Keeper.

**The census check does not treat the reviewed backup and restore flows as covered when they are not observed, and it needs no such rule.** It judges only the flows it observes:

- A flow that does not occur in the window is neither admitted nor refused by it.
- A flow that does occur is judged against the documents like every other.
- An unattributed backup-job line (a pod already gone) is still named under D55-2.

A rule that marked unobserved flows as covered would add nothing that the documents and the positive probe rows do not already prove.

**Added check (Task 55, read-only, before Task 56 starts).** The first scheduled backup run of each host after its installation's enforce must meet two conditions:

- its Job is `Complete`;
- `si_cnp_drops` shows no `POLICY_DENIED` flow towards that installation's ClickHouse since the enforce.

The check is recorded as `task55-backup-after-enforce.txt`. A failed backup or a drop reverts that policy with runbook §12.3 at once. Task 56 drill §5.3's backup (`kubectl create job --from=cronjob/...`) exercises indexer-c's backup flow again, under enforce.

## The text this amends

Wherever Plan S3-D says "7-day census", "a 7-day census", "7 days" or "at least seven samples on seven distinct UTC days" for the census, it now reads "the census of D42 (at least 1 hour, at least 6 Keeper samples at least 10 minutes apart)". This covers:

- the Goal and the Task 54 and Task 55 architecture bullets;
- K57;
- Review Focus 5;
- Task 55's header, Conventions, Step 5 and its expected output;
- contract §37.4;
- the runbook text quoted in Task 54.

In particular:

**Production (hardening runbook, a docs-only PR, inert on merge).**
- The §0 stage table: "Census of every client …, at least 1 hour (D42), read-only".
- `si_net_census`: the default start becomes `now() - INTERVAL 1 HOUR`. The runbook always passes `census-start`.
- §2: "At least one hour, read-only (K57, D42) … §2.3 samples every 10 minutes."
- §2.1: "over the window".
- §2.3: "Every 10 minutes of the window (at least six samples, `n` from 1)", with Expected "at least 6 samples".
- The comment in `storage-integrity-si-v2-network-policy.yaml` ("after the 7-day census") and the lockdown note in `test_render.py`, which concerns the lockdown runbook's own census, stay unchanged.

**Task 55 (local tools, `p4-task.sh`).** `p4_census_window` becomes:

```python
base = sys.argv[1]
start = datetime.datetime.strptime(open(os.path.join(base, "census-start")).read().strip(), "%Y-%m-%d %H:%M:%S")
now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
samples = sorted(glob.glob(os.path.join(base, "census-keeper-*.txt")))
sample_times = sorted(datetime.datetime.fromtimestamp(os.path.getmtime(p), datetime.timezone.utc).replace(tzinfo=None) for p in samples)
if not samples or any(t < start or t > now for t in sample_times):
    sys.exit("Keeper samples must belong to this census window, never before start or in the future: stop")
for p in samples:
    lines = open(p).read().splitlines()
    if not lines or lines[0] != "keeper\tclient\tport\tnamespace\tpod" or any(len(line.split("\t")) != 5 for line in lines[1:] if line.strip()):
        sys.exit("Keeper sample has no complete TSV inventory: stop")
gaps = [(b - a).total_seconds() / 60 for a, b in zip(sample_times, sample_times[1:])]
span = (now - start).total_seconds() / 3600
ok = span >= 1 and len(samples) >= 6 and all(g >= 10 for g in gaps)
print(f"census window (D42): started {start:%Y-%m-%d %H:%M:%S} UTC, {span:.2f} hours ago; {len(samples)} Keeper samples, "
      f"smallest gap {min(gaps) if gaps else 0:.1f} min: {'complete' if ok else 'not complete yet'}")
sys.exit(0 if ok else 1)
```

- **Generated text.** The enforce commit body (`p4_enforce_commit`), the Keeper PR commit body (`p4_keeper_commit`) and the run record (`p4_record_text`) say "after the census (at least 1 hour, D42)" and "a census of at least 1 hour (D42)" instead of "7-day" and "at least 7 days". Their other words, the gates and the record checks stay as they are.
- **Sampling.** The agent takes the §2.3 samples every 10 minutes, from new block files (D24), until `p4_census_window` reports `complete`.
- **Run record.** The `hardening run` record states: "D42: the census window was shortened to at least 1 hour (≥6 Keeper samples ≥10 min apart) by the user's decision of 2026-10-29; daily clients (backup CronJobs) were proven by the probe matrix and the post-enforce backup check, not by the census".

No Appendix C row changes. `s3-ledger.py` is unaffected. Plans S3-E4 and S3-F read no census value.
