# keeper-testbed — Phase 0

Local testbed for the housegate keeper-pool work (CH-egress proxy model).
Validates the **A4 key assumption** before any housegate keeper code is written.

## Topology

```
ch-1 ──<zookeeper>──▶ kproxy-1 ─┐
                                ├─▶ keeper-1 / keeper-2 / keeper-3  (3-node Raft quorum)
ch-2 ──<zookeeper>──▶ kproxy-2 ─┘
ch-1 ◀───── interserver 9009 (direct, Phase 0) ─────▶ ch-2
```

- **3 × `clickhouse-keeper:25.8`** — `keeper-1/2/3`, Raft on 9234, client on 9181,
  `enable_reconfiguration=true`, `snapshot_distance=50` (per architecture doc §4).
- **2 × `kproxy`** — minimal L4 stand-in for housegate's not-yet-built `keeper_proxy`
  (`kproxy/main.go`, seed of `pkg/keeper`). One co-located per CH. CH only ever
  talks to its local kproxy; kproxy forwards to the live quorum. Control endpoint:
  - `POST /drop`     — close every live conn (force a reconnect)
  - `POST /retarget` — rotate active upstream to the next member **and** drop, so the
    reconnect lands on a *different* keeper (the real reconfig/failover path)
  - `GET  /status`   — active upstream + conn counters
  - host ports: kproxy-1 control `:8181`, kproxy-2 control `:8182`
- **2 × `clickhouse-server:25.8`** — `ch-1/ch-2`, two replicas of one
  `ReplicatedMergeTree`. Each `<zookeeper>` points at its co-located kproxy.

## Run

```bash
docker compose up -d --build
./scripts/a4-reconnect-test.sh      # baseline replication + A4 self-heal + retarget
docker compose down -v              # teardown
```

CH native ports on host: ch-1 `:9001`, ch-2 `:9002` (HTTP `:8121` / `:8122`).

## A4 result (clickhouse 25.8, this testbed)

**Assumption:** when the thing fronting 9181 force-drops a CH's keeper connection
(as housegate will on a quorum change), ClickHouse self-heals with no operator
action and no config reload.

**Conclusion: ✅ confirmed.**

| case | observation |
|---|---|
| `/drop` (reconnect to same member) | both CHs healed in **~1.1s**; `is_session_expired=0`, not readonly, `active_replicas=2`; replication resumed (1000→1500 rows) |
| `/retarget` (reconnect to a *different* member) | ch-1 reconnected onto keeper-2 in **<0.5s**; **session continued** (`is_session_expired=0`) because ZK/Keeper sessions are cluster-global; stayed writable; insert replicated (→1600 rows) |

This is the green light for the design: housegate can keep a stable per-node
keeper endpoint and re-steer to the live quorum on membership change; CH treats
each re-steer as an ordinary keeper failover and recovers itself. Only a
force-recovery (which wipes Raft state, §5) would expire the session — and CH
re-establishes ephemerals/watches on the new session in that case too (to be
covered by the A3 force-recovery scenario in a later phase).

## Gotchas found while building this

- **CH 25.8 default config binds loopback only** (`listen_host` = `127.0.0.1`/`::1`).
  CH→keeper (outbound) and local clients work, but CH↔CH interserver (9009) fails
  with `Connection refused`. Fixed with `ch/*/config.d/listen.xml`
  (`<listen_host replace="replace">0.0.0.0</listen_host>`).
- kproxy is intentionally **protocol-unaware** (pure byte relay). A4 tests
  ClickHouse's keeper-client behaviour, not housegate code. Frame-aware parsing
  is Phase 2.
