# Issue #153 Tier 2 comment text

Post the block below to <https://github.com/housegate/housegate/issues/153> only after the final whole-change review. Issue #153 must remain open for Tier 3.

---

**Tier 2 signed inline `INSERT ... VALUES` code is complete and merged.** The implementation landed in PRs [#196](https://github.com/housegate/housegate/pull/196), [#197](https://github.com/housegate/housegate/pull/197), [#198](https://github.com/housegate/housegate/pull/198), [#199](https://github.com/housegate/housegate/pull/199), [#200](https://github.com/housegate/housegate/pull/200) and [#201](https://github.com/housegate/housegate/pull/201), followed by the account-context fix in [#202](https://github.com/housegate/housegate/pull/202). This closes the Tier 2 repository-code scope only: issue #153 remains open for Tier 3, and no production deployment or live production acceptance is claimed.

The issue body's premise that inline `INSERT ... VALUES` sends no row-bearing `ClientData` packet is version-dependent. The approved design records this historical 2026-09-22 raw TCP measurement between a real server and a real client of the same version, using `--compression 0` and table `t (x Int64)`; this table is attributed to that approved measurement and was not rerun during the final Task 6 acceptance:

| Client | Protocol revision | Mode | Query text as sent | Client Data packets after the Query |
|---|---:|---|---|---|
| 25.8 | 54479 | `--query` and interactive REPL | `INSERT INTO t VALUES `, truncated after `VALUES` | empty marker, one row block, empty terminator |
| 26.3 | 54484 | `--query` and interactive REPL | full statement including the rows | one empty marker only |
| 26.7.1 | 54487 | `--query` | full statement including the rows | one empty marker only |
| 26.8.1 | 54488 | `--query` | full statement including the rows | one empty marker only |
| any measured version | version-specific | `FORMAT Values` with rows on stdin | `INSERT INTO t FORMAT Values` | empty marker, one row block, empty terminator |

HouseGate's admission support is narrower than the wire fact: the 25.x truncated `VALUES` shape remains unsupported, while `FORMAT Values` with rows on stdin remains supported by the ordinary signed streaming lane. Tier 2 targets the full-statement 26.3+ shape and the equivalent shape produced by the pinned clickhouse-go `Exec`. The default-off agent lane materializes nondeterminism, applies the closed lexical policy, evaluates rows once through the current session's selected endpoint with no retry, encodes Native packets at the actual upstream revision, rewrites the statement to individually quoted columns plus `FORMAT Native`, and signs the exact bytes it forwards. `INSERT ... SELECT` / `WITH` remain Tier 3.

Local Task 6 acceptance used ClickHouse client `26.8.1.368`, ClickHouse server `25.8.28.1` and the cached rewriter FFI release `v0.11.0`. Exactly five top-level manual Docker cases ran and passed with zero skips. The synthesized admission at revision 54460 and the independent CLI stdin admission at revision 54470 produced the same exact 60-byte Native payload in the same serialization tier; both JWS tokens verified over their own SQL, payload and actual revision, replay produced the same state root, and physical read-back proved two rows after the inline INSERT and four after the independent reference INSERT. The closure-refused case produced the `storage_integrity inline VALUES: ` prefix with zero admissions and zero landed rows, the real 26.8 CLI inline case landed, and the truncated query-text case retained the existing 403 refusal.

The current one-row `String` capture is 28 bytes at revision 54470 and matches the synthesized bytes and both committed fixtures. The earlier 33-byte historical claim did not have a recoverable full capture and is not used as current evidence. Task 6 did not install or execute a real 25.x CLI; its truncated-query test used the exact Go-driver query text, while the existing raw-packet regression separately pins streamed-data behavior.

PR #201 CI installed ClickHouse client `26.10.1.448`, fetched the `v0.11.0` Linux FFI library, and passed all three configured Bazel targets: `//pkg/integration:integration_test`, `//pkg/integration/testenv:testenv_test` and `//pkg/integration/sipressurescale:sipressurescale_test`. The CI command used `--test_output=errors`, so that log does not enumerate the five Task 6 cases or independently prove their skip count; the five-case, zero-skip statement comes from the separately retained and reviewed local Docker run.

PR #202 fixed a late review finding by carrying the configured `agent.owner` payer and `agent.driver` request marker on the helper query with the same existing settings used by ordinary agent queries; the signer remains the operator/indexer key and the server remains the authorization authority. The helper pool now separates owner and driver contexts. The scoped local regressions decoded and validated the actual helper protocol settings, and PR #202's release, build and integration checks all passed. This fix does not mean every helper is unbilled: ordinary query usage follows the authorized payer/driver context, while the table-free helper SELECT emits no separate driver-INSERT `indexing_usage` report.

This acceptance verifies repository behavior, exact wire-byte parity, JWS verification, in-process replay, physical landing through the test admission-owner adapter, and the helper's decoded owner/driver protocol context. It does not execute a live production billing service, prove external charges, verify production ingress ACK2, verify `hg_unsafe` placement or candidate parts, exercise sentio-node/SNode `SourcePreparer`, or establish production deployment or live acceptance.

Operator guide: [docs/agent-inline-values.md](https://github.com/housegate/housegate/blob/main/docs/agent-inline-values.md). Design: [docs/superpowers/specs/2026-09-23-signed-inline-values-design.md](https://github.com/housegate/housegate/blob/main/docs/superpowers/specs/2026-09-23-signed-inline-values-design.md).
