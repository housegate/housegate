# User guide: from a fresh key to a safe read

This guide is for people who store and query data on the Sentio Network through a local HouseGate
agent. It covers installation, the account key, funding, creating a database and tables, writing,
reading, and operator keys. Operators running server-mode HouseGate should read the
[README](../README.md) instead.

The examples use **devnet2**, the network a key-only agent joins by default.

| | devnet2 |
|---|---|
| Chain id | `7892301` (native token ST) |
| Chain RPC | `https://sentio-devnet.test-rpc.sentio.xyz` |
| Explorer | `https://devnet-explorer.sentio.xyz` |
| AddressBook | `0xb7e9E56DFC27bcfA63698F2F5890e186426A0123` |
| Billing | `0x29308290173F324b7Fdb0596A94665144Df17C9b` |

## How it fits together

`clickhouse-client` talks to the agent on `127.0.0.1:9000`. The agent signs every query with your
key and forwards it to the indexer that hosts the database. Writes to storage-integrity (SI) tables
go through a signed statement lane: the network orders them, replicates them and later promotes them
to the verified ("safe") copy. Your address is your account: it owns the databases you create and
pays for the statements you run.

## 1. Install

You need the `housegate` binary and `clickhouse-client` **26.3 or newer** (older clients cannot send
inline `INSERT ... VALUES`; see §6).

```bash
# Homebrew (macOS / Linux)
brew tap housegate/housegate
brew install housegate

# or a release binary (linux-amd64 or darwin-arm64), from
#   https://github.com/housegate/housegate/releases
# or the container image
docker pull ghcr.io/housegate/housegate:latest
```

`foundry`'s `cast` is handy for the on-chain checks below but not required to use the agent.

## 2. Create a key

Any Ethereum private key works. Keep it in a file only you can read and pass it to the agent through
the environment, never as a command-line argument: arguments, including a `$(cat key)` expansion,
are visible in process listings.

```bash
mkdir -p ~/.config/housegate && chmod 700 ~/.config/housegate
umask 077
cast wallet new --json > ~/.config/housegate/new-wallet.json
jq -r '.[0].private_key' ~/.config/housegate/new-wallet.json > ~/.config/housegate/agent.key
jq -r '.[0].address' ~/.config/housegate/new-wallet.json > ~/.config/housegate/agent.address
rm ~/.config/housegate/new-wallet.json
cat ~/.config/housegate/agent.address        # your account address
```

The on-chain transactions below (§3, §8) are optional. To send them, import the same key once into
foundry's encrypted keystore (paste the key at the prompt, then choose a password) and sign with
`--account housegate`:

```bash
cast wallet import housegate --interactive
```

<a id="funding-an-account"></a>

## 3. Funding an account

Every statement is charged to the paying account's balance in the **Billing** contract, not to its
plain token balance:

- each query is one unit, each `INSERT` one unit (two for an inline `INSERT ... VALUES`), charged
  also when the statement is refused;
- each `CREATE DATABASE` and each `CREATE TABLE` is a one-time charge.

A new account's Billing balance is zero, so its first statement fails with:

```
INSUFFICIENT_BALANCE (code=1003): Insufficient balance: payer=0x… signer=0x…; fund the payer's Billing balance, …
```

**On devnet2, contact the Sentio Network team with your account address to get credit.** Credit is a
deposit into your Billing balance; you do not need ST of your own to query or create databases.

If you hold ST yourself, deposit it directly (`amount` in wei, and `--value` must equal it):

```bash
rpc=https://sentio-devnet.test-rpc.sentio.xyz
billing=0x29308290173F324b7Fdb0596A94665144Df17C9b
cast send $billing 'deposit(uint256)' 10000000000000000 --value 10000000000000000 \
  --rpc-url $rpc --account housegate
```

Check the balance (wei) at any time:

```bash
cast call $billing 'balances(address)(uint256)' "$(cat ~/.config/housegate/agent.address)" --rpc-url $rpc
```

`depositTo(account, amount)` funds another account the same way, and `withdraw(amount)` returns
unspent credit as ST.

## 4. Start the agent

```bash
export HOUSEGATE_AGENT_KEY="$(cat ~/.config/housegate/agent.key)"
housegate        # agent mode, devnet2, listening on 127.0.0.1:9000
```

With only a key and no config file the agent joins devnet2, discovers the network and table state
itself and keeps its write sequence counters on disk (see the README's agent section for the
location). Run one agent per key at a time. For verified reads by default, start it with
`-si-read-mode safe` (see §7).

## 5. Create a database and a table

```bash
clickhouse-client --host 127.0.0.1 --port 9000 --query 'CREATE DATABASE mydb'
clickhouse-client --host 127.0.0.1 --port 9000 \
  --query 'CREATE TABLE mydb.events (id UInt64, kind String, ts DateTime64(3)) ENGINE = MergeTree ORDER BY id'
```

The database is created on the indexer your session lands on and registered on chain with you as
its Owner. A new table starts *pending*: writes to it are refused with the retryable code 733
`... is pending activation (retryable)` until it turns active, usually within a minute.

**Admitted column types.** Which types a table may declare is decided by the network's table
admission rules version, a consensus parameter.

- Admission rules v1: `String`, `FixedString(32)`, `Bool`, `Int8`…`Int64`, `UInt8`…`UInt64`,
  `Float32`, `Float64`, `Date`, `DateTime`, `DateTime('UTC')`, and `DateTime64(0..9)` with an
  optional `'UTC'`.
- Admission rules v2, once the network has activated it, adds `Date32`, `UUID`, `Int128`,
  `Int256`, `UInt128`, `UInt256`, `LowCardinality(String)`, and `Nullable(T)` for every type above
  except `LowCardinality(String)`.

Nothing else under either version: no `Decimal`, `Array`, `Map`, `Tuple`, `Enum`, no other
`LowCardinality` and no nested `Nullable`. A `PARTITION BY`, if any, must be a single `String`
column. A `CREATE TABLE` with another type is refused before anything is created (code 392
`... refused by table admission rules ...: column_type: ...; nothing was created`).

**Append-only.** SI tables accept `INSERT` only. `UPDATE`, `DELETE`, `ALTER`, `TRUNCATE` and
`RENAME` are refused; to change a schema, drop the table and create it again.

**No views.** `CREATE VIEW` and `CREATE MATERIALIZED VIEW` are refused in a database with SI
tables (code 392 `... is in a storage-integrity database; views and materialized views cannot be
created there`), and so is a view in another database whose body reads an SI table. Query the
tables directly, or put the view's `SELECT` in your query (a `WITH` clause works). `DROP VIEW` still
works, so a view created before this rule can be removed. Ordinary databases keep views and
materialized views.

### Views read only their own database

A view or materialized view may reference only the database it is created in: its body, every
subquery, `IN` operand, CTE, `UNION` arm and `view()` in it, and a materialized view's `TO` table.
Anything else is refused before ClickHouse sees it:

```
permission: CREATE_VIEW references table "secret" of database "db2"; a view or materialized view may only reference its own database "db1"
```

Holding Owner on both databases does not change this, and `system` tables are refused in a view
body too (they always were). Query the other database directly, or join it in your own `SELECT`.

For the same reason `RENAME TABLE` and `EXCHANGE TABLES` cannot move a table or view to another
database (`... cannot move a table or view to another database`): a view renamed out of its
database would keep reading it, and a materialized view keeps firing on a source, and writing into
a `TO` table, that was moved elsewhere. Renames within one database work as before. To move a
table's data, `INSERT INTO <db2>.<t> SELECT * FROM <db1>.<t>` and drop the old one.

Why: every account's views run as the one ClickHouse user the server shares, so ClickHouse's own
`SQL SECURITY` does not separate accounts. A view in `db1` over `db2` would let every reader of
`db1` (including a public grant) read `db2`, and would go on doing so after the creator lost access
to `db2`. A materialized view would copy `db2` into `db1` on every insert into `db2`, by anyone. A
body that read a database hosted by another indexer would also be rewritten into a `remote()` call
carrying this server's peer credentials (a peer-relay token, or the static peer credentials), and
ClickHouse would store them in the view's definition.

What revoking access means: a view belongs to its database, not to the account that created it.
Reading it needs Read on that database, as reading its tables does. An account that loses access to
the database keeps no access through views it created, and the views stay until someone with Write
runs `DROP VIEW`; `DROP DATABASE` drops them with the database.

**Upgrading a server (operators).** Views created before this rule are not removed or re-checked,
so a cross-database view keeps serving the other database. After upgrading, find them on each
ClickHouse server, with the physical database (`rewriter.physical_database`) in place of `phys`:

```sql
SELECT name, engine
FROM system.tables
WHERE database = 'phys' AND engine IN ('View', 'MaterializedView')
  AND (position(create_table_query, 'remote(') > 0
       OR arrayExists(d -> d != splitByChar('.', name)[1],
                      extractAll(create_table_query, 'phys\\.[`"]([^`".]+)\\.')))
```

Each row is `<database>.<view>`. Drop it through HouseGate (`DROP VIEW <database>.<view>`, which
needs Write on that database) or directly on ClickHouse (`` DROP VIEW phys.`<database>.<view>` ``), and
tell the database's owner.

### Granting access

As Owner (or Admin) you grant other accounts with ordinary `GRANT` statements; addresses are quoted
identifiers:

```bash
clickhouse-client --host 127.0.0.1 --port 9000 --database mydb \
  --query 'GRANT INSERT ON mydb.* TO `0x1111111111111111111111111111111111111111`'   # writer (implies read)
clickhouse-client --host 127.0.0.1 --port 9000 --database mydb \
  --query 'GRANT SELECT ON mydb.* TO `0x2222222222222222222222222222222222222222`'   # reader
```

The grantee then runs its own agent with its own key.

## 6. Write

```bash
# streamed formats: Native, Values (rows on stdin), CSV, TSV, JSONEachRow
printf '1,click,2026-11-10 00:00:00.000\n2,view,2026-11-10 00:00:01.000\n' \
  | clickhouse-client --host 127.0.0.1 --port 9000 --query 'INSERT INTO mydb.events FORMAT CSV'

# inline VALUES (clickhouse-client 26.3 or newer)
clickhouse-client --host 127.0.0.1 --port 9000 \
  --query "INSERT INTO mydb.events VALUES (3, 'click', now64(3))"
```

Client-side constraints on SI writes:

- native protocol only (port 9000 of the agent); there is no HTTP interface;
- no compression: do not pass `--compression` (the client does not compress to a loopback address
  by default);
- no query settings on an SI `INSERT` (including `async_insert`);
- `INSERT ... SELECT` is not supported;
- a 25.x `clickhouse-client` sends inline `VALUES` in a shape the lane refuses: upgrade, or use
  `FORMAT Values` with the rows on stdin;
- at most 64 MiB per statement and 65536 rows per inline `VALUES`;
- in an inline `VALUES`, `now()`, `rand()` and `generateUUIDv4()` are replaced by constants before
  signing, so every row of one statement gets the same value.

A refusal that did not use up the statement's sequence number ends with `[client_seq unspent]`; the
agent reuses that number, and you can simply retry.

## 7. Read

Connect with `--database <db>` (or run `USE <db>`), so the session moves to the database's indexer:

```bash
clickhouse-client --host 127.0.0.1 --port 9000 --database mydb \
  --query "SELECT count() FROM events SETTINGS SQL_x_read_mode = 'safe'"
```

There are two read modes:

| Mode | Returns | Use it for |
|---|---|---|
| `safe` | only rows the network has verified and promoted. New rows appear after promotion, typically within minutes. | anything that must be final; the only mode a replica serves when the owning indexer is down |
| `unsafe_latest` | safe rows plus rows that are ordered but not yet promoted | dashboards that want the newest data and can tolerate rows that are not final |

The default is set by the indexer (devnet2 currently defaults to `unsafe_latest`). Choose per query
with `SETTINGS SQL_x_read_mode = 'safe'`, or for every query of an agent with
`housegate -si-read-mode safe`. When the owning indexer is unavailable the agent fails reads over to
a replica, which answers `safe` only and refuses `unsafe_latest`.

`SELECT *` and `DESCRIBE` hide the internal `_hg_row_id` column. `SHOW CREATE TABLE`, `EXPLAIN`,
session `SET`, `FINAL` and `PREWHERE` are refused on SI tables.

<a id="operator-keys"></a>

## 8. Operator keys

An account can let other keys act and pay on its behalf. This keeps the owning key offline and
gives each service its own key.

1. The owner adds the operator on chain. This is a transaction the owner sends itself, so the owner
   needs a little ST for gas:

   ```bash
   ab=0xb7e9E56DFC27bcfA63698F2F5890e186426A0123
   perms=$(cast call $ab 'getAddress(string)(address)' permissions --rpc-url $rpc)
   cast send $perms 'addOperator(address)' <operator address> \
     --rpc-url $rpc --account housegate
   cast call $perms 'getOperators(address)(address[])' <owner address> --rpc-url $rpc
   ```

2. The operator runs its agent with its own key and names the owner:

   ```bash
   export HOUSEGATE_AGENT_KEY="$(cat ~/.config/housegate/operator.key)"
   housegate -agent-owner <owner address>
   ```

   Its queries are billed to the owner, and its writes to the owner's databases are admitted as the
   owner's. `removeOperator(address)` revokes it.

A query that names a payer whose operator list does not include the signing key fails with
`UNAUTHORIZED_SIGNER (code=1002)`.

## 9. Common errors

| Error | Meaning | What to do |
|---|---|---|
| `INSUFFICIENT_BALANCE (code=1003)` | the payer's Billing balance is zero | §3 |
| `UNAUTHORIZED_SIGNER (code=1002)` | the signer is not an operator of the named payer | §8 |
| `QUERY_REJECTED (code=1001)` | the balance or operator check could not complete | retry |
| 733 `... (retryable)` | the table is pending, or a source is restarting | retry shortly |
| 497 `... is not a writer of database <db>` | your key has no Owner or Write grant on `<db>` | ask its Owner for `GRANT INSERT` |
| 392 `... was refused: ...` | the table uses a type outside the admitted set | `DROP TABLE` and create it with admitted types (§5) |
| `... cannot move a table or view to another database` | a `RENAME TABLE` or `EXCHANGE TABLES` names two databases | rename within one database; copy data with `INSERT ... SELECT` (§5) |
| `... a view or materialized view may only reference its own database ...` | the view's body, or a materialized view's `TO` table, names another database or a `system` table | query the other database directly (§5) |
| 392 `... views and materialized views cannot be created there` | views are not supported in a database with SI tables | query the tables directly (§5) |
| `... is append-only: only INSERT and SELECT are accepted, ...` | SI tables are append-only: `UPDATE`, `DELETE`, `ALTER`, `TRUNCATE`, `GRANT` and other DDL on them are refused, and signing does not change that | use `INSERT`; to change a schema, `DROP TABLE` and create it again (§5) |
| `... can be read only by SELECT, or by INSERT ... SELECT or CREATE TABLE ... AS SELECT into an ordinary table; ...` | a view, a table engine, an `ALTER` / `UPDATE` / `DELETE` subquery or a write into an SI table reads an SI table | query the SI table with `SELECT`, or copy it into an ordinary table first |
| `Cannot load data for command line suggestions: ... system table system.completions is not accessible; ...` (printed once when an interactive `clickhouse-client` starts) | `system.completions` would show every account's database, table and column names, so it is refused; keyword completion still works | nothing; start `clickhouse-client` with `-A` (`--disable_suggestion`) to hide the line |
| `... database information_schema is not accessible; ...`, or `system table system.<t> is not accessible` | `information_schema` and most `system` tables are shared by every account and are refused | use `SHOW DATABASES`, `SHOW TABLES` and `DESCRIBE TABLE` instead |
| `... accepts INSERT only through the signed statement lane, which is not enabled on this server` | the server you reached has no SI ingress lane | send the `INSERT` through the agent to an indexer that hosts the database |
