# Table-Reference Hardening — Plan B: rewriter-grpc (C++)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the C++ engine implement the same table-reference policy as rewriter-go v0.16.0 (Plan A), pass the shared corpus byte-for-byte, close the `SHOW CREATE` / `EXISTS` parameter crash and the `CREATE MATERIALIZED VIEW … TO <Active table>` parity gap, and release rewriter-grpc v0.16.0.

**Architecture:** A pre-handler `preflightTableReferences` in `rewriter-server.cc` mirrors Plan A's preflight (parameters, protected databases, allowlists); `dynamicRewriteWalk` and `collectAccessedTablePairsFromAST` learn IN operands; `rewriteEmbeddedViewBody` becomes `rewriteEmbeddedBody` and serves INSERT … SELECT and CTAS; the SELECT fallback refuses non-SELECT classes; the SI preflight reads `create->targets`. Nothing builds locally: every test cycle runs on the build box.

**Tech Stack:** C++23 against the vendored ClickHouse fork (`clickHouse/` submodule, 26.3-lts), gRPC/protobuf, GoogleTest (`build/tests/rewriter_tests`), CMake + Ninja through `./scripts.sh rebuild`, the private build box (`ssh -p 30100 sentio@64.38.131.242`, dev workdir `/home/sentio/chen/rewriter-grpc/`), CI on same-repo branches.

**Spec:** [housegate/docs/superpowers/specs/2026-09-26-table-reference-hardening-design.md](../specs/2026-09-26-table-reference-hardening-design.md) §5, §8, §10; Plan A ([2026-09-26-table-reference-hardening-engine-go.md](2026-09-26-table-reference-hardening-engine-go.md)) defines every message text and the corpus. Read Plan A's Global Constraints first; they apply here verbatim.

**Working copy:** `/Users/uranuswch/Dev/housegate/rewriter-grpc` is on an unrelated branch. `git fetch origin && git checkout -b feat/table-reference-hardening origin/main` (56a17b7). The build box's dev workdir has the ClickHouse submodule and a warm build cache; use it for the loop below.

**The test loop (every task's "run" step):**

```bash
# from the repo root, after editing
rsync -az --delete --exclude='.git' --exclude='build/' --exclude='clickHouse/' --exclude='contrib' --exclude='docs/' \
  -e "ssh -p 30100" ./ sentio@64.38.131.242:/home/sentio/chen/rewriter-grpc/
ssh -p 30100 sentio@64.38.131.242 "cd /home/sentio/chen/rewriter-grpc && ./scripts.sh rebuild && ninja -C build rewriter_tests && build/tests/rewriter_tests --gtest_filter='<Filter>'"
```

`<Filter>` is named in each step. A full `ctest --test-dir build --output-on-failure` runs before every commit. Pushing the branch runs CI (`.github/workflows/ci.yml`), which is the record for the PR; the rsync loop is for iteration.

## Global Constraints

- Plan A's constraints (message texts, precedence, corpus rules) apply unchanged.
- The corpus file `tests/testdata/storage_integrity_cases.json` is copied from Plan A's merged rewriter-go branch; `tests/si_corpus.h` pins its FNV-1a/64, byte count and case count; the PR description records the SHA-256 that rewriter-go's PR records.
- `third_party/rewriter-proto` is a git submodule; it moves to the v0.4.0 commit (Plan A Task 1).
- The C++ formatter quotes identifiers with backticks (`IdentifierQuotingRule::WhenNecessary`, `common.h`), so success pins that differ from Go only by quoting become `allow_sql_divergence` cases with `want_sql_cpp`; any other divergence is a bug.
- Tests: GoogleTest names follow the existing `TEST(Suite, Name)` layout in `tests/rewriter_test.cc`; new suites are `TableRefParam`, `TableRefProtected`, `TableRefSources`, `TableRefAllowlist`, `TableRefLookup`, `TableRefClasses`, `TableRefParity`.

## Review Focus

1. **`SHOW CREATE TABLE {p:Identifier}` and `EXISTS TABLE {p:Identifier}` in static mode and with no options** (Mode None / Static skip the preflight): the handlers themselves must refuse an empty table name before `setTable("")`. Test in Task 2.
2. **An IN operand that is a CTE alias** (`WITH c AS (…) SELECT … WHERE a IN c`): `dynamicRewriteWalk` must consult `cte_scope` for IN operands as it does for FROM. Test in Task 4.
3. **`INSERT … SELECT` with an inline `FORMAT` payload after the body** is impossible, but `INSERT INTO t SELECT … SETTINGS x = 1` is not: the body rewrite must keep the trailing settings. Test in Task 4.
4. **A `CREATE TABLE … AS SELECT` whose body is a `UNION`** (`ASTSelectWithUnionQuery` with two selects): both arms are rewritten and reported. Test in Task 4.
5. **`EXPLAIN` with an SI table in the body under V2**: refused as unmodelled (the catch-all already fires); with SI off it must also be refused, not rewritten. Test in Task 6.

---

### Task 1: pin rewriter-proto v0.4.0 and load `protected_databases` in the corpus runner

**Files:**
- Modify: `third_party/rewriter-proto` (submodule pointer), `tests/rewriter_test.cc:6100-6150` (`applyStorageIntegrityArgs`), `tests/si_corpus.h` (schema: `protected_databases` as a known `dynamic` key if the loader validates keys)

- [ ] **Step 1: Bump the submodule**

```bash
cd third_party/rewriter-proto && git fetch origin && git checkout v0.4.0 && cd ../..
git add third_party/rewriter-proto
```

- [ ] **Step 2: Write the failing test**

```cpp
TEST(TableRefCorpusLoader, ProtectedDatabasesAreSent) {
  Poco::JSON::Parser parser;
  auto d = parser.parse(R"({"database_map":{"db1":"phys"},"protected_databases":["phys","hg_safe"]})")
             .extract<Poco::JSON::Object::Ptr>();
  rewriter::RewriteTableDynamicArgs dyn;
  applyStorageIntegrityArgs(&dyn, d, "V1");
  ASSERT_EQ(dyn.protected_databases_size(), 2);
  EXPECT_EQ(dyn.protected_databases(0), "phys");
  EXPECT_EQ(dyn.protected_databases(1), "hg_safe");
}
```

- [ ] **Step 3: Run it to verify it fails**

Filter: `TableRefCorpusLoader.*`. Expected: compile error (`protected_databases_size` unknown) until the submodule bump is rsynced; after the bump, FAIL with size 0.

- [ ] **Step 4: Implement**

In `applyStorageIntegrityArgs`, after the `known_physical_databases` loop:

```cpp
  for (const auto &p : si_corpus::JsonStrings(d, "protected_databases"))
    dyn->add_protected_databases(p);
```

If `si_corpus.h`'s loader enumerates allowed `dynamic` keys, add `protected_databases`. Mirror Plan A's rule R9 in the C++ validator: a case named `si_tr_*` must carry a non-empty `protected_databases` array.

- [ ] **Step 5: Run, then commit**

Filter: `TableRefCorpusLoader.*:StorageIntegrityCorpus.*`. Expected: PASS (the corpus is still the old one, so the byte pin still holds).

```bash
git add tests/rewriter_test.cc tests/si_corpus.h third_party/rewriter-proto
git commit -m "chore: rewriter-proto v0.4.0; corpus loader sends protected_databases"
```

---

### Task 2: identifier parameters in table positions (T2) and the crash

**Files:**
- Create: `src/handlers/table_reference.h`, `src/handlers/table_reference.cc` (`hasParameterTableReference`, `preflightTableReferences` skeleton; Task 3 and 5 extend it)
- Modify: `src/rewriter-server.cc:440` (call the preflight before `handleUseQuery`), `src/handlers/writes.cc:227-272` (message text; `hasParameterWriteTarget` stays as the write half), `src/handlers/exists.cc:76`, `src/handlers/show_create.cc:76` (defensive empty-name refusal), `src/handlers/describe.cc:80` (a parameter target is refused, not passed through)
- Modify: `src/CMakeLists.txt` (add `handlers/table_reference.cc` to `rewriter_core`)
- Test: `tests/rewriter_test.cc` (`TableRefParam` suite; update `RejectedBeforeRewriteInEveryMode` and the multi-DROP row at `:4772` to the new message)

**Interfaces:**
- Produces:

```cpp
namespace rewriter_handlers {
constexpr std::string_view kIdentifierParameterMessage =
  "query parameters are not supported in a database or table position";
// True when any database or table position holds an Identifier query
// parameter or an empty name part (the parser's representation of one).
bool hasParameterTableReference(const DB::ASTPtr &ast);
// Dynamic-mode pre-handler pass (spec 2026-09-26 §5). Returns true when it
// populated a rejection.
bool preflightTableReferences(const DB::ASTPtr &ast,
                              const rewriter::RewriteSQLRequest *request,
                              rewriter::RewriteSQLResponse *response);
}
```

- [ ] **Step 1: Write the failing tests**

Add a test helper next to `AddV2Option` (reuse `AddSIContractOption`'s shape):

```cpp
// Plan A's tablerefDynamic: db1 → phys, phys known AND protected, hg_* protected,
// logical context db1; si adds the V2 surface with db1.t Active.
rewriter::RewriteTableDynamicArgs *AddTableRefOption(rewriter::RewriteSQLRequest &req, bool si) {
  auto *opt = req.add_options();
  opt->set_op(rewriter::RewriteOp::TableNameRewrite);
  auto *dyn = opt->mutable_table_name_args()->mutable_dynamic_args();
  (*dyn->mutable_database_map())["db1"] = "phys";
  dyn->add_known_physical_databases("phys");
  dyn->set_upstream_logical_database_in_context("db1");
  dyn->set_delim("_");
  for (const char *p : {"phys", "hg_safe", "hg_unsafe", "hg_promote"}) dyn->add_protected_databases(p);
  if (si) {
    auto *args = dyn->mutable_storage_integrity();
    args->set_contract_version(rewriter::STORAGE_INTEGRITY_CONTRACT_V2);
    args->set_read_mode(rewriter::StorageIntegrityArgs::READ_MODE_SAFE);
    args->set_reserved_row_id_column("_hg_row_id");
    for (const char *r : {"hg_safe", "hg_unsafe", "hg_promote"}) args->add_reserved_databases(r);
    auto &t = (*args->mutable_tables())["db1.t"];
    t.set_safe_table("hg_safe.db1__t");
    t.set_unsafe_table("hg_unsafe.db1__t");
  }
  return dyn;
}

rewriter::RewriteSQLResponse RunTableRef(const std::string &sql, bool si = false) {
  rewriter::RewriteSQLRequest req;
  req.set_sql(sql);
  AddTableRefOption(req, si);
  return RunDirectRewrite(std::move(req));
}
```

Tests:

```cpp
TEST(TableRefParam, RefusedInEveryTablePosition) {
  for (const bool si : {false, true}) {
    for (const char *sql : {
      "SELECT * FROM {p:Identifier}", "SELECT * FROM db1.{p:Identifier}", "SELECT * FROM {d:Identifier}.t",
      "SELECT * FROM db1.o AS a JOIN {p:Identifier} AS b USING (a)", "SELECT * FROM (SELECT * FROM {p:Identifier})",
      "SELECT * FROM db1.o WHERE a IN {p:Identifier}", "SELECT * FROM db1.o WHERE a IN db1.{p:Identifier}",
      "SELECT * FROM db1.o WHERE in(a, {p:Identifier})", "INSERT INTO db1.o SELECT * FROM {p:Identifier}",
      "CREATE TABLE db1.n ENGINE = Memory AS SELECT * FROM {p:Identifier}",
      "CREATE MATERIALIZED VIEW db1.mv TO db1.{p:Identifier} AS SELECT * FROM db1.o",
      "CREATE MATERIALIZED VIEW {p:Identifier} ENGINE = Memory AS SELECT * FROM db1.o",
      "DROP TABLE {p:Identifier}", "DROP TABLE db1.{p:Identifier}", "INSERT INTO db1.{p:Identifier} VALUES (1)",
      "CREATE TABLE {p:Identifier} (a UInt64) ENGINE = Memory", "EXISTS TABLE db1.{p:Identifier}",
      "SHOW CREATE TABLE db1.{p:Identifier}", "DESCRIBE TABLE db1.{p:Identifier}",
      "RENAME TABLE db1.{p:Identifier} TO db1.z", "SHOW TABLES FROM {d:Identifier}", "USE {d:Identifier}",
    }) {
      SCOPED_TRACE(std::string(sql) + (si ? " [si]" : ""));
      const auto resp = RunTableRef(sql, si);
      EXPECT_EQ(resp.code(), rewriter::RewriteCode::InvalidRewriteRequest) << resp.message();
      EXPECT_EQ(resp.message(), rewriter_handlers::kIdentifierParameterMessage);
      EXPECT_EQ(resp.sql_after_rewrite(), sql);
      EXPECT_EQ(resp.statement_type(), rewriter::STATEMENT_TYPE_UNSPECIFIED);
    }
  }
}

TEST(TableRefParam, ValueAndColumnParametersStayAllowed) {
  EXPECT_EQ(RunTableRef("SELECT * FROM db1.o WHERE a = {v:UInt64}").code(), rewriter::RewriteCode::Success);
  EXPECT_EQ(RunTableRef("SELECT {c:Identifier} FROM db1.o").code(), rewriter::RewriteCode::Success);
}

TEST(TableRefParam, ShowCreateAndExistsNeverReachTheFormatterWithAnEmptyName) {
  // Mode None and Static skip the dynamic preflight; the handlers must still refuse.
  for (const char *sql : {"SHOW CREATE TABLE {p:Identifier}", "EXISTS TABLE {p:Identifier}",
                          "SHOW CREATE TABLE db1.{p:Identifier}", "EXISTS TABLE db1.{p:Identifier}"}) {
    SCOPED_TRACE(sql);
    auto none = RunWriteOp(sql);
    EXPECT_EQ(none.code, rewriter::RewriteCode::InvalidRewriteRequest);
    rewriter::RewriteSQLRequest st;
    st.set_sql(sql);
    AddEmptyStaticOption(st);
    EXPECT_EQ(RunDirectRewrite(std::move(st)).code(), rewriter::RewriteCode::InvalidRewriteRequest);
  }
}
```

- [ ] **Step 2: Run to verify they fail**

Filter: `TableRefParam.*`. Expected: the first suite FAILs (`Success` for read positions); the third may crash the test binary (that is the bug) — if it does, note it and move to Step 3.

- [ ] **Step 3: Implement**

`src/handlers/table_reference.cc`:

```cpp
#include "table_reference.h"

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDescribeQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTQueryParameter.h>
#include <Parsers/ASTShowTablesQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTUseQuery.h>
#include <Parsers/TablePropertiesQueriesASTs.h>

#include "common.h"
#include "name_rewrite.h"
#include "storage_integrity.h"

namespace rewriter_handlers {
namespace {

bool identifierIsParameter(const DB::ASTPtr &node) {
  if (!node) return false;
  const auto *id = dynamic_cast<const DB::ASTIdentifier *>(node.get());
  if (!id) return node->as<DB::ASTQueryParameter>() != nullptr;
  if (id->isParam() || id->name().empty()) return true;
  // db.{p:Identifier} flattens to a compound whose last part is empty.
  const auto &parts = id->name_parts;
  return !parts.empty() && parts.back().empty();
}

bool walkForParameter(const DB::ASTPtr &node) {
  if (!node) return false;
  if (const auto *te = node->as<DB::ASTTableExpression>()) {
    if (te->database_and_table_name && identifierIsParameter(te->database_and_table_name)) return true;
  }
  if (const auto *fn = node->as<DB::ASTFunction>()) {
    if (inFamilyName(fn->name) && fn->arguments && fn->arguments->children.size() == 2
        && fn->arguments->children[1]->as<DB::ASTIdentifier>()
        && identifierIsParameter(fn->arguments->children[1]))
      return true;
  }
  for (const auto &child : node->children)
    if (walkForParameter(child)) return true;
  return false;
}

}  // namespace

// inFamilyName is exported through table_reference.h; Tasks 3–4 reuse it.
bool inFamilyName(const std::string &name) {
  static const std::unordered_set<std::string> names = {
    "in", "notin", "nullin", "notnullin", "globalin", "globalnotin", "globalnullin", "globalnotnullin",
    "inignoreset", "notinignoreset", "nullinignoreset", "notnullinignoreset",
    "globalinignoreset", "globalnotinignoreset", "globalnullinignoreset", "globalnotnullinignoreset"};
  return names.count(lower(name)) > 0;
}

bool hasParameterTableReference(const DB::ASTPtr &ast) {
  if (!ast) return false;
  if (hasParameterWriteTarget(ast)) return true;  // exported from writes.cc (see below)
  if (const auto *create = ast->as<DB::ASTCreateQuery>()) {
    if (create->targets) {
      for (const auto &child : create->targets->children)
        if (identifierIsParameter(child)) return true;
      if (create->hasTargetTableID(DB::ViewTarget::To)
          && create->getTargetTableID(DB::ViewTarget::To).table_name.empty())
        return true;
    }
  }
  if (const auto *show = ast->as<DB::ASTShowTablesQuery>()) {
    if (show->from && (show->from->as<DB::ASTQueryParameter>() || identifierIsParameter(show->from))) return true;
  }
  if (const auto *use = ast->as<DB::ASTUseQuery>()) {
    if (identifierIsParameter(use->database)) return true;
  }
  if (const auto *q = dynamic_cast<const DB::ASTQueryWithTableAndOutput *>(ast.get())) {
    if (identifierIsParameter(q->database) || identifierIsParameter(q->table)) return true;
  }
  if (const auto *desc = ast->as<DB::ASTDescribeQuery>()) {
    if (desc->table_expression && walkForParameter(desc->table_expression)) return true;
  }
  return walkForParameter(ast);
}

bool preflightTableReferences(const DB::ASTPtr &ast,
                              const rewriter::RewriteSQLRequest *request,
                              rewriter::RewriteSQLResponse *response) {
  const auto sel = findActiveTableRewrite(request->options());
  if (sel.mode != TableRewriteMode::Dynamic) return false;
  if (hasParameterTableReference(ast)) {
    response->set_code(rewriter::RewriteCode::InvalidRewriteRequest);
    response->set_message(std::string(kIdentifierParameterMessage));
    return true;
  }
  return false;  // Tasks 3 and 5 add the protected-name and allowlist steps here.
}

}  // namespace rewriter_handlers
```

`lower` is a file-local helper in `storage_integrity.cc`; expose it through `storage_integrity.h` (`std::string lowerName(std::string)`) or duplicate the four lines. `ASTUseQuery::database` is an `ASTPtr` on the 26.3 fork (`use.cc` reads `getDatabase()`); if it is a `std::string` there is no parameter form and the branch is dropped. Check the field names against the vendored headers on the box (`grep -n 'database' clickHouse/src/Parsers/ASTUseQuery.h`). `hasParameterWriteTarget` is currently `static`/anonymous in `writes.cc`: move its declaration to `writes.h` (or a new `write_targets.h`) and change `kQueryParameterWriteTargetMessage` to `kIdentifierParameterMessage`.

`rewriter-server.cc`, right after the existence-clause block and before `handleUseQuery`:

```cpp
    if (rewriter_handlers::preflightTableReferences(ast, request, response)) return;
```

`exists.cc` and `show_create.cc`, right after `origin_table` is read:

```cpp
  if (origin_table.empty() || identifierIsParameter(q->table)) {
    rejectInvalidRequest(response, std::string(kIdentifierParameterMessage));
    return ExistsDispatchResult::Handled;  // ShowCreateDispatchResult::Handled in show_create.cc
  }
```

(`identifierIsParameter` becomes a small exported helper in `table_reference.h` for this use.) `describe.cc`: when `parseDescribeTarget` returns `nullopt` and `hasParameterTableReference(ast)` is true, reject with the same message instead of `return false` (the fall-through was the "Success" path).

`src/CMakeLists.txt`: add `handlers/table_reference.cc` to the `rewriter_core` source list next to `handlers/storage_integrity.cc`.

- [ ] **Step 4: Update the existing parameter tests**

`WriteTargetQueryParameter.RejectedBeforeRewriteInEveryMode` and the multi-DROP row: expected message becomes `kIdentifierParameterMessage`. `kQueryParameterWriteTargetMessage` is deleted.

- [ ] **Step 5: Run, full ctest, commit**

Filter: `TableRefParam.*:WriteTargetQueryParameter.*:StorageIntegrityContractV2.*`. Expected: PASS, no crash. Then `ctest --test-dir build --output-on-failure`: the corpus suite still passes (no `si_tr_` cases yet).

```bash
git add src/handlers/table_reference.h src/handlers/table_reference.cc src/handlers/writes.cc src/handlers/writes.h src/handlers/exists.cc src/handlers/show_create.cc src/handlers/describe.cc src/rewriter-server.cc src/CMakeLists.txt tests/rewriter_test.cc
git commit -m "feat: refuse Identifier query parameters in every table position; close the SHOW CREATE / EXISTS empty-name crash (spec T2, T12)"
```

---

### Task 3: protected databases (T3)

**Files:**
- Modify: `src/handlers/storage_integrity.h/.cc` (`protectedDatabase`, `protectedDatabaseRejectMessage`, `validateProtectedDatabases`), `src/handlers/name_rewrite.cc:100-113` (`resolvePhysicalDatabase`: protected known-physical is not a pass-through), `src/handlers/table_reference.cc` (`collectDatabaseReferences`, the protected step), `src/rewriter-server.cc:338-360` (validate `protected_databases` for every dynamic request), `src/handlers/use.cc:46` (defensive check)
- Test: `tests/rewriter_test.cc` (`TableRefProtected` suite)

**Interfaces:**
- Produces:

```cpp
bool protectedDatabase(const std::string &name, const rewriter::RewriteTableDynamicArgs &args);
std::string protectedDatabaseRejectMessage(const std::string &name); // "protected database <name> is not addressable"
bool validateProtectedDatabases(const rewriter::RewriteTableDynamicArgs &args, std::string *error);
std::vector<std::string> collectDatabaseReferences(const DB::ASTPtr &ast, const rewriter::RewriteSQLRequest *request);
```

- [ ] **Step 1: Write the failing tests**

```cpp
TEST(TableRefProtected, RefusedInEveryPosition) {
  for (const char *db : {"phys", "hg_safe", "hg_unsafe", "hg_promote"}) {
    const std::string msg = std::string("protected database ") + db + " is not addressable";
    for (const char *shape : {
      "SELECT * FROM %s.`db2.x`", "SELECT * FROM db1.o AS a JOIN %s.`db2.x` AS b USING (a)",
      "SELECT * FROM (SELECT * FROM %s.`db2.x`)", "WITH c AS (SELECT * FROM %s.`db2.x`) SELECT * FROM c",
      "SELECT * FROM db1.o WHERE a IN %s.`db2.x`", "SELECT * FROM db1.o WHERE a IN (%s.`db2.x`)",
      "SELECT * FROM db1.o WHERE (a, b) IN %s.`db2.x`", "SELECT * FROM db1.o WHERE in(a, %s.`db2.x`)",
      "SELECT * FROM db1.o WHERE a GLOBAL IN %s.`db2.x`", "INSERT INTO db1.o SELECT * FROM %s.`db2.x`",
      "CREATE TABLE db1.n ENGINE = Memory AS SELECT * FROM %s.`db2.x`", "CREATE VIEW db1.v AS SELECT * FROM %s.`db2.x`",
      "CREATE MATERIALIZED VIEW db1.mv TO %s.`db2.x` AS SELECT * FROM db1.o", "INSERT INTO %s.`db2.x` VALUES (1)",
      "DROP TABLE %s.`db2.x`", "CREATE TABLE %s.`db2.x` (a UInt64) ENGINE = Memory", "CREATE TABLE db1.n AS %s.`db2.x`",
      "RENAME TABLE %s.`db2.x` TO db1.z", "EXISTS TABLE %s.`db2.x`", "SHOW CREATE TABLE %s.`db2.x`",
      "DESCRIBE TABLE %s.`db2.x`", "SHOW TABLES FROM %s", "USE %s", "CREATE DATABASE %s", "DROP DATABASE %s",
      "SELECT * FROM merge('%s', 'db2')", "SELECT * FROM remote('127.0.0.1:9000', '%s', 'db2.x')",
      "CREATE TABLE db1.n (a UInt64) ENGINE = Merge('%s', '^db2')", "SELECT joinGet('%s.`db2.x`', 'v', 1)",
    }) {
      std::string sql = shape;
      for (size_t pos; (pos = sql.find("%s")) != std::string::npos;) sql.replace(pos, 2, db);
      for (const bool si : {false, true}) {
        SCOPED_TRACE(sql + (si ? " [si]" : ""));
        const auto resp = RunTableRef(sql, si);
        EXPECT_NE(resp.code(), rewriter::RewriteCode::Success) << resp.message();
        if (!(si && std::string(db) != "phys")) {
          EXPECT_EQ(resp.code(), rewriter::RewriteCode::InvalidRewriteRequest);
          EXPECT_EQ(resp.message(), msg);
        } else {
          EXPECT_NE(resp.message().find("storage-integrity"), std::string::npos);  // the SI handler's own message
        }
        EXPECT_EQ(resp.sql_after_rewrite(), sql);
      }
    }
  }
}

TEST(TableRefProtected, ColumnAliasAndLiteralUsesAreAllowed) {
  EXPECT_EQ(RunTableRef("SELECT phys FROM db1.o").code(), rewriter::RewriteCode::Success);
  EXPECT_EQ(RunTableRef("SELECT a AS hg_safe FROM db1.o").code(), rewriter::RewriteCode::Success);
  EXPECT_EQ(RunTableRef("SELECT * FROM db1.o WHERE s = 'phys'").code(), rewriter::RewriteCode::Success);
}

TEST(TableRefProtected, ProtectedLogicalContextIsRefused) {
  rewriter::RewriteSQLRequest req;
  req.set_sql("SELECT * FROM o");
  AddTableRefOption(req, false)->set_upstream_logical_database_in_context("phys");
  const auto resp = RunDirectRewrite(std::move(req));
  EXPECT_EQ(resp.code(), rewriter::RewriteCode::InvalidRewriteRequest);
  EXPECT_EQ(resp.message(), "protected database phys is not addressable");
}

TEST(TableRefProtected, EntriesMustBeSimpleIdentifiers) {
  rewriter::RewriteSQLRequest req;
  req.set_sql("SELECT 1");
  AddTableRefOption(req, false)->add_protected_databases("hg-promote");
  const auto resp = RunDirectRewrite(std::move(req));
  EXPECT_EQ(resp.code(), rewriter::RewriteCode::InvalidRewriteRequest);
  EXPECT_EQ(resp.message(), "protected_databases entry \"hg-promote\" must be a simple identifier");
}
```

- [ ] **Step 2: Run to verify they fail**

Filter: `TableRefProtected.*`. Expected: FAIL (`Success` for the `phys` rows and the SI-off `hg_*` rows).

- [ ] **Step 3: Implement**

`storage_integrity.cc` (public section):

```cpp
bool protectedDatabase(const std::string &name, const rewriter::RewriteTableDynamicArgs &args) {
  if (name.empty()) return false;
  for (const auto &[_, physical] : args.database_map())
    if (physical == name) return true;
  for (const auto &p : args.protected_databases())
    if (p == name) return true;
  return isStorageIntegrityPhysicalDatabase(name, args);
}

std::string protectedDatabaseRejectMessage(const std::string &name) {
  return "protected database " + name + " is not addressable";
}

bool validateProtectedDatabases(const rewriter::RewriteTableDynamicArgs &args, std::string *error) {
  for (const auto &p : args.protected_databases()) {
    if (!simpleIdentifier(p)) {
      if (error) *error = "protected_databases entry \"" + p + "\" must be a simple identifier";
      return false;
    }
  }
  return true;
}
```

`name_rewrite.cc` `resolvePhysicalDatabase`: inside the `known_physical_databases` loop, before `return logical_db;`, `for (const auto &p : args.protected_databases()) if (p == logical_db) return std::nullopt;`.

`rewriter-server.cc`: after the SI validation block, for every `Dynamic` selection: `if (std::string err; !validateProtectedDatabases(*si_selection.dynamic_args, &err)) { response->set_sql_after_rewrite(original_query); response->set_message(err); response->set_code(InvalidRewriteRequest); return; }`.

`table_reference.cc`:

```cpp
std::vector<std::string> collectDatabaseReferences(const DB::ASTPtr &ast,
                                                   const rewriter::RewriteSQLRequest *request) {
  std::vector<std::string> out;
  std::unordered_set<std::string> seen;
  auto add = [&](const std::string &db) { if (!db.empty() && seen.insert(db).second) out.push_back(db); };
  auto addIdent = [&](const DB::ASTPtr &node) {
    if (const auto *id = node ? node->as<DB::ASTIdentifier>() : nullptr) {
      const auto [db, table] = splitTableIdentifier(id->name());  // storage_integrity.cc helper; expose it
      add(db);
    }
  };
  std::function<void(const DB::ASTPtr &)> walk = [&](const DB::ASTPtr &node) {
    if (!node) return;
    if (const auto *te = node->as<DB::ASTTableExpression>()) addIdent(te->database_and_table_name);
    if (const auto *fn = node->as<DB::ASTFunction>()) {
      if (inFamilyName(fn->name) && fn->arguments && fn->arguments->children.size() == 2)
        addIdent(fn->arguments->children[1]);
      if (isStringLookup(fn->name) && fn->arguments && !fn->arguments->children.empty()) {
        if (const auto *lit = fn->arguments->children[0]->as<DB::ASTLiteral>();
            lit && lit->value.getType() == DB::Field::Types::String) {
          const auto [db, table] = splitTableIdentifier(unquoteQualified(lit->value.safeGet<String>()));
          add(db);
        }
      }
    }
    for (const auto &child : node->children) walk(child);
  };
  walk(ast);
  for (const auto &ref : collectStorageIntegrityNamespaceRefs(ast)) add(ref.database);
  if (const auto *create = ast->as<DB::ASTCreateQuery>()) {
    add(create->getDatabase());
    add(create->as_database);
    if (create->targets && create->hasTargetTableID(DB::ViewTarget::To))
      add(create->getTargetTableID(DB::ViewTarget::To).database_name);
  }
  if (const auto *q = dynamic_cast<const DB::ASTQueryWithTableAndOutput *>(ast.get())) add(q->getDatabase());
  if (const auto *drop = ast->as<DB::ASTDropQuery>()) {
    add(drop->getDatabase());
    if (drop->database_and_tables)
      for (const auto &child : drop->database_and_tables->children) addIdent(child);
  }
  if (const auto *rename = ast->as<DB::ASTRenameQuery>())
    for (const auto &e : rename->getElements()) { add(e.from.getDatabase()); add(e.to.getDatabase()); }
  if (const auto *use = ast->as<DB::ASTUseQuery>()) add(use->getDatabase());
  if (const auto *show = ast->as<DB::ASTShowTablesQuery>()) add(show->getFrom());
  if (const auto *desc = ast->as<DB::ASTDescribeQuery>()) walk(desc->table_expression);
  return out;
}
```

`isStringLookup` is defined in Task 5 (declare it now, returning false until then, or implement the name set immediately). `unquoteQualified` strips backticks around each dotted part (`phys.\`db2.x\`` → `phys.db2.x`; the db part is what matters). Then, in `preflightTableReferences` after the parameter step:

```cpp
  const auto &args = *sel.dynamic_args;
  auto dbs = collectDatabaseReferences(ast, request);
  if (!args.upstream_logical_database_in_context().empty())
    dbs.push_back(args.upstream_logical_database_in_context());
  for (const auto &db : dbs) {
    if (!protectedDatabase(db, args)) continue;
    if (isStorageIntegrityPhysicalDatabase(db, args)) continue;  // the SI handlers own it while the surface is active
    recordAccessedDatabase(response, db, &args);
    response->set_code(rewriter::RewriteCode::InvalidRewriteRequest);
    response->set_message(protectedDatabaseRejectMessage(db));
    return true;
  }
```

`use.cc`: after the SI physical check add `if (protectedDatabase(origin_db, *dynamic)) { recordAccessedDatabase(response, origin_db, dynamic); rejectInvalid(response, protectedDatabaseRejectMessage(origin_db)); return true; }`.

- [ ] **Step 4: Run, ctest, commit**

Filter: `TableRefProtected.*:TableRefParam.*`. Expected: PASS. If an SI-active `hg_*` row returns the protected message instead of the SI message, the `isStorageIntegrityPhysicalDatabase` skip is not reached: check `storageIntegritySurfaceActive` on the request. Full ctest: PASS.

```bash
git add src/handlers/storage_integrity.h src/handlers/storage_integrity.cc src/handlers/name_rewrite.cc src/handlers/table_reference.cc src/handlers/table_reference.h src/handlers/use.cc src/rewriter-server.cc tests/rewriter_test.cc
git commit -m "feat: protected databases are refused in every table position (spec T3)"
```

---

### Task 4: IN operands and embedded bodies are rewritten and reported (T4)

**Files:**
- Modify: `src/handlers/select.cc:210-240` (`collectAccessedTablePairsFromAST`: IN operands), `:285-400` (`dynamicRewriteWalk`: IN operands), `:1023-1089` (`rewriteEmbeddedViewBody` → `rewriteEmbeddedBody(options, body, response, EmbeddedBodyKind)`), `src/handlers/select.h` (declaration)
- Modify: `src/handlers/writes.cc:388-394` (drop the non-view CREATE body SI rejection), `:484-489` (drop the INSERT SELECT SI rejection), `:607-621` (view path calls the renamed function with `EmbeddedBodyKind::View`), `:825-847` (CTAS body), `:1009-1051` (INSERT body)
- Modify: `src/handlers/storage_integrity.cc:245-277` (`decodeInFunction` keeps only literal/expression operands as namespace refs; identifier operands are ordinary tables now)
- Test: `tests/rewriter_test.cc` (`TableRefSources` suite; flip `CreateTable_AsSelect_ReturnsTargetOnly` → `…ReturnsTargetAndSource` (count 2), `Insert_Select_ReturnsTargetOnly` → `…ReturnsTargetAndSource` (count 2))

- [ ] **Step 1: Write the failing tests**

```cpp
TEST(TableRefSources, InOperandsAreRewrittenAndReported) {
  struct Case { const char *sql; const char *want_sql; std::vector<std::string> accessed; bool si = false; };
  const std::vector<Case> cases = {
    {"SELECT * FROM db1.o WHERE a IN db1.p", "SELECT * FROM phys.`db1.o` AS `db1.o` WHERE a IN phys.`db1.p`", {"db1.o", "db1.p"}},
    {"SELECT * FROM db1.o WHERE a IN (db1.p)", "SELECT * FROM phys.`db1.o` AS `db1.o` WHERE a IN phys.`db1.p`", {"db1.o", "db1.p"}},
    {"SELECT * FROM db1.o WHERE (a, b) IN db1.p", "SELECT * FROM phys.`db1.o` AS `db1.o` WHERE (a, b) IN phys.`db1.p`", {"db1.o", "db1.p"}},
    {"SELECT * FROM db1.o WHERE in(a, db1.p)", "SELECT * FROM phys.`db1.o` AS `db1.o` WHERE in(a, phys.`db1.p`)", {"db1.o", "db1.p"}},
    {"SELECT * FROM db1.o WHERE a GLOBAL IN db1.p", "SELECT * FROM phys.`db1.o` AS `db1.o` WHERE a GLOBAL IN phys.`db1.p`", {"db1.o", "db1.p"}},
    {"SELECT * FROM db1.o WHERE a IN p", "SELECT * FROM phys.`db1.o` AS `db1.o` WHERE a IN phys.`db1.p`", {"db1.o", "db1.p"}},
    {"SELECT * FROM db1.o WHERE a IN db1.o", "SELECT * FROM phys.`db1.o` AS `db1.o` WHERE a IN phys.`db1.o`", {"db1.o"}, true},
    {"SELECT * FROM db1.o WHERE a IN db1.t",
     "SELECT * FROM phys.`db1.o` AS `db1.o` WHERE a IN (SELECT * EXCEPT (_hg_row_id) FROM hg_safe.db1__t)", {"db1.o", "db1.t"}, true},
    {"WITH c AS (SELECT 1 AS a) SELECT * FROM db1.o WHERE a IN c",
     "WITH c AS (SELECT 1 AS a) SELECT * FROM phys.`db1.o` AS `db1.o` WHERE a IN c", {"db1.o"}},
    {"SELECT * FROM db1.o WHERE a IN system.tables", "SELECT * FROM phys.`db1.o` AS `db1.o` WHERE a IN system.tables", {"db1.o", "system.tables"}},
  };
  for (const auto &c : cases) {
    SCOPED_TRACE(c.sql);
    const auto resp = RunTableRef(c.sql, c.si);
    EXPECT_EQ(resp.code(), rewriter::RewriteCode::Success) << resp.message();
    EXPECT_EQ(si_corpus::NormalizeSIIdentifierQuotes(resp.sql_after_rewrite()),
              si_corpus::NormalizeSIIdentifierQuotes(c.want_sql));
    std::vector<std::string> got;
    for (const auto &a : resp.original_accessed_tables()) got.push_back(a.original_database() + "." + a.original_table());
    EXPECT_EQ(got, c.accessed);
  }
}

TEST(TableRefSources, EmbeddedBodiesAreRewrittenAndReported) {
  struct Case { const char *sql; const char *want_sql; std::vector<std::string> accessed; bool si = false; };
  const std::vector<Case> cases = {
    {"INSERT INTO db1.o SELECT * FROM db1.p", "INSERT INTO phys.`db1.o` SELECT * FROM phys.`db1.p` AS `db1.p`", {"db1.o", "db1.p"}},
    {"INSERT INTO db1.o SELECT * FROM p", "INSERT INTO phys.`db1.o` SELECT * FROM phys.`db1.p` AS p", {"db1.o", "db1.p"}},
    {"INSERT INTO db1.o SELECT * FROM db1.p SETTINGS max_threads = 1",
     "INSERT INTO phys.`db1.o` SELECT * FROM phys.`db1.p` AS `db1.p` SETTINGS max_threads = 1", {"db1.o", "db1.p"}},
    {"INSERT INTO db1.o SELECT * FROM (SELECT * FROM db1.p WHERE a IN db1.q)", "", {"db1.o", "db1.p", "db1.q"}},
    {"INSERT INTO db1.o SELECT * FROM db1.t",
     "INSERT INTO phys.`db1.o` SELECT * FROM (SELECT * EXCEPT (_hg_row_id) FROM hg_safe.db1__t) AS `db1.t`", {"db1.o", "db1.t"}, true},
    {"CREATE TABLE db1.n ENGINE = Memory AS SELECT * FROM db1.p", "CREATE TABLE phys.`db1.n` ENGINE = Memory AS SELECT * FROM phys.`db1.p` AS `db1.p`", {"db1.n", "db1.p"}},
    {"CREATE TABLE db1.n ENGINE = Memory AS SELECT a FROM db1.p UNION ALL SELECT a FROM db1.q", "", {"db1.n", "db1.p", "db1.q"}},
    {"CREATE TABLE db1.n ENGINE = Memory AS SELECT * FROM db1.t", "", {"db1.n", "db1.t"}, true},
  };
  for (const auto &c : cases) {
    SCOPED_TRACE(c.sql);
    const auto resp = RunTableRef(c.sql, c.si);
    EXPECT_EQ(resp.code(), rewriter::RewriteCode::Success) << resp.message();
    if (*c.want_sql)
      EXPECT_EQ(si_corpus::NormalizeSIIdentifierQuotes(resp.sql_after_rewrite()), si_corpus::NormalizeSIIdentifierQuotes(c.want_sql));
    std::vector<std::string> got;
    for (const auto &a : resp.original_accessed_tables()) got.push_back(a.original_database() + "." + a.original_table());
    EXPECT_EQ(got, c.accessed);
  }
  // A CTAS into an Active target stays refused; an INSERT into one stays a marked Success.
  EXPECT_EQ(RunTableRef("CREATE TABLE db1.t ENGINE = Memory AS SELECT * FROM db1.o", true).code(), rewriter::RewriteCode::UnsupportedStatement);
  const auto ins = RunTableRef("INSERT INTO db1.t SELECT * FROM db1.o", true);
  EXPECT_EQ(ins.code(), rewriter::RewriteCode::Success) << ins.message();
  ASSERT_GE(ins.original_accessed_tables_size(), 1);
  EXPECT_TRUE(ins.original_accessed_tables(0).is_storage_integrity());
}
```

The exact C++ spellings (`AS `db1.o``, `ENGINE = Memory AS SELECT`) are the current formatter's; correct them from the first green run's output where the formatter differs, then keep them (they become `want_sql_cpp` pins).

- [ ] **Step 2: Run to verify they fail**

Filter: `TableRefSources.*`. Expected: FAIL.

- [ ] **Step 3: Implement**

`select.cc` `collectAccessedTablePairsFromAST`: add, before the recursive visit,

```cpp
  if (const auto *fn = node->as<DB::ASTFunction>()) {
    if (inFamilyName(fn->name) && fn->arguments && fn->arguments->children.size() == 2) {
      if (const auto *id = fn->arguments->children[1]->as<DB::ASTIdentifier>()) {
        const auto full = id->name();
        const bool is_cte_ref = full.find('.') == std::string::npos && cte_scope.count(full) > 0;
        if (!full.empty() && !is_cte_ref) {
          AccessedTablePair p;
          if (const auto dot = full.find('.'); dot != std::string::npos) {
            p.original_db = full.substr(0, dot);
            p.original_table = full.substr(dot + 1);
          } else {
            p.original_table = full;
          }
          out.emplace(full, std::move(p));
        }
      }
    }
  }
```

(`inFamilyName` is Task 2's helper, declared in `table_reference.h`.) ClickHouse parses `a IN db1.p` as `ASTFunction("in")` whose second argument is an `ASTIdentifier` (not `ASTTableIdentifier`); `a IN (db1.p)` is the same shape; a tuple left side changes only the first argument.

`dynamicRewriteWalk`: add a branch before the generic recursion:

```cpp
  if (auto *fn = ast->as<DB::ASTFunction>();
      fn && inFamilyName(fn->name) && fn->arguments && fn->arguments->children.size() == 2) {
    auto &operand = fn->arguments->children[1];
    if (auto *id = operand->as<DB::ASTIdentifier>()) {
      const std::string full = id->name();
      const bool is_cte_ref = full.find('.') == std::string::npos && cte_scope.count(full) > 0;
      if (!full.empty() && !is_cte_ref) {
        auto [origin_db, origin_table] = splitTableIdentifier(full);
        if (auto hit = lookupStorageIntegrity(origin_db, origin_table, args)) {
          auto subquery = buildStorageIntegrityDerivedTable(*hit->table, args.storage_integrity());
          operand = subquery;
          fn->arguments->children[1] = subquery;
          if (out_table_rewrites) (*out_table_rewrites)[full] = hit->table->safe_table();
        } else {
          const auto outcome = applyDynamicRewrite(origin_db, origin_table, args, "IN");
          if (outcome.status == DynamicRewriteStatus::OK) {
            auto replacement = DB::makeRewriterAST<DB::ASTTableIdentifier>(outcome.physical_db, outcome.new_table);
            fn->arguments->children[1] = replacement;
            if (out_table_rewrites) (*out_table_rewrites)[full] = outcome.physical_db + "." + outcome.new_table;
          } else if (outcome.status == DynamicRewriteStatus::OkRemote) {
            auto addr = DB::makeRewriterAST<DB::ASTLiteral>(outcome.remote_addr);
            auto db   = DB::makeRewriterAST<DB::ASTLiteral>(outcome.physical_db);
            auto tbl  = DB::makeRewriterAST<DB::ASTLiteral>(outcome.new_table);
            auto user = DB::makeRewriterAST<DB::ASTLiteral>(outcome.remote_user);
            auto pass = DB::makeRewriterAST<DB::ASTLiteral>(outcome.remote_password);
            fn->arguments->children[1] = DB::makeASTFunction("remote", addr, db, tbl, user, pass);
            if (out_table_rewrites) (*out_table_rewrites)[full] = outcome.physical_db + "." + outcome.new_table;
          }
          // InvalidRequest: lenient skip, as for FROM.
        }
      }
      // The left operand may hold subqueries; walk it.
      visitChildrenWithCTEVisibility(*fn->arguments->children[0], cte_scope,
        [&](const DB::ASTPtr &child, const CTEAliasScope &child_scope) { dynamicRewriteWalk(child, args, out_table_rewrites, child_scope); });
      return;
    }
  }
```

`buildStorageIntegrityDerivedTable` returns the `ASTSubquery` the FROM path installs; as an IN operand the same node formats as `IN (SELECT …)`. Check `ASTTableIdentifier`'s two-argument constructor on the fork (`Parsers/ASTIdentifier.h`); if it takes a `StorageID`, build one.

`storage_integrity.cc` `decodeInFunction`: return `std::nullopt` when `args[1]` is an `ASTIdentifier` (now an ordinary table reference), keep decoding literal/expression operands. `rejectStorageIntegrityNamespaces` then never sees identifier IN operands; `validateStorageIntegritySelectTargets` sees them through `originals` and keeps the physical/authorization messages.

`rewriteEmbeddedViewBody` → rename to `rewriteEmbeddedBody(options, body, response, EmbeddedBodyKind kind)` with `enum class EmbeddedBodyKind { View, InsertSelect, CreateSelect }`; the `recordLogicalStorageIntegrityRewrites` refusal (a view body may not capture an SI read) stays only for `View`; for the other two kinds the derived-table substitution inside `dynamicRewriteWalk` handles Active tables. Keep a one-line `rewriteEmbeddedViewBody` wrapper for the view call site or update it. In `writes.cc`:

- INSERT block: after `rewriteOneTarget`, `if (insert_query->select) { auto body = ownedChild(*insert_query, insert_query->select); if (body) { rewriteEmbeddedBody(request->options(), *body, response, EmbeddedBodyKind::InsertSelect); if (response->code() != Success) return Rejected; } }` before `formatAst`.
- CREATE TABLE block: after the `as_table` step, `if (!create_query->is_ordinary_view && !create_query->is_materialized_view && create_query->select) { … EmbeddedBodyKind::CreateSelect … }`.
- Delete the SI preflight blocks at `:388-394` and `:484-489` (the body pipeline decides sources; targets are unchanged).

`ownedChild` exists in `storage_integrity.cc` (`std::optional<DB::ASTPtr> ownedChild(const DB::IAST &, const DB::IAST *)`); expose it.

- [ ] **Step 4: Flip the two old tests, run, ctest, commit**

`CreateTable_AsSelect_ReturnsTargetOnly` → `CreateTable_AsSelect_ReturnsTargetAndSource`, comment and count `2`; same for `Insert_Select_…`. Filter: `TableRefSources.*:WriteOp.*`. Expected: PASS. Full ctest: the corpus `si_ordinary_in_table_allowed` and the CTE-bound `IN t` case now FAIL against the old corpus — expected until Task 8 copies the new corpus; every other corpus case must still pass.

```bash
git add src/handlers/select.cc src/handlers/select.h src/handlers/writes.cc src/handlers/storage_integrity.cc src/handlers/storage_integrity.h tests/rewriter_test.cc
git commit -m "feat: rewrite and report IN operands, INSERT … SELECT and CTAS sources (spec T4)"
```

---

### Task 5: allowlists (T5) and string lookups (T6)

**Files:**
- Modify: `src/handlers/table_reference.h/.cc` (lists, `classifyTableFunction`, `tableEngineAllowed`, `refusedTableSetting`, `isStringLookup`, the allowlist step of the preflight, `collectSourceFunctionNames`, `createTableStorage`)
- Modify: `src/handlers/select.cc` (string-lookup rewrite in `handleSelectQuery` and `rewriteEmbeddedBody`, before `applyRewriteOptions`)
- Test: `tests/rewriter_test.cc` (`TableRefAllowlist`, `TableRefLookup`)

- [ ] **Step 1: Write the failing tests** — port Plan A Task 7 Step 1's two Go tables (`TestTableRef_TableFunctionsAndEnginesAreAllowlisted` and the lookup rows) into `TEST(TableRefAllowlist, FunctionsEnginesAndSettings)` and `TEST(TableRefLookup, ResolvesOrRefuses)`, using `RunTableRef` and asserting `code`, `message` (exact), and for successes the normalized SQL. The lookup rows use the string form Plan A measured (copy it from Plan A's `lookups.go` comment).

- [ ] **Step 2: Run to verify they fail** — Filter: `TableRefAllowlist.*:TableRefLookup.*`. Expected: FAIL.

- [ ] **Step 3: Implement the lists** (`table_reference.cc`), the same sets as Plan A's `allowlists.go`, lower-cased:

```cpp
enum class TableFunctionClass { Unknown, Refused, DataOnly, External };
TableFunctionClass classifyTableFunction(const std::string &name);   // "mergetree" prefix → Refused
bool tableEngineAllowed(const std::string &name, size_t arg_count);   // Replicated* only with 0 args
bool refusedTableSetting(const std::string &name);                    // disk, storage_policy
bool isStringLookup(const std::string &name);                          // joinget, joingetornull, dictget* prefix, dicthas, dictgethierarchy, dictisin, dictgetchildren, dictgetdescendants, hascolumnintable
constexpr std::string_view kUnsupportedStatementMessage = "statement is not supported";
std::string tableFunctionRefusedMessage(const std::string &name);  // "table function <name> is not accepted"
std::string tableFunctionUnknownMessage(const std::string &name);  // "table function <name> is not recognised"
std::string tableEngineRefusedMessage(const std::string &name);    // "table engine <name> is not accepted"
std::string tableSettingRefusedMessage(const std::string &name);   // "table setting <name> is not accepted"
```

`collectSourceFunctionNames(ast)`: every `ASTTableExpression::table_function`'s name, `ASTInsertQuery::table_function`, `ASTCreateQuery::as_table_function` (the function node's `name`), recursively including subqueries and view bodies. `createTableStorage(ast)`: `create->storage->engine->name` and its argument count; settings from `create->storage->settings->changes` (`ASTSetQuery`); for `ASTAlterQuery`, each `ASTAlterCommand` with `type == MODIFY_SETTING` contributes its `settings_changes` names. Preflight step after the protected one, in this order: source functions (Refused → `UnsupportedStatement` + refused message; Unknown → unknown message), then engine, then settings.

String lookups (`select.cc`, both entry points, right after `populateAccessedTables`): walk for `ASTFunction` with `isStringLookup(name)`; first argument must be an `ASTLiteral` String (else reject `InvalidRewriteRequest` `<fn> target "<text>" does not resolve through the caller's databases`); split on the first `.` after `unquoteQualified`, or use the logical context when unqualified (`hasColumnInTable` takes db and table as two literals); `applyDynamicRewrite(db, table, args, fn->name)`; on `OK` replace the literal's value with the measured string form and add the pair to `originals` before `populateAccessedTables` runs (move the walk ahead of it); an SI table, a protected or unmapped database, or a remote logical → the reject message. If Plan A measured "no parseable form", every lookup naming a table is refused with that message and the corpus pins it.

- [ ] **Step 4: Run, ctest, commit**

Filter: `TableRefAllowlist.*:TableRefLookup.*:TableRef*`. Expected: PASS.

```bash
git add src/handlers/table_reference.h src/handlers/table_reference.cc src/handlers/select.cc tests/rewriter_test.cc
git commit -m "feat: allowlist table functions, engines and settings; resolve string lookups (spec T5, T6)"
```

---

### Task 6: unmodelled classes (T7) and the MV TO parity fix (T12)

**Files:**
- Modify: `src/handlers/select.cc:887-895` (`handleSelectQuery` gate), `src/rewriter-server.cc:470-473` (order unchanged; the gate lives in the handler so Mode None keeps its pass-through)
- Modify: `src/handlers/writes.cc:362-396` (`preflightStorageIntegrityWrite` CREATE branch reads `create->targets`)
- Test: `tests/rewriter_test.cc` (`TableRefClasses`, `TableRefParity`; flip `StorageIntegrityCatchAll.UnmodelledStatementPassesThroughWithoutStorageIntegrity` → `…IsRefusedWithoutStorageIntegrity`; `StorageIntegrityContractV2.V1EmptyMapStaysLegacy` → SYSTEM refused, SET passes)

- [ ] **Step 1: Write the failing tests**

```cpp
TEST(TableRefClasses, UnmodelledClassesAreRefusedWithoutSI) {
  for (const char *sql : {"SYSTEM RELOAD CONFIG", "EXPLAIN SELECT * FROM db1.o", "CHECK TABLE db1.o",
                          "CREATE USER u1", "CREATE FUNCTION f AS x -> x + 1", "KILL QUERY WHERE 1"}) {
    SCOPED_TRACE(sql);
    const auto resp = RunTableRef(sql);
    EXPECT_EQ(resp.code(), rewriter::RewriteCode::UnsupportedStatement) << resp.message();
    EXPECT_EQ(resp.statement_type(), rewriter::STATEMENT_TYPE_UNSPECIFIED);
    EXPECT_EQ(resp.sql_after_rewrite(), sql);
  }
  const auto set = RunTableRef("SET max_threads = 1");
  EXPECT_EQ(set.code(), rewriter::RewriteCode::Success);
  EXPECT_EQ(set.sql_after_rewrite(), "SET max_threads = 1");
  EXPECT_EQ(RunTableRef("SET max_threads = 1", true).code(), rewriter::RewriteCode::UnsupportedStatement);
  EXPECT_EQ(RunTableRef("EXPLAIN SELECT * FROM db1.t", true).code(), rewriter::RewriteCode::UnsupportedStatement);
  EXPECT_EQ(RunTableRef("SELECT 1").code(), rewriter::RewriteCode::Success);
  // Mode None keeps the legacy pass-through for callers without rewrite options.
  EXPECT_EQ(RunWriteOp("SYSTEM RELOAD CONFIG").code, rewriter::RewriteCode::Success);
}

TEST(TableRefParity, MaterializedViewToActiveTableIsRefused) {
  for (const char *sql : {"CREATE MATERIALIZED VIEW db1.mv TO db1.t AS SELECT * FROM db1.o",
                          "CREATE MATERIALIZED VIEW db1.mv REFRESH EVERY 1 HOUR APPEND TO db1.t AS SELECT * FROM db1.o"}) {
    SCOPED_TRACE(sql);
    const auto resp = RunTableRef(sql, true);
    EXPECT_EQ(resp.code(), rewriter::RewriteCode::UnsupportedStatement) << resp.message();
    EXPECT_EQ(resp.message(), storageIntegrityWriteRejectMessage("db1.t"));
    EXPECT_EQ(resp.sql_after_rewrite(), sql);
  }
  const auto ok = RunTableRef("CREATE MATERIALIZED VIEW db1.mv TO db1.o AS SELECT * FROM db1.p", true);
  EXPECT_EQ(ok.code(), rewriter::RewriteCode::Success) << ok.message();
}
```

- [ ] **Step 2: Run to verify they fail** — Filter: `TableRefClasses.*:TableRefParity.*`. Expected: FAIL (SYSTEM → Success/SELECT; MV TO → Success).

- [ ] **Step 3: Implement**

`handleSelectQuery`, first lines:

```cpp
  const auto selection = findActiveTableRewrite(request->options());
  const bool is_select = ast->as<DB::ASTSelectWithUnionQuery>() || ast->as<DB::ASTSelectQuery>()
    || ast->as<DB::ASTSelectIntersectExceptQuery>();
  if (!is_select && selection.mode == TableRewriteMode::Dynamic) {
    // Spec 2026-09-26 T7: every unmodelled class is refused; a session SET
    // passes while the SI surface is inactive (the V2 catch-all already
    // refused it before this point when the surface is active).
    if (ast->as<DB::ASTSetQuery>()) {
      setSuccessResponse(response, formatAst(ast), rewriter::STATEMENT_TYPE_UNSPECIFIED);
      return;
    }
    response->set_code(rewriter::RewriteCode::UnsupportedStatement);
    response->set_message(std::string(kUnsupportedStatementMessage));
    return;
  }
```

(`selection` is computed later in the function today; hoist it.) `EXPLAIN` parses as `ASTExplainQuery`, so it falls into this branch; the previous behaviour of rewriting its inner query goes away for dynamic requests.

`preflightStorageIntegrityWrite`, in the CREATE branch after the `as_table` check:

```cpp
    if (create->targets) {
      for (const auto target : {DB::ViewTarget::To}) {
        if (!create->hasTargetTableID(target)) continue;
        const auto id = create->getTargetTableID(target);
        if (rejectStorageIntegrityWriteTarget(response, sel, id.database_name, id.table_name, false)) return true;
      }
    }
```

`APPEND TO` of a refreshable view is stored under the same `ViewTarget::To` on 26.3; confirm with the second test row (if the fork stores it elsewhere, iterate `create->targets->targets` and check every `table_id`).

- [ ] **Step 4: Flip the old tests, run, ctest, commit**

Filter: `TableRefClasses.*:TableRefParity.*:StorageIntegrityCatchAll.*:StorageIntegrityContractV2.*`. Expected: PASS.

```bash
git add src/handlers/select.cc src/handlers/writes.cc tests/rewriter_test.cc
git commit -m "feat: refuse unmodelled statement classes without SI; refuse MV TO an Active table (spec T7, T12)"
```

---

### Task 7: the shared corpus, byte-for-byte

**Files:**
- Replace: `tests/testdata/storage_integrity_cases.json` with Plan A's merged file
- Modify: `tests/si_corpus.h:48-50` (`kCorpusFingerprint`, `kCorpusBytes`, `kCorpusCases`)

- [ ] **Step 1: Copy and pin**

```bash
cp /Users/uranuswch/Dev/housegate/rewriter-go/internal/harness/testdata/storage_integrity_cases.json tests/testdata/storage_integrity_cases.json
cmp tests/testdata/storage_integrity_cases.json /Users/uranuswch/Dev/housegate/rewriter-go/internal/harness/testdata/storage_integrity_cases.json && shasum -a 256 tests/testdata/storage_integrity_cases.json
```

Update the three constants in `si_corpus.h` from the values Plan A Task 8 recorded (they are printed by rewriter-go's `TestSICorpusIsBytePinned`; the C++ `StorageIntegrityCorpus.IsBytePinnedToRewriterGo` prints the same three on mismatch).

- [ ] **Step 2: Run the corpus suite**

Filter: `StorageIntegrityCorpus.*:SpecG/StorageIntegrityGolden.*`. Expected: every `si_tr_*` case PASS except success cases whose SQL differs from Go by quoting only — list them.

- [ ] **Step 3: Reconcile divergences with the oracle run**

For each listed case, confirm the difference is quoting (`NormalizeSIIdentifierQuotes` should already fold backticks vs. double quotes; a remaining diff is a real divergence such as the presence of `AS` before an alias). Quoting-only diffs: the normalizer covers them, so the case passes; a structural diff is a bug in one engine — fix the engine that deviates from the spec's rule (`targets first, then sources in statement order`; `IN` operands take no alias), never the corpus. Then run Plan A Task 9 Step 3 from the rewriter-go branch with `REWRITER_ORACLE_ADDR` pointed at this build (`clickhousegate_rewriter 50051` on the box, tunnelled) and require zero divergences; `UPDATE_GOLDEN=1` may be used there to regenerate `want_sql_go` / `want_sql_cpp` pairs, after which the regenerated file is copied back here and the constants updated again.

- [ ] **Step 4: Commit**

```bash
git add tests/testdata/storage_integrity_cases.json tests/si_corpus.h
git commit -m "test(corpus): table-reference hardening cases, byte-identical to rewriter-go (sha256 <value>)"
```

---

### Task 8: documentation, CI, PR, release

**Files:**
- Modify: `CLAUDE.md` / `AGENTS.md` (the `table_reference.cc` module, the preflight order, the message list), `docs/` if a handler overview exists

- [ ] **Step 1: Document** (no wrapping).

- [ ] **Step 2: Push and watch CI**

```bash
git push -u origin feat/table-reference-hardening
gh pr create --title "feat: table-reference hardening (spec 2026-09-26)" --body "$(cat <<'EOF'
C++ half of the table-reference hardening, paired with housegate/rewriter-go PR <link>. Corpus sha256: <value> (cmp-identical). Differential run: <date>, zero divergences.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

CI (`ci.yml`) runs the full CTest on the build box; `required-check` must be green.

- [ ] **Step 3: Release** — after the user approves the merge: merge, then `gh workflow run cut-release.yml` and confirm the tag is `v0.16.0` (the workflow dispatches `release.yml`, which builds and pushes the docker image). Ask the user before merging and before cutting the release.
