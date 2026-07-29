---
name: add-housegate-plugin
description: Use when adding a new query/connection plugin to housegate (the ClickHouse proxy in this repo) — covers the package layout, hook interfaces, Config integration, naming-collision avoidance, and the standard wiring into cmd.
---

# Adding a Plugin to Housegate

Housegate runs every client connection through a `pkg/plugin.PluginChain`. A plugin is a small, self-contained package under `pkg/plugins/<name>/` that implements one or more lifecycle hooks. Adding a plugin should be a 4-file, ~one-screen change. This skill is the recipe.

## When to Use

- Adding any plugin behavior: auth gate, rate limiter, audit logger, query rewriter, signing, observability hook, etc.
- The behavior fits the per-query (or per-connection) lifecycle defined by `pkg/plugin.Hooks`.

If your behavior is genuinely cross-cutting and doesn't fit a hook, extend `pkg/plugin.Hooks` first (a separate, bigger task — the hook interface is part of the contract).

## Architecture Recap

```
pkg/plugin/                Hook interfaces + PluginChain (the contract)
pkg/plugins/<your-plugin>/ YOUR plugin lives here
pkg/<leaf>/                Reusable leaves: auth, billing, credentials, network, rewriter, route
                           Plugins import these — NOT pkg/proxy
pkg/cfgtypes/              cfgtypes.Duration (use this in your Config for time fields)
pkg/config/                Root Config — gets a new field that holds your plugin's Config
build.go (root)            Wires plugin into the chain (buildServer / buildAgent)
```

**Plugins MUST NOT import `pkg/proxy`.** That's the architectural invariant — plugins depend on small leaf packages, not on the god package. If you find yourself wanting to import `pkg/proxy`, the type you need probably belongs in a leaf package; extract it.

## The 5-Step Recipe

### Step 1 — Pick your hook(s)

Read `pkg/plugin/plugin.go`. Your plugin implements one or more of:

| Interface | Fires | Typical use |
|---|---|---|
| `HelloPlugin.OnHello` | Once per connection, after ClientHello decode | Credential injection, route prefix stripping |
| `QueryPlugin.OnQuery` | Per query, BEFORE forwarding upstream | Auth check, rewrite, signing, billing gate, concurrency acquire |
| `QueryCompletePlugin.OnQueryComplete` | Per query, exactly once when its lifecycle ends (chain reject / EOS / Exception / forward fail) | Permit release, span close, in-flight counter dec |
| `ExceptionPlugin.OnException` | When upstream Exception is decoded (only when SessionState.HasActiveRewrite) | Error reverse-mapping |
| `ClosePlugin.OnClose` | Once per session at teardown | Safety-net cleanup for resources held across queries |

A plugin commonly implements 2–3 hooks (e.g. concurrency limiter implements `QueryPlugin` + `QueryCompletePlugin` + `ClosePlugin`).

### Step 2 — Create `pkg/plugins/<name>/`

Files:
```
pkg/plugins/<name>/
  <name>.go        # Plugin struct + hook methods + interface assertions
  config.go        # Config struct (operator-tunable surface)
  <name>_test.go   # Unit tests (with a fake of any backend dep)
  BUILD.bazel
```

**Package name caveat — if your plugin name collides with a leaf package, rename the package (NOT the directory).**

Example: `pkg/plugins/auth/` package is `authplugin` (because `pkg/auth` is also `package auth`). Same trick for `pkg/plugins/route/` (`package routeplugin`). Other plugin packages (concurrency, agent, usage, credential, rewrite, state) keep their natural name because no collision exists.

#### Plugin file template

```go
// Package <name> implements <one-line description>.
package <name>  // OR <name>plugin if there's a leaf-package collision

import (
    "context"

    "github.com/housegate/housegate/pkg/chsession"
    "github.com/housegate/housegate/pkg/plugin"
    // ... import your leaf-package backend
)

type Plugin struct {
    Backend Backend // accept an interface so tests can substitute a fake
    // ... other config-derived fields
}

func (p *Plugin) OnQuery(ctx context.Context, qctx *plugin.QueryContext) error {
    // ...
    return nil
}

// Compile-time interface assertions — keeps the contract tight.
var _ plugin.QueryPlugin = (*Plugin)(nil)
```

Look at `pkg/plugins/concurrency/limiter.go` for a 3-hook example, `pkg/plugins/sessionstate/tracker.go` for a 1-hook minimal example.

#### Config file template

```go
package <name>

import "github.com/housegate/housegate/pkg/cfgtypes"

// Config is the operator-tunable surface for the <name> plugin.
type Config struct {
    Enabled  bool              `json:"enabled"   yaml:"enabled"`
    // ... fields. Use cfgtypes.Duration for time values.
    Timeout  cfgtypes.Duration `json:"timeout"   yaml:"timeout"`
    // For Redis-using plugins:
    RedisAddr string           `json:"redis_addr" yaml:"redis_addr"` // empty → root.RedisDefaultAddr
}

// (Optional) Validate is called by config.Config.Validate when the
// plugin is enabled. Use it for cross-field invariants only — single
// required-field checks live in the root validator.
func (c Config) Validate() error { ... }
```

#### BUILD.bazel template

```python
load("@rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "<name>",  # bazel rule name — keep matching dir basename
    srcs = [
        "<name>.go",
        "config.go",
    ],
    importpath = "github.com/housegate/housegate/pkg/plugins/<name>",
    visibility = ["//visibility:public"],
    deps = [
        "//pkg/cfgtypes",      # if Config has Duration
        "//pkg/chsession",     # almost always
        "//pkg/log",           # housegate's slog-based logger
        "//pkg/plugin",        # always
        # ... your leaf packages
    ],
)

go_test(
    name = "<name>_test",
    srcs = ["<name>_test.go"],
    embed = [":<name>"],
    deps = [
        "//pkg/chproto",
        "//pkg/chsession",
        "//pkg/plugin",
        # mocks, miniredis, etc.
    ],
)
```

### Step 3 — Add Config field to root config

Edit `pkg/config/config.go`:

```go
import (
    // ...
    yourplugin "github.com/housegate/housegate/pkg/plugins/<name>"
)

type Config struct {
    // ... existing
    YourPlugin yourplugin.Config `json:"your_plugin" yaml:"your_plugin"`
}

// In Default():
YourPlugin: yourplugin.Config{
    // sensible defaults
},

// In Validate(): add cross-section checks (e.g. Redis required when enabled)
if c.YourPlugin.Enabled && c.ResolveRedisAddr(c.YourPlugin.RedisAddr) == "" {
    errs = append(errs, errors.New("your_plugin.redis_addr (or top-level redis_default_addr) is required when your_plugin.enabled"))
}
```

Add the dep to `pkg/config/BUILD.bazel` (`deps` of both `config` library and `config_test` test).

### Step 4 — Wire into the lib

Edit `build.go` at the module root (`housegate` package). Build your plugin in `buildServer` (and/or `buildAgent` if it should fire there too) — that's where the existing wiring lives:

```go
import (
    yourplugin "github.com/housegate/housegate/pkg/plugins/<name>"
)

// inside buildServer, after auth/usage:
if cfg.YourPlugin.Enabled {
    yp := &yourplugin.Plugin{
        // construct from cfg.YourPlugin and shared backends
    }
    queryPlugins = append(queryPlugins, yp)
    // also queryCompletePlugins / closePlugins if your plugin implements those
}
```

For Redis-backed plugins, fetch a client via `rf.get(cfg.YourPlugin.RedisAddr)` (the `*redisFactory` is passed into each builder by `New`). Do not create a new `redis.NewClient` directly — the factory dedupes by resolved address and respects caller-supplied `Options.RedisClients`.

Update the root `BUILD.bazel` `deps` to include `//pkg/plugins/<name>`. The cmd-side `cmd/BUILD.bazel` does NOT need changes — plugin deps reach the binary transitively via `//:housegate`.

### Step 5 — Build + test

```bash
bazel build //pkg/plugins/<name>:<name>      # plugin compiles standalone
bazel test  //pkg/plugins/<name>:<name>_test # unit tests pass
bazel build //...                             # everything else still compiles
bazel test  //pkg/config:config_test          # config wiring valid
```

Baseline: `pkg/rewriter:rewriter_test` has 14 expected failures (need external `localhost:50051` rewriter gRPC). Anything else failing is your regression.

## Quick Reference

| Need | Use this leaf package |
|---|---|
| Validate JWS / get RelaySigner / inject auth token | `pkg/auth` |
| Look up real ClickHouse credentials | `pkg/credentials` |
| Rewrite SQL via gRPC rewriter | `pkg/rewriter` |
| Read sentio decentralized network state | `pkg/network` |
| Charge/check user balance via sentio-node | `pkg/billing` |
| Parse `__route__|target|user` envelope | `pkg/route` |
| Parse `__peer__|address` envelope | `pkg/peer` |
| Duration in your Config | `pkg/cfgtypes` |
| Sign / validate peer-relay JWS at handshake | `pkg/auth` (`PeerSigner` / `PeerValidator`) |

### Marker interfaces (which sessions does my plugin fire on?)

| Interface | Default for non-implementers | When to implement |
|---|---|---|
| `plugin.RouteAware` (`RunOnRouted() bool`) | **skip** on routed sessions | Implement returning `true` if your plugin must fire on proxy-to-proxy routed traffic (e.g. metrics, the route signer itself). Most plugins do NOT implement this. |
| `plugin.PeerTrustAware` (`RunOnPeerTrust() bool`) | **run** on peer-trusted sessions | Implement returning `false` only if your plugin's semantics break on a session whose SQL was already rewritten by an upstream housegate (e.g. SQL-binding auth, second-pass rewriter). Most plugins do NOT implement this. |

The two filters compose. `OnHello` and lifecycle hooks (`OnConnect` / `OnDisconnect` / `OnClose`) are exempt from the peer-trust filter.

| Plugin chain ordering (server mode, current) |
|---|
| Hello: `routeplugin.Stripper` → `credential.Plugin` |
| Query: `authplugin.Plugin` → `usage.Plugin` → `concurrency.Plugin` → `state.Plugin` → `rewrite.Plugin` → `routeplugin.Signer` |
| Exception: `rewrite.ErrorReverseMap` |

Place your plugin where its semantics dictate:
- **Auth/balance gates** → early (cheap rejection before expensive work)
- **State capture** (USE/SET tracking) → before SQL transformations
- **SQL mutators** (rewrite, signing) → late
- **Resource holders** (concurrency permits) → after cheap gates, before expensive transformations

## Common Mistakes

| Mistake | Fix |
|---|---|
| Importing `pkg/proxy` from your plugin | Find a leaf package that owns the type you need. If none exists, extract one — don't pull in pkg/proxy. |
| Plugin package collides with leaf package name | Rename plugin package (e.g. `package authplugin`), keep dir name. Cmd-side imports use the path basename; alias only when the importer needs both. |
| Forgetting to register the same plugin into multiple plugin lists when it implements multiple hooks | A `&Plugin{...}` instance must appear in EACH of `QueryPlugins`, `QueryCompletePlugins`, `ClosePlugins` it implements — they're separate slices, no auto-discovery. |
| Creating a fresh `redis.NewClient` in your plugin or in build.go | Use `rf.get(cfg.YourPlugin.RedisAddr)` from the *redisFactory passed into the builder. Falls back to RedisDefaultAddr; dedupes connections; respects Options.RedisClients injection. |
| Plugin Config has `time.Duration` field | Use `cfgtypes.Duration` so `"5s"` strings parse from JSON/YAML. |
| Not adding `var _ plugin.XxxPlugin = (*Plugin)(nil)` | Compile-time assertion catches "I forgot to keep implementing the interface" bugs early. Always add. |
| Adding a `Validate()` method that double-checks fields the root validator already covers | Root `Config.Validate()` covers cross-section invariants. Plugin-level `Validate()` is for plugin-internal consistency only. |

## Concrete Examples in the Repo

- **Single-hook, zero leaf deps**: `pkg/plugins/sessionstate/` — regex-based USE/SET tracker, simplest possible plugin.
- **Triple-hook with Redis**: `pkg/plugins/concurrency/` — Plugin + Config + Resolver pattern + miniredis e2e tests.
- **Wraps a backend interface**: `pkg/plugins/auth/` — accepts `auth.Validator` interface (not a concrete type) so tests can fake.
- **Two plugins sharing a sentinel**: `pkg/plugins/route/` (Stripper + Signer + private `routeTargetSentinel` helper).

## Red Flags — Stop and Reconsider

- "I'll just put this Config field in pkg/config directly" → You're skipping Step 2. Plugin owns its Config.
- "I need a hook that doesn't exist yet" → Stop, propose extending `pkg/plugin.Hooks`. Don't shoehorn into a wrong hook.
- "It's easier to import pkg/proxy here" → Don't. Trace what you actually need; it lives in (or belongs in) a leaf package.
- "I'll skip the unit test, the e2e test will cover it" → e2e tests are slow and external-dependency-laden. Unit test the resolution logic with a fake backend.
- "The plugin needs Redis but I'll just hard-code the addr" → Use `RedisAddr` field + factory pattern. Future operators will thank you.
