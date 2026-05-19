# Encrypted config files (age)

housegate supports transparent age encryption for config files, so
credentials-bearing files (notably `ckh_manager.*.yaml`, which holds
ClickHouse replica passwords) can live on untrusted hosts as ciphertext.
The running proxy decrypts them in memory at startup.

## Threat model

This protects against:

- Operators with file-system access (`docker exec`, SSH) reading plaintext
  passwords via `cat config.yaml`.
- Plaintext leaking through image layers, snapshots, backups, or log/audit
  captures of config file contents.

This does **not** protect against:

- Attackers with a debugger attached to the running proxy (secrets are live
  in memory while the process runs; no userspace scheme can hide that).
- Attackers who can read the age private key from whatever source the
  proxy loads it from. Key-source security is the operator's responsibility
  (see "Identity injection" below).

## How it works

1. Operators encrypt `ckh_manager.local.yaml` (or any config) to a set of
   age public keys using `housegate secret-encrypt` or `housegate secret-edit`.
2. The ciphertext is ASCII-armored (`-----BEGIN AGE ENCRYPTED FILE-----`),
   so it diffs cleanly in git and survives editor line-ending conversion.
3. On startup, the proxy sniffs each config path it loads. If it looks
   age-encrypted, it resolves the identity from environment, decrypts in
   memory, and writes the plaintext to a Linux `memfd` (anonymous
   in-memory file). The downstream loader reads from `/proc/self/fd/N`.
4. Once the loader returns, the `memfd` is closed and the plaintext is gone
   from the process address space. Plaintext never touches a visible
   filesystem path.

On non-Linux platforms (macOS local dev), a tempfile is used instead — this
is a development convenience, not the production substrate.

## CLI

All commands ship in the `housegate` binary itself; no extra tools needed
on operator machines.

### `housegate secret-keygen`

Generate a new X25519 keypair.

```
housegate secret-keygen -o admin.age-key
# Public key printed to stderr and embedded as a comment in the file.
```

The file is created with mode 0600. The command refuses to overwrite an
existing path (private keys are one-shot material — generate a new one if
you lose it).

### `housegate secret-encrypt`

Encrypt a plaintext file.

```
housegate secret-encrypt \
  -r age1proxybinary...pubkey \
  -r age1admin...pubkey \
  configs/ckh_manager.prod.yaml configs/ckh_manager.prod.yaml.age
```

Recipients are collected from (in order, all merged into one set):
1. `-r <pubkey>` flags
2. `HOUSEGATE_AGE_RECIPIENTS` env var (comma-separated)
3. Sibling `<target>.recipients` file (one pubkey per line, `#` comments)

The companion file is the recommended production layout — commit it next to
the ciphertext in git so "who can read this file" is explicit and reviewable.

### `housegate secret-decrypt`

Decrypt to stdout (for scripts and one-off inspection).

```
HOUSEGATE_AGE_IDENTITY_FILE=~/.config/housegate/admin.age-key \
  housegate secret-decrypt configs/ckh_manager.prod.yaml.age
```

### `housegate secret-edit`

Decrypt → `$EDITOR` → re-encrypt in place. The standard workflow for
ongoing admin changes.

```
export EDITOR=vim  # or "code -w" etc
HOUSEGATE_AGE_IDENTITY_FILE=~/.config/housegate/admin.age-key \
  housegate secret-edit configs/ckh_manager.prod.yaml.age
```

Unchanged edits are skipped (no re-encrypt, so `git diff` stays clean).
If the target is not yet encrypted, the first save encrypts it.

## Identity injection (production)

The proxy binary reads the age private key from one of:

| Env var                | Contents                                              |
|------------------------|-------------------------------------------------------|
| `HOUSEGATE_AGE_IDENTITY`      | Inline `AGE-SECRET-KEY-1...` string                  |
| `HOUSEGATE_AGE_IDENTITY_FILE` | Path to a file with one identity per line (mode 0600) |

Pick one, and **do NOT check the identity into git** (`.gitignore` already
excludes `*.age-key`, `*.age-identity`, `age-keys.txt`).

### Kubernetes

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: housegate-age-identity
type: Opaque
stringData:
  identity: |
    AGE-SECRET-KEY-1...
---
apiVersion: apps/v1
kind: Deployment
spec:
  template:
    spec:
      containers:
        - name: housegate
          env:
            - name: HOUSEGATE_AGE_IDENTITY
              valueFrom:
                secretKeyRef:
                  name: housegate-age-identity
                  key: identity
```

Pair with [`encryption-at-rest`](https://kubernetes.io/docs/tasks/administer-cluster/encrypt-data/)
so etcd doesn't store the identity in plaintext.

### Docker (single-host)

```bash
# One-time: store identity out of image, out of git
sudo install -m 0600 /dev/stdin /etc/housegate/age.key <<< "AGE-SECRET-KEY-1..."

docker run \
  -v /etc/housegate/age.key:/etc/housegate/age.key:ro \
  -e HOUSEGATE_AGE_IDENTITY_FILE=/etc/housegate/age.key \
  -v "$(pwd)/configs:/configs:ro" \
  housegate:latest -config /configs/local.yaml
```

### systemd

```ini
[Service]
LoadCredential=age-identity:/etc/housegate/age.key
Environment=HOUSEGATE_AGE_IDENTITY_FILE=%d/age-identity
ExecStart=/usr/local/bin/housegate -config /etc/housegate/local.yaml
```

`LoadCredential=` copies the file to a tmpfs owned by the service's PID —
it disappears when the service stops, and `docker exec`-equivalent access
to the host cannot read it from a different process's view.

## Key rotation

1. Generate a new binary key: `housegate secret-keygen -o binary-v2.key`
2. Add its public key to every `<file>.recipients` companion.
3. Re-encrypt in place, preserving all existing recipients:
   ```
   for f in configs/*.age; do
     HOUSEGATE_AGE_IDENTITY_FILE=admin.age-key \
       housegate secret-decrypt "$f" \
       | housegate secret-encrypt /dev/stdin "$f.new"
     mv "$f.new" "$f"
   done
   ```
4. Deploy binaries with the new `HOUSEGATE_AGE_IDENTITY` injected. Both old and
   new keys decrypt during the rollout window.
5. Once the rollout is stable, remove the old pubkey from the companion
   files, re-encrypt again, and destroy the old private key.

(For single-recipient workflows or large file sets, `age --decrypt | age
--encrypt -r ...` does the same in one pipeline — this is all upstream
age machinery.)

## Recommended repo layout

```
configs/
  local.yaml                        # references ckh_manager.local.yaml.age
  ckh_manager.prod.yaml.age         # ciphertext, committed
  ckh_manager.prod.yaml.age.recipients
                                    # companion: binary pubkey + admin pubkey, committed
  ckh_manager.local.yaml            # dev-only plaintext, gitignored
```

In the main config, point at whichever path is on disk at deploy time:

```yaml
ckh_manager_config_path: "./configs/ckh_manager.prod.yaml.age"
```

The proxy's `secretsload.Resolve` detects the `.age` suffix's contents
(not the name) so you can use any filename convention you like.
