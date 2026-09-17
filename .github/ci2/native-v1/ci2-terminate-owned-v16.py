#!/usr/bin/env python3
"""Boundedly quiesce one exact recorded operation process group."""

from __future__ import annotations

import argparse
import errno
import hashlib
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path


OPERATIONS = {
    "setup": "setup",
    "homebrew": "homebrew",
    "ci-build": "ci-build",
    "ci-test-ffi": "ci-test-ffi",
    "release-linux": "release-linux",
    "release-darwin": "release-darwin",
    "finalize-pass": "finalize",
    "finalize-fail": "abort-finalize",
}


class RecoveryError(RuntimeError):
    pass


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def proc_start_token(pid: int) -> str | None:
    stat = Path(f"/proc/{pid}/stat")
    if stat.is_file():
        try:
            text = stat.read_text(encoding="ascii")
            rest = text[text.rindex(")") + 1 :].split()
            return f"proc:{rest[19]}"
        except (OSError, IndexError, ValueError):
            return None
    result = subprocess.run(
        ["ps", "-p", str(pid), "-o", "lstart="],
        check=False,
        capture_output=True,
        text=True,
        timeout=1,
    )
    value = " ".join(result.stdout.split())
    return f"ps:{value}" if result.returncode == 0 and value else None


def process_command(pid: int) -> str | None:
    cmdline = Path(f"/proc/{pid}/cmdline")
    if cmdline.is_file():
        try:
            return cmdline.read_bytes().replace(b"\0", b" ").decode("utf-8", errors="replace")
        except OSError:
            return None
    result = subprocess.run(
        ["ps", "-p", str(pid), "-o", "command="],
        check=False,
        capture_output=True,
        text=True,
        timeout=1,
    )
    value = result.stdout.strip()
    return value if result.returncode == 0 and value else None


def group_members(pgid: int) -> dict[int, str]:
    result = subprocess.run(
        ["ps", "-eo", "pid=,pgid=,stat="],
        check=True,
        capture_output=True,
        text=True,
        timeout=1,
    )
    members: dict[int, str] = {}
    for line in result.stdout.splitlines():
        fields = line.split()
        if len(fields) < 3:
            continue
        try:
            pid, member_group = int(fields[0]), int(fields[1])
        except ValueError:
            continue
        if member_group != pgid or fields[2].startswith("Z"):
            continue
        token = proc_start_token(pid)
        if token is not None:
            members[pid] = token
    return members


def parse_identity(data: bytes) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw in data.decode("utf-8", errors="strict").splitlines():
        if "=" not in raw:
            raise RecoveryError("malformed identity line")
        key, value = raw.split("=", 1)
        if key in values or not key or not value:
            raise RecoveryError("duplicate or empty identity field")
        values[key] = value
    expected = {"run_id", "owner_token", "phase", "pid", "pgid", "starttime", "runner_sha256"}
    if set(values) != expected:
        raise RecoveryError("identity fields are not exact")
    return values


def checkpoint(root: Path | None, name: str, timeout: float) -> None:
    if root is None:
        return
    root.mkdir(parents=True, exist_ok=True)
    (root / f"{name}.reached").write_text("reached\n", encoding="utf-8")
    deadline = time.monotonic() + timeout
    continuation = root / f"{name}.continue"
    while not continuation.exists():
        if time.monotonic() >= deadline:
            raise RecoveryError(f"test checkpoint timed out: {name}")
        time.sleep(0.01)


class Terminator:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.identity_data = args.identity.read_bytes()
        self.identity_sha = sha256_bytes(self.identity_data)
        self.identity = parse_identity(self.identity_data)
        self.pid = int(self.identity["pid"])
        self.pgid = int(self.identity["pgid"])
        self.operation = self.identity["phase"]
        self.snapshot: dict[int, str] | None = None
        self.deadline = time.monotonic() + args.total_seconds

    def log(self, state: str, detail: str = "") -> None:
        print(f"state={state} operation={self.operation} pgid={self.pgid} {detail}".rstrip(), flush=True)

    def time_left(self) -> float:
        return self.deadline - time.monotonic()

    def identity_state(self) -> str:
        try:
            current = self.args.identity.read_bytes()
        except FileNotFoundError:
            return "missing"
        return "same" if current == self.identity_data else "changed"

    def validate_static(self) -> None:
        if self.identity["run_id"] != self.args.expected_run:
            raise RecoveryError("run identity mismatch")
        if self.identity["owner_token"] != self.args.expected_owner:
            raise RecoveryError("owner identity mismatch")
        if self.operation not in OPERATIONS:
            raise RecoveryError("operation is not allowed")
        if self.pid <= 1 or self.pgid != self.pid:
            raise RecoveryError("recorded leader is not its own process group")
        if self.identity["runner_sha256"] != self.args.expected_runner_sha:
            raise RecoveryError("recorded runner hash mismatch")
        if sha256_file(self.args.runner) != self.args.expected_runner_sha:
            raise RecoveryError("actual runner hash mismatch")

    def validate_leader(self) -> str:
        token = proc_start_token(self.pid)
        if token is None:
            return "absent"
        if token != self.identity["starttime"]:
            raise RecoveryError("leader PID start identity changed")
        result = subprocess.run(
            ["ps", "-p", str(self.pid), "-o", "pgid="],
            check=False,
            capture_output=True,
            text=True,
            timeout=1,
        )
        try:
            actual_pgid = int(result.stdout.strip())
        except ValueError as exc:
            raise RecoveryError("leader PGID unavailable") from exc
        if actual_pgid != self.pgid:
            raise RecoveryError("leader PGID changed")
        command = process_command(self.pid)
        if not command:
            return "absent"
        expected = self.args.expected_command_fragment
        if expected is None:
            expected = f"{self.args.runner} {OPERATIONS[self.operation]}"
        if expected not in command:
            raise RecoveryError("leader command does not match recorded operation")
        return "valid"

    def observe(self, require_prior: bool) -> dict[int, str]:
        state = self.identity_state()
        members = group_members(self.pgid)
        if state == "changed":
            raise RecoveryError("operation identity changed during recovery")
        if not members:
            return {}
        if state == "missing":
            raise RecoveryError("identity disappeared while recorded group remains live")
        leader = self.validate_leader()
        if leader == "absent" and not require_prior:
            raise RecoveryError("leader disappeared before an exact group snapshot was established")
        if self.snapshot is not None:
            for pid, token in members.items():
                if self.snapshot.get(pid) != token:
                    raise RecoveryError("group acquired a new or reused process identity")
        return members

    def remove_exact_identity_and_lock(self) -> None:
        state = self.identity_state()
        if state == "changed":
            raise RecoveryError("refusing to remove changed operation identity")
        if state == "same":
            self.args.identity.unlink()
        shadow = self.args.shadow_identity
        if shadow is not None and shadow != self.args.identity:
            try:
                shadow_data = shadow.read_bytes()
            except FileNotFoundError:
                pass
            else:
                if shadow_data != self.identity_data:
                    raise RecoveryError("refusing to remove changed shadow identity")
                shadow.unlink()
        if not self.operation.startswith("finalize-"):
            return
        lock = self.args.finalizer_lock
        if lock is None or not lock.exists():
            return
        lock_identity = lock / "identity"
        try:
            lock_data = lock_identity.read_bytes()
        except OSError as exc:
            raise RecoveryError("finalizer lock identity unavailable") from exc
        if lock_data != self.identity_data:
            raise RecoveryError("finalizer lock does not match terminated identity")
        shutil.rmtree(lock)

    def quiescent(self, cause: str) -> int:
        if group_members(self.pgid):
            raise RecoveryError("group repopulated at quiescent transition")
        self.remove_exact_identity_and_lock()
        self.log("QUIESCENT", f"cause={cause} identity_sha256={self.identity_sha}")
        return 0

    def wait_for_empty(self, seconds: float, prior: dict[int, str]) -> bool:
        end = min(self.deadline, time.monotonic() + seconds)
        self.snapshot = prior
        while time.monotonic() < end:
            members = self.observe(require_prior=True)
            if not members:
                return True
            time.sleep(0.05)
        return not self.observe(require_prior=True)

    def send_group(self, sig: signal.Signals) -> bool:
        try:
            os.killpg(self.pgid, sig)
            self.log(f"SENT_{sig.name}")
            return True
        except ProcessLookupError:
            self.log(f"ESRCH_{sig.name}")
            return False
        except OSError as exc:
            if exc.errno == errno.ESRCH:
                self.log(f"ESRCH_{sig.name}")
                return False
            raise

    def run(self) -> int:
        self.validate_static()
        self.log("VALIDATED")
        initial = self.observe(require_prior=False)
        if not initial:
            return self.quiescent("gone-before-snapshot")
        checkpoint(self.args.test_state_dir, "before-snapshot", self.args.test_checkpoint_seconds)
        current = self.observe(require_prior=False)
        if not current:
            return self.quiescent("gone-before-snapshot")
        self.snapshot = current
        self.log("SNAPSHOT", f"members={len(current)}")

        # A phase may spawn or reap children while recovery begins. Refresh only
        # while the exact leader remains valid, and stop after four transitions.
        for transition in range(4):
            current = self.observe(require_prior=True)
            if not current:
                return self.quiescent("gone-before-term")
            self.snapshot = current
            time.sleep(0.02)
            next_members = group_members(self.pgid)
            if not next_members:
                return self.quiescent("gone-before-term")
            if next_members == current:
                break
            if self.validate_leader() != "valid":
                raise RecoveryError("leader disappeared during group identity transition")
            self.snapshot = None
            current = self.observe(require_prior=False)
            self.snapshot = current
            self.log("SNAPSHOT_REFRESH", f"transition={transition + 1} members={len(current)}")
        else:
            raise RecoveryError("group identities did not stabilize before TERM")

        checkpoint(self.args.test_state_dir, "before-term", self.args.test_checkpoint_seconds)
        current = self.observe(require_prior=True)
        if not current:
            return self.quiescent("gone-before-term")
        self.snapshot = current
        self.send_group(signal.SIGTERM)
        if self.wait_for_empty(self.args.term_seconds, current):
            return self.quiescent("term-or-natural-exit")

        checkpoint(self.args.test_state_dir, "before-kill", self.args.test_checkpoint_seconds)
        current = self.observe(require_prior=True)
        if not current:
            return self.quiescent("gone-before-kill")
        self.snapshot = current
        self.send_group(signal.SIGKILL)
        if self.wait_for_empty(self.args.kill_seconds, current):
            return self.quiescent("kill-or-esrch")
        raise RecoveryError("owned operation group survived bounded TERM/KILL")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    start = sub.add_parser("start-token")
    start.add_argument("pid", type=int)
    terminate = sub.add_parser("terminate")
    terminate.add_argument("--identity", type=Path, required=True)
    terminate.add_argument("--shadow-identity", type=Path)
    terminate.add_argument("--runner", type=Path, required=True)
    terminate.add_argument("--expected-run", required=True)
    terminate.add_argument("--expected-owner", required=True)
    terminate.add_argument("--expected-runner-sha", required=True)
    terminate.add_argument("--finalizer-lock", type=Path)
    terminate.add_argument("--expected-command-fragment")
    terminate.add_argument("--term-seconds", type=float, default=20.0)
    terminate.add_argument("--kill-seconds", type=float, default=5.0)
    terminate.add_argument("--total-seconds", type=float, default=30.0)
    terminate.add_argument("--test-state-dir", type=Path)
    terminate.add_argument("--test-checkpoint-seconds", type=float, default=3.0)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.command == "start-token":
        token = proc_start_token(args.pid)
        if token is None:
            raise SystemExit("process start identity unavailable")
        print(token)
        return 0
    if min(args.term_seconds, args.kill_seconds, args.total_seconds) <= 0:
        raise SystemExit("termination bounds must be positive")
    try:
        return Terminator(args).run()
    except (RecoveryError, OSError, subprocess.SubprocessError) as exc:
        print(f"recovery-error: {exc}", file=sys.stderr)
        return 73


if __name__ == "__main__":
    raise SystemExit(main())
