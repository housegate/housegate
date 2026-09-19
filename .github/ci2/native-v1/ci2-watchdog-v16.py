#!/usr/bin/env python3
"""Run one owned command under absolute soft and hard monotonic deadlines."""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import tempfile
import time

TIMEOUT = 124
ORPHAN = 125
KILL_RESERVE_SECONDS = 0.15


def time_left(deadline: float) -> float:
    return max(0.0, deadline - time.monotonic())


def process_table(deadline: float) -> dict[int, tuple[int, str]]:
    remaining = time_left(deadline)
    if remaining <= 0:
        return {}
    with tempfile.TemporaryFile() as output:
        probe = subprocess.Popen(
            ["ps", "-axo", "pid=,ppid=,lstart="],
            text=True,
            stdout=output,
            stderr=subprocess.DEVNULL,
        )
        probe_end = min(deadline, time.monotonic() + 0.25)
        while probe.poll() is None and time.monotonic() < probe_end:
            time.sleep(min(0.01, time_left(probe_end)))
        if probe.poll() is None:
            probe.kill()
            return {}
        output.seek(0)
        text = output.read().decode(errors="replace")
    table: dict[int, tuple[int, str]] = {}
    for line in text.splitlines():
        fields = line.split(maxsplit=2)
        if len(fields) != 3:
            continue
        try:
            table[int(fields[0])] = (int(fields[1]), fields[2])
        except ValueError:
            continue
    return table


def refresh_owned(root_pid: int, owned: dict[int, str], deadline: float) -> None:
    table = process_table(deadline)
    root = table.get(root_pid)
    if root is not None:
        owned.setdefault(root_pid, root[1])
    changed = True
    while changed:
        changed = False
        for pid, (ppid, started) in table.items():
            if pid not in owned and ppid in owned:
                owned[pid] = started
                changed = True


def signal_owned(owned: dict[int, str], sig: signal.Signals, deadline: float) -> None:
    table = process_table(deadline)
    for pid, started in reversed(list(owned.items())):
        if table.get(pid, (None, None))[1] != started:
            continue
        try:
            os.kill(pid, sig)
        except ProcessLookupError:
            pass


def owned_alive(owned: dict[int, str], deadline: float) -> bool:
    table = process_table(deadline)
    return any(table.get(pid, (None, None))[1] == started for pid, started in owned.items())


def group_exists(pgid: int) -> bool:
    try:
        os.killpg(pgid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def signal_group(pgid: int, sig: signal.Signals) -> None:
    try:
        os.killpg(pgid, sig)
    except ProcessLookupError:
        pass


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--soft-seconds", type=float, required=True)
    parser.add_argument("--hard-seconds", type=float, required=True)
    parser.add_argument("--kill-descendant-tree", action="store_true")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.command and args.command[0] == "--":
        args.command = args.command[1:]
    if not args.command:
        parser.error("missing command")
    if not 0 < args.soft_seconds <= args.hard_seconds:
        parser.error("require 0 < soft-seconds <= hard-seconds")

    proc = subprocess.Popen(args.command, start_new_session=True)
    pgid = proc.pid
    started = time.monotonic()
    soft_at = started + args.soft_seconds
    hard_at = started + args.hard_seconds
    kill_at = max(soft_at, hard_at - KILL_RESERVE_SECONDS)
    owned: dict[int, str] = {}
    refresh_owned(proc.pid, owned, hard_at)
    timed_out = False
    term_sent = False
    kill_sent = False
    requested_signal: signal.Signals | None = None

    def send(sig: signal.Signals) -> None:
        if args.kill_descendant_tree:
            refresh_owned(proc.pid, owned, hard_at)
            signal_owned(owned, sig, hard_at)
        else:
            signal_group(pgid, sig)

    def forward(sig: int, _frame: object) -> None:
        nonlocal requested_signal, timed_out
        timed_out = True
        requested_signal = signal.Signals(sig)

    signal.signal(signal.SIGTERM, forward)
    signal.signal(signal.SIGINT, forward)

    returncode: int | None = None
    while time.monotonic() < hard_at:
        returncode = proc.poll()
        if returncode is not None:
            break
        if args.kill_descendant_tree:
            refresh_owned(proc.pid, owned, hard_at)
        now = time.monotonic()
        if requested_signal is not None and not term_sent:
            timed_out = True
            term_sent = True
            send(requested_signal)
        elif not term_sent and now >= soft_at:
            timed_out = True
            term_sent = True
            send(signal.SIGTERM)
        if not kill_sent and now >= kill_at:
            timed_out = True
            kill_sent = True
            send(signal.SIGKILL)
        time.sleep(min(0.05, time_left(hard_at)))

    returncode = proc.poll()
    if returncode is None:
        # No wait follows the hard boundary. The outer owner records unresolved state.
        return TIMEOUT

    escaped = owned_alive(owned, hard_at) if args.kill_descendant_tree else group_exists(pgid)
    if escaped and time.monotonic() < hard_at:
        orphan_without_timeout = not timed_out
        send(signal.SIGTERM)
        term_end = min(hard_at, time.monotonic() + 0.5)
        while time.monotonic() < term_end:
            escaped = owned_alive(owned, hard_at) if args.kill_descendant_tree else group_exists(pgid)
            if not escaped:
                break
            time.sleep(min(0.05, time_left(term_end)))
        escaped = owned_alive(owned, hard_at) if args.kill_descendant_tree else group_exists(pgid)
        if escaped and time_left(hard_at) > KILL_RESERVE_SECONDS:
            send(signal.SIGKILL)
        if orphan_without_timeout:
            return ORPHAN
        return TIMEOUT

    if timed_out:
        return TIMEOUT
    if returncode < 0:
        return 128 + (-returncode)
    return returncode


if __name__ == "__main__":
    raise SystemExit(main())
