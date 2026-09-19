#!/usr/bin/env python3
"""Bound one token-owned FAIL output using only its evidence and host artifacts."""

from __future__ import annotations

import argparse
import os
import shutil
import stat
from pathlib import Path


def tree_bytes(root: Path) -> int:
    total = 0
    for current, _, files in os.walk(root, followlinks=False):
        for name in files:
            total += os.lstat(Path(current) / name).st_size
    return total


def assert_owned(path: Path, token: str) -> None:
    if not path.is_dir() or path.is_symlink():
        raise SystemExit(f"not an owned directory: {path}")
    marker = path / ".ci2-owner"
    if not marker.is_file() or marker.is_symlink() or marker.read_text().strip() != token:
        raise SystemExit(f"owner token mismatch: {path}")


def read_retained(path: Path, limit: int) -> bytes:
    # Validate the opened object, not only the pathname checked before reading.
    # O_NONBLOCK prevents malformed FIFO fixtures from blocking the cleanup path.
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_size > limit:
            raise SystemExit(f"invalid or oversized retention entry: {path}")
        with os.fdopen(fd, "rb", closefd=False) as handle:
            data = handle.read(limit + 1)
        after = os.fstat(fd)
        if len(data) > limit or (before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns):
            raise SystemExit(f"retention entry changed while reading: {path}")
        return data
    finally:
        os.close(fd)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--owner-token", required=True)
    parser.add_argument("--limit", type=int, required=True)
    args = parser.parse_args()
    output = args.output
    assert_owned(output, args.owner_token)
    host = output / "host"
    assert_owned(host, args.owner_token)
    size = tree_bytes(output)
    removed_evidence = False
    rebuilt_host = False

    evidence = output / "evidence"
    if size > args.limit and evidence.exists():
        assert_owned(evidence, args.owner_token)
        shutil.rmtree(evidence)
        removed_evidence = True
        size = tree_bytes(output)

    if size > args.limit:
        assert_owned(host, args.owner_token)
        failure = host / "failure.txt"
        snapshot = failure.read_text(errors="replace")[:65536] if failure.is_file() and not failure.is_symlink() else "failure details unavailable\n"
        # Keep the bounded durable diagnostic copy even after sealed export fails.
        retained = {}
        retained_status = {}
        for name in ("operation-status.txt", "diagnostic-status.txt"):
            path = host / name
            if os.path.lexists(path):
                retained_status[name] = read_retained(path, 16384)
        diagnostics = host / "unsealed-diagnostics"
        if diagnostics.exists():
            assert_owned(diagnostics, args.owner_token)
            for item in diagnostics.iterdir():
                retained[item.name] = read_retained(item, 458752)
        # Exact boundary statuses share the existing aggregate retention budget;
        # even the retained failure snapshot is included before destructive work.
        retained_bytes = sum(map(len, retained.values())) + sum(map(len, retained_status.values())) + len(snapshot.encode("utf-8"))
        # Leave 4KiB for the rebuilt owner/failure/accounting records.
        if retained_bytes + 4096 > 4194304:
            raise SystemExit("diagnostics and statuses exceed fixed 4MiB retention budget")
        shutil.rmtree(host)
        host.mkdir(mode=0o700)
        if retained:
            diagnostics.mkdir(mode=0o700)
            for name, data in retained.items():
                (diagnostics / name).write_bytes(data)
        for name, data in retained_status.items():
            (host / name).write_bytes(data)
        (host / ".ci2-owner").write_text(args.owner_token + "\n")
        (host / "failure.txt").write_text(snapshot + "minimal_failure_record=true\n")
        rebuilt_host = True
        size = tree_bytes(output)

    with (host / "failure.txt").open("a", encoding="utf-8") as handle:
        handle.write(f"removed_exact_owned_partial_evidence={str(removed_evidence).lower()}\n")
        handle.write(f"rebuilt_exact_owned_host={str(rebuilt_host).lower()}\n")
        handle.write(f"host_output_bytes_before_final_record={size}\n")
        handle.write(f"host_output_limit={args.limit}\n")
    size = tree_bytes(output)
    if size > args.limit:
        raise SystemExit(f"owned FAIL output cannot be reduced below limit: {size}>{args.limit}")
    (host / "FAIL_OUTPUT_BOUND").write_text(f"bytes={size}\nlimit={args.limit}\n")
    if tree_bytes(output) > args.limit:
        raise SystemExit("final bound marker exceeded limit")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
