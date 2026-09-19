#!/usr/bin/env python3
"""Drain stdin to EOF while storing at most a fixed number of bytes."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

TRUNCATED = 73
LOGGER_ERROR = 74


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    parser.add_argument("limit", type=int)
    args = parser.parse_args()
    if args.limit < 0:
        parser.error("limit must be non-negative")

    output = None
    first_error: Exception | None = None
    stored = 0
    received = 0
    truncated = False

    try:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        output = args.output.open("wb")
    except Exception as exc:  # Continue draining so the producer cannot hang.
        first_error = exc

    while True:
        try:
            chunk = sys.stdin.buffer.read(1024 * 1024)
        except Exception as exc:
            first_error = first_error or exc
            break
        if not chunk:
            break
        received += len(chunk)
        if stored < args.limit and output is not None and first_error is None:
            keep = chunk[: args.limit - stored]
            try:
                output.write(keep)
                stored += len(keep)
            except Exception as exc:
                first_error = first_error or exc
        if received > args.limit:
            truncated = True

    if output is not None:
        try:
            output.flush()
            os.fsync(output.fileno())
            output.close()
        except Exception as exc:
            first_error = first_error or exc

    if truncated:
        try:
            marker = args.output.with_name(args.output.name + ".truncated")
            marker.write_text(
                f"received_bytes={received} stored_bytes={stored} limit_bytes={args.limit}; "
                "input was drained and PASS is forbidden\n",
                encoding="utf-8",
            )
        except Exception as exc:
            first_error = first_error or exc

    if first_error is not None:
        print(f"bounded logger error after draining input: {first_error}", file=sys.stderr)
        return LOGGER_ERROR
    if truncated:
        return TRUNCATED
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
