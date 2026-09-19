#!/usr/bin/env python3
"""Fail closed if the shell-only Homebrew target touches C++/Zig setup."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path


FORBIDDEN = re.compile(
    r"tools/cpp:toolchain_type|hermetic_cc_toolchain|@{1,2}zig_sdk|"
    r"zig_wrapper|cc_wrapper|linux_amd64_gnu|darwin_arm64|local_config_cc|"
    r"(?:fetch|download|repository setup)[^\n]*(?:zig|hermetic_cc)",
    re.IGNORECASE,
)


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--log", type=Path, required=True)
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--profile-limit", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    for path in (args.log, args.profile):
        if path.with_name(path.name + ".truncated").exists():
            raise SystemExit(f"truncated Homebrew evidence: {path}")
        if not path.is_file() or path.stat().st_size == 0:
            raise SystemExit(f"missing Homebrew evidence: {path}")
    if args.profile.stat().st_size > args.profile_limit:
        raise SystemExit("Homebrew profile exceeds retained bound")
    text = args.log.read_text(encoding="utf-8", errors="replace")
    if "Build completed successfully" not in text:
        raise SystemExit("Homebrew Bazel success marker missing")
    if not re.search(r"//:homebrew_formula_updater_test.*PASSED", text):
        raise SystemExit("Homebrew target PASS marker missing")
    forbidden = FORBIDDEN.search(text)
    if forbidden:
        raise SystemExit(f"Homebrew touched forbidden C++/Zig setup: {forbidden.group(0)}")
    report = {
        "result": "PASS",
        "proof": "shell-only target; no C++ resolution, Zig/hermetic setup, or local compiler setup",
        "log_bytes": args.log.stat().st_size,
        "log_sha256": digest(args.log),
        "profile_bytes": args.profile.stat().st_size,
        "profile_sha256": digest(args.profile),
    }
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
