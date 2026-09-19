#!/usr/bin/env python3
"""Capture bounded diagnostics for one pre-identified busy Bazel Java server."""

from __future__ import annotations

import argparse
import hashlib
import os
import signal
import shutil
import subprocess
import time
from pathlib import Path


ACTIVE: subprocess.Popen[bytes] | None = None


def stop_active(sig: int, _frame: object) -> None:
    if ACTIVE is not None and ACTIVE.poll() is None:
        ACTIVE.terminate()
        try:
            ACTIVE.wait(timeout=0.5)
        except subprocess.TimeoutExpired:
            ACTIVE.kill()
    os._exit(128 + sig)


def proc_identity(proc_root: Path, pid: int) -> tuple[str, str, str]:
    stat = (proc_root / str(pid) / "stat").read_text()
    closing = stat.rfind(")")
    if closing < 0:
        raise ValueError("malformed proc stat")
    fields = stat[closing + 2 :].split()
    if len(fields) < 20:
        raise ValueError("short proc stat")
    state = fields[0]
    starttime = fields[19]
    cmdline = (proc_root / str(pid) / "cmdline").read_bytes().replace(b"\0", b" ").decode(errors="replace")
    return state, starttime, cmdline


def write_bounded(path: Path, data: bytes, limit: int) -> bool:
    path.write_bytes(data[:limit])
    if len(data) > limit:
        path.with_name(path.name + ".truncated").write_text(
            f"received_bytes={len(data)} stored_bytes={limit} limit_bytes={limit}\n"
        )
        return False
    return True


def main() -> int:
    global ACTIVE
    parser = argparse.ArgumentParser()
    parser.add_argument("--pid", type=int, required=True)
    parser.add_argument("--starttime", required=True)
    parser.add_argument("--expected-cmd-fragment", required=True)
    parser.add_argument("--delay-seconds", type=float, default=240.0)
    parser.add_argument("--proc-root", type=Path, default=Path("/proc"))
    parser.add_argument("--ps", default="/usr/bin/ps")
    parser.add_argument("--jcmd", default="/usr/bin/jcmd")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--log-limit", type=int, required=True)
    parser.add_argument("--profile-limit", type=int, required=True)
    args = parser.parse_args()
    signal.signal(signal.SIGTERM, stop_active)
    signal.signal(signal.SIGINT, stop_active)
    time.sleep(args.delay_seconds)
    args.output_dir.mkdir(parents=True, exist_ok=True)

    state, starttime, cmdline = proc_identity(args.proc_root, args.pid)
    if state == "Z" or starttime != args.starttime:
        raise SystemExit("busy Bazel server identity changed")
    lowered = cmdline.lower()
    if args.expected_cmd_fragment not in cmdline or "java" not in lowered or "bazel" not in lowered:
        raise SystemExit("PID is not the intended Bazel Java server")
    (args.output_dir / "homebrew-server-identity.txt").write_text(
        f"pid={args.pid}\nstarttime={starttime}\ncmdline={cmdline}\n"
    )

    ACTIVE = subprocess.Popen(
        [args.ps, "-eo", "pid,ppid,pgid,user,stat,etime,args"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    try:
        ps_stdout, _ = ACTIVE.communicate(timeout=5)
    except subprocess.TimeoutExpired:
        ACTIVE.kill()
        ps_stdout, _ = ACTIVE.communicate(timeout=0.5)
        raise SystemExit("bounded ps timeout")
    ps_returncode = ACTIVE.returncode
    ACTIVE = None
    if ps_returncode != 0:
        raise SystemExit(f"ps failed: {ps_returncode}")
    if not write_bounded(args.output_dir / "homebrew-240s-processes.txt", ps_stdout, args.log_limit):
        raise SystemExit("process diagnostic truncated")

    try:
        ACTIVE = subprocess.Popen(
            [args.jcmd, str(args.pid), "Thread.print"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        jcmd_stdout, _ = ACTIVE.communicate(timeout=15)
    except subprocess.TimeoutExpired as exc:
        ACTIVE.kill()
        try:
            tail, _ = ACTIVE.communicate(timeout=0.5)
        except subprocess.TimeoutExpired:
            tail = b""
        output = (exc.stdout or b"") + (exc.stderr or b"") + tail
        write_bounded(args.output_dir / "homebrew-jcmd-thread-print.log", output, args.log_limit)
        raise SystemExit("bounded jcmd timeout") from exc
    jcmd_returncode = ACTIVE.returncode
    ACTIVE = None
    if not write_bounded(args.output_dir / "homebrew-jcmd-thread-print.log", jcmd_stdout, args.log_limit):
        raise SystemExit("jcmd diagnostic truncated")
    if jcmd_returncode != 0:
        raise SystemExit(f"jcmd failed: {jcmd_returncode}")

    if args.profile.is_file():
        size = args.profile.stat().st_size
        if size <= args.profile_limit:
            shutil.copyfile(args.profile, args.output_dir / "homebrew.profile.partial.gz")
        else:
            (args.output_dir / "profile-omitted.txt").write_text(
                f"profile_bytes={size} limit={args.profile_limit}\n"
            )
    invocation = f"{args.jcmd} {args.pid} Thread.print\n".encode()
    (args.output_dir / "homebrew-jcmd-invocation.sha256").write_text(hashlib.sha256(invocation).hexdigest() + "\n")
    (args.output_dir / "homebrew-240s.txt").write_text("capture=complete\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
