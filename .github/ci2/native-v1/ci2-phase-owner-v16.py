#!/usr/bin/env python3
"""Own only one ordinary phase and its failure terminator inside one Docker exec.

No finalizer is launched here. Incomplete notification, timeout, cancellation or
uncertain retirement leaves a refused receipt for exact-container teardown.
"""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import re
import select
import signal
import stat
import subprocess
import sys
import time

LIMIT = 4096
QUEUE_LIMIT = 65536
READ_LIMIT = 16384
STREAM_LIMIT = 131072
PHASES = ('setup', 'homebrew', 'ci-build', 'ci-test-ffi', 'release-linux', 'release-darwin')
FIELDS = {'version', 'state', 'run_id', 'owner_token', 'phase', 'runner_sha256',
          'helper_sha256', 'terminator_sha256', 'wrapper', 'phase_child',
          'terminator_child', 'failure_epoch', 'original_rc', 'phase_wait',
          'terminator_wait', 'phase_absent', 'terminator_absent', 'refusal', 'capture'}


def require(value, message):
    if not value:
        raise RuntimeError(message)


def read_record(path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        before = os.fstat(fd)
        require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1 and
                before.st_size <= LIMIT, 'receipt is not bounded unique regular data')
        data = os.read(fd, LIMIT + 1)
        after = os.fstat(fd)
        require(len(data) <= LIMIT and (before.st_ino, before.st_size, before.st_mtime_ns,
                before.st_ctime_ns) == (after.st_ino, after.st_size, after.st_mtime_ns,
                after.st_ctime_ns), 'receipt changed while read')
    finally:
        os.close(fd)
    def unique(pairs):
        value = {}
        for key, item in pairs:
            require(key not in value, 'duplicate receipt field')
            value[key] = item
        return value
    value = json.loads(data, object_pairs_hook=unique)
    require(set(value) == FIELDS, 'receipt fields differ')
    return value


def save_record(path, row):
    require(set(row) == FIELDS, 'internal receipt fields differ')
    data = (json.dumps(row, sort_keys=True, separators=(',', ':')) + '\n').encode()
    require(len(data) <= LIMIT, 'receipt exceeds 4 KiB')
    temporary = path.with_name(path.name + '.tmp')
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        require(os.write(fd, data) == len(data), 'short receipt write')
    finally:
        os.close(fd)
    os.replace(temporary, path)


def group_absent(pgid):
    # Includes zombies; permission/error is never absence. Never sends a signal.
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return True
    return False


def child_exited(child):
    # Observe without releasing the direct-child PID/PGID identity anchor.
    return os.waitid(os.P_PID, child.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT) is not None


def observe_identity_stat(child_pid, primary_token):
    result = dict(status='unavailable', binding='unknown')
    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
    try:
        fd = os.open('/proc/' + str(child_pid) + '/stat', flags)
        try:
            data = os.read(fd, 4097)
        finally:
            os.close(fd)
    except FileNotFoundError:
        result['status'] = 'absent'
        return result
    except OSError:
        return result
    if len(data) > 4096:
        result['status'] = 'oversized'
        return result
    try:
        text = data.decode('ascii', 'strict')
        head, tail = text.split(' (', 1)
        fields = tail[tail.rindex(')') + 1:].split()
        require(int(head) == child_pid and len(fields) >= 20 and
                fields[0] in ('R', 'S', 'D', 'Z', 'T', 't', 'X', 'x', 'K', 'W', 'P', 'I'),
                'invalid stat identity')
        values = [fields[index] for index in (1, 2, 3, 19)]
        require(all(value.isdigit() and len(value) <= 20 for value in values),
                'invalid stat scalar')
        ppid, pgrp, session, starttime = values
        comparable = None
        if isinstance(primary_token, str) and primary_token.startswith('proc:'):
            encoded = primary_token[5:]
            if encoded.isascii() and encoded.isdigit() and len(encoded) <= 20:
                comparable = int(encoded)
        observed = int(starttime)
        if comparable is None:
            status = 'valid'
            binding = 'original_token_unavailable' if not primary_token else 'unknown'
        elif comparable == observed:
            status = 'valid'
            binding = 'same_starttime'
        else:
            status = 'reused'
            binding = 'different_starttime'
        result = dict(status=status, pid=child_pid, ppid=int(ppid), pgrp=int(pgrp),
                      session=int(session), starttime=starttime, state=fields[0], binding=binding)
    except (UnicodeError, ValueError, RuntimeError):
        result = dict(status='malformed', binding='unknown')
    return result


def emit_identity_refusal(site, child_pid, token, primary_getpgid):
    prefix = b'CI2_IDENTITY_REFUSAL_V1 '
    fallback = prefix + b'{"status":"diagnostic_unavailable"}\n'
    try:
        row = dict(site=site, requestedPID=child_pid,
                   primarytokenpresent=bool(token),
                   primarygetpgid=primary_getpgid,
                   laterstat=observe_identity_stat(child_pid, token))
        data = prefix + json.dumps(row, sort_keys=True, separators=(',', ':')).encode() + b'\n'
        if len(data) > 1024:
            data = fallback
    except Exception:
        data = fallback
    try:
        os.write(2, data)
    except (BlockingIOError, BrokenPipeError, OSError):
        pass


def identity(site, child_pid, owned):
    token = owned.proc_start_token(child_pid)
    primary_getpgid = dict(status='not_attempted')
    try:
        if token:
            try:
                pgid = os.getpgid(child_pid)
                primary_getpgid = dict(status='value', value=pgid)
            except OSError as exc:
                primary_getpgid = dict(status='errno', errno=(exc.errno if type(exc.errno) is int else -1))
                raise
        require(token and pgid == child_pid, 'child is not exact group leader')
    except (RuntimeError, OSError):
        emit_identity_refusal(site, child_pid, token, primary_getpgid)
        raise
    return dict(pid=child_pid, pgid=child_pid, start_token=token)


class PrivateOutput:
    """Only this owner holds Docker output; children receive private pipes."""
    def __init__(self):
        self.pipes = {}
        self.pending = {'stdout': bytearray(), 'stderr': bytearray()}
        self.observed = {'stdout': 0, 'stderr': 0}
        self.forwarded = {'stdout': 0, 'stderr': 0}
        self.maximum = {'stdout': 0, 'stderr': 0}
        self.complete = False
        self.flags = {fd: os.get_blocking(fd) for fd in (1, 2)}
        for fd in self.flags:
            os.set_blocking(fd, False)

    def attach(self, child):
        for name, pipe in (('stdout', child.stdout), ('stderr', child.stderr)):
            os.set_blocking(pipe.fileno(), False)
            self.pipes[pipe.fileno()] = (pipe, name)

    def pump(self):
        for name, fd in (('stdout', 1), ('stderr', 2)):
            if self.pending[name]:
                try:
                    count = os.write(fd, self.pending[name])
                except BlockingIOError:
                    continue
                require(count > 0, 'output forwarding made no progress')
                del self.pending[name][:count]
                self.forwarded[name] += count
        eligible = [fd for fd, (_, name) in self.pipes.items()
                    if len(self.pending[name]) < QUEUE_LIMIT]
        ready, _, _ = select.select(eligible, [], [], 0)
        for fd in ready:
            pipe, name = self.pipes[fd]
            allowance = min(READ_LIMIT, QUEUE_LIMIT - len(self.pending[name]))
            if not allowance:
                continue
            try:
                data = os.read(fd, allowance)
            except BlockingIOError:
                continue
            if not data:
                pipe.close(); del self.pipes[fd]
                continue
            self.observed[name] += len(data)
            require(self.observed[name] <= STREAM_LIMIT, 'private output exceeds host stream limit')
            self.pending[name].extend(data)
            self.maximum[name] = max(self.maximum[name], len(self.pending[name]))

    def finish(self, deadline):
        while self.pipes or any(self.pending.values()):
            require(time.monotonic() < deadline, 'private output EOF/forwarding deadline')
            self.pump()
            time.sleep(.01)
        self.complete = True

    def record(self):
        return dict(complete=self.complete, streams={name: dict(observed=self.observed[name],
                    forwarded=self.forwarded[name], pending=len(self.pending[name]),
                    maximum_pending=self.maximum[name], eof=not any(n == name for _, n in self.pipes.values()))
                    for name in self.pending})

    def close(self):
        # Refusal does not wait for a residual descendant's private writer EOF.
        for pipe, _ in self.pipes.values():
            pipe.close()
        self.pipes.clear()
        for fd, flag in self.flags.items():
            os.set_blocking(fd, flag)


def wait_child(child, deadline, output):
    while time.monotonic() < deadline:
        output.pump()
        if child_exited(child):
            return child.wait(timeout=0)
        time.sleep(.01)
    raise TimeoutError('owned child wait deadline')


def wait_absent(pgid, deadline, output):
    while time.monotonic() < deadline:
        output.pump()
        if group_absent(pgid):
            return
        time.sleep(.01)
    raise TimeoutError('owned group still present after child wait')


def check_receipt(row, expected, observed_rc, owned):
    require(set(row) == FIELDS, 'receipt fields differ')
    for key, value in expected.items():
        require(row[key] == value, 'receipt binding mismatch: ' + key)
    require(row['version'] == 13 and row['state'] == 'failed-retired' and row['refusal'] is None,
            'ordinary handoff is incomplete or refused')
    require(type(row['original_rc']) is int and 1 <= row['original_rc'] <= 255 and
            row['original_rc'] == observed_rc, 'original exit is uncertain')
    require(type(row['failure_epoch']) is int and 0 < row['failure_epoch'] <= int(time.time()),
            'first failure time is uncertain')
    require(type(row['terminator_wait']) is int and row['terminator_wait'] == 0 and
            type(row['phase_wait']) is int and row['phase_wait'] in (-15, -9, 143, 137) and
            row['phase_absent'] is True and row['terminator_absent'] is True,
            'wait/retirement proof is incomplete')
    capture = row['capture']
    require(isinstance(capture, dict) and set(capture) == {'complete', 'streams'} and
            capture['complete'] is True and set(capture['streams']) == {'stdout', 'stderr'},
            'private output capture incomplete')
    for item in capture['streams'].values():
        require(set(item) == {'observed', 'forwarded', 'pending', 'maximum_pending', 'eof'} and
                item['eof'] is True and item['pending'] == 0 and
                type(item['observed']) is int and type(item['forwarded']) is int and
                0 <= item['observed'] == item['forwarded'] <= STREAM_LIMIT and
                0 <= item['maximum_pending'] <= QUEUE_LIMIT, 'private output capture not exact')
    for key in ('wrapper', 'phase_child', 'terminator_child'):
        item = row[key]
        require(isinstance(item, dict) and set(item) == {'pid', 'pgid', 'start_token'},
                'child identity fields differ')
        require(type(item['pid']) is int and item['pid'] > 1 and item['pgid'] == item['pid']
                and isinstance(item['start_token'], str) and item['start_token'], 'bad child identity')
        # No takeover of even a reused identifier; the entire group must be absent.
        require(owned.proc_start_token(item['pid']) is None and group_absent(item['pgid']),
                'recorded owner or group remains present')


def run_phase(phase, runner, terminator, control, expected, deadline_epoch, owned):
    path = control / ('ordinary-' + phase + '.json')
    require(not os.path.lexists(path), 'ordinary phase receipt already exists')
    left = deadline_epoch - time.time()
    require(left > 0, 'ordinary phase deadline already expired')
    deadline = time.monotonic() + left
    row = dict(version=13, state='running', **expected, wrapper=identity('wrapper-admission', os.getpid(), owned),
               phase_child=None, terminator_child=None, failure_epoch=None, original_rc=None,
               phase_wait=None, terminator_wait=None, phase_absent=False,
               terminator_absent=False, refusal=None, capture=None)
    save_record(path, row)
    notify_read, notify_write = os.pipe()
    permit_read, permit_write = os.pipe()
    os.set_blocking(notify_read, False)
    child = None
    output = PrivateOutput()
    try:
        environment = dict(os.environ, CI2_OWNER_NOTIFY_FD=str(notify_write),
                           CI2_OWNER_PERMIT_FD=str(permit_read))
        gate = 'IFS= read -r permit <&"$CI2_OWNER_PERMIT_FD"; [[ "$permit" == CI2_PHASE ]] || exit 79; exec "$@"'
        child = subprocess.Popen(['/bin/bash', '-c', gate, 'ci2-phase-gate', '/bin/bash',
                                 str(runner), phase], start_new_session=True, env=environment,
                                 pass_fds=(notify_write, permit_read),
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output.attach(child)
        os.close(notify_write); notify_write = -1
        os.close(permit_read); permit_read = -1
        row['phase_child'] = identity('phase-admission', child.pid, owned)
        save_record(path, row)
        require(os.write(permit_write, b'CI2_PHASE\n') == 10, 'phase permit write incomplete')
        notification = bytearray()
        while time.monotonic() < deadline:
            output.pump()
            ready, _, _ = select.select([notify_read], [], [], .02)
            if ready:
                data = os.read(notify_read, 129)
                notification.extend(data)
                require(len(notification) <= 128, 'oversized failure notification')
                if b'\n' in notification:
                    break
            if child_exited(child):
                # Normal completion is not a group-quiescence claim. A nonzero
                # exit without the private handoff never launches a terminator.
                row['phase_wait'] = child.wait(timeout=0)
                require(not notification and row['phase_wait'] == 0, 'lost anchor or missing handoff')
                output.finish(deadline)
                row['state'] = 'passed'
                row['capture'] = output.record()
                save_record(path, row)
                return 0
        else:
            raise TimeoutError('ordinary phase absolute deadline')
        match = re.fullmatch(rb'([0-9]+) ([1-9][0-9]{0,2}) ([0-9]+)\n', notification)
        require(match is not None, 'incomplete or malformed failure notification')
        pid, rc, failed = map(int, match.groups())
        require(pid == child.pid and 1 <= rc <= 255 and 0 < failed <= int(time.time()),
                'failure notification binding mismatch')
        row.update(failure_epoch=failed, original_rc=rc, state='terminating')
        # Charge 20/5/30 termination to the existing work/phase deadline and to
        # the original failure, never to later host receipt or recovery time.
        deadline = min(deadline, time.monotonic() + max(0, failed + 30 - time.time()))
        require(time.monotonic() < deadline, 'handoff termination budget exhausted')
        recorded = owned.parse_identity((control / 'phase.last-identity').read_bytes())
        require(recorded == dict(run_id=expected['run_id'], owner_token=expected['owner_token'],
                phase=phase, pid=str(pid), pgid=str(pid), starttime=row['phase_child']['start_token'],
                runner_sha256=expected['runner_sha256']), 'phase identity does not match owned child')
        require((control / 'phase.identity').read_bytes() == (control / 'phase.last-identity').read_bytes(),
                'phase shadow identity differs')
        require(identity('phase-anchor-recheck', pid, owned) == row['phase_child'], 'phase anchor changed before termination')
        command = [sys.executable, str(terminator), 'terminate', '--identity',
                   str(control / 'phase.last-identity'), '--shadow-identity', str(control / 'phase.identity'),
                   '--runner', str(runner), '--expected-run', expected['run_id'], '--expected-owner',
                   expected['owner_token'], '--expected-runner-sha', expected['runner_sha256'],
                   '--finalizer-lock', str(control / 'finalizer.lock'), '--term-seconds', '20',
                   '--kill-seconds', '5', '--total-seconds', '30']
        # This direct child alone is the independent-PGID terminator. It remains
        # in the same synchronous Docker exec lifetime as this owner wrapper.
        terminator_child = subprocess.Popen(command, start_new_session=True,
                                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output.attach(terminator_child)
        row['terminator_child'] = identity('terminator-admission', terminator_child.pid, owned)
        save_record(path, row)
        row['terminator_wait'] = wait_child(terminator_child, deadline, output)
        require(row['terminator_wait'] == 0, 'strict terminator refused')
        wait_absent(terminator_child.pid, deadline, output)
        row['terminator_absent'] = True
        row['phase_wait'] = wait_child(child, deadline, output)
        require(row['phase_wait'] in (-15, -9, 143, 137), 'phase did not exit by owned termination')
        # No signal can occur after either direct-child identity anchor is reaped.
        wait_absent(child.pid, deadline, output)
        row['phase_absent'] = True
        require(not (control / 'phase.identity').exists() and not (control / 'phase.last-identity').exists(),
                'strict retirement left an operation identity')
        row['state'] = 'failed-retired'
        output.finish(deadline)
        row['capture'] = output.record()
        save_record(path, row)
        return rc
    except Exception as exc:
        row.update(state='refused', refusal=(type(exc).__name__ + ': ' + str(exc))[:256])
        row['capture'] = output.record()
        row['capture']['complete'] = False
        save_record(path, row)
        # Even the refusal message cannot block on Docker's output backpressure.
        try:
            os.write(2, ('CI2_PHASE_HANDOFF_REFUSED: ' + row['refusal'] + '\n').encode())
        except (BlockingIOError, BrokenPipeError):
            pass
        # No retry, background finalizer, broad kill, or unverifiable cleanup.
        return 74
    finally:
        output.close()
        for fd in (notify_read, notify_write, permit_read, permit_write):
            if fd >= 0:
                os.close(fd)


def main():
    require(sys.platform == 'linux', 'phase owner runtime requires Linux')
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=('run', 'receipt'))
    parser.add_argument('phase', choices=PHASES)
    parser.add_argument('--observed-rc', type=int)
    args = parser.parse_args()
    root = Path('/ci2'); control = root / 'control'
    if args.mode == 'run':
        control.mkdir(mode=0o700, exist_ok=True)
    info = control.lstat()
    require(stat.S_ISDIR(info.st_mode) and (info.st_uid, info.st_gid, stat.S_IMODE(info.st_mode)) ==
            (0, 0, 0o700), 'private root control boundary differs')
    runner = root / 'run-linux-qualification-v16-container.sh'
    terminator = root / 'ci2-terminate-owned-v16.py'
    spec = importlib.util.spec_from_file_location('owned', terminator)
    owned = importlib.util.module_from_spec(spec); spec.loader.exec_module(owned)
    expected = dict(run_id=os.environ['CI2_RUN_ID'], owner_token=os.environ['CI2_OWNER_TOKEN'],
                    phase=args.phase, runner_sha256=os.environ['CI2_RUNNER_SHA'],
                    helper_sha256=os.environ['CI2_PHASE_OWNER_SHA'],
                    terminator_sha256=os.environ['CI2_TERMINATOR_SHA'])
    require(re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,47}', expected['run_id']), 'invalid run id')
    for key in ('owner_token', 'runner_sha256', 'helper_sha256', 'terminator_sha256'):
        require(re.fullmatch('[0-9a-f]{64}', expected[key]), 'invalid digest binding')
    for path, key in ((runner, 'runner_sha256'), (Path(__file__), 'helper_sha256'),
                      (terminator, 'terminator_sha256')):
        require(owned.sha256_file(path) == expected[key], 'runtime hash differs: ' + key)
    if args.mode == 'receipt':
        row = read_record(control / ('ordinary-' + args.phase + '.json'))
        # Timing may only shorten teardown's deadline; it grants no authority
        # to finalize. Missing/invalid timing remains explicitly unproved.
        for key, value in expected.items():
            require(row[key] == value, 'receipt binding mismatch: ' + key)
        require(row['version'] == 13, 'receipt version differs')
        failure = row['failure_epoch']
        require(type(failure) is int and 0 < failure <= int(time.time()), 'first failure timing unproved')
        try:
            check_receipt(row, expected, args.observed_rc, owned)
        except (RuntimeError, OSError, TypeError):
            print('refused', failure)
        else:
            print('retired', failure)
        return 0
    def cancelled(*_):
        raise TimeoutError('phase owner cancelled or absolute deadline reached')
    signal.signal(signal.SIGTERM, cancelled)
    signal.signal(signal.SIGINT, cancelled)
    signal.signal(signal.SIGALRM, cancelled)
    deadline = int(os.environ['CI2_PHASE_DEADLINE'])
    require(deadline > time.time(), 'phase deadline expired')
    signal.setitimer(signal.ITIMER_REAL, deadline - time.time())
    try:
        return run_phase(args.phase, runner, terminator, control, expected, deadline, owned)
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as exc:
        try:
            os.set_blocking(2, False)
            os.write(2, ('CI2_PHASE_OWNER_REFUSED: ' + str(exc)[:256] + '\n').encode())
        except OSError:
            pass
        sys.exit(74)
