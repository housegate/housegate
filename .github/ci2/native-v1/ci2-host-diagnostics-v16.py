#!/usr/bin/env python3
"""Fixed, unsealed failure diagnostics and bounded host launch output. Not evidence."""
import argparse
import json
import os
from pathlib import Path
import selectors
import signal
import stat
import subprocess
import sys
import time

STREAM_LIMIT = 131072
# One fixed read, no directory walk, process discovery, archive or source access.
FILES = (
 'evidence/diagnostics/ci-build-steps.txt',
 'control/ordinary-ci-build.json',
 'evidence/logs/ci-build.log.exit',
 'evidence/logs/ci-build.log',
 'evidence/toolchains/ci-resolution.log.exit',
 'evidence/toolchains/ci-resolution.log',
 'evidence/toolchains/ci-aquery.json.exit',
 'evidence/toolchains/ci-aquery.stderr',
 'evidence/toolchains/ci-cquery.json.exit',
 'evidence/toolchains/ci-cquery.stderr',
 'evidence/toolchains/ci-execution-root.stderr',
 'evidence/toolchains/ci-verifier.stderr',
 'evidence/toolchains/ci-verifier.stdout',
 'evidence/toolchains/ci-invocation.json',
 'evidence/toolchains/ci-verified.json',
 'evidence/usage.log',
 'evidence/diagnostics/homebrew-steps.txt',
 'evidence/diagnostics/server-pid.stdout', 'evidence/diagnostics/server-pid.stderr',
 'evidence/diagnostics/server-pid.stdout.exit',
 'evidence/diagnostics/homebrew-server-preidentified.txt',
 'evidence/diagnostics/isolation.stdout', 'evidence/diagnostics/isolation.stderr',
 'evidence/diagnostics/isolation.stdout.exit',
 'evidence/logs/homebrew.log', 'evidence/logs/homebrew.log.exit',
 'evidence/phase-aborts.txt', 'evidence/diagnostics/shutdown.stdout',
 'evidence/diagnostics/shutdown.stderr', 'evidence/diagnostics/shutdown.json',
 'control/phase.identity', 'control/phase.last-identity',
 'control/finalizer.lock/identity', 'control/EVIDENCE_READY', 'control/WORKLOAD_COMPLETE',
)
PER_FILE = 8192
AGGREGATE = 311296
REMOTE = r'''
import base64,json,os,signal,stat,sys,time
files=json.loads(sys.argv[1]); owner=sys.argv[2]
assert len(files)==35 and len(set(files))==35 and all('..' not in p.split('/') for p in files)
assert len(owner)<=128 and all(x in '0123456789abcdef' for x in owner)
slice_files={'evidence/logs/ci-build.log','evidence/toolchains/ci-resolution.log','evidence/toolchains/ci-aquery.stderr','evidence/toolchains/ci-cquery.stderr','evidence/toolchains/ci-execution-root.stderr','evidence/toolchains/ci-verifier.stdout','evidence/toolchains/ci-verifier.stderr','evidence/usage.log','evidence/logs/homebrew.log','evidence/diagnostics/server-pid.stderr','evidence/diagnostics/isolation.stdout','evidence/diagnostics/isolation.stderr','evidence/diagnostics/shutdown.stdout','evidence/diagnostics/shutdown.stderr'}
start=time.monotonic(); total=0; encoded=0
signal.signal(signal.SIGALRM,lambda *a: (_ for _ in ()).throw(TimeoutError()))
def emit(row,header=False):
 global encoded
 meta={k:v for k,v in row.items() if k!='data_base64'}
 assert len(json.dumps(meta,separators=(',',':')).encode())<= (2048 if header else 1024)
 line=json.dumps(row,separators=(',',':'))+'\n'
 assert encoded+len(line.encode())<=458752
 encoded+=len(line.encode());print(line,end='',flush=True)
emit(dict(kind='UNSEALED_FAILURE_DIAGNOSTICS',owner=owner,complete_acquisition=False),True)
for name in files:
 row=dict(path=name,status='failed',bytes=0,complete_bytes=False)
 remaining=7-(time.monotonic()-start)
 if remaining<=0:
  row['status']='not_attempted_deadline';emit(row);continue
 if total>=311296:
  row['status']='not_attempted_raw_limit';emit(row);continue
 signal.setitimer(signal.ITIMER_REAL,min(1,remaining))
 try:
  fd=os.open('/ci2',os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
  try:
   parts=name.split('/')
   for part in parts[:-1]:
    child=os.open(part,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=fd);os.close(fd);fd=child
   leaf=os.open(parts[-1],os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=fd)
   try:
    before=os.fstat(leaf)
    if not stat.S_ISREG(before.st_mode) or before.st_nlink!=1:raise ValueError('not unique regular file')
    cap=min(8192,311296-total);size=before.st_size
    if size<0 or size>9223372036854775807:raise ValueError('size outside fixed metadata bound')
    row['observed_size']=size
    if size>cap and name not in slice_files:
     row['status']='refused_oversized_complete_record';data=b'';offsets=[]
    elif size>cap:
     head=cap//2;data=os.read(leaf,head);offsets=[[0,len(data)]]
     os.lseek(leaf,size-(cap-head),os.SEEK_SET);tail=os.read(leaf,cap-head)
     offsets.append([size-(cap-head),len(tail)]);data+=tail
     row['status']='truncated_head_tail'
    else:
     data=os.read(leaf,size);offsets=[[0,len(data)]];row['status']='observed' if len(data)==size else 'partial_short_read'
    after=os.fstat(leaf)
   finally:os.close(leaf)
  finally:os.close(fd)
  changed=any(getattr(before,k)!=getattr(after,k) for k in ('st_dev','st_ino','st_nlink','st_size','st_mtime_ns','st_ctime_ns'))
  if changed:row['status']='partial_changed'
  total+=len(data)
  row.update(bytes=len(data),offsets=offsets,complete_bytes=row['status']=='observed',data_base64=base64.b64encode(data).decode())
 except FileNotFoundError:row['status']='missing'
 except TimeoutError:row['status']='partial_timeout'
 except Exception as e:row['error']=type(e).__name__[:64]
 finally:signal.setitimer(signal.ITIMER_REAL,0)
 emit(row)
emit(dict(kind='snapshot_end',files=len(files),bytes=total,complete_acquisition=False),True)
'''

def owned(path, owner):
    if path.is_symlink() or not path.is_dir(): raise ValueError('unowned directory')
    marker=path/'.ci2-owner'
    if marker.is_symlink() or not marker.is_file() or marker.read_text()!=owner+'\n': raise ValueError('owner mismatch')

def capture(command, directory, name, limit=STREAM_LIMIT):
    # Caller watchdog owns this process group and all local descendants. Durable
    # files exist before the CLI starts, including when that watchdog expires.
    streams={}
    with (directory/(name+'.status.json')).open('x') as f:
        json.dump(dict(kind='HOST_COMMAND_BOUNDARY',state='started',remote_quiescence=False),f)
    for key in ('stdout','stderr'):
        streams[key]=open(directory/(name+'.'+key),'xb',buffering=0)
    try:
        proc=subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        sel=selectors.DefaultSelector(); seen={'stdout':0,'stderr':0}
        for key, pipe in [('stdout',proc.stdout),('stderr',proc.stderr)]: sel.register(pipe,selectors.EVENT_READ,key)
        while sel.get_map():
            for key,_ in sel.select(.1):
                data=os.read(key.fileobj.fileno(),65536)
                if not data: sel.unregister(key.fileobj); key.fileobj.close(); continue
                label=key.data; kept=max(0,limit-seen[label]); streams[label].write(data[:kept]); seen[label]+=len(data)
        rc=proc.wait()
        record=dict(kind='HOST_COMMAND_BOUNDARY',state='returned',exit=rc,remote_quiescence=False,
                    streams={k:dict(observed_bytes=n,retained_bytes=min(n,limit),truncated=n>limit) for k,n in seen.items()})
        (directory/(name+'.status.json')).write_text(json.dumps(record,sort_keys=True)+'\n')
        return rc if rc>=0 else 128-rc
    finally:
        for f in streams.values(): f.close()

def main():
    p=argparse.ArgumentParser(); p.add_argument('mode',choices=['capture','snapshot']); p.add_argument('--output-root',type=Path,required=True); p.add_argument('--owner',required=True); p.add_argument('--name',required=True); p.add_argument('--container')
    # Split explicitly: argparse REMAINDER before options would swallow options.
    raw=sys.argv[1:]; split=raw.index('--'); a=p.parse_args(raw[:split]); command=raw[split+1:]
    if not command or not a.name.replace('-','').isalnum(): raise ValueError('invalid command/name')
    owned(a.output_root,a.owner); owned(a.output_root/'host',a.owner)
    directory=a.output_root/'host'/'unsealed-diagnostics'
    if not directory.exists(): directory.mkdir(mode=0o700); (directory/'.ci2-owner').write_text(a.owner+'\n')
    owned(directory,a.owner)
    if a.mode=='snapshot':
        if not a.container or len(a.container)!=64 or any(x not in '0123456789abcdef' for x in a.container): raise ValueError('exact container id required')
        command += ['exec',a.container,'/usr/bin/python3','-c',REMOTE,json.dumps(FILES),a.owner]
        # <=38*8KiB raw, <=1024 metadata/record and <=4096 header/footer. Streams are capped
        # independently, malformed or interrupted streams remain unsealed bytes.
        limit=458752
    else: limit=STREAM_LIMIT
    return capture(command,directory,a.name,limit)

if __name__=='__main__':
    try: sys.exit(main())
    except Exception as e: print('diagnostic capture refused: '+str(e),file=sys.stderr); sys.exit(74)
