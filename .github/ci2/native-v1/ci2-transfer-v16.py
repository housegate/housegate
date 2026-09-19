#!/usr/bin/env python3
"""Bounded binary exec streams for the actual /ci2 tmpfs mount.

The caller's reviewed watchdog owns our process group, including Docker. No
child starts another session. This helper never calls archive extract methods.
"""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import selectors
import stat
import struct
import subprocess
import sys

# This exact shared parser is also sent to container python via exec -c. Its
# bytes are part of this manifest-pinned helper, not downloaded/bootstrap code.
COMMON = r'''
import hashlib, json, os, re, stat, struct, sys
from pathlib import Path
MAGIC = b"CI2FS008"
HEADER_MAX = 1048576
FILES_MAX = 8192
TOTAL_MAX = 524288000

def check(value, message):
    if not value:
        raise ValueError(message)

def digest(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(65536), b""):
            h.update(block)
    return h.hexdigest()

def file_limit(name):
    if name.endswith(("-aquery.json", "-cquery.json")):
        return 67108864
    if name.endswith((".log", ".stderr", ".stdout")):
        return 33554432
    if name.endswith(".profile.gz"):
        return 16777216
    return TOTAL_MAX

def inventory(entries, limit=TOTAL_MAX):
    check(isinstance(entries, list) and 0 < len(entries) <= FILES_MAX, "invalid file count")
    paths = set()
    total = 0
    for entry in entries:
        check(isinstance(entry, dict) and set(entry) == {"path", "kind", "size", "sha256"}, "invalid entry schema")
        name = entry["path"]
        check(isinstance(name, str) and 0 < len(name) <= 1024 and all(re.fullmatch(r"[A-Za-z0-9_.+-]+", x) and x not in (".", "..") for x in name.split("/")), "unsafe archive path")
        check(entry["kind"] == "file", "non-regular archive entry")
        check(name not in paths, "duplicate archive path")
        check(type(entry["size"]) is int and 0 <= entry["size"] <= file_limit(name), "invalid file size")
        check(isinstance(entry["sha256"], str) and re.fullmatch(r"[0-9a-f]{64}", entry["sha256"]), "invalid file hash")
        paths.add(name)
        total += entry["size"]
        check(total <= limit, "aggregate file limit exceeded")
    for name in paths:
        parts = name.split("/")
        check(not any("/".join(parts[:i]) in paths for i in range(1,len(parts))), "file/directory path collision")
    return total

def read_exact(f, size):
    data = bytearray()
    while len(data) < size:
        block = f.read(min(65536, size-len(data)))
        check(block, "truncated stream")
        data.extend(block)
    return bytes(data)

def read_header(f):
    check(read_exact(f, 8) == MAGIC, "wrong stream magic")
    size = struct.unpack(">I", read_exact(f, 4))[0]
    check(0 < size <= HEADER_MAX, "oversized inventory header")
    entries = json.loads(read_exact(f,size))
    inventory(entries)
    return entries

def header(entries):
    inventory(entries)
    data = json.dumps(entries,separators=(",",":")).encode()
    check(len(data) <= HEADER_MAX, "oversized inventory header")
    return MAGIC + struct.pack(">I",len(data)) + data

def plain(path, sealed=False):
    value = path.lstat()
    check(stat.S_ISREG(value.st_mode) and value.st_nlink == 1, "not a unique regular file: " + str(path))
    check(not sealed or not value.st_mode & 0o222, "evidence file is not sealed")
    return value

def unchanged(before, after):
    return (before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns,before.st_ctime_ns) == (after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns,after.st_ctime_ns)
'''
exec(COMMON)

UPLOAD_REMOTE = COMMON + r'''
import signal
signal.alarm(50)  # Bound the actual mounted consumer even if Docker disconnects.
root, owner, expected_text = sys.argv[1:]
root = Path(root)
expected = json.loads(expected_text)
check(root.is_dir() and not root.is_symlink(), "bad mounted runtime root")
entries = read_header(sys.stdin.buffer)
check(entries == expected and inventory(entries,1048576) <= 1048576, "upload inventory/hash contract mismatch")
check(all("/" not in x["path"] for x in entries), "upload requires fixed basenames")
check(all(not os.path.lexists(root/x["path"]) for x in entries), "refusing helper overwrite")
stage = root/(".ci2-upload-"+owner)
stage.mkdir(mode=0o700)
(stage/".ci2-owner").write_text(owner+"\n")
for item in entries:
    path = stage/item["path"]
    with path.open("xb") as f:
        h = hashlib.sha256()
        left = item["size"]
        while left:
            block = read_exact(sys.stdin.buffer,min(left,65536))
            f.write(block); h.update(block); left -= len(block)
    check(h.hexdigest() == item["sha256"], "uploaded helper hash mismatch")
    path.chmod(0o555)
check(sys.stdin.buffer.read(1) == b"", "trailing upload bytes")
for item in entries:
    check(not os.path.lexists(root/item["path"]), "helper destination appeared")
    # Hard-link provides atomic no-overwrite installation, then removes the
    # staging name. The file is a unique regular file before live verification.
    os.link(stage/item["path"],root/item["path"])
    (stage/item["path"]).unlink()
for item in entries:
    path = root/item["path"]
    check(plain(path).st_size == item["size"] and digest(path) == item["sha256"], "live-mount helper verification failed")
(stage/".ci2-owner").unlink(); stage.rmdir()
print(json.dumps({"installed":entries,"owner":owner},separators=(",",":")))
'''

DOWNLOAD_REMOTE = COMMON + r'''
import signal
signal.alarm(80)  # Bound the actual mounted producer even if Docker disconnects.
root, owner, result, expected_bytes, expected_manifest = sys.argv[1:]
root = Path(root)
control = root.parent/"control"
check(root.is_dir() and not root.is_symlink(), "bad evidence root")
marker = (control/"EVIDENCE_READY").read_bytes()
values = dict(line.split("=",1) for line in marker.decode().splitlines())
check(values == {"result":result,"file_bytes":expected_bytes,"manifest_sha256":expected_manifest}, "finalized marker mismatch")
check((control/"WORKLOAD_COMPLETE").is_file(), "workload not complete")
plain(root/"MANIFEST.sha256",True)
check(digest(root/"MANIFEST.sha256") == expected_manifest, "sealed manifest hash mismatch")
meta = dict(line.split("=",1) for line in (root/"MANIFEST.meta").read_text().splitlines())
check(meta.get("owner_token") == owner and meta.get("result") == result, "evidence owner/result mismatch")
entries=[]
for directory, dirs, files in os.walk(root,followlinks=False):
    for name in dirs:
        check(not (Path(directory)/name).is_symlink(), "symlink evidence directory")
    for name in files:
        path=Path(directory)/name
        before=plain(path,True)
        check(before.st_size <= file_limit(str(path.relative_to(root))), "oversized sealed evidence file")
        check(sum(x["size"] for x in entries)+before.st_size<=TOTAL_MAX, "oversized sealed evidence aggregate")
        entries.append({"path":str(path.relative_to(root)),"kind":"file","size":before.st_size,"sha256":digest(path)})
        check(unchanged(before,plain(path,True)), "evidence changed while hashing")
        check(len(entries)<=FILES_MAX,"too many evidence files")
entries.sort(key=lambda x:x["path"])
check(inventory(entries) == int(expected_bytes), "sealed evidence byte count mismatch")
sys.stdout.buffer.write(header(entries))
for item in entries:
    path=root/item["path"]
    before=plain(path,True)
    with path.open("rb") as f:
        left=item["size"]
        while left:
            block=f.read(min(left,65536))
            check(block,"evidence file shortened")
            sys.stdout.buffer.write(block); left-=len(block)
        check(f.read(1)==b"","evidence file grew")
    check(unchanged(before,plain(path,True)),"evidence changed during transfer")
check((control/"EVIDENCE_READY").read_bytes() == marker,"finalized marker changed during transfer")
sys.stdout.buffer.flush()
'''

UPLOAD_NAMES = (
    'run-linux-qualification-v16-container.sh', 'ci2-bounded-log-v16.py',
    'ci2-toolchain-evidence-v16.py', 'ci2-bazel-stall-v16.py',
    'ci2-homebrew-isolation-v16.py', 'ci2-terminate-owned-v16.py',
    'ci2-phase-owner-v16.py',
)


def tree_bytes(path):
    return sum((Path(d)/f).lstat().st_size for d,_,files in os.walk(path) for f in files)


def owned(path, owner):
    check(path.is_dir() and not path.is_symlink(), 'unowned directory')
    marker=path/'.ci2-owner'
    plain(marker)
    check(marker.read_text()==owner+'\n', 'owner token mismatch')


class Download:
    def __init__(self, parent, owner, expected_bytes, manifest, result, available):
        owned(parent,owner)
        self.parent=parent
        self.owner=owner
        self.expected_bytes=expected_bytes
        self.manifest=manifest
        self.result=result
        self.available=available
        self.stage=parent/'payload.part'
        check(not os.path.lexists(parent/'payload') and not os.path.lexists(self.stage), 'refusing payload overwrite')
        self.buffer=bytearray()
        self.entries=None
        self.index=0
        self.current=None
        self.hash=None
        self.left=0
        self.header_size=None

    def feed(self, data):
        self.buffer.extend(data)
        if self.header_size is None:
            if len(self.buffer)<12: return
            check(self.buffer[:8]==MAGIC,'wrong stream magic')
            self.header_size=struct.unpack('>I',self.buffer[8:12])[0]
            check(0<self.header_size<=HEADER_MAX,'oversized inventory header')
            del self.buffer[:12]
        if self.entries is None:
            if len(self.buffer)<self.header_size: return
            self.entries=json.loads(self.buffer[:self.header_size])
            del self.buffer[:self.header_size]
            check(inventory(self.entries)==self.expected_bytes and self.expected_bytes<=self.available,'wrong evidence byte count or insufficient host output budget')
            manifests=[x for x in self.entries if x['path']=='MANIFEST.sha256']
            check(len(manifests)==1 and manifests[0]['sha256']==self.manifest,'wrong finalized manifest in inventory')
            # The full inventory is validated before any payload directory or
            # file is created. No generic tar/zip extraction is used.
            owned(self.parent,self.owner)
            self.stage.mkdir(mode=0o700)
        while self.index<len(self.entries):
            item=self.entries[self.index]
            if self.current is None:
                target=self.stage/item['path']
                target.parent.mkdir(parents=True,exist_ok=True)
                self.current=target.open('xb')
                self.hash=hashlib.sha256()
                self.left=item['size']
            take=min(self.left,len(self.buffer))
            if take:
                block=self.buffer[:take]
                self.current.write(block); self.hash.update(block)
                del self.buffer[:take]; self.left-=take
            if self.left: return
            self.current.close(); self.current=None
            check(self.hash.hexdigest()==item['sha256'],'downloaded file hash mismatch')
            self.index+=1
        check(not self.buffer,'trailing/oversized stream')

    def finish(self):
        check(self.entries is not None and self.index==len(self.entries) and not self.buffer,'incomplete evidence stream')
        # Existing shell verification still runs, but promotion itself also
        # requires the sealed manifest to name exactly the transferred files.
        data=(self.stage/'MANIFEST.sha256').read_text()
        expected={x['path']:x['sha256'] for x in self.entries if x['path']!='MANIFEST.sha256'}
        actual={}
        for line in data.splitlines():
            check(len(line)>68 and line[64:68]=='  ./','invalid sealed manifest line')
            name=line[68:]
            check(name not in actual,'duplicate sealed manifest path')
            actual[name]=line[:64]
        check(actual==expected,'sealed manifest differs from transferred inventory')
        meta=dict(line.split('=',1) for line in (self.stage/'MANIFEST.meta').read_text().splitlines())
        check(meta.get('owner_token')==self.owner and meta.get('result')==self.result,'transferred evidence owner/result mismatch')
        owned(self.parent,self.owner)
        check(not os.path.lexists(self.parent/'payload'),'payload destination appeared')
        self.stage.rename(self.parent/'payload')

    def close(self):
        if self.current is not None: self.current.close()


def transfer(command, payload, consume, stderr_path, stdout_limit):
    # The caller's existing watchdog bounds the entire helper and inherited
    # process group, including a blocked Docker CLI and its descendants.
    selector=selectors.DefaultSelector()
    process=None
    sent=received=errbytes=0
    try:
        with stderr_path.open('xb') as error_log:
            process=subprocess.Popen(command,stdin=subprocess.PIPE if payload is not None else subprocess.DEVNULL,
                                     stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            for stream,kind in ((process.stdout,'stdout'),(process.stderr,'stderr')):
                os.set_blocking(stream.fileno(),False); selector.register(stream,selectors.EVENT_READ,kind)
            if payload is not None:
                os.set_blocking(process.stdin.fileno(),False); selector.register(process.stdin,selectors.EVENT_WRITE,'stdin')
            while selector.get_map():
                for key,_ in selector.select(0.05):
                    if key.data=='stdin':
                        count=os.write(key.fileobj.fileno(),payload[sent:sent+65536]); sent+=count
                        if sent==len(payload): selector.unregister(key.fileobj); key.fileobj.close()
                        continue
                    block=os.read(key.fileobj.fileno(),65536)
                    if not block:
                        selector.unregister(key.fileobj); key.fileobj.close(); continue
                    if key.data=='stderr':
                        keep=block[:max(0,1048576-errbytes)]
                        error_log.write(keep); errbytes+=len(block)
                        check(errbytes<=1048576,'transport stderr exceeds 1 MiB')
                    else:
                        received+=len(block)
                        check(received<=stdout_limit,'transport stdout bound exceeded')
                        consume(block)
            check(payload is None or sent==len(payload),'upload producer incomplete')
            rc=process.wait()
            check(rc==0,'container-exec transport/remote producer-consumer exit '+str(rc))
    finally:
        selector.close()
        if process is not None and process.poll() is None:
            process.kill()
            try: process.wait(timeout=0.2)
            except subprocess.TimeoutExpired: pass
    return dict(transport_exit=0,stdout_bytes=received,stderr_bytes=errbytes,sent_bytes=sent)


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('mode',choices=['upload','download'])
    for name in ('output-root','owner','container','stderr'):
        parser.add_argument('--'+name,required=True)
    parser.add_argument('--runtime-dir')
    parser.add_argument('--destination')
    parser.add_argument('--expected-bytes',type=int)
    parser.add_argument('--expected-manifest')
    parser.add_argument('--expected-result',choices=['PASS','FAIL'])
    check('--' in sys.argv[1:], 'missing Docker command separator')
    split=sys.argv.index('--')
    args=parser.parse_args(sys.argv[1:split])
    args.docker=sys.argv[split+1:]
    check(re.fullmatch('[0-9a-f]{64}',args.owner) and re.fullmatch('[0-9a-f]{64}',args.container),'invalid exact owner/container id')
    output=Path(args.output_root)
    owned(output,args.owner); owned(output/'host',args.owner)
    available=536870912-tree_bytes(output)-1048576-65536
    check(available>=0,'host output budget exhausted')
    stderr=Path(args.stderr)
    check(stderr.parent==output/'host' and stderr.name in ('upload.stderr','download-PASS.stderr','download-FAIL.stderr'),'unowned stderr destination')
    docker=args.docker[1:] if args.docker[:1]==['--'] else args.docker
    check(docker,'missing Docker argv prefix')
    if args.mode=='upload':
        root=Path(args.runtime_dir)
        owned(root,args.owner)
        check(root==output/'runtime','unowned runtime directory')
        digests={}
        for line in (root/'ci2-runtime-v16.sha256').read_text().splitlines():
            value,name=line.split(); check(name not in digests,'duplicate runtime manifest name'); digests[name]=value
        entries=[]; payloads=[]
        for name in UPLOAD_NAMES:
            path=root/name; before=plain(path)
            data=path.read_bytes()
            check(unchanged(before,plain(path)) and hashlib.sha256(data).hexdigest()==digests.get(name),'reviewed helper hash mismatch')
            entries.append(dict(path=name,kind='file',size=len(data),sha256=digests[name])); payloads.append(data)
        inventory(entries,1048576)
        payload=header(entries)+b''.join(payloads)
        reply=bytearray()
        command=[*docker,'exec','-i','--user','0:0',args.container,'/usr/bin/python3','-c',UPLOAD_REMOTE,'/ci2',args.owner,json.dumps(entries,separators=(',',':'))]
        result=transfer(command,payload,reply.extend,stderr,65536)
        check(json.loads(reply)==dict(installed=entries,owner=args.owner),'missing live-mount installation verification')
        result['installed']=entries
    else:
        check(args.expected_result and args.expected_manifest and re.fullmatch('[0-9a-f]{64}',args.expected_manifest),'invalid sealed result/manifest')
        check(args.expected_bytes is not None and 0<args.expected_bytes<=TOTAL_MAX,'invalid sealed byte count')
        parent=Path(args.destination)
        check(parent==output/'evidence','unowned evidence destination')
        receiver=Download(parent,args.owner,args.expected_bytes,args.expected_manifest,args.expected_result,available)
        command=[*docker,'exec','--user','0:0',args.container,'/usr/bin/python3','-c',DOWNLOAD_REMOTE,'/ci2/evidence',args.owner,args.expected_result,str(args.expected_bytes),args.expected_manifest]
        try:
            result=transfer(command,None,receiver.feed,stderr,args.expected_bytes+HEADER_MAX+12)
            # Existing host bound is checked before promotion as well as by the
            # caller. No archive spool doubles the payload footprint.
            total=tree_bytes(output)
            check(total<=536870912,'host output bound exceeded before promotion')
            receiver.finish()
        finally: receiver.close()
        result['files']=len(receiver.entries)
        result['manifest_sha256']=args.expected_manifest
    print(json.dumps(result,sort_keys=True))

if __name__=='__main__':
    try: main()
    except Exception as exc:
        print('tmpfs transfer failed: '+str(exc),file=sys.stderr)
        raise SystemExit(1)
