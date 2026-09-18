#!/usr/bin/env bash
set -Eeuo pipefail

# Native bootstrap. The historical watchdog remains the sole runtime owner.
ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
mode=${1:-}
case "$mode" in
  prepare|observe)
    remaining=$((300 - $(date +%s) + ${CI2_PREPARATION_STARTED:?missing preparation clock}))
    (( remaining > 10 && remaining <= 300 ))
    export CI2_PREPARATION_REMAINING=$remaining
    exec /usr/bin/python3 "$ROOT/ci2-watchdog-v16.py" --soft-seconds "$((remaining - 5))" --hard-seconds "$remaining" --kill-descendant-tree -- /bin/bash "$0" "internal-$mode"
    ;;
  archive)
    exec /usr/bin/python3 "$ROOT/ci2-watchdog-v16.py" --soft-seconds 110 --hard-seconds 120 --kill-descendant-tree -- /bin/bash "$0" internal-archive
    ;;
  qualify)
    # Entry cancellation refuses new authority; the running owner never resets clocks.
    [[ "${CI2_CANCELLED_AT_ENTRY:-true}" == false ]]
    /usr/bin/python3 "$ROOT/ci2-watchdog-v16.py" --soft-seconds 10 --hard-seconds 15 -- /bin/bash "$0" entry
    job="$RUNNER_TEMP/ci2-native-$GITHUB_RUN_ID-$GITHUB_RUN_ATTEMPT"
    export CI2_JOB_ROOT="$job"
    export CI2_V16_OUTPUT_DIR="$job/execution"
    export CI2_V16_RUNTIME_DIR="$job/execution/runtime"
    export CI2_OWNER_TOKEN
    CI2_OWNER_TOKEN=$(cat "$job/.ci2-owner")
    export CI2_V16_STARTED_EPOCH
    CI2_V16_STARTED_EPOCH=$(date +%s)
    export CI2_RUN_ID="$GITHUB_RUN_ID-$GITHUB_RUN_ATTEMPT"
    export CI2_LINUX_QUALIFICATION_AUTHORIZED=ab9ec0a1c27e257f7d96be1e3011f6e5f274a609
    export CI2_V16_MANIFEST_SHA256="$CI2_NATIVE_MANIFEST_SHA256"
    set +e
    /usr/bin/python3 "$CI2_V16_RUNTIME_DIR/ci2-watchdog-v16.py" --soft-seconds 3060 --hard-seconds 3300 --kill-descendant-tree -- /usr/bin/python3 "$CI2_V16_RUNTIME_DIR/ci2-host-diagnostics-v16.py" capture --output-root "$CI2_V16_OUTPUT_DIR" --owner "$CI2_OWNER_TOKEN" --name native-owner -- env CI2_V16_WORKER=1 /bin/bash "$CI2_V16_RUNTIME_DIR/run-linux-qualification-native-v1-worker.sh"
    owner_rc=$?
    set -e
    /usr/bin/python3 "$ROOT/ci2-watchdog-v16.py" --soft-seconds 5 --hard-seconds 10 -- /bin/bash "$0" owner-return "$owner_rc"
    exit "$owner_rc"
    ;;
  entry|host-admit|container-admit|live-init-admit|terminal-metrics|identity-check|phase-disk|retirement|owner-return|internal-checkout|internal-prepare|internal-observe|internal-archive) ;;
  *) echo 'Refused: unknown native carrier entry point' >&2; exit 64 ;;
esac

# BEGIN PYTHON
/usr/bin/python3 - "$ROOT" "$mode" "${@:2}" <<'PY'
"""Finite carrier adapters, not a replacement qualification supervisor."""
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import platform
import re
import shutil
import stat
import subprocess
import sys
import tarfile
import time

HERE = Path(sys.argv[1])
MIB = 1024 * 1024
MAX_FILES = 20000
REUSE = {
    'ci2-bazel-stall-v16.py': '8babaacd265ae5204bc32fdeecffd59e37091079d7dbc18e76e4719312c0ee02',
    'ci2-bounded-log-v16.py': 'c8c5b1fdd1c1aa46cab4ff2952332922d38b40b0635b541aa8b4d54772dd6d69',
    'ci2-enforce-output-bound-v16.py': '202b0f290a36afb73cf3c827aeb6b16944ccda2e865bb01d768b715868ef3739',
    'ci2-homebrew-isolation-v16.py': '7fdf20afdee9bedd58ffe1a0c09094310d814edced7610ad970322fa3740ea71',
    'ci2-host-diagnostics-v16.py': 'e57393b408949214f4afebeb555227b47c7fc7f73374a7208312528360ce4944',
    'ci2-phase-owner-v16.py': '8b6cc74dd0a54d5f9d0e9e2826726683d66b48cc366ee2a4da23c0659630d4c6',
    'ci2-terminate-owned-v16.py': 'b3501559e84dcbcd01bc6b600b0aeee4a79c4990facb0a04fd57ce2491f3f6fa',
    'ci2-toolchain-evidence-v16.py': '6e6fa83aeca059af92bc6ca90206cd98fa63310688161013129abadb36569195',
    'ci2-transfer-v16.py': 'a802ca56bc8e340c1d147474d6d32544f5ad32673ee36b554967023655c67fdd',
    'ci2-watchdog-v16.py': '17ef41289b1a164ad978446649f0446527f388cc99ec6f342ba18ad748b622f9',
}
RUNTIME_NAMES = set(REUSE) | {'run-linux-qualification-v16-container.sh', 'run-linux-qualification-native-v1.sh', 'run-linux-qualification-native-v1-worker.sh'}
PROFILE_SHA = '9b5d015b91dbbaec0b3b332b6309a8b1b1a3fa39fdb7c9b3a45d94e800283d74'
IMAGE_FIELD_NAMES = ('os', 'architecture', 'size', 'user', 'entrypoint', 'workdir')
IMAGE_STRING_MAX = 512
IMAGE_LIST_MAX = 16
IMAGE_SIZE_MAX = (1 << 63) - 1
IMAGE_REFUSAL_MAX_BYTES = 16 * 1024


class ImageConfigurationRefusal(ValueError):
    def __init__(self, diagnostic):
        self.diagnostic = diagnostic
        super().__init__('image configuration')


def require(ok, message):
    if not ok:
        raise ValueError(message)


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, 'duplicate JSON field')
        result[key] = value
    return result


def read_file(path, limit):
    path = Path(path)
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        before = os.fstat(fd)
        require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1 and 0 <= before.st_size <= limit, 'unsafe/oversized regular input')
        with os.fdopen(fd, 'rb', closefd=False) as stream:
            data = stream.read(limit + 1)
        after = os.fstat(fd)
        require(len(data) == before.st_size and all(getattr(before, k) == getattr(after, k) for k in ('st_ino', 'st_dev', 'st_size', 'st_mtime_ns', 'st_ctime_ns')), 'input changed while reading')
        return data
    finally:
        os.close(fd)


def load_json(path, limit=MIB):
    return json.loads(read_file(path, limit), object_pairs_hook=unique_object)


def sha(path, limit=1024*MIB):
    h = hashlib.sha256()
    path = Path(path)
    require(not path.is_symlink() and path.is_file() and path.stat().st_size <= limit, 'unsafe hash input')
    with path.open('rb') as stream:
        for data in iter(lambda: stream.read(MIB), b''):
            h.update(data)
    return h.hexdigest()


def write_json(path, record):
    data = (json.dumps(record, sort_keys=True, separators=(',', ':')) + '\n').encode()
    require(len(data) <= MIB, 'receipt too large')
    with Path(path).open('xb') as stream:
        stream.write(data)
    Path(path).chmod(0o444)


def canonical_json(record):
    return (json.dumps(record, sort_keys=True, separators=(',', ':')) + '\n').encode()


def bounded_image_string(value, name):
    require(isinstance(value, str) and len(value.encode()) <= IMAGE_STRING_MAX, name+' type/size')
    return value


def bounded_image_list(value, name):
    require(isinstance(value, list) and len(value) <= IMAGE_LIST_MAX, name+' type/count')
    return [bounded_image_string(item, name+' member') for item in value]


def bounded_image_size(value, name):
    require(type(value) is int and 0<=value<=IMAGE_SIZE_MAX,name+' type/range')
    return value


def export_refusal(root, mode, error, env=os.environ):
    record=dict(schema='ci2-native-refusal-v1',mode=mode,error=str(error)[:512],workload_created=False,profile_sha256=PROFILE_SHA,run_id=env.get('GITHUB_RUN_ID'),attempt=env.get('GITHUB_RUN_ATTEMPT'))
    if isinstance(error, ImageConfigurationRefusal):
        head=env.get('CI2_CARRIER_HEAD','')
        require(mode=='internal-prepare' and re.fullmatch('[0-9a-f]{40}',head) is not None,'image refusal head/mode binding')
        require(re.fullmatch('[1-9][0-9]{0,19}',env.get('GITHUB_RUN_ID','')) is not None and env.get('GITHUB_RUN_ATTEMPT')=='1','image refusal run/attempt binding')
        record['head']=head
        record['image_admission']=error.diagnostic
        require(len(canonical_json(record))<=IMAGE_REFUSAL_MAX_BYTES,'image refusal receipt too large')
    filename='bootstrap-refusal.json' if mode=='internal-checkout' else 'refusal.json'
    path=Path(root)/'export'/filename
    write_json(path,record)
    return path


def profile(directory=HERE):
    p = load_json(directory / 'profile.json')
    require(sha(directory / 'profile.json', MIB) == PROFILE_SHA, 'profile digest mismatch')
    require(set(p) == {'schema','repository','head_branch','source','image','resources','clocks','docker_endpoint','admitted_host_tuples','historical_runtime_manifest_sha256','native_manifest_policy','native_fixture_authority','workload_storage'}, 'profile schema fields')
    require(p['schema'] == 'housegate-ci2-native-hosted-v1', 'profile schema')
    require(isinstance(p['admitted_host_tuples'], list) and len(p['admitted_host_tuples']) <= 8, 'finite tuple list required')
    for item in p['admitted_host_tuples']:
        require(set(item) == {'runner_image','runner_version','client_version','server_version','architecture','storage_driver','cgroup_driver','cgroup_version','init_path','init_sha256','init_version'}, 'tuple schema')
        require(all(isinstance(v, str) and 0 < len(v) <= 512 for v in item.values()), 'tuple value')
        require(re.fullmatch('[0-9a-f]{64}', item['init_sha256']), 'init hash')
    require(p['workload_storage'] == dict(layout='tmpfs-single-output-v1',output_user_root='/ci2/cache/output-root',disk_cache='disabled',disk_cache_sentinel='/ci2/cache/disk',disk_cache_gc='not-applicable'), 'workload storage policy')
    gc=p['resources']['cold_cache_gc_target']
    require(type(gc) is int and gc == 0, 'disabled disk cache has no GC target')
    c=p['clocks']
    require(c['preparation']+c['owner']+c['archive']+c['transitions']==c['job'] and c['work']+c['cleanup']==c['owner'], 'clock arithmetic')
    return p


def manifest(directory=HERE):
    rows = {}
    for line in read_file(directory/'ci2-runtime-v16.sha256', 4096).decode().splitlines():
        match = re.fullmatch(r'([0-9a-f]{64})  ([a-zA-Z0-9.-]+)', line)
        require(match is not None, 'manifest syntax')
        digest, name = match.groups()
        require(name not in rows, 'duplicate runtime name')
        rows[name] = digest
    require(set(rows) == RUNTIME_NAMES, 'runtime inventory')
    for name, digest in rows.items():
        require(sha(directory/name, MIB) == digest, 'runtime digest mismatch: '+name)
    require(all(rows[name] == digest for name,digest in REUSE.items()), 'historical helper changed')
    return sha(directory/'ci2-runtime-v16.sha256', 4096)


def admission(event, env, p, lane):
    require(lane in ('observe', 'qualify'), 'admission lane')
    require(env.get('GITHUB_EVENT_NAME') == 'pull_request' and env.get('GITHUB_RUN_ATTEMPT') == '1', 'event/attempt refused')
    require(env.get('CI2_CANCELLED_AT_ENTRY') == 'false', 'cancelled/unknown entry')
    require(event.get('action') == 'labeled', 'exact labeled event required')
    pr = event['pull_request']
    require(pr['base']['repo']['full_name'] == pr['head']['repo']['full_name'] == env.get('GITHUB_REPOSITORY') == p['repository'], 'repository/fork refused')
    require(pr['head']['ref'] == p['head_branch'] and pr['draft'] is False, 'branch/draft refused')
    head = pr['head']['sha']
    require(isinstance(head,str) and re.fullmatch('[0-9a-f]{40}', head), 'head SHA')
    label = ('ci2-env-' if lane == 'observe' else 'ci2-ok-') + head
    require(event.get('label',{}).get('name') == label, 'event label is not the exact current-head admission')
    labels = pr.get('labels', [])
    require(len(labels) <= 100 and label in [x['name'] for x in labels], 'fresh exact-head label required')
    require(env.get('CI2_CARRIER_HEAD') == head, 'carrier checkout binding')
    if lane == 'qualify':
        require(bool(p['admitted_host_tuples']), 'qualification disabled: no admitted Docker/init tuple')
    require(re.fullmatch('[1-9][0-9]{0,19}', env.get('GITHUB_RUN_ID','')), 'run id')
    return dict(schema='ci2-native-admission-v1',lane=lane,head=head,head_ref=pr['head']['ref'],repository=p['repository'],pr=event['number'],event_merge_sha=env.get('GITHUB_SHA'),run_id=env['GITHUB_RUN_ID'],attempt=1,profile_sha256=PROFILE_SHA,issued_epoch=int(time.time()))


def entry_grant(env,admitted,now):
    stamp=env.get('CI2_OWNER_ENTRY_EPOCH','')
    require(re.fullmatch('[0-9]{10}',stamp),'malformed owner entry time')
    age=now-int(stamp)
    require(0<=age<=15,'owner entry grant expired/future')
    for key,expected in [('CI2_OWNER_ENTRY_HEAD',admitted['head']),('CI2_OWNER_ENTRY_PROFILE',admitted['profile_sha256']),('CI2_OWNER_ENTRY_RUN',admitted['run_id']),('CI2_OWNER_ENTRY_ATTEMPT',str(admitted['attempt']))]:
        require(env.get(key)==expected,'owner entry grant binding mismatch')
    return dict(epoch=int(stamp),age_seconds=age,head=admitted['head'],profile_sha256=admitted['profile_sha256'],run_id=admitted['run_id'],attempt=admitted['attempt'])


def inventory(root, limit=536870912, source_links=False):
    root=Path(root); rows=[]; total=0; entries=0
    require(root.is_dir() and not root.is_symlink(), 'inventory root')
    for parent, dirs, files in os.walk(root, followlinks=False):
        for name in dirs + files:
            entries+=1
            path=Path(parent)/name; s=path.lstat()
            if stat.S_ISLNK(s.st_mode) and source_links:
                # Git may contain tracked symlinks. Never follow or export this
                # verification checkout; git status/tree bind their exact bytes.
                require(s.st_size<=4096,'source symlink bound')
                total+=s.st_size
                require(total<=limit,'source link byte bound')
                continue
            require(not stat.S_ISLNK(s.st_mode) and (stat.S_ISDIR(s.st_mode) or stat.S_ISREG(s.st_mode)), 'unsafe staged type')
            require(entries <= MAX_FILES, 'entry count')
            if stat.S_ISREG(s.st_mode):
                require(s.st_nlink == 1, 'hard link refused')
                total += s.st_size
                require(total <= limit and len(rows) < MAX_FILES, 'staging/evidence bound')
                rows.append((path.relative_to(root).as_posix(), s.st_size))
    return sorted(rows), total


def staging_inventory(components, limit=536870912):
    """Charge carrier/history, source and runtime together, never evidence.

    Root aliases/overlap are canonicalized and inode-deduplicated; distinct
    files on the same device still consume distinct bytes. Child symlinks are
    charged as Git stores them and never followed. This is a measured bound.
    """
    require(set(components)=={'carrier','source','runtime'},'staging components')
    seen=set(); total=0; count=0; roots={}; devices={}
    for label,path in components.items():
        path=Path(path).resolve(strict=True)
        require(path.is_dir(),'staging root must be a directory')
        rootstat=path.stat(); charged=0; pending=[path]
        while pending:
            directory=pending.pop(); ds=directory.stat()
            identity=(ds.st_dev,ds.st_ino)
            if identity in seen:continue
            seen.add(identity)
            with os.scandir(directory) as entries:
                for entry in entries:
                    s=entry.stat(follow_symlinks=False); key=(s.st_dev,s.st_ino)
                    if key in seen:continue
                    count+=1; require(count<=MAX_FILES,'combined staging entry bound')
                    if stat.S_ISDIR(s.st_mode):
                        pending.append(Path(entry.path));continue
                    require(stat.S_ISREG(s.st_mode) or stat.S_ISLNK(s.st_mode),'unsafe staging entry')
                    if stat.S_ISREG(s.st_mode):require(s.st_nlink==1,'staging hard link refused')
                    else:require(s.st_size<=4096,'staging symlink bound')
                    seen.add(key);total+=s.st_size;charged+=s.st_size
                    devices[str(s.st_dev)]=devices.get(str(s.st_dev),0)+s.st_size
                    require(total<=limit,'combined source/carrier staging bound: observed_bytes='+str(total)+' limit_bytes='+str(limit))
        roots[label]=dict(path=str(path),device=rootstat.st_dev,charged_bytes=charged)
    return dict(total_bytes=total,entries=count,roots=roots,device_bytes=devices,limit_bytes=limit)


def staging_components(root,checkout):
    runtime=root/'execution'/'runtime'
    # Before runtime installation its owned parent exists and contains evidence;
    # use a separately created empty staging directory, never that parent.
    if not runtime.exists():runtime.mkdir()
    return dict(carrier=Path(checkout),source=root/'input',runtime=runtime)


def owned_root(create=False):
    temp=Path(os.environ['RUNNER_TEMP'])
    require(temp.is_absolute() and temp.resolve() == temp and temp.is_dir(), 'runner temp path')
    run=os.environ['GITHUB_RUN_ID']; attempt=os.environ['GITHUB_RUN_ATTEMPT']
    require(re.fullmatch('[1-9][0-9]{0,19}',run) and attempt=='1', 'run lease')
    root=temp/('ci2-native-'+run+'-'+attempt)
    if create:
        root.mkdir(mode=0o700)
        owner=os.urandom(32).hex()
        (root/'.ci2-owner').write_text(owner+'\n'); (root/'.ci2-owner').chmod(0o400)
        for name in ('host','input','execution','docker-config','export'):
            (root/name).mkdir(mode=0o700)
            (root/name/'.ci2-owner').write_text(owner+'\n')
        (root/'execution'/'host').mkdir()
        (root/'execution'/'host'/'.ci2-owner').write_text(owner+'\n')
    require(root.is_dir() and not root.is_symlink(), 'owned root missing')
    owner=read_file(root/'.ci2-owner',65).decode().strip()
    require(re.fullmatch('[0-9a-f]{64}',owner), 'owner token')
    return root,owner


class Commands:
    """Fixed preparation probes under the unchanged watchdog/capture contracts."""
    def __init__(self, root, owner, deadline, carrier_root=None):
        self.root,self.owner,self.deadline=root,owner,deadline
        self.count=0
        self.disk_watch=None
        self.carrier_root=Path(carrier_root) if carrier_root is not None else Path(os.environ['GITHUB_WORKSPACE'])
        self.env={k:os.environ[k] for k in ('PATH','LANG','LC_ALL') if k in os.environ}
        self.env.update(PATH='/usr/bin:/bin:/usr/sbin:/sbin',HOME=str(root/'input'),GIT_CONFIG_NOSYSTEM='1',GIT_CONFIG_GLOBAL='/dev/null',GIT_TERMINAL_PROMPT='0',GIT_CONFIG_COUNT='0',DOCKER_CONFIG=str(root/'docker-config'),PYTHONDONTWRITEBYTECODE='1')

    def __call__(self, argv, seconds=20, ok=(0,)):
        require(isinstance(argv,list) and all(isinstance(x,str) for x in argv), 'argv only')
        remaining=min(seconds,self.deadline-time.monotonic())
        require(remaining>2, 'preparation/transport deadline')
        self.count+=1; name='native-'+str(os.getpid())+'-'+str(self.count)
        before=staging_inventory(staging_components(self.root,self.carrier_root));peak=before
        command=['/usr/bin/python3',str(HERE/'ci2-watchdog-v16.py'),'--soft-seconds',str(remaining-1),'--hard-seconds',str(remaining),'--kill-descendant-tree','--','/usr/bin/python3',str(HERE/'ci2-host-diagnostics-v16.py'),'capture','--output-root',str(self.root/'execution'),'--owner',self.owner,'--name',name,'--']+argv
        proc=subprocess.Popen(command,env=self.env,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        # The existing watchdog owns cleanup; this adapter only measures staging.
        overage=False
        staging_error=''
        while proc.poll() is None:
            try:
                current=staging_inventory(staging_components(self.root,self.carrier_root))
                if current['total_bytes']>peak['total_bytes']:peak=current
                inventory(self.root/'execution',512*MIB)
                if self.disk_watch is not None:
                    for path,disk_before in self.disk_watch.items():
                        v=os.statvfs(path)
                        require(Path(path).stat().st_dev==disk_before['device'] and disk_before['free']-v.f_bavail*v.f_frsize<=3*1024*MIB,'image allocation monitor')
            except (ValueError,OSError) as error:
                overage=True;staging_error=str(error)[:512];proc.terminate()
            time.sleep(.05)
        rc=proc.wait()
        try:after=staging_inventory(staging_components(self.root,self.carrier_root))
        except (ValueError,OSError) as error:
            after=None;overage=True;staging_error=str(error)[:512]
        write_json(self.root/'execution'/'host'/(name+'-staging.json'),dict(before=before,peak_observed=peak,after=after,refused=overage,error=staging_error,owned_child_exit=rc,owned_child_reaped=True,monitored_not_kernel_quota=True))
        require(not overage and rc in ok, 'bounded command failed: '+argv[0])
        directory=self.root/'execution'/'host'/'unsealed-diagnostics'
        status=load_json(directory/(name+'.status.json'))
        require(status['state']=='returned' and status['exit'] in ok and all(not x['truncated'] for x in status['streams'].values()), 'incomplete/truncated command')
        return read_file(directory/(name+'.stdout'),131072)


def docker(args):
    return ['/usr/bin/docker','--host','unix:///var/run/docker.sock']+args


def host_observation(run, root):
    require(platform.system()=='Linux' and platform.machine()=='x86_64', 'native Linux/x64 required')
    socket=Path('/var/run/docker.sock'); st=socket.stat()
    require(stat.S_ISSOCK(st.st_mode) and st.st_uid==0, 'local root-owned Docker socket required')
    info=json.loads(run(docker(['info','--format','{{json .}}'])),object_pairs_hook=unique_object)
    version=json.loads(run(docker(['version','--format','{{json .}}'])),object_pairs_hook=unique_object)
    require(info['OSType']=='linux' and info['Architecture'] in ('x86_64','amd64'), 'native daemon architecture')
    # Fixed candidate locations only. Missing identity is a refusal, never TOFU.
    candidates=[Path('/usr/libexec/docker/docker-init'),Path('/usr/lib/docker/docker-init'),Path('/usr/bin/docker-init')]
    init=[path for path in candidates if path.is_file() and not path.is_symlink()]
    require(len(init)==1, 'init binary candidate unavailable/ambiguous')
    init=init[0]; init_version=run([str(init),'--version']).decode().strip()
    mem={}
    with open('/proc/meminfo','rb') as memfile:
        memdata=memfile.read(65537)
    require(len(memdata)<=65536,'memory observation bound')
    for line in memdata.decode().splitlines():
        match=re.fullmatch(r'(MemTotal|MemAvailable):\s+([0-9]+) kB',line)
        if match: mem[match[1]]=int(match[2])*1024
    require(set(mem)=={'MemTotal','MemAvailable'}, 'memory observations missing')
    endpoint=dict(socket_device=st.st_dev,socket_inode=st.st_ino,socket_uid=st.st_uid,daemon_id=info['ID'],docker_root=info['DockerRootDir'],endpoint='unix:///var/run/docker.sock')
    identity=dict(runner_image=os.environ.get('ImageOS',''),runner_version=os.environ.get('ImageVersion',''),client_version=version['Client']['Version'],server_version=version['Server']['Version'],architecture=info['Architecture'],storage_driver=info['Driver'],cgroup_driver=info['CgroupDriver'],cgroup_version=info['CgroupVersion'],init_path=str(init),init_sha256=sha(init,8*MIB),init_version=init_version)
    disks={}
    for path in (root,run.carrier_root,Path(info['DockerRootDir'])):
        require(path.is_dir() and path.is_absolute() and not path.is_symlink(), 'disk path')
        s=path.stat(); v=os.statvfs(path)
        disks[str(path)]=dict(device=s.st_dev,free=v.f_bavail*v.f_frsize)
    running=run(docker(['ps','--no-trunc','--format','{{.ID}}'])).decode().splitlines()
    return dict(identity=identity,endpoint=endpoint,host_cpu=os.cpu_count(),daemon_cpu=info['NCPU'],host_memory=mem['MemTotal'],available_memory=mem['MemAvailable'],daemon_memory=info['MemTotal'],memory_limit=info.get('MemoryLimit'),swap_limit=info.get('SwapLimit'),disks=disks,running=running,kernel=platform.release())


def admit_host(observed,p,precreate=False):
    require(observed['identity'] in p['admitted_host_tuples'], 'Docker/init tuple not admitted')
    r=p['resources']
    require(min(observed['host_cpu'],observed['daemon_cpu'])>=r['host_cpu_min'], 'CPU capacity')
    require(min(observed['host_memory'],observed['daemon_memory'])>=r['host_memory_min'] and observed['available_memory']>=r['available_memory_min'], 'memory capacity')
    require(observed['memory_limit'] is True and observed['swap_limit'] is True and observed['identity']['cgroup_version']=='2', 'cgroup v2 limit support')
    floor=r['precreate_disk_min'] if precreate else r['prepull_disk_min']
    require(all(x['free']>=floor for x in observed['disks'].values()), 'disk capacity')
    require(not observed['running'], 'unrelated running container')


def prepull_disks(observed,p):
    """Fresh admission at the actual pull boundary, after source preparation."""
    measured={}
    for path,before in observed['disks'].items():
        actual=Path(path).stat();space=os.statvfs(path)
        free=space.f_bavail*space.f_frsize
        require(actual.st_dev==before['device'],'pre-pull backing device changed')
        require(free>=p['resources']['prepull_disk_min'],'pre-pull disk capacity')
        measured[path]=dict(device=actual.st_dev,free=free)
    return measured


def image_admit(image,p):
    e=p['image']
    require(isinstance(image,list) and len(image)==1, 'image count')
    x=image[0]
    require(isinstance(x,dict),'image object type')
    image_id=bounded_image_string(x['Id'],'image id')
    expected_id=bounded_image_string(e['id'],'expected image id')
    id_matches=image_id==expected_id
    require(id_matches, 'image digest/id')
    repo_digests=bounded_image_list(x['RepoDigests'],'image repo digests')
    expected_reference=bounded_image_string(e['reference'],'expected image reference')
    reference_matches=expected_reference in repo_digests
    require(reference_matches, 'image digest/id')

    actual_os=bounded_image_string(x['Os'],'image os')
    actual_architecture=bounded_image_string(x['Architecture'],'image architecture')
    actual_size=bounded_image_size(x['Size'],'image size')
    config=x['Config'];require(isinstance(config,dict),'image config type')
    actual_user=bounded_image_string(config['User'],'image user')
    actual_entrypoint=bounded_image_list(config['Entrypoint'],'image entrypoint')
    actual_workdir=bounded_image_string(config['WorkingDir'],'image working directory')
    actual=dict(os=actual_os,architecture=actual_architecture,size=actual_size,user=actual_user,entrypoint=actual_entrypoint,workdir=actual_workdir)
    expected={
        'os': bounded_image_string(e['os'],'expected image os'),
        'architecture': bounded_image_string(e['architecture'],'expected image architecture'),
        'size': bounded_image_size(e['size'],'expected image size'),
        'user': bounded_image_string(e['user'],'expected image user'),
        'entrypoint': bounded_image_list(e['entrypoint'],'expected image entrypoint'),
        'workdir': bounded_image_string(e['workdir'],'expected image working directory'),
    }
    actual_tuple=tuple(actual[name] for name in IMAGE_FIELD_NAMES)
    expected_tuple=tuple(expected[name] for name in IMAGE_FIELD_NAMES)
    if actual_tuple!=expected_tuple:
        mismatch_names=[name for name,actual_value,expected_value in zip(IMAGE_FIELD_NAMES,actual_tuple,expected_tuple) if actual_value!=expected_value]
        diagnostic=dict(actual=actual,expected=expected,mismatch_names=mismatch_names,id_matches_expected=id_matches,reference_in_repo_digests=reference_matches)
        require(len(canonical_json(diagnostic))<=IMAGE_REFUSAL_MAX_BYTES,'image diagnostic too large')
        raise ImageConfigurationRefusal(diagnostic)


def admitted_image_summary(image, p, prepared):
    # p came from profile(): exact bytes/schema, then the prepared admission binding.
    require(prepared['admission']['profile_sha256'] == PROFILE_SHA, 'image summary profile binding')
    e=p['image']
    require(set(e) == {'reference','id','os','architecture','size','user','entrypoint','workdir'}, 'image summary schema')
    image_admit(image,p)
    fields=[e['id'],e['os'],e['architecture'],str(e['size']),e['user'],json.dumps(e['entrypoint'],separators=(',',':')),e['workdir']]
    require(all('|' not in field and '\n' not in field and '\r' not in field for field in fields), 'image summary delimiter')
    summary='|'.join(fields)
    require(len(summary.encode()) <= IMAGE_REFUSAL_MAX_BYTES, 'image summary bound')
    return summary


def git(run, repo, args, seconds=20):
    return run(['/usr/bin/git','--no-replace-objects','-C',str(repo)]+args,seconds)


def verify_source(run,repo,p,bare=True):
    s=p['source']; gitdir=repo if bare else repo/'.git'
    for name in ('objects/info/alternates','info/grafts','shallow'):
        require(not (gitdir/name).exists(), 'alternate/graft/shallow refused')
    require(not git(run,repo,['for-each-ref','--format=%(refname)','refs/replace']), 'replacement refused')
    require(git(run,repo,['rev-parse',s['commit']+'^{tree}']).decode().strip()==s['tree'], 'wrong source tree')
    git(run,repo,['merge-base','--is-ancestor',s['base'],s['commit']])
    git(run,repo,['fsck','--full','--strict'],60)
    delta=git(run,repo,['diff','--name-status',s['base'],s['commit']]).decode().splitlines()
    require(delta==['M\t'+name for name in s['delta']], 'five-file source delta')
    if not bare:
        require(git(run,repo,['rev-parse','HEAD']).decode().strip()==s['commit'], 'wrong source HEAD')
        require(not git(run,repo,['status','--porcelain','--untracked-files=all']), 'dirty source/overlay')
    commits={}
    for name in ('base','commit'):
        raw=git(run,repo,['cat-file','commit',s[name]])
        require(hashlib.sha1(b'commit '+str(len(raw)).encode()+b'\0'+raw).hexdigest()==s[name], 'raw commit object mismatch')
        commits[name]=dict(oid=s[name],raw_sha256=hashlib.sha256(raw).hexdigest(),raw=raw.decode())
    return commits


def prepare_source(run,root,p):
    src=root/'input'/'source.git'; src.mkdir()
    git(run,src,['init','--bare'])
    git(run,src,['fetch','--no-tags','--no-recurse-submodules','https://github.com/housegate/housegate.git',p['source']['commit']],90)
    raw=verify_source(run,src,p)
    git(run,src,['update-ref',p['source']['ref'],p['source']['commit']])
    bundle=root/'input'/'source.bundle'
    git(run,src,['bundle','create','--version=2',str(bundle),p['source']['ref']],30)
    require(bundle.stat().st_size<=p['source']['bundle_max_bytes'], 'bundle bound')
    require(git(run,src,['bundle','list-heads',str(bundle)]).decode()==p['source']['commit']+' '+p['source']['ref']+'\n', 'advertised bundle ref')
    # A v2 complete bundle has an empty prerequisite block after the one ref.
    with bundle.open('rb') as stream:
        header=stream.read(4096).split(b'\n\n',1)[0]
    require(header==('# v2 git bundle\n'+p['source']['commit']+' '+p['source']['ref']).encode(), 'bundle history/prerequisites')
    verify=root/'input'/'verify'
    run(['/usr/bin/git','clone','--no-hardlinks','--no-checkout',str(bundle),str(verify)],30)
    git(run,verify,['checkout','--detach',p['source']['commit']],30)
    cloned=verify_source(run,verify,p,False)
    require(raw==cloned, 'clone raw history mismatch')
    files={name:hashlib.sha256(read_file(verify/name,8*MIB)).hexdigest() for name in p['source']['delta']}
    inventory(root/'input',p['resources']['staging_max'],source_links=True)
    bundle.chmod(0o444)
    return dict(source=p['source'],raw_commits=raw,tracked_file_hashes=files,bundle_bytes=bundle.stat().st_size,bundle_sha256=sha(bundle,8*MIB),bundle_name='source.bundle')


def bind_carrier(run,receipt):
    checkout=Path(os.environ['GITHUB_WORKSPACE'])
    require((checkout/'.git').is_dir() and not (checkout/'.git').is_symlink(),'carrier must have in-checkout Git history')
    receipt['staging']=staging_inventory(staging_components(run.root,checkout))
    require(git(run,checkout,['rev-parse','HEAD']).decode().strip()==receipt['head'], 'not the PR head checkout')
    require(not git(run,checkout,['status','--porcelain','--untracked-files=all']), 'carrier overlay')
    require(git(run,checkout,['rev-list','--count','HEAD']).decode().strip()=='1','carrier must contain exactly one reachable commit')
    require(not git(run,checkout,['for-each-ref','refs/replace']), 'carrier replacements')
    require(not (checkout/'.git/objects/info/alternates').exists() and not (checkout/'.git/info/grafts').exists(),'carrier alternate/graft')
    receipt['carrier_tree']=git(run,checkout,['rev-parse','HEAD^{tree}']).decode().strip()
    receipt['runtime_manifest_sha256']=manifest()
    require(receipt['runtime_manifest_sha256']==os.environ.get('CI2_NATIVE_MANIFEST_SHA256') and PROFILE_SHA==os.environ.get('CI2_PROFILE_SHA256'), 'workflow binding mismatch')
    receipt['workflow_sha256']=sha(checkout/'.github/workflows/ci2-native.yml',MIB)
    checked=load_json(run.root/'input'/'checkout.json')
    for key,expected in [('head',receipt['head']),('profile_sha256',PROFILE_SHA),('runtime_manifest_sha256',receipt['runtime_manifest_sha256']),('run_id',receipt['run_id']),('attempt',1)]:
        require(checked[key]==expected,'checkout receipt binding')
    require(checked['tree']==receipt['carrier_tree'],'checkout tree binding')
    receipt['checkout']=checked


def carrier_checkout(run,root,head):
    """One exact-head depth-one sparse checkout under existing command owners."""
    checkout=run.carrier_root
    require(checkout.is_dir() and not checkout.is_symlink() and not any(checkout.iterdir()),'checkout destination must be empty')
    staging_inventory(staging_components(root,checkout))
    git(run,checkout,['init','--template='])
    git(run,checkout,['config','core.sparseCheckout','true'])
    sparse=checkout/'.git/info/sparse-checkout';sparse.parent.mkdir(exist_ok=True)
    with sparse.open('x') as stream:
        stream.write('/.github/ci2/native-v1/\n/.github/workflows/ci2-native.yml\n')
    git(run,checkout,['remote','add','origin','https://github.com/housegate/housegate.git'])
    git(run,checkout,['-c','protocol.version=2','fetch','--depth=1','--filter=blob:none','--no-tags','--no-recurse-submodules','origin',head],40)
    git(run,checkout,['-c','protocol.version=2','checkout','--detach',head],30)
    require(git(run,checkout,['rev-parse','HEAD']).decode().strip()==head,'checkout wrong head')
    require(git(run,checkout,['rev-list','--count','HEAD']).decode().strip()=='1','checkout history depth')
    require(not git(run,checkout,['status','--porcelain','--untracked-files=all']),'checkout overlay')
    tree=git(run,checkout,['rev-parse','HEAD^{tree}']).decode().strip()
    require(re.fullmatch('[0-9a-f]{40}',tree),'checkout tree')
    require(manifest(checkout/'.github/ci2/native-v1')==os.environ['CI2_NATIVE_MANIFEST_SHA256'],'checked-out runtime binding')
    require(sha(checkout/'.github/ci2/native-v1/profile.json',MIB)==PROFILE_SHA,'checked-out profile binding')
    measured=staging_inventory(staging_components(root,checkout))
    initial=load_json(root/'input'/'bootstrap.json')
    require(initial['head']==head and initial['run_id']==os.environ['GITHUB_RUN_ID'] and initial['attempt']==1,'initial bootstrap identity')
    receipt=dict(head=head,tree=tree,run_id=os.environ['GITHUB_RUN_ID'],attempt=1,profile_sha256=PROFILE_SHA,runtime_manifest_sha256=os.environ['CI2_NATIVE_MANIFEST_SHA256'],staging=measured,bootstrap=initial)
    write_json(root/'input'/'checkout.json',receipt)
    write_json(root/'execution'/'host'/'checkout-staging.json',receipt)


def container_admit(x,receipt,p,bundle):
    require(len(x)==1,'container count'); x=x[0]; h=x['HostConfig']; r=p['resources']
    require(h.get('Init') is True,'explicit init required')
    require(x['Image']==p['image']['id'] and x['Config']['User']=='0:0','container image/user')
    for key,value in {'NanoCpus':r['container_nano_cpus'],'Memory':r['container_memory'],'MemorySwap':r['container_memory_swap'],'PidsLimit':r['pids'],'ShmSize':r['shm']}.items():
        require(h[key]==value,'container resource: '+key)
    require(h['Privileged'] is False and h['PidMode']=='' and h['RestartPolicy']['Name']=='no','container privilege/restart')
    require(h['Tmpfs']=={'/ci2':'rw,exec,nosuid,nodev,size=6g,mode=0755','/tmp':'rw,noexec,nosuid,nodev,size=128m,mode=1777'},'tmpfs topology')
    require(h['LogConfig']=={'Type':'local','Config':{'max-size':'20m','max-file':'2'}},'log rotation')
    mounts=x['Mounts']
    binds=[m for m in mounts if m['Type']=='bind']
    require(len(binds)==1 and binds[0]['Source']==str(bundle) and binds[0]['Destination']=='/input/source.bundle' and binds[0]['RW'] is False and binds[0]['Propagation']=='rprivate','input-only mount')
    require(all(m['Type'] in ('bind','tmpfs') for m in mounts),'unexpected volume')


def archive_validate(path,expected=None):
    require(path.stat().st_size<=1024*MIB,'archive bound')
    require(path.stat().st_size>=1024 and path.stat().st_size%512==0,'archive framing')
    with path.open('rb') as raw:
        raw.seek(-1024,2)
        require(raw.read(1024)==b'\0'*1024,'archive terminal padding')
    if expected is not None: require(sha(path)==expected,'archive digest')
    seen=set(); total=0; count=0
    with tarfile.open(path,'r:') as archive:
        for entry in archive:
            count+=1
            require(count<=MAX_FILES,'archive count')
            name=entry.name; parts=PurePosixPath(name).parts
            require(name not in seen and name==str(PurePosixPath(name)) and not name.startswith('/') and '..' not in parts and parts and parts[0] in ('execution','receipts'),'archive path/duplicate')
            require(entry.isfile() and not entry.issym() and not entry.islnk(),'archive file type')
            require(0<=entry.size<=536870912,'archive entry bound')
            total+=entry.size; require(total<=537919488,'archive payload bound')
            seen.add(name)
            stream=archive.extractfile(entry); copied=0
            while True:
                chunk=stream.read(MIB)
                if not chunk: break
                copied+=len(chunk)
            require(copied==entry.size,'archive truncated')
    require(bool(seen),'empty archive')
    return dict(files=count,bytes=total)


def cleanup_receipts(execution,owner,prepared):
    returned=load_json(execution/'host'/'owner-return.json')
    absent=load_json(execution/'host'/'container-absence.json')
    container=load_json(execution/'host'/'container-admitted.json')[0]
    require(returned['owner_token']==owner and returned['returned'] is True and returned['cleanup_proved'] is True and type(returned['exit']) is int and returned['exit'] not in (124,125,130,137,143),'owner return does not prove cleanup')
    require(re.fullmatch('[0-9a-f]{64}',absent['container_id']) and absent['container_id']==container['Id'],'absence id binding')
    name='ci2-native-ab9-'+prepared['admission']['run_id']+'-1'
    require(absent['name']==name and container['Name']=='/'+name and absent['ids']==[] and absent['names']==[],'absence name/list binding')
    require(absent['endpoint']==prepared['host']['endpoint'],'absence daemon binding')
    labels=container['Config']['Labels']
    require(labels['com.housegate.ci2.owner']==owner and labels['com.housegate.ci2.commit']==prepared['source']['commit'] and labels['com.housegate.ci2.run']==prepared['admission']['run_id']+'-1' and labels['com.housegate.ci2.task']=='release-qualification-v16','terminal ownership labels')
    return returned


def main():
    mode=sys.argv[2]; p=profile(); manifest()
    if mode=='internal-checkout':
        head=os.environ['CI2_CARRIER_HEAD']
        require(re.fullmatch('[0-9a-f]{40}',head),'checkout head')
        root,owner=owned_root()
        remain=min(90-(time.time()-int(os.environ['CI2_PREPARATION_STARTED'])),300-(time.time()-int(os.environ['CI2_PREPARATION_STARTED'])))
        require(remain>5,'checkout preparation reserve exhausted')
        run=Commands(root,owner,time.monotonic()+remain-5)
        carrier_checkout(run,root,head)
    elif mode in ('internal-prepare','internal-observe'):
        lane='observe' if mode=='internal-observe' else 'qualify'
        receipt=admission(load_json(os.environ['GITHUB_EVENT_PATH']),os.environ,p,lane)
        root,owner=owned_root(); run=Commands(root,owner,time.monotonic()+int(os.environ['CI2_PREPARATION_REMAINING'])-10)
        bind_carrier(run,receipt)
        write_json(root/'input'/'admission.json',receipt)
        observed=host_observation(run,root)
        write_json(root/'input'/'observation.json',dict(admission=receipt,observation=observed,qualification=False))
        if lane=='observe':
            shutil.copyfile(root/'input'/'observation.json',root/'export'/'observation.json')
            print('Read-only host observation; no qualification authority')
            return
        admit_host(observed,p)
        staging_inventory(staging_components(root,run.carrier_root),p['resources']['staging_max'])
        source=prepare_source(run,root,p)
        staging_inventory(staging_components(root,run.carrier_root),p['resources']['staging_max'])
        before_pull=prepull_disks(observed,p)
        run.disk_watch=before_pull
        run(docker(['pull',p['image']['reference']]),90)
        run.disk_watch=None
        image_admit(json.loads(run(docker(['image','inspect',p['image']['reference']]))),p)
        after=host_observation(run,root); admit_host(after,p,True)
        require(after['endpoint']==observed['endpoint'] and after['identity']==observed['identity'],'daemon drift during preparation')
        for path,before in observed['disks'].items():
            require(before['free']-after['disks'][path]['free']<=p['resources']['image_allocation_max']+p['resources']['staging_max'],'image/preparation allocation bound')
            require(before_pull[path]['free']-after['disks'][path]['free']<=p['resources']['image_allocation_max'],'image allocation bound')
        write_json(root/'execution'/'host'/'disk-preparation.json',dict(before_source=observed['disks'],before_pull=before_pull,after_pull=after['disks'],same_device_reserves_are_not_summed=True,monitored_not_kernel_quota=True))
        runtime=root/'execution'/'runtime'
        for name in sorted(RUNTIME_NAMES|{'ci2-runtime-v16.sha256','profile.json'}):
            measured=staging_inventory(staging_components(root,run.carrier_root),p['resources']['staging_max'])
            require(measured['total_bytes']+(HERE/name).stat().st_size<=p['resources']['staging_max'],'runtime copy exceeds combined staging')
            shutil.copyfile(HERE/name,runtime/name); (runtime/name).chmod(0o444)
            staging_inventory(staging_components(root,run.carrier_root),p['resources']['staging_max'])
        (runtime/'.ci2-owner').write_text(owner+'\n'); (runtime/'.ci2-owner').chmod(0o400)
        runtime.chmod(0o555)
        source.update(admission=receipt,host=after,prepared_epoch=int(time.time()))
        write_json(root/'input'/'prepared.json',source)
        source_staging=staging_inventory(staging_components(root,run.carrier_root),p['resources']['staging_max'])
        write_json(root/'execution'/'host'/'combined-staging.json',source_staging)
        inventory(root/'execution',p['resources']['host_evidence_max'])
    elif mode=='entry':
        current=admission(load_json(os.environ['GITHUB_EVENT_PATH']),os.environ,p,'qualify')
        grant=entry_grant(os.environ,current,time.time())
        root,owner=owned_root(); prepared=load_json(root/'input'/'prepared.json')
        require(current['head']==prepared['admission']['head'] and current['run_id']==prepared['admission']['run_id'],'stale preparation')
        require(0<=time.time()-prepared['prepared_epoch']<=300,'expired preparation')
        require(manifest(root/'execution'/'runtime')==prepared['admission']['runtime_manifest_sha256'],'runtime binding')
        require(manifest()==prepared['admission']['runtime_manifest_sha256']==os.environ.get('CI2_NATIVE_MANIFEST_SHA256'),'entry carrier binding')
        require(PROFILE_SHA==prepared['admission']['profile_sha256']==os.environ.get('CI2_PROFILE_SHA256'),'entry profile binding')
        require(not (root/'input'/'attempt-lease.json').exists(),'attempt already consumed')
        require(sha(root/'input'/'source.bundle',8*MIB)==prepared['bundle_sha256'],'bundle changed')
        write_json(root/'input'/'attempt-lease.json',dict(admission=current,entry_grant=grant,owner=owner,started_epoch=int(time.time())))
    elif mode=='host-admit':
        root,owner=owned_root(); prepared=load_json(root/'input'/'prepared.json')
        run=Commands(root,owner,time.monotonic()+55)
        observed=host_observation(run,root); admit_host(observed,p,True)
        require(observed['endpoint']==prepared['host']['endpoint'] and observed['identity']==prepared['host']['identity'],'daemon identity drift')
        require(sha(root/'input'/'source.bundle',8*MIB)==prepared['bundle_sha256'],'bundle changed before mount')
        summary=admitted_image_summary(json.loads(run(docker(['image','inspect',p['image']['reference']]))),p,prepared)
        observed.update(profile_sha256=PROFILE_SHA,image_summary=summary)
        write_json(root/'execution'/'host'/'native-admission.json',observed)
        print(summary)
    elif mode=='container-admit':
        root,owner=owned_root(); cid=sys.argv[3]
        require(re.fullmatch('[0-9a-f]{64}',cid),'container id')
        run=Commands(root,owner,time.monotonic()+45)
        x=json.loads(run(docker(['inspect',cid])))
        prepared=load_json(root/'input'/'prepared.json')
        container_admit(x,prepared,p,root/'input'/'source.bundle')
        write_json(root/'execution'/'host'/'container-admitted.json',x)
    elif mode in ('live-init-admit','terminal-metrics'):
        root,owner=owned_root(); cid=sys.argv[3]
        require(re.fullmatch('[0-9a-f]{64}',cid),'container id')
        run=Commands(root,owner,time.monotonic()+25)
        code="""import hashlib,json,os,pathlib
exe=pathlib.Path('/proc/1/exe')
with exe.open('rb') as f: data=f.read(8388609)
assert len(data)<=8388608
files={}
for name in ('memory.max','memory.swap.max','pids.max','cpu.max','memory.peak','memory.events'):
 p=pathlib.Path('/sys/fs/cgroup')/name
 with p.open('rb') as f: data2=f.read(4097)
 assert len(data2)<=4096
 files[name]=data2.decode().strip()
print(json.dumps(dict(executable=os.readlink(exe),sha256=hashlib.sha256(data).hexdigest(),cgroup=files)))"""
        live=json.loads(run(docker(['exec',cid,'/usr/bin/python3','-c',code])))
        host=load_json(root/'input'/'prepared.json')['host']
        require(live['sha256']==host['identity']['init_sha256'],'injected init hash mismatch')
        r=p['resources']; cg=live['cgroup']
        require(cg['memory.max']==str(r['container_memory']) and cg['memory.swap.max']=='0' and cg['pids.max']==str(r['pids']),'effective cgroup limits')
        cpu=cg['cpu.max'].split(); require(len(cpu)==2 and int(cpu[0])==3*int(cpu[1]),'effective CPU limit')
        name='live-init-cgroup.json' if mode=='live-init-admit' else 'terminal-cgroup.json'
        if mode=='terminal-metrics':
            live['docker_state']=json.loads(run(docker(['inspect','--size',cid])))
        write_json(root/'execution'/'host'/name,live)
    elif mode in ('identity-check','phase-disk'):
        root,owner=owned_root(); run=Commands(root,owner,time.monotonic()+15)
        prior=load_json(root/'input'/'prepared.json')['host']
        info=json.loads(run(docker(['info','--format','{{json .}}'])))
        st=Path('/var/run/docker.sock').stat(); endpoint=prior['endpoint']
        require((st.st_dev,st.st_ino,st.st_uid,info['ID'],info['DockerRootDir'],info['ServerVersion'],info['Driver'],info['Architecture'],info['CgroupDriver'],info['CgroupVersion'])==(endpoint['socket_device'],endpoint['socket_inode'],endpoint['socket_uid'],endpoint['daemon_id'],endpoint['docker_root'],prior['identity']['server_version'],prior['identity']['storage_driver'],prior['identity']['architecture'],prior['identity']['cgroup_driver'],prior['identity']['cgroup_version']),'daemon identity changed')
        if mode=='phase-disk':
            stage=sys.argv[3]; require(re.fullmatch('[a-z-]{1,40}',stage),'phase name')
            measured={}
            for path,before in prior['disks'].items():
                v=os.statvfs(path); free=v.f_bavail*v.f_frsize
                r=p['resources']; runtime_disk=r['layer_max']+r['docker_logs_max']+r['host_evidence_max']+r['scratch_max']
                require(Path(path).stat().st_dev==before['device'] and before['free']-free<=runtime_disk,'phase disk component/device bound')
                measured[path]=dict(device=before['device'],free=free)
            write_json(root/'execution'/'host'/('disk-'+stage+'.json'),measured)
    elif mode=='owner-return':
        root,owner=owned_root(); rc=int(sys.argv[3])
        require(0<=rc<=255,'owner exit')
        write_json(root/'execution'/'host'/'owner-return.json',dict(exit=rc,returned=True,cleanup_proved=rc not in (124,125,130,137,143),owner_token=owner))
    elif mode=='retirement':
        root,owner=owned_root(); cid,name=sys.argv[3:5]
        require(re.fullmatch('[0-9a-f]{64}',cid) and re.fullmatch('ci2-native-ab9-[0-9]+-1',name),'retirement identity')
        run=Commands(root,owner,time.monotonic()+25)
        before=load_json(root/'input'/'prepared.json')['host']
        info=json.loads(run(docker(['info','--format','{{json .}}'])))
        st=Path('/var/run/docker.sock').stat()
        require((st.st_dev,st.st_ino,st.st_uid,info['ID'],info['DockerRootDir'])==(before['endpoint']['socket_device'],before['endpoint']['socket_inode'],before['endpoint']['socket_uid'],before['endpoint']['daemon_id'],before['endpoint']['docker_root']),'retirement daemon drift')
        ids=run(docker(['ps','-aq','--no-trunc','--filter','id='+cid])).decode().splitlines()
        names=run(docker(['ps','-aq','--no-trunc','--filter','name=^/'+name+'$'])).decode().splitlines()
        require(not ids and not names,'exact container absence unproved')
        write_json(root/'execution'/'host'/'container-absence.json',dict(container_id=cid,name=name,ids=ids,names=names,endpoint=before['endpoint'],observed_epoch=int(time.time())))
    elif mode=='internal-archive':
        root,owner=owned_root(); export=root/'export'
        execution=root/'execution'; rows,total=inventory(execution)
        # Host seal follows owner return plus exact-ID retirement. Failure is explicit.
        status=os.environ.get('CI2_QUALIFICATION_OUTCOME','unknown')
        owner_return=load_json(execution/'host'/'owner-return.json') if (execution/'host'/'owner-return.json').exists() else {}
        cleanup=False; cleanup_error=''
        try:
            cleanup_receipts(execution,owner,load_json(root/'input'/'prepared.json'))
            cleanup=True
        except (ValueError,KeyError,IndexError,OSError) as error:
            cleanup_error=str(error)[:512]
        terminal=dict(outcome=status,cleanup_proved=cleanup,cleanup_error=cleanup_error,owner_return_observed=owner_return.get('returned') is True,sealed=False)
        captures=list((execution/'host'/'unsealed-diagnostics').glob('*.status.json'))
        require(len(captures)<=500,'host capture count')
        statuses=[load_json(x) for x in captures]
        terminal['host_captures_returned']=bool(statuses) and all(x.get('state')=='returned' and all(not stream['truncated'] for stream in x['streams'].values()) for x in statuses)
        terminal['container_seal_received']=any((execution/'host'/('evidence-'+result+'-complete')).is_file() for result in ('PASS','FAIL'))
        terminal['sealed']=terminal['cleanup_proved'] and terminal['owner_return_observed'] and terminal['host_captures_returned'] and terminal['container_seal_received']
        write_json(execution/'host'/'terminal.json',terminal)
        rows,total=inventory(execution)
        if terminal['sealed']:
            content=''.join(sha(execution/name)+'  '+name+'\n' for name,_ in rows)
            require(len(content.encode())<=4*MIB,'host manifest bound')
            (execution/'host'/'HOST-MANIFEST.sha256').write_text(content)
        archive=export/'ci2-native.tar'
        require(not archive.exists(),'archive overwrite refused')
        receipts=[]
        for name in ('admission.json','observation.json','prepared.json','attempt-lease.json'):
            path=root/'input'/name
            if path.exists(): receipts.append((path,'receipts/'+name))
        rows,total=inventory(execution)
        with tarfile.open(archive,'x',format=tarfile.USTAR_FORMAT) as output:
            for path,name in [(execution/name,'execution/'+name) for name,_ in rows]+receipts:
                output.add(path,arcname=name,recursive=False)
                require(archive.stat().st_size<=1024*MIB,'archive staging bound')
        digest=sha(archive); checked=archive_validate(archive,digest)
        write_json(export/'archive.json',dict(schema='ci2-native-archive-v1',sha256=digest,archive_bytes=archive.stat().st_size,inventory=checked,terminal=terminal,admission=load_json(root/'input'/'admission.json')))
        print('Artifact prepared; upload/green is not CI2 acceptance')


if __name__=='__main__':
    try: main()
    except Exception as error:
        print('Native carrier refused: '+str(error),file=sys.stderr)
        if sys.argv[2] in ('internal-checkout','internal-prepare','internal-observe'):
            try:
                try: refusal_root,_=owned_root()
                except FileNotFoundError: refusal_root,_=owned_root(True)
                except ValueError:
                    # A new path may be created, but an existing path is never adopted.
                    refusal_root,_=owned_root(True)
                export_refusal(refusal_root,sys.argv[2],error)
            except Exception:
                print('Refusal artifact unavailable; no ownership was assumed',file=sys.stderr)
        raise SystemExit(74)
PY
# END PYTHON
