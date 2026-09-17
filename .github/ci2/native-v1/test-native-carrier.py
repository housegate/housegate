#!/usr/bin/env python3
"""Finite carrier source fixtures. No Docker or Linux process proof is run here."""
import argparse
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import time
import unittest

ROOT=Path(__file__).resolve().parent
BOOT=ROOT/'run-linux-qualification-native-v1.sh'
source=BOOT.read_text().split("<<'PY'\n",1)[1].rsplit('\nPY\n',1)[0]
carrier={'__name__':'ci2_native_test_import'}
oldargv=sys.argv
try:
    sys.argv=[str(BOOT),str(ROOT)]
    exec(compile(source,str(BOOT), 'exec'),carrier)
finally:
    sys.argv=oldargv


def call(name,*args,**kwargs):
    return carrier[name](*args,**kwargs)


class CarrierTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='ci2-native-fixture-')
        self.root=Path(self.temp.name)
        self.p=json.loads((ROOT/'profile.json').read_text())

    def tearDown(self):
        self.temp.cleanup()

    def refused(self,fn,*args,**kwargs):
        with self.assertRaises((ValueError,AssertionError,KeyError,tarfile.TarError,OSError)):
            fn(*args,**kwargs)

    def admission_input(self,lane='qualify'):
        head='a'*40
        label=('ci2-ok-' if lane=='qualify' else 'ci2-env-')+head
        event={'action':'labeled','label':{'name':label},'number':7,'pull_request':{'base':{'repo':{'full_name':self.p['repository']}},'head':{'repo':{'full_name':self.p['repository']},'ref':self.p['head_branch'],'sha':head},'draft':False,'labels':[{'name':label}]}}
        env={'GITHUB_EVENT_NAME':'pull_request','GITHUB_RUN_ATTEMPT':'1','CI2_CANCELLED_AT_ENTRY':'false','GITHUB_REPOSITORY':self.p['repository'],'CI2_CARRIER_HEAD':head,'GITHUB_RUN_ID':'123','GITHUB_SHA':'b'*40}
        return event,env

    def test_manifest_and_profile_binding(self):
        self.assertEqual(call('profile'),self.p)
        digest=call('manifest')
        workflow=(ROOT.parent.parent/'workflows/ci2-native.yml').read_text()
        self.assertIn(digest,workflow)
        self.assertIn(carrier['PROFILE_SHA'],workflow)
        self.assertEqual(len(carrier['RUNTIME_NAMES']),13)
        self.assertEqual(self.p['admitted_host_tuples'],[])

    def test_initial_qualification_is_disabled(self):
        event,env=self.admission_input()
        self.refused(carrier['admission'],event,env,self.p,'qualify')

    def test_exact_event_and_lane_admission(self):
        for lane in ('qualify','observe'):
            event,env=self.admission_input(lane)
            p=copy.deepcopy(self.p);p['admitted_host_tuples']=[{'fixture':True}]
            result=call('admission',event,env,p,lane)
            self.assertEqual(result['head'],'a'*40)
            self.assertNotEqual(result['head'],result['event_merge_sha'])
            for action in ('opened','reopened','synchronize'):
                bad=copy.deepcopy(event);bad['action']=action
                self.refused(carrier['admission'],bad,env,p,lane)
            for label in ('other','ci2-ok-'+'b'*40,'ci2-env-'+'b'*40,('ci2-env-' if lane=='qualify' else 'ci2-ok-')+'a'*40):
                bad=copy.deepcopy(event);bad['label']['name']=label
                self.refused(carrier['admission'],bad,env,p,lane)
            for field,value in [('GITHUB_RUN_ATTEMPT','2'),('GITHUB_EVENT_NAME','pull_request_target'),('CI2_CARRIER_HEAD','b'*40),('CI2_CANCELLED_AT_ENTRY','true')]:
                bad=dict(env);bad[field]=value
                self.refused(carrier['admission'],event,bad,p,lane)
            for target,value in [('draft',True),('labels',[])]:
                bad=copy.deepcopy(event);bad['pull_request'][target]=value
                self.refused(carrier['admission'],bad,env,p,lane)
            bad=copy.deepcopy(event);bad['pull_request']['head']['repo']['full_name']='fork/housegate'
            self.refused(carrier['admission'],bad,env,p,lane)
            bad=copy.deepcopy(event);bad['pull_request']['head']['ref']='main'
            self.refused(carrier['admission'],bad,env,p,lane)

    def test_json_duplicate_size_and_symlink(self):
        path=self.root/'a.json';path.write_text('{"a":1,"a":2}')
        self.refused(carrier['load_json'],path)
        path.write_text('x'*1025)
        self.refused(carrier['read_file'],path,1024)
        link=self.root/'link';link.symlink_to(path)
        self.refused(carrier['read_file'],link,2048)
        path.unlink();call('write_json',path,{'a':1})
        self.refused(carrier['write_json'],path,{'a':2})

    def test_entry_grant_age_and_binding(self):
        admitted=dict(head='a'*40,profile_sha256='b'*64,run_id='123',attempt=1)
        env=dict(CI2_OWNER_ENTRY_EPOCH='1800000000',CI2_OWNER_ENTRY_HEAD='a'*40,CI2_OWNER_ENTRY_PROFILE='b'*64,CI2_OWNER_ENTRY_RUN='123',CI2_OWNER_ENTRY_ATTEMPT='1')
        for age in (0,3.25,15):
            self.assertEqual(call('entry_grant',env,admitted,1800000000+age)['age_seconds'],age)
        for now in (1799999999,1800000015.01):
            self.refused(carrier['entry_grant'],env,admitted,now)
        for stamp in ('','x','1800000000.0','+1800000000','01800000000'):
            bad=dict(env);bad['CI2_OWNER_ENTRY_EPOCH']=stamp
            self.refused(carrier['entry_grant'],bad,admitted,1800000000)
        for field in ('CI2_OWNER_ENTRY_HEAD','CI2_OWNER_ENTRY_PROFILE','CI2_OWNER_ENTRY_RUN','CI2_OWNER_ENTRY_ATTEMPT'):
            bad=dict(env);bad[field]='wrong'
            self.refused(carrier['entry_grant'],bad,admitted,1800000000)

    def test_capacity_tuple_and_disk_refusals(self):
        p=copy.deepcopy(self.p);identity={'cgroup_version':'2','architecture':'amd64','init_sha256':'a'*64};p['admitted_host_tuples']=[identity]
        observed=dict(identity=identity,host_cpu=4,daemon_cpu=4,host_memory=16000000000,daemon_memory=16000000000,available_memory=13958643712,memory_limit=True,swap_limit=True,disks={'a':{'device':1,'free':10737418240}},running=[])
        call('admit_host',observed,p)
        for key,value in [('host_cpu',3),('daemon_cpu',3),('host_memory',14999999999),('daemon_memory',14999999999),('available_memory',13958643711),('memory_limit',False),('swap_limit',False),('running',['a'*64]),('identity',{'architecture':'arm64'})]:
            bad=copy.deepcopy(observed);bad[key]=value
            self.refused(carrier['admit_host'],bad,p)
        bad=copy.deepcopy(observed);bad['disks']['a']['free']=10737418239
        self.refused(carrier['admit_host'],bad,p)
        call('admit_host',bad,p,True)
        bad['disks']['a']['free']=8589934591
        self.refused(carrier['admit_host'],bad,p,True)

    def test_image_id_digest_architecture(self):
        e=self.p['image']; good=[dict(Id=e['id'],RepoDigests=[e['reference']],Os=e['os'],Architecture=e['architecture'],Size=e['size'],Config={'User':e['user'],'Entrypoint':e['entrypoint'],'WorkingDir':e['workdir']})]
        call('image_admit',good,self.p)
        for key,value in [('Id','sha256:'+'0'*64),('RepoDigests',[]),('Architecture','arm64'),('Size',0)]:
            bad=copy.deepcopy(good);bad[0][key]=value
            self.refused(carrier['image_admit'],bad,self.p)

    def test_container_init_resources_and_mounts(self):
        r=self.p['resources'];bundle=self.root/'source.bundle'
        h=dict(Init=True,NanoCpus=r['container_nano_cpus'],Memory=r['container_memory'],MemorySwap=r['container_memory_swap'],PidsLimit=r['pids'],ShmSize=r['shm'],Privileged=False,PidMode='',RestartPolicy={'Name':'no'},Tmpfs={'/ci2':'rw,exec,nosuid,nodev,size=6g,mode=0755','/tmp':'rw,noexec,nosuid,nodev,size=128m,mode=1777'},LogConfig={'Type':'local','Config':{'max-size':'20m','max-file':'2'}})
        good=[dict(Image=self.p['image']['id'],Config={'User':'0:0'},HostConfig=h,Mounts=[dict(Type='bind',Source=str(bundle),Destination='/input/source.bundle',RW=False,Propagation='rprivate')])]
        call('container_admit',good,{},self.p,bundle)
        for value in (False,None):
            bad=copy.deepcopy(good);bad[0]['HostConfig']['Init']=value
            self.refused(carrier['container_admit'],bad,{},self.p,bundle)
        bad=copy.deepcopy(good);del bad[0]['HostConfig']['Init']
        self.refused(carrier['container_admit'],bad,{},self.p,bundle)
        for field in ('Memory','MemorySwap','NanoCpus','PidsLimit','ShmSize'):
            bad=copy.deepcopy(good);bad[0]['HostConfig'][field]+=1
            self.refused(carrier['container_admit'],bad,{},self.p,bundle)
        bad=copy.deepcopy(good);bad[0]['Mounts'].append(dict(Type='bind',Source='/var/run/docker.sock',Destination='/socket',RW=True))
        self.refused(carrier['container_admit'],bad,{},self.p,bundle)

    def make_archive(self,entries):
        path=self.root/('archive-'+str(len(list(self.root.glob('archive-*'))))+'.tar')
        with tarfile.open(path,'w',format=tarfile.USTAR_FORMAT) as output:
            for name,kind,data in entries:
                entry=tarfile.TarInfo(name);entry.type=kind;entry.size=len(data)
                if kind==tarfile.SYMTYPE:entry.linkname='../../outside'
                output.addfile(entry,io.BytesIO(data))
        return path

    def test_archive_adverse_inputs(self):
        good=self.make_archive([('execution/host/a',tarfile.REGTYPE,b'hello')])
        self.assertEqual(call('archive_validate',good)['bytes'],5)
        self.refused(carrier['archive_validate'],good,'0'*64)
        for entries in ([('../outside',tarfile.REGTYPE,b'x')],[('/absolute',tarfile.REGTYPE,b'x')],[('execution/a',tarfile.SYMTYPE,b'')],[('execution/a',tarfile.REGTYPE,b'x'),('execution/a',tarfile.REGTYPE,b'y')],[('source.bundle',tarfile.REGTYPE,b'x')]):
            self.refused(carrier['archive_validate'],self.make_archive(entries))
        cut=self.make_archive([('execution/a',tarfile.REGTYPE,b'x'*2048)])
        cut.write_bytes(cut.read_bytes()[:600])
        self.refused(carrier['archive_validate'],cut)

    def test_inventory_bounds_types(self):
        (self.root/'x').write_bytes(b'a'*20)
        self.refused(carrier['inventory'],self.root,19)
        (self.root/'link').symlink_to('x')
        self.refused(carrier['inventory'],self.root)
        call('inventory',self.root,100,source_links=True)

    def test_transfer_parser_and_actual_framed_download(self):
        module={'__name__':'ci2_transfer_fixture'}
        path=ROOT/'ci2-transfer-v16.py'
        exec(compile(path.read_text(),str(path),'exec'),module)
        owner='a'*64
        files={'MANIFEST.meta':('owner_token='+owner+'\nresult=PASS\n').encode(),'logs/a.log':b'complete log\n'}
        files['MANIFEST.sha256']=''.join(hashlib.sha256(data).hexdigest()+'  ./'+name+'\n' for name,data in sorted(files.items())).encode()
        entries=[dict(path=name,kind='file',size=len(data),sha256=hashlib.sha256(data).hexdigest()) for name,data in sorted(files.items())]
        wire=module['header'](entries)+b''.join(files[x['path']] for x in entries)
        for name,wire_data,good in [('good',wire,True),('trailing',wire+b'x',False),('truncated',wire[:-1],False),('digest',wire[:-1]+b'x',False)]:
            parent=self.root/name;parent.mkdir();(parent/'.ci2-owner').write_text(owner+'\n')
            receiver=module['Download'](parent,owner,sum(len(x) for x in files.values()),hashlib.sha256(files['MANIFEST.sha256']).hexdigest(),'PASS',10000)
            def receive():
                for i in range(0,len(wire_data),17):receiver.feed(wire_data[i:i+17])
                receiver.finish()
            try:
                if good:receive();self.assertEqual((parent/'payload'/'logs/a.log').read_bytes(),files['logs/a.log'])
                else:self.refused(receive)
            finally:receiver.close()
        for edit in ('path','kind','duplicate','size'):
            bad=copy.deepcopy(entries)
            if edit=='path':bad[0]['path']='../escape'
            elif edit=='kind':bad[0]['kind']='symlink'
            elif edit=='duplicate':bad.append(bad[0])
            else:bad[0]['size']=524288001
            self.refused(module['inventory'],bad)

    def test_terminal_cleanup_binding(self):
        owner='a'*64;cid='b'*64;host=self.root/'host';host.mkdir()
        endpoint={'daemon_id':'fixture'}
        prepared=dict(admission={'run_id':'123'},host={'endpoint':endpoint},source={'commit':'c'*40})
        returned=dict(owner_token=owner,returned=True,cleanup_proved=True,exit=0)
        absent=dict(container_id=cid,name='ci2-native-ab9-123-1',ids=[],names=[],endpoint=endpoint)
        container=[dict(Id=cid,Name='/ci2-native-ab9-123-1',Config={'Labels':{'com.housegate.ci2.owner':owner,'com.housegate.ci2.commit':'c'*40,'com.housegate.ci2.run':'123-1','com.housegate.ci2.task':'release-qualification-v16'}})]
        for name,data in [('owner-return',returned),('container-absence',absent),('container-admitted',container)]:
            (host/(name+'.json')).write_text(json.dumps(data))
        call('cleanup_receipts',self.root,owner,prepared)
        for key,value in [('container_id','d'*64),('name','other'),('ids',[cid]),('names',[cid]),('endpoint',{'daemon_id':'changed'})]:
            bad=copy.deepcopy(absent);bad[key]=value;(host/'container-absence.json').write_text(json.dumps(bad))
            self.refused(carrier['cleanup_receipts'],self.root,owner,prepared)
        (host/'container-absence.json').write_text(json.dumps(absent))
        for key,value in [('exit',124),('returned',False),('owner_token','d'*64),('cleanup_proved',False)]:
            bad=copy.deepcopy(returned);bad[key]=value;(host/'owner-return.json').write_text(json.dumps(bad))
            self.refused(carrier['cleanup_receipts'],self.root,owner,prepared)

    def test_frozen_phase_clocks_and_single_finalizer(self):
        worker=(ROOT/'run-linux-qualification-native-v1-worker.sh').read_text()
        for name,seconds in zip(('setup','homebrew','ci-build','ci-test-ffi','release-linux','release-darwin'),self.p['clocks']['phases']):
            self.assertIn('run_phase '+name+' '+str(seconds)+' '+name,worker)
        self.assertIn('FINALIZER_SELECTED == 0',worker)
        self.assertEqual(worker.count('FINALIZER_SELECTED=1'),2)
        self.assertNotIn('HOST-MANIFEST.sha256',worker)
        self.assertIn('retirement "$CONTAINER_ID" "$CONTAINER"',worker)
        self.assertIn('--init --cpus 3 --memory 12g --memory-swap 12g',worker)
        self.assertNotIn('docker cp',worker.replace('# docker cp',''))
        self.assertEqual(sum(self.p['clocks']['phases']),2820)
        self.assertEqual(self.p['clocks']['owner'],3300)

    def test_real_git_history_and_failure_cases(self):
        # Real finite Git fixture, exercising the production source verifier.
        owner='a'*64
        for path in (self.root,self.root/'input',self.root/'execution',self.root/'execution'/'host',self.root/'docker-config'):
            path.mkdir(exist_ok=True);(path/'.ci2-owner').write_text(owner+'\n')
        run=carrier['Commands'](self.root,owner,time.monotonic()+70)
        repo=self.root/'input'/'repo';repo.mkdir()
        def git(args): return call('git',run,repo,args).decode().strip()
        git(['init'])
        for name in self.p['source']['delta']:
            path=repo/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_text('base\n')
        git(['add','.']);git(['-c','user.name=CI2 Fixture','-c','user.email=fixture@example.invalid','commit','-m','base'])
        base=git(['rev-parse','HEAD'])
        for name in self.p['source']['delta']:(repo/name).write_text('head\n')
        git(['add','.']);git(['-c','user.name=CI2 Fixture','-c','user.email=fixture@example.invalid','commit','-m','head'])
        head=git(['rev-parse','HEAD']);tree=git(['rev-parse','HEAD^{tree}'])
        p=copy.deepcopy(self.p);p['source'].update(base=base,commit=head,tree=tree)
        records=call('verify_source',run,repo,p,False)
        self.assertEqual(records['commit']['oid'],head)
        def local_fetch(argv,seconds=20,ok=(0,)):
            # Exercise the actual packing/clone verifier with a finite local
            # Git fixture instead of a network fetch of production history.
            argv=list(argv)
            if 'https://github.com/housegate/housegate.git' in argv:
                argv[argv.index('https://github.com/housegate/housegate.git')]=str(repo)
            return run(argv,seconds,ok)
        packed=call('prepare_source',local_fetch,self.root,p)
        self.assertLessEqual(packed['bundle_bytes'],8388608)
        self.assertNotEqual(packed['bundle_sha256'],p['source']['historical_bundle_sha256'])
        self.assertEqual(packed['bundle_sha256'],call('sha',self.root/'input'/'source.bundle'))
        self.assertEqual(packed['raw_commits'],records)
        bad=copy.deepcopy(p);bad['source']['tree']='0'*40
        self.refused(carrier['verify_source'],run,repo,bad,False)
        bad=copy.deepcopy(p);bad['source']['base']='0'*40
        self.refused(carrier['verify_source'],run,repo,bad,False)
        (repo/'extra').write_text('overlay')
        self.refused(carrier['verify_source'],run,repo,p,False);(repo/'extra').unlink()
        for name in ('shallow','info/grafts','objects/info/alternates'):
            path=repo/'.git'/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_text('fixture')
            self.refused(carrier['verify_source'],run,repo,p,False);path.unlink()
        git(['update-ref','refs/replace/'+head,base])
        self.refused(carrier['verify_source'],run,repo,p,False)

    def test_native_fixture_authority_is_pending(self):
        for name in ('init_adoption','github_cancellation'):
            with self.assertRaises(SystemExit) as result:
                native_fixture_entry(name)
            self.assertIn('PENDING',str(result.exception))


def native_fixture_entry(name):
    p=call('profile')
    if not p['native_fixture_authority'][name]:
        raise SystemExit('PENDING: separately authorized native '+name+' fixture required; no execution')
    admitted=call('admission',call('load_json',os.environ['GITHUB_EVENT_PATH']),os.environ,p,'qualify')
    call('require',os.environ.get('CI2_NATIVE_FIXTURE_AUTHORITY')==name+':'+admitted['head']+':'+admitted['profile_sha256'],'separate exact fixture authority required')
    root,owner=call('owned_root')
    prepared=call('load_json',root/'input'/'prepared.json')
    call('require',prepared['admission']['head']==admitted['head'] and prepared['admission']['run_id']==admitted['run_id'],'fixture preparation binding')
    call('write_json',root/'input'/'native-fixture-lease.json',dict(name=name,admission=admitted,qualification=False))
    run=carrier['Commands'](root,owner,time.monotonic()+110)
    if name=='github_cancellation':
        # Root separately requests ordinary cancellation after the start marker.
        # The receipt alone cannot claim that a cancellation request occurred.
        call('write_json',root/'execution'/'host'/'cancellation-fixture-start.json',dict(epoch=time.time(),admission=admitted))
        run(['/usr/bin/python3','-c','import json,time; print(json.dumps(dict(started=time.time())),flush=True); time.sleep(20); print(json.dumps(dict(finished=time.time())),flush=True)'],25)
        call('write_json',root/'execution'/'host'/'cancellation-fixture-return.json',dict(epoch=time.time(),owner_child_returned=True,external_cancellation_request_proved=False,qualification=False))
        return
    # A disposable, exactly labeled init probe. No Bazel server is signalled.
    observation=call('host_observation',run,root);call('admit_host',observation,p,True)
    call('require',observation['endpoint']==prepared['host']['endpoint'],'fixture daemon drift')
    cname='ci2-init-fixture-'+admitted['run_id']+'-1'
    docker=carrier['docker']
    call('require',not run(docker(['ps','-aq','--filter','name=^/'+cname+'$'])),'fixture name collision')
    argv=['create','--pull=never','--platform','linux/amd64','--name',cname,'--init','--user','0:0','--cpus','3','--memory','12g','--memory-swap','12g','--pids-limit','1024','--shm-size','64m','--tmpfs','/ci2:rw,exec,nosuid,nodev,size=6g,mode=0755','--tmpfs','/tmp:rw,noexec,nosuid,nodev,size=128m,mode=1777','--label','com.housegate.ci2.fixture=init-adoption','--label','com.housegate.ci2.owner='+owner,'--label','com.housegate.ci2.run='+admitted['run_id'],'--log-driver','local','--log-opt','max-size=20m','--log-opt','max-file=2','--entrypoint','/usr/bin/tail',p['image']['reference'],'-f','/dev/null']
    cid=run(docker(argv),20).decode().strip()
    call('require',len(cid)==64 and all(x in '0123456789abcdef' for x in cid),'fixture full id required')
    def exact():
        x=json.loads(run(docker(['inspect',cid])))[0]
        call('require',x['Id']==cid and x['Name']=='/'+cname and x['Config']['Labels'].get('com.housegate.ci2.fixture')=='init-adoption' and x['Config']['Labels'].get('com.housegate.ci2.owner')==owner and x['Config']['Labels'].get('com.housegate.ci2.run')==admitted['run_id'],'fixture ownership lost')
        return x
    exact()
    try:
        call('require',exact()['HostConfig'].get('Init') is True,'fixture explicit init missing')
        run(docker(['start',cid]),10)
        code="""import hashlib,json,os,pathlib,time
r,w=os.pipe(); child=os.fork()
if child==0:
 os.close(r); grand=os.fork()
 if grand==0:
  os.close(w); time.sleep(2); os._exit(0)
 os.write(w,str(grand).encode()); os.close(w); os._exit(0)
os.close(w); target=int(os.read(r,32)); os.close(r)
joined,status=os.waitpid(child,0); assert joined==child and status==0
path=pathlib.Path('/proc')/str(target); start=time.monotonic(); adopted=False; absent=False
while time.monotonic()-start<10:
 try:
  record=(path/'stat').read_text(); fields=record[record.rfind(')')+2:].split()
  adopted=adopted or fields[1]=='1'
 except FileNotFoundError:
  absent=True; break
 time.sleep(.02)
with open('/proc/1/exe','rb') as f: data=f.read(8388609)
assert len(data)<=8388608 and adopted and absent
print(json.dumps(dict(child_joined=True,adopted_by_pid1=adopted,strict_target_absent=absent,target=target,init_sha256=hashlib.sha256(data).hexdigest())))"""
        proof=json.loads(run(docker(['exec',cid,'/usr/bin/python3','-c',code]),20))
        call('require',proof['init_sha256']==observation['identity']['init_sha256'],'fixture injected init mismatch')
        call('write_json',root/'execution'/'host'/'init-fixture-proof.json',dict(proof=proof,admission=admitted,qualification=False))
    finally:
        exact();run(docker(['stop','--time','10',cid]),15)
        exact();run(docker(['rm',cid]),10)
        call('require',not run(docker(['ps','-aq','--no-trunc','--filter','id='+cid])) and not run(docker(['ps','-aq','--no-trunc','--filter','name=^/'+cname+'$'])),'fixture absence unproved')
        call('write_json',root/'execution'/'host'/'init-fixture-absence.json',dict(container_id=cid,name=cname,exact_absence=True,qualification=False))


def bounded_suite():
    # Reuse the historical owner and private-pipe collector, including in CI.
    # This is a finite test launcher, not a new runtime supervisor.
    root=Path(tempfile.mkdtemp(prefix='ci2-native-checks-'))
    owner=os.urandom(32).hex()
    (root/'.ci2-owner').write_text(owner+'\n');(root/'host').mkdir()
    (root/'host'/'.ci2-owner').write_text(owner+'\n')
    argv=[sys.executable,str(ROOT/'ci2-watchdog-v16.py'),'--soft-seconds','110','--hard-seconds','115','--kill-descendant-tree','--',sys.executable,str(ROOT/'ci2-host-diagnostics-v16.py'),'capture','--output-root',str(root),'--owner',owner,'--name','suite','--',sys.executable,'-B',str(Path(__file__).resolve()),'--suite']
    call('write_json',root/'command.json',dict(argv=argv,per_stream_limit=131072,soft_seconds=110,hard_seconds=115))
    process=subprocess.Popen(argv,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    rc=process.wait()
    path=root/'host'/'unsealed-diagnostics'
    record=call('load_json',path/'suite.status.json')
    complete=record.get('state')=='returned' and all(not x['truncated'] for x in record.get('streams',{}).values())
    call('write_json',root/'result.json',dict(owner_exit=rc,owner_reaped=True,pipe_eof=complete,complete=complete))
    for name,stream in [('stdout',sys.stdout),('stderr',sys.stderr)]:
        stream.write(call('read_file',path/('suite.'+name),131072).decode())
    print('Bounded preparation evidence: '+str(root),flush=True)
    raise SystemExit(rc if complete else 125)


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--native-fixture',choices=('init_adoption','github_cancellation'))
    parser.add_argument('--suite',action='store_true',help=argparse.SUPPRESS)
    args=parser.parse_args()
    if args.native_fixture: native_fixture_entry(args.native_fixture)
    if not args.suite: bounded_suite()
    print('Local parser/source fixtures only; native init/process/cancellation proofs remain PENDING.',flush=True)
    unittest.main(argv=[sys.argv[0]],verbosity=2)
