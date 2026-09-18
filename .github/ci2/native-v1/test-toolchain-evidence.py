#!/usr/bin/env python3
"""Finite synthetic fixtures only; never execute compiler or qualify an archive."""
import importlib.util
import copy
import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('toolchain', ROOT/'ci2-toolchain-evidence-v16.py')
v = importlib.util.module_from_spec(spec)
spec.loader.exec_module(v)


class Fixture:
    """Small invented graph, observed flag grammar; not copied native evidence."""
    def __init__(self, root, mode='ci-linux', direct=False):
        self.root, self.mode = root, mode
        self.exec = root/'exec'; self.exec.mkdir()
        self.src = root/'src'; self.src.mkdir()
        self.spec = copy.deepcopy(v.MODES[mode])
        triple = self.spec[2]
        self.platform = '//:linux_amd64' if mode == 'ci-linux' else '@@rules_go+//go/toolchain:' + self.spec[1]
        self.repo = 'hermetic_cc_toolchain++toolchains+zig_config'
        self.tc = '@@' + self.repo + '//:' + triple + '_cc'
        self.cc = 'external/' + self.repo + '/tools/' + triple + '/c++'
        self.zig = 'external/' + self.repo + '/zig'
        self.sdk = 'external/rules_go++go_sdk+main___download_0_linux_amd64'
        self.builder = 'bazel-out/k8-opt-exec/bin/' + self.sdk + '/builder_reset/builder'
        self.built = 'bazel-out/k8-opt-exec-ST-test/bin/' + self.sdk + '/builder'
        self.pack = 'bazel-out/k8-opt-exec-ST-test/bin/' + self.sdk + '/pack.exe'
        self.sdk_label = '@@' + self.sdk[9:] + '//:go_sdk'
        self.reset_label = '@@' + self.sdk[9:] + '//:builder_reset'
        self.built_label = '@@' + self.sdk[9:] + '//:builder'
        self.target = '@@gazelle++go_deps+com_github_ethereum_go_ethereum//crypto/secp256k1:secp256k1'
        self.marker = 'bazel-out/k8-fastbuild/bin/secp.a.cgo'
        self.archive = 'bazel-out/k8-fastbuild/bin/secp.a'
        self.output = 'bazel-out/k8-fastbuild/bin/cmd/housegate_/housegate'
        self.pins = {}
        for token in v.SOURCE_PINS:
            data = ('fixture source only: ' + token + '\n').encode()
            if token == v.SECP_SOURCE:
                data = b'//go:build !gofuzz && cgo\npackage secp256k1\nimport "C"\n'
            self.write(token, data)
            self.pins[token] = hashlib.sha256(data).hexdigest()
        for token in (self.cc, self.zig, self.builder, self.built, self.sdk+'/bin/go'):
            self.write(token, b'FIXTURE NEVER EXECUTE\n', executable=True)
        self.aq = dict(configuration=[dict(id=1,checksum='a'*64,platformName='k8'),dict(id=2,checksum='b'*64,platformName='k8')], targets=[], actions=[], pathFragments=[], artifacts=[], depSetOfFiles=[])
        cfg = lambda ident: dict(id=ident,checksum=('a' if ident==1 else 'b')*64,fragmentOptions=[dict(name='x.PlatformOptions', options=[dict(name='platforms',value='['+self.platform+']')])])
        self.cq = dict(configurations=[cfg(1),cfg(2)],results=[])
        self.nodes = {}
        self.go_tc='@@'+self.sdk[9:]+'//:go_'+self.spec[1]+'-impl'
        self.node('//cmd:housegate','go_binary',[self.target,self.tc,self.go_tc])
        self.node(self.target,'go_library',[self.tc,self.go_tc],attrs=[self.attr('cgo','true')])
        self.node(self.go_tc,'go_toolchain',[self.reset_label,self.sdk_label],attrs=[self.attr('builder',self.reset_label,'LABEL'),self.attr('sdk',self.sdk_label,'LABEL')])
        self.node(self.tc,'cc_toolchain',[self.tc+'_config',self.tc[:-3]+'_compiler_files'],attrs=[self.attr('toolchain_config',self.tc+'_config','LABEL'),self.attr('compiler_files',self.tc[:-3]+'_compiler_files','LABEL')])
        self.node(self.tc+'_config','cc_toolchain_config',[],attrs=[self.attr('target',triple),dict(name='tool_paths',stringDictValue=[dict(key='gcc',value='tools/'+triple+'/c++')])])
        self.node(self.tc[:-3]+'_compiler_files','filegroup',['@@'+self.repo+'//:tools/'+triple+'/c++','@@'+self.repo+'//:zig'])
        self.node(self.reset_label,'non_go_reset_target',[self.built_label],cfg=2)
        self.node(self.built_label,'go_tool_binary',[self.sdk_label],cfg=2,attrs=[self.attr('sdk',self.sdk_label,'LABEL')])
        self.node(self.sdk_label,'go_sdk',[],cfg=2,attrs=[self.attr('version','1.26.3'),self.attr('goos','linux'),self.attr('goarch','amd64'),self.attr('go',self.sdk_label.replace(':go_sdk',':bin/go'),'LABEL')])
        os_name,cpu=('osx','aarch64') if mode=='release-darwin' else ('linux','x86_64')
        self.node(self.platform,'platform',[],attrs=[dict(name='constraint_values',type='LABEL_LIST',stringListValue=['@@platforms//os:'+os_name,'@@platforms//cpu:'+cpu])])
        base=[self.builder,'compilepkg','-sdk',self.sdk,'-installsuffix',self.spec[1],'-src',v.SECP_SOURCE,'-p','github.com/ethereum/go-ethereum/crypto/secp256k1','-cgo_go_srcs',self.marker,'-cflags','-D__DATE__="redacted" -I "a b"']
        env=dict(CC=self.cc,CGO_ENABLED='1',GOOS='darwin' if mode=='release-darwin' else 'linux',GOARCH='arm64' if mode=='release-darwin' else 'amd64',GOTOOLCHAIN='local')
        self.compile=self.action(self.target,'CppCompile' if direct else 'GoCompilePkg',[self.cc,'-c','x.c'] if direct else base,[self.cc,self.zig,self.builder,v.SECP_SOURCE],[self.marker,self.archive],env)
        self.link=self.action('//cmd:housegate','GoLink',[self.builder,'link','-sdk',self.sdk,'-installsuffix',self.spec[1],'-arc',self.target+'=github.com/ethereum/go-ethereum/crypto/secp256k1='+self.archive,'-o',self.output,'--','-extld',self.cc,'-extldflags','-fno-lto'],[self.cc,self.zig,self.builder,self.archive],[self.output],env)
        self.reset=self.action(self.reset_label,'ExecutableSymlink',[],[self.built],[self.builder],{},cfg=2)
        sources=sorted(p for p in self.pins if '/go/tools/builders/' in p and p.endswith('.go'))
        wrapper='external/rules_go+/go/private/rules/binary_wrapper.sh'
        self.producer=self.action(self.built_label,'GoToolchainBinaryBuild',[wrapper,self.pack,self.built,*sources],[wrapper,self.sdk+'/bin/go',*sources],[self.built,self.pack],dict(GOMAXPROCS='1',GOTOOLCHAIN='local',GO111MODULE='off',GOTELEMETRY='off',GOENV='off',GO_BINARY=self.sdk+'/bin/go',LD_FLAGS='-X main.rulesGoStdlibPrefix=@@rules_go+//stdlib:'),cfg=2)

    @staticmethod
    def attr(name,value,kind='STRING'):
        return dict(name=name,stringValue=value,type=kind)

    def write(self, token, data, executable=False):
        base=self.exec if token.startswith(('external/','bazel-out/')) else self.src
        path=base/token;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(data)
        if executable: path.chmod(0o700)

    def node(self,name,kind,children,cfg=1,attrs=None):
        def edge(child):
            ident=2 if child in (self.reset_label,self.built_label,self.sdk_label) else cfg
            return dict(label=child,configurationId=ident,configurationChecksum=('a' if ident==1 else 'b')*64)
        rule=dict(name=name,ruleClass=kind,configuredRuleInput=[edge(x) for x in children],attribute=attrs or [])
        self.cq['results'].append(dict(target=dict(rule=rule),configurationId=cfg));self.nodes[name]=rule

    def artifact(self,path):
        parent=0
        for piece in path.split('/'):
            ident=len(self.aq['pathFragments'])+1
            self.aq['pathFragments'].append(dict(id=ident,label=piece,parentId=parent));parent=ident
        ident=len(self.aq['artifacts'])+1
        self.aq['artifacts'].append(dict(id=ident,pathFragmentId=parent));return ident

    def action(self,target,mnemonic,args,inputs,outputs,env,cfg=1):
        target_id=len(self.aq['targets'])+1;self.aq['targets'].append(dict(id=target_id,label=target))
        ds=len(self.aq['depSetOfFiles'])+1;self.aq['depSetOfFiles'].append(dict(id=ds,directArtifactIds=[self.artifact(p) for p in inputs]))
        row=dict(targetId=target_id,mnemonic=mnemonic,configurationId=cfg,arguments=args,inputDepSetIds=[ds],outputIds=[self.artifact(p) for p in outputs],executionPlatform='@@platforms//host:host',environmentVariables=[dict(key=k,value=v) for k,v in env.items()],actionKey=str(target_id)*64)
        self.aq['actions'].append(row);return row

    def run(self, mutate_receipt=None, mutate_invocation=None):
        flags=self.spec[3]
        invocation=dict(target='//cmd:housegate',flags=flags,aquery=['aquery',*flags,'--include_commandline','--output=jsonproto','deps(//cmd:housegate)'],cquery=['cquery',*flags,'--consistent_labels','--output=jsonproto','--transitions=lite','--proto:include_configurations','deps(//cmd:housegate)'])
        if mutate_invocation: mutate_invocation(invocation)
        registered='@@hermetic_cc_toolchain++toolchains+zig_sdk//toolchain:'+self.spec[0]
        resolution=f'INFO: ToolchainResolution: Performing resolution of @@bazel_tools//tools/cpp:toolchain_type for target platform {self.platform}\nToolchain {registered} (resolves to {self.tc}) is compatible with target platform\nSelected {self.tc} to run on execution platform @@platforms//host:host\n'
        (self.root/'resolution').write_text(resolution)
        with patch.object(v,'SOURCE_PINS',self.pins):
            receipt=v.source_evidence(self.exec,self.root,self.src)
            if mutate_receipt: mutate_receipt(receipt)
            for name,value in [('aq',self.aq),('cq',self.cq),('invocation',invocation),('sources',receipt)]:
                (self.root/name).write_text(json.dumps(value))
            argv=['v','--mode',self.mode,'--resolution',str(self.root/'resolution'),'--aquery',str(self.root/'aq'),'--cquery',str(self.root/'cq'),'--invocation',str(self.root/'invocation'),'--sources',str(self.root/'sources'),'--exec-root',str(self.exec),'--owned-root',str(self.root),'--source-root',str(self.src),'--output',str(self.root/'result')]
            with patch.object(sys,'argv',argv): v.main()
        return json.loads((self.root/'result').read_text())


class ToolchainFixtures(unittest.TestCase):
    def fixture(self, mode='ci-linux',direct=False):
        temporary=tempfile.TemporaryDirectory(prefix='ci2-toolchain-fixture-');self.addCleanup(temporary.cleanup)
        return Fixture(Path(temporary.name).resolve(),mode,direct)

    def test_full_supported_cgo_each_mode(self):
        for mode in v.MODES:
            with self.subTest(mode=mode):
                result=self.fixture(mode).run()
                self.assertEqual(result['matching_compiler_actions'][0]['proof_type'],'rules_go_0_62_cgo_compilepkg')
                self.assertEqual(result['configured_root_linker']['proof_type'],'configured_go_linker_only')

    def test_direct_cpp_each_mode(self):
        for mode in v.MODES:
            with self.subTest(mode=mode): self.assertEqual(self.fixture(mode,True).run()['matching_compiler_actions'][0]['proof_type'],'CppCompile')

    def test_graph_refusals(self):
        mutations=[
            lambda f:f.nodes[f.target]['configuredRuleInput'].clear(),
            lambda f:f.nodes['//cmd:housegate']['configuredRuleInput'].clear(),
            lambda f:f.compile.update(configurationId=2),
            lambda f:f.compile.update(executionPlatform='@@platforms//host:other'),
            lambda f:f.nodes[f.tc]['attribute'][0].update(stringValue='@@other//:config'),
            lambda f:f.nodes[f.tc+'_config']['attribute'][0].update(stringValue='wrong'),
            lambda f:f.nodes[f.tc[:-3]+'_compiler_files']['configuredRuleInput'].clear(),
            lambda f:f.nodes[f.platform]['attribute'][0].update(stringListValue=['@@platforms//os:linux']),
            lambda f:f.nodes[f.target].update(name='@com_github_ethereum_go_ethereum//crypto/secp256k1:secp256k1'),
            lambda f:f.cq['results'].append(copy.deepcopy(f.cq['results'][0])),
            lambda f:f.nodes[f.target]['configuredRuleInput'][0].update(configurationChecksum='b'*64),
            lambda f:f.nodes[f.reset_label]['configuredRuleInput'].clear(),
        ]
        for index,mutate in enumerate(mutations):
            with self.subTest(index=index):
                f=self.fixture();mutate(f)
                with self.assertRaises(ValueError):f.run()

    def test_action_and_source_refusals(self):
        mutations=[
            lambda f:f.compile['arguments'].__setitem__(0,'external/unsupported/builder'),
            lambda f:f.compile['arguments'].__setitem__(1,'compile'),
            lambda f:f.compile['arguments'].__setitem__(f.compile['arguments'].index(v.SECP_SOURCE),'other.go'),
            lambda f:f.compile['arguments'].extend(['-tags','gofuzz']),
            lambda f:f.compile['arguments'].extend(['-cflags','--target=wrong']),
            lambda f:f.compile['arguments'].__setitem__(-1,'"--target=wrong"'),
            lambda f:f.compile['arguments'].__setitem__(-1,'@flags'),
            lambda f:f.compile['environmentVariables'].append(dict(key='CC',value=f.cc)),
            lambda f:f.compile['environmentVariables'][1].update(value='0'),
            lambda f:f.compile['environmentVariables'][2].update(value='darwin'),
            lambda f:f.compile['environmentVariables'][0].update(value='/usr/bin/cc'),
            lambda f:f.aq['depSetOfFiles'][0]['directArtifactIds'].clear(),
            lambda f:f.compile['outputIds'].clear(),
            lambda f:f.producer['arguments'].__setitem__(-1,'unsupported.go'),
            lambda f:f.aq['actions'].append(copy.deepcopy(f.producer)),
            lambda f:(f.exec/f.cc).chmod(0o600),
            lambda f:f.write(v.SECP_SOURCE,b'package no_cgo\n'),
            lambda f:(f.exec/'external/zig_sdk/lib').mkdir(parents=True),
            lambda f:f.compile.update(mnemonic='GoStdlib'),
        ]
        for index,mutate in enumerate(mutations):
            with self.subTest(index=index):
                f=self.fixture();mutate(f)
                with self.assertRaises((ValueError,OSError)):f.run()

    def test_link_refusals(self):
        mutations=[lambda f:f.aq['actions'].remove(f.link),lambda f:f.link['arguments'].extend(['-extld',f.cc]),lambda f:f.link['arguments'].__setitem__(-1,'--target=bad'),lambda f:f.link['outputIds'].clear(),lambda f:f.link.update(configurationId=2)]
        for index,mutate in enumerate(mutations):
            with self.subTest(index=index):
                f=self.fixture();mutate(f)
                with self.assertRaises(ValueError):f.run()

    def test_invocation_and_receipt_refusals(self):
        f=self.fixture()
        with self.assertRaises(ValueError):f.run(mutate_receipt=lambda r:r.update(execution_root='/wrong'))
        for mutate in (lambda r:r['cquery'].remove('--consistent_labels'),lambda r:r['cquery'].append('--consistent_labels'),lambda r:r['flags'].append('--consistent_labels')):
            with self.subTest(mutate=mutate),self.assertRaises(ValueError):self.fixture().run(mutate_invocation=mutate)

    def test_source_bounds_missing_hash_and_escape(self):
        for change in ('missing','oversized','symlink'):
            with self.subTest(change=change):
                f=self.fixture();path=f.exec/v.SECP_SOURCE
                if change=='missing':path.unlink()
                elif change=='oversized':path.write_bytes(b'x'*131073)
                else:path.unlink();path.symlink_to('/etc/hosts')
                with self.assertRaises((ValueError,OSError)):f.run()

    def test_main_spelling_and_normalized_duplicates(self):
        f=self.fixture()
        f.nodes[f.platform]['name']='@@'+f.platform
        f.run()
        f=self.fixture();duplicate=copy.deepcopy(f.cq['results'][0]);duplicate['target']['rule']['name']='@@//cmd:housegate';f.cq['results'].append(duplicate)
        with self.assertRaisesRegex(ValueError,'duplicate cquery'):f.run()

    def test_hidden_params_and_link_flags(self):
        for value in ('-param=hidden','-Wl,@response','-B/host','--target=host'):
            with self.subTest(value=value):
                f=self.fixture();f.compile['arguments'][-1]=value
                with self.assertRaises(ValueError):f.run()
        f=self.fixture();f.link['arguments'][-2:]=['-extldflags="--target=other"']
        with self.assertRaises(ValueError):f.run()
        f=self.fixture();f.link['arguments'][-2:]=['-extldflags=-static'];f.run()

    def test_receipt_truncation_and_output_immutability(self):
        f=self.fixture();(f.root/'sources.truncated').touch()
        with self.assertRaisesRegex(ValueError,'truncated'):f.run()
        f=self.fixture();f.run()
        previous=(f.root/'result').read_bytes()
        with self.assertRaises(FileExistsError):f.run()
        self.assertEqual(previous,(f.root/'result').read_bytes())

    def test_source_receipt_schema_and_each_hash(self):
        f=self.fixture()
        with self.assertRaisesRegex(ValueError,'source evidence changed'):
            f.run(mutate_receipt=lambda r:r['sources'][v.SECP_SOURCE].update(sha256='0'*64))
        for token in v.SOURCE_PINS:
            with self.subTest(token=token):
                f=self.fixture();f.write(token,b'changed')
                with self.assertRaisesRegex(ValueError,'unsupported source bytes'):f.run()

    def test_builder_provider_and_source_bindings(self):
        mutations=[lambda f:f.nodes[f.go_tc]['attribute'][0].update(stringValue='@@other//:builder'),lambda f:f.nodes[f.sdk_label]['attribute'][0].update(stringValue='1.25.0'),lambda f:f.nodes[f.target]['attribute'][0].update(stringValue='false'),lambda f:f.compile['environmentVariables'].append(dict(key='GOFLAGS',value='-tags=gofuzz')),lambda f:f.write(f.built,b'different builder',True),lambda f:f.producer['environmentVariables'].append(dict(key='GOFLAGS',value='-overlay=hidden'))]
        for index,mutate in enumerate(mutations):
            with self.subTest(index=index):
                f=self.fixture();mutate(f)
                with self.assertRaises(ValueError):f.run()

    def test_canonical_labels(self):
        self.assertEqual(v.label('@@//:linux_amd64'), '//:linux_amd64')
        self.assertEqual(v.external('@@platforms//cpu:x86_64'), ('platforms', 'cpu:x86_64'))
        for value in ('@rules_go+//:x', '@platforms//cpu:x86_64', '@@[unknown repo]//:x', '//x', ''):
            with self.subTest(value=value), self.assertRaises(ValueError):
                v.label(value)

    def test_local_platform_mode_policy(self):
        self.assertEqual(v.target_platform('//:linux_amd64', v.MODES['ci-linux']), '//:linux_amd64')
        for mode, value in [('ci-linux', '//:other'), ('release-linux', '//:linux_amd64'), ('ci-linux', '@@rules_go+//go/toolchain:linux_amd64')]:
            with self.subTest(mode=mode), self.assertRaises(ValueError):
                v.target_platform(value, v.MODES[mode])

    def test_quoted_compiler_flags(self):
        self.assertEqual(v.split_quoted('-D__DATE__="redacted" -I "a b"'), ['-D__DATE__=redacted', '-I', 'a b'])
        for value in ('--target=x', '"--target=x"', '@flags', '-Wl,@flags'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                v.safe_flags(v.split_quoted(value))


if __name__ == '__main__':
    unittest.main(verbosity=2)
