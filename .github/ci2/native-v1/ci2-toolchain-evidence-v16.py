#!/usr/bin/env python3
"""Correlate Bazel 9.1 configured dependency edges with Hermetic 4.2.0 tools.

platformName is a CPU string, never a platform label. aquery and cquery IDs
are local to their own dumps; only full configuration checksums cross dumps.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import os
import re
from pathlib import Path

MODES = {
    'ci-linux': ('linux_amd64_gnu.2.31', 'linux_amd64', 'x86_64-linux-gnu.2.31', ['--config=ci']),
    'release-linux': ('linux_amd64_gnu.2.31', 'linux_amd64', 'x86_64-linux-gnu.2.31', ['--extra_toolchains=@zig_sdk//toolchain:linux_amd64_gnu.2.31', '--platforms=@rules_go//go/toolchain:linux_amd64', '--@rules_go//go/config:static=true']),
    'release-darwin': ('darwin_arm64', 'darwin_arm64', 'aarch64-macos-none', ['--extra_toolchains=@zig_sdk//toolchain:darwin_arm64', '--platforms=@rules_go//go/toolchain:darwin_arm64']),
}

# Reviewed local source authority; future owned bytes must independently match.
SECP_SOURCE = "external/gazelle++go_deps+com_github_ethereum_go_ethereum/crypto/secp256k1/secp256.go"
SOURCE_PINS = {'MODULE.bazel': '7371615e46f69d9c3773c73c91034e919b1c538ac4acca724490782778d1d3b2',
 'external/gazelle++go_deps+com_github_ethereum_go_ethereum/crypto/secp256k1/BUILD.bazel': '0eff1a4cb1ac2b652959e2de122f56bb9d3bff74381ea01c4e387a756e07a84c',
 'external/gazelle++go_deps+com_github_ethereum_go_ethereum/crypto/secp256k1/secp256.go': 'ea84abcaaae8dc04137cb110c10280e844f14c55a518388e79faed04964c9fec',
 'external/gazelle++go_deps+com_github_ethereum_go_ethereum/go.mod': '82ed3bdb0efc7392f913361c306af3cd11089fa9c9815815ee6dbcc99966fe06',
 'external/rules_go+/MODULE.bazel': '8ee616065c3d2b2f7ac0880108316ce8d0c332b3a30aad24e95c0bc124ec853e',
 'external/rules_go+/go/private/actions/compilepkg.bzl': 'b8c643d8ec597ec6d33b6409ceab98367c778910eba453470699dcf0de70f764',
 'external/rules_go+/go/private/actions/link.bzl': '61d2aa1a062eb3a1773a4e1c6858ae3ee4f13d887cf617231dee074531dca649',
 'external/rules_go+/go/private/context.bzl': 'fe49a927884b81d5db544e015cf381de3c92e88a7946bbed5afccddf04e91e9f',
 'external/rules_go+/go/private/mode.bzl': 'f326147c718a68db86f384654eeb6f9a052984b345d1d6088c40cd40c60d5a00',
 'external/rules_go+/go/private/rules/binary_wrapper.sh': '24e5f7910319b9aad22489369b8ea8d3b11b9a5745bb7dbce5ae8439c00b175d',
 'external/rules_go+/go/tools/builders/BUILD.bazel': '6f488cb09f0ef6507027f6fd41a6bb35ca9a52008409ae85cab7a8a7e633f201',
 'external/rules_go+/go/tools/builders/ar.go': 'accd1829fadb1b746a7d542da1959bc7c6a25f82abedfc74737ccd9e7b20e6ff',
 'external/rules_go+/go/tools/builders/asm.go': '8e05d2f012aa8e126a73ee4afe6ccbcc31acb332de75d163279736c76831e269',
 'external/rules_go+/go/tools/builders/builder.go': 'f45115c5dcee310e142c84ed974cd1814bc457cc0e656bc0aa252fab6c310a86',
 'external/rules_go+/go/tools/builders/cc.go': '7efa3f2dceab0e59549d836806ce4d6bdf09d23b96ad6aea3adec00057b57996',
 'external/rules_go+/go/tools/builders/cgo2.go': 'b6003da0657928d956db9b3e4a6f038f5c67c420c20b0f8791f7c12426fd3f31',
 'external/rules_go+/go/tools/builders/cgo_response.go': 'f83554be4f6987de0c7f15368bfe093e8a2f6cff411ffbae01c85de2d568114c',
 'external/rules_go+/go/tools/builders/compilepkg.go': '67ea5313835931eab40be5940c4b7602977cf8deae2efbb81a753fe32824eff9',
 'external/rules_go+/go/tools/builders/constants.go': '63669cc9caf133710b29a2a1be6b4d0cebef98a342c31a5bba4c360777ea30b4',
 'external/rules_go+/go/tools/builders/cover.go': '9c3da228f7c65aca2afb67e09db4c7e472631b72cf5c67837b89d54a65ddd99a',
 'external/rules_go+/go/tools/builders/edit.go': 'a68b1d372b90a494eb7aa45a4721c469664905cb83bd11fb2471cd5abe28e6ef',
 'external/rules_go+/go/tools/builders/embedcfg.go': '9a824b3403129836fb89747c2116d85cc4edddc4dcfac4b420d45f40482efc3c',
 'external/rules_go+/go/tools/builders/env.go': 'b06d887b352543c67689394cd297c507af75bbb5c030ac3385b2e1692cb6f59b',
 'external/rules_go+/go/tools/builders/filter.go': 'e0f5457e5aec44b710761e43ded4681bd01a188bbe14fc7595dc11f92681de20',
 'external/rules_go+/go/tools/builders/filter_buildid.go': '0048b7379675662a07b4fde7673cfcc14f0d642feb5f0a93d7a09a3444966ade',
 'external/rules_go+/go/tools/builders/flags.go': '1ee3db0027897f6c1a090d78585813e5881805e2117f7e1eec1e6f8d495828d6',
 'external/rules_go+/go/tools/builders/generate_nogo_main.go': '046d42c93b30923f6a45035fed6a36dce8d7ec640991f3d96afe164ef7f85356',
 'external/rules_go+/go/tools/builders/generate_test_main.go': '31f08229096cb30e04a59a5e3f59b4a5c29c3a1f3a3a0420a95594be27031da4',
 'external/rules_go+/go/tools/builders/importcfg.go': '4c991425c30ad662e8438b3e641132084677745b8a3e08b1cdb160ce961b4ab3',
 'external/rules_go+/go/tools/builders/link.go': '474cb6887355b8485ba54f1ab044b4d2b07d39920827891f19236b0d248c31e1',
 'external/rules_go+/go/tools/builders/nogo.go': '78cc3ae892a023dba3c75c238c528bf76e27dc04bce80a4b5f8bc69b29e276ea',
 'external/rules_go+/go/tools/builders/nogo_validation.go': 'c25426adb342641d020f89689c8d0d2a04ece834efaabbb0a0637c44975e109f',
 'external/rules_go+/go/tools/builders/path.go': '58e9c058a180451dc07f224cd91825b2934d32f03d226dfda953aa3685a25e13',
 'external/rules_go+/go/tools/builders/read.go': '586e8426c9385ec703ab87261db452b9ee50b3e01a329fe6debcd4cc32a1487d',
 'external/rules_go+/go/tools/builders/replicate.go': 'd1ebba9df7bb792bfd43683fe0a8843a241b947e82aac6da0ce220770b3eb64c',
 'external/rules_go+/go/tools/builders/stdlib.go': '68630884785d0c091a682a6ac53cd4dd4ed7aacddd6fb51b0384e62f0241f2f7',
 'external/rules_go+/go/tools/builders/stdliblist.go': '4f0afbc3c23d94ea6a25de6fb2d4aa9879c00511af303f49f51ec1544a1d0dcc',
 'go.mod': '3732c3e9151c3f738a94f191b89ed45d60b9841d394615699351a5693036af61',
 'go.sum': 'f7dba9e858ff4710343d56a1118fe5a668dd775957b2a2afbc1d7d19c765058d',
 'third_party/com_github_ethereum_go_ethereum_secp256k1.patch': '5e4de698a8e72764c44f52104b1b9fdbf5351950b5d6e95d39aed698d1b09ad6'}

def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def label(value):
    # --consistent_labels governs label-typed query fields, not string metadata.
    require(isinstance(value, str), 'label is not a string')
    if value.startswith('@@//'):
        value = value[2:]
    require(re.fullmatch(r'(?:@@[A-Za-z0-9_+.-]+)?//[^\s:@]*:[^\s:@]+', value)
            and not any(x in ('.', '..') for x in value.split('/')), 'noncanonical/malformed label: ' + value)
    return value


def external(value):
    match = re.fullmatch(r'@@([^/]+)//([^\s]+)', label(value))
    require(match is not None, 'missing canonical external label: ' + value)
    return match.groups()


def unique(items, key, description):
    result = {}
    for item in items:
        k = str(item.get(key, ''))
        require(k and k not in result, 'missing/duplicate ' + description)
        result[k] = item
    return result


def attributes(rule):
    return unique(rule.get('attribute', []), 'name', 'rule attribute')


def scalar(rule, name):
    value = attributes(rule).get(name, {}).get('stringValue')
    require(isinstance(value, str) and value, 'missing rule attribute ' + name)
    return value


def target_platform(value, spec):
    value = label(value)
    if spec[3] == ['--config=ci']:
        require(value == '//:linux_amd64', 'wrong local CI platform')
    else:
        repo, target = external(value)
        require(repo == 'rules_go+' and target == 'go/toolchain:' + spec[1], 'wrong release platform')
    return value


def contexts(text, spec):
    registered_target, platform, triple, _ = spec
    result = []
    blocks = re.split(r'(?=INFO: ToolchainResolution: Performing resolution of )', text)
    for block in blocks:
        match = re.search(r'Performing resolution of (\S+tools/cpp:toolchain_type) for target platform (\S+)', block)
        if not match:
            continue
        try:
            platform_label = target_platform(match[2], spec)
        except ValueError:
            continue
        mappings = []
        for match in re.finditer(r'Toolchain (\S+) \(resolves to (\S+)\) is compatible with target platform', block):
            registered, implementation = label(match[1]), label(match[2])
            rr, rt = external(registered)
            ir, it = external(implementation)
            if (rr.startswith('hermetic_cc_toolchain+') and rr.endswith('+zig_sdk')
                    and rt == 'toolchain:' + registered_target
                    and ir.startswith('hermetic_cc_toolchain+') and ir.endswith('+zig_config')
                    and it == ':' + triple + '_cc'):
                mappings.append((registered, implementation))
        for selected in re.finditer(r'Selected (\S+) to run on execution platform (\S+)', block):
            for registered, implementation in mappings:
                if label(selected[1]) == implementation:
                    result.append(dict(registered_label=registered, selected_label=implementation,
                                       target_platform=platform_label, execution_platform=label(selected[2]),
                                       resolution_block=block))
    require(result, 'no exact registered/selected Hermetic target context')
    return result


def platform_option(config, spec):
    values = [o.get('value') for f in config.get('fragmentOptions', [])
              if f.get('name', '').endswith('.PlatformOptions')
              for o in f.get('options', []) if o.get('name') == 'platforms']
    require(len(values) == 1 and isinstance(values[0], str), 'missing/ambiguous configured platforms option')
    value = values[0]
    # ConfigurationForOutput formats a list option via its Java toString.
    require(value.startswith('[') and value.endswith(']') and ',' not in value, 'not exactly one configured platform')
    return target_platform(value[1:-1], spec)


class ConfiguredGraph:
    def __init__(self, data):
        self.configs = unique(data.get('configurations', []), 'id', 'cquery configuration id')
        self.nodes = {}
        for record in data.get('results', []):
            rule = record.get('target', {}).get('rule')
            if not rule:
                continue
            cfg = self.configs.get(str(record.get('configurationId', '')))
            require(cfg is not None and cfg.get('checksum'), 'cquery rule has no configuration')
            checksum = cfg['checksum']
            require(isinstance(checksum, str) and re.fullmatch('[0-9a-f]{64}', checksum), 'not a full cquery checksum')
            deprecated = record.get('configuration', {}).get('checksum')
            require(deprecated in (None, checksum), 'cquery checksum disagreement')
            key = (label(rule.get('name', '')), checksum)
            for edge in rule.get('configuredRuleInput', []):
                label(edge.get('label', ''))
            for value in rule.get('ruleInput', []):
                label(value)
            for attr in rule.get('attribute', []):
                if attr.get('type') == 'LABEL' and attr.get('stringValue'):
                    label(attr['stringValue'])
                if attr.get('type') == 'LABEL_LIST':
                    for value in attr.get('stringListValue', []):
                        label(value)
            require(key not in self.nodes, 'duplicate cquery configured target')
            self.nodes[key] = (rule, cfg)
        require(self.nodes, 'missing cquery configured targets')

    def reachable(self, platform, spec):
        roots = [key for key, (_, cfg) in self.nodes.items()
                 if key[0] == '//cmd:housegate' and platform_option(cfg, spec) == platform]
        require(len(roots) == 1, 'missing/ambiguous intended configured root')
        pending, seen = list(roots), set()
        while pending:
            key = pending.pop()
            if key in seen:
                continue
            seen.add(key)
            rule, _ = self.nodes[key]
            for edge in rule.get('configuredRuleInput', []):
                child = (label(edge.get('label', '')), edge.get('configurationChecksum'))
                if child in self.nodes:
                    cfg = self.configs.get(str(edge.get('configurationId', '')), {})
                    require(cfg.get('checksum') == child[1], 'root traversal edge checksum mismatch')
                    pending.append(child)
        return roots[0], seen

    def platform(self, platform, spec):
        rows = [r for (name, _), (r, _) in self.nodes.items() if name == platform]
        require(rows and all(r.get('ruleClass') == 'platform' for r in rows), 'missing target platform rule')
        os_name, cpu = ('osx', 'aarch64') if spec[1] == 'darwin_arm64' else ('linux', 'x86_64')
        wanted = {'@@platforms//os:' + os_name, '@@platforms//cpu:' + cpu}
        for rule in rows:
            values = attributes(rule).get('constraint_values', {}).get('stringListValue', [])
            require(wanted <= {label(x) for x in values}, 'wrong platform constraints')

    def edge(self, rule, wanted):
        matches = [e for e in rule.get('configuredRuleInput', []) if label(e.get('label', '')) == label(wanted)]
        require(len(matches) == 1, 'missing/ambiguous configured dependency edge: ' + wanted)
        edge = matches[0]
        cfg = self.configs.get(str(edge.get('configurationId', '')))
        require(cfg is not None and cfg.get('checksum') == edge.get('configurationChecksum'), 'configured edge checksum mismatch')
        node = self.nodes.get((label(wanted), cfg['checksum']))
        require(node is not None, 'configured dependency node absent: ' + wanted)
        return node


def artifact_paths(data):
    fragments = unique(data.get('pathFragments', []), 'id', 'path fragment id')
    cache = {}
    def path(ident):
        ident = str(ident)
        if ident in cache:
            return cache[ident]
        pieces, visited = [], set()
        while ident and ident != '0':
            require(ident not in visited and ident in fragments, 'cyclic/missing path fragment')
            visited.add(ident)
            fragment = fragments[ident]
            piece = fragment.get('label', '')
            require(piece and '/' not in piece and piece not in ('.', '..'), 'unsafe artifact path fragment')
            pieces.append(piece)
            ident = str(fragment.get('parentId', '0'))
        return '/'.join(reversed(pieces))
    return {key: path(item.get('pathFragmentId', '')) for key, item in unique(data.get('artifacts', []), 'id', 'artifact id').items()}


def input_paths(action, data, paths):
    depsets = unique(data.get('depSetOfFiles', []), 'id', 'input depset id')
    pending, seen, result = list(action.get('inputDepSetIds', [])), set(), set()
    while pending:
        key = str(pending.pop())
        if key in seen:
            continue
        require(key in depsets, 'missing action input depset')
        seen.add(key)
        item = depsets[key]
        pending.extend(item.get('transitiveDepSetIds', []))
        for ident in item.get('directArtifactIds', []):
            require(str(ident) in paths, 'missing input artifact')
            result.add(paths[str(ident)])
    return result


def executable(token, exec_root, owned_root):
    lexical = exec_root / token
    require(not Path(token).is_absolute() and '..' not in Path(token).parts, 'compiler path is not execroot relative')
    resolved = lexical.resolve(strict=True)
    resolved.relative_to(owned_root)
    require(resolved.is_file() and os.access(resolved, os.X_OK), 'compiler is not an owned executable')
    return dict(argv_token=token, lexical_execroot_path=str(lexical), resolved_path=str(resolved),
                sha256=sha(resolved), size_bytes=resolved.stat().st_size)


def split_quoted(text):
    """rules_go 0.62 flags.go splitQuoted grammar (not shell expansion)."""
    result, word, quote, escaped, quoted = [], '', '', False, False
    for char in text:
        if escaped:
            escaped = False
        elif char == '\\':
            escaped = True
            continue
        elif quote:
            if char == quote:
                quote = ''
                continue
        elif char in ('"', "'"):
            quote, quoted = char, True
            continue
        elif char.isspace():
            if quoted or word:
                result.append(word)
                word, quoted = '', False
            continue
        word += char
    require(not quote and not escaped, 'unfinished quoted compiler flags')
    if quoted or word:
        result.append(word)
    return result


def safe_flags(tokens):
    for token in tokens:
        # The supported driver path needs no phase passthrough. Its operands
        # use a different grammar (e.g. -Xclang -triple), so checking only the
        # driver's target flags cannot establish the selected target.
        require(not token.startswith(('-Wp,', '-Wa,')) and not any(
            part.startswith(('-X', '-cc1')) or part == '-mllvm' or part.startswith('-mllvm=')
            for part in token.split(',')), 'unsupported compiler phase passthrough')
        require(not any(x.startswith('@') for x in token.split(',')), 'unexpanded compiler response file')
        require(not re.search(r'(^|,)(?:--?target(?:=|$)|-arch(?:=|$)|-m32$|-m64$|-march(?:=|$)|-mcpu(?:=|$)|-B|--gcc-toolchain(?:=|$)|--sysroot(?:=|$)|-isysroot(?:=|$)|-extld(?:=|$))', token), 'unexpected compiler target/tool override')
        require('local_config_cc' not in token and 'external/zig_sdk/' not in token, 'forbidden host/legacy tool')


def builder_flags(args, verb):
    require(len(args) > 2 and args[1] == verb, 'unsupported builder verb')
    require(not any(x.startswith('-param=') for x in args), 'unexpanded builder response file')
    common = {'sdk', 'goroot', 'installsuffix', 'tags'}
    compile_flags = {'src','cover','embedsrc','embedlookupdir','embedroot','arc','importpath','p','gcflags','asmflags','cppflags','cflags','cxxflags','objcflags','objcxxflags','ldflags','package_list','pack','cover_mode','lo','o','cgoexport','cgo_go_srcs','testfilter','cover_format','recompile_internal_deps','pgoprofile'}
    link_flags = {'main','p','o','arc','package_list','buildmode','X','stamp'}
    repeat = {'src','cover','embedsrc','embedlookupdir','embedroot','arc','recompile_internal_deps','X','stamp'}
    flags, index = {}, 2
    while index < len(args) and args[index] != '--':
        token = args[index]
        require(token.startswith('-') and not token.startswith('--'), 'unsupported builder flag')
        pair = token[1:].split('=', 1)
        name = pair[0]
        require(name in common | (compile_flags if verb == 'compilepkg' else link_flags), 'unknown builder flag: ' + name)
        if len(pair) == 2:
            value = pair[1]
        else:
            index += 1
            require(index < len(args), 'missing builder flag value')
            value = args[index]
        require(name not in flags or name in repeat, 'duplicate builder selector: ' + name)
        require(not value.startswith('@') or name == 'arc', 'builder response indirection')
        flags.setdefault(name, []).append(value)
        index += 1
    tail = args[index + 1:] if index < len(args) else []
    require(verb == 'link' or not tail and index == len(args), 'unexpected compilepkg tool tail')
    for name in ('cppflags','cflags','cxxflags','objcflags','objcxxflags','ldflags','gcflags','asmflags'):
        for value in flags.get(name, []):
            safe_flags(split_quoted(value))
    return flags, tail


def one(flags, name):
    values = flags.get(name, [])
    require(len(values) == 1, 'missing/ambiguous builder ' + name)
    return values[0]


def linker_flags(tokens):
    flags, index = {}, 0
    while index < len(tokens):
        token = tokens[index]
        require(token.startswith('-') and not token.startswith('--'), 'unsupported linker argument')
        pair = token[1:].split('=', 1)
        name = pair[0]
        require(name in {'extar','extld','extldflags','buildid','s','w','linkmode','buildmode'} and name not in flags, 'unknown/duplicate linker selector')
        if len(pair) == 2:
            value = pair[1]
        elif name in ('s','w'):
            value = 'true'
        else:
            index += 1
            require(index < len(tokens), 'missing linker flag value')
            value = tokens[index]
        flags[name] = [value]
        if name != 'extld':
            safe_flags(split_quoted(value) if name == 'extldflags' else [value])
        index += 1
    return flags


def source_evidence(exec_root, owned_root, source_root):
    """Finite explicit source set only; no repository walk or compiler execution."""
    rows, total = {}, 0
    for token, expected in SOURCE_PINS.items():
        path = (exec_root if token.startswith("external/") else source_root) / token
        resolved = path.resolve(strict=True)
        resolved.relative_to(owned_root)
        require(resolved.is_file() and resolved.stat().st_size <= 131072, 'source witness size/type')
        with resolved.open('rb') as stream:
            data = stream.read(131073)
        total += len(data)
        require(len(data) <= 131072 and total <= 1048576, 'source witness collection bound')
        require(hashlib.sha256(data).hexdigest() == expected, 'unsupported source bytes: ' + token)
        rows[token] = dict(lexical_path=str(path), resolved_path=str(resolved), size_bytes=len(data), sha256=expected, utf8=data.decode('utf-8'))
    return dict(schema='rules_go_0_62_cgo_sources_v1', execution_root=str(exec_root), owned_root=str(owned_root), source_root=str(source_root), sources=rows)


def outputs(action, paths):
    return {paths[str(i)] for i in action.get('outputIds', [])}


def builder_identity(action, aq, graph, paths, flags, exec_root, owned_root):
    token = action['arguments'][0]
    sdk = one(flags, 'sdk')
    require(sdk == 'external/rules_go++go_sdk+main___download_0_linux_amd64', 'unsupported builder SDK')
    require(re.fullmatch(r'bazel-out/[^/]+/bin/' + re.escape(sdk) + r'/builder_reset/builder', token), 'unsupported builder executable')
    require(token in input_paths(action, aq, paths), 'builder absent from action inputs')
    candidates = [a for a in aq['actions'] if token in outputs(a, paths)]
    require(len(candidates) == 1 and candidates[0]['mnemonic'] == 'ExecutableSymlink', 'missing/ambiguous builder reset producer')
    reset = candidates[0]
    upstream = input_paths(reset, aq, paths)
    require(len(upstream) == 1, 'ambiguous builder reset input')
    built = next(iter(upstream))
    producers = [a for a in aq['actions'] if built in outputs(a, paths)]
    require(len(producers) == 1 and producers[0]['mnemonic'] == 'GoToolchainBinaryBuild', 'unsupported builder producer')
    producer = producers[0]
    targets = unique(aq['targets'], 'id', 'target')
    configs = unique(aq['configuration'], 'id', 'configuration')
    builder_nodes = []
    for item, suffix in ((reset, ':builder_reset'), (producer, ':builder')):
        name = label(targets[str(item['targetId'])]['label'])
        require(name == '@@' + sdk[len('external/'):] + '//' + suffix, 'wrong builder target')
        checksum = configs[str(item['configurationId'])]['checksum']
        require(re.fullmatch('[0-9a-f]{64}', checksum) and (name, checksum) in graph.nodes, 'missing builder configured identity')
        builder_nodes.append((name, checksum))
        require(label(item['executionPlatform']) == label(action['executionPlatform']), 'builder execution platform mismatch')
    reset_rule, _ = graph.nodes[builder_nodes[0]]
    built_rule, built_cfg = graph.edge(reset_rule, builder_nodes[1][0])
    require(built_cfg['checksum'] == builder_nodes[1][1], 'builder reset configuration disagreement')
    sdk_label = '@@' + sdk[len('external/'):] + '//:go_sdk'
    require(label(scalar(built_rule, 'sdk')) == sdk_label, 'builder SDK provider disagreement')
    sdk_rule, _ = graph.edge(built_rule, sdk_label)
    require(sdk_rule.get('ruleClass') == 'go_sdk', 'unsupported builder SDK provider')
    require(scalar(sdk_rule, 'version') == '1.26.3' and scalar(sdk_rule, 'goos') == 'linux'
            and scalar(sdk_rule, 'goarch') == 'amd64'
            and label(scalar(sdk_rule, 'go')) == sdk_label.replace(':go_sdk', ':bin/go'), 'unsupported SDK version/host executable')
    action_name = label(targets[str(action['targetId'])]['label'])
    action_checksum = configs[str(action['configurationId'])]['checksum']
    action_rule, _ = graph.nodes[(action_name, action_checksum)]
    go_platform = one(flags, 'installsuffix')
    go_toolchain, _ = graph.edge(action_rule, '@@' + sdk[len('external/'):] + '//:go_' + go_platform + '-impl')
    require(go_toolchain.get('ruleClass') == 'go_toolchain' and label(scalar(go_toolchain, 'builder')) == builder_nodes[0][0]
            and label(scalar(go_toolchain, 'sdk')) == sdk_label, 'selected Go toolchain builder/SDK mismatch')
    _, selected_builder_cfg = graph.edge(go_toolchain, builder_nodes[0][0])
    require(selected_builder_cfg['checksum'] == builder_nodes[0][1], 'selected builder configuration mismatch')
    producer_inputs = input_paths(producer, aq, paths)
    argv = producer.get('arguments', [])
    source_tokens = {p for p in SOURCE_PINS if p.startswith('external/rules_go+/go/tools/builders/') and p.endswith('.go')}
    require(len(argv) == len(source_tokens) + 3 and set(argv[3:]) == source_tokens and argv[2] == built
            and argv[0] == 'external/rules_go+/go/private/rules/binary_wrapper.sh', 'unsupported builder source argv')
    require(source_tokens | {argv[0], sdk + '/bin/go'} <= producer_inputs, 'builder source/SDK action inputs missing')
    env = unique(producer.get('environmentVariables', []), 'key', 'builder environment')
    expected_env = dict(GOMAXPROCS='1', GOTOOLCHAIN='local', GO111MODULE='off', GOTELEMETRY='off', GOENV='off',
                        GO_BINARY=sdk + '/bin/go', LD_FLAGS='-X main.rulesGoStdlibPrefix=@@rules_go+//stdlib:')
    require({k: item.get('value', '') for k, item in env.items()} == expected_env, 'unsupported builder generation environment')
    identity = executable(token, exec_root, owned_root)
    require(identity['sha256'] == executable(built, exec_root, owned_root)['sha256'], 'reset builder differs from built executable')
    return dict(executable=identity, sdk_go=executable(sdk + '/bin/go', exec_root, owned_root), producer=producer, reset=reset)


def cgo_proof(action, aq, graph, paths, flags, compiler_token, spec, inputs, exec_root, owned_root):
    env = unique(action.get('environmentVariables', []), 'key', 'action environment')
    require(set(env) <= {'CC','CGO_ENABLED','GOOS','GOARCH','GOTOOLCHAIN','GOEXPERIMENT','GOROOT','GOROOT_FINAL','GOPATH','GODEBUG','PATH'}, 'unsupported cgo environment key')
    expected_os, expected_arch = ('darwin','arm64') if spec[1] == 'darwin_arm64' else ('linux','amd64')
    for key, value in [('CC',compiler_token),('CGO_ENABLED','1'),('GOOS',expected_os),('GOARCH',expected_arch),('GOTOOLCHAIN','local')]:
        require(env.get(key, {}).get('value') == value, 'wrong cgo environment ' + key)
    require(one(flags, 'installsuffix') == expected_os + '_' + expected_arch, 'wrong cgo install suffix')
    require(not any(key in env for key in ('CGO_CFLAGS','CGO_CPPFLAGS','CGO_CXXFLAGS','CGO_LDFLAGS','GOFLAGS','GOENV'))
            and not env.get('GOEXPERIMENT', {}).get('value'), 'unsupported compiler environment override')
    require(flags.get('tags', []) in ([], ['']), 'unsupported source build tags')
    require(flags.get('testfilter', []) in ([], ['off']) and not flags.get('cover'), 'unsupported active source filtering')
    require(flags.get('p') == ['github.com/ethereum/go-ethereum/crypto/secp256k1'], 'unsupported cgo package')
    require(flags.get('src', []).count(SECP_SOURCE) == 1 and SECP_SOURCE in inputs, 'active cgo source absent')
    require(one(flags, 'cgo_go_srcs') in outputs(action, paths), 'cgo source output marker not an output')
    return builder_identity(action, aq, graph, paths, flags, exec_root, owned_root)


def prove(action, aq, graph, paths, context, spec, exec_root, owned_root):
    _, _, triple, _ = spec
    configs = unique(aq.get('configuration', []), 'id', 'aquery configuration id')
    targets = unique(aq.get('targets', []), 'id', 'aquery target id')
    cfg = configs.get(str(action.get('configurationId', '')), {})
    checksum = cfg.get('checksum')
    require(isinstance(checksum, str) and re.fullmatch('[0-9a-f]{64}', checksum), 'missing full action configuration checksum')
    target = targets.get(str(action.get('targetId', '')), {}).get('label', '')
    node = graph.nodes.get((label(target), checksum))
    require(node is not None, 'no cquery node for action target and checksum')
    root, reachable = graph.reachable(context['target_platform'], spec)
    graph.platform(context['target_platform'], spec)
    require((label(target), checksum) in reachable, 'compile action is not a configured dependency of intended root')
    rule, configured = node
    require(platform_option(configured, spec) == context['target_platform'], 'action configuration uses another target platform')
    require(label(action.get('executionPlatform', '')) == context['execution_platform'], 'action execution platform mismatch')
    tc, tc_cfg = graph.edge(rule, context['selected_label'])
    require(tc.get('ruleClass') == 'cc_toolchain', 'selected dependency is not a cc_toolchain')
    repo, _ = external(context['selected_label'])
    config_label = '@@' + repo + '//:' + triple + '_cc_config'
    files_label = '@@' + repo + '//:' + triple + '_compiler_files'
    require(label(scalar(tc, 'toolchain_config')) == config_label, 'selected toolchain has wrong config provider')
    require(label(scalar(tc, 'compiler_files')) == files_label, 'selected toolchain has wrong compiler_files provider')
    tc_config, _ = graph.edge(tc, config_label)
    require(scalar(tc_config, 'target') == triple, 'selected provider has wrong target')
    tool_paths = attributes(tc_config).get('tool_paths', {}).get('stringDictValue', [])
    tools = unique(tool_paths, 'key', 'tool path')
    gcc = tools.get('gcc', {}).get('value')
    require(gcc == 'tools/' + triple + '/c++', 'selected gcc tool path differs from pinned Hermetic layout')
    compiler_token = 'external/' + repo + '/' + gcc
    zig_token = 'external/' + repo + '/zig'
    args = action.get('arguments', [])
    require(args, 'missing action argv')
    files, _ = graph.edge(tc, files_label)
    file_edges = {label(e.get('label', '')) for e in files.get('configuredRuleInput', [])}
    require({'@@' + repo + '//:' + gcc, '@@' + repo + '//:zig'} <= file_edges, 'compiler_files omits compiler or Zig binary')
    inputs = input_paths(action, aq, paths)
    require(not any('local_config_cc' in token for token in inputs), 'local_config_cc action input')
    require({compiler_token, zig_token} <= inputs, 'action does not consume selected compiler and Zig binary')
    require(not os.path.lexists(exec_root / 'external/zig_sdk/lib') and not any(x.startswith('external/zig_sdk/') for x in inputs), 'legacy wrapper SDK override prevents exact Zig identity')
    compiler = executable(compiler_token, exec_root, owned_root)
    zig = executable(zig_token, exec_root, owned_root)
    proof_type, builder = 'CppCompile', None
    if action['mnemonic'] == 'CppCompile':
        require(args[0] == compiler_token, 'actual compiler position is not selected provider gcc')
        safe_flags(args[1:])
    elif action['mnemonic'] == 'GoCompilePkg':
        flags, _ = builder_flags(args, 'compilepkg')
        require(label(target) == '@@gazelle++go_deps+com_github_ethereum_go_ethereum//crypto/secp256k1:secp256k1', 'unsupported cgo witness target')
        require(scalar(rule, 'cgo') == 'true', 'cgo rule disabled')
        builder = cgo_proof(action, aq, graph, paths, flags, compiler_token, spec, inputs, exec_root, owned_root)
        proof_type = 'rules_go_0_62_cgo_compilepkg'
    elif action['mnemonic'] == 'GoLink':
        require((label(target), checksum) == root, 'GoLink is not exact configured root')
        flags, tail = builder_flags(args, 'link')
        env = unique(action.get('environmentVariables', []), 'key', 'link environment')
        goos, goarch = ('darwin','arm64') if spec[1] == 'darwin_arm64' else ('linux','amd64')
        require(one(flags, 'installsuffix') == goos + '_' + goarch and env.get('GOOS', {}).get('value') == goos
                and env.get('GOARCH', {}).get('value') == goarch and env.get('CGO_ENABLED', {}).get('value') == '1', 'wrong configured linker platform/cgo')
        link_flags = linker_flags(tail)
        require(one(link_flags, 'extld') == compiler_token, 'wrong configured extld')
        require(link_flags.get('linkmode', []) in ([], ['external'], ['auto']), 'unsupported configured linkmode')
        output = one(flags, 'o')
        require(output in outputs(action, paths) and re.fullmatch(r'bazel-out/[^/]+/bin/cmd/housegate_/housegate', output), 'wrong GoLink root output')
        for arc in flags.get('arc', []):
            parts = arc.split('=')
            require(len(parts) == 3 and parts[2] in inputs, 'invalid link archive metadata/input')
            label(parts[0])
        builder = builder_identity(action, aq, graph, paths, flags, exec_root, owned_root)
        proof_type = 'configured_go_linker_only'
    else:
        raise ValueError('unsupported compiler proof type')
    return dict(root_target=root[0], root_configuration_checksum=root[1], target_label=target, configuration_checksum=checksum, configuration_cpu=cfg.get('platformName'),
                configuration_id=action['configurationId'], target_platform=context['target_platform'],
                execution_platform=action['executionPlatform'], selected_label=context['selected_label'],
                selected_configuration_checksum=tc_cfg['checksum'], registered_label=context['registered_label'],
                config_provider_label=config_label, compiler_files_label=files_label,
                compiler_target=triple, compiler_argv_position=0 if proof_type == 'CppCompile' else None, compiler=compiler, zig=zig,
                proof_type=proof_type, builder=builder, action_inputs=sorted(inputs), environment=action.get('environmentVariables', []),
                action_key=action.get('actionKey'), arguments=args)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--collect-sources', action='store_true')
    parser.add_argument('--mode', choices=MODES)
    for option in ('resolution', 'aquery', 'cquery', 'invocation', 'sources'):
        parser.add_argument('--' + option, type=Path)
    for option in ('exec-root', 'owned-root', 'source-root', 'output'):
        parser.add_argument('--' + option, type=Path, required=True)
    args = parser.parse_args()
    exec_root = args.exec_root.resolve(strict=True)
    owned_root = args.owned_root.resolve(strict=True)
    exec_root.relative_to(owned_root)
    source_root = args.source_root.resolve(strict=True)
    source_root.relative_to(owned_root)
    if args.collect_sources:
        content = json.dumps(source_evidence(exec_root, owned_root, source_root), sort_keys=True)
        require(len(content.encode()) <= 2097152, 'serialized source receipt bound')
        with args.output.open('x') as stream:
            stream.write(content)
        return 0
    require(args.mode in MODES, 'missing verifier mode')
    for path in (args.resolution, args.aquery, args.cquery, args.invocation, args.sources):
        require(path is not None, 'missing evidence argument')
        require(path.is_file() and path.stat().st_size > 0, 'missing/empty evidence: ' + str(path))
        require(not path.with_name(path.name + '.truncated').exists(), 'truncated evidence: ' + str(path))
    spec = MODES[args.mode]
    invocation = json.loads(args.invocation.read_text())
    common = invocation.get('flags', [])
    require(common == spec[3], 'invocation flags differ from exact mode policy')
    require(invocation.get('target') == '//cmd:housegate', 'wrong invocation target')
    require(invocation.get('aquery') == ['aquery', *common, '--include_commandline', '--output=jsonproto', 'deps(//cmd:housegate)'], 'aquery invocation mismatch')
    require(invocation.get('cquery') == ['cquery', *common, '--consistent_labels', '--output=jsonproto', '--transitions=lite', '--proto:include_configurations', 'deps(//cmd:housegate)'], 'cquery invocation mismatch')
    require(args.sources.stat().st_size <= 2097152, 'source receipt bound')
    sources = json.loads(args.sources.read_text())
    require(sources == source_evidence(exec_root, owned_root, source_root), 'source evidence changed or wrong invocation root')
    text = args.resolution.read_text()
    require(not re.search(r'Selected .*local_config_cc', text), 'local_config_cc selected')
    selected = contexts(text, spec)
    aq = json.loads(args.aquery.read_text())
    graph = ConfiguredGraph(json.loads(args.cquery.read_text()))
    paths = artifact_paths(aq)
    matches, rejected, links = [], [], []
    for action in aq.get('actions', []):
        if action.get('mnemonic') not in ('CppCompile', 'GoCompilePkg', 'GoLink'):
            continue
        require('local_config_cc' not in json.dumps(action), 'local_config_cc action')
        errors = []
        for context in selected:
            try:
                proof = prove(action, aq, graph, paths, context, spec, exec_root, owned_root)
                (links if action['mnemonic'] == 'GoLink' else matches).append(proof)
                break
            except (ValueError, OSError) as exc:
                errors.append(str(exc))
        else:
            rejected.append(dict(target_id=action.get('targetId'), reasons=errors))
    require(matches, 'no qualifying compiler tuple: ' + json.dumps(rejected))
    require(len(links) == 1, 'missing/ambiguous configured root GoLink')
    require(any(all(x[k] == links[0][k] for k in ('root_configuration_checksum','selected_label','selected_configuration_checksum','target_platform','execution_platform')) for x in matches), 'compiler/linker context disagreement')
    report = dict(mode=args.mode, invocation=invocation, selected_contexts=selected,
                  matching_compiler_actions=matches, rejected_candidates=rejected, configured_root_linker=links[0],
                  evidence_kind='source-backed analysis; no child syscall observation or runtime acceptance',
                  input_sha256={str(p): sha(p) for p in (args.resolution, args.aquery, args.cquery, args.invocation, args.sources)},
                  execution_root=str(exec_root), owned_root=str(owned_root))
    with args.output.open('x') as stream:
        stream.write(json.dumps(report, indent=2, sort_keys=True) + '\n')
    return 0

if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (ValueError, OSError, KeyError, TypeError) as exc:
        raise SystemExit('insufficient toolchain evidence: ' + str(exc))
