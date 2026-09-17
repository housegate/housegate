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
    # Canonical external repository names contain '+'. Never guess apparent
    # repository mapping: @rules_go is NOT equivalent to @@rules_go+.
    require(isinstance(value, str), 'label is not a string')
    if value.startswith('@@//'):
        return value[2:]
    if value.startswith('@') and not value.startswith('@@') and '+' in value.split('//')[0]:
        return '@' + value
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


def contexts(text, spec):
    registered_target, platform, triple, _ = spec
    result = []
    blocks = re.split(r'(?=INFO: ToolchainResolution: Performing resolution of )', text)
    for block in blocks:
        match = re.search(r'Performing resolution of (\S+tools/cpp:toolchain_type) for target platform (\S+)', block)
        if not match:
            continue
        target_platform = label(match[2])
        repo, target = external(target_platform)
        if not repo.startswith('rules_go+') or target != 'go/toolchain:' + platform:
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
                                       target_platform=target_platform, execution_platform=label(selected[2]),
                                       resolution_block=block))
    require(result, 'no exact registered/selected Hermetic target context')
    return result


def platform_option(config):
    values = [o.get('value') for f in config.get('fragmentOptions', [])
              if f.get('name', '').endswith('.PlatformOptions')
              for o in f.get('options', []) if o.get('name') == 'platforms']
    require(len(values) == 1 and isinstance(values[0], str), 'missing/ambiguous configured platforms option')
    value = values[0]
    # ConfigurationForOutput formats a list option via its Java toString.
    require(value.startswith('[') and value.endswith(']') and ',' not in value, 'not exactly one configured platform')
    result = label(value[1:-1])
    external(result)
    return result


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
            deprecated = record.get('configuration', {}).get('checksum')
            require(deprecated in (None, checksum), 'cquery checksum disagreement')
            key = (label(rule.get('name', '')), checksum)
            require(key not in self.nodes, 'duplicate cquery configured target')
            self.nodes[key] = (rule, cfg)
        require(self.nodes, 'missing cquery configured targets')

    def reachable(self, target_platform):
        roots = [key for key, (_, cfg) in self.nodes.items()
                 if key[0] == '//cmd:housegate' and platform_option(cfg) == target_platform]
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
    root, reachable = graph.reachable(context['target_platform'])
    require((label(target), checksum) in reachable, 'compile action is not a configured dependency of intended root')
    rule, configured = node
    require(platform_option(configured) == context['target_platform'], 'action configuration uses another target platform')
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
    require(args and args[0] == compiler_token, 'actual compiler position is not selected provider gcc')
    # The pinned wrapper obtains the exact target from argv[0]. An extra target
    # override is not needed on the supported path and is refused outright.
    require(not any(x in ('-target', '--target') or x.startswith(('-target=', '--target=')) for x in args[1:]), 'unexpected compiler target override')
    require(not any(x.startswith('@') for x in args[1:]), 'unexpanded compiler response file')
    files, _ = graph.edge(tc, files_label)
    file_edges = {label(e.get('label', '')) for e in files.get('configuredRuleInput', [])}
    require({'@@' + repo + '//:' + gcc, '@@' + repo + '//:zig'} <= file_edges, 'compiler_files omits compiler or Zig binary')
    inputs = input_paths(action, aq, paths)
    require({compiler_token, zig_token} <= inputs, 'action does not consume selected compiler and Zig binary')
    require(not os.path.lexists(exec_root / 'external/zig_sdk/lib') and not any(x.startswith('external/zig_sdk/') for x in inputs), 'legacy wrapper SDK override prevents exact Zig identity')
    compiler = executable(compiler_token, exec_root, owned_root)
    zig = executable(zig_token, exec_root, owned_root)
    return dict(root_target=root[0], root_configuration_checksum=root[1], target_label=target, configuration_checksum=checksum, configuration_cpu=cfg.get('platformName'),
                configuration_id=action['configurationId'], target_platform=context['target_platform'],
                execution_platform=action['executionPlatform'], selected_label=context['selected_label'],
                selected_configuration_checksum=tc_cfg['checksum'], registered_label=context['registered_label'],
                config_provider_label=config_label, compiler_files_label=files_label,
                compiler_target=triple, compiler_argv_position=0, compiler=compiler, zig=zig,
                action_key=action.get('actionKey'), arguments=args)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=MODES, required=True)
    for option in ('resolution', 'aquery', 'cquery', 'invocation', 'exec-root', 'owned-root', 'output'):
        parser.add_argument('--' + option, type=Path, required=True)
    args = parser.parse_args()
    for path in (args.resolution, args.aquery, args.cquery, args.invocation):
        require(path.is_file() and path.stat().st_size > 0, 'missing/empty evidence: ' + str(path))
        require(not path.with_name(path.name + '.truncated').exists(), 'truncated evidence: ' + str(path))
    spec = MODES[args.mode]
    invocation = json.loads(args.invocation.read_text())
    common = invocation.get('flags', [])
    require(all(common.count(f) == 1 for f in spec[3]), 'required invocation flags missing or repeated')
    require(invocation.get('target') == '//cmd:housegate', 'wrong invocation target')
    require(invocation.get('aquery') == ['aquery', *common, '--include_commandline', '--output=jsonproto', 'deps(//cmd:housegate)'], 'aquery invocation mismatch')
    require(invocation.get('cquery') == ['cquery', *common, '--output=jsonproto', '--transitions=lite', '--proto:include_configurations', 'deps(//cmd:housegate)'], 'cquery invocation mismatch')
    text = args.resolution.read_text()
    require(not re.search(r'Selected .*local_config_cc', text), 'local_config_cc selected')
    selected = contexts(text, spec)
    aq = json.loads(args.aquery.read_text())
    graph = ConfiguredGraph(json.loads(args.cquery.read_text()))
    paths = artifact_paths(aq)
    exec_root = args.exec_root.resolve(strict=True)
    owned_root = args.owned_root.resolve(strict=True)
    exec_root.relative_to(owned_root)
    matches, rejected = [], []
    for action in aq.get('actions', []):
        if action.get('mnemonic') != 'CppCompile':
            continue
        require(not any('local_config_cc' in x for x in action.get('arguments', [])), 'local_config_cc compile action')
        errors = []
        for context in selected:
            try:
                matches.append(prove(action, aq, graph, paths, context, spec, exec_root, owned_root))
                break
            except (ValueError, OSError) as exc:
                errors.append(str(exc))
        else:
            rejected.append(dict(target_id=action.get('targetId'), reasons=errors))
    require(matches, 'no correlated CppCompile action: ' + json.dumps(rejected))
    report = dict(mode=args.mode, invocation=invocation, selected_contexts=selected,
                  matching_cpp_compile_actions=matches, other_cpp_compile_actions=rejected,
                  input_sha256={str(p): sha(p) for p in (args.resolution, args.aquery, args.cquery, args.invocation)},
                  execution_root=str(exec_root), owned_root=str(owned_root))
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + '\n')
    return 0

if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (ValueError, OSError, KeyError, TypeError) as exc:
        raise SystemExit('insufficient toolchain evidence: ' + str(exc))
