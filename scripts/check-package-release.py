#!/usr/bin/env python3
"""Real temporary Git/tar fixtures; no application build or deployed state."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile

HELPER = Path(__file__).with_name('package-release.py').resolve()
spec = importlib.util.spec_from_file_location('release_packager', HELPER)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def write(root, name, data):
    path = root / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


def git(root, *args):
    return subprocess.check_output(['git', '-C', str(root), '-c', 'core.autocrlf=false',
                                    '-c', 'commit.gpgsign=false', '-c', 'core.hooksPath=' + str(root / '.no-hooks'),
                                    *args], stderr=subprocess.PIPE).decode().strip()


def freeze(root):
    git(root, 'add', '.')
    git(root, '-c', 'user.name=fixture', '-c', 'user.email=fixture@example.invalid', 'commit', '-qm', 'fixture')
    return git(root, 'rev-parse', 'HEAD')


with tempfile.TemporaryDirectory(prefix='ccpanel-package-') as temporary:
    parent = Path(temporary).resolve()
    root = parent / 'repo'
    root.mkdir()
    git(root, 'init', '-q')
    for name, data in {
        '.gitignore': b'dist/\n', 'infra/compose.yaml': b'services: {}\n',
        'scripts/tool.sh': b'#!/bin/sh\ntrue\n', 'scripts/build.mjs': b'// synthetic build fixture\n',
        'package.json': b'{"name":"fixture"}\n', 'pnpm-lock.yaml': b'lockfileVersion: "9.0"\n',
        'pnpm-workspace.yaml': b'packages: []\n',
        'packages/server/src/migrate.ts': b'export const schemaVersion=22;\n',
        'apps/web/public/sdk-lab.html': b'<p>committed public fixture</p>\n',
    }.items():
        write(root, name, data)
    commit = freeze(root)
    for name in module.NODE_BUNDLES:
        write(root, 'dist/' + name + '.mjs', ('export const fixture=true;\n//# sourceMappingURL=' + name + '.mjs.map\n').encode())
        write(root, 'dist/' + name + '.mjs.map', b'{"version":3,"sources":["private-source.ts"],"sourcesContent":["do not ship"]}\n')
    for name, data in {
        'dist/schema-version.txt': b'22\n',
        'dist/web/index.html': b'<script src="/assets/index-AAAAAAAA.js"></script><link href="/assets/index-BBBBBBBB.css">\n',
        'dist/web/assets/index-AAAAAAAA.js': b'import("./lazy-CCCCCCCC.js");\n',
        'dist/web/assets/lazy-CCCCCCCC.js': b'export const lazy=true;\n',
        'dist/web/assets/index-BBBBBBBB.css': b'body{color:black}\n',
        'dist/web/sdk/ccpanel.js': b'export const sdk=true;\n',
        'dist/web/sdk-lab.html': b'<p>committed public fixture</p>\n',
    }.items():
        write(root, name, data)

    first, second = parent / 'first.tar.gz', parent / 'different-output-name.tar.gz'
    receipt = module.package(root, first, commit)
    for file in (root / 'dist').rglob('*'):
        os.utime(file, (1234567890, 1234567890))
        file.chmod(0o755 if file.is_dir() else 0o600)
    other = module.package(root, second, commit)
    assert first.read_bytes() == second.read_bytes() and receipt == other
    header = first.read_bytes()
    assert header[:3] == b'\x1f\x8b\x08' and header[3] & 8 == 0 and header[4:8] == b'\0' * 4
    with tarfile.open(first, 'r:gz') as archive:
        members = archive.getmembers()
        assert len({member.name for member in members}) == len(members)
        assert all(member.uid == member.gid == member.mtime == 0 and member.uname == member.gname == '' for member in members)
        assert all(member.mode == (0o755 if member.isdir() else 0o644) and (member.isdir() or member.isfile()) for member in members)
        files = {member.name: archive.extractfile(member).read() for member in members if member.isfile()}
    manifest = json.loads(files.pop(module.MANIFEST))
    assert manifest['source_commit'] == commit and manifest['source_tree'] == git(root, 'rev-parse', 'HEAD^{tree}')
    assert manifest['dist_source_maps_excluded'] is True and manifest['build_source_verified'] is False
    assert manifest['files'] == [{'path': name, 'bytes': len(data), 'sha256': hashlib.sha256(data).hexdigest()} for name, data in sorted(files.items())]
    assert not any(name.endswith('.map') or name.startswith(('apps/', 'packages/')) or name.startswith('.env') for name in files)
    assert {'dist/web/assets/lazy-CCCCCCCC.js', 'dist/web/sdk/ccpanel.js', 'dist/deletion-ledger.mjs', 'scripts/tool.sh'}.issubset(files)
    assert receipt['archive_sha256'] == hashlib.sha256(first.read_bytes()).hexdigest()

    failures = []

    def rejected(label, change, restore, expected):
        output = parent / (label + '.tar.gz')
        change()
        try:
            try:
                module.package(root, output, commit)
                raise RuntimeError('unexpected_package_success:' + label)
            except module.PackageReleaseError as error:
                assert str(error) == expected, (label, str(error))
            assert not output.exists()
            failures.append(label)
        finally:
            restore()

    stale = root / 'dist/api-polling-child.mjs'
    rejected('stale-loose-tool', lambda: stale.write_bytes(b'stale'), stale.unlink, 'package_release_dist_inventory')
    old = root / 'dist/web/assets/old-DDDDDDDD.js'
    rejected('unreferenced-old-web-chunk', lambda: old.write_bytes(b'old'), old.unlink, 'package_release_dist_inventory')
    extra = root / 'dist/.env'
    rejected('private-env', lambda: extra.write_bytes(b'never ship'), extra.unlink, 'package_release_unsafe_file')
    api = root / 'dist/api.mjs'
    api_bytes = api.read_bytes()
    rejected('missing-required-bundle', api.unlink, lambda: api.write_bytes(api_bytes), 'package_release_dist_inventory')
    source_map = root / 'dist/api.mjs.map'
    map_bytes = source_map.read_bytes()
    rejected('missing-required-map', source_map.unlink, lambda: source_map.write_bytes(map_bytes), 'package_release_dist_inventory')
    public = root / 'dist/web/sdk-lab.html'
    public_bytes = public.read_bytes()
    rejected('public-copy-differs', lambda: public.write_bytes(b'wrong'), lambda: public.write_bytes(public_bytes), 'package_release_public_mismatch')
    schema = root / 'dist/schema-version.txt'
    rejected('schema-mismatch', lambda: schema.write_bytes(b'21\n'), lambda: schema.write_bytes(b'22\n'), 'package_release_schema_mismatch')
    tracked = root / 'scripts/tool.sh'
    rejected('dirty-tracked-source', lambda: tracked.write_bytes(b'changed'), lambda: tracked.write_bytes(files['scripts/tool.sh']), 'package_release_source_not_frozen')
    try:
        module.package(root, parent / 'wrong-commit.tar.gz', '0' * 40)
        raise RuntimeError('wrong_commit_accepted')
    except module.PackageReleaseError as error:
        assert str(error) == 'package_release_source_not_frozen'
        failures.append('wrong-frozen-commit')
    try:
        module.package(root, first, commit)
        raise RuntimeError('existing_archive_overwritten')
    except module.PackageReleaseError as error:
        assert str(error) == 'package_release_output_exists'
        assert first.read_bytes() == header
        failures.append('existing-archive')
    for confusing in ('./scripts/tool.sh', 'scripts//tool.sh', 'scripts/../outside', '/absolute', 'scripts\\tool.sh'):
        try:
            module.safe_name(confusing)
            raise RuntimeError('confusing_path_accepted')
        except module.PackageReleaseError:
            pass
    # A new script is deliberately absent until committed, then automatically
    # enters the release without another hand-maintained source-file list.
    write(root, 'scripts/new-check.py', b'print("committed fixture")\n')
    unpublished = parent / 'unpublished.tar.gz'
    module.package(root, unpublished, commit)
    with tarfile.open(unpublished) as archive:
        assert 'scripts/new-check.py' not in archive.getnames()
    commit = freeze(root)
    published = parent / 'published.tar.gz'
    module.package(root, published, commit)
    with tarfile.open(published) as archive:
        assert archive.extractfile('scripts/new-check.py').read() == b'print("committed fixture")\n'
    # Symlink creation is not available to every Windows account; Linux CI
    # exercises actual link rejection rather than substituting fake metadata.
    link = root / 'dist/unsafe-link'
    symlink_checked = False
    try:
        link.symlink_to(root / 'package.json')
    except OSError:
        if sys.platform != 'win32':
            raise
    else:
        try:
            try:
                module.package(root, parent / 'symlink.tar.gz', commit)
                raise RuntimeError('symlink_accepted')
            except module.PackageReleaseError as error:
                assert str(error) == 'package_release_unsafe_file'
                symlink_checked = True
        finally:
            link.unlink()
    # Exercise the real CLI and its JSON receipt, not only imported functions.
    cli = subprocess.run([sys.executable, str(HELPER), '--root', str(root), '--output', str(parent / 'cli.tar.gz'), '--source-commit', commit], capture_output=True, text=True)
    assert cli.returncode == 0 and not cli.stderr and json.loads(cli.stdout)['source_commit'] == commit
    print(json.dumps({'passed': True, 'deterministic_tar_and_gzip': True, 'frozen_git_source': True,
                      'tracked_script_auto_inclusion': True, 'source_maps_excluded': True,
                      'actual_symlink_rejected': symlink_checked, 'real_cli': True,
                      'rejected_cases': failures, 'build_source_verified': False}))
