#!/usr/bin/env python3
"""Package committed deployment tools and an explicitly fresh, complete dist.

This records frozen source and artifact hashes; it cannot prove that ignored
build outputs were compiled from that source. Run a clean build before use.
"""
import argparse
import gzip
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import re
import stat
import subprocess
import tarfile
import sys

METADATA = ('package.json', 'pnpm-lock.yaml', 'pnpm-workspace.yaml')
# Explicit outputs of build.mjs. Unexpected loose tooling bundles fail closed.
NODE_BUNDLES = ('api', 'worker', 'migrate', 'bootstrap', 'schema', 'deletion-ledger',
                'pressure-observe', 'ingest-preflight', 'consumer-progress',
                'kafka-durability', 'company-provision')
MANIFEST = 'release-source.json'
MAX_BYTES = 128 * 1024 * 1024
ASSET = r'[A-Za-z0-9_-]+-[A-Za-z0-9_-]{8}\.(?:js|css|svg|png|jpe?g|gif|webp|ico|woff2?|ttf)'
ASSET_REF = re.compile(r'(?<![A-Za-z0-9_./-])(?:(?:\./)|(?:/)?assets/)?(' + ASSET + r')(?![A-Za-z0-9_./-])')


class PackageReleaseError(Exception):
    pass


def require(condition, code):
    if not condition:
        raise PackageReleaseError(code)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def git(root, *args):
    return subprocess.run(['git', '-c', 'core.autocrlf=false', '-c', 'core.eol=lf', '-C', str(root), *args], check=True,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30).stdout


def safe_name(name):
    parts = PurePosixPath(name).parts
    require(parts and str(PurePosixPath(name)) == name and not name.startswith('/') and all(re.fullmatch(r'[A-Za-z0-9_.-]+', p) and p not in ('.', '..') for p in parts), 'package_release_unsafe_file')
    require(all(not p.lower().startswith('.env') and p.lower() not in ('id_rsa', 'id_ed25519') and not p.lower().endswith(('.key', '.pem', '.p12', '.pfx')) for p in parts), 'package_release_unsafe_file')


def committed_files(root, source_commit):
    require(re.fullmatch(r'[a-f0-9]{40}', source_commit) is not None, 'package_release_source_not_frozen')
    require(Path(git(root, 'rev-parse', '--show-toplevel').decode().strip()).resolve() == root, 'package_release_source_not_frozen')
    require(git(root, 'rev-parse', 'HEAD').decode().strip() == source_commit, 'package_release_source_not_frozen')
    require(not git(root, 'status', '--porcelain=v1', '-z', '--untracked-files=no'), 'package_release_source_not_frozen')
    source_tree = git(root, 'rev-parse', source_commit + '^{tree}').decode().strip()
    # Auxiliary source files validate copied static files/schema but are never
    # included as app/package source in the deployment archive.
    archive = git(root, 'archive', '--format=tar', source_commit, '--', 'infra', 'scripts', *METADATA,
                  'apps/web/public', 'packages/server/src/migrate.ts')
    require(len(archive) <= MAX_BYTES, 'package_release_size_limit')
    files = {}
    with tarfile.open(fileobj=io.BytesIO(archive), mode='r:') as source:
        for member in source:
            safe_name(member.name.rstrip('/'))
            if member.isdir():
                continue
            require(member.isfile() and member.name not in files, 'package_release_unsafe_file')
            require(member.size <= MAX_BYTES, 'package_release_size_limit')
            files[member.name] = source.extractfile(member).read()
    require(all(name in files for name in METADATA), 'package_release_source_inventory')
    return files, source_tree


def dist_files(root, source):
    directory = root / 'dist'
    require(directory.is_dir() and not directory.is_symlink(), 'package_release_dist_inventory')
    actual = {}
    directories = set()

    def scan(path):
        for child in sorted(path.iterdir()):
            name = child.relative_to(root).as_posix()
            safe_name(name)
            info = child.lstat()
            require(not stat.S_ISLNK(info.st_mode), 'package_release_unsafe_file')
            if stat.S_ISDIR(info.st_mode):
                directories.add(name)
                scan(child)
            else:
                require(stat.S_ISREG(info.st_mode), 'package_release_unsafe_file')
                require(info.st_size <= MAX_BYTES, 'package_release_size_limit')
                actual[name] = child.read_bytes()
                require(len(actual) <= 4096 and sum(map(len, actual.values())) <= MAX_BYTES, 'package_release_size_limit')

    scan(directory)
    fixed = {'dist/schema-version.txt', 'dist/web/index.html', 'dist/web/sdk/ccpanel.js'}
    maps = {'dist/' + name + '.mjs.map' for name in NODE_BUNDLES}
    fixed.update('dist/' + name + '.mjs' for name in NODE_BUNDLES)
    fixed.update(maps)
    public = {name[len('apps/web/public/'):]: data for name, data in source.items() if name.startswith('apps/web/public/')}
    require(public and 'index.html' not in public and not any(name.startswith(('assets/', 'sdk/')) for name in public), 'package_release_source_inventory')
    fixed.update('dist/web/' + name for name in public)
    require(fixed.issubset(actual), 'package_release_dist_inventory')
    require(all(actual[name] for name in fixed), 'package_release_dist_inventory')
    schema = re.search(rb'^export const schemaVersion=([0-9]+);$', source['packages/server/src/migrate.ts'], re.M)
    require(schema is not None and actual['dist/schema-version.txt'] == schema[1] + b'\n', 'package_release_schema_mismatch')
    for name, data in public.items():
        require(actual['dist/web/' + name] == data, 'package_release_public_mismatch')
    for name in NODE_BUNDLES:
        require(actual['dist/' + name + '.mjs'].rstrip().endswith(('//# sourceMappingURL=' + name + '.mjs.map').encode()), 'package_release_dist_inventory')
        value = json.loads(actual['dist/' + name + '.mjs.map'])
        require(value.get('version') == 3 and isinstance(value.get('sources'), list), 'package_release_dist_inventory')
    assets = {name for name in actual if name.startswith('dist/web/assets/')}
    require(all(re.fullmatch(ASSET, name[len('dist/web/assets/'):]) for name in assets), 'package_release_dist_inventory')
    reachable = set()
    pending = ['dist/web/index.html']
    while pending:
        name = pending.pop()
        if not name.endswith(('.html', '.js', '.css')):
            continue
        for match in ASSET_REF.finditer(actual[name].decode('utf-8')):
            asset = 'dist/web/assets/' + match[1]
            require(asset in assets, 'package_release_dist_inventory')
            if asset not in reachable:
                reachable.add(asset)
                pending.append(asset)
    require(reachable and reachable == assets and set(actual) == fixed | assets, 'package_release_dist_inventory')
    expected_directories = {str(parent) for name in actual for parent in PurePosixPath(name).parents if str(parent) not in ('.', 'dist')}
    require(directories == expected_directories, 'package_release_dist_inventory')
    # Source maps contain source text. Require the complete expected build, but
    # deliberately keep those maps out of the runtime deployment artifact.
    return {name: data for name, data in actual.items() if name not in maps}


def package(root, output, source_commit):
    root = Path(root).resolve()
    output = Path(output).absolute()
    require(not output.exists() and not output.is_symlink(), 'package_release_output_exists')
    require(output.parent.is_dir() and output.parent.resolve() == output.parent, 'package_release_output_unsafe')
    require(not any(output == root / path or root / path in output.parents for path in ('dist', 'infra', 'scripts', *METADATA)), 'package_release_output_unsafe')
    source, tree = committed_files(root, source_commit)
    files = {name: data for name, data in source.items() if name in METADATA or name.startswith(('infra/', 'scripts/'))}
    files.update(dist_files(root, source))
    inventory = [{'path': name, 'bytes': len(data), 'sha256': sha(data)} for name, data in sorted(files.items())]
    manifest = {'version': 1, 'source_commit': source_commit, 'source_tree': tree,
                'dist_source_maps_excluded': True, 'build_source_verified': False, 'files': inventory}
    files[MANIFEST] = (json.dumps(manifest, sort_keys=True, separators=(',', ':')) + '\n').encode()
    require(sum(map(len, files.values())) <= MAX_BYTES and len(files) <= 4096, 'package_release_size_limit')
    directories = {str(parent) for name in files for parent in PurePosixPath(name).parents if str(parent) != '.'}
    owned = False
    try:
        with output.open('xb') as raw:
            owned = True
            with gzip.GzipFile(filename='', fileobj=raw, mode='wb', compresslevel=9, mtime=0) as compressed:
                with tarfile.open(fileobj=compressed, mode='w', format=tarfile.PAX_FORMAT) as archive:
                    for name in sorted(directories, key=lambda item: (len(PurePosixPath(item).parts), item)) + sorted(files):
                        info = tarfile.TarInfo(name)
                        info.uid = info.gid = info.mtime = 0
                        info.uname = info.gname = ''
                        info.mode = 0o755 if name in directories else 0o644
                        if name in directories:
                            info.type = tarfile.DIRTYPE
                            archive.addfile(info)
                        else:
                            info.size = len(files[name])
                            archive.addfile(info, io.BytesIO(files[name]))
        # Detect a tracked edit or HEAD movement during packaging as well.
        require(git(root, 'rev-parse', 'HEAD').decode().strip() == source_commit and not git(root, 'status', '--porcelain=v1', '-z', '--untracked-files=no'), 'package_release_source_not_frozen')
    except BaseException:
        if owned:
            output.unlink()
        raise
    data = output.read_bytes()
    return {'archive_sha256': sha(data), 'archive_bytes': len(data), 'files': len(files),
            'source_commit': source_commit, 'source_tree': tree, 'inventory_sha256': sha(files[MANIFEST]),
            'dist_source_maps_excluded': True, 'build_source_verified': False, 'fresh_clean_build_required': True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', default='.')
    parser.add_argument('--output', required=True)
    parser.add_argument('--source-commit', required=True)
    args = parser.parse_args()
    try:
        print(json.dumps(package(args.root, args.output, args.source_commit), sort_keys=True))
        return 0
    except PackageReleaseError as error:
        print(json.dumps({'code': str(error)}), file=sys.stderr)
    except (OSError, ValueError, KeyError, subprocess.SubprocessError, tarfile.TarError):
        print(json.dumps({'code': 'package_release_failed'}), file=sys.stderr)
    return 1


if __name__ == '__main__':
    sys.exit(main())
