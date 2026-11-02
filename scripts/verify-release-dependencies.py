#!/usr/bin/env python3
"""Verify an explicit, completed production dependency stage. Never installs."""
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys
import tarfile

STAGE_PARENT = Path('/opt/ccpanel/shared')
OWNER_UID = 0
OWNER_GID = 0
INPUTS = ('package.json', 'pnpm-lock.yaml', 'pnpm-workspace.yaml')
MARKER = '.completed.json'
NODE_VERSION = 'v22.22.3'
PNPM_VERSION = '12.3.4'
MAX_FILE_BYTES = 64 * 1024 * 1024
MAX_TREE_BYTES = 256 * 1024 * 1024
MAX_TREE_NODES = 100000


class DependencyError(Exception):
    pass


def require(condition, code='release_dependencies_invalid'):
    if not condition:
        raise DependencyError(code)


def canonical(path):
    value = Path(path)
    require(value.is_absolute() and value.resolve(strict=True) == value, 'release_dependencies_path_unsafe')
    return value


def owned(info):
    require(info.st_uid == OWNER_UID and info.st_gid == OWNER_GID and not info.st_mode & 0o7000, 'release_dependencies_owner_unsafe')


def stage_root(path):
    require(os.name == 'posix', 'release_dependencies_posix_required')
    root = canonical(path)
    info = root.lstat()
    owned(info)
    require(root.parent == STAGE_PARENT and re.fullmatch(r'runtime-deps\.[A-Za-z0-9]{6}', root.name), 'release_dependencies_path_unsafe')
    require(stat.S_ISDIR(info.st_mode) and stat.S_IMODE(info.st_mode) == 0o700, 'release_dependencies_stage_permissions')
    return root


def unchanged(a, b):
    return (a.st_dev, a.st_ino, a.st_mode, a.st_uid, a.st_gid, a.st_size, a.st_nlink, a.st_mtime_ns, a.st_ctime_ns) == (b.st_dev, b.st_ino, b.st_mode, b.st_uid, b.st_gid, b.st_size, b.st_nlink, b.st_mtime_ns, b.st_ctime_ns)


def regular(path, limit, private=False):
    before = path.lstat()
    owned(before)
    require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1 and 0 < before.st_size <= limit, 'release_dependencies_file_unsafe')
    require(not before.st_mode & 0o022 and (not private or stat.S_IMODE(before.st_mode) == 0o600), 'release_dependencies_file_permissions')
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        require(unchanged(before, os.fstat(fd)), 'release_dependencies_changed')
        data = bytearray()
        while len(data) <= limit:
            chunk = os.read(fd, min(65536, limit + 1 - len(data)))
            if not chunk:
                break
            data.extend(chunk)
        require(len(data) == before.st_size and unchanged(before, os.fstat(fd)) and unchanged(before, path.lstat()), 'release_dependencies_changed')
        return bytes(data)
    finally:
        os.close(fd)


def input_hashes(candidate):
    path = canonical(candidate)
    if path.is_dir():
        return {name: hashlib.sha256(regular(path / name, 4 * 1024 * 1024)).hexdigest() for name in INPUTS}
    # The trusted archive can be checked before creating a release directory.
    info = path.lstat()
    owned(info)
    require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and 0 < info.st_size <= 64 * 1024 * 1024, 'release_dependencies_archive_unsafe')
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    hashes = {}
    try:
        require(unchanged(info, os.fstat(fd)), 'release_dependencies_changed')
        with os.fdopen(os.dup(fd), 'rb') as source, tarfile.open(fileobj=source, mode='r|*') as archive:
            total = 0
            for count, entry in enumerate(archive, 1):
                total += entry.size
                require(count <= 10000 and total <= 128 * 1024 * 1024, 'release_dependencies_archive_budget')
                if entry.name in INPUTS:
                    require(entry.name not in hashes and entry.isfile() and 0 < entry.size <= 4 * 1024 * 1024, 'release_dependencies_archive_unsafe')
                    stream = archive.extractfile(entry)
                    require(stream is not None)
                    with stream:
                        data = stream.read(4 * 1024 * 1024 + 1)
                    require(len(data) == entry.size, 'release_dependencies_archive_unsafe')
                    hashes[entry.name] = hashlib.sha256(data).hexdigest()
        require(unchanged(info, os.fstat(fd)) and unchanged(info, path.lstat()), 'release_dependencies_changed')
    finally:
        os.close(fd)
    require(set(hashes) == set(INPUTS), 'release_dependencies_inputs_missing')
    return {name: hashes[name] for name in INPUTS}


def dependency_tree(path, writable=False):
    root = canonical(path)
    require(stat.S_ISDIR(root.lstat().st_mode), 'release_dependencies_tree_unsafe')
    digest = hashlib.sha256()
    nodes, total, entries = 0, 0, []

    def visit(current):
        nonlocal nodes, total
        before = current.lstat()
        owned(before)
        nodes += 1
        require(nodes <= MAX_TREE_NODES, 'release_dependencies_tree_budget')
        relative = '.' if current == root else current.relative_to(root).as_posix()
        require(not re.search(r'[\x00-\x1f\x7f]', relative), 'release_dependencies_tree_unsafe')
        mode = stat.S_IMODE(before.st_mode)
        if stat.S_ISLNK(before.st_mode):
            target = os.readlink(current)
            require(target and not os.path.isabs(target) and len(os.fsencode(target)) <= 4096 and not re.search(r'[\x00-\x1f\x7f]', target), 'release_dependencies_symlink_unsafe')
            resolved = current.resolve(strict=True)
            require(resolved == root or root in resolved.parents, 'release_dependencies_symlink_escape')
            require(unchanged(before, current.lstat()), 'release_dependencies_changed')
            record = [relative, 'symlink', mode, len(os.fsencode(target)), target]
        elif stat.S_ISDIR(before.st_mode):
            require(not mode & (0o022 if writable else 0o222) and mode & 0o555 == 0o555, 'release_dependencies_tree_permissions')
            record = [relative, 'directory', mode & ~0o222, 0]
        elif stat.S_ISREG(before.st_mode):
            require(before.st_nlink == 1 and before.st_size <= MAX_FILE_BYTES, 'release_dependencies_hardlink_or_size')
            require(not mode & (0o022 if writable else 0o222) and mode & 0o444 == 0o444, 'release_dependencies_tree_permissions')
            total += before.st_size
            require(total <= MAX_TREE_BYTES, 'release_dependencies_tree_budget')
            fd = os.open(current, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            try:
                require(unchanged(before, os.fstat(fd)), 'release_dependencies_changed')
                content = hashlib.sha256()
                size = 0
                while size <= before.st_size:
                    chunk = os.read(fd, min(65536, before.st_size + 1 - size))
                    if not chunk:
                        break
                    size += len(chunk)
                    content.update(chunk)
                require(size == before.st_size and unchanged(before, os.fstat(fd)) and unchanged(before, current.lstat()), 'release_dependencies_changed')
            finally:
                os.close(fd)
            record = [relative, 'file', mode & ~0o222, size, content.hexdigest()]
        else:
            raise DependencyError('release_dependencies_tree_unsafe')
        digest.update(json.dumps(record, ensure_ascii=True, separators=(',', ':')).encode('ascii') + b'\n')
        entries.append((current, before))
        if stat.S_ISDIR(before.st_mode):
            for child in sorted(current.iterdir(), key=lambda value: value.name):
                visit(child)
            require(unchanged(before, current.lstat()), 'release_dependencies_changed')

    visit(root)
    require(nodes > 1 and total > 0, 'release_dependencies_tree_empty')
    return digest.hexdigest(), nodes, total, entries


def no_duplicate_keys(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, 'release_dependencies_marker_invalid')
        result[key] = value
    return result


def manifest(root):
    value = json.loads(regular(root / MARKER, 4096, private=True).decode('utf-8'), object_pairs_hook=no_duplicate_keys)
    require(isinstance(value, dict) and set(value) == {'version', 'nodeVersion', 'pnpmVersion', 'inputs', 'treeSha256'}, 'release_dependencies_marker_invalid')
    require(type(value['version']) is int and value['version'] == 1 and value['nodeVersion'] == NODE_VERSION and value['pnpmVersion'] == PNPM_VERSION, 'release_dependencies_toolchain_mismatch')
    require(isinstance(value['inputs'], dict) and set(value['inputs']) == set(INPUTS), 'release_dependencies_marker_invalid')
    require(all(isinstance(item, str) and re.fullmatch(r'[a-f0-9]{64}', item) for item in [value['treeSha256'], *value['inputs'].values()]), 'release_dependencies_marker_invalid')
    return value


def create(stage, node_version, pnpm_version):
    root = stage_root(stage)
    require(node_version == NODE_VERSION and pnpm_version == PNPM_VERSION, 'release_dependencies_toolchain_mismatch')
    require(not os.path.lexists(root / MARKER), 'release_dependencies_marker_exists')
    inputs = input_hashes(root)
    before_hash, _, _, entries = dependency_tree(root / 'node_modules', writable=True)
    # Validate hardlinks and escaping links BEFORE chmod can affect another tree.
    for path, before in reversed(entries):
        require(unchanged(before, path.lstat()), 'release_dependencies_changed')
        if not stat.S_ISLNK(before.st_mode):
            os.chmod(path, stat.S_IMODE(before.st_mode) & ~0o222)
    tree_hash, nodes, size, _ = dependency_tree(root / 'node_modules')
    require(tree_hash == before_hash and input_hashes(root) == inputs, 'release_dependencies_changed')
    value = {'version': 1, 'nodeVersion': node_version, 'pnpmVersion': pnpm_version, 'inputs': inputs, 'treeSha256': tree_hash}
    fd = os.open(root / MARKER, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(fd, 'wb', closefd=False) as output:
            output.write(json.dumps(value, separators=(',', ':')).encode('ascii') + b'\n')
            output.flush()
            os.fsync(fd)
    finally:
        os.close(fd)
    directory = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    return {'completed': True, 'treeSha256': tree_hash, 'nodes': nodes, 'bytes': size}


def verify(stage, candidate, copied=None):
    root = stage_root(stage)
    value = manifest(root)
    require(input_hashes(root) == value['inputs'] == input_hashes(candidate), 'release_dependencies_inputs_mismatch')
    tree_hash, nodes, size, _ = dependency_tree(root / 'node_modules')
    require(tree_hash == value['treeSha256'], 'release_dependencies_tree_mismatch')
    if copied is not None:
        destination = canonical(copied)
        require(destination != root / 'node_modules', 'release_dependencies_copy_unsafe')
        require(dependency_tree(destination)[:3] == (tree_hash, nodes, size), 'release_dependencies_copy_mismatch')
        # Copying must not modify the prepared source or its completed marker.
        require(manifest(root) == value and dependency_tree(root / 'node_modules')[:3] == (tree_hash, nodes, size), 'release_dependencies_changed')
    return {'verified': True, 'treeSha256': tree_hash, 'nodes': nodes, 'bytes': size}


def main(args):
    try:
        if len(args) == 4 and args[0] == 'create':
            result = create(*args[1:])
        elif len(args) in (3, 4) and args[0] in ('verify', 'verify-copy') and len(args) == (3 if args[0] == 'verify' else 4):
            result = verify(*args[1:])
        else:
            raise DependencyError('release_dependencies_usage')
        print(json.dumps(result, separators=(',', ':')))
        return 0
    except Exception as error:
        print(json.dumps({'code': str(error) if isinstance(error, DependencyError) else 'release_dependencies_invalid'}), file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main(sys.argv[1:]))
