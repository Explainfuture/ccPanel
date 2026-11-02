#!/usr/bin/env python3
"""Keep exactly the previous build's assets beside a staged new build."""
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import sys

MANIFEST = '.owned-web-assets.json'
MAX_BYTES = 16 * 1024 * 1024


def checked_files(web, names):
    assert isinstance(names, list) and len(names) <= 512
    assert len(set(names)) == len(names)
    result = []
    for name in names:
        assert isinstance(name, str) and re.fullmatch(r'[A-Za-z0-9_.-]+', name)
        assert name not in ('.', '..')
        file = web / 'assets' / name
        info = file.lstat()
        assert stat.S_ISREG(info.st_mode) and info.st_size <= MAX_BYTES
        result.append((file, info.st_size))
    return result


def retain(previous, current):
    previous, current = Path(previous).resolve(), Path(current).resolve()
    assert previous != current
    for web in (previous, current):
        assert not (web / 'assets').is_symlink()
        assert (web / 'assets').resolve().parent == web
    owned = sorted(file.name for file in (current / 'assets').iterdir())
    serialized = json.dumps(owned)
    assert len(serialized.encode('utf-8')) <= 65536
    current_files = checked_files(current, owned)
    old_manifest = previous / MANIFEST
    if old_manifest.exists():
        assert not old_manifest.is_symlink() and old_manifest.stat().st_size <= 65536
        old_names = json.loads(old_manifest.read_text(encoding='utf-8'))
    else:
        # First upgrade from releases made before the ownership manifest.
        old_names = sorted(file.name for file in (previous / 'assets').iterdir())
    previous_files = checked_files(previous, old_names)
    assert sum(size for _, size in current_files + previous_files) <= MAX_BYTES
    for source, _ in previous_files:
        target = current / 'assets' / source.name
        if target.exists():
            assert not target.is_symlink()
            assert hashlib.sha256(target.read_bytes()).digest() == hashlib.sha256(source.read_bytes()).digest(), 'asset_hash_collision'
        else:
            shutil.copyfile(source, target)
            target.chmod(0o644)
    # Only this build's original assets go into its manifest. Assets inherited
    # from an older build are not copied yet again on the next deployment.
    destination = current / MANIFEST
    with destination.open('x', encoding='utf-8') as file:
        file.write(serialized)
        file.flush()
        os.fsync(file.fileno())
    destination.chmod(0o644)
    return {'owned': len(owned), 'previous': len(previous_files)}


if __name__ == '__main__':
    previous, current = (Path(value).resolve() for value in sys.argv[1:])
    for release in (previous, current):
        assert release.parent == Path('/opt/ccpanel/releases') and release.is_dir()
    print(json.dumps(retain(previous / 'dist/web', current / 'dist/web')))
