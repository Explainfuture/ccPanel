#!/usr/bin/env python3
"""Real bounded archive validation/extraction; no Docker/production claims."""
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tarfile
import tempfile
import unittest

if os.name != 'posix':
    print(json.dumps({'importer_checks': 'skipped', 'reason': 'native_posix_required'}))
    raise SystemExit(0)
SCRIPT = (Path(__file__).parent / 'import-release-dependencies.sh').read_text()
BODY = SCRIPT.split("<<'PY'\n", 1)[1].split('\nPY\n', 1)[0]
HASH = lambda value: hashlib.sha256(value).hexdigest()


class Extraction(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='ccpanel-import-check-')
        self.root = Path(self.temporary.name)
        self.stage = self.root / 'runtime-deps.Abc123'
        self.stage.mkdir(mode=0o700)
        self.helper = self.root / 'verify-release-dependencies.py'
        self.helper.write_bytes(b'# matching trusted verifier fixture\n')
        self.helper.chmod(0o600)
        self.outside = self.root / 'outside'
        self.outside.write_bytes(b'old release remains unchanged\n')
        self.inputs = {'package.json': b'{"name":"synthetic"}\n', 'pnpm-lock.yaml': b'fixture-lock\n', 'pnpm-workspace.yaml': b'fixture-workspace\n'}
        self.candidate = self.root / 'candidate.tar.gz'
        with tarfile.open(self.candidate, 'w:gz') as archive:
            for name, data in {**self.inputs, 'scripts/verify-release-dependencies.py': self.helper.read_bytes()}.items():
                self.add(archive, {'path': name, 'kind': 'file', 'mode': 0o644, 'size': len(data)}, data)
        self.payloads = {**self.inputs, 'node_modules/.pnpm/pkg@1/node_modules/pkg/index.js': b'module.exports = 1;\n', 'node_modules/.pnpm/pkg@1/node_modules/pkg/run': b'#!/bin/sh\nexit 0\n', 'node_modules/.pnpm/pkg@1/node_modules/pkg/empty': b''}
        self.entries = []
        directories = set()
        for name in self.payloads:
            directories.update(str(parent) for parent in Path(name).parents if str(parent) != '.')
        for name in directories:
            self.entries.append({'path': name, 'kind': 'directory', 'mode': 0o755, 'size': 0})
        for name, data in self.payloads.items():
            self.entries.append({'path': name, 'kind': 'file', 'mode': 0o755 if name.endswith('/run') else 0o644, 'size': len(data), 'sha256': HASH(data)})
        target = '.pnpm/pkg@1/node_modules/pkg'
        self.entries.append({'path': 'node_modules/pkg', 'kind': 'symlink', 'mode': 0o777, 'size': len(target.encode()), 'target': target})
        self.entries.sort(key=lambda entry: entry['path'])
        self.receipt = {'version': 1, 'nodeVersion': 'v22.22.3', 'pnpmVersion': '12.3.4', 'inputs': {name: HASH(data) for name, data in self.inputs.items()}, 'candidateArchiveSha256': HASH(self.candidate.read_bytes()), 'nodeBinarySha256': 'a' * 64, 'pnpmBinarySha256': 'b' * 64, 'files': self.entries}

    def tearDown(self):
        self.temporary.cleanup()

    @staticmethod
    def add(archive, entry, data=b'', override=None):
        item = tarfile.TarInfo(entry['path'])
        item.uid = item.gid = item.mtime = 0
        item.mode = entry['mode']
        item.type = {'file': tarfile.REGTYPE, 'directory': tarfile.DIRTYPE, 'symlink': tarfile.SYMTYPE}[entry['kind']]
        item.size = len(data) if entry['kind'] == 'file' else 0
        if entry['kind'] == 'symlink':
            item.linkname = entry['target']
        if override:
            for key, value in override.items():
                setattr(item, key, value)
            if item.type not in (tarfile.REGTYPE, tarfile.AREGTYPE):
                item.size = 0
        archive.addfile(item, io.BytesIO(data) if item.size else None)

    def artifact(self, receipt=None, payloads=None, missing=False, duplicate=False, override=None):
        receipt = receipt or self.receipt
        payloads = self.payloads if payloads is None else payloads
        output = self.root / 'dependencies.tar.gz'
        raw = json.dumps(receipt, separators=(',', ':')).encode()
        with tarfile.open(output, 'w:gz') as archive:
            self.add(archive, {'path': 'build-toolchain.json', 'kind': 'file', 'mode': 0o644, 'size': len(raw)}, raw)
            for index, entry in enumerate(receipt['files']):
                if missing and index == len(receipt['files']) - 1:
                    continue
                self.add(archive, entry, payloads.get(entry['path'], b''), override if entry['path'].endswith('/index.js') else None)
            if duplicate:
                entry = receipt['files'][-1]
                self.add(archive, entry, payloads.get(entry['path'], b''))
        return output

    def run_extract(self, artifact, digest=None):
        return subprocess.run([sys.executable, '-c', BODY, str(self.candidate), HASH(self.candidate.read_bytes()), str(artifact), digest or HASH(artifact.read_bytes()), str(self.stage), str(self.helper)], capture_output=True, text=True, timeout=15)

    def refused(self, artifact, digest=None):
        result = self.run_extract(artifact, digest)
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertEqual(list(self.stage.iterdir()), [], 'prevalidation must precede all artifact writes')
        self.assertEqual(self.outside.read_bytes(), b'old release remains unchanged\n')
        self.assertEqual(self.outside.stat().st_nlink, 1)
        self.assertFalse((self.stage / '.completed.json').exists())

    def test_real_extraction_binds_inputs_hashes_modes_and_internal_symlinks(self):
        result = self.run_extract(self.artifact())
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertFalse(json.loads(result.stdout)['build_provenance_verified'])
        for name, data in self.payloads.items():
            self.assertEqual((self.stage / name).read_bytes(), data)
            self.assertEqual((self.stage / name).stat().st_nlink, 1)
        self.assertEqual(stat.S_IMODE((self.stage / 'package.json').stat().st_mode), 0o600)
        self.assertEqual(stat.S_IMODE((self.stage / 'node_modules/.pnpm/pkg@1/node_modules/pkg/run').stat().st_mode), 0o755)
        self.assertTrue((self.stage / 'node_modules/pkg').resolve().is_relative_to(self.stage / 'node_modules'))
        self.assertFalse((self.stage / '.completed.json').exists(), 'native import and verifier completion are later independent steps')

    def test_wrong_artifact_pin_is_rejected_before_writes(self):
        self.refused(self.artifact(), '0' * 64)

    def test_file_data_tampering_is_rejected_before_writes(self):
        values = dict(self.payloads)
        values['node_modules/.pnpm/pkg@1/node_modules/pkg/index.js'] = b'tampered content'
        self.refused(self.artifact(payloads=values))

    def test_metadata_candidate_or_toolchain_mismatch_is_rejected(self):
        for field, value in [('candidateArchiveSha256', '0' * 64), ('nodeVersion', 'v22.22.2'), ('pnpmVersion', '12.3.3'), ('inputs', {**self.receipt['inputs'], 'package.json': '0' * 64})]:
            with self.subTest(field=field):
                receipt = copy.deepcopy(self.receipt)
                receipt[field] = value
                self.refused(self.artifact(receipt))

    def test_duplicate_missing_and_special_members_are_refused(self):
        self.refused(self.artifact(duplicate=True))
        self.refused(self.artifact(missing=True))
        for kind in (tarfile.LNKTYPE, tarfile.FIFOTYPE, tarfile.GNUTYPE_SPARSE):
            with self.subTest(kind=kind):
                self.refused(self.artifact(override={'type': kind, 'linkname': '../outside'}))

    def test_escaping_absolute_and_looping_symlinks_are_refused(self):
        for target in ('../../outside', '/outside', 'pkg'):
            with self.subTest(target=target):
                receipt = copy.deepcopy(self.receipt)
                entry = next(item for item in receipt['files'] if item['path'] == 'node_modules/pkg')
                entry.update(target=target, size=len(target.encode()))
                self.refused(self.artifact(receipt))

    def test_children_of_symlink_parents_are_rejected(self):
        receipt = copy.deepcopy(self.receipt)
        receipt['files'].append({'path': 'node_modules/pkg/child.js', 'kind': 'file', 'mode': 0o644, 'size': 0, 'sha256': HASH(b'')})
        receipt['files'].sort(key=lambda entry: entry['path'])
        self.refused(self.artifact(receipt))

    def test_traversal_and_absolute_inventory_paths_are_refused(self):
        for name in ('../outside', '/outside', 'node_modules/../outside', 'node_modules//outside'):
            with self.subTest(name=name):
                receipt = copy.deepcopy(self.receipt)
                entry = next(item for item in receipt['files'] if item['path'].endswith('/index.js'))
                entry['path'] = name
                receipt['files'].sort(key=lambda item: item['path'])
                self.refused(self.artifact(receipt))

    def test_writable_or_unbound_manifest_headers_are_refused(self):
        self.refused(self.artifact(override={'mode': 0o666}))
        self.refused(self.artifact(override={'uid': 1000}))
        receipt = copy.deepcopy(self.receipt)
        next(item for item in receipt['files'] if item['path'].endswith('/index.js'))['mode'] = 0o666
        self.refused(self.artifact(receipt))

    def test_existing_stage_files_are_never_overwritten(self):
        existing = self.stage / 'package.json'
        existing.write_bytes(b'existing owned evidence')
        result = self.run_extract(self.artifact())
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(existing.read_bytes(), b'existing owned evidence')
        self.assertEqual(sorted(item.name for item in self.stage.iterdir()), ['package.json'])

    def test_expired_budget_never_becomes_timeout_zero(self):
        helper = next(line for line in SCRIPT.splitlines() if line.startswith('remaining(){'))
        command = 'set -eu\n' + helper + '\ndeadline=$SECONDS\ntimeout "$(remaining)s" sh -c "echo unexpected-execution"\n'
        result = subprocess.run(['bash', '-c', command], capture_output=True, text=True, timeout=3)
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn('unexpected-execution', result.stdout)


if __name__ == '__main__':
    unittest.main(verbosity=2)
