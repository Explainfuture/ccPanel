#!/usr/bin/env python3
"""Linux checks: real files, chmod/copy, and guarded deployment control flow."""
import contextlib
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import tarfile
import tempfile
import unittest

assert os.name == 'posix', 'POSIX dependency-stage checks only'
SCRIPTS = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('release_dependency_verifier', SCRIPTS / 'verify-release-dependencies.py')
verifier = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verifier)


class Checks(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='ccpanel-dependency-check-')
        self.root = Path(self.temporary.name)
        self.shared = self.root / 'shared'
        self.shared.mkdir()
        self.previous_globals = verifier.STAGE_PARENT, verifier.OWNER_UID, verifier.OWNER_GID
        # A private fixture boundary replaces /opt and root ownership in CI.
        # The public CLI has no environment override for these trust checks.
        verifier.STAGE_PARENT, verifier.OWNER_UID, verifier.OWNER_GID = self.shared, os.getuid(), os.getgid()
        self.stage = self.shared / 'runtime-deps.Abc123'
        self.stage.mkdir(mode=0o700)
        self.inputs = {'package.json': b'{"name":"fixture","dependencies":{"pkg":"1.0.0"}}\n', 'pnpm-lock.yaml': b'lockfileVersion: 9.0\nfixture: new\n', 'pnpm-workspace.yaml': b'allowBuilds: {}\n'}
        for name, data in self.inputs.items():
            (self.stage / name).write_bytes(data)
            (self.stage / name).chmod(0o600)
        self.modules = self.stage / 'node_modules'
        self.package = self.modules / '.pnpm/pkg@1.0.0/node_modules/pkg'
        self.package.mkdir(parents=True)
        self.index = self.package / 'index.js'
        self.index.write_bytes(b'module.exports = 1;\n')
        self.index.chmod(0o644)
        (self.package / 'run').write_bytes(b'#!/bin/sh\nexit 0\n')
        (self.package / 'run').chmod(0o755)
        (self.modules / 'pkg').symlink_to('.pnpm/pkg@1.0.0/node_modules/pkg', target_is_directory=True)
        self.candidate = self.root / 'candidate'
        self.candidate.mkdir()
        for name, data in self.inputs.items():
            (self.candidate / name).write_bytes(data)

    def tearDown(self):
        verifier.STAGE_PARENT, verifier.OWNER_UID, verifier.OWNER_GID = self.previous_globals
        for folder, dirs, files in os.walk(self.root, topdown=True, followlinks=False):
            Path(folder).chmod(0o700)
            for name in files:
                item = Path(folder) / name
                if not item.is_symlink():
                    item.chmod(0o600)
        self.temporary.cleanup()

    def complete(self):
        return verifier.create(self.stage, verifier.NODE_VERSION, verifier.PNPM_VERSION)

    def marker_change(self, change):
        path = self.stage / verifier.MARKER
        value = json.loads(path.read_text())
        change(value)
        path.write_text(json.dumps(value))

    def archive(self, path=None, duplicate=False, symlink=False):
        path = path or self.shared / 'release.tar.gz'
        with tarfile.open(path, 'w:gz') as archive:
            for name, data in self.inputs.items():
                entry = tarfile.TarInfo(name)
                entry.size = len(data)
                if symlink and name == 'package.json':
                    entry.type = tarfile.SYMTYPE
                    entry.linkname = 'outside'
                    entry.size = 0
                    archive.addfile(entry)
                else:
                    archive.addfile(entry, io.BytesIO(data))
            if duplicate:
                entry = tarfile.TarInfo('package.json')
                entry.size = len(self.inputs['package.json'])
                archive.addfile(entry, io.BytesIO(self.inputs['package.json']))
        return path

    def test_freezes_real_tree_and_preserves_portable_copy_hash(self):
        completed = self.complete()
        self.assertTrue(completed['completed'])
        self.assertEqual(stat.S_IMODE(self.index.stat().st_mode), 0o444)
        self.assertEqual(stat.S_IMODE((self.package / 'run').stat().st_mode), 0o555)
        self.assertEqual(stat.S_IMODE(self.modules.stat().st_mode), 0o555)
        self.assertEqual(stat.S_IMODE((self.stage / verifier.MARKER).stat().st_mode), 0o600)
        self.assertEqual(verifier.verify(self.stage, self.candidate)['treeSha256'], completed['treeSha256'])
        destination = self.root / 'copy/node_modules'
        destination.parent.mkdir()
        subprocess.run(['cp', '-a', str(self.modules), str(destination)], check=True, timeout=10)
        self.assertNotEqual(self.index.stat().st_ino, (destination / self.index.relative_to(self.modules)).stat().st_ino)
        self.assertEqual(verifier.verify(self.stage, self.archive(), destination)['treeSha256'], completed['treeSha256'])
        self.assertTrue((destination / 'pkg').resolve().is_relative_to(destination))
        self.assertEqual(self.index.stat().st_nlink, 1)

    def test_missing_or_existing_marker_cannot_be_assumed_complete(self):
        with self.assertRaises(Exception):
            verifier.verify(self.stage, self.candidate)
        self.complete()
        with self.assertRaisesRegex(verifier.DependencyError, 'marker_exists'):
            self.complete()

    def test_toolchain_and_marker_shape_are_strict(self):
        with self.assertRaisesRegex(verifier.DependencyError, 'toolchain_mismatch'):
            verifier.create(self.stage, 'v22.22.2', '12.3.4')
        self.complete()
        self.marker_change(lambda value: value.update(pnpmVersion='12.3.3'))
        with self.assertRaisesRegex(verifier.DependencyError, 'toolchain_mismatch'):
            verifier.verify(self.stage, self.candidate)
        (self.stage / verifier.MARKER).write_text('{"version":1,"version":1}')
        with self.assertRaisesRegex(verifier.DependencyError, 'marker_invalid'):
            verifier.verify(self.stage, self.candidate)

    def test_each_raw_candidate_metadata_change_is_rejected(self):
        self.complete()
        for name, data in self.inputs.items():
            with self.subTest(name=name):
                (self.candidate / name).write_bytes(data + b'\n')
                with self.assertRaisesRegex(verifier.DependencyError, 'inputs_mismatch'):
                    verifier.verify(self.stage, self.candidate)
                (self.candidate / name).write_bytes(data)

    def test_source_tampering_and_reenabled_write_bits_are_rejected(self):
        self.complete()
        self.index.chmod(0o644)
        with self.assertRaisesRegex(verifier.DependencyError, 'tree_permissions'):
            verifier.verify(self.stage, self.candidate)
        self.index.write_bytes(b'module.exports = 2;\n')
        self.index.chmod(0o444)
        with self.assertRaisesRegex(verifier.DependencyError, 'tree_mismatch'):
            verifier.verify(self.stage, self.candidate)

    def test_copy_tampering_is_rejected_and_stage_unchanged(self):
        self.complete()
        destination = self.root / 'copy'
        shutil.copytree(self.modules, destination, symlinks=True)
        copied = destination / self.index.relative_to(self.modules)
        copied.chmod(0o644)
        copied.write_bytes(b'changed copy')
        copied.chmod(0o444)
        with self.assertRaisesRegex(verifier.DependencyError, 'copy_mismatch'):
            verifier.verify(self.stage, self.candidate, destination)
        self.assertTrue(verifier.verify(self.stage, self.candidate)['verified'])
        self.assertEqual(self.index.read_bytes(), b'module.exports = 1;\n')

    def test_external_symlink_refused_before_any_chmod(self):
        outside = self.root / 'outside.js'
        outside.write_bytes(b'private fixture')
        outside.chmod(0o644)
        (self.package / 'escape').symlink_to(os.path.relpath(outside, self.package))
        with self.assertRaises(Exception):
            self.complete()
        self.assertEqual(stat.S_IMODE(outside.stat().st_mode), 0o644)
        self.assertEqual(stat.S_IMODE(self.index.stat().st_mode), 0o644)
        self.assertFalse((self.stage / verifier.MARKER).exists())

    def test_absolute_symlink_refused_even_inside_stage(self):
        (self.package / 'absolute').symlink_to(self.index)
        with self.assertRaisesRegex(verifier.DependencyError, 'symlink_unsafe'):
            self.complete()

    def test_external_hardlink_refused_without_mutating_old_tree(self):
        previous = self.root / 'old-release.js'
        previous.write_bytes(b'old release bytes')
        previous.chmod(0o644)
        os.link(previous, self.package / 'external-hardlink.js')
        with self.assertRaisesRegex(verifier.DependencyError, 'hardlink_or_size'):
            self.complete()
        self.assertEqual(previous.read_bytes(), b'old release bytes')
        self.assertEqual(stat.S_IMODE(previous.stat().st_mode), 0o644)
        self.assertEqual(stat.S_IMODE(self.index.stat().st_mode), 0o644)

    def test_stage_owner_and_private_mode_are_required(self):
        self.stage.chmod(0o755)
        with self.assertRaisesRegex(verifier.DependencyError, 'stage_permissions'):
            self.complete()
        self.stage.chmod(0o700)
        verifier.OWNER_UID = os.getuid() + 1
        with self.assertRaisesRegex(verifier.DependencyError, 'owner_unsafe'):
            self.complete()

    def test_archive_duplicate_and_link_metadata_are_rejected(self):
        self.complete()
        for duplicate, symlink in [(True, False), (False, True)]:
            with self.subTest(duplicate=duplicate, symlink=symlink):
                with self.assertRaisesRegex(verifier.DependencyError, 'archive_unsafe'):
                    verifier.verify(self.stage, self.archive(duplicate=duplicate, symlink=symlink))

    def test_cli_redacts_native_errors_and_sensitive_paths(self):
        out, error = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(error):
            result = verifier.main(['verify', str(self.root / 'private-path-sentinel'), str(self.candidate)])
        self.assertEqual(result, 1)
        self.assertEqual(out.getvalue(), '')
        self.assertNotIn('private-path-sentinel', error.getvalue())
        self.assertEqual(set(json.loads(error.getvalue())), {'code'})

    def deploy_fixture(self):
        previous = self.root / 'releases/old'
        previous.mkdir(parents=True)
        old_modules = previous / 'node_modules'
        old_modules.mkdir()
        (old_modules / 'old.js').write_bytes(b'old immutable dependency')
        for name, data in self.inputs.items():
            (previous / name).write_bytes(data if name != 'pnpm-lock.yaml' else b'old lock\n')
        (self.root / 'current').symlink_to(previous, target_is_directory=True)
        helper = (SCRIPTS / 'verify-release-dependencies.py').read_text().replace("STAGE_PARENT = Path('/opt/ccpanel/shared')", 'STAGE_PARENT = Path(' + repr(str(self.shared)) + ')').replace('OWNER_UID = 0', 'OWNER_UID = ' + str(os.getuid())).replace('OWNER_GID = 0', 'OWNER_GID = ' + str(os.getgid()))
        helper_path = self.shared / 'verify-release-dependencies.py'
        helper_path.write_text(helper)
        helper_path.chmod(0o600)
        archive_path = self.shared / 'release.tar.gz'
        payload = {**self.inputs, 'scripts/verify-release-dependencies.py': helper.encode(), 'scripts/retain-web-assets.py': b'print("synthetic unrelated asset helper")\n', 'dist/api.mjs': b'// synthetic boundary; Docker must not run\n', 'infra/compose.yaml': b'# synthetic unrelated boundary\n'}
        with tarfile.open(archive_path, 'w:gz') as archive:
            for name, data in payload.items():
                entry = tarfile.TarInfo(name)
                entry.size = len(data)
                entry.mode = 0o644
                archive.addfile(entry, io.BytesIO(data))
        (self.shared / 'runtime.env').write_text('FIXTURE_ONLY=true\n')
        binary = self.root / 'bin'
        binary.mkdir()
        trace = self.root / 'docker.log'
        for name, content in {
            'docker': '#!/bin/sh\nprintf "%s\\n" "$*" >> "$DEPLOY_TRACE"\nexit 73\n',
            'chgrp': '#!/bin/sh\nexit 0\n',
            'stat': '#!/bin/sh\nif [ "$1" = -c ] && [ "$2" = %u ];then echo 0;else exec ' + shutil.which('stat') + ' "$@";fi\n',
        }.items():
            (binary / name).write_text(content)
            (binary / name).chmod(0o700)
        script = self.root / 'deploy.sh'
        script.write_text((SCRIPTS / 'deploy-remote.sh').read_text().replace('/opt/ccpanel', str(self.root)))
        environment = {**os.environ, 'PATH': str(binary) + os.pathsep + os.environ['PATH'], 'CCPANEL_RELEASE': 'new', 'CCPANEL_DEPENDENCIES_DIR': str(self.stage), 'DEPLOY_TRACE': str(trace)}
        return previous, old_modules, archive_path, trace, script, environment

    def test_changed_lock_uses_independent_copy_before_any_service_stop(self):
        previous, old_modules, archive, trace, script, environment = self.deploy_fixture()
        self.complete()
        old_bytes = (old_modules / 'old.js').read_bytes()
        result = subprocess.run(['bash', str(script)], env=environment, capture_output=True, timeout=20)
        self.assertEqual(result.returncode, 73, (result.stdout, result.stderr))
        copied = self.root / 'releases/new/node_modules'
        self.assertTrue(verifier.verify(self.stage, archive, copied)['verified'])
        self.assertEqual((old_modules / 'old.js').read_bytes(), old_bytes)
        self.assertEqual((old_modules / 'old.js').stat().st_nlink, 1)
        self.assertNotEqual(self.index.stat().st_ino, (copied / self.index.relative_to(self.modules)).stat().st_ino)
        self.assertEqual(self.index.stat().st_nlink, 1)
        calls = trace.read_text().splitlines()
        self.assertEqual(len(calls), 1)
        self.assertNotIn(' stop ', calls[0])
        self.assertNotIn('pnpm', calls[0])
        self.assertNotIn('npm install', (SCRIPTS / 'deploy-remote.sh').read_text())

    def test_incomplete_stage_blocks_deploy_before_new_directory_or_docker(self):
        _, _, _, trace, script, environment = self.deploy_fixture()
        result = subprocess.run(['bash', str(script)], env=environment, capture_output=True, timeout=20)
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse((self.root / 'releases/new').exists())
        self.assertFalse(trace.exists())

    def test_same_lock_but_other_dependency_metadata_change_still_requires_stage(self):
        previous, _, _, trace, script, environment = self.deploy_fixture()
        (previous / 'pnpm-lock.yaml').write_bytes(self.inputs['pnpm-lock.yaml'])
        (previous / 'pnpm-workspace.yaml').write_bytes(b'changed allowBuilds\n')
        environment.pop('CCPANEL_DEPENDENCIES_DIR')
        result = subprocess.run(['bash', str(script)], env=environment, capture_output=True, timeout=20)
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse((self.root / 'releases/new').exists())
        self.assertFalse(trace.exists())

    def test_unchanged_metadata_keeps_read_only_hardlink_reuse(self):
        previous, old_modules, _, trace, script, environment = self.deploy_fixture()
        (previous / 'pnpm-lock.yaml').write_bytes(self.inputs['pnpm-lock.yaml'])
        environment.pop('CCPANEL_DEPENDENCIES_DIR')
        result = subprocess.run(['bash', str(script)], env=environment, capture_output=True, timeout=20)
        self.assertEqual(result.returncode, 73, (result.stdout, result.stderr))
        copied = self.root / 'releases/new/node_modules/old.js'
        self.assertEqual(copied.stat().st_ino, (old_modules / 'old.js').stat().st_ino)
        self.assertEqual(copied.read_bytes(), b'old immutable dependency')
        self.assertEqual(len(trace.read_text().splitlines()), 1)


if __name__ == '__main__':
    unittest.main(verbosity=2)
