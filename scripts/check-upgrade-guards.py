"""Linux CI: pre-stop rejection paths with synthetic files and mocked tools."""
import hashlib, io, json, os, pathlib, subprocess, tarfile, tempfile

assert os.name == 'posix', 'Linux CI only; requires POSIX paths and shell tools'
source = pathlib.Path(__file__).with_name('upgrade-product-preview.sh').read_text()
cases = ['unsupported_pair', 'archive_hash', 'archive_schema', 'helper_hash', 'active_capacity', 'retention_enabled', 'source_schema']
pairs=[(19,20),(20,21),(21,21),(21,22),(22,22)]
for source_version,target_version in pairs:
    for case in cases:
        with tempfile.TemporaryDirectory(prefix='ccpanel-upgrade-guard-') as folder:
            root = pathlib.Path(folder)
            shared = root / 'shared'
            shared.mkdir()
            (root / 'current' / 'infra').mkdir(parents=True)
            binary = root / 'bin'
            binary.mkdir()
            trace = root / 'docker.log'
            trace.write_text('')
            docker = binary / 'docker'
            docker.write_text('#!/bin/sh\nprintf "%s\\n" "$*" >> "$GUARD_TRACE"\nprintf "%s\\n" "$GUARD_SCHEMA"\n')
            docker.chmod(0o700)
            systemctl = binary / 'systemctl'
            systemctl.write_text('#!/bin/sh\nif [ "$GUARD_ACTIVE" = true ]; then echo synthetic-capacity-running; fi\nexit 0\n')
            systemctl.chmod(0o700)
            helper = b'# synthetic helper; these rejection paths must not execute it\n'
            (shared / 'operational-collection-env.py').write_bytes(b'mismatched' if case == 'helper_hash' else helper)
            (shared / 'runtime.env').write_text('RETENTION_ENABLED=' + ('true' if case == 'retention_enabled' else 'false') + '\n')
            archive = shared / 'release.tar.gz'
            with tarfile.open(str(archive), 'w:gz') as tar:
                for name, content in [('dist/schema-version.txt', str(target_version-1 if case == 'archive_schema' else target_version).encode()+b'\n'), ('scripts/operational-collection-env.py', helper)]:
                    info = tarfile.TarInfo(name)
                    info.size = len(content)
                    tar.addfile(info, io.BytesIO(content))
            script = root / 'upgrade.sh'
            # All absolute fixture paths remain inside this temporary directory.
            script.write_text(source.replace('/opt/ccpanel', str(root)))
            env = dict(os.environ, PATH=str(binary) + os.pathsep + os.environ['PATH'], GUARD_TRACE=str(trace), GUARD_SCHEMA=str(source_version-1), GUARD_ACTIVE='true' if case == 'active_capacity' else 'false', CCPANEL_SOURCE_SCHEMA='21' if case == 'unsupported_pair' else str(source_version), CCPANEL_TARGET_SCHEMA='20' if case == 'unsupported_pair' else str(target_version), CCPANEL_RELEASE='synthetic-release', CCPANEL_ARCHIVE_SHA256='0' * 64 if case == 'archive_hash' else hashlib.sha256(archive.read_bytes()).hexdigest())
            result = subprocess.run(['bash', str(script), 'prepare'], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10)
            assert result.returncode != 0, case
            calls = trace.read_text().splitlines()
            assert not any('stop' in call or ' up ' in call or ' run ' in call for call in calls), (case, calls)
            if case == 'source_schema':
                assert len(calls) == 1 and 'SELECT max(version)' in calls[0], calls
            else:
                assert calls == [], (case, calls)
            expected = {'unsupported_pair': b'Unsupported trusted upgrade pair', 'archive_schema': b'Archive schema does not match', 'helper_hash': b'archive-matching collection helper', 'active_capacity': b'Capacity sampler still running', 'retention_enabled': b'Disable physical retention'}
            if case in expected:
                assert expected[case] in result.stderr, (case, result.stderr)
print(json.dumps({'passed': True, 'synthetic_host_tools': True, 'pre_stop_rejections': cases, 'pairs': pairs, 'rejection_count': len(cases)*len(pairs), 'real_services_touched': False}))
