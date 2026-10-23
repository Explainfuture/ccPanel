"""Linux CI: actual prepare script/helper with synthetic host tools, no services."""
import hashlib, io, json, os, pathlib, subprocess, tarfile, tempfile

assert os.name == 'posix', 'Linux CI only'
scripts=pathlib.Path(__file__).parent
source=(scripts/'upgrade-product-preview.sh').read_text()
helper=(scripts/'operational-collection-env.py').read_text().encode()
exporter=b'// immutable synthetic archive exporter\n'
checked=[]
for source_version,target_version in ((20,21),(21,21),(21,22),(22,22)):
    for ledger_kind in ('complete','legacy','missing_inventory'):
        with tempfile.TemporaryDirectory(prefix='ccpanel-upgrade21-') as folder:
            root=pathlib.Path(folder);shared=root/'shared';infra=root/'current'/'infra';binary=root/'bin'
            for p in (shared,infra,binary,root/'releases'):p.mkdir(parents=True)
            trace=root/'docker.log';trace.write_text('')
            (shared/'operational-collection-env.py').write_bytes(helper)
            initial='RETENTION_ENABLED=false\nOPERATIONAL_COLLECTION_ENABLED=true\nPUBLIC_ORIGIN=https://synthetic.invalid\n'
            for p in (shared/'runtime.env',infra/'.env'):p.write_text(initial)
            ledger={'schema_version':1 if ledger_kind=='legacy' else 2}
            if ledger_kind!='missing_inventory':ledger['retention']=[]
            docker=binary/'docker'
            docker.write_text('''#!/bin/sh
printf '%s\\n' "$*" >> "$GUARD_TRACE"
case "$*" in
  *'SELECT max(version)'*) printf '%s\\n' "$GUARD_SCHEMA";;
  *'node dist/deletion-ledger.mjs export'*) printf '%s\\n' "$GUARD_LEDGER";;
  *'psql '*) cat >/dev/null;;
esac
''');docker.chmod(0o700)
            systemctl=binary/'systemctl';systemctl.write_text('#!/bin/sh\nexit 0\n');systemctl.chmod(0o700)
            (shared/'backup-remote.sh').write_text('printf "%s\\n" /synthetic/private-backup\n')
            archive=shared/'release.tar.gz'
            with tarfile.open(archive,'w:gz') as tar:
                for name,content in [('dist/schema-version.txt',(str(target_version)+'\n').encode()),('scripts/operational-collection-env.py',helper),('dist/deletion-ledger.mjs',exporter)]:
                    info=tarfile.TarInfo(name);info.size=len(content);tar.addfile(info,io.BytesIO(content))
            script=root/'upgrade.sh';script.write_text(source.replace('/opt/ccpanel',str(root)))
            env=dict(os.environ,PATH=str(binary)+os.pathsep+os.environ['PATH'],GUARD_TRACE=str(trace),GUARD_SCHEMA=str(source_version),GUARD_LEDGER=json.dumps(ledger),CCPANEL_SOURCE_SCHEMA=str(source_version),CCPANEL_TARGET_SCHEMA=str(target_version),CCPANEL_RELEASE='synthetic-release',CCPANEL_ARCHIVE_SHA256=hashlib.sha256(archive.read_bytes()).hexdigest())
            result=subprocess.run(['bash',str(script),'prepare'],env=env,capture_output=True,timeout=10)
            calls=trace.read_text().splitlines()
            assert calls.count('compose stop api worker')==1,calls
            exports=[c for c in calls if 'node dist/deletion-ledger.mjs export' in c]
            assert len(exports)==1 and '/source-ledger-exporter.mjs:/app/dist/deletion-ledger.mjs:ro' in exports[0],calls
            evidence=pathlib.Path((shared/'product-upgrade.path').read_text().strip())
            assert evidence.is_relative_to(shared) and evidence.name.startswith('product-upgrade.')
            assert (evidence/'source-ledger-exporter.mjs').read_bytes()==exporter
            assert json.loads((evidence/'source-ledger.json').read_text())==ledger
            assert (evidence/'target-schema.txt').read_text().strip()==str(target_version)
            for p in (shared/'runtime.env',infra/'.env'):
                assert p.read_text()=='RETENTION_ENABLED=false\nPUBLIC_ORIGIN=https://synthetic.invalid\nOPERATIONAL_COLLECTION_ENABLED=false\n'
                assert p.stat().st_mode & 0o777 == 0o600
            assert not any('deploy-remote' in c or 'compose up ' in c for c in calls)
            assert result.returncode==0 if ledger_kind=='complete' else result.returncode!=0,(ledger_kind,result.stderr)
            if ledger_kind=='complete':
                # No proof means no apply, even with otherwise coherent metadata.
                trace.write_text('');env['CCPANEL_UPGRADE_DIR']=str(evidence)
                applied=subprocess.run(['bash',str(script),'apply'],env=env,capture_output=True,timeout=10)
                assert applied.returncode!=0
                assert trace.read_text().splitlines()==[calls[0]]
                assert not (root/'releases'/'synthetic-release').exists()
            checked.append([source_version,target_version,ledger_kind])
print(json.dumps({'passed':True,'synthetic_host_tools':True,'actual_collection_helper':True,'archive_exporter_selected':True,'incomplete_v2_refused':True,'missing_backup_proof_refused':True,'cases':checked,'real_services_touched':False}))
