"""Linux CI: actual apply control flow, synthetic backup proof and host tools."""
import hashlib,io,json,os,pathlib,subprocess,tarfile,tempfile
assert os.name=='posix','Linux CI only'
scripts=pathlib.Path(__file__).parent;source=(scripts/'upgrade-product-preview.sh').read_text();helper=(scripts/'operational-collection-env.py').read_text().encode()
cases=['valid','unexpected_dlq','changed_original','bad_backup_hash','collector_enabled']
for case in cases:
    with tempfile.TemporaryDirectory(prefix='ccpanel-post22-') as folder:
        root=pathlib.Path(folder);shared=root/'shared';infra=root/'current'/'infra';binary=root/'bin';evidence=shared/'product-upgrade.Abc123';backup=shared/'backups'/'20260101T000000Z-1234abcd'
        for p in (shared,infra,binary,evidence,backup,root/'releases'):p.mkdir(parents=True,exist_ok=True)
        (shared/'operational-collection-env.py').write_bytes(helper)
        enabled=case=='collector_enabled';initial='RETENTION_ENABLED=false\nOPERATIONAL_COLLECTION_ENABLED='+str(enabled).lower()+'\n'
        for p in (shared/'runtime.env',infra/'.env'):p.write_text(initial)
        trace=root/'docker.log';events=root/'events.log';schema=root/'schema.txt'
        for p in (trace,events):p.write_text('')
        schema.write_text('21\n')
        archive=shared/'release.tar.gz'
        with tarfile.open(archive,'w:gz') as tar:
            for name,content in [('dist/schema-version.txt',b'22\n'),('scripts/operational-collection-env.py',helper)]:
                info=tarfile.TarInfo(name);info.size=len(content);tar.addfile(info,io.BytesIO(content))
        sha=hashlib.sha256(archive.read_bytes()).hexdigest()
        for name,text in [('target-release.txt','synthetic-release'),('source-schema.txt','21'),('target-schema.txt','22'),('archive.sha256',sha),('previous-release.txt',str(root/'current')),('backup.path',str(backup)),('before.jsonl','stable'),('fingerprint.sql',"SELECT 'synthetic-projection';")]:
            (evidence/name).write_text(text+'\n')
        proof={'offhost_verified':True,'isolated_restore_passed':True}
        for key,name in [('plaintext_sha256','database.dump'),('ciphertext_sha256','database.ccpbak'),('fingerprint_sha256','fingerprint.jsonl')]:
            content=('synthetic '+name).encode();(backup/name).write_bytes(content);proof[key]=hashlib.sha256(content).hexdigest()
        (evidence/'backup-verified.json').write_text(json.dumps(proof))
        if case=='bad_backup_hash':(backup/'database.dump').write_bytes(b'changed after proof')
        docker=binary/'docker';docker.write_text('''#!/usr/bin/env python3
import os,pathlib,sys
args=' '.join(sys.argv[1:]);schema=pathlib.Path(os.environ['GUARD_SCHEMA_FILE']).read_text().strip()
with open(os.environ['GUARD_TRACE'],'a') as f:f.write(args+'\\n')
if args.startswith('inspect '):print('false')
elif 'SELECT max(version)' in args:print(schema)
elif 'FROM dlq_alert_states' in args:print('1' if os.environ['GUARD_CASE']=='unexpected_dlq' else '0')
elif 'psql ' in args:print('changed' if schema=='22' and os.environ['GUARD_CASE']=='changed_original' else 'stable')
''');docker.chmod(0o700)
        systemctl=binary/'systemctl';systemctl.write_text('#!/bin/sh\nexit 0\n');systemctl.chmod(0o700)
        curl=binary/'curl';curl.write_text('#!/bin/sh\nprintf "ready\\n" >> "$GUARD_EVENTS"\n');curl.chmod(0o700)
        (shared/'deploy-remote.sh').write_text('printf "22\\n" > "$GUARD_SCHEMA_FILE"\nprintf "deploy\\n" >> "$GUARD_EVENTS"\n')
        script=root/'upgrade.sh';script.write_text(source.replace('/opt/ccpanel',str(root)))
        env=dict(os.environ,PATH=str(binary)+os.pathsep+os.environ['PATH'],GUARD_TRACE=str(trace),GUARD_EVENTS=str(events),GUARD_SCHEMA_FILE=str(schema),GUARD_CASE=case,CCPANEL_SOURCE_SCHEMA='21',CCPANEL_TARGET_SCHEMA='22',CCPANEL_RELEASE='synthetic-release',CCPANEL_ARCHIVE_SHA256=sha,CCPANEL_UPGRADE_DIR=str(evidence))
        result=subprocess.run(['bash',str(script),'apply'],env=env,capture_output=True,timeout=10)
        calls=trace.read_text().splitlines();flow=events.read_text().splitlines()
        assert (shared/'runtime.env').read_text()==initial and (infra/'.env').read_text()==initial
        if case=='valid':
            assert result.returncode==0,result.stderr
            assert flow==['deploy','ready'] and not any('compose stop ' in c for c in calls)
            assert (evidence/'before.jsonl').read_bytes()==(evidence/'after.jsonl').read_bytes()
        elif case in ('unexpected_dlq','changed_original'):
            assert result.returncode!=0 and flow==['deploy'],(case,result.stderr,flow)
            assert calls.count('compose stop api worker')==1,calls
            expected=b'Unexpected DLQ collection' if case=='unexpected_dlq' else b'Original data changed'
            assert expected in result.stderr
        else:
            assert result.returncode!=0 and flow==[],(case,result.stderr,flow)
            assert not any('compose stop ' in c or 'compose run ' in c or 'compose up ' in c for c in calls),calls
            assert schema.read_text().strip()=='21'
print(json.dumps({'passed':True,'synthetic_host_tools':True,'schema21_to22_apply_cases':cases,'unexpected_dlq_stops_services':True,'changed_original_stops_services':True,'bad_proof_or_active_collection_blocks_deploy':True,'real_services_touched':False}))
