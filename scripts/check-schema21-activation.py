"""Linux CI: supported activation and rejection paths, mocked host services."""
import json,os,pathlib,subprocess,tempfile
assert os.name=='posix','Linux CI only'
scripts=pathlib.Path(__file__).parent
cases=[]
for version in (21,22):
    for kind in ('operational-collection','retention'):
        for case in ('build_mismatch','database_mismatch','fingerprint_mismatch','missing_proof','valid'):
            with tempfile.TemporaryDirectory(prefix='ccpanel-activate21-') as folder:
                root=pathlib.Path(folder);shared=root/'shared';release=root/'releases'/'synthetic-release';infra=release/'infra';binary=root/'bin';evidence=shared/'product-upgrade.Abc123'
                for p in (shared,infra,binary,evidence,release/'dist',release/'scripts'):p.mkdir(parents=True,exist_ok=True)
                (root/'current').symlink_to(release,target_is_directory=True)
                for name,text in [('target-release.txt','synthetic-release'),('target-schema.txt',str(version)),('before.jsonl','same'),('after.jsonl','changed' if case=='fingerprint_mismatch' else 'same')]:
                    (evidence/name).write_text(text+'\n')
                if case!='missing_proof':(evidence/'backup-verified.json').write_text('{"synthetic_fixture":true}')
                (release/'dist'/'schema-version.txt').write_text(str(version-1 if case=='build_mismatch' else version)+'\n')
                initial='RETENTION_ENABLED=false\nOPERATIONAL_COLLECTION_ENABLED=false\nPUBLIC_ORIGIN=https://synthetic.invalid\n'
                for p in (shared/'runtime.env',infra/'.env'):p.write_text(initial)
                (release/'scripts'/'operational-collection-env.py').write_text((scripts/'operational-collection-env.py').read_text())
                trace=root/'docker.log';trace.write_text('')
                docker=binary/'docker';docker.write_text('''#!/usr/bin/env python3
import os,pathlib,sys
args=' '.join(sys.argv[1:])
with open(os.environ['GUARD_TRACE'],'a') as f:f.write(args+'\\n')
if 'SELECT max(version)' in args:print(os.environ['GUARD_SCHEMA'])
elif 'process.env.OPERATIONAL_COLLECTION_ENABLED' in args:
 print(next(x.split('=',1)[1] for x in pathlib.Path(os.environ['GUARD_RUNTIME']).read_text().splitlines() if x.startswith('OPERATIONAL_COLLECTION_ENABLED=')))
''');docker.chmod(0o700)
                for name in ('systemctl','curl'):
                    p=binary/name;p.write_text('#!/bin/sh\nexit 0\n');p.chmod(0o700)
                script=root/'activate.sh';script.write_text((scripts/('enable-'+kind+'-preview.sh')).read_text().replace('/opt/ccpanel',str(root)))
                env=dict(os.environ,PATH=str(binary)+os.pathsep+os.environ['PATH'],GUARD_TRACE=str(trace),GUARD_SCHEMA=str(version-1 if case=='database_mismatch' else version),GUARD_RUNTIME=str(shared/'runtime.env'),CCPANEL_EXPECTED_RELEASE='synthetic-release',CCPANEL_UPGRADE_DIR=str(evidence),CCPANEL_COLLECTION_STATE='true',CCPANEL_MAINTENANCE_STATE='true')
                result=subprocess.run(['bash',str(script)],env=env,capture_output=True,timeout=10)
                calls=trace.read_text().splitlines();updated=(shared/'runtime.env').read_text()
                assert (infra/'.env').read_text()==updated
                if case=='valid':
                    assert result.returncode==0,(kind,result.stderr)
                    expected='OPERATIONAL_COLLECTION_ENABLED=true' if kind=='operational-collection' else 'RETENTION_ENABLED=true'
                    assert expected in updated.splitlines()
                    assert sum('compose up ' in call for call in calls)==1,calls
                    assert (shared/'runtime.env').stat().st_mode & 0o777==0o600
                else:
                    assert result.returncode!=0,(kind,case)
                    assert updated==initial
                    assert not any('compose up ' in call or 'compose run ' in call for call in calls),calls
                cases.append([version,kind,case])
print(json.dumps({'passed':True,'synthetic_host_tools':True,'actual_private_env_updates':True,'supported_activation_cases':cases,'real_services_touched':False}))
