"""Execute the installer's real extraction and cancellation controllers locally.

No Docker daemon, real pressure, service state or /opt paths are mutated. The
native Bash controller is exercised against explicitly controlled CLI fixtures.
"""
import copy,hashlib,io,json,os,pathlib,shlex,shutil,stat,subprocess,sys,tarfile,tempfile

ROOT=pathlib.Path(__file__).resolve().parents[1]
SCRIPT=(ROOT/'scripts/prepare-release-dependencies.sh').read_text(encoding='utf-8')
if os.name!='posix':
    print(json.dumps({'installer_checks':'skipped','reason':'native_posix_required'}));raise SystemExit(0)
body=SCRIPT.split("<<'PY'\n",1)[1].split('\nPY\n',1)[0]
original={name:(ROOT/name).read_bytes() for name in ['package.json','pnpm-lock.yaml','pnpm-workspace.yaml']}
verifier_bytes=b'# immutable verifier fixture\n'

def member(tar,name,data=b'x',kind=tarfile.REGTYPE,link=''):
    info=tarfile.TarInfo(name);info.type=kind;info.mode=0o644;info.linkname=link
    info.size=len(data) if kind==tarfile.REGTYPE else 0;tar.addfile(info,io.BytesIO(data) if info.size else None)

with tempfile.TemporaryDirectory(prefix='ccpanel-deps-installer-') as temporary:
    root=pathlib.Path(temporary);outside=root/'outside';outside.write_text('keep-original-release')
    verifier=root/'verify-release-dependencies.py';verifier.write_bytes(verifier_bytes)
    def archive(name,change=None):
        data=dict(original);extras=[];schema=b'22\n';helper=verifier_bytes
        if change=='schema':schema=b'21\n'
        elif change=='verifier':helper=b'# another helper\n'
        elif change=='missing':del data['pnpm-lock.yaml']
        elif change=='workspace':data['pnpm-workspace.yaml']=data['pnpm-workspace.yaml'].replace(b'ssh2: false',b'ssh2: true')
        elif change in ('dependency','lifecycle'):
            package=json.loads(data['package.json']);package=copy.deepcopy(package)
            if change=='dependency':package['dependencies']['ssh2']='0.0.0-unreviewed'
            else:package['scripts']['postinstall']='never-run-this-fixture'
            data['package.json']=json.dumps(package).encode()
        elif change=='oversize':data['package.json']=b' '*65537
        elif change=='duplicate':extras.append(('package.json',data['package.json'],tarfile.REGTYPE,''))
        elif change=='traversal':extras.append(('../outside',b'changed',tarfile.REGTYPE,''))
        elif change=='absolute':extras.append(('/outside',b'changed',tarfile.REGTYPE,''))
        elif change=='env':extras.append(('infra/.env',b'never-extract',tarfile.REGTYPE,''))
        elif change=='symlink':extras.append(('unsafe-link',b'',tarfile.SYMTYPE,'../outside'))
        target=root/(name+'.tar.gz')
        with tarfile.open(target,'w:gz') as tar:
            for file,content in data.items():member(tar,file,content)
            member(tar,'dist/schema-version.txt',schema);member(tar,'scripts/verify-release-dependencies.py',helper)
            for args in extras:member(tar,*args)
        target.chmod(0o600);return target
    cases=0
    for change in [None,'hash','schema','verifier','missing','workspace','dependency','lifecycle','oversize','duplicate','traversal','absolute','env','symlink']:
        file=archive('archive-'+str(cases),change);stage=root/('stage-'+str(cases));stage.mkdir(mode=0o700)
        digest=hashlib.sha256(file.read_bytes()).hexdigest() if change!='hash' else '0'*64
        run=subprocess.run([sys.executable,'-',str(file),digest,str(stage),str(verifier)],input=body,text=True,capture_output=True,timeout=10)
        if change is None:
            assert run.returncode==0,run.stderr
            assert {p.name for p in stage.iterdir()}==set(original)
            for name,expected in original.items():
                item=stage/name;assert item.read_bytes()==expected
                assert stat.S_IMODE(item.stat().st_mode)==0o600 and item.stat().st_nlink==1
        else:
            assert run.returncode!=0,change;assert not list(stage.iterdir()),change
        assert outside.read_text()=='keep-original-release';cases+=1

    def function(name):
        start=SCRIPT.index(name+'(){\n');return SCRIPT[start:SCRIPT.index('\n}',start)+2]
    functions='\n'.join(function(name) for name in ['owned_container','remove_owned','run_container'])
    binary=root/'bin';binary.mkdir()
    docker=binary/'docker'
    docker.write_text('''#!/usr/bin/env python3
import json,os,pathlib,sys,time
root=pathlib.Path(os.environ['FAKE_ROOT']);state=root/'running.json';events=root/'events.jsonl';args=sys.argv[1:]
def log(value):
    with events.open('a') as f:f.write(json.dumps(value)+'\\n')
if args[0]=='inspect':
    name=args[-1]
    if not state.exists():print('Error: No such object: '+name,file=sys.stderr);raise SystemExit(1)
    if os.environ.get('FAKE_INSPECT_UNCERTAIN')=='1':raise SystemExit(124)
    value=json.loads(state.read_text());assert value['name']==name;print(value['owner']);raise SystemExit(0)
if args[0]=='run':
    name=args[args.index('--name')+1];label=args[args.index('--label')+1].split('=',1)[1]
    assert not state.exists();state.write_text(json.dumps({'name':name,'owner':os.environ.get('FAKE_OWNER',label)}));log(['run',name])
    start=time.monotonic()
    while state.exists():
        if os.environ.get('FAKE_MODE')=='success' and time.monotonic()-start>.2:state.unlink();raise SystemExit(0)
        time.sleep(.02)
    raise SystemExit(137)
if args[:2]==['rm','-f']:
    value=json.loads(state.read_text());assert value['name']==args[-1];log(['rm',args[-1]]);state.unlink();raise SystemExit(0)
raise SystemExit(9)
''',encoding='utf-8');docker.chmod(0o755)
    awk=binary/'awk';awk.write_text('#!/bin/sh\nprintf "%s\\n" "${FAKE_AVAILABLE:-500000}"\n',encoding='utf-8');awk.chmod(0o755)
    systemctl=binary/'systemctl';systemctl.write_text('#!/usr/bin/env python3\nimport os,sys,time\nif os.environ.get("FAKE_SYSTEMCTL_SLEEP")=="true":time.sleep(10)\nsys.exit(int(os.environ.get("FAKE_SYSTEMCTL_STATUS","0")))\n',encoding='utf-8');systemctl.chmod(0o755)
    capacity_guard=SCRIPT.split('capacity_units=$(',1)[1].split('\ntest "$(awk',1)[0]
    capacity_guard='capacity_units=$('+capacity_guard
    for name,extra,allowed in [('empty_success',{},True),('empty_failure',{'FAKE_SYSTEMCTL_STATUS':'1'},False),('real_timeout',{'FAKE_SYSTEMCTL_SLEEP':'true'},False)]:
        env={**os.environ,'PATH':str(binary)+os.pathsep+os.environ['PATH'],**extra}
        run=subprocess.run(['bash','-c','set -e\n'+capacity_guard+'\nprintf "PREFLIGHT_ACCEPTED\\n"\n'],env=env,text=True,capture_output=True,timeout=8)
        assert (run.returncode==0)==allowed,name
        assert (run.stdout=='PREFLIGHT_ACCEPTED\n')==allowed,name
        cases+=1
    # These run the actual shell functions, native timeout and background watcher.
    # Fake CLI outcomes are explicit; no production pressure or Docker behavior
    # is inferred from this controller-only proof.
    for name,extra,budget,success in [
        ('success',{},4,True),
        ('resource',{'FAKE_AVAILABLE':'100000'},5,False),
        ('missing_memory',{'FAKE_AVAILABLE':'invalid'},5,False),
        ('timeout',{},1,False),
        ('uncertain_owner',{'FAKE_INSPECT_UNCERTAIN':'1'},1,False),
        ('wrong_owner',{'FAKE_OWNER':'another-controller','FAKE_AVAILABLE':'100000'},1,False),
    ]:
        sandbox=root/name;sandbox.mkdir(mode=0o700);stage=sandbox/'stage';stage.mkdir(mode=0o700)
        env={**os.environ,'PATH':str(binary)+os.pathsep+os.environ['PATH'],'FAKE_ROOT':str(sandbox),'FAKE_MODE':'success' if success else 'blocked',**extra}
        shell='set -u\n'+functions+'\n'+f'stage={shlex.quote(str(stage))};owner=owned-test-controller;watcher="";active_container="";deadline=$((SECONDS+{budget}))\n'
        shell+='run_container ccpanel-deps-test fixture-image\nstatus=$?\nprintf "CONTROL_STATUS=%s\\n" "$status"\nexit "$status"\n'
        run=subprocess.run(['bash','-c',shell],env=env,text=True,capture_output=True,timeout=15)
        assert (run.returncode==0)==success,(name,run.returncode,run.stderr)
        events=[json.loads(line) for line in (sandbox/'events.jsonl').read_text().splitlines()]
        assert events[0]==['run','ccpanel-deps-test'];assert all(row[1]=='ccpanel-deps-test' for row in events)
        if name in ('resource','missing_memory','timeout'):
            assert ['rm','ccpanel-deps-test'] in events and not (sandbox/'running.json').exists(),name
        elif name in ('uncertain_owner','wrong_owner'):
            assert all(row[0]!='rm' for row in events) and (sandbox/'running.json').exists(),name
        if name in ('resource','missing_memory'):assert (stage/'resource-aborted').read_text()=='resource_safety\n'
        cases+=1
    assert outside.read_text()=='keep-original-release'
print(json.dumps({'installer_guard_checks_passed':cases,'actual_archive_extraction':True,'actual_native_bash_controller':True,'fake_cli_scope_only':True,'no_docker_or_host_pressure_claim':True}))
