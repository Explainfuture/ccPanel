"""Real input/artifact tests. These fixtures never run a network install."""
import hashlib,io,json,os,pathlib,subprocess,tarfile,tempfile
ROOT=pathlib.Path(__file__).resolve().parents[1]
SCRIPT=(ROOT/'scripts/build-release-dependencies.sh').read_text(encoding='utf-8')
if os.name!='posix':
    print(json.dumps({'builder_checks':'skipped','reason':'native_linux_required'}));raise SystemExit(0)
blocks=[];remaining=SCRIPT
while "<<'PY'\n" in remaining:
    rest=remaining.split("<<'PY'\n",1)[1];body,remaining=rest.split('\nPY\n',1);blocks.append(body)
assert len(blocks)==2
inputs={name:(ROOT/name).read_bytes() for name in ['package.json','pnpm-lock.yaml','pnpm-workspace.yaml']}
def sha(data):return hashlib.sha256(data).hexdigest()
def run(body,args):return subprocess.run(['python3','-',*map(str,args)],input=body,text=True,capture_output=True,timeout=15)
def member(tar,name,data):
    info=tarfile.TarInfo(name);info.mode=0o644;info.size=len(data);tar.addfile(info,io.BytesIO(data))
cases=0
with tempfile.TemporaryDirectory(prefix='ccpanel-offhost-deps-check-') as temporary:
    root=pathlib.Path(temporary);header=b'\x7fELF\x02\x01'+b'\0'*12+b'\x3e\x00'
    node=root/'node';node.write_bytes(header+b'node-fixture');pnpm=root/'pnpm';pnpm.write_bytes(header+b'pnpm-fixture')
    for name,change in [('valid',None),('bad_archive_pin','pin'),('duplicate_metadata','duplicate'),('unreviewed_dependency','dependency'),('wrong_workspace','workspace'),('windows_tool','windows')]:
        folder=root/name;folder.mkdir();payload=folder/'payload';payload.mkdir(mode=0o700);archive=folder/'candidate.tar.gz';data=dict(inputs)
        if change=='dependency':
            value=json.loads(data['package.json']);value['dependencies']['ssh2']='unreviewed';data['package.json']=json.dumps(value).encode()
        if change=='workspace':data['pnpm-workspace.yaml']=data['pnpm-workspace.yaml'].replace(b'ssh2: false',b'ssh2: true')
        with tarfile.open(archive,'w:gz') as tar:
            for file,content in data.items():member(tar,file,content)
            if change=='duplicate':member(tar,'package.json',data['package.json'])
        native=node
        if change=='windows':native=folder/'node.exe';native.write_bytes(b'MZ-windows-fixture')
        digest=sha(archive.read_bytes()) if change!='pin' else '0'*64
        result=run(blocks[0],[native,sha(native.read_bytes()),pnpm,sha(pnpm.read_bytes()),archive,digest,payload])
        if change is None:
            assert result.returncode==0,result.stderr
            assert {p.name for p in payload.iterdir()}==set(inputs)
            assert json.loads(result.stdout)=={name:sha(value) for name,value in inputs.items()}
        else:assert result.returncode!=0 and not list(payload.iterdir()),name
        cases+=1
    def fixture(name,change=None):
        work=root/name;work.mkdir();payload=work/'payload';payload.mkdir(mode=0o700)
        for file,content in inputs.items():(payload/file).write_bytes(content)
        modules=payload/'node_modules';modules.mkdir(mode=0o755);package=modules/'example';package.mkdir(mode=0o755)
        (package/'index.js').write_bytes(b'export const value=1;\n');(package/'run').write_bytes(b'#!/bin/sh\nexit 0\n');(package/'run').chmod(0o755)
        (modules/'alias').symlink_to('example',target_is_directory=True)
        source=work/'inputs.json';source.write_text(json.dumps({name:sha(value) for name,value in inputs.items()}))
        output=work/'published-dependencies.tar.gz';outside=work/'outside';outside.write_text('keep')
        if change=='escape':(modules/'escape').symlink_to('../../outside')
        elif change=='hardlink':os.link(outside,package/'hardlink')
        elif change=='fifo':os.mkfifo(package/'pipe')
        elif change=='extra':(payload/'private-env').write_text('never-pack')
        elif change=='input_change':(payload/'package.json').write_text('{}')
        elif change=='existing_output':output.write_text('keep-output')
        elif change=='completed_marker':(payload/'.completed.json').write_text('{}')
        result=run(blocks[1],[payload,source,'a'*64,'b'*64,'c'*64,'v22.22.3','12.3.4',work,output])
        return work,payload,output,outside,result
    hashes=[]
    for name in ['first','second']:
        work,payload,output,outside,result=fixture(name);assert result.returncode==0,result.stderr
        receipt=json.loads(pathlib.Path(str(output)+'.receipt.json').read_text());assert receipt['archiveSha256']==sha(output.read_bytes()) and receipt['archiveBytes']==output.stat().st_size
        assert receipt['pnpmBinarySha256']=='c'*64 and 'pnpmEntrySha256' not in receipt
        with tarfile.open(output) as tar:
            members=tar.getmembers();assert members[0].name=='build-toolchain.json'
            raw=tar.extractfile(members[0]).read();manifest=json.loads(raw);assert receipt['buildToolchainSha256']==sha(raw)
            assert [m.name for m in members[1:]]==[entry['path'] for entry in manifest['files']]
            assert [entry['path'] for entry in manifest['files']]==sorted(entry['path'] for entry in manifest['files'])
            assert manifest['inputs']=={name:sha(value) for name,value in inputs.items()}
            assert manifest['candidateArchiveSha256']=='a'*64 and manifest['nodeBinarySha256']=='b'*64 and manifest['pnpmBinarySha256']=='c'*64
            for entry,item in zip(manifest['files'],members[1:]):
                assert item.mode==entry['mode'] and item.uid==0 and item.gid==0 and item.mtime==0 and item.uname==item.gname==''
                if entry['kind']=='file':assert item.isfile() and sha(tar.extractfile(item).read())==entry['sha256'] and item.size==entry['size']
                elif entry['kind']=='directory':assert item.isdir() and entry['size']==0
                else:assert item.issym() and item.linkname==entry['target'] and entry['size']==len(item.linkname.encode())
            assert not any('.completed' in m.name or '/store/' in m.name for m in members)
        hashes.append(receipt['archiveSha256']);assert outside.read_text()=='keep';cases+=1
    assert hashes[0]==hashes[1],'archive bytes depend on temp path/mtime'
    for change in ['escape','hardlink','fifo','extra','input_change','existing_output','completed_marker']:
        work,payload,output,outside,result=fixture('reject-'+change,change);assert result.returncode!=0,change
        if change=='existing_output':assert output.read_text()=='keep-output'
        else:assert not output.exists()
        assert not pathlib.Path(str(output)+'.receipt.json').exists() and outside.read_text()=='keep';cases+=1
    result=subprocess.run(['bash',str(ROOT/'scripts/build-release-dependencies.sh')],env={**os.environ,'CCPANEL_NODE_BINARY':'','CCPANEL_PNPM_BINARY':'','CCPANEL_ARCHIVE_FILE':'','CCPANEL_DEPENDENCIES_OUTPUT':''},text=True,capture_output=True,timeout=3)
    assert result.returncode!=0 and result.stdout==''
    assert json.loads(result.stderr)=={'status':'failed','phase':'preflight','workDirectory':'','admitted':False};cases+=1
print(json.dumps({'offhost_builder_checks_passed':cases,'real_metadata_pin_and_elf_guards':True,'real_deterministic_tar_and_inventory':True,'root_or_missing_inputs_refused':True,'network_install_executed':False,'linux_runtime_smoke_executed':False}))
