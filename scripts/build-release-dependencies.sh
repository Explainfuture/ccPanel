#!/usr/bin/env bash
# Off-host Linux build only. No VPS paths, source credentials or old dependency
# tree are used; the produced archive is unadmitted until the importer checks it.
set -euo pipefail
umask 077
work='';phase=preflight;finished=false
cleanup(){
  local code=$?;trap - EXIT;set +e
  if [ "$finished" = true ] && [ "$code" = 0 ]; then
    if [[ "$work" =~ ^/tmp/ccpanel-deps-build\.[A-Za-z0-9]{6}$ ]] && [ ! -L "$work" ]; then rm -rf -- "$work";fi
  else printf '{"status":"failed","phase":"%s","workDirectory":"%s","admitted":false}\n' "$phase" "$work" >&2;code=1;fi
  exit "$code"
}
trap cleanup EXIT
trap 'exit 143' TERM
trap 'exit 130' INT
test "$(uname -s)" = Linux
test "$(id -u)" != 0
test ! -e /opt/ccpanel/current && test ! -L /opt/ccpanel/current
node=${CCPANEL_NODE_BINARY:-};node_sha=${CCPANEL_NODE_SHA256:-}
pnpm=${CCPANEL_PNPM_BINARY:-};pnpm_sha=${CCPANEL_PNPM_SHA256:-}
archive=${CCPANEL_ARCHIVE_FILE:-};archive_sha=${CCPANEL_ARCHIVE_SHA256:-}
output=${CCPANEL_DEPENDENCIES_OUTPUT:-}
[[ "$node" = /* && "$pnpm" = /* && "$archive" = /* && "$output" = /* ]]
[[ "$node_sha" =~ ^[a-f0-9]{64}$ && "$pnpm_sha" =~ ^[a-f0-9]{64}$ && "$archive_sha" =~ ^[a-f0-9]{64}$ ]]
test -x "$node" && test -f "$node" && test ! -L "$node"
test -x "$pnpm" && test -f "$pnpm" && test ! -L "$pnpm"
test -f "$archive" && test ! -L "$archive"
test ! -e "$output" && test ! -L "$output"
test ! -e "$output.receipt.json" && test ! -L "$output.receipt.json"
work=$(mktemp -d /tmp/ccpanel-deps-build.XXXXXX)
test "$(readlink -f "$work")" = "$work"
case "$(stat -f -c %T "$work")" in ext2/ext3|ext4|xfs|btrfs|tmpfs|overlayfs|overlay) ;; *) exit 1;; esac
mkdir -m 700 "$work/payload" "$work/store" "$work/logs"
deadline=$((SECONDS+300))
bounded(){
  local remaining=$((deadline-SECONDS));test "$remaining" -gt 0
  timeout --signal=TERM --kill-after=5s "${remaining}s" "$@"
}
unset NODE_PATH NODE_OPTIONS NODE_COMPILE_CACHE
export NODE_OPTIONS=--max-old-space-size=256
export NPM_CONFIG_USERCONFIG=/dev/null npm_config_userconfig=/dev/null
export NPM_CONFIG_REGISTRY=https://registry.npmjs.org/ npm_config_registry=https://registry.npmjs.org/
export PATH="$(dirname "$node"):$PATH"
phase=pinned_inputs
bounded python3 - "$node" "$node_sha" "$pnpm" "$pnpm_sha" "$archive" "$archive_sha" "$work/payload" >"$work/logs/inputs.json" 2>"$work/logs/inputs.log" <<'PY'
import hashlib,io,json,os,pathlib,stat,sys,tarfile
def digest(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        for part in iter(lambda:f.read(65536),b''):h.update(part)
    return h.hexdigest()
node,pnpm,archive=[pathlib.Path(sys.argv[i]) for i in (1,3,5)];destination=pathlib.Path(sys.argv[7])
for path,expected in [(node,sys.argv[2]),(pnpm,sys.argv[4]),(archive,sys.argv[6])]:
    assert path.is_absolute() and not path.is_symlink() and path.is_file() and digest(path)==expected
for tool in (node,pnpm):
    with tool.open('rb') as f:header=f.read(20)
    assert len(header)==20 and header[:6]==b'\x7fELF\x02\x01' and header[18:20]==b'\x3e\x00','Expected pinned Linux x86_64 native tool'
assert 0<archive.stat().st_size<=16*1024*1024
raw=archive.read_bytes();assert 0<len(raw)<=16*1024*1024 and hashlib.sha256(raw).hexdigest()==sys.argv[6]
wanted={'package.json':65536,'pnpm-lock.yaml':2*1024*1024,'pnpm-workspace.yaml':65536};selected={};total=0
with tarfile.open(fileobj=io.BytesIO(raw),mode='r:gz') as tar:
    for index,member in enumerate(tar):
        assert index<4096 and member.size<=16*1024*1024 and (member.isfile() or member.isdir())
        path=pathlib.PurePosixPath(member.name);assert not path.is_absolute() and '..' not in path.parts and '\\' not in member.name
        total+=member.size;assert total<=64*1024*1024
        if member.name in wanted:
            assert member.name not in selected and member.isfile() and 0<member.size<=wanted[member.name]
            selected[member.name]=tar.extractfile(member).read(wanted[member.name]+1);assert len(selected[member.name])==member.size
assert set(selected)==set(wanted)
package=json.loads(selected['package.json']);reviewed={'@hono/node-server':'2.1.5','@radix-ui/react-slot':'1.5.0','class-variance-authority':'0.7.1','clsx':'2.1.1','hono':'4.13.13','kafkajs':'2.2.4','lucide-react':'1.61.0','pg':'8.23.1','react':'19.3.1','react-dom':'19.3.1','ssh2':'1.17.0','tailwind-merge':'3.7.0'}
assert package.get('private') is True and package.get('packageManager')=='pnpm@12.3.4' and package.get('dependencies')==reviewed
assert not {'preinstall','install','postinstall','prepare'}.intersection(package.get('scripts',{}))
workspace=[line.strip() for line in selected['pnpm-workspace.yaml'].decode('utf-8').splitlines() if line.strip()]
assert workspace==['packages:','- apps/*','- packages/*','allowBuilds:','cpu-features: false','esbuild: true','ssh2: false']
inputs={}
for name,data in selected.items():
    with (destination/name).open('xb') as f:f.write(data);f.flush();os.fsync(f.fileno())
    inputs[name]=hashlib.sha256(data).hexdigest()
print(json.dumps(inputs,sort_keys=True))
PY
phase=toolchain_versions
actual_node=$(bounded "$node" --version 2>"$work/logs/node-version.log")
actual_pnpm=$(bounded "$pnpm" --version 2>"$work/logs/pnpm-version.log")
test "$actual_node" = v22.22.3 && test "$actual_pnpm" = 12.3.4
phase=production_install
(
  cd "$work/payload"
  umask 022
  bounded "$pnpm" install --prod --frozen-lockfile --ignore-scripts --package-import-method=copy --child-concurrency=1 --network-concurrency=2 --registry=https://registry.npmjs.org/ --store-dir "$work/store" --reporter=append-only
) >"$work/logs/install.log" 2>&1
phase=native_import_smoke
(
  cd "$work/payload"
  bounded "$node" --input-type=module -e '
    import assert from "node:assert/strict";
    assert.equal(process.platform,"linux");assert.notEqual(process.getuid(),0);assert.equal(process.version,"v22.22.3");
    const names=["@hono/node-server","@radix-ui/react-slot","class-variance-authority","clsx","hono","kafkajs","lucide-react","pg","react","react-dom","ssh2","tailwind-merge"];
    for(const name of names)await import(name);
    console.log(JSON.stringify({nodeVersion:process.version,imported:12,nativeLinuxImports:true,uid:process.getuid()}));
  '
) >"$work/logs/smoke.json" 2>"$work/logs/smoke.log"
phase=deterministic_artifact
bounded python3 - "$work/payload" "$work/logs/inputs.json" "$archive_sha" "$node_sha" "$pnpm_sha" "$actual_node" "$actual_pnpm" "$work" "$output" >"$work/logs/artifact.json" 2>"$work/logs/artifact.log" <<'PY'
import gzip,hashlib,json,os,pathlib,re,shutil,stat,sys,tarfile
payload=pathlib.Path(sys.argv[1]);inputs=json.loads(pathlib.Path(sys.argv[2]).read_text());work=pathlib.Path(sys.argv[8]);output=pathlib.Path(sys.argv[9])
assert output.is_absolute() and output.parent.resolve()==output.parent and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,127}\.tar\.gz',output.name)
assert not output.exists() and not output.is_symlink() and not pathlib.Path(str(output)+'.receipt.json').exists() and not pathlib.Path(str(output)+'.receipt.json').is_symlink()
def digest(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        for part in iter(lambda:f.read(65536),b''):h.update(part)
    return h.hexdigest()
assert set(inputs)=={'package.json','pnpm-lock.yaml','pnpm-workspace.yaml'}
assert {p.name for p in payload.iterdir()}==set(inputs)|{'node_modules'}
assert (payload/'node_modules').is_dir() and not (payload/'node_modules').is_symlink()
for name,expected in inputs.items():assert digest(payload/name)==expected
files=[];total=0
def visit(path):
    global total
    assert len(files)<100000
    relative=path.relative_to(payload).as_posix();relative.encode('utf-8');assert len(relative.encode('utf-8'))<=4096 and not re.search(r'[\x00-\x1f\x7f]',relative)
    info=path.lstat()
    if stat.S_ISLNK(info.st_mode):
        assert relative.startswith('node_modules/');target=os.readlink(path);assert target and not pathlib.PurePosixPath(target).is_absolute() and '\\' not in target and not re.search(r'[\x00-\x1f\x7f]',target)
        assert path.resolve(strict=True).is_relative_to(payload/'node_modules')
        files.append({'path':relative,'kind':'symlink','mode':0o777,'size':len(target.encode('utf-8')),'target':target})
    elif stat.S_ISDIR(info.st_mode):
        files.append({'path':relative,'kind':'directory','mode':0o755,'size':0})
        for child in sorted(path.iterdir(),key=lambda p:p.name):visit(child)
    else:
        assert stat.S_ISREG(info.st_mode) and info.st_nlink==1 and info.st_size<=64*1024*1024
        total+=info.st_size;assert total<=256*1024*1024
        files.append({'path':relative,'kind':'file','mode':0o755 if info.st_mode&0o111 else 0o644,'size':info.st_size,'sha256':digest(path)})
for name in sorted(set(inputs)|{'node_modules'}):visit(payload/name)
files.sort(key=lambda item:item['path']);assert len({entry['path'] for entry in files})==len(files)
receipt={'version':1,'nodeVersion':sys.argv[6],'pnpmVersion':sys.argv[7],'inputs':inputs,'candidateArchiveSha256':sys.argv[3],'nodeBinarySha256':sys.argv[4],'pnpmBinarySha256':sys.argv[5],'files':files}
assert receipt['nodeVersion']=='v22.22.3' and receipt['pnpmVersion']=='12.3.4'
encoded=json.dumps(receipt,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode('utf-8');assert len(encoded)<=8*1024*1024
receipt_file=payload/'build-toolchain.json'
with receipt_file.open('xb') as f:f.write(encoded);f.flush();os.fsync(f.fileno())
archive=work/'dependencies.tar.gz'
with archive.open('xb') as outer:
    with gzip.GzipFile(filename='',mode='wb',fileobj=outer,mtime=0,compresslevel=9) as compressed:
        with tarfile.open(fileobj=compressed,mode='w|',format=tarfile.GNU_FORMAT) as tar:
            for entry in [{'path':'build-toolchain.json','kind':'file','mode':0o644,'size':len(encoded),'sha256':hashlib.sha256(encoded).hexdigest()}]+files:
                info=tarfile.TarInfo(entry['path']);info.uid=info.gid=0;info.uname=info.gname='';info.mtime=0;info.mode=entry['mode']
                if entry['kind']=='directory':info.type=tarfile.DIRTYPE;info.size=0;tar.addfile(info)
                elif entry['kind']=='symlink':info.type=tarfile.SYMTYPE;info.linkname=entry['target'];info.size=0;tar.addfile(info)
                else:
                    path=payload/entry['path'];assert digest(path)==entry['sha256'];info.size=entry['size']
                    with path.open('rb') as source:tar.addfile(info,source)
    outer.flush();os.fsync(outer.fileno())
archive_sha=digest(archive);archive_bytes=archive.stat().st_size
with output.open('xb') as destination,archive.open('rb') as source:shutil.copyfileobj(source,destination,65536);destination.flush();os.fsync(destination.fileno())
assert digest(output)==archive_sha
summary={key:receipt[key] for key in ['version','nodeVersion','pnpmVersion','inputs','candidateArchiveSha256','nodeBinarySha256','pnpmBinarySha256']}
summary.update(archiveSha256=archive_sha,archiveBytes=archive_bytes,buildToolchainSha256=hashlib.sha256(encoded).hexdigest())
with pathlib.Path(str(output)+'.receipt.json').open('x',encoding='utf-8') as f:json.dump(summary,f,sort_keys=True,separators=(',',':'));f.flush();os.fsync(f.fileno())
print(json.dumps({'status':'built_unadmitted','archiveSha256':archive_sha,'archiveBytes':archive_bytes,'entries':len(files),'contentBytes':total,'admitted':False},sort_keys=True))
PY
cat "$work/logs/artifact.json"
phase=complete;finished=true
