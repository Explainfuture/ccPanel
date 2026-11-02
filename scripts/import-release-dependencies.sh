#!/usr/bin/env bash
# Import an explicit off-host build while the existing preview remains online.
# A build receipt is an operator claim, not an authenticated build attestation.
set -euo pipefail
umask 077
stage='';active_container='';watcher='';owner='';phase=preflight;finished=false
deadline=$((SECONDS+180))
owned_container(){
  local names found
  [[ "$1" =~ ^ccpanel-deps-import-[a-f0-9]{16}$ ]] || return 1
  names=$(timeout 5s docker container ls --all --filter "name=^/$1$" --format '{{.Names}}' 2>/dev/null) || return 1
  test -n "$names" || return 3
  test "$names" = "$1" || return 1
  found=$(timeout 5s docker inspect --format '{{index .Config.Labels "ccpanel.runtime-deps.owner"}}' "$1" 2>/dev/null) || return 1
  [[ "$found" = "$owner" ]]
}
remove_owned(){
  local name=$1 status=0
  test -n "$name" || return 0
  if owned_container "$name";then
    if timeout 8s docker rm -f "$name" >/dev/null 2>&1;then return 0;fi
    if owned_container "$name";then return 1;else status=$?;fi
  else status=$?;fi
  test "$status" = 3
}
cleanup(){
  local code=$?;trap - EXIT;set +e
  if [ -n "$watcher" ];then kill "$watcher" 2>/dev/null;wait "$watcher" 2>/dev/null;fi
  if ! remove_owned "$active_container";then code=1;phase=container_cleanup;fi
  if [ "$finished" != true ] || [ "$code" != 0 ];then
    if [[ "$stage" =~ ^/opt/ccpanel/shared/runtime-deps\.[A-Za-z0-9]{6}$ ]];then rm -f -- "$stage/.completed.json" || phase=marker_cleanup;fi
    printf '{"status":"incomplete","phase":"%s","stage":"%s","completed":false}\n' "$phase" "$stage" >&2
    code=1
  fi
  exit "$code"
}
trap cleanup EXIT
trap 'exit 143' TERM
trap 'exit 130' INT
remaining(){ local left=$((deadline-SECONDS));test "$left" -gt 0 || return 1;printf '%s\n' "$left"; }
test "$(id -u)" = 0;test "$(uname -s)" = Linux
test "${CCPANEL_EXPECTED_RELEASE:-}" = m4-sdk-013
test "$(readlink -f /opt/ccpanel/current)" = /opt/ccpanel/releases/m4-sdk-013
archive=${CCPANEL_ARCHIVE_FILE:-};archive_hash=${CCPANEL_ARCHIVE_SHA256:-}
artifact=${CCPANEL_DEPENDENCIES_ARTIFACT:-};artifact_hash=${CCPANEL_DEPENDENCIES_SHA256:-}
for value in "$archive" "$artifact";do
  [[ "$value" =~ ^/opt/ccpanel/shared/[A-Za-z0-9][A-Za-z0-9._-]{0,127}\.tar\.gz$ ]]
  test "$value" != /opt/ccpanel/shared/release.tar.gz
  test -f "$value";test ! -L "$value";test "$(readlink -f "$value")" = "$value"
done
test "$archive" != "$artifact"
[[ "$archive_hash" =~ ^[a-f0-9]{64}$ && "$artifact_hash" =~ ^[a-f0-9]{64}$ ]]
verifier="$(cd -P "$(dirname "$0")" && pwd)/verify-release-dependencies.py"
test -f "$verifier";test ! -L "$verifier";test "$(stat -c %u "$verifier")" = 0
verifier_mode=$(stat -c %a "$verifier");(( (8#$verifier_mode & 022) == 0 ))
exec 8>/opt/ccpanel/shared/deploy.lock;flock -n 8
capacity_units=$(timeout 5s systemctl list-units --type=service --state=running 'ccpanel-capacity-*' --no-legend --plain)
test -z "$capacity_units"
test "$(awk '/MemAvailable:/ {print ($2 >= 307200)}' /proc/meminfo)" = 1
curl --connect-timeout 5 --max-time 10 -fsS https://192.236.151.125/ready >/dev/null 2>&1
image=$(timeout 5s docker image inspect --format '{{.Id}}' node:22.22.3-bookworm-slim 2>/dev/null)
[[ "$image" =~ ^sha256:[a-f0-9]{64}$ ]]
stage=$(mktemp -d /opt/ccpanel/shared/runtime-deps.XXXXXX);chmod 700 "$stage"
owner=$(openssl rand -hex 16)
phase=artifact_validation
timeout --signal=TERM --kill-after=5s "$(remaining)s" python3 - "$archive" "$archive_hash" "$artifact" "$artifact_hash" "$stage" "$verifier" >"$stage/extract.log" 2>&1 <<'PY'
import hashlib,json,os,pathlib,posixpath,re,resource,stat,sys,tarfile
resource.setrlimit(resource.RLIMIT_AS,(192*1024*1024,192*1024*1024))
LIMIT=256*1024*1024;MAX_NODES=100000;RECEIPT='build-toolchain.json'
INPUTS=('package.json','pnpm-lock.yaml','pnpm-workspace.yaml')
def check(value):
    if not value:raise ValueError('dependency_artifact_invalid')
def signature(value):return(value.st_dev,value.st_ino,value.st_mode,value.st_uid,value.st_gid,value.st_size,value.st_nlink,value.st_mtime_ns,value.st_ctime_ns)
def source(path,digest,limit):
    path=pathlib.Path(path);before=path.lstat()
    check(path.is_absolute() and path.resolve(strict=True)==path and stat.S_ISREG(before.st_mode) and before.st_uid==os.geteuid() and before.st_gid==os.getegid() and before.st_nlink==1 and not before.st_mode&0o022 and 0<before.st_size<=limit)
    file=open(os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK),'rb',closefd=True)
    try:
        check(signature(before)==signature(os.fstat(file.fileno())))
        hashed=hashlib.sha256();count=0
        while chunk:=file.read(min(65536,before.st_size-count+1)):
            count+=len(chunk);check(count<=before.st_size);hashed.update(chunk)
        check(count==before.st_size and hashed.hexdigest()==digest and signature(before)==signature(os.fstat(file.fileno())) and signature(before)==signature(path.lstat()))
        file.seek(0);return path,before,file
    except BaseException:file.close();raise
def stable(value):
    path,before,file=value
    check(signature(before)==signature(os.fstat(file.fileno())) and signature(before)==signature(path.lstat()))
def unique(pairs):
    result={}
    for key,value in pairs:check(key not in result);result[key]=value
    return result
def safe_name(name):
    check(isinstance(name,str) and len(name.encode('utf-8'))<=2048 and name and not re.search(r'[\x00-\x1f\x7f\\]',name))
    check(not name.startswith('/') and all(part not in ('','.','..') for part in name.split('/')))
def stream(file):
    file.seek(0);return tarfile.open(fileobj=file,mode='r|gz')
def next_entry(tar):
    member=tar.next()
    # Hardlinks are forbidden; no member-name lookup is needed. Bound TarFile's
    # cache rather than retaining a second full inventory while streaming.
    tar.members.clear()
    return member
def read_member(tar,member,limit):
    check(member is not None and member.type==tarfile.REGTYPE and 0<member.size<=limit)
    with tar.extractfile(member) as file:data=file.read(limit+1)
    check(len(data)==member.size);return data
opened=[]
try:
    candidate=source(sys.argv[1],sys.argv[2],16*1024*1024);opened.append(candidate)
    selected={};total=0
    with stream(candidate[2]) as tar:
        for count in range(4097):
            member=next_entry(tar)
            if member is None:break
            check(count<4096 and (member.isfile() or member.isdir()) and member.size<=16*1024*1024)
            safe_name(member.name.rstrip('/'));total+=member.size;check(total<=64*1024*1024)
            if member.name in (*INPUTS,'scripts/verify-release-dependencies.py'):
                check(member.name not in selected);selected[member.name]=read_member(tar,member,4*1024*1024)
    check(set(selected)==set((*INPUTS,'scripts/verify-release-dependencies.py')))
    helper=pathlib.Path(sys.argv[6]);info=helper.lstat()
    check(stat.S_ISREG(info.st_mode) and info.st_uid==os.geteuid() and not info.st_mode&0o022 and not helper.is_symlink() and helper.resolve()==helper)
    check(info.st_size<=131072 and selected['scripts/verify-release-dependencies.py']==helper.read_bytes())
    inputs={name:hashlib.sha256(selected[name]).hexdigest() for name in INPUTS};stable(candidate)
    artifact=source(sys.argv[3],sys.argv[4],128*1024*1024);opened.append(artifact)
    with stream(artifact[2]) as tar:
        first=next_entry(tar);check(first is not None and first.name==RECEIPT and first.mode==0o644 and first.uid==first.gid==0)
        receipt_bytes=read_member(tar,first,8*1024*1024)
        receipt=json.loads(receipt_bytes.decode('utf-8'),object_pairs_hook=unique)
        check(type(receipt) is dict and set(receipt)=={'version','nodeVersion','pnpmVersion','inputs','candidateArchiveSha256','nodeBinarySha256','pnpmBinarySha256','files'})
        check(type(receipt['version']) is int and receipt['version']==1 and receipt['nodeVersion']=='v22.22.3' and receipt['pnpmVersion']=='12.3.4' and receipt['candidateArchiveSha256']==sys.argv[2] and receipt['inputs']==inputs)
        check(all(type(receipt[name]) is str and re.fullmatch('[a-f0-9]{64}',receipt[name]) for name in ('nodeBinarySha256','pnpmBinarySha256')))
        entries=receipt['files'];check(type(entries) is list and 3<len(entries)<=MAX_NODES)
        paths={};last='';total=0
        for entry in entries:
            check(type(entry) is dict and type(entry.get('path')) is str);name=entry['path'];safe_name(name)
            check(name>last and (name in INPUTS or name=='node_modules' or name.startswith('node_modules/')));last=name
            kind=entry.get('kind');check(kind in ('file','directory','symlink'))
            check(set(entry)=={'path','kind','mode','size'}|({'sha256'} if kind=='file' else {'target'} if kind=='symlink' else set()))
            check(type(entry['mode']) is int and type(entry['size']) is int and entry['size']>=0)
            if kind=='file':
                check(entry['mode'] in (0o644,0o755) and entry['size']<=64*1024*1024 and type(entry['sha256']) is str and re.fullmatch('[a-f0-9]{64}',entry['sha256']))
                total+=entry['size'];check(total<=LIMIT)
            elif kind=='directory':check(entry['mode']==0o755 and entry['size']==0)
            else:
                target=entry['target'];check(type(target) is str and target and not posixpath.isabs(target) and not re.search(r'[\x00-\x1f\x7f\\]',target) and len(target.encode('utf-8'))<=4096 and entry['mode']==0o777 and entry['size']==len(target.encode('utf-8')))
            if name in INPUTS:check(kind=='file' and entry['mode']==0o644 and entry['sha256']==inputs[name])
            paths[name]=entry
        check(all(name in paths for name in INPUTS) and paths.get('node_modules',{}).get('kind')=='directory')
        for name,entry in paths.items():
            if name.startswith('node_modules/'):check(paths.get(posixpath.dirname(name),{}).get('kind')=='directory')
            if entry['kind']!='symlink':continue
            pending=posixpath.normpath(posixpath.join(posixpath.dirname(name),entry['target']));seen=set()
            for _ in range(100):
                check(pending=='node_modules' or pending.startswith('node_modules/'));check(pending not in seen);seen.add(pending)
                parts=pending.split('/');changed=False
                for index in range(1,len(parts)+1):
                    prefix='/'.join(parts[:index]);target_entry=paths.get(prefix);check(target_entry is not None)
                    if target_entry['kind']=='symlink':
                        pending=posixpath.normpath(posixpath.join(posixpath.dirname(prefix),target_entry['target'],*parts[index:]));changed=True;break
                    check(index==len(parts) or target_entry['kind']=='directory')
                if not changed:break
            else:check(False)
        def compare(member,entry,contents=True):
            check(member is not None and member.name==entry['path'] and member.mode==entry['mode'] and member.uid==member.gid==0 and set(member.pax_headers)<= {'path','linkpath'})
            kind=entry['kind'];check((kind=='file' and member.type==tarfile.REGTYPE) or (kind=='directory' and member.type==tarfile.DIRTYPE) or (kind=='symlink' and member.type==tarfile.SYMTYPE))
            check(member.size==(entry['size'] if kind=='file' else 0))
            if kind=='symlink':check(member.linkname==entry['target'])
            if kind!='file' or not contents:return
            hashed=hashlib.sha256();count=0
            with tar.extractfile(member) as file:
                while chunk:=file.read(65536):hashed.update(chunk);count+=len(chunk)
            check(count==entry['size'] and hashed.hexdigest()==entry['sha256'])
        for entry in entries:compare(next_entry(tar),entry)
        check(next_entry(tar) is None)
    stable(artifact)
    # Nothing from either archive has been written before all headers, content
    # hashes and the entire link graph above passed comprehensive validation.
    stage=pathlib.Path(sys.argv[5]);info=stage.lstat()
    check(stage.resolve()==stage and stat.S_ISDIR(info.st_mode) and info.st_uid==os.geteuid() and stat.S_IMODE(info.st_mode)==0o700)
    check(not any((stage/name).exists() or (stage/name).is_symlink() for name in (*INPUTS,'node_modules',RECEIPT,'.completed.json')))
    def write(path,data,mode):
        fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
        with os.fdopen(fd,'wb') as file:file.write(data);file.flush();os.fsync(file.fileno());os.fchmod(file.fileno(),mode)
    write(stage/RECEIPT,receipt_bytes,0o600)
    with stream(artifact[2]) as tar:
        check(read_member(tar,next_entry(tar),8*1024*1024)==receipt_bytes)
        for entry in entries:
            member=next_entry(tar);compare(member,entry,contents=False);path=stage/entry['path']
            check(path.parent.resolve(strict=True)==path.parent)
            if entry['kind']=='directory':os.mkdir(path,0o700);os.chmod(path,0o755)
            elif entry['kind']=='symlink':os.symlink(entry['target'],path)
            else:
                fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600);hashed=hashlib.sha256();count=0
                with os.fdopen(fd,'wb') as file,tar.extractfile(member) as source_file:
                    while chunk:=source_file.read(65536):file.write(chunk);hashed.update(chunk);count+=len(chunk)
                    check(count==entry['size'] and hashed.hexdigest()==entry['sha256']);file.flush();os.fsync(file.fileno());os.fchmod(file.fileno(),0o600 if entry['path'] in INPUTS else entry['mode'])
        check(next_entry(tar) is None)
    stable(artifact);stable(candidate)
    for name in INPUTS:check((stage/name).read_bytes()==selected[name])
    directory=os.open(stage,os.O_RDONLY|os.O_DIRECTORY)
    try:os.fsync(directory)
    finally:os.close(directory)
    print(json.dumps({'artifact_validated':True,'files':len(entries),'bytes':total,'build_provenance_verified':False}))
except BaseException:
    print('{"code":"dependency_artifact_invalid"}');raise SystemExit(1)
finally:
    for _,_,file in opened:file.close()
PY
phase=runtime_import_smoke
name="ccpanel-deps-import-${owner:0:16}"
if owned_container "$name";then exit 1;else test "$?" = 3;fi
active_container=$name
timeout --signal=TERM --kill-after=5s "$(remaining)s" docker run --rm --name "$name" --label "ccpanel.runtime-deps.owner=$owner" --network=none --read-only --user=1000:1000 --memory=128m --memory-swap=160m --cpus=0.5 --pids-limit=64 --security-opt=no-new-privileges --mount "type=bind,src=$stage/node_modules,dst=/runtime/node_modules,readonly" --workdir /runtime --env NODE_OPTIONS=--max-old-space-size=80 "$image" node --input-type=module -e '
  import assert from "node:assert/strict";
  assert.equal(process.version,"v22.22.3");assert.equal(process.getuid(),1000);
  const names=["@hono/node-server","@radix-ui/react-slot","class-variance-authority","clsx","hono","kafkajs","lucide-react","pg","react","react-dom","ssh2","tailwind-merge"];
  for(const name of names)await import(name);
  console.log(JSON.stringify({nodeVersion:process.version,imported:12,runtimeUid:1000,nativeDynamicImports:true}));
' >"$stage/smoke.json" 2>"$stage/smoke.log" &
runner=$!
(
  while kill -0 "$runner" 2>/dev/null;do
    available=$(awk '/MemAvailable:/ {print $2}' /proc/meminfo) || available=''
    if ! [[ "$available" =~ ^[0-9]+$ ]] || [ "$available" -lt 204800 ] || [ -f "$stage/resource-aborted" ];then
      printf 'resource_safety\n' >"$stage/resource-aborted" || true
      remove_owned "$name" || true
    fi
    sleep 1
  done
) &
watcher=$!
status=0;wait "$runner" || status=$?
kill "$watcher" 2>/dev/null || true;wait "$watcher" 2>/dev/null || true;watcher=''
test "$status" = 0;test ! -f "$stage/resource-aborted"
remove_owned "$name";active_container=''
available=$(awk '/MemAvailable:/ {print $2}' /proc/meminfo)
[[ "$available" =~ ^[0-9]+$ ]] && test "$available" -ge 204800
phase=stage_verification
timeout --signal=TERM --kill-after=5s "$(remaining)s" python3 - "$stage" <<'PY'
import json,pathlib,sys
stage=pathlib.Path(sys.argv[1]);smoke=json.loads((stage/'smoke.json').read_text())
assert smoke=={'nodeVersion':'v22.22.3','imported':12,'runtimeUid':1000,'nativeDynamicImports':True}
PY
test "$(readlink -f /opt/ccpanel/current)" = /opt/ccpanel/releases/m4-sdk-013
timeout --signal=TERM --kill-after=5s "$(remaining)s" curl --connect-timeout 5 --max-time 10 -fsS https://192.236.151.125/ready >/dev/null 2>&1
timeout --signal=TERM --kill-after=5s "$(remaining)s" python3 "$verifier" create "$stage" v22.22.3 12.3.4 >"$stage/create-proof.json" 2>"$stage/create-proof.log"
timeout --signal=TERM --kill-after=5s "$(remaining)s" python3 "$verifier" verify "$stage" "$archive" >"$stage/verify-proof.json" 2>"$stage/verify-proof.log"
test "$SECONDS" -lt "$deadline";test ! -f "$stage/resource-aborted"
available=$(awk '/MemAvailable:/ {print $2}' /proc/meminfo)
[[ "$available" =~ ^[0-9]+$ ]] && test "$available" -ge 204800
phase=complete;finished=true
printf '{"status":"prepared","stage":"%s","candidateArchiveSha256":"%s","dependenciesArchiveSha256":"%s","nodeVersion":"v22.22.3","pnpmVersion":"12.3.4","nativeImportChecked":true,"build_provenance_verified":false,"completed":true}\n' "$stage" "$archive_hash" "$artifact_hash"
