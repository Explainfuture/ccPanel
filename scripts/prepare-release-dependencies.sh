#!/usr/bin/env bash
# Prepare an independent runtime dependency tree while the current preview is
# still online. Never install into an existing release or copy old hardlinks.
set -euo pipefail
umask 077
stage='';active_container='';watcher='';owner='';phase=preflight
finished=false
owned_container(){
  local found status=0
  found=$(timeout 5s docker inspect --format '{{index .Config.Labels "ccpanel.runtime-deps.owner"}}' "$1" 2>&1) || status=$?
  if [ "$status" = 0 ]; then [[ "$found" = "$owner" ]];return;fi
  if [ "$status" = 1 ] && [[ "$found" = "Error: No such object: $1" || "$found" = "Error: No such container: $1" || "$found" = "Error response from daemon: No such container: $1" ]]; then return 3;fi
  # Timeout, daemon errors and another owner's label are never absence proof.
  return 1
}
remove_owned(){
  local name=$1 status=0
  test -n "$name" || return 0
  if owned_container "$name"; then
    if timeout 8s docker rm -f "$name" >/dev/null 2>&1; then return 0;fi
    # --rm may have completed between the label read and removal request.
    if owned_container "$name"; then return 1;else status=$?;fi
  else status=$?;fi
  test "$status" = 3
}
cleanup(){
  local code=$?;trap - EXIT;set +e
  if [ -n "$watcher" ]; then kill "$watcher" 2>/dev/null;wait "$watcher" 2>/dev/null;fi
  if ! remove_owned "$active_container"; then code=1;phase=container_cleanup;fi
  if [ "$finished" != true ] || [ "$code" -ne 0 ]; then
    if [[ "$stage" =~ ^/opt/ccpanel/shared/runtime-deps\.[A-Za-z0-9]{6}$ ]]; then
      # This new, private stage belongs only to this controller. A failed run
      # must never leave an admission marker, even after uncertain publication.
      rm -f -- "$stage/.completed.json" || phase=marker_cleanup
    fi
    printf '{"status":"incomplete","phase":"%s","stage":"%s","completed":false}\n' "$phase" "$stage" >&2
    code=1
  fi
  exit "$code"
}
trap cleanup EXIT
trap 'exit 143' TERM
trap 'exit 130' INT
test "$(id -u)" = 0
test "$(uname -s)" = Linux
test "${CCPANEL_EXPECTED_RELEASE:-}" = m4-sdk-013
test "$(readlink -f /opt/ccpanel/current)" = /opt/ccpanel/releases/m4-sdk-013
archive=${CCPANEL_ARCHIVE_FILE:-};expected_hash=${CCPANEL_ARCHIVE_SHA256:-}
[[ "$archive" =~ ^/opt/ccpanel/shared/[A-Za-z0-9][A-Za-z0-9._-]{0,127}\.tar\.gz$ ]]
test "$archive" != /opt/ccpanel/shared/release.tar.gz
test -f "$archive" && test ! -L "$archive"
[[ "$expected_hash" =~ ^[a-f0-9]{64}$ ]]
verifier="$(cd "$(dirname "$0")" && pwd)/verify-release-dependencies.py"
test -f "$verifier" && test ! -L "$verifier"
exec 8>/opt/ccpanel/shared/deploy.lock
flock -n 8
capacity_units=$(timeout 5s systemctl list-units --type=service --state=running 'ccpanel-capacity-*' --no-legend --plain 2>/dev/null)
test -z "$capacity_units"
test "$(awk '/MemAvailable:/ {print ($2 >= 307200)}' /proc/meminfo)" = 1
curl --connect-timeout 5 --max-time 10 -fsS https://192.236.151.125/ready >/dev/null 2>&1
image=$(timeout 5s docker image inspect --format '{{.Id}}' node:22.22.3-bookworm-slim 2>/dev/null)
[[ "$image" =~ ^sha256:[a-f0-9]{64}$ ]]
stage=$(mktemp -d /opt/ccpanel/shared/runtime-deps.XXXXXX)
[[ "$stage" =~ ^/opt/ccpanel/shared/runtime-deps\.[A-Za-z0-9]{6}$ ]]
chmod 700 "$stage"
owner=$(openssl rand -hex 16)
phase=archive_extraction
timeout --signal=TERM --kill-after=5s 30s python3 - "$archive" "$expected_hash" "$stage" "$verifier" >"$stage/extract.log" 2>&1 <<'PY'
import hashlib,io,json,os,pathlib,stat,sys,tarfile
archive=pathlib.Path(sys.argv[1]);stage=pathlib.Path(sys.argv[3]);verifier=pathlib.Path(sys.argv[4])
assert archive.resolve()==archive and stage.resolve()==stage and not archive.is_symlink()
info=stage.stat();assert info.st_uid==os.geteuid() and stat.S_IMODE(info.st_mode)==0o700
fd=os.open(archive,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
with os.fdopen(fd,'rb') as source:
    initial=os.fstat(source.fileno());assert stat.S_ISREG(initial.st_mode) and initial.st_uid==os.geteuid() and not initial.st_mode&0o022
    raw=source.read(16*1024*1024+1);assert 0<len(raw)<=16*1024*1024 and len(raw)==initial.st_size
    final=os.fstat(source.fileno());current=archive.lstat()
    assert (initial.st_dev,initial.st_ino,initial.st_size,initial.st_mtime_ns,initial.st_ctime_ns)==(final.st_dev,final.st_ino,final.st_size,final.st_mtime_ns,final.st_ctime_ns)
    assert (current.st_dev,current.st_ino)==(initial.st_dev,initial.st_ino) and not stat.S_ISLNK(current.st_mode)
assert hashlib.sha256(raw).hexdigest()==sys.argv[2]
wanted={'package.json':65536,'pnpm-lock.yaml':2*1024*1024,'pnpm-workspace.yaml':65536,'dist/schema-version.txt':32,'scripts/verify-release-dependencies.py':131072}
selected={};total=0
with tarfile.open(fileobj=io.BytesIO(raw),mode='r:gz') as tar:
    for index,member in enumerate(tar):
        assert index<4096 and member.size<=16*1024*1024 and (member.isfile() or member.isdir())
        path=pathlib.PurePosixPath(member.name);assert not path.is_absolute() and '..' not in path.parts and '\\' not in member.name
        assert not any(part.startswith('.env') for part in path.parts)
        total+=member.size;assert total<=64*1024*1024
        if member.name in wanted:
            assert member.name not in selected and member.isfile() and 0<member.size<=wanted[member.name]
            selected[member.name]=tar.extractfile(member).read(wanted[member.name]+1);assert len(selected[member.name])==member.size
assert set(selected)==set(wanted)
assert selected['dist/schema-version.txt'].strip()==b'22'
assert selected['scripts/verify-release-dependencies.py']==verifier.read_bytes()
package=json.loads(selected['package.json'].decode('utf-8'))
reviewed={'@hono/node-server':'2.1.5','@radix-ui/react-slot':'1.5.0','class-variance-authority':'0.7.1','clsx':'2.1.1','hono':'4.13.13','kafkajs':'2.2.4','lucide-react':'1.61.0','pg':'8.23.1','react':'19.3.1','react-dom':'19.3.1','ssh2':'1.17.0','tailwind-merge':'3.7.0'}
assert package.get('private') is True and package.get('packageManager')=='pnpm@12.3.4' and package.get('dependencies')==reviewed
assert not {'preinstall','install','postinstall','prepare'}.intersection(package.get('scripts',{}))
workspace=[line.strip() for line in selected['pnpm-workspace.yaml'].decode('utf-8').splitlines() if line.strip()]
assert workspace==['packages:','- apps/*','- packages/*','allowBuilds:','cpu-features: false','esbuild: true','ssh2: false']
for name in ['package.json','pnpm-lock.yaml','pnpm-workspace.yaml']:
    fd=os.open(stage/name,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    with os.fdopen(fd,'wb') as output:output.write(selected[name]);output.flush();os.fsync(output.fileno())
directory=os.open(stage,os.O_RDONLY|os.O_DIRECTORY)
try:os.fsync(directory)
finally:os.close(directory)
PY
# All network/install work is bounded, labeled, and independent from the source
# application containers. The shared archive and live release are read-only.
# The following180s budget covers install, smoke and final verification;
# prerequisite probes above each have their own short fixed bounds.
deadline=$((SECONDS+180))
run_container(){
  local name=$1;shift
  local remaining=$((deadline-SECONDS))
  test "$remaining" -gt 0
  if owned_container "$name"; then return 1;else test "$?" = 3 || return 1;fi
  active_container=$name
  timeout --signal=TERM --kill-after=5s "${remaining}s" docker run --rm --name "$name" --label "ccpanel.runtime-deps.owner=$owner" "$@" &
  local runner=$!
  (
    while kill -0 "$runner" 2>/dev/null; do
      available=$(awk '/MemAvailable:/ {print $2}' /proc/meminfo 2>/dev/null) || available=''
      if [[ ! "$available" =~ ^[0-9]+$ ]] || [ "$available" -lt 204800 ] || [ -f "$stage/resource-aborted" ]; then
        printf 'resource_safety\n' >"$stage/resource-aborted"
        remove_owned "$name" || true
      fi
      sleep 1
    done
  ) &
  watcher=$!
  local status=0;wait "$runner" || status=$?
  kill "$watcher" 2>/dev/null || true;wait "$watcher" 2>/dev/null || true;watcher=''
  remove_owned "$name" || return 1;active_container=''
  test "$status" = 0 && test ! -f "$stage/resource-aborted"
}
phase=dependency_install
run_container "ccpanel-deps-${owner:0:16}" --memory=160m --memory-swap=224m --cpus=0.5 --pids-limit=128 --security-opt=no-new-privileges --mount "type=bind,src=$stage,dst=/candidate" --workdir /candidate --env NODE_OPTIONS=--max-old-space-size=96 "$image" sh -eu -c '
  umask 022
  test "$(node --version)" = v22.22.3
  npm install -g pnpm@12.3.4 --ignore-scripts --no-audit --no-fund --fetch-retries=1 --fetch-timeout=30000
  test "$(pnpm --version)" = 12.3.4
  pnpm install --prod --frozen-lockfile --ignore-scripts --package-import-method=copy --child-concurrency=1 --network-concurrency=2 --reporter=append-only
  node -e '\''const fs=require("node:fs"),cp=require("node:child_process");const value={nodeVersion:process.version,pnpmVersion:cp.execFileSync("pnpm",["--version"],{encoding:"utf8"}).trim()};if(value.nodeVersion!=="v22.22.3"||value.pnpmVersion!=="12.3.4")throw Error("toolchain_version_mismatch");fs.writeFileSync("install-toolchain.json",JSON.stringify(value),{flag:"wx",mode:0o600});'\''
' >"$stage/install.log" 2>&1
phase=runtime_import_smoke
run_container "ccpanel-deps-smoke-${owner:0:16}" --network=none --read-only --user=1000:1000 --memory=128m --memory-swap=160m --cpus=0.5 --pids-limit=64 --security-opt=no-new-privileges --mount "type=bind,src=$stage/node_modules,dst=/runtime/node_modules,readonly" --workdir /runtime --env NODE_OPTIONS=--max-old-space-size=80 "$image" node --input-type=module -e '
  import assert from "node:assert/strict";
  assert.equal(process.version,"v22.22.3");assert.equal(process.getuid(),1000);
  const names=["@hono/node-server","@radix-ui/react-slot","class-variance-authority","clsx","hono","kafkajs","lucide-react","pg","react","react-dom","ssh2","tailwind-merge"];
  for(const name of names)await import(name);
  console.log(JSON.stringify({nodeVersion:process.version,imported:names.length,runtimeUid:process.getuid(),nativeDynamicImports:true}));
' >"$stage/smoke.json" 2>"$stage/smoke.log"
phase=stage_verification
test "$SECONDS" -lt "$deadline"
test "$(readlink -f /opt/ccpanel/current)" = /opt/ccpanel/releases/m4-sdk-013
remaining=$((deadline-SECONDS));test "$remaining" -gt 0
timeout --signal=TERM --kill-after=5s "${remaining}s" curl --connect-timeout 5 --max-time 10 -fsS https://192.236.151.125/ready >/dev/null 2>&1
python3 - "$stage" >"$stage/toolchain-verified.json" 2>"$stage/toolchain-verified.log" <<'PY'
import json,pathlib,sys
stage=pathlib.Path(sys.argv[1]);install=json.loads((stage/'install-toolchain.json').read_text());smoke=json.loads((stage/'smoke.json').read_text())
assert install=={'nodeVersion':'v22.22.3','pnpmVersion':'12.3.4'}
assert smoke=={'nodeVersion':'v22.22.3','imported':12,'runtimeUid':1000,'nativeDynamicImports':True}
print(json.dumps({'nodeVersion':smoke['nodeVersion'],'pnpmVersion':install['pnpmVersion']}))
PY
actual_node=$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["nodeVersion"])' "$stage/toolchain-verified.json" 2>>"$stage/toolchain-verified.log")
actual_pnpm=$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["pnpmVersion"])' "$stage/toolchain-verified.json" 2>>"$stage/toolchain-verified.log")
remaining=$((deadline-SECONDS));test "$remaining" -gt 0
timeout --signal=TERM --kill-after=5s "${remaining}s" python3 "$verifier" create "$stage" "$actual_node" "$actual_pnpm" >"$stage/create-proof.json" 2>"$stage/create-proof.log"
remaining=$((deadline-SECONDS));test "$remaining" -gt 0
timeout --signal=TERM --kill-after=5s "${remaining}s" python3 "$verifier" verify "$stage" "$archive" >"$stage/verify-proof.json" 2>"$stage/verify-proof.log"
test "$SECONDS" -lt "$deadline"
test ! -f "$stage/resource-aborted"
phase=complete;finished=true
printf '{"status":"prepared","stage":"%s","archiveSha256":"%s","nodeVersion":"v22.22.3","pnpmVersion":"12.3.4","completed":true}\n' "$stage" "$expected_hash"
