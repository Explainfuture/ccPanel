#!/usr/bin/env bash
# Two read-only production monitor captures. No workload, new DB or restart.
# Launcher: root MainPID, RuntimeMaxSec180/TimeoutStopSec60/MemoryMax64M,
# KillMode=control-group; ExecStopPost=/bin/bash THIS_CANONICAL_SCRIPT cleanup.
# The full policy verifier runs OFF HOST after this unit's actual terminal state.
set +x
set -euo pipefail
umask 077
deadline=$((SECONDS+145))
phase=controller_preflight
test "$(id -u)" = 0
test "${CCPANEL_EXPECTED_RELEASE:-}" = m4-polling-014
: "${CCPANEL_MONITOR_CONTROLLER_UNIT:?Set the supervised monitor unit}"
: "${CCPANEL_MONITOR_STATE_DIR:?Set the launcher-created private state directory}"
unit=$CCPANEL_MONITOR_CONTROLLER_UNIT;state=$CCPANEL_MONITOR_STATE_DIR
[[ "$unit" =~ ^ccpanel-monitor222-[a-f0-9]{16}\.service$ ]]
[[ "$state" =~ ^/opt/ccpanel/shared/monitor222\.[A-Za-z0-9]{6}$ ]]
test -d "$state";test ! -L "$state";test "$(readlink -f "$state")" = "$state"
test "$(stat -c '%u:%a' "$state")" = 0:700
container="${unit%.service}-capture";label="monitor222-${unit#ccpanel-monitor222-}";label=${label%.service}

cleanup_owned(){
  local owned identity failed=0 removed=false
  if [ -e "$state/owner" ] || [ -L "$state/owner" ];then
    test -f "$state/owner" && test ! -L "$state/owner" && test "$(stat -c '%u:%a' "$state/owner")" = 0:600 || return 1
    test "$(wc -c <"$state/owner")" -le 512 || return 1
    local -a owner_fields
    mapfile -t owner_fields <"$state/owner" || return 1
    test "${#owner_fields[@]}" = 2 && test "${owner_fields[0]}" = "$unit" && test "${owner_fields[1]}" = "$container" || return 1
    owned=$(timeout --signal=TERM --kill-after=2s 5s docker ps -a --filter "label=ccpanel.monitor.unit=$unit" --format '{{.Names}}') || return 1
    if [ -n "$owned" ];then
      # An unexpected label inventory is evidence, never a wildcard kill list.
      test "$owned" = "$container" || return 1
      identity=$(timeout --signal=TERM --kill-after=2s 5s docker inspect --format '{{index .Config.Labels "ccpanel.monitor.unit"}}|{{index .Config.Labels "ccpanel.monitor.state"}}' "$container") || return 1
      test "$identity" = "$unit|$state" || return 1
      timeout --signal=TERM --kill-after=2s 5s docker rm -f "$container" >"$state/container-cleanup.log" 2>&1 || failed=1
    fi
    if [ -e "$state/capture.env" ] || [ -L "$state/capture.env" ];then
      test -f "$state/capture.env" && test ! -L "$state/capture.env" && test "$(stat -c '%u:%a' "$state/capture.env")" = 0:600 || return 1
      rm -f -- "$state/capture.env" || return 1
    fi
    owned=$(timeout --signal=TERM --kill-after=2s 5s docker ps -a --filter "label=ccpanel.monitor.unit=$unit" --format '{{.Names}}') || return 1
    if [ -z "$owned" ];then removed=true;else failed=1;fi
  else
    removed=true
  fi
  printf '{"owned_capture_removed":%s,"credential_file_removed":%s}\n' "$removed" "$([ ! -e "$state/capture.env" ] && echo true || echo false)" >"$state/cleanup-result.json"
  return "$failed"
}
if [ "${1:-}" = cleanup ] && [ "$#" = 1 ];then
  if [ "$(readlink /proc/self/fd/8 2>/dev/null || true)" != /opt/ccpanel/shared/deploy.lock ];then exec 8>/opt/ccpanel/shared/deploy.lock;fi
  flock -w 5 8
  cleanup_owned
  exit
fi
test "$#" = 0
: "${CCPANEL_MONITOR_CAPTURE_FILE:?Set the canonical private compiled capture bundle}"
: "${CCPANEL_MONITOR_CAPTURE_SHA256:?Set its independently checked SHA256}"
[[ "$CCPANEL_MONITOR_CAPTURE_SHA256" =~ ^[a-f0-9]{64}$ ]]
self=$(readlink -f "${BASH_SOURCE[0]}")
for file in "$self" "$CCPANEL_MONITOR_CAPTURE_FILE";do
  [[ "$file" = /* ]];test -f "$file";test -s "$file";test ! -L "$file";test "$(readlink -f "$file")" = "$file"
  test "$(stat -c '%u:%a' "$file")" = 0:600
  test "$(stat -c '%u:%a' "$(dirname "$file")")" = 0:700
  case "$file" in /opt/ccpanel/releases/*) echo 'Stage monitor tooling outside immutable releases' >&2;exit 64;; esac
done
test "$(sha256sum "$CCPANEL_MONITOR_CAPTURE_FILE" | cut -d' ' -f1)" = "$CCPANEL_MONITOR_CAPTURE_SHA256"
test -z "$(find "$state" -mindepth 1 -maxdepth 1 -print -quit)"
unit_object=$(timeout --signal=TERM --kill-after=2s 5s busctl call org.freedesktop.systemd1 /org/freedesktop/systemd1 org.freedesktop.systemd1.Manager GetUnit s "$unit")
[[ "$unit_object" =~ ^o\ \"(/org/freedesktop/systemd1/unit/[A-Za-z0-9_]+)\"$ ]];unit_object=${BASH_REMATCH[1]}
unit_property(){ timeout --signal=TERM --kill-after=2s 5s busctl get-property org.freedesktop.systemd1 "$unit_object" org.freedesktop.systemd1.Service "$1"; }
test "$(unit_property User)" = 's "root"';test "$(unit_property MainPID)" = "u $$"
test "$(unit_property RuntimeMaxUSec)" = 't 180000000';test "$(unit_property TimeoutStopUSec)" = 't 60000000'
test "$(unit_property KillMode)" = 's "control-group"'
test "$(timeout --signal=TERM --kill-after=2s 5s busctl get-property org.freedesktop.systemd1 "$unit_object" org.freedesktop.systemd1.Service MemoryMax)" = 't 67108864'
stop_post=$(timeout --signal=TERM --kill-after=2s 5s systemctl show "$unit" --property=ExecStopPost --value)
[[ "$stop_post" == *"argv[]=/bin/bash $self cleanup ;"* ]]
exec 8>/opt/ccpanel/shared/deploy.lock;flock -n 8
release=/opt/ccpanel/releases/m4-polling-014
test "$(readlink -f /opt/ccpanel/current)" = "$release"
test "$(sha256sum "$release/dist/api.mjs" | cut -d' ' -f1)" = f87bcc8d07b2d84fc87ec59e6e91773c8870ef96be6db86ed86bd6718f8731a9
test "$(sha256sum "$release/dist/worker.mjs" | cut -d' ' -f1)" = fafdbe510b80547d20da6f2f8821cd60066f5a78e9b2999db2aba0a4a07f8d29
test "$(sha256sum "$release/dist/web/sdk/ccpanel.js" | cut -d' ' -f1)" = 449f14f4719751a13ef1a666ff141511dd223ddc93b246968d87bf2688c4b661
capacity_units=$(timeout --signal=TERM --kill-after=2s 5s systemctl list-units --type=service --state=running 'ccpanel-capacity-*' 'ccpanel-window*' --no-legend --plain);test -z "$capacity_units"
curl --max-time 5 -fsS https://192.236.151.125/ready >/dev/null
test "$(awk '/MemAvailable:/ {print ($2 >= 307200)}' /proc/meminfo)" = 1
test "$(df -Pk / /opt/ccpanel/shared | awk 'NR>1 && $2>0 {if($4*100/$2<20)bad=1;n++} END{print(n>0&&!bad)}')" = 1
image=$(timeout --signal=TERM --kill-after=2s 5s docker inspect --format '{{.Image}}' ccpanel-api-1)
[[ "$image" =~ ^sha256:[a-f0-9]{64}$ ]]
test "$(timeout --signal=TERM --kill-after=2s 5s docker image inspect node:22.22.3-bookworm-slim --format '{{.Id}}')" = "$image"
network=$(timeout --signal=TERM --kill-after=2s 5s docker inspect --format '{{range $name,$value := .NetworkSettings.Networks}}{{$name}}{{end}}' ccpanel-api-1);test "$network" = ccpanel_default
cd "$release/infra"
watcher=;before_ok=false;after_ok=false;baseline_ready=false
pg(){ timeout --signal=TERM --kill-after=2s 7s docker exec -i ccpanel-postgres-1 psql -X -q -U ccpanel -d ccpanel -At -v ON_ERROR_STOP=1 "$@"; }
fingerprints(){
  pg <<'SQL'
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL timezone='UTC'; SET LOCAL statement_timeout='5s'; SET LOCAL lock_timeout='200ms';
SELECT format('SELECT json_build_array(%L,count(*),coalesce(bit_xor(hashtextextended(row_to_json(t)::text,0)),0)) FROM %I.%I t;',schemaname||'.'||tablename,schemaname,tablename)
FROM pg_tables WHERE schemaname='public' AND tablename NOT IN ('operational_samples','operational_alert_history','pressure_signal_history','dlq_alert_history','operational_alert_states','pressure_signal_states','dlq_alert_states') ORDER BY tablename
\gexec
ROLLBACK;
SQL
}
configuration(){
  readlink -f /opt/ccpanel/current || return 1
  sha256sum "$release/dist/api.mjs" "$release/dist/worker.mjs" "$release/dist/web/sdk/ccpanel.js" "$release/infra/compose.yaml" || return 1
  printf 'swappiness=';cat /proc/sys/vm/swappiness || return 1
  local name identity inspected index observed_name observed_image started restarts oom running memory memory_swap nano_cpus cpu_period cpu_quota extra
  local -a names=(api worker kafka postgres) rows
  inspected=$(timeout --signal=TERM --kill-after=2s 5s docker inspect --format '{{.Name}}|{{.Id}}|{{.Image}}|{{.State.StartedAt}}|{{.RestartCount}}|{{.State.OOMKilled}}|{{.State.Running}}|{{.HostConfig.Memory}}|{{.HostConfig.MemorySwap}}|{{.HostConfig.NanoCpus}}|{{.HostConfig.CpuPeriod}}|{{.HostConfig.CpuQuota}}' ccpanel-api-1 ccpanel-worker-1 ccpanel-kafka-1 ccpanel-postgres-1) || return 1
  mapfile -t rows <<<"$inspected";test "${#rows[@]}" = 4 || return 1
  for index in 0 1 2 3;do
    name=${names[$index]}
    IFS='|' read -r observed_name identity observed_image started restarts oom running memory memory_swap nano_cpus cpu_period cpu_quota extra <<<"${rows[$index]}"
    test "$observed_name" = "/ccpanel-$name-1" && test -z "$extra" && test -n "$started" || return 1
    [[ "$identity" =~ ^[a-f0-9]{64}$ && "$observed_image" =~ ^sha256:[a-f0-9]{64}$ ]] || return 1
    test "$running|$oom|$restarts" = 'true|false|0' || return 1
    printf '%s\n' "${rows[$index]}"
    printf '%s:' "$name";cat "/sys/fs/cgroup/system.slice/docker-$identity.scope/memory.max" "/sys/fs/cgroup/system.slice/docker-$identity.scope/memory.swap.max" "/sys/fs/cgroup/system.slice/docker-$identity.scope/cpu.max" || return 1
  done
  for name in api worker;do
    timeout --signal=TERM --kill-after=2s 5s docker exec "ccpanel-$name-1" node -e "const v={collection:process.env.OPERATIONAL_COLLECTION_ENABLED==='true',retention:process.env.RETENTION_ENABLED==='true'};console.log(JSON.stringify(v));if(!v.collection||!v.retention)process.exitCode=1;" || return 1
  done
}
sessions_gone(){
  local attempt value
  for attempt in $(seq 1 4);do
    value=$(pg -c "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND application_name='ccpanel_monitor_preservation_capture'") || return 1
    if [ "$value" = 0 ];then return 0;fi
    sleep 0.5
  done
  return 1
}
finish(){
  local original_status=$?
  local status=$original_status original_phase=$phase unchanged=false ready=false sessions=false comparison_status=not_performed_missing_baseline
  trap - EXIT INT TERM;set +e
  if [ -n "$watcher" ];then kill "$watcher" 2>/dev/null;wait "$watcher" 2>/dev/null;fi
  cleanup_owned >"$state/cleanup.log" 2>&1 || status=1
  if sessions_gone >"$state/sessions.log" 2>&1;then sessions=true;else status=1;fi
  if configuration >"$state/configuration-after.txt" 2>"$state/configuration-after.log" && fingerprints >"$state/public-after.jsonl" 2>"$state/public-after.log";then
    if [ "$baseline_ready" = true ];then
      if cmp -s "$state/configuration-before.txt" "$state/configuration-after.txt" && cmp -s "$state/public-before.jsonl" "$state/public-after.jsonl";then unchanged=true;comparison_status=matched
      else status=1;comparison_status=mismatch
      fi
    else status=1
    fi
  else status=1;comparison_status=not_performed_final_observation_failed
  fi
  if curl --max-time 5 -fsS https://192.236.151.125/ready >/dev/null 2>&1;then ready=true;else status=1;fi
  if [ -f "$state/resource-guard.json" ] || [ "$before_ok" != true ] || [ "$after_ok" != true ];then status=1;fi
  CCPANEL_MONITOR_STATUS="$status" python3 - "$state" "$before_ok" "$after_ok" "$unchanged" "$ready" "$sessions" "$CCPANEL_MONITOR_CAPTURE_SHA256" "$image" "$original_status" "$original_phase" "$comparison_status" <<'PY' || status=1
import json,os,pathlib,sys
p=pathlib.Path(sys.argv[1]);passed=os.environ['CCPANEL_MONITOR_STATUS']=='0'
result={'capture_controller_completed':passed,'actual_capture_exit_zero':[sys.argv[2]=='true',sys.argv[3]=='true'],'production_configuration_and_nonmonitor_rows_unchanged':sys.argv[4]=='true' if sys.argv[11] in ('matched','mismatch') else None,'comparison_status':sys.argv[11],'original_shell_exit':int(sys.argv[9]),'original_phase':sys.argv[10],'https_ready':sys.argv[5]=='true','capture_pg_sessions_gone':sys.argv[6]=='true','helper_sha256':sys.argv[7],'node_image_id':sys.argv[8],'policy_verifier_run_on_vps':False,'actual_delete_cause_verified':False,'source_authenticated':False,'continuous_availability_verified':False,'capacity_verified':False,'latest_state_verified':False}
for name in ('before','after'):
    try:result[name]=json.loads((p/(name+'-integrity.json')).read_text())
    except (OSError,ValueError):result[name]=None
(p/'result.json').write_text(json.dumps(result)+'\n');print(json.dumps(result))
PY
  echo "Private monitor captures retained for OFFHOST policy verification: $state" >&2
  exit "$status"
}
trap finish EXIT;trap 'exit 143' TERM;trap 'exit 130' INT
phase=ownership_setup
printf '%s\n%s\n' "$unit" "$container" >"$state/owner.tmp";mv -- "$state/owner.tmp" "$state/owner"
install -d -m 700 -o 1000 -g 1000 "$state/captures"
install -m 644 "$CCPANEL_MONITOR_CAPTURE_FILE" "$state/capture.mjs"
test "$(sha256sum "$state/capture.mjs" | cut -d' ' -f1)" = "$CCPANEL_MONITOR_CAPTURE_SHA256"
phase=environment_parse
timeout --signal=TERM --kill-after=2s 5s python3 - "$state/capture.env" >"$state/env-parse.log" 2>&1 <<'PY'
import os,re,stat,sys,urllib.parse

def write_capture_env(source,output):
    descriptor=os.open(source,os.O_RDONLY|getattr(os,'O_NOFOLLOW',0)|getattr(os,'O_NONBLOCK',0))
    with os.fdopen(descriptor,'rb') as file:
        before=os.fstat(file.fileno())
        if not stat.S_ISREG(before.st_mode) or before.st_uid!=os.geteuid() or stat.S_IMODE(before.st_mode)!=0o600 or before.st_nlink!=1 or before.st_size>65536:raise ValueError('unsafe_input')
        data=file.read(65537);after=os.fstat(file.fileno());current=os.lstat(source)
        identity=lambda value:(value.st_dev,value.st_ino,value.st_size,value.st_mtime_ns,value.st_ctime_ns)
        if len(data)>65536 or identity(before)!=identity(after) or identity(before)!=identity(current) or not stat.S_ISREG(current.st_mode):raise ValueError('changed_input')
    if b'\r' in data or b'\0' in data:raise ValueError('invalid_line_endings')
    matches=[]
    for entry in data.decode('utf-8').split('\n'):
        if not re.match(r'^[ \t]*(?:export[ \t]+)?DATABASE_URL(?:[ \t=]|$)',entry):continue
        match=re.fullmatch(r'[ \t]*(?:export[ \t]+)?DATABASE_URL[ \t]*=(.*)',entry)
        if match is None:raise ValueError('invalid_entry')
        value=match[1].strip();quote=value[:1]
        if quote in ("'",'"'):
            closing=value.find(quote,1)
            if closing<0:raise ValueError('invalid_quote')
            remainder=value[closing+1:].strip()
            if remainder and not remainder.startswith('#'):raise ValueError('invalid_quote')
            value=value[1:closing]
        else:
            value=re.split(r'[ \t]+#',value,maxsplit=1)[0].strip()
        # Expand or execute nothing. Ambiguous Compose substitutions/escapes
        # are unsupported; literal single-quoted dollar signs remain literal.
        if not value or any(ord(c)<=32 or ord(c)==127 for c in value) or '\\' in value or (quote!="'" and '$' in value):raise ValueError('invalid_value')
        matches.append(value)
    if len(matches)!=1:raise ValueError('ambiguous_database_entry')
    value=matches[0];uri=urllib.parse.urlsplit(value);query=urllib.parse.parse_qsl(uri.query,keep_blank_values=True)
    if uri.scheme not in ('postgres','postgresql') or uri.username!='ccpanel' or uri.hostname!='postgres' or uri.path!='/ccpanel' or uri.port not in (None,5432) or uri.fragment or len(query)>1 or any(k!='sslmode' or v not in ('disable','allow','prefer','require','verify-ca','verify-full') for k,v in query):raise ValueError('invalid_uri')
    with os.fdopen(os.open(output,os.O_WRONLY|os.O_CREAT|os.O_EXCL|getattr(os,'O_NOFOLLOW',0),0o600),'wb') as file:
        file.write(('CCPANEL_MONITOR_DATABASE_URL='+value+'\nNODE_OPTIONS=--max-old-space-size=32\n').encode('utf-8'));file.flush();os.fsync(file.fileno())

try:write_capture_env('/opt/ccpanel/shared/runtime.env',sys.argv[1])
except (OSError,ValueError,UnicodeError,TypeError):raise SystemExit('monitor_database_env_invalid')
PY
phase=schema_check
schema=$(pg -c 'SELECT max(version) FROM schema_migrations')
test "$schema" = 22
phase=initial_capture_sessions
sessions_gone
phase=baseline_configuration
configuration >"$state/configuration-before.txt" 2>"$state/configuration-before.log"
phase=baseline_public_fingerprints
fingerprints >"$state/public-before.jsonl" 2>"$state/public-before.log"
test "$(wc -l <"$state/public-before.jsonl")" -gt 0
baseline_ready=true
(
  while true;do
    available=$(awk '/^MemAvailable:/ {print $2}' /proc/meminfo 2>/dev/null) || available=
    reason=
    if ! [[ "$available" =~ ^[0-9]{1,15}$ ]];then reason=mem_available_unknown
    elif (( 10#$available < 204800 ));then reason=mem_available_below_200MiB
    fi
    if [ -n "$reason" ];then
      printf '{"resource_guard":"%s"}\n' "$reason" >"$state/resource-guard.json"
      cleanup_owned >"$state/resource-cleanup.log" 2>&1 || true
      exit 1
    fi
    sleep 1
  done
) & watcher=$!
capture(){
  local capture_name=$1 reserve=$2 remaining limit
  phase="${capture_name}_capture_budget"
  test ! -f "$state/resource-guard.json"
  remaining=$((deadline-SECONDS-reserve));test "$remaining" -ge 10
  limit=$remaining;if [ "$limit" -gt 90 ];then limit=90;fi
  phase="${capture_name}_capture_container"
  timeout --signal=TERM --kill-after=3s "${limit}s" docker run --rm --pull never --name "$container" --label "ccpanel.monitor.unit=$unit" --label "ccpanel.monitor.state=$state" \
    --user 1000:1000 --network "$network" --read-only --cap-drop ALL --security-opt no-new-privileges --pids-limit 32 --memory 96m --memory-swap 128m --cpus 0.5 \
    --env-file "$state/capture.env" -e "CCPANEL_MONITOR_CAPTURE_TIMEOUT_MS=$(( (limit-5)*1000 ))" \
    -v "$state/capture.mjs:/tool/capture.mjs:ro" -v "$release/node_modules:/tool/node_modules:ro" -v "$state/captures:/capture-output" --workdir /tool --entrypoint /bin/sh "$image" \
    -c 'test "$(node --version)" = v22.22.3 || exit 1; exec node /tool/capture.mjs "$@"' sh "$label" "/capture-output/$capture_name.jsonl" >"$state/$capture_name-receipt.json" 2>"$state/$capture_name.log"
  # Constant-memory artifact integrity only; do NOT run the policy verifier here.
  phase="${capture_name}_capture_integrity"
  python3 - "$state/captures/$capture_name.jsonl" "$state/$capture_name-receipt.json" >"$state/$capture_name-integrity.json" <<'PY'
import hashlib,json,os,stat,sys
p=sys.argv[1];s=os.lstat(p)
if not stat.S_ISREG(s.st_mode) or s.st_uid!=1000 or stat.S_IMODE(s.st_mode)!=0o600 or s.st_nlink!=1 or not 1<=s.st_size<=32*1024*1024:raise SystemExit('monitor_capture_file_unsafe')
with open(sys.argv[2],'rb') as f:r=f.read(4097)
if len(r)>4096:raise SystemExit('monitor_capture_receipt_invalid')
r=json.loads(r)
if r.get('status')!='captured_unverified' or r.get('bytes')!=s.st_size or r.get('sourceAuthenticated') is not False or r.get('causalPruneVerified') is not False:raise SystemExit('monitor_capture_receipt_invalid')
with open(p,'rb') as f:
    f.seek(max(0,s.st_size-4096));tail=f.read()
footer_line=tail.splitlines(keepends=True)[-1]
if not footer_line.endswith(b'\n'):raise SystemExit('monitor_capture_footer_missing')
footer=json.loads(footer_line);prefix=hashlib.sha256();complete=hashlib.sha256();remaining=s.st_size-len(footer_line)
with open(p,'rb') as f:
    while remaining:
        chunk=f.read(min(65536,remaining))
        if not chunk:raise SystemExit('monitor_capture_partial_file')
        prefix.update(chunk);complete.update(chunk);remaining-=len(chunk)
    complete.update(f.read())
if footer.get('kind')!='footer' or footer.get('rows')!=r.get('rows') or footer.get('counts')!=r.get('counts') or footer.get('prefix_sha256')!=prefix.hexdigest() or r.get('prefixSha256')!=prefix.hexdigest() or r.get('sha256')!=complete.hexdigest():raise SystemExit('monitor_capture_integrity_mismatch')
print(json.dumps({'capture_exit_zero':True,'bytes':s.st_size,'rows':r['rows'],'counts':r['counts'],'sha256':complete.hexdigest(),'prefix_sha256':prefix.hexdigest(),'footer_present':True,'policy_verified':False}))
PY
  phase="${capture_name}_capture_sessions"
  sessions_gone
  test ! -f "$state/resource-guard.json"
}
capture before 80;before_ok=true
phase=between_captures
sleep 35
capture after 35;after_ok=true
phase=completed
