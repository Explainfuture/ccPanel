#!/usr/bin/env bash
# Two short real empty-DB polling windows; production services stay running.
# Launcher precreates state0700 and provides a supervised root MainPID unit.
set +x
set -euo pipefail
umask 077
: "${CCPANEL_POLLING_STATE_DIR:?Set the private launcher-created state directory}"
: "${CCPANEL_POLLING_CONTROLLER_UNIT:?Set the independently supervised unit}"
test "$(id -u)" = 0
test "${CCPANEL_EXPECTED_RELEASE:-}" = m4-sdk-013
state=$CCPANEL_POLLING_STATE_DIR
[[ "$state" =~ ^/root/ccpanel-polling220\.[A-Za-z0-9]{6}$ ]]
[[ "$CCPANEL_POLLING_CONTROLLER_UNIT" =~ ^ccpanel-polling220-[a-f0-9]{16}\.service$ ]]
test -d "$state";test ! -L "$state";test "$(readlink -f "$state")" = "$state"
test "$(stat -c '%u:%a' "$state")" = 0:700
test -z "$(find "$state" -mindepth 1 -maxdepth 1 -print -quit)"
operator=$(cd -P "$(dirname "${BASH_SOURCE[0]}")" && pwd)
[[ "$operator" =~ ^/opt/ccpanel/shared/memory220\.[A-Za-z0-9]{6}$ ]]
test "$(stat -c '%u:%a' "$operator")" = 0:700
cleanup_script="$operator/api-polling-vps-cleanup.sh"
for file in "$operator/api-polling-vps-remote.sh" "$cleanup_script" "$operator/api-polling-check.mjs" "$operator/api-polling-child.mjs";do
  test -f "$file";test -s "$file";test ! -L "$file";test "$(readlink -f "$file")" = "$file";test "$(stat -c '%u:%a' "$file")" = 0:600
done
unit_object=$(timeout --signal=TERM --kill-after=2s 5s busctl call org.freedesktop.systemd1 /org/freedesktop/systemd1 org.freedesktop/systemd1.Manager GetUnit s "$CCPANEL_POLLING_CONTROLLER_UNIT")
[[ "$unit_object" =~ ^o\ \"(/org/freedesktop/systemd1/unit/[A-Za-z0-9_]+)\"$ ]];unit_object=${BASH_REMATCH[1]}
unit_property(){ timeout --signal=TERM --kill-after=2s 5s busctl get-property org.freedesktop.systemd1 "$unit_object" org.freedesktop.systemd1.Service "$1"; }
test "$(unit_property User)" = 's "root"';test "$(unit_property MainPID)" = "u $$"
test "$(unit_property RuntimeMaxUSec)" = 't 180000000';test "$(unit_property TimeoutStopUSec)" = 't 60000000'
test "$(unit_property KillMode)" = 's "control-group"';test "$(unit_property MemoryMax)" = 't 100663296'
stop_post=$(timeout --signal=TERM --kill-after=2s 5s systemctl show "$CCPANEL_POLLING_CONTROLLER_UNIT" --property=ExecStopPost --value)
[[ "$stop_post" == *"argv[]=/bin/bash $cleanup_script ;"* ]]
exec 8>/opt/ccpanel/shared/deploy.lock;flock -n 8
release=/opt/ccpanel/releases/m4-sdk-013
test "$(readlink -f /opt/ccpanel/current)" = "$release"
test "$(sha256sum "$release/dist/api.mjs" | cut -d' ' -f1)" = f49be1a9c3e9cfd202f47faa87ccd7ace190e4685cc04aae77c85133bb0bd6cf
test "$(sha256sum "$release/dist/worker.mjs" | cut -d' ' -f1)" = fafdbe510b80547d20da6f2f8821cd60066f5a78e9b2999db2aba0a4a07f8d29
test "$(sha256sum "$release/dist/web/sdk/ccpanel.js" | cut -d' ' -f1)" = 449f14f4719751a13ef1a666ff141511dd223ddc93b246968d87bf2688c4b661
node=/opt/ccpanel/shared/tools207/node
test -f "$node";test ! -L "$node";test "$(stat -c %u "$node")" = 0
test "$(sha256sum "$node" | cut -d' ' -f1)" = e6ec2c188d83d813f81f2de8aea084d74dce603ac1abedd0a30ad941b10087b2
test "$(timeout --signal=TERM --kill-after=2s 5s "$node" --version)" = v22.22.3
capacity_units=$(timeout --signal=TERM --kill-after=2s 5s systemctl list-units --type=service --state=running 'ccpanel-capacity-*' --no-legend --plain);test -z "$capacity_units"
curl --max-time 5 -fsS https://192.236.151.125/ready >/dev/null
test "$(awk '/MemAvailable:/ {print ($2 >= 307200)}' /proc/meminfo)" = 1
if [ ! -e "$operator/node_modules" ] && [ ! -L "$operator/node_modules" ];then ln -s "$release/node_modules" "$operator/node_modules";fi
test -L "$operator/node_modules";test "$(readlink -f "$operator/node_modules")" = "$release/node_modules"
source /opt/ccpanel/shared/runtime.env
cd "$release/infra"
benchmark=;watcher=;benchmark_start=
identity(){
  local line tail;test -r "/proc/$1/stat" || return 1;line=$(<"/proc/$1/stat");tail=${line##*) };read -ra proc_fields <<<"$tail"
  test "${proc_fields[2]}" = "$1" || return 1;printf '%s' "${proc_fields[19]}"
}
stop_benchmark(){
  if [ -n "$watcher" ];then kill "$watcher" 2>/dev/null || true;wait "$watcher" 2>/dev/null || true;watcher=;fi
  if [ -n "$benchmark" ] && [ "$(identity "$benchmark" 2>/dev/null || true)" = "$benchmark_start" ];then kill -TERM -- "-$benchmark" 2>/dev/null || true;fi
}
finish(){
  local status=$? cleanup_status=0
  trap - EXIT INT TERM;set +e;stop_benchmark
  timeout --signal=TERM --kill-after=5s 50s /bin/bash "$cleanup_script" >"$state/cleanup.log" 2>&1 || cleanup_status=$?
  if [ "$cleanup_status" != 0 ];then status=1;fi
  CCPANEL_POLLING_STATUS="$status" python3 - "$state" <<'PY' || status=1
import json,os,pathlib,sys
p=pathlib.Path(sys.argv[1]);cleanup={}
try:cleanup=json.loads((p/'cleanup-result.json').read_text())
except (OSError,ValueError):pass
passed=os.environ['CCPANEL_POLLING_STATUS']=='0' and cleanup.get('passed') is True
result={'passed':passed,'isolated_empty_database_polling':True,'production_services_modified':False,'temporary_database_removed':cleanup.get('temporary_database_removed',False),'https_ready':cleanup.get('https_ready',False),'root_business_counts_checked':cleanup.get('root_business_counts_checked',False),'original_services_running_and_unchanged':cleanup.get('original_services_running_and_unchanged',False),'capacity_verified':False}
(p/'result.json').write_text(json.dumps(result)+'\n');print(json.dumps(result))
PY
  echo "Private polling evidence: $state" >&2
  exit "$status"
}
trap finish EXIT;trap 'exit 143' TERM;trap 'exit 130' INT
timeout --signal=TERM --kill-after=2s 5s docker inspect --format '{{.Name}}|{{.Id}}|{{.State.StartedAt}}|{{.State.Running}}' ccpanel-api-1 ccpanel-worker-1 ccpanel-kafka-1 >"$state/original-services-before.txt" 2>"$state/original-services-before.log"
test "$(grep -c '|true$' "$state/original-services-before.txt")" = 3
test "$(timeout --signal=TERM --kill-after=2s 10s docker compose exec -T postgres psql -X -U ccpanel -d ccpanel -Atqc 'SELECT max(version) FROM schema_migrations')" = 22
pg_host=$(timeout --signal=TERM --kill-after=2s 5s docker inspect --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' ccpanel-postgres-1)
[[ "$pg_host" =~ ^172\.(1[6-9]|2[0-9]|3[01])\.[0-9]{1,3}\.[0-9]{1,3}$ ]]
db="ccpanel_restore_$(date -u +%Y%m%d%H%M%S)_$(openssl rand -hex 4)"
[[ "$db" =~ ^ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$ ]]
existing=$(timeout --signal=TERM --kill-after=2s 10s docker compose exec -T postgres psql -X -U ccpanel -d postgres -At -v ON_ERROR_STOP=1 -c "SELECT 1 FROM pg_database WHERE datname='$db'");test -z "$existing"
container="${CCPANEL_POLLING_CONTROLLER_UNIT%.service}-setup"
printf '{"version":1,"unit":"%s","database":"%s","setupContainer":"%s"}\n' "$CCPANEL_POLLING_CONTROLLER_UNIT" "$db" "$container" >"$state/owned.tmp"
mv -- "$state/owned.tmp" "$state/owned.json"
native_uri=$(DATABASE_URL="$DATABASE_URL" CCPANEL_TEST_PG_HOST="$pg_host" CCPANEL_TEMP_DB="$db" python3 - "$state/test.env" <<'PY'
import ipaddress,os,pathlib,sys,urllib.parse
u=urllib.parse.urlsplit(os.environ['DATABASE_URL']);db=os.environ['CCPANEL_TEMP_DB'];ip=os.environ['CCPANEL_TEST_PG_HOST']
if u.scheme not in ('postgres','postgresql') or u.username!='ccpanel' or u.path!='/ccpanel' or u.fragment or u.port not in (None,5432) or any(k!='sslmode' for k,v in urllib.parse.parse_qsl(u.query)):raise SystemExit('polling_url_invalid')
if ipaddress.ip_address(ip) not in ipaddress.ip_network('172.16.0.0/12'):raise SystemExit('polling_ip_invalid')
base=urllib.parse.urlunsplit((u.scheme,u.netloc,'/'+db,u.query,''))
if '\n' in base or '\r' in base:raise SystemExit('polling_url_invalid')
with open(sys.argv[1],'x',opener=lambda p,f:os.open(p,f,0o600)) as f:f.write('DATABASE_URL='+base+'\nRETENTION_ENABLED=false\nOPERATIONAL_COLLECTION_ENABLED=false\n')
auth=u.netloc.rsplit('@',1)[0];native=urllib.parse.urlunsplit((u.scheme,auth+'@'+ip+':5432','/'+db,u.query,''));print(native)
PY
)
timeout --signal=TERM --kill-after=2s 10s docker compose exec -T postgres createdb -U ccpanel "$db" >"$state/database-create.log" 2>&1
run(){ timeout --signal=TERM --kill-after=3s 20s docker compose run --rm -T --no-deps --name "$container" --env-from-file "$state/test.env" api node "dist/$1.mjs" "${@:2}"; }
run migrate >"$state/migrate.log" 2>&1
run deletion-ledger export >"$state/ledger.json" 2>"$state/ledger-export.log"
timeout --signal=TERM --kill-after=3s 20s docker compose run --rm -T --no-deps --name "$container" --env-from-file "$state/test.env" -e CCPANEL_OFFLINE_RESTORE=true api node dist/deletion-ledger.mjs apply <"$state/ledger.json" >"$state/ledger-apply.log" 2>&1
export DATABASE_URL="$native_uri" CCPANEL_ISOLATED_POLLING_VPS=true CCPANEL_EXPECTED_RELEASE=m4-sdk-013 NODE_OPTIONS=--max-old-space-size=32 RETENTION_ENABLED=false OPERATIONAL_COLLECTION_ENABLED=false
setsid timeout --signal=TERM --kill-after=3s 110s "$node" "$operator/api-polling-check.mjs" >"$state/proof.json" 2>"$state/check.log" & benchmark=$!
for attempt in $(seq 1 20);do benchmark_start=$(identity "$benchmark" 2>/dev/null || true);if [ -n "$benchmark_start" ];then break;fi;sleep 0.05;done
test -n "$benchmark_start"
(
  while [ "$(identity "$benchmark" 2>/dev/null || true)" = "$benchmark_start" ];do
    if test "$(awk '/MemAvailable:/ {print ($2 < 204800)}' /proc/meminfo)" = 1;then
      printf '{"resource_guard":"mem_available_below_200MiB"}\n' >"$state/resource-guard.json"
      kill -TERM -- "-$benchmark" 2>/dev/null || true;exit 1
    fi
    sleep 1
  done
) & watcher=$!
wait "$benchmark"
stop_benchmark
test ! -f "$state/resource-guard.json"
python3 - "$state/proof.json" <<'PY'
import json,sys
v=json.load(open(sys.argv[1]))
if v.get('passed') is not True or v.get('surface')!='isolated_vps_tooling' or v.get('vps_private_backend_memory_measured') is not True:raise SystemExit('polling_proof_failed')
PY
