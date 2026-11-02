#!/usr/bin/env bash
# Idempotent ExecStopPost: only this unit's reserved DB/container/files.
# Original API, Worker and Kafka are inspected, never stopped or restarted.
set +x
set -euo pipefail
umask 077
test "$(id -u)" = 0
: "${CCPANEL_POLLING_STATE_DIR:?Set the private launcher-created state directory}"
: "${CCPANEL_POLLING_CONTROLLER_UNIT:?Set the owning polling unit}"
state=$CCPANEL_POLLING_STATE_DIR
[[ "$state" =~ ^/root/ccpanel-polling220\.[A-Za-z0-9]{6}$ ]]
[[ "$CCPANEL_POLLING_CONTROLLER_UNIT" =~ ^ccpanel-polling220-[a-f0-9]{16}\.service$ ]]
test -d "$state";test ! -L "$state";test "$(readlink -f "$state")" = "$state"
test "$(stat -c '%u:%a' "$state")" = 0:700
test "${CCPANEL_EXPECTED_RELEASE:-}" = m4-sdk-013
test "$(readlink -f /opt/ccpanel/current)" = /opt/ccpanel/releases/m4-sdk-013
if [ "$(readlink /proc/self/fd/8 2>/dev/null || true)" != /opt/ccpanel/shared/deploy.lock ];then exec 8>/opt/ccpanel/shared/deploy.lock;fi
flock -w 5 8
failed=0;removed=false;ready=false;counts=false;services=false
marker="$state/owned.json"
if [ -e "$marker" ] || [ -L "$marker" ];then
  # Parse data only, with exact fields and fixed resource-name patterns. Never
  # source a marker or accept a database/container name from command arguments.
  owned=$(CCPANEL_POLLING_CONTROLLER_UNIT="$CCPANEL_POLLING_CONTROLLER_UNIT" python3 - "$marker" <<'PY'
import json,os,re,stat,sys
p=sys.argv[1];s=os.lstat(p)
if not stat.S_ISREG(s.st_mode) or s.st_uid!=0 or stat.S_IMODE(s.st_mode)!=0o600 or s.st_size>2048:raise SystemExit('polling_marker_unsafe')
v=json.load(open(p));unit=os.environ['CCPANEL_POLLING_CONTROLLER_UNIT']
if set(v)!={'version','unit','database','setupContainer'} or v['version']!=1 or v['unit']!=unit:raise SystemExit('polling_marker_invalid')
if not isinstance(v['database'],str) or not re.fullmatch(r'ccpanel_restore_[0-9]{14}_[a-f0-9]{8}',v['database']):raise SystemExit('polling_marker_invalid')
expected=unit.removesuffix('.service')+'-setup'
if v['setupContainer']!=expected:raise SystemExit('polling_marker_invalid')
print(v['database']);print(v['setupContainer'])
PY
  )
  mapfile -t owned_fields <<<"$owned";test "${#owned_fields[@]}" = 2
  db=${owned_fields[0]};container=${owned_fields[1]}
  source /opt/ccpanel/shared/runtime.env
  cd /opt/ccpanel/releases/m4-sdk-013/infra
  timeout --signal=TERM --kill-after=1s 3s docker rm -f "$container" >"$state/container-cleanup.log" 2>&1 || true
  if remaining_container=$(timeout --signal=TERM --kill-after=1s 3s docker ps -aq --filter "name=^/${container}$") && [ -z "$remaining_container" ];then :
  else failed=1
  fi
  if ! timeout --signal=TERM --kill-after=2s 8s docker compose exec -T postgres dropdb -U ccpanel --if-exists --force "$db" >"$state/database-cleanup.log" 2>&1;then failed=1;fi
  if remaining=$(timeout --signal=TERM --kill-after=1s 3s docker compose exec -T postgres psql -X -U ccpanel -d postgres -At -v ON_ERROR_STOP=1 -c "SELECT 1 FROM pg_database WHERE datname='$db'") && [ -z "$remaining" ];then removed=true
  else failed=1
  fi
  # Exact files owned under this marker are the only filesystem removals.
  rm -f -- "$state/test.env" "$state/ledger.json"
else
  # A failure before reservation owns no DB or setup container to remove.
  removed=true
  source /opt/ccpanel/shared/runtime.env
  cd /opt/ccpanel/releases/m4-sdk-013/infra
fi
if timeout --signal=TERM --kill-after=1s 3s docker compose exec -T postgres psql -X -U ccpanel -d ccpanel -At -v ON_ERROR_STOP=1 -c "SELECT json_build_object('raw',(SELECT count(*) FROM events),'dedup',(SELECT count(*) FROM event_dedup),'dlq',(SELECT count(*) FROM dlq_records),'offset',(SELECT coalesce(sum(next_offset),0)::text FROM consumer_progress))" >"$state/root-business-after.json" 2>"$state/root-business-after.log" &&
  python3 - "$state/root-business-after.json" <<'PY'
import json,sys
if json.load(open(sys.argv[1]))!={'raw':2,'dedup':3,'dlq':0,'offset':'15618'}:raise SystemExit('polling_root_business_check_failed')
PY
then counts=true
else failed=1
fi
if timeout --signal=TERM --kill-after=1s 3s docker inspect --format '{{.Name}}|{{.Id}}|{{.State.StartedAt}}|{{.State.Running}}' ccpanel-api-1 ccpanel-worker-1 ccpanel-kafka-1 >"$state/original-services-after.txt" 2>"$state/original-services-after.log" &&
  test "$(grep -c '|true$' "$state/original-services-after.txt")" = 3;then
  if [ -f "$state/original-services-before.txt" ] && cmp -s "$state/original-services-before.txt" "$state/original-services-after.txt";then services=true
  else failed=1
  fi
else failed=1
fi
if curl --max-time 5 -fsS https://192.236.151.125/ready >/dev/null 2>&1;then ready=true
else failed=1
fi
passed=false;if [ "$failed" = 0 ];then passed=true;fi
printf '{"passed":%s,"temporary_database_removed":%s,"original_services_running_and_unchanged":%s,"root_business_counts_checked":%s,"https_ready":%s}\n' "$passed" "$removed" "$services" "$counts" "$ready" >"$state/cleanup-result.json"
exit "$failed"
