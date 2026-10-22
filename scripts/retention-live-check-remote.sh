#!/usr/bin/env bash
# Never target the running namespace: every destructive Kafka operation is
# additionally checked by retention-live-check against its temporary prefix.
set -euo pipefail
umask 077
source /opt/ccpanel/shared/runtime.env
cd /opt/ccpanel/current/infra
db="ccpanel_restore_$(date -u +%Y%m%d%H%M%S)_$(openssl rand -hex 4)"
[[ "$db" =~ ^ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$ ]]
prefix="ccpanel.restore.$(openssl rand -hex 6)"
api="ccpanel-retention-api-$(openssl rand -hex 4)"
worker="ccpanel-retention-worker-$(openssl rand -hex 4)"
dir=$(mktemp -d /opt/ccpanel/shared/retention-live.XXXXXX)
mkdir "$dir/control"; chown 1000:1000 "$dir/control"; chmod 700 "$dir/control"
printf 'DATABASE_URL=%s/%s\nCCPANEL_REPLAY_PREFIX=%s\nTEST_API=http://%s:3000\nKAFKA_EVENTS_TOPIC=%s.events\nKAFKA_DLQ_TOPIC=%s.dlq\nKAFKA_GROUP_ID=%s.writer\n' "${DATABASE_URL%/*}" "$db" "$prefix" "$api" "$prefix" "$prefix" "$prefix" >"$dir/test.env"
created=false;topics=false
run(){ docker compose run --rm -T --no-deps --env-from-file "$dir/test.env" api node "$@"; }
check(){ docker compose run --rm -T --no-deps --env-from-file "$dir/test.env" -v "$dir/control:/control" api node scripts/retention-live-check.mjs "$@"; }
start_api(){ docker compose run --rm -d -T --name "$api" --no-deps --env-from-file "$dir/test.env" -v /opt/ccpanel/shared/telemetry:/telemetry:ro api node scripts/staged-api-v10.mjs >"$dir/api-id"; }
cleanup(){
  docker stop --time 10 "$api" "$worker" >/dev/null 2>&1 || true
  docker rm -f "$api" "$worker" >/dev/null 2>&1 || true
  if [ "$topics" = true ]; then run scripts/dlq-live-check.mjs cleanup || true; fi
  if [ "$created" = true ]; then docker compose exec -T postgres dropdb -U ccpanel --if-exists --force "$db"; fi
  rm -f "$dir/test.env" "$dir/ledger.json" "$dir/control/fixture.json" "$dir/control/gap.json"
}
trap cleanup EXIT
started=$(date +%s)
docker compose exec -T postgres createdb -U ccpanel "$db"; created=true
run scripts/staged-migrate-v10.mjs
run scripts/pressure-live-check.mjs create >"$dir/control/fixture.json"
chown 1000:1000 "$dir/control/fixture.json"
run scripts/staged-deletion-ledger-v10.mjs export >"$dir/ledger.json"
docker compose run --rm -T --no-deps --env-from-file "$dir/test.env" -e CCPANEL_OFFLINE_RESTORE=true api node scripts/staged-deletion-ledger-v10.mjs apply <"$dir/ledger.json"
topics=true
run scripts/staged-consumer-progress-v10.mjs prepare
start_api
docker compose run --rm -d -T --name "$worker" --no-deps --env-from-file "$dir/test.env" api node scripts/staged-worker-v10.mjs >"$dir/worker-id"
check initialize
docker stop --time 10 "$worker" >/dev/null
check enqueue
docker stop --time 10 "$api" >/dev/null
check remove
start_api
check verify
# This restart must retain the same durable incident, not simply latch in RAM.
docker stop --time 10 "$api" >/dev/null
start_api
check verify-restart
if timeout 30 docker compose run --rm -T --no-deps --env-from-file "$dir/test.env" api node scripts/staged-worker-v10.mjs >"$dir/rejected-worker.log" 2>&1; then
  echo 'Worker unexpectedly started through a known history gap' >&2; exit 1
else
  code=$?; test "$code" = 1
fi
printf 'LIVE RETENTION COMPLETE worker_restart_rejected=true elapsed_seconds=%s\n' "$(( $(date +%s)-started ))"
