#!/usr/bin/env bash
# Temporary DB triggers and SIGKILL are confined to this private test runtime.
set -euo pipefail
umask 077
source /opt/ccpanel/shared/runtime.env
cd /opt/ccpanel/current/infra
db="ccpanel_restore_$(date -u +%Y%m%d%H%M%S)_$(openssl rand -hex 4)"
[[ "$db" =~ ^ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$ ]]
prefix="ccpanel.restore.$(openssl rand -hex 6)"
api="ccpanel-crash-api-$(openssl rand -hex 4)"
worker="ccpanel-crash-worker-$(openssl rand -hex 4)"
runner="ccpanel-crash-runner-$(openssl rand -hex 4)"
dir=$(mktemp -d /opt/ccpanel/shared/commit-crash.XXXXXX)
mkdir "$dir/control"; chown 1000:1000 "$dir/control"; chmod 700 "$dir/control"
printf 'DATABASE_URL=%s/%s\nCCPANEL_REPLAY_PREFIX=%s\nTEST_API=http://%s:3000\nKAFKA_EVENTS_TOPIC=%s.events\nKAFKA_DLQ_TOPIC=%s.dlq\nKAFKA_GROUP_ID=%s.writer\n' "${DATABASE_URL%/*}" "$db" "$prefix" "$api" "$prefix" "$prefix" "$prefix" >"$dir/test.env"
created=false;topics=false
run(){ docker compose run --rm -T --no-deps --env-from-file "$dir/test.env" api node "$@"; }
start_worker(){ docker compose run -d -T --name "$worker" --no-deps --env-from-file "$dir/test.env" api node scripts/staged-worker-v10.mjs >"$dir/worker-id"; }
cleanup(){
  docker stop --time 10 "$runner" "$api" "$worker" >/dev/null 2>&1 || true
  docker rm -f "$runner" "$api" "$worker" >/dev/null 2>&1 || true
  if [ "$topics" = true ]; then run scripts/dlq-live-check.mjs cleanup || true; fi
  if [ "$created" = true ]; then docker compose exec -T postgres dropdb -U ccpanel --if-exists --force "$db"; fi
  rm -f "$dir/test.env" "$dir/ledger.json" "$dir/control/fixture.json"
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
docker compose run --rm -d -T --name "$api" --no-deps --env-from-file "$dir/test.env" -v /opt/ccpanel/shared/telemetry:/telemetry:ro api node scripts/staged-api-v10.mjs >"$dir/api-id"
start_worker
for phase in before_raw after_raw; do
  docker compose run -d -T --name "$runner" --no-deps --env-from-file "$dir/test.env" -v "$dir/control:/control" api node scripts/commit-crash-check.mjs "$phase" >"$dir/runner-id"
  blocked=false
  for attempt in $(seq 1 900); do
    if [ -s "$dir/control/$phase.blocked" ]; then blocked=true; break; fi
    # Do not spend a Docker CLI call on every 100ms control-file poll.
    if [ $(( attempt % 30 )) = 0 ] && [ "$(docker inspect -f '{{.State.Running}}' "$runner" 2>/dev/null || true)" != true ]; then break; fi
    sleep 0.1
  done
  if [ "$blocked" != true ]; then docker logs "$runner" >"$dir/$phase.log" 2>&1 || true; echo "Crash window not reached; protected diagnostics: $dir" >&2; exit 1; fi
  docker kill --signal KILL "$worker" >/dev/null
  test "$(docker wait "$worker")" = 137
  docker logs "$worker" >"$dir/$phase.worker.log" 2>&1 || true
  docker rm "$worker" >/dev/null
  printf 'killed\n' >"$dir/control/$phase.killed"
  chmod 644 "$dir/control/$phase.killed"
  resume=false
  for attempt in $(seq 1 100); do
    if [ -s "$dir/control/$phase.resume" ]; then resume=true; break; fi
    sleep 0.1
  done
  if [ "$resume" != true ]; then docker logs "$runner" >"$dir/$phase.log" 2>&1 || true; echo "Crash rollback not verified; protected diagnostics: $dir" >&2; exit 1; fi
  start_worker
  done_waiting=false
  for attempt in $(seq 1 110); do
    if [ "$(docker inspect -f '{{.State.Running}}' "$runner" 2>/dev/null || true)" != true ]; then done_waiting=true; break; fi
    sleep 1
  done
  docker logs "$runner" >"$dir/$phase.log" 2>&1 || true
  if [ "$done_waiting" != true ] || [ "$(docker wait "$runner")" != 0 ]; then echo "Crash recovery failed; protected diagnostics: $dir" >&2; exit 1; fi
  cat "$dir/$phase.log"
  docker rm "$runner" >/dev/null
done
printf 'LIVE COMMIT CRASH COMPLETE elapsed_seconds=%s\n' "$(( $(date +%s)-started ))"
