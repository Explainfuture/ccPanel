#!/usr/bin/env bash
# Private PG response proxy + actual Worker SIGKILL after durable checkpoint.
set -euo pipefail
umask 077
source /opt/ccpanel/shared/runtime.env
cd /opt/ccpanel/current/infra
db="ccpanel_restore_$(date -u +%Y%m%d%H%M%S)_$(openssl rand -hex 4)"
[[ "$db" =~ ^ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$ ]]
prefix="ccpanel.restore.$(openssl rand -hex 6)"
api="ccpanel-checkpoint-api-$(openssl rand -hex 4)"
worker="ccpanel-checkpoint-worker-$(openssl rand -hex 4)"
runner="ccpanel-checkpoint-runner-$(openssl rand -hex 4)"
proxy="ccpanel-checkpoint-proxy-$(openssl rand -hex 4)"
dir=$(mktemp -d /opt/ccpanel/shared/checkpoint-live.XXXXXX)
mkdir "$dir/control"; chown 1000:1000 "$dir/control"; chmod 700 "$dir/control"
printf 'DATABASE_URL=%s/%s\nCCPANEL_REPLAY_PREFIX=%s\nTEST_API=http://%s:3000\nKAFKA_EVENTS_TOPIC=%s.events\nKAFKA_DLQ_TOPIC=%s.dlq\nKAFKA_GROUP_ID=%s.writer\n' "${DATABASE_URL%/*}" "$db" "$prefix" "$api" "$prefix" "$prefix" "$prefix" >"$dir/test.env"
created=false;topics=false
run(){ docker compose run --rm -T --no-deps --env-from-file "$dir/test.env" api node "$@"; }
start_worker(){ docker compose run -d -T --name "$worker" --no-deps --env-from-file "$dir/worker.env" api node scripts/staged-worker-v10.mjs >"$dir/worker-id"; }
cleanup(){
  docker stop --time 10 "$runner" "$worker" "$proxy" "$api" >/dev/null 2>&1 || true
  docker rm -f "$runner" "$worker" "$proxy" "$api" >/dev/null 2>&1 || true
  if [ "$topics" = true ]; then run scripts/dlq-live-check.mjs cleanup || true; fi
  if [ "$created" = true ]; then docker compose exec -T postgres dropdb -U ccpanel --if-exists --force "$db"; fi
  rm -f "$dir/test.env" "$dir/worker.env" "$dir/ledger.json" "$dir/control/fixture.json"
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
while IFS= read -r line; do case "$line" in DATABASE_URL=*) ;; *) printf '%s\n' "$line";; esac; done <"$dir/test.env" >"$dir/worker.env"
CCPANEL_PROXY_HOST="$proxy" docker compose run --rm -T --no-deps --env-from-file "$dir/test.env" -e CCPANEL_PROXY_HOST api node scripts/checkpoint-live-check.mjs worker-env >>"$dir/worker.env"
docker compose run -d -T --name "$proxy" --no-deps --env-from-file "$dir/test.env" -v "$dir/control:/control" api node scripts/checkpoint-commit-proxy.mjs >"$dir/proxy-id"
ready=false
for attempt in $(seq 1 100); do if [ -s "$dir/control/proxy-ready" ]; then ready=true; break; fi; sleep 0.1; done
test "$ready" = true
docker compose run --rm -d -T --name "$api" --no-deps --env-from-file "$dir/test.env" -v /opt/ccpanel/shared/telemetry:/telemetry:ro api node scripts/staged-api-v10.mjs >"$dir/api-id"
start_worker
docker compose run -d -T --name "$runner" --no-deps --env-from-file "$dir/test.env" -v "$dir/control:/control" api node scripts/checkpoint-live-check.mjs verify >"$dir/runner-id"
blocked=false
for attempt in $(seq 1 100); do
  if [ -s "$dir/control/verified-blocked" ]; then blocked=true; break; fi
  if [ "$(docker inspect -f '{{.State.Running}}' "$runner" 2>/dev/null || true)" != true ]; then break; fi
  sleep 1
done
if [ "$blocked" != true ]; then docker logs "$runner" >"$dir/runner.log" 2>&1 || true; docker logs "$proxy" >"$dir/proxy.log" 2>&1 || true; echo "Checkpoint window not verified; protected diagnostics: $dir" >&2; exit 1; fi
docker kill --signal KILL "$worker" >/dev/null
test "$(docker wait "$worker")" = 137
docker rm "$worker" >/dev/null
printf 'killed\n' >"$dir/control/killed"; chmod 644 "$dir/control/killed"
start_worker
finished=false
for attempt in $(seq 1 110); do
  if [ "$(docker inspect -f '{{.State.Running}}' "$runner" 2>/dev/null || true)" != true ]; then finished=true; break; fi
  sleep 1
done
docker logs "$runner" >"$dir/runner.log" 2>&1 || true
if [ "$finished" != true ] || [ "$(docker wait "$runner")" != 0 ]; then echo "Checkpoint replay failed; protected diagnostics: $dir" >&2; exit 1; fi
cat "$dir/runner.log"
printf 'LIVE CHECKPOINT CRASH COMPLETE elapsed_seconds=%s\n' "$(( $(date +%s)-started ))"
