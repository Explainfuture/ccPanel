#!/usr/bin/env bash
# Changes only disposable private containers, database and topic namespace.
set -euo pipefail
umask 077
source /opt/ccpanel/shared/runtime.env
cd /opt/ccpanel/current/infra
db="ccpanel_restore_$(date -u +%Y%m%d%H%M%S)_$(openssl rand -hex 4)"
[[ "$db" =~ ^ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$ ]]
prefix="ccpanel.restore.$(openssl rand -hex 6)"
api="ccpanel-pressure-api-$(openssl rand -hex 4)"
worker="ccpanel-pressure-worker-$(openssl rand -hex 4)"
runner="ccpanel-pressure-sdk-$(openssl rand -hex 4)"
dir=$(mktemp -d /opt/ccpanel/shared/pressure-live.XXXXXX)
mkdir "$dir/control"
chown 1000:1000 "$dir/control"
chmod 700 "$dir/control"
printf 'DATABASE_URL=%s/%s\nCCPANEL_REPLAY_PREFIX=%s\nTEST_API=http://%s:3000\nKAFKA_EVENTS_TOPIC=%s.events\nKAFKA_DLQ_TOPIC=%s.dlq\nKAFKA_GROUP_ID=%s.writer\n' "${DATABASE_URL%/*}" "$db" "$prefix" "$api" "$prefix" "$prefix" "$prefix" >"$dir/test.env"
created=false;topics=false
run(){ docker compose run --rm -T --no-deps --env-from-file "$dir/test.env" api node "$@"; }
cleanup(){
  docker stop --time 10 "$runner" "$api" "$worker" >/dev/null 2>&1 || true
  docker rm -f "$runner" "$api" "$worker" >/dev/null 2>&1 || true
  if [ "$topics" = true ]; then run scripts/dlq-live-check.mjs cleanup || true; fi
  if [ "$created" = true ]; then docker compose exec -T postgres dropdb -U ccpanel --if-exists --force "$db"; fi
  rm -f "$dir/test.env" "$dir/ledger.json" "$dir/control/fixture.json" "$dir/control/blocked.json"
}
trap cleanup EXIT
started=$(date +%s)
docker compose exec -T postgres createdb -U ccpanel "$db"
created=true
run scripts/staged-migrate-v10.mjs
run scripts/pressure-live-check.mjs create >"$dir/control/fixture.json"
chown 1000:1000 "$dir/control/fixture.json"
run scripts/staged-deletion-ledger-v10.mjs export >"$dir/ledger.json"
docker compose run --rm -T --no-deps --env-from-file "$dir/test.env" -e CCPANEL_OFFLINE_RESTORE=true api node scripts/staged-deletion-ledger-v10.mjs apply <"$dir/ledger.json"
# Prepare private topics with the production creation policy before either app.
topics=true
run scripts/staged-consumer-progress-v10.mjs prepare
docker compose run --rm -d -T --name "$api" --no-deps --env-from-file "$dir/test.env" -e INGEST_MIN_FREE_BYTES=1099511627776 -v /opt/ccpanel/shared/telemetry:/telemetry:ro api node scripts/staged-api-v10.mjs >"$dir/api-id"
docker compose run --rm -d -T --name "$worker" --no-deps --env-from-file "$dir/test.env" api node scripts/staged-worker-v10.mjs >"$dir/worker-id"
docker compose run -d -T --name "$runner" --no-deps --env-from-file "$dir/test.env" -v "$dir/control:/control" api node scripts/pressure-live-check.mjs verify >"$dir/runner-id"
blocked=false
for attempt in $(seq 1 90); do
  if [ -s "$dir/control/blocked.json" ]; then blocked=true; break; fi
  if [ "$(docker inspect -f '{{.State.Running}}' "$runner" 2>/dev/null || true)" != true ]; then break; fi
  sleep 1
done
if [ "$blocked" != true ]; then docker logs "$runner" >"$dir/result.log" 2>&1 || true; echo "Pressure verification failed; protected diagnostics: $dir" >&2; exit 1; fi
# Recover only this test API's configured threshold. Real host sensor stays on.
docker stop --time 10 "$api" >/dev/null
docker compose run --rm -d -T --name "$api" --no-deps --env-from-file "$dir/test.env" -v /opt/ccpanel/shared/telemetry:/telemetry:ro api node scripts/staged-api-v10.mjs >"$dir/api-id"
# Keep the runner until cleanup so exit code and logs survive normal completion.
passed=false
for attempt in $(seq 1 100); do
  docker logs "$runner" >"$dir/result.log" 2>&1 || true
  if grep -q '"passed":true' "$dir/result.log"; then passed=true; break; fi
  if [ "$(docker inspect -f '{{.State.Running}}' "$runner" 2>/dev/null || true)" != true ]; then break; fi
  sleep 1
done
if [ "$passed" != true ]; then echo "Pressure recovery failed; protected diagnostics: $dir" >&2; exit 1; fi
exit_code=$(docker wait "$runner")
test "$exit_code" = 0
cat "$dir/result.log"
printf 'LIVE PRESSURE COMPLETE elapsed_seconds=%s\n' "$(( $(date +%s)-started ))"
