#!/usr/bin/env bash
# Staged binaries exercise real services without switching current or publishing
# ports. All resources are generated here and confined to disposable namespaces.
set -euo pipefail
umask 077
source /opt/ccpanel/shared/runtime.env
cd /opt/ccpanel/current/infra
db="ccpanel_restore_$(date -u +%Y%m%d%H%M%S)_$(openssl rand -hex 4)"
[[ "$db" =~ ^ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$ ]]
prefix="ccpanel.restore.$(openssl rand -hex 6)"
api="ccpanel-replay-api-$(openssl rand -hex 4)"
worker="ccpanel-replay-worker-$(openssl rand -hex 4)"
dir="/opt/ccpanel/shared/dlq-live-$(openssl rand -hex 6)"
mkdir -m 700 "$dir"
target="${DATABASE_URL%/*}/$db"
printf 'DATABASE_URL=%s\nCCPANEL_REPLAY_PREFIX=%s\nTEST_API=http://%s:3000\nKAFKA_EVENTS_TOPIC=%s.events\nKAFKA_DLQ_TOPIC=%s.dlq\nKAFKA_GROUP_ID=%s.writer\n' "$target" "$prefix" "$api" "$prefix" "$prefix" "$prefix" >"$dir/test.env"
topics=false
run(){ docker compose run --rm -T --no-deps --env-from-file "$dir/test.env" api node "$@"; }
cleanup(){
  docker stop --time 10 "$api" "$worker" >/dev/null 2>&1 || true
  docker rm -f "$api" "$worker" >/dev/null 2>&1 || true
  if [ "$topics" = true ]; then run scripts/dlq-live-check.mjs cleanup || true; fi
  docker compose exec -T postgres dropdb -U ccpanel --if-exists --force "$db"
  rm -f "$dir/fixture.json" "$dir/ledger.json" "$dir/test.env"
}
trap cleanup EXIT
started=$(date +%s)
docker compose exec -T postgres createdb -U ccpanel "$db"
run scripts/staged-migrate-v8.mjs
run scripts/dlq-live-check.mjs create >"$dir/fixture.json"
# This is a newly-created synthetic source, never an old restored backup. Apply
# its explicit fresh ledger through the same offline gate instead of UPDATE SQL.
run scripts/staged-deletion-ledger-v8.mjs export >"$dir/ledger.json"
docker compose run --rm -T --no-deps --env-from-file "$dir/test.env" -e CCPANEL_OFFLINE_RESTORE=true api node scripts/staged-deletion-ledger-v8.mjs apply <"$dir/ledger.json"
topics=true
run scripts/dlq-live-check.mjs prepare <"$dir/fixture.json"
docker compose run --rm -d -T --name "$api" --no-deps --env-from-file "$dir/test.env" api node scripts/staged-api-v8.mjs >"$dir/api-id"
docker compose run --rm -d -T --name "$worker" --no-deps --env-from-file "$dir/test.env" api node scripts/staged-worker-v8.mjs >"$dir/worker-id"
if ! run scripts/dlq-live-check.mjs verify <"$dir/fixture.json" >"$dir/result.json"; then
  docker logs "$api" >"$dir/api.log" 2>&1 || true
  docker logs "$worker" >"$dir/worker.log" 2>&1 || true
  echo "Live replay verification failed; protected diagnostics: $dir" >&2
  exit 1
fi
cat "$dir/result.json"
printf 'LIVE REPLAY COMPLETE elapsed_seconds=%s\n' "$(( $(date +%s)-started ))"
