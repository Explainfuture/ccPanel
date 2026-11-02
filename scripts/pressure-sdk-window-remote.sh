#!/usr/bin/env bash
# Preview-only: pause API/Worker, test isolated DB/topics using the actual Worker.
set -euo pipefail
umask 077
: "${CCPANEL_EXPECTED_RELEASE:?Set the exact current preview release}"
[[ "$CCPANEL_EXPECTED_RELEASE" =~ ^[A-Za-z0-9_-]+$ ]]
test "$(readlink -f /opt/ccpanel/current)" = "/opt/ccpanel/releases/$CCPANEL_EXPECTED_RELEASE"
exec 8>/opt/ccpanel/shared/deploy.lock
flock -n 8
test -z "$(systemctl list-units --type=service --state=running 'ccpanel-capacity-*' --no-legend --plain)"
curl -fsS https://192.236.151.125/ready >/dev/null
test "$(awk '/MemAvailable:/ {print ($2 >= 204800)}' /proc/meminfo)" = 1
operator_script="$(cd "$(dirname "$0")" && pwd)/pressure-sdk-window-check.mjs"
test -s "$operator_script"
source /opt/ccpanel/shared/runtime.env
cd /opt/ccpanel/current/infra
test "$(docker compose exec -T postgres psql -X -U ccpanel -d ccpanel -Atqc 'SELECT max(version) FROM schema_migrations')" = 22
root=$(mktemp -d /opt/ccpanel/shared/pressure-sdk-window.XXXXXX)
install -d -m 700 -o 1000 -g 1000 "$root/worker"
db="ccpanel_restore_$(date -u +%Y%m%d%H%M%S)_$(openssl rand -hex 4)"
[[ "$db" =~ ^ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$ ]]
suffix=$(openssl rand -hex 8);prefix="ccpanel.drill.pressuresdk.$suffix"
container="ccpanel-pressuresdk-$suffix";created=false;paused=false
check(){ docker compose run --rm -T --no-deps --env-from-file "$root/test.env" -v "$operator_script:/app/scripts/fault-check.mjs:ro" api node scripts/fault-check.mjs "$@"; }
cleanup(){
  docker rm -f "$container" >/dev/null 2>&1 || true
  if [ -f "$root/test.env" ]; then check cleanup >"$root/kafka-cleanup.log" 2>&1 || echo "Isolated Kafka cleanup needs attention: $root" >&2; fi
  if [ "$created" = true ]; then docker compose exec -T postgres dropdb -U ccpanel --if-exists --force "$db" || echo "Temporary database cleanup needs attention: $root" >&2; fi
  rm -f "$root/test.env" "$root/ledger.json"
  if [ "$paused" = true ]; then docker compose start api worker >/dev/null || echo 'Preview startup needs attention' >&2; fi
}
trap cleanup EXIT
trap 'exit 143' TERM
trap 'exit 130' INT
printf 'DATABASE_URL=%s/%s\nRETENTION_ENABLED=false\nOPERATIONAL_COLLECTION_ENABLED=false\nKAFKA_EVENTS_TOPIC=%s.events\nKAFKA_DLQ_TOPIC=%s.dlq\nKAFKA_GROUP_ID=%s.writer\nCCPANEL_ISOLATED_PRESSURE_SDK_CHECK=true\n' "${DATABASE_URL%/*}" "$db" "$prefix" "$prefix" "$prefix" >"$root/test.env"
paused=true;docker compose stop api worker
fingerprint(){
  docker compose exec -T postgres psql -X -U ccpanel -d ccpanel -At -v ON_ERROR_STOP=1 <../scripts/backup-fingerprint.sql
  docker compose exec -T postgres psql -X -U ccpanel -d ccpanel -At -v ON_ERROR_STOP=1 -c "SELECT json_build_array('public.recovery_gate',count(*),coalesce(bit_xor(hashtextextended(row_to_json(t)::text,0)),0)) FROM recovery_gate t; SELECT json_build_array('public.recovery_import_scopes',count(*),coalesce(bit_xor(hashtextextended(row_to_json(t)::text,0)),0)) FROM recovery_import_scopes t;"
}
fingerprint >"$root/before.jsonl"
docker compose exec -T postgres createdb -U ccpanel "$db";created=true
run(){ docker compose run --rm -T --no-deps --env-from-file "$root/test.env" api node "dist/$1.mjs" "${@:2}"; }
run migrate >"$root/migrate.log" 2>&1
run deletion-ledger export >"$root/ledger.json"
docker compose run --rm -T --no-deps --env-from-file "$root/test.env" -e CCPANEL_OFFLINE_RESTORE=true api node dist/deletion-ledger.mjs apply <"$root/ledger.json" >"$root/ledger-apply.log" 2>&1
# Reuse an existing file mount target under the read-only release bind mount.
# The supplied harness is mounted only into this disposable container.
if ! timeout --signal=TERM --kill-after=10s 240s docker compose run --rm -T --no-deps --name "$container" --env-from-file "$root/test.env" -v "$operator_script:/app/scripts/fault-check.mjs:ro" -v "$root/worker:/drill-evidence" api node scripts/fault-check.mjs >"$root/check.log" 2>&1; then
  echo "Isolated pressure-SDK check failed; inspect private evidence: $root" >&2
  exit 1
fi
cat "$root/check.log"
check cleanup >"$root/kafka-cleanup.log" 2>&1
docker compose exec -T postgres dropdb -U ccpanel --force "$db";created=false
fingerprint >"$root/after.jsonl"
cmp "$root/before.jsonl" "$root/after.jsonl"
rm -f "$root/test.env" "$root/ledger.json"
docker compose start api worker >/dev/null
for attempt in $(seq 1 60); do
  if curl -fsS https://192.236.151.125/ready >/dev/null 2>&1; then paused=false;break;fi
  sleep 1
done
test "$paused" = false
echo "Actual current pressure-SDK failure check passed; root public fingerprints unchanged; evidence: $root"
