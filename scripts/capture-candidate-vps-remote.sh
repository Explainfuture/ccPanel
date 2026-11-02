#!/usr/bin/env bash
# Preview-only. All captured data and keys belong to fresh synthetic fixtures.
set -euo pipefail
umask 077
: "${CCPANEL_EXPECTED_RELEASE:?exact release required}"
: "${CCPANEL_CAPTURE_NODE:?verified private Node executable required}"
: "${CCPANEL_CAPTURE_NODE_SHA:?verified executable hash required}"
test "$CCPANEL_EXPECTED_RELEASE" = m4-sdk-013
test "$(readlink -f /opt/ccpanel/current)" = "/opt/ccpanel/releases/$CCPANEL_EXPECTED_RELEASE"
[[ "$CCPANEL_CAPTURE_NODE_SHA" =~ ^[a-f0-9]{64}$ ]]
test "$(sha256sum "$CCPANEL_CAPTURE_NODE" | cut -d' ' -f1)" = "$CCPANEL_CAPTURE_NODE_SHA"
test "$("$CCPANEL_CAPTURE_NODE" --version)" = v22.22.3
operator=$(cd "$(dirname "$0")" && pwd)
test -s "$operator/backup-capture-cli.mjs";test -s "$operator/backup-capture.mjs";test -s "$operator/encrypt-backup.mjs"
exec 8>/opt/ccpanel/shared/deploy.lock
flock -n 8
test -z "$(systemctl list-units --type=service --state=running 'ccpanel-capacity-*' --no-legend --plain)"
curl -fsS https://192.236.151.125/ready >/dev/null
test "$(awk '/MemAvailable:/ {print ($2 >= 204800)}' /proc/meminfo)" = 1
source /opt/ccpanel/shared/runtime.env
cd /opt/ccpanel/current/infra
test "$(docker compose exec -T postgres psql -X -U ccpanel -d ccpanel -Atqc 'SELECT max(version) FROM schema_migrations')" = 22
root=$(mktemp -d /opt/ccpanel/shared/candidate-vps.XXXXXX)
install -d -m 700 "$root/fixtures"
db="ccpanel_restore_$(date -u +%Y%m%d%H%M%S)_$(openssl rand -hex 4)"
[[ "$db" =~ ^ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$ ]]
restore_db="ccpanel_restore_$(date -u +%Y%m%d%H%M%S)_$(openssl rand -hex 4)"
[[ "$restore_db" =~ ^ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$ ]];test "$restore_db" != "$db"
unit="ccpanel-candidate-207-$(openssl rand -hex 8)"
created=false;restored_created=false;paused=false
cleanup(){
  systemctl stop "$unit.service" >/dev/null 2>&1 || true
  if [ "$restored_created" = true ]; then docker compose exec -T postgres dropdb -U ccpanel --if-exists --force "$restore_db" || echo "Isolated restore cleanup needs attention: $root" >&2; fi
  if [ "$created" = true ]; then docker compose exec -T postgres dropdb -U ccpanel --if-exists --force "$db" || echo "Isolated source cleanup needs attention: $root" >&2; fi
  rm -f "$root/test.env" "$root/host.env" "$root/ledger.json"
  if [ "$paused" = true ]; then docker compose start api worker >/dev/null || echo 'Preview startup needs attention' >&2; fi
}
trap cleanup EXIT
trap 'exit 143' TERM
trap 'exit 130' INT
printf 'DATABASE_URL=%s/%s\nRETENTION_ENABLED=false\nOPERATIONAL_COLLECTION_ENABLED=false\n' "${DATABASE_URL%/*}" "$db" >"$root/test.env"
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
docker compose exec -T postgres createdb -U ccpanel "$restore_db";restored_created=true
pg_host=$(docker inspect --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' ccpanel-postgres-1)
[[ "$pg_host" =~ ^172\.(1[6-9]|2[0-9]|3[01])\.[0-9]{1,3}\.[0-9]{1,3}$ ]]
printf 'DATABASE_URL=%s/%s\nCCPANEL_TEST_PG_HOST=%s\nCCPANEL_CAPTURE_RESTORE_DB=%s\n' "${DATABASE_URL%/*}" "$db" "$pg_host" "$restore_db" >"$root/host.env"
printf 'CCPANEL_PG_CONTAINER=ccpanel-postgres-1\nCCPANEL_ISOLATED_CAPTURE_VPS=true\nCCPANEL_CAPTURE_VPS_READY=true\nCCPANEL_CAPTURE_CLI=%s/backup-capture-cli.mjs\nCCPANEL_CAPTURE_CHECK=%s/backup-capture.mjs\nCCPANEL_EXPECTED_RELEASE=%s\nTMPDIR=%s/fixtures\nNODE_OPTIONS=--max-old-space-size=32\n' "$operator" "$operator" "$CCPANEL_EXPECTED_RELEASE" "$root" >>"$root/host.env"
printf '%s\n' "$unit.service" >"$root/unit.txt"
# URL transformation stays in a private wrapper; no credentials are printed.
if ! systemd-run --quiet --wait --pipe --collect --unit="$unit" --property=MemoryMax=192M --property=RuntimeMaxSec=180s --property=TimeoutStopSec=5s --property=KillMode=control-group --property="EnvironmentFile=$root/host.env" "$CCPANEL_CAPTURE_NODE" --input-type=module -e 'import{pathToFileURL}from"node:url";const u=new URL(process.env.DATABASE_URL);u.hostname=process.env.CCPANEL_TEST_PG_HOST;process.env.DATABASE_URL=u.href;await import(pathToFileURL(process.env.CCPANEL_CAPTURE_CHECK).href);' >"$root/check.log" 2>&1; then
  echo "Candidate VPS check failed; inspect private evidence: $root" >&2;exit 1
fi
cat "$root/check.log"
test ! -d "/sys/fs/cgroup/system.slice/$unit.service"
docker compose exec -T postgres dropdb -U ccpanel --force "$restore_db";restored_created=false
docker compose exec -T postgres dropdb -U ccpanel --force "$db";created=false
fingerprint >"$root/after.jsonl";cmp "$root/before.jsonl" "$root/after.jsonl"
test -z "$(find "$root/fixtures" -mindepth 1 -print -quit)"
rm -f "$root/test.env" "$root/host.env" "$root/ledger.json"
docker compose start api worker >/dev/null
for attempt in $(seq 1 60); do if curl -fsS https://192.236.151.125/ready >/dev/null 2>&1; then paused=false;break;fi;sleep 1;done
test "$paused" = false
echo "Isolated candidate CLI and restore passed; root public fingerprints unchanged; evidence: $root"
