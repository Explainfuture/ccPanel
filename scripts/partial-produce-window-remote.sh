#!/usr/bin/env bash
# Preview-only: fresh synthetic DB/topics; actual pinned API/Worker run in turn.
# Stage this controller, its sibling harness and the Node SDK outside releases.
# systemd-run must execute this Bash process directly as the unit's MainPID,
# with RuntimeMaxSec=420, TimeoutStopSec=300, KillMode=mixed and an independent
# ExecStopPost rescue that starts the original API/Worker and checks readiness.
set +x
set -euo pipefail
umask 077
: "${CCPANEL_EXPECTED_RELEASE:?Set the exact current preview release}"
: "${CCPANEL_PARTIAL_SDK_FILE:?Set the absolute staged Node SDK bundle path}"
: "${CCPANEL_PARTIAL_SDK_SHA256:?Set the verified staged Node SDK SHA256}"
: "${CCPANEL_PARTIAL_CONTROLLER_UNIT:?Run in a separately supervised root systemd unit}"
test "$(id -u)" = 0
test "$CCPANEL_EXPECTED_RELEASE" = m4-sdk-013
[[ "$CCPANEL_PARTIAL_SDK_FILE" = /* ]]
[[ "$CCPANEL_PARTIAL_SDK_SHA256" =~ ^[a-f0-9]{64}$ ]]
[[ "$CCPANEL_PARTIAL_CONTROLLER_UNIT" =~ ^ccpanel-partialproduce-[a-f0-9]{16}\.service$ ]]
# Read exact D-Bus microseconds; systemctl's human-readable time strings vary.
unit_object=$(timeout --signal=TERM --kill-after=2s 5s busctl call org.freedesktop.systemd1 /org/freedesktop/systemd1 org.freedesktop.systemd1.Manager GetUnit s "$CCPANEL_PARTIAL_CONTROLLER_UNIT")
[[ "$unit_object" =~ ^o\ \"(/org/freedesktop/systemd1/unit/[A-Za-z0-9_]+)\"$ ]]
unit_object=${BASH_REMATCH[1]}
unit_property(){ timeout --signal=TERM --kill-after=2s 5s busctl get-property org.freedesktop.systemd1 "$unit_object" org.freedesktop.systemd1.Service "$1"; }
test "$(unit_property User)" = 's "root"'
test "$(unit_property MainPID)" = "u $$"
test "$(unit_property RuntimeMaxUSec)" = 't 420000000'
test "$(unit_property TimeoutStopUSec)" = 't 300000000'
test "$(unit_property KillMode)" = 's "mixed"'
release="/opt/ccpanel/releases/$CCPANEL_EXPECTED_RELEASE"
exec 8>/opt/ccpanel/shared/deploy.lock
flock -n 8
test "$(readlink -f /opt/ccpanel/current)" = "$release"

# Pin the actual deployed bundles, rather than substituting a local rebuild.
test "$(sha256sum "$release/dist/api.mjs" | cut -d' ' -f1)" = f49be1a9c3e9cfd202f47faa87ccd7ace190e4685cc04aae77c85133bb0bd6cf
test "$(sha256sum "$release/dist/worker.mjs" | cut -d' ' -f1)" = fafdbe510b80547d20da6f2f8821cd60066f5a78e9b2999db2aba0a4a07f8d29
test "$(sha256sum "$release/dist/web/sdk/ccpanel.js" | cut -d' ' -f1)" = 449f14f4719751a13ef1a666ff141511dd223ddc93b246968d87bf2688c4b661
operator=$(cd -P "$(dirname "${BASH_SOURCE[0]}")" && pwd)
operator_script="$operator/partial-produce-window-check.mjs"
operator_rescue="$operator/partial-produce-resume-remote.sh"
for file in "$(readlink -f "${BASH_SOURCE[0]}")" "$operator_script" "$operator_rescue" "$CCPANEL_PARTIAL_SDK_FILE"; do
  test -f "$file";test -s "$file";test ! -L "$file"
  test "$(readlink -f "$file")" = "$file"
  case "$file" in /opt/ccpanel/releases/*) echo 'Stage drill tools outside immutable releases' >&2;exit 64;; esac
  test "$(stat -c %u "$file")" = 0
  mode=$(stat -c %a "$file");(( (8#$mode & 022) == 0 ))
done
stop_post=$(timeout --signal=TERM --kill-after=2s 5s systemctl show "$CCPANEL_PARTIAL_CONTROLLER_UNIT" --property=ExecStopPost --value)
# Inspect the configured argv as data only. Require Bash to invoke precisely
# this validated canonical sibling, even if the main controller is killed.
[[ "$stop_post" == *"argv[]=/bin/bash $operator_rescue ;"* ]]
test "$(sha256sum "$CCPANEL_PARTIAL_SDK_FILE" | cut -d' ' -f1)" = "$CCPANEL_PARTIAL_SDK_SHA256"
capacity_units=$(timeout --signal=TERM --kill-after=2s 5s systemctl list-units --type=service --state=running 'ccpanel-capacity-*' --no-legend --plain)
test -z "$capacity_units"
curl --max-time 10 -fsS https://192.236.151.125/ready >/dev/null
test "$(awk '/MemAvailable:/ {print ($2 >= 204800)}' /proc/meminfo)" = 1
source /opt/ccpanel/shared/runtime.env
[[ "$DATABASE_URL" != *$'\n'* && "$DATABASE_URL" != *$'\r'* ]]
cd "$release/infra"
test "$(timeout --signal=TERM --kill-after=5s 30s docker compose exec -T postgres psql -X -U ccpanel -d ccpanel -Atqc 'SELECT max(version) FROM schema_migrations')" = 22

db="ccpanel_restore_$(date -u +%Y%m%d%H%M%S)_$(openssl rand -hex 4)"
[[ "$db" =~ ^ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$ ]]
suffix=$(openssl rand -hex 8);prefix="ccpanel.drill.partialproduce.$suffix"
container="ccpanel-partialproduce-$suffix";cleanup_container="$container-cleanup"
setup_container="$container-setup"
created=false;paused=false;captured=false
root=$(mktemp -d /root/ccpanel-partial-produce.XXXXXX)

fingerprint(){
  timeout --signal=TERM --kill-after=5s 15s docker compose exec -T postgres psql -X -U ccpanel -d ccpanel -At -v ON_ERROR_STOP=1 <../scripts/backup-fingerprint.sql || return 1
  timeout --signal=TERM --kill-after=5s 15s docker compose exec -T postgres psql -X -U ccpanel -d ccpanel -At -v ON_ERROR_STOP=1 -c "SELECT json_build_array('public.recovery_gate',count(*),coalesce(bit_xor(hashtextextended(row_to_json(t)::text,0)),0)) FROM recovery_gate t; SELECT json_build_array('public.recovery_import_scopes',count(*),coalesce(bit_xor(hashtextextended(row_to_json(t)::text,0)),0)) FROM recovery_import_scopes t;"
}
helper(){
  local name=$1 limit=$2;shift 2
  timeout --signal=TERM --kill-after=10s "$limit" docker compose run --rm -T --no-deps --name "$name" --env-from-file "$root/test.env" \
    -v "$operator_script:/app/scripts/fault-check.mjs:ro" -v "$root/evidence:/drill-evidence" -v "$root/sdk:/drill-sdk:ro" \
    api node --max-old-space-size=40 scripts/fault-check.mjs "$@"
}
wait_ready(){
  local attempt
  for attempt in $(seq 1 30);do
    if curl --max-time 1 -fsS https://192.236.151.125/ready >/dev/null 2>&1;then return 0;fi
    sleep 1
  done
  return 1
}
cleanup(){
  local status=$? failed=0 root_hash=unavailable check_hash=unavailable fingerprints_equal=false restarted=false startup=false
  trap - EXIT INT TERM
  set +e
  # Killing the timed-out Compose client alone does not guarantee child exit.
  timeout --signal=TERM --kill-after=5s 15s docker rm -f "$container" "$setup_container" >"$root/container-cleanup.log" 2>&1
  local remaining_containers
  if remaining_containers=$(timeout --signal=TERM --kill-after=2s 5s docker ps -q --filter "name=^/${container}(-setup)?$") && [ -z "$remaining_containers" ];then :
  else failed=1;echo "Isolated application container cleanup needs attention: $root" >&2
  fi
  # Compare while the original apps are still paused, including on failure.
  if [ "$captured" = true ];then
    if fingerprint >"$root/after.jsonl" 2>"$root/fingerprint-after.log" &&
      test "$(wc -l <"$root/after.jsonl")" = 44 && cmp -s "$root/before.jsonl" "$root/after.jsonl";then
      fingerprints_equal=true;root_hash=$(sha256sum "$root/before.jsonl" | cut -d' ' -f1)
    else
      failed=1;echo "Root public fingerprint comparison needs attention: $root" >&2
    fi
  else failed=1
  fi
  # Resume before slower isolated Kafka/DB removal. All subprocesses have their
  # own bounds, leaving room inside the independent 300s systemd stop budget.
  if [ "$paused" = true ];then
    if timeout --signal=TERM --kill-after=5s 20s docker compose start api worker >"$root/restart.log" 2>&1;then startup=true
    else failed=1;echo "Preview startup needs attention: $root" >&2
    fi
    if wait_ready;then
      paused=false
      if [ "$startup" = true ];then restarted=true;fi
    else
      failed=1;echo "Preview restart/readiness needs attention: $root" >&2
    fi
  elif wait_ready;then restarted=true
  else failed=1
  fi
  if [ -f "$root/test.env" ];then
    if ! helper "$cleanup_container" 60s cleanup >"$root/kafka-cleanup.log" 2>&1;then
      failed=1;echo "Isolated Kafka cleanup needs attention: $root" >&2
    fi
    timeout --signal=TERM --kill-after=5s 10s docker rm -f "$cleanup_container" >"$root/cleanup-container.log" 2>&1
  fi
  if [ "$created" = true ];then
    if ! timeout --signal=TERM --kill-after=5s 20s docker compose exec -T postgres dropdb -U ccpanel --if-exists --force "$db" >"$root/database-cleanup.log" 2>&1;then
      failed=1;echo "Temporary database cleanup needs attention: $root" >&2
    fi
  fi
  # Only exact owned files are unlinked; synthetic outbox evidence is retained.
  rm -f -- "$root/test.env" "$root/ledger.json"
  if [ -f "$root/check.log" ];then check_hash=$(sha256sum "$root/check.log" | cut -d' ' -f1);fi
  if [ "$status" != 0 ] || [ "$failed" != 0 ];then status=1;fi
  local passed=false
  if [ "$status" = 0 ];then passed=true;fi
  printf '{"passed":%s,"current_release":"m4-sdk-013","isolated_partial_produce":true,"root_public_relations":44,"root_fingerprints_unchanged":%s,"root_fingerprint_sha256":"%s","check_log_sha256":"%s","original_services_ready":%s}\n' \
    "$passed" "$fingerprints_equal" "$root_hash" "$check_hash" "$restarted" >"$root/result.json"
  cat "$root/result.json"
  echo "Private partial-produce evidence: $root" >&2
  exit "$status"
}
trap cleanup EXIT
trap 'exit 143' TERM
trap 'exit 130' INT

install -d -m 700 -o 1000 -g 1000 "$root/evidence" "$root/sdk"
install -m 400 -o 1000 -g 1000 "$CCPANEL_PARTIAL_SDK_FILE" "$root/sdk/index.mjs"
test "$(sha256sum "$root/sdk/index.mjs" | cut -d' ' -f1)" = "$CCPANEL_PARTIAL_SDK_SHA256"
printf 'DATABASE_URL=%s/%s\nRETENTION_ENABLED=false\nOPERATIONAL_COLLECTION_ENABLED=false\nKAFKA_EVENTS_TOPIC=%s.events\nKAFKA_DLQ_TOPIC=%s.dlq\nKAFKA_GROUP_ID=%s.writer\nCCPANEL_ISOLATED_PARTIAL_PRODUCE_CHECK=true\nCCPANEL_PARTIAL_SDK_SHA256=%s\n' \
  "${DATABASE_URL%/*}" "$db" "$prefix" "$prefix" "$prefix" "$CCPANEL_PARTIAL_SDK_SHA256" >"$root/test.env"
paused=true;timeout --signal=TERM --kill-after=10s 45s docker compose stop api worker >"$root/pause.log" 2>&1
fingerprint >"$root/before.jsonl" 2>"$root/fingerprint-before.log"
test "$(wc -l <"$root/before.jsonl")" = 44;captured=true

existing_db=$(timeout --signal=TERM --kill-after=5s 30s docker compose exec -T postgres psql -X -U ccpanel -d postgres -At -v ON_ERROR_STOP=1 -c "SELECT 1 FROM pg_database WHERE datname='$db'")
test -z "$existing_db"
# Reserve this checked-absent random name even if createdb's client times out
# after PostgreSQL has created it. Cleanup may drop only this owned name.
created=true
timeout --signal=TERM --kill-after=5s 30s docker compose exec -T postgres createdb -U ccpanel "$db" >"$root/database-create.log" 2>&1
run(){ timeout --signal=TERM --kill-after=10s 60s docker compose run --rm -T --no-deps --name "$setup_container" --env-from-file "$root/test.env" api node "dist/$1.mjs" "${@:2}"; }
run migrate >"$root/migrate.log" 2>&1
run deletion-ledger export >"$root/ledger.json" 2>"$root/ledger-export.log"
timeout --signal=TERM --kill-after=10s 60s docker compose run --rm -T --no-deps --name "$setup_container" --env-from-file "$root/test.env" -e CCPANEL_OFFLINE_RESTORE=true api node dist/deletion-ledger.mjs apply <"$root/ledger.json" >"$root/ledger-apply.log" 2>&1

# The harness sequences private API and Worker; no host port is published.
# Both normal and error exits flow through bounded helper cleanup and restart.
helper "$container" 300s >"$root/check.log" 2>&1
