#!/usr/bin/env bash
# Controlled preview-only outage drill. No real customer traffic should use this environment.
set -euo pipefail
umask 077
cd /opt/ccpanel/current/infra
state=/opt/ccpanel/shared/fault-check
install -d -m 700 -o 1000 -g 1000 "$state"
# The API deliberately exits when its database lease is lost. Keep the test
# controller and private state outside that container so observation survives.
trap 'docker compose start postgres kafka api worker >/dev/null 2>&1 || true' EXIT
trap 'echo "Fault drill failed at line $LINENO (status $?)" >&2' ERR
run() { docker compose run --rm -T --no-deps -v "$state:/fault-state" -e CCPANEL_FAULT_STATE_FILE=/fault-state/state.json -e TEST_API=http://api:3000 api node scripts/fault-check.mjs "$@"; }
ready() {
  for attempt in $(seq 1 90); do
    if curl -fsS http://127.0.0.1:3000/ready >/dev/null 2>&1; then return; fi
    sleep 1
  done
  echo 'API did not recover readiness' >&2; return 1
}
run prepare
docker compose stop worker
run send 1
run count 0
docker compose up -d --no-deps worker
run wait 1
docker compose stop worker
run send 2
run checkpoint
outage_started=$(date -u +%Y-%m-%dT%H:%M:%SZ)
docker compose stop postgres
docker compose up -d --no-deps worker
sleep 8
test "$(docker inspect -f '{{.State.Running}}' "$(docker compose ps -aq postgres)")" = false
docker compose logs --since "$outage_started" api >"$state/api-outage.log" 2>&1
docker compose logs --since "$outage_started" worker >"$state/worker-outage.log" 2>&1
grep -Eq 'database_write_retry|recovery_lease_lost|ECONNREFUSED|ENOTFOUND' "$state/worker-outage.log"
grep -q recovery_lease_lost "$state/api-outage.log"
run noadvance
docker compose up -d --wait postgres
run wait 2
ready
docker compose stop kafka
run unavailable 3
docker compose start kafka
ready
run send 3
run wait 3
run poison
run cleanup
test ! -e "$state/state.json"
trap - EXIT
docker compose ps
docker stats --no-stream --format 'table {{.Name}}\t{{.MemUsage}}\t{{.CPUPerc}}'
printf '\nFAULT DRILL COMPLETE\n'
