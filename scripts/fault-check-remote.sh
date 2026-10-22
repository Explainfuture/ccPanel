#!/usr/bin/env bash
# Controlled preview-only outage drill. No real customer traffic should use this environment.
set -euo pipefail
cd /opt/ccpanel/current/infra
trap 'docker compose start postgres kafka worker >/dev/null 2>&1 || true' EXIT
run() { docker compose exec -T api node scripts/fault-check.mjs "$@"; }
run prepare
docker compose stop worker
run send 1
run count 0
docker compose up -d --no-deps worker
run wait 1
docker compose stop worker
run send 2
run checkpoint
docker compose stop postgres
docker compose up -d --no-deps worker
sleep 8
test "$(docker inspect -f '{{.State.Running}}' "$(docker compose ps -aq postgres)")" = false
docker compose logs --since 15s worker | grep -Eq 'database_write_retry|recovery_lease_lost'
run noadvance
docker compose up -d --wait postgres
run wait 2
docker compose stop kafka
run unavailable 3
docker compose start kafka
for attempt in $(seq 1 40); do
  if curl -fsS http://127.0.0.1:3000/ready >/dev/null 2>&1; then break; fi
  sleep 1
done
run send 3
run wait 3
run poison
run cleanup
docker compose ps
docker stats --no-stream --format 'table {{.Name}}\t{{.MemUsage}}\t{{.CPUPerc}}'
printf '\nFAULT DRILL COMPLETE\n'
