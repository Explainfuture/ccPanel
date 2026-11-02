#!/usr/bin/env bash
# Independent ExecStopPost rescue for the supervised A14 controller.
# This only starts the already deployed original apps; it never changes release.
set +x
set -euo pipefail
umask 077
test "$(id -u)" = 0
test "${CCPANEL_EXPECTED_RELEASE:-}" = m4-sdk-013
exec 8>/opt/ccpanel/shared/deploy.lock
flock -w 10 8
test "$(readlink -f /opt/ccpanel/current)" = /opt/ccpanel/releases/m4-sdk-013
source /opt/ccpanel/shared/runtime.env
cd /opt/ccpanel/releases/m4-sdk-013/infra
timeout --signal=TERM --kill-after=10s 30s docker compose start api worker >/dev/null
for attempt in $(seq 1 40);do
  if curl --max-time 2 -fsS https://192.236.151.125/ready >/dev/null 2>&1;then
    printf '{"partial_produce_rescue":"original_services_ready"}\n'
    exit 0
  fi
  sleep 1
done
echo 'Partial-produce rescue readiness failed' >&2
exit 1
