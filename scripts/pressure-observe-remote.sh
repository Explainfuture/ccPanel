#!/usr/bin/env bash
# Read-only one-shot observation, not a running admission controller.
set -euo pipefail
umask 077
test -s /opt/ccpanel/shared/telemetry/host.json
cd /opt/ccpanel/current/infra
docker compose run --rm -T --no-deps api node "${CCPANEL_PRESSURE_SCRIPT:-dist/pressure-observe.mjs}" </opt/ccpanel/shared/telemetry/host.json
