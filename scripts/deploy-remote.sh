#!/usr/bin/env bash
# Run on the VPS after uploading a locally built archive. No credentials in source.
set -euo pipefail
exec 8>/opt/ccpanel/shared/deploy.lock
flock -n 8 || { echo 'Another preview deployment is active' >&2; exit 1; }
: "${CCPANEL_RELEASE:?set release name, e.g. m1-003}"
case "$CCPANEL_RELEASE" in *[!a-zA-Z0-9_-]*|'') echo 'Invalid release name' >&2; exit 1;; esac
release="/opt/ccpanel/releases/$CCPANEL_RELEASE"
previous=$(readlink -f /opt/ccpanel/current)
test ! -e "$release"
test -f /opt/ccpanel/shared/release.tar.gz
mkdir -p "$release"
# Parent operator shells may use umask 077 for private ledger files. The
# unprivileged runtime must still traverse its code mount; secrets remain 600.
chmod 755 "$release"
tar -xzf /opt/ccpanel/shared/release.tar.gz -C "$release"
# Dependencies are read-only to the containers. Install anew if the runtime lock changes.
cp -al "$previous/node_modules" "$release/node_modules"
if ! cmp -s "$previous/pnpm-lock.yaml" "$release/pnpm-lock.yaml"; then
  docker run --rm -v "$release:/app" -w /app node:22.22.3-bookworm-slim sh -c 'npm install -g pnpm@12.3.4 --ignore-scripts --no-audit --no-fund && pnpm install --prod --frozen-lockfile'
fi
cp /opt/ccpanel/shared/runtime.env "$release/infra/.env"
chmod 600 "$release/infra/.env"
find "$release/dist" -type d -exec chmod 755 {} +
find "$release/dist" -type f -exec chmod 644 {} +
chgrp www-data /opt/ccpanel /opt/ccpanel/releases
chmod 750 /opt/ccpanel /opt/ccpanel/releases
python3 "$release/scripts/retain-web-assets.py" "$previous" "$release"
cd "$release/infra"
docker compose run --rm -T --no-deps api node -e 'const fs=require("node:fs");for(const file of ["dist/api.mjs","dist/worker.mjs","dist/migrate.mjs","dist/deletion-ledger.mjs"])fs.accessSync(file,fs.constants.R_OK);require("pg");'
docker compose run --rm -T --no-deps api node dist/ingest-preflight.mjs
docker compose stop api worker
docker compose run --rm -T --no-deps api node dist/migrate.mjs
if ! docker compose run --rm -T --no-deps api node dist/deletion-ledger.mjs check >/dev/null 2>&1; then
  : "${CCPANEL_RECOVERY_LEDGER:?Recovery gate closed; provide a fresh trusted source ledger, never an export from an old restored DB.}"
  test -s "$CCPANEL_RECOVERY_LEDGER"
  docker compose run --rm -T --no-deps -e CCPANEL_OFFLINE_RESTORE=true api node dist/deletion-ledger.mjs apply <"$CCPANEL_RECOVERY_LEDGER"
fi
docker compose run --rm -T --no-deps api node dist/consumer-progress.mjs prepare
if [ -n "${CCPANEL_PROGRESS_BOOTSTRAP_REASON:-}" ]; then
  CCPANEL_OFFLINE_RESTORE=true CCPANEL_PROGRESS_REASON="$CCPANEL_PROGRESS_BOOTSTRAP_REASON" docker compose run --rm -T --no-deps -e CCPANEL_OFFLINE_RESTORE -e CCPANEL_PROGRESS_REASON api node dist/consumer-progress.mjs bootstrap
fi
docker compose run --rm -T --no-deps api node dist/consumer-progress.mjs check
docker compose up -d api worker
healthy=false
for attempt in $(seq 1 30); do
  if curl -fsS http://127.0.0.1:3000/ready >/dev/null; then healthy=true; break; fi
  sleep 1
done
if [ "$healthy" != true ]; then
  echo 'New runtime not ready; current static release was not switched. Inspect containers and roll back compatible runtime.' >&2
  exit 1
fi
ln -sfn "$release" /opt/ccpanel/current
nginx -t
systemctl reload nginx
curl --cacert /opt/ccpanel/shared/tls.crt -fsS https://192.236.151.125/ready
printf '\nRelease %s ready\n' "$CCPANEL_RELEASE"
