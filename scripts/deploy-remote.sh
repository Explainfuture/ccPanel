#!/usr/bin/env bash
# Run on the VPS after uploading a locally built archive. No credentials in source.
set -euo pipefail
: "${CCPANEL_RELEASE:?set release name, e.g. m1-003}"
case "$CCPANEL_RELEASE" in *[!a-zA-Z0-9_-]*|'') echo 'Invalid release name' >&2; exit 1;; esac
release="/opt/ccpanel/releases/$CCPANEL_RELEASE"
previous=$(readlink -f /opt/ccpanel/current)
test ! -e "$release"
test -f /opt/ccpanel/shared/release.tar.gz
mkdir -p "$release"
tar -xzf /opt/ccpanel/shared/release.tar.gz -C "$release"
# Dependencies are read-only to the containers. Install anew if the runtime lock changes.
cmp "$previous/pnpm-lock.yaml" "$release/pnpm-lock.yaml"
cp -al "$previous/node_modules" "$release/node_modules"
cp /opt/ccpanel/shared/runtime.env "$release/infra/.env"
chmod 600 "$release/infra/.env"
find "$release/dist" -type d -exec chmod 755 {} +
find "$release/dist" -type f -exec chmod 644 {} +
chgrp www-data /opt/ccpanel /opt/ccpanel/releases
chmod 750 /opt/ccpanel /opt/ccpanel/releases
cd "$release/infra"
docker compose run --rm -T --no-deps api node dist/migrate.mjs
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
