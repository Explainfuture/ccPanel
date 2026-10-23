#!/usr/bin/env bash
# Enable only after a trusted supported upgrade reconciles original data.
set -euo pipefail
umask 077
exec 8>/opt/ccpanel/shared/deploy.lock
flock -n 8 || { echo 'Another deployment is active' >&2; exit 1; }
: "${CCPANEL_EXPECTED_RELEASE:?Set reconciled release name}"
: "${CCPANEL_UPGRADE_DIR:?Set verified trusted upgrade evidence}"
state=${CCPANEL_COLLECTION_STATE:?Set explicit true or false}
[[ "$state" = true || "$state" = false ]]
[[ "$CCPANEL_EXPECTED_RELEASE" =~ ^[A-Za-z0-9_-]+$ ]]
test "$(readlink -f /opt/ccpanel/current)" = "/opt/ccpanel/releases/$CCPANEL_EXPECTED_RELEASE"
running_capacity=$(systemctl list-units --type=service --state=running 'ccpanel-capacity-*' --no-legend --plain)
test -z "$running_capacity" || { echo 'Capacity sampler active; defer collection configuration' >&2; exit 1; }
upgrade=$(realpath -e "$CCPANEL_UPGRADE_DIR")
[[ "$upgrade" =~ ^/opt/ccpanel/shared/product-upgrade\.[A-Za-z0-9]{6}$ ]]
test "$(cat "$upgrade/target-release.txt")" = "$CCPANEL_EXPECTED_RELEASE"
schema=$(cat "$upgrade/target-schema.txt")
[[ "$schema" = 19 || "$schema" = 20 || "$schema" = 21 ]]
test -s "$upgrade/backup-verified.json"
cmp "$upgrade/before.jsonl" "$upgrade/after.jsonl"
cd /opt/ccpanel/current/infra
test "$(cat ../dist/schema-version.txt)" = "$schema"
test "$(docker compose exec -T postgres psql -X -U ccpanel -d ccpanel -Atqc 'SELECT max(version) FROM schema_migrations')" = "$schema"
docker compose run --rm -T --no-deps api node dist/deletion-ledger.mjs check
docker compose run --rm -T --no-deps api node dist/consumer-progress.mjs check
before=$(docker compose exec -T api node -e 'process.stdout.write(process.env.OPERATIONAL_COLLECTION_ENABLED||"unset")')
[[ "$before" = true || "$before" = false ]]
test "$(docker compose exec -T worker node -e 'process.stdout.write(process.env.OPERATIONAL_COLLECTION_ENABLED||"unset")')" = "$before"
python3 ../scripts/operational-collection-env.py check "$before" /opt/ccpanel/shared/runtime.env "$PWD/.env"
python3 ../scripts/operational-collection-env.py set "$state" /opt/ccpanel/shared/runtime.env "$PWD/.env"
if [ "$state" != "$before" ]; then docker compose up -d --no-deps api worker; fi
for service in api worker; do
  test "$(docker compose exec -T "$service" node -e 'process.stdout.write(process.env.OPERATIONAL_COLLECTION_ENABLED||"unset")')" = "$state"
done
healthy=false
for attempt in $(seq 1 30); do
  if curl -fsS http://127.0.0.1:3000/ready >/dev/null 2>&1; then healthy=true;break;fi
  sleep 1
done
test "$healthy" = true
printf 'Operational collection state applied: %s; observe subsequent samples and evaluations\n' "$state"
