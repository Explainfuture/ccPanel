#!/usr/bin/env bash
# Activate only a reconciled trusted schema17 preview upgrade, never a restore.
set -euo pipefail
umask 077
exec 8>/opt/ccpanel/shared/deploy.lock
flock -n 8 || { echo 'Another deployment is active' >&2; exit 1; }
: "${CCPANEL_EXPECTED_RELEASE:?Set reconciled release name}"
state=${CCPANEL_MAINTENANCE_STATE:-true}
[[ "$state" = true || "$state" = false ]]
[[ "$CCPANEL_EXPECTED_RELEASE" =~ ^[A-Za-z0-9_-]+$ ]]
test "$(readlink -f /opt/ccpanel/current)" = "/opt/ccpanel/releases/$CCPANEL_EXPECTED_RELEASE"
running_capacity=$(systemctl list-units --type=service --state=running 'ccpanel-capacity-*' --no-legend --plain)
test -z "$running_capacity" || { echo 'Capacity sampler active; defer maintenance activation' >&2; exit 1; }
upgrade=$(realpath -e "${CCPANEL_UPGRADE_DIR:?Set verified trusted upgrade evidence}")
[[ "$upgrade" =~ ^/opt/ccpanel/shared/product-upgrade\.[A-Za-z0-9]{6}$ ]]
test "$(cat "$upgrade/target-release.txt")" = "$CCPANEL_EXPECTED_RELEASE"
schema=$(cat "$upgrade/target-schema.txt")
[[ "$schema" = 17 || "$schema" = 18 || "$schema" = 19 || "$schema" = 20 ]]
test -s "$upgrade/backup-verified.json"
cmp "$upgrade/before.jsonl" "$upgrade/after.jsonl"
cd /opt/ccpanel/current/infra
test "$(docker compose exec -T postgres psql -X -U ccpanel -d ccpanel -Atqc 'SELECT max(version) FROM schema_migrations')" = "$schema"
docker compose run --rm -T --no-deps api node dist/deletion-ledger.mjs check
docker compose run --rm -T --no-deps api node dist/consumer-progress.mjs check
cmp /opt/ccpanel/shared/runtime.env .env
python3 - "$state" <<'PY'
import os,pathlib,sys,tempfile
state=sys.argv[1];assert state in ('true','false')
paths=[pathlib.Path('/opt/ccpanel/shared/runtime.env'),pathlib.Path('/opt/ccpanel/current/infra/.env')]
source=paths[0].read_text();lines=source.splitlines()
values=[line.split('=',1)[1].strip().strip('\"\'') for line in lines if line.startswith('RETENTION_ENABLED=')]
assert not values or values in (['false'],['true']),'Ambiguous retention configuration'
text='\n'.join([line for line in lines if not line.startswith('RETENTION_ENABLED=')]+['RETENTION_ENABLED='+state])+'\n'
for path in paths:
    fd,name=tempfile.mkstemp(prefix='.retention-env-',dir=path.parent)
    try:
        with os.fdopen(fd,'w') as file:
            os.fchmod(file.fileno(),0o600);file.write(text);file.flush();os.fsync(file.fileno())
        os.replace(name,path)
        directory=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY)
        try:os.fsync(directory)
        finally:os.close(directory)
    finally:
        if os.path.exists(name):os.unlink(name)
print('Maintenance state configured without exposing other values')
PY
docker compose up -d --no-deps worker
curl -fsS https://192.236.151.125/ready
echo "Maintenance state applied: $state; inspect the new Worker and maintenance evidence"
