#!/usr/bin/env bash
set -euo pipefail
umask 077
cd /opt/ccpanel/current/infra
install -d -m 700 /opt/ccpanel/shared/backups
exec 9>/opt/ccpanel/shared/backups/.lock
flock -n 9 || { echo 'Backup already running' >&2; exit 1; }
id="$(date -u +%Y%m%dT%H%M%SZ)-$(openssl rand -hex 4)"
dir="/opt/ccpanel/shared/backups/$id"
mkdir -m 700 "$dir"
complete=false
trap 'if [ "$complete" != true ]; then echo "Incomplete backup retained for inspection: $dir" >&2; fi' EXIT
docker compose exec -T postgres psql -X -U ccpanel -d ccpanel -At -v ON_ERROR_STOP=1 <../scripts/backup-fingerprint.sql >"$dir/before.jsonl"
docker compose exec -T postgres pg_dump -U ccpanel -d ccpanel --format=custom --no-owner --no-acl >"$dir/database.dump"
docker compose exec -T postgres psql -X -U ccpanel -d ccpanel -At -v ON_ERROR_STOP=1 <../scripts/backup-fingerprint.sql >"$dir/fingerprint.jsonl"
# The dump is always internally consistent. This first recovery drill additionally
# requires stable source rows so exact before/after fingerprints can be compared.
cmp "$dir/before.jsonl" "$dir/fingerprint.jsonl" || { echo 'Source changed during dump; retry the controlled recovery drill during a quiet window.' >&2; exit 1; }
sha256sum "$dir/database.dump" >"$dir/database.sha256"
date -u +%FT%TZ >"$dir/completed-at.txt"
complete=true
echo "$dir"
