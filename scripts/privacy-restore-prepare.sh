#!/usr/bin/env bash
set -euo pipefail
umask 077
name=${CCPANEL_DRILL_NAME:?Set a new privacy-restore-NAME}
[[ "$name" =~ ^privacy-restore-[a-zA-Z0-9_-]+$ ]]
dir="/opt/ccpanel/shared/$name"
test -s "$dir/fixture.json"
test ! -e "$dir/database.dump"
cd /opt/ccpanel/current/infra
runtime=${CCPANEL_DRILL_RUNTIME:-auto}
if [ "$runtime" = auto ]; then if [ -f ../dist/deletion-ledger.mjs ]; then runtime=installed; else runtime=staged; fi; fi
case "$runtime" in installed) ledger=dist/deletion-ledger.mjs;; staged) ledger=scripts/staged-deletion-ledger.mjs;; *) echo 'Invalid drill runtime' >&2; exit 1;; esac
docker compose exec -T postgres psql -X -U ccpanel -d ccpanel -At -v ON_ERROR_STOP=1 <../scripts/backup-fingerprint.sql >"$dir/fingerprint.jsonl"
docker compose exec -T postgres pg_dump -U ccpanel -d ccpanel --format=custom --no-owner --no-acl --exclude-table-data=public.recovery_gate --exclude-table-data=public.recovery_import_scopes >"$dir/database.dump.part"
docker compose exec -T postgres psql -X -U ccpanel -d ccpanel -At -v ON_ERROR_STOP=1 <../scripts/backup-fingerprint.sql >"$dir/fingerprint-after.jsonl"
cmp "$dir/fingerprint.jsonl" "$dir/fingerprint-after.jsonl"
mv "$dir/database.dump.part" "$dir/database.dump"
docker compose run --rm -T --no-deps api node scripts/privacy-restore-fixture.mjs delete <"$dir/fixture.json"
docker compose run --rm -T --no-deps api node "$ledger" export >"$dir/deletion-ledger.json.part"
mv "$dir/deletion-ledger.json.part" "$dir/deletion-ledger.json"
date -u +%FT%TZ >"$dir/prepared-at.txt"
echo 'Old snapshot and fresh post-deletion ledger prepared; transfer and encrypt off-host before restoring.'
