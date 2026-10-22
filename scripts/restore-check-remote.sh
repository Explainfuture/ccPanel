#!/usr/bin/env bash
set -euo pipefail
umask 077
file=$(realpath -e "${CCPANEL_RESTORE_FILE:?Set the uploaded restore dump path}")
case "$file" in /opt/ccpanel/shared/restore/*/database.dump) ;; *) echo 'Restore input must be inside the dedicated restore staging directory' >&2; exit 1;; esac
expected="$(dirname "$file")/fingerprint.jsonl"
test -s "$expected"
cd /opt/ccpanel/current/infra
db="ccpanel_restore_$(date -u +%Y%m%d%H%M%S)_$(openssl rand -hex 4)"
[[ "$db" =~ ^ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$ ]]
cleanup(){ docker compose exec -T postgres dropdb -U ccpanel --if-exists --force "$db"; }
trap cleanup EXIT
started=$(date +%s)
docker compose exec -T postgres createdb -U ccpanel "$db"
docker compose exec -T postgres pg_restore -U ccpanel --dbname="$db" --single-transaction --exit-on-error --no-owner --no-acl <"$file"
docker compose exec -T postgres psql -X -U ccpanel -d "$db" -At -v ON_ERROR_STOP=1 <../scripts/backup-fingerprint.sql >"$(dirname "$file")/restored-fingerprint.jsonl"
cmp "$expected" "$(dirname "$file")/restored-fingerprint.jsonl"
printf '{"passed":true,"restore_seconds":%s,"target":"isolated_temporary_database","checks":"all public relation counts and row fingerprints"}\n' "$(( $(date +%s)-started ))"
