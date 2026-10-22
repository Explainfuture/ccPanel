#!/usr/bin/env bash
# One-time trusted LIVE schema8 -> schema10 preview upgrade, never restore.
# Deliberately no automatic rollback to a worker that cannot maintain progress.
set -euo pipefail
umask 077
: "${CCPANEL_RELEASE:?Set a new release name}"
: "${CCPANEL_ARCHIVE_SHA256:?Set the locally verified release archive SHA256}"
[[ "$CCPANEL_ARCHIVE_SHA256" =~ ^[a-f0-9]{64}$ ]]
[[ "$CCPANEL_RELEASE" =~ ^[a-zA-Z0-9_-]+$ ]]
test "$(sha256sum /opt/ccpanel/shared/release.tar.gz | cut -d' ' -f1)" = "$CCPANEL_ARCHIVE_SHA256"
cd /opt/ccpanel/current/infra
test "$(docker compose exec -T postgres psql -X -U ccpanel -d ccpanel -Atqc 'SELECT max(version) FROM schema_migrations')" = 8
test "$(docker compose exec -T postgres psql -X -U ccpanel -d ccpanel -Atqc "SELECT to_regclass('public.consumer_progress') IS NULL")" = t
# Preflight the exact new reader before stopping the old runtime.
docker compose run --rm -T --no-deps -v /opt/ccpanel/shared/telemetry:/telemetry:ro api node scripts/staged-ingest-preflight-v10.mjs
dir=$(mktemp -d /opt/ccpanel/shared/integrity-upgrade.XXXXXX)
printf '%s\n' "$dir" >/opt/ccpanel/shared/integrity-upgrade.path
readlink -f /opt/ccpanel/current >"$dir/previous-release.txt"
docker compose stop api worker
# From here failures intentionally leave services offline for inspection.
# Snapshot all original application relations before the two new migrations.
docker compose exec -T postgres psql -X -U ccpanel -d ccpanel -At -v ON_ERROR_STOP=1 >"$dir/fingerprint.sql" <<'SQL'
SELECT format('SELECT json_build_array(%L,count(*),coalesce(bit_xor(hashtextextended(row_to_json(t)::text,0)),0)) FROM %I.%I t;',schemaname||'.'||tablename,schemaname,tablename)
FROM pg_tables WHERE schemaname='public' AND tablename NOT IN ('schema_migrations','recovery_gate','recovery_import_scopes') ORDER BY tablename;
SQL
docker compose exec -T postgres psql -X -U ccpanel -d ccpanel -At -v ON_ERROR_STOP=1 <"$dir/fingerprint.sql" >"$dir/before.jsonl"
bash /opt/ccpanel/shared/backup-remote.sh >"$dir/backup.path"
docker compose run --rm -T --no-deps api node dist/deletion-ledger.mjs export >"$dir/source-ledger.json"
# This reason is only valid for the explicitly checked live old runtime above.
CCPANEL_RECOVERY_LEDGER="$dir/source-ledger.json" CCPANEL_PROGRESS_BOOTSTRAP_REASON="Trusted live schema8 preview upgrade; services stopped; original source backup and fingerprints in $(basename "$dir"). Not a restored snapshot." bash /opt/ccpanel/shared/deploy-remote.sh
cd /opt/ccpanel/current/infra
docker compose exec -T postgres psql -X -U ccpanel -d ccpanel -At -v ON_ERROR_STOP=1 <"$dir/fingerprint.sql" >"$dir/after.jsonl"
cmp "$dir/before.jsonl" "$dir/after.jsonl"
docker compose run --rm -T --no-deps api node dist/consumer-progress.mjs check
docker compose exec -T postgres psql -X -U ccpanel -d ccpanel -At -v ON_ERROR_STOP=1 <<'SQL'
SELECT json_build_object('migration',max(version),'raw_events',(SELECT count(*) FROM events),'stored_dedup',(SELECT count(*) FROM event_dedup WHERE status='stored'),'recovery_open',(SELECT NOT blocked FROM recovery_gate WHERE id=true),'integrity_open',(SELECT NOT blocked FROM ingest_integrity_gate WHERE id=true),'progress_partitions',(SELECT count(*) FROM consumer_progress),'bootstrap_audits',(SELECT count(*) FROM consumer_progress_audit WHERE action='operator_bootstrap')) FROM schema_migrations;
SQL
printf 'INTEGRITY UPGRADE COMPLETE original_fingerprints_preserved=true evidence=%s\n' "$dir"
