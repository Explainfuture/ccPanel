#!/usr/bin/env bash
# Trusted explicit live preview upgrades. Never restore an older database.
set -euo pipefail
umask 077
mode=${1:?prepare or apply}
source_schema=${CCPANEL_SOURCE_SCHEMA:-10}
target_schema=${CCPANEL_TARGET_SCHEMA:-15}
case "$source_schema:$target_schema" in 10:15|15:16|16:17|17:17|17:18|18:18|18:19|19:19|19:20|20:20) ;; *) echo 'Unsupported trusted upgrade pair' >&2;exit 1;; esac
# A trusted live upgrade must retain its already-open gate and durable cursor.
unset CCPANEL_PROGRESS_BOOTSTRAP_REASON CCPANEL_RECOVERY_LEDGER
: "${CCPANEL_RELEASE:?Set a new release name}"
: "${CCPANEL_ARCHIVE_SHA256:?Set verified archive SHA256}"
[[ "$CCPANEL_RELEASE" =~ ^[a-zA-Z0-9_-]+$ ]]
[[ "$CCPANEL_ARCHIVE_SHA256" =~ ^[a-f0-9]{64}$ ]]
test "$(sha256sum /opt/ccpanel/shared/release.tar.gz | cut -d' ' -f1)" = "$CCPANEL_ARCHIVE_SHA256"
archive_schema=$(tar -xOf /opt/ccpanel/shared/release.tar.gz dist/schema-version.txt)
test "$archive_schema" = "$target_schema" || { echo 'Archive schema does not match explicit upgrade target; no services stopped' >&2; exit 1; }
if [ "$target_schema" = 19 ] || [ "$target_schema" = 20 ]; then
  helper_sha=$(tar -xOf /opt/ccpanel/shared/release.tar.gz scripts/operational-collection-env.py | sha256sum | cut -d' ' -f1)
  test "$(sha256sum /opt/ccpanel/shared/operational-collection-env.py | cut -d' ' -f1)" = "$helper_sha" || { echo 'Stage the archive-matching collection helper before stopping services' >&2; exit 1; }
fi
running_capacity=$(systemctl list-units --type=service --state=running 'ccpanel-capacity-*' --no-legend --plain)
test -z "$running_capacity" || { echo 'Capacity sampler still running; defer deployment' >&2; exit 1; }
# Physical expiry legitimately changes rows: keep it disabled until the
# post-migration original-column reconciliation has completed.
if [ "$target_schema" = 17 ] || [ "$target_schema" = 18 ] || [ "$target_schema" = 19 ] || [ "$target_schema" = 20 ]; then
  python3 - <<'PY'
from pathlib import Path
lines=Path('/opt/ccpanel/shared/runtime.env').read_text().splitlines()
values=[line.split('=',1)[1].strip().strip('\"\'') for line in lines if line.startswith('RETENTION_ENABLED=')]
assert not values or values==['false'],'Disable physical retention before trusted upgrade'
PY
fi
cd /opt/ccpanel/current/infra
test "$(docker compose exec -T postgres psql -X -U ccpanel -d ccpanel -Atqc 'SELECT max(version) FROM schema_migrations')" = "$source_schema"
if [ "$mode" = prepare ]; then
  test ! -e "/opt/ccpanel/releases/$CCPANEL_RELEASE"
  docker compose run --rm -T --no-deps api node dist/ingest-preflight.mjs
  docker compose run --rm -T --no-deps api node dist/consumer-progress.mjs check
  docker compose run --rm -T --no-deps api node dist/deletion-ledger.mjs check
  dir=$(mktemp -d /opt/ccpanel/shared/product-upgrade.XXXXXX)
  printf '%s\n' "$dir" >/opt/ccpanel/shared/product-upgrade.path
  readlink -f /opt/ccpanel/current >"$dir/previous-release.txt"
  printf '%s\n' "$CCPANEL_RELEASE" >"$dir/target-release.txt"
  printf '%s\n' "$source_schema" >"$dir/source-schema.txt"
  printf '%s\n' "$target_schema" >"$dir/target-schema.txt"
  printf '%s\n' "$CCPANEL_ARCHIVE_SHA256" >"$dir/archive.sha256"
  docker compose stop api worker
  if [ "$target_schema" = 19 ] || [ "$target_schema" = 20 ]; then
    python3 /opt/ccpanel/shared/operational-collection-env.py set false /opt/ccpanel/shared/runtime.env /opt/ccpanel/current/infra/.env
  fi
  # Freeze the original column projection. New metadata columns may be added,
  # but every existing value and relation count must remain unchanged.
  docker compose exec -T postgres psql -X -U ccpanel -d ccpanel -At -v ON_ERROR_STOP=1 >"$dir/fingerprint.sql" <<'SQL'
SELECT format('SELECT json_build_array(%L,count(*),coalesce(bit_xor(hashtextextended(row_to_json(t)::text,0)),0)) FROM (SELECT %s FROM %I.%I) t;',c.table_schema||'.'||c.table_name,string_agg(quote_ident(c.column_name),',' ORDER BY c.ordinal_position),c.table_schema,c.table_name)
FROM information_schema.columns c JOIN pg_tables p ON p.schemaname=c.table_schema AND p.tablename=c.table_name
WHERE c.table_schema='public' AND c.table_name NOT IN ('schema_migrations','recovery_gate','recovery_import_scopes') GROUP BY c.table_schema,c.table_name ORDER BY c.table_name;
SQL
  docker compose exec -T postgres psql -X -U ccpanel -d ccpanel -At -v ON_ERROR_STOP=1 <"$dir/fingerprint.sql" >"$dir/before.jsonl"
  bash /opt/ccpanel/shared/backup-remote.sh >"$dir/backup.path"
  if [ "$source_schema:$target_schema" = 20:20 ]; then
    # Use the SHA-verified target CLI to include the new v2 retention inventory
    # even when the old schema20 runtime can only export legacy v1 ledgers.
    tar -xOf /opt/ccpanel/shared/release.tar.gz dist/deletion-ledger.mjs >"$dir/source-ledger-exporter.mjs"
    chmod 644 "$dir/source-ledger-exporter.mjs"
    docker compose run --rm -T --no-deps -v "$dir/source-ledger-exporter.mjs:/app/dist/deletion-ledger.mjs:ro" api node dist/deletion-ledger.mjs export >"$dir/source-ledger.json"
    python3 - "$dir/source-ledger.json" <<'PY'
import json,pathlib,sys
ledger=json.loads(pathlib.Path(sys.argv[1]).read_text())
assert ledger.get('schema_version')==2 and isinstance(ledger.get('retention'),list),'Schema20 upgrade requires a complete v2 source ledger'
PY
  else
    docker compose run --rm -T --no-deps api node dist/deletion-ledger.mjs export >"$dir/source-ledger.json"
  fi
  echo "Upgrade prepared; services offline; verify an off-host encrypted backup and isolated restore before apply: $dir"
elif [ "$mode" = apply ]; then
  if [ "$target_schema" = 19 ] || [ "$target_schema" = 20 ]; then
    python3 /opt/ccpanel/shared/operational-collection-env.py check false /opt/ccpanel/shared/runtime.env /opt/ccpanel/current/infra/.env
  fi
  dir=$(realpath -e "${CCPANEL_UPGRADE_DIR:?Set prepared upgrade directory}")
  [[ "$dir" =~ ^/opt/ccpanel/shared/product-upgrade\.[A-Za-z0-9]{6}$ ]]
  test "$(cat "$dir/target-release.txt")" = "$CCPANEL_RELEASE"
  test "$(cat "$dir/source-schema.txt")" = "$source_schema"
  test "$(cat "$dir/target-schema.txt")" = "$target_schema"
  test "$(cat "$dir/archive.sha256")" = "$CCPANEL_ARCHIVE_SHA256"
  test "$(readlink -f /opt/ccpanel/current)" = "$(cat "$dir/previous-release.txt")"
  test -s "$dir/backup-verified.json"
  python3 - "$dir" <<'PY'
import hashlib,json,pathlib,re,sys
root=pathlib.Path(sys.argv[1]);backup=pathlib.Path((root/'backup.path').read_text().strip())
assert re.fullmatch(r'/opt/ccpanel/shared/backups/[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}',str(backup))
proof=json.loads((root/'backup-verified.json').read_text())
assert proof['offhost_verified'] is True and proof['isolated_restore_passed'] is True
for key,name in [('plaintext_sha256','database.dump'),('ciphertext_sha256','database.ccpbak'),('fingerprint_sha256','fingerprint.jsonl')]:
    assert hashlib.sha256((backup/name).read_bytes()).hexdigest()==proof[key]
PY
  for name in api worker; do test "$(docker inspect --format '{{.State.Running}}' "ccpanel-$name-1")" = false; done
  docker compose exec -T postgres psql -X -U ccpanel -d ccpanel -At -v ON_ERROR_STOP=1 <"$dir/fingerprint.sql" >"$dir/before-apply.jsonl"
  cmp "$dir/before.jsonl" "$dir/before-apply.jsonl"
  # Existing live recovery/integrity/progress state is retained. No bootstrap,
  # restored-source export or automatic legacy rollback is permitted here.
  bash /opt/ccpanel/shared/deploy-remote.sh
  cd /opt/ccpanel/current/infra
  docker compose exec -T postgres psql -X -U ccpanel -d ccpanel -At -v ON_ERROR_STOP=1 <"$dir/fingerprint.sql" >"$dir/after.jsonl"
  if ! cmp "$dir/before.jsonl" "$dir/after.jsonl"; then docker compose stop api worker; echo 'Original data changed; services stopped for inspection' >&2; exit 1; fi
  test "$(docker compose exec -T postgres psql -X -U ccpanel -d ccpanel -Atqc 'SELECT max(version) FROM schema_migrations')" = "$target_schema"
  if [ "$source_schema:$target_schema" = 19:20 ]; then
    if [ "$(docker compose exec -T postgres psql -X -U ccpanel -d ccpanel -Atqc 'SELECT COALESCE(bool_and(retention_days=30 AND retention_version=1 AND retention_floor IS NULL),true) FROM projects')" != t ]; then
      docker compose stop api worker;echo 'Unexpected initial retention policies; services stopped for inspection' >&2;exit 1
    fi
  fi
  docker compose run --rm -T --no-deps api node dist/consumer-progress.mjs check
  curl -fsS https://192.236.151.125/ready
  echo 'Product preview upgraded; original columns and counts preserved; capacity remains unapproved'
else echo 'Expected prepare or apply' >&2; exit 1; fi
