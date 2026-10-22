#!/usr/bin/env bash
# Trusted live schema10 -> schema15 preview upgrade. Never run on a restore.
set -euo pipefail
umask 077
mode=${1:?prepare or apply}
: "${CCPANEL_RELEASE:?Set a new release name}"
: "${CCPANEL_ARCHIVE_SHA256:?Set verified archive SHA256}"
[[ "$CCPANEL_RELEASE" =~ ^[a-zA-Z0-9_-]+$ ]]
[[ "$CCPANEL_ARCHIVE_SHA256" =~ ^[a-f0-9]{64}$ ]]
test "$(sha256sum /opt/ccpanel/shared/release.tar.gz | cut -d' ' -f1)" = "$CCPANEL_ARCHIVE_SHA256"
cd /opt/ccpanel/current/infra
test "$(docker compose exec -T postgres psql -X -U ccpanel -d ccpanel -Atqc 'SELECT max(version) FROM schema_migrations')" = 10
if [ "$mode" = prepare ]; then
  test ! -e "/opt/ccpanel/releases/$CCPANEL_RELEASE"
  docker compose run --rm -T --no-deps api node dist/ingest-preflight.mjs
  docker compose run --rm -T --no-deps api node dist/consumer-progress.mjs check
  dir=$(mktemp -d /opt/ccpanel/shared/product-upgrade.XXXXXX)
  printf '%s\n' "$dir" >/opt/ccpanel/shared/product-upgrade.path
  readlink -f /opt/ccpanel/current >"$dir/previous-release.txt"
  printf '%s\n' "$CCPANEL_RELEASE" >"$dir/target-release.txt"
  printf '%s\n' "$CCPANEL_ARCHIVE_SHA256" >"$dir/archive.sha256"
  docker compose stop api worker
  # Freeze the original column projection. New metadata columns may be added,
  # but every existing value and relation count must remain unchanged.
  docker compose exec -T postgres psql -X -U ccpanel -d ccpanel -At -v ON_ERROR_STOP=1 >"$dir/fingerprint.sql" <<'SQL'
SELECT format('SELECT json_build_array(%L,count(*),coalesce(bit_xor(hashtextextended(row_to_json(t)::text,0)),0)) FROM (SELECT %s FROM %I.%I) t;',c.table_schema||'.'||c.table_name,string_agg(quote_ident(c.column_name),',' ORDER BY c.ordinal_position),c.table_schema,c.table_name)
FROM information_schema.columns c JOIN pg_tables p ON p.schemaname=c.table_schema AND p.tablename=c.table_name
WHERE c.table_schema='public' AND c.table_name NOT IN ('schema_migrations','recovery_gate','recovery_import_scopes') GROUP BY c.table_schema,c.table_name ORDER BY c.table_name;
SQL
  docker compose exec -T postgres psql -X -U ccpanel -d ccpanel -At -v ON_ERROR_STOP=1 <"$dir/fingerprint.sql" >"$dir/before.jsonl"
  bash /opt/ccpanel/shared/backup-remote.sh >"$dir/backup.path"
  docker compose run --rm -T --no-deps api node dist/deletion-ledger.mjs export >"$dir/source-ledger.json"
  echo "Upgrade prepared; services offline; verify an off-host encrypted backup and isolated restore before apply: $dir"
elif [ "$mode" = apply ]; then
  dir=$(realpath -e "${CCPANEL_UPGRADE_DIR:?Set prepared upgrade directory}")
  [[ "$dir" =~ ^/opt/ccpanel/shared/product-upgrade\.[A-Za-z0-9]{6}$ ]]
  test "$(cat "$dir/target-release.txt")" = "$CCPANEL_RELEASE"
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
  test "$(docker compose exec -T postgres psql -X -U ccpanel -d ccpanel -Atqc 'SELECT max(version) FROM schema_migrations')" = 15
  docker compose run --rm -T --no-deps api node dist/consumer-progress.mjs check
  curl -fsS https://192.236.151.125/ready
  echo 'Product preview upgraded; original columns and counts preserved; capacity remains unapproved'
else echo 'Expected prepare or apply' >&2; exit 1; fi
