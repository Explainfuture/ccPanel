#!/usr/bin/env bash
# Run after real capacity generator completion and verified resource export.
# Every suite receives a separate empty database; running schema is untouched.
set -euo pipefail
umask 077
source /opt/ccpanel/shared/runtime.env
evidence=$(realpath -e "${CCPANEL_CAPACITY_DIR:?Set completed capacity evidence directory}")
[[ "$evidence" =~ ^/opt/ccpanel/shared/capacity-integrity\.[A-Za-z0-9]{6}$ ]]
python3 - "$evidence" <<'PY'
import datetime,hashlib,json,pathlib,sys,time
root=pathlib.Path(sys.argv[1]);meta=json.loads((root/'final.json').read_text())
load=json.loads((root/'load-result.json').read_text());end=datetime.datetime.fromisoformat(load['completed_at'].replace('Z','+00:00'))
assert end.tzinfo is not None and end.utcoffset()==datetime.timedelta(0)
assert meta['loadFinishedAt']==int((end-datetime.datetime(1970,1,1,tzinfo=datetime.timezone.utc))//datetime.timedelta(milliseconds=1))
assert meta['loadFinishedAt']<=time.time()*1000
assert hashlib.sha256((root/'samples-finished.jsonl').read_bytes()).hexdigest()==meta['samplesSha256']
PY
version=${CCPANEL_VALIDATION_VERSION:-16}
[[ "$version" =~ ^(15|16)$ ]]
stage="/opt/ccpanel/shared/validation-v$version"
case "${CCPANEL_VALIDATION_SUITE:-all}" in
  all) suites='company-provision report-edit query-export';;
  company-provision) suites=company-provision;;
  report-edit) suites=report-edit;;
  query-export) suites=query-export;;
  *) echo 'Unsupported isolated validation suite' >&2;exit 1;;
esac
# The parent /app source mount is read-only. Docker needs the nested mount
# point to exist on the host checkout before assembling this one-off container.
install -d -m 755 /opt/ccpanel/current/scripts/validation
cd /opt/ccpanel/current/infra
dir=$(mktemp -d /opt/ccpanel/shared/provision-report-check.XXXXXX)
db='';created=false
cleanup(){
  if [ "$created" = true ]; then docker compose exec -T postgres dropdb -U ccpanel --if-exists --force "$db"; fi
  rm -f "$dir/test.env" "$dir/ledger.json"
}
trap cleanup EXIT
for suite in $suites; do
  db="ccpanel_restore_$(date -u +%Y%m%d%H%M%S)_$(openssl rand -hex 4)"
  [[ "$db" =~ ^ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$ ]]
  printf 'DATABASE_URL=%s/%s\n' "${DATABASE_URL%/*}" "$db" >"$dir/test.env"
  docker compose exec -T postgres createdb -U ccpanel "$db";created=true
  docker compose run --rm -T --no-deps --env-from-file "$dir/test.env" -v "$stage:/app/scripts/validation:ro" api node "scripts/validation/migrate-v$version.mjs"
  # A fresh empty database exports its own fresh source ledger. This is not
  # an old backup and never overrides the running database recovery gate.
  docker compose run --rm -T --no-deps --env-from-file "$dir/test.env" -v "$stage:/app/scripts/validation:ro" api node scripts/validation/deletion-ledger.mjs export >"$dir/ledger.json"
  docker compose run --rm -T --no-deps --env-from-file "$dir/test.env" -e CCPANEL_OFFLINE_RESTORE=true -v "$stage:/app/scripts/validation:ro" api node scripts/validation/deletion-ledger.mjs apply <"$dir/ledger.json"
  rm -f "$dir/ledger.json"
  if ! docker compose run --rm -T --no-deps --env-from-file "$dir/test.env" -v "$stage:/app/scripts/validation:ro" api node "scripts/validation/$suite-check.mjs" >"$dir/$suite.log" 2>&1; then
    echo "Isolated $suite validation failed; protected diagnostics: $dir" >&2;exit 1
  fi
  cat "$dir/$suite.log"
  docker compose exec -T postgres dropdb -U ccpanel --force "$db";created=false
done
rm -f "$dir/test.env" "$dir/ledger.json"
echo "Isolated schema$version checks completed; running release not migrated"
