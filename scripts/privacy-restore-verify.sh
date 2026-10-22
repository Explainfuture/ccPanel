#!/usr/bin/env bash
set -euo pipefail
umask 077
name=${CCPANEL_DRILL_NAME:?Set the prepared privacy-restore-NAME}
[[ "$name" =~ ^privacy-restore-[a-zA-Z0-9_-]+$ ]]
dir="/opt/ccpanel/shared/$name"
test -s "$dir/restored-database.dump"
test -s "$dir/restored-ledger.json"
source /opt/ccpanel/shared/runtime.env
cd /opt/ccpanel/current/infra
runtime=${CCPANEL_DRILL_RUNTIME:-auto}
if [ "$runtime" = auto ]; then if [ -f ../dist/deletion-ledger.mjs ]; then runtime=installed; else runtime=staged; fi; fi
case "$runtime" in
  installed) ledger=dist/deletion-ledger.mjs; migrate=dist/migrate.mjs; api=dist/api.mjs;;
  staged) ledger=scripts/staged-deletion-ledger.mjs; migrate=scripts/staged-migrate.mjs; api=scripts/staged-api.mjs;;
  *) echo 'Invalid drill runtime' >&2; exit 1;;
esac
db="ccpanel_restore_$(date -u +%Y%m%d%H%M%S)_$(openssl rand -hex 4)"
[[ "$db" =~ ^ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$ ]]
probe="ccpanel-restore-probe-$(openssl rand -hex 4)"
target="${DATABASE_URL%/*}/$db"
cleanup(){
  docker rm -f "$probe" >/dev/null 2>&1 || true
  docker compose exec -T postgres dropdb -U ccpanel --if-exists --force "$db"
}
trap cleanup EXIT
run(){ docker compose run --rm -T --no-deps -e DATABASE_URL="$target" -e CCPANEL_OFFLINE_RESTORE=true api node "$@"; }
gate(){ docker compose exec -T postgres psql -X -U ccpanel -d "$db" -At -c 'SELECT blocked FROM recovery_gate WHERE id=true'; }
apply(){ run "$ledger" apply <"$1"; }
reject(){
  local file=$1 expected=$2
  if apply "$file" >"$dir/rejection.log" 2>&1; then echo "Unexpected recovery acceptance: $expected" >&2; exit 1; fi
  grep -q "$expected" "$dir/rejection.log"
  test "$(gate)" = t
}
started=$(date +%s)
docker compose exec -T postgres createdb -U ccpanel "$db"
docker compose exec -T postgres pg_restore -U ccpanel --dbname="$db" --single-transaction --exit-on-error --no-owner --no-acl <"$dir/restored-database.dump"
docker compose exec -T postgres psql -X -U ccpanel -d "$db" -At -v ON_ERROR_STOP=1 <../scripts/backup-fingerprint.sql >"$dir/restored-fingerprint.jsonl"
cmp "$dir/fingerprint.jsonl" "$dir/restored-fingerprint.jsonl"
run "$migrate"
run scripts/privacy-restore-fixture.mjs before <"$dir/fixture.json"
if run "$ledger" check >"$dir/gate-initial.log" 2>&1; then echo 'Restore unexpectedly open before ledger application' >&2; exit 1; fi
grep -q recovery_gate_closed "$dir/gate-initial.log"
set +e
timeout --signal=TERM --kill-after=3s 10s docker compose run --rm -T --name "$probe" --no-deps -e DATABASE_URL="$target" api node "$api" >"$dir/blocked-api.log" 2>&1
boot_status=$?
set -e
test "$boot_status" -ne 0
test "$boot_status" -ne 124
grep -q recovery_gate_closed "$dir/blocked-api.log"
printf '{}\n' >"$dir/invalid-ledger.json"
reject "$dir/invalid-ledger.json" invalid_deletion_ledger
python3 - "$dir" <<'PY'
import json,sys,uuid,datetime
from pathlib import Path
d=Path(sys.argv[1]); data=json.loads((d/'restored-ledger.json').read_text())
stale=dict(data); stale['exported_at']=(datetime.datetime.now(datetime.timezone.utc)-datetime.timedelta(hours=2)).isoformat().replace('+00:00','Z')
(d/'stale-ledger.json').write_text(json.dumps(stale))
missing=json.loads(json.dumps(data)); project=missing['projects'][0]; old=project['id']; tenant=project['tenant_id']; new=str(uuid.uuid4()); project['id']=new
for job in missing['jobs']:
 if job['tenant_id']==tenant and job['project_id']==old: job['project_id']=new
for barrier in missing['barriers']:
 if barrier['tenant_id']==tenant and barrier['project_id']==old: barrier['project_id']=new
(d/'missing-scope-ledger.json').write_text(json.dumps(missing))
PY
reject "$dir/stale-ledger.json" deletion_ledger_not_fresh
reject "$dir/missing-scope-ledger.json" restore_scope_metadata_missing
apply "$dir/restored-ledger.json"
run scripts/privacy-restore-fixture.mjs after <"$dir/fixture.json"
apply "$dir/restored-ledger.json"
run scripts/privacy-restore-fixture.mjs after <"$dir/fixture.json"
# A real isolated API process now holds the service lease; recovery must refuse
# without closing its already-open gate. No service ports are published.
docker compose run --rm -d -T --name "$probe" --no-deps -e DATABASE_URL="$target" api node "$api" >"$dir/online-probe-id"
held=false
for n in $(seq 1 30); do
  locks=$(docker compose exec -T postgres psql -X -U ccpanel -d "$db" -At -c "SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND database=(SELECT oid FROM pg_database WHERE datname=current_database()) AND objid=169202617 AND mode='ShareLock' AND granted")
  if [ "$locks" -gt 0 ]; then held=true; break; fi
  sleep 1
done
test "$held" = true
if apply "$dir/restored-ledger.json" >"$dir/online-rejection.log" 2>&1; then echo 'Online service did not fence recovery' >&2; exit 1; fi
grep -q services_still_online "$dir/online-rejection.log"
test "$(gate)" = f
docker rm -f "$probe" >/dev/null
printf '{"passed":true,"elapsed_seconds":%s,"old_rows":4,"remaining_rows":1,"deleted_rows":3,"default_closed":true,"invalid_and_stale_rejected":true,"missing_scope_rejected":true,"online_service_fenced":true,"repeat_apply_idempotent":true,"actor_absent_from_backup":true}\n' "$(( $(date +%s)-started ))" | tee "$dir/result.json"
