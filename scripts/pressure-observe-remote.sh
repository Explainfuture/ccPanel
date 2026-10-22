#!/usr/bin/env bash
# Read-only one-shot observation, not a running admission controller.
set -euo pipefail
umask 077
dir=$(mktemp -d /opt/ccpanel/shared/pressure-observe.XXXXXX)
cleanup(){ rm -f "$dir/disk.json"; rmdir "$dir"; }
trap cleanup EXIT
python3 - <<'PY' >"$dir/disk.json"
import subprocess,os,time,json
samples=[]
for kind,volume in [('kafka','ccpanel_kafka_data'),('postgres','ccpanel_pg_data')]:
    path=subprocess.check_output(['docker','volume','inspect','--format','{{.Mountpoint}}',volume],text=True).strip()
    value=os.statvfs(path)
    samples.append({'kind':kind,'sampledAt':int(time.time()*1000),'totalBytes':str(value.f_blocks*value.f_frsize),'availableBytes':str(value.f_bavail*value.f_frsize),'totalInodes':str(value.f_files),'availableInodes':str(value.f_favail)})
print(json.dumps(samples))
PY
cd /opt/ccpanel/current/infra
docker compose run --rm -T --no-deps api node "${CCPANEL_PRESSURE_SCRIPT:-dist/pressure-observe.mjs}" <"$dir/disk.json"
