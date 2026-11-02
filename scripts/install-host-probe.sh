#!/usr/bin/env bash
set -euo pipefail
umask 077
source_dir=${CCPANEL_PROBE_SOURCE:-/opt/ccpanel/current/infra/host-probe}
test -f "$source_dir/host-telemetry.py"
test -f "$source_dir/ccpanel-host-probe.service"
test -f "$source_dir/ccpanel-host-probe.timer"
install -d -m 755 /opt/ccpanel/shared/telemetry
install -m 755 "$source_dir/host-telemetry.py" /opt/ccpanel/shared/host-telemetry.py
# Resolve mountpoints once, not two Docker CLI processes on every timer tick.
# Volume recreation requires this installer to be rerun before reopening intake.
python3 - <<'PY'
import json,subprocess,os
from pathlib import Path
config=[]
for kind,volume in [('kafka','ccpanel_kafka_data'),('postgres','ccpanel_pg_data')]:
    path=subprocess.check_output(['docker','volume','inspect','--format','{{.Mountpoint}}',volume],text=True,timeout=10).strip()
    if not path or '\n' in path or not Path(path).is_absolute() or not Path(path).is_dir(): raise ValueError('invalid_volume_mountpoint')
    config.append({'kind':kind,'path':path})
pending=Path('/opt/ccpanel/shared/host-probe-config.pending.json')
pending.write_text(json.dumps(config));pending.chmod(0o600)
os.replace(pending,Path('/opt/ccpanel/shared/host-probe-config.json'))
PY
python3 /opt/ccpanel/shared/host-telemetry.py --config /opt/ccpanel/shared/host-probe-config.json --output /opt/ccpanel/shared/telemetry/host.json
install -m 644 "$source_dir/ccpanel-host-probe.service" /etc/systemd/system/ccpanel-host-probe.service
install -m 644 "$source_dir/ccpanel-host-probe.timer" /etc/systemd/system/ccpanel-host-probe.timer
systemd-analyze verify /etc/systemd/system/ccpanel-host-probe.service /etc/systemd/system/ccpanel-host-probe.timer
systemctl daemon-reload
systemctl start ccpanel-host-probe.service
systemctl enable --now ccpanel-host-probe.timer
systemctl is-active ccpanel-host-probe.timer
