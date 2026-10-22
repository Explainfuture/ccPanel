#!/usr/bin/env bash
# Creates exactly one new independent run; never infer completion from old files.
set -euo pipefail
umask 077
: "${CCPANEL_EXPECTED_RELEASE:?Set fixed release name}"
: "${CCPANEL_CAPACITY_LABEL:?Set configuration label}"
[[ "$CCPANEL_EXPECTED_RELEASE" =~ ^[A-Za-z0-9_-]+$ ]]
[[ "$CCPANEL_CAPACITY_LABEL" =~ ^[A-Za-z0-9_-]{1,40}$ ]]
test "$(readlink -f /opt/ccpanel/current)" = "/opt/ccpanel/releases/$CCPANEL_EXPECTED_RELEASE"
test -z "$(systemctl list-units --type=service --state=running 'ccpanel-capacity-*' --no-legend --plain)"
cd /opt/ccpanel/current/infra
test "$(docker compose exec -T postgres psql -X -U ccpanel -d ccpanel -Atqc "SELECT count(*) FROM tenants WHERE name='CAPACITY EXTERNAL TEMP'")" = 0
dir=$(mktemp -d /opt/ccpanel/shared/capacity-integrity.XXXXXX)
printf '%s\n' "$dir" >/opt/ccpanel/shared/capacity-integrity.path
printf '%s\n' "$CCPANEL_CAPACITY_LABEL" >"$dir/label.txt"
readlink -f /opt/ccpanel/current >"$dir/release.txt"
docker compose run --rm -T --no-deps api node scripts/load-fixture.mjs create >"$dir/fixture.json"
fixture_ready=$(date +%s%3N)
python3 /opt/ccpanel/shared/capacity-host-state.py >"$dir/configuration.json"
python3 - "$dir" <<'PY'
import json,pathlib,subprocess,sys
root=pathlib.Path(sys.argv[1]);config={};states={}
for name in ['api','worker','kafka','postgres']:
    fmt='{{json .Id}}|{{.RestartCount}}|{{json .State.OOMKilled}}|{{json .State.StartedAt}}|{{json .State.Running}}'
    values=subprocess.check_output(['docker','inspect','--format',fmt,'ccpanel-'+name+'-1'],text=True).strip().split('|')
    state={'id':json.loads(values[0]),'restartCount':int(values[1]),'oomKilled':json.loads(values[2]),'startedAt':json.loads(values[3]),'running':json.loads(values[4])}
    assert state['running'] and not state['oomKilled'],'container not healthy at baseline'
    config[name]=state['id'];states[name]=state
(root/'containers.json').write_text(json.dumps(config))
(root/'baseline.json').write_text(json.dumps({'release':(root/'release.txt').read_text().strip(),'containers':states,'configuration':json.loads((root/'configuration.json').read_text())}))
PY
unit="ccpanel-capacity-$(date -u +%Y%m%d%H%M%S)"
printf '%s\n' "$unit" >"$dir/unit.txt"
systemd-run --unit "$unit" --property=MemoryMax=32M --property=CPUQuota=5% --property=RuntimeMaxSec=2050 --property=NoNewPrivileges=true --property=ProtectSystem=strict --property=ProtectHome=true --property=PrivateTmp=true --property=PrivateNetwork=true --property=ProtectControlGroups=true --property=CapabilityBoundingSet= --property="ReadWritePaths=$dir" /usr/bin/python3 /opt/ccpanel/shared/capacity-sample.py --config "$dir/containers.json" --output "$dir/samples.jsonl" --seconds 1980
for attempt in $(seq 1 30); do if [ -s "$dir/samples.jsonl" ]; then break; fi; sleep 0.1; done
python3 - "$dir/samples.jsonl" <<'PY'
import json,sys
row=json.loads(open(sys.argv[1]).readline());assert 'error' not in row
print(json.dumps({'sampler_first_frame_valid':True,'capturedAt':row['capturedAt']}))
PY
# Exclude operator fixture setup from the measured window. Require two
# distinct fresh host frames after that one-off container has exited.
python3 - "$dir/samples.jsonl" "$fixture_ready" <<'PY'
import json,pathlib,sys,time
path=pathlib.Path(sys.argv[1]);ready=int(sys.argv[2]);deadline=time.monotonic()+40
while time.monotonic()<deadline:
    rows=[json.loads(line) for line in path.read_text().splitlines()]
    valid=[r for r in rows if 'error' not in r and r.get('telemetry',{}).get('sampledAt',0)>=ready and r['capturedAt']>=r['telemetry']['sampledAt']]
    if len({r['telemetry']['sampledAt'] for r in valid})>=2 and valid[-1]==rows[-1]:
        print(json.dumps({'post_setup_baseline_ready':True,'fresh_host_samples':len({r['telemetry']['sampledAt'] for r in valid})}));break
    time.sleep(.25)
else:raise RuntimeError('post_setup_host_baseline_timeout')
PY
systemctl is-active --quiet "$unit.service"
printf 'CAPACITY_RUN evidence=%s unit=%s label=%s\n' "$dir" "$unit" "$CCPANEL_CAPACITY_LABEL"
