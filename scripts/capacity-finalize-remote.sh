#!/usr/bin/env bash
# Export only AFTER the actual generator has completed. This does not stop or
# restart either workload/sampler and performs no application writes.
set -euo pipefail
umask 077
dir=$(realpath -e "${CCPANEL_CAPACITY_DIR:?Set existing capacity evidence directory}")
[[ "$dir" =~ ^/opt/ccpanel/shared/capacity-integrity\.[A-Za-z0-9]{6}$ ]]
finish=${CCPANEL_CAPACITY_FINISHED_MS:?Set actual load completed_at in Unix milliseconds}
[[ "$finish" =~ ^[0-9]{13}$ ]]
exec 9>"$dir/.finalize.lock"
flock -n 9 || { echo 'Capacity export already running' >&2; exit 1; }
python3 - "$dir" "$finish" <<'PY'
import hashlib,json,os,pathlib,subprocess,sys,time
root=pathlib.Path(sys.argv[1]);finish=int(sys.argv[2])
if (root/'final.json').exists() and (root/'samples-finished.jsonl').exists():
    previous=json.loads((root/'final.json').read_text())
    assert previous['loadFinishedAt']==finish,'different load window'
    assert hashlib.sha256((root/'samples-finished.jsonl').read_bytes()).hexdigest()==previous['samplesSha256'],'snapshot pair mismatch'
    print(json.dumps({'capacity_export_complete':True,'existing_snapshot':True}));sys.exit(0)
raw=(root/'samples.jsonl').read_bytes()
assert raw and len(raw)<=8*1024*1024 and raw.endswith(b'\n'),'incomplete sample read; wait and retry same run'
rows=[json.loads(line) for line in raw.splitlines()]
assert any(row.get('capturedAt',0)>=finish and row.get('telemetry',{}).get('sampledAt',0)>=finish for row in rows),'host window not yet covered; wait for same sampler'
states={}
for name in ['api','worker','kafka','postgres']:
    # Select only state metadata; never export config/env/process arguments.
    fmt='{{json .Id}}|{{.RestartCount}}|{{json .State.OOMKilled}}|{{json .State.StartedAt}}|{{json .State.Running}}'
    values=subprocess.check_output(['docker','inspect','--format',fmt,'ccpanel-'+name+'-1'],text=True).strip().split('|')
    states[name]={'id':json.loads(values[0]),'restartCount':int(values[1]),'oomKilled':json.loads(values[2]),'startedAt':json.loads(values[3]),'running':json.loads(values[4])}
final={'loadFinishedAt':finish,'capturedAt':int(time.time()*1000),'releaseStart':(root/'release.txt').read_text().strip(),'release':str(pathlib.Path('/opt/ccpanel/current').resolve()),'containers':states,'samplesSha256':hashlib.sha256(raw).hexdigest()}
def publish(name,data):
    temporary=root/(name+'.tmp')
    with temporary.open('wb') as file:file.write(data);file.flush();os.fsync(file.fileno())
    os.replace(temporary,root/name)
publish('samples-finished.jsonl',raw)
publish('final.json',json.dumps(final).encode())
print(json.dumps({'capacity_export_complete':True,'existing_snapshot':False,'frames':len(rows)}))
PY
