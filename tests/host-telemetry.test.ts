import { expect,it } from 'vitest';
import { execFileSync } from 'node:child_process';
import { resolve } from 'node:path';
const modulePath=resolve('infra/host-probe/host-telemetry.py');
function check(code:string){return execFileSync(process.platform==='win32'?'python':'python3',['-c',`import importlib.util,sys,tempfile,json,os
from pathlib import Path
from unittest.mock import patch
spec=importlib.util.spec_from_file_location('probe',sys.argv[1]);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
${code}` ,modulePath],{encoding:'utf8',env:{...process.env,PYTHONDONTWRITEBYTECODE:'1'}}).trim();}
it('publishes complete atomic snapshots and preserves the previous sample on a replace or encoding failure',()=>{
  expect(check(`with tempfile.TemporaryDirectory() as d:
    output=Path(d)/'host.json';m.publish(output,{'version':1,'sampledAt':1});previous=output.read_bytes()
    with patch.object(m.os,'replace',side_effect=OSError('simulated unavailable filesystem')):
        try: m.publish(output,{'version':1,'sampledAt':2})
        except OSError: pass
        else: raise AssertionError('replace should fail')
    assert output.read_bytes()==previous
    try: m.publish(output,{'sampledAt':float('nan')})
    except ValueError: pass
    else: raise AssertionError('non-finite sample should fail')
    assert output.read_bytes()==previous
    assert list(Path(d).iterdir())==[output]
    m.publish(output,{'version':1,'sampledAt':3});assert json.loads(output.read_text())['sampledAt']==3
print('passed')`)).toBe('passed');
});
it('counts swap pages and CPU ticks without double-counting guest time or exporting raw process data',()=>{
  expect(check(`with tempfile.TemporaryDirectory() as d:
    p=Path(d)
    (p/'meminfo').write_text('MemTotal: 1000 kB\\nMemAvailable: 600 kB\\nSwapTotal: 500 kB\\nSwapFree: 400 kB\\nDirty: 3 kB\\n')
    (p/'vmstat').write_text('pswpin 12\\npswpout 3\\n')
    (p/'stat').write_text('cpu 10 2 3 20 4 1 2 1 5 1\\n')
    (p/'loadavg').write_text('0.10 0.20 0.30 1/123 456')
    with patch.object(m.os,'sysconf',create=True,side_effect=lambda key: 100 if key=='SC_CLK_TCK' else 4096):
        value=m.host_counters(p)
    assert value['memoryBytes']['available']=='614400'
    assert value['swapInPages']=='12' and value['swapOutPages']=='3'
    assert value['cpu']['totalTicks']=='43'
    assert value['cpu']['idleTicks']=='20'
    assert '123' not in json.dumps(value) and '456' not in json.dumps(value)
print('passed')`)).toBe('passed');
});
