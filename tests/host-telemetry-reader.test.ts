import { expect,it } from 'vitest';
import { mkdtemp,writeFile,unlink,rmdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { decodeHostTelemetry,parseHostTelemetry,readHostTelemetry,type HostTelemetry } from '../packages/server/src/host-telemetry';
const at=1792698000000;
const fixture=():HostTelemetry=>({version:1,sampledAt:at,disks:['kafka','postgres'].map(kind=>({kind:kind as 'kafka'|'postgres',sampledAt:at,totalBytes:'21474836480',availableBytes:'12884901888',totalInodes:'100000',availableInodes:'50000'})),host:{memoryBytes:{total:'1024000',available:'614400',swapTotal:'512000',swapFree:'409600',dirty:'3072'},swapInPages:'12',swapOutPages:'3',pageBytes:4096,cpu:{userTicks:'12',systemTicks:'6',idleTicks:'20',ioWaitTicks:'4',stealTicks:'1',totalTicks:'43',ticksPerSecond:100},loadAverage:[0.1,0.2,0.3]}});
it('preserves exact resource counters and lets pressure policy decide freshness',()=>{
  const f=fixture();f.host.swapInPages='9007199254741001';
  expect(decodeHostTelemetry(Buffer.from(JSON.stringify(f)))).toEqual(f);
  expect(parseHostTelemetry(f).sampledAt).toBe(at);
});
it('rejects incomplete, leaked or inconsistent samples rather than creating zero gauges',()=>{
  const changes=[(f:HostTelemetry)=>{f.disks[0]!.sampledAt++;},(f:HostTelemetry)=>{f.disks[1]!.kind='kafka';},(f:HostTelemetry)=>{f.host.cpu.totalTicks='49';},(f:HostTelemetry)=>{f.host.memoryBytes.available='9999999';},(f:HostTelemetry)=>{f.host.swapOutPages='-1';},(f:HostTelemetry)=>{f.host.loadAverage[0]=NaN;},(f:HostTelemetry)=>{Object.assign(f.host,{processArguments:'must not be exported'});}];
  for(const change of changes){const f=fixture();change(f);expect(()=>parseHostTelemetry(f)).toThrow('host_probe_invalid');}
  expect(()=>decodeHostTelemetry(Buffer.from([0xff]))).toThrow('host_probe_invalid');
  expect(()=>decodeHostTelemetry(Buffer.alloc(32769))).toThrow('host_probe_invalid');
});
it('reads only a bounded regular snapshot and returns sanitized errors for unavailable/invalid files',async()=>{
  const dir=await mkdtemp(join(tmpdir(),'ccpanel-host-probe-')),file=join(dir,'host.json');
  try{
    await expect(readHostTelemetry(file)).rejects.toThrow('host_probe_unavailable');
    await writeFile(file,JSON.stringify(fixture()));expect(await readHostTelemetry(file)).toEqual(fixture());
    await writeFile(file,'x'.repeat(32769));await expect(readHostTelemetry(file)).rejects.toThrow('host_probe_invalid');
    await writeFile(file,'[]');await expect(readHostTelemetry(file)).rejects.toThrow('host_probe_invalid');
  }finally{await unlink(file).catch(()=>{});await rmdir(dir);}
});
