import {expect,it} from 'vitest';
import {execFileSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import {mkdtempSync,writeFileSync,readFileSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve,dirname} from 'node:path';
import {capacityReport,type CapacityEvidence,type CapacityFrame} from '../scripts/capacity-report';
const names=['api','worker','kafka','postgres'];
function fixture():CapacityEvidence{
  const frames:CapacityFrame[]=[0,10000,20000,30000].map((at,i)=>({capturedAt:100000+at,elapsedMs:at,telemetry:{version:1,sampledAt:100000+at,disks:['kafka','postgres'].map(kind=>({kind:kind as 'kafka'|'postgres',sampledAt:100000+at,totalBytes:'1000000000',availableBytes:'700000000',totalInodes:'10000',availableInodes:'9000'})),host:{memoryBytes:{total:'1000000000',available:'300000000',swapTotal:'1000000000',swapFree:'900000000',dirty:'0'},swapInPages:'100',swapOutPages:'20',pageBytes:4096,cpu:{userTicks:String(100+i*20),systemTicks:String(100+i*10),idleTicks:String(100+i*70),ioWaitTicks:'0',stealTicks:'0',totalTicks:String(300+i*100),ticksPerSecond:100},loadAverage:[0.1,0.2,0.3]}},containers:Object.fromEntries(names.map(name=>[name,{memoryBytes:20000000,memoryEvents:{oom:0,oom_kill:0},cpu:{usage_usec:100+i*10000}}]))}));
  const state=Object.fromEntries(names.map((name,i)=>[name,{id:String(i+1).repeat(64),restartCount:0,oomKilled:false,startedAt:'2026-10-22T00:00:00Z',running:true}]));
  return{startedAt:100500,finishedAt:129500,releaseStart:'m4-integrity-001',releaseEnd:'m4-integrity-001',frames,baseline:structuredClone(state),final:structuredClone(state)};
}
it('reports weighted CPU, actual memory/disk and sufficient quiet-resource evidence',()=>{
  const report=capacityReport(fixture());expect(report.evidenceValid).toBe(true);expect(report.conservativeResourcePass).toBe(true);expect(report.cpuIdlePercent).toBe(70);expect(report.diskAvailableMinPercent).toBe(70);expect(report.memoryAvailableMinBytes).toBe('300000000');
});
it('requires both capture and host-counter coverage through the end',()=>{
  const data=fixture();data.frames[3]!.telemetry!.sampledAt=129000;for(const disk of data.frames[3]!.telemetry!.disks)disk.sampledAt=129000;
  expect(capacityReport(data).reasons).toContain('window_not_covered');
  const extra=structuredClone(data.frames[3]!);extra.capturedAt=140000;extra.elapsedMs=40000;extra.telemetry!.sampledAt=140000;for(const disk of extra.telemetry!.disks)disk.sampledAt=140000;
  extra.telemetry!.host.cpu.userTicks='180';extra.telemetry!.host.cpu.systemTicks='140';extra.telemetry!.host.cpu.idleTicks='380';extra.telemetry!.host.cpu.totalTicks='700';for(const c of Object.values(extra.containers!))c.cpu.usage_usec=40100;
  data.frames.push(extra);expect(capacityReport(data).evidenceValid).toBe(true);
});
it('never passes missing, erroneous, stale or gapped evidence as zero activity',()=>{
  const missing=fixture();missing.frames.splice(1,2);expect(capacityReport(missing).reasons).toContain('capture_gap');
  const failed=fixture();failed.frames[1]={capturedAt:110000,elapsedMs:10000,error:'OSError'};expect(capacityReport(failed).conservativeResourcePass).toBe(false);expect(capacityReport(failed).reasons).toContain('sample_error');
  const stale=fixture();stale.frames[2]!.telemetry!.sampledAt=80000;for(const disk of stale.frames[2]!.telemetry!.disks)disk.sampledAt=80000;expect(capacityReport(stale).evidenceValid).toBe(false);
});
it('invalidates reset counters and distinguishes OOM, restart and replacement',()=>{
  const reset=fixture();reset.frames[2]!.telemetry!.host.swapInPages='0';expect(capacityReport(reset).reasons).toContain('host_counter_reset');
  const cpu=fixture();cpu.frames[2]!.containers!.api!.cpu.usage_usec=0;expect(capacityReport(cpu).reasons).toContain('container_counter_reset');
  const restarted=fixture();restarted.final.api!.restartCount=1;expect(capacityReport(restarted).restarts).toBe(1);expect(capacityReport(restarted).conservativeResourcePass).toBe(false);
  const replaced=fixture();replaced.final.api!.id='f'.repeat(64);expect(capacityReport(replaced).reasons).toContain('container_replaced_or_reset');
  const oom=fixture();oom.frames[2]!.containers!.api!.memoryEvents.oom=1;oom.frames[3]!.containers!.api!.memoryEvents.oom=1;expect(capacityReport(oom).oomObserved).toBe(true);expect(capacityReport(oom).conservativeResourcePass).toBe(false);
});
it('reports swap rates and continuous periods without conflating occupied swap with activity',()=>{
  const data=fixture();data.frames[1]!.telemetry!.host.swapInPages='110';data.frames[2]!.telemetry!.host.swapInPages='120';data.frames[3]!.telemetry!.host.swapInPages='120';
  const report=capacityReport(data);expect(report.swapActiveIntervals).toBe(2);expect(report.longestSwapActivitySeconds).toBe(20);expect(report.swapInMaxKiBPerSecond).toBe(4);expect(report.swapReviewRequired).toBe(true);expect(report.conservativeResourcePass).toBe(false);
});
it('does not accept stopped containers or missing final running state despite unchanged restart counters',()=>{
  const stopped=fixture();stopped.final.api!.running=false;const report=capacityReport(stopped);expect(report.evidenceValid).toBe(true);expect(report.notRunning).toEqual(['api']);expect(report.conservativeResourcePass).toBe(false);
  const unknown=fixture();delete unknown.final.api!.running;expect(capacityReport(unknown).reasons).toContain('container_state_missing');
});
it('CLI binds exact UTC milliseconds and checksum to the exported source bytes',()=>{
  const dir=mkdtempSync(join(tmpdir(),'ccpanel-capacity-cli-')),data=fixture();
  try{
    const raw=data.frames.map(f=>JSON.stringify(f)).join('\n')+'\n';
    const configuration={swappiness:10,limits:{api:'128m',worker:'96m'}};
    const final={containers:data.final,release:data.releaseEnd,releaseStart:data.releaseStart,loadFinishedAt:data.finishedAt,samplesSha256:createHash('sha256').update(raw).digest('hex'),configuration};
    writeFileSync(join(dir,'samples'),raw);writeFileSync(join(dir,'load'),JSON.stringify({started_at:new Date(data.startedAt).toISOString(),completed_at:new Date(data.finishedAt).toISOString()}));writeFileSync(join(dir,'baseline'),JSON.stringify({containers:data.baseline,release:data.releaseStart,configuration}));writeFileSync(join(dir,'final'),JSON.stringify(final));
    const run=(name:string)=>execFileSync(process.execPath,['--import','tsx','scripts/capacity-report-cli.ts',...['samples','load','baseline','final',name].map(p=>join(dir,p))],{stdio:'pipe'});
    run('valid');const valid=JSON.parse(readFileSync(join(dir,'valid'),'utf8'));expect(valid.evidenceValid).toBe(true);expect(valid.configurationChecked).toBe(true);
    const originalLoad=readFileSync(join(dir,'load'),'utf8');writeFileSync(join(dir,'load'),JSON.stringify({...JSON.parse(originalLoad),termination:{state:'aborted',reason:'resource_safety',planned_events:1800}}));expect(()=>run('aborted')).toThrow();expect(()=>readFileSync(join(dir,'aborted'))).toThrow();writeFileSync(join(dir,'load'),originalLoad);
    writeFileSync(join(dir,'final'),JSON.stringify({...final,configuration:{...configuration,swappiness:60}}));expect(()=>run('changed-config')).toThrow();expect(JSON.parse(readFileSync(join(dir,'changed-config'),'utf8')).reasons).toContain('configuration_changed');
    for(const drift of [38,8*3600000]){writeFileSync(join(dir,'final'),JSON.stringify({...final,loadFinishedAt:data.finishedAt+drift}));expect(()=>run('invalid-window')).toThrow();}
    writeFileSync(join(dir,'final'),JSON.stringify(final));writeFileSync(join(dir,'samples'),raw.replace('300000000','299999999'));expect(()=>run('invalid-hash')).toThrow();
    writeFileSync(join(dir,'samples'),raw.slice(0,-1));expect(()=>run('incomplete')).toThrow();
  }finally{
    if(dirname(resolve(dir))!==resolve(tmpdir())||!dir.startsWith(join(tmpdir(),'ccpanel-capacity-cli-')))throw new Error('unexpected_temporary_directory');
    rmSync(dir,{recursive:true});
  }
});
it('invalidates changed or missing configuration while ignoring JSON object key order',()=>{
  const data=fixture();data.configurationStart={swappiness:10,limits:{worker:'96m',api:'128m'}};data.configurationEnd={limits:{api:'128m',worker:'96m'},swappiness:10};
  expect(capacityReport(data).evidenceValid).toBe(true);expect(capacityReport(data).configurationChecked).toBe(true);
  data.configurationEnd={swappiness:60,limits:{worker:'96m',api:'128m'}};expect(capacityReport(data).reasons).toContain('configuration_changed');
  data.configurationEnd=undefined;expect(capacityReport(data).reasons).toContain('configuration_missing');
});
it('reports weighted CPU and exact optional pressure deltas without changing the resource gate',()=>{
  const data=fixture();data.frames.forEach((frame,index)=>{for(const value of Object.values(frame.containers!)){value.memoryEvents.high=9007199254740000+index*2;value.memoryEvents.max=100+index*3;value.cpu.nr_throttled=200+index;value.cpu.throttled_usec=1000+index*250;}});
  const report=capacityReport(data);expect(report.evidenceValid).toBe(true);expect(report.conservativeResourcePass).toBe(true);
  expect(report.cpuBreakdownPercent).toEqual({userTicks:20,systemTicks:10,idleTicks:70,ioWaitTicks:0,stealTicks:0});
  expect(report.containerPressureDeltas.api).toEqual({memory_high:6,memory_max:9,throttle_periods:3,throttle_usec:750});
});
it('keeps missing optional pressure counters unknown and rejects resets inside the window',()=>{
  const data=fixture();expect(capacityReport(data).containerPressureDeltas.api).toEqual({memory_high:null,memory_max:null,throttle_periods:null,throttle_usec:null});
  data.frames.forEach((frame,index)=>{for(const value of Object.values(frame.containers!))value.memoryEvents.max=[100,110,90,120][index]!;});
  const reset=capacityReport(data);expect(reset.evidenceValid).toBe(false);expect(reset.reasons).toContain('container_counter_reset');expect(reset.containerPressureDeltas.api!.memory_max).toBe(null);expect(reset.cpuBreakdownPercent.idleTicks).toBe(null);
});
