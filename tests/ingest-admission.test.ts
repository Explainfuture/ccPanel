import { expect,it } from 'vitest';
import { IngestAdmission,IngestionPaused } from '../packages/server/src/ingest-admission';
import { defaultPressurePolicy,type PressureSnapshot } from '../packages/server/src/ingest-pressure';
const initial=Date.parse('2026-10-23T00:00:00Z');
function fixture(at:number,high='0',committed='0'):PressureSnapshot{return {startedAt:at,partitions:[{partition:0,low:'0',high,committed,ageBoundary:'0'}],disks:['kafka','postgres'].map(kind=>({kind:kind as 'kafka'|'postgres',sampledAt:at,totalBytes:'21474836480',availableBytes:'12884901888',totalInodes:'100000',availableInodes:'50000'}))};}
it('closes intake before the first probe and immediately after a probe failure',()=>{
  const gate=new IngestAdmission({...defaultPressurePolicy},()=>initial);
  expect(()=>gate.reserve(1)).toThrow(IngestionPaused);gate.observe(fixture(initial));expect(gate.status().allowed).toBe(true);
  gate.probeFailed('host_probe_unavailable');expect(gate.status()).toMatchObject({allowed:false,reason:'host_probe_unavailable'});
  gate.observe(fixture(initial+1));expect(gate.status().allowed).toBe(true);
});
it('reserves synchronously so simultaneous requests cannot each spend the same headroom',async()=>{
  const gate=new IngestAdmission({...defaultPressurePolicy},()=>initial);gate.observe(fixture(initial));
  const attempts=await Promise.allSettled(Array.from({length:20},async()=>gate.reserve(20)));
  expect(attempts.filter(r=>r.status==='fulfilled')).toHaveLength(12);
  expect(gate.status()).toMatchObject({reservedMessages:240,remainingMessages:16,pendingRequests:12});
  expect(()=>gate.reserve(20)).toThrow(IngestionPaused);gate.reserve(16);expect(gate.status().allowed).toBe(false);
});
it('does not free in-flight or recently acknowledged sends using an older probe',()=>{
  let now=initial;const gate=new IngestAdmission({...defaultPressurePolicy},()=>now);gate.observe(fixture(now));
  const r=gate.reserve(100);now+=100;gate.observe(fixture(now));expect(gate.status().reservedMessages).toBe(100);
  now+=100;r.settle(true);gate.observe(fixture(initial+150));expect(gate.status().reservedMessages).toBe(100);
  now+=100;gate.observe(fixture(now,'100'));expect(gate.status()).toMatchObject({reservedMessages:0,remainingMessages:156});
  gate.observe(fixture(initial+200));expect(gate.status().remainingMessages).toBe(156);
});
it('keeps unknown outcomes for an extra minute and waits for a subsequent fresh observation',()=>{
  let now=initial;const gate=new IngestAdmission({...defaultPressurePolicy},()=>now);gate.observe(fixture(now));
  const r=gate.reserve(20);r.settle(false);r.settle(true);
  now+=59999;gate.observe(fixture(now));expect(gate.status()).toMatchObject({unknownMessages:20,reservedMessages:20});
  now+=2;expect(gate.status().reservedMessages).toBe(20);gate.observe(fixture(now));expect(gate.status().reservedMessages).toBe(0);
});
it('re-evaluates sample expiry at admission time rather than only on probe completion',()=>{
  let now=initial;const gate=new IngestAdmission({...defaultPressurePolicy},()=>now);gate.observe(fixture(now));now+=15001;
  expect(gate.status()).toMatchObject({allowed:false,reason:'pressure_probe_stale'});expect(()=>gate.reserve(1)).toThrow(IngestionPaused);
});
it('latches observed retention gaps instead of automatically reopening after a consumer reset',()=>{
  let now=initial;const gate=new IngestAdmission({...defaultPressurePolicy},()=>now),gap=fixture(now,'100','10');gap.partitions[0]!.low='20';gap.partitions[0]!.ageBoundary='20';gate.observe(gap);
  expect(gate.status().reason).toBe('retained_offset_gap');now+=100;gate.observe(fixture(now,'100','100'));expect(gate.status()).toMatchObject({allowed:false,reason:'retained_offset_gap'});
});
it('does not let a simultaneous disk warning hide an observed integrity gap',()=>{
  const gate=new IngestAdmission({...defaultPressurePolicy},()=>initial),gap=fixture(initial,'100','10');gap.partitions[0]!.low='20';gap.partitions[0]!.ageBoundary='20';gap.disks[0]!.availableBytes='0';gate.observe(gap);
  expect(gate.status()).toMatchObject({allowed:false,integrityReason:'retained_offset_gap'});
});
