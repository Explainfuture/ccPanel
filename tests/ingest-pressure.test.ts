import { expect,it } from 'vitest';
import { defaultPressurePolicy,evaluatePressure,pressurePolicy,type PressureSnapshot } from '../packages/server/src/ingest-pressure';
const now=Date.parse('2026-10-23T00:00:00Z');
const fixture=():PressureSnapshot=>({startedAt:now,partitions:[{partition:0,low:'0',high:'20',committed:'10',ageBoundary:'0'},{partition:1,low:'0',high:'0',committed:null,ageBoundary:'0'}],disks:['kafka','postgres'].map(kind=>({kind:kind as 'kafka'|'postgres',sampledAt:now,totalBytes:'21474836480',availableBytes:'12884901888',totalInodes:'100000',availableInodes:'50000'}))});
it('reports exact lag and conservative partition headroom, including an empty new group',()=>{
  expect(evaluatePressure(fixture(),defaultPressurePolicy,now)).toMatchObject({allowed:true,maxLagMessages:'10',totalLagMessages:'10',remainingMessages:246});
  const f=fixture();f.partitions[0]={partition:0,low:'9007199254740990',high:'9007199254741001',committed:'9007199254741000',ageBoundary:'9007199254740990'};
  expect(evaluatePressure(f,defaultPressurePolicy,now)).toMatchObject({allowed:true,maxLagMessages:'1'});
});
it('refuses expired probes and future or malformed counters instead of guessing zero',()=>{
  const f=fixture();f.startedAt=now-defaultPressurePolicy.maxProbeAgeMs-1;
  expect(evaluatePressure(f,defaultPressurePolicy,now).reason).toBe('pressure_probe_stale');
  for(const counter of ['-1','1.5','NaN','9223372036854775808',10]){const g=fixture();g.partitions[0]!.high=counter as string;expect(evaluatePressure(g,defaultPressurePolicy,now).reason).toBe('pressure_probe_invalid');}
  f.startedAt=now+1001;expect(evaluatePressure(f,defaultPressurePolicy,now).allowed).toBe(false);
});
it('does not reset expired or unknown consumer offsets to the earliest retained record',()=>{
  const f=fixture();f.partitions[0]={partition:0,low:'12',high:'20',committed:'10',ageBoundary:'12'};
  expect(evaluatePressure(f,defaultPressurePolicy,now).reason).toBe('retained_offset_gap');
  f.partitions[0].committed=null;expect(evaluatePressure(f,defaultPressurePolicy,now).reason).toBe('consumer_offset_unknown');
});
it('rejects both excessive lag and old unconsumed messages but allows a fully caught-up old log',()=>{
  const f=fixture();f.partitions[0]!.high='266';expect(evaluatePressure(f,defaultPressurePolicy,now).reason).toBe('consumer_lag_high');
  f.partitions[0]!.high='20';f.partitions[0]!.ageBoundary='15';expect(evaluatePressure(f,defaultPressurePolicy,now).reason).toBe('consumer_lag_old');
  f.partitions[0]!.committed='20';f.partitions[0]!.ageBoundary='20';expect(evaluatePressure(f,defaultPressurePolicy,now).allowed).toBe(true);
});
it('requires fresh PG and Kafka filesystems with byte and inode recovery reserve',()=>{
  const f=fixture();f.disks[1]!.sampledAt=now-defaultPressurePolicy.maxDiskAgeMs-1;expect(evaluatePressure(f,defaultPressurePolicy,now).reason).toBe('disk_probe_stale');
  f.disks[1]!.sampledAt=now;f.disks[1]!.availableBytes='2147483648';expect(evaluatePressure(f,defaultPressurePolicy,now).reason).toBe('disk_space_low');
  f.disks[1]!.availableBytes='12884901888';f.disks[0]!.availableInodes='1024';expect(evaluatePressure(f,defaultPressurePolicy,now).reason).toBe('disk_inodes_low');
});
it('fails closed for incomplete, duplicated or internally inconsistent observations',()=>{
  for(const change of [(f:PressureSnapshot)=>f.disks.pop(),(f:PressureSnapshot)=>{f.disks[1]!.kind='kafka';},(f:PressureSnapshot)=>{f.partitions[1]!.partition=0;},(f:PressureSnapshot)=>{f.partitions[0]!.committed='21';},(f:PressureSnapshot)=>{f.partitions[0]!.ageBoundary='21';}]){const f=fixture();change(f);expect(evaluatePressure(f,defaultPressurePolicy,now).reason).toBe('pressure_probe_invalid');}
});
it('validates operator budgets and never treats blank or disabled safety limits as defaults',()=>{
  expect(pressurePolicy({})).toEqual(defaultPressurePolicy);expect(pressurePolicy({INGEST_MAX_LAG_MESSAGES:'500'}).maxLagMessages).toBe(500);
  for(const env of [{INGEST_MAX_LAG_MESSAGES:'0'},{INGEST_MAX_LAG_MESSAGES:''},{INGEST_MIN_FREE_BYTES:'NaN'},{INGEST_MAX_LAG_AGE_MS:String(72*3600000)}])expect(()=>pressurePolicy(env)).toThrow('invalid_pressure_config');
});
