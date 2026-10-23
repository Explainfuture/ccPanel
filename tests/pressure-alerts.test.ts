import {expect,it} from 'vitest';
import {evaluatePressureAlerts} from '../packages/server/src/pressure-alerts';
import {defaultPressurePolicy,evaluatePressure,type PressureSnapshot} from '../packages/server/src/ingest-pressure';
import {IngestAdmission} from '../packages/server/src/ingest-admission';
const now=Date.UTC(2026,9,23),p=defaultPressurePolicy;
const fixture=():PressureSnapshot=>({startedAt:now,partitions:[{partition:0,low:'0',high:'20',committed:'10',ageBoundary:'0'}],disks:['kafka','postgres'].map(kind=>({kind:kind as 'kafka'|'postgres',sampledAt:now,totalBytes:'21474836480',availableBytes:'12884901888',totalInodes:'100000',availableInodes:'50000'}))});
const signals=(value:unknown,at=now)=>evaluatePressureAlerts(value,p,at).signals;
it('agrees with the admission guard across combined lag age byte inode and freshness boundaries',()=>{
  for(const lag of[0,255,256,257])for(const free of['2147483647','2147483648','2147483649'])for(const inodes of['1023','1024','1025'])for(const old of[false,true])for(const stale of[false,true]){
    const f=fixture();f.startedAt=now-(stale?p.maxProbeAgeMs+1:0);f.partitions[0]={partition:0,low:'0',high:String(lag),committed:'0',ageBoundary:old&&lag>0?'1':'0'};f.disks[0]!.availableBytes=free;f.disks[1]!.availableInodes=inodes;
    expect(signals(f).every(r=>r.state==='clear')).toBe(evaluatePressure(f,p,now).allowed);
  }
});
it('reports independently healthy domains with source-specific expiry and exact lag',()=>{
  const rows=signals(fixture());expect(rows.map(r=>r.state)).toEqual(['clear','clear','clear']);expect(rows[0]).toMatchObject({max_lag_messages:'10',total_lag_messages:'10',expires_at:new Date(now+p.maxProbeAgeMs).toISOString()});expect(rows[2]!.expires_at).toBe(new Date(now+p.maxDiskAgeMs).toISOString());
  expect(signals(fixture(),now+p.maxProbeAgeMs)[0]!.state).toBe('clear');expect(signals(fixture(),now+p.maxProbeAgeMs+1).map(r=>r.state)).toEqual(['unknown','unknown','clear']);expect(signals(fixture(),now+p.maxDiskAgeMs+1).map(r=>r.state)).toEqual(['unknown','unknown','unknown']);
});
it('does not let disk thresholds mask Kafka lag or offset integrity',()=>{
  const f=fixture();f.partitions[0]!.high='266';f.disks[0]!.availableBytes='2147483648';
  expect(signals(f).map(r=>r.state)).toEqual(['firing','clear','firing']);expect(signals(f)[0]!.reason).toBe('consumer_lag_high');
  f.partitions[0]={partition:0,low:'12',high:'20',committed:'10',ageBoundary:'12'};
  expect(signals(f).map(r=>r.state)).toEqual(['unknown','firing','firing']);expect(signals(f)[0]!.max_lag_messages).toBeNull();
  f.partitions[0]!.committed=null;expect(signals(f)[1]!.reason).toBe('consumer_offset_unknown');
});
it('uses exact 64-bit offsets, inclusive reserves and the oldest disk sample',()=>{
  const f=fixture();f.partitions=[{partition:0,low:'0',high:'9223372036854775807',committed:'0',ageBoundary:'0'},{partition:1,low:'0',high:'9223372036854775807',committed:'0',ageBoundary:'0'}];
  expect(signals(f)[0]).toMatchObject({state:'firing',max_lag_messages:'9223372036854775807',total_lag_messages:'18446744073709551614'});
  f.disks[1]!.sampledAt=now-1000;f.disks[1]!.availableInodes='1024';expect(signals(f)[2]).toMatchObject({state:'firing',reason:'disk_inodes_low',observed_at:new Date(now-1000).toISOString()});
});
it('keeps malformed, missing, future and failed observations unknown without echoing payloads',()=>{
  expect(signals(null).every(r=>r.state==='unknown')).toBe(true);
  for(const value of [{...fixture(),secret:'private'}, {...fixture(),partitions:[]}, {...fixture(),partitions:[{...fixture().partitions[0],high:'9223372036854775808'}]}])expect(signals(value)[0]!.state).toBe('unknown');
  const f=fixture();f.startedAt=now+1;expect(signals(f)[0]!.reason).toBe('future_sample');f.disks[1]!.sampledAt=now+1;expect(signals(f)[2]!.reason).toBe('future_sample');
  const failed=evaluatePressureAlerts(fixture(),p,now,{unavailable:'private-failure-detail'});expect(failed.signals.every(r=>r.state==='unknown')).toBe(true);expect(JSON.stringify(failed)).not.toContain('private-failure-detail');
});
it('latches integrity failures across fresh and failed probes without changing admission state',()=>{
  const gate=new IngestAdmission({...p},()=>now);gate.observe(fixture());const before=gate.status();expect(gate.pressureAlerts().signals.every(r=>r.state==='clear')).toBe(true);expect(gate.status()).toEqual(before);
  gate.restoreIntegrityGap('retained_offset_gap');gate.probeFailed('kafka_probe_unavailable');const paused=gate.status();expect(gate.pressureAlerts().signals.map(r=>r.state)).toEqual(['unknown','firing','unknown']);expect(gate.status()).toEqual(paused);
  expect(()=>evaluatePressureAlerts(fixture(),p,NaN)).toThrow('invalid_pressure_alert_time');
  expect(()=>evaluatePressureAlerts(fixture(),{...p,minFreePercent:101},now)).toThrow('invalid_pressure_alert_policy');
});
