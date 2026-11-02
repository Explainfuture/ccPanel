import {expect,it} from 'vitest';
import {HostResourceStatus} from '../packages/server/src/host-resource-status';
import type {HostTelemetry} from '../packages/server/src/host-telemetry';
const fixture=(at:number,d=0):HostTelemetry=>({version:1,sampledAt:at,disks:['kafka','postgres'].map(kind=>({kind:kind as 'kafka'|'postgres',sampledAt:at,totalBytes:'1000',availableBytes:'250',totalInodes:'0',availableInodes:'0'})),host:{memoryBytes:{total:'1000',available:'300',swapTotal:'1000',swapFree:'900',dirty:'0'},swapInPages:String(9007199254740993n+BigInt(d*4)),swapOutPages:String(20+d*2),pageBytes:4096,cpu:{userTicks:String(10+d*20),systemTicks:String(20+d*10),idleTicks:String(70+d*70),ioWaitTicks:'0',stealTicks:'0',totalTicks:String(100+d*100),ticksPerSecond:100},loadAverage:[.1,.2,.3]}});
it('uses distinct fresh samples and exact large-counter deltas, never manufactures zero rates for the first sample',()=>{
  let now=1000000;const status=new HostResourceStatus(()=>now);expect(status.snapshot().state).toBe('unavailable');
  status.observe(fixture(now));expect(status.snapshot()).toMatchObject({state:'ready',rate_state:'baseline_required',rates:null,gauges:{swap_used_bytes:'100'}});
  now+=10000;const input=fixture(now,1);status.observe(input);let snapshot=status.snapshot();
  expect(snapshot.rates).toMatchObject({interval_ms:10000,cpu_idle_percent:70,cpu_user_percent:20,cpu_system_percent:10,swap_in_bytes_per_second:'1638',swap_out_bytes_per_second:'819'});
  expect(snapshot.gauges!.disks[0]).toMatchObject({available_percent:25,available_inode_percent:null});
  status.observe(structuredClone(input));expect(status.snapshot().rates?.interval_ms).toBe(10000);
  input.host.memoryBytes.available='1';expect(status.snapshot().gauges?.memory_bytes.available).toBe('300');
  snapshot.rates!.cpu_idle_percent=0;snapshot.gauges!.load_average[0]=999;expect(status.snapshot().rates?.cpu_idle_percent).toBe(70);expect(status.snapshot().gauges?.load_average[0]).toBe(.1);
});
it('clears stale/invalid/failed snapshots and does not turn gaps or counter resets into healthy zero activity',()=>{
  let now=1000000;const status=new HostResourceStatus(()=>now);status.observe(fixture(now));now+=10000;status.observe(fixture(now,1));
  now+=30001;expect(status.snapshot()).toMatchObject({state:'stale',gauges:null,rates:null});status.observe(fixture(now,2));expect(status.snapshot()).toMatchObject({state:'ready',rate_state:'sample_gap',rates:null});
  now+=10000;status.observe(fixture(now));expect(status.snapshot()).toMatchObject({rate_state:'counter_reset',rates:null});
  now+=10000;status.observe(fixture(now,1));expect(status.snapshot().rate_state).toBe('ready');status.failed();expect(status.snapshot()).toMatchObject({state:'unavailable',gauges:null,rates:null});
  status.observe(fixture(now+1));expect(status.snapshot().state).toBe('stale');status.observe({version:1});expect(status.snapshot().state).toBe('invalid');
});
it('rejects contradictory same-timestamp counters and backwards samples, while object key order is immaterial',()=>{
  const now=1000000,status=new HostResourceStatus(()=>now);const frame=fixture(now);status.observe(frame);status.observe({host:frame.host,disks:frame.disks,sampledAt:frame.sampledAt,version:1});expect(status.snapshot().state).toBe('ready');
  status.observe(fixture(now,1));expect(status.snapshot()).toMatchObject({state:'invalid',gauges:null});
  status.observe(frame);status.observe(fixture(now-1));expect(status.snapshot()).toMatchObject({state:'invalid',rates:null});
});
