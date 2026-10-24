import {expect,it} from 'vitest';
import {IntakeMetrics} from '../packages/server/src/intake-metrics';
import {WorkerMetrics} from '../packages/server/src/worker-metrics';
import {parseOperationalSnapshot} from '../packages/server/src/operational-snapshot';
it('accepts fixed first-party snapshots, retains precise counters and freezes data before asynchronous storage',()=>{
  const metrics=new IntakeMetrics();metrics.observe({status:202,events:3,durationMs:5});const input=metrics.snapshot();input.responses[0]!.batches='9007199254740993';
  const parsed=parseOperationalSnapshot(input);expect(parsed.source).toBe('api');expect((parsed.snapshot as typeof input).responses[0]!.batches).toBe('9007199254740993');
  const reordered=Object.fromEntries(Object.entries(input).reverse());expect(parseOperationalSnapshot(reordered).payload_hash).toBe(parsed.payload_hash);
  input.responses[0]!.batches='1';expect((parsed.snapshot as typeof input).responses[0]!.batches).toBe('9007199254740993');
  expect(parseOperationalSnapshot(new WorkerMetrics().snapshot()).source).toBe('worker');
});
it('rejects arbitrary fields, dynamic labels, payloads, bad counters and oversized input',()=>{
  const base=new IntakeMetrics().snapshot();
  for(const value of [{...base,payload:{email:'private'}},{...base,responses:[...base.responses,{status:'user-id',batches:'0',known_events:'0',unknown_event_batches:'0'}]},{...base,accepted_processing_unknown:'-1'},{...base,accepted_processing_unknown:'1'.repeat(41)},{...base,scope:'tenant_metrics'},{...base,payload:'x'.repeat(13000)}])expect(()=>parseOperationalSnapshot(value)).toThrow('invalid_operational_snapshot');
  const worker=JSON.parse(JSON.stringify(new WorkerMetrics().snapshot()));worker.operations[0].outcomes[0].outcome='customer';expect(()=>parseOperationalSnapshot(worker)).toThrow();
  const invalid=JSON.parse(JSON.stringify(base));invalid.accepted_processing_ms[0].upper_bound_ms=2;expect(()=>parseOperationalSnapshot(invalid)).toThrow();
});
it('rejects noncanonical time, stale/future observations and incompatible counter semantics',()=>{
  const base=new IntakeMetrics().snapshot(),now=Date.now();
  for(const value of [{...base,started_at:new Date(now+10000).toISOString()},{...base,observed_at:new Date(now+6000).toISOString()},{...base,observed_at:new Date(now-8*86400000).toISOString()},{...base,observed_at:'yesterday'},{...base,counter_semantics:'sum_every_snapshot'}])expect(()=>parseOperationalSnapshot(value,now)).toThrow();
});
