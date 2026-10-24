import {expect,it} from 'vitest';
import {IntakeMetrics} from '../packages/server/src/intake-metrics';
it('separates response batches and submitted events, keeps fixed cardinality and precise snapshot copies',()=>{
  const metrics=new IntakeMetrics();metrics.observe({status:202,events:20,durationMs:5});metrics.observe({status:202,events:20,durationMs:31000});metrics.observe({status:413,events:null,durationMs:1});
  for(let status=100;status<600;status++)if(![202,400,401,403,413,429,500,503].includes(status))metrics.observe({status,events:101,durationMs:100});
  const snapshot=metrics.snapshot();expect(snapshot.responses).toHaveLength(9);expect(snapshot.responses.find(r=>r.status==='202')).toMatchObject({batches:'2',known_events:'40'});expect(snapshot.responses.find(r=>r.status==='413')?.unknown_event_batches).toBe('1');
  expect(snapshot.accepted_processing_ms.find(r=>r.upper_bound_ms===5)?.count).toBe('1');expect(snapshot.accepted_processing_ms.at(-1)?.count).toBe('1');
  snapshot.responses[0].batches='999';expect(metrics.snapshot().responses[0].batches).toBe('2');
  expect(snapshot.counter_semantics).toBe('cumulative_since_process_start');expect(new IntakeMetrics().snapshot().instance_id).not.toBe(snapshot.instance_id);
});
it('does not treat an invalid latency sample or event count as a real zero',()=>{
  const metrics=new IntakeMetrics();metrics.observe({status:202,events:NaN,durationMs:NaN});
  const value=metrics.snapshot();expect(value.accepted_processing_unknown).toBe('1');expect(value.accepted_processing_ms.every(r=>r.count==='0')).toBe(true);expect(value.responses[0]).toMatchObject({known_events:'0',unknown_event_batches:'1'});
});
