import { expect,it,vi } from 'vitest';
import { ConsumerProgressError,progressOffset,progressPlan,refreshProgressHigh } from '../packages/server/src/consumer-progress';
it('starts a truly new retained stream at zero but requires explicit legacy baseline',()=>{
  expect(progressPlan([{partition:0,low:'0',high:'3',committed:null}],null,[]).initialize).toEqual([{partition:0,next_offset:'0'}]);
  expect(()=>progressPlan([{partition:0,low:'0',high:'10',committed:'10'}],null,[])).toThrow('progress_bootstrap_required');
  expect(progressPlan([{partition:0,low:'2',high:'10',committed:'10'}],null,[],true).initialize).toEqual([{partition:0,next_offset:'10'}]);
});
it('does not treat unknown expired history or a missing known checkpoint as a fresh partition',()=>{
  expect(()=>progressPlan([{partition:0,low:'2',high:'10',committed:null}],null,[],true)).toThrow('bootstrap_history_unknown');
  expect(()=>progressPlan([{partition:0,low:'0',high:'10',committed:'0'}],1,[])).toThrow('progress_partition_missing');
});
it('allows crash-window replay behind the durable cursor without resetting the cursor',()=>{
  expect(progressPlan([{partition:0,low:'0',high:'10',committed:'8'}],1,[{partition:0,next_offset:'9'}]).initialize).toEqual([]);
  expect(()=>progressPlan([{partition:0,low:'0',high:'10',committed:'10'}],1,[{partition:0,next_offset:'9'}])).toThrow('broker_ahead_of_database');
});
it('detects a retained gap even when Kafka has already reset its own commit',()=>{
  try{progressPlan([{partition:0,low:'20',high:'20',committed:'20'}],1,[{partition:0,next_offset:'10'}]);throw new Error('must fail');}catch(e){expect(e).toBeInstanceOf(ConsumerProgressError);expect((e as ConsumerProgressError).reason).toBe('retained_offset_gap');}
});
it('preserves exact 64-bit positions and rejects truncation or malformed topology',()=>{
  expect(progressOffset('9007199254741001')).toBe(9007199254741001n);
  expect(()=>progressPlan([{partition:0,low:'0',high:'8',committed:'8'}],1,[{partition:0,next_offset:'9'}])).toThrow('progress_after_log');
  expect(()=>progressPlan([{partition:1,low:'0',high:'0',committed:null}],null,[])).toThrow('invalid_progress_topology');
  for(const value of ['-1','1.5','9223372036854775808'])expect(()=>progressOffset(value)).toThrow('invalid_progress_offset');
});
it('only initializes genuinely appended partitions while preserving existing proof',()=>{
  const positions=[{partition:1,low:'0',high:'1',committed:null},{partition:0,low:'0',high:'5',committed:'5'}];
  expect(progressPlan(positions,1,[{partition:0,next_offset:'5'}]).initialize).toEqual([{partition:1,next_offset:'0'}]);
});
it('refreshes a stale high watermark without mixing a later retention boundary with the fixed PG checkpoint',async()=>{
  const positions=[{partition:0,low:'0',high:'10',committed:'9'}],rows=[{partition:0,next_offset:'12'}];
  const refresh=vi.fn(async()=>[{partition:0,high:'20',low:'15'}]);const adjusted=await refreshProgressHigh(positions,rows,refresh);
  expect(adjusted[0]).toEqual({partition:0,low:'0',high:'20',committed:'9'});expect(positions[0]?.high).toBe('10');expect(()=>progressPlan(adjusted,1,rows)).not.toThrow();
});
it('does not turn a real high-watermark regression into healthy progress',async()=>{
  await expect(refreshProgressHigh([{partition:0,low:'0',high:'10',committed:'9'}],[{partition:0,next_offset:'12'}],async()=>[{partition:0,high:'8'}])).rejects.toThrow('broker_history_regressed');
});
