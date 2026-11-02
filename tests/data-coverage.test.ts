import {expect,it} from 'vitest';
import {dataCoverage} from '../packages/server/src/data-coverage';
const now=Date.parse('2026-10-23T08:00:00Z'),day=86400000;
const spec=(from:number,to:number)=>({from:new Date(from).toISOString(),to:new Date(to).toISOString()});
it('marks expired range even when no events matched and preserves exact cutoff',()=>{
  const partial=dataCoverage(spec(now-30*day-1,now-day),now);expect(partial.expired_overlap).toBe(true);expect(partial.incomplete).toBe(true);
  const complete=dataCoverage(spec(now-30*day,now),now);expect(complete.incomplete).toBe(false);expect(complete.retained_from).toBe(new Date(now-30*day).toISOString());
});
it('distinguishes future observation from expired data without depending on machine timezone',()=>{
  const future=dataCoverage(spec(now-day,now+1),now);expect(future.future_overlap).toBe(true);expect(future.expired_overlap).toBe(false);
  expect(dataCoverage(spec(now-31*day,now+1),now)).toMatchObject({future_overlap:true,expired_overlap:true,incomplete:true});
});
