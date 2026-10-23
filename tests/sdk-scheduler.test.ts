import {afterEach,expect,it,vi} from 'vitest';
import {Analytics} from '../packages/sdk-browser/src/index';
import {MemoryQueue} from '../packages/sdk-browser/src/queue';
import type{AnalyticsEvent} from '../packages/schema/src/index';
afterEach(()=>{vi.useRealTimers();vi.restoreAllMocks();});
const base={endpoint:'https://example.invalid/v1/batch',projectId:'schedule-test',writeKey:'synthetic',consent:true};
const ack:typeof fetch=async(_url,init)=>{const events:AnalyticsEvent[]=JSON.parse(init!.body as string).events;return Response.json({accepted_event_ids:events.map(e=>e.event_id)},{status:202});};
it('does no idle storage polling, wakes for new events and stops again after exact confirmation',async()=>{
  vi.useFakeTimers();const queue=new MemoryQueue(),stats=vi.spyOn(queue,'stats'),send=vi.fn(ack),options={...base,queue,fetch:send,flushInterval:3000},c=new Analytics(options);
  try{await c.flush();stats.mockClear();await vi.advanceTimersByTimeAsync(12000);expect(stats).not.toHaveBeenCalled();expect(send).not.toHaveBeenCalled();
    await c.track('wake');options.flushInterval=100;await vi.advanceTimersByTimeAsync(2999);expect(send).not.toHaveBeenCalled();await vi.advanceTimersByTimeAsync(1);expect(send).toHaveBeenCalledTimes(1);expect((await c.getQueueStatus()).count).toBe(0);
    stats.mockClear();await vi.advanceTimersByTimeAsync(12000);expect(stats).not.toHaveBeenCalled();await c.track('wake_again');await vi.advanceTimersByTimeAsync(3000);expect(send).toHaveBeenCalledTimes(2);
  }finally{c.destroy();}
});
it('keeps bounded retry scheduling and respects Retry-After without new tracks',async()=>{
  vi.useFakeTimers();let attempts=0;const send=vi.fn<typeof fetch>(async(...args)=>++attempts===1?new Response('',{status:503,headers:{'retry-after':'60'}}):ack(...args)),c=new Analytics({...base,queue:new MemoryQueue(),fetch:send});
  try{await c.flush();await c.track('retry');await vi.advanceTimersByTimeAsync(3000);expect(send).toHaveBeenCalledTimes(1);await vi.advanceTimersByTimeAsync(59000);expect(send).toHaveBeenCalledTimes(1);await vi.advanceTimersByTimeAsync(1000);expect(send).toHaveBeenCalledTimes(2);expect((await c.getQueueStatus()).count).toBe(0);}finally{c.destroy();}
});
it('recovers unknown persistent storage on a timer without requiring a new event',async()=>{
  vi.useFakeTimers();class Unavailable extends MemoryQueue{override readonly persistent=true;fail=false;override async stats(){if(this.fail)throw Error('unavailable');return super.stats();}override async expire(before:number){if(this.fail)throw Error('unavailable');return super.expire(before);}}
  const queue=new Unavailable(),event:AnalyticsEvent={event_id:crypto.randomUUID(),event_name:'restored',distinct_id:'A',event_time:new Date().toISOString(),properties:{},context:{},schema_version:1};await queue.add(event,1000,5000000);queue.fail=true;
  const send=vi.fn(ack),c=new Analytics({...base,queue,fetch:send});
  try{await c.flush();expect(send).not.toHaveBeenCalled();queue.fail=false;await vi.advanceTimersByTimeAsync(3000);expect(send).toHaveBeenCalledTimes(1);expect((await c.getQueueStatus()).count).toBe(0);}finally{c.destroy();}
});
it('cancels pending scheduling on withdrawal and destroy',async()=>{
  vi.useFakeTimers();const send=vi.fn(ack),c=new Analytics({...base,queue:new MemoryQueue(),fetch:send});
  try{await c.flush();await c.track('withdraw');await c.setConsent(false);await vi.advanceTimersByTimeAsync(6000);expect(send).not.toHaveBeenCalled();await c.setConsent(true);await c.flush();await c.track('destroy');c.destroy();await vi.advanceTimersByTimeAsync(6000);expect(send).not.toHaveBeenCalled();}finally{c.destroy();}
});
it('stops credential-paused polling and resumes the retained event after key rotation',async()=>{
  vi.useFakeTimers();let attempts=0;const send=vi.fn<typeof fetch>(async(...args)=>++attempts===1?new Response('',{status:401}):ack(...args)),queue=new MemoryQueue(),stats=vi.spyOn(queue,'stats'),c=new Analytics({...base,queue,fetch:send});
  try{await c.flush();await c.track('paused');await vi.advanceTimersByTimeAsync(3000);expect(send).toHaveBeenCalledTimes(1);stats.mockClear();await vi.advanceTimersByTimeAsync(9000);expect(stats).not.toHaveBeenCalled();expect(send).toHaveBeenCalledTimes(1);c.updateWriteKey('rotated-synthetic');await c.flush();expect(send).toHaveBeenCalledTimes(2);expect((await c.getQueueStatus()).count).toBe(0);}finally{c.destroy();}
});
