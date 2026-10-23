import 'fake-indexeddb/auto';
import {afterEach,expect,it,vi} from 'vitest';
import {Analytics} from '../packages/sdk-browser/src/index';
import {MemoryQueue} from '../packages/sdk-browser/src/queue';
import {IndexedQueue} from '../packages/sdk-browser/src/indexed-queue';
import {limits,type AnalyticsEvent} from '../packages/schema/src/index';
afterEach(()=>{vi.restoreAllMocks();vi.unstubAllGlobals();});
const options={endpoint:'https://example.invalid/v1/batch',projectId:'exit-expiry',writeKey:'test',consent:true,flushInterval:60000};
it('filters only expired exit records without deleting unconfirmed fresh or expired queue data',async()=>{
  let now=Date.now();vi.spyOn(Date,'now').mockImplementation(()=>now);
  const queue=new MemoryQueue(),sendBeacon=vi.fn(()=>true);vi.stubGlobal('navigator',{sendBeacon});
  const c=new Analytics({...options,queue,fetch:async()=>new Response('',{status:503})});
  try{
    await c.flush();await c.track('old');now+=limits.ttl+1;const fresh=await c.track('fresh',{}, {event_time:new Date(now).toISOString()});
    expect(c.beacon()).toBe(true);expect(c.beacon()).toBe(false);
    const body=sendBeacon.mock.calls[0] as unknown as [string,Blob];const payload=JSON.parse(await body[1].text());expect(payload.events.map((e:AnalyticsEvent)=>e.event_id)).toEqual([fresh.event_id]);expect(Object.keys(payload.events[0])).not.toContain('created');
    expect((await c.getQueueStatus()).count).toBe(2);expect((await c.getQueueStatus()).counters.expired).toBe(0);
    await c.flush();expect((await c.getQueueStatus()).count).toBe(1);expect((await c.getQueueStatus()).counters.expired).toBe(1);
  }finally{c.destroy();}
});
it('uses restored IndexedDB creation time and keeps the exact TTL boundary inclusive',async()=>{
  let now=Date.now();vi.spyOn(Date,'now').mockImplementation(()=>now);
  const queue=new IndexedQueue('restored-exit-'+crypto.randomUUID()),event:AnalyticsEvent={event_id:crypto.randomUUID(),event_name:'restored',distinct_id:'A',event_time:new Date(now).toISOString(),properties:{},context:{},schema_version:1};
  await queue.add(event,20,10000,{created:now-limits.ttl,next:0,attempts:0});
  const sendBeacon=vi.fn(()=>false);vi.stubGlobal('navigator',{sendBeacon});
  const c=new Analytics({...options,queue,fetch:async()=>new Response('',{status:503})});
  try{await c.flush();expect(c.beacon()).toBe(false);expect(sendBeacon).toHaveBeenCalledTimes(1);expect((await c.getQueueStatus()).count).toBe(1);}finally{c.destroy();}
  now++;
  // A second instance restores the same original creation time; its queue sweep
  // removes the expired record rather than refreshing its lifetime.
  const second=new Analytics({...options,queue,fetch:async()=>{throw Error('expired record must not send');}});
  try{await second.flush();expect(second.beacon()).toBe(false);expect(sendBeacon).toHaveBeenCalledTimes(1);expect((await second.getQueueStatus()).counters.expired).toBe(1);}finally{second.destroy();}
});
it('does not send an entirely expired pending snapshot or a destroyed client',async()=>{
  let now=Date.now();vi.spyOn(Date,'now').mockImplementation(()=>now);
  const sendBeacon=vi.fn(()=>true);vi.stubGlobal('navigator',{sendBeacon});
  const c=new Analytics({...options,queue:new MemoryQueue(),fetch:async()=>new Response('',{status:503})});
  try{await c.flush();await c.track('old');now+=limits.ttl+1;expect(c.beacon()).toBe(false);expect(sendBeacon).not.toHaveBeenCalled();expect((await c.getQueueStatus()).count).toBe(1);await c.track('fresh',{}, {event_time:new Date(now).toISOString()});c.destroy();expect(c.beacon()).toBe(false);expect(sendBeacon).not.toHaveBeenCalled();}finally{c.destroy();}
});
it('does not refresh a restored snapshot TTL during a failed flush',async()=>{
  let now=Date.now();vi.spyOn(Date,'now').mockImplementation(()=>now);
  const queue=new IndexedQueue('aged-exit-'+crypto.randomUUID()),event:AnalyticsEvent={event_id:crypto.randomUUID(),event_name:'restored',distinct_id:'A',event_time:new Date(now).toISOString(),properties:{},context:{},schema_version:1};
  await queue.add(event,20,10000,{created:now-limits.ttl+1,next:0,attempts:0});
  const sendBeacon=vi.fn(()=>true);vi.stubGlobal('navigator',{sendBeacon});
  const c=new Analytics({...options,queue,fetch:async()=>new Response('',{status:503})});
  try{await c.flush();now+=2;expect(c.beacon()).toBe(false);expect(sendBeacon).not.toHaveBeenCalled();expect((await c.getQueueStatus()).count).toBe(1);await c.flush();expect((await c.getQueueStatus()).counters.expired).toBe(1);}finally{c.destroy();}
});
