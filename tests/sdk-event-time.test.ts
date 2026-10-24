import 'fake-indexeddb/auto';
import {afterEach,expect,it,vi} from 'vitest';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join,resolve,relative,isAbsolute} from 'node:path';
import {Analytics,type TrackOptions} from '../packages/sdk-browser/src/index';
import {IndexedQueue} from '../packages/sdk-browser/src/indexed-queue';
import {createNodeAnalytics} from '../packages/sdk-node/src/index';
import {limits,type AnalyticsEvent} from '../packages/schema/src/index';
afterEach(()=>vi.restoreAllMocks());
const base={endpoint:'https://example.invalid/v1/batch',projectId:'time-test',writeKey:'test',consent:true,flushInterval:60000};
it('snapshots business time and preserves it through IndexedDB reopen and an unacknowledged retry',async()=>{
  let now=Date.now();vi.spyOn(Date,'now').mockImplementation(()=>now);
  const time=new Date(now-86400000).toISOString(),options={event_time:time},name='time-'+crypto.randomUUID();
  const attempts:AnalyticsEvent[][]=[];
  const send:typeof fetch=async(_url,init)=>{const events=JSON.parse(init!.body as string).events;attempts.push(events);return new Response('',{status:503,headers:{'retry-after':'60'}});};
  const first=new Analytics({...base,queue:new IndexedQueue(name),fetch:send});
  let id:string|undefined;
  try{await first.flush();const pending=first.track('order_paid',{amount:5},options);options.event_time=new Date(now).toISOString();id=(await pending).event_id;await first.flush();}finally{first.destroy();}
  now+=60001;
  const second=new Analytics({...base,queue:new IndexedQueue(name),fetch:async(_url,init)=>{const events:AnalyticsEvent[]=JSON.parse(init!.body as string).events;attempts.push(events);return Response.json({accepted_event_ids:events.map(e=>e.event_id)},{status:202});}});
  try{await second.flush();expect(attempts).toHaveLength(2);expect(attempts[0]![0]!.event_time).toBe(time);expect(attempts[1]).toEqual(attempts[0]);expect(attempts[1]![0]!.event_id).toBe(id);expect((await second.getQueueStatus()).count).toBe(0);}finally{second.destroy();}
});
it('rejects invalid, expired and too-far-future times before queueing or transport',async()=>{
  const now=Date.now();vi.spyOn(Date,'now').mockReturnValue(now);
  const send=vi.fn<typeof fetch>(async()=>new Response('',{status:503})),errors:string[]=[];
  const c=new Analytics({...base,queue:new IndexedQueue('invalid-time-'+crypto.randomUUID()),fetch:send,onError:code=>errors.push(code)});
  try{
    await c.flush();
    const invalid:unknown[]=[{event_time:'bad'},{event_time:new Date(now-limits.ttl-1).toISOString()},{event_time:new Date(now+300001).toISOString()},{event_time:123},null,{event_time:new Date(now).toISOString(),extra:true}];
    for(const options of invalid)expect(await c.track('click',{},options as TrackOptions)).toEqual({status:'rejected',reason:'invalid_event_time'});
    await c.flush();expect((await c.getQueueStatus()).count).toBe(0);expect(send).not.toHaveBeenCalled();expect(errors).toEqual(invalid.map(()=>'invalid_event_time'));
    expect((await c.track('lower_boundary',{}, {event_time:new Date(now-limits.ttl).toISOString()})).status).toBe('queued-persistent');
    expect((await c.track('upper_boundary',{}, {event_time:new Date(now+300000).toISOString()})).status).toBe('queued-persistent');
  }finally{c.destroy();}
});
it('preserves explicit per-user business time in Node file outbox recovery',async()=>{
  const dir=await mkdtemp(join(tmpdir(),'ccpanel-time-')),outboxPath=join(dir,'outbox.json'),time=new Date(Date.now()-86400000).toISOString();
  const first=createNodeAnalytics({...base,outboxPath,fetch:async()=>new Response('',{status:503})});
  let ids:(string|undefined)[]=[];
  try{
    await first.flush();ids=(await Promise.all([first.trackFor('A','paid',{amount:1},{event_time:time}),first.trackFor('B','paid',{amount:2},{event_time:time})])).map(r=>r.event_id);
  }finally{first.destroy();await first.shutdown();}
  const seen:AnalyticsEvent[]=[];
  // A future clock lets the persisted retry lease expire without sleeping.
  vi.spyOn(Date,'now').mockReturnValue(Date.now()+120000);
  const second=createNodeAnalytics({...base,outboxPath,fetch:async(_url,init)=>{const events:AnalyticsEvent[]=JSON.parse(init!.body as string).events;seen.push(...events);return Response.json({accepted_event_ids:events.map(e=>e.event_id)},{status:202});}});
  try{await second.flush();expect(seen.map(e=>e.event_id)).toEqual(ids);expect(seen.map(e=>[e.distinct_id,e.event_time])).toEqual([['A',time],['B',time]]);expect((await second.getQueueStatus()).count).toBe(0);}finally{await second.shutdown();const target=resolve(dir),within=relative(resolve(tmpdir()),target);if(!within||within.startsWith('..')||isAbsolute(within)||!target.includes('ccpanel-time-'))throw new Error('unsafe_test_cleanup');await rm(target,{recursive:true,force:true});}
});
