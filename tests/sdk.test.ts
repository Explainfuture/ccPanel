import 'fake-indexeddb/auto';
import { afterEach,describe,expect,it,vi } from 'vitest';
import { Analytics } from '../packages/sdk-browser/src/index';
import { IndexedQueue,MemoryQueue,type Queue } from '../packages/sdk-browser/src/queue';
import type { AnalyticsEvent } from '../packages/schema/src/index';
const clients:Analytics[]=[];
afterEach(()=>{for(const c of clients)c.destroy();clients.length=0;vi.restoreAllMocks();vi.unstubAllGlobals();});
const event=():AnalyticsEvent=>({event_id:crypto.randomUUID(),event_name:'click',distinct_id:'A',event_time:new Date().toISOString(),properties:{},context:{},schema_version:1});
function client(queue:Queue=new MemoryQueue(),send:typeof fetch=async()=>new Response('',{status:503})){
  const c=new Analytics({endpoint:'https://example.invalid/v1/batch',projectId:'project-a',writeKey:'test',consent:true,queue,fetch:send,flushInterval:60000});clients.push(c);return c;
}
describe('persistent and bounded delivery',()=>{
  it('leases across browser instances and recovers after expiry without deleting a newer lease',async()=>{
    const name='test-'+crypto.randomUUID(),a=new IndexedQueue(name),b=new IndexedQueue(name);const e=event();
    expect(await a.add(e,10,10000)).toBe(true);
    expect((await a.claim(1000,20,10000,'tab-a')).length).toBe(1);
    expect((await b.claim(1001,20,10000,'tab-b')).length).toBe(0);
    expect((await b.claim(32000,20,10000,'tab-b')).length).toBe(1);
    await a.finish([e.event_id],'tab-a');expect((await a.stats()).count).toBe(1);
    await b.finish([e.event_id],'tab-b');expect((await a.stats()).count).toBe(0);
  });
  it('atomically enforces capacity with simultaneous tabs',async()=>{
    const name='cap-'+crypto.randomUUID(),a=new IndexedQueue(name),b=new IndexedQueue(name);
    const results=await Promise.all([a.add(event(),1,10000),b.add(event(),1,10000)]);
    expect(results.filter(Boolean)).toHaveLength(1);expect((await a.stats()).count).toBe(1);
  });
  it('snapshots caller data and identity, deletes only after exact acknowledgement',async()=>{
    let sent:AnalyticsEvent[]=[];
    const c=client(new MemoryQueue(),async(_u,init)=>{sent=JSON.parse(init!.body as string).events;return Response.json({accepted_event_ids:sent.map(e=>e.event_id)},{status:202});});
    await c.flush();const props={button:'buy'};const p=c.track('click',props);props.button='changed';await p;await c.identify('user-a');
    await c.flush();expect(sent[0]!.properties.button).toBe('buy');expect(sent[0]!.distinct_id).not.toBe('user-a');expect(sent[1]!.distinct_id).toBe('user-a');expect((await c.getQueueStatus()).count).toBe(0);
  });
  it('retains records on invalid acknowledgements and unknown transport outcomes',async()=>{
    const c=client(new MemoryQueue(),async()=>Response.json({accepted_event_ids:[crypto.randomUUID()]},{status:202}));await c.flush();const queued=await c.track('click');await c.flush();
    expect(queued.status).toBe('queued-memory');expect((await c.getQueueStatus()).count).toBe(1);
  });
  it('honors Retry-After even if flush is called repeatedly',async()=>{
    const send=vi.fn<typeof fetch>(async()=>new Response('',{status:429,headers:{'retry-after':'60'}}));const c=client(new MemoryQueue(),send);await c.flush();await c.track('click');await c.flush();await c.flush();expect(send).toHaveBeenCalledTimes(1);
  });
  it('does not collect without consent and clears queues on withdrawal',async()=>{
    const c=client();await c.track('click');await c.setConsent(false);expect((await c.getQueueStatus()).count).toBe(0);expect((await c.track('click')).reason).toBe('consent_required');
  });
  it('does not delete events just because the browser accepted a beacon',async()=>{
    const c=client();await c.flush();await c.track('click');const sendBeacon=vi.fn(()=>true);vi.stubGlobal('navigator',{sendBeacon});
    expect(c.beacon()).toBe(true);expect(c.beacon()).toBe(false);expect(sendBeacon).toHaveBeenCalledTimes(1);expect((await c.getQueueStatus()).count).toBe(1);
  });
  it('restores persisted IDs after reopening and confirms them without generating replacement IDs',async()=>{
    const name='reopen-'+crypto.randomUUID(),first=client(new IndexedQueue(name));await first.flush();const r=await first.track('click');first.destroy();
    const seen:string[]=[];const second=client(new IndexedQueue(name),async(_url,init)=>{const ids=JSON.parse(init!.body as string).events.map((e:AnalyticsEvent)=>e.event_id);seen.push(...ids);return Response.json({accepted_event_ids:ids},{status:202});});
    await second.flush();expect(seen).toEqual([r.event_id]);expect((await second.getQueueStatus()).count).toBe(0);
  });
});
