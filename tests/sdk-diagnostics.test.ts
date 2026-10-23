import 'fake-indexeddb/auto';
import {afterEach,expect,it,vi} from 'vitest';
import {Analytics} from '../packages/sdk-browser/src/index';
import {MemoryQueue} from '../packages/sdk-browser/src/queue';
import {IndexedQueue} from '../packages/sdk-browser/src/indexed-queue';
import {limits} from '../packages/schema/src/index';
afterEach(()=>vi.restoreAllMocks());
const options={endpoint:'https://example.invalid/v1/batch',projectId:'diagnostics',writeKey:'test',consent:true,flushInterval:60000};
for(const persistent of [false,true])it(`counts each expired record once without sending it (${persistent?'indexed':'memory'})`,async()=>{
  let now=Date.now();vi.spyOn(Date,'now').mockImplementation(()=>now);
  const queue=persistent?new IndexedQueue('expiry-'+crypto.randomUUID()):new MemoryQueue(),send=vi.fn<typeof fetch>(async()=>new Response('',{status:503})),errors:string[]=[];
  const c=new Analytics({...options,queue,fetch:send,onError:code=>{errors.push(code);throw Error('diagnostic callback cannot break delivery');}});
  try{
    await c.flush();await c.track('first');await c.track('second');
    now+=limits.ttl+1;await c.flush();await c.flush();
    const status=await c.getQueueStatus();expect(status.count).toBe(0);expect(status.bytes).toBe(0);expect(status.counters).toEqual({expired:2,rejected_overflow:0,rejected_pending_overflow:0,storage_error:0});expect(errors).toEqual(['expired:2']);expect(send).not.toHaveBeenCalled();
    status.counters.expired=999;expect((await c.getQueueStatus()).counters.expired).toBe(2);
    await c.setConsent(false);await c.setConsent(true);expect((await c.getQueueStatus()).counters.expired).toBe(2);
  }finally{c.destroy();}
});
it('exposes a storage failure while retaining the event in bounded memory fallback',async()=>{
  class FailingQueue extends MemoryQueue{override readonly persistent=true;override async add():Promise<boolean>{throw Error('disk unavailable');}}
  const errors:string[]=[],c=new Analytics({...options,queue:new FailingQueue(),fetch:async()=>new Response('',{status:503}),onError:code=>errors.push(code)});
  try{await c.flush();expect((await c.track('retained')).status).toBe('queued-memory');const status=await c.getQueueStatus();expect(status.count).toBe(1);expect(status.storage).toBe('memory-fallback');expect(status.counters.storage_error).toBe(1);expect(errors).toEqual(['storage_error']);}finally{c.destroy();}
});
