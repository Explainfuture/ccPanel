import 'fake-indexeddb/auto';
import{expect,it}from'vitest';
import{Analytics}from'../packages/sdk-browser/src/index';
import{IndexedQueue}from'../packages/sdk-browser/src/indexed-queue';
it('migrates bounded memory events back to durable storage with unchanged IDs and retry state',async()=>{
  class FlakyQueue extends IndexedQueue{
    fail=true;
    override async add(...args:Parameters<IndexedQueue['add']>){if(this.fail)throw new Error('temporary storage failure');return super.add(...args);}
  }
  const q=new FlakyQueue('recovery-'+crypto.randomUUID());let sent:string[]=[];
  const client=new Analytics({endpoint:'https://example.invalid/v1/batch',projectId:'p',writeKey:'k',consent:true,queue:q,flushInterval:60000,fetch:async(_u,init)=>{sent=JSON.parse(init!.body as string).events.map((e:{event_id:string})=>e.event_id);return Response.json({accepted_event_ids:sent},{status:202});}});
  try{await client.flush();const row=await client.track('click');expect(row.status).toBe('queued-memory');q.fail=false;await client.flush();expect(sent).toEqual([row.event_id]);expect((await client.getQueueStatus()).count).toBe(0);expect((await q.stats()).count).toBe(0);}finally{client.destroy();}
});
it('serializes consent withdrawal and regrant without retaining old records',async()=>{
  const q=new IndexedQueue('consent-race-'+crypto.randomUUID());
  const c=new Analytics({endpoint:'https://example.invalid/v1/batch',projectId:'p',writeKey:'k',consent:true,queue:q,flushInterval:60000,fetch:async()=>new Response('',{status:503})});
  try{await c.flush();await c.track('old_click');const withdraw=c.setConsent(false),grant=c.setConsent(true);await Promise.all([withdraw,grant]);expect((await c.getQueueStatus()).count).toBe(0);expect((await c.track('new_click')).status).toBe('queued-persistent');}finally{c.destroy();}
});
