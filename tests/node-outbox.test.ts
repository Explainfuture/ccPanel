import { mkdtemp,rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { expect,it } from 'vitest';
import { FileQueue } from '../packages/sdk-node/src/index';
it('recovers committed file events and prevents concurrent writers',async()=>{
  const dir=await mkdtemp(join(tmpdir(),'ccpanel-'));
  try{
    const path=join(dir,'outbox.json'),a=new FileQueue(path);const event={event_id:crypto.randomUUID(),event_name:'order_completed',distinct_id:'user',event_time:new Date().toISOString(),properties:{},context:{},schema_version:1 as const};
    await a.add(event,1000,100000);const competing=new FileQueue(path);await expect(competing.stats()).rejects.toThrow('outbox_already_open');await a.close();
    const b=new FileQueue(path);expect((await b.peek(10))[0]!.event.event_id).toBe(event.event_id);const claimed=await b.claim(Date.now(),20,100000,'owner');await b.finish(claimed.map(r=>r.event.event_id),'owner');await b.close();
    const c=new FileQueue(path);expect((await c.stats()).count).toBe(0);await c.close();
  }finally{await rm(dir,{recursive:true,force:true});}
});
