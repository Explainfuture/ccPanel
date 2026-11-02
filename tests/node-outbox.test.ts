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

it('releases a failed open without erasing the corrupt source or another owner lock',async()=>{
  const {writeFile,readFile,access}=await import('node:fs/promises');
  const dir=await mktemp();
  try{
    const path=join(dir,'outbox.json');await writeFile(path,'broken-json');
    const queue=new FileQueue(path);await expect(queue.stats()).rejects.toThrow();
    await expect(access(path+'.lock')).rejects.toMatchObject({code:'ENOENT'});
    expect(await readFile(path,'utf8')).toBe('broken-json');
    await expect(queue.close()).rejects.toThrow();
    await writeFile(path,JSON.stringify({version:1,rows:[]}));
    const owner=new FileQueue(path);expect((await owner.stats()).count).toBe(0);
    const competing=new FileQueue(path);await expect(competing.stats()).rejects.toThrow('outbox_already_open');
    await expect(competing.close()).rejects.toThrow('outbox_already_open');
    expect(await readFile(path+'.lock','utf8')).toBe(String(process.pid));
    await owner.close();await owner.close();
    await expect(owner.stats()).rejects.toThrow('outbox_closed');
  }finally{await rm(dir,{recursive:true,force:true});}
});
it('invalid SDK configuration never opens the file queue',async()=>{
  const {access}=await import('node:fs/promises');
  const {createNodeAnalytics}=await import('../packages/sdk-node/src/index');
  const dir=await mktemp();
  try{
    const path=join(dir,'outbox.json');
    expect(()=>createNodeAnalytics({endpoint:'http://invalid.example',projectId:'project',writeKey:'key',consent:true,outboxPath:path})).toThrow('HTTPS');
    await new Promise(r=>setTimeout(r,20));
    await expect(access(path+'.lock')).rejects.toMatchObject({code:'ENOENT'});
    const unused=new FileQueue(path);await unused.close();
    await expect(access(path+'.lock')).rejects.toMatchObject({code:'ENOENT'});
  }finally{await rm(dir,{recursive:true,force:true});}
});
it('close drains already queued persistence and refuses subsequent writes',async()=>{
  const dir=await mktemp();
  try{
    const path=join(dir,'outbox.json'),queue=new FileQueue(path);
    const event={event_id:crypto.randomUUID(),event_name:'view',distinct_id:'user',event_time:new Date().toISOString(),properties:{},context:{},schema_version:1 as const};
    const write=queue.add(event,1000,100000),closed=queue.close();
    await expect(queue.add({...event,event_id:crypto.randomUUID()},1000,100000)).rejects.toThrow('outbox_closed');
    expect(await write).toBe(true);await closed;
    const reopened=new FileQueue(path);expect((await reopened.peek(10)).map(r=>r.event.event_id)).toEqual([event.event_id]);await reopened.close();
  }finally{await rm(dir,{recursive:true,force:true});}
});
const mktemp=()=>mkdtemp(join(tmpdir(),'ccpanel-'));
it('serializes stale-owner recovery and never removes another reclaim marker',async()=>{
  const {writeFile,readFile,access}=await import('node:fs/promises');
  const {vi}=await import('vitest');
  const dir=await mktemp(),dead=2147483000;
  const kill=vi.spyOn(process,'kill').mockImplementation(((pid:number)=>{if(pid===dead)throw Object.assign(new Error('dead'),{code:'ESRCH'});return true;}) as typeof process.kill);
  try{
    const path=join(dir,'outbox.json');await writeFile(path+'.lock',String(dead));await writeFile(path+'.lock.reclaim','operator-inspection');
    const blocked=new FileQueue(path);await expect(blocked.stats()).rejects.toThrow('outbox_recovery_locked');
    expect(await readFile(path+'.lock','utf8')).toBe(String(dead));expect(await readFile(path+'.lock.reclaim','utf8')).toBe('operator-inspection');
    await expect(blocked.close()).rejects.toThrow('outbox_recovery_locked');await rm(path+'.lock.reclaim');
    const a=new FileQueue(path),b=new FileQueue(path),results=await Promise.allSettled([a.stats(),b.stats()]);
    expect(results.filter(r=>r.status==='fulfilled')).toHaveLength(1);
    expect(await readFile(path+'.lock','utf8')).toBe(String(process.pid));
    await Promise.allSettled([a.close(),b.close()]);
    await expect(access(path+'.lock')).rejects.toMatchObject({code:'ENOENT'});
    await expect(access(path+'.lock.reclaim')).rejects.toMatchObject({code:'ENOENT'});
  }finally{kill.mockRestore();await rm(dir,{recursive:true,force:true});}
});
