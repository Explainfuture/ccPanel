import {afterEach,expect,it,vi} from 'vitest';
import {createNodeAnalytics} from '../packages/sdk-node/src/index';
const base={endpoint:'https://example.invalid/v1/batch',projectId:'shutdown-test',writeKey:'synthetic',consent:true,flushInterval:60000};
afterEach(()=>{vi.useRealTimers();vi.restoreAllMocks();});
it('stops new events immediately and shares a successful shutdown result',async()=>{
 const client=createNodeAnalytics({...base,fetch:async(_url,init)=>{const body=JSON.parse(init!.body as string);return Response.json({accepted_event_ids:body.events.map((e:{event_id:string})=>e.event_id)},{status:202});}});
 try{await client.flush();await client.track('before');const result=client.shutdown();expect(client.shutdown()).toBe(result);expect(await client.track('after')).toEqual({status:'rejected',reason:'shutdown'});expect(await client.trackFor('A','after')).toEqual({status:'rejected',reason:'shutdown'});await result;expect((await client.getQueueStatus()).count).toBe(0);}finally{client.destroy();}
});
it('rejects after ten seconds without inventing confirmation or restarting an ignored in-flight fetch',async()=>{
 vi.useFakeTimers();let release!:(r:Response)=>void;const gate=new Promise<Response>(resolve=>{release=resolve;}),send=vi.fn<typeof fetch>(async()=>gate),onError=vi.fn(()=>{throw Error('diagnostic callback');}),client=createNodeAnalytics({...base,fetch:send,onError});
 try{await client.flush();const event=await client.trackFor('A','pending');const shutdown=client.shutdown(),failure=shutdown.catch(e=>e);await vi.advanceTimersByTimeAsync(10000);expect((await failure).message).toBe('shutdown_timeout');expect(onError).toHaveBeenCalledWith('shutdown_timeout');expect((await client.getQueueStatus()).count).toBe(1);expect(client.shutdown()).toBe(shutdown);expect(await client.track('after')).toEqual({status:'rejected',reason:'shutdown'});expect(send).toHaveBeenCalledTimes(1);
 release(Response.json({accepted_event_ids:[event.event_id]},{status:202}));await client.flush();expect((await client.getQueueStatus()).count).toBe(0);expect(send).toHaveBeenCalledTimes(1);
 }finally{release(new Response('',{status:503}));client.destroy();}
});
import {FileQueue} from '../packages/sdk-node/src/index';
import {mkdtemp,unlink,rmdir} from 'node:fs/promises';
import {existsSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve,dirname} from 'node:path';
it('bounds slow file close while keeping the real outbox lock until close finishes',async()=>{
 vi.useFakeTimers();const dir=await mkdtemp(join(tmpdir(),'ccpanel-shutdown-')),path=join(dir,'outbox.json'),original=FileQueue.prototype.close;
 let release!:()=>void,entered!:()=>void,background:Promise<void>|undefined;const gate=new Promise<void>(r=>{release=r;}),entry=new Promise<void>(r=>{entered=r;});
 const spy=vi.spyOn(FileQueue.prototype,'close').mockImplementation(function(this:FileQueue){entered();return background=gate.then(()=>original.call(this));});
 const client=createNodeAnalytics({...base,outboxPath:path,fetch:async()=>new Response('',{status:503})});
 try{await client.flush();const shutdown=client.shutdown(),failure=shutdown.catch(e=>e);await entry;expect(existsSync(path+'.lock')).toBe(true);await vi.advanceTimersByTimeAsync(10000);expect((await failure).message).toBe('shutdown_timeout');expect(existsSync(path+'.lock')).toBe(true);release();await background;expect(existsSync(path+'.lock')).toBe(false);}
 finally{release();await background;client.destroy();spy.mockRestore();if(dirname(resolve(dir))!==resolve(tmpdir())||!dir.includes('ccpanel-shutdown-'))throw Error('unsafe_cleanup');for(const name of ['outbox.json','outbox.json.tmp'])try{await unlink(join(dir,name));}catch(e){if((e as NodeJS.ErrnoException).code!=='ENOENT')throw e;}await rmdir(dir);}
});
