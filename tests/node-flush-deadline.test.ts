import {afterEach,expect,it,vi} from 'vitest';
import {createNodeAnalytics} from '../packages/sdk-node/src/index';
import {MemoryQueue} from '../packages/sdk-browser/src/queue';
const base={endpoint:'https://example.invalid/v1/batch',projectId:'flush-deadline',writeKey:'synthetic',consent:true,flushInterval:60000};
afterEach(()=>{vi.useRealTimers();vi.restoreAllMocks();});
it('bounds the Node flush view but keeps one actual in-flight event until its real acknowledgement',async()=>{
 vi.useFakeTimers();let release!:(r:Response)=>void;const gate=new Promise<Response>(r=>{release=r;}),send=vi.fn<typeof fetch>(async()=>gate),errors:string[]=[],c=createNodeAnalytics({...base,fetch:send,onError:code=>errors.push(code)});
 try{await c.flush();const event=await c.track('pending'),view=c.flush(),failure=view.catch(e=>e);expect(c.flush()).toBe(view);await vi.advanceTimersByTimeAsync(10000);expect((await failure).message).toBe('flush_timeout');expect(errors).toContain('flush_timeout');expect((await c.getQueueStatus()).count).toBe(1);expect(c.flush()).toBe(view);await vi.advanceTimersByTimeAsync(10000);expect(send).toHaveBeenCalledTimes(1);
 release(Response.json({accepted_event_ids:[event.event_id]},{status:202}));await vi.advanceTimersByTimeAsync(0);expect((await c.getQueueStatus()).count).toBe(0);expect(await c.flush()).toEqual({confirmed:0,pending:0});expect(send).toHaveBeenCalledTimes(1);
 }finally{release(new Response('',{status:503}));c.destroy();}
});
it('bounds unknown storage reads without requiring a fetch or generating unhandled initialization errors',async()=>{
 vi.useFakeTimers();let release!:()=>void,entered!:()=>void;const gate=new Promise<void>(r=>{release=r;}),entry=new Promise<void>(r=>{entered=r;}),original=MemoryQueue.prototype.stats;
 vi.spyOn(MemoryQueue.prototype,'stats').mockImplementation(function(this:MemoryQueue){entered();return gate.then(()=>original.call(this));});
 const send=vi.fn<typeof fetch>(async()=>new Response('',{status:503})),errors:string[]=[],c=createNodeAnalytics({...base,fetch:send,onError:code=>errors.push(code)});
 try{const view=c.flush(),failure=view.catch(e=>e);await entry;await vi.advanceTimersByTimeAsync(10000);expect((await failure).message).toBe('flush_timeout');expect(errors).toEqual(['flush_timeout']);expect(send).not.toHaveBeenCalled();expect(c.flush()).toBe(view);release();await vi.advanceTimersByTimeAsync(0);expect(await c.flush()).toEqual({confirmed:0,pending:0});}finally{release();c.destroy();}
});
