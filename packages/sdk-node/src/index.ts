import { mkdir,open,rename,readFile,unlink } from 'node:fs/promises';
import { dirname,resolve } from 'node:path';
import { Analytics,type AnalyticsOptions,type EventMap,type TypedAnalytics } from '../../sdk-browser/src/index';
import { MemoryQueue,type QueueRow,type RestoredRow } from '../../sdk-browser/src/queue';
import { validateEvent,type AnalyticsEvent } from '../../schema/src/index';
export class FileQueue extends MemoryQueue {
  override readonly persistent=true;
  private path:string;private ready:Promise<void>|undefined;private chain:Promise<unknown>=Promise.resolve();
  private lockPath:string;private ownsLock=false;private closed=false;private closing:Promise<void>|undefined;
  constructor(path:string){super();this.path=resolve(path);this.lockPath=this.path+'.lock';}
  private async acquireLock(){
    const lock=await open(this.lockPath,'wx',0o600);this.ownsLock=true;
    try{await lock.writeFile(String(process.pid));await lock.sync();}finally{await lock.close();}
  }
  private async releaseLock(){if(this.ownsLock){await unlink(this.lockPath);this.ownsLock=false;}}
  private async load(){
    try{await this.loadRows();}catch(error){await this.releaseLock();throw error;}
  }
  private async loadRows(){
    await mkdir(dirname(this.path),{recursive:true,mode:0o700});
    try{await this.acquireLock();}
    catch(e){
      if((e as NodeJS.ErrnoException).code!=='EEXIST')throw e;
      // Serialize dead-owner reclamation. A crashed reclaimer intentionally
      // leaves this separate marker for operator inspection, rather than let
      // two restarters unlink a newly acquired live owner's lock.
      let guard;
      try{guard=await open(this.lockPath+'.reclaim','wx',0o600);}
      catch(error){if((error as NodeJS.ErrnoException).code==='EEXIST')throw new Error('outbox_recovery_locked');throw error;}
      try{
        await guard.writeFile(String(process.pid));await guard.sync();
        let pid:number|undefined;
        try{pid=Number(await readFile(this.lockPath,'utf8'));}catch(error){if((error as NodeJS.ErrnoException).code!=='ENOENT')throw error;}
        if(pid!==undefined){
          if(!Number.isInteger(pid)||pid<=0)throw new Error('invalid_outbox_lock');
          let alive=true;try{process.kill(pid,0);}catch(error){if((error as NodeJS.ErrnoException).code==='ESRCH')alive=false;}
          if(alive)throw new Error('outbox_already_open');
          await unlink(this.lockPath);
        }
        await this.acquireLock();
      }finally{await guard.close();await unlink(this.lockPath+'.reclaim');}
    }
    try{
      const data=JSON.parse(await readFile(this.path,'utf8')) as {version:number;rows:QueueRow[]};
      if(data.version!==1||!Array.isArray(data.rows))throw new Error('invalid_outbox');
      for(const row of data.rows){if(validateEvent(row.event,Date.now(),false)||!Number.isFinite(row.created)||!Number.isFinite(row.bytes)||!Number.isFinite(row.next)||!Number.isInteger(row.attempts))throw new Error('corrupt_outbox');this.rows.set(row.event.event_id,row);}
    }catch(e){if((e as NodeJS.ErrnoException).code!=='ENOENT')throw e;}
  }
  private async save(){
    const temp=this.path+'.tmp';const f=await open(temp,'w',0o600);
    try{await f.writeFile(JSON.stringify({version:1,rows:[...this.rows.values()]}));await f.sync();}finally{await f.close();}
    await rename(temp,this.path);
    if(process.platform!=='win32'){const dir=await open(dirname(this.path),'r');try{await dir.sync();}finally{await dir.close();}}
  }
  private serial<T>(fn:()=>Promise<T>,mutate=false):Promise<T>{
    if(this.closed)return Promise.reject(new Error('outbox_closed'));
    const task=this.chain.then(async()=>{await(this.ready??=this.load());const old=structuredClone(this.rows);try{const result=await fn();if(mutate)await this.save();return result;}catch(e){this.rows=old;throw e;}});
    this.chain=task.catch(()=>{});return task;
  }
  override add(e:AnalyticsEvent,count:number,bytes:number,restore?:RestoredRow){return this.serial(()=>super.add(e,count,bytes,restore),true);}
  override claim(now:number,count:number,bytes:number,owner:string){return this.serial(()=>super.claim(now,count,bytes,owner),true);}
  override finish(ids:string[],owner:string,retryAt?:number){return this.serial(()=>super.finish(ids,owner,retryAt),true);}
  override peek(count:number){return this.serial(()=>super.peek(count));}
  override stats(){return this.serial(()=>super.stats());}
  override expire(before:number){return this.serial(()=>super.expire(before),true);}
  override clear(){return this.serial(()=>super.clear(),true);}
  close(){
    if(this.closing)return this.closing;
    this.closed=true;
    return this.closing=(async()=>{await this.chain;try{await this.ready;}finally{await this.releaseLock();}})();
  }
}
export type NodeOptions=Omit<AnalyticsOptions,'source'|'queue'>&{outboxPath?:string};
export function createNodeAnalytics(options:NodeOptions):Analytics&{shutdown():Promise<void>};
export function createNodeAnalytics<Events extends EventMap>(options:NodeOptions):TypedAnalytics<Events>&{shutdown():Promise<void>};
export function createNodeAnalytics(options:NodeOptions):unknown{
  const queue=options.outboxPath?new FileQueue(options.outboxPath):new MemoryQueue();
  const analytics=new Analytics({...options,queue,source:'node'});
  return Object.assign(analytics,{async shutdown(){try{await analytics.flush();}finally{analytics.destroy();if(queue instanceof FileQueue)await queue.close();}}});
}
