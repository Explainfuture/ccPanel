import { mkdir,open,rename,readFile,unlink } from 'node:fs/promises';
import { dirname,resolve } from 'node:path';
import { Analytics,type AnalyticsOptions } from '../../sdk-browser/src/index';
import { MemoryQueue,type QueueRow,type RestoredRow } from '../../sdk-browser/src/queue';
import { validateEvent,type AnalyticsEvent } from '../../schema/src/index';
export class FileQueue extends MemoryQueue {
  override readonly persistent=true;
  private path:string;private ready:Promise<void>;private chain:Promise<unknown>=Promise.resolve();
  private lockPath:string;
  constructor(path:string){super();this.path=resolve(path);this.lockPath=this.path+'.lock';this.ready=this.load();}
  private async load(){
    await mkdir(dirname(this.path),{recursive:true,mode:0o700});
    try{const lock=await open(this.lockPath,'wx',0o600);await lock.writeFile(String(process.pid));await lock.close();}
    catch(e){
      if((e as NodeJS.ErrnoException).code!=='EEXIST')throw e;
      const pid=Number(await readFile(this.lockPath,'utf8'));let alive=true;
      if(!Number.isInteger(pid)||pid<=0)throw new Error('invalid_outbox_lock');
      try{process.kill(pid,0);}catch(err){if((err as NodeJS.ErrnoException).code==='ESRCH')alive=false;}
      if(alive)throw new Error('outbox_already_open');
      // Reclaim a crashed process lock with exclusive creation; never share an outbox across processes.
      await unlink(this.lockPath);const lock=await open(this.lockPath,'wx',0o600);await lock.writeFile(String(process.pid));await lock.close();
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
    const task=this.chain.then(async()=>{await this.ready;const old=structuredClone(this.rows);try{const result=await fn();if(mutate)await this.save();return result;}catch(e){this.rows=old;throw e;}});
    this.chain=task.catch(()=>{});return task;
  }
  override add(e:AnalyticsEvent,count:number,bytes:number,restore?:RestoredRow){return this.serial(()=>super.add(e,count,bytes,restore),true);}
  override claim(now:number,count:number,bytes:number,owner:string){return this.serial(()=>super.claim(now,count,bytes,owner),true);}
  override finish(ids:string[],owner:string,retryAt?:number){return this.serial(()=>super.finish(ids,owner,retryAt),true);}
  override peek(count:number){return this.serial(()=>super.peek(count));}
  override stats(){return this.serial(()=>super.stats());}
  override expire(before:number){return this.serial(()=>super.expire(before),true);}
  override clear(){return this.serial(()=>super.clear(),true);}
  async close(){await this.chain;await this.ready;await unlink(this.lockPath);}
}
export function createNodeAnalytics(options:Omit<AnalyticsOptions,'source'|'queue'>&{outboxPath?:string}){
  const queue=options.outboxPath?new FileQueue(options.outboxPath):new MemoryQueue();
  const analytics=new Analytics({...options,queue,source:'node'});
  return Object.assign(analytics,{async shutdown(){await analytics.flush();analytics.destroy();if(queue instanceof FileQueue)await queue.close();}});
}
