import {byteLength,type AnalyticsEvent} from '../../schema/src/index.js';
import type {Queue,QueueRow,QueueStats,RestoredRow} from './queue.js';
const request=<T>(r:IDBRequest<T>)=>new Promise<T>((resolve,reject)=>{r.onsuccess=()=>resolve(r.result);r.onerror=()=>reject(r.error);});
const complete=(tx:IDBTransaction)=>new Promise<void>((resolve,reject)=>{tx.oncomplete=()=>resolve();tx.onabort=()=>reject(tx.error||new Error('idb_aborted'));tx.onerror=()=>reject(tx.error);});
export class IndexedQueue implements Queue {
  readonly persistent=true;
  private db:Promise<IDBDatabase>;
  constructor(private name:string,private factory:IDBFactory=globalThis.indexedDB){
    this.db=this.open();
  }
  private open(){
    const name=this.name,factory=this.factory;
    return new Promise<IDBDatabase>((resolve,reject)=>{
      const r=factory.open(name,1);
      r.onupgradeneeded=()=>{const s=r.result.createObjectStore('events',{keyPath:'event.event_id'});s.createIndex('ready','next');s.createIndex('created','created');r.result.createObjectStore('meta');};
      r.onsuccess=()=>{r.result.onversionchange=()=>r.result.close();resolve(r.result);};r.onerror=()=>reject(r.error);r.onblocked=()=>reject(new Error('idb_blocked'));
    });
  }
  async recover(){try{(await this.db).close();}catch{/* A rejected open is retried on the next bounded recovery probe. */}this.db=this.open();await this.db;}
  private async transaction(mode:IDBTransactionMode){const db=await this.db;const tx=db.transaction(['events','meta'],mode);return{tx,done:complete(tx),store:tx.objectStore('events'),meta:tx.objectStore('meta')};}
  async add(event:AnalyticsEvent,maxCount:number,maxBytes:number,restore?:RestoredRow){
    const {done,store,meta}=await this.transaction('readwrite');
    const stats=(await request(meta.get('stats'))||{count:0,bytes:0}) as {count:number;bytes:number};const bytes=byteLength(event);
    const existing=await request(store.get(event.event_id)) as QueueRow|undefined;if(existing){await done;return JSON.stringify(existing.event)===JSON.stringify(event);}
    if(stats.count>=maxCount||stats.bytes+bytes>maxBytes){await done;return false;}
    store.add({event,bytes,created:restore?.created??Date.now(),next:restore?.next??0,attempts:restore?.attempts??0} satisfies QueueRow);
    meta.put({count:stats.count+1,bytes:stats.bytes+bytes},'stats');await done;return true;
  }
  async claim(now:number,count:number,maxBytes:number,owner:string){
    const {done,store}=await this.transaction('readwrite');const rows:QueueRow[]=[];let size=0;
    await new Promise<void>((resolve,reject)=>{const r=store.index('ready').openCursor(IDBKeyRange.upperBound(now));r.onerror=()=>reject(r.error);r.onsuccess=()=>{
      const cursor=r.result;if(!cursor||rows.length>=count){resolve();return;}const row=cursor.value as QueueRow;
      if((row.leaseUntil||0)>now){cursor.continue();return;}
      if(size+row.bytes>maxBytes){resolve();return;}
      row.lease=owner;row.leaseUntil=now+30000;cursor.update(row);rows.push(row);size+=row.bytes;cursor.continue();
    };});await done;return rows;
  }
  async finish(ids:string[],owner:string,retryAt?:number){
    const {done,store,meta}=await this.transaction('readwrite');const stats=(await request(meta.get('stats'))||{count:0,bytes:0}) as {count:number;bytes:number};
    for(const id of ids){const row=await request(store.get(id)) as QueueRow|undefined;if(!row||(owner&&row.lease!==owner))continue;
      if(retryAt===undefined){store.delete(id);stats.count--;stats.bytes-=row.bytes;}else{row.next=retryAt;row.attempts++;delete row.lease;delete row.leaseUntil;store.put(row);}}
    meta.put(stats,'stats');await done;
  }
  async peek(count:number){const{done,store}=await this.transaction('readonly');const rows=await request(store.getAll(undefined,count)) as QueueRow[];await done;return rows;}
  async stats(){const{done,meta,store}=await this.transaction('readonly');const m=await request(meta.get('stats'))||{count:0,bytes:0};const oldest=await request(store.index('created').openCursor());await done;return{...m,oldest:oldest?(oldest.value as QueueRow).created:null} as QueueStats;}
  async expire(before:number){
    const{done,store,meta}=await this.transaction('readwrite');const stats=await request(meta.get('stats'))||{count:0,bytes:0};let count=0;
    await new Promise<void>((resolve,reject)=>{const r=store.index('created').openCursor(IDBKeyRange.upperBound(before,true));r.onerror=()=>reject(r.error);r.onsuccess=()=>{const c=r.result;if(!c){resolve();return;}stats.count--;stats.bytes-=(c.value as QueueRow).bytes;c.delete();count++;c.continue();};});
    meta.put(stats,'stats');await done;return count;
  }
  async clear(){const{done,store,meta}=await this.transaction('readwrite');store.clear();meta.clear();await done;}
}
