import { byteLength, type AnalyticsEvent } from '../../schema/src/index';
export interface QueueRow { event: AnalyticsEvent; bytes: number; created: number; next: number; attempts: number; lease?: string; leaseUntil?: number }
export interface QueueStats { count: number; bytes: number; oldest: number | null }
export interface Queue {
  readonly persistent: boolean;
  add(event: AnalyticsEvent, maxCount: number, maxBytes: number): Promise<boolean>;
  claim(now: number, count: number, maxBytes: number, owner: string): Promise<QueueRow[]>;
  finish(ids: string[], owner: string, retryAt?: number): Promise<void>;
  peek(count: number): Promise<QueueRow[]>;
  stats(): Promise<QueueStats>;
  expire(before: number): Promise<number>;
  clear(): Promise<void>;
}
export class MemoryQueue implements Queue {
  readonly persistent:boolean=false;
  protected rows=new Map<string,QueueRow>();
  async add(event:AnalyticsEvent,maxCount:number,maxBytes:number){
    const bytes=byteLength(event);let currentBytes=0;for(const row of this.rows.values())currentBytes+=row.bytes;
    if(this.rows.size>=maxCount||currentBytes+bytes>maxBytes)return false;
    this.rows.set(event.event_id,{event,bytes,created:Date.now(),next:0,attempts:0});return true;
  }
  async claim(now:number,count:number,maxBytes:number,owner:string){
    const result:QueueRow[]=[];let size=0;
    for(const row of this.rows.values()){
      if(row.next>now||(row.leaseUntil||0)>now)continue;
      if(result.length>=count||size+row.bytes>maxBytes)break;
      row.lease=owner;row.leaseUntil=now+30000;result.push(structuredClone(row));size+=row.bytes;
    }return result;
  }
  async finish(ids:string[],owner:string,retryAt?:number){for(const id of ids){const row=this.rows.get(id);if(!row||(owner&&row.lease!==owner))continue;if(retryAt===undefined)this.rows.delete(id);else{row.lease=undefined;row.leaseUntil=undefined;row.next=retryAt;row.attempts++;}}}
  async peek(count:number){return Array.from(this.rows.values()).slice(0,count).map(r=>structuredClone(r));}
  async stats(){let bytes=0,oldest:number|null=null;for(const r of this.rows.values()){bytes+=r.bytes;oldest=oldest===null?r.created:Math.min(oldest,r.created);}return{count:this.rows.size,bytes,oldest};}
  async expire(before:number){let count=0;for(const [k,r]of this.rows)if(r.created<before){this.rows.delete(k);count++;}return count;}
  async clear(){this.rows.clear();}
}
const request=<T>(r:IDBRequest<T>)=>new Promise<T>((resolve,reject)=>{r.onsuccess=()=>resolve(r.result);r.onerror=()=>reject(r.error);});
const complete=(tx:IDBTransaction)=>new Promise<void>((resolve,reject)=>{tx.oncomplete=()=>resolve();tx.onabort=()=>reject(tx.error||new Error('idb_aborted'));tx.onerror=()=>reject(tx.error);});
export class IndexedQueue implements Queue {
  readonly persistent=true;
  private db:Promise<IDBDatabase>;
  constructor(name:string,factory:IDBFactory=globalThis.indexedDB){
    this.db=new Promise((resolve,reject)=>{
      const r=factory.open(name,1);
      r.onupgradeneeded=()=>{const s=r.result.createObjectStore('events',{keyPath:'event.event_id'});s.createIndex('ready','next');s.createIndex('created','created');r.result.createObjectStore('meta');};
      r.onsuccess=()=>{r.result.onversionchange=()=>r.result.close();resolve(r.result);};r.onerror=()=>reject(r.error);r.onblocked=()=>reject(new Error('idb_blocked'));
    });
  }
  private async transaction(mode:IDBTransactionMode){const db=await this.db;const tx=db.transaction(['events','meta'],mode);return{tx,done:complete(tx),store:tx.objectStore('events'),meta:tx.objectStore('meta')};}
  async add(event:AnalyticsEvent,maxCount:number,maxBytes:number){
    const {done,store,meta}=await this.transaction('readwrite');
    const stats=(await request(meta.get('stats'))||{count:0,bytes:0}) as {count:number;bytes:number};const bytes=byteLength(event);
    if(stats.count>=maxCount||stats.bytes+bytes>maxBytes){await done;return false;}
    store.add({event,bytes,created:Date.now(),next:0,attempts:0} satisfies QueueRow);
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
