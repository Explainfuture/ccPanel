import { byteLength, type AnalyticsEvent } from '../../schema/src/index.js';
export interface QueueRow { event: AnalyticsEvent; bytes: number; created: number; next: number; attempts: number; lease?: string; leaseUntil?: number }
export interface QueueStats { count: number; bytes: number; oldest: number | null }
export type RestoredRow=Pick<QueueRow,'created'|'next'|'attempts'>;
export interface Queue {
  readonly persistent: boolean;
  add(event: AnalyticsEvent, maxCount: number, maxBytes: number,restore?:RestoredRow): Promise<boolean>;
  recover?():Promise<void>;
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
  async add(event:AnalyticsEvent,maxCount:number,maxBytes:number,restore?:RestoredRow){
    const existing=this.rows.get(event.event_id);if(existing)return JSON.stringify(existing.event)===JSON.stringify(event);
    const bytes=byteLength(event);let currentBytes=0;for(const row of this.rows.values())currentBytes+=row.bytes;
    if(this.rows.size>=maxCount||currentBytes+bytes>maxBytes)return false;
    this.rows.set(event.event_id,{event,bytes,created:restore?.created??Date.now(),next:restore?.next??0,attempts:restore?.attempts??0});return true;
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
