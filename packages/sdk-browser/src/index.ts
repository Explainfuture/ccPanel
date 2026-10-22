import { byteLength, limits, validateEvent, type AnalyticsEvent, type Json } from '../../schema/src/index';
import { IndexedQueue,MemoryQueue,type Queue,type QueueRow } from './queue';
export type { AnalyticsEvent,Json } from '../../schema/src/index';
export interface AnalyticsOptions {
  endpoint:string;projectId:string;writeKey:string;consent:boolean;
  flushInterval?:number;batchSize?:number;queue?:Queue;fetch?:typeof globalThis.fetch;
  propertyAllowlist?:Readonly<Record<string,readonly string[]>>;
  onError?:(code:string,details?:{event_ids:string[]})=>void;source?:'browser'|'node';
}
export interface TrackResult{status:'queued-persistent'|'queued-memory'|'rejected';event_id?:string;reason?:string}
const sensitive=/password|secret|token|authorization|cookie|phone|email/i;
export class Analytics {
  private queue:Queue;
  private memory=new MemoryQueue();
  private consent:boolean;
  private anonymous='';private user:string|undefined;
  private timer:ReturnType<typeof setInterval>|undefined;
  private flight:Promise<{confirmed:number;pending:number}>|undefined;
  private snapshot:AnalyticsEvent[]=[];
  private hiddenSent=false;private paused=false;private lastError:string|undefined;private epoch=0;
  private owner=crypto.randomUUID();private identityKey:string;
  private batchSize:number;private running=true;private pendingWrites=0;
  private needsRecovery=false;private nextRecovery=0;
  private consentChanges:Promise<void>=Promise.resolve();
  private allowedProperties:Map<string,Set<string>>|undefined;
  private cleanup:(()=>void)[]=[];
  constructor(private options:AnalyticsOptions){
    const url=new URL(options.endpoint);
    if(url.protocol!=='https:'&&!(url.protocol==='http:'&&['localhost','127.0.0.1'].includes(url.hostname)))throw new Error('HTTPS endpoint required');
    if(!options.projectId||!options.writeKey||options.writeKey.length>256)throw new Error('projectId and writeKey required');
    this.batchSize=options.batchSize??limits.batchEvents;
    if(!Number.isInteger(this.batchSize)||this.batchSize<1||this.batchSize>100)throw new Error('invalid_batch_size');
    if(options.flushInterval!==undefined&&options.flushInterval<100)throw new Error('invalid_flush_interval');
    if(options.propertyAllowlist!==undefined){
      const list=options.propertyAllowlist;
      if(!list||typeof list!=='object'||Array.isArray(list)||![Object.prototype,null].includes(Object.getPrototypeOf(list))||Object.keys(list).length>1000)throw new Error('invalid_property_allowlist');
      this.allowedProperties=new Map();
      for(const[name,keys]of Object.entries(list)){
        if(!name||name.length>128||!Array.isArray(keys)||keys.length>100||!keys.every(k=>typeof k==='string'&&k.length>0&&k.length<=128))throw new Error('invalid_property_allowlist');
        this.allowedProperties.set(name,new Set(keys));
      }
    }
    this.consent=options.consent;
    this.identityKey=`ccpanel:identity:${url.origin}${url.pathname}:${options.projectId}`;
    this.queue=options.queue??(typeof indexedDB!=='undefined'?new IndexedQueue(this.identityKey+':queue'):this.memory);
    if(this.consent)this.loadIdentity();
    this.timer=setInterval(()=>{void this.flush();},options.flushInterval??3000);
    this.timer.unref?.();
    if(typeof window!=='undefined'){
      const hidden=()=>{if(document.visibilityState==='hidden')this.beacon();else{this.hiddenSent=false;void this.flush();}};
      const pagehide=()=>this.beacon();const online=()=>void this.flush();
      const storage=(e:StorageEvent)=>{if(e.key===this.identityKey&&this.consent)this.loadIdentity();};
      document.addEventListener('visibilitychange',hidden);window.addEventListener('pagehide',pagehide);window.addEventListener('online',online);window.addEventListener('storage',storage);
      this.cleanup.push(()=>{document.removeEventListener('visibilitychange',hidden);window.removeEventListener('pagehide',pagehide);window.removeEventListener('online',online);window.removeEventListener('storage',storage);});
    }
    void this.flush();
  }
  private error(code:string,details?:{event_ids:string[]}){this.lastError=code;try{this.options.onError?.(code,details);}catch{/* diagnostics must not break business code */}}
  private loadIdentity(){
    try{const s=typeof localStorage!=='undefined'?localStorage.getItem(this.identityKey):null;if(s){const v=JSON.parse(s);if(typeof v.anonymous==='string'){this.anonymous=v.anonymous;this.user=typeof v.user==='string'?v.user:undefined;return;}}}catch{this.error('identity_storage_error');}
    this.anonymous=crypto.randomUUID();this.user=undefined;this.saveIdentity();
  }
  private saveIdentity(){try{if(typeof localStorage!=='undefined')localStorage.setItem(this.identityKey,JSON.stringify({anonymous:this.anonymous,user:this.user}));}catch{this.error('identity_storage_error');}}
  async track(name:string,properties:Record<string,Json>={}):Promise<TrackResult>{
    if(!this.consent||!this.running)return{status:'rejected',reason:'consent_required'};
    const version=this.epoch;
    // Freeze the payload before the first await so identity changes and caller mutation cannot rewrite it.
    let event:AnalyticsEvent;
    try{
      if(this.allowedProperties){
        if(!properties||typeof properties!=='object'||Array.isArray(properties)||Object.getPrototypeOf(properties)!==Object.prototype)throw new Error('invalid_properties');
        const allow=this.allowedProperties.get(name);
        // Read only approved own fields; unknown event names have no approved
        // properties. Constructor copies configuration so later mutation cannot
        // silently expand collection. Sensitive-key filtering still applies.
        properties=Object.fromEntries(Object.keys(properties).filter(k=>allow?.has(k)).map(k=>[k,properties[k]!]));
      }
      const context:Record<string,Json>={sdk:'ccpanel',version:'0.1.0',source:this.options.source||'browser'};
      if(typeof location!=='undefined')context.page=location.pathname;
      event={event_id:crypto.randomUUID(),event_name:name,distinct_id:this.user||this.anonymous,anonymous_id:this.anonymous,event_time:new Date().toISOString(),properties,context,schema_version:1};
      const invalid=validateEvent(event);if(invalid){this.error(invalid);return{status:'rejected',reason:invalid};}
      event=JSON.parse(JSON.stringify(event));
      // Default privacy filter applies recursively without touching the caller's object.
      const scrub=(v:Json):Json=>Array.isArray(v)?v.map(scrub):v&&typeof v==='object'?Object.fromEntries(Object.entries(v).filter(([k])=>!sensitive.test(k)).map(([k,x])=>[k,scrub(x)])):v;
      event.properties=scrub(event.properties) as Record<string,Json>;
    }catch{this.error('invalid_properties');return{status:'rejected',reason:'invalid_properties'};}
    if(this.snapshot.length<this.batchSize&&byteLength(this.envelope([...this.snapshot,event]))<=limits.batchBytes)this.snapshot.push(event);
    this.pendingWrites++;
    try{
      let stored=false;
      try{stored=await this.queue.add(event,this.queue.persistent?1000:100,this.queue.persistent?5*1024*1024:512*1024);}
      catch{this.needsRecovery=true;this.error('storage_error');stored=await this.memory.add(event,100,512*1024);}
      if(!this.consent||version!==this.epoch){await this.queue.finish([event.event_id],'');await this.memory.finish([event.event_id],'');return{status:'rejected',reason:'consent_revoked'};}
      if(!stored){this.snapshot=this.snapshot.filter(e=>e.event_id!==event.event_id);this.error('rejected_overflow');return{status:'rejected',reason:'rejected_overflow'};}
      const status=this.queue.persistent&&!(await this.memory.peek(100)).some(r=>r.event.event_id===event.event_id)?'queued-persistent':'queued-memory';
      const stats=await this.getQueueStatus();if(stats.count>=this.batchSize||stats.bytes>=limits.batchBytes)queueMicrotask(()=>void this.flush());
      return{status,event_id:event.event_id};
    }finally{this.pendingWrites--;}
  }
  async identify(userId:string){if(!userId||userId.length>256)throw new Error('invalid_user_id');if(!this.consent)return;this.user=userId;this.saveIdentity();return this.track('$identify');}
  reset(){this.user=undefined;this.anonymous=crypto.randomUUID();if(this.consent)this.saveIdentity();}
  setConsent(value:boolean){
    const version=++this.epoch;this.consent=false;this.snapshot=[];
    const change=this.consentChanges.catch(()=>{}).then(async()=>{
      if(!value){if(this.flight)await this.flight;await this.queue.clear();await this.memory.clear();try{localStorage.removeItem(this.identityKey);}catch{/* optional storage */}this.user=undefined;this.anonymous='';}
      if(version!==this.epoch)return;
      this.consent=value;if(value){this.loadIdentity();void this.flush();}
    });
    this.consentChanges=change;return change;
  }
  updateWriteKey(key:string){if(!key||key.length>256)throw new Error('invalid_key');this.options.writeKey=key;this.paused=false;void this.flush();}
  private envelope(events:AnalyticsEvent[]){return{write_key:this.options.writeKey,project_id:this.options.projectId,schema_version:1,events};}
  private async stores(){return this.queue===this.memory?[this.queue]:[this.queue,this.memory];}
  async getQueueStatus(){let count=0,bytes=0,persistent_count=0,memory_count=0,oldest:number|null=null;for(const q of await this.stores()){try{const s=await q.stats();count+=s.count;bytes+=s.bytes;if(q.persistent)persistent_count+=s.count;else memory_count+=s.count;if(s.oldest!==null)oldest=oldest===null?s.oldest:Math.min(oldest,s.oldest);}catch{this.error('storage_error');}}return{count,bytes,persistent_count,memory_count,oldest,last_error:this.lastError,storage:this.needsRecovery?'memory-fallback':this.queue.persistent?'persistent-with-memory-fallback':'memory'};}
  flush():Promise<{confirmed:number;pending:number}>{
    if(this.flight)return this.flight;
    this.flight=this.send().catch(()=>{this.error('send_error');return{confirmed:0,pending:0};}).finally(()=>{this.flight=undefined;});return this.flight;
  }
  private async send(){
    let confirmed=0;
    if(!this.consent||this.paused||!this.running)return{confirmed,pending:(await this.getQueueStatus()).count};
    if(this.needsRecovery&&!this.pendingWrites&&Date.now()>=this.nextRecovery){
      this.nextRecovery=Date.now()+30000;
      try{await this.queue.recover?.();await this.queue.stats();this.needsRecovery=false;this.lastError=undefined;}catch{this.error('storage_error');}
    }
    if(this.queue.persistent&&!this.needsRecovery){
      const expiredMemory=await this.memory.expire(Date.now()-limits.ttl);if(expiredMemory)this.error('expired:'+expiredMemory);
      for(const row of await this.memory.peek(100)){
        try{if(await this.queue.add(row.event,1000,5*1024*1024,row))await this.memory.finish([row.event.event_id],'');else break;}
        catch{this.needsRecovery=true;this.error('storage_error');break;}
      }
    }
    for(const q of await this.stores()){
      let rows:QueueRow[]=[];
      try{
        const expired=await q.expire(Date.now()-limits.ttl);if(expired)this.error('expired:'+expired);
        rows=await q.claim(Date.now(),this.batchSize,limits.batchBytes-2048,this.owner);
      }catch{this.error('storage_error');continue;}
      if(!rows.length)continue;
      // Respect the exact envelope byte limit, not just the summed event bodies.
      while(rows.length&&byteLength(this.envelope(rows.map(r=>r.event)))>limits.batchBytes){const r=rows.pop()!;await q.finish([r.event.event_id],this.owner,Date.now());}
      if(!rows.length)continue;
      const ids=rows.map(r=>r.event.event_id),epoch=this.epoch;
      let retryAt=Date.now()+Math.floor(Math.random()*Math.min(60000,1000*2**Math.min(16,Math.max(...rows.map(r=>r.attempts)))));
      try{
        if(!this.consent||epoch!==this.epoch)break;
        const response=await(this.options.fetch||globalThis.fetch)(this.options.endpoint,{method:'POST',headers:{'content-type':'text/plain;charset=UTF-8'},credentials:'omit',body:JSON.stringify(this.envelope(rows.map(r=>r.event))),signal:AbortSignal.timeout(10000)});
        if(response.status===202){
          const b=await response.json() as {accepted_event_ids?:unknown};const ack=b.accepted_event_ids;
          if(!Array.isArray(ack)||ack.length!==ids.length||new Set(ack).size!==ids.length||!ids.every(id=>ack.includes(id)))throw new Error('invalid_ack');
          await q.finish(ids,this.owner);confirmed+=ids.length;
        }else if(response.status===401||response.status===403){this.paused=true;this.error('credentials_or_origin_error');await q.finish(ids,this.owner,retryAt);}
        else if(response.status===400){
          const b=await response.json() as {errors?:{index:number;code:string}[]};
          if(!Array.isArray(b.errors)||!b.errors.length||b.errors.some(e=>!Number.isInteger(e.index)||e.index<0||e.index>=ids.length)){this.paused=true;this.error('unlocated_bad_request');await q.finish(ids,this.owner,retryAt);}
          else{const rejected=[...new Set(b.errors.map(e=>ids[e.index]!))];await q.finish(rejected,this.owner);await q.finish(ids.filter(id=>!rejected.includes(id)),this.owner,Date.now());for(const e of b.errors)this.error('event_rejected:'+e.code,{event_ids:[ids[e.index]!]});}
        }else if(response.status===413){
          if(rows.length===1){await q.finish(ids,this.owner);this.error('event_too_large');}else{this.batchSize=Math.max(1,Math.floor(rows.length/2));await q.finish(ids,this.owner,Date.now());}
        }else{
          if(response.status===429||response.status===503){const h=response.headers.get('retry-after');if(h){const wait=/^\d+$/.test(h)?Date.now()+Number(h)*1000:Date.parse(h);if(Number.isFinite(wait))retryAt=Math.max(retryAt,wait);}}
          await q.finish(ids,this.owner,retryAt);this.error('retry_http_'+response.status);
        }
      }catch{await q.finish(ids,this.owner,retryAt).catch(()=>this.error('storage_error'));this.error('network_or_ack_error');}
    }
    const current:AnalyticsEvent[]=[];
    for(const q of await this.stores())try{for(const r of await q.peek(this.batchSize)){if(current.length<this.batchSize&&byteLength(this.envelope([...current,r.event]))<=limits.batchBytes)current.push(r.event);}}catch{this.error('storage_error');}
    // Do not replace the exit snapshot while a track write is still in progress.
    if(!this.pendingWrites)this.snapshot=current;
    return{confirmed,pending:(await this.getQueueStatus()).count};
  }
  beacon(){if(!this.consent||this.paused||this.hiddenSent||!this.snapshot.length||typeof navigator==='undefined'||!navigator.sendBeacon)return false;this.hiddenSent=true;return navigator.sendBeacon(this.options.endpoint,new Blob([JSON.stringify(this.envelope(this.snapshot))],{type:'text/plain;charset=UTF-8'}));}
  destroy(){this.running=false;if(this.timer)clearInterval(this.timer);for(const fn of this.cleanup)fn();this.cleanup=[];}
}
export type EventMap=Record<string,Record<string,Json>>;
export type TypedAnalytics<Events extends EventMap>=Omit<Analytics,'track'>&{
  track<Name extends Extract<keyof Events,string>>(name:Name,...args:{} extends Events[Name]?[properties?:Events[Name]]:[properties:Events[Name]]):Promise<TrackResult>;
};
export function createAnalytics(options:AnalyticsOptions):Analytics;
export function createAnalytics<Events extends EventMap>(options:AnalyticsOptions):TypedAnalytics<Events>;
export function createAnalytics(options:AnalyticsOptions):unknown{return new Analytics(options);}
