import {afterEach,expect,it,vi} from 'vitest';
import {randomUUID} from 'node:crypto';
import {createApp} from '../apps/api/src/app';
import type {IntakeObservation} from '../packages/server/src/intake-metrics';
import type {AdmissionStatus} from '../packages/server/src/ingest-admission';
vi.mock('../packages/server/src/db',()=>({pool:{},query:vi.fn(async(sql:string)=>{
  if(sql.includes('FROM write_keys k'))return {rows:[{tenant_id:'00000000-0000-4000-8000-000000000001',project_id:'00000000-0000-4000-8000-000000000002',origins:['https://allowed.invalid'],schema_mode:'loose',retention_cutoff:new Date(0)}]};
  if(sql.includes('FROM event_schemas'))return {rows:[]};throw new Error('unexpected_query');
})}));
afterEach(()=>vi.restoreAllMocks());
const open:AdmissionStatus={allowed:true,reason:null,integrityReason:null,retryAfterSeconds:0,totalLagMessages:'0',maxLagMessages:'0',remainingMessages:256,reservedMessages:0,unknownMessages:0,pendingRequests:0,reservationCount:0};
const event=()=>({event_id:randomUUID(),event_name:'click',distinct_id:'private-user',event_time:new Date().toISOString(),properties:{private:'do-not-log'},context:{},schema_version:1});
it('records actual final responses including middleware/error-handler paths without payload or credential fields',async()=>{
  vi.spyOn(console,'error').mockImplementation(()=>{});const seen:IntakeObservation[]=[],publish=vi.fn(async()=>{}),app=createApp({ready:()=>true,admissionStatus:()=>open,publish,observeIntake:v=>{seen.push(v);}});
  const request=(body:string,origin?:string)=>app.request('/v1/batch',{method:'POST',headers:origin?{origin}:{},body});
  await app.request('/v1/batch',{method:'OPTIONS'});expect(seen).toHaveLength(0);
  expect((await request('x'.repeat(65537))).status).toBe(413);expect((await request('invalid JSON')).status).toBe(400);expect((await request('{}')).status).toBe(401);
  const value={write_key:'public-but-do-not-log',schema_version:1,events:[event()]};
  expect((await request(JSON.stringify(value),'https://wrong.invalid')).status).toBe(403);
  expect((await request(JSON.stringify(value))).status).toBe(202);
  publish.mockRejectedValueOnce(new Error('broker unavailable'));expect((await request(JSON.stringify(value))).status).toBe(503);
  expect(seen.map(r=>r.status)).toEqual([413,400,401,403,202,503]);expect(seen.map(r=>r.events)).toEqual([null,null,null,1,1,1]);expect(seen.every(r=>Number.isFinite(r.durationMs)&&r.durationMs>=0)).toBe(true);
  const serialized=JSON.stringify(seen);for(const secret of ['private-user','do-not-log',value.write_key,value.events[0].event_id])expect(serialized).not.toContain(secret);
});
it('does not retract Kafka acknowledgement when sync or async telemetry fails',async()=>{
  vi.spyOn(console,'error').mockImplementation(()=>{});
  for(const observeIntake of [()=>{throw new Error('telemetry failed');},async()=>{throw new Error('async telemetry failed');}]){
    const publish=vi.fn(async()=>{}),app=createApp({ready:()=>true,admissionStatus:()=>open,publish,observeIntake});const e=event();
    const response=await app.request('/v1/batch',{method:'POST',body:JSON.stringify({write_key:'test',schema_version:1,events:[e]})});
    expect(response.status).toBe(202);expect(await response.json()).toEqual({accepted_event_ids:[e.event_id]});expect(publish).toHaveBeenCalledTimes(1);
  }
});
it('keeps event counts isolated when concurrent batches complete in reverse order',async()=>{
  const seen:IntakeObservation[]=[],pending:{count:number;complete:()=>void}[]=[];
  const app=createApp({ready:()=>true,admissionStatus:()=>open,observeIntake:v=>{seen.push(v);},publish:records=>new Promise<void>(complete=>{pending.push({count:records.length,complete});})});
  const request=(n:number)=>app.request('/v1/batch',{method:'POST',body:JSON.stringify({write_key:'test',schema_version:1,events:Array.from({length:n},event)})});
  const one=request(1),three=request(3);await vi.waitFor(()=>expect(pending).toHaveLength(2));
  pending.find(v=>v.count===3)!.complete();expect((await three).status).toBe(202);pending.find(v=>v.count===1)!.complete();expect((await one).status).toBe(202);
  expect(seen.map(v=>v.events)).toEqual([3,1]);
});
