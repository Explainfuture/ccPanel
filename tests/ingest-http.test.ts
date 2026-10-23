import { afterEach,expect,it,vi } from 'vitest';
import { randomUUID } from 'node:crypto';
import { createApp } from '../apps/api/src/app';
import { IngestAdmission,publishWithAdmission } from '../packages/server/src/ingest-admission';
import { defaultPressurePolicy } from '../packages/server/src/ingest-pressure';
const scope=vi.hoisted(()=>({tenant:'00000000-0000-4000-8000-000000000001',project:'00000000-0000-4000-8000-000000000002'}));
vi.mock('../packages/server/src/db',()=>({pool:{},query:vi.fn(async(sql:string)=>{
  if(sql.includes('FROM write_keys k'))return {rows:[{tenant_id:scope.tenant,project_id:scope.project,origins:['https://allowed.invalid'],schema_mode:'loose',retention_cutoff:new Date(0)}]};
  if(sql.includes('FROM event_schemas'))return {rows:[]};if(sql==='SELECT 1')return {rows:[{}]};throw new Error('unexpected_test_query');
})}));
afterEach(()=>vi.restoreAllMocks());
function setup(){
  const now=Date.now(),gate=new IngestAdmission({...defaultPressurePolicy},()=>now);gate.observe({startedAt:now,partitions:[{partition:0,low:'0',high:'0',committed:'0',ageBoundary:'0'}],disks:['kafka','postgres'].map(kind=>({kind:kind as 'kafka'|'postgres',sampledAt:now,totalBytes:'21474836480',availableBytes:'12884901888',totalInodes:'100000',availableInodes:'50000'}))});
  const send=vi.fn<()=>Promise<void>>(async()=>{}),app=createApp({ready:()=>true,admissionStatus:()=>gate.status(),publish:records=>publishWithAdmission(gate,records.length,send)});
  const event={event_id:randomUUID(),event_name:'pressure_fixture',distinct_id:'synthetic',event_time:new Date().toISOString(),properties:{},context:{},schema_version:1};
  const request=()=>app.request('/v1/batch',{method:'POST',headers:{origin:'https://allowed.invalid'},body:JSON.stringify({write_key:'synthetic',schema_version:1,events:[event]})});return {gate,send,app,event,request};
}
it('returns retryable 503 without acknowledgement or publication when headroom is reserved',async()=>{
  const {gate,send,request}=setup();gate.reserve(100);gate.reserve(100);gate.reserve(56);
  const r=await request();expect(r.status).toBe(503);expect(r.headers.get('Retry-After')).toBe('5');expect(r.headers.get('Access-Control-Expose-Headers')).toBe('Retry-After');
  expect(await r.json()).toEqual({error:'ingestion_paused',reason:'ingest_reservations_full'});expect(send).not.toHaveBeenCalled();
});
it('keeps liveness available while pressure closes readiness and intake',async()=>{
  const {gate,app,request}=setup();gate.probeFailed('host_probe_unavailable');expect((await app.request('/health')).status).toBe(200);expect((await app.request('/ready')).status).toBe(503);expect((await request()).status).toBe(503);
});
it('does not retract a genuine Kafka acknowledgement if pressure changes while publishing',async()=>{
  const {gate,send,event,request}=setup();let done!:()=>void;send.mockImplementation(()=>new Promise(resolve=>{done=resolve;}));
  const pending=request();await vi.waitFor(()=>expect(send).toHaveBeenCalledTimes(1));gate.probeFailed('host_probe_unavailable');done();
  const r=await pending;expect(r.status).toBe(202);expect(await r.json()).toEqual({accepted_event_ids:[event.event_id]});expect(gate.status()).toMatchObject({reservedMessages:1,unknownMessages:0,pendingRequests:0});
});
it('never fabricates 202 when publication rejects and retains unknown reserve',async()=>{
  vi.spyOn(console,'error').mockImplementation(()=>{});const {gate,send,request}=setup();send.mockRejectedValue(new Error('simulated broker timeout'));
  const r=await request();expect(r.status).toBe(503);expect(await r.json()).toEqual({error:'temporarily_unavailable'});expect(gate.status()).toMatchObject({reservedMessages:1,unknownMessages:1});
});
it('does not open ingestion when an application forgets to provide its pressure status',async()=>{
  const {event}=setup(),publish=vi.fn(async()=>{}),app=createApp({ready:()=>true,publish});
  const r=await app.request('/v1/batch',{method:'POST',body:JSON.stringify({write_key:'synthetic',schema_version:1,events:[event]})});
  expect(r.status).toBe(503);expect(await r.json()).toEqual({error:'ingestion_paused',reason:'pressure_probe_unavailable'});expect(publish).not.toHaveBeenCalled();
});
