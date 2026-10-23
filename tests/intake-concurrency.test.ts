import {afterEach,expect,it,vi} from 'vitest';
import {randomUUID} from 'node:crypto';
import {createApp} from '../apps/api/src/app';
import {serve} from '@hono/node-server';
import {Agent,request as httpRequest} from 'node:http';
import {once} from 'node:events';
import {query} from '../packages/server/src/db';
import type {AdmissionStatus} from '../packages/server/src/ingest-admission';
vi.mock('../packages/server/src/db',()=>({pool:{},query:vi.fn()}));
afterEach(()=>{vi.restoreAllMocks();vi.resetAllMocks();});
const key={tenant_id:randomUUID(),project_id:randomUUID(),origins:[],schema_mode:'loose'};
const open:AdmissionStatus={allowed:true,reason:null,integrityReason:null,retryAfterSeconds:0,totalLagMessages:'0',maxLagMessages:'0',remainingMessages:256,reservedMessages:0,unknownMessages:0,pendingRequests:0,reservationCount:0};
const body=()=>JSON.stringify({write_key:'test',schema_version:1,events:[{event_id:randomUUID(),event_name:'click',distinct_id:'user',event_time:new Date().toISOString(),properties:{},context:{},schema_version:1}]});
it('rejects excess requests before body access and PG while existing authentication is pending',async()=>{
  const waits:(()=>void)[]=[];
  vi.mocked(query).mockImplementation((sql:string)=>sql.includes('FROM write_keys k')?new Promise(resolve=>waits.push(()=>resolve({rows:[key]} as never))):Promise.resolve({rows:[]}) as never);
  const publish=vi.fn(async()=>{}),app=createApp({maxIntakeInFlight:2,ready:()=>true,admissionStatus:()=>open,publish});
  const first=app.request('/v1/batch',{method:'POST',body:body()}),second=app.request('/v1/batch',{method:'POST',body:body()});
  await vi.waitFor(()=>expect(waits).toHaveLength(2));
  const excess=new Request('http://localhost/v1/batch',{method:'POST',body:'invalid JSON'}),access=vi.spyOn(excess,'body','get');
  const rejected=await app.fetch(excess);expect(rejected.status).toBe(503);expect(await rejected.json()).toEqual({error:'ingestion_paused',reason:'intake_concurrency_full'});
  expect(rejected.headers.get('Retry-After')).toBe('5');expect(rejected.headers.get('Access-Control-Allow-Origin')).toBe('*');expect(access).not.toHaveBeenCalled();expect(query).toHaveBeenCalledTimes(2);expect(publish).not.toHaveBeenCalled();
  expect((await app.request('/v1/batch',{method:'OPTIONS'})).status).toBe(204);expect((await app.request('/health')).status).toBe(200);
  for(const release of waits)release();expect((await first).status).toBe(202);expect((await second).status).toBe(202);
});
it('releases slots after parsing and publishing failures, and does not retract in-flight success',async()=>{
  vi.spyOn(console,'error').mockImplementation(()=>{});vi.mocked(query).mockImplementation(async(sql:string)=>({rows:sql.includes('FROM write_keys k')?[key]:[]}) as never);
  let release!:()=>void;const publish=vi.fn(()=>new Promise<void>(resolve=>{release=resolve;})),app=createApp({maxIntakeInFlight:1,ready:()=>true,admissionStatus:()=>open,publish});
  const send=()=>app.request('/v1/batch',{method:'POST',body:body()});const pending=send();await vi.waitFor(()=>expect(publish).toHaveBeenCalledOnce());
  expect((await send()).status).toBe(503);release();expect((await pending).status).toBe(202);
  expect((await app.request('/v1/batch',{method:'POST',body:'bad JSON'})).status).toBe(400);
  publish.mockRejectedValueOnce(new Error('broker failure'));expect((await send()).status).toBe(503);
  publish.mockResolvedValueOnce();expect((await send()).status).toBe(202);
});
it('rejects invalid budgets instead of silently disabling the bound',()=>{
  for(const maxIntakeInFlight of [0,-1,129,1.5,NaN,Infinity])expect(()=>createApp({maxIntakeInFlight,ready:()=>false,publish:async()=>{}})).toThrow('invalid_intake_concurrency_budget');
});
it('rejects a real HTTP body under saturation and serves subsequent requests after release',async()=>{
  vi.mocked(query).mockImplementation(async(sql:string)=>({rows:sql.includes('FROM write_keys k')?[key]:[]}) as never);
  let release!:()=>void;const publish=vi.fn(()=>new Promise<void>(resolve=>{release=resolve;}));
  const app=createApp({maxIntakeInFlight:1,ready:()=>true,admissionStatus:()=>open,publish}),server=serve({fetch:app.fetch,hostname:'127.0.0.1',port:0});
  await once(server,'listening');const address=server.address();if(!address||typeof address==='string')throw new Error('test_listener_unavailable');
  const a=new Agent({keepAlive:true}),b=new Agent({keepAlive:true,maxSockets:1});
  const send=(data:string,agent:Agent,method='POST')=>new Promise<{status:number;body:string}>((resolve,reject)=>{
    const request=httpRequest({host:'127.0.0.1',port:address.port,path:'/v1/batch',method,agent,headers:{'content-length':Buffer.byteLength(data),'content-type':'text/plain'}},response=>{
      let output='';response.setEncoding('utf8');response.on('data',chunk=>{output+=chunk;});response.on('end',()=>resolve({status:response.statusCode!,body:output}));response.on('error',reject);
    });request.on('error',reject);request.setTimeout(4000,()=>request.destroy(new Error('test_http_timeout')));request.end(data);
  });
  try{
    const pending=send(body(),a);await vi.waitFor(()=>expect(publish).toHaveBeenCalledOnce());
    expect((await send('x'.repeat(64000),b)).status).toBe(503);expect((await send('',b,'OPTIONS')).status).toBe(204);
    release();expect((await pending).status).toBe(202);publish.mockResolvedValueOnce();expect((await send(body(),b)).status).toBe(202);
  }finally{release?.();a.destroy();b.destroy();if('closeAllConnections' in server)server.closeAllConnections();await new Promise<void>(resolve=>server.close(()=>resolve()));}
});
