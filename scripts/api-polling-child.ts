// Isolated measurement of real idle API task runners. Wrappers below observe
// native pg calls; they never replace a client, SQL result, or scheduling delay.
import assert from 'node:assert/strict';
import {realpath} from 'node:fs/promises';
import {performance} from 'node:perf_hooks';
import type {PoolClient} from 'pg';
import {pool} from '../packages/server/src/db';
import {holdRecoveryLease} from '../packages/server/src/recovery-gate';
import {startQueryJobs} from '../packages/server/src/query-jobs';
import {startDeletions} from '../packages/server/src/deletion-service';
import {startReplayJobs} from '../packages/server/src/dlq-replay';

assert.equal(process.env.CCPANEL_API_POLLING_CHILD,'true');
const mode=process.argv[2];assert.ok(mode==='baseline'||mode==='staggered');
const source=new URL(process.env.DATABASE_URL!);assert.match(source.pathname,/^\/ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$/);
assert.ok([...source.searchParams.keys()].every(name=>name==='sslmode'));
const vps=process.env.CCPANEL_ISOLATED_POLLING_VPS==='true';
if(vps){
  assert.notEqual(process.env.GITHUB_ACTIONS,'true');assert.equal(process.env.CCPANEL_EXPECTED_RELEASE,'m4-sdk-013');
  assert.equal(await realpath('/opt/ccpanel/current'),'/opt/ccpanel/releases/m4-sdk-013');
  assert.equal(source.username,'ccpanel');assert.match(source.hostname,/^172\.(1[6-9]|2\d|3[01])\.\d{1,3}\.\d{1,3}$/);
}else{assert.equal(process.env.GITHUB_ACTIONS,'true');assert.equal(source.username,'ccpanel_ci');assert.ok(['127.0.0.1','localhost'].includes(source.hostname));}
assert.match(process.env.PGAPPNAME??'',new RegExp('^ccpanel_polling_'+mode+'_[a-f0-9]{16}$'));
assert.equal(pool.options.max,4);assert.equal(pool.options.idleTimeoutMillis,10000);

type Runner='query'|'deletion'|'replay';
const ticks:Record<Runner,number>={query:0,deletion:0,replay:0},failedTicks:Record<Runner,number>={query:0,deletion:0,replay:0};
const used=new Map<number,{checkouts:number;query:number;deletion:number;replay:number}>();
let active=0,pending=0,peakCheckedOut=0,peakRequested=0,connectErrors=0,queryErrors=0,callbackCheckouts=0,promiseCheckouts=0;
let maxPoolTotal=0,maxPoolIdle=0,maxWaiting=0,leaseAcquired=0,leaseReleased=0,publishCalls=0;
const frames:{at:number;elapsed:number;total:number;idle:number;waiting:number;checkedOut:number;pending:number}[]=[];
const originalConnect=pool.connect;
// pg.Pool.query itself calls the callback overload, whereas the runners and
// recovery lease use promises. Preserve both native overloads and identities.
const nativeConnect=originalConnect.bind(pool) as (...args:any[])=>any;
function marker(text:unknown):Runner|undefined{
  if(typeof text!=='string')return;
  if(/pg_try_advisory_lock\(169202612\)/.test(text))return'query';
  if(/pg_try_advisory_lock\(169202614\)/.test(text))return'deletion';
  if(/pg_try_advisory_xact_lock\(169202618\)/.test(text))return'replay';
}
function observeCheckout(client:PoolClient){
  active++;peakCheckedOut=Math.max(peakCheckedOut,active);peakRequested=Math.max(peakRequested,active+pending);
  const pid=(client as PoolClient&{processID:number}).processID;assert.ok(Number.isInteger(pid)&&pid>0);
  const usage=used.get(pid)??{checkouts:0,query:0,deletion:0,replay:0};usage.checkouts++;used.set(pid,usage);
  const originalQuery=client.query,originalRelease=client.release;let released=false;
  client.query=function(...args:any[]){
    const text=typeof args[0]==='string'?args[0]:args[0]?.text,runner=marker(text);
    const completed=(error?:unknown)=>{
      if(error){queryErrors++;if(runner)failedTicks[runner]++;return;}
      if(runner){ticks[runner]++;usage[runner]++;}
      if(typeof text==='string'&&text.includes('pg_advisory_lock_shared('))leaseAcquired++;
      if(typeof text==='string'&&text.includes('pg_advisory_unlock_shared('))leaseReleased++;
    };
    const input=[...args],last=input.length-1;
    if(typeof input[last]==='function'){
      const callback=input[last];input[last]=function(...values:any[]){completed(values[0]);return Reflect.apply(callback,this,values);};
      return Reflect.apply(originalQuery,client,input);
    }
    const result=Reflect.apply(originalQuery,client,input);
    if(result&&typeof result.then==='function')void result.then(()=>completed(),(error:unknown)=>completed(error));
    return result;
  } as typeof client.query;
  client.release=function(...args:Parameters<PoolClient['release']>){
    if(!released){released=true;client.query=originalQuery;client.release=originalRelease;active--;}
    return Reflect.apply(originalRelease,client,args);
  };
  return client;
}
(pool as unknown as {connect:(...args:any[])=>any}).connect=function(...args:any[]){
  pending++;peakRequested=Math.max(peakRequested,active+pending);
  if(typeof args[0]==='function'){
    callbackCheckouts++;const callback=args[0];
    return nativeConnect((error:unknown,client:PoolClient|undefined,release:unknown)=>{
      pending--;if(error||!client){connectErrors++;return callback(error,client,release);}
      const observed=observeCheckout(client);return callback(null,observed,observed.release);
    });
  }
  assert.equal(args.length,0);promiseCheckouts++;
  return nativeConnect().then((client:PoolClient)=>{pending--;return observeCheckout(client);},(error:unknown)=>{pending--;connectErrors++;throw error;});
};
const delay=(ms:number)=>new Promise<void>(done=>setTimeout(done,ms));
let releaseLease:(()=>Promise<void>)|undefined,sampler:ReturnType<typeof setInterval>|undefined;
const stops:(()=>void)[]=[];
let started=0,startedAt=0,finishedAt=0,finalPool:{total:number;idle:number;waiting:number}|undefined;
let stage='recovery_lease',failed=false,failureType='Error',sqlState:string|null=null;
const failure=(error:unknown)=>{
  if(failed)return;failed=true;
  const value=error as {name?:unknown;code?:unknown};
  if(['AssertionError','TypeError','RangeError'].includes(String(value?.name)))failureType=String(value.name);
  if(typeof value?.code==='string'&&/^[0-9A-Z]{5}$/.test(value.code))sqlState=value.code;
};
try{
  releaseLease=await holdRecoveryLease();
  stage='empty_source_preflight';
  const initial=(await pool.query(`SELECT current_database() AS database,
    (SELECT max(version) FROM schema_migrations) AS schema,
    (SELECT count(*)::int FROM tenants) AS tenants,
    (SELECT count(*)::int FROM query_jobs) AS query_jobs,
    (SELECT count(*)::int FROM deletion_jobs) AS deletion_jobs,
    (SELECT count(*)::int FROM replay_jobs) AS replay_jobs,
    (SELECT count(*)::int FROM replay_items) AS replay_items,
    (SELECT blocked FROM ingest_integrity_gate WHERE id=true) AS integrity_blocked`)).rows[0];
  assert.equal(initial.database,decodeURIComponent(source.pathname.slice(1)));assert.equal(initial.schema,22);
  for(const field of ['tenants','query_jobs','deletion_jobs','replay_jobs','replay_items'])assert.equal(initial[field],0);
  assert.equal(initial.integrity_blocked,false);assert.equal(active,1);assert.equal(pending,0);assert.equal(leaseAcquired,1);
  // The one preflight Pool.query call is an equal warm-up for both processes.
  // No added sleep query, fake work, or database workload amplifies contention.
  peakCheckedOut=active;peakRequested=active;started=performance.now();startedAt=Date.now();
  const sample=()=>{
    const row={at:Date.now(),elapsed:Math.round(performance.now()-started),total:pool.totalCount,idle:pool.idleCount,waiting:pool.waitingCount,checkedOut:active,pending};
    frames.push(row);maxPoolTotal=Math.max(maxPoolTotal,row.total);maxPoolIdle=Math.max(maxPoolIdle,row.idle);maxWaiting=Math.max(maxWaiting,row.waiting);
  };
  sample();sampler=setInterval(sample,100);stage='runner_start';
  const phases=mode==='baseline'?[2000,2000,2000]:[0,667,1333];
  stops.push(startQueryJobs(phases[0]));stops.push(startDeletions(phases[1]));
  stops.push(startReplayJobs(async()=>{publishCalls++;throw Error('unexpected_publish_on_empty_queue');},phases[2]));
  stage='measurement';await delay(25000);finishedAt=Date.now();sample();stage='runner_drain';
  for(const stop of stops)stop();stops.length=0;clearInterval(sampler);sampler=undefined;
  const drain=performance.now()+3000;while((active!==1||pending!==0)&&performance.now()<drain)await delay(10);
  assert.equal(active,1);assert.equal(pending,0);assert.equal(connectErrors,0);assert.equal(queryErrors,0);assert.equal(publishCalls,0);stage='measurement_invariants';
  for(const runner of ['query','deletion','replay'] as const){assert.ok(ticks[runner]>=10&&ticks[runner]<=14,'real runner tick count out of observation range');assert.equal(failedTicks[runner],0);}
  assert.ok(callbackCheckouts>=1&&promiseCheckouts>=31);assert.ok(peakCheckedOut<=4);assert.ok(maxPoolTotal<=4);
  finalPool={total:pool.totalCount,idle:pool.idleCount,waiting:pool.waitingCount};
}catch(error){failure(error);}finally{
  for(const stop of stops)stop();if(sampler)clearInterval(sampler);
  try{await releaseLease?.();}catch(error){if(!failed)stage='lease_cleanup';failure(error);}
  pool.connect=originalConnect;try{await pool.end();}catch(error){if(!failed)stage='pool_cleanup';failure(error);}
}
const settled=frames.filter(row=>row.elapsed>=20000);
if(!failed)try{stage='final_cleanup_invariants';assert.equal(active,0);assert.equal(pending,0);assert.equal(leaseReleased,1);assert.ok(settled.length>=40);}catch(error){failure(error);}
if(failed){
  console.error(JSON.stringify({code:'api_polling_child_failed',stage,type:failureType,sqlState,ticks,failedTicks,active,pending,connectErrors,queryErrors,leaseAcquired,leaseReleased}));process.exitCode=1;
}else console.log(JSON.stringify({passed:true,mode,surface:vps?'isolated_vps_tooling':'ci',periodMs:2000,initialPhasesMs:mode==='baseline'?[2000,2000,2000]:[0,667,1333],measurementStartedAt:startedAt,measurementFinishedAt:finishedAt,measurementMs:finishedAt-startedAt,settledWindowStartedAt:startedAt+20000,poolMax:4,idleTimeoutMs:10000,ticks,failedTicks,checkoutObservations:{callbackCheckouts,promiseCheckouts,peakCheckedOut,peakRequested,maxPoolTotal,maxPoolIdle,maxWaiting,connectErrors,queryErrors},settledPool:{samples:settled.length,minTotal:Math.min(...settled.map(row=>row.total)),maxTotal:Math.max(...settled.map(row=>row.total)),minIdle:Math.min(...settled.map(row=>row.idle)),maxIdle:Math.max(...settled.map(row=>row.idle))},finalPool,nativeBackendUsage:[...used].map(([pid,value])=>({pid,...value})).sort((a,b)=>a.pid-b.pid),recoveryLeaseAcquired:true,recoveryLeaseReleased:true,publishCalls,allPoolClientsClosed:true}));
