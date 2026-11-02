// Two sequential real empty-queue processes in one temporary database. Native
// pool metrics and a separate PG observer are evidence, not a RAM benchmark.
import assert from 'node:assert/strict';
import {spawn,execFile} from 'node:child_process';
import {randomBytes} from 'node:crypto';
import {realpath,readFile} from 'node:fs/promises';
import {dirname,join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {performance} from 'node:perf_hooks';
import {promisify} from 'node:util';
import {isIP} from 'node:net';
import pg from 'pg';

const source=new URL(process.env.DATABASE_URL!);assert.match(source.pathname,/^\/ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$/);
assert.ok([...source.searchParams.keys()].every(name=>name==='sslmode'));
const vps=process.env.CCPANEL_ISOLATED_POLLING_VPS==='true';
if(vps){
  assert.equal(process.platform,'linux');assert.equal(process.geteuid?.(),0);
  assert.notEqual(process.env.GITHUB_ACTIONS,'true');assert.equal(process.env.CCPANEL_EXPECTED_RELEASE,'m4-sdk-013');
  assert.equal(await realpath('/opt/ccpanel/current'),'/opt/ccpanel/releases/m4-sdk-013');
  assert.equal(source.username,'ccpanel');assert.match(source.hostname,/^172\.(1[6-9]|2\d|3[01])\.\d{1,3}\.\d{1,3}$/);
}else{assert.equal(process.env.GITHUB_ACTIONS,'true');assert.equal(source.username,'ccpanel_ci');assert.ok(['127.0.0.1','localhost'].includes(source.hostname));}
const observer=new pg.Client({connectionString:source.href,application_name:'ccpanel_polling_observer',statement_timeout:3000});
const executable=join(dirname(fileURLToPath(import.meta.url)),'api-polling-child.mjs');
const delay=(ms:number)=>new Promise<void>(done=>setTimeout(done,ms));
type BackendMemory={pid:number;privateCleanBytes:number;privateDirtyBytes:number;privateRssBytes:number;pssBytes:number;swapPssBytes:number};
type MemorySample={at:number;backends:BackendMemory[];privateRssBytes:number;pssBytes:number;swapPssBytes:number;pssPlusSwapPssBytes:number};
type MemoryTarget={pid:number;startTicks:string};
let memoryTarget:MemoryTarget|null=null;
async function proc(path:string){const value=await readFile(path,'utf8');assert.ok(Buffer.byteLength(value)<=65536,'proc numeric snapshot too large');return value;}
function startTicks(stat:string){
  const close=stat.lastIndexOf(')');assert.ok(close>0);const fields=stat.slice(close+2).trim().split(/\s+/);
  assert.ok(fields.length>=20&&/^[0-9]+$/.test(fields[19]!));return fields[19]!;
}
function namespacePid(status:string){
  const row=/^NSpid:\s+([0-9\t ]+)$/m.exec(status);assert.ok(row,'kernel namespace PID field unavailable');
  const values=row[1]!.trim().split(/\s+/).map(Number);assert.ok(values.length>=2&&values.every(value=>Number.isSafeInteger(value)&&value>0));return values.at(-1)!;
}
async function guardPostmaster(target:MemoryTarget){
  assert.equal(startTicks(await proc('/proc/'+target.pid+'/stat')),target.startTicks,'postmaster process changed');
  assert.equal(namespacePid(await proc('/proc/'+target.pid+'/status')),1,'fixed container process is not its namespace postmaster');
}
async function identifyMemoryTarget(){
  assert.ok(vps);assert.ok(source.port===''||source.port==='5432');
  // Whitelist only one fixed container's host PID and network IPs. This command
  // runs once before either observation window, never in the sampling loop.
  const {stdout}=await promisify(execFile)('docker',['inspect','--format','{{.State.Pid}}|{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}','ccpanel-postgres-1'],{encoding:'utf8',timeout:5000,maxBuffer:4096});
  const parts=stdout.trim().split('|');assert.equal(parts.length,2);assert.match(parts[0]!,/^[1-9][0-9]{0,9}$/);
  const pid=Number(parts[0]),ips=parts[1]!.trim().split(/\s+/);assert.ok(Number.isSafeInteger(pid)&&pid>0);assert.ok(ips.length>=1&&ips.length<=8&&ips.every(ip=>isIP(ip)===4));assert.ok(ips.includes(source.hostname),'temporary PG URL is not the fixed container IP');
  const target={pid,startTicks:startTicks(await proc('/proc/'+pid+'/stat'))};await guardPostmaster(target);return target;
}
async function backendMemory(pids:number[]):Promise<MemorySample|null>{
  assert.ok(memoryTarget&&vps);if(!pids.length)return null;assert.ok(pids.length<=4);
  await guardPostmaster(memoryTarget);
  const listed=(await proc('/proc/'+memoryTarget.pid+'/task/'+memoryTarget.pid+'/children')).trim();
  const children=listed?listed.split(/\s+/):[];assert.ok(children.length<=128&&children.every(pid=>/^[1-9][0-9]{0,9}$/.test(pid)));
  const mapping=new Map<number,string>();
  for(const hostPid of children)try{
    const pid=namespacePid(await proc('/proc/'+hostPid+'/status'));
    if(pids.includes(pid)){assert.ok(!mapping.has(pid),'ambiguous backend PID mapping');mapping.set(pid,hostPid);}
  }catch(error){if((error as NodeJS.ErrnoException).code!=='ENOENT')throw error;}
  if(mapping.size!==pids.length)return null;
  const backends:BackendMemory[]=[];
  for(const pid of pids){
    const hostPid=mapping.get(pid)!;let text:string;
    try{text=await proc('/proc/'+hostPid+'/smaps_rollup');}catch(error){if((error as NodeJS.ErrnoException).code==='ENOENT')return null;throw error;}
    const bytes=(field:string)=>{
      const matches=[...text.matchAll(new RegExp('^'+field+':\\s+([0-9]+) kB$','gm'))];assert.equal(matches.length,1,'missing numeric backend memory field');
      const value=Number(matches[0]![1])*1024;assert.ok(Number.isSafeInteger(value)&&value>=0);return value;
    };
    const privateCleanBytes=bytes('Private_Clean'),privateDirtyBytes=bytes('Private_Dirty'),pssBytes=bytes('Pss'),swapPssBytes=bytes('SwapPss');
    // Recheck the host-to-container identity after reading; process exit/reuse
    // cannot silently turn an unrelated process into a measured backend.
    try{assert.equal(namespacePid(await proc('/proc/'+hostPid+'/status')),pid);}catch(error){if((error as NodeJS.ErrnoException).code==='ENOENT')return null;throw error;}
    backends.push({pid,privateCleanBytes,privateDirtyBytes,privateRssBytes:privateCleanBytes+privateDirtyBytes,pssBytes,swapPssBytes});
  }
  await guardPostmaster(memoryTarget);
  const sum=(field:'privateRssBytes'|'pssBytes'|'swapPssBytes')=>backends.reduce((total,row)=>total+row[field],0);
  return{at:Date.now(),backends,privateRssBytes:sum('privateRssBytes'),pssBytes:sum('pssBytes'),swapPssBytes:sum('swapPssBytes'),pssPlusSwapPssBytes:sum('pssBytes')+sum('swapPssBytes')};
}
function memoryStats(samples:MemorySample[]){
  const fields=['privateRssBytes','pssBytes','swapPssBytes','pssPlusSwapPssBytes'] as const;
  return Object.fromEntries(fields.map(field=>[field,{min:Math.min(...samples.map(sample=>sample[field])),max:Math.max(...samples.map(sample=>sample[field])),mean:Math.round(samples.reduce((total,sample)=>total+sample[field],0)/samples.length)}])) as Record<typeof fields[number],{min:number;max:number;mean:number}>;
}
async function fingerprints(){
  const names=(await observer.query("SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename")).rows;
  const result:string[]=[];
  for(const row of names){
    const name=row.tablename as string;assert.match(name,/^[a-z_][a-z0-9_]*$/);
    result.push((await observer.query(`SELECT json_build_array('public.${name}',count(*),coalesce(bit_xor(hashtextextended(row_to_json(t)::text,0)),0))::text AS value FROM public.${name} t`)).rows[0].value);
  }
  return result;
}
async function backends(appname:string){
  return(await observer.query(`SELECT a.pid,a.state,
    EXISTS(SELECT 1 FROM pg_locks l WHERE l.pid=a.pid AND l.locktype='advisory'
      AND l.classid=0 AND l.objid=169202617 AND l.objsubid=1 AND l.mode='ShareLock' AND l.granted) AS recovery_lease
    FROM pg_stat_activity a WHERE a.datname=$1 AND a.application_name=$2 ORDER BY a.pid`,[decodeURIComponent(source.pathname.slice(1)),appname])).rows as {pid:number;state:string;recovery_lease:boolean}[];
}
async function run(mode:'baseline'|'staggered'){
  const appname='ccpanel_polling_'+mode+'_'+randomBytes(8).toString('hex');
  const child=spawn(process.execPath,[executable,mode],{env:{...process.env,PGAPPNAME:appname,CCPANEL_API_POLLING_CHILD:'true'},stdio:['ignore','pipe','pipe']});
  let stdout='',stderr='',size=0,forced=false,done=false,observationError:unknown;
  const sample:{at:number;pids:number[];leases:number}[]=[];
  const memorySamples:MemorySample[]=[],memorySkipped:{at:number;reason:'no_backends_or_incomplete_native_mapping'}[]=[];let lastMemory=-Infinity;
  child.stdout.on('data',chunk=>{size+=chunk.length;if(size>131072){forced=true;child.kill('SIGKILL');}else stdout+=chunk.toString();});
  child.stderr.on('data',chunk=>{stderr=(stderr+chunk.toString()).slice(-16384);});
  const completion=new Promise<{code:number|null;signal:NodeJS.Signals|null}>((resolve,reject)=>{child.once('error',reject);child.once('close',(code,signal)=>resolve({code,signal}));});
  const watchdog=setTimeout(()=>{forced=true;child.kill('SIGKILL');},40000);
  const observe=(async()=>{while(!done){
    const rows=await backends(appname),pids=rows.map(row=>row.pid);sample.push({at:Date.now(),pids,leases:rows.filter(row=>row.recovery_lease).length});
    if(vps&&performance.now()-lastMemory>=1000){
      lastMemory=performance.now();const memory=await backendMemory(pids);
      if(memory)memorySamples.push(memory);else memorySkipped.push({at:Date.now(),reason:'no_backends_or_incomplete_native_mapping'});
    }
    await delay(100);
  }})().catch(error=>{observationError=error;done=true;});
  try{
    const terminal=await completion;done=true;await observe;
    assert.equal(observationError===undefined,true,'native backend observation failed');assert.equal(forced,false,'child watchdog fired');
    if(terminal.code!==0){
      // Print only fixed diagnostic enums and bounded numeric runner counters,
      // never an arbitrary driver message, SQL argument, URL or row value.
      let diagnostic:unknown;try{diagnostic=JSON.parse(stderr.trim().split('\n').at(-1)!);}catch{/* A crashed child may not have emitted its own diagnostic. */}
      const value=diagnostic as Record<string,unknown>|undefined;
      const stages=['recovery_lease','empty_source_preflight','runner_start','measurement','runner_drain','measurement_invariants','lease_cleanup','pool_cleanup','final_cleanup_invariants'];
      const safe=value?.code==='api_polling_child_failed'&&stages.includes(String(value.stage))?{stage:value.stage,sqlState:typeof value.sqlState==='string'&&/^[0-9A-Z]{5}$/.test(value.sqlState)?value.sqlState:null,ticks:Object.fromEntries(['query','deletion','replay'].map(name=>{const count=(value.ticks as Record<string,unknown>|undefined)?.[name];return[name,typeof count==='number'&&Number.isSafeInteger(count)&&count>=0&&count<=10000?count:null];}))}:{stage:'no_safe_child_diagnostic'};
      throw Error('native_polling_child_failed:'+JSON.stringify(safe));
    }
    assert.equal(terminal.signal,null);assert.equal(stderr.length,0,'unexpected child stderr');
    const lines=stdout.trim().split('\n');assert.equal(lines.length,1);const proof=JSON.parse(lines[0]!);
    assert.equal(proof.passed,true);assert.equal(proof.mode,mode);assert.equal(proof.surface,vps?'isolated_vps_tooling':'ci');assert.ok(proof.measurementMs>=24900&&proof.measurementMs<30000);
    // Keep observer results away from the child cleanup boundary: query result
    // delivery and millisecond wall-clock timestamps are not atomic together.
    const settled=sample.filter(row=>row.at>=proof.settledWindowStartedAt&&row.at<proof.measurementFinishedAt-100);assert.ok(settled.length>=30,'missing settled native backend observations');assert.ok(settled.every(row=>row.leases===1),'actual shared lease missing');
    const nativePids=[...new Set(sample.flatMap(row=>row.pids))].sort((a,b)=>a-b),instrumentedPids=proof.nativeBackendUsage.map((row:{pid:number})=>row.pid).sort((a:number,b:number)=>a-b);
    assert.deepEqual(nativePids,instrumentedPids,'server PIDs differ from native checkout identities');
    const settledMin=Math.min(...settled.map(row=>row.pids.length)),settledMax=Math.max(...settled.map(row=>row.pids.length));
    assert.equal(settledMin,proof.settledPool.minTotal);assert.equal(settledMax,proof.settledPool.maxTotal);
    const memoryWindow=(row:{at:number})=>row.at>=proof.settledWindowStartedAt&&row.at<proof.measurementFinishedAt-100;
    const settledMemory=memorySamples.filter(memoryWindow),settledMemorySkipped=memorySkipped.filter(memoryWindow);
    if(vps){assert.ok(settledMemory.length>=3,'insufficient settled native memory samples');assert.equal(settledMemorySkipped.length,0,'settled backend memory mapping incomplete');assert.ok(settledMemory.every(row=>row.backends.length>=settledMin&&row.backends.length<=settledMax));}
    const cleanupDeadline=Date.now()+3000;while((await backends(appname)).length&&Date.now()<cleanupDeadline)await delay(20);assert.equal((await backends(appname)).length,0,'server sessions outlived bounded cleanup observation');
    const lock=(await observer.query('SELECT pg_try_advisory_lock(169202617) AS held')).rows[0].held;assert.equal(lock,true,'recovery lease leaked');await observer.query('SELECT pg_advisory_unlock(169202617)');
    return{...proof,actualPgObservation:{samples:sample.length,settledSamples:settled.length,uniqueBackends:nativePids.length,settledMin,settledMax,settledSharedLeaseExactlyOne:true,serverSessionsGone:true,exclusiveRecoveryLockAvailable:true},settledPrivateMemory:vps?{measured:true,units:'bytes',minimumIntervalMs:1000,totalSamples:memorySamples.length,skippedSamples:memorySkipped.length,settledSamples:settledMemory.length,settledSkippedSamples:0,stats:memoryStats(settledMemory),samples:settledMemory}:null,naturalExit:true};
  }finally{
    done=true;clearTimeout(watchdog);
    if(child.exitCode===null&&child.signalCode===null){child.kill('SIGKILL');await completion.catch(()=>{});}
    await observe.catch(()=>{});
  }
}
try{
  await observer.connect();
  if(vps)memoryTarget=await identifyMemoryTarget();
  assert.equal((await observer.query('SELECT max(version) AS version FROM schema_migrations')).rows[0].version,22);
  assert.equal((await observer.query('SELECT count(*)::int AS n FROM tenants')).rows[0].n,0);
  const before=await fingerprints(),baseline=await run('baseline');assert.deepEqual(await fingerprints(),before);
  const staggered=await run('staggered');assert.deepEqual(await fingerprints(),before);
  const baselineConnections=baseline.actualPgObservation.settledMax,staggeredConnections=staggered.actualPgObservation.settledMax;
  const memoryComparison=vps?{measured:true,units:'bytes',fixedContainerIpAndPostmasterVerified:true,privateRssMeanReductionBytes:baseline.settledPrivateMemory!.stats.privateRssBytes.mean-staggered.settledPrivateMemory!.stats.privateRssBytes.mean,pssPlusSwapPssMeanReductionBytes:baseline.settledPrivateMemory!.stats.pssPlusSwapPssBytes.mean-staggered.settledPrivateMemory!.stats.pssPlusSwapPssBytes.mean,sharedPssReallocationPossible:true,wholePostgresOrHostMemorySavingsVerified:false}:null;
  console.log(JSON.stringify({passed:true,surface:vps?'isolated_vps_tooling':'ci',actual_native_pg:true,same_empty_source_database:true,sequential_processes:true,all_three_real_runners:true,period_ms:2000,pool_max:4,recovery_lease_included:true,baseline,staggered,observed_settled_backend_reduction:baselineConnections-staggeredConnections,measured_connection_reduction:baselineConnections>staggeredConnections,vps_private_backend_memory_measured:vps,privateBackendMemoryComparison:memoryComparison,all_source_relation_fingerprints_unchanged:true,recovery_integrity_gates_and_progress_unchanged:true,natural_children_exit:true,server_sessions_and_leases_released:true,mocked_database:false,artificial_query_sleep:false,production_api_ram_saving_verified:false,full_capacity_verified:false}));
}finally{await observer.end();}
