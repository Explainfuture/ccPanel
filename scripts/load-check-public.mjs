// Run off-host against trusted HTTPS; no database ports or credentials exposed.
import { readFile,open,stat } from 'node:fs/promises';
import { randomUUID,createHash } from 'node:crypto';
import { performance } from 'node:perf_hooks';
import {publishEvidence} from './load-evidence.mjs';
const [config,output,duration='1800']=process.argv.slice(2),seconds=Number(duration);
if(!config||!output||!Number.isInteger(seconds)||seconds<10||seconds>3600)throw new Error('Usage: node scripts/load-check-public.mjs private-config result-file 10..3600');
const f=JSON.parse(await readFile(config,'utf8')),base=f.origin,root=`${base}/api/tenants/${f.tenant}/projects/${f.project}`;
if(new URL(base).protocol!=='https:')throw new Error('Trusted HTTPS is required');
const artifacts=Object.freeze(Object.fromEntries(await Promise.all(['load-check-public.mjs','load-evidence.mjs'].map(async name=>[name,createHash('sha256').update(await readFile(new URL('./'+name,import.meta.url))).digest('hex')]))));
const ingest=[],visible=[],queries=[],errors=[],pending=new Map(),backlog=[],retryCopies=[],ids=[];
const workload={ingest_attempts:0,ingest_accepted_requests:0,ingest_failed_requests:0,ingest_attempted_events:0,ingest_body_bytes_total:0,ingest_body_bytes_min:null,ingest_body_bytes_max:null,query_opportunities:0,query_attempts:0,query_successes:0,query_failures:0,query_skipped_busy:0,visibility_requests:0};
const accounting=()=>({...workload,ingest_body_bytes_mean:workload.ingest_attempts?workload.ingest_body_bytes_total/workload.ingest_attempts:null,query_schedule_coverage:workload.query_opportunities>0&&workload.query_skipped_busy===0});
const p95=a=>{const s=[...a].sort((a,b)=>a-b);return s.length?s[Math.min(s.length-1,Math.floor(s.length*.95))]:null;};
let abortReason=null,wake,finishing=false;
const abort=reason=>{if(!finishing){abortReason??=reason;wake?.();}};
for(const signal of ['SIGINT','SIGTERM'])process.once(signal,()=>abort('signal_'+signal.toLowerCase()));
const sleep=ms=>abortReason?Promise.resolve():new Promise(resolve=>{const done=()=>{clearTimeout(timer);wake=undefined;resolve();};const timer=setTimeout(done,ms);wake=done;});
const manifest=()=>({version:1,writer_pid:process.pid,artifacts,status:abortReason?'aborted':'running',termination_reason:abortReason,tenant:f.tenant,project:f.project,planned_events:seconds,started_at:startedAt,event_ids:[...ids],confirmed_event_ids:[...confirmedIds]});
const confirmedIds=new Set();
// Own the output namespace before making any requests. Existing evidence must
// never be overwritten by accidentally restarting an old workload.
for(const path of [output,output+'.progress']){try{await stat(path);throw new Error('workload_output_exists');}catch(e){if(e.code!=='ENOENT')throw e;}}
await publishEvidence(output+'.manifest.json',{version:1,writer_pid:process.pid,artifacts,status:'initializing',tenant:f.tenant,project:f.project,planned_events:seconds,event_ids:[],confirmed_event_ids:[]},{exclusive:true});
async function checkStop(){let file;try{file=await open(output+'.stop','r');const buffer=Buffer.alloc(1025),{bytesRead}=await file.read(buffer,0,buffer.length,0);if(bytesRead>1024)throw new Error('stop_file_too_large');const v=JSON.parse(buffer.subarray(0,bytesRead).toString('utf8'));abort(['resource_safety','operator_requested'].includes(v.reason)?v.reason:'invalid_stop_request');}catch(e){if(e.code!=='ENOENT')abort('invalid_stop_request');}finally{await file?.close();}}
const login=await fetch(base+'/api/auth/login',{method:'POST',headers:{origin:base,'content-type':'application/json'},body:JSON.stringify({email:f.email,password:f.password}),signal:AbortSignal.timeout(15000)});
if(login.status!==200)throw new Error('fixture_login_failed');const cookie=login.headers.get('set-cookie').split(';')[0];
let acknowledged=0,duplicates=0,sending=false,observing=false,querying=false,stopped=false,maxScheduleLag=0;
let sender=Promise.resolve(),observer=Promise.resolve(),querier=Promise.resolve();
async function post(events){
  const before=performance.now();
  const body=JSON.stringify({write_key:f.key,schema_version:1,events}),bytes=Buffer.byteLength(body,'utf8');
  workload.ingest_attempts++;workload.ingest_attempted_events+=events.length;workload.ingest_body_bytes_total+=bytes;workload.ingest_body_bytes_min=workload.ingest_body_bytes_min===null?bytes:Math.min(workload.ingest_body_bytes_min,bytes);workload.ingest_body_bytes_max=workload.ingest_body_bytes_max===null?bytes:Math.max(workload.ingest_body_bytes_max,bytes);
  try{
    const r=await fetch(base+'/v1/batch',{method:'POST',headers:{origin:base,'content-type':'text/plain'},body,signal:AbortSignal.timeout(12000)});
    if(r.status!==202)throw new Error('ingest_'+r.status);const value=await r.json();
    if(!Array.isArray(value.accepted_event_ids)||value.accepted_event_ids.length!==events.length||events.some(e=>!value.accepted_event_ids.includes(e.event_id)))throw new Error('invalid_ack');
    for(const event of events)confirmedIds.add(event.event_id);workload.ingest_accepted_requests++;ingest.push(performance.now()-before);return true;
  }catch(e){workload.ingest_failed_requests++;errors.push(e.message);return false;}
}
function pump(){
  if(sending||stopped||abortReason||!backlog.length&&!retryCopies.length)return;
  sending=true;
  const isNew=backlog.length>0,batch=isNew?backlog.slice(0,20):[retryCopies[0]];
  sender=post(batch).then(ok=>{if(ok){if(isNew){acknowledged+=batch.length;backlog.splice(0,batch.length);}else{duplicates++;retryCopies.shift();}}}).finally(()=>{sending=false;if(!stopped&&!abortReason&&(backlog.length||retryCopies.length))setTimeout(pump,250);});
}
function observe(){
  if(observing||stopped||abortReason||!pending.size)return observer;
  observing=true;
  observer=(async()=>{
    for(let page=0;page<10&&pending.size&&!abortReason;page++){
      workload.visibility_requests++;
      const r=await fetch(root+'/events?event_name=capacity_public_probe&page='+page,{headers:{cookie},signal:AbortSignal.timeout(15000)});
      if(r.status!==200)throw new Error('visibility_'+r.status);const body=await r.json(),now=Date.now();
      for(const e of body.events){if(pending.has(e.event_id)){visible.push(now-pending.get(e.event_id));pending.delete(e.event_id);}}
      if(body.events.length<50||!pending.size)break;
      if(Date.parse(body.events.at(-1).event_time)<Math.min(...pending.values()))break;
    }
  })().catch(e=>errors.push(e.message)).finally(()=>{observing=false;});
  return observer;
}
function queryTrend(){
  if(stopped||abortReason)return;
  workload.query_opportunities++;
  if(querying){workload.query_skipped_busy++;return;}
  workload.query_attempts++;
  querying=true;const before=performance.now();
  querier=fetch(root+'/trends',{headers:{cookie},signal:AbortSignal.timeout(15000)}).then(async r=>{if(r.status!==200)throw new Error('query_'+r.status);await r.json();workload.query_successes++;queries.push(performance.now()-before);}).catch(e=>{workload.query_failures++;errors.push(e.message);}).finally(()=>{querying=false;});
}
const start=Date.now(),startedAt=new Date(start).toISOString();
await publishEvidence(output+'.manifest.json',manifest());
const progress=async()=>{const value={elapsed_seconds:Math.round((Date.now()-start)/1000),generated:ids.length,acknowledged,backlog:backlog.length,not_yet_visible:pending.size,max_generation_lag_ms:maxScheduleLag,ingest_p95_ms:p95(ingest),visible_p95_ms:p95(visible),query_p95_ms:p95(queries),errors:errors.length,workload:accounting()};await publishEvidence(output+'.progress',value);console.log(JSON.stringify(value));};
const observeTimer=setInterval(()=>void observe(),1000),queryTimer=setInterval(queryTrend,10000),progressTimer=setInterval(()=>void progress().catch(()=>abort('evidence_write_failed')),60000);
await checkStop();queryTrend();
for(let n=0;n<seconds;n++){
  await checkStop();if(abortReason)break;
  await sleep(Math.max(0,start+n*1000-Date.now()));
  await checkStop();if(abortReason)break;
  maxScheduleLag=Math.max(maxScheduleLag,Date.now()-(start+n*1000));
  const event={event_id:randomUUID(),event_name:'capacity_public_probe',distinct_id:'probe-'+(n%20),event_time:new Date().toISOString(),properties:{padding:'x'.repeat(720),test:true},context:{source:'off-host-capacity-probe'},schema_version:1};
  ids.push(event.event_id);
  try{await publishEvidence(output+'.manifest.json',manifest());}catch{ids.pop();abort('evidence_write_failed');break;}
  pending.set(event.event_id,Date.now());backlog.push(event);if(n>0&&n%50===0)retryCopies.push(event);pump();
}
const generationSeconds=(Date.now()-start)/1000;
await sleep(Math.max(0,start+seconds*1000-Date.now()));
await checkStop();
const deadline=Date.now()+30000;
while(!abortReason&&(pending.size||backlog.length||retryCopies.length||sending)&&Date.now()<deadline){await checkStop();if(abortReason)break;pump();await observe();await sleep(250);}
clearInterval(observeTimer);clearInterval(queryTimer);clearInterval(progressTimer);stopped=true;
await Promise.allSettled([sender,observer,querier]);
finishing=true;
const workloadResult=accounting();
const result={tenant:f.tenant,project:f.project,started_at:startedAt,completed_at:new Date().toISOString(),duration_seconds:Math.round((Date.now()-start)/1000),generation_seconds:generationSeconds,max_generation_lag_ms:maxScheduleLag,target_eps:1,generated:ids.length,acknowledged,duplicate_requests:duplicates,backlog:backlog.length,not_yet_visible:pending.size,ingest_p95_ms:p95(ingest),observed_visible_p95_ms:p95(visible),query_p95_ms:p95(queries),errors,passed:!abortReason&&ids.length===seconds&&maxScheduleLag<1000&&acknowledged===seconds&&!pending.size&&!backlog.length&&!retryCopies.length&&!errors.length&&p95(ingest)<1000&&p95(visible)<10000,event_ids:ids};
result.workload=workloadResult;
result.termination={state:abortReason?'aborted':'completed',reason:abortReason,planned_events:seconds};
result.confirmed_event_ids=[...confirmedIds];
result.artifacts=artifacts;
result.passed=result.passed&&workloadResult.query_schedule_coverage&&workloadResult.query_successes>0;
await publishEvidence(output+'.manifest.json',{...manifest(),status:abortReason?'aborted':'completed',completed_at:result.completed_at});
await publishEvidence(output,result,{exclusive:true});console.log(JSON.stringify({...result,event_ids:undefined,confirmed_event_ids:undefined}));if(!result.passed)process.exitCode=1;
