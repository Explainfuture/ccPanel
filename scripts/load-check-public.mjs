// Run off-host against trusted HTTPS; no database ports or credentials exposed.
import { readFile,writeFile } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import { performance } from 'node:perf_hooks';
const [config,output,duration='1800']=process.argv.slice(2),seconds=Number(duration);
if(!config||!output||!Number.isInteger(seconds)||seconds<10||seconds>3600)throw new Error('Usage: node scripts/load-check-public.mjs private-config result-file 10..3600');
const f=JSON.parse(await readFile(config,'utf8')),base=f.origin,root=`${base}/api/tenants/${f.tenant}/projects/${f.project}`;
if(new URL(base).protocol!=='https:')throw new Error('Trusted HTTPS is required');
const ingest=[],visible=[],queries=[],errors=[],pending=new Map(),backlog=[],retryCopies=[],ids=[];
const p95=a=>{const s=[...a].sort((a,b)=>a-b);return s.length?s[Math.min(s.length-1,Math.floor(s.length*.95))]:null;};
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
const login=await fetch(base+'/api/auth/login',{method:'POST',headers:{origin:base,'content-type':'application/json'},body:JSON.stringify({email:f.email,password:f.password}),signal:AbortSignal.timeout(15000)});
if(login.status!==200)throw new Error('fixture_login_failed');const cookie=login.headers.get('set-cookie').split(';')[0];
let acknowledged=0,duplicates=0,sending=false,observing=false,querying=false,stopped=false,maxScheduleLag=0;
let sender=Promise.resolve(),observer=Promise.resolve(),querier=Promise.resolve();
async function post(events){
  const before=performance.now();
  try{
    const r=await fetch(base+'/v1/batch',{method:'POST',headers:{origin:base,'content-type':'text/plain'},body:JSON.stringify({write_key:f.key,schema_version:1,events}),signal:AbortSignal.timeout(12000)});
    if(r.status!==202)throw new Error('ingest_'+r.status);const value=await r.json();
    if(!Array.isArray(value.accepted_event_ids)||value.accepted_event_ids.length!==events.length||events.some(e=>!value.accepted_event_ids.includes(e.event_id)))throw new Error('invalid_ack');
    ingest.push(performance.now()-before);return true;
  }catch(e){errors.push(e.message);return false;}
}
function pump(){
  if(sending||stopped||!backlog.length&&!retryCopies.length)return;
  sending=true;
  const isNew=backlog.length>0,batch=isNew?backlog.slice(0,20):[retryCopies[0]];
  sender=post(batch).then(ok=>{if(ok){if(isNew){acknowledged+=batch.length;backlog.splice(0,batch.length);}else{duplicates++;retryCopies.shift();}}}).finally(()=>{sending=false;if(!stopped&&(backlog.length||retryCopies.length))setTimeout(pump,250);});
}
function observe(){
  if(observing||stopped||!pending.size)return observer;
  observing=true;
  observer=(async()=>{
    for(let page=0;page<10&&pending.size;page++){
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
  if(querying||stopped)return;
  querying=true;const before=performance.now();
  querier=fetch(root+'/trends',{headers:{cookie},signal:AbortSignal.timeout(15000)}).then(async r=>{if(r.status!==200)throw new Error('query_'+r.status);await r.json();queries.push(performance.now()-before);}).catch(e=>errors.push(e.message)).finally(()=>{querying=false;});
}
const start=Date.now(),startedAt=new Date(start).toISOString();
const progress=async()=>{const value={elapsed_seconds:Math.round((Date.now()-start)/1000),generated:ids.length,acknowledged,backlog:backlog.length,not_yet_visible:pending.size,max_generation_lag_ms:maxScheduleLag,ingest_p95_ms:p95(ingest),visible_p95_ms:p95(visible),query_p95_ms:p95(queries),errors:errors.length};await writeFile(output+'.progress',JSON.stringify(value),{mode:0o600});console.log(JSON.stringify(value));};
const observeTimer=setInterval(()=>void observe(),1000),queryTimer=setInterval(queryTrend,10000),progressTimer=setInterval(()=>void progress(),60000);
queryTrend();
for(let n=0;n<seconds;n++){
  await sleep(Math.max(0,start+n*1000-Date.now()));
  maxScheduleLag=Math.max(maxScheduleLag,Date.now()-(start+n*1000));
  const event={event_id:randomUUID(),event_name:'capacity_public_probe',distinct_id:'probe-'+(n%20),event_time:new Date().toISOString(),properties:{padding:'x'.repeat(720),test:true},context:{source:'off-host-capacity-probe'},schema_version:1};
  ids.push(event.event_id);pending.set(event.event_id,Date.now());backlog.push(event);if(n>0&&n%50===0)retryCopies.push(event);pump();
}
const generationSeconds=(Date.now()-start)/1000;
await sleep(Math.max(0,start+seconds*1000-Date.now()));
const deadline=Date.now()+30000;
while((pending.size||backlog.length||retryCopies.length||sending)&&Date.now()<deadline){pump();await observe();await sleep(250);}
clearInterval(observeTimer);clearInterval(queryTimer);clearInterval(progressTimer);stopped=true;
await Promise.allSettled([sender,observer,querier]);
const result={tenant:f.tenant,project:f.project,started_at:startedAt,completed_at:new Date().toISOString(),duration_seconds:Math.round((Date.now()-start)/1000),generation_seconds:generationSeconds,max_generation_lag_ms:maxScheduleLag,target_eps:1,generated:ids.length,acknowledged,duplicate_requests:duplicates,backlog:backlog.length,not_yet_visible:pending.size,ingest_p95_ms:p95(ingest),observed_visible_p95_ms:p95(visible),query_p95_ms:p95(queries),errors,passed:ids.length===seconds&&maxScheduleLag<1000&&acknowledged===seconds&&!pending.size&&!backlog.length&&!retryCopies.length&&!errors.length&&p95(ingest)<1000&&p95(visible)<10000,event_ids:ids};
await writeFile(output,JSON.stringify(result,null,2),{mode:0o600});console.log(JSON.stringify({...result,event_ids:undefined}));if(!result.passed)process.exitCode=1;
