const button=document.querySelector('#start'),output=document.querySelector('#output');
const quantile=(a,p)=>{const sorted=[...a].sort((x,y)=>x-y);return sorted.length?sorted[Math.min(sorted.length-1,Math.floor(sorted.length*p))]:null;};
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
let phase='',longTasks=[],visibilityChanges=0;
document.addEventListener('visibilitychange',()=>{visibilityChanges++;});
if(PerformanceObserver.supportedEntryTypes.includes('longtask'))new PerformanceObserver(list=>{for(const e of list.getEntries())longTasks.push({phase,duration_ms:e.duration});}).observe({type:'longtask',buffered:false});
async function sample(label,seconds,analytics,active){
  phase=label;const sync=[],queued=[],errors=[],start=performance.now();
  let persistent=0,memory=0;
  for(let n=0;n<seconds*20;n++){
    await sleep(Math.max(0,start+n*50-performance.now()));
    const properties={padding:'x'.repeat(720),iteration:n,test:true};
    const before=performance.now(),pending=active?analytics.track('sdk_performance_probe',properties):null;
    sync.push(performance.now()-before);
    if(pending){const result=await pending;queued.push(performance.now()-before);if(result.status==='queued-persistent')persistent++;else if(result.status==='queued-memory')memory++;else errors.push(result.reason);}
    if(n%100===0)output.textContent=JSON.stringify({running:label,completed:n,target:seconds*20},null,2);
  }
  return{phase:label,samples:sync.length,elapsed_ms:performance.now()-start,synchronous_p95_ms:quantile(sync,.95),synchronous_p99_ms:quantile(sync,.99),queued_p95_ms:quantile(queued,.95),queued_p99_ms:quantile(queued,.99),persistent,memory,errors};
}
button.addEventListener('click',async()=>{
  button.disabled=true;longTasks=[];visibilityChanges=0;let analytics;
  try{
    const baseline=await sample('without_sdk',10,null,false);
    const{createAnalytics}=await import('/sdk/ccpanel.js');
    const diagnostics=[],project='benchmark-'+crypto.randomUUID();
    analytics=createAnalytics({endpoint:location.origin+'/__sdk_benchmark_sink__',projectId:project,writeKey:'benchmark-not-a-real-key',consent:true,onError:code=>diagnostics.push(code),fetch:async(_url,options)=>{const body=JSON.parse(options.body);return new Response(JSON.stringify({accepted_event_ids:body.events.map(e=>e.event_id)}),{status:202,headers:{'content-type':'application/json'}});}});
    const idle=await sample('sdk_idle',10,analytics,false);
    const active=await sample('sdk_20_eps',60,analytics,true);
    await analytics.flush();const queue=await analytics.getQueueStatus();await analytics.setConsent(false);
    phase='complete';
    const result={user_agent:navigator.userAgent,hardware_concurrency:navigator.hardwareConcurrency,device_memory_gib:navigator.deviceMemory??null,longtask_supported:PerformanceObserver.supportedEntryTypes.includes('longtask'),visibility_changes:visibilityChanges,transport:'in-memory HTTP 202 stub; no ingestion',baseline,idle,active,long_tasks:longTasks,queue_after_flush:queue,diagnostics,passed:visibilityChanges===0&&active.synchronous_p95_ms<=2&&active.persistent===1200&&active.errors.length===0&&diagnostics.length===0&&queue.count===0};
    window.__ccpanelBenchmark=result;output.textContent=JSON.stringify(result,null,2);
  }catch(e){output.textContent=String(e);}
  finally{analytics?.destroy();button.disabled=false;}
});
