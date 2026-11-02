// Runs the real CLI with synthetic fetch responses; no VPS/account access.
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {createHash} from 'node:crypto';
import {mkdir,mkdtemp,writeFile,readFile,rm} from 'node:fs/promises';
import path from 'node:path';
import {pathToFileURL} from 'node:url';
import {build} from 'esbuild';
await mkdir('.local',{recursive:true});const root=path.resolve('.local'),dir=await mkdtemp(path.join(root,'load-accounting-'));
const preload=path.join(dir,'fake-fetch.mjs');
const legacyEnv={...process.env};delete legacyEnv.CCPANEL_LOAD_RUN_ID;
const unleased={mode:'unleased',run_id:null,lease_ms:null};
const artifactNames=['load-check-public.mjs','load-evidence.mjs','load-control-lease.mjs','load-stop-reader.mjs'];
const expectedArtifacts=Object.fromEntries(await Promise.all(artifactNames.map(async name=>[name,createHash('sha256').update(await readFile(path.join('scripts',name))).digest('hex')])));
try{
  const verifier=path.join(dir,'workload-report.mjs');
  await build({entryPoints:['scripts/capacity-workload-report.ts'],outfile:verifier,bundle:true,platform:'node',target:'node22',format:'esm',packages:'external'});
  const {capacityWorkloadReport}=await import(pathToFileURL(verifier).href);
  await writeFile(preload,`
import {writeFile,readFile} from 'node:fs/promises';
import {writeFileSync} from 'node:fs';
import assert from 'node:assert/strict';
const rows=new Map(),bytes=[],slow=process.env.CCPANEL_ACCOUNTING_CASE==='negative';let posts=0;
if(process.env.CCPANEL_ACCOUNTING_CASE==='early_wake'){
 const nativeSetTimeout=globalThis.setTimeout,observations=[];
 // Delegate to real native timers, with an intentional early wake. Keep the
 // real clock, interval scheduling, AbortSignal budgets and timer handles.
 globalThis.setTimeout=function(callback,delay,...args){
  if(typeof delay!=='number'||delay<100)return nativeSetTimeout(callback,delay,...args);
  const started=performance.now(),delegated=delay-50;
  return nativeSetTimeout(function(...values){observations.push({requested_ms:delay,delegated_ms:delegated,elapsed_ms:performance.now()-started});return Reflect.apply(callback,this,values);},delegated,...args);
 };
 process.once('exit',()=>writeFileSync(process.env.CCPANEL_ACCOUNTING_TIMER_CAPTURE,JSON.stringify({native_timer_delegation:true,observations}),{mode:0o600,flag:'wx'}));
}
const response=(body,status=200,headers={})=>new Response(JSON.stringify(body),{status,headers:{'content-type':'application/json',...headers}});
globalThis.fetch=async(input,options={})=>{
 const url=new URL(String(input));
 if(url.pathname==='/api/auth/login')return response({csrf_token:'synthetic'},200,{'set-cookie':'ccpanel_session=synthetic; HttpOnly'});
 if(url.pathname==='/v1/batch'){
  bytes.push(Buffer.byteLength(options.body,'utf8'));const body=JSON.parse(options.body);posts++;
  const manifest=JSON.parse(await readFile(process.argv[3]+'.manifest.json','utf8'));
  assert.ok(body.events.every(e=>manifest.event_ids.includes(e.event_id)),'every attempted ID must be durable before fetch');
  assert.equal(Object.hasOwn(manifest,'key'),false);assert.equal(Object.hasOwn(manifest,'password'),false);
  if(slow&&posts===1)return response({error:'synthetic'},503);
  for(const event of body.events)rows.set(event.event_id,event);
  await writeFile(process.env.CCPANEL_ACCOUNTING_CAPTURE,JSON.stringify({bytes,ids:[...rows.keys()]}));
  return response({accepted_event_ids:body.events.map(event=>event.event_id)},202);
 }
 if(url.pathname.endsWith('/events'))return response({events:[...rows.values()].sort((a,b)=>b.event_time.localeCompare(a.event_time)).slice(Number(url.searchParams.get('page'))*50,Number(url.searchParams.get('page'))*50+50)});
 if(url.pathname.endsWith('/trends')){if(slow)await new Promise(resolve=>setTimeout(resolve,11000));return response({series:[]});}
 throw new Error('unexpected_synthetic_request');
};`);
  const config=path.join(dir,'config.json');await writeFile(config,JSON.stringify({origin:'https://load.invalid',tenant:'synthetic',project:'synthetic',email:'fake@integration.invalid',password:'synthetic',key:'非ASCII合成密钥'}));
  const run=mode=>new Promise((resolve,reject)=>{
    const output=path.join(dir,mode+'.json'),capture=path.join(dir,mode+'-capture.json');
    const timerCapture=path.join(dir,mode+'-timers.json');
    const child=spawn(process.execPath,['--import',pathToFileURL(preload).href,'scripts/load-check-public.mjs',config,output,'11'],{env:{...legacyEnv,CCPANEL_ACCOUNTING_CASE:mode,CCPANEL_ACCOUNTING_CAPTURE:capture,CCPANEL_ACCOUNTING_TIMER_CAPTURE:timerCapture},stdio:['ignore','pipe','pipe']});
    let error='',log='';child.stdout.on('data',chunk=>{log+=chunk;});child.stderr.on('data',chunk=>{error+=chunk;});
    const timer=setTimeout(()=>child.kill(),55000);
    child.on('error',reject);child.on('close',async code=>{clearTimeout(timer);try{
      assert.equal(code,mode==='negative'?1:0,error+log);
      const result=JSON.parse(await readFile(output,'utf8')),captured=JSON.parse(await readFile(capture,'utf8')),w=result.workload;
      const manifest=JSON.parse(await readFile(output+'.manifest.json','utf8'));assert.equal(manifest.status,'completed');assert.deepEqual(manifest.event_ids,result.event_ids);assert.equal(result.termination.state,'completed');assert.deepEqual(result.artifacts,manifest.artifacts);assert.deepEqual(result.artifacts,expectedArtifacts);assert.equal(Object.keys(manifest.artifacts).length,4);assert.ok(Object.values(manifest.artifacts).every(v=>/^[a-f0-9]{64}$/.test(v)));assert.equal(result.confirmed_event_ids.length,result.acknowledged);assert.deepEqual(manifest.control,unleased);assert.deepEqual(result.control,unleased);
      assert.equal(result.generated,11);assert.equal(result.acknowledged,11);assert.deepEqual([...result.event_ids].sort(),captured.ids.sort());
      assert.equal(w.ingest_attempts,captured.bytes.length);assert.equal(w.ingest_attempts,w.ingest_accepted_requests+w.ingest_failed_requests);
      assert.equal(w.ingest_body_bytes_total,captured.bytes.reduce((a,b)=>a+b,0));assert.equal(w.ingest_body_bytes_min,Math.min(...captured.bytes));assert.equal(w.ingest_body_bytes_max,Math.max(...captured.bytes));assert.equal(w.ingest_body_bytes_mean,w.ingest_body_bytes_total/w.ingest_attempts);
      assert.equal(w.query_opportunities,w.query_attempts+w.query_skipped_busy);assert.equal(w.query_attempts,w.query_successes+w.query_failures);assert.ok(w.visibility_requests>0);
      assert.equal(result.passed,mode!=='negative');assert.equal(w.query_schedule_coverage,mode!=='negative');
      const verified=capacityWorkloadReport(result,11);assert.equal(verified.evidenceValid,true,JSON.stringify(verified));assert.equal(verified.dataLoadPass,mode!=='negative');
      if(mode==='early_wake'){
        const timers=JSON.parse(await readFile(timerCapture,'utf8'));assert.equal(timers.native_timer_delegation,true);assert.ok(timers.observations.some(row=>row.elapsed_ms<row.requested_ms),'a real callback must actually wake before its requested deadline');assert.ok(timers.observations.every(row=>row.delegated_ms===row.requested_ms-50));
        assert.ok(Date.parse(result.completed_at)-Date.parse(result.started_at)>=11000,'normal completion must recompute an early-woken deadline');
        const shortened=capacityWorkloadReport({...result,completed_at:new Date(Date.parse(result.started_at)+10999).toISOString(),duration_seconds:11},11);assert.equal(shortened.evidenceValid,false);assert.ok(shortened.reasons.includes('invalid_timing'),'one millisecond short remains invalid');
      }
      if(mode==='negative'){assert.equal(w.ingest_failed_requests,1);assert.ok(w.ingest_attempted_events>11);assert.equal(w.query_skipped_busy,1);}else{assert.equal(w.ingest_failed_requests,0);assert.equal(w.query_skipped_busy,0);assert.equal(w.ingest_attempted_events,11);}
      resolve();
    }catch(error){reject(error);}});
  });
  async function runAbort(mode){
    const output=path.join(dir,mode+'.json'),capture=path.join(dir,mode+'-capture.json');
    const child=spawn(process.execPath,['--import',pathToFileURL(preload).href,'scripts/load-check-public.mjs',config,output,'11'],{env:{...legacyEnv,CCPANEL_ACCOUNTING_CASE:mode,CCPANEL_ACCOUNTING_CAPTURE:capture},stdio:['ignore','pipe','pipe']});
    let stderr='',stdout='',exitCode;child.stdout.on('data',b=>{stdout+=b;});child.stderr.on('data',b=>{stderr=(stderr+b).slice(-12000);});
    const ended=new Promise((resolve,reject)=>{child.once('error',reject);child.once('close',code=>{exitCode=code;resolve();});});
    const timer=setTimeout(()=>child.kill('SIGKILL'),25000);
    try{
      const deadline=Date.now()+10000;let captured;
      while(Date.now()<deadline){try{captured=JSON.parse(await readFile(capture,'utf8'));if(captured.ids.length>=2)break;}catch{}await new Promise(r=>setTimeout(r,50));}
      assert.ok(captured?.ids.length>=2,'fixture must actually transmit before interruption');
      if(mode==='kill')child.kill('SIGKILL');else if(mode==='signal'||mode==='sigint')child.kill(mode==='signal'?'SIGTERM':'SIGINT');else await writeFile(output+'.stop',mode==='invalid_stop'?'x'.repeat(2048):JSON.stringify({reason:'resource_safety'}),{mode:0o600});
      await ended;
      const bytes=await readFile(output+'.manifest.json'),manifest=JSON.parse(bytes);
      assert.deepEqual(manifest.control,unleased);assert.deepEqual(manifest.artifacts,expectedArtifacts);
      captured=JSON.parse(await readFile(capture,'utf8'));assert.ok(captured.ids.every(id=>manifest.event_ids.includes(id)));assert.equal(new Set(manifest.event_ids).size,manifest.event_ids.length);
      if(mode==='kill'){
        assert.notEqual(exitCode,0);await assert.rejects(readFile(output),{code:'ENOENT'});assert.equal(manifest.status,'running');
      }else{
        assert.equal(exitCode,1,stderr);const result=JSON.parse(await readFile(output,'utf8'));assert.equal(result.passed,false);assert.equal(result.termination.state,'aborted');assert.equal(result.termination.reason,mode==='signal'?'signal_sigterm':mode==='sigint'?'signal_sigint':mode==='invalid_stop'?'invalid_stop_request':'resource_safety');assert.ok(result.generated>=2&&result.generated<11);assert.deepEqual(result.event_ids,manifest.event_ids);assert.equal(result.confirmed_event_ids.length,result.acknowledged);assert.deepEqual([...result.confirmed_event_ids].sort(),captured.ids.sort());
        assert.deepEqual(result.control,unleased);assert.deepEqual(result.artifacts,expectedArtifacts);const observed=stdout.trim().split('\n').map(line=>JSON.parse(line)).filter(v=>v.kind==='workload_abort_observed');assert.equal(observed.length,1);assert.equal(observed[0].reason,result.termination.reason);assert.equal(observed[0].run_id,null);assert.equal(observed[0].pid,child.pid);assert.ok(Number.isFinite(observed[0].monotonic_ms));
        const report=capacityWorkloadReport(result,11);assert.equal(report.dataLoadPass,false);assert.ok(report.reasons.includes('aborted_workload'));
      }
      const restart=spawn(process.execPath,['--import',pathToFileURL(preload).href,'scripts/load-check-public.mjs',config,output,'11'],{env:{...legacyEnv,CCPANEL_ACCOUNTING_CASE:mode,CCPANEL_ACCOUNTING_CAPTURE:capture},stdio:'ignore'});
      assert.equal(await new Promise((resolve,reject)=>{restart.once('error',reject);restart.once('close',resolve);}),1,'an old evidence namespace cannot restart');assert.deepEqual(await readFile(output+'.manifest.json'),bytes);
    }finally{clearTimeout(timer);if(child.exitCode===null&&child.signalCode===null){child.kill('SIGKILL');await ended;}}
  }
  const results=await Promise.allSettled([run('positive'),run('negative'),run('early_wake'),...['stop','invalid_stop','kill',...(process.platform==='win32'?[]:['signal','sigint'])].map(runAbort)]);for(const result of results)if(result.status==='rejected')throw result.reason;
  console.log(JSON.stringify({passed:true,real_cli:true,synthetic_fetch:true,actual_native_early_timer_wake:true,normal_deadline_recomputed:true,one_millisecond_short_still_rejected:true,exact_utf8_bytes:true,retries_counted:true,busy_queries_counted:true,missed_query_schedule_fails:true,write_ahead_ids:true,cooperative_abort:true,hard_kill_manifest:true,restart_refused:true,explicit_unleased_control:true,exact_four_artifact_hashes:true,first_abort_observed_once:true,posix_sigint_sigterm_checked:process.platform!=='win32'}));
}finally{assert.equal(path.dirname(dir),root);assert.ok(path.basename(dir).startsWith('load-accounting-'));await rm(dir,{recursive:true,force:true});}
