// Real generator children, native HTTP/fetch/signals and actual filesystem
// operations. The URL adapter only redirects the synthetic HTTPS origin to
// loopback HTTP. This does not exercise TLS, SSH, a VPS or a capacity window.
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {createServer} from 'node:http';
import {createHash,randomUUID} from 'node:crypto';
import {mkdtemp,mkdir,writeFile,readFile,lstat,readlink,symlink,unlink,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {resolve,join,dirname,basename} from 'node:path';
import {pathToFileURL} from 'node:url';
import {performance} from 'node:perf_hooks';
import {publishFirstStop} from './load-first-stop.mjs';

const directory=await mkdtemp(join(tmpdir(),'ccpanel-native-control-'));
const preload=join(directory,'native-http-adapter.mjs'),active=new Set(),checks=[],observations=[];
const names=['load-check-public.mjs','load-evidence.mjs','load-control-lease.mjs','load-stop-reader.mjs'];
const sleep=ms=>new Promise(done=>setTimeout(done,ms));
const digest=bytes=>createHash('sha256').update(bytes).digest('hex');
const artifactHashes=Object.fromEntries(await Promise.all(names.map(async name=>[name,digest(await readFile(new URL('./'+name,import.meta.url)))])));
async function until(predicate,label,timeout=10000){
  const end=performance.now()+timeout;
  while(performance.now()<end){const value=await predicate();if(value)return value;await sleep(10);}
  throw Error('native_integration_timeout:'+label);
}
const expectedControl=leased=>({mode:leased?'stdin_monotonic_lease':'unleased',run_id:leased?leased:null,lease_ms:leased?5000:null});
async function optionalJson(path){try{return JSON.parse(await readFile(path,'utf8'));}catch(error){if(error.code==='ENOENT')return null;throw error;}}

await writeFile(preload,`
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import {readFileSync} from 'node:fs';
import {syncBuiltinESMExports} from 'node:module';
import {createHash} from 'node:crypto';
import {performance} from 'node:perf_hooks';
const nativeOpen=fs.open,nativeFetch=globalThis.fetch,output=process.argv[3];
const manifestPrefix=output+'.manifest.json',synced=new Set();let handles=0,syncs=0,sequence=0;
const hash=bytes=>createHash('sha256').update(bytes).digest('hex');
const emit=value=>console.log(JSON.stringify(value));
fs.open=async(...args)=>{
 const handle=await nativeOpen(...args),path=String(args[0]);
 if(!path.startsWith(manifestPrefix))return handle;
 handles++;let closed=false,fileSynced=false;
 const initial=path===manifestPrefix,release=process.env.CCPANEL_NATIVE_INITIAL_MANIFEST_RELEASE;
 return new Proxy(handle,{get(target,key){
  if(key==='writeFile'&&initial&&release)return async(...writeArgs)=>{
   // A real wx open already exposed this empty file. Hold the real write so
   // the parent can observe the exact startup race without mocking I/O success.
   emit({kind:'native_initial_manifest_write_held'});
   const end=performance.now()+5000;
   while(true){
    try{await fs.lstat(release);break;}catch(error){if(error.code!=='ENOENT')throw error;}
    if(performance.now()>=end)throw Error('native_initial_manifest_gate_timeout');
    await new Promise(done=>setTimeout(done,10));
   }
   return target.writeFile(...writeArgs);
  };
  if(key==='sync')return async()=>{await target.sync();synced.add(hash(await fs.readFile(path)));syncs++;fileSynced=true;};
  if(key==='close')return async()=>{
   let succeeded=false;
   try{const value=await target.close();succeeded=true;return value;}
   finally{if(!closed){closed=true;handles--;if(initial&&fileSynced&&succeeded)emit({kind:'native_manifest_file_synced_closed',pathKind:'initial'});}}
  };
  const value=Reflect.get(target,key,target);return typeof value==='function'?value.bind(target):value;
 }});
};
syncBuiltinESMExports();
globalThis.fetch=(input,options={})=>{
 const url=new URL(String(input));assert.equal(url.origin,'https://load.invalid','test adapter refuses other origins');
 assert.ok(options.signal instanceof AbortSignal,'native request must keep its original signal');
 const id=++sequence,started=performance.now();let proof;
 if(url.pathname==='/v1/batch'){
  const raw=readFileSync(manifestPrefix),manifest=JSON.parse(raw),body=JSON.parse(options.body);
  assert.ok(synced.has(hash(raw)),'actual manifest fsync must finish before native POST');
  assert.ok(body.events.every(event=>manifest.event_ids.includes(event.event_id)),'every POST ID must already be durable');
  assert.equal(Object.hasOwn(manifest,'key'),false);assert.equal(Object.hasOwn(manifest,'password'),false);
  proof={manifest_fsync_verified:true,event_ids:body.events.map(event=>event.event_id)};
 }
 emit({kind:'native_fetch_started',id,path:url.pathname,query:url.search,method:options.method??'GET',...proof});
 const timedOut=()=>emit({kind:'native_signal_aborted',id,path:url.pathname,elapsed_ms:performance.now()-started,name:options.signal.reason?.name});
 options.signal.addEventListener('abort',timedOut,{once:true});
 const local=new URL(url.pathname+url.search,process.env.CCPANEL_NATIVE_HTTP_ORIGIN);
 return nativeFetch(local,options).then(response=>{
  emit({kind:'native_fetch_settled',id,path:url.pathname,elapsed_ms:performance.now()-started,status:response.status});return response;
 },error=>{
  emit({kind:'native_fetch_settled',id,path:url.pathname,elapsed_ms:performance.now()-started,error_name:error.name});throw error;
 }).finally(()=>options.signal.removeEventListener('abort',timedOut));
};
process.once('exit',()=>emit({kind:'native_test_exit',manifest_handles:handles,manifest_syncs:syncs}));
`,{mode:0o600,flag:'wx'});

async function launch(name,{leased=false,preStop=false,loginStatus=200,holdLogin=false,holdPost=false,holdTrend=false,drainPages=false,holdInitialManifestWrite=false,namespace}={}){
  const path=join(directory,name);await mkdir(path,{mode:0o700});
  const output=join(path,'result.json'),config=join(path,'config.json'),runId=leased?'NATIVE_CONTROL_'+randomUUID().replaceAll('-',''):null;
  await writeFile(config,JSON.stringify({origin:'https://load.invalid',tenant:'synthetic',project:'synthetic',email:'native@integration.invalid',password:'synthetic-only',key:'synthetic-only'}),{mode:0o600,flag:'wx'});
  await writeFile(join(path,'operator-note'),'preserve-private-note',{mode:0o600,flag:'wx'});
  if(preStop)assert.deepEqual(await publishFirstStop(output+'.stop','resource_safety'),{status:'published'});
  let existing;
  if(namespace){
    const target=namespace.target==='result'?output:output+'.progress',missing=join(path,'never-created-link-target');
    if(namespace.type==='file')await writeFile(target,'existing-private-evidence\n',{mode:0o600,flag:'wx'});else await symlink(missing,target,'file');
    existing={...namespace,path:target,missing,identity:await lstat(target,{bigint:true}),bytes:namespace.type==='file'?await readFile(target):await readlink(target)};
  }
  const state={rows:new Map(),visibleIds:new Set(),requests:[],holds:new Map(),firstPost:null,drainStarted:false,serverErrors:[],responded:new Set()};
  const reply=(response,body,status=200)=>{state.responded.add(response);response.writeHead(status,{'content-type':'application/json',...(status===200&&body.login?{'set-cookie':'ccpanel_session=synthetic; HttpOnly'}:{})});response.end(JSON.stringify(body));};
  const decoys=Array.from({length:50},()=>({event_id:randomUUID(),event_time:new Date(Date.now()+3600000).toISOString()}));
  const server=createServer((request,response)=>void(async()=>{
    const url=new URL(request.url,'http://127.0.0.1');state.requests.push({path:url.pathname,query:url.search,at:performance.now()});
    if(url.pathname==='/api/auth/login'){
      if(holdLogin){state.holds.set('login',response);return;}
      reply(response,{login:true},loginStatus);return;
    }
    if(url.pathname==='/v1/batch'){
      let raw='';for await(const chunk of request){raw+=chunk;if(Buffer.byteLength(raw)>65536)throw Error('native_fixture_body_too_large');}
      const body=JSON.parse(raw);assert.ok(body.events.length>0);
      const manifest=JSON.parse(await readFile(output+'.manifest.json','utf8'));
      assert.ok(body.events.every(event=>manifest.event_ids.includes(event.event_id)),'server independently reads the original manifest before accepting IDs');
      state.firstPost??=performance.now();
      if(holdPost){state.holds.set('post',response);return;}
      for(const event of body.events)state.rows.set(event.event_id,event);
      reply(response,{accepted_event_ids:body.events.map(event=>event.event_id)},202);return;
    }
    if(url.pathname.endsWith('/trends')){
      if(holdTrend&&!state.holds.has('trend')){state.holds.set('trend',response);return;}
      reply(response,{series:[]});return;
    }
    if(url.pathname.endsWith('/events')){
      const page=Number(url.searchParams.get('page'));
      if(drainPages){
        if(page===0&&state.rows.size===10&&performance.now()-state.firstPost>=10000){state.drainStarted=true;reply(response,{events:decoys});return;}
        if(state.drainStarted&&page===1){state.holds.set('page1',response);return;}
        reply(response,{events:[]});return;
      }
      const events=[...state.rows.values()].sort((a,b)=>b.event_time.localeCompare(a.event_time)).slice(page*50,page*50+50);for(const event of events)state.visibleIds.add(event.event_id);reply(response,{events});return;
    }
    throw Error('unexpected_native_fixture_route');
  })().catch(error=>{state.serverErrors.push(error);if(!response.headersSent)reply(response,{error:'native_fixture_failed'},500);else response.destroy();}));
  await new Promise((accept,reject)=>{server.once('error',reject);server.listen(0,'127.0.0.1',accept);});
  const initialManifestRelease=holdInitialManifestWrite?join(path,'initial-manifest-release'):null;
  const env={...process.env,CCPANEL_NATIVE_HTTP_ORIGIN:'http://127.0.0.1:'+server.address().port};delete env.CCPANEL_LOAD_RUN_ID;delete env.CCPANEL_NATIVE_INITIAL_MANIFEST_RELEASE;if(runId)env.CCPANEL_LOAD_RUN_ID=runId;if(initialManifestRelease)env.CCPANEL_NATIVE_INITIAL_MANIFEST_RELEASE=initialManifestRelease;
  const child=spawn(process.execPath,['--import',pathToFileURL(preload).href,resolve('scripts/load-check-public.mjs'),config,output,'10'],{env,stdio:['pipe','pipe','pipe']});
  const value={name,path,output,child,server,state,runId,existing,initialManifestRelease,records:[],stderr:'',closed:false,watchdogFired:false,stdinEndedByParent:false,heartbeat:null,sequence:0,started:performance.now(),reply};
  active.add(value);let partial='',size=0;
  child.stdout.on('data',chunk=>{
    size+=chunk.length;if(size>512*1024){child.kill('SIGKILL');return;}
    partial+=chunk;let index;
    while((index=partial.indexOf('\n'))!==-1){const line=partial.slice(0,index);partial=partial.slice(index+1);try{value.records.push({value:JSON.parse(line),at:performance.now()});}catch{value.stderr+='invalid_child_json_line\n';}}
  });
  child.stderr.on('data',chunk=>{value.stderr=(value.stderr+chunk).slice(-16384);});child.stdin.on('error',()=>{});
  value.done=new Promise((accept,reject)=>{
    child.once('error',reject);
    child.once('close',(code,signal)=>{value.closed=true;value.code=code;value.signal=signal;value.closedAt=performance.now();clearTimeout(value.watchdog);clearInterval(value.heartbeat);accept(value);});
  });
  value.watchdog=setTimeout(()=>{value.watchdogFired=true;child.kill('SIGKILL');},30000);
  value.frame=()=>{assert.equal(value.closed,false);value.sequence++;child.stdin.write(JSON.stringify({v:1,run_id:runId,seq:String(value.sequence)})+'\n');value.lastFrameAt=performance.now();};
  value.renew=()=>{value.frame();value.heartbeat=setInterval(()=>{if(!value.closed)value.frame();},500);};
  value.stop=async()=>{value.stopAt=performance.now();assert.deepEqual(await publishFirstStop(output+'.stop','resource_safety'),{status:'published'});};
  value.eof=()=>{value.eofAt=performance.now();value.stdinEndedByParent=true;clearInterval(value.heartbeat);child.stdin.end();};
  value.observed=()=>value.records.find(record=>record.value.kind==='workload_abort_observed');
  value.initialManifest=async()=>{
    // This observes only actual file fsync+close, not completion of the entire
    // publisher/directory fsync, nor admission by the real control lease.
    await until(()=>value.records.some(record=>record.value.kind==='native_manifest_file_synced_closed'&&record.value.pathKind==='initial'),'initial manifest file sync and close');
    const manifest=JSON.parse(await readFile(output+'.manifest.json','utf8'));
    assert.equal(manifest.status,'initializing');return manifest;
  };
  return value;
}

async function common(value,{reason,controlCode,passed=false,zero=false,loginFailure=false}={}){
  await value.done;assert.equal(value.watchdogFired,false,value.name);assert.equal(value.signal,null,value.name);assert.equal(value.code,passed?0:1,value.stderr);
  assert.equal(value.state.serverErrors.length,0,value.state.serverErrors.map(error=>error.message).join(','));
  const exit=value.records.find(record=>record.value.kind==='native_test_exit');assert.ok(exit,'real child exit handler must run before original native close');assert.equal(exit.value.manifest_handles,0);
  const manifest=await optionalJson(value.output+'.manifest.json');assert.ok(manifest);assert.deepEqual(manifest.control,expectedControl(value.runId));
  assert.deepEqual(manifest.artifacts,artifactHashes,'bind all four actual source files');
  const started=value.records.filter(record=>record.value.kind==='native_fetch_started');
  if(loginFailure){
    assert.ok(value.stderr.includes('fixture_login_failed'));assert.equal(await optionalJson(value.output),null);assert.equal(value.observed(),undefined);assert.equal(started.length,1);assert.equal(started[0].value.path,'/api/auth/login');assert.ok(value.closedAt-value.started<4000,'login failure must close before lease expiry with writer still open');
  }else{
    assert.equal(value.stderr,'');const result=await optionalJson(value.output);assert.ok(result);assert.equal(result.passed,passed);assert.deepEqual(result.control,expectedControl(value.runId));assert.deepEqual(result.artifacts,artifactHashes);assert.deepEqual(result.event_ids,manifest.event_ids);assert.equal(result.confirmed_event_ids.length,result.acknowledged);assert.equal(manifest.status,passed?'completed':'aborted');assert.equal(result.termination.state,passed?'completed':'aborted');assert.equal(result.termination.reason,passed?null:reason);
    const observed=value.records.filter(record=>record.value.kind==='workload_abort_observed');assert.equal(observed.length,passed?0:1,'first observed abort must be latched once');
    if(!passed){
      const receipt=observed[0];assert.equal(receipt.value.reason,reason);assert.equal(receipt.value.run_id,value.runId);assert.equal(receipt.value.pid,value.child.pid);assert.ok(Number.isFinite(receipt.value.monotonic_ms)&&receipt.value.monotonic_ms>=0);assert.ok(Number.isFinite(Date.parse(receipt.value.observed_at)));
      if(controlCode)assert.equal(receipt.value.control_code,controlCode);else assert.equal(Object.hasOwn(receipt.value,'control_code'),false);
      const at=value.records.indexOf(receipt);assert.equal(value.records.slice(at+1).some(record=>record.value.kind==='native_fetch_started'),false,'no new native dispatch after abort observation');
      const trigger=value.stopAt??value.eofAt;if(trigger!==undefined)assert.ok(receipt.at-trigger<=5000,'healthy local observation must occur within five seconds; native request drain has a separate bound');
    }
    if(zero){assert.equal(result.generated,0);assert.equal(started.some(record=>record.value.path!=='/api/auth/login'),false);}
    if(passed){assert.equal(result.generated,10);assert.equal(result.acknowledged,10);assert.equal(result.not_yet_visible,0);assert.equal(value.stdinEndedByParent,false,'normal leased close must not require the parent ending its pipe');}
    for(const request of started.filter(record=>record.value.path==='/v1/batch')){assert.equal(request.value.manifest_fsync_verified,true);assert.ok(request.value.event_ids.every(id=>manifest.event_ids.includes(id)));}
  }
  assert.equal(await readFile(join(value.path,'operator-note'),'utf8'),'preserve-private-note');
  const receipt=value.observed(),trigger=value.stopAt??value.eofAt,result=await optionalJson(value.output);
  observations.push({name:value.name,exit_code:value.code,exit_signal:value.signal,watchdog_fired:value.watchdogFired,native_close_elapsed_ms:Math.ceil(value.closedAt-value.started),control_mode:manifest.control.mode,parent_ended_stdin:value.stdinEndedByParent,generated:result?.generated??null,result_passed:result?.passed??null,abort_reason:receipt?.value.reason??null,control_code:receipt?.value.control_code??null,local_abort_observation_ms:receipt&&trigger!==undefined?Math.ceil(receipt.at-trigger):null,manifest_syncs:exit.value.manifest_syncs,post_dispatches:started.filter(record=>record.value.path==='/v1/batch').length,native_signal_timeouts:value.records.filter(record=>record.value.kind==='native_signal_aborted').map(record=>({path:record.value.path,elapsed_ms:Math.ceil(record.value.elapsed_ms),name:record.value.name}))});
  checks.push(value.name);
}

async function dispose(value){
  clearInterval(value.heartbeat);if(!value.closed){value.child.kill('SIGKILL');await value.done;}clearTimeout(value.watchdog);
  value.server.closeAllConnections();await new Promise(done=>value.server.close(done));active.delete(value);
}
async function scenario(name,options,action,verification){
  const value=await launch(name,options);
  try{await action(value);await common(value,verification);}
  catch(error){console.error(JSON.stringify({kind:'native_integration_failure',name,closed:value.closed,code:value.code,signal:value.signal,watchdog:value.watchdogFired,records:value.records.slice(-12),stderr:value.stderr}));throw error;}
  finally{await dispose(value);}
}

async function namespaceScenario(target,type){
  const name='existing_'+target+'_'+type+'_refused_before_manifest',value=await launch(name,{namespace:{target,type}});
  try{
    await value.done;assert.equal(value.watchdogFired,false);assert.equal(value.signal,null);assert.equal(value.code,1);assert.ok(value.stderr.includes('workload_output_exists'));
    assert.equal(value.state.requests.length,0,'namespace refusal must precede real login/server activity');assert.equal(value.state.serverErrors.length,0);
    assert.equal(value.records.some(record=>record.value.kind==='native_fetch_started'),false,'namespace refusal must precede every native fetch dispatch');assert.equal(value.observed(),undefined,'startup refusal is not an aborted workload result');
    await assert.rejects(lstat(value.output+'.manifest.json'),{code:'ENOENT'});
    await assert.rejects(lstat(target==='result'?value.output+'.progress':value.output),{code:'ENOENT'});
    const before=value.existing,after=await lstat(before.path,{bigint:true});assert.equal(after.dev,before.identity.dev);assert.equal(after.ino,before.identity.ino);
    if(type==='file'){assert.equal(after.isFile(),true);assert.deepEqual(await readFile(before.path),before.bytes);}
    else{assert.equal(after.isSymbolicLink(),true);assert.equal(await readlink(before.path),before.bytes);await assert.rejects(lstat(before.missing),{code:'ENOENT'});}
    assert.equal(await readFile(join(value.path,'operator-note'),'utf8'),'preserve-private-note');
    const exit=value.records.find(record=>record.value.kind==='native_test_exit');assert.ok(exit);assert.equal(exit.value.manifest_handles,0);assert.equal(exit.value.manifest_syncs,0);
    observations.push({name,scope:'startup_namespace_refusal',existing_target:target,existing_type:type,exit_code:value.code,exit_signal:value.signal,watchdog_fired:value.watchdogFired,native_close_elapsed_ms:Math.ceil(value.closedAt-value.started),manifest_created:false,native_dispatches:0,original_entry_unchanged:true});checks.push(name);
  }catch(error){console.error(JSON.stringify({kind:'native_namespace_refusal_failure',name,closed:value.closed,code:value.code,signal:value.signal,records:value.records,stderr:value.stderr}));throw error;}
  finally{await dispose(value);}
}

try{
  const probe=join(directory,'symlink-permission-probe');let danglingSupported=false;
  try{await symlink(join(directory,'absent-probe-target'),probe,'file');danglingSupported=true;await unlink(probe);}
  catch(error){if(process.platform!=='win32'||!['EPERM','EACCES'].includes(error.code))throw error;}
  const scenarios=[
    ()=>scenario('initial_stop_prevents_login',{preStop:true,leased:true},async value=>{await value.done;assert.equal(value.state.requests.length,0);},{reason:'resource_safety',zero:true}),
    ()=>scenario('stop_while_waiting_for_first_lease',{leased:true},async value=>{
      await value.initialManifest();await sleep(250);assert.equal(value.state.requests.length,0);await value.stop();await until(()=>value.observed(),'pre-ready independent stop');assert.equal(value.state.requests.length,0);
    },{reason:'resource_safety',zero:true}),
    ()=>scenario('lease_ready_gate_then_native_eof',{leased:true,holdInitialManifestWrite:true},async value=>{
      await until(()=>value.records.some(record=>record.value.kind==='native_initial_manifest_write_held'),'actual initial wx open before native write');
      const empty=await readFile(value.output+'.manifest.json');assert.equal(empty.length,0,'real initial exclusive path can exist before its write');assert.throws(()=>JSON.parse(empty.toString('utf8')),SyntaxError);
      assert.equal(value.state.requests.length,0);assert.equal(value.records.some(record=>record.value.kind==='native_fetch_started'||record.value.kind==='native_manifest_file_synced_closed'),false,'empty initial manifest is neither a file-completion nor request-admission barrier');
      await writeFile(value.initialManifestRelease,'release-real-write',{mode:0o600,flag:'wx'});
      await value.initialManifest();await sleep(350);assert.equal(value.state.requests.length,0,'no login before an actual valid lease frame');value.frame();
      await until(()=>value.state.rows.size>=1,'leased native POST');value.eof();
    },{reason:'control_lost',controlCode:'control_eof'}),
    ()=>scenario('normal_leased_native_completion',{leased:true},async value=>{value.renew();},{passed:true}),
    ()=>scenario('active_lease_timeout_cannot_complete',{leased:true},async value=>{
      value.frame();await until(()=>value.state.rows.size>=1,'native POST before lease expiry');await until(()=>value.observed(),'actual lease timeout',9000);
      assert.equal(value.observed().value.control_code,'control_timeout');assert.ok(value.observed().at-value.lastFrameAt>=4500);assert.ok(value.observed().at-value.lastFrameAt<=8000);
    },{reason:'control_lost',controlCode:'control_timeout'}),
    ()=>scenario('stop_during_native_delayed_login',{leased:true,holdLogin:true},async value=>{
      value.renew();
      await until(()=>value.state.holds.has('login'),'held native login');await value.stop();await until(()=>value.observed(),'independent login stop');await sleep(150);
      value.eof();await sleep(100);assert.equal(value.records.some(record=>record.value.kind==='native_fetch_settled'&&record.value.path==='/api/auth/login'),false,'stop must preserve original login request instead of canceling it');value.reply(value.state.holds.get('login'),{login:true});
    },{reason:'resource_safety',zero:true}),
    ()=>scenario('stop_preserves_native_post_12s_timeout',{holdPost:true},async value=>{
      await until(()=>value.state.holds.has('post'),'held native POST');await value.stop();await until(()=>value.observed(),'independent POST stop');await value.done;
      const signal=value.records.find(record=>record.value.kind==='native_signal_aborted'&&record.value.path==='/v1/batch');assert.ok(signal);assert.equal(signal.value.name,'TimeoutError');assert.ok(signal.value.elapsed_ms>=11500&&signal.value.elapsed_ms<=15000,'keep the original native twelve-second signal');
    },{reason:'resource_safety'}),
    ()=>scenario('stop_between_native_drain_pages',{drainPages:true},async value=>{
      await until(()=>value.state.holds.has('page1'),'post-generation multi-page drain',16000);await value.stop();await until(()=>value.observed(),'independent page stop');await sleep(150);
      const page=value.records.find(record=>record.value.kind==='native_fetch_started'&&record.value.query==='?event_name=capacity_public_probe&page=1');assert.ok(page);assert.equal(value.records.some(record=>record.value.kind==='native_fetch_settled'&&record.value.id===page.value.id),false);value.reply(value.state.holds.get('page1'),{events:Array.from({length:50},()=>({event_id:randomUUID(),event_time:new Date(Date.now()+3600000).toISOString()}))});
    },{reason:'resource_safety'}),
    ()=>scenario('stop_during_native_final_settlement',{holdTrend:true},async value=>{
      await until(()=>value.state.rows.size===10,'all ten native events');await until(()=>performance.now()-value.state.firstPost>=10500,'after generation deadline',5000);
      const manifest=await optionalJson(value.output+'.manifest.json');assert.equal(manifest.event_ids.length,10);assert.ok(manifest.event_ids.every(id=>value.state.visibleIds.has(id)),'all generated IDs have an actual visibility response before settlement');
      const pending=value.records.filter(record=>record.value.kind==='native_fetch_started'&&!value.records.some(other=>other.value.kind==='native_fetch_settled'&&other.value.id===record.value.id));assert.equal(pending.length,1);assert.ok(pending[0].value.path.endsWith('/trends'));
      await value.stop();await until(()=>value.observed(),'independent final-settlement stop');await value.done;
      const signal=value.records.find(record=>record.value.kind==='native_signal_aborted'&&record.value.path.endsWith('/trends'));assert.ok(signal);assert.equal(signal.value.name,'TimeoutError');assert.ok(signal.value.elapsed_ms>=14500&&signal.value.elapsed_ms<=18000,'keep the original native fifteen-second signal');
    },{reason:'resource_safety'}),
    ()=>scenario('leased_login_failure_releases_control',{leased:true,loginStatus:401},async value=>{value.renew();},{loginFailure:true}),
    ()=>namespaceScenario('result','file'),
    ()=>namespaceScenario('progress','file'),
    ...(danglingSupported?[()=>namespaceScenario('result','dangling_symlink'),()=>namespaceScenario('progress','dangling_symlink')]:[])
  ];
  // Keep the local check small while still exercising independent real clients.
  let next=0;const workers=Array.from({length:3},async()=>{while(next<scenarios.length){const index=next++;await scenarios[index]();}});
  const results=await Promise.allSettled(workers);for(const result of results)if(result.status==='rejected')throw result.reason;
  console.log(JSON.stringify({passed:true,platform:process.platform,node_version:process.version,checks,observations,artifacts:artifactHashes,initial_empty_manifest_observed_before_real_write:true,initial_manifest_barrier_requires_actual_file_sync_and_close:true,initial_manifest_barrier_proves_publisher_complete:false,final_artifact_json_parsing_strict:true,regular_output_and_progress_refusal_checked:true,dangling_output_checked:danglingSupported,dangling_progress_checked:danglingSupported,windows_symlink_permission_skip:process.platform==='win32'&&!danglingSupported,real_original_generator_children:true,original_native_close_observed:true,native_loopback_http:true,native_fetch_and_abort_signals:true,actual_manifest_fsync_before_post:true,all_four_artifact_hashes_verified:true,healthy_local_stop_observation_bound_ms:5000,request_timeouts_ms:[12000,15000],parent_ended_normal_lease_pipe:false,coordinator_hard_kill_exercised:false,orphan_native_terminal_claimed:false,tls_verified:false,vps_accessed:false,full_cross_host_bridge_verified:false,capacity_verified:false}));
}finally{
  await Promise.allSettled([...active].map(dispose));assert.equal(dirname(resolve(directory)),resolve(tmpdir()));assert.ok(basename(directory).startsWith('ccpanel-native-control-'));await rm(directory,{recursive:true,force:true});
}
