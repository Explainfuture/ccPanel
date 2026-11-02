// Actual local processes and pipes. The transport is explicitly NOT SSH, the
// producer is a native control fixture, and no workload/capacity is accepted.
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {randomBytes,randomUUID,createHash} from 'node:crypto';
import {readFile,writeFile,mkdir,mkdtemp,realpath,rm,open,stat} from 'node:fs/promises';
import {createInterface} from 'node:readline';
import {join,resolve,dirname,basename} from 'node:path';
import {tmpdir} from 'node:os';
import {fileURLToPath} from 'node:url';
import {attachControlLease} from './load-control-lease.mjs';
import {readStopReason} from './load-stop-reader.mjs';
import {publishEvidence} from './load-evidence.mjs';
import {runCapacityWindow,validateWindowConfig} from './run-capacity-window.mjs';
const self=fileURLToPath(import.meta.url),sha=b=>createHash('sha256').update(b).digest('hex'),bytes=v=>Buffer.from(JSON.stringify(v)+'\n'),sleep=ms=>new Promise(resolve=>setTimeout(resolve,ms));
const names=['load-check-public.mjs','load-evidence.mjs','load-control-lease.mjs','load-stop-reader.mjs'];
async function store(path,data){const f=await open(path,'wx',0o600);try{await f.writeFile(data);await f.sync();}finally{await f.close();}}

if(process.argv[2]==='--peer'){
  const root=process.argv[3],data=JSON.parse(await readFile(join(root,'case.json'),'utf8')),mode=data.mode,cfg=data.cfg;
  process.stdout.on('error',()=>{});let completed=false,aborted=null,terminal=null,freezeHash=null,feedReplies=0,admission=false;
  const owner={v:1,kind:'owner',run_id:cfg.run_id,run_sha256:cfg.remote.run_sha256,unit:`ccpanel-capacity-window225-${cfg.run_id}.service`,invocation_id:'1'.repeat(32),main_pid:process.pid,start_ticks:'1',controller_sha256:cfg.remote.script_sha256,lock_custody:'pid1_fdstore'},ownerRaw=bytes(owner),ownerHash=sha(ownerRaw);
  await store(join(root,'owner.json'),ownerRaw);
  const ready={...data.ready,owner_sha256:ownerHash};await store(join(root,'ready.json'),bytes(ready));const readyHash=sha(bytes(ready));
  process.stdout.write(ownerRaw);
  const samplerRaw=bytes({fixture:true,native_sampler_verified:false});await store(join(root,'sampler_terminal.json'),samplerRaw);
  const input=createInterface({input:process.stdin,crlfDelay:Infinity});
  const end=()=>{input.close();process.stdin.pause();process.stdin.unref?.();clearInterval(collector);process.exitCode=aborted?1:0;};
  async function freeze(){
    if(freezeHash)return;
    const value={v:1,kind:'freeze',run_id:cfg.run_id,run_sha256:cfg.remote.run_sha256,owner_sha256:ownerHash,controller_outcome:aborted?'aborted':'completed',client_terminal_sha256:terminal?.artifacts.find(a=>a.role==='client_terminal')?.sha256??null,sampler_terminal_sha256:sha(samplerRaw),artifacts:[...(terminal?.artifacts??[]),{role:'sampler_terminal',bytes:samplerRaw.length,sha256:sha(samplerRaw)}],cleanup:aborted?'deferred':'completed',capacity_verified:false,source_authenticated:false,latest_state_verified:false,recovery_verified:false};
    if(mode==='freeze_wrong_run')value.run_sha256='0'.repeat(64);
    if(mode==='freeze_wrong_terminal')value.client_terminal_sha256='0'.repeat(64);
    if(mode==='freeze_duplicate_role')value.artifacts.push({...value.artifacts[0]});
    if(mode==='freeze_future_terminal')value.artifacts.push({role:'controller_terminal',bytes:1,sha256:'0'.repeat(64)});
    if(mode==='complete_remote_inventory'){
      for(const [role,definition]of Object.entries(data.freezeRoles)){
        if(value.artifacts.some(ref=>ref.role===role))continue;
        // Native fixture metadata only, not a claim to have captured live PG,
        // host or sampler data. Every referenced fixture file really exists.
        let raw;
        try{raw=await readFile(join(root,definition.name));}catch(error){if(error.code!=='ENOENT')throw error;raw=role==='resource_baseline'?Buffer.alloc(131072,32):bytes({fixture_only:true,role});await store(join(root,definition.name),raw);}
        value.artifacts.push({role,bytes:raw.length,sha256:sha(raw)});
      }
      assert.deepEqual(value.artifacts.map(ref=>ref.role).sort(),Object.keys(data.freezeRoles).sort());
    }
    const raw=bytes(value);await store(join(root,'freeze.json'),raw);freezeHash=sha(raw);
  }
  const collector=setInterval(()=>{
    if(!terminal||freezeHash)return;
    void Promise.all(terminal.artifacts.map(async ref=>{const raw=await readFile(join(root,'uploaded-'+ref.role));return raw.length===ref.bytes&&sha(raw)===ref.sha256;})).then(results=>{if(results.every(Boolean))return freeze();}).catch(()=>{});
  },10);
  const reply=request=>{
    const active=request.client!==null;if(active)feedReplies++;
    if(mode==='resource_abort'&&active&&feedReplies>=2)aborted??={code:'resource_memory',observed_at_us:String(BigInt(Date.now())*1000n),phase:'running'};
    const nativeOwner={unit:owner.unit,invocation_id:owner.invocation_id,main_pid:owner.main_pid,start_ticks:owner.start_ticks};
    if(mode==='owner_swap'&&active)nativeOwner.main_pid++;
    return{v:1,kind:'status',run_id:cfg.run_id,owner_sha256:ownerHash,seq:request.seq,nonce:mode==='wrong_nonce'&&active?'0'.repeat(32):request.nonce,server_us:String(BigInt(Date.now())*1000n),phase:freezeHash?(aborted?'aborted':'completed'):aborted?'settling':active?'running':'ready',native_owner:nativeOwner,ready_sha256:readyHash,freeze_sha256:freezeHash,may_feed_client:active&&!aborted&&!freezeHash,abort:aborted,sampler_elapsed_ms:'1000',post_window_work_allowed:mode==='delayed_admission'?admission:true};
  };
  input.on('line',line=>{
    const request=JSON.parse(line);
    if(request.kind==='abort')aborted??={code:request.code,observed_at_us:String(BigInt(Date.now())*1000n),phase:'running'};
    if(request.kind==='client_terminal'){terminal=request;if(mode==='delayed_admission')setTimeout(()=>{admission=true;void store(join(root,'admission.json'),bytes({admitted:true}));},300);}
    const send=()=>{const response=reply(request);process.stdout.write(bytes(response));if(response.freeze_sha256){completed=true;setTimeout(end,20);}};
    if(mode==='late_reply'&&request.client){setTimeout(send,2300);return;}
    send();
    if(mode==='duplicate_reply')process.stdout.write(bytes(reply(request)));
  });
  input.on('close',()=>{if(!completed){aborted??={code:'bridge_stale',observed_at_us:String(BigInt(Date.now())*1000n),phase:'running'};clearInterval(collector);process.stdin.pause();process.stdin.unref?.();process.exitCode=1;}});
  if(mode==='premature_exit'){clearInterval(collector);input.close();process.stdin.pause();process.stdin.unref?.();process.exitCode=0;}
}else if(process.argv[2]==='--producer'){
  const root=process.argv[3],directory=process.argv[4],data=JSON.parse(await readFile(join(root,'case.json'),'utf8')),cfg=data.cfg,out=join(directory,'client-result.json'),control={mode:'stdin_monotonic_lease',run_id:cfg.run_id,lease_ms:5000},fixture=JSON.parse(await readFile(join(directory,'fixture.json'),'utf8'));
  let reason=null,resolveAbort;const aborted=new Promise(resolve=>{resolveAbort=resolve;});
  const manifest={version:1,writer_pid:process.pid,status:'initializing',artifacts:cfg.client.artifacts,control,event_ids:[],confirmed_event_ids:[]};
  await publishEvidence(out+'.manifest.json',manifest,{exclusive:true});
  process.stdout.write(bytes({kind:'workload_initialized',run_id:cfg.run_id,writer_pid:process.pid,manifest_sha256:data.mode==='bad_initialized'?'0'.repeat(64):sha(await readFile(out+'.manifest.json'))}));
  const abort=value=>{if(reason)return;reason=value;process.stdout.write(bytes({kind:'workload_abort_observed',reason,run_id:cfg.run_id,pid:process.pid,observed_at:new Date().toISOString(),monotonic_ms:performance.now()}));resolveAbort();};
  const lease=attachControlLease({input:process.stdin,runId:cfg.run_id,onAbort:()=>abort('control_lost')});
  const check=()=>void readStopReason(out+'.stop').then(value=>{if(value)abort(value);},()=>abort('invalid_stop_request'));
  const timer=setInterval(check,25);check();let ready=false;
  try{
    await Promise.race([lease.ready.then(()=>{ready=true;},()=>{}),aborted]);
    if(!reason)await Promise.race([sleep(data.mode==='resource_abort'?2400:120),aborted]);
    const state=reason?'aborted':'completed';await publishEvidence(out+'.manifest.json',{...manifest,status:state});
    if(data.mode==='malformed_result')await store(out,Buffer.from('{'));
    else if(data.mode!=='missing_result')await publishEvidence(out,{tenant:fixture.tenant,project:fixture.project,artifacts:cfg.client.artifacts,control,termination:{state,reason,planned_events:1800},lease_ready:ready,passed:!reason,event_ids:[]},{exclusive:true});
    if(reason)process.exitCode=1;
  }finally{clearInterval(timer);lease.close();}
}else{
  assert.equal(process.argv.length,2);const parent=await realpath(tmpdir()),root=await mkdtemp(join(parent,'ccpanel-coordinator-'));await mkdir(join(root,'cases'),{mode:0o700});
  const checks=[];const known='SHA256:'+Buffer.alloc(32,1).toString('base64').replace(/=+$/,'');
  try{
    const remoteSource=await readFile('scripts/capacity-window-remote.py','utf8'),table=/^FILES = \{([\s\S]*?)\}\r?\nUPLOAD =/m.exec(remoteSource);assert.ok(table,'Read the actual remote FILES contract without executing Python');
    const freezeRoles={};
    for(const match of table[1].matchAll(/'([a-z_]+)':\('([^']+)',([0-9*]+)\)/g)){
      const [_,role,name,expression]=match,cap=expression.split('*').map(Number).reduce((a,b)=>a*b,1);assert.ok(Number.isSafeInteger(cap)&&cap>0);
      if(!['freeze','fixture','controller_terminal'].includes(role))freezeRoles[role]={name,cap};
    }
    assert.equal(Object.keys(freezeRoles).length,26,'Cover all current freeze-emittable roles, not a shortened hand-written inventory');
    const artifacts=Object.fromEntries(await Promise.all(names.map(async name=>[name,sha(await readFile(join('scripts',name)))])));
    const operator={controller:'e'.repeat(64),coordinator:sha(await readFile('scripts/run-capacity-window.mjs')),sampler:'d'.repeat(64),host_state:'c'.repeat(64),capture:'b'.repeat(64),db_operator:'a'.repeat(64),first_stop:sha(await readFile('scripts/load-first-stop.mjs')),load_check:artifacts[names[0]],load_evidence:artifacts[names[1]],load_lease:artifacts[names[2]],load_stop_reader:artifacts[names[3]]};
    async function run(mode){
      const folder=join(root,'cases',mode);await mkdir(folder,{mode:0o700});
      const cfg={version:1,run_id:randomBytes(16).toString('hex'),ssh:{executable:process.execPath,config_file:join(folder,'ssh.conf'),known_hosts_file:join(folder,'known_hosts'),identity_file:join(folder,'identity'),host_alias:'native-fixture',host_key_sha256:known},remote:{script:'/opt/ccpanel/shared/window225-tools.abcdef/controller.py',state_dir:'/opt/ccpanel/shared/window225.abcdef',script_sha256:operator.controller,run_sha256:'0'.repeat(64)},client:{directory:resolve('scripts'),artifacts},expected_origin:'https://capacity.invalid'};
      const configuration='9'.repeat(64),run={v:1,kind:'run',run_id:cfg.run_id,release:'m4-polling-014',schema:22,operator_commit:'8'.repeat(40),profile:'window225_fixed_v1',expected_origin:cfg.expected_origin,expected_runtime_sha256:configuration,tools_sha256:operator};
      const runRaw=bytes(run);cfg.remote.run_sha256=sha(runRaw);await store(join(folder,'run.json'),runRaw);
      const fixture={tenant:randomUUID(),project:randomUUID(),user:randomUUID(),key:'PRIVATE_SYNTHETIC_WRITE_KEY',email:'control@integration.invalid',password:'PRIVATE_SYNTHETIC_PASSWORD',origin:cfg.expected_origin,created_at:new Date().toISOString(),run_id:cfg.run_id,write_key_id:randomUUID()},fixtureRaw=bytes(fixture);await store(join(folder,'fixture.json'),fixtureRaw);
      const binding={version:1,kind:'capacity_window_binding',run_id:cfg.run_id,planned_events:1800,product:{release:'m4-polling-014',source_commit:'b9b213f2313151c1e21be30e924a9af3a1f84a82',schema:22,api_sha256:'1'.repeat(64),worker_sha256:'2'.repeat(64),sdk_sha256:'3'.repeat(64),compose_sha256:'4'.repeat(64)},containers:Object.fromEntries(['api','worker','kafka','postgres'].map((name,index)=>[name,{id:String(index+1).repeat(64),started_at:new Date().toISOString(),restart_count:0,image_id:'sha256:'+'5'.repeat(64)}])),tools:{db_operator_sha256:operator.db_operator,generator_sha256:operator.load_check,evidence_sha256:operator.load_evidence,lease_sha256:operator.load_lease,reader_sha256:operator.load_stop_reader,sampler_sha256:operator.sampler,controller_sha256:operator.controller},namespace:{events:'fixture.events',dlq:'fixture.dlq',group:'fixture.group'},configuration_sha256:configuration},bindingRaw=bytes(binding);await store(join(folder,'binding.json'),bindingRaw);
      const ready={v:1,kind:'ready',run_id:cfg.run_id,owner_sha256:null,binding_sha256:sha(bindingRaw),configuration_sha256:configuration,business_before_sha256:'6'.repeat(64),monitor_before_sha256:'7'.repeat(64),sampler_owner_sha256:'8'.repeat(64),fixture_ref:{role:'fixture',bytes:fixtureRaw.length,sha256:sha(fixtureRaw)},captured_at_us:String(BigInt(Date.now())*1000n)};
      await store(join(folder,'case.json'),bytes({cfg,ready,mode,freezeRoles}));let producerCount=0;
      const transportFactory=async()=>({
        process:spawn(process.execPath,[self,'--peer',folder],{stdio:['pipe','pipe','pipe'],windowsHide:true}),actualSSH:false,
        async readArtifact(role,path){let raw=await readFile(join(folder,role+'.json'));if(mode==='corrupt_binding'&&role==='binding')raw=Buffer.concat([raw,Buffer.from(' ')]);await store(path,raw);},
        async receiveArtifact(role,path,ref,ownerHash){if(mode==='delayed_admission')assert.equal(JSON.parse(await readFile(join(folder,'admission.json'),'utf8')).admitted,true,'No artifact transfer before remote admission');const raw=await readFile(path);assert.equal(raw.length,ref.bytes);assert.equal(sha(raw),ref.sha256);await store(join(folder,'uploaded-'+role),raw);return{v:1,kind:'artifact_received',run_id:cfg.run_id,owner_sha256:ownerHash,role,...ref};}
      });
      const generatorFactory=async(_cfg,directory)=>{producerCount++;return spawn(process.execPath,[self,'--producer',folder,directory],{stdio:['pipe','pipe','pipe'],windowsHide:true});};
      const directory=join(folder,'evidence'),result=await runCapacityWindow(cfg,directory,{testOnly:true,transportFactory,generatorFactory});
      assert.equal(result.actual_ssh,false);assert.equal(result.actual_generator,false);assert.equal(result.actual_tls,false);assert.equal(result.node_runtime.child_node_options_removed,false);assert.equal(result.node_runtime.child_tls_reject_unauthorized,null);assert.equal(result.capacity_verified,false);assert.equal(result.source_authenticated,false);
      const raw=await readFile(join(directory,'coordinator-freeze.json'));assert.deepEqual(JSON.parse(raw),result);
      for(const [name,ref]of Object.entries(result.artifacts)){const content=await readFile(join(directory,name));assert.equal(content.length,ref.bytes,name);assert.equal(sha(content),ref.sha256,name);}
      if(mode==='good'||mode==='delayed_admission'||mode==='complete_remote_inventory'){
        assert.equal(result.controller_completed,true);assert.equal(result.client_native_close.code,0);assert.equal(result.ssh_native_close.code,0);assert.equal(result.client_watchdog_fired,false);
        const terminal=JSON.parse(await readFile(join(directory,'client-terminal.json')));assert.equal(terminal.pid,JSON.parse(await readFile(join(directory,'client-close.json'))).pid);assert.equal(terminal.close.code,0);assert.equal(terminal.close.signal,null);assert.equal(terminal.observed_at,result.client_native_close.observed_at);assert.deepEqual(terminal.result,result.artifacts['client-result.json']);assert.deepEqual(terminal.manifest,result.artifacts['client-result.json.manifest.json']);assert.deepEqual(terminal.artifacts,artifacts);
        await assert.rejects(runCapacityWindow(cfg,directory,{testOnly:true,transportFactory,generatorFactory}),{code:'EEXIST'});assert.deepEqual(await readFile(join(directory,'coordinator-freeze.json')),raw);assert.equal(producerCount,1);
      }else{
        assert.equal(result.controller_completed,false,mode);assert.ok(result.first_abort||result.failure,mode);
        if(['corrupt_binding','premature_exit','duplicate_reply'].includes(mode))assert.equal(producerCount,0,mode);
        if(['wrong_nonce','owner_swap','late_reply','bad_initialized'].includes(mode)){const value=JSON.parse(await readFile(join(directory,'client-result.json')));assert.equal(value.lease_ready,false,mode);}
        if(mode==='malformed_result'){assert.equal(result.failure,'invalid_final_client_json');assert.equal(result.artifacts['client-result.json'].sha256,sha(Buffer.from('{')));assert.equal(result.client_native_close.code,0);}
      }
      checks.push(mode);
    }
    for(const mode of ['good','wrong_nonce','owner_swap','late_reply','bad_initialized','malformed_result','missing_result','corrupt_binding','premature_exit','duplicate_reply','resource_abort','freeze_wrong_run','freeze_wrong_terminal','freeze_duplicate_role','freeze_future_terminal','delayed_admission','complete_remote_inventory'])await run(mode);
    assert.throws(()=>validateWindowConfig({version:1}),/invalid_window_config/);
    console.log(JSON.stringify({passed:true,checks,real_native_transport_process:true,actual_ssh:false,actual_generator:false,actual_tls:false,raw_hashes_and_original_native_closes:true,old_namespace_restart_refused:true,full_workload_verified:false,capacity_verified:false,platform:process.platform}));
  }finally{const target=resolve(root);assert.equal(dirname(target),parent);assert.ok(basename(target).startsWith('ccpanel-coordinator-'));await rm(target,{recursive:true,force:true});}
}
