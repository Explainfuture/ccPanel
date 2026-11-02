// Real local processes and dedicated pipes; no HTTP, SSH, VPS or stop files.
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {Readable,PassThrough} from 'node:stream';
import {performance} from 'node:perf_hooks';
import {openSync,writeFileSync,fsyncSync,closeSync} from 'node:fs';
import {mkdtemp,readFile,rm,realpath} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join,dirname,basename,resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {attachControlLease} from './load-control-lease.mjs';

const self=fileURLToPath(import.meta.url),runId='lease_test_0000000001';
const frame=(seq,id=runId)=>JSON.stringify({v:1,run_id:id,seq:String(seq)})+'\n';
const sleep=ms=>new Promise(resolve=>setTimeout(resolve,ms));
function durable(path,value){const file=openSync(path,'wx',0o600);try{writeFileSync(file,JSON.stringify(value)+'\n');fsyncSync(file);}finally{closeSync(file);}}

if(process.argv[2]==='--lease-child'){
  const timeout=Number(process.argv[3]),mode=process.argv[4],receipt=process.argv[5],started=performance.now();
  let isReady=false,work,ticks=0,lease,failed=false;
  process.stdout.on('error',()=>{});
  const baselineListeners=Object.fromEntries(['data','end','close','error'].map(name=>[name,process.stdin.listenerCount(name)]));
  const send=value=>{process.stdout.write(JSON.stringify(value)+'\n');};
  if(mode==='throw')process.once('uncaughtExceptionMonitor',error=>send({kind:'callback_failed_safely',code:error.code,paused:process.stdin.isPaused(),baselineListeners,listeners:Object.fromEntries(['data','end','close','error'].map(name=>[name,process.stdin.listenerCount(name)]))}));
  // Independent test-harness bound. A case that reaches this cannot pass.
  const failsafe=setTimeout(()=>{
    clearInterval(work);lease?.close();process.stdin.destroy();
    send({kind:'harness_timeout'});process.exitCode=2;
  },5000);
  lease=attachControlLease({input:process.stdin,runId,timeoutMs:timeout,onAbort:reason=>{
    if(mode==='throw')throw Error('PRIVATE_CALLBACK_PAYLOAD_MUST_NOT_ESCAPE');
    failed=true;clearTimeout(failsafe);clearInterval(work);
    const value={kind:'abort',reason,ready:isReady,pid:process.pid,run_id:runId,elapsed_ms:performance.now()-started,ticks,native_terminal_verified:false};
    if(receipt)durable(receipt+'.abort.json',value);
    send(value);process.exitCode=1;
  }});
  process.once('exit',code=>{if(receipt)durable(receipt+'.exit-handler.json',{pid:process.pid,run_id:runId,code,failed,native_terminal_verified:false});});
  lease.ready.then(()=>{
    isReady=true;
    if(receipt)durable(receipt+'.ready.json',{pid:process.pid,run_id:runId});
    send({kind:'ready'});
    if(mode==='orphan')work=setInterval(()=>{ticks++;},20);
    if(mode==='close')setTimeout(()=>{
      lease.close();clearTimeout(failsafe);
      send({kind:'closed',paused:process.stdin.isPaused(),listeners:Object.fromEntries(['data','end','close','error'].map(name=>[name,process.stdin.listenerCount(name)]))});
    },80);
  },error=>send({kind:'ready_rejected',code:error.code}));
}else if(process.argv[2]==='--coordinator'){
  const receipt=process.argv[3];
  // Explicitly permit an orphan, including on Windows where a normal child
  // may otherwise share its coordinator's native job lifetime.
  const child=spawn(process.execPath,[self,'--lease-child','400','orphan',receipt],{stdio:['pipe','ignore','ignore'],detached:true,windowsHide:true});
  child.stdin.on('error',()=>{});
  let seq=1n;child.stdin.write(frame(seq++));
  const timer=setInterval(()=>child.stdin.write(frame(seq++)),40);
  child.once('close',()=>{clearInterval(timer);process.exitCode=2;});
  process.stdout.write(JSON.stringify({kind:'owned_child',pid:child.pid,run_id:runId})+'\n');
}else{
  assert.equal(process.argv.length,2,'No checker arguments');
  const checks=[];
  function options(input=new PassThrough(),patch={}){return{input,runId,onAbort:()=>{},...patch};}
  for(const patch of[{runId:'short'},{runId:'a'.repeat(129)},{runId:'é'.repeat(16)},{timeoutMs:99},{timeoutMs:60001},{timeoutMs:100.1},{timeoutMs:NaN},{onAbort:null},{input:{}}])assert.throws(()=>attachControlLease(options(undefined,patch)),/invalid_control_lease_options/);
  const encoded=new PassThrough();encoded.setEncoding('utf8');assert.throws(()=>attachControlLease(options(encoded)),/invalid_control_lease_options/);
  const consumed=new PassThrough();consumed.on('data',()=>{});assert.throws(()=>attachControlLease(options(consumed)),/invalid_control_lease_options/);consumed.destroy();
  checks.push('strict_options_and_dedicated_unencoded_stream');

  const input=new PassThrough(),callbacks=[],preserved=()=>{};
  input.on('end',preserved);const lease=attachControlLease({input,runId,onAbort:r=>callbacks.push(r),timeoutMs:300});
  input.write(frame(1));await lease.ready;lease.close();lease.close();
  assert.equal(input.isPaused(),true);assert.equal(input.listenerCount('data'),0);assert.equal(input.listenerCount('readable'),0);assert.equal(input.listenerCount('end'),1);assert.equal(input.listenerCount('close'),0);assert.equal(input.listenerCount('error'),0);
  input.emit('end');input.emit('close');await sleep(350);assert.deepEqual(callbacks,[]);input.removeListener('end',preserved);input.destroy();
  checks.push('close_cancels_own_timer_listeners_pauses_and_preserves_other_listener');

  const earlyInput=new PassThrough(),early=attachControlLease(options(earlyInput,{timeoutMs:200}));early.close();await assert.rejects(early.ready,{code:'control_lease_closed'});earlyInput.destroy();
  const delayedInput=new PassThrough(),unhandled=[];const observedUnhandled=reason=>unhandled.push(reason);process.on('unhandledRejection',observedUnhandled);
  const delayed=attachControlLease(options(delayedInput,{timeoutMs:100}));await sleep(180);await assert.rejects(delayed.ready,{code:'control_timeout'});assert.deepEqual(unhandled,[]);process.removeListener('unhandledRejection',observedUnhandled);delayedInput.destroy();
  checks.push('early_close_and_delayed_ready_rejection_without_unhandled_rejection');

  const badInput=new PassThrough(),reasons=[];const bad=attachControlLease(options(badInput,{onAbort:r=>{reasons.push(r);throw Error('synthetic_callback_failure');}}));
  assert.throws(()=>badInput.emit('error',Error('synthetic_input_failure')),{code:'control_abort_callback_failed'});await assert.rejects(bad.ready,{code:'control_input_error'});badInput.emit('end');badInput.emit('close');bad.close();assert.deepEqual(reasons,['control_input_error']);assert.equal(badInput.listenerCount('data'),0);badInput.destroy();
  checks.push('input_error_latches_first_fault_and_callback_error_fails_closed');

  async function nativeCase(name,write,{reason,ready=false,timeoutMs=220,mode='plain',closed=false,crashed=false}={}){
    const child=spawn(process.execPath,[self,'--lease-child',String(timeoutMs),mode],{stdio:['pipe','pipe','pipe']});
    let stdout='',stderr='',writeError;
    child.stdin.on('error',()=>{});child.stdout.on('data',chunk=>{stdout+=chunk;});child.stderr.on('data',chunk=>{stderr+=chunk;});
    const terminal=new Promise((resolve,reject)=>{child.once('error',reject);child.once('close',(code,signal)=>resolve({code,signal}));});
    const limit=setTimeout(()=>child.kill('SIGKILL'),6500);
    const rows=()=>stdout.trim().split('\n').filter(Boolean).map(line=>JSON.parse(line));
    const wait=async kind=>{const end=performance.now()+4000;while(performance.now()<end){const row=rows().find(r=>r.kind===kind);if(row)return row;await sleep(5);}throw Error('native_receipt_timeout:'+name+':'+stdout+stderr);};
    const writing=Promise.resolve().then(()=>write(child,wait)).catch(error=>{writeError=error;child.kill('SIGKILL');});
    try{
      const result=await terminal;await writing;if(writeError)throw writeError;
      assert.equal(result.signal,null,name+':'+stderr);assert.equal(result.code,closed?0:1,name+':'+stdout+stderr);
      const messages=rows();assert.equal(messages.some(r=>r.kind==='harness_timeout'),false,name);
      assert.equal(messages.filter(r=>r.kind==='ready').length,ready?1:0,name);
      if(crashed){const safe=messages.find(r=>r.kind==='callback_failed_safely');assert.equal(safe.code,'control_abort_callback_failed');assert.equal(safe.paused,true);assert.deepEqual(safe.listeners,safe.baselineListeners);assert.equal(stderr.includes('PRIVATE_CALLBACK_PAYLOAD_MUST_NOT_ESCAPE'),false);assert.ok(stderr.includes('control_abort_callback_failed'));}
      else if(closed){assert.equal(messages.some(r=>r.kind==='abort'),false);assert.equal(messages.find(r=>r.kind==='closed').paused,true);}
      else{const faults=messages.filter(r=>r.kind==='abort');assert.equal(faults.length,1,name);assert.equal(faults[0].reason,reason,name);}
      checks.push(name);return messages;
    }finally{clearTimeout(limit);if(child.exitCode===null&&child.signalCode===null){child.kill('SIGKILL');await terminal;}}
  }

  const invalid=[
    ['numeric_sequence',JSON.stringify({v:1,run_id:runId,seq:1})+'\n'],
    ['leading_zero',frame('01')],['zero',frame('0')],['negative',frame('-1')],['int64_overflow',frame('9223372036854775808')],
    ['unknown_field',JSON.stringify({v:1,run_id:runId,seq:'1',extra:true})+'\n'],
    ['duplicate_field','{"v":1,"run_id":"'+runId+'","seq":"1","seq":"1"}\n'],
    ['noncanonical_whitespace',' '+frame('1')],['noncanonical_field_order',JSON.stringify({seq:'1',run_id:runId,v:1})+'\n'],
    ['crlf',frame('1').replace('\n','\r\n')],['invalid_json','{\n'],['nonascii_byte',Buffer.from([0xff,10])]
  ];
  // Actual pipes/processes, bounded concurrency. These are protocol tests only.
  for(let at=0;at<invalid.length;at+=3){const results=await Promise.allSettled(invalid.slice(at,at+3).map(([name,bytes])=>nativeCase('native_reject_'+name,child=>child.stdin.write(bytes),{reason:'control_frame_invalid'})));for(const r of results)if(r.status==='rejected')throw r.reason;}
  await nativeCase('native_wrong_run',child=>child.stdin.write(frame(1,'different_run_0001')),{reason:'control_run_mismatch'});
  await nativeCase('native_oversize_without_lf',child=>child.stdin.write(Buffer.alloc(256,120)),{reason:'control_frame_too_large'});
  await nativeCase('native_initial_eof_never_ready',child=>child.stdin.end(),{reason:'control_eof'});
  await nativeCase('native_initial_timeout_never_ready',()=>{},{reason:'control_timeout'});
  await nativeCase('native_callback_throw_from_data_fails_closed',child=>child.stdin.write('invalid\n'),{mode:'throw',crashed:true});
  await nativeCase('native_callback_throw_from_timer_fails_closed',()=>{},{mode:'throw',crashed:true});
  await nativeCase('native_partial_frame_eof',child=>child.stdin.end('{"v":1'),{reason:'control_eof'});
  await nativeCase('native_fragmented_valid_then_eof',async(child,wait)=>{const bytes=frame(1);child.stdin.write(bytes.slice(0,12));await sleep(20);child.stdin.write(bytes.slice(12));await wait('ready');child.stdin.end();},{reason:'control_eof',ready:true});
  await nativeCase('native_int64_max_and_multiple_frames',async(child,wait)=>{child.stdin.write(frame('9223372036854775806')+frame('9223372036854775807'));await wait('ready');child.stdin.end();},{reason:'control_eof',ready:true});
  await nativeCase('native_regression_latches_before_eof',async(child,wait)=>{child.stdin.write(frame(10));await wait('ready');child.stdin.end(frame(9));},{reason:'control_sequence_regressed',ready:true});
  await nativeCase('native_close_then_eof_no_abort',async(child,wait)=>{child.stdin.write(frame(1));await wait('closed');child.stdin.end();},{ready:true,mode:'close',closed:true,timeoutMs:300});
  const duplicated=await nativeCase('native_duplicates_do_not_renew',async(child,wait)=>{child.stdin.write(frame(1));await wait('ready');for(let n=0;n<5;n++){await sleep(60);if(!child.stdin.destroyed)child.stdin.write(frame(1));}},{reason:'control_timeout',ready:true,timeoutMs:220});
  assert.ok(duplicated.find(r=>r.kind==='abort').elapsed_ms<600,'Duplicate replay extended deadline');
  const renewed=await nativeCase('native_strictly_new_sequence_renews_monotonic_lease',async(child,wait)=>{child.stdin.write(frame(1));await wait('ready');for(let n=2;n<=5;n++){await sleep(80);child.stdin.write(frame(n));}},{reason:'control_timeout',ready:true,timeoutMs:220});
  assert.ok(renewed.find(r=>r.kind==='abort').elapsed_ms>=480,'New valid heartbeat failed to renew');

  const tempParent=await realpath(tmpdir()),folder=await mkdtemp(join(tempParent,'ccpanel-lease-check-')),receipt=join(folder,'orphan');
  let coordinator;
  try{
    coordinator=spawn(process.execPath,[self,'--coordinator',receipt],{stdio:['ignore','pipe','pipe']});
    let output='',error='';coordinator.stdout.on('data',chunk=>{output+=chunk;});coordinator.stderr.on('data',chunk=>{error+=chunk;});
    const terminal=new Promise((resolve,reject)=>{coordinator.once('error',reject);coordinator.once('close',(code,signal)=>resolve({code,signal}));});
    const readReceipt=async suffix=>{const end=performance.now()+6500;while(performance.now()<end){try{return JSON.parse(await readFile(receipt+suffix,'utf8'));}catch(e){if(e.code!=='ENOENT')throw e;}await sleep(10);}throw Error('orphan_receipt_timeout:'+suffix+':'+error);};
    const ready=await readReceipt('.ready.json');assert.equal(ready.run_id,runId);await sleep(120);
    const owned=JSON.parse(output.trim());assert.equal(owned.pid,ready.pid);assert.equal(owned.run_id,runId);
    assert.equal(coordinator.kill('SIGKILL'),true);const actualCoordinatorTerminal=await terminal;
    assert.ok(actualCoordinatorTerminal.code!==0||actualCoordinatorTerminal.signal!==null);
    const orphanAbort=await readReceipt('.abort.json'),exitHandler=await readReceipt('.exit-handler.json');
    assert.equal(orphanAbort.pid,owned.pid);assert.equal(orphanAbort.run_id,runId);assert.equal(orphanAbort.ready,true);assert.ok(['control_eof','control_input_error','control_timeout'].includes(orphanAbort.reason));assert.ok(orphanAbort.ticks>0);
    assert.equal(exitHandler.pid,owned.pid);assert.equal(exitHandler.failed,true);assert.equal(exitHandler.code,1);assert.equal(exitHandler.native_terminal_verified,false);
    checks.push('actual_coordinator_hard_death_orphan_self_aborts_with_fsynced_receipts');
  }finally{
    if(coordinator&&coordinator.exitCode===null&&coordinator.signalCode===null)coordinator.kill('SIGKILL');
    // The orphan has its own 5s failsafe if a broken lease causes this test to
    // fail. The supervisor never writes a stop file or fabricates its exit.
    const target=resolve(folder);assert.equal(dirname(target),tempParent);assert.ok(basename(target).startsWith('ccpanel-lease-check-'));await rm(target,{recursive:true,force:true});
  }
  console.log(JSON.stringify({passed:true,checks,real_native_children:true,dedicated_stdin_pipes:true,actual_coordinator_hard_death:true,orphan_abort_receipt_fsynced:true,orphan_exit_handler_receipt_fsynced:true,orphan_native_terminal_observed:false,test_parent_wrote_stop:false,platform:process.platform,full_capacity_bridge_verified:false}));
}
