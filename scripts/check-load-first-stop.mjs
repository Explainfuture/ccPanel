// Real child processes, links and delegated native fs faults. No server access,
// fake cryptography or claims of power-loss/disk-failure durability.
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {mkdtemp,mkdir,writeFile,readFile,readdir,lstat,symlink,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join,dirname,resolve,basename} from 'node:path';
import {pathToFileURL} from 'node:url';
import {publishFirstStop} from './load-first-stop.mjs';

const root=await mkdtemp(join(tmpdir(),'ccpanel-first-stop-'));
const children=new Set(),checks=[];let serial=0,symlinksChecked=false;
const preload=join(root,'native-faults.mjs'),writer=join(root,'writer.mjs');
const sleep=ms=>new Promise(done=>setTimeout(done,ms));
async function waitFile(path){
  const end=Date.now()+10000;
  while(Date.now()<end){try{await lstat(path);return;}catch(error){if(error.code!=='ENOENT')throw error;}await sleep(10);}
  throw Error('native_test_barrier_timeout');
}
function start(target,reason='resource_safety',mode='',gates={}){
  const child=spawn(process.execPath,['--import',pathToFileURL(preload).href,writer,target,reason],{env:{...process.env,FIRST_STOP_MODE:mode,...gates},stdio:['ignore','pipe','pipe']});
  children.add(child);let stdout='',stderr='',expired=false;
  child.stdout.on('data',chunk=>{stdout+=chunk;if(stdout.length>32768)child.kill('SIGKILL');});
  child.stderr.on('data',chunk=>{stderr=(stderr+chunk).slice(-16384);});
  const timer=setTimeout(()=>{expired=true;child.kill('SIGKILL');},15000);
  const done=new Promise((accept,reject)=>{
    child.once('error',reject);
    child.once('close',(code,signal)=>{
      clearTimeout(timer);children.delete(child);
      try{assert.equal(expired,false);assert.equal(signal,null);assert.equal(stderr,'');const value=JSON.parse(stdout);assert.equal(code,value.error?1:0);assert.equal(value.native.handles,0);accept(value);}catch(error){reject(error);}
    });
  });
  return{child,done};
}
async function fixture(){const path=join(root,'case-'+(++serial));await mkdir(path,{mode:0o700});await writeFile(join(path,'operator-note'),'preserve-note',{mode:0o600});return{path,target:join(path,'request.stop')};}
async function note(value){assert.equal(await readFile(join(value.path,'operator-note'),'utf8'),'preserve-note');}
async function missing(path){await assert.rejects(lstat(path),{code:'ENOENT'});}
const json=reason=>JSON.stringify({reason})+'\n';

try{
  await writeFile(preload,`
import fs from 'node:fs/promises';
import {basename,dirname} from 'node:path';
import {syncBuiltinESMExports} from 'node:module';
const native={...fs},mode=process.env.FIRST_STOP_MODE;
const state=globalThis.__firstStopNative={handles:0,opens:0,temporary:'',syncDone:false,syncReturned:false,tempClosed:false,linkCalled:false,linkCreated:false,fired:false,unlinkCalls:0,directorySyncs:0};
const io=()=>Object.assign(Error('controlled_native_io_fault'),{code:'EIO'});
const temporary=path=>basename(String(path)).startsWith('.load-stop-')&&String(path).endsWith('.tmp');
fs.open=async(...args)=>{
 const path=String(args[0]),temp=temporary(path);
 if(temp){state.temporary=path;if(mode==='collision'&&!state.fired){await native.writeFile(path,'unowned-temp-marker',{flag:'wx',mode:0o600});state.fired=true;}}
 const handle=await native.open(...args);state.opens++;state.handles++;let closed=false;
 return new Proxy(handle,{get(target,key){
  if(key==='sync')return async()=>{
   await target.sync();
   if(temp){
    state.syncDone=true;
    if(mode==='sync-failure'&&!state.fired){state.fired=true;throw io();}
    if(mode==='sync-gate'){
     await native.writeFile(process.env.FIRST_STOP_SYNC_READY,JSON.stringify({temporary:path,syncDone:true}),{mode:0o600,flag:'wx'});
     while(true){try{await native.lstat(process.env.FIRST_STOP_SYNC_RELEASE);break;}catch(error){if(error.code!=='ENOENT')throw error;}await new Promise(done=>setTimeout(done,10));}
    }
    state.syncReturned=true;
   }else{
    state.directorySyncs++;
    if(mode==='directory-sync'&&state.linkCreated&&!state.fired){state.fired=true;throw io();}
   }
  };
  if(key==='close')return async()=>{
   try{await target.close();}finally{if(!closed){closed=true;state.handles--;if(temp)state.tempClosed=true;}}
  };
  const value=Reflect.get(target,key,target);return typeof value==='function'?value.bind(target):value;
 }});
};
fs.link=async(...args)=>{
 state.linkCalled=true;
 if(!state.syncDone||!state.syncReturned||!state.tempClosed)throw Error('native_sync_and_close_must_finish_before_link');
 if(mode==='replace-before-eexist'&&!state.fired){
  await native.rename(String(args[0]),String(args[0])+'.owned-evidence');await native.writeFile(String(args[0]),'unowned-replacement',{flag:'wx',mode:0o600});state.fired=true;
 }
 await native.link(...args);state.linkCreated=true;
 if(mode==='after-link'&&!state.fired){state.fired=true;throw io();}
};
fs.unlink=async(...args)=>{
 if(temporary(args[0])){state.unlinkCalls++;if(mode==='unlink-failure'&&!state.fired){state.fired=true;throw io();}}
 return native.unlink(...args);
};
syncBuiltinESMExports();
`,{mode:0o600});
  await writeFile(writer,`
import {publishFirstStop} from ${JSON.stringify(pathToFileURL(resolve('scripts/load-first-stop.mjs')).href)};
import {writeFile,lstat} from 'node:fs/promises';
if(process.env.FIRST_STOP_RACE_READY){await writeFile(process.env.FIRST_STOP_RACE_READY,'ready',{mode:0o600,flag:'wx'});while(true){try{await lstat(process.env.FIRST_STOP_RACE_GATE);break;}catch(error){if(error.code!=='ENOENT')throw error;}await new Promise(done=>setTimeout(done,10));}}
let result,error;
try{result=await publishFirstStop(process.argv[2],process.argv[3]);}catch(value){error=value.code??'unexpected_error';process.exitCode=1;}
console.log(JSON.stringify({result,error,requestedReason:process.argv[3],native:globalThis.__firstStopNative}));
`,{mode:0o600});

  const race=await fixture(),go=join(root,'race-go'),readyA=join(root,'race-a'),readyB=join(root,'race-b');
  const a=start(race.target,'resource_safety','',{FIRST_STOP_RACE_READY:readyA,FIRST_STOP_RACE_GATE:go});
  const b=start(race.target,'operator_requested','',{FIRST_STOP_RACE_READY:readyB,FIRST_STOP_RACE_GATE:go});
  await Promise.all([waitFile(readyA),waitFile(readyB)]);await writeFile(go,'go',{mode:0o600,flag:'wx'});
  const raced=await Promise.all([a.done,b.done]);assert.ok(raced.every(value=>value.result),JSON.stringify(raced));assert.deepEqual(raced.map(value=>value.result.status).sort(),['already_latched','published']);
  const winner=raced.find(value=>value.result.status==='published');assert.equal(await readFile(race.target,'utf8'),json(winner.requestedReason));
  assert.deepEqual((await readdir(race.path)).sort(),['operator-note','request.stop']);await note(race);checks.push('actual_concurrent_first_writer_wins');

  const gate=await fixture(),syncReady=join(root,'sync-ready'),syncRelease=join(root,'sync-release');
  const gated=start(gate.target,'resource_safety','sync-gate',{FIRST_STOP_SYNC_READY:syncReady,FIRST_STOP_SYNC_RELEASE:syncRelease});
  await waitFile(syncReady);const syncing=JSON.parse(await readFile(syncReady,'utf8'));assert.equal(syncing.syncDone,true);assert.equal(dirname(syncing.temporary),gate.path);
  assert.equal(await readFile(syncing.temporary,'utf8'),json('resource_safety'));await missing(gate.target);
  await writeFile(syncRelease,'continue',{mode:0o600,flag:'wx'});const published=await gated.done;
  assert.deepEqual(published.result,{status:'published'});assert.equal(published.native.syncReturned,true);assert.equal(published.native.tempClosed,true);assert.equal(published.native.linkCreated,true);assert.equal(published.native.directorySyncs,process.platform==='win32'?0:2);await missing(syncing.temporary);await note(gate);checks.push('actual_native_sync_and_close_before_exclusive_link');

  for(const type of ['file','directory']){
    const value=await fixture();if(type==='file')await writeFile(value.target,'existing-unvalidated-stop',{mode:0o600});else{await mkdir(value.target,{mode:0o700});await writeFile(join(value.target,'evidence'),'keep',{mode:0o600});}
    const result=await start(value.target).done;assert.deepEqual(result.result,{status:'already_latched'});
    assert.equal(await readFile(type==='file'?value.target:join(value.target,'evidence'),'utf8'),type==='file'?'existing-unvalidated-stop':'keep');await note(value);checks.push('existing_'+type+'_untouched');
  }
  const alias=await fixture(),outside=join(root,'symlink-evidence');await writeFile(outside,'untouched-link-target',{mode:0o600});
  try{await symlink(outside,alias.target,'file');}catch(error){if(process.platform!=='win32'||!['EPERM','EACCES'].includes(error.code))throw error;}
  if((await readdir(alias.path)).includes('request.stop')){
    const result=await start(alias.target).done;assert.deepEqual(result.result,{status:'already_latched'});assert.equal((await lstat(alias.target)).isSymbolicLink(),true);assert.equal(await readFile(outside,'utf8'),'untouched-link-target');await note(alias);symlinksChecked=true;checks.push('existing_symlink_untouched');
  }
  const parent=await fixture(),parentAlias=join(root,'parent-alias');await symlink(parent.path,parentAlias,process.platform==='win32'?'junction':'dir');
  const rejected=await start(join(parentAlias,'alias.stop')).done;assert.equal(rejected.error,'first_stop_invalid');assert.equal(rejected.native.opens,0);assert.deepEqual(await readdir(parent.path),['operator-note']);await note(parent);checks.push('static_parent_alias_rejected');

  const collision=await fixture(),collided=await start(collision.target,'resource_safety','collision').done;
  assert.equal(collided.error,'first_stop_io_failed');assert.equal(collided.native.fired,true);assert.equal(collided.native.unlinkCalls,0);assert.equal(await readFile(collided.native.temporary,'utf8'),'unowned-temp-marker');await missing(collision.target);await note(collision);checks.push('unowned_wx_collision_preserved');
  const syncFailure=await fixture(),syncFailed=await start(syncFailure.target,'resource_safety','sync-failure').done;
  assert.equal(syncFailed.error,'first_stop_io_failed');assert.equal(syncFailed.native.syncDone,true);assert.equal(syncFailed.native.fired,true);assert.equal(syncFailed.native.linkCalled,false);await missing(syncFailure.target);assert.deepEqual(await readdir(syncFailure.path),['operator-note']);await note(syncFailure);checks.push('native_sync_failure_before_publication_cleans_owned_temp');

  for(const existing of [false,true]){
    const value=await fixture();if(existing)await writeFile(value.target,'preexisting-stop',{mode:0o600});
    const result=await start(value.target,'resource_safety','unlink-failure').done;
    assert.equal(result.error,existing?'first_stop_cleanup_failed':'first_stop_publish_uncertain');assert.equal(result.native.fired,true);assert.equal(await readFile(result.native.temporary,'utf8'),json('resource_safety'));
    const expected=existing?'preexisting-stop':json('resource_safety');assert.equal(await readFile(value.target,'utf8'),expected);
    const retried=await start(value.target,'operator_requested').done;assert.deepEqual(retried.result,{status:'already_latched'});assert.equal(await readFile(value.target,'utf8'),expected);assert.equal(await readFile(result.native.temporary,'utf8'),json('resource_safety'));await note(value);checks.push(existing?'already_latched_cleanup_failure_explicit':'published_cleanup_failure_uncertain_and_final_retained');
  }
  const replaced=await fixture();await writeFile(replaced.target,'original-stop',{mode:0o600});
  const replacement=await start(replaced.target,'resource_safety','replace-before-eexist').done;
  assert.equal(replacement.error,'first_stop_cleanup_failed');assert.equal(replacement.native.fired,true);assert.equal(replacement.native.unlinkCalls,0);assert.equal(await readFile(replacement.native.temporary,'utf8'),'unowned-replacement');assert.equal(await readFile(replaced.target,'utf8'),'original-stop');await note(replaced);checks.push('replaced_temp_inode_never_unlinked');

  for(const mode of ['after-link',...(process.platform==='win32'?[]:['directory-sync'])]){
    const value=await fixture(),result=await start(value.target,'resource_safety',mode).done;
    assert.equal(result.error,'first_stop_publish_uncertain');assert.equal(result.native.fired,true);assert.equal(result.native.linkCreated,true);assert.equal(await readFile(value.target,'utf8'),json('resource_safety'));await missing(result.native.temporary);await note(value);checks.push(mode+'_uncertainty_retains_final');
  }
  const invalid=await fixture(),bad=await start(invalid.target,'must-not-be-reflected').done;
  assert.equal(bad.error,'first_stop_invalid');assert.equal(bad.native.opens,0);assert.deepEqual(await readdir(invalid.path),['operator-note']);await note(invalid);
  await assert.rejects(publishFirstStop(null,'resource_safety'),{code:'first_stop_invalid'});checks.push('strict_reasons_and_invalid_input_before_fs');
  console.log(JSON.stringify({passed:true,native_child_processes:true,delegated_actual_fs_faults:true,native_symlinks_checked:symlinksChecked,posix_directory_sync_checked:process.platform!=='win32',checks,power_loss_verified:false,real_disk_failure_verified:false}));
}finally{
  for(const child of children)if(child.exitCode===null&&child.signalCode===null)child.kill('SIGKILL');
  await Promise.all([...children].map(child=>new Promise(done=>child.once('close',done))));
  assert.equal(dirname(resolve(root)),resolve(tmpdir()));assert.ok(basename(root).startsWith('ccpanel-first-stop-'));
  await rm(root,{recursive:true,force:true});
}
