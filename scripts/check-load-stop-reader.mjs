// Native files and native-I/O barriers only; no replacement stat/read results.
import assert from 'node:assert/strict';
import {constants,promises as fs} from 'node:fs';
import {spawn,execFileSync} from 'node:child_process';
import {tmpdir} from 'node:os';
import {join,dirname,resolve,basename} from 'node:path';
import {fileURLToPath} from 'node:url';
import {syncBuiltinESMExports} from 'node:module';
import {readStopReason,StopReadError} from './load-stop-reader.mjs';
import {publishFirstStop} from './load-first-stop.mjs';

const pause=ms=>new Promise(done=>setTimeout(done,ms));

if(process.argv[2]==='--publisher-child'){
  const link=fs.link.bind(fs);
  fs.link=async(...args)=>{
    const result=await link(...args);
    const released=new Promise(done=>process.once('message',value=>{assert.equal(value,'release');done();}));
    process.send({phase:'barrier'});await released;return result;
  };
  syncBuiltinESMExports();
  const result=await publishFirstStop(process.argv[3],'resource_safety');console.log(JSON.stringify(result));process.disconnect();
}else if(process.argv[2]==='--reader-child'){
  const target=process.argv[3],mode=process.argv[4]??'',native={lstat:fs.lstat.bind(fs),open:fs.open.bind(fs)};
  const observed={opens:0,closes:0,reads:0,eofs:0,nofollow:null,nonblock:null};let lstats=0,stats=0;
  const barrier=async()=>{
    const released=new Promise(done=>process.once('message',value=>{assert.equal(value,'release');done();}));
    process.send({phase:'barrier'});await released;
  };
  fs.lstat=async(...args)=>{const result=await native.lstat(...args);lstats++;if(mode==='after-initial-lstat'&&lstats===1)await barrier();return result;};
  fs.open=async(...args)=>{
    const file=await native.open(...args);observed.opens++;
    observed.nofollow=constants.O_NOFOLLOW===undefined?null:!!(args[1]&constants.O_NOFOLLOW);
    observed.nonblock=constants.O_NONBLOCK===undefined?null:!!(args[1]&constants.O_NONBLOCK);
    const read=file.read.bind(file),stat=file.stat.bind(file),close=file.close.bind(file);
    file.read=async(...values)=>{const result=await read(...values);observed.reads++;if(result.bytesRead===0)observed.eofs++;if(mode==='after-first-read'&&observed.reads===1)await barrier();return result;};
    file.stat=async(...values)=>{const result=await stat(...values);stats++;if(mode==='after-final-fstat'&&stats===2)await barrier();return result;};
    file.close=async()=>{await close();observed.closes++;};
    return file;
  };
  let reason,error;
  try{reason=await readStopReason(target);}catch(value){error={name:value.name,code:value.code,message:value.message,keys:Object.keys(value).sort()};assert.ok(value instanceof StopReadError);process.exitCode=1;}
  console.log(JSON.stringify({reason,error,observed}));process.disconnect();
}else{
  const root=await fs.mkdtemp(join(tmpdir(),'ccpanel-stop-reader-')),checks=[],children=new Set();let serial=0,fileSymlink=false,publicationOverlap;
  const payload=reason=>JSON.stringify({reason})+'\n';
  const resource=payload('resource_safety');
  async function fixture(body=resource){const folder=join(root,'case-'+(++serial));await fs.mkdir(folder,{mode:0o700});const target=join(folder,'request.stop');if(body!==null)await fs.writeFile(target,body,{mode:0o600,flag:'wx'});await fs.writeFile(join(folder,'operator-note'),'untouched',{mode:0o600,flag:'wx'});return{folder,target};}
  async function preserved(value,body){assert.deepEqual(await fs.readFile(value.target),Buffer.from(body));assert.equal(await fs.readFile(join(value.folder,'operator-note'),'utf8'),'untouched');}
  function start(target,mode=''){
    const child=spawn(process.execPath,[fileURLToPath(import.meta.url),'--reader-child',target,mode],{stdio:['ignore','pipe','pipe','ipc']});children.add(child);
    let stdout='',stderr='',closed=false,expired=false,readyResolve,readyReject;
    const ready=new Promise((accept,reject)=>{readyResolve=accept;readyReject=reject;});void ready.catch(()=>{});
    child.on('message',value=>{assert.deepEqual(value,{phase:'barrier'});readyResolve();});
    child.stdout.on('data',data=>{stdout+=data;if(stdout.length>16384)child.kill('SIGKILL');});child.stderr.on('data',data=>{stderr=(stderr+data).slice(-16384);});
    const timer=setTimeout(()=>{expired=true;child.kill('SIGKILL');},10000);
    const done=new Promise((accept,reject)=>{
      child.once('error',error=>{clearTimeout(timer);children.delete(child);readyReject(error);reject(error);});
      child.once('close',(code,signal)=>{
        closed=true;clearTimeout(timer);children.delete(child);readyReject(Error('native_reader_closed_before_barrier'));
        try{assert.equal(expired,false,'native reader exceeded bounded observation');assert.equal(signal,null);assert.equal(stderr,'');const value=JSON.parse(stdout);assert.equal(code,value.error?1:0);assert.equal(value.observed.opens,value.observed.closes,'native handles closed');if(value.observed.opens){if(constants.O_NOFOLLOW!==undefined)assert.equal(value.observed.nofollow,true);if(constants.O_NONBLOCK!==undefined)assert.equal(value.observed.nonblock,true);}accept(value);}catch(error){reject(error);}
      });
    });void done.catch(()=>{});
    return{ready,done,release(){assert.equal(closed,false);child.send('release');}};
  }
  function startPublisher(target){
    const child=spawn(process.execPath,[fileURLToPath(import.meta.url),'--publisher-child',target],{stdio:['ignore','pipe','pipe','ipc']});children.add(child);
    let stdout='',stderr='',readyResolve,readyReject,expired=false;
    const ready=new Promise((accept,reject)=>{readyResolve=accept;readyReject=reject;});void ready.catch(()=>{});
    child.on('message',value=>{assert.deepEqual(value,{phase:'barrier'});readyResolve();});
    child.stdout.on('data',data=>{stdout+=data;if(stdout.length>16384)child.kill('SIGKILL');});child.stderr.on('data',data=>{stderr=(stderr+data).slice(-16384);});
    const timer=setTimeout(()=>{expired=true;child.kill('SIGKILL');},10000);
    const done=new Promise((accept,reject)=>{
      child.once('error',error=>{clearTimeout(timer);children.delete(child);readyReject(error);reject(error);});
      child.once('close',(code,signal)=>{clearTimeout(timer);children.delete(child);readyReject(Error('native_publisher_closed_before_barrier'));try{assert.equal(expired,false);assert.equal(signal,null);assert.equal(code,0);assert.equal(stderr,'');assert.deepEqual(JSON.parse(stdout),{status:'published'});accept();}catch(error){reject(error);}});
    });void done.catch(()=>{});
    return{ready,done,release(){child.send('release');}};
  }
  function invalid(value){assert.deepEqual(value.error,{name:'StopReadError',code:'invalid_stop_request',message:'invalid_stop_request',keys:['code','name']});assert.equal(value.reason,undefined);}
  async function accepted(body,reason,label){const value=await fixture(body),result=await start(value.target).done;assert.equal(result.reason,reason);assert.equal(result.error,undefined);assert.ok(result.observed.reads>=2);assert.equal(result.observed.eofs,1);await preserved(value,body);checks.push(label);}
  async function rejected(body,label){const value=await fixture(body);invalid(await start(value.target).done);await preserved(value,body);checks.push(label);}
  async function race(mode,mutate,label){const value=await fixture(resource),reading=start(value.target,mode);await reading.ready;const verify=await mutate(value);reading.release();invalid(await reading.done);if(verify)await verify();assert.equal(await fs.readFile(join(value.folder,'operator-note'),'utf8'),'untouched');checks.push(label);}
  try{
    const absent=await fixture(null),missing=await start(absent.target).done;assert.equal(missing.reason,null);assert.equal(missing.error,undefined);assert.equal(missing.observed.opens,0);assert.equal(await readStopReason(absent.target),null);checks.push('initial_enoent_only_is_null');
    await accepted(resource,'resource_safety','resource_safety_native_eof');
    await accepted(payload('operator_requested'),'operator_requested','operator_requested_native_eof');
    await accepted(' \t\r\n{ \n"reason"\t:\r"resource_safety" }\n','resource_safety','ascii_json_whitespace');
    await accepted('{"\\u0072eason":"operator_\\u0072equested"}','operator_requested','valid_json_escapes_semantic_reason');
    await accepted(resource+' '.repeat(1024-Buffer.byteLength(resource)),'resource_safety','exact_1024_bytes_and_eof');
    for(const [label,body]of [
      ['empty',''],['partial','{"reason":'],['array','["resource_safety"]'],['null','null'],['missing_member','{}'],['wrong_key','{"other":"resource_safety"}'],
      ['unknown_reason','{"reason":"other"}'],['wrong_type','{"reason":true}'],['extra_member','{"reason":"resource_safety","extra":1}'],
      ['duplicate_same','{"reason":"resource_safety","reason":"resource_safety"}'],['duplicate_other','{"reason":"resource_safety","reason":"operator_requested"}'],
      ['duplicate_escaped','{"reason":"resource_safety","\\u0072eason":"resource_safety"}'],['trailing_json',resource+'{}'],['invalid_escape','{"reason":"resource_\\qafety"}'],
      ['non_json_ascii_space','\v'+resource],['unicode_space','\u00a0'+resource],['bom','\ufeff'+resource],['bad_utf8',Buffer.concat([Buffer.from([0xc3,0x28]),Buffer.from(resource)])],
      ['overlong_utf8',Buffer.concat([Buffer.from([0xc0,0xaf]),Buffer.from(resource)])],['encoded_surrogate',Buffer.concat([Buffer.from([0xed,0xa0,0x80]),Buffer.from(resource)])],
      ['oversized_1025',resource+' '.repeat(1025-Buffer.byteLength(resource))]
    ])await rejected(body,'reject_'+label);
    const directory=await fixture(null);await fs.mkdir(directory.target);invalid(await start(directory.target).done);assert.equal((await fs.lstat(directory.target)).isDirectory(),true);checks.push('directory_rejected_untouched');
    const alias=await fixture(null),linked=join(alias.folder,'linked');await fs.writeFile(linked,resource,{mode:0o600});
    try{await fs.symlink(linked,alias.target,'file');fileSymlink=true;}catch(error){if(process.platform!=='win32'||!['EPERM','EACCES'].includes(error.code))throw error;}
    if(fileSymlink){const result=await start(alias.target).done;invalid(result);assert.equal(result.observed.opens,0);assert.equal((await fs.lstat(alias.target)).isSymbolicLink(),true);assert.equal(await fs.readFile(linked,'utf8'),resource);checks.push('file_symlink_not_opened_or_modified');}
    const directoryAlias=await fixture(null),realDirectory=join(directoryAlias.folder,'directory');await fs.mkdir(realDirectory);await fs.symlink(realDirectory,directoryAlias.target,process.platform==='win32'?'junction':'dir');invalid(await start(directoryAlias.target).done);assert.equal((await fs.lstat(directoryAlias.target)).isSymbolicLink(),true);checks.push('directory_symlink_not_followed');
    if(process.platform!=='win32'){
      const fifo=await fixture(null);execFileSync('mkfifo',[fifo.target],{timeout:3000});const result=await start(fifo.target).done;invalid(result);assert.equal(result.observed.opens,0);assert.equal((await fs.lstat(fifo.target)).isFIFO(),true);checks.push('fifo_refused_without_blocking_open');
      const denied=await fixture();await fs.chmod(denied.target,0);try{if(process.geteuid?.()!==0){invalid(await start(denied.target).done);checks.push('native_eacces_safe_error');}}finally{await fs.chmod(denied.target,0o600);}
    }
    const published=await fixture(null);assert.deepEqual(await publishFirstStop(published.target,'resource_safety'),{status:'published'});assert.equal((await start(published.target).done).reason,'resource_safety');assert.deepEqual(await publishFirstStop(published.target,'operator_requested'),{status:'already_latched'});assert.equal((await start(published.target).done).reason,'resource_safety');await preserved(published,resource);checks.push('actual_atomic_publisher_first_winner_compatible');
    const hardlinked=await fixture(null),temporary=join(hardlinked.folder,'published.tmp'),handle=await fs.open(temporary,'wx',0o600);try{await handle.writeFile(resource);await handle.sync();}finally{await handle.close();}await fs.link(temporary,hardlinked.target);assert.equal((await fs.lstat(hardlinked.target)).nlink,2);assert.equal((await start(hardlinked.target).done).reason,'resource_safety');assert.equal((await fs.lstat(hardlinked.target)).nlink,2);checks.push('complete_hardlinked_file_nlink_two_allowed');
    const finishing=await fixture(null),publisher=startPublisher(finishing.target);await publisher.ready;
    assert.equal((await fs.lstat(finishing.target)).nlink,2);assert.equal((await start(finishing.target).done).reason,'resource_safety');checks.push('actual_publisher_stable_complete_link_readable_before_cleanup');
    const overlapping=start(finishing.target,'after-first-read');await overlapping.ready;const beforeUnlink=await fs.lstat(finishing.target,{bigint:true});
    publisher.release();await publisher.done;const afterUnlink=await fs.lstat(finishing.target,{bigint:true});assert.equal(afterUnlink.nlink,1n);
    overlapping.release();const overlapResult=await overlapping.done,changed=beforeUnlink.ctimeNs!==afterUnlink.ctimeNs||beforeUnlink.mtimeNs!==afterUnlink.mtimeNs;
    if(changed)invalid(overlapResult);else assert.equal(overlapResult.reason,'resource_safety');
    publicationOverlap={metadata_changed:changed,result:overlapResult.error?'invalid_stop_request':overlapResult.reason,valid_concurrent_publication_may_be_rejected:true,zero_false_rejections_claimed:false};
    await preserved(finishing,resource);assert.deepEqual((await fs.readdir(finishing.folder)).sort(),['operator-note','request.stop']);checks.push('actual_publisher_cleanup_overlap_bounded_complete_or_conservative_rejection');
    await race('after-initial-lstat',async value=>{await fs.unlink(value.target);return async()=>assert.rejects(fs.lstat(value.target),{code:'ENOENT'});},'existing_then_missing_never_null');
    await race('after-initial-lstat',async value=>{const old=value.target+'.original';await fs.rename(value.target,old);await fs.writeFile(value.target,resource,{mode:0o600});return async()=>{await preserved(value,resource);assert.equal(await fs.readFile(old,'utf8'),resource);};},'replacement_between_lstat_and_open_rejected');
    if(fileSymlink)await race('after-initial-lstat',async value=>{const old=value.target+'.original';await fs.rename(value.target,old);await fs.symlink(old,value.target,'file');return async()=>{assert.equal((await fs.lstat(value.target)).isSymbolicLink(),true);assert.equal(await fs.readFile(old,'utf8'),resource);};},'symlink_swap_before_open_rejected');
    if(process.platform!=='win32')await race('after-initial-lstat',async value=>{await fs.rename(value.target,value.target+'.original');execFileSync('mkfifo',[value.target],{timeout:3000});return async()=>assert.equal((await fs.lstat(value.target)).isFIFO(),true);},'fifo_swap_before_open_nonblocking_rejected');
    await race('after-first-read',async value=>{await fs.appendFile(value.target,' ');return async()=>preserved(value,resource+' ');},'native_growth_during_read_rejected');
    await race('after-first-read',async value=>{await fs.truncate(value.target,8);return async()=>preserved(value,Buffer.from(resource).subarray(0,8));},'native_truncation_during_read_rejected');
    await race('after-first-read',async value=>{const body=resource.slice(0,-1)+' ';await fs.writeFile(value.target,body);const later=new Date(Date.now()+2000);await fs.utimes(value.target,later,later);return async()=>preserved(value,body);},'valid_same_size_rewrite_during_read_rejected');
    if(process.platform!=='win32')await race('after-first-read',async value=>{const before=await fs.lstat(value.target,{bigint:true});await fs.chmod(value.target,0o400);const after=await fs.lstat(value.target,{bigint:true});assert.equal(after.mtimeNs,before.mtimeNs);assert.notEqual(after.ctimeNs,before.ctimeNs);return async()=>preserved(value,resource);},'ctime_only_permission_change_rejected');
    await race('after-first-read',async value=>{await fs.unlink(value.target);return async()=>assert.rejects(fs.lstat(value.target),{code:'ENOENT'});},'path_disappearing_after_read_rejected');
    await race('after-final-fstat',async value=>{await fs.rename(value.target,value.target+'.original');await fs.writeFile(value.target,resource,{mode:0o600});return async()=>preserved(value,resource);},'replacement_after_descriptor_stat_rejected');
    await assert.rejects(readStopReason('bad\0path'),error=>error instanceof StopReadError&&error.message==='invalid_stop_request'&&!('path'in error));checks.push('safe_error_without_path_or_content');
    console.log(JSON.stringify({passed:true,platform:process.platform,native_files_and_barriers:true,native_file_symlink_checked:fileSymlink,native_fifo_checked:process.platform!=='win32',atomic_publisher_compatible:true,publication_cleanup_overlap:publicationOverlap,checks,hostile_parent_replacement_immunity_claimed:false}));
  }finally{
    for(const child of children)child.kill('SIGKILL');
    const target=resolve(root);assert.equal(dirname(target),resolve(tmpdir()));assert.ok(basename(target).startsWith('ccpanel-stop-reader-'));await fs.rm(target,{recursive:true,force:true});
  }
}
