// Operator-only coordinator. Raw credentials stay in private files/pipes.
// A completed control flow is not a capacity, HA or recovery acceptance.
import {spawn,ChildProcess} from 'node:child_process';
import {constants,createReadStream} from 'node:fs';
import {open,lstat,realpath,mkdir,readFile} from 'node:fs/promises';
import {resolve,dirname,join,isAbsolute,basename} from 'node:path';
import {fileURLToPath} from 'node:url';
import {createHash,randomBytes} from 'node:crypto';
import {performance} from 'node:perf_hooks';
import {publishFirstStop} from './load-first-stop.mjs';

const hashPattern=/^[a-f0-9]{64}$/,runPattern=/^[a-f0-9]{32}$/;
const tokenPattern=/^[A-Za-z0-9_-]{1,64}$/;
const clientFiles=['load-check-public.mjs','load-evidence.mjs','load-control-lease.mjs','load-stop-reader.mjs'];
const maximum={wire:8192,request:4096,fixture:4096,binding:16384,ready:16384,result:2*1024*1024,manifest:2*1024*1024,stdout:256*1024,stderr:64*1024};
const freezeRoleCaps={fixture:4096,binding:16384,owner:4096,ready:16384,run:16384,client_result:2*1024*1024,client_manifest:2*1024*1024,client_progress:65536,client_terminal:16384,client_stdout:256*1024,client_stderr:65536,samples:8*1024*1024,sampler_owner:16384,sampler_terminal:16384,configuration_before:65536,configuration_after:65536,resource_baseline:65536,resource_final:65536,monitor_before:32*1024*1024,monitor_after:32*1024*1024,db_baseline:1024*1024,db_verified:1024*1024,db_cleanup:65536,key_revoked:16384};
const sha=bytes=>createHash('sha256').update(bytes).digest('hex');
const canonicalPath=value=>process.platform==='win32'?value.toLowerCase():value;
const sleep=ms=>new Promise(resolve=>setTimeout(resolve,ms));
const positive=value=>typeof value==='string'&&/^[1-9][0-9]{0,18}$/.test(value)&&BigInt(value)<=9223372036854775807n;
const exact=(value,keys)=>value&&typeof value==='object'&&!Array.isArray(value)&&Object.keys(value).length===keys.length&&keys.every(key=>Object.hasOwn(value,key));

export class WindowError extends Error{constructor(code){super(code);this.name='WindowError';this.code=code;}}
const fail=code=>{throw new WindowError(code);};
function json(bytes,limit,code='invalid_window_json'){
  if(!Buffer.isBuffer(bytes)||bytes.length>limit||!bytes.length)fail(code);
  try{return JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(bytes));}catch{fail(code);}
}
function wire(bytes){
  const value=json(bytes,maximum.wire,'invalid_window_wire');
  // Compact reserialization preserves key order while rejecting duplicate keys,
  // ambiguous numbers/escapes and whitespace. It is not origin authentication.
  if(JSON.stringify(value)!==bytes.toString('utf8'))fail('invalid_window_wire');
  return value;
}
async function syncDirectory(path){if(process.platform==='win32')return;const file=await open(path,constants.O_RDONLY|(constants.O_DIRECTORY??0));try{await file.sync();}finally{await file.close();}}
async function exclusive(path,bytes){
  const file=await open(path,constants.O_WRONLY|constants.O_CREAT|constants.O_EXCL|(constants.O_NOFOLLOW??0),0o600);
  try{await file.writeFile(bytes);await file.sync();}finally{await file.close();}
  await syncDirectory(dirname(path));return{bytes:bytes.length,sha256:sha(bytes)};
}
const encoded=value=>Buffer.from(JSON.stringify(value)+'\n');
async function canonicalFile(path,{privateFile=false}={}){
  if(typeof path!=='string'||!isAbsolute(path)||/[\x00-\x1f\x7f]/.test(path))fail('invalid_private_path');
  const at=resolve(path),info=await lstat(at);
  if(!info.isFile()||info.isSymbolicLink()||privateFile&&info.nlink!==1||canonicalPath(await realpath(at))!==canonicalPath(at))fail('invalid_private_path');
  if(privateFile&&process.platform!=='win32'&&(info.uid!==process.geteuid()||(info.mode&0o077)!==0))fail('private_permissions_unverified');
  return at;
}
async function stableBytes(path,limit,{privateFile=false}={}){
  await canonicalFile(path,{privateFile});
  const before=await lstat(path,{bigint:true}),file=await open(path,constants.O_RDONLY|(constants.O_NOFOLLOW??0)|(constants.O_NONBLOCK??0));
  try{
    const opened=await file.stat({bigint:true});
    const same=(a,b)=>a.ino===b.ino&&(a.dev===b.dev||process.platform==='win32')&&a.size===b.size&&a.mtimeNs===b.mtimeNs&&a.ctimeNs===b.ctimeNs;
    if(!opened.isFile()||opened.ino===0n||!same(before,opened)||opened.size>BigInt(limit))fail('changed_private_artifact');
    const chunks=[];let length=0;
    while(length<=limit){const chunk=Buffer.alloc(Math.min(65536,limit+1-length)),result=await file.read(chunk,0,chunk.length,null);if(!result.bytesRead)break;chunks.push(chunk.subarray(0,result.bytesRead));length+=result.bytesRead;}
    const bytes=Buffer.concat(chunks,length),after=await file.stat({bigint:true}),current=await lstat(path,{bigint:true});
    if(bytes.length>limit||!same(before,after)||!same(before,current)||!current.isFile()||current.isSymbolicLink())fail('changed_private_artifact');
    return bytes;
  }finally{await file.close();}
}
function artifactGraph(value){return exact(value,clientFiles)&&clientFiles.every(name=>typeof value[name]==='string'&&hashPattern.test(value[name]));}
function sameGraph(a,b){return artifactGraph(a)&&artifactGraph(b)&&clientFiles.every(name=>a[name]===b[name]);}
function controlMatches(value,id){return exact(value,['mode','run_id','lease_ms'])&&value.mode==='stdin_monotonic_lease'&&value.run_id===id&&value.lease_ms===5000;}
function reference(value){return exact(value,['bytes','sha256'])&&Number.isSafeInteger(value.bytes)&&value.bytes>=0&&hashPattern.test(value.sha256);}
function hashReference(value){return value&&hashPattern.test(value.sha256)&&(value.bytes===undefined||Number.isSafeInteger(value.bytes)&&value.bytes>=0);}

export function validateWindowConfig(value){
  if(!exact(value,['version','run_id','ssh','remote','client','expected_origin'])||value.version!==1||!runPattern.test(value.run_id))fail('invalid_window_config');
  const s=value.ssh,r=value.remote,c=value.client;
  if(!exact(s,['executable','config_file','known_hosts_file','identity_file','host_alias','host_key_sha256'])||![s.executable,s.config_file,s.known_hosts_file,s.identity_file].every(v=>typeof v==='string'&&isAbsolute(v)&&!/[\x00-\x1f\x7f"]/u.test(v))||!/^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$/.test(s.host_alias)||!/^SHA256:[A-Za-z0-9+/]{43}$/.test(s.host_key_sha256))fail('invalid_window_config');
  if(!exact(r,['script','state_dir','script_sha256','run_sha256'])||!/^\/opt\/ccpanel\/shared\/window225-tools\.[A-Za-z0-9]{6}\/controller\.py$/.test(r.script)||!/^\/opt\/ccpanel\/shared\/window225\.[A-Za-z0-9]{6}$/.test(r.state_dir)||!hashPattern.test(r.script_sha256)||!hashPattern.test(r.run_sha256))fail('invalid_window_config');
  if(!exact(c,['directory','artifacts'])||typeof c.directory!=='string'||!isAbsolute(c.directory)||!artifactGraph(c.artifacts))fail('invalid_window_config');
  let origin;try{origin=new URL(value.expected_origin);}catch{fail('invalid_window_config');}
  if(origin.protocol!=='https:'||origin.origin!==value.expected_origin||origin.username||origin.password)fail('invalid_window_config');
  return value;
}

function subject(child,onLine,onFault,{limit=maximum.wire,stdoutLimit=maximum.stdout,stderrLimit=maximum.stderr}={}){
  if(!(child instanceof ChildProcess)||!child.stdout||!child.stderr)fail('native_child_required');
  let pending=Buffer.alloc(0),stdoutBytes=0,stderrBytes=0,closed=false;
  const stdout=[],stderr=[];
  const done=new Promise(resolve=>{
    child.once('error',()=>onFault('native_spawn_failed'));
    child.once('close',(code,signal)=>{closed=true;if(pending.length)onFault('partial_native_stdout');resolve({code,signal,observed_at:new Date().toISOString()});});
  });
  child.stdout.on('data',chunk=>{
    stdoutBytes+=chunk.length;
    if(stdoutBytes>stdoutLimit){onFault('native_stdout_overflow');return;}
    stdout.push(Buffer.from(chunk));
    pending=Buffer.concat([pending,chunk]);
    while(true){const end=pending.indexOf(10);if(end===-1)break;const line=pending.subarray(0,end);pending=pending.subarray(end+1);if(line.length>limit){onFault('native_line_overflow');continue;}try{onLine(line);}catch{onFault('invalid_native_stdout');}}
    if(pending.length>limit){pending=Buffer.alloc(0);onFault('native_line_overflow');}
  });
  child.stderr.on('data',chunk=>{stderrBytes+=chunk.length;if(stderrBytes>stderrLimit){onFault('native_stderr_overflow');return;}stderr.push(Buffer.from(chunk));});
  return{child,done,get closed(){return closed;},get stdout(){return Buffer.concat(stdout);},get stderr(){return Buffer.concat(stderr);}};
}
function writePipe(pipe,bytes,timeout=1000){return new Promise((resolve,reject)=>{
  if(!pipe||pipe.destroyed||!pipe.writable){reject(new WindowError('control_pipe_closed'));return;}
  const timer=setTimeout(()=>reject(new WindowError('control_pipe_timeout')),timeout);
  pipe.write(bytes,error=>{clearTimeout(timer);if(error)reject(new WindowError('control_pipe_failed'));else resolve();});
});}
class Exchange{
  constructor(child,config,onFault){
    this.config=config;this.onFault=onFault;this.seq=0n;this.pending=null;this.failure=null;this.owner=null;this.ownerHash=null;this.ownerRaw=null;this.tail=Promise.resolve();this.freeze=null;
    this.ownerReady=new Promise((resolve,reject)=>{this.resolveOwner=resolve;this.rejectOwner=reject;});void this.ownerReady.catch(()=>{});
    child.stdin.on('error',()=>this.reject('remote_control_closed'));
    this.native=subject(child,line=>this.line(line),code=>this.reject(code),{stdoutLimit:4*1024*1024});
    this.native.done.then(()=>{if(!this.owner)this.reject('remote_bootstrap_failed');else if(this.pending||!this.freeze)this.reject('remote_control_closed');});
  }
  reject(code){if(this.failure)return;this.failure=new WindowError(code);this.rejectOwner(this.failure);if(this.pending){clearTimeout(this.pending.timer);this.pending.reject(this.failure);this.pending=null;}this.onFault(code);}
  line(raw){
    const value=wire(raw),cfg=this.config;
    if(value.kind==='owner'){
      if(this.owner||!exact(value,['v','kind','run_id','run_sha256','unit','invocation_id','main_pid','start_ticks','controller_sha256','lock_custody'])||value.v!==1||value.run_id!==cfg.run_id||value.run_sha256!==cfg.remote.run_sha256||value.unit!==`ccpanel-capacity-window225-${cfg.run_id}.service`||!runPattern.test(value.invocation_id)||!Number.isSafeInteger(value.main_pid)||value.main_pid<=0||!positive(value.start_ticks)||value.controller_sha256!==cfg.remote.script_sha256||value.lock_custody!=='pid1_fdstore')fail('remote_owner_mismatch');
      this.owner=value;this.ownerRaw=Buffer.concat([raw,Buffer.from('\n')]);this.ownerHash=sha(this.ownerRaw);this.resolveOwner(value);return;
    }
    if(!this.owner||!this.pending)fail('unexpected_remote_reply');
    const waiting=this.pending;
    if(!exact(value,['v','kind','run_id','owner_sha256','seq','nonce','server_us','phase','native_owner','ready_sha256','freeze_sha256','may_feed_client','abort','sampler_elapsed_ms','post_window_work_allowed'])||value.v!==1||value.kind!=='status'||value.run_id!==cfg.run_id||value.owner_sha256!==this.ownerHash||value.seq!==waiting.seq||value.nonce!==waiting.nonce||!positive(value.server_us)||performance.now()-waiting.started>=2000)fail('stale_remote_reply');
    const owner=value.native_owner;
    if(!exact(owner,['unit','invocation_id','main_pid','start_ticks'])||['unit','invocation_id','main_pid','start_ticks'].some(key=>owner[key]!==this.owner[key])||!['preparing','ready','running','settling','freezing','cleaning','completed','aborted','failed'].includes(value.phase)||typeof value.may_feed_client!=='boolean'||typeof value.post_window_work_allowed!=='boolean'||!(value.ready_sha256===null||hashPattern.test(value.ready_sha256))||!(value.sampler_elapsed_ms===null||typeof value.sampler_elapsed_ms==='string'&&/^(0|[1-9][0-9]{0,9})$/.test(value.sampler_elapsed_ms)))fail('remote_owner_mismatch');
    if(!(value.freeze_sha256===null||hashPattern.test(value.freeze_sha256))||value.abort!==null&&(!exact(value.abort,['code','observed_at_us','phase'])||!tokenPattern.test(value.abort.code)||!positive(value.abort.observed_at_us)||!tokenPattern.test(value.abort.phase)))fail('invalid_remote_abort');
    this.lastTiming={sent:waiting.sent,received:Date.now()};
    if(value.freeze_sha256)this.freeze=value.freeze_sha256;
    clearTimeout(waiting.timer);this.pending=null;waiting.resolve(value);
  }
  call(kind,body={}){
    const next=this.tail.then(async()=>{
      if(this.failure)throw this.failure;if(!this.owner)fail('remote_owner_missing');if(this.seq>=9223372036854775807n)fail('wire_sequence_exhausted');
      const seq=String(++this.seq),nonce=randomBytes(16).toString('hex'),message={v:1,kind,run_id:this.config.run_id,owner_sha256:this.ownerHash,seq,nonce,...body},bytes=encoded(message);
      if(bytes.length>maximum.request)fail('wire_request_overflow');
      return new Promise((resolve,reject)=>{
        const timer=setTimeout(()=>this.reject('remote_exchange_timeout'),2000);
        this.pending={seq,nonce,started:performance.now(),sent:Date.now(),timer,resolve,reject};
        void writePipe(this.native.child.stdin,bytes).catch(()=>this.reject('remote_control_failed'));
      });
    });
    this.tail=next.catch(()=>{});return next;
  }
}

function sshArguments(config,globalKnownHosts,command){
  const p=value=>'"'+value.replaceAll('\\','/')+'"',s=config.ssh;
  return['-T','-F',s.config_file,'-i',s.identity_file,'-o','BatchMode=yes','-o','IdentitiesOnly=yes','-o','StrictHostKeyChecking=yes','-o','PasswordAuthentication=no','-o','KbdInteractiveAuthentication=no','-o','ForwardAgent=no','-o','ClearAllForwardings=yes','-o','PermitLocalCommand=no','-o','ConnectTimeout=3','-o','HostKeyAlias='+s.host_alias,'-o','UserKnownHostsFile='+p(s.known_hosts_file),'-o','GlobalKnownHostsFile='+p(globalKnownHosts),s.host_alias,...command];
}
async function sshTransport(config,globalKnownHosts){
  for(const file of [config.ssh.executable,config.ssh.config_file,config.ssh.known_hosts_file,config.ssh.identity_file])await canonicalFile(file,{privateFile:file!==config.ssh.executable});
  const hosts=(await stableBytes(config.ssh.known_hosts_file,16384,{privateFile:true})).toString('utf8').split(/\r?\n/).filter(line=>line.trim()&&!line.trim().startsWith('#'));
  if(hosts.length!==1)fail('host_pin_invalid');const fields=hosts[0].trim().split(/\s+/);
  if(fields.length!==3||fields[0]!==config.ssh.host_alias||!/^[A-Za-z0-9@._+-]+$/.test(fields[1])||!fields[2]||'SHA256:'+createHash('sha256').update(Buffer.from(fields[2],'base64')).digest('base64').replace(/=+$/,'')!==config.ssh.host_key_sha256)fail('host_pin_invalid');
  const launch=(mode,args)=>spawn(config.ssh.executable,sshArguments(config,globalKnownHosts,['/usr/bin/python3',config.remote.script,mode,config.remote.state_dir,...args]),{stdio:['pipe','pipe','pipe'],windowsHide:true});
  return{
    process:launch('launch',[config.remote.run_sha256,config.remote.script_sha256]),actualSSH:true,
    async readArtifact(role,path,ref,ownerHash){
      if(!['run','owner','ready','fixture','binding','freeze','sampler_terminal'].includes(role)||!hashReference(ref)||!hashPattern.test(ownerHash))fail('invalid_artifact_role');
      const child=launch('read-artifact',[config.run_id,ownerHash,role,ref.sha256]);return download(child,path,ref,maximum[role]??16384);
    },
    async receiveArtifact(role,path,ref,ownerHash){
      if(!['client_result','client_manifest','client_progress','client_terminal','client_stdout','client_stderr'].includes(role)||!reference(ref)||!hashPattern.test(ownerHash))fail('invalid_artifact_role');
      const child=launch('receive-artifact',[config.run_id,ownerHash,role,ref.sha256,String(ref.bytes)]);return upload(child,path,ref,{role,run_id:config.run_id,owner_sha256:ownerHash});
    }
  };
}
async function download(child,path,ref,limit){
  if(ref.bytes!==undefined&&ref.bytes>limit)fail('artifact_size_exceeded');
  child.stdin.end();let failure=false,count=0;const hash=createHash('sha256'),file=await open(path,'wx',0o600);
  const terminal=new Promise(resolve=>{child.once('error',()=>{failure=true;});child.once('close',(code,signal)=>resolve({code,signal}));});
  let stderr=0;child.stderr.on('data',data=>{stderr+=data.length;if(stderr>16384){failure=true;child.kill('SIGKILL');}});
  const timer=setTimeout(()=>{failure=true;child.kill('SIGKILL');},15000);
  try{
    for await(const chunk of child.stdout){count+=chunk.length;if(count>limit||ref.bytes!==undefined&&count>ref.bytes){failure=true;child.kill('SIGKILL');break;}hash.update(chunk);await file.writeFile(chunk);}
    const ended=await terminal;if(failure||ended.code!==0||ended.signal!==null||ref.bytes!==undefined&&count!==ref.bytes||hash.digest('hex')!==ref.sha256)fail('artifact_download_failed');
    await file.sync();await file.close();await syncDirectory(dirname(path));
    return{bytes:count,sha256:ref.sha256};
  }finally{clearTimeout(timer);await file.close().catch(()=>{});if(child.exitCode===null&&child.signalCode===null)child.kill('SIGKILL');}
}
async function upload(child,path,ref,expected){
  let stdout=Buffer.alloc(0),failure=false;child.stdout.on('data',data=>{stdout=Buffer.concat([stdout,data]);if(stdout.length>maximum.wire){failure=true;child.kill('SIGKILL');}});child.stderr.on('data',()=>{});child.stdin.on('error',()=>{failure=true;});
  const terminal=new Promise(resolve=>{child.once('error',()=>{failure=true;});child.once('close',(code,signal)=>resolve({code,signal}));});
  const timer=setTimeout(()=>{failure=true;child.kill('SIGKILL');},15000);
  try{
    let count=0;const hash=createHash('sha256');
    for await(const chunk of createReadStream(path)){count+=chunk.length;if(count>ref.bytes)fail('changed_upload_artifact');hash.update(chunk);await writePipe(child.stdin,chunk,5000);}
    child.stdin.end();const ended=await terminal;
    if(failure||ended.code!==0||ended.signal!==null||count!==ref.bytes||hash.digest('hex')!==ref.sha256)fail('artifact_upload_failed');
    const receipt=json(stdout,maximum.wire);if(!exact(receipt,['v','kind','run_id','owner_sha256','role','bytes','sha256'])||receipt.v!==1||receipt.kind!=='artifact_received'||['run_id','owner_sha256','role'].some(key=>receipt[key]!==expected[key])||receipt.bytes!==ref.bytes||receipt.sha256!==ref.sha256)fail('artifact_upload_failed');
    return receipt;
  }finally{clearTimeout(timer);if(child.exitCode===null&&child.signalCode===null)child.kill('SIGKILL');}
}

function validateRun(value,cfg,selfHash,firstStopHash){
  const keys=['v','kind','run_id','release','schema','operator_commit','profile','expected_origin','expected_runtime_sha256','tools_sha256'];
  const toolNames=['controller','coordinator','sampler','host_state','capture','db_operator','first_stop','load_check','load_evidence','load_lease','load_stop_reader'];
  if(!exact(value,keys)||value.v!==1||value.kind!=='run'||value.run_id!==cfg.run_id||value.release!=='m4-polling-014'||value.schema!==22||!/^[a-f0-9]{40}$/.test(value.operator_commit)||value.profile!=='window225_fixed_v1'||value.expected_origin!==cfg.expected_origin||!hashPattern.test(value.expected_runtime_sha256)||!exact(value.tools_sha256,toolNames)||!toolNames.every(key=>hashPattern.test(value.tools_sha256[key])))fail('invalid_remote_run');
  const t=value.tools_sha256,c=cfg.client.artifacts;
  if(t.controller!==cfg.remote.script_sha256||t.coordinator!==selfHash||t.first_stop!==firstStopHash||t.load_check!==c['load-check-public.mjs']||t.load_evidence!==c['load-evidence.mjs']||t.load_lease!==c['load-control-lease.mjs']||t.load_stop_reader!==c['load-stop-reader.mjs'])fail('operator_tool_hash_mismatch');
}
function validateReady(value,cfg,ownerHash){
  if(!exact(value,['v','kind','run_id','owner_sha256','binding_sha256','configuration_sha256','business_before_sha256','monitor_before_sha256','sampler_owner_sha256','fixture_ref','captured_at_us'])||value.v!==1||value.kind!=='ready'||value.run_id!==cfg.run_id||value.owner_sha256!==ownerHash||!['binding_sha256','configuration_sha256','business_before_sha256','monitor_before_sha256','sampler_owner_sha256'].every(key=>hashPattern.test(value[key]))||!positive(value.captured_at_us)||!exact(value.fixture_ref,['role','bytes','sha256'])||value.fixture_ref.role!=='fixture'||!reference({bytes:value.fixture_ref.bytes,sha256:value.fixture_ref.sha256})||value.fixture_ref.bytes>maximum.fixture)fail('invalid_remote_ready');
}
function validateBinding(value,cfg,run,ready){
  if(!exact(value,['version','kind','run_id','planned_events','product','containers','tools','namespace','configuration_sha256'])||value.version!==1||value.kind!=='capacity_window_binding'||value.run_id!==cfg.run_id||value.planned_events!==1800||value.configuration_sha256!==ready.configuration_sha256)fail('invalid_window_binding');
  const p=value.product,t=value.tools,map={db_operator_sha256:'db_operator',generator_sha256:'load_check',evidence_sha256:'load_evidence',lease_sha256:'load_lease',reader_sha256:'load_stop_reader',sampler_sha256:'sampler',controller_sha256:'controller'};
  if(!exact(p,['release','source_commit','schema','api_sha256','worker_sha256','sdk_sha256','compose_sha256'])||p.release!=='m4-polling-014'||p.source_commit!=='b9b213f2313151c1e21be30e924a9af3a1f84a82'||p.schema!==22||!['api_sha256','worker_sha256','sdk_sha256','compose_sha256'].every(key=>hashPattern.test(p[key]))||!exact(t,Object.keys(map))||Object.entries(map).some(([key,tool])=>t[key]!==run.tools_sha256[tool]))fail('invalid_window_binding');
  if(!exact(value.containers,['api','worker','kafka','postgres'])||Object.values(value.containers).some(c=>!exact(c,['id','started_at','restart_count','image_id'])||!hashPattern.test(c.id)||!Number.isFinite(Date.parse(c.started_at))||!Number.isSafeInteger(c.restart_count)||c.restart_count<0||!/^sha256:[a-f0-9]{64}$/.test(c.image_id))||!exact(value.namespace,['events','dlq','group'])||Object.values(value.namespace).some(name=>typeof name!=='string'||!/^[A-Za-z0-9._-]{1,249}$/.test(name)))fail('invalid_window_binding');
}
function validateFixture(value,cfg){
  const uuid=/^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/;
  if(!exact(value,['tenant','project','user','key','email','password','origin','created_at','run_id','write_key_id'])||value.run_id!==cfg.run_id||value.origin!==cfg.expected_origin||!['tenant','project','user','write_key_id'].every(key=>uuid.test(value[key]))||!['key','email','password'].every(key=>typeof value[key]==='string'&&value[key].length>0&&value[key].length<=512&&!/[\x00-\x1f\x7f]/.test(value[key]))||!Number.isFinite(Date.parse(value.created_at)))fail('invalid_private_fixture');
}
function validateRemoteFreeze(value,cfg,ownerHash,terminalRef,submitted){
  if(!exact(value,['v','kind','run_id','run_sha256','owner_sha256','controller_outcome','client_terminal_sha256','sampler_terminal_sha256','artifacts','cleanup','capacity_verified','source_authenticated','latest_state_verified','recovery_verified'])||value.v!==1||value.kind!=='freeze'||value.run_id!==cfg.run_id||value.run_sha256!==cfg.remote.run_sha256||value.owner_sha256!==ownerHash||!['completed','aborted','failed'].includes(value.controller_outcome)||!['completed','deferred','not_started'].includes(value.cleanup)||!(value.client_terminal_sha256===null||hashPattern.test(value.client_terminal_sha256))||!(value.sampler_terminal_sha256===null||hashPattern.test(value.sampler_terminal_sha256))||!Array.isArray(value.artifacts)||value.artifacts.length>64||['capacity_verified','source_authenticated','latest_state_verified','recovery_verified'].some(key=>value[key]!==false))fail('invalid_remote_freeze');
  const refs=new Map();
  for(const item of value.artifacts){
    if(!exact(item,['role','bytes','sha256'])||typeof item.role!=='string'||refs.has(item.role)||!reference({bytes:item.bytes,sha256:item.sha256}))fail('invalid_remote_freeze');
    // This file is published BEFORE controller ExecStopPost. It cannot bind a
    // future controller_terminal receipt; the original SSH native close is
    // observed separately. The role may be read later by the final operator.
    const status=/^status_([1-9][0-9]{0,18})$/.exec(item.role),cap=Object.hasOwn(freezeRoleCaps,item.role)?freezeRoleCaps[item.role]:status&&positive(status[1])?8192:null;
    if(cap===null||item.bytes>cap)fail('invalid_remote_freeze');refs.set(item.role,item);
  }
  if(value.controller_outcome==='completed'){
    if(!terminalRef||value.client_terminal_sha256!==terminalRef.sha256||!hashPattern.test(value.sampler_terminal_sha256)||refs.get('sampler_terminal')?.sha256!==value.sampler_terminal_sha256||refs.get('sampler_terminal')?.bytes===0)fail('remote_terminal_binding_mismatch');
    for(const [role,ref]of submitted){const actual=refs.get(role);if(!actual||actual.bytes!==ref.bytes||actual.sha256!==ref.sha256)fail('remote_client_artifact_mismatch');}
  }
}
async function deadline(promise,ms,code){let timer;try{return await Promise.race([promise,new Promise((_,reject)=>{timer=setTimeout(()=>reject(new WindowError(code)),ms);})]);}finally{clearTimeout(timer);}}

export async function runCapacityWindow(configuration,runDirectory,options={}){
  const cfg=validateWindowConfig(configuration);
  if(Object.keys(options).some(key=>!['testOnly','transportFactory','generatorFactory'].includes(key))||(options.transportFactory||options.generatorFactory)&&options.testOnly!==true)fail('invalid_coordinator_options');
  const directory=resolve(runDirectory),parent=dirname(directory),parentInfo=await lstat(parent);
  if(!parentInfo.isDirectory()||parentInfo.isSymbolicLink()||canonicalPath(await realpath(parent))!==canonicalPath(parent)||process.platform!=='win32'&&(parentInfo.uid!==process.geteuid()||(parentInfo.mode&0o077)!==0))fail('invalid_private_directory');
  await mkdir(directory,{mode:0o700});await syncDirectory(parent);
  const output=join(directory,'client-result.json'),selfHash=sha(await readFile(fileURLToPath(import.meta.url))),firstStopHash=sha(await readFile(new URL('./load-first-stop.mjs',import.meta.url)));
  const runDeadline=performance.now()+2700000;
  let transport,rpc,peer,client,clientExited=false,clientReady=false,initializedSeen=false,clientBinding=null,clientLeaseSeq=0n;
  let lastStatus=null,firstAbort=null,remoteAbort=null,pollTimer,pollInFlight,stopPublication,notifyAbort,overallTimer,clientWatchdog=false;
  let bindingHash=null,ready=null,remoteFrozen=null,clientClose=null,sshClose=null,terminalRef=null,failure=null;
  const files={},events=[];let initializedResolve,initializedReject;
  const initialized=new Promise((resolve,reject)=>{initializedResolve=resolve;initializedReject=reject;});void initialized.catch(()=>{});
  const safeCode=error=>typeof error?.code==='string'&&/^[a-z_]{1,64}$/.test(error.code)?error.code:'coordinator_failed';
  const latch=code=>{
    if(firstAbort)return;
    firstAbort={code:/^[a-z_]{1,64}$/.test(code)?code:'coordinator_failed',observed_at:new Date().toISOString(),monotonic_ms:performance.now()};events.push({kind:'first_abort',...firstAbort});
    if(client&&!client.closed)stopPublication=publishFirstStop(output+'.stop',['resource_memory','resource_disk'].includes(firstAbort.code)?'resource_safety':'operator_requested').then(result=>{events.push({kind:'stop_publication',status:result.status});},error=>{events.push({kind:'stop_publication',error:safeCode(error)});});
    if(rpc?.owner&&!rpc.failure)notifyAbort=rpc.call('abort',{code:['resource_memory','resource_disk','resource_invalid','bridge_stale','owner_changed','sampler_failed','sampler_coverage_budget','client_spawn_failed','protocol_invalid'].includes(firstAbort.code)?firstAbort.code:'operator_requested'}).catch(()=>{});
  };
  const record=async(name,bytes)=>{const ref=await exclusive(join(directory,name),bytes);files[name]=ref;return ref;};
  const readArtifact=async(role,ref)=>{
    if(!hashReference(ref))fail('invalid_artifact_reference');const path=join(directory,role+'.json');
    await transport.readArtifact(role,path,ref,rpc.ownerHash);const bytes=await stableBytes(path,maximum[role]??16384,{privateFile:true});
    if(sha(bytes)!==ref.sha256||ref.bytes!==undefined&&bytes.length!==ref.bytes)fail('artifact_download_failed');files[basename(path)]={bytes:bytes.length,sha256:sha(bytes)};return bytes;
  };
  const poll=()=>{
    if(pollInFlight||!rpc?.owner||rpc.failure)return pollInFlight;
    pollInFlight=rpc.call('heartbeat',{client:clientBinding}).then(async status=>{
      lastStatus=status;
      const clock=rpc.lastTiming,server=BigInt(status.server_us)/1000n;
      if(server-BigInt(clock.received)<-1000n||server-BigInt(clock.sent)>1000n)fail('clock_offset_unverified');
      if(status.abort){remoteAbort??=status.abort;latch(status.abort.code);}
      if(status.sampler_elapsed_ms!==null&&BigInt(status.sampler_elapsed_ms)>=1940000n&&client&&!client.closed)latch('sampler_coverage_budget');
      if(status.freeze_sha256){clearInterval(pollTimer);return;}
      if(clientReady&&client&&!client.closed&&!clientExited&&!firstAbort&&status.may_feed_client&&status.abort===null){
        if(status.sampler_elapsed_ms===null)fail('sampler_identity_missing');
        if(clientLeaseSeq===0n&&BigInt(status.sampler_elapsed_ms)>90000n)fail('insufficient_sampler_budget');
        const frame=encoded({v:1,run_id:cfg.run_id,seq:String(++clientLeaseSeq)});
        try{await writePipe(client.child.stdin,frame);}catch(error){if(!clientExited&&!client.closed)throw error;}
      }
    }).catch(error=>latch(safeCode(error))).finally(()=>{pollInFlight=undefined;});
    return pollInFlight;
  };
  const waitStatus=async predicate=>{
    while(!predicate(lastStatus)){
      if(performance.now()>=runDeadline||firstAbort&&performance.now()-firstAbort.monotonic_ms>=150000)fail('window_deadline');
      if(rpc?.failure)throw rpc.failure;
      if(peer?.closed)fail('remote_ended_without_freeze');
      if(firstAbort&&(!client||!client.closed)&&!predicate(lastStatus))fail(firstAbort.code);
      await sleep(20);
    }
    return lastStatus;
  };
  const awaitClient=async()=>{
    if(!client||clientClose)return clientClose;
    let timer;
    const watch=setInterval(()=>{
      if(firstAbort&&!timer)timer=setTimeout(()=>{if(!client.closed){clientWatchdog=true;client.child.kill('SIGKILL');}},25000);
    },100);
    try{clientClose=await deadline(client.done,Math.max(1,runDeadline-performance.now()+30000),'client_native_terminal_missing');return clientClose;}finally{clearInterval(watch);clearTimeout(timer);}
  };
  const signals=new Map(['SIGINT','SIGTERM'].map(signal=>[signal,()=>latch('operator_requested')]));
  for(const [signal,handler]of signals)process.once(signal,handler);
  try{
    for(const name of clientFiles){const bytes=await stableBytes(join(cfg.client.directory,name),512*1024);if(sha(bytes)!==cfg.client.artifacts[name])fail('client_tool_hash_mismatch');}
    await record('client-artifacts.json',encoded(cfg.client.artifacts));
    const globalPath=join(directory,'empty-global-known-hosts');await record('empty-global-known-hosts',Buffer.alloc(0));
    transport=options.transportFactory?await options.transportFactory(cfg,directory):await sshTransport(cfg,globalPath);
    if(!transport||!(transport.process instanceof ChildProcess)||typeof transport.readArtifact!=='function'||typeof transport.receiveArtifact!=='function')fail('native_transport_required');
    rpc=new Exchange(transport.process,cfg,latch);peer=rpc.native;
    overallTimer=setTimeout(()=>latch('window_deadline'),2700000);
    await deadline(rpc.ownerReady,20000,'remote_owner_timeout');await record('owner.json',rpc.ownerRaw);
    pollTimer=setInterval(()=>void poll(),1000);await poll();
    await deadline(waitStatus(status=>status?.ready_sha256!==null&&!!status?.ready_sha256),180000,'remote_ready_timeout');
    if(firstAbort)fail(firstAbort.code);
    if(lastStatus.sampler_elapsed_ms===null||BigInt(lastStatus.sampler_elapsed_ms)>90000n)fail('insufficient_sampler_budget');
    const run=json(await readArtifact('run',{sha256:cfg.remote.run_sha256}),16384);validateRun(run,cfg,selfHash,firstStopHash);
    ready=json(await readArtifact('ready',{sha256:lastStatus.ready_sha256}),maximum.ready);validateReady(ready,cfg,rpc.ownerHash);if(ready.configuration_sha256!==run.expected_runtime_sha256)fail('runtime_configuration_mismatch');
    bindingHash=ready.binding_sha256;const binding=json(await readArtifact('binding',{sha256:bindingHash}),maximum.binding);validateBinding(binding,cfg,run,ready);
    const fixtureBytes=await readArtifact('fixture',{bytes:ready.fixture_ref.bytes,sha256:ready.fixture_ref.sha256}),fixture=json(fixtureBytes,maximum.fixture);validateFixture(fixture,cfg);
    if(firstAbort)fail(firstAbort.code);
    const environment={...process.env,CCPANEL_LOAD_RUN_ID:cfg.run_id};delete environment.NODE_OPTIONS;if(environment.NODE_TLS_REJECT_UNAUTHORIZED==='0')fail('untrusted_https_environment');environment.NODE_TLS_REJECT_UNAUTHORIZED='1';
    const native=options.generatorFactory?await options.generatorFactory(cfg,directory):spawn(process.execPath,[join(cfg.client.directory,'load-check-public.mjs'),join(directory,'fixture.json'),output,'1800'],{env:environment,stdio:['pipe','pipe','pipe'],windowsHide:true});
    client=subject(native,line=>{
      const value=json(line,maximum.wire,'invalid_client_diagnostic');
      if(value.kind==='workload_initialized'){
        if(initializedSeen||!exact(value,['kind','run_id','writer_pid','manifest_sha256'])||value.run_id!==cfg.run_id||value.writer_pid!==native.pid||!hashPattern.test(value.manifest_sha256))fail('invalid_client_initialization');
        initializedSeen=true;initializedResolve(value);
      }else if(value.kind==='workload_abort_observed'){
        if(value.run_id!==cfg.run_id||value.pid!==native.pid||typeof value.reason!=='string'||!tokenPattern.test(value.reason))fail('invalid_client_diagnostic');
        events.push({kind:'client_abort',reason:value.reason});latch('client_abort_observed');
      }
    },code=>{initializedReject(new WindowError(code));latch(code);});
    native.stdin.on('error',()=>{if(!clientExited&&!client.closed)latch('client_control_closed');});native.once('exit',()=>{clientExited=true;});
    if(!Number.isSafeInteger(native.pid)||native.pid<=0)fail('client_spawn_failed');
    const init=await deadline(Promise.race([initialized,client.done.then(()=>{throw new WindowError('client_initialization_missing');})]),10000,'client_initialization_timeout');
    const firstManifestBytes=await stableBytes(output+'.manifest.json',maximum.manifest,{privateFile:true}),firstManifest=json(firstManifestBytes,maximum.manifest);
    if(sha(firstManifestBytes)!==init.manifest_sha256||firstManifest.writer_pid!==native.pid||firstManifest.status!=='initializing'||!controlMatches(firstManifest.control,cfg.run_id)||!sameGraph(firstManifest.artifacts,cfg.client.artifacts)||!Array.isArray(firstManifest.event_ids)||firstManifest.event_ids.length!==0)fail('client_initial_manifest_mismatch');
    clientBinding={pid:native.pid,spawn_nonce:randomBytes(16).toString('hex')};clientReady=true;await poll();
    await awaitClient();
    const close={code:clientClose.code,signal:clientClose.signal,watchdog_fired:clientWatchdog};
    await record('client-close.json',encoded({version:1,kind:'capacity_client_close',run_id:cfg.run_id,pid:native.pid,close,observed_at:clientClose.observed_at}));
    await record('client-stdout.log',client.stdout);await record('client-stderr.log',client.stderr);
    const captured=await Promise.allSettled([stableBytes(output,maximum.result,{privateFile:true}),stableBytes(output+'.manifest.json',maximum.manifest,{privateFile:true})]);
    for(const [index,name]of ['client-result.json','client-result.json.manifest.json'].entries())if(captured[index].status==='fulfilled'){const bytes=captured[index].value;files[name]={bytes:bytes.length,sha256:sha(bytes)};}
    if(captured.some(item=>item.status==='rejected'))fail('client_evidence_missing');
    const resultBytes=captured[0].value,manifestBytes=captured[1].value;
    const result=json(resultBytes,maximum.result,'invalid_final_client_json'),manifest=json(manifestBytes,maximum.manifest,'invalid_final_client_json');
    if(!sameGraph(result.artifacts,cfg.client.artifacts)||!sameGraph(manifest.artifacts,cfg.client.artifacts)||!controlMatches(result.control,cfg.run_id)||!controlMatches(manifest.control,cfg.run_id)||manifest.writer_pid!==native.pid||result.tenant!==fixture.tenant||result.project!==fixture.project||!['completed','aborted'].includes(result.termination?.state)||manifest.status!==result.termination.state)fail('invalid_final_client_evidence');
    terminalRef=await record('client-terminal.json',encoded({version:1,kind:'capacity_client_terminal',run_id:cfg.run_id,binding_sha256:bindingHash,pid:native.pid,close,observed_at:clientClose.observed_at,result:files['client-result.json'],manifest:files['client-result.json.manifest.json'],artifacts:result.artifacts,control:result.control}));
    const transfers=[['client_result','client-result.json'],['client_manifest','client-result.json.manifest.json'],['client_terminal','client-terminal.json'],['client_stdout','client-stdout.log'],['client_stderr','client-stderr.log']];
    try{const bytes=await stableBytes(output+'.progress',65536,{privateFile:true});files['client-result.json.progress']={bytes:bytes.length,sha256:sha(bytes)};transfers.push(['client_progress','client-result.json.progress']);}catch(error){if(error.code!=='ENOENT')throw error;}
    await rpc.call('client_terminal',{client:clientBinding,close:{exit_code:clientClose.code,signal:clientClose.signal,observed_at:clientClose.observed_at},termination:result.termination.state,artifacts:transfers.map(([role,name])=>({role,...files[name]}))});
    // The remote gate waits for the SAME 1980s sampler's natural terminal,
    // not merely the first frame after the client's ~1800s completion. Leave
    // bounded room for its remaining ~180s plus StopPost/poll, still subject
    // to the original absolute 45-minute overall deadline in waitStatus().
    await deadline(waitStatus(status=>status?.post_window_work_allowed===true),210000,'post_window_boundary_missing');
    for(const [role,name]of transfers)await transport.receiveArtifact(role,join(directory,name),files[name],rpc.ownerHash);
    await deadline(waitStatus(status=>!!status?.freeze_sha256),2700000,'remote_freeze_missing');
    const freezeBytes=await readArtifact('freeze',{sha256:lastStatus.freeze_sha256});remoteFrozen=json(freezeBytes,16384,'invalid_remote_freeze');
    validateRemoteFreeze(remoteFrozen,cfg,rpc.ownerHash,terminalRef,new Map(transfers.map(([role,name])=>[role,files[name]])));
    sshClose=await deadline(peer.done,150000,'remote_native_terminal_missing');
    if(sshClose.code!==0||sshClose.signal!==null||remoteFrozen.controller_outcome!=='completed')fail('remote_controller_incomplete');
  }catch(error){failure=safeCode(error);latch(failure);}
  finally{
    clearInterval(pollTimer);clearTimeout(overallTimer);for(const [signal,handler]of signals)process.removeListener(signal,handler);
    await Promise.allSettled([stopPublication,notifyAbort,pollInFlight]);
    if(client&&!clientClose)try{await awaitClient();}catch{failure??='client_native_terminal_missing';}
    if(client){
      if(clientClose&&!files['client-close.json'])try{await record('client-close.json',encoded({version:1,kind:'capacity_client_close',run_id:cfg.run_id,pid:client.child.pid,close:{code:clientClose.code,signal:clientClose.signal,watchdog_fired:clientWatchdog},observed_at:clientClose.observed_at}));}catch{failure??='local_freeze_failed';}
      for(const [name,bytes]of [['client-stdout.log',client.stdout],['client-stderr.log',client.stderr]])if(!files[name])try{await record(name,bytes);}catch{failure??='local_freeze_failed';}
      if(clientClose)for(const [name,limit]of [['client-result.json',maximum.result],['client-result.json.manifest.json',maximum.manifest],['client-result.json.progress',65536]]){
        try{const bytes=await stableBytes(join(directory,name),limit,{privateFile:true});files[name]={bytes:bytes.length,sha256:sha(bytes)};if(name!=='client-result.json.progress')try{json(bytes,limit,'invalid_final_client_json');}catch{events.push({kind:'client_evidence_error',code:'invalid_final_client_json'});failure??='invalid_final_client_json';}}
        catch(error){if(error.code!=='ENOENT')failure??='client_artifact_unverified';}
      }
    }
    if(peer&&!sshClose){
      peer.child.stdin.end();
      try{sshClose=await deadline(peer.done,150000,'remote_native_terminal_missing');}catch{failure??='remote_native_terminal_missing';peer.child.kill('SIGKILL');sshClose=await deadline(peer.done,5000,'remote_native_terminal_missing').catch(()=>null);}
    }
  }
  if(peer){await record('ssh-stdout.log',peer.stdout);await record('ssh-stderr.log',peer.stderr);}
  const productionChildStarted=!options.generatorFactory&&Number.isSafeInteger(client?.child.pid);
  const freeze={version:1,kind:'capacity_window_coordinator_freeze',run_id:cfg.run_id,binding_sha256:bindingHash,actual_ssh:options.testOnly!==true&&transport?.actualSSH===true&&Number.isSafeInteger(peer?.child.pid),actual_generator:productionChildStarted,actual_tls:false,production_https_policy_enforced:options.testOnly!==true&&productionChildStarted,node_runtime:{version:process.version,exec_path_sha256:sha(Buffer.from(process.execPath)),exec_argv_count:process.execArgv.length,child_node_options_removed:productionChildStarted,child_tls_reject_unauthorized:productionChildStarted?'1':null},controller_completed:!failure&&!firstAbort&&sshClose?.code===0&&sshClose?.signal===null,first_abort:firstAbort,remote_abort:remoteAbort,failure,client_native_close:clientClose,client_watchdog_fired:clientWatchdog,ssh_native_close:sshClose,client_terminal:terminalRef,remote_freeze_sha256:files['freeze.json']?.sha256??null,artifacts:files,events,capacity_verified:false,source_authenticated:false,latest_state_verified:false,recovery_verified:false,windows_private_acl_verified:false};
  await exclusive(join(directory,'coordinator-freeze.json'),encoded(freeze));return freeze;
}

if(process.argv[1]&&canonicalPath(resolve(process.argv[1]))===canonicalPath(fileURLToPath(import.meta.url))){
  try{
    const [configPath,directory,...extra]=process.argv.slice(2);if(!configPath||!directory||extra.length)fail('usage_capacity_window_private_config_new_directory');
    const cfg=json(await stableBytes(resolve(configPath),16384,{privateFile:true}),16384,'invalid_window_config');
    const result=await runCapacityWindow(cfg,directory);console.log(JSON.stringify({run_id:result.run_id,controller_completed:result.controller_completed,failure:result.failure,capacity_verified:false}));if(!result.controller_completed)process.exitCode=1;
  }catch(error){console.error(JSON.stringify({code:error instanceof WindowError?error.code:'capacity_window_failed'}));process.exitCode=1;}
}
