// Real loopback OpenSSH/SFTP fixture. Never targets a configured VPS/destination.
import assert from 'node:assert/strict';
import {spawn,execFileSync,type ChildProcess} from 'node:child_process';
import {createCipheriv,createHash,randomBytes} from 'node:crypto';
import {mkdtemp,mkdir,writeFile,readFile,readdir,stat,unlink,rmdir,lstat,chmod} from 'node:fs/promises';
import {createServer,createConnection,type Server,type Socket} from 'node:net';
import {tmpdir,userInfo} from 'node:os';
import {join,resolve,sep} from 'node:path';
import {transferBackupCandidateSftp} from '../packages/server/src/backup-sftp';
import {decryptBackupCandidate} from '../packages/server/src/backup-decrypt';

assert.equal(process.platform,'linux','real OpenSSH fixture requires Linux');
assert.ok(process.env.GITHUB_ACTIONS==='true'||process.env.CCPANEL_ISOLATED_SFTP_FIXTURE==='true','explicit isolated fixture required');
const username=userInfo().username;assert.match(username,/^[A-Za-z_][A-Za-z0-9_-]{0,63}$/);
const directory=await mkdtemp(join(tmpdir(),'ccpanel-sftp-fixture-'));
const sourceDirectory=join(directory,'source'),downloadRoot=join(directory,'download'),remoteRoot=join(directory,'remote');
const identityFile=join(directory,'identity'),wrongIdentity=join(directory,'wrong-identity'),hostKey=join(directory,'host'),wrongHostKey=join(directory,'wrong-host');
const knownHostsFile=join(directory,'known_hosts'),wrongHostsFile=join(directory,'wrong_hosts'),encryptionKey=join(directory,'encryption-key'),output=join(directory,'authenticated.dump');
let daemon:ChildProcess|undefined,daemonClosed:Promise<void>|undefined,stage='setup',failureCode:string|undefined,daemonDiagnostic:string|undefined,summary:Record<string,unknown>|undefined;
let relay:Server|undefined;
const relaySockets=new Set<Socket>(),relayClosed:Promise<void>[]=[];
function trackRelaySocket(socket:Socket){relaySockets.add(socket);relayClosed.push(new Promise(resolve=>socket.once('close',()=>{relaySockets.delete(socket);resolve();})));}
async function stopRelay(){
  for(const socket of relaySockets)socket.destroy();
  if(relay?.listening)await new Promise<void>((resolve,reject)=>relay!.close(error=>error?reject(error):resolve()));
  await Promise.all(relayClosed);
}
const hash=(bytes:Buffer)=>createHash('sha256').update(bytes).digest('hex');
const delay=(ms:number)=>new Promise<void>(resolve=>setTimeout(resolve,ms));
async function unusedPort(){
  const socket=createServer();await new Promise<void>((resolve,reject)=>{socket.once('error',reject);socket.listen(0,'127.0.0.1',resolve);});
  const address=socket.address();assert.ok(address&&typeof address==='object');const port=address.port;
  await new Promise<void>((resolve,reject)=>socket.close(error=>error?reject(error):resolve()));return port;
}
async function connectable(port:number){
  return new Promise<boolean>(resolve=>{
    const socket=createConnection({host:'127.0.0.1',port});let settled=false;
    const done=(value:boolean)=>{if(settled)return;settled=true;socket.destroy();resolve(value);};
    socket.setTimeout(500,()=>done(false));socket.once('connect',()=>done(true));socket.once('error',()=>done(false));
  });
}
async function cleanupOwned(path:string){
  const absolute=resolve(path);assert.ok(absolute===directory||absolute.startsWith(directory+sep),'fixture cleanup outside owned root');
  const info=await lstat(absolute);
  if(!info.isDirectory()||info.isSymbolicLink()){await unlink(absolute);return;}
  await chmod(absolute,0o700);
  for(const item of await readdir(absolute))await cleanupOwned(join(absolute,item));await rmdir(absolute);
}
async function rejectedWithoutWrites(operation:()=>Promise<unknown>,expected?:string){
  const beforeRemote=(await readdir(remoteRoot)).sort(),beforeDownload=(await readdir(downloadRoot)).sort();
  const result=await operation().then(value=>({value,error:undefined}),error=>({value:undefined,error}));
  assert.ok(result.error,'negative SFTP fixture unexpectedly completed');
  if(expected)assert.equal(result.error.code,expected);else assert.match(String(result.error.code),/^sftp_/);
  assert.deepEqual((await readdir(remoteRoot)).sort(),beforeRemote);assert.deepEqual((await readdir(downloadRoot)).sort(),beforeDownload);
}
try{
  for(const path of [sourceDirectory,downloadRoot,remoteRoot])await mkdir(path,{mode:0o700});
  // Generate disposable local identities only. No authorized user key is read.
  for(const path of [identityFile,wrongIdentity,hostKey,wrongHostKey])execFileSync('ssh-keygen',['-q','-t','ed25519','-N','','-f',path],{stdio:'ignore',timeout:10000});
  const authorized=(await readFile(identityFile+'.pub','utf8')).trim();
  const authorizedKeys=join(directory,'authorized_keys');await writeFile(authorizedKeys,authorized+'\n',{mode:0o600,flag:'wx'});
  const port=await unusedPort(),config=join(directory,'sshd_config');
  const configuration=[
    `Port ${port}`,'ListenAddress 127.0.0.1',`HostKey ${hostKey}`,`PidFile ${join(directory,'sshd.pid')}`,
    // This owned fixture lives under /tmp; bypass only sshd's ancestor-path
    // StrictModes check. Client credential uid/mode checks remain exercised.
    `AuthorizedKeysFile ${authorizedKeys}`,'StrictModes no','PasswordAuthentication no','KbdInteractiveAuthentication no',
    'PubkeyAuthentication yes','AuthenticationMethods publickey','UsePAM no','PermitRootLogin prohibit-password',
    `AllowUsers ${username}`,'AllowTcpForwarding no','X11Forwarding no','PermitTunnel no','PermitTTY no',
    'Subsystem sftp internal-sftp','LogLevel ERROR',
  ];
  await writeFile(config,configuration.join('\n')+'\n',{mode:0o600,flag:'wx'});
  stage='sshd_start';
  // Non-root, high-port daemon; does not use or alter /etc/ssh or host services.
  daemon=spawn('/usr/sbin/sshd',['-D','-e','-f',config],{stdio:['ignore','ignore','pipe']});
  let daemonError=false;daemon.once('error',()=>{daemonError=true;daemonDiagnostic='sshd_spawn_failed';});
  daemon.stderr?.on('data',(chunk:Buffer)=>{
    // Retain only fixed diagnostic categories, never raw server/key/path text.
    const text=chunk.toString('utf8').slice(0,4096);
    if(text.includes('Missing privilege separation directory'))daemonDiagnostic='privilege_separation_directory_missing';
    else if(text.includes('account is locked'))daemonDiagnostic='fixture_account_locked';
    else if(text.includes('Could not load host key'))daemonDiagnostic='fixture_host_key_load_failed';
    else if(text.includes('Bad ownership')||text.includes('bad ownership'))daemonDiagnostic='fixture_authorized_path_rejected';
  });
  daemonClosed=new Promise(resolve=>{daemon!.once('close',()=>resolve());});
  const readyDeadline=Date.now()+10000;let ready=false;
  while(Date.now()<readyDeadline){
    if(daemonError||daemon.exitCode!==null)break;
    if(await connectable(port)){ready=true;break;}await delay(50);
  }
  assert.ok(ready&&!daemonError&&daemon.exitCode===null,'isolated sshd did not become ready');
  const publicHost=(await readFile(hostKey+'.pub','utf8')).trim().split(/\s+/).slice(0,2).join(' ');
  const publicWrongHost=(await readFile(wrongHostKey+'.pub','utf8')).trim().split(/\s+/).slice(0,2).join(' ');
  await writeFile(knownHostsFile,`[127.0.0.1]:${port} ${publicHost}\n`,{mode:0o600,flag:'wx'});
  await writeFile(wrongHostsFile,`[127.0.0.1]:${port} ${publicWrongHost}\n`,{mode:0o600,flag:'wx'});
  const plain=Buffer.concat([Buffer.from('isolated SFTP authenticated fixture\n'),Buffer.alloc(256*1024,29)]);
  const key=randomBytes(32),iv=randomBytes(12),magic=Buffer.from('CCPBK001'),cipher=createCipheriv('aes-256-gcm',key,iv);cipher.setAAD(magic);
  const encrypted=Buffer.concat([magic,iv,cipher.update(plain),cipher.final(),cipher.getAuthTag()]);
  await writeFile(encryptionKey,key,{mode:0o600,flag:'wx'});key.fill(0);
  const manifest={version:1,status:'captured_unverified',startedAt:'2026-11-02T00:00:00.000Z',capturedAt:'2026-11-02T00:00:01.000Z',recoveryVerified:false,offhostVerified:false,latestStateVerified:false,source:{database:'sftp_fixture',schemaVersion:22,releaseVerified:false},fingerprints:['["public.tenants", 0, 0]'],archive:{file:'database.dump.enc',format:'CCPBK001',bytes:encrypted.length,sha256:hash(encrypted),plaintextBytes:plain.length,plaintextSha256:hash(plain)}};
  const rawManifest=JSON.stringify(manifest)+'\n';
  await writeFile(join(sourceDirectory,'capture.json'),rawManifest,{mode:0o600,flag:'wx'});await writeFile(join(sourceDirectory,'database.dump.enc'),encrypted,{mode:0o600,flag:'wx'});
  await writeFile(join(remoteRoot,'operator-note'),'keep-unknown-object',{mode:0o600,flag:'wx'});
  const options={sourceDirectory,downloadRoot,remoteRoot,id:'candidate-001',expectedSchema:22,maxBytes:encrypted.length,timeoutMs:15000,connection:{host:'127.0.0.1',port,user:username,knownHostsFile,identityFile}};
  stage='transfer';const copied=await transferBackupCandidateSftp(options);
  assert.equal(copied.status,'transferred_unverified');assert.equal(copied.hostKeyVerified,true);assert.equal(copied.downloadReadbackChecked,true);assert.equal(copied.sourceManifestUnchanged,true);assert.equal(copied.remoteDirectoryDurabilityVerified,false);
  assert.equal(copied.sha256,hash(encrypted));assert.equal(copied.manifestSha256,hash(Buffer.from(rawManifest)));assert.equal(copied.bytes,encrypted.length);
  for(const field of ['recoveryVerified','offhostVerified','latestStateVerified','sourceAuthenticated'] as const)assert.equal(copied[field],false);
  const downloaded=join(downloadRoot,options.id),remote=join(remoteRoot,options.id);
  for(const path of [downloaded,remote]){
    assert.deepEqual((await readdir(path)).sort(),['capture.json','database.dump.enc']);
    assert.equal(await readFile(join(path,'capture.json'),'utf8'),rawManifest);assert.deepEqual(await readFile(join(path,'database.dump.enc')),encrypted);
    assert.equal((await stat(path)).mode&0o777,0o700);
    for(const file of ['capture.json','database.dump.enc'])assert.equal((await stat(join(path,file))).mode&0o777,0o600);
  }
  stage='destination_decrypt';const decrypted=await decryptBackupCandidate({directory:downloaded,keyFile:encryptionKey,output,expectedSchema:22,maxBytes:encrypted.length,timeoutMs:15000});
  assert.equal(decrypted.authenticatedDecryption,true);assert.equal(decrypted.plaintextReadbackChecked,true);assert.equal(decrypted.recoveryVerified,false);assert.deepEqual(await readFile(output),plain);
  stage='repeat_id';await rejectedWithoutWrites(()=>transferBackupCandidateSftp(options),'sftp_destination_exists');
  stage='wrong_host_pin';await rejectedWithoutWrites(()=>transferBackupCandidateSftp({...options,id:'wrong-pin',connection:{...options.connection,knownHostsFile:wrongHostsFile}}));
  stage='wrong_auth_key';await rejectedWithoutWrites(()=>transferBackupCandidateSftp({...options,id:'wrong-auth',connection:{...options.connection,identityFile:wrongIdentity}}));
  stage='preabort';const abort=new AbortController();abort.abort();await rejectedWithoutWrites(()=>transferBackupCandidateSftp({...options,id:'cancelled',signal:abort.signal}),'sftp_aborted');
  stage='mid_upload_abort';
  const midAbort=new AbortController();let thresholdObserved=false,clientBytes=0;
  // Real encrypted TCP relay: the same sshd host key is pinned at this port.
  // The cancellation barrier withholds the chunk crossing the threshold; no
  // SFTP packet, callback or filesystem operation is fabricated.
  relay=createServer(front=>{
    const upstream=createConnection({host:'127.0.0.1',port});trackRelaySocket(front);trackRelaySocket(upstream);
    front.on('data',(chunk:Buffer)=>{
      clientBytes+=chunk.length;
      if(!thresholdObserved&&clientBytes>=128*1024){thresholdObserved=true;midAbort.abort();return;}
      if(!thresholdObserved&&!upstream.write(chunk))front.pause();
    });
    upstream.on('drain',()=>front.resume());upstream.pipe(front);
    front.once('end',()=>upstream.end());
    front.on('error',()=>upstream.destroy());upstream.on('error',()=>front.destroy());
    front.once('close',()=>upstream.destroy());upstream.once('close',()=>front.destroy());
  });
  await new Promise<void>((resolve,reject)=>{relay!.once('error',reject);relay!.listen(0,'127.0.0.1',resolve);});
  const relayAddress=relay.address();assert.ok(relayAddress&&typeof relayAddress==='object');
  const relayPort=relayAddress.port,relayHosts=join(directory,'relay_hosts');
  await writeFile(relayHosts,`[127.0.0.1]:${relayPort} ${publicHost}\n`,{mode:0o600,flag:'wx'});
  const cancelledId='cancel-upload';
  const cancelled=await transferBackupCandidateSftp({...options,id:cancelledId,signal:midAbort.signal,connection:{...options.connection,port:relayPort,knownHostsFile:relayHosts}}).then(value=>({value,error:undefined}),error=>({value:undefined,error}));
  // First prove the real network precondition before judging cancellation.
  assert.equal(thresholdObserved,true,'mid-upload TCP threshold not observed');assert.ok(clientBytes>=128*1024);assert.equal(cancelled.error?.code,'sftp_aborted');
  await stopRelay();assert.equal(relaySockets.size,0);assert.equal(await connectable(relayPort),false);
  const partial=join(remoteRoot,cancelledId),partialNames=(await readdir(partial)).sort();
  assert.ok(partialNames.includes('database.dump.enc'));assert.ok(!partialNames.includes('capture.json'));
  assert.ok(!(await readdir(join(downloadRoot,cancelledId))).includes('capture.json'));
  const partialBytes=await readFile(join(partial,'database.dump.enc'));assert.ok(partialBytes.length>0&&partialBytes.length<encrypted.length,'partial upload boundary not observed');
  stage='mid_abort_same_id_retry';
  await rejectedWithoutWrites(()=>transferBackupCandidateSftp({...options,id:cancelledId}),'sftp_destination_exists');
  assert.deepEqual((await readdir(partial)).sort(),partialNames);assert.deepEqual(await readFile(join(partial,'database.dump.enc')),partialBytes);
  assert.deepEqual(await readFile(join(remote,'database.dump.enc')),encrypted);assert.equal(await readFile(join(remote,'capture.json'),'utf8'),rawManifest);
  stage='corrupt_source';const changed=Buffer.from(encrypted);changed[100]^=1;await writeFile(join(sourceDirectory,'database.dump.enc'),changed);
  await rejectedWithoutWrites(()=>transferBackupCandidateSftp({...options,id:'corrupt-source'}),'backup_candidate_cipher_mismatch');
  assert.equal(await readFile(join(remoteRoot,'operator-note'),'utf8'),'keep-unknown-object');assert.equal(await readFile(join(sourceDirectory,'capture.json'),'utf8'),rawManifest);
  summary={passed:true,surface:'ci_loopback_openssh',actual_ssh_sftp:true,host_pin_verified:true,wrong_pin_refused_without_writes:true,wrong_identity_refused_without_writes:true,duplicate_id_preserved:true,preabort_without_writes:true,mid_upload_tcp_barrier_observed:true,mid_upload_abort_without_manifest:true,partial_same_id_retry_preserved:true,owned_relay_closed:true,corrupt_source_without_upload:true,remote_and_download_bytes_exact:true,destination_authenticated_decryption:true,unknown_remote_object_preserved:true,source_manifest_unchanged:true,source_identity_verified:false,independent_failure_domain_verified:false,remote_directory_durability_verified:false,actual_pg_restore:false,bytes:encrypted.length};
}catch(error){
  const code=(error as {code?:unknown})?.code;failureCode=typeof code==='string'&&/^[a-z0-9_]{1,96}$/.test(code)?code:'fixture_assertion_failed';
}finally{
  await stopRelay();
  if(daemon&&daemon.exitCode===null){daemon.kill('SIGTERM');await Promise.race([daemonClosed,delay(3000)]);if(daemon.exitCode===null){daemon.kill('SIGKILL');await daemonClosed;}}
  await cleanupOwned(directory);
}
if(failureCode){console.error(JSON.stringify({passed:false,code:failureCode,stage,...(daemonDiagnostic?{daemonDiagnostic}: {})}));process.exitCode=1;}
else console.log(JSON.stringify({...summary,owned_daemon_closed:true,private_fixture_removed:true}));
