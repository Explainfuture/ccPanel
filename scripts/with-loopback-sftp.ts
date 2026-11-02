// CI-only real OpenSSH service for a caller-owned backup/restore verification.
// It never receives user SSH identities, external hosts or production paths.
import {spawn,execFileSync,type ChildProcess} from 'node:child_process';
import {mkdtemp,mkdir,writeFile,readFile,readdir,lstat,realpath,chmod,unlink,rmdir} from 'node:fs/promises';
import {createServer,createConnection} from 'node:net';
import {tmpdir,userInfo} from 'node:os';
import {join,resolve,sep} from 'node:path';

export interface LoopbackSftpContext{
  connection:{host:string;port:number;user:string;knownHostsFile:string;identityFile:string};
  remoteRoot:string;downloadRoot:string;directory:string;
}
export class LoopbackSftpFixtureError extends Error{
  constructor(public code:string){super(code);this.name='LoopbackSftpFixtureError';}
}
const fail=(code:string):never=>{throw new LoopbackSftpFixtureError(code);};
const delay=(ms:number)=>new Promise<void>(done=>setTimeout(done,ms));
async function availablePort(){
  const listener=createServer();
  try{
    await new Promise<void>((done,reject)=>{listener.once('error',reject);listener.listen(0,'127.0.0.1',done);});
    const address=listener.address();if(!address||typeof address!=='object')return fail('loopback_sftp_port_unavailable');return address.port;
  }finally{if(listener.listening)await new Promise<void>((done,reject)=>listener.close(error=>error?reject(error):done()));}
}
async function listening(port:number){
  return new Promise<boolean>(done=>{
    const socket=createConnection({host:'127.0.0.1',port});let settled=false;
    const finish=(value:boolean)=>{if(settled)return;settled=true;socket.destroy();done(value);};
    socket.setTimeout(500,()=>finish(false));socket.once('connect',()=>finish(true));socket.once('error',()=>finish(false));
  });
}
async function waitBriefly(closed:Promise<void>){
  let timer:ReturnType<typeof setTimeout>|undefined;
  try{await Promise.race([closed,new Promise<void>(done=>{timer=setTimeout(done,3000);})]);}
  finally{if(timer)clearTimeout(timer);}
}

export async function withLoopbackSftp<T>(run:(context:LoopbackSftpContext)=>Promise<T>):Promise<T>{
  if(process.platform!=='linux'||process.env.GITHUB_ACTIONS!=='true'||process.env.CCPANEL_SFTP_PG_CI!=='true'||process.env.CCPANEL_ISOLATED_CAPTURE_VPS==='true')fail('loopback_sftp_ci_only');
  const user=userInfo().username;if(!/^[A-Za-z_][A-Za-z0-9_-]{0,63}$/.test(user))fail('loopback_sftp_user_invalid');
  const directory=await mkdtemp(join(tmpdir(),'ccpanel-sftp-pg-'));
  let daemon:ChildProcess|undefined,daemonClosed:Promise<void>|undefined,hasClosed=false,port:number|undefined,diagnostic:string|undefined;
  let result:T|undefined,primaryError:unknown,failed=false,callbackStarted=false,cleanupFailed=false;
  const cleanup=async(path:string):Promise<void>=>{
    const absolute=resolve(path);if(absolute!==directory&&!absolute.startsWith(directory+sep))fail('loopback_sftp_cleanup_outside_root');
    const info=await lstat(absolute);
    if(!info.isDirectory()||info.isSymbolicLink()){await unlink(absolute);return;}
    await chmod(absolute,0o700);
    for(const entry of await readdir(absolute))await cleanup(join(absolute,entry));await rmdir(absolute);
  };
  try{
    // Directives use only bounded, whitespace-free owned paths/current user.
    if(resolve(directory)!==directory||await realpath(directory)!==directory||/[\x00-\x20"\\]/.test(directory))fail('loopback_sftp_directory_unsafe');
    const remoteRoot=join(directory,'remote'),downloadRoot=join(directory,'download');
    for(const path of [remoteRoot,downloadRoot])await mkdir(path,{mode:0o700});
    const identityFile=join(directory,'identity'),hostKey=join(directory,'host'),knownHostsFile=join(directory,'known_hosts');
    for(const path of [identityFile,hostKey])execFileSync('ssh-keygen',['-q','-t','ed25519','-N','','-f',path],{stdio:'ignore',timeout:10000});
    const authorizedKeys=join(directory,'authorized_keys');
    await writeFile(authorizedKeys,(await readFile(identityFile+'.pub','utf8')).trim()+'\n',{mode:0o600,flag:'wx'});
    const selectedPort=await availablePort();port=selectedPort;const config=join(directory,'sshd_config');
    await writeFile(config,[`Port ${selectedPort}`,'ListenAddress 127.0.0.1',`HostKey ${hostKey}`,`PidFile ${join(directory,'sshd.pid')}`,`AuthorizedKeysFile ${authorizedKeys}`,
      // /tmp ancestors are shared. Only this disposable daemon bypasses its
      // ancestor-path StrictModes check; transfer uid/mode checks stay active.
      'StrictModes no','PasswordAuthentication no','KbdInteractiveAuthentication no','PubkeyAuthentication yes','AuthenticationMethods publickey','UsePAM no','PermitRootLogin prohibit-password',
      `AllowUsers ${user}`,'AllowTcpForwarding no','X11Forwarding no','PermitTunnel no','PermitTTY no','Subsystem sftp internal-sftp','LogLevel ERROR',''].join('\n'),{mode:0o600,flag:'wx'});
    daemon=spawn('/usr/sbin/sshd',['-D','-e','-f',config],{stdio:['ignore','ignore','pipe']});
    daemonClosed=new Promise<void>(done=>daemon!.once('close',()=>{hasClosed=true;done();}));
    daemon.once('error',()=>{diagnostic='loopback_sftp_daemon_spawn_failed';});
    daemon.stderr?.on('data',(chunk:Buffer)=>{
      const text=chunk.toString('utf8').slice(0,4096);
      if(text.includes('Missing privilege separation directory'))diagnostic='loopback_sftp_privilege_directory_missing';
      else if(text.includes('account is locked'))diagnostic='loopback_sftp_fixture_account_locked';
      else if(text.includes('Could not load host key'))diagnostic='loopback_sftp_host_key_unavailable';
    });
    const deadline=performance.now()+10000;let ready=false;
    while(performance.now()<deadline){
      if(diagnostic||hasClosed)break;if(await listening(selectedPort)){ready=true;break;}await delay(50);
    }
    if(!ready||hasClosed||diagnostic)fail(diagnostic??'loopback_sftp_daemon_not_ready');
    const publicHost=(await readFile(hostKey+'.pub','utf8')).trim().split(/\s+/).slice(0,2).join(' ');
    await writeFile(knownHostsFile,`[127.0.0.1]:${selectedPort} ${publicHost}\n`,{mode:0o600,flag:'wx'});
    callbackStarted=true;result=await run({directory,remoteRoot,downloadRoot,connection:{host:'127.0.0.1',port:selectedPort,user,knownHostsFile,identityFile}});
  }catch(error){failed=true;primaryError=callbackStarted?error:error instanceof LoopbackSftpFixtureError?error:new LoopbackSftpFixtureError('loopback_sftp_setup_failed');}
  finally{
    if(daemon&&daemonClosed){
      try{
        if(!hasClosed){daemon.kill('SIGTERM');await waitBriefly(daemonClosed);if(!hasClosed){daemon.kill('SIGKILL');await daemonClosed;}}
        await daemonClosed;
      }catch{cleanupFailed=true;}
    }
    // Do not signal any unrelated process that wins a race for the old port.
    if(port!==undefined&&await listening(port))cleanupFailed=true;
    if(!daemon||hasClosed){
      try{await cleanup(directory);try{await lstat(directory);cleanupFailed=true;}catch(error){if((error as NodeJS.ErrnoException).code!=='ENOENT')cleanupFailed=true;}}
      catch{cleanupFailed=true;}
    }else cleanupFailed=true; // Keep private evidence if our daemon did not close.
  }
  if(cleanupFailed)fail('loopback_sftp_cleanup_failed');
  if(failed)throw primaryError;
  return result as T;
}
