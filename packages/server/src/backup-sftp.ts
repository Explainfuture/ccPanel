import ssh2,{type SFTPWrapper,type Stats} from 'ssh2';
import {constants} from 'node:fs';
import {open,lstat,realpath,mkdir,statfs} from 'node:fs/promises';
import {isAbsolute,resolve,join,sep,posix} from 'node:path';
import {isIP} from 'node:net';
import {createHash} from 'node:crypto';
import {inspectBackupCandidate,BackupCandidateError} from './backup-candidate';
import {createBackupHostVerifier,BackupHostPinError} from './backup-host-pins';

export class SftpBackupError extends Error{constructor(public code:string){super(code);this.name='SftpBackupError';}}
export interface SftpBackupOptions{
  sourceDirectory:string;downloadRoot:string;id:string;remoteRoot:string;expectedSchema:number;maxBytes:number;timeoutMs:number;signal?:AbortSignal;
  connection:{host:string;port:number;user:string;knownHostsFile:string;identityFile:string};
}
type TransferReceipt=Omit<Awaited<ReturnType<typeof inspectBackupCandidate>>,'status'>&{status:'transferred_unverified';hostKeyVerified:boolean;remoteFileSyncChecked:boolean;remoteDirectoryDurabilityVerified:boolean;downloadReadbackChecked:boolean;sourceManifestUnchanged:boolean};
// SFTP-only transport. No remote exec, passwords, agent forwarding or TOFU.
// Remote host identity and byte readback do not verify independent failure
// domains, latest-state authenticity or remote directory crash durability.
export async function transferBackupCandidateSftp(options:SftpBackupOptions){
  const fail=(code:string):never=>{throw new SftpBackupError(code);};
  const c=options.connection;
  const host=(v:string)=>isIP(v)!==0||v.split('.').every(label=>/^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/i.test(label));
  if(!c||typeof c.host!=='string'||!c.host.length||c.host.length>253||!host(c.host)||!Number.isInteger(c.port)||c.port<1||c.port>65535||typeof c.user!=='string'||!/^[a-z_][a-z0-9_-]{0,63}$/i.test(c.user)||![options.sourceDirectory,options.downloadRoot,c.knownHostsFile,c.identityFile].every(p=>typeof p==='string'&&isAbsolute(p))||typeof options.id!=='string'||!/^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$/.test(options.id)||typeof options.remoteRoot!=='string'||options.remoteRoot.length>4096||!posix.isAbsolute(options.remoteRoot)||options.remoteRoot!==posix.normalize(options.remoteRoot)||/[\\\x00-\x1f\x7f]/.test(options.remoteRoot)||options.remoteRoot.split('/').some(p=>p==='.'||p==='..')||![options.expectedSchema,options.maxBytes,options.timeoutMs].every(n=>Number.isSafeInteger(n)&&n>0)||options.timeoutMs>1800000)fail('sftp_options_invalid');
  if(options.signal?.aborted)fail('sftp_aborted');if(process.platform==='win32')fail('sftp_permissions_unverified');
  const source=resolve(options.sourceDirectory),root=resolve(options.downloadRoot),download=join(root,options.id),remote=posix.join(options.remoteRoot,options.id);
  if(source===root||source.startsWith(root+sep)||root.startsWith(source+sep))fail('sftp_local_overlap');
  const client=new ssh2.Client(),deadline=performance.now()+options.timeoutMs;
  let stopError:SftpBackupError|undefined,rejectStop!:(error:Error)=>void,privateKey:Buffer|undefined,connecting=false,finishing=false,hostRejected=false,hostVerified=false,publicationAttempted=false;
  const stopped=new Promise<never>((_,reject)=>{rejectStop=reject;});void stopped.catch(()=>{});
  const closed=new Promise<void>(done=>client.once('close',()=>{done();if(!finishing)stop('sftp_connection_closed');}));
  const stop=(code:string)=>{stopError??=new SftpBackupError(code);rejectStop(stopError);client.destroy();};
  const check=()=>{if(stopError)throw stopError;if(options.signal?.aborted)fail('sftp_aborted');if(performance.now()>=deadline)fail('sftp_timeout');};
  const abort=()=>stop('sftp_aborted'),timer=setTimeout(()=>stop('sftp_timeout'),options.timeoutMs);
  options.signal?.addEventListener('abort',abort,{once:true});if(options.signal?.aborted)abort();
  client.on('error',()=>stop(hostRejected?'sftp_host_key_rejected':'sftp_connection_failed'));
  const rpc=<T>(invoke:(done:(error:Error|null|undefined,value?:T)=>void)=>void):Promise<T>=>{check();return Promise.race([new Promise<T>((accept,reject)=>{try{invoke((error,value)=>error?reject(error):accept(value as T));}catch(error){reject(error);}}),stopped]);};
  const inspect=(directory:string)=>{check();return inspectBackupCandidate({directory,expectedSchema:options.expectedSchema,maxBytes:options.maxBytes,timeoutMs:Math.max(1,Math.ceil(deadline-performance.now())),signal:options.signal});};
  const flags=constants.O_RDONLY|(constants.O_NOFOLLOW??0)|(constants.O_NONBLOCK??0);
  const readLocal=async(path:string,max:number,consume:(chunk:Buffer)=>Promise<void>)=>{
    check();const file=await open(path,flags);
    try{
      const before=await file.stat();if(!before.isFile()||before.uid!==process.geteuid?.()||(before.mode&0o077)!==0||before.size<1||before.size>max)fail('sftp_local_file_unsafe');
      const buffer=Buffer.alloc(65536),hash=createHash('sha256');let bytes=0;
      while(true){check();const result=await file.read(buffer,0,Math.min(buffer.length,max-bytes+1),null);if(!result.bytesRead)break;bytes+=result.bytesRead;if(bytes>max)fail('sftp_size_limit');const chunk=buffer.subarray(0,result.bytesRead);await consume(chunk);hash.update(chunk);}
      const after=await file.stat(),current=await lstat(path);check();if(bytes!==before.size||after.size!==before.size||after.mtimeMs!==before.mtimeMs||after.ctimeMs!==before.ctimeMs||current.dev!==before.dev||current.ino!==before.ino||current.isSymbolicLink())fail('sftp_source_changed');
      return{bytes,sha256:hash.digest('hex')};
    }finally{await file.close();}
  };
  const credentials=async(path:string,max:number)=>{const chunks:Buffer[]=[];try{await readLocal(path,max,async chunk=>{chunks.push(Buffer.from(chunk));});return Buffer.concat(chunks);}finally{for(const chunk of chunks)chunk.fill(0);}};
  let operation:Promise<TransferReceipt>|undefined;
  try{
    operation=(async()=>{
      const parent=await lstat(root);
      const localRoot=async()=>{const current=await lstat(root);if(!current.isDirectory()||current.isSymbolicLink()||current.dev!==parent.dev||current.ino!==parent.ino||current.uid!==process.geteuid?.()||(current.mode&0o077)!==0||await realpath(root)!==root)fail('sftp_download_root_unsafe');check();};
      await localRoot();const initial=await inspect(source);
      const space=await statfs(root,{bigint:true});if(space.bavail*space.bsize<BigInt(initial.bytes)+2097152n)fail('sftp_local_space_low');
      const known=await credentials(c.knownHostsFile,1048576);let verifier:(raw:Buffer)=>boolean;
      try{verifier=createBackupHostVerifier(known.toString('utf8'),c.host,c.port);}finally{known.fill(0);}
      privateKey=await credentials(c.identityFile,65536);const parsed=ssh2.utils.parseKey(privateKey);
      if(parsed instanceof Error||!parsed.isPrivateKey())fail('sftp_identity_invalid');
      const ready=new Promise<void>(done=>client.once('ready',done));
      check();client.connect({host:c.host,port:c.port,username:c.user,privateKey,authHandler:['publickey'],tryKeyboard:false,readyTimeout:Math.max(1,Math.ceil(deadline-performance.now())),keepaliveInterval:1000,keepaliveCountMax:2,hostVerifier:(raw:Buffer|string)=>{hostVerified=Buffer.isBuffer(raw)&&verifier(raw);hostRejected=!hostVerified;return hostVerified;}});connecting=true;
      await Promise.race([ready,stopped]);check();
      const sftp=await rpc<SFTPWrapper>(done=>client.sftp(done));
      const remoteStat=(path:string)=>rpc<Stats>(done=>sftp.lstat(path,done));
      const remotePath=(path:string)=>rpc<string>(done=>sftp.realpath(path,done));
      const owner=(await remoteStat(options.remoteRoot)).uid;
      if(!Number.isSafeInteger(owner)||owner<0||owner>4294967295)fail('sftp_remote_owner_unverified');
      const remoteDirectory=async(path:string)=>{const stats=await remoteStat(path);if(!stats.isDirectory()||stats.isSymbolicLink()||(stats.mode&0o077)!==0||stats.uid!==owner||await remotePath(path)!==path)fail('sftp_remote_directory_unsafe');check();};
      await remoteDirectory(options.remoteRoot);
      const remoteAlreadyExists=await remoteStat(remote).then(()=>true,error=>{if((error as {code?:number}).code===2)return false;throw error;});
      if(remoteAlreadyExists)fail('sftp_destination_exists');
      // Reserve local download before any remote mutation; never reuse a set.
      await localRoot();check();try{await mkdir(download,{mode:0o700});}catch(error){if((error as NodeJS.ErrnoException).code==='EEXIST')fail('sftp_download_exists');throw error;}
      const downloadStat=await lstat(download);
      const localDownload=async()=>{const current=await lstat(download);if(!current.isDirectory()||current.isSymbolicLink()||current.dev!==downloadStat.dev||current.ino!==downloadStat.ino||current.uid!==process.geteuid?.()||(current.mode&0o077)!==0||await realpath(download)!==download)fail('sftp_download_root_unsafe');check();};
      await localDownload();
      try{await rpc<void>(done=>sftp.mkdir(remote,{mode:0o700},done));}catch(error){
        if(stopError)throw stopError;
        const exists=await remoteStat(remote).then(()=>true,()=>false);if(exists)fail('sftp_destination_exists');throw error;
      }
      await remoteDirectory(remote);
      const remoteFile=async(path:string)=>{const stats=await remoteStat(path);if(!stats.isFile()||stats.isSymbolicLink()||(stats.mode&0o077)!==0||stats.uid!==owner||!Number.isSafeInteger(stats.mtime)||stats.mtime<0)fail('sftp_remote_file_unsafe');return stats;};
      const upload=async(localName:string,remoteName:string,max:number,expectedHash:string,expectedBytes?:number)=>{
        await remoteDirectory(remote);check();const path=posix.join(remote,remoteName),handle=await rpc<Buffer>(done=>sftp.open(path,'wx',{mode:0o600},done));
        try{
          const stats=await rpc<Stats>(done=>sftp.fstat(handle,done));if(!stats.isFile()||stats.uid!==owner||(stats.mode&0o077)!==0||stats.size!==0)fail('sftp_remote_file_unsafe');
          let offset=0;const actual=await readLocal(join(source,localName),max,async chunk=>{await rpc<void>(done=>sftp.write(handle,chunk,0,chunk.length,offset,done));offset+=chunk.length;});
          if(actual.sha256!==expectedHash||(expectedBytes!==undefined&&actual.bytes!==expectedBytes))fail('sftp_source_changed');
          await rpc<void>(done=>sftp.ext_openssh_fsync(handle,done));
        }finally{if(!stopError)await rpc<void>(done=>sftp.close(handle,done));}
      };
      const downloadFile=async(remoteName:string,localName:string,max:number,expectedHash:string,expectedBytes?:number)=>{
        await localRoot();await localDownload();await remoteDirectory(remote);const path=posix.join(remote,remoteName),before=await remoteFile(path);
        if(!Number.isSafeInteger(before.size)||before.size<1||before.size>max)fail('sftp_size_limit');
        const handle=await rpc<Buffer>(done=>sftp.open(path,'r',done));let file;
        try{
          const stats=await rpc<Stats>(done=>sftp.fstat(handle,done));if(!stats.isFile()||stats.uid!==owner||(stats.mode&0o077)!==0||stats.size!==before.size)fail('sftp_remote_file_unsafe');
          file=await open(join(download,localName),constants.O_WRONLY|constants.O_CREAT|constants.O_EXCL|(constants.O_NOFOLLOW??0),0o600);
          const hash=createHash('sha256'),buffer=Buffer.alloc(65536);let bytes=0;
          while(true){check();const length=Math.min(buffer.length,max-bytes+1),count=await rpc<number>(done=>sftp.read(handle,buffer,0,length,bytes,(err,n)=>done(err,n)));if(!count)break;bytes+=count;if(bytes>max)fail('sftp_size_limit');const chunk=buffer.subarray(0,count);hash.update(chunk);let offset=0;while(offset<count){check();const result=await file.write(chunk,offset,count-offset,null);if(!result.bytesWritten)fail('sftp_local_write_failed');offset+=result.bytesWritten;}}
          if(bytes!==before.size||hash.digest('hex')!==expectedHash||(expectedBytes!==undefined&&bytes!==expectedBytes))fail('sftp_download_mismatch');
          const after=await rpc<Stats>(done=>sftp.fstat(handle,done)),current=await remoteFile(path);if(after.size!==before.size||after.mtime!==before.mtime||current.size!==before.size||current.mtime!==before.mtime)fail('sftp_remote_changed');
          await file.sync();check();
        }finally{await file?.close();if(!stopError)await rpc<void>(done=>sftp.close(handle,done));}
      };
      await upload('database.dump.enc','database.dump.enc',initial.bytes,initial.sha256,initial.bytes);
      await upload('capture.json','.capture.pending',1048576,initial.manifestSha256);
      // Download from the remote handles before publishing the candidate name.
      await downloadFile('database.dump.enc','database.dump.enc',initial.bytes,initial.sha256,initial.bytes);
      await downloadFile('.capture.pending','capture.json',1048576,initial.manifestSha256);
      const downloaded=await inspect(download),finalSource=await inspect(source);
      if(downloaded.manifestSha256!==initial.manifestSha256||finalSource.manifestSha256!==initial.manifestSha256||downloaded.sha256!==initial.sha256||finalSource.sha256!==initial.sha256)fail('sftp_source_changed');
      await remoteDirectory(remote);check();publicationAttempted=true;
      await rpc<void>(done=>sftp.ext_openssh_hardlink(posix.join(remote,'.capture.pending'),posix.join(remote,'capture.json'),done));
      await rpc<void>(done=>sftp.unlink(posix.join(remote,'.capture.pending'),done));
      // Re-open the published manifest and compare its complete bounded bytes.
      const handle=await rpc<Buffer>(done=>sftp.open(posix.join(remote,'capture.json'),'r',done));
      try{
        await remoteFile(posix.join(remote,'capture.json'));const hash=createHash('sha256'),buffer=Buffer.alloc(65536);let bytes=0;
        while(true){check();const count=await rpc<number>(done=>sftp.read(handle,buffer,0,Math.min(buffer.length,1048577-bytes),bytes,(err,n)=>done(err,n)));if(!count)break;bytes+=count;if(bytes>1048576)fail('sftp_size_limit');hash.update(buffer.subarray(0,count));}
        if(hash.digest('hex')!==initial.manifestSha256)fail('sftp_download_mismatch');
      }finally{if(!stopError)await rpc<void>(done=>sftp.close(handle,done));}
      // Local directory durability is checked. SFTP file fsync is not proof of
      // remote parent-directory fsync, so that claim remains explicitly false.
      for(const path of [download,root]){const directory=await open(path,constants.O_RDONLY|(constants.O_DIRECTORY??0)|(constants.O_NOFOLLOW??0));try{await directory.sync();}finally{await directory.close();}}
      await remoteDirectory(options.remoteRoot);await remoteDirectory(remote);await localRoot();await localDownload();check();
      return{...downloaded,status:'transferred_unverified' as const,hostKeyVerified:hostVerified,remoteFileSyncChecked:true,remoteDirectoryDurabilityVerified:false,downloadReadbackChecked:true,sourceManifestUnchanged:true};
    })();
    return await Promise.race([operation,stopped]);
  }catch(error){
    if(publicationAttempted)throw new SftpBackupError('sftp_publish_uncertain');
    if(error instanceof SftpBackupError)throw error;
    if(error instanceof BackupHostPinError||error instanceof BackupCandidateError)throw new SftpBackupError(error.code);
    throw new SftpBackupError('sftp_transfer_failed');
  }finally{
    finishing=true;clearTimeout(timer);options.signal?.removeEventListener('abort',abort);client.destroy();
    // Let all local descriptors settle even when a timed-out RPC is aborted.
    await operation?.catch(()=>{});if(connecting)await closed;privateKey?.fill(0);
  }
}
