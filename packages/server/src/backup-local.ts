import {constants} from 'node:fs';
import {open,lstat,realpath,mkdir,statfs,link,unlink} from 'node:fs/promises';
import {isAbsolute,resolve,join,sep} from 'node:path';
import {createHash,randomBytes} from 'node:crypto';
import {inspectBackupCandidate,BackupCandidateError} from './backup-candidate';

export class BackupLocalError extends Error{constructor(public code:string){super(code);this.name='BackupLocalError';}}
export interface LocalCopyOptions{sourceDirectory:string;destinationRoot:string;id:string;expectedSchema:number;maxBytes:number;timeoutMs:number;signal?:AbortSignal}
// Explicit transport component: destination root must already be private and
// independently provisioned. Copying locally does not verify a failure domain.
export async function copyBackupCandidateToLocal(options:LocalCopyOptions){
  const fail=(code:string):never=>{throw new BackupLocalError(code);};
  if(![options.sourceDirectory,options.destinationRoot].every(p=>typeof p==='string'&&isAbsolute(p))||typeof options.id!=='string'||!/^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$/.test(options.id)||![options.expectedSchema,options.maxBytes,options.timeoutMs].every(n=>Number.isSafeInteger(n)&&n>0)||options.timeoutMs>1800000)fail('local_copy_options_invalid');
  const deadline=performance.now()+options.timeoutMs;
  const check=()=>{if(options.signal?.aborted)fail('local_copy_aborted');if(performance.now()>=deadline)fail('local_copy_timeout');};check();
  if(process.platform==='win32')fail('local_copy_permissions_unverified');
  const source=resolve(options.sourceDirectory),root=resolve(options.destinationRoot),output=join(root,options.id);
  if(source===root||source.startsWith(root+sep)||root.startsWith(source+sep))fail('local_copy_overlap');
  let published=false;
  try{
    const rootStat=await lstat(root);
    const privateDirectory=async(path:string,expected:{dev:number;ino:number})=>{
      const current=await lstat(path);
      if(!current.isDirectory()||current.isSymbolicLink()||current.dev!==expected.dev||current.ino!==expected.ino||current.uid!==process.geteuid?.()||(current.mode&0o077)!==0||await realpath(path)!==path)fail('local_copy_root_unsafe');
    };
    await privateDirectory(root,rootStat);
    const inspect=(directory:string)=>{check();return inspectBackupCandidate({directory,expectedSchema:options.expectedSchema,maxBytes:options.maxBytes,timeoutMs:Math.max(1,Math.ceil(deadline-performance.now())),signal:options.signal});};
    const initial=await inspect(source);check();
    const space=await statfs(root,{bigint:true});if(space.bavail*space.bsize<BigInt(initial.bytes)+2097152n)fail('local_copy_space_low');
    await privateDirectory(root,rootStat);check();
    try{await mkdir(output,{mode:0o700});}catch(error){if((error as NodeJS.ErrnoException).code==='EEXIST')fail('local_copy_exists');throw error;}
    const outputStat=await lstat(output);
    const sync=async(path:string)=>{check();const directory=await open(path,constants.O_RDONLY|(constants.O_DIRECTORY??0)|(constants.O_NOFOLLOW??0));try{await directory.sync();}finally{await directory.close();}};
    await sync(root);
    const read=async(path:string,max:number,consume?:(chunk:Buffer)=>Promise<void>)=>{
      check();const file=await open(path,constants.O_RDONLY|(constants.O_NOFOLLOW??0)|(constants.O_NONBLOCK??0));
      try{
        const before=await file.stat();
        if(!before.isFile()||before.uid!==process.geteuid?.()||(before.mode&0o077)!==0||before.size<1||before.size>max)fail('local_copy_file_unsafe');
        const hash=createHash('sha256'),buffer=Buffer.alloc(65536);let bytes=0;
        while(true){check();const result=await file.read(buffer,0,Math.min(buffer.length,max-bytes+1),null);if(!result.bytesRead)break;bytes+=result.bytesRead;if(bytes>max)fail('local_copy_size_limit');const chunk=buffer.subarray(0,result.bytesRead);if(consume)await consume(chunk);hash.update(chunk);}
        const after=await file.stat(),current=await lstat(path);check();
        if(bytes!==before.size||after.size!==before.size||after.mtimeMs!==before.mtimeMs||after.ctimeMs!==before.ctimeMs||current.dev!==before.dev||current.ino!==before.ino||current.isSymbolicLink())fail('local_copy_changed');
        return{bytes,sha256:hash.digest('hex')};
      }finally{await file.close();}
    };
    const copy=async(name:string,target:string,max:number,expectedHash:string,expectedBytes?:number)=>{
      await privateDirectory(root,rootStat);await privateDirectory(output,outputStat);check();
      const file=await open(join(output,target),constants.O_WRONLY|constants.O_CREAT|constants.O_EXCL|(constants.O_NOFOLLOW??0),0o600);
      let copied;
      try{
        copied=await read(join(source,name),max,async chunk=>{let offset=0;while(offset<chunk.length){check();const {bytesWritten}=await file.write(chunk,offset,chunk.length-offset,null);if(bytesWritten<1)fail('local_copy_write_failed');offset+=bytesWritten;}});
        if(copied.sha256!==expectedHash||(expectedBytes!==undefined&&copied.bytes!==expectedBytes))fail('local_copy_changed');
        await file.sync();check();
      }finally{await file.close();}
      const reread=await read(join(output,target),max);
      if(reread.bytes!==copied.bytes||reread.sha256!==copied.sha256)fail('local_copy_readback_failed');
    };
    // No plaintext or key is copied. An incomplete set has no capture.json.
    await copy('database.dump.enc','database.dump.enc',initial.bytes,initial.sha256,initial.bytes);
    const temporary='.capture-'+randomBytes(12).toString('hex')+'.tmp';
    await copy('capture.json',temporary,1048576,initial.manifestSha256);
    const finalSource=await inspect(source);
    if(finalSource.manifestSha256!==initial.manifestSha256||finalSource.sha256!==initial.sha256||finalSource.bytes!==initial.bytes)fail('local_copy_changed');
    await privateDirectory(root,rootStat);await privateDirectory(output,outputStat);check();
    // Exclusive hardlink, not rename-overwrite. The candidate manifest is last;
    // it is not a verified manifest and never updates a success index.
    await link(join(output,temporary),join(output,'capture.json'));published=true;
    await sync(output);await unlink(join(output,temporary));await sync(output);await sync(root);
    const destination=await inspect(output);
    if(destination.manifestSha256!==initial.manifestSha256||destination.sha256!==initial.sha256||destination.bytes!==initial.bytes)fail('local_copy_readback_failed');
    await privateDirectory(root,rootStat);await privateDirectory(output,outputStat);check();
    return{...destination,status:'copied_unverified' as const,destinationReadbackChecked:true,sourceManifestUnchanged:true};
  }catch(error){
    // Retain only the owned incomplete set for inspection. Never recursively
    // delete paths or remove existing sets, and never retry under the same id.
    if(published)throw new BackupLocalError('local_copy_publish_uncertain');
    if(error instanceof BackupLocalError)throw error;
    if(error instanceof BackupCandidateError)throw new BackupLocalError(error.code);
    throw new BackupLocalError('local_copy_failed');
  }
}
