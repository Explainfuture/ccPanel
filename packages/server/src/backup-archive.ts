import {spawn} from 'node:child_process';
import {createHash,randomBytes} from 'node:crypto';
import {constants} from 'node:fs';
import {lstat,realpath,open,link,unlink} from 'node:fs/promises';
import {isAbsolute,resolve,dirname,basename,join} from 'node:path';
import {Writable} from 'node:stream';
import {pipeline} from 'node:stream/promises';

export class BackupArchiveError extends Error {
  constructor(public code:string){super(code);this.name='BackupArchiveError';}
}
export interface ArchiveOptions {
  output:string;command:string;args:string[];env?:NodeJS.ProcessEnv;
  maxBytes:number;timeoutMs:number;signal?:AbortSignal;
}
// Captures an unverified artifact, not a completed/recoverable backup. Commands
// are trusted adapters (e.g. pg_dump), never user-supplied shell expressions.
export async function captureBackupArchive(options:ArchiveOptions){
  const fail=(code:string):never=>{throw new BackupArchiveError(code);};
  if(!Number.isSafeInteger(options.maxBytes)||options.maxBytes<1||!Number.isSafeInteger(options.timeoutMs)||options.timeoutMs<100||options.timeoutMs>1800000||typeof options.output!=='string'||!isAbsolute(options.output)||!options.command||!Array.isArray(options.args)||options.args.some(arg=>typeof arg!=='string'))fail('archive_options_invalid');
  if(options.signal?.aborted)fail('archive_aborted');
  // An enabled backup preflight already refuses unverified Windows ACLs.
  if(process.platform==='win32')fail('archive_permissions_unverified');
  const output=resolve(options.output),directory=dirname(output),name=basename(output);
  if(!/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(name)||name==='.'||name==='..')fail('archive_name_invalid');
  let parent;
  try{parent=await lstat(directory);if(!parent.isDirectory()||parent.isSymbolicLink()||(parent.mode&0o077)!==0||parent.uid!==process.geteuid?.()||await realpath(directory)!==directory)fail('archive_directory_unsafe');}
  catch(error){if(error instanceof BackupArchiveError)throw error;fail('archive_directory_unavailable');}
  try{await lstat(output);fail('archive_output_exists');}catch(error){if(error instanceof BackupArchiveError)throw error;if((error as NodeJS.ErrnoException).code!=='ENOENT')fail('archive_output_unavailable');}
  const temporary=join(directory,'.'+name+'.'+randomBytes(12).toString('hex')+'.tmp');
  let file;
  try{file=await open(temporary,constants.O_WRONLY|constants.O_CREAT|constants.O_EXCL|(constants.O_NOFOLLOW??0),0o600);}catch{fail('archive_temporary_unavailable');}
  const handle=file!;
  let child:ReturnType<typeof spawn>|undefined,closed=false,published=false,bytes=0,failure:BackupArchiveError|undefined;
  let finished:Promise<{code:number|null;signal:NodeJS.Signals|null}>|undefined;
  let timer:ReturnType<typeof setTimeout>|undefined;
  const hash=createHash('sha256');
  const stop=(code:string)=>{
    failure??=new BackupArchiveError(code);
    if(child&&!closed){try{process.kill(-child.pid!,'SIGKILL');}catch{child.kill('SIGKILL');}}
    child?.stdout?.destroy(failure);
  };
  const abort=()=>stop('archive_aborted');
  try{
    if(options.signal?.aborted)fail('archive_aborted');
    child=spawn(options.command,options.args,{env:options.env??process.env,stdio:['ignore','pipe','pipe'],shell:false,detached:true});
    finished=new Promise(resolve=>child!.once('close',(code,signal)=>{closed=true;resolve({code,signal});}));
    child.on('error',()=>stop('archive_spawn_failed'));
    child.stderr!.resume(); // Do not retain or echo SQL, payloads or credentials.
    timer=setTimeout(()=>stop('archive_timeout'),options.timeoutMs);
    options.signal?.addEventListener('abort',abort,{once:true});if(options.signal?.aborted)abort();
    const sink=new Writable({highWaterMark:65536,write(chunk:Buffer,_encoding,done){
      if(bytes+chunk.length>options.maxBytes){done(new BackupArchiveError('archive_size_limit'));return;}
      bytes+=chunk.length;hash.update(chunk);
      const write=async()=>{let offset=0;while(offset<chunk.length){const result=await handle.write(chunk,offset,chunk.length-offset);if(result.bytesWritten<1)throw Error('short_write');offset+=result.bytesWritten;}};
      void write().then(()=>done(),()=>done(new BackupArchiveError('archive_write_failed')));
    }});
    try{await pipeline(child.stdout!,sink);}catch(error){stop(error instanceof BackupArchiveError?error.code:'archive_stream_failed');}
    const status=await finished;
    if(failure)throw failure;
    if(status.code!==0||status.signal!==null)fail('archive_child_failed');
    if(bytes===0)fail('archive_empty');
    await handle.sync();if(failure)throw failure;
    const current=await lstat(directory);
    if(current.dev!==parent!.dev||current.ino!==parent!.ino)fail('archive_directory_changed');
    // link is exclusive: an existing output, including a racing publisher,
    // remains untouched. An artifact is not a verified-backup manifest.
    try{await link(temporary,output);}catch(error){if((error as NodeJS.ErrnoException).code==='EEXIST')fail('archive_output_exists');fail('archive_publish_failed');}
    published=true;
    await unlink(temporary);
    const dir=await open(directory,constants.O_RDONLY|(constants.O_DIRECTORY??0)|(constants.O_NOFOLLOW??0));
    try{await dir.sync();}finally{await dir.close();}
    if(failure)throw failure;
    return {status:'captured_unverified' as const,bytes,sha256:hash.digest('hex')};
  }catch(error){
    stop(published?'archive_publish_uncertain':error instanceof BackupArchiveError?error.code:'archive_io_failed');
    if(published)failure=new BackupArchiveError('archive_publish_uncertain');
    if(finished)await finished;
    throw failure;
  }finally{
    if(timer)clearTimeout(timer);options.signal?.removeEventListener('abort',abort);
    try{await handle.close();}catch{throw new BackupArchiveError('archive_cleanup_failed');}
    await unlink(temporary).catch(error=>{if(error.code!=='ENOENT')throw new BackupArchiveError('archive_cleanup_failed');});
  }
}
