import {constants} from 'node:fs';
import {open,lstat,realpath,statfs,link,unlink,type FileHandle} from 'node:fs/promises';
import {resolve,isAbsolute,dirname,basename,join,sep} from 'node:path';
import {createDecipheriv,createHash,randomBytes} from 'node:crypto';
import {inspectBackupCandidate,parseBackupCandidate,BackupCandidateError} from './backup-candidate';

export class BackupDecryptError extends Error{constructor(public code:string){super(code);this.name='BackupDecryptError';}}
export interface DecryptCandidateOptions{directory:string;keyFile:string;output:string;expectedSchema:number;maxBytes:number;timeoutMs:number;signal?:AbortSignal}
// Authenticated plaintext staging only. Actual PG restore, trustworthy source
// identity/latest-state import and recovery-gate validation are separate steps.
export async function decryptBackupCandidate(options:DecryptCandidateOptions){
  const fail=(code:string):never=>{throw new BackupDecryptError(code);};
  if(![options.directory,options.keyFile,options.output].every(p=>typeof p==='string'&&isAbsolute(p))||![options.expectedSchema,options.maxBytes,options.timeoutMs].every(n=>Number.isSafeInteger(n)&&n>0)||options.timeoutMs>1800000)fail('decrypt_options_invalid');
  const deadline=performance.now()+options.timeoutMs;
  const check=()=>{if(options.signal?.aborted)fail('decrypt_aborted');if(performance.now()>=deadline)fail('decrypt_timeout');};check();
  if(process.platform==='win32')fail('decrypt_permissions_unverified');
  const directory=resolve(options.directory),output=resolve(options.output),parent=dirname(output);
  if(!/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(basename(output))||output.startsWith(directory+sep)||output===directory)fail('decrypt_options_invalid');
  let keyHandle:FileHandle|undefined,source:FileHandle|undefined,destination:FileHandle|undefined,key:Buffer|undefined,temporary:string|undefined,ownedTemporary=false,published=false;
  try{
    const initialParent=await lstat(parent);
    const checkParent=async()=>{const current=await lstat(parent);if(!current.isDirectory()||current.isSymbolicLink()||current.dev!==initialParent.dev||current.ino!==initialParent.ino||current.uid!==process.geteuid?.()||(current.mode&0o077)!==0||await realpath(parent)!==parent)fail('decrypt_parent_unsafe');check();};
    await checkParent();
    try{await lstat(output);fail('decrypt_output_exists');}catch(error){if(error instanceof BackupDecryptError)throw error;if((error as NodeJS.ErrnoException).code!=='ENOENT')throw error;}
    const inspect=()=>{check();return inspectBackupCandidate({directory,expectedSchema:options.expectedSchema,maxBytes:options.maxBytes,timeoutMs:Math.max(1,Math.ceil(deadline-performance.now())),signal:options.signal});};
    const initial=await inspect();
    const flags=constants.O_RDONLY|(constants.O_NOFOLLOW??0)|(constants.O_NONBLOCK??0);
    // Bounded manifest read; its exact bytes must match the inspected receipt.
    const metadata=await open(join(directory,'capture.json'),flags);
    let archive:Record<string,unknown>;
    try{
      const info=await metadata.stat();if(!info.isFile()||info.uid!==process.geteuid?.()||(info.mode&0o077)!==0||info.size<1||info.size>1048576)fail('decrypt_source_changed');
      const bytes=Buffer.alloc(1048577);let length=0;
      while(length<bytes.length){check();const result=await metadata.read(bytes,length,bytes.length-length,null);if(!result.bytesRead)break;length+=result.bytesRead;}
      if(length!==info.size||createHash('sha256').update(bytes.subarray(0,length)).digest('hex')!==initial.manifestSha256)fail('decrypt_source_changed');
      archive=parseBackupCandidate(JSON.parse(bytes.subarray(0,length).toString('utf8'))).archive as Record<string,unknown>;
    }finally{await metadata.close();}
    const plaintextBytes=archive!.plaintextBytes as number,plaintextSha256=archive!.plaintextSha256 as string;
    const space=await statfs(parent,{bigint:true});if(space.bavail*space.bsize<BigInt(plaintextBytes)+1048576n)fail('decrypt_space_low');
    try{
      keyHandle=await open(options.keyFile,flags);const before=await keyHandle.stat();
      if(!before.isFile()||before.uid!==process.geteuid?.()||(before.mode&0o077)!==0||before.size!==32)fail('decrypt_key_unsafe');
      const bytes=Buffer.alloc(33);
      try{if((await keyHandle.read(bytes,0,33,0)).bytesRead!==32)fail('decrypt_key_unsafe');key=Buffer.from(bytes.subarray(0,32));}finally{bytes.fill(0);}
      const after=await keyHandle.stat();if(before.size!==after.size||before.mtimeMs!==after.mtimeMs||before.ctimeMs!==after.ctimeMs)fail('decrypt_key_unsafe');
    }catch{fail('decrypt_key_unsafe');}
    source=await open(join(directory,'database.dump.enc'),flags);const before=await source.stat();
    if(!before.isFile()||before.uid!==process.geteuid?.()||(before.mode&0o077)!==0||before.size!==initial.bytes)fail('decrypt_source_changed');
    const readExact=async(buffer:Buffer,position:number)=>{let offset=0;while(offset<buffer.length){check();const result=await source!.read(buffer,offset,buffer.length-offset,position+offset);if(!result.bytesRead)fail('decrypt_source_changed');offset+=result.bytesRead;}};
    const header=Buffer.alloc(20),tag=Buffer.alloc(16);await readExact(header,0);await readExact(tag,before.size-16);
    if(!header.subarray(0,8).equals(Buffer.from('CCPBK001')))fail('decrypt_source_changed');
    const decipher=createDecipheriv('aes-256-gcm',key!,header.subarray(8));decipher.setAAD(header.subarray(0,8));decipher.setAuthTag(tag);
    await checkParent();check();temporary=join(parent,'.'+basename(output)+'.'+randomBytes(12).toString('hex')+'.tmp');
    destination=await open(temporary,constants.O_WRONLY|constants.O_CREAT|constants.O_EXCL|(constants.O_NOFOLLOW??0),0o600);
    ownedTemporary=true;
    const cipherHash=createHash('sha256').update(header),plainHash=createHash('sha256');let bytes=0;
    const write=async(chunk:Buffer)=>{
      bytes+=chunk.length;if(bytes>plaintextBytes)fail('decrypt_plaintext_mismatch');plainHash.update(chunk);
      let offset=0;while(offset<chunk.length){check();const result=await destination!.write(chunk,offset,chunk.length-offset,null);if(!result.bytesWritten)fail('decrypt_write_failed');offset+=result.bytesWritten;}
    };
    const buffer=Buffer.alloc(65536);let position=20;
    while(position<before.size-16){check();const length=Math.min(buffer.length,before.size-16-position);await readExact(buffer.subarray(0,length),position);const chunk=buffer.subarray(0,length);position+=length;cipherHash.update(chunk);await write(decipher.update(chunk));}
    cipherHash.update(tag);if(cipherHash.digest('hex')!==initial.sha256)fail('decrypt_source_changed');
    let tail:Buffer;try{tail=decipher.final();}catch{fail('decrypt_authentication_failed');}
    await write(tail!);if(bytes!==plaintextBytes||plainHash.digest('hex')!==plaintextSha256)fail('decrypt_plaintext_mismatch');
    const after=await source.stat();if(after.size!==before.size||after.mtimeMs!==before.mtimeMs||after.ctimeMs!==before.ctimeMs)fail('decrypt_source_changed');
    const final=await inspect();if(final.manifestSha256!==initial.manifestSha256||final.sha256!==initial.sha256||final.bytes!==initial.bytes)fail('decrypt_source_changed');
    await destination.sync();const written=await destination.stat();await destination.close();destination=undefined;
    // Verify the actual staged plaintext bytes before exposing any output name.
    const readback=await open(temporary,flags);
    try{
      const before=await readback.stat();if(!before.isFile()||before.dev!==written.dev||before.ino!==written.ino||before.size!==bytes||before.uid!==process.geteuid?.()||(before.mode&0o077)!==0)fail('decrypt_plaintext_mismatch');
      const hash=createHash('sha256'),chunk=Buffer.alloc(65536);let total=0;
      while(true){check();const result=await readback.read(chunk,0,Math.min(chunk.length,plaintextBytes-total+1),null);if(!result.bytesRead)break;total+=result.bytesRead;if(total>plaintextBytes)fail('decrypt_plaintext_mismatch');hash.update(chunk.subarray(0,result.bytesRead));}
      const after=await readback.stat(),current=await lstat(temporary);
      if(total!==plaintextBytes||hash.digest('hex')!==plaintextSha256||after.size!==before.size||after.mtimeMs!==before.mtimeMs||after.ctimeMs!==before.ctimeMs||current.dev!==before.dev||current.ino!==before.ino||current.isSymbolicLink())fail('decrypt_plaintext_mismatch');
    }finally{await readback.close();}
    await checkParent();check();
    try{await link(temporary,output);}catch(error){if((error as NodeJS.ErrnoException).code==='EEXIST')fail('decrypt_output_exists');throw error;}published=true;
    const syncParent=async()=>{const handle=await open(parent,constants.O_RDONLY|(constants.O_DIRECTORY??0)|(constants.O_NOFOLLOW??0));try{await handle.sync();}finally{await handle.close();}};
    await syncParent();await unlink(temporary);temporary=undefined;ownedTemporary=false;await syncParent();await checkParent();check();
    return{status:'decrypted_unverified' as const,authenticatedDecryption:true,plaintextHashChecked:true,plaintextReadbackChecked:true,bytes,sha256:plaintextSha256,manifestSha256:initial.manifestSha256,recoveryVerified:false,offhostVerified:false,latestStateVerified:false,sourceAuthenticated:false};
  }catch(error){
    if(published)throw new BackupDecryptError('decrypt_publish_uncertain');
    if(error instanceof BackupDecryptError)throw error;
    if(error instanceof BackupCandidateError)throw new BackupDecryptError(error.code);
    throw new BackupDecryptError('decrypt_failed');
  }finally{
    key?.fill(0);
    const closed=await Promise.allSettled([destination?.close(),source?.close(),keyHandle?.close()]);
    let cleanupFailed=closed.some(result=>result.status==='rejected');
    if(temporary&&ownedTemporary)try{await unlink(temporary);}catch(error){if((error as NodeJS.ErrnoException).code!=='ENOENT')cleanupFailed=true;}
    if(cleanupFailed)throw new BackupDecryptError(published?'decrypt_publish_uncertain':'decrypt_cleanup_failed');
  }
}
