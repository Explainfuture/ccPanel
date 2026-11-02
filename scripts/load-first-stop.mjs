import {constants} from 'node:fs';
import {open,link,unlink,lstat,realpath} from 'node:fs/promises';
import {resolve,dirname,join} from 'node:path';
import {randomUUID} from 'node:crypto';

export class FirstStopError extends Error{
  constructor(code){super(code);this.name='FirstStopError';this.code=code;}
}
const same=(left,right)=>left.dev===right.dev&&left.ino===right.ino;
const canonical=value=>process.platform==='win32'?value.toLowerCase():value;

// The caller owns a private, stable parent directory. Canonical-parent checks
// reject static aliases; this is not immunity to hostile concurrent replacement
// of that directory. Never read/follow/unlink the final target. already_latched
// means EEXIST, not that the existing content/type is a valid stop request.
// Windows has no portable directory fsync here. No power-loss promise is made.
export async function publishFirstStop(path,reason){
  if(typeof path!=='string'||!path.length||path.includes('\0')||!['resource_safety','operator_requested'].includes(reason))throw new FirstStopError('first_stop_invalid');
  // Fix the entire payload before the first await or filesystem operation.
  const bytes=Buffer.from(JSON.stringify({reason})+'\n','utf8');
  const target=resolve(path),parent=dirname(target),temporary=join(parent,'.load-stop-'+randomUUID()+'.tmp');
  let directoryIdentity,tempIdentity,file,owned=false,attempted=false,published=false,result,failure;
  const parentSafe=async()=>{
    const info=await lstat(parent,{bigint:true});
    if(!info.isDirectory()||info.isSymbolicLink()||!same(info,directoryIdentity)||canonical(await realpath(parent))!==canonical(parent))throw new FirstStopError('first_stop_invalid');
  };
  const tempSafe=async()=>{
    const info=await lstat(temporary,{bigint:true});
    if(!info.isFile()||info.isSymbolicLink()||!same(info,tempIdentity)||info.size!==BigInt(bytes.length))throw new FirstStopError('first_stop_io_failed');
  };
  const syncParent=async()=>{
    if(process.platform==='win32')return;
    await parentSafe();
    const directory=await open(parent,constants.O_RDONLY|(constants.O_DIRECTORY??0)|(constants.O_NOFOLLOW??0));
    try{await directory.sync();}finally{await directory.close();}
  };
  try{
    directoryIdentity=await lstat(parent,{bigint:true});
    await parentSafe();
    file=await open(temporary,'wx',0o600);owned=true;
    const openedIdentity=await file.stat({bigint:true});
    const pathIdentity=await lstat(temporary,{bigint:true});
    // Native Windows lstat can report dev=0 while fstat reports the volume
    // serial. Compare their nonzero inode there, then use path dev+ino below.
    const openedMatches=same(openedIdentity,pathIdentity)||(process.platform==='win32'&&openedIdentity.ino>0n&&openedIdentity.ino===pathIdentity.ino);
    if(!openedIdentity.isFile()||!pathIdentity.isFile()||pathIdentity.isSymbolicLink()||!openedMatches)throw new FirstStopError('first_stop_io_failed');
    tempIdentity=pathIdentity;
    await file.writeFile(bytes);await file.sync();await file.close();file=undefined;
    await parentSafe();await tempSafe();
    attempted=true;
    try{
      await link(temporary,target);published=true;
      await syncParent();
      result={status:'published'};
    }catch(error){
      if(!published&&error?.code==='EEXIST')result={status:'already_latched'};
      else throw error;
    }
  }catch(error){
    failure=new FirstStopError(attempted?'first_stop_publish_uncertain':error instanceof FirstStopError?error.code:'first_stop_io_failed');
  }finally{
    let cleanupFailed=false,removed=false;
    if(file){try{await file.close();}catch{cleanupFailed=true;}}
    if(owned){
      try{
        await parentSafe();
        let current;
        try{current=await lstat(temporary,{bigint:true});}catch(error){if(error?.code==='ENOENT')removed=true;else throw error;}
        if(current){
          if(!current.isFile()||current.isSymbolicLink()||!tempIdentity||!same(current,tempIdentity))cleanupFailed=true;
          else{await unlink(temporary);removed=true;}
        }
      }catch{cleanupFailed=true;}
      if(removed)try{await syncParent();}catch{cleanupFailed=true;}
    }
    if(cleanupFailed)failure=new FirstStopError(published||(attempted&&!result)?'first_stop_publish_uncertain':'first_stop_cleanup_failed');
  }
  if(failure)throw failure;
  return result;
}
