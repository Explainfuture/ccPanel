import {open,rename,unlink} from 'node:fs/promises';
import {dirname} from 'node:path';
import {randomUUID} from 'node:crypto';
// File fsync precedes publication. Windows does not expose a portable directory
// fsync: this protects process-crash evidence, not arbitrary power/storage loss.
export async function publishEvidence(path,value,{exclusive=false}={}){
  const data=JSON.stringify(value),temporary=exclusive?path:path+'.'+randomUUID()+'.tmp';
  let file;
  try{
    file=await open(temporary,'wx',0o600);await file.writeFile(data);await file.sync();await file.close();file=undefined;
    if(!exclusive)await rename(temporary,path);
    if(process.platform!=='win32'){const directory=await open(dirname(path),'r');try{await directory.sync();}finally{await directory.close();}}
  }catch(error){
    await file?.close().catch(()=>{});
    if(!exclusive)await unlink(temporary).catch(()=>{});
    throw error;
  }
}
