import {constants} from 'node:fs';
import {open,lstat} from 'node:fs/promises';
import {resolve} from 'node:path';
import {pathToFileURL} from 'node:url';
import {parseBackupConfig} from '../packages/server/src/backup-config';
import {publishRecoveryStateLocal,RecoveryStatePublishError} from '../packages/server/src/recovery-state-publish';

// Explicit publication only. No source connection, key generation or scheduler.
export async function main(args:string[]){
  try{
    const [configPath,stateFile,candidateDirectory,expected,...extra]=args;
    if(!configPath||!stateFile||!candidateDirectory||!expected||extra.length)throw new RecoveryStatePublishError('usage_backup_publish_state_local_config_state_candidate_expected_head_or_genesis');
    const file=await open(configPath,constants.O_RDONLY|(constants.O_NOFOLLOW??0)|(constants.O_NONBLOCK??0));
    let config;
    try{
      const before=await file.stat();if(!before.isFile()||before.size<1||before.size>65536)throw new RecoveryStatePublishError('state_publish_config_invalid');
      const bytes=Buffer.alloc(before.size+1);let total=0;
      try{
        while(total<bytes.length){const {bytesRead}=await file.read(bytes,total,bytes.length-total,null);if(!bytesRead)break;total+=bytesRead;}
        const after=await file.stat(),current=await lstat(configPath);
        if(total!==before.size||after.size!==before.size||after.mtimeMs!==before.mtimeMs||after.ctimeMs!==before.ctimeMs||
          (process.platform!=='win32'&&current.dev!==before.dev)||current.ino!==before.ino||current.isSymbolicLink())throw new RecoveryStatePublishError('state_publish_config_changed');
        config=parseBackupConfig(JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(bytes.subarray(0,total))));
        if(config.enabled&&config.version===2&&config.state?.enabled&&(process.platform==='win32'||before.uid!==process.geteuid?.()||(before.mode&0o077)!==0))throw new RecoveryStatePublishError('state_publish_config_permissions_unverified');
      }finally{bytes.fill(0);}
    }finally{await file.close();}
    const result=await publishRecoveryStateLocal({config,stateFile:resolve(stateFile),candidateDirectory:resolve(candidateDirectory),expectedHeadFile:expected==='--genesis'?null:resolve(expected)});
    console.log(JSON.stringify(result));return result.status==='disabled'?0:2;
  }catch(error){
    const code=(error as {code?:unknown})?.code;
    console.error(JSON.stringify({code:typeof code==='string'&&/^(?:state_|journal_|backup_candidate_|invalid_backup_config|source_destination_|encryption_key_)[a-z_]*$/.test(code)?code:'state_publish_failed'}));return 1;
  }
}
if(process.argv[1]&&import.meta.url===pathToFileURL(resolve(process.argv[1])).href)process.exitCode=await main(process.argv.slice(2));
