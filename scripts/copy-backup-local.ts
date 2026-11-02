import {resolve} from 'node:path';
import {copyBackupCandidateToLocal,BackupLocalError} from '../packages/server/src/backup-local';
try{
  const [sourceDirectory,destinationRoot,id,schema,...extra]=process.argv.slice(2),max=process.env.CCPANEL_BACKUP_MAX_BYTES??'268435456';
  if(!sourceDirectory||!destinationRoot||!id||!schema||extra.length||!/^\d+$/.test(schema)||!/^\d+$/.test(max))throw new BackupLocalError('usage_backup_copy_local_source_root_id_schema');
  const plaintextLimit=Number(max);
  if(!Number.isSafeInteger(plaintextLimit)||plaintextLimit<1||plaintextLimit>Number.MAX_SAFE_INTEGER-36)throw new BackupLocalError('local_copy_options_invalid');
  console.log(JSON.stringify(await copyBackupCandidateToLocal({sourceDirectory:resolve(sourceDirectory),destinationRoot:resolve(destinationRoot),id,expectedSchema:Number(schema),maxBytes:plaintextLimit+36,timeoutMs:120000})));
  process.exitCode=2;
}catch(error){console.error(JSON.stringify({code:error instanceof BackupLocalError?error.code:'local_copy_failed'}));process.exitCode=1;}
