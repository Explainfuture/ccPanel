import {resolve} from 'node:path';
import {decryptBackupCandidate,BackupDecryptError} from '../packages/server/src/backup-decrypt';
try{
  const [directory,keyFile,output,schema,...extra]=process.argv.slice(2),max=process.env.CCPANEL_BACKUP_MAX_BYTES??'268435456';
  if(!directory||!keyFile||!output||!schema||extra.length||!/^\d+$/.test(schema)||!/^\d+$/.test(max))throw new BackupDecryptError('usage_backup_decrypt_candidate_directory_key_output_schema');
  const plaintextLimit=Number(max);if(!Number.isSafeInteger(plaintextLimit)||plaintextLimit<1||plaintextLimit>Number.MAX_SAFE_INTEGER-36)throw new BackupDecryptError('decrypt_options_invalid');
  console.log(JSON.stringify(await decryptBackupCandidate({directory:resolve(directory),keyFile:resolve(keyFile),output:resolve(output),expectedSchema:Number(schema),maxBytes:plaintextLimit+36,timeoutMs:120000})));
  process.exitCode=2;
}catch(error){console.error(JSON.stringify({code:error instanceof BackupDecryptError?error.code:'decrypt_failed'}));process.exitCode=1;}
