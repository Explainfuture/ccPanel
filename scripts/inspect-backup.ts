import {resolve} from 'node:path';
import {inspectBackupCandidate,BackupCandidateError} from '../packages/server/src/backup-candidate';
try{
  const [directory,schema,...extra]=process.argv.slice(2),max=process.env.CCPANEL_BACKUP_MAX_BYTES??'268435492';
  if(!directory||!schema||extra.length||!/^\d+$/.test(schema)||!/^\d+$/.test(max))throw new BackupCandidateError('usage_backup_inspect_directory_schema');
  console.log(JSON.stringify(await inspectBackupCandidate({directory:resolve(directory),expectedSchema:Number(schema),maxBytes:Number(max),timeoutMs:120000})));
  process.exitCode=2;
}catch(error){console.error(JSON.stringify({code:error instanceof BackupCandidateError?error.code:'backup_candidate_inspection_failed'}));process.exitCode=1;}
