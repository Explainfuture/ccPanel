// stdout is sensitive recovery metadata; redirect to a private file before
// encryption. No chain anchoring, source authentication or import is implied.
import {exportRecoverySourceState,RecoveryStateExportError} from '../packages/server/src/recovery-state-export';
try{
  const [sourceId,schema,...extra]=process.argv.slice(2),uri=process.env.CCPANEL_BACKUP_DATABASE_URL;
  if(!sourceId||!schema||extra.length||!/^\d+$/.test(schema)||!uri)throw new RecoveryStateExportError('usage_backup_export_state_source_schema');
  const result=await exportRecoverySourceState({connectionString:uri,sourceId,expectedSchema:Number(schema)});
  const payload=JSON.stringify(result)+'\n';if(Buffer.byteLength(payload)>16*1024*1024)throw new RecoveryStateExportError('state_export_budget_exceeded');
  process.stdout.on('error',()=>{});
  await new Promise<void>((done,reject)=>process.stdout.write(payload,error=>error?reject(error):done()));process.exitCode=2;
}catch(error){console.error(JSON.stringify({code:error instanceof RecoveryStateExportError?error.code:'state_export_failed'}));process.exitCode=1;}
