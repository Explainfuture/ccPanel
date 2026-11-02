// Explicit manual local command. Does not enable the off-host config/scheduler.
import {resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {captureSnapshotCandidate,BackupCaptureError} from '../packages/server/src/backup-capture';
try{
  const [output,keyFile,schema,...extra]=process.argv.slice(2);
  if(!output||!keyFile||!schema||extra.length||!/^\d+$/.test(schema)||!process.env.CCPANEL_BACKUP_DATABASE_URL)throw new BackupCaptureError('usage_backup_capture_output_key_schema');
  const source=new URL(process.env.CCPANEL_BACKUP_DATABASE_URL);
  if(!['postgres:','postgresql:'].includes(source.protocol)||!source.hostname||!source.username||!source.pathname.slice(1)||[...source.searchParams.keys()].some(key=>key!=='sslmode'))throw new BackupCaptureError('backup_capture_connection_invalid');
  const ssl=source.searchParams.get('sslmode');if(ssl&&!['disable','allow','prefer','require','verify-ca','verify-full'].includes(ssl))throw new BackupCaptureError('backup_capture_connection_invalid');
  const max=process.env.CCPANEL_BACKUP_MAX_BYTES??'268435456',timeout=process.env.CCPANEL_BACKUP_TIMEOUT_MS??'120000';
  if(!/^\d+$/.test(max)||!/^\d+$/.test(timeout))throw new BackupCaptureError('backup_capture_options_invalid');
  const container=process.env.CCPANEL_BACKUP_PG_CONTAINER;if(container&&!/^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$/.test(container))throw new BackupCaptureError('backup_capture_container_invalid');
  const result=await captureSnapshotCandidate({connectionString:source.href,output:resolve(output),keyFile:resolve(keyFile),cryptoScript:fileURLToPath(new URL('./encrypt-backup.mjs',import.meta.url)),expectedSchema:Number(schema),maxBytes:Number(max),timeoutMs:Number(timeout)},snapshotId=>{
    const args=['--format=custom','--no-owner','--no-acl','--exclude-table-data=public.recovery_gate','--exclude-table-data=public.recovery_import_scopes','--snapshot='+snapshotId];
    const env={...process.env,PGHOST:source.hostname.replace(/^\[|\]$/g,''),PGPORT:source.port||'5432',PGDATABASE:decodeURIComponent(source.pathname.slice(1)),PGUSER:decodeURIComponent(source.username),PGPASSWORD:decodeURIComponent(source.password),...(ssl?{PGSSLMODE:ssl}:{})};
    return container?{command:'docker',args:['exec','-i',container,'pg_dump','--username='+env.PGUSER,'--dbname='+env.PGDATABASE,...args],env}:{command:'pg_dump',args,env};
  });
  console.log(JSON.stringify(result));process.exitCode=2;
}catch(error){console.error(JSON.stringify({code:error instanceof BackupCaptureError?error.code:'backup_capture_failed'}));process.exitCode=1;}
