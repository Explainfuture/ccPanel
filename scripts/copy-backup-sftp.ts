import {constants} from 'node:fs';
import {open,lstat} from 'node:fs/promises';
import {resolve,posix} from 'node:path';
import {pathToFileURL} from 'node:url';
import {parseBackupConfig,BackupConfigError} from '../packages/server/src/backup-config';
import {transferBackupCandidateSftp,SftpBackupError,type SftpBackupOptions} from '../packages/server/src/backup-sftp';

// Explicit candidate transfer only, not a source capture/restore/scheduler run.
export async function main(args:string[],transfer:(options:SftpBackupOptions)=>Promise<unknown>=transferBackupCandidateSftp){
  try{
    const [configPath,source,download,id,...extra]=args;if(!configPath||!source||!download||!id||extra.length)throw new SftpBackupError('usage_backup_copy_sftp_config_source_download_id');
    const file=await open(configPath,constants.O_RDONLY|(constants.O_NOFOLLOW??0)|(constants.O_NONBLOCK??0));
    let config;
    try{
      const before=await file.stat();if(!before.isFile()||before.size<1||before.size>65536)throw new SftpBackupError('sftp_config_invalid');
      const bytes=Buffer.alloc(65537);let length=0;
      while(length<bytes.length){const {bytesRead}=await file.read(bytes,length,bytes.length-length,null);if(!bytesRead)break;length+=bytesRead;}
      // Windows lstat dev can be zero while fstat dev is populated. Enabled
      // execution still refuses Windows until ACL validation is implemented.
      const after=await file.stat(),current=await lstat(configPath);if(length!==before.size||after.size!==before.size||after.mtimeMs!==before.mtimeMs||after.ctimeMs!==before.ctimeMs||(process.platform!=='win32'&&current.dev!==before.dev)||current.ino!==before.ino||current.isSymbolicLink())throw new SftpBackupError('sftp_config_changed');
      config=parseBackupConfig(JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(bytes.subarray(0,length))));
      if(config.enabled&&(process.platform==='win32'||before.uid!==process.geteuid?.()||(before.mode&0o077)!==0))throw new SftpBackupError('sftp_config_permissions_unverified');
    }finally{await file.close();}
    if(!config.enabled){console.log(JSON.stringify({status:'disabled',transferEnabled:false,credentialsInspected:0}));return 0;}
    if(config.destination.kind!=='sftp'||!config.destination.sftp)throw new SftpBackupError('sftp_destination_required');
    const destination=config.destination.sftp,max=config.limits.exportMaxBytes,timeout=config.limits.runTimeoutSeconds*1000;
    if(max>Number.MAX_SAFE_INTEGER-36||!Number.isSafeInteger(timeout)||timeout>1800000)throw new SftpBackupError('sftp_options_invalid');
    const result=await transfer({sourceDirectory:resolve(source),downloadRoot:resolve(download),id,remoteRoot:posix.join(destination.remoteRoot!,config.destination.prefix),expectedSchema:config.source.expectedSchema,maxBytes:max+36,timeoutMs:timeout,connection:{host:destination.host!,port:destination.port,user:destination.user!,knownHostsFile:destination.knownHostsFile!,identityFile:destination.identityFile!}});
    console.log(JSON.stringify(result));return 2;
  }catch(error){console.error(JSON.stringify({code:error instanceof SftpBackupError||error instanceof BackupConfigError?error.code:'sftp_transfer_failed'}));return 1;}
}
if(process.argv[1]&&import.meta.url===pathToFileURL(resolve(process.argv[1])).href)process.exitCode=await main(process.argv.slice(2));
