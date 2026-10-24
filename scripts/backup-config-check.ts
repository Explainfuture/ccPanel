// Read-only configuration preflight. No transfer or scheduler implementation.
import {constants} from 'node:fs';
import {open,lstat} from 'node:fs/promises';
import {resolve} from 'node:path';
import {pathToFileURL} from 'node:url';
import {BackupConfigError,parseBackupConfig,type BackupConfig} from '../packages/server/src/backup-config';

const fail=(field:string,code:string):never=>{throw new BackupConfigError(code,field);};
export async function inspectFile(path:string,field:string,max:number,key=false):Promise<void>{
  let file;
  try{
    const before=await lstat(path);
    if(!before.isFile()||before.isSymbolicLink())return fail(field,'credential_not_regular_file');
    file=await open(path,constants.O_RDONLY|(constants.O_NOFOLLOW??0)|(constants.O_NONBLOCK??0));
    const after=await file.stat();
    if(!after.isFile())return fail(field,'credential_not_regular_file');
    if(after.size<1||after.size>max||(key&&after.size!==32))return fail(field,'credential_size_invalid');
    // POSIX mode bits do not prove Windows ACLs. Never silently claim otherwise.
    if(process.platform==='win32')return fail(field,'credential_permissions_unverified');
    if(before.dev!==after.dev||before.ino!==after.ino)return fail(field,'credential_file_changed');
    if((after.mode&0o077)!==0)return fail(field,'credential_permissions_too_open');
  }catch(error){if(error instanceof BackupConfigError)throw error;return fail(field,'credential_unavailable');}
  finally{await file?.close();}
}
export async function preflightBackup(config:BackupConfig,inspect=inspectFile){
  if(!config.enabled)return {status:'disabled',transferEnabled:false,credentialsInspected:0} as const;
  const files:[string,string,number,boolean?][]=[
    [config.source.knownHostsFile!,'source.knownHostsFile',1048576],
    [config.source.identityFile!,'source.identityFile',65536],
    [config.encryption.keyFile!,'encryption.keyFile',32,true],
  ];
  if(config.destination.kind==='sftp'){
    files.push([config.destination.sftp!.knownHostsFile!,'destination.sftp.knownHostsFile',1048576],[config.destination.sftp!.identityFile!,'destination.sftp.identityFile',65536]);
  }
  // Metadata/size checks only: no credential bytes or paths are printed.
  for(const [path,field,max,key]of files)await inspect(path,field,max,key);
  return {status:'configured_executor_unavailable',transferEnabled:false,credentialsInspected:files.length,hostKeyVerified:false,independentFailureDomainVerified:false} as const;
}
export async function main(args:string[]){
  try{
    if(args.length!==1)fail('arguments','usage_backup_config_check_file');
    const configFile=await open(args[0],constants.O_RDONLY|(constants.O_NOFOLLOW??0)|(constants.O_NONBLOCK??0));
    let raw:Buffer;
    try{
      const stat=await configFile.stat();if(!stat.isFile()||stat.size<1||stat.size>65536)fail('config','config_file_invalid');
      raw=Buffer.alloc(65537);const read=await configFile.read(raw,0,raw.length,0);if(read.bytesRead!==stat.size)fail('config','config_file_changed');raw=raw.subarray(0,read.bytesRead);
    }finally{await configFile.close();}
    let value:unknown;try{value=JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(raw));}catch{fail('config','config_json_invalid');}
    const config=parseBackupConfig(value);
    if(config.enabled)await inspectFile(args[0],'config',65536);
    const result=await preflightBackup(config);console.log(JSON.stringify(result));return result.status==='disabled'?0:2;
  }catch(error){
    const safe=error instanceof BackupConfigError?{code:error.code,field:error.field}:{code:'config_unavailable',field:'config'};
    console.error(JSON.stringify(safe));return 1;
  }
}
if(process.argv[1]&&import.meta.url===pathToFileURL(resolve(process.argv[1])).href)process.exitCode=await main(process.argv.slice(2));
