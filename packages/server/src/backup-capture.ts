import {constants} from 'node:fs';
import {open,lstat,realpath,statfs,mkdir,link,unlink} from 'node:fs/promises';
import {isAbsolute,resolve,dirname,basename,join} from 'node:path';
import {withBackupSnapshot,BackupSnapshotError} from './backup-snapshot';
import {captureBackupArchive,BackupArchiveError,type ArchiveOptions} from './backup-archive';
export class BackupCaptureError extends Error{constructor(public code:string){super(code);this.name='BackupCaptureError';}}
export interface CaptureOptions{connectionString:string;output:string;keyFile:string;cryptoScript:string;expectedSchema:number;maxBytes:number;timeoutMs:number}
type DumpCommand=Pick<ArchiveOptions,'command'|'args'|'env'>;
// Source-side/local capture core. buildDump is a trusted process adapter, not
// configuration code. No scheduler, transport or verified-backup publication.
export async function captureSnapshotCandidate(options:CaptureOptions,buildDump:(snapshotId:string)=>DumpCommand){
  const fail=(code:string):never=>{throw new BackupCaptureError(code);};
  if(!Number.isSafeInteger(options.maxBytes)||options.maxBytes<1||options.maxBytes>Number.MAX_SAFE_INTEGER-36||!Number.isSafeInteger(options.expectedSchema)||options.expectedSchema<1||!Number.isSafeInteger(options.timeoutMs)||options.timeoutMs<1000||options.timeoutMs>1800000||![options.output,options.keyFile,options.cryptoScript].every(v=>typeof v==='string'&&isAbsolute(v)))fail('backup_capture_options_invalid');
  if(process.platform==='win32')fail('backup_capture_permissions_unverified');
  const output=resolve(options.output),parent=dirname(output);
  if(!/^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$/.test(basename(output)))fail('backup_capture_name_invalid');
  try{
    const directory=await lstat(parent);
    if(!directory.isDirectory()||directory.isSymbolicLink()||directory.uid!==process.geteuid?.()||(directory.mode&0o077)!==0||await realpath(parent)!==parent)fail('backup_capture_parent_unsafe');
    const key=await open(options.keyFile,constants.O_RDONLY|(constants.O_NOFOLLOW??0)|(constants.O_NONBLOCK??0));
    try{const metadata=await key.stat();if(!metadata.isFile()||metadata.size!==32||(metadata.mode&0o077)!==0||metadata.uid!==process.geteuid?.())fail('backup_capture_key_unsafe');}finally{await key.close();}
    const space=await statfs(parent,{bigint:true});
    if(space.bavail*space.bsize<BigInt(options.maxBytes)*2n+1048612n)fail('backup_capture_space_low');
    // mkdir is exclusive; retries never reuse or overwrite an existing set.
    try{await mkdir(output,{mode:0o700});}catch(error){if((error as NodeJS.ErrnoException).code==='EEXIST')fail('backup_capture_output_exists');throw error;}
    const parentHandle=await open(parent,constants.O_RDONLY|(constants.O_DIRECTORY??0)|(constants.O_NOFOLLOW??0));try{await parentHandle.sync();}finally{await parentHandle.close();}
    const startedAt=new Date().toISOString(),plain=join(output,'database.dump'),cipher=join(output,'database.dump.enc');
    const candidate=await withBackupSnapshot(options.connectionString,{expectedSchema:options.expectedSchema,timeoutMs:options.timeoutMs},async snapshot=>{
      const raw=await captureBackupArchive({...buildDump(snapshot.snapshotId),output:plain,maxBytes:options.maxBytes,timeoutMs:options.timeoutMs,signal:snapshot.signal});
      const encrypted=await captureBackupArchive({output:cipher,command:process.execPath,args:[options.cryptoScript,'encrypt-stream',plain,options.keyFile],maxBytes:options.maxBytes+36,timeoutMs:options.timeoutMs,signal:snapshot.signal});
      if(encrypted.bytes!==raw.bytes+36)fail('backup_capture_cipher_size_invalid');
      return {source:{database:snapshot.sourceDatabase,schemaVersion:snapshot.schemaVersion,releaseVerified:false},fingerprints:snapshot.fingerprints,archive:{file:'database.dump.enc',format:'CCPBK001',bytes:encrypted.bytes,sha256:encrypted.sha256,plaintextBytes:raw.bytes,plaintextSha256:raw.sha256}};
    });
    // A returned snapshot callback and a captured cipher are still not a restore
    // proof. This file is deliberately separate from any verified manifest.
    const manifest={version:1,status:'captured_unverified',startedAt,capturedAt:new Date().toISOString(),recoveryVerified:false,offhostVerified:false,latestStateVerified:false,...candidate};
    const bytes=Buffer.from(JSON.stringify(manifest)+'\n');if(bytes.length>1048576)fail('backup_capture_manifest_too_large');
    const temporary=join(output,'.capture.json.tmp'),destination=join(output,'capture.json');
    const file=await open(temporary,'wx',0o600);try{await file.writeFile(bytes);await file.sync();}finally{await file.close();}
    await link(temporary,destination);await unlink(temporary);
    const sync=async()=>{const directory=await open(output,constants.O_RDONLY|(constants.O_DIRECTORY??0)|(constants.O_NOFOLLOW??0));try{await directory.sync();}finally{await directory.close();}};
    await sync();await unlink(plain);await sync();
    return {status:'captured_unverified' as const,bytes:candidate.archive.bytes,sha256:candidate.archive.sha256,recoveryVerified:false,offhostVerified:false,latestStateVerified:false};
  }catch(error){
    if(error instanceof BackupCaptureError)throw error;
    if(error instanceof BackupSnapshotError||error instanceof BackupArchiveError)throw new BackupCaptureError(error.code);
    // Keep incomplete private sets for inspection. Never delete another set or
    // mistake a leftover cipher/capture.json for a verified backup.
    throw new BackupCaptureError('backup_capture_failed');
  }
}
