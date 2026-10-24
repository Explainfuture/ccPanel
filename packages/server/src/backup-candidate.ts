import {constants} from 'node:fs';
import {open,lstat,realpath,readdir} from 'node:fs/promises';
import {isAbsolute,resolve,join} from 'node:path';
import {createHash} from 'node:crypto';

export class BackupCandidateError extends Error{constructor(public code:string){super(code);this.name='BackupCandidateError';}}
const fail=(code='backup_candidate_manifest_invalid'):never=>{throw new BackupCandidateError(code);};
function shape(value:unknown,keys:string[]):asserts value is Record<string,unknown>{
  if(!value||typeof value!=='object'||Array.isArray(value)||Object.keys(value).length!==keys.length||keys.some(key=>!Object.hasOwn(value,key)))fail();
}
const integer=(value:unknown):value is number=>typeof value==='number'&&Number.isSafeInteger(value)&&value>0;
const digest=(value:unknown):value is string=>typeof value==='string'&&/^[a-f0-9]{64}$/.test(value);
const timestamp=(value:unknown):value is string=>typeof value==='string'&&/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z$/.test(value)&&Number.isFinite(Date.parse(value))&&new Date(value).toISOString()===value;

// Parse untrusted captured metadata without converting PostgreSQL bigint hashes
// to JavaScript numbers. This proves structure, never source authenticity.
export function parseBackupCandidate(value:unknown){
  shape(value,['version','status','startedAt','capturedAt','recoveryVerified','offhostVerified','latestStateVerified','source','fingerprints','archive']);
  if(value.version!==1||value.status!=='captured_unverified'||value.recoveryVerified!==false||value.offhostVerified!==false||value.latestStateVerified!==false||!timestamp(value.startedAt)||!timestamp(value.capturedAt)||value.startedAt>value.capturedAt)fail();
  shape(value.source,['database','schemaVersion','releaseVerified']);
  if(typeof value.source.database!=='string'||!value.source.database.length||Buffer.byteLength(value.source.database)>63||/[\x00-\x1f\x7f]/.test(value.source.database)||!integer(value.source.schemaVersion)||value.source.releaseVerified!==false)fail();
  if(!Array.isArray(value.fingerprints)||value.fingerprints.length<1||value.fingerprints.length>1024)fail();
  const relations=new Set<string>();
  for(const fingerprint of value.fingerprints as unknown[]){
    if(typeof fingerprint!=='string'||fingerprint.length>256)fail();
    const match=/^\["(public\.[a-z_][a-z0-9_]*)",\s*(0|[1-9][0-9]*),\s*(-?(?:0|[1-9][0-9]*))\]$/.exec(fingerprint as string);
    if(!match||relations.has(match[1]!)||['public.recovery_gate','public.recovery_import_scopes'].includes(match[1]!))fail();
    if(BigInt(match![2]!)>9223372036854775807n||BigInt(match![3]!)< -9223372036854775808n||BigInt(match![3]!)>9223372036854775807n)fail();
    relations.add(match![1]!);
  }
  shape(value.archive,['file','format','bytes','sha256','plaintextBytes','plaintextSha256']);
  const archive=value.archive;
  if(archive.file!=='database.dump.enc'||archive.format!=='CCPBK001'||!integer(archive.bytes)||!integer(archive.plaintextBytes)||archive.bytes-archive.plaintextBytes!==36||!digest(archive.sha256)||!digest(archive.plaintextSha256))fail();
  return value;
}

export async function inspectBackupCandidate(options:{directory:string;expectedSchema:number;maxBytes:number;timeoutMs:number;signal?:AbortSignal}){
  if(!isAbsolute(options.directory)||!integer(options.expectedSchema)||!integer(options.maxBytes)||!integer(options.timeoutMs)||options.timeoutMs>1800000)fail('backup_candidate_options_invalid');
  const checkpoint=()=>{if(options.signal?.aborted)fail('backup_candidate_aborted');if(performance.now()>=deadline)fail('backup_candidate_timeout');};
  const deadline=performance.now()+options.timeoutMs;checkpoint();
  if(process.platform==='win32')fail('backup_candidate_permissions_unverified');
  const directory=resolve(options.directory);
  try{
    const initial=await lstat(directory);
    if(!initial.isDirectory()||initial.isSymbolicLink()||initial.uid!==process.geteuid?.()||(initial.mode&0o077)!==0||await realpath(directory)!==directory)fail('backup_candidate_directory_unsafe');
    if(JSON.stringify((await readdir(directory)).sort())!==JSON.stringify(['capture.json','database.dump.enc']))fail('backup_candidate_files_invalid');
    const read=async(name:string,max:number,consume:(chunk:Buffer)=>void)=>{
      const file=await open(join(directory,name),constants.O_RDONLY|(constants.O_NOFOLLOW??0)|(constants.O_NONBLOCK??0));
      try{
        const before=await file.stat();
        if(!before.isFile()||before.uid!==process.geteuid?.()||(before.mode&0o077)!==0)fail('backup_candidate_file_unsafe');
        if(before.size<1||before.size>max)fail('backup_candidate_size_invalid');
        let total=0;const buffer=Buffer.alloc(65536);
        while(true){checkpoint();const {bytesRead}=await file.read(buffer,0,Math.min(buffer.length,max-total+1),null);if(!bytesRead)break;total+=bytesRead;if(total>max)fail('backup_candidate_size_invalid');consume(buffer.subarray(0,bytesRead));}
        const after=await file.stat(),current=await lstat(join(directory,name));
        if(total!==before.size||after.size!==before.size||after.mtimeMs!==before.mtimeMs||after.ctimeMs!==before.ctimeMs||current.dev!==before.dev||current.ino!==before.ino||current.isSymbolicLink())fail('backup_candidate_changed');
        return total;
      }finally{await file.close();}
    };
    const chunks:Buffer[]=[];await read('capture.json',1048576,chunk=>chunks.push(Buffer.from(chunk)));
    let manifest:ReturnType<typeof parseBackupCandidate>;
    try{manifest=parseBackupCandidate(JSON.parse(Buffer.concat(chunks).toString('utf8')));}catch(error){if(error instanceof BackupCandidateError)throw error;fail();}
    if(manifest!.source&&((manifest!.source as Record<string,unknown>).schemaVersion!==options.expectedSchema))fail('backup_candidate_schema_mismatch');
    const archive=manifest!.archive as Record<string,unknown>;
    if((archive.bytes as number)>options.maxBytes)fail('backup_candidate_size_invalid');
    const hash=createHash('sha256');let header=Buffer.alloc(0);
    const bytes=await read('database.dump.enc',archive.bytes as number,chunk=>{hash.update(chunk);if(header.length<8)header=Buffer.concat([header,chunk.subarray(0,8-header.length)]);});
    if(bytes!==archive.bytes||hash.digest('hex')!==archive.sha256||!header.equals(Buffer.from('CCPBK001')))fail('backup_candidate_cipher_mismatch');
    const repeated:Buffer[]=[];await read('capture.json',1048576,chunk=>repeated.push(Buffer.from(chunk)));
    if(!Buffer.concat(chunks).equals(Buffer.concat(repeated)))fail('backup_candidate_changed');
    const final=await lstat(directory);checkpoint();
    if(final.dev!==initial.dev||final.ino!==initial.ino||await realpath(directory)!==directory||JSON.stringify((await readdir(directory)).sort())!==JSON.stringify(['capture.json','database.dump.enc']))fail('backup_candidate_changed');
    return{status:'captured_unverified' as const,cipherIntegrityChecked:true,bytes,sha256:archive.sha256 as string,relations:(manifest!.fingerprints as string[]).length,recoveryVerified:false,offhostVerified:false,latestStateVerified:false,sourceAuthenticated:false};
  }catch(error){if(error instanceof BackupCandidateError)throw error;throw new BackupCandidateError('backup_candidate_inspection_failed');}
}
