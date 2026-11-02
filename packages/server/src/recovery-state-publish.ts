import {constants} from 'node:fs';
import {open,lstat,realpath} from 'node:fs/promises';
import {isAbsolute,resolve,join,basename,sep} from 'node:path';
import {createHash,createPrivateKey,createPublicKey} from 'node:crypto';
import {parseBackupConfig,BackupConfigError} from './backup-config';
import {inspectBackupCandidate,parseBackupCandidate,BackupCandidateError} from './backup-candidate';
import {parseRecoverySourceState,RecoveryStateFormatError} from './recovery-state-format';
import {parseRecoveryStateAnchor,signRecoveryStateRecord,RecoveryStateRecordError} from './recovery-state-record';
import {appendRecoveryStateJournal,verifyRecoveryStateJournal,RecoveryStateJournalError} from './recovery-state-journal';

export class RecoveryStatePublishError extends Error{
  constructor(public code:string){super(code);this.name='RecoveryStatePublishError';}
}
export interface PublishRecoveryStateOptions{
  config:unknown;stateFile:string;candidateDirectory:string;expectedHeadFile:string|null;
  signal?:AbortSignal;now?:number;
}
const fail=(code:string):never=>{throw new RecoveryStatePublishError(code);};
const inside=(path:string,root:string)=>path===root||path.startsWith(root+sep);
const sha=(bytes:Buffer)=>createHash('sha256').update(bytes).digest('hex');

// Publish an already exported state and already captured candidate. This does
// not connect to the source, authenticate its origin, or establish anchor custody.
export async function publishRecoveryStateLocal(options:PublishRecoveryStateOptions){
  const config=parseBackupConfig(options.config);
  if(!config.enabled||config.version!==2||!config.state?.enabled)return{status:'disabled' as const,statePublicationEnabled:false,credentialsInspected:0};
  const stateConfig=config.state,timeout=config.limits.runTimeoutSeconds*1000,max=config.limits.exportMaxBytes;
  if(!Number.isSafeInteger(timeout)||timeout<100||timeout>1800000||max>Number.MAX_SAFE_INTEGER-36||
    ![options.stateFile,options.candidateDirectory].every(path=>typeof path==='string'&&isAbsolute(path))||
    (options.expectedHeadFile!==null&&(typeof options.expectedHeadFile!=='string'||!isAbsolute(options.expectedHeadFile))))fail('state_publish_options_invalid');
  const deadline=performance.now()+timeout;
  const check=()=>{if(options.signal?.aborted)fail('state_publish_aborted');if(performance.now()>=deadline)fail('state_publish_timeout');};
  const remaining=()=>{check();const left=Math.floor(deadline-performance.now());if(left<100)fail('state_publish_timeout');return left;};
  check();if(process.platform==='win32')fail('state_publish_permissions_unverified');
  const candidate=resolve(options.candidateDirectory),journal=resolve(stateConfig.journalRoot!),anchors=resolve(stateConfig.anchorRoot!);
  const stateFile=resolve(options.stateFile),backupId=basename(candidate);
  if(!/^[A-Za-z0-9_-]{1,128}$/.test(backupId)||inside(stateFile,journal)||inside(stateFile,anchors)||
    inside(candidate,journal)||inside(journal,candidate)||inside(candidate,anchors)||inside(anchors,candidate))fail('state_publish_paths_unsafe');

  // Existing private canonical files only, bounded even if a regular file grows
  // while being read. Never print paths, file bytes or native crypto errors.
  const read=async(path:string,limit:number)=>{
    check();if(await realpath(path)!==path)fail('state_publish_file_unsafe');
    const file=await open(path,constants.O_RDONLY|(constants.O_NOFOLLOW??0)|(constants.O_NONBLOCK??0));
    let bytes:Buffer|undefined;
    try{
      const before=await file.stat();
      const safe=(stat:typeof before)=>stat.isFile()&&stat.uid===process.geteuid?.()&&(stat.mode&0o077)===0;
      if(!safe(before)||before.size<1||before.size>limit)fail('state_publish_file_unsafe');
      bytes=Buffer.alloc(before.size+1);let total=0;
      while(total<bytes.length){check();const {bytesRead}=await file.read(bytes,total,Math.min(65536,bytes.length-total),null);if(!bytesRead)break;total+=bytesRead;}
      const after=await file.stat(),current=await lstat(path);check();
      if(total!==before.size||!safe(after)||!safe(current)||current.isSymbolicLink()||current.dev!==before.dev||current.ino!==before.ino||
        after.size!==before.size||after.mtimeMs!==before.mtimeMs||after.ctimeMs!==before.ctimeMs||
        current.size!==before.size||current.mtimeMs!==before.mtimeMs||current.ctimeMs!==before.ctimeMs||await realpath(path)!==path)fail('state_publish_file_changed');
      return bytes.subarray(0,total);
    }catch(error){bytes?.fill(0);throw error;}finally{try{await file.close();}catch(error){bytes?.fill(0);throw error;}}
  };
  const parse=(bytes:Buffer)=>{try{return JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(bytes));}catch{fail('state_publish_json_invalid');}};
  const buffers:Buffer[]=[];let published=false;
  const ownedRead=async(path:string,limit:number)=>{const bytes=await read(path,limit);buffers.push(bytes);return bytes;};
  try{
    const state=parseRecoverySourceState(parse(await ownedRead(stateFile,16*1024*1024)),{now:options.now,maxAgeSeconds:config.limits.stateMaxAgeSeconds});
    if(state.source.id!==stateConfig.sourceId||state.source.schema_version!==config.source.expectedSchema)fail('state_publish_source_mismatch');
    const rawManifest=await ownedRead(join(candidate,'capture.json'),1048576),manifest=parseBackupCandidate(parse(rawManifest));
    const source=manifest.source as {database:string;schemaVersion:number};
    if(source.database!==state.source.database||source.schemaVersion!==state.source.schema_version)fail('state_publish_source_mismatch');
    if(state.exported_at<(manifest.capturedAt as string))fail('state_publish_state_predates_backup');
    const manifestHash=sha(rawManifest);
    const inspected=await inspectBackupCandidate({directory:candidate,expectedSchema:config.source.expectedSchema,maxBytes:max+36,timeoutMs:remaining(),signal:options.signal});
    if(inspected.manifestSha256!==manifestHash)fail('state_publish_candidate_changed');
    const previous=options.expectedHeadFile===null?null:parseRecoveryStateAnchor(parse(await ownedRead(resolve(options.expectedHeadFile),8192)));
    const privatePem=await ownedRead(stateConfig.signingKeyFile!,8192),publicPem=await ownedRead(stateConfig.publicKeyFile!,8192);
    let signer,pinnedPublic;
    try{
      signer=createPrivateKey({key:privatePem,format:'pem',type:'pkcs8'});pinnedPublic=createPublicKey({key:publicPem,format:'pem',type:'spki'});
      const canonicalPrivate=Buffer.from(signer.export({format:'pem',type:'pkcs8'})),canonicalPublic=Buffer.from(pinnedPublic.export({format:'pem',type:'spki'}));
      try{
        if(signer.asymmetricKeyType!=='ed25519'||pinnedPublic.asymmetricKeyType!=='ed25519'||!canonicalPrivate.equals(privatePem)||!canonicalPublic.equals(publicPem)||
          !createPublicKey(signer).export({format:'der',type:'spki'}).equals(pinnedPublic.export({format:'der',type:'spki'})))fail('state_publish_signer_invalid');
      }finally{canonicalPrivate.fill(0);canonicalPublic.fill(0);}
    }catch{fail('state_publish_signer_invalid');}
    check();
    const signed=signRecoveryStateRecord({state,backup:{id:backupId,manifest_sha256:manifestHash},previous,privateKey:signer!,now:options.now,maxAgeSeconds:config.limits.stateMaxAgeSeconds});
    const journalOptions={journalRoot:journal,anchorRoot:anchors,encryptionKeyFile:config.encryption.keyFile!,publicKey:pinnedPublic!,expectedSourceId:stateConfig.sourceId!,signal:options.signal,now:options.now,maxAgeSeconds:config.limits.stateMaxAgeSeconds};
    const stored=await appendRecoveryStateJournal({...journalOptions,timeoutMs:remaining(),expectedHead:previous,record:signed.record});published=true;
    // The just-published anchor checks storage self-consistency only. It is not
    // an independently acquired latest tip and cannot authorize recovery.
    const verified=await verifyRecoveryStateJournal({...journalOptions,timeoutMs:remaining(),trustedTip:stored.anchor});
    const lastManifest=await read(join(candidate,'capture.json'),1048576);
    try{if(sha(lastManifest)!==manifestHash)fail('state_publish_candidate_changed');}finally{lastManifest.fill(0);}
    check();
    return{status:'stored_unverified' as const,backupId,backupManifestSha256:manifestHash,anchor:stored.anchor,recordsVerified:verified.recordsVerified,
      encryptedAtRest:true,storedChainVerified:true,independentTipVerified:false,sourceEndpointVerified:false,anchorCustodyVerified:false,latestStateVerified:false,recoveryVerified:false,offhostVerified:false};
  }catch(error){
    if(published)throw new RecoveryStatePublishError('state_publish_uncertain');
    if(error instanceof RecoveryStatePublishError||error instanceof BackupConfigError||error instanceof BackupCandidateError||error instanceof RecoveryStateFormatError||error instanceof RecoveryStateRecordError||error instanceof RecoveryStateJournalError)throw error;
    throw new RecoveryStatePublishError('state_publish_failed');
  }finally{for(const buffer of buffers)buffer.fill(0);}
}
