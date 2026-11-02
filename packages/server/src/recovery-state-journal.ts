import {constants} from 'node:fs';
import {open,lstat,realpath,opendir,mkdir,rmdir,unlink,link,rename,statfs} from 'node:fs/promises';
import {resolve,isAbsolute,join,sep} from 'node:path';
import {KeyObject,createCipheriv,createDecipheriv,randomBytes,createHash} from 'node:crypto';
import {parseRecoveryStateAnchor,verifyRecoveryStateRecord,type RecoveryStateAnchor,type RecoveryStateRecord} from './recovery-state-record';
import {assertRecoveryStateContinuity} from './recovery-state-continuity';
import {verifyRecoveryStateChainAsync} from './recovery-state-chain';

export class RecoveryStateJournalError extends Error{constructor(public code:string){super(code);this.name='RecoveryStateJournalError';}}
interface JournalOptions{
  journalRoot:string;anchorRoot:string;encryptionKeyFile:string;publicKey:KeyObject;expectedSourceId:string;
  timeoutMs?:number;maxRecordBytes?:number;signal?:AbortSignal;now?:number;maxAgeSeconds?:number;
}
type AppendOptions=JournalOptions&{expectedHead:RecoveryStateAnchor|null;record:unknown};
type VerifyOptions=JournalOptions&{trustedTip:unknown};
const magic=Buffer.from('CCPBK001');
const fail=(code:string):never=>{throw new RecoveryStateJournalError(code);};
const same=(a:RecoveryStateAnchor,b:RecoveryStateAnchor)=>a.source_id===b.source_id&&a.key_fingerprint===b.key_fingerprint&&a.sequence===b.sequence&&a.record_hash===b.record_hash&&a.exported_at===b.exported_at;
const inside=(path:string,root:string)=>path===root||path.startsWith(root+sep);
const objectName=(anchor:RecoveryStateAnchor)=>anchor.sequence+'-'+anchor.record_hash+'.state.enc';
function safeError(error:unknown):never{
  if(error instanceof RecoveryStateJournalError)throw error;
  const code=(error as {code?:unknown})?.code;
  if(typeof code==='string'&&/^state_[a-z_]{1,80}$/.test(code))throw new RecoveryStateJournalError(code);
  throw new RecoveryStateJournalError('journal_io_failed');
}

async function context(options:JournalOptions){
  const timeout=options.timeoutMs??30000,max=options.maxRecordBytes??24*1024*1024;
  if(![options.journalRoot,options.anchorRoot,options.encryptionKeyFile].every(p=>typeof p==='string'&&isAbsolute(p))||!Number.isSafeInteger(timeout)||timeout<100||timeout>1800000||!Number.isSafeInteger(max)||max<1024||max>32*1024*1024||typeof options.expectedSourceId!=='string'||!/^[A-Za-z0-9_-]{1,128}$/.test(options.expectedSourceId))fail('journal_options_invalid');
  const deadline=performance.now()+timeout;
  const check=()=>{if(options.signal?.aborted)fail('journal_aborted');if(performance.now()>=deadline)fail('journal_timeout');};check();
  if(process.platform==='win32')fail('journal_permissions_unverified');
  if(!(options.publicKey instanceof KeyObject)||options.publicKey.type!=='public'||options.publicKey.asymmetricKeyType!=='ed25519')fail('journal_key_invalid');
  const journal=resolve(options.journalRoot),anchors=resolve(options.anchorRoot),keyFile=resolve(options.encryptionKeyFile);
  if(inside(journal,anchors)||inside(anchors,journal))fail('journal_directory_unsafe');
  const initialJournal=await lstat(journal),initialAnchors=await lstat(anchors);
  if(initialJournal.dev===initialAnchors.dev&&initialJournal.ino===initialAnchors.ino)fail('journal_directory_unsafe');
  const directory=async(path:string,initial:{dev:number;ino:number})=>{
    check();const current=await lstat(path);
    if(!current.isDirectory()||current.isSymbolicLink()||current.dev!==initial.dev||current.ino!==initial.ino||current.uid!==process.geteuid?.()||(current.mode&0o077)!==0||await realpath(path)!==path)fail('journal_directory_unsafe');check();
  };
  const guards=async()=>{await directory(journal,initialJournal);await directory(anchors,initialAnchors);};await guards();
  const read=async(path:string,limit:number)=>{
    check();const file=await open(path,constants.O_RDONLY|(constants.O_NOFOLLOW??0)|(constants.O_NONBLOCK??0));
    let bytes:Buffer|undefined;
    try{
      const before=await file.stat();if(!before.isFile()||before.uid!==process.geteuid?.()||(before.mode&0o077)!==0||before.size<1||before.size>limit)fail('journal_file_unsafe');
      bytes=Buffer.alloc(before.size+1);let count=0;
      while(count<bytes.length){check();const result=await file.read(bytes,count,Math.min(65536,bytes.length-count),null);if(!result.bytesRead)break;count+=result.bytesRead;}
      const after=await file.stat(),current=await lstat(path);check();
      if(count!==before.size||after.size!==before.size||after.mtimeMs!==before.mtimeMs||after.ctimeMs!==before.ctimeMs||current.dev!==before.dev||current.ino!==before.ino||current.isSymbolicLink())fail('journal_file_changed');
      return bytes.subarray(0,count);
    }catch(error){bytes?.fill(0);throw error;}finally{try{await file.close();}catch(error){bytes?.fill(0);throw error;}}
  };
  let key:Buffer;
  try{
    if(await realpath(keyFile)!==keyFile||inside(keyFile,journal)||inside(keyFile,anchors))fail('journal_key_unsafe');
    key=await read(keyFile,32);if(key.length!==32){key.fill(0);fail('journal_key_unsafe');}
  }catch(error){if(error instanceof RecoveryStateJournalError&&['journal_aborted','journal_timeout'].includes(error.code))throw error;fail('journal_key_unsafe');}
  const head=async():Promise<RecoveryStateAnchor|null>=>{
    try{const bytes=await read(join(anchors,'head.json'),4096);return parseRecoveryStateAnchor(JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(bytes)));}
    catch(error){if((error as NodeJS.ErrnoException).code==='ENOENT')return null;throw error;}
  };
  const index=async(head:RecoveryStateAnchor|null)=>{
    await guards();const files=new Map<number,string>();
    let entries=0;
    for await(const entry of await opendir(journal)){
      if(++entries>20000)fail('journal_recovery_required');const name=entry.name;
      const match=/^([1-9][0-9]{0,18})-([a-f0-9]{64})\.state\.enc$/.exec(name);if(!match){if(name.endsWith('.state.enc'))fail('journal_recovery_required');continue;}
      const sequence=BigInt(match[1]);if(sequence>10000n||files.has(Number(sequence)))fail('journal_recovery_required');files.set(Number(sequence),name);
    }
    const expected=head?BigInt(head.sequence):0n;if(expected>10000n||BigInt(files.size)!==expected)fail('journal_recovery_required');
    for(let n=1;n<=files.size;n++)if(!files.has(n))fail('journal_recovery_required');
    if(head&&files.get(Number(head.sequence))!==objectName(head))fail('journal_recovery_required');return files;
  };
  const decode=async(name:string)=>{
    await guards();const cipher=await read(join(journal,name),max+36);let plain:Buffer|undefined;
    try{
      if(cipher.length<37||!cipher.subarray(0,8).equals(magic))fail('journal_authentication_failed');
      const decipher=createDecipheriv('aes-256-gcm',key!,cipher.subarray(8,20));decipher.setAAD(magic);decipher.setAuthTag(cipher.subarray(cipher.length-16));
      let part:Buffer|undefined,tail:Buffer|undefined;
      try{part=decipher.update(cipher.subarray(20,cipher.length-16));tail=decipher.final();plain=Buffer.concat([part,tail]);}catch{fail('journal_authentication_failed');}finally{part?.fill(0);tail?.fill(0);}
      if(plain!.length>max)fail('journal_record_too_large');
      const text=new TextDecoder('utf-8',{fatal:true}).decode(plain),record:unknown=JSON.parse(text);
      if(JSON.stringify(record)!==text)fail('journal_record_invalid');
      const checked=verifyRecoveryStateRecord({record,publicKey:options.publicKey,expectedSourceId:options.expectedSourceId,allowHistorical:true,now:options.now,maxAgeSeconds:options.maxAgeSeconds});
      if(objectName(checked.anchor)!==name)fail('journal_record_invalid');check();return{record,checked};
    }finally{plain?.fill(0);}
  };
  const sync=async(path:string,cleanup=false)=>{if(!cleanup)check();const file=await open(path,constants.O_RDONLY|(constants.O_DIRECTORY??0)|(constants.O_NOFOLLOW??0));try{await file.sync();}finally{await file.close();}if(!cleanup)check();};
  const write=async(path:string,bytes:Buffer,owned:Set<string>)=>{
    check();const file=await open(path,constants.O_WRONLY|constants.O_CREAT|constants.O_EXCL|(constants.O_NOFOLLOW??0),0o600);owned.add(path);
    try{let offset=0;while(offset<bytes.length){check();const result=await file.write(bytes,offset,Math.min(65536,bytes.length-offset),null);if(!result.bytesWritten)fail('journal_write_failed');offset+=result.bytesWritten;}await file.sync();check();}finally{await file.close();}
    const actual=await read(path,bytes.length);if(!actual.equals(bytes))fail('journal_readback_failed');
  };
  return{journal,anchors,max,key:key!,check,guards,head,index,decode,read,sync,write};
}

// The local head is a CAS cursor, not an independently protected latest tip.
// Append verifies the prior signed head, not all historical ciphertext again.
export async function appendRecoveryStateJournal(options:AppendOptions){
  let ctx:Awaited<ReturnType<typeof context>>|undefined,locked=false,lockIdentity:{dev:number;ino:number}|undefined,publicationAttempted=false;
  const owned=new Set<string>();
  try{
    const expected=options.expectedHead===null?null:parseRecoveryStateAnchor(options.expectedHead);
    ctx=await context(options);const lock=join(ctx.anchors,'.append.lock');
    try{await mkdir(lock,{mode:0o700});locked=true;lockIdentity=await lstat(lock);}catch(error){if((error as NodeJS.ErrnoException).code==='EEXIST')fail('journal_busy');throw error;}
    await ctx.guards();const head=await ctx.head();
    if((head===null)!==(expected===null)||(head&&expected&&!same(head,expected)))fail('journal_head_conflict');
    await ctx.index(head);
    const checked=verifyRecoveryStateRecord({record:options.record,publicKey:options.publicKey,expectedSourceId:options.expectedSourceId,now:options.now,maxAgeSeconds:options.maxAgeSeconds});
    // Keep immutable wire strings immediately after synchronous validation;
    // caller mutation while the previous head is read must not change storage.
    const input=options.record as RecoveryStateRecord;
    const pinnedRecord:RecoveryStateRecord={version:1,algorithm:'Ed25519',payload:input.payload,signature:input.signature};
    if(checked.body.sequence!==(head?(BigInt(head.sequence)+1n).toString():'1')||checked.body.previous_hash!==(head?.record_hash??null))fail('journal_head_conflict');
    if(BigInt(checked.anchor.sequence)>10000n)fail('journal_recovery_required');
    if(head){const previous=await ctx.decode(objectName(head));if(!same(previous.checked.anchor,head))fail('journal_recovery_required');assertRecoveryStateContinuity(previous.checked.body.state,checked.body.state);}
    const plain=Buffer.from(JSON.stringify(pinnedRecord));let encrypted:Buffer;
    try{
      if(plain.length>ctx.max)fail('journal_record_too_large');
      const iv=randomBytes(12),encryptor=createCipheriv('aes-256-gcm',ctx.key,iv);encryptor.setAAD(magic);
      encrypted=Buffer.concat([magic,iv,encryptor.update(plain),encryptor.final(),encryptor.getAuthTag()]);
    }finally{plain.fill(0);}
    const space=await statfs(ctx.journal,{bigint:true});if(space.bavail*space.bsize<BigInt(encrypted!.length)+1048576n)fail('journal_space_low');
    const temporary=join(ctx.journal,'.record-'+randomBytes(12).toString('hex')+'.tmp'),destination=join(ctx.journal,objectName(checked.anchor));
    await ctx.write(temporary,encrypted!,owned);await ctx.guards();ctx.check();publicationAttempted=true;
    await link(temporary,destination);await ctx.sync(ctx.journal);await unlink(temporary);owned.delete(temporary);await ctx.sync(ctx.journal);
    const stored=await ctx.decode(objectName(checked.anchor));if(!same(stored.checked.anchor,checked.anchor))fail('journal_readback_failed');
    const headTemporary=join(ctx.anchors,'.head-'+randomBytes(12).toString('hex')+'.tmp');
    await ctx.write(headTemporary,Buffer.from(JSON.stringify(checked.anchor)+'\n'),owned);
    const current=await ctx.head();if((current===null)!==(head===null)||(current&&head&&!same(current,head)))fail('journal_head_conflict');
    await ctx.guards();ctx.check();await rename(headTemporary,join(ctx.anchors,'head.json'));owned.delete(headTemporary);await ctx.sync(ctx.anchors);
    const final=await ctx.head();if(!final||!same(final,checked.anchor))fail('journal_readback_failed');ctx.check();
    return{status:'stored_unverified' as const,anchor:checked.anchor,encryptedAtRest:true,previousHeadVerified:true,historicalRecordsReverified:false,recordCipherSha256:createHash('sha256').update(encrypted!).digest('hex'),sourceEndpointVerified:false,anchorCustodyVerified:false,latestStateVerified:false};
  }catch(error){if(publicationAttempted)throw new RecoveryStateJournalError('journal_publish_uncertain');safeError(error);}
  finally{
    ctx?.key.fill(0);let failed=false;
    for(const path of owned)try{await unlink(path);}catch(error){if((error as NodeJS.ErrnoException).code!=='ENOENT')failed=true;}
    if(locked&&ctx)try{const path=join(ctx.anchors,'.append.lock'),identity=await lstat(path);if(identity.dev!==lockIdentity?.dev||identity.ino!==lockIdentity?.ino)failed=true;else{await rmdir(path);await ctx.sync(ctx.anchors,true);}}catch{failed=true;}
    if(failed)throw new RecoveryStateJournalError(publicationAttempted?'journal_publish_uncertain':'journal_cleanup_failed');
  }
}

export async function verifyRecoveryStateJournal(options:VerifyOptions){
  let ctx:Awaited<ReturnType<typeof context>>|undefined;
  try{
    if(options.trustedTip===null||options.trustedTip===undefined)fail('journal_anchor_required');
    const expected=parseRecoveryStateAnchor(options.trustedTip);ctx=await context(options);
    const head=await ctx.head();if(!head||!same(head,expected))fail('journal_head_conflict');
    const files=await ctx.index(head),contextValue=ctx;
    async function* records(){for(let n=1;n<=files.size;n++){contextValue.check();yield(await contextValue.decode(files.get(n)!)).record;}}
    const result=await verifyRecoveryStateChainAsync({records:records(),publicKey:options.publicKey,expectedSourceId:options.expectedSourceId,trustedTip:expected,now:options.now,maxAgeSeconds:options.maxAgeSeconds});
    await ctx.guards();const final=await ctx.head();if(!final||!same(final,expected))fail('journal_head_conflict');ctx.check();
    return{...result,status:'journal_verified_unadmitted' as const,encryptedAtRest:true};
  }catch(error){safeError(error);}finally{ctx?.key.fill(0);}
}
