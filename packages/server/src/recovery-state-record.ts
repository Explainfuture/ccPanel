import {KeyObject,createPublicKey,createHash,sign,verify} from 'node:crypto';
import {parseRecoverySourceState,RecoveryStateFormatError,type RecoverySourceState} from './recovery-state-format';

export interface RecoveryStateRecord{version:1;algorithm:'Ed25519';payload:string;signature:string}
export interface RecoveryStateBody{
  version:1;source_id:string;sequence:string;previous_hash:string|null;
  backup:{id:string;manifest_sha256:string};state:RecoverySourceState;
}
export interface RecoveryStateAnchor{
  version:1;source_id:string;key_fingerprint:string;sequence:string;record_hash:string;exported_at:string;
}
export class RecoveryStateRecordError extends Error{
  constructor(public code:string){super(code);this.name='RecoveryStateRecordError';}
}
const signatureDomain=Buffer.from('ccpanel.recovery-state.v1\0');
const hashDomain=Buffer.from('ccpanel.recovery-state.hash.v1\0');
const maxSequence=9223372036854775807n,maxPayload=16*1024*1024+4096;
const fail=(code='state_record_invalid'):never=>{throw new RecoveryStateRecordError(code);};
const token=(value:unknown):value is string=>typeof value==='string'&&/^[A-Za-z0-9_-]{1,128}$/.test(value);
const hash=(value:unknown):value is string=>typeof value==='string'&&/^[a-f0-9]{64}$/.test(value);
const sequence=(value:unknown):value is string=>typeof value==='string'&&/^[1-9][0-9]{0,18}$/.test(value)&&BigInt(value)<=maxSequence;
const iso=(value:unknown):value is string=>typeof value==='string'&&/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z$/.test(value)&&Number.isFinite(Date.parse(value))&&new Date(value).toISOString()===value;
function shape(value:unknown,keys:string[]):asserts value is Record<string,unknown>{
  if(!value||typeof value!=='object'||Array.isArray(value)||![Object.prototype,null].includes(Object.getPrototypeOf(value))||Reflect.ownKeys(value).length!==keys.length)fail();
  const descriptors=Object.getOwnPropertyDescriptors(value);
  if(keys.some(key=>!Object.hasOwn(descriptors,key)||!descriptors[key]!.enumerable||!Object.hasOwn(descriptors[key]!,'value')))fail();
}
function base64(value:unknown,max:number){
  // A simple character scan and canonical re-encode avoid recursive regular
  // expression backtracking on a maximum-size untrusted payload.
  if(typeof value!=='string'||!value.length||value.length>Math.ceil(max/3)*4||value.length%4||/[^A-Za-z0-9+/=]/.test(value))fail();
  const bytes=Buffer.from(value as string,'base64');
  if(!bytes.length||bytes.length>max||bytes.toString('base64')!==value)fail();return bytes;
}
function publicKey(key:KeyObject,kind:'private'|'public'){
  if(!(key instanceof KeyObject)||key.type!==kind||key.asymmetricKeyType!=='ed25519')fail('state_record_key_invalid');
  return kind==='private'?createPublicKey(key):key;
}
function fingerprint(key:KeyObject){return createHash('sha256').update(key.export({format:'der',type:'spki'})).digest('hex');}
function recordHash(payload:Buffer,signature:Buffer){
  const length=Buffer.alloc(4);length.writeUInt32BE(payload.length);
  return createHash('sha256').update(hashDomain).update(length).update(payload).update(signature).digest('hex');
}
function state(input:unknown,options:{now?:number;maxAgeSeconds?:number}):RecoverySourceState{
  try{return parseRecoverySourceState(input,options);}catch(error){if(error instanceof RecoveryStateFormatError)return fail(error.code);return fail();}
}
function anchor(body:RecoveryStateBody,key:KeyObject,payload:Buffer,signature:Buffer):RecoveryStateAnchor{
  return{version:1,source_id:body.source_id,key_fingerprint:fingerprint(key),sequence:body.sequence,record_hash:recordHash(payload,signature),exported_at:body.state.exported_at};
}
export function parseRecoveryStateAnchor(input:unknown):RecoveryStateAnchor{
  shape(input,['version','source_id','key_fingerprint','sequence','record_hash','exported_at']);
  if(input.version!==1||!token(input.source_id)||!hash(input.key_fingerprint)||!sequence(input.sequence)||!hash(input.record_hash)||!iso(input.exported_at))fail();
  return{version:1,source_id:input.source_id as string,key_fingerprint:input.key_fingerprint as string,sequence:input.sequence as string,record_hash:input.record_hash as string,exported_at:input.exported_at as string};
}

// Only signs the supplied state and append position. It does not persist/CAS an
// anchor, prove the publisher's source endpoint or advance any recovery gate.
export function signRecoveryStateRecord(options:{state:RecoverySourceState;backup:{id:string;manifest_sha256:string};previous:RecoveryStateAnchor|null;privateKey:KeyObject;now?:number;maxAgeSeconds?:number}):{record:RecoveryStateRecord;anchor:RecoveryStateAnchor}{
  try{
    const key=publicKey(options.privateKey,'private'),checked=state(options.state,{now:options.now,maxAgeSeconds:options.maxAgeSeconds});
    shape(options.backup,['id','manifest_sha256']);if(!token(options.backup.id)||!hash(options.backup.manifest_sha256))fail();
    let next='1',previousHash:string|null=null;
    if(options.previous!==null){
      const previous=parseRecoveryStateAnchor(options.previous);
      if(previous.source_id!==checked.source.id||previous.key_fingerprint!==fingerprint(key))fail('state_record_previous_mismatch');
      if(Date.parse(checked.exported_at)<Date.parse(previous.exported_at))fail('state_record_time_regressed');
      if(BigInt(previous.sequence)===maxSequence)fail('state_record_sequence_overflow');
      next=(BigInt(previous.sequence)+1n).toString();previousHash=previous.record_hash;
    }
    const body:RecoveryStateBody={version:1,source_id:checked.source.id,sequence:next,previous_hash:previousHash,backup:{id:options.backup.id,manifest_sha256:options.backup.manifest_sha256},state:checked};
    const payload=Buffer.from(JSON.stringify(body));if(payload.length>maxPayload)fail();
    const signature=sign(null,Buffer.concat([signatureDomain,payload]),options.privateKey);
    if(signature.length!==64)fail('state_record_key_invalid');
    return{record:{version:1,algorithm:'Ed25519',payload:payload.toString('base64'),signature:signature.toString('base64')},anchor:anchor(body,key,payload,signature)};
  }catch(error){if(error instanceof RecoveryStateRecordError)throw error;return fail('state_record_invalid');}
}

// Historical mode verifies authenticated old records for a chain prefix. It
// does not make them fresh; latest-tip validation must use the actual clock.
export function verifyRecoveryStateRecord(options:{record:unknown;publicKey:KeyObject;expectedSourceId:string;now?:number;maxAgeSeconds?:number;allowHistorical?:boolean}):{body:RecoveryStateBody;anchor:RecoveryStateAnchor;signatureVerified:true}{
  try{
    const key=publicKey(options.publicKey,'public');
    if(!token(options.expectedSourceId)||(options.allowHistorical!==undefined&&typeof options.allowHistorical!=='boolean'))fail();
    if((options.now!==undefined&&(!Number.isSafeInteger(options.now)||options.now<0))||(options.maxAgeSeconds!==undefined&&(!Number.isSafeInteger(options.maxAgeSeconds)||options.maxAgeSeconds<1||options.maxAgeSeconds>=3600)))fail('state_format_options_invalid');
    shape(options.record,['version','algorithm','payload','signature']);
    if(options.record.version!==1||options.record.algorithm!=='Ed25519')fail();
    const payload=base64(options.record.payload,maxPayload),signature=base64(options.record.signature,64);if(signature.length!==64)fail();
    if(!verify(null,Buffer.concat([signatureDomain,payload]),key,signature))fail('state_record_signature_invalid');
    let decoded:unknown;try{decoded=JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(payload));}catch{fail();}
    shape(decoded,['version','source_id','sequence','previous_hash','backup','state']);
    if(decoded.version!==1||!token(decoded.source_id)||!sequence(decoded.sequence)||(decoded.sequence==='1'?decoded.previous_hash!==null:!hash(decoded.previous_hash)))fail();
    if(decoded.source_id!==options.expectedSourceId)fail('state_record_source_mismatch');
    shape(decoded.backup,['id','manifest_sha256']);if(!token(decoded.backup.id)||!hash(decoded.backup.manifest_sha256))fail();
    // The historical clock is read only after the complete payload signature
    // passed. Malformed or self-declared trust flags are still rejected.
    const stateValue=decoded.state as Record<string,unknown>|null;
    const historicalAt=stateValue?.exported_at;
    if(options.allowHistorical&&!iso(historicalAt))fail();
    const checked=state(decoded.state,{now:options.allowHistorical?Date.parse(historicalAt as string):options.now,maxAgeSeconds:options.maxAgeSeconds});
    if(checked.source.id!==decoded.source_id)fail('state_record_source_mismatch');
    // Our signer emits JSON.stringify bytes. Roundtrip equality excludes signed
    // duplicate keys and alternate encodings that different consumers could
    // interpret differently. Outer record field order remains immaterial.
    if(!Buffer.from(JSON.stringify(decoded)).equals(payload))fail();
    const body:RecoveryStateBody={version:1,source_id:decoded.source_id as string,sequence:decoded.sequence as string,previous_hash:decoded.previous_hash as string|null,backup:{id:decoded.backup.id as string,manifest_sha256:decoded.backup.manifest_sha256 as string},state:checked};
    return{body,anchor:anchor(body,key,payload,signature),signatureVerified:true};
  }catch(error){if(error instanceof RecoveryStateRecordError)throw error;return fail();}
}
