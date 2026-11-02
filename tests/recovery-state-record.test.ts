import {it,expect} from 'vitest';
import {generateKeyPairSync,createSecretKey,sign} from 'node:crypto';
import {parseRecoverySourceState,type RecoverySourceState} from '../packages/server/src/recovery-state-format';
import {signRecoveryStateRecord,verifyRecoveryStateRecord,parseRecoveryStateAnchor,type RecoveryStateRecord} from '../packages/server/src/recovery-state-record';

const now=Date.parse('2026-11-02T00:10:00.000Z'),at='2026-11-02T00:09:00.000Z';
const keys=generateKeyPairSync('ed25519'),other=generateKeyPairSync('ed25519');
const backup={id:'backup-001',manifest_sha256:'a'.repeat(64)};
function state(exported_at=at):RecoverySourceState{return parseRecoverySourceState({version:1,status:'exported_unverified',source:{id:'source-001',database:'fixture',schema_version:22,authenticated:false},exported_at,deletion:{schema_version:2,exported_at,projects:[],jobs:[],barriers:[],retention:[]},integrity:{blocked:false,reason:null,detected_at:null,details:{},audit_high_water:'9007199254740993',streams:[{topic:'events',group_id:'worker',partition_count:1,initialized_at:exported_at,positions:[{partition:0,next_offset:'9223372036854775807',updated_at:exported_at}]}]},kafka_observation_verified:false,latest_state_verified:false},{now});}
const genesis=()=>signRecoveryStateRecord({state:state(),backup,previous:null,privateKey:keys.privateKey,now});
const verifyRecord=(record:unknown,extra:Partial<Parameters<typeof verifyRecoveryStateRecord>[0]>={})=>verifyRecoveryStateRecord({record,publicKey:keys.publicKey,expectedSourceId:'source-001',now,...extra});
function manuallySigned(body:unknown,bytes?:Buffer):RecoveryStateRecord{
  const payload=bytes??Buffer.from(JSON.stringify(body));
  return{version:1,algorithm:'Ed25519',payload:payload.toString('base64'),signature:sign(null,Buffer.concat([Buffer.from('ccpanel.recovery-state.v1\0'),payload]),keys.privateKey).toString('base64')};
}
it('signs and verifies actual Ed25519 bytes without promoting source or latest trust',()=>{
  const created=genesis(),verified=verifyRecord(created.record);
  expect(verified.signatureVerified).toBe(true);expect(verified.anchor).toEqual(created.anchor);expect(parseRecoveryStateAnchor(created.anchor)).toEqual(created.anchor);
  expect(verified.body.sequence).toBe('1');expect(verified.body.previous_hash).toBeNull();expect(verified.body.backup).toEqual(backup);
  expect(verified.body.state.integrity.audit_high_water).toBe('9007199254740993');expect(verified.body.state.integrity.streams[0]!.positions[0]!.next_offset).toBe('9223372036854775807');
  expect(verified.body.state.source.authenticated).toBe(false);expect(verified.body.state.latest_state_verified).toBe(false);
});
it('binds the next record to the exact previous hash and backup manifest',()=>{
  const first=genesis(),second=signRecoveryStateRecord({state:state('2026-11-02T00:09:30.000Z'),backup:{...backup,id:'backup-002'},previous:first.anchor,privateKey:keys.privateKey,now});
  const verified=verifyRecord(second.record);expect(verified.body.sequence).toBe('2');expect(verified.body.previous_hash).toBe(first.anchor.record_hash);expect(verified.anchor.record_hash).not.toBe(first.anchor.record_hash);
  const reordered={signature:second.record.signature,payload:second.record.payload,algorithm:'Ed25519',version:1};expect(verifyRecord(reordered).anchor.record_hash).toBe(second.anchor.record_hash);
});
it('rejects tampered payload, signature, wrong pinned key and wrong expected source',()=>{
  const {record}=genesis(),bytes=Buffer.from(record.payload,'base64');bytes[20]^=1;
  expect(()=>verifyRecord({...record,payload:bytes.toString('base64')})).toThrow('state_record_signature_invalid');
  const signature=Buffer.from(record.signature,'base64');signature[0]^=1;expect(()=>verifyRecord({...record,signature:signature.toString('base64')})).toThrow('state_record_signature_invalid');
  expect(()=>verifyRecord(record,{publicKey:other.publicKey})).toThrow('state_record_signature_invalid');expect(()=>verifyRecord(record,{expectedSourceId:'source-002'})).toThrow('state_record_source_mismatch');
});
it('refuses key-kind and algorithm confusion instead of deriving a pin from a record',()=>{
  const {record}=genesis(),secret=createSecretKey(Buffer.alloc(32,1)),rsa=generateKeyPairSync('rsa',{modulusLength:1024});
  for(const publicKey of [keys.privateKey,secret,rsa.publicKey])expect(()=>verifyRecord(record,{publicKey})).toThrow('state_record_key_invalid');
  for(const privateKey of [keys.publicKey,secret,rsa.privateKey])expect(()=>signRecoveryStateRecord({state:state(),backup,previous:null,privateKey,now})).toThrow('state_record_key_invalid');
  expect(()=>verifyRecord({...record,algorithm:'RSA-SHA256'})).toThrow('state_record_invalid');expect(()=>verifyRecord({...record,publicKey:other.publicKey.export({format:'pem',type:'spki'})})).toThrow('state_record_invalid');
});
it('strictly bounds/canonicalizes the outer encoding and fields',()=>{
  const {record}=genesis();
  for(const changed of [{...record,payload:record.payload+'\n'},{...record,payload:'AAAA='},{...record,payload:'A'.repeat(Math.ceil((16*1024*1024+4096)/3)*4+4)},{...record,signature:record.signature.replace(/=+$/,'')},{...record,signature:Buffer.alloc(63).toString('base64')},{...record,extra:true},{...record,version:2}])expect(()=>verifyRecord(changed)).toThrow('state_record_invalid');
});
it('rejects correctly signed invalid body structure, sequence relationships and trust claims',()=>{
  const first=genesis(),body=verifyRecord(first.record).body;
  const changes=[(b:any)=>b.extra=true,(b:any)=>b.sequence='0',(b:any)=>b.sequence='01',(b:any)=>b.sequence='9223372036854775808',(b:any)=>b.previous_hash='a'.repeat(64),(b:any)=>{b.sequence='2';b.previous_hash=null;},(b:any)=>b.backup.extra=true,(b:any)=>b.backup.manifest_sha256='not-a-hash',(b:any)=>b.state.source.authenticated=true];
  for(const change of changes){const value=structuredClone(body);change(value);expect(()=>verifyRecord(manuallySigned(value))).toThrow();}
  const mismatched=structuredClone(body);mismatched.state.source.id='source-002';expect(()=>verifyRecord(manuallySigned(mismatched))).toThrow('state_record_source_mismatch');
});
it('rejects malformed UTF-8 and JSON even when signed by the pinned key',()=>{
  expect(()=>verifyRecord(manuallySigned(null,Buffer.from([0xff,0xfe])))).toThrow('state_record_invalid');expect(()=>verifyRecord(manuallySigned(null,Buffer.from('{')))).toThrow('state_record_invalid');
});
it('rejects signed ambiguous JSON and signatures from outside this protocol domain',()=>{
  const first=genesis(),body=verifyRecord(first.record).body,encoded=JSON.stringify(body);
  for(const payload of [Buffer.from(' '+encoded),Buffer.from(encoded.replace('{"version":1,','{"version":2,"version":1,'))])expect(()=>verifyRecord(manuallySigned(null,payload))).toThrow('state_record_invalid');
  expect(()=>verifyRecord({...first.record,signature:sign(null,Buffer.from(first.record.payload,'base64'),keys.privateKey).toString('base64')})).toThrow('state_record_signature_invalid');
});
it('requires a valid exact previous anchor and prevents key/source/time/sequence regression',()=>{
  const first=genesis();
  for(const previous of [{...first.anchor,extra:true},{...first.anchor,sequence:'0'},{...first.anchor,record_hash:'bad'}])expect(()=>signRecoveryStateRecord({state:state(),backup,previous,privateKey:keys.privateKey,now})).toThrow('state_record_invalid');
  for(const previous of [{...first.anchor,source_id:'other-source'},{...first.anchor,key_fingerprint:'0'.repeat(64)}])expect(()=>signRecoveryStateRecord({state:state(),backup,previous,privateKey:keys.privateKey,now})).toThrow('state_record_previous_mismatch');
  expect(()=>signRecoveryStateRecord({state:state('2026-11-02T00:08:59.000Z'),backup,previous:first.anchor,privateKey:keys.privateKey,now})).toThrow('state_record_time_regressed');
  expect(()=>signRecoveryStateRecord({state:state(),backup,previous:{...first.anchor,sequence:'9223372036854775807'},privateKey:keys.privateKey,now})).toThrow('state_record_sequence_overflow');
  const next=signRecoveryStateRecord({state:state(),backup,previous:{...first.anchor,sequence:'9007199254740993'},privateKey:keys.privateKey,now});expect(verifyRecord(next.record).body.sequence).toBe('9007199254740994');
});
it('requires current freshness by default and permits authenticated historical prefixes only explicitly',()=>{
  const {record}=genesis();expect(()=>verifyRecord(record,{now:now+3600000})).toThrow('state_not_fresh');
  expect(verifyRecord(record,{now:now+3600000,allowHistorical:true}).body.state.exported_at).toBe(at);
  const invalid=structuredClone(record);invalid.payload=Buffer.from(JSON.stringify({state:{exported_at:at}})).toString('base64');expect(()=>verifyRecord(invalid,{allowHistorical:true})).toThrow('state_record_signature_invalid');
  expect(()=>verifyRecord(record,{allowHistorical:true,maxAgeSeconds:3600})).toThrow('state_format_options_invalid');
});
it('rejects malformed anchors independently of state records',()=>{
  const first=genesis();for(const changed of [{...first.anchor,exported_at:'2026-02-30T00:00:00.000Z'},{...first.anchor,key_fingerprint:'x'.repeat(64)},{...first.anchor,sequence:1},{...first.anchor,source_id:'bad/source'}])expect(()=>parseRecoveryStateAnchor(changed)).toThrow('state_record_invalid');
});
