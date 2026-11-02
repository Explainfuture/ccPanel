import {it,expect} from 'vitest';
import {generateKeyPairSync} from 'node:crypto';
import {signRecoveryStateRecord} from '../packages/server/src/recovery-state-record';
import {verifyRecoveryStateChain} from '../packages/server/src/recovery-state-chain';
import type {RecoverySourceState} from '../packages/server/src/recovery-state-format';

const {privateKey,publicKey}=generateKeyPairSync('ed25519');
const now=Date.parse('2026-11-02T00:10:00.000Z');
const state=(at:string):RecoverySourceState=>({version:1,status:'exported_unverified',source:{id:'source-fixture',database:'fixture',schema_version:22,authenticated:false},exported_at:at,deletion:{schema_version:2,exported_at:at,projects:[],jobs:[],barriers:[],retention:[]},integrity:{blocked:false,reason:null,detected_at:null,details:{},audit_high_water:'0',streams:[]},kafka_observation_verified:false,latest_state_verified:false});
const backup={id:'fixture-backup',manifest_sha256:'a'.repeat(64)};
const first=signRecoveryStateRecord({state:state('2026-11-02T00:09:00.000Z'),backup,previous:null,privateKey,now});
const second=signRecoveryStateRecord({state:state('2026-11-02T00:10:00.000Z'),backup,previous:first.anchor,privateKey,now});
const options=()=>({records:[first.record,second.record],publicKey,expectedSourceId:'source-fixture',trustedTip:second.anchor,now});

it('matches independently supplied exact tip and preserves all admission limits',()=>{
  const result=verifyRecoveryStateChain(options());
  expect(result).toMatchObject({recordsVerified:2,signaturesVerified:true,anchorMatched:true,semanticContinuityChecked:true,sourceEndpointVerified:false,anchorCustodyVerified:false,latestStateVerified:false});
  expect(result.state.exported_at).toBe(second.anchor.exported_at);expect(result.state.latest_state_verified).toBe(false);
});
it('requires a tip and refuses replayed prefix against the newer tip',()=>{
  expect(()=>verifyRecoveryStateChain({...options(),trustedTip:null})).toThrow('state_chain_anchor_required');
  expect(()=>verifyRecoveryStateChain({...options(),records:[first.record]})).toThrow('state_chain_anchor_mismatch');
});
it('refuses reorder, missing genesis, duplicated records and changed source/key pins',()=>{
  for(const records of [[second.record,first.record],[second.record],[first.record,first.record,second.record]])expect(()=>verifyRecoveryStateChain({...options(),records})).toThrow('state_chain_discontinuous');
  expect(()=>verifyRecoveryStateChain({...options(),expectedSourceId:'other-source'})).toThrow('state_chain_anchor_mismatch');
  expect(()=>verifyRecoveryStateChain({...options(),publicKey:generateKeyPairSync('ed25519').publicKey})).toThrow('state_chain_anchor_mismatch');
});
it('rejects signature tampering rather than treating matching metadata as proof',()=>{
  const damaged={...second.record,signature:Buffer.alloc(64).toString('base64')};
  expect(()=>verifyRecoveryStateChain({...options(),records:[first.record,damaged]})).toThrow('state_record_signature_invalid');
});
it('allows expired history but requires fresh actual-clock tip',()=>{
  const old=signRecoveryStateRecord({state:state('2026-11-01T00:00:00.000Z'),backup,previous:null,privateKey,now:Date.parse('2026-11-01T00:00:00.000Z')});
  const current=signRecoveryStateRecord({state:state('2026-11-02T00:10:00.000Z'),backup,previous:old.anchor,privateKey,now});
  expect(verifyRecoveryStateChain({...options(),records:[old.record,current.record],trustedTip:current.anchor}).recordsVerified).toBe(2);
  expect(()=>verifyRecoveryStateChain({...options(),records:[old.record],trustedTip:old.anchor})).toThrow('state_not_fresh');
});
it('does not claim current anchor custody when storage and an old tip are replayed together',()=>{
  // Exact-anchor correspondence alone cannot establish that supplied control
  // plane metadata is current. This evidence must never open a recovery gate.
  const result=verifyRecoveryStateChain({...options(),records:[first.record],trustedTip:first.anchor});
  expect(result.anchorMatched).toBe(true);expect(result.latestStateVerified).toBe(false);expect(result.anchorCustodyVerified).toBe(false);
});
it('rejects a newly signed semantic rollback even when all signatures and links are valid',()=>{
  const advanced=state('2026-11-02T00:09:00.000Z');advanced.integrity.audit_high_water='9007199254740993';
  const a=signRecoveryStateRecord({state:advanced,backup,previous:null,privateKey,now});
  const rolled=state('2026-11-02T00:10:00.000Z');rolled.integrity.audit_high_water='9007199254740992';
  const b=signRecoveryStateRecord({state:rolled,backup,previous:a.anchor,privateKey,now});
  expect(()=>verifyRecoveryStateChain({...options(),records:[a.record,b.record],trustedTip:b.anchor})).toThrow('state_audit_regressed');
});
it('bounds record count and closes a streaming iterator on rejection',()=>{
  let closed=false;
  function* records(){try{yield first.record;yield second.record;}finally{closed=true;}}
  expect(()=>verifyRecoveryStateChain({...options(),records:records(),maxRecords:1})).toThrow('state_chain_budget_exceeded');expect(closed).toBe(true);
  expect(()=>verifyRecoveryStateChain({...options(),records:[]})).toThrow('state_chain_empty');
});
