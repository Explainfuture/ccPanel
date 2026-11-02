import {it,expect,vi} from 'vitest';
import {generateKeyPairSync} from 'node:crypto';
import {signRecoveryStateRecord} from '../packages/server/src/recovery-state-record';
import {verifyRecoveryStateChain,verifyRecoveryStateChainAsync} from '../packages/server/src/recovery-state-chain';
import type {RecoverySourceState} from '../packages/server/src/recovery-state-format';

const {privateKey,publicKey}=generateKeyPairSync('ed25519');
const now=Date.parse('2026-11-02T00:10:00.000Z');
const state=(at:string):RecoverySourceState=>({version:1,status:'exported_unverified',source:{id:'async-source',database:'fixture',schema_version:22,authenticated:false},exported_at:at,deletion:{schema_version:2,exported_at:at,projects:[],jobs:[],barriers:[],retention:[]},integrity:{blocked:false,reason:null,detected_at:null,details:{},audit_high_water:'0',streams:[]},kafka_observation_verified:false,latest_state_verified:false});
const backup={id:'fixture-backup',manifest_sha256:'a'.repeat(64)};
const first=signRecoveryStateRecord({state:state('2026-11-02T00:09:00.000Z'),backup,previous:null,privateKey,now});
const second=signRecoveryStateRecord({state:state('2026-11-02T00:10:00.000Z'),backup,previous:first.anchor,privateKey,now});
const base=()=>({publicKey,expectedSourceId:'async-source',trustedTip:second.anchor,now});
async function* stream(records:unknown[]){for(const record of records){await Promise.resolve();yield record;}}

it('authenticates an async record stream with the same result and admission limits as sync',async()=>{
  const records=[first.record,second.record];
  const result=await verifyRecoveryStateChainAsync({...base(),records:stream(records)});
  expect(result).toEqual(verifyRecoveryStateChain({...base(),records}));
  expect(result).toMatchObject({recordsVerified:2,signaturesVerified:true,anchorMatched:true,semanticContinuityChecked:true,sourceEndpointVerified:false,anchorCustodyVerified:false,latestStateVerified:false});
});
it.each([
  [[second.record,first.record],'state_chain_discontinuous'],
  [[first.record,{...second.record,signature:Buffer.alloc(64).toString('base64')}],'state_record_signature_invalid'],
])('closes the async generator when a record fails verification',async(records,code)=>{
  let closed=false;
  async function* source(){try{for(const record of records){await Promise.resolve();yield record;}}finally{closed=true;}}
  await expect(verifyRecoveryStateChainAsync({...base(),records:source()})).rejects.toThrow(code);
  expect(closed).toBe(true);
});
it('enforces the shared record bound without eagerly reading the rest of history',async()=>{
  let closed=false,reads=0;
  async function* source(){try{for(const record of [first.record,second.record,second.record]){reads++;yield record;}}finally{closed=true;}}
  await expect(verifyRecoveryStateChainAsync({...base(),records:source(),maxRecords:1})).rejects.toThrow('state_chain_budget_exceeded');
  expect(closed).toBe(true);expect(reads).toBe(2);
});
it('allows old authenticated prefixes while rejecting a stale terminal record at the actual clock',async()=>{
  const historicAt=Date.parse('2026-11-01T00:00:00.000Z');
  const old=signRecoveryStateRecord({state:state(new Date(historicAt).toISOString()),backup,previous:null,privateKey,now:historicAt});
  const current=signRecoveryStateRecord({state:state(new Date(now).toISOString()),backup,previous:old.anchor,privateKey,now});
  const result=await verifyRecoveryStateChainAsync({...base(),records:stream([old.record,current.record]),trustedTip:current.anchor});
  expect(result.recordsVerified).toBe(2);
  await expect(verifyRecoveryStateChainAsync({...base(),records:stream([old.record]),trustedTip:old.anchor})).rejects.toThrow('state_not_fresh');
});
it('propagates reader failure and runs its cleanup without producing chain evidence',async()=>{
  let closed=false;
  async function* source(){try{yield first.record;throw Error('controlled_reader_failure');}finally{closed=true;}}
  await expect(verifyRecoveryStateChainAsync({...base(),records:source()})).rejects.toThrow('controlled_reader_failure');
  expect(closed).toBe(true);
});
it('rejects a non-async source before consuming it',async()=>{
  await expect(verifyRecoveryStateChainAsync({...base(),records:[first.record] as unknown as AsyncIterable<unknown>})).rejects.toThrow('state_chain_options_invalid');
});
it('rechecks actual-clock tip freshness after a long read while preserving an explicit clock',async()=>{
  let wallClock=now;
  const clock=vi.spyOn(Date,'now').mockImplementation(()=>wallClock);
  async function* source(){
    yield first.record;await Promise.resolve();yield second.record;
    // The reader finishes only after the initially fresh terminal has expired.
    wallClock=now+600001;
  }
  try{
    await expect(verifyRecoveryStateChainAsync({...base(),now:undefined,records:source()})).rejects.toThrow('state_not_fresh');
    wallClock=now;
    const explicit=await verifyRecoveryStateChainAsync({...base(),records:source()});
    expect(explicit.recordsVerified).toBe(2);expect(wallClock).toBe(now+600001);
  }finally{clock.mockRestore();}
});
