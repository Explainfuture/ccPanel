import {createHash,type KeyObject} from 'node:crypto';
import {parseRecoverySourceState,type RecoverySourceState} from './recovery-state-format';
import {parseRecoveryStateAnchor,verifyRecoveryStateRecord,type RecoveryStateAnchor} from './recovery-state-record';
import {assertRecoveryStateContinuity} from './recovery-state-continuity';

export class RecoveryStateChainError extends Error{constructor(public code:string){super(code);this.name='RecoveryStateChainError';}}
export interface StateChainOptions{
  records:Iterable<unknown>;publicKey:KeyObject;expectedSourceId:string;trustedTip:unknown;
  now?:number;maxAgeSeconds?:number;maxRecords?:number;
}
export type AsyncStateChainOptions=Omit<StateChainOptions,'records'>&{records:AsyncIterable<unknown>};
// The tip and pinned key must come from a separate trusted control plane. This
// checks correspondence to that supplied tip, not that its custody is current.
// A rolled-back tip replayed with matching records is not detectable here.
function createVerifier(options:Omit<StateChainOptions,'records'>){
  const fail=(code:string):never=>{throw new RecoveryStateChainError(code);};
  const explicitNow=options.now,now=explicitNow??Date.now(),age=options.maxAgeSeconds??600,limit=options.maxRecords??10000;
  const publicKey=options.publicKey,expectedSourceId=options.expectedSourceId;
  if(typeof expectedSourceId!=='string'||!/^[A-Za-z0-9_-]{1,128}$/.test(expectedSourceId)||!Number.isSafeInteger(now)||now<0||!Number.isSafeInteger(age)||age<1||age>=3600||!Number.isSafeInteger(limit)||limit<1||limit>10000)fail('state_chain_options_invalid');
  if(options.trustedTip===null||options.trustedTip===undefined)fail('state_chain_anchor_required');
  const tip={...parseRecoveryStateAnchor(options.trustedTip)};
  if(tip.source_id!==expectedSourceId)fail('state_chain_anchor_mismatch');
  if(publicKey?.type!=='public'||publicKey.asymmetricKeyType!=='ed25519')fail('state_chain_key_invalid');
  const fingerprint=createHash('sha256').update(publicKey.export({type:'spki',format:'der'})).digest('hex');
  if(tip.key_fingerprint!==fingerprint)fail('state_chain_anchor_mismatch');
  let previous:RecoveryStateAnchor|undefined,state:RecoverySourceState|undefined,count=0;
  const push=(record:unknown)=>{
    if(++count>limit)fail('state_chain_budget_exceeded');
    // Historical prefix records may be older than the fresh tip. Each is still
    // signature/format validated; actual-clock freshness is checked below.
    const checked=verifyRecoveryStateRecord({record,publicKey,expectedSourceId,now,maxAgeSeconds:age,allowHistorical:true});
    const expected=previous?(BigInt(previous.sequence)+1n).toString():'1';
    if(checked.body.sequence!==expected||checked.body.previous_hash!==(previous?.record_hash??null))fail('state_chain_discontinuous');
    if(state)assertRecoveryStateContinuity(state,checked.body.state);
    state=checked.body.state;previous=checked.anchor;
  };
  const finish=()=>{
    if(!previous||!state)fail('state_chain_empty');
    if(previous!.source_id!==tip.source_id||previous!.key_fingerprint!==tip.key_fingerprint||previous!.sequence!==tip.sequence||previous!.record_hash!==tip.record_hash||previous!.exported_at!==tip.exported_at)fail('state_chain_anchor_mismatch');
    // Storage reads may take time. Recheck the wall clock at completion unless
    // the caller explicitly supplied a stable validation clock.
    state=parseRecoverySourceState(state!,{now:explicitNow??Date.now(),maxAgeSeconds:age});
    return{state,anchor:previous!,recordsVerified:count,signaturesVerified:true,anchorMatched:true,semanticContinuityChecked:true,sourceEndpointVerified:false,anchorCustodyVerified:false,latestStateVerified:false};
  };
  return{push,finish};
}
export function verifyRecoveryStateChain(options:StateChainOptions){
  if(!options.records||typeof options.records[Symbol.iterator]!=='function')throw new RecoveryStateChainError('state_chain_options_invalid');
  const verifier=createVerifier(options);
  for(const record of options.records)verifier.push(record);
  return verifier.finish();
}
// Async storage readers yield one record at a time. for-await invokes their
// return/finally path on rejection, without retaining or collecting history.
export async function verifyRecoveryStateChainAsync(options:AsyncStateChainOptions){
  if(!options.records||typeof options.records[Symbol.asyncIterator]!=='function')throw new RecoveryStateChainError('state_chain_options_invalid');
  const verifier=createVerifier(options);
  for await(const record of options.records)verifier.push(record);
  return verifier.finish();
}
