import { canonical,storageSafeText,validateEvent,type StoredEnvelope } from '../../schema/src/index';
import { digest } from './security';

const uuid=/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const day=86400000;
export class ReplayRejected extends Error {
  constructor(public code:string){super(code);}
}
const reject=(code:string):never=>{throw new ReplayRejected(code);};
export function parseReplayRequest(input:unknown){
  if(!input||typeof input!=='object'||Array.isArray(input))return reject('invalid_replay_request');
  const b=input as Record<string,unknown>;
  if(Object.keys(b).some(k=>!['request_id','record_ids','reason'].includes(k))||typeof b.request_id!=='string'||!uuid.test(b.request_id)||!Array.isArray(b.record_ids)||b.record_ids.length<1||b.record_ids.length>20||b.record_ids.some(id=>typeof id!=='string'||!/^[a-f0-9]{64}$/.test(id))||new Set(b.record_ids).size!==b.record_ids.length||typeof b.reason!=='string'||b.reason.trim().length<5||b.reason.length>500||!storageSafeText(b.reason))return reject('invalid_replay_request');
  return {request_id:b.request_id.toLowerCase(),record_ids:[...b.record_ids].sort() as string[],reason:b.reason.trim()};
}
export interface ReplayRegistration {payload_hash:string;created_at:Date|string}
// This policy does not grant authorization or bypass deletion barriers. The
// caller must scope the DLQ row and registration and hold the project privacy
// lock while checking current barriers and enqueueing the task.
export function replayCandidate(input:{payload:unknown;reason:string;tenant:string;project:string;registration:ReplayRegistration|null},now=Date.now()){
  if(['payload_conflict','identity_conflict','scope_mismatch'].includes(input.reason))return reject('manual_review_required');
  const p=input.payload as Record<string,unknown>|null;
  if(!p||typeof p!=='object'||p.redacted===true)return reject('payload_unavailable');
  if(p.processor_version!==2||typeof p.raw_base64!=='string'||p.raw_base64.length===0||p.raw_base64.length>87384||!/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(p.raw_base64))return reject('unsupported_replay_payload');
  const raw=Buffer.from(p.raw_base64,'base64');
  if(raw.length>65536||raw.toString('base64')!==p.raw_base64)return reject('unsupported_replay_payload');
  let record:StoredEnvelope;
  try{record=JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(raw));}catch{return reject('invalid_replay_envelope');}
  if(!record||typeof record!=='object'||Array.isArray(record)||typeof record.tenant_id!=='string'||typeof record.project_id!=='string'||!uuid.test(record.tenant_id)||!uuid.test(record.project_id)||record.tenant_id.toLowerCase()!==input.tenant.toLowerCase()||record.project_id.toLowerCase()!==input.project.toLowerCase())return reject('replay_scope_mismatch');
  if(validateEvent(record.event,now,false))return reject('event_still_invalid');
  if(record.validation!==undefined&&(!record.validation||typeof record.validation!=='object'||typeof record.validation.registered!=='boolean'||!Number.isSafeInteger(record.validation.version)||record.validation.version<0))return reject('invalid_replay_envelope');
  const eventTime=Date.parse(record.event.event_time),received=typeof record.received_at==='string'&&/Z$/.test(record.received_at)?Date.parse(record.received_at):NaN;
  if(!Number.isFinite(received)||received>now+300000||eventTime>now+300000||eventTime<now-30*day||received<now-30*day)return reject('replay_window_expired');
  const registration=input.registration;
  if(!registration)return reject('dedup_registration_missing');
  const created=new Date(registration.created_at).getTime();
  if(!Number.isFinite(created)||created<now-45*day||created>now+300000)return reject('dedup_registration_expired');
  if(registration.payload_hash!==digest(canonical(record.event)))return reject('manual_review_required');
  return {record,raw};
}
