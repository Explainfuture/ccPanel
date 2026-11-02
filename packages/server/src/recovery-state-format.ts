import {parseDeletionLedger,type DeletionLedger} from './deletion-ledger-format';

export interface RecoverySourceState{
  version:1;
  status:'exported_unverified';
  source:{id:string;database:string;schema_version:22;authenticated:false};
  exported_at:string;
  deletion:Extract<DeletionLedger,{schema_version:2}>;
  integrity:{
    blocked:boolean;reason:null|'retained_offset_gap'|'consumer_offset_unknown';detected_at:string|null;
    details:Record<string,unknown>;audit_high_water:string;
    streams:{topic:string;group_id:string;partition_count:number;initialized_at:string;positions:{partition:number;next_offset:string;updated_at:string}[]}[];
  };
  kafka_observation_verified:false;
  latest_state_verified:false;
}
export class RecoveryStateFormatError extends Error{
  constructor(public code:'invalid_recovery_state'|'state_not_fresh'|'state_format_options_invalid'){super(code);this.name='RecoveryStateFormatError';}
}
const invalid=():never=>{throw new RecoveryStateFormatError('invalid_recovery_state');};
function shape(value:unknown,keys:string[]):asserts value is Record<string,unknown>{
  if(!value||typeof value!=='object'||Array.isArray(value)||![Object.prototype,null].includes(Object.getPrototypeOf(value))||Reflect.ownKeys(value).length!==keys.length||keys.some(key=>!Object.hasOwn(value,key)))invalid();
}
const iso=(value:unknown):value is string=>typeof value==='string'&&/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z$/.test(value)&&Number.isFinite(Date.parse(value))&&new Date(value).toISOString()===value;
const offset=(value:unknown):value is string=>typeof value==='string'&&/^(0|[1-9][0-9]{0,18})$/.test(value)&&BigInt(value)<=9223372036854775807n;
function jsonDetails(value:unknown){
  if(!value||typeof value!=='object'||Array.isArray(value)||![Object.prototype,null].includes(Object.getPrototypeOf(value)))invalid();
  const pending:[unknown,number][]=[[value,0]],seen=new Set<object>();let nodes=0;
  while(pending.length){
    const [item,depth]=pending.pop()!;if(++nodes>65536||depth>64)invalid();
    if(item===null||typeof item==='string'||typeof item==='boolean')continue;
    if(typeof item==='number'){if(!Number.isFinite(item))invalid();continue;}
    if(typeof item!=='object')invalid();if(seen.has(item as object))invalid();seen.add(item as object);
    if(!Array.isArray(item)&&![Object.prototype,null].includes(Object.getPrototypeOf(item)))invalid();
    const descriptors=Object.getOwnPropertyDescriptors(item);
    for(const [key,descriptor]of Object.entries(descriptors)){
      if(Array.isArray(item)&&key==='length')continue;
      if(!descriptor.enumerable||!Object.hasOwn(descriptor,'value'))invalid();pending.push([descriptor.value,depth+1]);
    }
    if(Object.getOwnPropertySymbols(item).length)invalid();
  }
  if(Buffer.byteLength(JSON.stringify(value))>65536)invalid();
}

// This validates a bounded source-state description, not its provenance,
// completeness against Kafka, or an independently anchored latest-state chain.
export function parseRecoverySourceState(input:unknown,options:{now?:number;maxAgeSeconds?:number}={}):RecoverySourceState{
  const now=options.now??Date.now(),maxAge=options.maxAgeSeconds??600;
  if(!Number.isSafeInteger(now)||now<0||!Number.isSafeInteger(maxAge)||maxAge<1||maxAge>=3600)throw new RecoveryStateFormatError('state_format_options_invalid');
  shape(input,['version','status','source','exported_at','deletion','integrity','kafka_observation_verified','latest_state_verified']);
  if(input.version!==1||input.status!=='exported_unverified'||input.kafka_observation_verified!==false||input.latest_state_verified!==false||!iso(input.exported_at))invalid();
  const exportedAt=Date.parse(input.exported_at as string);
  if(exportedAt>now+300000||now-exportedAt>maxAge*1000)throw new RecoveryStateFormatError('state_not_fresh');
  shape(input.source,['id','database','schema_version','authenticated']);
  const source=input.source;
  if(typeof source.id!=='string'||!/^[A-Za-z0-9_-]{1,128}$/.test(source.id)||typeof source.database!=='string'||!source.database.length||Buffer.byteLength(source.database)>63||/[\x00-\x1f\x7f]/.test(source.database)||source.schema_version!==22||source.authenticated!==false)invalid();
  let deletion:DeletionLedger;
  try{deletion=parseDeletionLedger(input.deletion,now);}catch{invalid();}
  if(deletion!.schema_version!==2||deletion!.exported_at!==input.exported_at)invalid();
  shape(input.integrity,['blocked','reason','detected_at','details','audit_high_water','streams']);
  const integrity=input.integrity;
  if(typeof integrity.blocked!=='boolean'||(integrity.blocked?!['retained_offset_gap','consumer_offset_unknown'].includes(integrity.reason as string):integrity.reason!==null)||(integrity.detected_at!==null&&!iso(integrity.detected_at))||(integrity.blocked&&integrity.detected_at===null)||!offset(integrity.audit_high_water))invalid();
  jsonDetails(integrity.details);
  if(!Array.isArray(integrity.streams)||integrity.streams.length>10000)invalid();
  const streams=new Set<string>();let positions=0;
  for(const stream of integrity.streams as unknown[]){
    shape(stream,['topic','group_id','partition_count','initialized_at','positions']);
    if(typeof stream.topic!=='string'||!/^[A-Za-z0-9._-]{1,249}$/.test(stream.topic)||['.','..'].includes(stream.topic)||typeof stream.group_id!=='string'||!/^[A-Za-z0-9._-]{1,128}$/.test(stream.group_id)||!Number.isSafeInteger(stream.partition_count)||(stream.partition_count as number)<1||(stream.partition_count as number)>1024||!iso(stream.initialized_at)||!Array.isArray(stream.positions)||stream.positions.length!==stream.partition_count)invalid();
    const key=stream.topic+'\0'+stream.group_id;if(streams.has(key))invalid();streams.add(key);
    const partitions=new Set<number>();positions+=(stream.positions as unknown[]).length;if(positions>10000)invalid();
    for(const position of stream.positions as unknown[]){
      shape(position,['partition','next_offset','updated_at']);
      if(!Number.isSafeInteger(position.partition)||(position.partition as number)<0||(position.partition as number)>=(stream.partition_count as number)||partitions.has(position.partition as number)||!offset(position.next_offset)||!iso(position.updated_at))invalid();
      partitions.add(position.partition as number);
    }
  }
  // All structures have already been restricted to JSON values. Keep offset
  // strings untouched; never round them via Number during parsing or return.
  if(Buffer.byteLength(JSON.stringify(input))>16*1024*1024)invalid();
  return{...(input as unknown as RecoverySourceState),deletion:deletion! as RecoverySourceState['deletion']};
}
