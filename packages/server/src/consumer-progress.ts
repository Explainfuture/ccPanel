import { pool } from './db';
import { storageSafeText } from '../../schema/src/index';
import { recoveryLock } from './recovery-gate';

export interface ConsumerStream {topic:string;group:string}
export interface ConsumerPosition {partition:number;low:string;high:string;committed:string|null}
export interface ProgressRow {partition:number;next_offset:string}
export class ConsumerProgressError extends Error {
  readonly retriable=false;
  constructor(public reason:'retained_offset_gap'|'consumer_offset_unknown',public kind:string,public details:Record<string,unknown>={}){super(kind);this.name='ConsumerProgressError';}
}
const fail=(kind:string,details:Record<string,unknown>={},reason:'retained_offset_gap'|'consumer_offset_unknown'='consumer_offset_unknown'):never=>{throw new ConsumerProgressError(reason,kind,details);};
const max=9223372036854775807n;
export function progressOffset(value:unknown):bigint{
  if(typeof value!=='string'||!/^(0|[1-9]\d{0,18})$/.test(value)||BigInt(value)>max)return fail('invalid_progress_offset');
  return BigInt(value);
}
export function progressPlan(positions:ConsumerPosition[],knownCount:number|null,rows:ProgressRow[],operator=false){
  if(!Array.isArray(positions)||positions.length<1||positions.length>1024||(knownCount!==null&&(!Number.isSafeInteger(knownCount)||knownCount<1||knownCount>positions.length)))return fail('invalid_progress_topology');
  const sorted=[...positions].sort((a,b)=>a.partition-b.partition),cursors=new Map<number,string>();
  for(const row of rows){if(!Number.isSafeInteger(row.partition)||row.partition<0||cursors.has(row.partition))return fail('invalid_progress_rows');progressOffset(row.next_offset);cursors.set(row.partition,row.next_offset);}
  if(knownCount===null&&rows.length)return fail('progress_stream_missing');
  if(rows.some(row=>row.partition>=positions.length))return fail('progress_partition_removed');
  const initialize:ProgressRow[]=[];
  for(let index=0;index<sorted.length;index++){
    const p=sorted[index]!;if(p.partition!==index)return fail('invalid_progress_topology');
    const low=progressOffset(p.low),high=progressOffset(p.high),commit=p.committed===null?null:progressOffset(p.committed);
    if(low>high||(commit!==null&&commit>high))return fail('broker_history_regressed',{partition:p.partition,low:p.low,high:p.high,committed:p.committed});
    const value=cursors.get(p.partition);
    if(value!==undefined){
      const next=progressOffset(value);
      if(next<low)return fail('progress_before_retained_log',{partition:p.partition,expected:value,low:p.low,high:p.high},'retained_offset_gap');
      if(next>high)return fail('progress_after_log',{partition:p.partition,expected:value,high:p.high});
      if(commit!==null&&commit>next)return fail('broker_ahead_of_database',{partition:p.partition,expected:value,committed:p.committed});
    }else{
      if(knownCount!==null&&p.partition<knownCount)return fail('progress_partition_missing',{partition:p.partition});
      if(low>0n&&(commit===null||commit<low))return fail('bootstrap_history_unknown',{partition:p.partition,low:p.low,committed:p.committed});
      if(commit!==null&&commit>0n&&!operator)return fail('progress_bootstrap_required',{partition:p.partition,committed:p.committed});
      initialize.push({partition:p.partition,next_offset:(commit??0n).toString()});
    }
  }
  return {initialize,partitionCount:sorted.length,create:knownCount===null};
}
function validStream(stream:ConsumerStream){
  if(!/^[A-Za-z0-9._-]{1,249}$/.test(stream.topic)||['.','..'].includes(stream.topic)||!/^[A-Za-z0-9._-]{1,128}$/.test(stream.group))return fail('invalid_progress_stream');
}
type Run=(sql:string,params:unknown[])=>Promise<{rowCount:number|null}>;
async function pauseUsing(run:Run,stream:ConsumerStream,error:ConsumerProgressError){
  validStream(stream);
  const details=JSON.stringify({...error.details,topic:stream.topic,group:stream.group,kind:error.kind});
  const result=await run("WITH changed AS (UPDATE ingest_integrity_gate SET blocked=true,reason=$1,detected_at=clock_timestamp(),details=$2::jsonb WHERE id=true AND NOT blocked RETURNING id) INSERT INTO consumer_progress_audit(topic,group_id,action,reason,details) SELECT $3,$4,'paused',$5,$2::jsonb FROM changed RETURNING id",[error.reason,details,stream.topic,stream.group,error.kind]);
  if(result.rowCount!==1&&(await run('SELECT id FROM ingest_integrity_gate WHERE id=true AND blocked',[])).rowCount!==1)throw new Error('integrity_control_missing');
}
export async function pauseForProgress(stream:ConsumerStream,error:ConsumerProgressError){return pauseUsing((sql,params)=>pool.query(sql,params),stream,error);}
export async function ensureConsumerProgress(stream:ConsumerStream,positions:ConsumerPosition[],operatorReason?:string){
  validStream(stream);
  if(operatorReason!==undefined&&(operatorReason.trim().length<5||operatorReason.length>500||!storageSafeText(operatorReason)))throw new Error('invalid_progress_reason');
  if(operatorReason!==undefined&&process.env.CCPANEL_OFFLINE_RESTORE!=='true')throw new Error('offline_progress_bootstrap_required');
  const db=await pool.connect();
  try{
    await db.query('BEGIN');
    if(operatorReason!==undefined&&!(await db.query('SELECT pg_try_advisory_xact_lock($1) AS held',[recoveryLock])).rows[0].held)throw new Error('services_still_online');
    await db.query('SELECT pg_advisory_xact_lock(hashtextextended($1,0))',['ccpanel:consumer-progress:'+stream.group+':'+stream.topic]);
    const state=(await db.query('SELECT partition_count FROM consumer_streams WHERE topic=$1 AND group_id=$2',[stream.topic,stream.group])).rows[0];
    const rows=await db.query('SELECT partition_id AS partition,next_offset::text FROM consumer_progress WHERE topic=$1 AND group_id=$2',[stream.topic,stream.group]);
    const plan=progressPlan(positions,state?.partition_count??null,rows.rows,operatorReason!==undefined);
    if(plan.create)await db.query('INSERT INTO consumer_streams(topic,group_id,partition_count) VALUES($1,$2,$3)',[stream.topic,stream.group,plan.partitionCount]);
    for(const row of plan.initialize)await db.query('INSERT INTO consumer_progress(topic,group_id,partition_id,next_offset) VALUES($1,$2,$3,$4)',[stream.topic,stream.group,row.partition,row.next_offset]);
    if(plan.partitionCount!==state?.partition_count)await db.query('UPDATE consumer_streams SET partition_count=$3 WHERE topic=$1 AND group_id=$2',[stream.topic,stream.group,plan.partitionCount]);
    if(plan.create||plan.initialize.length)await db.query('INSERT INTO consumer_progress_audit(topic,group_id,action,reason,details) VALUES($1,$2,$3,$4,$5)',[stream.topic,stream.group,operatorReason?'operator_bootstrap':'new_partitions',operatorReason??'New retained stream begins at offset zero',JSON.stringify({positions,initialized:plan.initialize})]);
    await db.query('COMMIT');return plan;
  }catch(error){await db.query('ROLLBACK');if(error instanceof ConsumerProgressError)await pauseUsing((sql,params)=>db.query(sql,params),stream,error);throw error;}finally{db.release();}
}
export async function withConsumerProgress(stream:ConsumerStream,partition:number,offset:string,process:()=>Promise<boolean>):Promise<boolean>{
  validStream(stream);const current=progressOffset(offset);if(current===max||!Number.isSafeInteger(partition)||partition<0)return fail('invalid_progress_message');
  const db=await pool.connect();
  try{
    await db.query('BEGIN');
    const control=(await db.query('SELECT blocked,reason FROM ingest_integrity_gate WHERE id=true')).rows[0];
    if(!control||control.blocked)return fail('integrity_paused',{},control?.reason==='retained_offset_gap'?'retained_offset_gap':'consumer_offset_unknown');
    const row=(await db.query('SELECT next_offset::text FROM consumer_progress WHERE topic=$1 AND group_id=$2 AND partition_id=$3 FOR UPDATE',[stream.topic,stream.group,partition])).rows[0];
    if(!row)return fail('progress_partition_missing',{partition});
    const expected=progressOffset(row.next_offset);
    if(current>expected)return fail('consumer_progress_jump',{partition,expected:row.next_offset,observed:offset});
    // Raw writes / DLQ index transactions are committed by the callback first.
    // The independent checkpoint then commits before the Kafka group offset.
    // A crash before this commit replays; a crash after it cannot double count.
    if(!await process()){await db.query('ROLLBACK');return false;}
    await db.query('UPDATE consumer_progress SET next_offset=GREATEST(next_offset,$4::bigint),updated_at=clock_timestamp() WHERE topic=$1 AND group_id=$2 AND partition_id=$3',[stream.topic,stream.group,partition,(current+1n).toString()]);
    await db.query('COMMIT');return true;
  }catch(error){await db.query('ROLLBACK');if(error instanceof ConsumerProgressError)await pauseUsing((sql,params)=>db.query(sql,params),stream,error);throw error;}finally{db.release();}
}
