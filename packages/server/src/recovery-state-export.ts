import pg from 'pg';
import {readDeletionLedgerSnapshot} from './deletion-ledger-export';
import {parseRecoverySourceState,RecoveryStateFormatError,type RecoverySourceState} from './recovery-state-format';

export class RecoveryStateExportError extends Error{constructor(public code:string){super(code);this.name='RecoveryStateExportError';}}
export interface StateExportOptions{connectionString:string;sourceId:string;expectedSchema:number;timeoutMs?:number;maxAgeSeconds?:number}
// Same session key as recovery-gate.ts. Do not import that live-service module:
// this exporter must not initialize its global DATABASE_URL pool.
const recoveryLock=169202617;
// Current source metadata only. No recovery-state import, checkpoint fast
// forward, authentication, chain anchor or recovery-gate update is performed.
export async function exportRecoverySourceState(options:StateExportOptions):Promise<RecoverySourceState>{
  const fail=(code:string):never=>{throw new RecoveryStateExportError(code);};
  const timeout=options.timeoutMs??30000,age=options.maxAgeSeconds??600;
  if(typeof options.connectionString!=='string'||!options.connectionString||typeof options.sourceId!=='string'||!/^[A-Za-z0-9_-]{1,128}$/.test(options.sourceId)||!Number.isSafeInteger(options.expectedSchema)||options.expectedSchema<1||!Number.isSafeInteger(timeout)||timeout<100||timeout>120000||!Number.isSafeInteger(age)||age<1||age>=3600)fail('state_export_options_invalid');
  const db=new pg.Client({connectionString:options.connectionString,application_name:'ccpanel_backup_state_export',options:'-c timezone=UTC',connectionTimeoutMillis:Math.min(timeout,5000),statement_timeout:Math.min(timeout,10000)});
  const deadline=performance.now()+timeout;let stopped:RecoveryStateExportError|undefined,ending:Promise<void>|undefined;
  const check=()=>{if(stopped)throw stopped;if(performance.now()>=deadline)fail('state_export_timeout');};
  const timer=setTimeout(()=>{stopped??=new RecoveryStateExportError('state_export_timeout');ending??=db.end();void ending.catch(()=>{});},timeout);
  db.on('error',()=>{stopped??=new RecoveryStateExportError('state_export_connection_lost');});
  try{
    await db.connect();check();
    // Serialize with managed schema changes and offline recovery before taking
    // the RR snapshot. Live service leases are shared and remain compatible.
    for(const lock of [169202610,recoveryLock])if((await db.query('SELECT pg_try_advisory_lock_shared($1) AS held',[lock])).rows[0]?.held!==true)fail('state_export_lock_busy');
    await db.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
    const header=(await db.query('SELECT current_database() AS database,transaction_timestamp() AS exported_at,(SELECT max(version)::int FROM schema_migrations) AS schema_version')).rows[0];
    if(header.schema_version!==22||header.schema_version!==options.expectedSchema)fail('state_export_schema_mismatch');
    const gate=(await db.query('SELECT blocked FROM recovery_gate WHERE id=true')).rows;
    if(gate.length!==1||gate[0].blocked!==false)fail('state_export_source_not_ready');
    const deletion=await readDeletionLedgerSnapshot(db);check();
    if(deletion.schema_version!==2)fail('state_export_schema_mismatch');
    // Read topology before gate/progress, all in the same MVCC snapshot. This
    // exports PG knowledge; broker retained ranges are explicitly not observed.
    const streams=(await db.query('SELECT topic,group_id,partition_count,initialized_at FROM consumer_streams ORDER BY topic,group_id LIMIT 10001')).rows;
    const positions=(await db.query('SELECT topic,group_id,partition_id AS partition,next_offset::text,updated_at FROM consumer_progress ORDER BY topic,group_id,partition_id LIMIT 10001')).rows;
    if(streams.length>10000||positions.length>10000)fail('state_export_budget_exceeded');
    const integrity=(await db.query("SELECT blocked,reason,detected_at,CASE WHEN octet_length(details::text)<=65536 THEN details ELSE NULL END AS details FROM ingest_integrity_gate WHERE id=true")).rows;
    if(integrity.length!==1||integrity[0].details===null)fail('state_export_integrity_invalid');
    const high=(await db.query('SELECT coalesce(max(id),0)::text AS value FROM consumer_progress_audit')).rows[0].value;
    const grouped=new Map<string,typeof positions>();
    for(const row of positions){const key=JSON.stringify([row.topic,row.group_id]);const values=grouped.get(key)??[];values.push(row);grouped.set(key,values);}
    let count=0;
    const normalizedStreams=streams.map(row=>{
      const values=grouped.get(JSON.stringify([row.topic,row.group_id]))??[];count+=values.length;
      return{topic:row.topic,group_id:row.group_id,partition_count:row.partition_count,initialized_at:row.initialized_at.toISOString(),positions:values.map(p=>({partition:p.partition,next_offset:p.next_offset,updated_at:p.updated_at.toISOString()}))};
    });
    if(count!==positions.length)fail('state_export_integrity_invalid');
    const incident=integrity[0];
    const state=parseRecoverySourceState({version:1,status:'exported_unverified',source:{id:options.sourceId,database:header.database,schema_version:header.schema_version,authenticated:false},exported_at:header.exported_at.toISOString(),deletion,integrity:{blocked:incident.blocked,reason:incident.reason,detected_at:incident.detected_at?.toISOString()??null,details:incident.details,audit_high_water:high,streams:normalizedStreams},kafka_observation_verified:false,latest_state_verified:false},{now:Date.now(),maxAgeSeconds:age});
    check();await db.query('ROLLBACK');check();return state;
  }catch(error){
    if(stopped)throw stopped;
    if(error instanceof RecoveryStateExportError)throw error;
    if(error instanceof RecoveryStateFormatError)throw new RecoveryStateExportError(error.code);
    if(error instanceof Error&&error.message==='deletion_ledger_budget_exceeded')throw new RecoveryStateExportError('state_export_budget_exceeded');
    throw new RecoveryStateExportError('state_export_failed');
  }finally{
    clearTimeout(timer);ending??=db.end();try{await ending;}catch{throw new RecoveryStateExportError('state_export_cleanup_failed');}
  }
}
