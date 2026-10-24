import pg from 'pg';

export class BackupSnapshotError extends Error {
  constructor(public code:string){super(code);this.name='BackupSnapshotError';}
}
export interface BackupSnapshot {
  readonly snapshotId:string;
  readonly sourceDatabase:string;
  readonly schemaVersion:number;
  readonly fingerprints:readonly string[];
  readonly signal:AbortSignal;
}
// Same explicit relation scope and SQL representation as backup-fingerprint.sql.
// Keep the bigint hash in PostgreSQL text, never round it through JSON numbers.
// Callers use UTC sessions (as withBackupSnapshot does).
export async function readBackupFingerprints(db:pg.Client):Promise<readonly string[]>{
  const tables=(await db.query("SELECT schemaname,tablename FROM pg_tables WHERE schemaname='public' AND tablename NOT IN ('recovery_gate','recovery_import_scopes') ORDER BY tablename")).rows;
  if(tables.length<1||tables.length>1024)throw new BackupSnapshotError('backup_relation_scope_invalid');
  const identifier=(value:string)=>'"'+value.replace(/"/g,'""')+'"';
  const result:string[]=[];
  for(const table of tables){
    const relation=identifier(table.schemaname)+'.'+identifier(table.tablename);
    const row=(await db.query(`SELECT json_build_array($1::text,count(*),coalesce(bit_xor(hashtextextended(row_to_json(t)::text,0)),0))::text AS fingerprint FROM ${relation} t`,[table.schemaname+'.'+table.tablename])).rows[0];
    result.push(row.fingerprint);
  }
  return Object.freeze(result);
}
// Callback must use --snapshot=snapshotId on pg_dump and honor signal when
// running external processes. No publication/transfer is performed here.
export async function withBackupSnapshot<T>(connectionString:string,options:{expectedSchema:number;timeoutMs?:number},consume:(snapshot:BackupSnapshot)=>Promise<T>):Promise<T>{
  const timeout=options.timeoutMs??120000;
  if(!Number.isSafeInteger(options.expectedSchema)||options.expectedSchema<1||!Number.isSafeInteger(timeout)||timeout<1000||timeout>1800000)throw new BackupSnapshotError('backup_snapshot_options_invalid');
  const controller=new AbortController();
  const db=new pg.Client({connectionString,connectionTimeoutMillis:Math.min(timeout,5000),statement_timeout:Math.min(timeout,10000),application_name:'ccpanel_backup_snapshot',options:'-c timezone=UTC'});
  let interrupt!:(error:Error)=>void;
  const stopped=new Promise<never>((_resolve,reject)=>{interrupt=reject;});void stopped.catch(()=>{});
  const stop=(code:string)=>{interrupt(new BackupSnapshotError(code));controller.abort();};
  db.on('error',()=>stop('backup_snapshot_connection_lost'));
  const timer=setTimeout(()=>stop('backup_snapshot_timeout'),timeout);
  let pending:Promise<T>|undefined;
  try{
    const operation=async()=>{
      await db.connect();
      // migrate() and maintainPartitions() take exclusive key 169202610.
      // Take a session shared lease BEFORE any repeatable-read snapshot: waiting
      // inside the transaction could otherwise retain pre-migration catalogs.
      if(!(await db.query('SELECT pg_try_advisory_lock_shared(169202610) AS held')).rows[0].held)throw new BackupSnapshotError('backup_managed_ddl_busy');
      await db.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
      await db.query("SELECT set_config('idle_in_transaction_session_timeout',$1,true)",[String(timeout)]);
      const version=(await db.query('SELECT max(version) AS version FROM schema_migrations')).rows[0]?.version;
      if(version!==options.expectedSchema)throw new BackupSnapshotError('backup_schema_mismatch');
      const row=(await db.query('SELECT pg_export_snapshot() AS snapshot,current_database() AS database')).rows[0];
      if(typeof row.snapshot!=='string'||!/^[0-9A-Fa-f-]{1,128}$/.test(row.snapshot))throw new BackupSnapshotError('backup_snapshot_id_invalid');
      const fingerprints=await readBackupFingerprints(db);
      if(controller.signal.aborted)throw new BackupSnapshotError('backup_snapshot_interrupted');
      const result=await consume(Object.freeze({snapshotId:row.snapshot,sourceDatabase:row.database,schemaVersion:version,fingerprints,signal:controller.signal}));
      if(controller.signal.aborted)throw new BackupSnapshotError('backup_snapshot_interrupted');
      // Prove exporter still owns a live transaction before returning success.
      await db.query('SELECT 1');await db.query('ROLLBACK');return result;
    };
    pending=operation();return await Promise.race([pending,stopped]);
  }catch(error){
    if(error instanceof BackupSnapshotError)throw error;
    throw new BackupSnapshotError('backup_snapshot_failed');
  }finally{
    clearTimeout(timer);controller.abort();await db.end().catch(()=>{});
    // Cancellation requests are bounded, but ownership is retained until the
    // callback has actually settled (e.g. its archive writer has closed).
    await pending?.catch(()=>{});
  }
}
