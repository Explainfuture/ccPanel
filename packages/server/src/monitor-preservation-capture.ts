import pg from 'pg';
import {constants} from 'node:fs';
import {open,lstat,realpath} from 'node:fs/promises';
import {isAbsolute,resolve,dirname,basename} from 'node:path';
import {createHash} from 'node:crypto';

export class MonitorCaptureError extends Error{constructor(public code:string){super(code);this.name='MonitorCaptureError';}}
export interface MonitorCaptureOptions{connectionString:string;output:string;sourceLabel:string;expectedSchema:number;timeoutMs?:number;signal?:AbortSignal}
const tables=['operational_samples','operational_alert_history','pressure_signal_history','dlq_alert_history'] as const;
const limits=[50000,5000,5000,5000] as const;
type DescriptorRow={source?:string;id:string;cursor_time:string;policy_us:string;row_sha256:string};
const int64=(value:unknown):value is string=>typeof value==='string'&&/^(0|-?[1-9][0-9]{0,18})$/.test(value)&&BigInt(value)>=-9223372036854775808n&&BigInt(value)<=9223372036854775807n;
const positive=(value:unknown):value is string=>int64(value)&&BigInt(value)>0n;
const uuid=(value:unknown):value is string=>typeof value==='string'&&/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/.test(value);

// Descriptor capture only. The caller must bind the source connection/container;
// row hashes and this caller label do not establish origin or causal pruning.
export async function captureMonitorPreservation(options:MonitorCaptureOptions){
  const fail=(code:string):never=>{throw new MonitorCaptureError(code);};
  const timeout=options.timeoutMs??30000;
  if(!Number.isSafeInteger(timeout)||timeout<100||timeout>120000||options.expectedSchema!==22||typeof options.sourceLabel!=='string'||!/^[A-Za-z0-9_-]{1,128}$/.test(options.sourceLabel)||typeof options.output!=='string'||!isAbsolute(options.output))fail('monitor_capture_options_invalid');
  let uri:URL;try{uri=new URL(options.connectionString);}catch{fail('monitor_capture_connection_invalid');}
  if(!['postgres:','postgresql:'].includes(uri!.protocol)||!uri!.hostname||!uri!.username||!uri!.pathname.slice(1)||uri!.hash||[...uri!.searchParams.keys()].some(key=>key!=='sslmode')||uri!.searchParams.getAll('sslmode').length>1)fail('monitor_capture_connection_invalid');
  const ssl=uri!.searchParams.get('sslmode');if(ssl&&!['disable','allow','prefer','require','verify-ca','verify-full'].includes(ssl))fail('monitor_capture_connection_invalid');
  if(options.signal?.aborted)fail('monitor_capture_aborted');
  if(process.platform==='win32')fail('monitor_capture_permissions_unverified');
  const output=resolve(options.output),parent=dirname(output);
  if(!/^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\.jsonl$/.test(basename(output)))fail('monitor_capture_options_invalid');
  const initial=await lstat(parent);
  if(!initial.isDirectory()||initial.isSymbolicLink()||initial.uid!==process.geteuid?.()||(initial.mode&0o077)!==0||await realpath(parent)!==parent)fail('monitor_capture_directory_unsafe');
  // A local socket close alone does not end a backend blocked on a relation
  // lock. Ask this session to check for kernel-reported disconnects while a
  // query runs; no global setting or other application session is changed.
  const db=new pg.Client({connectionString:options.connectionString,application_name:'ccpanel_monitor_preservation_capture',options:'-c timezone=UTC -c client_connection_check_interval=100ms',connectionTimeoutMillis:Math.min(timeout,5000),statement_timeout:Math.min(timeout,5000)});
  let ending:Promise<void>|undefined,stopped:MonitorCaptureError|undefined,file:Awaited<ReturnType<typeof open>>|undefined,footerAttempted=false;
  const deadline=performance.now()+timeout;
  const stop=(code:string)=>{stopped??=new MonitorCaptureError(code);ending??=db.end();void ending.catch(()=>{});};
  const check=()=>{if(stopped)throw stopped;if(options.signal?.aborted)fail('monitor_capture_aborted');if(performance.now()>=deadline)fail('monitor_capture_timeout');};
  const aborted=()=>stop('monitor_capture_aborted');options.signal?.addEventListener('abort',aborted,{once:true});
  const timer=setTimeout(()=>stop('monitor_capture_timeout'),timeout);db.on('error',()=>{stopped??=new MonitorCaptureError('monitor_capture_connection_lost');});
  const prefix=createHash('sha256'),complete=createHash('sha256'),counts=Object.fromEntries(tables.map(name=>[name,0])) as Record<typeof tables[number],number>;
  let rows=0,bytes=0;
  try{
    check();file=await open(output,constants.O_WRONLY|constants.O_CREAT|constants.O_EXCL|(constants.O_NOFOLLOW??0),0o600);
    const write=async(value:unknown,hash=true)=>{
      check();const data=Buffer.from(JSON.stringify(value)+'\n');if(data.length>4096||bytes+data.length>32*1024*1024)fail('monitor_capture_budget_exceeded');
      let at=0;while(at<data.length){check();const result=await file!.write(data,at,data.length-at,null);if(!result.bytesWritten)fail('monitor_capture_write_failed');at+=result.bytesWritten;}
      bytes+=data.length;complete.update(data);if(hash)prefix.update(data);check();
    };
    await db.connect();check();
    if((await db.query('SHOW client_connection_check_interval')).rows[0]?.client_connection_check_interval!=='100ms')fail('monitor_capture_session_unverified');
    // Same compatible leases as source recovery-state export; acquire BEFORE
    // the snapshot to avoid waiting while retaining pre-migration catalog data.
    for(const lock of [169202610,169202617])if((await db.query('SELECT pg_try_advisory_lock_shared($1) AS held',[lock])).rows[0]?.held!==true)fail('monitor_capture_lock_busy');
    await db.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
    const metadata=(await db.query(`SELECT current_database() AS database,
      (SELECT oid::text FROM pg_database WHERE datname=current_database()) AS oid,
      (SELECT max(version)::int FROM schema_migrations) AS schema_version,
      current_setting('server_version_num') AS server_version_num,
      ((extract(epoch FROM pg_postmaster_start_time())*1000000)::bigint)::text AS postmaster_started_us,
      ((extract(epoch FROM transaction_timestamp())*1000000)::bigint)::text AS snapshot_started_us,
      ((extract(epoch FROM clock_timestamp())*1000000)::bigint)::text AS captured_us`)).rows[0];
    if(metadata.schema_version!==22)fail('monitor_capture_schema_mismatch');
    if(typeof metadata.database!=='string'||!metadata.database||Buffer.byteLength(metadata.database)>63||/[\x00-\x1f\x7f]/.test(metadata.database)||![metadata.oid,metadata.server_version_num,metadata.postmaster_started_us,metadata.snapshot_started_us,metadata.captured_us].every(positive)||BigInt(metadata.postmaster_started_us)>BigInt(metadata.snapshot_started_us)||BigInt(metadata.snapshot_started_us)>BigInt(metadata.captured_us))fail('monitor_capture_metadata_invalid');
    const sourceOrder=(await db.query("SELECT array_agg(source ORDER BY source) AS ordered FROM (VALUES('api'::text),('host'),('worker')) s(source)")).rows[0].ordered;
    if(JSON.stringify(sourceOrder)!==JSON.stringify(['api','host','worker']))fail('monitor_capture_collation_unsupported');
    const gate=(await db.query('SELECT blocked FROM recovery_gate WHERE id=true')).rows;
    if(gate.length!==1||gate[0].blocked!==false)fail('monitor_capture_source_not_ready');
    const expected:Record<string,number>={};
    for(const [index,table]of tables.entries()){
      const count=(await db.query('SELECT count(*)::text AS count FROM '+table)).rows[0].count;
      if(!int64(count)||BigInt(count)<0n||BigInt(count)>BigInt(limits[index]!))fail('monitor_capture_inventory_exceeded');expected[table]=Number(count);check();
    }
    await write({kind:'header',version:1,database:{name:metadata.database,oid:metadata.oid},schema_version:22,server_version_num:metadata.server_version_num,postmaster_started_us:metadata.postmaster_started_us,snapshot_started_us:metadata.snapshot_started_us,captured_us:metadata.captured_us,source_label:options.sourceLabel});
    for(const [index,table]of tables.entries()){
      let cursorTime:string|null=null,cursorSource:string|null=null,cursorId:string|null=null;
      while(true){
        check();const samples=table==='operational_samples',time=samples?'observed_at':table==='operational_alert_history'?'evaluated_at':'assessed_at';
        const page:DescriptorRow[]=(await db.query(samples?`SELECT source,instance_id::text AS id,observed_at::text AS cursor_time,
          ((extract(epoch FROM observed_at)*1000000)::bigint)::text AS policy_us,
          encode(sha256(convert_to(row_to_json(t)::text,'UTF8')),'hex') AS row_sha256
          FROM operational_samples t WHERE $1::timestamptz IS NULL OR (observed_at,source,instance_id)>($1::timestamptz,$2::text,$3::uuid)
          ORDER BY observed_at,source,instance_id LIMIT 250`:`SELECT id::text AS id,${time}::text AS cursor_time,
          ((extract(epoch FROM ${time})*1000000)::bigint)::text AS policy_us,
          encode(sha256(convert_to(row_to_json(t)::text,'UTF8')),'hex') AS row_sha256
          FROM ${table} t WHERE $1::timestamptz IS NULL OR (${time},id)>($1::timestamptz,$2::bigint)
          ORDER BY ${time},id LIMIT 250`,samples?[cursorTime,cursorSource,cursorId]:[cursorTime,cursorId])).rows;
        if(!page.length)break;
        for(const row of page){
          if(!int64(row.policy_us)||typeof row.cursor_time!=='string'||!row.cursor_time||typeof row.row_sha256!=='string'||!/^[a-f0-9]{64}$/.test(row.row_sha256)||(samples?(!['api','host','worker'].includes(row.source??'')||!uuid(row.id)):!positive(row.id)))fail('monitor_capture_row_invalid');
          counts[table]++;rows++;if(counts[table]>limits[index]!||rows>100000)fail('monitor_capture_budget_exceeded');
          await write({kind:'row',table,pk:samples?[row.source,row.id,row.policy_us]:[row.id],policy_us:row.policy_us,row_sha256:row.row_sha256});
          cursorTime=row.cursor_time;cursorSource=samples?row.source!:null;cursorId=row.id;
        }
        if(page.length<250)break;
      }
      if(counts[table]!==expected[table])fail('monitor_capture_count_mismatch');
    }
    check();await db.query('ROLLBACK');check();ending??=db.end();await ending;check();
    // All rows and the completed read-only transaction are observed before a
    // footer can make the private output independently parseable.
    await file.sync();check();const prefixSha256=prefix.digest('hex');footerAttempted=true;
    await write({kind:'footer',version:1,rows,counts,prefix_sha256:prefixSha256},false);await file.sync();check();
    const current=await file.stat(),path=await lstat(output),directory=await lstat(parent);
    if(!current.isFile()||current.size!==bytes||current.uid!==process.geteuid?.()||(current.mode&0o077)!==0||current.nlink!==1||!path.isFile()||path.isSymbolicLink()||path.uid!==current.uid||(path.mode&0o077)!==0||path.nlink!==1||path.dev!==current.dev||path.ino!==current.ino||!directory.isDirectory()||directory.isSymbolicLink()||directory.uid!==process.geteuid?.()||(directory.mode&0o077)!==0||directory.dev!==initial.dev||directory.ino!==initial.ino||await realpath(parent)!==parent)fail('monitor_capture_output_changed');
    await file.close();file=undefined;const parentFile=await open(parent,constants.O_RDONLY|(constants.O_DIRECTORY??0));try{await parentFile.sync();}finally{await parentFile.close();}check();
    return{status:'captured_unverified' as const,rows,counts,bytes,prefixSha256,sha256:complete.digest('hex'),sourceAuthenticated:false,causalPruneVerified:false,continuousAvailabilityVerified:false};
  }catch(error){
    if(footerAttempted)throw new MonitorCaptureError('monitor_capture_publish_uncertain');
    if(stopped)throw stopped;if(error instanceof MonitorCaptureError)throw error;
    if((error as NodeJS.ErrnoException).code==='EEXIST')throw new MonitorCaptureError('monitor_capture_output_exists');
    throw new MonitorCaptureError('monitor_capture_failed');
  }finally{
    clearTimeout(timer);options.signal?.removeEventListener('abort',aborted);
    let failed=false;try{ending??=db.end();await ending;}catch{failed=true;}try{await file?.close();}catch{failed=true;}
    if(failed)throw new MonitorCaptureError(footerAttempted?'monitor_capture_publish_uncertain':'monitor_capture_cleanup_failed');
  }
}
