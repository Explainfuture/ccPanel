// CI host acceptance: requires Python3 for offline preservation CLI checks.
import assert from 'node:assert/strict';
import {spawn,spawnSync} from 'node:child_process';
import {randomUUID,createHash} from 'node:crypto';
import {mkdtemp,writeFile,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {resolve,join,dirname,basename} from 'node:path';
import {pool} from '../packages/server/src/db';
import {persist} from '../packages/server/src/consume';
import {aggregateNext} from '../packages/server/src/aggregations';
import {IntakeMetrics} from '../packages/server/src/intake-metrics';
import {WorkerMetrics} from '../packages/server/src/worker-metrics';
import {storeOperationalSnapshot} from '../packages/server/src/operational-history';
import {evaluateAndStoreOperationalAlerts} from '../packages/server/src/operational-alert-store';
const parent=resolve(tmpdir()),dir=await mkdtemp(join(parent,'ccpanel-capacity-proof-')),file=join(dir,'result.json');
let fixture:Record<string,string>|undefined;
function child(mode:string){return spawnSync(process.execPath,['scripts/load-fixture.mjs',mode,...(mode==='verify'?[file]:[])],{input:fixture?JSON.stringify(fixture):'',encoding:'utf8',timeout:30000,maxBuffer:2*1024*1024});}
const rejectedDurations:number[]=[];
function rejectVerify(message:string){const started=performance.now(),rejected=child('verify');rejectedDurations.push(Math.ceil(performance.now()-started));assert.equal(rejected.status,1);assert.ok(rejected.stderr.includes(message),'invalid fixture must fail its actual reconciliation check');}
async function aggregate(){const db=await pool.connect();try{assert.equal(await aggregateNext(db),'completed');assert.ok(['completed','empty'].includes(await aggregateNext(db)),'second bucket may already be processed');}finally{db.release();}}
async function verifyAfterAggregate(){
  const appname='ccpanel_capacity_wait_'+randomUUID(),proc=spawn(process.execPath,['scripts/load-fixture.mjs','verify',file],{env:{...process.env,PGAPPNAME:appname},stdio:['pipe','pipe','pipe']});
  let stdout='',stderr='',closed=false,overflow=false,size=0;
  const watchdog=setTimeout(()=>proc.kill('SIGKILL'),30000);
  const done=new Promise<{status:number|null;stdout:string;stderr:string}>(resolve=>{
    const collect=(out:boolean)=>(data:Buffer)=>{size+=data.length;if(size>2*1024*1024){overflow=true;proc.kill('SIGKILL');return;}if(out)stdout+=data.toString();else stderr+=data.toString();};
    proc.stdout.on('data',collect(true));proc.stderr.on('data',collect(false));proc.stdin.on('error',()=>{});
    proc.once('error',()=>{closed=true;clearTimeout(watchdog);resolve({status:null,stdout,stderr});});
    proc.once('close',status=>{closed=true;clearTimeout(watchdog);resolve({status:overflow?null:status,stdout,stderr});});
  });
  proc.stdin.end(JSON.stringify(fixture));
  try{
    const end=Date.now()+5000;let observed=false;
    while(Date.now()<end&&!closed){
      // A real completed read-only round sees pending work, rolls back, then
      // waits outside its snapshot. This is an independent server observation.
      observed=!!(await pool.query("SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND application_name=$1 AND state='idle' AND query='ROLLBACK'",[appname])).rowCount;
      if(observed)break;await new Promise(resolve=>setTimeout(resolve,20));
    }
    assert.equal(observed,true,'actual verifier waiting round not observed');assert.equal(closed,false,'pending aggregate must not already verify successfully');
    assert.ok((await pool.query('SELECT count(*)::int AS count FROM aggregation_jobs a JOIN projects p ON p.tenant_id=a.tenant_id AND p.id=a.project_id WHERE a.tenant_id=$1 AND a.project_id=$2 AND (a.processed_at IS NULL OR a.processed_epoch IS DISTINCT FROM p.privacy_epoch)',[fixture!.tenant,fixture!.project])).rows[0].count>0,'native aggregate still pending');
    await aggregate();const verified=await done;assert.equal(verified.status,0,'current-epoch aggregate should verify after real COMMIT');return JSON.parse(verified.stdout);
  }finally{if(!closed)proc.kill('SIGTERM');await done;clearTimeout(watchdog);}
}
async function fingerprints(){
  const tables=(await pool.query("SELECT tablename AS name FROM pg_tables WHERE schemaname='public' AND tablename NOT IN ('recovery_gate','recovery_import_scopes') ORDER BY tablename")).rows;
  const lines=[];for(const {name}of tables){assert.match(name,/^[a-z_][a-z0-9_]*$/);lines.push((await pool.query(`SELECT json_build_array('public.${name}',count(*),coalesce(bit_xor(hashtextextended(row_to_json(t)::text,0)),0))::text AS line FROM public.${name} t`)).rows[0].line);}
  return lines.join('\n')+'\n';
}
function preservation(args:string[]){return spawnSync(process.env.PYTHON??'python3',['scripts/capacity-preservation.py',...args],{encoding:'utf8',timeout:30000,maxBuffer:2*1024*1024});}
try{
  assert.match((await pool.query('SELECT current_database() AS name')).rows[0].name,/^ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$/);
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM tenants')).rows[0].n,0);
  const monitoring=await pool.connect();try{await storeOperationalSnapshot(monitoring,new IntakeMetrics().snapshot());await storeOperationalSnapshot(monitoring,new WorkerMetrics().snapshot());await evaluateAndStoreOperationalAlerts(monitoring);}finally{monitoring.release();}
  const beforeFile=join(dir,'before.jsonl'),afterFile=join(dir,'after.jsonl'),prefixFile=join(dir,'prefix.jsonl');await writeFile(beforeFile,await fingerprints());
  const created=child('create');assert.equal(created.status,0,'fixture create');fixture=JSON.parse(created.stdout);
  const f=fixture!,ids=[randomUUID(),randomUUID()];
  for(const event_id of ids)await persist({tenant_id:f.tenant!,project_id:f.project!,received_at:new Date().toISOString(),event:{event_id,event_name:'capacity_proof',distinct_id:'synthetic',event_time:new Date().toISOString(),properties:{synthetic:true},context:{},schema_version:1}});
  const result={tenant:f.tenant,project:f.project,generated:2,event_ids:ids};await writeFile(file,JSON.stringify(result),{mode:0o600});
  const proof=await verifyAfterAggregate();
  assert.equal(proof.raw_ids_matched,2);assert.equal(proof.job_ids_matched,2);assert.equal(proof.unique_ids_matched,2);
  assert.equal(proof.raw_unique_ids,2);assert.equal(proof.exact_id_sets,true);assert.equal(proof.dedup_stored,true);assert.equal(proof.aggregates.processed,2);
  assert.equal(proof.event_ids_sha256,createHash('sha256').update(JSON.stringify([...ids].sort())).digest('hex'));
  for(const [table,message]of [['events','exact raw event IDs'],['aggregation_jobs','exact aggregation job IDs']]){
    const changed=randomUUID();await pool.query(`UPDATE ${table} SET event_id=$3 WHERE project_id=$1 AND event_id=$2`,[f.project,ids[0],changed]);
    rejectVerify(message!);
    await pool.query(`UPDATE ${table} SET event_id=$3 WHERE project_id=$1 AND event_id=$2`,[f.project,changed,ids[0]]);
  }
  const originalRaw=(await pool.query('SELECT event_time::text AS time FROM events WHERE tenant_id=$1 AND project_id=$2 AND event_id=$3',[f.tenant,f.project,ids[0]])).rows[0];
  const moved=(await pool.query("UPDATE events SET event_id=$4,event_time=event_time+interval '1 microsecond' WHERE tenant_id=$1 AND project_id=$2 AND event_id=$3 RETURNING event_time::text AS time",[f.tenant,f.project,ids[0],ids[1]])).rows;
  assert.equal(moved.length,1);
  try{
    const trap=(await pool.query('SELECT count(*)::int AS total,count(*) FILTER(WHERE event_id=ANY($3::uuid[]))::int AS matched,count(DISTINCT event_id)::int AS unique_ids FROM events WHERE tenant_id=$1 AND project_id=$2',[f.tenant,f.project,ids])).rows[0];
    assert.deepEqual(trap,{total:2,matched:2,unique_ids:1},'real same-count raw set defeats old count/ANY checks');rejectVerify('exact raw event IDs');
  }finally{assert.equal((await pool.query('UPDATE events SET event_id=$3,event_time=$5::timestamptz WHERE tenant_id=$1 AND project_id=$2 AND event_id=$4 AND event_time=$6::timestamptz',[f.tenant,f.project,ids[0],ids[1],originalRaw.time,moved[0].time])).rowCount,1);}
  const epoch=(await pool.query('SELECT privacy_epoch FROM projects WHERE tenant_id=$1 AND id=$2',[f.tenant,f.project])).rows[0].privacy_epoch;
  for(const pendingEpoch of [epoch-1,null]){
    await pool.query('UPDATE aggregation_jobs SET processed_epoch=$4 WHERE tenant_id=$1 AND project_id=$2 AND event_id=$3',[f.tenant,f.project,ids[0],pendingEpoch]);
    const repaired=await verifyAfterAggregate();assert.equal(repaired.aggregates.processed,2,'stale or NULL epoch only passes after real recomputation');
  }
  await pool.query('UPDATE aggregation_jobs SET processed_epoch=NULL WHERE tenant_id=$1 AND project_id=$2 AND event_id=$3',[f.tenant,f.project,ids[0]]);
  const epochRejectedAt=performance.now();
  try{rejectVerify('completed aggregation epoch matches project');}finally{await pool.query('UPDATE aggregation_jobs SET processed_epoch=$4 WHERE tenant_id=$1 AND project_id=$2 AND event_id=$3',[f.tenant,f.project,ids[0],epoch]);}
  const unrepairedEpochRejectionMs=Math.ceil(performance.now()-epochRejectedAt);
  await pool.query("UPDATE event_dedup SET status='failed' WHERE tenant_id=$1 AND project_id=$2 AND event_id=$3",[f.tenant,f.project,ids[0]]);
  try{rejectVerify('event registrations are stored');}finally{await pool.query("UPDATE event_dedup SET status='stored' WHERE tenant_id=$1 AND project_id=$2 AND event_id=$3",[f.tenant,f.project,ids[0]]);}
  for(const [kind,name,hidden]of [['TABLE','event_count_buckets','capacity_buckets_hidden'],['VIEW','visible_event_count_buckets','capacity_visible_hidden']]){
    // Autocommit makes the missing relation visible to the actual CLI. Do not
    // retain uncommitted DDL locks and mistake a child timeout for rejection.
    await pool.query(`ALTER ${kind} public.${name} RENAME TO ${hidden}`);
    try{rejectVerify('expected aggregate bucket schema');}finally{await pool.query(`ALTER ${kind} public.${hidden} RENAME TO ${name}`);}
  }
  assert.equal(child('verify').status,0,'exact fixture verifies after every adversarial change is restored');
  await writeFile(file,JSON.stringify({...result,event_ids:[ids[0],ids[0]]}));assert.equal(child('verify').status,1,'duplicate requested IDs');
  await writeFile(file,JSON.stringify({...result,event_ids:['invalid',ids[1]]}));assert.equal(child('verify').status,1,'invalid requested IDs');
  assert.equal(child('cleanup').status,0);fixture=undefined;
  const more=new IntakeMetrics(),db2=await pool.connect();try{await storeOperationalSnapshot(db2,more.snapshot());await new Promise(resolve=>setTimeout(resolve,5));more.observe({status:500,events:0,durationMs:1});await storeOperationalSnapshot(db2,more.snapshot());await evaluateAndStoreOperationalAlerts(db2);}finally{db2.release();}
  const prefixSql=preservation(['sql',beforeFile]);assert.equal(prefixSql.status,0,prefixSql.stderr);
  const prefix=async()=>{const statements=prefixSql.stdout.trim().split('\n'),lines=[];for(const statement of statements){const result=await pool.query(statement.replace(/;$/,'').replace(') FROM (SELECT',')::text AS line FROM (SELECT'));lines.push(result.rows[0].line);}await writeFile(prefixFile,lines.join('\n')+'\n');};
  await writeFile(afterFile,await fingerprints());await prefix();const preserved=preservation(['verify',beforeFile,afterFile,prefixFile,join(dir,'preservation.json')]);assert.equal(preserved.status,0,preserved.stderr+preserved.stdout);assert.equal(JSON.parse(preserved.stdout).preserved,true);
  const original=(await pool.query("SELECT instance_id,payload_hash FROM operational_samples WHERE source='worker' LIMIT 1")).rows[0];await pool.query("UPDATE operational_samples SET payload_hash=repeat('f',64) WHERE instance_id=$1",[original.instance_id]);
  await writeFile(afterFile,await fingerprints());await prefix();const altered=preservation(['verify',beforeFile,afterFile,prefixFile,join(dir,'altered-preservation.json')]);assert.equal(altered.status,1,altered.stderr);assert.ok(JSON.parse(altered.stdout).reasons.includes('original_monitor_rows_changed:public.operational_samples'));await pool.query('UPDATE operational_samples SET payload_hash=$2 WHERE instance_id=$1',[original.instance_id,original.payload_hash]);
  console.log(JSON.stringify({capacity_event_set_proof:true,exact_ids:true,equal_count_wrong_raw_rejected:true,equal_count_wrong_job_rejected:true,equal_count_expected_id_raw_duplicate_rejected:true,failed_dedup_status_rejected:true,actual_pending_aggregate_waited:true,stale_and_null_epochs_recomputed_before_success:true,unrepaired_null_epoch_rejected:true,unrepaired_epoch_rejection_ms:unrepairedEpochRejectionMs,missing_bucket_table_and_view_rejected:true,reconciliation_rejection_ms:rejectedDurations,invalid_requested_set_rejected:true,real_pg_preservation:true,monitor_append_retains_originals:true,altered_original_monitor_rejected:true}));
}finally{
  try{if(fixture)assert.equal(child('cleanup').status,0,'synthetic fixture cleanup');}finally{
    await pool.end();const target=resolve(dir);assert.equal(dirname(target),parent);assert.ok(basename(target).startsWith('ccpanel-capacity-proof-'));await rm(target,{recursive:true,force:true});
  }
}
