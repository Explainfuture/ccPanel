// CI host acceptance: requires Python3 for offline preservation CLI checks.
import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
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
  const db=await pool.connect();try{assert.equal(await aggregateNext(db),'completed');assert.ok(['completed','empty'].includes(await aggregateNext(db)),'second bucket may already be processed');}finally{db.release();}
  const result={tenant:f.tenant,project:f.project,generated:2,event_ids:ids};await writeFile(file,JSON.stringify(result),{mode:0o600});
  const valid=child('verify');assert.equal(valid.status,0,'exact set verification');const proof=JSON.parse(valid.stdout);
  assert.equal(proof.raw_ids_matched,2);assert.equal(proof.job_ids_matched,2);assert.equal(proof.unique_ids_matched,2);
  assert.equal(proof.event_ids_sha256,createHash('sha256').update(JSON.stringify([...ids].sort())).digest('hex'));
  for(const [table,message]of [['events','exact raw event IDs'],['aggregation_jobs','exact aggregation job IDs']]){
    const changed=randomUUID();await pool.query(`UPDATE ${table} SET event_id=$3 WHERE project_id=$1 AND event_id=$2`,[f.project,ids[0],changed]);
    const rejected=child('verify');assert.equal(rejected.status,1);assert.ok(rejected.stderr.includes(message!),'same total, wrong set must fail');
    await pool.query(`UPDATE ${table} SET event_id=$3 WHERE project_id=$1 AND event_id=$2`,[f.project,changed,ids[0]]);
  }
  await writeFile(file,JSON.stringify({...result,event_ids:[ids[0],ids[0]]}));assert.equal(child('verify').status,1,'duplicate requested IDs');
  await writeFile(file,JSON.stringify({...result,event_ids:['invalid',ids[1]]}));assert.equal(child('verify').status,1,'invalid requested IDs');
  assert.equal(child('cleanup').status,0);fixture=undefined;
  const more=new IntakeMetrics(),db2=await pool.connect();try{await storeOperationalSnapshot(db2,more.snapshot());await new Promise(resolve=>setTimeout(resolve,5));more.observe({status:500,events:0,durationMs:1});await storeOperationalSnapshot(db2,more.snapshot());await evaluateAndStoreOperationalAlerts(db2);}finally{db2.release();}
  const prefixSql=preservation(['sql',beforeFile]);assert.equal(prefixSql.status,0,prefixSql.stderr);
  const prefix=async()=>{const statements=prefixSql.stdout.trim().split('\n'),lines=[];for(const statement of statements){const result=await pool.query(statement.replace(/;$/,'').replace(') FROM (SELECT',')::text AS line FROM (SELECT'));lines.push(result.rows[0].line);}await writeFile(prefixFile,lines.join('\n')+'\n');};
  await writeFile(afterFile,await fingerprints());await prefix();const preserved=preservation(['verify',beforeFile,afterFile,prefixFile,join(dir,'preservation.json')]);assert.equal(preserved.status,0,preserved.stderr+preserved.stdout);assert.equal(JSON.parse(preserved.stdout).preserved,true);
  const original=(await pool.query("SELECT instance_id,payload_hash FROM operational_samples WHERE source='worker' LIMIT 1")).rows[0];await pool.query("UPDATE operational_samples SET payload_hash=repeat('f',64) WHERE instance_id=$1",[original.instance_id]);
  await writeFile(afterFile,await fingerprints());await prefix();const altered=preservation(['verify',beforeFile,afterFile,prefixFile,join(dir,'altered-preservation.json')]);assert.equal(altered.status,1,altered.stderr);assert.ok(JSON.parse(altered.stdout).reasons.includes('original_monitor_rows_changed:public.operational_samples'));await pool.query('UPDATE operational_samples SET payload_hash=$2 WHERE instance_id=$1',[original.instance_id,original.payload_hash]);
  console.log(JSON.stringify({capacity_event_set_proof:true,exact_ids:true,equal_count_wrong_raw_rejected:true,equal_count_wrong_job_rejected:true,invalid_requested_set_rejected:true,real_pg_preservation:true,monitor_append_retains_originals:true,altered_original_monitor_rejected:true}));
}finally{
  try{if(fixture)assert.equal(child('cleanup').status,0,'synthetic fixture cleanup');}finally{
    await pool.end();const target=resolve(dir);assert.equal(dirname(target),parent);assert.ok(basename(target).startsWith('ccpanel-capacity-proof-'));await rm(target,{recursive:true,force:true});
  }
}
