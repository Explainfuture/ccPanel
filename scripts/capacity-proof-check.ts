import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {randomUUID,createHash} from 'node:crypto';
import {mkdtemp,writeFile,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {resolve,join,dirname,basename} from 'node:path';
import {pool} from '../packages/server/src/db';
import {persist} from '../packages/server/src/consume';
import {aggregateNext} from '../packages/server/src/aggregations';
const parent=resolve(tmpdir()),dir=await mkdtemp(join(parent,'ccpanel-capacity-proof-')),file=join(dir,'result.json');
let fixture:Record<string,string>|undefined;
function child(mode:string){return spawnSync(process.execPath,['scripts/load-fixture.mjs',mode,...(mode==='verify'?[file]:[])],{input:fixture?JSON.stringify(fixture):'',encoding:'utf8',timeout:30000,maxBuffer:2*1024*1024});}
try{
  assert.match((await pool.query('SELECT current_database() AS name')).rows[0].name,/^ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$/);
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM tenants')).rows[0].n,0);
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
  console.log(JSON.stringify({capacity_event_set_proof:true,exact_ids:true,equal_count_wrong_raw_rejected:true,equal_count_wrong_job_rejected:true,invalid_requested_set_rejected:true}));
}finally{
  try{if(fixture)assert.equal(child('cleanup').status,0,'synthetic fixture cleanup');}finally{
    await pool.end();const target=resolve(dir);assert.equal(dirname(target),parent);assert.ok(basename(target).startsWith('ccpanel-capacity-proof-'));await rm(target,{recursive:true,force:true});
  }
}
