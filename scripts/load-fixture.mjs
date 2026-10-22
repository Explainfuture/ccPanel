// Operator-only fixture lifecycle. Create output contains credentials: redirect
// to a mode-600 file, transfer over SSH, and never include it in logs or Git.
import pg from 'pg';
import { randomUUID,randomBytes,scryptSync,createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { readFileSync } from 'node:fs';
import assert from 'node:assert/strict';
const pool=new pg.Pool({connectionString:process.env.DATABASE_URL,max:1});
try{
  const mode=process.argv[2];
  if(mode==='create'){
    await pool.query('BEGIN');
    const tenant=randomUUID(),project=randomUUID(),user=randomUUID(),key=randomBytes(24).toString('hex'),password=randomBytes(24).toString('hex'),salt=randomBytes(16).toString('hex'),email=`public-load-${user}@integration.invalid`;
    await pool.query('INSERT INTO tenants VALUES($1,$2)',[tenant,'CAPACITY EXTERNAL TEMP']);
    await pool.query('INSERT INTO users VALUES($1,$2,$3)',[user,email,salt+':'+scryptSync(password,salt,64).toString('hex')]);
    await pool.query("INSERT INTO tenant_memberships VALUES($1,$2,'admin')",[tenant,user]);
    await pool.query('INSERT INTO projects(id,tenant_id,name,origins) VALUES($1,$2,$3,$4)',[project,tenant,'CAPACITY EXTERNAL TEMP',[process.env.PUBLIC_ORIGIN]]);
    await pool.query('INSERT INTO write_keys(id,tenant_id,project_id,key_hash,label) VALUES($1,$2,$3,$4,$5)',[randomUUID(),tenant,project,createHash('sha256').update(key).digest('hex'),'capacity-external']);
    await pool.query('COMMIT');
    console.log(JSON.stringify({tenant,project,user,key,email,password,origin:process.env.PUBLIC_ORIGIN,created_at:new Date().toISOString()}));
  }else{
    const f=JSON.parse(readFileSync(0,'utf8'));
    const owned=await pool.query("SELECT 1 FROM tenants t JOIN projects p ON p.tenant_id=t.id JOIN tenant_memberships m ON m.tenant_id=t.id JOIN users u ON u.id=m.user_id WHERE t.id=$1 AND p.id=$2 AND m.user_id=$3 AND u.email=$4 AND u.email LIKE 'public-load-%@integration.invalid' AND t.name='CAPACITY EXTERNAL TEMP'",[f.tenant,f.project,f.user,f.email]);assert.equal(owned.rowCount,1,'expected isolated capacity fixture and its own user');
    if(mode==='verify'){
      const result=JSON.parse(await readFile(process.argv[3],'utf8'));
      assert.equal(result.tenant,f.tenant);assert.equal(result.project,f.project);assert.equal(new Set(result.event_ids).size,result.generated);
      const counts=await pool.query('SELECT (SELECT count(*)::int FROM events WHERE tenant_id=$1 AND project_id=$2) AS events,(SELECT count(*)::int FROM event_dedup WHERE tenant_id=$1 AND project_id=$2) AS dedup,(SELECT count(*)::int FROM aggregation_jobs WHERE tenant_id=$1 AND project_id=$2) AS aggregate_jobs,(SELECT count(*)::int FROM dlq_records WHERE tenant_id=$1 AND project_id=$2) AS dlq',( [f.tenant,f.project] ));
      const matched=await pool.query('SELECT count(*)::int AS count FROM event_dedup WHERE tenant_id=$1 AND project_id=$2 AND event_id=ANY($3::uuid[])',[f.tenant,f.project,result.event_ids]);
      for(const name of ['events','dedup','aggregate_jobs'])assert.equal(counts.rows[0][name],result.generated,name);
      assert.equal(counts.rows[0].dlq,0);assert.equal(matched.rows[0].count,result.generated);
      console.log(JSON.stringify({passed:true,unique_ids_matched:matched.rows[0].count,...counts.rows[0]}));
    }else if(mode==='cleanup'){
      for(const table of ['query_jobs','dashboards','reports','dlq_records','aggregation_jobs','ingest_metrics','events','event_dedup','identity_links','event_schemas','write_keys','audit_logs','tenant_memberships','projects'])await pool.query(`DELETE FROM ${table} WHERE tenant_id=$1`,[f.tenant]);
      await pool.query('DELETE FROM sessions WHERE user_id=$1',[f.user]);await pool.query('DELETE FROM users WHERE id=$1',[f.user]);await pool.query('DELETE FROM tenants WHERE id=$1',[f.tenant]);console.log('Isolated capacity fixture cleaned');
    }else throw new Error('Expected create, verify, or cleanup');
  }
}finally{await pool.end();}
