// Explicit synthetic companies for real browser/API validation. Credentials go
// only to protected redirected stdout; never print this command's create output.
import assert from 'node:assert/strict';
import {randomUUID,randomBytes} from 'node:crypto';
import {readFileSync} from 'node:fs';
import {pool} from '../packages/server/src/db';
import {digest,passwordHash} from '../packages/server/src/security';
assert.equal(process.env.CCPANEL_PRODUCT_UI_TEST,'true');
const label='PRODUCT UI TEMP',mode=process.argv[2];
try{
  if(mode==='create'){
    assert.equal((await pool.query("SELECT 1 FROM tenants WHERE name LIKE $1",[label+'%'])).rowCount,0);
    const db=await pool.connect();try{
      await db.query('BEGIN');
      const f={origin:process.env.PUBLIC_ORIGIN!,tenant:randomUUID(),foreign_tenant:randomUUID(),project:randomUUID(),second_project:randomUUID(),foreign_project:randomUUID(),key:randomBytes(24).toString('hex'),owner:{id:randomUUID(),email:'',password:randomBytes(24).toString('hex'),token:randomBytes(24).toString('hex')},target:{id:randomUUID(),email:'',password:randomBytes(24).toString('hex'),token:randomBytes(24).toString('hex')}};
      for(const [kind,user]of Object.entries({owner:f.owner,target:f.target})){
        user.email=`product-ui-${kind}-${user.id}@integration.invalid`;
        await db.query('INSERT INTO users VALUES($1,$2,$3)',[user.id,user.email,passwordHash(user.password)]);
        await db.query("INSERT INTO sessions VALUES($1,$2,$3,now()+interval '1 hour')",[digest(user.token),user.id,digest(digest(user.token+':csrf'))]);
      }
      for(const [id,name]of[[f.tenant,label+' A'],[f.foreign_tenant,label+' B']])await db.query('INSERT INTO tenants VALUES($1,$2)',[id,name]);
      await db.query("INSERT INTO tenant_memberships VALUES($1,$2,'admin'),($3,$4,'viewer')",[f.tenant,f.owner.id,f.foreign_tenant,f.target.id]);
      for(const [id,tenant,name]of[[f.project,f.tenant,'产品验收项目'],[f.second_project,f.tenant,'同公司第二项目'],[f.foreign_project,f.foreign_tenant,'另一公司项目']])await db.query("INSERT INTO projects(id,tenant_id,name,timezone,origins) VALUES($1,$2,$3,'UTC',$4)",[id,tenant,name,[f.origin]]);
      await db.query('INSERT INTO write_keys(id,tenant_id,project_id,key_hash,label) VALUES($1,$2,$3,$4,$5)',[randomUUID(),f.tenant,f.project,digest(f.key),'product-ui']);
      await db.query('COMMIT');console.log(JSON.stringify(f));
    }catch(error){await db.query('ROLLBACK');throw error;}finally{db.release();}
  }else{
    const f=JSON.parse(readFileSync(0,'utf8'));
    for(const [tenant,name,user,role]of[[f.tenant,label+' A',f.owner.id,'owner'],[f.foreign_tenant,label+' B',f.target.id,'target']]){
      assert.equal((await pool.query('SELECT 1 FROM tenants WHERE id=$1 AND name=$2',[tenant,name])).rowCount,1);
      assert.equal((await pool.query('SELECT 1 FROM users WHERE id=$1 AND email=$2',[user,f[role].email])).rowCount,1);
    }
    if(mode==='cleanup'){
      const db=await pool.connect();try{
        await db.query('BEGIN');
        const tenants=[f.tenant,f.foreign_tenant],users=[f.owner.id,f.target.id];
        assert.equal((await db.query('SELECT 1 FROM tenant_memberships WHERE user_id=ANY($1::uuid[]) AND NOT(tenant_id=ANY($2::uuid[]))',[users,tenants])).rowCount,0);
        for(const table of ['tenant_invitations','event_count_buckets','replay_items','replay_jobs','recovery_import_scopes','deletion_barriers','deletion_jobs','query_jobs','dashboards','reports','dlq_records','aggregation_jobs','ingest_metrics','events','event_dedup','identity_links','event_schemas','write_keys','audit_logs','tenant_memberships','projects'])await db.query(`DELETE FROM ${table} WHERE tenant_id=ANY($1::uuid[])`,[tenants]);
        await db.query('DELETE FROM sessions WHERE user_id=ANY($1::uuid[])',[users]);await db.query('DELETE FROM users WHERE id=ANY($1::uuid[])',[users]);await db.query('DELETE FROM tenants WHERE id=ANY($1::uuid[])',[tenants]);await db.query('COMMIT');
        console.log('Synthetic product UI companies and accounts cleaned');
      }catch(error){await db.query('ROLLBACK');throw error;}finally{db.release();}
    }else if(mode==='verify'){
      const memberships=(await pool.query('SELECT tenant_id,role FROM tenant_memberships WHERE user_id=$1',[f.target.id])).rows;
      assert.equal(memberships.find(r=>r.tenant_id===f.tenant)?.role,'analyst');assert.equal(memberships.find(r=>r.tenant_id===f.foreign_tenant)?.role,'viewer');
      const n=(await pool.query('SELECT (SELECT count(*)::int FROM events WHERE tenant_id=$1) AS events,(SELECT count(*)::int FROM aggregation_jobs WHERE tenant_id=$1 AND processed_at IS NOT NULL) AS processed,(SELECT count(*)::int FROM tenant_invitations WHERE tenant_id=$1 AND accepted_at IS NOT NULL) AS accepted',[f.tenant])).rows[0];
      assert.equal(n.events,4);assert.equal(n.processed,4);assert.equal(n.accepted,1);console.log(JSON.stringify({passed:true,...n,roles_per_company:true}));
    }else throw new Error('invalid_product_ui_fixture_mode');
  }
}finally{await pool.end();}
