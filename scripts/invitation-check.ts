// Real PG + Hono requests in-process; no external messages or Kafka required.
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {pool} from '../packages/server/src/db';
import {token,digest} from '../packages/server/src/security';
import {createApp} from '../apps/api/src/app';
try{
  assert.match((await pool.query('SELECT current_database() AS name')).rows[0].name,/^ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$/);
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM tenants')).rows[0].n,0);
  const a=randomUUID(),b=randomUUID(),owner=randomUUID(),target=randomUUID(),wrong=randomUUID();
  for(const tenant of [a,b])await pool.query('INSERT INTO tenants VALUES($1,$2)',[tenant,'INVITATION TEMP']);
  const headers=new Map<string,Record<string,string>>(),emails=new Map<string,string>();
  const origin=process.env.PUBLIC_ORIGIN!;assert.ok(origin);
  for(const user of [owner,target,wrong]){
    const email=`invite-${user}@integration.invalid`,secret=token(),csrf=digest(secret+':csrf');emails.set(user,email);
    await pool.query('INSERT INTO users VALUES($1,$2,$3)',[user,email,'unchanged-test-password']);
    await pool.query("INSERT INTO sessions VALUES($1,$2,$3,clock_timestamp()+interval '1 hour')",[digest(secret),user,digest(csrf)]);
    headers.set(user,{cookie:'ccpanel_session='+secret,origin,'x-csrf-token':csrf,'content-type':'application/json'});
  }
  await pool.query("INSERT INTO tenant_memberships VALUES($1,$2,'admin'),($3,$4,'admin')",[a,owner,b,target]);
  const projects=[randomUUID(),randomUUID()];for(const id of projects)await pool.query('INSERT INTO projects(id,tenant_id,name) VALUES($1,$2,$3)',[id,a,'INVITATION TEMP']);
  const app=createApp({ready:()=>true,publish:async()=>{throw new Error('unexpected_publish');}});
  const req=(actor:string,path:string,body?:unknown,method=body?'POST':'GET')=>app.request(path,{method,headers:headers.get(actor),...(body?{body:JSON.stringify(body)}:{})});
  async function invite(role='analyst',actor=owner){const response=await req(actor,`/api/tenants/${a}/invitations`,{email:emails.get(target),role});assert.equal(response.status,201);const value=await response.json();assert.equal(value.token.length,43);return value;}
  const invited=await invite();
  const listing=await req(owner,`/api/tenants/${a}/invitations`);assert.equal(listing.status,200);const listed=await listing.text();assert.ok(!listed.includes(invited.token)&&!listed.includes('token_hash'));
  assert.equal((await req(target,`/api/tenants/${a}/invitations`)).status,404);
  assert.equal((await req(wrong,'/api/invitations/accept',{token:invited.token})).status,404);
  assert.equal((await req(wrong,'/api/invitations/preview',{token:invited.token})).status,404);
  const preview=await req(target,'/api/invitations/preview',{token:invited.token});assert.equal(preview.status,200);assert.equal((await preview.json()).company_name,'INVITATION TEMP');
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM tenant_memberships WHERE tenant_id=$1 AND user_id=$2',[a,target])).rows[0].n,0,'preview never grants membership');
  const noCsrf={...headers.get(target)!};delete noCsrf['x-csrf-token'];assert.equal((await app.request('/api/invitations/accept',{method:'POST',headers:noCsrf,body:JSON.stringify({token:invited.token})})).status,403);
  const accepted=await Promise.all([req(target,'/api/invitations/accept',{token:invited.token}),req(target,'/api/invitations/accept',{token:invited.token})]);
  assert.ok(accepted.every(r=>r.status===200));const statuses=await Promise.all(accepted.map(r=>r.json()));assert.equal(statuses.filter(r=>r.already_accepted).length,1);
  assert.equal((await pool.query("SELECT count(*)::int AS n FROM audit_logs WHERE tenant_id=$1 AND action='invitation_accepted'",[a])).rows[0].n,1);
  const companyProjects=await req(target,`/api/tenants/${a}/projects`);assert.equal(companyProjects.status,200);assert.equal((await companyProjects.json()).projects.length,2);
  assert.equal((await req(target,`/api/tenants/${a}/invitations`,{email:emails.get(wrong)})).status,403,'admin in B remains analyst in A');
  assert.equal((await req(owner,`/api/tenants/${b}/projects`)).status,404);
  assert.equal((await req(owner,`/api/tenants/${a}/members`,{email:emails.get(target),role:'admin',password:'unused-password-12345'})).status,409);
  assert.equal((await pool.query('SELECT password_hash FROM users WHERE id=$1',[target])).rows[0].password_hash,'unchanged-test-password');
  assert.equal((await req(owner,`/api/tenants/${a}/members/${target}`,undefined,'DELETE')).status,200);
  assert.equal((await req(target,`/api/tenants/${a}/projects`)).status,404);assert.equal((await req(target,'/api/invitations/accept',{token:invited.token})).status,404);
  assert.equal((await req(target,'/api/invitations/preview',{token:invited.token})).status,404);
  assert.equal((await req(target,`/api/tenants/${b}/projects`)).status,200);
  const pending=await invite('admin');await pool.query("INSERT INTO tenant_memberships VALUES($1,$2,'viewer')",[a,target]);
  const preserved=await req(target,'/api/invitations/accept',{token:pending.token});assert.equal(preserved.status,200);assert.equal((await preserved.json()).role,'viewer');
  await req(owner,`/api/tenants/${a}/members/${target}`,undefined,'DELETE');
  const expired=await invite();await pool.query("UPDATE tenant_invitations SET created_at=clock_timestamp()-interval '8 days',expires_at=clock_timestamp()-interval '1 day' WHERE id=$1",[expired.id]);
  assert.equal((await req(target,'/api/invitations/accept',{token:expired.token})).status,404);
  const revoked=await invite();assert.equal((await req(target,`/api/tenants/${b}/invitations/${revoked.id}`,undefined,'DELETE')).status,404);assert.equal((await req(owner,`/api/tenants/${a}/invitations/${revoked.id}`,undefined,'DELETE')).status,200);
  assert.equal((await req(target,'/api/invitations/accept',{token:revoked.token})).status,404);
  const old=await invite();const replacement=await invite();assert.notEqual(old.token,replacement.token);assert.equal((await req(target,'/api/invitations/accept',{token:old.token})).status,404);
  await pool.query("INSERT INTO tenant_memberships VALUES($1,$2,'viewer')",[a,target]);await req(owner,`/api/tenants/${a}/members/${target}`,undefined,'DELETE');
  assert.equal((await req(target,'/api/invitations/accept',{token:replacement.token})).status,404,'removal revokes pending invitations');
  await pool.query("INSERT INTO tenant_memberships VALUES($1,$2,'admin')",[a,wrong]);const fromRemoved=await invite('analyst',wrong);await req(owner,`/api/tenants/${a}/members/${wrong}`,undefined,'DELETE');
  assert.equal((await req(target,'/api/invitations/accept',{token:fromRemoved.token})).status,404);
  console.log(JSON.stringify({passed:true,concurrent_accept_once:true,matched_account_required:true,csrf_checked:true,all_company_projects_visible:true,role_scoped_per_company:true,removed_member_cannot_rejoin_with_old_link:true,existing_role_preserved:true,expired_revoked_replaced_links_rejected:true,removed_creator_link_revoked:true,password_unchanged:true,no_token_in_list:true}));
}finally{await pool.end();}
