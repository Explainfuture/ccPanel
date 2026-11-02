// Disposable PG only. Controller must apply a fresh empty-source recovery ledger
// before this script; never open the gate by importing from an old restored DB.
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {pool} from '../packages/server/src/db';
import {initializePlatformOperator,provisionCompany} from '../packages/server/src/company-provision';
import {token,digest,passwordMatches} from '../packages/server/src/security';
import {recoveryLock} from '../packages/server/src/recovery-gate';
import {createInvitation,acceptInvitation} from '../packages/server/src/invitations';
import {createApp} from '../apps/api/src/app';
try{
  assert.match((await pool.query('SELECT current_database() AS name')).rows[0].name,/^ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$/);
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM tenants')).rows[0].n,0);
  assert.equal((await pool.query('SELECT blocked FROM recovery_gate WHERE id=true')).rows[0].blocked,false,'fresh-source ledger required');
  const actor=randomUUID(),ordinary=randomUUID(),oldCompany=randomUUID(),email=`operator-${actor}@integration.invalid`,ordinaryEmail=`admin-${ordinary}@integration.invalid`;
  for(const [id,address]of[[actor,email],[ordinary,ordinaryEmail]])await pool.query('INSERT INTO users VALUES($1,$2,$3)',[id,address,'fixture-only-password']);
  await pool.query('INSERT INTO tenants VALUES($1,$2)',[oldCompany,'PROVISION TEMP']);await pool.query("INSERT INTO tenant_memberships VALUES($1,$2,'admin')",[oldCompany,ordinary]);
  const request=randomUUID(),password=token(),spec={name:'新公司',owner_email:`owner-${request}@integration.invalid`,owner_password:password};
  await assert.rejects(()=>provisionCompany(ordinaryEmail,request,spec),/platform_operator_required/);
  await initializePlatformOperator(email);await assert.rejects(()=>initializePlatformOperator(ordinaryEmail),/platform_operator_already_initialized/);
  const offline=await pool.connect();try{
    await offline.query('SELECT pg_advisory_lock($1)',[recoveryLock]);await assert.rejects(()=>provisionCompany(email,request,spec),/recovery_in_progress/);
  }finally{await offline.query('SELECT pg_advisory_unlock($1)',[recoveryLock]);offline.release();}
  const results=await Promise.all([provisionCompany(email,request,spec),provisionCompany(email,request,spec)]);assert.equal(results.filter(r=>r.reused).length,1);
  const created=results[0]!;assert.ok(results.every(r=>r.tenant_id===created.tenant_id));
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM company_provision_requests')).rows[0].n,1);
  const originalHash=(await pool.query('SELECT password_hash FROM users WHERE id=$1',[created.owner_id])).rows[0].password_hash;assert.ok(passwordMatches(password,originalHash));
  await provisionCompany(email,request,{...spec,owner_password:'different-retry-password'});assert.equal((await pool.query('SELECT password_hash FROM users WHERE id=$1',[created.owner_id])).rows[0].password_hash,originalHash);
  await assert.rejects(()=>provisionCompany(email,request,{...spec,name:'different company'}),/provision_request_conflict/);
  await assert.rejects(()=>provisionCompany(email,randomUUID(),{...spec,owner_email:ordinaryEmail}),/owner_account_already_exists/);
  const app=createApp({ready:()=>true,publish:async()=>{throw new Error('unexpected_publish');}}),headers=new Map<string,Record<string,string>>(),origin=process.env.PUBLIC_ORIGIN!;
  for(const id of[actor,ordinary,created.owner_id]){const secret=token(),csrf=digest(secret+':csrf');await pool.query("INSERT INTO sessions VALUES($1,$2,$3,now()+interval '1 hour')",[digest(secret),id,digest(csrf)]);headers.set(id,{cookie:'ccpanel_session='+secret,origin,'x-csrf-token':csrf,'content-type':'application/json'});}
  const req=(user:string,company:string)=>app.request(`/api/tenants/${company}/projects`,{headers:headers.get(user)});
  assert.equal((await req(actor,created.tenant_id)).status,404,'platform operator has no implicit data membership');assert.equal((await req(ordinary,created.tenant_id)).status,404);assert.equal((await req(created.owner_id,oldCompany)).status,404);
  assert.equal((await app.request(`/api/tenants/${created.tenant_id}/projects`,{method:'POST',headers:headers.get(created.owner_id),body:JSON.stringify({name:'首个项目',origins:[]})})).status,201);
  const invited=await createInvitation(created.tenant_id,created.owner_id,{email:ordinaryEmail,role:'admin'});await acceptInvitation(ordinary,{token:invited.token});assert.equal((await req(ordinary,created.tenant_id)).status,200);
  await assert.rejects(()=>provisionCompany(ordinaryEmail,randomUUID(),spec),/platform_operator_required/);
  const before=(await pool.query('SELECT (SELECT count(*)::int FROM users) AS users,(SELECT count(*)::int FROM tenants) AS tenants')).rows[0];
  await pool.query("ALTER TABLE company_provision_requests ADD CONSTRAINT fixture_provision_failure CHECK(request_hash='never') NOT VALID");
  const retryId=randomUUID(),retrySpec={...spec,owner_email:`owner-${retryId}@integration.invalid`};
  await assert.rejects(()=>provisionCompany(email,retryId,retrySpec),e=>(e as {code?:string}).code==='23514');
  assert.deepEqual((await pool.query('SELECT (SELECT count(*)::int FROM users) AS users,(SELECT count(*)::int FROM tenants) AS tenants')).rows[0],before,'failure rolls back user, company and membership');
  await pool.query('ALTER TABLE company_provision_requests DROP CONSTRAINT fixture_provision_failure');await provisionCompany(email,retryId,retrySpec);
  await pool.query('UPDATE platform_operators SET revoked_at=clock_timestamp() WHERE user_id=$1',[actor]);await assert.rejects(()=>provisionCompany(email,request,spec),/platform_operator_required/);
  assert.equal((await pool.query("SELECT count(*)::int AS n FROM platform_audit WHERE action='company_provisioned'")).rows[0].n,2);
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM tenant_memberships WHERE user_id=$1',[actor])).rows[0].n,0);
  console.log(JSON.stringify({passed:true,concurrent_receipt_once:true,retry_keeps_password:true,conflict_rejected:true,existing_account_requires_consent:true,company_admin_not_platform_operator:true,platform_operator_no_implicit_data_access:true,first_owner_can_create_project:true,recovery_exclusive_blocks_provision:true,failure_no_orphan_accounts:true,revoked_operator_rejected:true,companies_created:2}));
}finally{await pool.end();}
