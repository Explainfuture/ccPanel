import {randomUUID} from 'node:crypto';
import {pool} from './db';
import {parseInvitation} from './invitations';
import {canonical,storageSafeText} from '../../schema/src/index';
import {digest,passwordHash} from './security';
import {recoveryLock} from './recovery-gate';
import type {PoolClient} from 'pg';
export class ProvisionError extends Error{}
export function parseCompanyProvision(input:unknown){
  if(!input||typeof input!=='object'||Array.isArray(input))throw new ProvisionError('invalid_company_provision');
  const b=input as Record<string,unknown>,name=typeof b.name==='string'?b.name.trim():'';
  let email:string;try{email=parseInvitation({email:b.owner_email}).email;}catch{throw new ProvisionError('invalid_company_provision');}
  if(Object.keys(b).some(k=>!['name','owner_email','owner_password'].includes(k))||!name||name.length>128||!storageSafeText(name)||typeof b.owner_password!=='string'||b.owner_password.length<12||b.owner_password.length>1024||!storageSafeText(b.owner_password)||/[\r\n]/.test(b.owner_password))throw new ProvisionError('invalid_company_provision');
  return{name,owner_email:email,owner_password:b.owner_password};
}
async function transaction<T>(run:(db:PoolClient)=>Promise<T>){
  const db=await pool.connect();try{
    await db.query('BEGIN');
    if(!(await db.query('SELECT pg_try_advisory_xact_lock_shared($1) AS held',[recoveryLock])).rows[0].held)throw new ProvisionError('recovery_in_progress');
    if((await db.query('SELECT blocked FROM recovery_gate WHERE id=true')).rows[0]?.blocked!==false)throw new ProvisionError('recovery_gate_closed');
    await db.query('SELECT pg_advisory_xact_lock(169202633)');
    const result=await run(db);await db.query('COMMIT');return result;
  }catch(error){await db.query('ROLLBACK');throw error;}finally{db.release();}
}
// Deployment-only initialization. Company admin membership never grants this.
export async function initializePlatformOperator(email:string){
  let normalized:string;try{normalized=parseInvitation({email}).email;}catch{throw new ProvisionError('invalid_operator_email');}
  return transaction(async db=>{
    if((await db.query('SELECT 1 FROM platform_operators LIMIT 1')).rowCount)throw new ProvisionError('platform_operator_already_initialized');
    const user=(await db.query('SELECT id FROM users WHERE email=$1',[normalized])).rows[0];if(!user)throw new ProvisionError('operator_account_required');
    await db.query('INSERT INTO platform_operators(user_id) VALUES($1)',[user.id]);
    await db.query("INSERT INTO platform_audit(operator_id,action) VALUES($1,'platform_operator_initialized')",[user.id]);return{operator_id:user.id};
  });
}
export async function provisionCompany(operatorEmail:string,requestId:string,input:unknown){
  const spec=parseCompanyProvision(input);
  if(!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(requestId))throw new ProvisionError('invalid_provision_request_id');
  let email:string;try{email=parseInvitation({email:operatorEmail}).email;}catch{throw new ProvisionError('invalid_operator_email');}
  return transaction(async db=>{
    const operator=(await db.query('SELECT o.user_id FROM platform_operators o JOIN users u ON u.id=o.user_id WHERE u.email=$1 AND o.revoked_at IS NULL FOR SHARE OF o',[email])).rows[0];
    if(!operator)throw new ProvisionError('platform_operator_required');
    // Never retain a fast hash of the password. Receipt replay never resets it.
    const hash=digest(canonical({version:1,name:spec.name,owner_email:spec.owner_email,operator_id:operator.user_id}));
    const old=(await db.query('SELECT tenant_id,owner_id,operator_id,request_hash FROM company_provision_requests WHERE id=$1',[requestId])).rows[0];
    if(old){if(old.request_hash!==hash||old.operator_id!==operator.user_id)throw new ProvisionError('provision_request_conflict');return{tenant_id:old.tenant_id,owner_id:old.owner_id,reused:true,credential_unchanged:true};}
    // Existing accounts must consent through company invitations. Provisioning
    // must not reset their password or silently add a company membership.
    if((await db.query('SELECT 1 FROM users WHERE email=$1',[spec.owner_email])).rowCount)throw new ProvisionError('owner_account_already_exists');
    const tenant=randomUUID(),owner=randomUUID();
    await db.query('INSERT INTO users(id,email,password_hash) VALUES($1,$2,$3)',[owner,spec.owner_email,passwordHash(spec.owner_password)]);
    await db.query('INSERT INTO tenants(id,name) VALUES($1,$2)',[tenant,spec.name]);
    await db.query("INSERT INTO tenant_memberships(tenant_id,user_id,role) VALUES($1,$2,'admin')",[tenant,owner]);
    await db.query('INSERT INTO company_provision_requests(id,operator_id,tenant_id,owner_id,request_hash) VALUES($1,$2,$3,$4,$5)',[requestId,operator.user_id,tenant,owner,hash]);
    await db.query("INSERT INTO platform_audit(operator_id,tenant_id,action,details) VALUES($1,$2,'company_provisioned',$3)",[operator.user_id,tenant,JSON.stringify({request_id:requestId,owner_id:owner})]);
    await db.query("INSERT INTO audit_logs(tenant_id,action,details) VALUES($1,'company_provisioned',$2)",[tenant,JSON.stringify({request_id:requestId,owner_id:owner})]);
    return{tenant_id:tenant,owner_id:owner,reused:false,credential_unchanged:true};
  });
}
