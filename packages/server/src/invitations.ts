import type {Hono} from 'hono';
import type {PoolClient} from 'pg';
import type {Env} from '../../../apps/api/src/app';
import {pool,query} from './db';
import {randomUUID} from 'node:crypto';
import {digest,token} from './security';
import {storageSafeText} from '../../schema/src/index';
type Role='admin'|'analyst'|'viewer';
export class InvitationError extends Error{constructor(public status:400|403|404|409|429,message:string){super(message);}}
export function parseInvitation(input:unknown):{email:string;role:Role}{
  if(!input||typeof input!=='object'||Array.isArray(input))throw new InvitationError(400,'invalid_invitation');
  const b=input as Record<string,unknown>,email=typeof b.email==='string'?b.email.trim().toLowerCase():'',role=b.role??'analyst';
  if(Object.keys(b).some(k=>!['email','role'].includes(k))||email.length>254||!storageSafeText(email)||!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)||typeof role!=='string'||!['admin','analyst','viewer'].includes(role))throw new InvitationError(400,'invalid_invitation');
  return{email,role:role as Role};
}
export function parseInvitationToken(input:unknown){
  if(!input||typeof input!=='object'||Array.isArray(input)||Object.keys(input).length!==1||!('token'in input)||typeof input.token!=='string'||!/^[A-Za-z0-9_-]{43}$/.test(input.token))throw new InvitationError(400,'invalid_invitation');
  return input.token;
}
export async function lockMembership(db:PoolClient,tenant:string){await db.query('SELECT pg_advisory_xact_lock(hashtextextended($1,0))',['ccpanel:members:'+tenant.toLowerCase()]);}
export async function membershipAdmin(db:PoolClient,tenant:string,user:string){return(await db.query("SELECT 1 FROM tenant_memberships WHERE tenant_id=$1 AND user_id=$2 AND role='admin' FOR SHARE",[tenant,user])).rowCount===1;}
async function transaction<T>(run:(db:PoolClient)=>Promise<T>){
  const db=await pool.connect();try{await db.query('BEGIN');const result=await run(db);await db.query('COMMIT');return result;}catch(error){await db.query('ROLLBACK');throw error;}finally{db.release();}
}
const unavailable=()=>new InvitationError(404,'not_found');
export async function createInvitation(tenant:string,actor:string,input:unknown){
  const spec=parseInvitation(input),secret=token(),id=randomUUID();
  return transaction(async db=>{
    await lockMembership(db,tenant);if(!await membershipAdmin(db,tenant,actor))throw new InvitationError(403,'forbidden');
    if((await db.query('SELECT 1 FROM tenant_memberships m JOIN users u ON u.id=m.user_id WHERE m.tenant_id=$1 AND u.email=$2',[tenant,spec.email])).rowCount)throw new InvitationError(409,'already_member');
    const active=(await db.query('SELECT count(*)::int AS n FROM tenant_invitations WHERE tenant_id=$1 AND email<>$2 AND accepted_at IS NULL AND revoked_at IS NULL AND expires_at>clock_timestamp()',[tenant,spec.email])).rows[0].n;
    if(active>=100)throw new InvitationError(429,'invitation_limit');
    // A replacement invalidates the earlier link, including expired links.
    await db.query('UPDATE tenant_invitations SET revoked_at=clock_timestamp() WHERE tenant_id=$1 AND email=$2 AND accepted_at IS NULL AND revoked_at IS NULL',[tenant,spec.email]);
    const row=(await db.query('INSERT INTO tenant_invitations(id,tenant_id,email,role,token_hash,created_by) VALUES($1,$2,$3,$4,$5,$6) RETURNING id,email,role,expires_at',[id,tenant,spec.email,spec.role,digest(secret),actor])).rows[0];
    await db.query("INSERT INTO audit_logs(tenant_id,user_id,action,details) VALUES($1,$2,'invitation_created',$3)",[tenant,actor,JSON.stringify({invitation_id:id,role:spec.role})]);
    return{...row,token:secret};
  });
}
export async function revokeInvitation(tenant:string,actor:string,id:string){
  if(!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(id))throw unavailable();
  return transaction(async db=>{
    await lockMembership(db,tenant);if(!await membershipAdmin(db,tenant,actor))throw new InvitationError(403,'forbidden');
    if(!(await db.query('UPDATE tenant_invitations SET revoked_at=clock_timestamp() WHERE tenant_id=$1 AND id=$2 AND accepted_at IS NULL AND revoked_at IS NULL RETURNING id',[tenant,id])).rowCount)throw unavailable();
    await db.query("INSERT INTO audit_logs(tenant_id,user_id,action,details) VALUES($1,$2,'invitation_revoked',$3)",[tenant,actor,JSON.stringify({invitation_id:id})]);return{ok:true};
  });
}
export async function acceptInvitation(user:string,input:unknown){
  const hash=digest(parseInvitationToken(input));
  return transaction(async db=>{
    // Locate using the hash but reveal nothing until authenticated email matches.
    const hint=(await db.query('SELECT tenant_id FROM tenant_invitations WHERE token_hash=$1',[hash])).rows[0];if(!hint)throw unavailable();
    await lockMembership(db,hint.tenant_id);
    const row=(await db.query('SELECT i.*,i.expires_at>clock_timestamp() AS fresh,u.email AS user_email FROM tenant_invitations i JOIN users u ON u.id=$2 WHERE i.token_hash=$1 FOR UPDATE OF i',[hash,user])).rows[0];
    if(!row||row.email!==row.user_email||row.revoked_at)throw unavailable();
    const existing=(await db.query('SELECT role FROM tenant_memberships WHERE tenant_id=$1 AND user_id=$2',[row.tenant_id,user])).rows[0];
    if(row.accepted_at){if(row.accepted_by!==user||!existing)throw unavailable();return{tenant_id:row.tenant_id,role:existing.role,already_accepted:true};}
    if(!row.fresh||!row.created_by||!await membershipAdmin(db,row.tenant_id,row.created_by))throw unavailable();
    // Accepting a stale invitation cannot upgrade an already-existing role.
    await db.query('INSERT INTO tenant_memberships(tenant_id,user_id,role) VALUES($1,$2,$3) ON CONFLICT DO NOTHING',[row.tenant_id,user,row.role]);
    const role=existing?.role??row.role;
    await db.query('UPDATE tenant_invitations SET accepted_at=clock_timestamp(),accepted_by=$2 WHERE id=$1',[row.id,user]);
    await db.query("INSERT INTO audit_logs(tenant_id,user_id,action,details) VALUES($1,$2,'invitation_accepted',$3)",[row.tenant_id,user,JSON.stringify({invitation_id:row.id,role})]);
    return{tenant_id:row.tenant_id,role,already_accepted:false};
  });
}
export async function previewInvitation(user:string,input:unknown){
  const hash=digest(parseInvitationToken(input));
  const row=(await query(`SELECT t.name AS company_name,i.email,coalesce(m.role,i.role) AS role,i.expires_at,i.accepted_at IS NOT NULL AS already_accepted
    FROM tenant_invitations i JOIN tenants t ON t.id=i.tenant_id JOIN users u ON u.id=$2 AND u.email=i.email
    LEFT JOIN tenant_memberships m ON m.tenant_id=i.tenant_id AND m.user_id=$2
    WHERE i.token_hash=$1 AND i.revoked_at IS NULL AND (
      (i.accepted_at IS NOT NULL AND i.accepted_by=$2 AND m.user_id IS NOT NULL) OR
      (i.accepted_at IS NULL AND i.expires_at>clock_timestamp() AND EXISTS(SELECT 1 FROM tenant_memberships a WHERE a.tenant_id=i.tenant_id AND a.user_id=i.created_by AND a.role='admin'))
    )`,[hash,user])).rows[0];
  if(!row)throw unavailable();return row;
}
export function registerInvitationAcceptance(app:Hono<Env>,rate:(key:string,max:number,interval:number)=>boolean){
  app.post('/api/invitations/preview',async c=>{
    if(!rate('invitation-preview:'+c.get('userId'),30,60000))return c.json({error:'rate_limited'},429);
    try{return c.json(await previewInvitation(c.get('userId'),await c.req.json().catch(()=>null)));}
    catch(error){if(error instanceof InvitationError)return c.json({error:error.message},error.status);throw error;}
  });
  app.post('/api/invitations/accept',async c=>{
    if(!rate('invitation-accept:'+c.get('userId'),20,60000))return c.json({error:'rate_limited'},429);
    try{return c.json(await acceptInvitation(c.get('userId'),await c.req.json().catch(()=>null)));}
    catch(error){if(error instanceof InvitationError)return c.json({error:error.message},error.status);throw error;}
  });
}
export function registerCompanyInvitations(app:Hono<Env>,rate:(key:string,max:number,interval:number)=>boolean){
  const path='/api/tenants/:tenant/invitations';
  app.get(path,async c=>{
    if(c.get('role')!=='admin')return c.json({error:'forbidden'},403);
    return c.json({invitations:(await query("SELECT id,email,role,created_at,expires_at,accepted_at,revoked_at FROM tenant_invitations WHERE tenant_id=$1 ORDER BY created_at DESC LIMIT 100",[c.get('tenantId')])).rows});
  });
  app.post(path,async c=>{
    if(c.get('role')!=='admin')return c.json({error:'forbidden'},403);
    if(!rate('invitation-create:'+c.get('tenantId')+':'+c.get('userId'),20,60000))return c.json({error:'rate_limited'},429);
    try{return c.json(await createInvitation(c.get('tenantId'),c.get('userId'),await c.req.json().catch(()=>null)),201);}
    catch(error){if(error instanceof InvitationError)return c.json({error:error.message},error.status);throw error;}
  });
  app.delete(path+'/:invitation',async c=>{
    if(c.get('role')!=='admin')return c.json({error:'forbidden'},403);
    try{return c.json(await revokeInvitation(c.get('tenantId'),c.get('userId'),c.req.param('invitation')||''));}
    catch(error){if(error instanceof InvitationError)return c.json({error:error.message},error.status);throw error;}
  });
}
