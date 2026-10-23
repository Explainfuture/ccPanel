import {pool} from './db';
import {lockProjectPrivacy} from './privacy';
import {lockMembership,membershipAdmin} from './invitations';
import {parseRetentionUpdate,planRetentionChange,retentionLimits} from './retention-policy';
// Internal service only until all public read/replay paths and UI are verified.
export class ProjectRetentionError extends Error{constructor(readonly status:403|404|409,message:string){super(message);}}
export async function changeProjectRetention(tenant:string,project:string,actor:string,input:unknown){
  parseRetentionUpdate(input);const db=await pool.connect();
  try{
    await db.query('BEGIN');await db.query("SELECT set_config('lock_timeout','200ms',true),set_config('statement_timeout','2000ms',true)");
    await lockMembership(db,tenant);
    if(!await membershipAdmin(db,tenant,actor))throw new ProjectRetentionError(403,'forbidden');
    await lockProjectPrivacy(db,project,true);
    const row=(await db.query('SELECT retention_days AS raw_days,retention_version AS version,retention_floor AS floor,clock_timestamp() AS now FROM projects WHERE tenant_id=$1 AND id=$2 FOR UPDATE',[tenant,project])).rows[0];
    if(!row)throw new ProjectRetentionError(404,'not_found');
    const current={raw_days:row.raw_days,version:row.version,floor:row.floor?.toISOString()??null};
    const plan=planRetentionChange(current,input,row.now.getTime());
    if(plan.changed){
      await db.query('UPDATE projects SET retention_days=$3,retention_version=$4,retention_floor=$5,privacy_epoch=privacy_epoch+1 WHERE tenant_id=$1 AND id=$2',[tenant,project,plan.next.raw_days,plan.next.version,plan.next.floor]);
      await db.query("INSERT INTO audit_logs(tenant_id,user_id,action,details) VALUES($1,$2,'retention_policy_changed',$3)",[tenant,actor,JSON.stringify({project_id:project,before:current,after:plan.next})]);
    }
    await db.query('COMMIT');return{...plan,limits:retentionLimits};
  }catch(error){await db.query('ROLLBACK');if((error as {code?:string}).code==='55P03')throw new ProjectRetentionError(409,'retention_policy_busy');throw error;}finally{db.release();}
}
