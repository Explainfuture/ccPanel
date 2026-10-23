import {pool} from './db';
const uuid=/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
export class OperationalReadError extends Error{constructor(readonly status:400|403|404){super('operational_history_read_denied');}}
export function parseOperationalHistoryQuery(params:URLSearchParams){
  for(const key of params.keys())if(!['source','cursor'].includes(key)||params.getAll(key).length!==1)throw new OperationalReadError(400);
  const source=params.get('source');if(source!==null&&!['api','worker'].includes(source))throw new OperationalReadError(400);
  let cursor:null|[string,string,string]=null;
  if(params.has('cursor')){
    const raw=params.get('cursor')!;
    try{
      if(raw.length>256||!raw||!/^[A-Za-z0-9_-]+$/.test(raw))throw 0;
      const bytes=Buffer.from(raw,'base64url');if(bytes.toString('base64url')!==raw)throw 0;
      const value:unknown=JSON.parse(bytes.toString('utf8'));
      if(!Array.isArray(value)||value.length!==3||!value.every(v=>typeof v==='string'))throw 0;
      const [time,kind,id]=value as [string,string,string];
      if(!/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}Z$/.test(time)||new Date(time).toISOString()!==time.slice(0,23)+'Z'||!['api','worker'].includes(kind)||!uuid.test(id)||(source&&source!==kind))throw 0;
      cursor=[time,kind,id];
    }catch{throw new OperationalReadError(400);}
  }
  return{source,cursor};
}
export async function readOperationalHistory(user:string,tenant:string,project:string,params:URLSearchParams){
  const {source,cursor}=parseOperationalHistoryQuery(params),db=await pool.connect();
  try{
    await db.query('BEGIN');
    await db.query("SELECT set_config('lock_timeout','200ms',true),set_config('statement_timeout','1000ms',true)");
    const membership=(await db.query('SELECT m.role FROM tenant_memberships m JOIN projects p ON p.tenant_id=m.tenant_id WHERE m.tenant_id=$1 AND m.user_id=$2 AND p.id=$3 FOR SHARE OF m,p',[tenant,user,project])).rows[0];
    if(!membership)throw new OperationalReadError(404);if(membership.role!=='admin')throw new OperationalReadError(403);
    if(!(await db.query('SELECT 1 FROM platform_operators WHERE user_id=$1 AND revoked_at IS NULL FOR SHARE',[user])).rowCount)throw new OperationalReadError(403);
    const rows=(await db.query(`SELECT source,instance_id,started_at,observed_at,snapshot,to_char(observed_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS cursor_time
      FROM operational_samples WHERE observed_at>=CURRENT_TIMESTAMP-interval '7 days' AND observed_at<=CURRENT_TIMESTAMP+interval '5 seconds'
      AND ($1::text IS NULL OR source=$1) AND source IN ('api','worker')
      AND ($2::timestamptz IS NULL OR (observed_at,source,instance_id)<($2::timestamptz,$3::text,$4::uuid))
      ORDER BY observed_at DESC,source DESC,instance_id DESC LIMIT 51`,[source,...(cursor??[null,null,null])])).rows;
    const page=rows.slice(0,50),last=page.at(-1);
    const next=rows.length>50&&last?Buffer.from(JSON.stringify([last.cursor_time,last.source,last.instance_id])).toString('base64url'):null;
    await db.query('COMMIT');
    return{scope:'platform_aggregate' as const,computed_at:new Date().toISOString(),retention_days:7,page_limit:50,samples:page.map(({cursor_time,...row})=>row),next_cursor:next};
  }catch(error){await db.query('ROLLBACK');throw error;}finally{db.release();}
}
