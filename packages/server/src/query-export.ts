import type {Hono} from 'hono';
import type {Env} from '../../../apps/api/src/app';
import {pool} from './db';
import {lockProjectPrivacy} from './privacy';
import {reportId} from './report-spec';
import {ExportError,formatQueryExport} from './query-export-format';
async function prepare(tenant:string,project:string,actor:string,id:string,format:'json'|'csv'){
  const db=await pool.connect();try{
    await db.query('BEGIN');await db.query("SET LOCAL statement_timeout='2000ms'");await db.query("SET LOCAL lock_timeout='2000ms'");
    if(!(await db.query('SELECT pg_try_advisory_xact_lock(169202634) AS held')).rows[0].held)throw new ExportError(429,'export_busy');
    const member=(await db.query('SELECT role FROM tenant_memberships WHERE tenant_id=$1 AND user_id=$2 FOR SHARE',[tenant,actor])).rows[0];
    if(!member)throw new ExportError(404,'not_found');if(member.role==='viewer')throw new ExportError(403,'forbidden');
    await lockProjectPrivacy(db,project);
    const job=(await db.query('SELECT j.* FROM query_jobs j JOIN projects p ON p.tenant_id=j.tenant_id AND p.id=j.project_id AND p.privacy_epoch=j.privacy_epoch WHERE j.tenant_id=$1 AND j.project_id=$2 AND j.id=$3 AND j.expires_at>clock_timestamp() FOR SHARE OF j',[tenant,project,id])).rows[0];
    if(!job)throw new ExportError(404,'not_found');
    if(!(await db.query('SELECT 1 FROM tenant_memberships WHERE tenant_id=$1 AND user_id=$2 FOR SHARE',[tenant,job.user_id])).rowCount)throw new ExportError(404,'not_found');
    if(job.status!=='succeeded'||job.result===null)throw new ExportError(409,'query_not_ready');
    const body=formatQueryExport({format_version:1,tenant_id:tenant,project_id:project,job_id:job.id,created_at:job.created_at,finished_at:job.finished_at,exported_at:new Date().toISOString(),spec:job.spec,report_snapshot:job.report_id?{id:job.report_id,version:job.report_version,timezone:job.timezone}:null,result:job.result},format);
    await db.query("INSERT INTO audit_logs(tenant_id,user_id,action,details) VALUES($1,$2,'query_export_prepared',$3)",[tenant,actor,JSON.stringify({project_id:project,job_id:id,format,bytes:Buffer.byteLength(body)})]);
    await db.query('COMMIT');return body;
  }catch(error){await db.query('ROLLBACK');throw error;}finally{db.release();}
}
export function registerQueryExports(app:Hono<Env>,rate:(key:string,max:number,interval:number)=>boolean){
  let active=false;
  app.get('/api/tenants/:tenant/projects/:project/query-jobs/:job/download',async c=>{
    if(c.get('role')==='viewer')return c.json({error:'forbidden'},403);
    const id=c.req.param('job')!,parameters=new URL(c.req.url).searchParams;
    if(!reportId(id))return c.json({error:'not_found'},404);
    const format=parameters.has('format')?parameters.get('format')!:'json';
    if(!['json','csv'].includes(format)||parameters.getAll('format').length>1||[...parameters.keys()].some(key=>key!=='format'))return c.json({error:'invalid_export_format'},400);
    if(active||!rate('export:'+c.get('tenantId')+':'+c.get('userId'),10,60000)){c.header('Retry-After','5');return c.json({error:'export_busy'},429);}
    active=true;let handedOff=false;
    try{
      const text=await prepare(c.get('tenantId'),c.req.param('project')!,c.get('userId'),id.toLowerCase(),format as 'json'|'csv');
      const bytes=new TextEncoder().encode(text);let offset=0,finished=false;
      let controller:ReadableStreamDefaultController<Uint8Array>;
      // Node's response finish/close covers the actual adapter write, not just
      // a prefetched final stream chunk. Standalone Fetch tests use stream EOF.
      const outgoing=(c.env as {outgoing?:{once:(event:string,fn:()=>void)=>unknown;off:(event:string,fn:()=>void)=>unknown;destroy:(error?:Error)=>unknown}}|undefined)?.outgoing;
      const finish=()=>{if(!finished){finished=true;clearTimeout(timer);outgoing?.off('finish',finish);outgoing?.off('close',finish);active=false;}};
      const timer=setTimeout(()=>{const error=new Error('export_stream_timeout');controller.error(error);outgoing?.destroy(error);finish();},10000);timer.unref();
      outgoing?.once('finish',finish);outgoing?.once('close',finish);
      const stream=new ReadableStream<Uint8Array>({start(value){controller=value;},pull(value){if(offset>=bytes.length){value.close();if(!outgoing)finish();return;}value.enqueue(bytes.subarray(offset,offset+=Math.min(65536,bytes.length-offset)));},cancel(){outgoing?.destroy();finish();}});
      handedOff=true;
      return c.body(stream,200,{'Content-Type':format==='csv'?'text/csv; charset=utf-8':'application/json; charset=utf-8','Content-Disposition':`attachment; filename="ccpanel-${id.toLowerCase()}.${format}"`,'Cache-Control':'no-store','X-Content-Type-Options':'nosniff','Content-Length':String(bytes.length)});
    }catch(error){
      if(error instanceof ExportError){if(error.status===429)c.header('Retry-After','5');return c.json({error:error.message},error.status);}
      if(['57014','55P03'].includes((error as {code?:string}).code||'')){c.header('Retry-After','2');return c.json({error:'export_busy'},503);}throw error;
    }finally{if(!handedOff)active=false;}
  });
}
