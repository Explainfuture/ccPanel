import type {Hono} from 'hono';
import {bodyLimit} from 'hono/body-limit';
import type {Env} from '../../../apps/api/src/app';
import {readProjectRetention,changeProjectRetention} from './project-retention';
export function registerRetentionRoutes(app:Hono<Env>,rate:(key:string,max:number,interval:number)=>boolean){
  const path='/api/tenants/:tenant/projects/:project/retention';
  app.use(path,async(c,next)=>{
    if(new URL(c.req.url).search)return c.json({error:'invalid_retention_policy'},400);
    await next();
  });
  app.get(path,async c=>c.json(await readProjectRetention(c.get('tenantId'),c.req.param('project')!,c.get('userId'))));
  app.patch(path,bodyLimit({maxSize:1024,onError:c=>c.json({error:'retention_request_too_large'},413)}),async c=>{
    if(c.get('role')!=='admin')return c.json({error:'forbidden'},403);
    if(!rate('retention:'+c.get('tenantId')+':'+c.get('userId'),10,60000)){c.header('Retry-After','60');return c.json({error:'retention_rate_limited'},429);}
    const result=await changeProjectRetention(c.get('tenantId'),c.req.param('project')!,c.get('userId'),await c.req.json().catch(()=>null));
    return c.json({changed:result.changed,policy:{...result.next,effective_from:result.next_cutoff},observed_at:result.observed_at,limits:result.limits});
  });
}
