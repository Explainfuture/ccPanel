import type {PoolClient} from 'pg';
import {pool} from './db';

export function monthPartition(now:Date,offset:number){
  const from=new Date(Date.UTC(now.getUTCFullYear(),now.getUTCMonth()+offset,1));
  const to=new Date(Date.UTC(from.getUTCFullYear(),from.getUTCMonth()+1,1));
  return {name:'events_'+from.toISOString().slice(0,7).replace('-','_'),from:from.toISOString(),to:to.toISOString()};
}
function validBound(bound:string,from:string,to:string){
  const match=/^FOR VALUES FROM \('([^']+)'\) TO \('([^']+)'\)$/.exec(bound);
  return !!match&&Date.parse(match[1])===Date.parse(from)&&Date.parse(match[2])===Date.parse(to);
}

// No user-controlled identifiers. DDL waits for neither active writers nor
// migrations: contention rolls back and a later maintenance tick retries.
export async function maintainPartitions(db:PoolClient):Promise<'busy'|'completed'> {
  await db.query('BEGIN');
  try{
    await db.query("SET LOCAL statement_timeout='3s'");await db.query("SET LOCAL lock_timeout='200ms'");await db.query("SET LOCAL timezone='UTC'");
    if(!(await db.query('SELECT pg_try_advisory_xact_lock(169202610) AS held')).rows[0].held){await db.query('ROLLBACK');return 'busy';}
    await db.query('LOCK TABLE ONLY public.events IN ACCESS EXCLUSIVE MODE NOWAIT');
    const now=new Date((await db.query('SELECT CURRENT_TIMESTAMP AS now')).rows[0].now);
    const children=(await db.query(`SELECT c.relname AS name,pg_get_expr(c.relpartbound,c.oid) AS bound
      FROM pg_inherits i JOIN pg_class c ON c.oid=i.inhrelid JOIN pg_namespace n ON n.oid=c.relnamespace
      WHERE i.inhparent='public.events'::regclass AND n.nspname='public'`)).rows as {name:string;bound:string}[];
    for(let offset=0;offset<=2;offset++){
      const plan=monthPartition(now,offset),existing=children.find(c=>c.name===plan.name);
      if(existing){if(!validBound(existing.bound,plan.from,plan.to))throw new Error('partition_topology_mismatch');}
      else await db.query(`CREATE TABLE public.${plan.name} PARTITION OF public.events FOR VALUES FROM ('${plan.from}') TO ('${plan.to}')`);
    }
    // One old partition maximum per tick. Unknown names/bounds and non-empty
    // partitions are retained; row TTL cleanup must finish before reclamation.
    for(const child of children.sort((a,b)=>a.name.localeCompare(b.name))){
      const match=/^events_(\d{4})_(0[1-9]|1[0-2])$/.exec(child.name);if(!match)continue;
      const plan=monthPartition(new Date(`${match[1]}-${match[2]}-01T00:00:00.000Z`),0);
      if(!validBound(child.bound,plan.from,plan.to)||Date.parse(plan.to)>now.getTime()-30*86400000)continue;
      await db.query(`LOCK TABLE public.${child.name} IN ACCESS EXCLUSIVE MODE NOWAIT`);
      if((await db.query(`SELECT 1 FROM public.${child.name} LIMIT 1`)).rowCount)continue;
      await db.query(`DROP TABLE public.${child.name}`);break;
    }
    await db.query('COMMIT');return 'completed';
  }catch(error){await db.query('ROLLBACK');throw error;}
}

export function startPartitions(){
  if(process.env.RETENTION_ENABLED!=='true')return()=>{};
  let busy=false,stopped=false,next=0;
  const timer=setInterval(async()=>{
    if(stopped||busy||Date.now()<next)return;busy=true;let db:PoolClient|undefined;
    try{db=await pool.connect();const result=await maintainPartitions(db);next=Date.now()+(result==='busy'?60000:3600000);}
    catch{next=Date.now()+60000;console.error(JSON.stringify({code:'partition_maintenance_retry'}));}
    finally{db?.release();busy=false;}
  },15000);
  timer.unref();return()=>{stopped=true;clearInterval(timer);};
}
