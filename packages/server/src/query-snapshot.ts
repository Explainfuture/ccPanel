import type {PoolClient,QueryResultRow} from 'pg';
import {pool,type query} from './db';
import {lockProjectPrivacy} from './privacy';

export class QuerySnapshotError extends Error{
  constructor(readonly status:404|429|504,message:string){super(message);}
}
// Call immediately after BEGIN ISOLATION LEVEL REPEATABLE READ. The row lock
// detects a policy commit after the snapshot but before the advisory lock:
// PostgreSQL raises 40001 rather than allowing the old row to be returned.
export async function lockQuerySnapshot(db:PoolClient,tenant:string,project:string){
  await lockProjectPrivacy(db,project);
  const row=await db.query('SELECT privacy_epoch FROM projects WHERE tenant_id=$1 AND id=$2 FOR SHARE',[tenant,project]);
  if(!row.rowCount)throw new QuerySnapshotError(404,'not_found');
  return row.rows[0].privacy_epoch as number;
}
export async function withQuerySnapshot<T>(tenant:string,project:string,work:(run:typeof query)=>Promise<T>):Promise<T>{
  const db=await pool.connect(),deadline=performance.now()+5000;
  try{
    await db.query('BEGIN ISOLATION LEVEL REPEATABLE READ');
    await db.query("SET LOCAL lock_timeout='200ms'");
    await db.query("SET LOCAL statement_timeout='5000ms'");
    await lockQuerySnapshot(db,tenant,project);
    // Serialize Promise.all callers on this connection and bound the entire
    // read, including lock acquisition, rather than allocating 5s per query.
    let tail:Promise<unknown>=Promise.resolve();
    const run=<R extends QueryResultRow=QueryResultRow>(sql:string,values:unknown[]=[])=>{
      const pending=tail.then(async()=>{
        const remaining=Math.floor(deadline-performance.now());
        if(remaining<=0)throw new QuerySnapshotError(504,'query_timeout');
        await db.query("SELECT set_config('statement_timeout',$1,true)",[String(remaining)]);
        return db.query<R>(sql,values);
      });
      tail=pending;return pending;
    };
    const result=await work(run);await tail;await db.query('COMMIT');return result;
  }catch(error){
    await db.query('ROLLBACK');
    if(['55P03','40001','40P01'].includes((error as {code?:string}).code??''))throw new QuerySnapshotError(429,'query_busy');
    if((error as {code?:string}).code==='57014')throw new QuerySnapshotError(504,'query_timeout');
    throw error;
  }finally{db.release();}
}
