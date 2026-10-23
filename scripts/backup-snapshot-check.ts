// Dedicated CI PG service only. A fresh source DB is supplied by the CI runner.
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {randomBytes,randomUUID} from 'node:crypto';
import {readFile} from 'node:fs/promises';
import pg from 'pg';
import {withBackupSnapshot,readBackupFingerprints} from '../packages/server/src/backup-snapshot';
assert.equal(process.env.GITHUB_ACTIONS,'true');
const source=new URL(process.env.DATABASE_URL!);
assert.ok(['127.0.0.1','localhost'].includes(source.hostname));assert.equal(source.username,'ccpanel_ci');
assert.match(source.pathname,/^\/ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$/);
const container=process.env.CCPANEL_PG_CONTAINER!;assert.match(container,/^[a-f0-9]{12,64}$/);
const restored='ccpanel_restore_'+new Date().toISOString().replace(/\D/g,'').slice(0,14)+'_'+randomBytes(4).toString('hex');
const target=new URL(source);target.pathname='/'+restored;
const adminUrl=new URL(source);adminUrl.pathname='/postgres';
const admin=new pg.Client({connectionString:adminUrl.href,statement_timeout:10000});
const writer=new pg.Client({connectionString:source.href,statement_timeout:5000,options:'-c timezone=UTC'});
const restoreDb=new pg.Client({connectionString:target.href,statement_timeout:10000,options:'-c timezone=UTC'});
let created=false,restoreConnected=false;
function command(args:string[],input?:Buffer,signal?:AbortSignal):Promise<{code:number|null;signal:NodeJS.Signals|null;data:Buffer}>{
  return new Promise((resolve,reject)=>{
    const child=spawn('docker',['exec','-i',container,...args],{stdio:['pipe','pipe','pipe']});
    const chunks:Buffer[]=[];let bytes=0,overflow=false;
    child.stdout.on('data',(chunk:Buffer)=>{bytes+=chunk.length;if(bytes>8*1024*1024){overflow=true;child.kill('SIGKILL');}else chunks.push(chunk);});
    // Keep diagnostics private: PG stderr can include identifiers/SQL.
    child.stderr.resume();child.stdin.on('error',()=>{});
    const cancel=()=>{child.kill('SIGKILL');};signal?.addEventListener('abort',cancel,{once:true});if(signal?.aborted)cancel();
    const timer=setTimeout(cancel,30000);
    child.once('error',error=>{clearTimeout(timer);signal?.removeEventListener('abort',cancel);reject(error);});
    child.once('close',(code,term)=>{clearTimeout(timer);signal?.removeEventListener('abort',cancel);if(overflow)reject(Error('backup_fixture_output_overflow'));else resolve({code,signal:term,data:Buffer.concat(chunks)});});
    child.stdin.end(input);
  });
}
const dump=(id:string,signal?:AbortSignal)=>command(['pg_dump','-U','ccpanel_ci','-d',source.pathname.slice(1),'--format=custom','--no-owner','--no-acl','--exclude-table-data=public.recovery_gate','--exclude-table-data=public.recovery_import_scopes','--snapshot='+id],undefined,signal);
try{
  await admin.connect();await writer.connect();
  assert.equal((await writer.query('SELECT count(*)::int AS n FROM tenants')).rows[0].n,0);
  const kept=randomUUID(),deleted=randomUUID(),later=randomUUID();
  await writer.query("INSERT INTO tenants(id,name) VALUES($1,'before'),($2,'deleted-after-snapshot')",[kept,deleted]);
  await admin.query(`CREATE DATABASE ${restored}`);created=true;
  let expired='';
  const result=await withBackupSnapshot(source.href,{expectedSchema:22},async snapshot=>{
    expired=snapshot.snapshotId;assert.equal(snapshot.sourceDatabase,source.pathname.slice(1));
    // Committed writes in another actual session, after snapshot creation.
    await writer.query('BEGIN');
    await writer.query("UPDATE tenants SET name='after' WHERE id=$1",[kept]);
    await writer.query('DELETE FROM tenants WHERE id=$1',[deleted]);
    await writer.query("INSERT INTO tenants(id,name) VALUES($1,'inserted-after-snapshot')",[later]);
    await writer.query('COMMIT');
    const live=await readBackupFingerprints(writer);assert.notDeepEqual(live,snapshot.fingerprints);
    const operatorSql=await readFile('scripts/backup-fingerprint.sql','utf8');
    const independently=await command(['psql','-X','-A','-t','-q','-v','ON_ERROR_STOP=1','-U','ccpanel_ci','-d',source.pathname.slice(1)],Buffer.from(`BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;\nSET TRANSACTION SNAPSHOT '${snapshot.snapshotId}';\nSET LOCAL TIME ZONE 'UTC';\n${operatorSql}\nROLLBACK;\n`),snapshot.signal);
    assert.equal(independently.code,0);assert.equal(independently.signal,null);
    assert.deepEqual(independently.data.toString('utf8').trim().split('\n'),snapshot.fingerprints,'new fingerprint implementation must match existing operator SQL');
    const archive=await dump(snapshot.snapshotId,snapshot.signal);assert.equal(archive.code,0);assert.equal(archive.signal,null);assert.ok(archive.data.length>0);
    const restoredResult=await command(['pg_restore','-U','ccpanel_ci','-d',restored,'--single-transaction','--exit-on-error','--no-owner','--no-acl'],archive.data,snapshot.signal);
    assert.equal(restoredResult.code,0);assert.equal(restoredResult.signal,null);
    await restoreDb.connect();restoreConnected=true;
    assert.deepEqual(await readBackupFingerprints(restoreDb),snapshot.fingerprints);
    assert.deepEqual((await restoreDb.query('SELECT id::text,name FROM tenants ORDER BY id')).rows,[{id:kept,name:'before'},{id:deleted,name:'deleted-after-snapshot'}].sort((a,b)=>a.id.localeCompare(b.id)));
    assert.equal((await restoreDb.query('SELECT count(*)::int AS n FROM recovery_gate')).rows[0].n,0,'old snapshot cannot restore an open recovery gate');
    return {relations:snapshot.fingerprints.length,bytes:archive.data.length};
  });
  const invalid=await dump(expired);assert.notEqual(invalid.code,0,'exported snapshot must expire after transaction ends');
  await assert.rejects(withBackupSnapshot(source.href,{expectedSchema:999},async()=>{throw Error('must not run');}),/backup_schema_mismatch/);
  await assert.rejects(withBackupSnapshot(source.href,{expectedSchema:22,timeoutMs:1000},async snapshot=>new Promise((_resolve,reject)=>snapshot.signal.addEventListener('abort',()=>reject(Error('aborted')),{once:true}))),/backup_snapshot_timeout/);
  assert.equal((await admin.query("SELECT count(*)::int AS n FROM pg_stat_activity WHERE application_name='ccpanel_backup_snapshot'")).rows[0].n,0);
  console.log(JSON.stringify({passed:true,shared_pg_snapshot:true,concurrent_insert_update_delete_committed:true,live_fingerprint_differs:true,existing_operator_sql_matches:true,actual_pg_dump_restore:true,restored_fingerprints_exact:true,recovery_gate_rows:0,expired_snapshot_rejected:true,schema_mismatch_rejected:true,timeout_aborted_callback:true,exporter_sessions_remaining:0,...result}));
}finally{
  if(restoreConnected)await restoreDb.end();await writer.end();
  if(created)await admin.query(`DROP DATABASE ${restored} WITH (FORCE)`);await admin.end();
}
