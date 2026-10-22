// Dedicated CI PostgreSQL service only. Never accepts DATABASE_URL as input.
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {randomBytes} from 'node:crypto';
import {mkdir,mkdtemp,rm} from 'node:fs/promises';
import path from 'node:path';
import pg from 'pg';
import {build} from 'esbuild';
assert.equal(process.env.GITHUB_ACTIONS,'true','CI service only');
const url=new URL(process.env.CCPANEL_PG_TEST_URL??'');
assert.equal(url.protocol,'postgresql:');assert.ok(['localhost','127.0.0.1'].includes(url.hostname));
assert.equal(url.pathname,'/postgres');assert.equal(url.username,'ccpanel_ci');assert.equal(url.search,'');
await mkdir('.local',{recursive:true});const root=path.resolve('.local'),dir=await mkdtemp(path.join(root,'ci-postgres-'));
const admin=new pg.Client({connectionString:url.href,connectionTimeoutMillis:5000,statement_timeout:10000});
function child(file,env,input='',args=[]){
  return new Promise((resolve,reject)=>{
    const proc=spawn(process.execPath,[path.join(dir,file+'.mjs'),...args],{env,stdio:['pipe','pipe','pipe']});
    let out='',err='',size=0,overflow=false;
    const collect=which=>chunk=>{size+=chunk.length;if(size>9*1024*1024){overflow=true;proc.kill('SIGKILL');return;}if(which==='out')out+=chunk;else err+=chunk;};
    proc.stdout.on('data',collect('out'));proc.stderr.on('data',collect('err'));proc.stdin.on('error',()=>{});
    const timer=setTimeout(()=>proc.kill('SIGKILL'),120000);
    proc.once('error',error=>{clearTimeout(timer);reject(error);});
    proc.once('close',code=>{clearTimeout(timer);if(code!==0||overflow)reject(new Error(`${file} failed: ${err.slice(-12000)} ${out.slice(-12000)}`));else resolve(out);});
    proc.stdin.end(input);
  });
}
try{
  const entries={migrate:'packages/server/src/migrate.ts','deletion-ledger':'packages/server/src/deletion-ledger.ts'};
  const suites=['company-provision','report-edit','query-export','retention','partitions'];
  for(const suite of suites)entries[suite]=`scripts/${suite}-check.ts`;
  for(const[name,entry]of Object.entries(entries))await build({entryPoints:[entry],outfile:path.join(dir,name+'.mjs'),bundle:true,platform:'node',target:'node22',format:'esm',packages:'external'});
  await admin.connect();
  for(const suite of suites){
    const database='ccpanel_restore_'+new Date().toISOString().replace(/\D/g,'').slice(0,14)+'_'+randomBytes(4).toString('hex');
    assert.match(database,/^ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$/);let created=false;
    const dburl=new URL(url);dburl.pathname='/'+database;
    const env={...process.env,DATABASE_URL:dburl.href,PUBLIC_ORIGIN:'https://ci.ccpanel.invalid',RETENTION_ENABLED:'false'};
    delete env.CCPANEL_PROGRESS_BOOTSTRAP_REASON;delete env.CCPANEL_RECOVERY_LEDGER;delete env.CCPANEL_OFFLINE_RESTORE;
    try{
      await admin.query(`CREATE DATABASE ${database}`);created=true;
      await child('migrate',env);
      // Export from this freshly created empty source, never from a restored DB.
      const ledger=await child('deletion-ledger',env,'',['export']);
      assert.ok(ledger.length>0);
      const parsed=JSON.parse(ledger);assert.equal(parsed.schema_version,1);assert.equal(parsed.projects.length,0);
      await child('deletion-ledger',{...env,CCPANEL_OFFLINE_RESTORE:'true'},ledger,['apply']);
      process.stdout.write(await child(suite,env));
    }finally{if(created)await admin.query(`DROP DATABASE ${database} WITH (FORCE)`);}
  }
}finally{
  await admin.end();
  assert.equal(path.dirname(dir),root);assert.ok(path.basename(dir).startsWith('ci-postgres-'));await rm(dir,{recursive:true,force:true});
}
