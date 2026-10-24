// Actual candidate CLI: dedicated CI service or explicitly guarded VPS fixture.
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {randomBytes,randomUUID,createHash} from 'node:crypto';
import {mkdtemp,writeFile,readFile,readdir,stat,unlink,rmdir} from 'node:fs/promises';
import {createReadStream} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,dirname,isAbsolute} from 'node:path';
import pg from 'pg';
import {readBackupFingerprints} from '../packages/server/src/backup-snapshot';
const vps=process.env.CCPANEL_ISOLATED_CAPTURE_VPS==='true';
const source=new URL(process.env.DATABASE_URL!);assert.match(source.pathname,/^\/ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$/);
const container=process.env.CCPANEL_PG_CONTAINER!;
if(vps){
  assert.equal(process.env.CCPANEL_EXPECTED_RELEASE,'m4-sdk-013');assert.equal(process.version,'v22.22.3');assert.equal(source.username,'ccpanel');
  assert.match(source.hostname,/^172\.(1[6-9]|2\d|3[01])\.\d{1,3}\.\d{1,3}$/);assert.equal(container,'ccpanel-postgres-1');
  assert.equal(process.env.CCPANEL_CAPTURE_VPS_READY,'true');
}else{assert.equal(process.env.GITHUB_ACTIONS,'true');assert.equal(source.username,'ccpanel_ci');assert.ok(['127.0.0.1','localhost'].includes(source.hostname));assert.match(container,/^[a-f0-9]{12,64}$/);}
assert.ok(isAbsolute(process.env.CCPANEL_CAPTURE_CLI!));
const cryptoScript=join(dirname(process.env.CCPANEL_CAPTURE_CLI!),'encrypt-backup.mjs');
const directory=await mkdtemp(join(tmpdir(),'ccpanel-capture-cli-')),candidate=join(directory,'candidate'),key=join(directory,'key'),plain=join(directory,'authenticated.dump');
const targetName=vps?process.env.CCPANEL_CAPTURE_RESTORE_DB!:'ccpanel_restore_'+new Date().toISOString().replace(/\D/g,'').slice(0,14)+'_'+randomBytes(4).toString('hex');
assert.match(targetName,/^ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$/);
assert.notEqual('/'+targetName,source.pathname);
const targetUrl=new URL(source);targetUrl.pathname='/'+targetName;const adminUrl=new URL(source);adminUrl.pathname='/postgres';
const admin=new pg.Client({connectionString:adminUrl.href}),db=new pg.Client({connectionString:source.href,options:'-c timezone=UTC'}),restored=new pg.Client({connectionString:targetUrl.href,options:'-c timezone=UTC'});
let targetCreated=false,restoredConnected=false,authenticated=false,captured=false;
function run(file:string,args:string[],env=process.env,inputPath?:string):Promise<{code:number|null;stdout:string}>{
  return new Promise((resolve,reject)=>{
    const child=spawn(file,args,{env,stdio:['pipe','pipe','pipe']});let stdout='',size=0,failed=false;const input=inputPath?createReadStream(inputPath):undefined;
    child.stdout.on('data',chunk=>{size+=chunk.length;if(size>65536){failed=true;child.kill('SIGKILL');}else stdout+=chunk;});child.stderr.resume();child.stdin.on('error',()=>{});
    const timer=setTimeout(()=>{failed=true;child.kill('SIGKILL');},45000);
    child.once('error',error=>{clearTimeout(timer);input?.destroy();reject(error);});
    child.once('close',code=>{clearTimeout(timer);input?.destroy();if(failed)reject(Error('capture_cli_fixture_failed'));else resolve({code,stdout});});
    if(input){input.on('error',()=>{failed=true;child.kill('SIGKILL');});input.pipe(child.stdin);}else child.stdin.end();
  });
}
const capture=(output=candidate,schema='22')=>run(process.execPath,[process.env.CCPANEL_CAPTURE_CLI!,output,key,schema],{...process.env,CCPANEL_BACKUP_DATABASE_URL:source.href,CCPANEL_BACKUP_PG_CONTAINER:container,CCPANEL_BACKUP_MAX_BYTES:'8388608',CCPANEL_BACKUP_TIMEOUT_MS:'30000'});
try{
  await admin.connect();await db.connect();const tenant=randomUUID();await db.query("INSERT INTO tenants(id,name) VALUES($1,'CANDIDATE CLI FIXTURE')",[tenant]);
  await writeFile(key,randomBytes(32),{mode:0o600,flag:'wx'});
  const result=await capture();assert.equal(result.code,2,'capture is explicitly NOT a verified backup');captured=true;
  const receipt=JSON.parse(result.stdout);assert.equal(receipt.status,'captured_unverified');assert.equal(receipt.recoveryVerified,false);assert.equal(receipt.offhostVerified,false);assert.equal(receipt.latestStateVerified,false);
  assert.deepEqual((await readdir(candidate)).sort(),['capture.json','database.dump.enc']);
  const rawManifest=await readFile(join(candidate,'capture.json'),'utf8'),manifest=JSON.parse(rawManifest);
  assert.equal(manifest.version,1);assert.equal(manifest.status,'captured_unverified');assert.equal(manifest.source.releaseVerified,false);assert.equal(manifest.source.schemaVersion,22);assert.equal(manifest.source.database,source.pathname.slice(1));
  for(const field of ['recoveryVerified','offhostVerified','latestStateVerified'])assert.equal(manifest[field],false);
  assert.ok(!rawManifest.includes(source.password)&&!rawManifest.includes(key));
  assert.deepEqual(manifest.fingerprints,await readBackupFingerprints(db));
  const cipher=join(candidate,manifest.archive.file);assert.equal(manifest.archive.file,'database.dump.enc');assert.equal(manifest.archive.format,'CCPBK001');
  const hash=createHash('sha256');for await(const chunk of createReadStream(cipher))hash.update(chunk);assert.equal(hash.digest('hex'),manifest.archive.sha256);assert.equal(receipt.sha256,manifest.archive.sha256);assert.equal((await stat(cipher)).size,manifest.archive.bytes);
  assert.equal((await stat(candidate)).mode&0o777,0o700);assert.equal((await stat(cipher)).mode&0o777,0o600);assert.equal((await stat(join(candidate,'capture.json'))).mode&0o777,0o600);
  const again=await capture();assert.equal(again.code,1);assert.equal(await readFile(join(candidate,'capture.json'),'utf8'),rawManifest);
  const badOutput=join(directory,'schema-mismatch');const mismatch=await capture(badOutput,'999');assert.equal(mismatch.code,1);assert.deepEqual(await readdir(badOutput),[]);await rmdir(badOutput);
  const decrypted=await run(process.execPath,[cryptoScript,'decrypt',cipher,plain,key]);assert.equal(decrypted.code,0);authenticated=true;
  const plainHash=createHash('sha256');for await(const chunk of createReadStream(plain))plainHash.update(chunk);assert.equal(plainHash.digest('hex'),manifest.archive.plaintextSha256);
  if(vps){await restored.connect();restoredConnected=true;assert.equal((await restored.query("SELECT count(*)::int AS n FROM pg_tables WHERE schemaname='public'")).rows[0].n,0,'controller-owned restore database must be empty');}
  else{await admin.query(`CREATE DATABASE ${targetName}`);targetCreated=true;}
  const restore=await run('docker',['exec','-i',container,'pg_restore','-U',source.username,'-d',targetName,'--single-transaction','--exit-on-error','--no-owner','--no-acl'],process.env,plain);assert.equal(restore.code,0);
  if(!restoredConnected){await restored.connect();restoredConnected=true;}assert.deepEqual(await readBackupFingerprints(restored),manifest.fingerprints);assert.deepEqual((await restored.query('SELECT id::text,name FROM tenants')).rows,[{id:tenant,name:'CANDIDATE CLI FIXTURE'}]);assert.equal((await restored.query('SELECT count(*)::int AS n FROM recovery_gate')).rows[0].n,0);
  // Independent CI verification does not silently edit candidate state flags.
  assert.equal(await readFile(join(candidate,'capture.json'),'utf8'),rawManifest);
  console.log(JSON.stringify({passed:true,surface:vps?'isolated_vps':'ci',actual_candidate_cli:true,cli_exit:2,status:'captured_unverified',exclusive_output_preserved:true,schema_mismatch_has_no_manifest:true,plaintext_removed_after_capture:true,cipher_hash_matches:true,authenticated_plaintext_hash_matches:true,actual_pg_restore:true,fingerprints_match:true,candidate_flags_remain_unverified:true,bytes:manifest.archive.bytes,relations:manifest.fingerprints.length}));
}finally{
  if(restoredConnected)await restored.end();await db.end();if(targetCreated)await admin.query(`DROP DATABASE ${targetName} WITH (FORCE)`);await admin.end();
  if(authenticated)await unlink(plain);if(captured){await unlink(join(candidate,'capture.json'));await unlink(join(candidate,'database.dump.enc'));await rmdir(candidate);}await unlink(key);await rmdir(directory);
}
