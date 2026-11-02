// Actual candidate CLI: dedicated CI service or explicitly guarded VPS fixture.
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {randomBytes,randomUUID,createHash,generateKeyPairSync,createDecipheriv,verify} from 'node:crypto';
import {mkdtemp,mkdir,writeFile,readFile,readdir,stat,lstat,unlink,rmdir} from 'node:fs/promises';
import {createReadStream} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,dirname,isAbsolute,resolve,sep,basename} from 'node:path';
import {pathToFileURL} from 'node:url';
import pg from 'pg';
import {readBackupFingerprints} from '../packages/server/src/backup-snapshot';
import {inspectBackupCandidate} from '../packages/server/src/backup-candidate';
import {copyBackupCandidateToLocal} from '../packages/server/src/backup-local';
import {decryptBackupCandidate} from '../packages/server/src/backup-decrypt';
const vps=process.env.CCPANEL_ISOLATED_CAPTURE_VPS==='true';
const source=new URL(process.env.DATABASE_URL!);assert.match(source.pathname,/^\/ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$/);
const container=process.env.CCPANEL_PG_CONTAINER!;
if(vps){
  assert.notEqual(process.env.CCPANEL_SFTP_PG_CI,'true','CI SFTP fixture must never run on the VPS path');
  assert.equal(process.env.CCPANEL_EXPECTED_RELEASE,'m4-sdk-013');assert.equal(process.version,'v22.22.3');assert.equal(source.username,'ccpanel');
  assert.match(source.hostname,/^172\.(1[6-9]|2\d|3[01])\.\d{1,3}\.\d{1,3}$/);assert.equal(container,'ccpanel-postgres-1');
  assert.equal(process.env.CCPANEL_CAPTURE_VPS_READY,'true');
}else{assert.equal(process.env.GITHUB_ACTIONS,'true');assert.equal(process.env.CCPANEL_SFTP_PG_CI,'true');assert.equal(source.username,'ccpanel_ci');assert.ok(['127.0.0.1','localhost'].includes(source.hostname));assert.match(container,/^[a-f0-9]{12,64}$/);}
assert.ok(isAbsolute(process.env.CCPANEL_CAPTURE_CLI!));
const cryptoScript=join(dirname(process.env.CCPANEL_CAPTURE_CLI!),'encrypt-backup.mjs');
const directory=await mkdtemp(join(tmpdir(),'ccpanel-capture-cli-')),candidate=join(directory,'candidate'),key=join(directory,'key'),plain=join(directory,'authenticated.dump');
const targetName=vps?process.env.CCPANEL_CAPTURE_RESTORE_DB!:'ccpanel_restore_'+new Date().toISOString().replace(/\D/g,'').slice(0,14)+'_'+randomBytes(4).toString('hex');
assert.match(targetName,/^ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$/);
assert.notEqual('/'+targetName,source.pathname);
const targetUrl=new URL(source);targetUrl.pathname='/'+targetName;const adminUrl=new URL(source);adminUrl.pathname='/postgres';
const admin=new pg.Client({connectionString:adminUrl.href}),db=new pg.Client({connectionString:source.href,options:'-c timezone=UTC'}),restored=new pg.Client({connectionString:targetUrl.href,options:'-c timezone=UTC'});
let targetCreated=false,restoredConnected=false,authenticated=false,captured=false,localCopied=false;
const localRoot=join(directory,'destination'),localSet=join(localRoot,'candidate-copy');
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
  const inspected=await inspectBackupCandidate({directory:candidate,expectedSchema:22,maxBytes:8388644,timeoutMs:30000});
  assert.equal(inspected.cipherIntegrityChecked,true);assert.equal(inspected.sha256,manifest.archive.sha256);assert.equal(inspected.sourceAuthenticated,false);assert.equal(inspected.recoveryVerified,false);
  if(!vps){
    const inspectionArgs=[join(dirname(process.env.CCPANEL_CAPTURE_CLI!),'inspect-backup.mjs'),candidate,'22'];
    const inspection=await run(process.execPath,inspectionArgs,{...process.env,CCPANEL_BACKUP_MAX_BYTES:String(manifest.archive.plaintextBytes)});
    assert.equal(inspection.code,2);assert.deepEqual(JSON.parse(inspection.stdout),inspected);
    const overBudget=await run(process.execPath,inspectionArgs,{...process.env,CCPANEL_BACKUP_MAX_BYTES:String(manifest.archive.plaintextBytes-1)});
    assert.equal(overBudget.code,1);assert.equal(overBudget.stdout,'');
  }
  const again=await capture();assert.equal(again.code,1);assert.equal(await readFile(join(candidate,'capture.json'),'utf8'),rawManifest);
  const badOutput=join(directory,'schema-mismatch');const mismatch=await capture(badOutput,'999');assert.equal(mismatch.code,1);assert.deepEqual(await readdir(badOutput),[]);await rmdir(badOutput);
  await mkdir(localRoot,{mode:0o700});
  if(!vps){
    const args=[join(dirname(process.env.CCPANEL_CAPTURE_CLI!),'copy-backup-local.mjs'),candidate,localRoot,'candidate-copy','22'];
    const copied=await run(process.execPath,args,{...process.env,CCPANEL_BACKUP_MAX_BYTES:String(manifest.archive.plaintextBytes)});
    assert.equal(copied.code,2);const copyReceipt=JSON.parse(copied.stdout);assert.equal(copyReceipt.status,'copied_unverified');assert.equal(copyReceipt.destinationReadbackChecked,true);assert.equal(copyReceipt.sha256,inspected.sha256);assert.equal(copyReceipt.manifestSha256,inspected.manifestSha256);localCopied=true;
    const repeated=await run(process.execPath,args);assert.equal(repeated.code,1);
  }else{
    const copied=await copyBackupCandidateToLocal({sourceDirectory:candidate,destinationRoot:localRoot,id:'candidate-copy',expectedSchema:22,maxBytes:8388644,timeoutMs:30000});assert.equal(copied.status,'copied_unverified');localCopied=true;
  }
  assert.equal(await readFile(join(localSet,'capture.json'),'utf8'),rawManifest);
  assert.deepEqual((await readdir(localSet)).sort(),['capture.json','database.dump.enc']);
  const restoreFrom=async(destination:string)=>{
  // Authenticate and restore this read-back destination, never the original.
  if(!vps){
    const args=[join(dirname(process.env.CCPANEL_CAPTURE_CLI!),'decrypt-backup-candidate.mjs'),destination,key,plain,'22'];
    const decrypted=await run(process.execPath,args,{...process.env,CCPANEL_BACKUP_MAX_BYTES:String(manifest.archive.plaintextBytes)});assert.equal(decrypted.code,2);authenticated=true;
    const receipt=JSON.parse(decrypted.stdout);assert.equal(receipt.status,'decrypted_unverified');assert.equal(receipt.authenticatedDecryption,true);assert.equal(receipt.plaintextReadbackChecked,true);assert.equal(receipt.sha256,manifest.archive.plaintextSha256);assert.equal(receipt.manifestSha256,inspected.manifestSha256);assert.equal(receipt.recoveryVerified,false);assert.equal(receipt.latestStateVerified,false);
    const repeated=await run(process.execPath,args);assert.equal(repeated.code,1);
  }else{
    const decrypted=await decryptBackupCandidate({directory:destination,keyFile:key,output:plain,expectedSchema:22,maxBytes:8388644,timeoutMs:30000});assert.equal(decrypted.status,'decrypted_unverified');authenticated=true;
  }
  const plainHash=createHash('sha256');for await(const chunk of createReadStream(plain))plainHash.update(chunk);assert.equal(plainHash.digest('hex'),manifest.archive.plaintextSha256);
  if(vps){await restored.connect();restoredConnected=true;assert.equal((await restored.query("SELECT count(*)::int AS n FROM pg_tables WHERE schemaname='public'")).rows[0].n,0,'controller-owned restore database must be empty');}
  else{await admin.query(`CREATE DATABASE ${targetName}`);targetCreated=true;}
  const restore=await run('docker',['exec','-i',container,'pg_restore','-U',source.username,'-d',targetName,'--single-transaction','--exit-on-error','--no-owner','--no-acl'],process.env,plain);assert.equal(restore.code,0);
  if(!restoredConnected){await restored.connect();restoredConnected=true;}assert.deepEqual(await readBackupFingerprints(restored),manifest.fingerprints);assert.deepEqual((await restored.query('SELECT id::text,name FROM tenants')).rows,[{id:tenant,name:'CANDIDATE CLI FIXTURE'}]);assert.equal((await restored.query('SELECT count(*)::int AS n FROM recovery_gate')).rows[0].n,0);
  };
  let sftpEvidence:Record<string,unknown>={sftp_transport:false};
  if(!vps){
    // CI-only helper bundles are dynamically loaded. The retained VPS mode
    // never imports ssh2 or starts an SSH daemon from its older release deps.
    const {withLoopbackSftp}=await import(pathToFileURL(join(dirname(process.env.CCPANEL_CAPTURE_CLI!),'loopback-sftp-fixture.mjs')).href) as typeof import('./with-loopback-sftp');
    const {transferBackupCandidateSftp}=await import(pathToFileURL(join(dirname(process.env.CCPANEL_CAPTURE_CLI!),'backup-sftp-adapter.mjs')).href) as typeof import('../packages/server/src/backup-sftp');
    sftpEvidence=await withLoopbackSftp(async context=>{
      const transferred=await transferBackupCandidateSftp({sourceDirectory:localSet,downloadRoot:context.downloadRoot,remoteRoot:context.remoteRoot,id:'pg-candidate',expectedSchema:22,maxBytes:manifest.archive.bytes,timeoutMs:30000,connection:context.connection});
      assert.equal(transferred.status,'transferred_unverified');assert.equal(transferred.hostKeyVerified,true);assert.equal(transferred.downloadReadbackChecked,true);assert.equal(transferred.bytes,manifest.archive.bytes);assert.equal(transferred.sha256,inspected.sha256);assert.equal(transferred.manifestSha256,inspected.manifestSha256);
      for(const field of ['sourceAuthenticated','recoveryVerified','offhostVerified','latestStateVerified','remoteDirectoryDurabilityVerified'] as const)assert.equal(transferred[field],false);
      const downloaded=join(context.downloadRoot,'pg-candidate'),remote=join(context.remoteRoot,'pg-candidate');
      const fingerprint=async(path:string)=>{const digest=createHash('sha256');for await(const chunk of createReadStream(path))digest.update(chunk);return digest.digest('hex');};
      for(const set of [remote,downloaded]){
        assert.deepEqual((await readdir(set)).sort(),['capture.json','database.dump.enc']);
        assert.equal(await readFile(join(set,'capture.json'),'utf8'),rawManifest);assert.equal(await fingerprint(join(set,'database.dump.enc')),manifest.archive.sha256);
        assert.equal((await stat(set)).mode&0o777,0o700);for(const file of ['capture.json','database.dump.enc'])assert.equal((await stat(join(set,file))).mode&0o777,0o600);
      }
      // Every stage uses this same actual PG archive, not a synthetic payload or
      // the pre-transfer local copy. The restore runs before fixture teardown.
      await restoreFrom(downloaded);
      // This separate binding proof consumes the same downloaded manifest and
      // an actual post-capture export from the original source database. Neither
      // the restored target nor the earlier state suite's fixture hash is used.
      const stateParent=await mkdtemp(join(directory,'state-publish-'));
      assert.equal(dirname(stateParent),directory);assert.ok(basename(stateParent).startsWith('state-publish-'));
      const journalRoot=join(stateParent,'journal'),anchorRoot=join(stateParent,'anchors');
      const configFile=join(stateParent,'config.json'),stateFile=join(stateParent,'source-state.json'),wrongStateFile=join(stateParent,'wrong-source-state.json');
      const signingKeyFile=join(stateParent,'signer.pem'),publicKeyFile=join(stateParent,'public.pem');
      const removeStateFixture=async(path:string):Promise<void>=>{
        const absolute=resolve(path);assert.ok(absolute===stateParent||absolute.startsWith(stateParent+sep));
        const metadata=await lstat(absolute);
        if(!metadata.isDirectory()||metadata.isSymbolicLink()){await unlink(absolute);return;}
        for(const name of await readdir(absolute))await removeStateFixture(join(absolute,name));await rmdir(absolute);
      };
      let statePublicationEvidence:Record<string,unknown>;
      try{
        const sourceBefore=await readBackupFingerprints(db),sourceGateBefore=(await db.query('SELECT row_to_json(g) AS state FROM recovery_gate g')).rows;
        const {exportRecoverySourceState}=await import(pathToFileURL(join(dirname(process.env.CCPANEL_CAPTURE_CLI!),'recovery-state-export-adapter.mjs')).href) as typeof import('../packages/server/src/recovery-state-export');
        const {verifyRecoveryStateJournal}=await import(pathToFileURL(join(dirname(process.env.CCPANEL_CAPTURE_CLI!),'recovery-state-journal-adapter.mjs')).href) as typeof import('../packages/server/src/recovery-state-journal');
        const state=await exportRecoverySourceState({connectionString:source.href,sourceId:'ci-capture-source',expectedSchema:22,timeoutMs:15000,maxAgeSeconds:600});
        assert.equal(state.source.database,manifest.source.database);assert.ok(state.exported_at>=manifest.capturedAt);
        const signer=generateKeyPairSync('ed25519');
        await mkdir(journalRoot,{mode:0o700});await mkdir(anchorRoot,{mode:0o700});
        await writeFile(signingKeyFile,signer.privateKey.export({format:'pem',type:'pkcs8'}),{mode:0o600,flag:'wx'});
        await writeFile(publicKeyFile,signer.publicKey.export({format:'pem',type:'spki'}),{mode:0o600,flag:'wx'});
        const config={version:2,enabled:true,worker:{identity:'ci-configured-state',independentFailureDomainConfirmed:true},source:{host:source.hostname,port:22,user:'ccpanel_ci',knownHostsFile:join(stateParent,'source-known-not-read'),identityFile:join(stateParent,'source-identity-not-read'),expectedRelease:'m4-sdk-013',expectedSchema:22},encryption:{format:'CCPBK001',keyFile:key},destination:{kind:'local',root:localRoot,sftp:null,prefix:'ccpanel-backups'},schedule:{fullEveryMinutes:360,stateEveryMinutes:5},retention:{daily:7,weekly:4,monthly:3},limits:{runTimeoutSeconds:30,exportMaxBytes:manifest.archive.plaintextBytes,stateMaxAgeSeconds:600,incompleteRetentionHours:24,maxAttempts:1},state:{enabled:true,sourceId:'ci-capture-source',signingKeyFile,publicKeyFile,journalRoot,anchorRoot}};
        await writeFile(configFile,JSON.stringify(config)+'\n',{mode:0o600,flag:'wx'});
        await writeFile(stateFile,JSON.stringify(state)+'\n',{mode:0o600,flag:'wx'});
        await writeFile(wrongStateFile,JSON.stringify({...state,source:{...state.source,database:'unrelated_ci_source'}})+'\n',{mode:0o600,flag:'wx'});
        const publicationCli=join(dirname(process.env.CCPANEL_CAPTURE_CLI!),'publish-recovery-state-local.mjs');
        const rejected=await run(process.execPath,[publicationCli,configFile,wrongStateFile,downloaded,'--genesis']);
        assert.equal(rejected.code,1);assert.equal(rejected.stdout,'');assert.deepEqual(await readdir(journalRoot),[]);assert.deepEqual(await readdir(anchorRoot),[]);
        const published=await run(process.execPath,[publicationCli,configFile,stateFile,downloaded,'--genesis']);
        assert.equal(published.code,2,'state publication is explicitly NOT recovery admission');
        const publication=JSON.parse(published.stdout),actualManifestHash=createHash('sha256').update(await readFile(join(downloaded,'capture.json'))).digest('hex');
        assert.equal(publication.status,'stored_unverified');assert.equal(publication.backupId,'pg-candidate');assert.equal(publication.backupManifestSha256,actualManifestHash);assert.equal(actualManifestHash,inspected.manifestSha256);assert.equal(publication.recordsVerified,1);assert.equal(publication.storedChainVerified,true);
        for(const field of ['sourceEndpointVerified','anchorCustodyVerified','latestStateVerified','recoveryVerified','offhostVerified'])assert.equal(publication[field],false);
        const objectName='1-'+publication.anchor.record_hash+'.state.enc';
        assert.deepEqual(await readdir(journalRoot),[objectName]);assert.deepEqual(await readdir(anchorRoot),['head.json']);
        const stored=await readFile(join(journalRoot,objectName)),head=await readFile(join(anchorRoot,'head.json'));
        assert.deepEqual(JSON.parse(head.toString('utf8')),publication.anchor);assert.equal((await stat(join(journalRoot,objectName))).mode&0o777,0o600);
        const encryptionKey=await readFile(key);let plaintext:Buffer|undefined;
        try{
          assert.equal(stored.subarray(0,8).toString('ascii'),'CCPBK001');
          const decryptor=createDecipheriv('aes-256-gcm',encryptionKey,stored.subarray(8,20));decryptor.setAAD(stored.subarray(0,8));decryptor.setAuthTag(stored.subarray(-16));
          plaintext=Buffer.concat([decryptor.update(stored.subarray(20,-16)),decryptor.final()]);
          const wire=JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(plaintext));assert.equal(wire.algorithm,'Ed25519');assert.equal(wire.version,1);
          const payload=Buffer.from(wire.payload,'base64'),signature=Buffer.from(wire.signature,'base64'),length=Buffer.alloc(4);length.writeUInt32BE(payload.length);
          assert.equal(verify(null,Buffer.concat([Buffer.from('ccpanel.recovery-state.v1\0'),payload]),signer.publicKey,signature),true);
          const recordHash=createHash('sha256').update(Buffer.from('ccpanel.recovery-state.hash.v1\0')).update(length).update(payload).update(signature).digest('hex');assert.equal(recordHash,publication.anchor.record_hash);
          const body=JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(payload));
          assert.deepEqual(body.backup,{id:'pg-candidate',manifest_sha256:actualManifestHash});assert.deepEqual(body.state,state);assert.equal(body.source_id,'ci-capture-source');assert.equal(body.sequence,'1');assert.equal(body.previous_hash,null);
          payload.fill(0);signature.fill(0);
        }finally{encryptionKey.fill(0);plaintext?.fill(0);}
        const checked=await verifyRecoveryStateJournal({journalRoot,anchorRoot,encryptionKeyFile:key,publicKey:signer.publicKey,expectedSourceId:'ci-capture-source',trustedTip:publication.anchor,timeoutMs:30000,maxAgeSeconds:600});
        assert.equal(checked.recordsVerified,1);assert.deepEqual(checked.state,state);assert.deepEqual(checked.anchor,publication.anchor);
        assert.equal(checked.latestStateVerified,false);assert.equal(checked.sourceEndpointVerified,false);assert.equal(checked.anchorCustodyVerified,false);
        const repeated=await run(process.execPath,[publicationCli,configFile,stateFile,downloaded,'--genesis']);assert.equal(repeated.code,1);assert.equal(repeated.stdout,'');
        assert.deepEqual(await readFile(join(journalRoot,objectName)),stored);assert.deepEqual(await readFile(join(anchorRoot,'head.json')),head);
        assert.deepEqual(await readBackupFingerprints(db),sourceBefore);assert.deepEqual((await db.query('SELECT row_to_json(g) AS state FROM recovery_gate g')).rows,sourceGateBefore);
        assert.equal(await readFile(stateFile,'utf8'),JSON.stringify(state)+'\n');assert.equal(await readFile(join(downloaded,'capture.json'),'utf8'),rawManifest);
        statePublicationEvidence={configured_state_publication_cli:true,configured_state_pg_binding:true,state_binding_uses_actual_manifest:true,state_binding_backup_hash_is_fixture:false,source_export_after_same_candidate_capture:true,independent_signed_record_decryption_checked:true,configured_state_full_chain_checked:true,wrong_source_database_refused:true,duplicate_genesis_refused:true,source_and_gate_unchanged:true,state_anchor_custody_verified:false,state_latest_admission_verified:false};
      }finally{await removeStateFixture(stateParent);}
      await assert.rejects(lstat(stateParent),{code:'ENOENT'});statePublicationEvidence!.configured_state_fixture_removed=true;
      assert.equal(await readFile(join(downloaded,'capture.json'),'utf8'),rawManifest);assert.equal(await readFile(join(remote,'capture.json'),'utf8'),rawManifest);
      assert.deepEqual(await readdir(context.remoteRoot),['pg-candidate']);
      return{sftp_transport:true,same_candidate_sftp_pg_restore:true,host_pin_verified:true,sftp_cipher_hash_matches:true,sftp_manifest_hash_matches:true,remote_contains_no_plaintext_or_encryption_key:true,source_identity_verified:false,independent_failure_domain_verified:false,remote_directory_durability_verified:false,...statePublicationEvidence!};
    });
    sftpEvidence.sftp_fixture_cleanup_confirmed=true;
  }else await restoreFrom(localSet);
  // Independent CI verification does not silently edit candidate state flags.
  assert.equal(await readFile(join(candidate,'capture.json'),'utf8'),rawManifest);
  assert.equal(await readFile(join(localSet,'capture.json'),'utf8'),rawManifest);
  console.log(JSON.stringify({passed:true,surface:vps?'isolated_vps':'ci',actual_candidate_cli:true,cli_exit:2,status:'captured_unverified',read_only_candidate_inspection:true,local_destination_copy:true,bounded_authenticated_candidate_decryption:true,restored_from_destination:true,exclusive_output_preserved:true,schema_mismatch_has_no_manifest:true,plaintext_removed_after_capture:true,cipher_hash_matches:true,authenticated_plaintext_hash_matches:true,actual_pg_restore:true,fingerprints_match:true,candidate_flags_remain_unverified:true,...sftpEvidence,bytes:manifest.archive.bytes,relations:manifest.fingerprints.length}));
}finally{
  if(restoredConnected)await restored.end();await db.end();if(targetCreated)await admin.query(`DROP DATABASE ${targetName} WITH (FORCE)`);await admin.end();
  if(localCopied){await unlink(join(localSet,'capture.json'));await unlink(join(localSet,'database.dump.enc'));await rmdir(localSet);await rmdir(localRoot);}
  if(authenticated)await unlink(plain);if(captured){await unlink(join(candidate,'capture.json'));await unlink(join(candidate,'database.dump.enc'));await rmdir(candidate);}await unlink(key);await rmdir(directory);
}
