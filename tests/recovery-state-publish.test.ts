import {it,expect} from 'vitest';
import {mkdtemp,mkdir,writeFile,readFile,readdir,lstat,chmod,unlink,rmdir,symlink} from 'node:fs/promises';
import {join,resolve,sep,basename} from 'node:path';
import {tmpdir} from 'node:os';
import {spawnSync} from 'node:child_process';
import {generateKeyPairSync,randomBytes,createHash,createCipheriv,createDecipheriv} from 'node:crypto';
import example from '../infra/backup/offhost.example.json';
import type {RecoverySourceState} from '../packages/server/src/recovery-state-format';
import {verifyRecoveryStateRecord} from '../packages/server/src/recovery-state-record';
import {verifyRecoveryStateJournal} from '../packages/server/src/recovery-state-journal';
import {publishRecoveryStateLocal} from '../packages/server/src/recovery-state-publish';

const posix=it.skipIf(process.platform==='win32');
const sourceId='publication-source-fixture';
const hash=(bytes:Buffer)=>createHash('sha256').update(bytes).digest('hex');
const disabled={status:'disabled',statePublicationEnabled:false,credentialsInspected:0};
function configured(parent:string){
  const config={...structuredClone(example),version:2,enabled:true,
    worker:{identity:'fixture-worker',independentFailureDomainConfirmed:true},
    source:{...example.source,host:'source.example.invalid',user:'backup',knownHostsFile:join(parent,'unused-known-hosts'),identityFile:join(parent,'unused-ssh-key'),expectedRelease:'fixture-release'},
    encryption:{...example.encryption,keyFile:join(parent,'encryption-key')},destination:{...example.destination,root:join(parent,'unused-destination')},
    state:{enabled:true,sourceId,signingKeyFile:join(parent,'signer.pem'),publicKeyFile:join(parent,'public.pem'),journalRoot:join(parent,'records'),anchorRoot:join(parent,'anchors')}};
  config.limits.runTimeoutSeconds=5;return config;
}
async function createFixture(){
  const parent=await mkdtemp(join(tmpdir(),'ccpanel-state-publish-')),config=configured(parent);
  const candidateDirectory=join(parent,'candidate-001'),stateFile=join(parent,'state.json'),now=Date.now();
  await mkdir(candidateDirectory,{mode:0o700});await mkdir(config.state.journalRoot,{mode:0o700});await mkdir(config.state.anchorRoot,{mode:0o700});
  const key=randomBytes(32),keys=generateKeyPairSync('ed25519');
  await writeFile(config.encryption.keyFile!,key,{mode:0o600,flag:'wx'});
  await writeFile(config.state.signingKeyFile,keys.privateKey.export({format:'pem',type:'pkcs8'}),{mode:0o600,flag:'wx'});
  await writeFile(config.state.publicKeyFile,keys.publicKey.export({format:'pem',type:'spki'}),{mode:0o600,flag:'wx'});
  await writeFile(join(config.state.journalRoot,'operator-note'),'keep-record-note',{mode:0o600});
  await writeFile(join(config.state.anchorRoot,'operator-note'),'keep-anchor-note',{mode:0o600});
  // A real AES-GCM fixture exercises encrypted candidate bytes. It is not a
  // pg_dump or evidence of a recoverable database or authenticated source.
  const plain=Buffer.from('private candidate archive fixture'),iv=randomBytes(12),magic=Buffer.from('CCPBK001');
  const cipher=createCipheriv('aes-256-gcm',key,iv);cipher.setAAD(magic);
  const ciphertext=Buffer.concat([magic,iv,cipher.update(plain),cipher.final(),cipher.getAuthTag()]);
  config.limits.exportMaxBytes=plain.length;
  const manifest={version:1,status:'captured_unverified',startedAt:new Date(now-2000).toISOString(),capturedAt:new Date(now-1000).toISOString(),recoveryVerified:false,offhostVerified:false,latestStateVerified:false,source:{database:'fixture',schemaVersion:22,releaseVerified:false},fingerprints:['["public.tenants", 1, -9223372036854775808]'],archive:{file:'database.dump.enc',format:'CCPBK001',bytes:ciphertext.length,sha256:hash(ciphertext),plaintextBytes:plain.length,plaintextSha256:hash(plain)}};
  // Preserve deliberate whitespace: the signed binding must cover raw manifest
  // bytes, rather than a parse/re-serialize hash of equivalent JSON.
  const manifestBytes=Buffer.from(JSON.stringify(manifest,null,2)+'\n');
  await writeFile(join(candidateDirectory,'capture.json'),manifestBytes,{mode:0o600});
  await writeFile(join(candidateDirectory,'database.dump.enc'),ciphertext,{mode:0o600});
  const at=new Date(now).toISOString();
  const state:RecoverySourceState={version:1,status:'exported_unverified',source:{id:sourceId,database:'fixture',schema_version:22,authenticated:false},exported_at:at,deletion:{schema_version:2,exported_at:at,projects:[],jobs:[],barriers:[],retention:[]},integrity:{blocked:false,reason:null,detected_at:null,details:{},audit_high_water:'0',streams:[]},kafka_observation_verified:false,latest_state_verified:false};
  await writeFile(stateFile,JSON.stringify(state),{mode:0o600});
  return{parent,config,candidateDirectory,stateFile,now,key,keys,manifest,manifestBytes,ciphertext,state,options:{config,stateFile,candidateDirectory,expectedHeadFile:null,now}};
}
type Fixture=Awaited<ReturnType<typeof createFixture>>;
async function fixture(run:(value:Fixture)=>Promise<void>){
  const value=await createFixture();
  const cleanup=async(path:string):Promise<void>=>{
    const absolute=resolve(path);if(absolute!==value.parent&&!absolute.startsWith(value.parent+sep))throw Error('fixture_cleanup_outside_owned_root');
    const info=await lstat(absolute);if(!info.isDirectory()||info.isSymbolicLink()){await unlink(absolute);return;}
    await chmod(absolute,0o700);for(const name of await readdir(absolute))await cleanup(join(absolute,name));await rmdir(absolute);
  };
  try{await run(value);}finally{value.key.fill(0);await cleanup(value.parent);}
}
function decryptRecord(bytes:Buffer,key:Buffer){
  expect(bytes.subarray(0,8).toString('ascii')).toBe('CCPBK001');
  const decipher=createDecipheriv('aes-256-gcm',key,bytes.subarray(8,20));decipher.setAAD(bytes.subarray(0,8));decipher.setAuthTag(bytes.subarray(-16));
  return JSON.parse(Buffer.concat([decipher.update(bytes.subarray(20,-16)),decipher.final()]).toString('utf8'));
}
const verification=(value:Fixture)=>({journalRoot:value.config.state.journalRoot,anchorRoot:value.config.state.anchorRoot,encryptionKeyFile:value.config.encryption.keyFile!,publicKey:value.keys.publicKey,expectedSourceId:sourceId,now:value.now,timeoutMs:5000});
async function untouched(value:Fixture){
  expect(await readdir(value.config.state.journalRoot)).toEqual(['operator-note']);expect(await readdir(value.config.state.anchorRoot)).toEqual(['operator-note']);
  expect(await readFile(join(value.config.state.journalRoot,'operator-note'),'utf8')).toBe('keep-record-note');expect(await readFile(join(value.config.state.anchorRoot,'operator-note'),'utf8')).toBe('keep-anchor-note');
}
async function redactedFailure(operation:Promise<unknown>,value:Fixture){
  const result=await operation.then(()=>({error:undefined}),error=>({error}));expect(result.error).toBeInstanceOf(Error);
  expect(String(result.error)).not.toContain(value.parent);expect(String(result.error)).not.toContain('private-pem-sentinel');return result.error as Error&{code?:string};
}

it('keeps v1 and either disabled v2 gate inert without reading any supplied publication path',async()=>{
  const config=configured(resolve('unused-publication-fixture')),{state:_state,...v1}=config;
  for(const input of [example,{...v1,version:1},{...config,enabled:false},{...config,state:{...config.state,enabled:false}}]){
    let inspected=0;const options={config:input,get stateFile():string{inspected++;throw Error('state_path_read');},get candidateDirectory():string{inspected++;throw Error('candidate_path_read');},get expectedHeadFile():string|null{inspected++;throw Error('anchor_path_read');}};
    expect(await publishRecoveryStateLocal(options)).toEqual(disabled);expect(inspected).toBe(0);
  }
});
posix('publishes a signed encrypted record bound to the exact raw candidate manifest without promoting recovery trust',async()=>fixture(async value=>{
  const before={state:await readFile(value.stateFile),signer:await readFile(value.config.state.signingKeyFile),public:await readFile(value.config.state.publicKeyFile),key:await readFile(value.config.encryption.keyFile!)};
  const result=await publishRecoveryStateLocal(value.options);
  expect(result).toMatchObject({status:'stored_unverified',backupId:basename(value.candidateDirectory),backupManifestSha256:hash(value.manifestBytes),recordsVerified:1,storedChainVerified:true,sourceEndpointVerified:false,anchorCustodyVerified:false,latestStateVerified:false,recoveryVerified:false,offhostVerified:false});
  if(result.status!=='stored_unverified')throw Error('publication unexpectedly disabled');
  const object=result.anchor.sequence+'-'+result.anchor.record_hash+'.state.enc',encrypted=await readFile(join(value.config.state.journalRoot,object)),record=decryptRecord(encrypted,value.key);
  const checked=verifyRecoveryStateRecord({record,publicKey:value.keys.publicKey,expectedSourceId:sourceId,now:value.now});
  expect(checked.anchor).toEqual(result.anchor);expect(checked.body.backup).toEqual({id:'candidate-001',manifest_sha256:hash(value.manifestBytes)});expect(checked.body.state).toEqual(value.state);
  expect(hash(value.manifestBytes)).not.toBe(hash(Buffer.from(JSON.stringify(value.manifest))));expect(encrypted.includes(Buffer.from(record.payload))).toBe(false);
  const full=await verifyRecoveryStateJournal({...verification(value),trustedTip:result.anchor});expect(full.recordsVerified).toBe(1);expect(full.latestStateVerified).toBe(false);
  expect((await readdir(value.config.state.journalRoot)).sort()).toEqual([object,'operator-note'].sort());expect((await readdir(value.config.state.anchorRoot)).sort()).toEqual(['head.json','operator-note']);
  expect(await readFile(join(value.candidateDirectory,'capture.json'))).toEqual(value.manifestBytes);expect(await readFile(join(value.candidateDirectory,'database.dump.enc'))).toEqual(value.ciphertext);
  expect(await readFile(value.stateFile)).toEqual(before.state);expect(await readFile(value.config.state.signingKeyFile)).toEqual(before.signer);expect(await readFile(value.config.state.publicKeyFile)).toEqual(before.public);expect(await readFile(value.config.encryption.keyFile!)).toEqual(before.key);
  expect((await lstat(join(value.config.state.journalRoot,object))).mode&0o777).toBe(0o600);expect((await lstat(join(value.config.state.anchorRoot,'head.json'))).mode&0o777).toBe(0o600);
}));
posix('requires an explicit genesis or external expected anchor and preserves the journal on stale CAS',async()=>fixture(async value=>{
  expect((await redactedFailure(publishRecoveryStateLocal({...value.options,expectedHeadFile:undefined as never}),value)).code).toBe('state_publish_options_invalid');await untouched(value);
  const first=await publishRecoveryStateLocal(value.options);if(first.status!=='stored_unverified')throw Error('publication unexpectedly disabled');
  const expectedHeadFile=join(value.parent,'expected-anchor.json');await writeFile(expectedHeadFile,JSON.stringify(first.anchor),{mode:0o600});
  const at=new Date(value.now+1).toISOString();value.state.exported_at=at;value.state.deletion.exported_at=at;await writeFile(value.stateFile,JSON.stringify(value.state));
  const second=await publishRecoveryStateLocal({...value.options,expectedHeadFile,now:value.now+1});if(second.status!=='stored_unverified')throw Error('publication unexpectedly disabled');
  expect(second.anchor.sequence).toBe('2');expect(second.recordsVerified).toBe(2);
  const head=await readFile(join(value.config.state.anchorRoot,'head.json')),objects=await readdir(value.config.state.journalRoot);
  await expect(publishRecoveryStateLocal({...value.options,expectedHeadFile,now:value.now+1})).rejects.toMatchObject({code:'journal_head_conflict'});
  await expect(publishRecoveryStateLocal({...value.options,now:value.now+1})).rejects.toMatchObject({code:'journal_head_conflict'});
  expect(await readFile(join(value.config.state.anchorRoot,'head.json'))).toEqual(head);expect(await readdir(value.config.state.journalRoot)).toEqual(objects);
  expect((await verifyRecoveryStateJournal({...verification(value),now:value.now+1,trustedTip:second.anchor})).recordsVerified).toBe(2);
}));
posix('honors pre-abort before creating any journal state',async()=>fixture(async value=>{
  const controller=new AbortController();controller.abort();const error=await redactedFailure(publishRecoveryStateLocal({...value.options,signal:controller.signal}),value);
  expect(error.code).toBe('state_publish_aborted');await untouched(value);
}));
posix.each(['source-id','database','schema','before-capture','stale','future','candidate-size','candidate-corruption'] as const)('rejects incompatible or unbounded input: %s',async mode=>fixture(async value=>{
  if(mode==='source-id')value.state.source.id='another-source';
  if(mode==='database')value.state.source.database='another-database';
  if(mode==='schema'){value.manifest.source.schemaVersion=21;await writeFile(join(value.candidateDirectory,'capture.json'),JSON.stringify(value.manifest));}
  if(['before-capture','stale','future'].includes(mode)){const at=new Date(value.now+(mode==='before-capture'?-1001:mode==='stale'?-601000:300001)).toISOString();value.state.exported_at=at;value.state.deletion.exported_at=at;}
  if(mode==='candidate-size')value.config.limits.exportMaxBytes--;
  if(mode==='candidate-corruption'){const changed=Buffer.from(value.ciphertext);changed[25]^=1;await writeFile(join(value.candidateDirectory,'database.dump.enc'),changed);}
  const expected={'source-id':'state_publish_source_mismatch',database:'state_publish_source_mismatch',schema:'state_publish_source_mismatch','before-capture':'state_publish_state_predates_backup',stale:'state_not_fresh',future:'state_not_fresh','candidate-size':'backup_candidate_size_invalid','candidate-corruption':'backup_candidate_cipher_mismatch'};
  await writeFile(value.stateFile,JSON.stringify(value.state));expect((await redactedFailure(publishRecoveryStateLocal(value.options),value)).code).toBe(expected[mode]);await untouched(value);
}));
posix('requires a matching Ed25519 signing key and pinned public key',async()=>fixture(async value=>{
  const other=generateKeyPairSync('ed25519');await writeFile(value.config.state.publicKeyFile,other.publicKey.export({format:'pem',type:'spki'}));
  expect((await redactedFailure(publishRecoveryStateLocal(value.options),value)).code).toBe('state_publish_signer_invalid');await untouched(value);
}));
posix.each(['signer-mode','public-mode','signer-pem','signer-symlink','public-symlink','state-symlink','candidate-symlink','journal-mode','root-overlap','state-oversize','anchor-oversize'] as const)('refuses unsafe private files and paths: %s',async mode=>fixture(async value=>{
  let options={...value.options,expectedHeadFile:null as string|null};
  if(mode==='signer-mode')await chmod(value.config.state.signingKeyFile,0o644);
  if(mode==='public-mode')await chmod(value.config.state.publicKeyFile,0o644);
  if(mode==='signer-pem')await writeFile(value.config.state.signingKeyFile,Buffer.concat([await readFile(value.config.state.signingKeyFile),Buffer.from('private-pem-sentinel\n')]));
  if(mode==='journal-mode')await chmod(value.config.state.journalRoot,0o755);
  if(mode==='root-overlap')options={...options,config:{...value.config,state:{...value.config.state,anchorRoot:value.config.state.journalRoot}}};
  if(mode==='state-oversize')await writeFile(value.stateFile,Buffer.alloc(16*1024*1024+1,32));
  if(mode==='anchor-oversize'){options.expectedHeadFile=join(value.parent,'oversized-anchor');await writeFile(options.expectedHeadFile,Buffer.alloc(8193,32),{mode:0o600});}
  if(mode.endsWith('-symlink')){
    const target=mode==='signer-symlink'?value.config.state.signingKeyFile:mode==='public-symlink'?value.config.state.publicKeyFile:mode==='state-symlink'?value.stateFile:value.candidateDirectory,alias=join(value.parent,'unsafe-alias');
    await symlink(target,alias,mode==='candidate-symlink'?'dir':'file');
    if(mode==='signer-symlink')value.config.state.signingKeyFile=alias;else if(mode==='public-symlink')value.config.state.publicKeyFile=alias;else if(mode==='state-symlink')options.stateFile=alias;else options.candidateDirectory=alias;
  }
  const expected=mode==='signer-pem'?'state_publish_signer_invalid':mode==='journal-mode'?'journal_directory_unsafe':mode==='root-overlap'?'state_storage_roots_overlap':'state_publish_file_unsafe';
  expect((await redactedFailure(publishRecoveryStateLocal(options),value)).code).toBe(expected);await untouched(value);
}));

const runCli=(args:string[])=>spawnSync(process.execPath,[resolve('node_modules/tsx/dist/cli.mjs'),'scripts/publish-recovery-state-local.ts',...args],{encoding:'utf8',timeout:10000});
it('runs the real disabled CLI with nonexistent inputs and redacts malformed-config failures',async()=>{
  const parent=await mkdtemp(join(tmpdir(),'ccpanel-state-publish-cli-')),file=join(parent,'config.json'),missing=join(parent,'private-cli-sentinel');
  try{
    await writeFile(file,JSON.stringify(example),{mode:0o600});const inert=runCli([file,missing,missing,'--genesis']);
    expect(inert.status).toBe(0);expect(inert.stderr).toBe('');expect(JSON.parse(inert.stdout)).toEqual(disabled);
    await writeFile(file,'{"password":"private-cli-sentinel"}');const failed=runCli([file,missing,missing,'--genesis']);
    expect(failed.status).toBe(1);expect(failed.stdout).toBe('');expect(JSON.parse(failed.stderr).code).toMatch(/^[a-z0-9_]+$/);expect(failed.stderr).not.toContain('private-cli-sentinel');expect(failed.stderr).not.toContain(parent);
    expect(await readdir(parent)).toEqual(['config.json']);
  }finally{await unlink(file);await rmdir(parent);}
});
posix('real enabled CLI stores with exit 2 and accepts only the explicitly supplied expected anchor',async()=>fixture(async value=>{
  const configFile=join(value.parent,'config.json'),anchorFile=join(value.parent,'caller-anchor.json');await writeFile(configFile,JSON.stringify(value.config),{mode:0o600});
  const first=runCli([configFile,value.stateFile,value.candidateDirectory,'--genesis']);expect(first.status).toBe(2);expect(first.stderr).toBe('');const receipt=JSON.parse(first.stdout);expect(receipt.status).toBe('stored_unverified');expect(receipt.recordsVerified).toBe(1);
  await writeFile(anchorFile,JSON.stringify(receipt.anchor),{mode:0o600});const second=runCli([configFile,value.stateFile,value.candidateDirectory,anchorFile]);expect(second.status).toBe(2);expect(second.stderr).toBe('');expect(JSON.parse(second.stdout).recordsVerified).toBe(2);
  const stale=runCli([configFile,value.stateFile,value.candidateDirectory,anchorFile]);expect(stale.status).toBe(1);expect(stale.stdout).toBe('');expect(stale.stderr).not.toContain(value.parent);expect(JSON.parse(stale.stderr).code).toBe('journal_head_conflict');
}));
