import {it,expect,vi,afterEach} from 'vitest';
import {mkdtemp,mkdir,writeFile,readFile,readdir,lstat,chmod,unlink,rmdir} from 'node:fs/promises';
import {join,resolve,sep,dirname} from 'node:path';
import {tmpdir} from 'node:os';
import {generateKeyPairSync,randomBytes,createHash,createCipheriv,createDecipheriv} from 'node:crypto';
import example from '../infra/backup/offhost.example.json';
import type {RecoverySourceState} from '../packages/server/src/recovery-state-format';
import {parseRecoveryStateAnchor,verifyRecoveryStateRecord} from '../packages/server/src/recovery-state-record';
import {verifyRecoveryStateJournal} from '../packages/server/src/recovery-state-journal';
import {publishRecoveryStateLocal} from '../packages/server/src/recovery-state-publish';

const scheduling=vi.hoisted(()=>({
  mode:'' as ''|'rewrite-manifest'|'abort-signer',journal:'',anchors:'',manifest:'',signer:'',
  replacement:undefined as Buffer|undefined,abort:undefined as undefined|(()=>void),
  ownedHandles:0,recordLinked:false,headRenamed:false,lockCreated:false,
  manifestRewritten:false,finalManifestReadObserved:false,signerReadObserved:false,signerNativeBytes:0,signerClosed:false,
}));
// All filesystem operations and cryptography are native. The hooks only select
// when a real manifest rewrite or AbortController cancellation takes place;
// these tests do not simulate process death, disk faults or crash durability.
vi.mock('node:fs/promises',async importOriginal=>{
  const native=await importOriginal<typeof import('node:fs/promises')>();
  return {...native,mkdir:async(...args:Parameters<typeof native.mkdir>)=>{
    const result=await native.mkdir(...args);if(String(args[0])===join(scheduling.anchors,'.append.lock'))scheduling.lockCreated=true;return result;
  },link:async(...args:Parameters<typeof native.link>)=>{
    await native.link(...args);if(dirname(String(args[1]))===scheduling.journal&&String(args[1]).endsWith('.state.enc'))scheduling.recordLinked=true;
  },rename:async(...args:Parameters<typeof native.rename>)=>{
    await native.rename(...args);
    if(String(args[1])===join(scheduling.anchors,'head.json')){
      scheduling.headRenamed=true;
      if(scheduling.mode==='rewrite-manifest'&&!scheduling.manifestRewritten){
        await native.writeFile(scheduling.manifest,scheduling.replacement!);scheduling.manifestRewritten=true;
      }
    }
  },open:async(...args:Parameters<typeof native.open>)=>{
    const path=String(args[0]),handle=await native.open(...args);let closed=false;scheduling.ownedHandles++;
    return new Proxy(handle,{get(target,key){
      if(key==='read')return async(...readArgs:unknown[])=>{
        const result=await (target.read as (...args:unknown[])=>Promise<{bytesRead:number}>).apply(target,readArgs);
        if(path===scheduling.manifest&&scheduling.headRenamed&&result.bytesRead>0)scheduling.finalManifestReadObserved=true;
        if(path===scheduling.signer&&result.bytesRead>0){
          scheduling.signerNativeBytes+=result.bytesRead;
          if(!scheduling.signerReadObserved){scheduling.signerReadObserved=true;if(scheduling.mode==='abort-signer')scheduling.abort?.();}
        }
        return result;
      };
      if(key==='close')return async()=>{try{await target.close();}finally{if(!closed){closed=true;scheduling.ownedHandles--;if(path===scheduling.signer)scheduling.signerClosed=true;}}};
      const value=Reflect.get(target,key,target);return typeof value==='function'?value.bind(target):value;
    }});
  }};
});
afterEach(()=>{scheduling.mode='';scheduling.abort=undefined;scheduling.replacement=undefined;vi.restoreAllMocks();});
const posix=it.skipIf(process.platform==='win32');
const sourceId='publication-fault-fixture';
const sha=(bytes:Buffer)=>createHash('sha256').update(bytes).digest('hex');
async function createFixture(){
  const parent=await mkdtemp(join(tmpdir(),'ccpanel-state-publish-fault-'));
  const config={...structuredClone(example),version:2,enabled:true,
    worker:{identity:'fixture-worker',independentFailureDomainConfirmed:true},
    source:{...example.source,host:'source.example.invalid',user:'backup',knownHostsFile:join(parent,'unused-hosts'),identityFile:join(parent,'unused-identity'),expectedRelease:'fixture-release'},
    encryption:{...example.encryption,keyFile:join(parent,'encryption-key')},destination:{...example.destination,root:join(parent,'unused-destination')},
    state:{enabled:true,sourceId,signingKeyFile:join(parent,'signer.pem'),publicKeyFile:join(parent,'public.pem'),journalRoot:join(parent,'records'),anchorRoot:join(parent,'anchors')}};
  config.limits.runTimeoutSeconds=5;
  const candidateDirectory=join(parent,'candidate-001'),stateFile=join(parent,'state.json'),now=Date.now(),keys=generateKeyPairSync('ed25519'),key=randomBytes(32);
  await mkdir(candidateDirectory,{mode:0o700});await mkdir(config.state.journalRoot,{mode:0o700});await mkdir(config.state.anchorRoot,{mode:0o700});
  await writeFile(config.encryption.keyFile,key,{mode:0o600,flag:'wx'});
  await writeFile(config.state.signingKeyFile,keys.privateKey.export({format:'pem',type:'pkcs8'}),{mode:0o600,flag:'wx'});
  await writeFile(config.state.publicKeyFile,keys.publicKey.export({format:'pem',type:'spki'}),{mode:0o600,flag:'wx'});
  await writeFile(join(config.state.journalRoot,'operator-note'),'keep-record-note',{mode:0o600});await writeFile(join(config.state.anchorRoot,'operator-note'),'keep-anchor-note',{mode:0o600});
  // This is an authenticated AES-GCM fixture, not a recoverable pg_dump.
  const plaintext=Buffer.from('native publication scheduling fixture'),magic=Buffer.from('CCPBK001'),iv=randomBytes(12),cipher=createCipheriv('aes-256-gcm',key,iv);cipher.setAAD(magic);
  const ciphertext=Buffer.concat([magic,iv,cipher.update(plaintext),cipher.final(),cipher.getAuthTag()]);config.limits.exportMaxBytes=plaintext.length;
  const manifest={version:1,status:'captured_unverified',startedAt:new Date(now-2000).toISOString(),capturedAt:new Date(now-1000).toISOString(),recoveryVerified:false,offhostVerified:false,latestStateVerified:false,source:{database:'fixture',schemaVersion:22,releaseVerified:false},fingerprints:['["public.tenants", 1, -9223372036854775808]'],archive:{file:'database.dump.enc',format:'CCPBK001',bytes:ciphertext.length,sha256:sha(ciphertext),plaintextBytes:plaintext.length,plaintextSha256:sha(plaintext)}};
  const manifestFile=join(candidateDirectory,'capture.json'),manifestBytes=Buffer.from(JSON.stringify(manifest)+'\n');
  await writeFile(manifestFile,manifestBytes,{mode:0o600});await writeFile(join(candidateDirectory,'database.dump.enc'),ciphertext,{mode:0o600});
  const at=new Date(now).toISOString();
  const state:RecoverySourceState={version:1,status:'exported_unverified',source:{id:sourceId,database:'fixture',schema_version:22,authenticated:false},exported_at:at,deletion:{schema_version:2,exported_at:at,projects:[],jobs:[],barriers:[],retention:[]},integrity:{blocked:false,reason:null,detected_at:null,details:{},audit_high_water:'0',streams:[]},kafka_observation_verified:false,latest_state_verified:false};
  await writeFile(stateFile,JSON.stringify(state),{mode:0o600});
  const inputFiles=new Map<string,Buffer>();for(const path of [stateFile,manifestFile,join(candidateDirectory,'database.dump.enc'),config.state.signingKeyFile,config.state.publicKeyFile,config.encryption.keyFile])inputFiles.set(path,await readFile(path));
  scheduling.journal=config.state.journalRoot;scheduling.anchors=config.state.anchorRoot;scheduling.manifest=manifestFile;scheduling.signer=config.state.signingKeyFile;
  scheduling.recordLinked=false;scheduling.headRenamed=false;scheduling.lockCreated=false;scheduling.manifestRewritten=false;scheduling.finalManifestReadObserved=false;scheduling.signerReadObserved=false;scheduling.signerNativeBytes=0;scheduling.signerClosed=false;
  return{parent,config,candidateDirectory,stateFile,now,keys,key,manifestBytes,inputFiles,options:{config,stateFile,candidateDirectory,expectedHeadFile:null,now}};
}
type Fixture=Awaited<ReturnType<typeof createFixture>>;
async function fixture(run:(value:Fixture)=>Promise<void>){
  const value=await createFixture();
  const cleanup=async(path:string):Promise<void>=>{
    const absolute=resolve(path);if(absolute!==value.parent&&!absolute.startsWith(value.parent+sep))throw Error('fixture_cleanup_outside_owned_root');
    const info=await lstat(absolute);if(!info.isDirectory()||info.isSymbolicLink()){await unlink(absolute);return;}
    await chmod(absolute,0o700);for(const name of await readdir(absolute))await cleanup(join(absolute,name));await rmdir(absolute);
  };
  try{await run(value);}finally{scheduling.mode='';scheduling.abort=undefined;scheduling.replacement=undefined;value.key.fill(0);for(const bytes of value.inputFiles.values())bytes.fill(0);await cleanup(value.parent);}
}
async function preserved(value:Fixture,expectedManifest=value.manifestBytes){
  for(const [path,bytes]of value.inputFiles)expect(await readFile(path)).toEqual(path===scheduling.manifest?expectedManifest:bytes);
  expect(await readFile(join(value.config.state.journalRoot,'operator-note'),'utf8')).toBe('keep-record-note');expect(await readFile(join(value.config.state.anchorRoot,'operator-note'),'utf8')).toBe('keep-anchor-note');
  expect((await readdir(value.config.state.anchorRoot))).not.toContain('.append.lock');expect(scheduling.ownedHandles).toBe(0);
  expect((await readdir(value.candidateDirectory)).sort()).toEqual(['capture.json','database.dump.enc']);
}
function decryptRecord(ciphertext:Buffer,key:Buffer){
  expect(ciphertext.subarray(0,8).toString('ascii')).toBe('CCPBK001');const cipher=createDecipheriv('aes-256-gcm',key,ciphertext.subarray(8,20));cipher.setAAD(ciphertext.subarray(0,8));cipher.setAuthTag(ciphertext.subarray(-16));
  return JSON.parse(Buffer.concat([cipher.update(ciphertext.subarray(20,-16)),cipher.final()]).toString('utf8'));
}

posix('reports uncertainty and retains the signed journal when the manifest changes after actual head publication',async()=>fixture(async value=>{
  const changed=Buffer.concat([value.manifestBytes,Buffer.from('\n')]);scheduling.replacement=changed;scheduling.mode='rewrite-manifest';
  await expect(publishRecoveryStateLocal(value.options)).rejects.toMatchObject({code:'state_publish_uncertain'});
  expect(scheduling.recordLinked).toBe(true);expect(scheduling.headRenamed).toBe(true);expect(scheduling.manifestRewritten).toBe(true);expect(scheduling.finalManifestReadObserved).toBe(true);expect(scheduling.lockCreated).toBe(true);expect(scheduling.ownedHandles).toBe(0);
  expect(sha(changed)).not.toBe(sha(value.manifestBytes));
  const headFile=join(value.config.state.anchorRoot,'head.json'),headBytes=await readFile(headFile),head=parseRecoveryStateAnchor(JSON.parse(headBytes.toString('utf8'))),object=head.sequence+'-'+head.record_hash+'.state.enc';
  const encrypted=await readFile(join(value.config.state.journalRoot,object)),record=decryptRecord(encrypted,value.key);
  const checked=verifyRecoveryStateRecord({record,publicKey:value.keys.publicKey,expectedSourceId:sourceId,now:value.now});expect(checked.anchor).toEqual(head);expect(checked.body.backup).toEqual({id:'candidate-001',manifest_sha256:sha(value.manifestBytes)});
  const full=await verifyRecoveryStateJournal({journalRoot:value.config.state.journalRoot,anchorRoot:value.config.state.anchorRoot,encryptionKeyFile:value.config.encryption.keyFile,publicKey:value.keys.publicKey,expectedSourceId:sourceId,trustedTip:head,now:value.now,timeoutMs:5000});
  expect(full).toMatchObject({status:'journal_verified_unadmitted',recordsVerified:1,anchor:head,sourceEndpointVerified:false,anchorCustodyVerified:false,latestStateVerified:false});expect(full.state.source.authenticated).toBe(false);expect(full.state.kafka_observation_verified).toBe(false);
  expect(await readFile(headFile)).toEqual(headBytes);expect(await readFile(join(value.config.state.journalRoot,object))).toEqual(encrypted);
  expect((await readdir(value.config.state.journalRoot)).sort()).toEqual([object,'operator-note'].sort());expect((await readdir(value.config.state.anchorRoot)).sort()).toEqual(['head.json','operator-note']);await preserved(value,changed);
}));
posix('preserves cancellation after the actual bounded signer read and closes its handle before append',async()=>fixture(async value=>{
  const controller=new AbortController();scheduling.mode='abort-signer';scheduling.abort=()=>controller.abort();
  await expect(publishRecoveryStateLocal({...value.options,signal:controller.signal})).rejects.toMatchObject({code:'state_publish_aborted'});
  expect(scheduling.signerReadObserved).toBe(true);expect(scheduling.signerNativeBytes).toBe(value.inputFiles.get(value.config.state.signingKeyFile)!.length);expect(controller.signal.aborted).toBe(true);expect(scheduling.signerClosed).toBe(true);
  expect(scheduling.lockCreated).toBe(false);expect(scheduling.recordLinked).toBe(false);expect(scheduling.headRenamed).toBe(false);expect(scheduling.manifestRewritten).toBe(false);expect(scheduling.ownedHandles).toBe(0);
  expect(await readdir(value.config.state.journalRoot)).toEqual(['operator-note']);expect(await readdir(value.config.state.anchorRoot)).toEqual(['operator-note']);await preserved(value);
}));
