import {it,expect,vi,afterEach} from 'vitest';
import {mkdtemp,mkdir,writeFile,readFile,readdir,lstat,chmod,unlink,rmdir} from 'node:fs/promises';
import {join,resolve,sep,dirname,basename} from 'node:path';
import {tmpdir} from 'node:os';
import {generateKeyPairSync,randomBytes} from 'node:crypto';
import {signRecoveryStateRecord} from '../packages/server/src/recovery-state-record';
import type {RecoverySourceState} from '../packages/server/src/recovery-state-format';
import {appendRecoveryStateJournal} from '../packages/server/src/recovery-state-journal';

const faults=vi.hoisted(()=>({
  journal:'',anchors:'',keyFile:'',mode:'' as ''|'record-sync'|'head-sync'|'collision',
  fired:false,recordLinked:false,headRenamed:false,collisionPath:'',ownedHandles:0,nativeWritten:0,
  beforeWrite:undefined as undefined|((index:number)=>Promise<void>),
  afterKeyRead:undefined as undefined|(()=>void),keyReadObserved:false,nativeKeyBytes:0,
}));
// Native file contents, metadata, links, rename and close are real. Only test
// scheduling and the specified I/O failure are controlled; this does not prove
// actual disk exhaustion, power-loss durability or forced-process cleanup.
vi.mock('node:fs/promises',async importOriginal=>{
  const native=await importOriginal<typeof import('node:fs/promises')>();
  const failure=()=>Object.assign(Error('synthetic_journal_io_failure'),{code:'EIO'});
  const recordTemp=(path:string)=>dirname(path)===faults.journal&&basename(path).startsWith('.record-')&&path.endsWith('.tmp');
  return {...native,link:async(...args:Parameters<typeof native.link>)=>{
    await native.link(...args);
    if(dirname(String(args[1]))===faults.journal&&String(args[1]).endsWith('.state.enc'))faults.recordLinked=true;
  },rename:async(...args:Parameters<typeof native.rename>)=>{
    await native.rename(...args);if(String(args[1])===join(faults.anchors,'head.json'))faults.headRenamed=true;
  },open:async(...args:Parameters<typeof native.open>)=>{
    const path=String(args[0]),temporary=recordTemp(path);
    if(temporary&&faults.mode==='collision'&&!faults.fired){
      faults.fired=true;faults.collisionPath=path;
      await native.writeFile(path,'unowned-temp-marker',{mode:0o600,flag:'wx'});
    }
    const handle=await native.open(...args);let writes=0,closed=false;faults.ownedHandles++;
    return new Proxy(handle,{get(target,key){
      if(key==='read')return async(...readArgs:unknown[])=>{
        const result=await (target.read as (...args:unknown[])=>Promise<{bytesRead:number}>).apply(target,readArgs);
        if(path===faults.keyFile&&result.bytesRead>0){
          faults.nativeKeyBytes+=result.bytesRead;
          if(!faults.keyReadObserved){faults.keyReadObserved=true;faults.afterKeyRead?.();}
        }
        return result;
      };
      if(key==='write')return async(...writeArgs:unknown[])=>{
        if(temporary){writes++;await faults.beforeWrite?.(writes);}
        const result=await (target.write as (...args:unknown[])=>Promise<{bytesWritten:number}>).apply(target,writeArgs);
        if(temporary)faults.nativeWritten+=result.bytesWritten;return result;
      };
      if(key==='sync')return async()=>{
        const afterRecord=faults.mode==='record-sync'&&faults.recordLinked&&path===faults.journal;
        const afterHead=faults.mode==='head-sync'&&faults.headRenamed&&path===faults.anchors;
        if((afterRecord||afterHead)&&!faults.fired){faults.fired=true;throw failure();}
        return target.sync();
      };
      if(key==='close')return async()=>{try{await target.close();}finally{if(!closed){closed=true;faults.ownedHandles--;}}};
      const value=Reflect.get(target,key,target);return typeof value==='function'?value.bind(target):value;
    }});
  }};
});

afterEach(()=>{faults.beforeWrite=undefined;faults.afterKeyRead=undefined;faults.mode='';vi.restoreAllMocks();});
const posix=it.skipIf(process.platform==='win32');
const sourceId='journal-fault-fixture';
async function createFixture(){
  const parent=await mkdtemp(join(tmpdir(),'ccpanel-journal-fault-')),journalRoot=join(parent,'records'),anchorRoot=join(parent,'anchors'),encryptionKeyFile=join(parent,'encryption-key');
  await mkdir(journalRoot,{mode:0o700});await mkdir(anchorRoot,{mode:0o700});
  await writeFile(encryptionKeyFile,randomBytes(32),{mode:0o600,flag:'wx'});
  await writeFile(join(journalRoot,'operator-note'),'keep-record-note',{mode:0o600});
  await writeFile(join(anchorRoot,'operator-note'),'keep-anchor-note',{mode:0o600});
  const signer=generateKeyPairSync('ed25519'),now=Date.now(),at=new Date(now).toISOString();
  // The padding makes the real encrypted file require more than one64KiB
  // write, while the metadata remains within the64KiB details budget.
  const state:RecoverySourceState={version:1,status:'exported_unverified',source:{id:sourceId,database:'fixture',schema_version:22,authenticated:false},exported_at:at,deletion:{schema_version:2,exported_at:at,projects:[],jobs:[],barriers:[],retention:[]},integrity:{blocked:false,reason:null,detected_at:null,details:{padding:'x'.repeat(60000)},audit_high_water:'0',streams:[]},kafka_observation_verified:false,latest_state_verified:false};
  const signed=signRecoveryStateRecord({state,backup:{id:'fault-fixture',manifest_sha256:'a'.repeat(64)},previous:null,privateKey:signer.privateKey,now});
  faults.journal=journalRoot;faults.anchors=anchorRoot;faults.keyFile=encryptionKeyFile;faults.fired=false;faults.recordLinked=false;faults.headRenamed=false;faults.collisionPath='';faults.nativeWritten=0;faults.keyReadObserved=false;faults.nativeKeyBytes=0;
  return{parent,journalRoot,anchorRoot,encryptionKeyFile,signed,options:{journalRoot,anchorRoot,encryptionKeyFile,publicKey:signer.publicKey,expectedSourceId:sourceId,expectedHead:null,record:signed.record,timeoutMs:5000}};
}
type Fixture=Awaited<ReturnType<typeof createFixture>>;
async function fixture(run:(value:Fixture)=>Promise<void>){
  const value=await createFixture();
  const cleanup=async(path:string):Promise<void>=>{
    const absolute=resolve(path);if(absolute!==value.parent&&!absolute.startsWith(value.parent+sep))throw Error('fixture_cleanup_outside_owned_root');
    const info=await lstat(absolute);if(!info.isDirectory()||info.isSymbolicLink()){await unlink(absolute);return;}
    await chmod(absolute,0o700);for(const name of await readdir(absolute))await cleanup(join(absolute,name));await rmdir(absolute);
  };
  try{await run(value);}finally{faults.mode='';faults.beforeWrite=undefined;faults.afterKeyRead=undefined;await cleanup(value.parent);}
}
async function notesPreserved(value:Fixture){
  expect(await readFile(join(value.journalRoot,'operator-note'),'utf8')).toBe('keep-record-note');
  expect(await readFile(join(value.anchorRoot,'operator-note'),'utf8')).toBe('keep-anchor-note');
  expect((await readdir(value.anchorRoot))).not.toContain('.append.lock');expect(faults.ownedHandles).toBe(0);
}

posix('mid-write abort removes only owned encrypted temp and never creates an object or cursor',async()=>fixture(async value=>{
  let reached!:()=>void,release!:()=>void;
  const ready=new Promise<void>(done=>{reached=done;}),barrier=new Promise<void>(done=>{release=done;});
  faults.beforeWrite=async index=>{if(index===2){reached();await barrier;}};
  const controller=new AbortController();
  const operation=appendRecoveryStateJournal({...value.options,signal:controller.signal}).then(result=>({result,error:undefined}),error=>({result:undefined,error}));
  try{
    await Promise.race([ready,operation.then(()=>{throw Error('append ended before second native encrypted write');})]);
    expect(faults.nativeWritten).toBeGreaterThan(0);controller.abort();release();
    expect((await operation).error?.code).toBe('journal_aborted');expect(faults.recordLinked).toBe(false);expect(faults.headRenamed).toBe(false);
    expect(await readdir(value.journalRoot)).toEqual(['operator-note']);expect(await readdir(value.anchorRoot)).toEqual(['operator-note']);await notesPreserved(value);
  }finally{release();await operation;}
}));
posix('cancellation after an actual AES-key read remains an abort without creating journal state',async()=>fixture(async value=>{
  const before=await readFile(value.encryptionKeyFile),controller=new AbortController();
  faults.afterKeyRead=()=>controller.abort();
  const result=await appendRecoveryStateJournal({...value.options,signal:controller.signal}).then(result=>({result,error:undefined}),error=>({result:undefined,error}));
  expect(faults.keyReadObserved).toBe(true);expect(faults.nativeKeyBytes).toBe(32);expect(result.error?.code).toBe('journal_aborted');
  expect(faults.recordLinked).toBe(false);expect(faults.headRenamed).toBe(false);expect(faults.nativeWritten).toBe(0);
  expect(await readdir(value.journalRoot)).toEqual(['operator-note']);expect(await readdir(value.anchorRoot)).toEqual(['operator-note']);
  expect(await readFile(value.encryptionKeyFile)).toEqual(before);await notesPreserved(value);
}));
posix('simulated fsync EIO after record hardlink retains orphan ciphertext and refuses a blind retry',async()=>fixture(async value=>{
  faults.mode='record-sync';const result=await appendRecoveryStateJournal(value.options).then(result=>({result,error:undefined}),error=>({result:undefined,error}));
  expect(faults.fired).toBe(true);expect(faults.recordLinked).toBe(true);expect(faults.headRenamed).toBe(false);expect(result.error?.code).toBe('journal_publish_uncertain');
  const object='1-'+value.signed.anchor.record_hash+'.state.enc',bytes=await readFile(join(value.journalRoot,object));
  expect(bytes.subarray(0,8).toString('ascii')).toBe('CCPBK001');expect((await readdir(value.journalRoot)).sort()).toEqual([object,'operator-note'].sort());expect(await readdir(value.anchorRoot)).toEqual(['operator-note']);
  faults.mode='';await expect(appendRecoveryStateJournal(value.options)).rejects.toMatchObject({code:'journal_recovery_required'});
  expect(await readFile(join(value.journalRoot,object))).toEqual(bytes);await notesPreserved(value);
}));
posix('simulated anchor-directory fsync EIO after rename retains the new cursor and refuses the old CAS',async()=>fixture(async value=>{
  faults.mode='head-sync';const result=await appendRecoveryStateJournal(value.options).then(result=>({result,error:undefined}),error=>({result:undefined,error}));
  expect(faults.fired).toBe(true);expect(faults.recordLinked).toBe(true);expect(faults.headRenamed).toBe(true);expect(result.error?.code).toBe('journal_publish_uncertain');
  const object='1-'+value.signed.anchor.record_hash+'.state.enc',bytes=await readFile(join(value.journalRoot,object)),head=await readFile(join(value.anchorRoot,'head.json'));
  expect(JSON.parse(head.toString())).toEqual(value.signed.anchor);expect((await readdir(value.journalRoot)).sort()).toEqual([object,'operator-note'].sort());expect((await readdir(value.anchorRoot)).sort()).toEqual(['head.json','operator-note']);
  faults.mode='';await expect(appendRecoveryStateJournal(value.options)).rejects.toMatchObject({code:'journal_head_conflict'});
  expect(await readFile(join(value.journalRoot,object))).toEqual(bytes);expect(await readFile(join(value.anchorRoot,'head.json'))).toEqual(head);await notesPreserved(value);
}));
posix('a native wx collision does not delete a temporary path this append never owned',async()=>fixture(async value=>{
  faults.mode='collision';const result=await appendRecoveryStateJournal(value.options).then(result=>({result,error:undefined}),error=>({result:undefined,error}));
  expect(faults.fired).toBe(true);expect(result.error?.code).toBe('journal_io_failed');expect(faults.recordLinked).toBe(false);expect(faults.headRenamed).toBe(false);
  expect(await readFile(faults.collisionPath,'utf8')).toBe('unowned-temp-marker');expect((await readdir(value.journalRoot)).sort()).toEqual([basename(faults.collisionPath),'operator-note'].sort());expect(await readdir(value.anchorRoot)).toEqual(['operator-note']);await notesPreserved(value);
}));
