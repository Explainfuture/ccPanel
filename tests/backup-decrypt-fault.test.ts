import {it,expect,vi,afterEach} from 'vitest';
import {mkdtemp,mkdir,writeFile,readFile,readdir,unlink,rmdir} from 'node:fs/promises';
import {join,dirname,basename} from 'node:path';
import {tmpdir} from 'node:os';
import {createCipheriv,createHash,randomBytes} from 'node:crypto';
import {decryptBackupCandidate} from '../packages/server/src/backup-decrypt';

const faults=vi.hoisted(()=>({
  output:'',parent:'',mode:'' as ''|'write'|'sync-after'|'race'|'collision'|'corrupt-stage',collisionPath:'',fired:false,published:false,
  ownedHandles:0,nativeWritten:0,beforeWrite:undefined as undefined|((index:number)=>Promise<void>),
}));
// Deterministic scheduling/fault wrappers around actual native fs. These are
// simulated I/O errors, not evidence for real disk exhaustion or power loss.
vi.mock('node:fs/promises',async importOriginal=>{
  const native=await importOriginal<typeof import('node:fs/promises')>();
  const failure=(code:string)=>Object.assign(Error('synthetic_io_failure'),{code});
  return {...native,link:async(...args:Parameters<typeof native.link>)=>{
    if(String(args[1])===faults.output&&faults.mode==='race'&&!faults.fired){faults.fired=true;await native.writeFile(faults.output,'racing-winner',{flag:'wx',mode:0o600});}
    await native.link(...args);if(String(args[1])===faults.output)faults.published=true;
  },open:async(...args:Parameters<typeof native.open>)=>{
    const path=String(args[0]);
    const plaintext=dirname(path)===faults.parent&&basename(path).startsWith('.result.dump.')&&path.endsWith('.tmp');
    if(plaintext&&faults.mode==='collision'&&!faults.fired){faults.fired=true;faults.collisionPath=path;await native.writeFile(path,'unowned-existing-temp',{flag:'wx',mode:0o600});}
    const handle=await native.open(...args);
    let writes=0,closed=false;faults.ownedHandles++;
    return new Proxy(handle,{get(target,key){
      if(key==='write')return async(...writeArgs:unknown[])=>{
        if(plaintext){writes++;await faults.beforeWrite?.(writes);if(faults.mode==='write'&&writes===2&&!faults.fired){faults.fired=true;throw failure('ENOSPC');}}
        const result=await (target.write as (...args:unknown[])=>Promise<{bytesWritten:number}>).apply(target,writeArgs);
        if(plaintext)faults.nativeWritten+=result.bytesWritten;return result;
      };
      if(key==='sync')return async()=>{
        if(path===faults.parent&&faults.mode==='sync-after'&&faults.published&&!faults.fired){faults.fired=true;throw failure('EIO');}
        const result=await target.sync();
        if(plaintext&&faults.mode==='corrupt-stage'&&!faults.fired){
          // The decryptor has already authenticated/hash-checked its in-memory
          // plaintext. Alter the actual staged file at equal length so only
          // the subsequent bounded native readback can detect this mutation.
          faults.fired=true;const bytes=await native.readFile(path);bytes[100]^=1;await native.writeFile(path,bytes);
        }
        return result;
      };
      if(key==='close')return async()=>{try{await target.close();}finally{if(!closed){closed=true;faults.ownedHandles--;}}};
      const value=Reflect.get(target,key,target);return typeof value==='function'?value.bind(target):value;
    }});
  }};
});
afterEach(()=>{faults.beforeWrite=undefined;faults.mode='';vi.restoreAllMocks();});
const posix=it.skipIf(process.platform==='win32');
const plain=Buffer.alloc(512*1024,37);
type Options={directory:string;keyFile:string;output:string;expectedSchema:number;maxBytes:number;timeoutMs:number};
async function fixture(run:(options:Options)=>Promise<void>){
  const parent=await mkdtemp(join(tmpdir(),'ccpanel-decrypt-fault-')),directory=join(parent,'candidate'),keyFile=join(parent,'key'),output=join(parent,'result.dump');
  await mkdir(directory,{mode:0o700});
  const key=randomBytes(32),iv=randomBytes(12),magic=Buffer.from('CCPBK001'),cipher=createCipheriv('aes-256-gcm',key,iv);cipher.setAAD(magic);
  const encrypted=Buffer.concat([magic,iv,cipher.update(plain),cipher.final(),cipher.getAuthTag()]);
  const manifest={version:1,status:'captured_unverified',startedAt:'2026-11-02T00:00:00.000Z',capturedAt:'2026-11-02T00:00:01.000Z',recoveryVerified:false,offhostVerified:false,latestStateVerified:false,source:{database:'fixture',schemaVersion:22,releaseVerified:false},fingerprints:['["public.tenants", 1, -9223372036854775808]'],archive:{file:'database.dump.enc',format:'CCPBK001',bytes:encrypted.length,sha256:createHash('sha256').update(encrypted).digest('hex'),plaintextBytes:plain.length,plaintextSha256:createHash('sha256').update(plain).digest('hex')}};
  await writeFile(keyFile,key,{mode:0o600});await writeFile(join(directory,'capture.json'),JSON.stringify(manifest),{mode:0o600});await writeFile(join(directory,'database.dump.enc'),encrypted,{mode:0o600});key.fill(0);
  faults.output=output;faults.parent=parent;faults.fired=false;faults.published=false;faults.nativeWritten=0;faults.collisionPath='';
  try{await run({directory,keyFile,output,expectedSchema:22,maxBytes:encrypted.length,timeoutMs:5000});}
  finally{
    faults.beforeWrite=undefined;faults.mode='';
    for(const name of await readdir(directory))await unlink(join(directory,name));await rmdir(directory);
    for(const name of await readdir(parent))await unlink(join(parent,name));await rmdir(parent);
  }
}
async function noPlaintext(options:Options){
  expect(faults.ownedHandles).toBe(0);expect(faults.published).toBe(false);
  expect((await readdir(dirname(options.output))).sort()).toEqual(['candidate','key']);
}
posix('mid-decrypt abort closes handles and removes only owned unpublished plaintext',async()=>fixture(async options=>{
  let release!:()=>void,reached!:()=>void;
  const barrier=new Promise<void>(resolve=>{release=resolve;}),ready=new Promise<void>(resolve=>{reached=resolve;});
  faults.beforeWrite=async index=>{if(index===2){reached();await barrier;}};
  const controller=new AbortController(),operation=decryptBackupCandidate({...options,signal:controller.signal}).then(value=>({value,error:undefined}),error=>({value:undefined,error}));
  try{
    await Promise.race([ready,operation.then(()=>{throw Error('decrypt ended before second native plaintext write');})]);
    expect(faults.nativeWritten).toBeGreaterThan(0);controller.abort();release();
    expect((await operation).error?.code).toBe('decrypt_aborted');await noPlaintext(options);
  }finally{release();await operation;}
}));
posix('simulated ENOSPC after a real plaintext chunk leaves no output or owned temp',async()=>fixture(async options=>{
  faults.mode='write';const result=await decryptBackupCandidate(options).then(value=>({value,error:undefined}),error=>({value:undefined,error}));
  expect(faults.fired).toBe(true);expect(faults.nativeWritten).toBeGreaterThan(0);expect(result.error?.code).toBe('decrypt_failed');await noPlaintext(options);
}));
posix('a racing output survives unchanged and unpublished plaintext is cleaned',async()=>fixture(async options=>{
  faults.mode='race';const result=await decryptBackupCandidate(options).then(value=>({value,error:undefined}),error=>({value:undefined,error}));
  expect(faults.fired).toBe(true);expect(result.error?.code).toBe('decrypt_output_exists');expect(faults.published).toBe(false);expect(faults.ownedHandles).toBe(0);
  expect(await readFile(options.output,'utf8')).toBe('racing-winner');expect((await readdir(dirname(options.output))).sort()).toEqual(['candidate','key','result.dump']);
}));
posix('a wx collision never removes a temporary file this operation did not create',async()=>fixture(async options=>{
  faults.mode='collision';const result=await decryptBackupCandidate(options).then(value=>({value,error:undefined}),error=>({value:undefined,error}));
  expect(faults.fired).toBe(true);expect(result.error?.code).toBe('decrypt_failed');expect(faults.published).toBe(false);expect(faults.ownedHandles).toBe(0);
  expect(await readFile(faults.collisionPath,'utf8')).toBe('unowned-existing-temp');expect((await readdir(dirname(options.output))).sort()).not.toContain('result.dump');
}));
posix('post-fsync staged plaintext mutation is rejected by native readback and cleaned',async()=>fixture(async options=>{
  faults.mode='corrupt-stage';const result=await decryptBackupCandidate(options).then(value=>({value,error:undefined}),error=>({value:undefined,error}));
  expect(faults.fired).toBe(true);expect(faults.nativeWritten).toBe(plain.length);expect(result.error?.code).toBe('decrypt_plaintext_mismatch');await noPlaintext(options);
}));
posix('post-link directory fsync failure retains authenticated output and reports uncertainty',async()=>fixture(async options=>{
  faults.mode='sync-after';const result=await decryptBackupCandidate(options).then(value=>({value,error:undefined}),error=>({value:undefined,error}));
  expect(faults.fired).toBe(true);expect(result.error?.code).toBe('decrypt_publish_uncertain');expect(faults.published).toBe(true);expect(faults.ownedHandles).toBe(0);
  expect(await readFile(options.output)).toEqual(plain);expect((await readdir(dirname(options.output))).sort()).toEqual(['candidate','key','result.dump']);
}));
