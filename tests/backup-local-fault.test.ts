import {it,expect,vi,afterEach} from 'vitest';
import {mkdtemp,mkdir,writeFile,readFile,readdir,unlink,rmdir,stat} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {createHash} from 'node:crypto';
import {copyBackupCandidateToLocal} from '../packages/server/src/backup-local';

const faults=vi.hoisted(()=>({
  output:'',mode:'' as ''|'write'|'sync-before'|'sync-after',published:false,fired:false,
  ownedHandles:0,nativeWritten:0,beforeWrite:undefined as undefined|((index:number)=>Promise<void>),
}));
// Controlled faults around actual native file I/O, not a real full disk or
// process crash. Only the requested write/sync fails; all bytes, metadata,
// hardlinks and handle cleanup otherwise use the actual filesystem.
vi.mock('node:fs/promises',async importOriginal=>{
  const native=await importOriginal<typeof import('node:fs/promises')>();
  const failure=(code:string)=>Object.assign(Error('synthetic_io_failure'),{code});
  return {...native,link:async(...args:Parameters<typeof native.link>)=>{
    await native.link(...args);if(String(args[1])===join(faults.output,'capture.json'))faults.published=true;
  },open:async(...args:Parameters<typeof native.open>)=>{
    const handle=await native.open(...args),path=String(args[0]);let writes=0,closed=false;
    faults.ownedHandles++;
    return new Proxy(handle,{get(target,key){
      if(key==='write')return async(...writeArgs:unknown[])=>{
        const archive=path===join(faults.output,'database.dump.enc');
        if(archive){await faults.beforeWrite?.(++writes);if(faults.mode==='write'&&writes===2&&!faults.fired){faults.fired=true;throw failure('ENOSPC');}}
        const result=await (target.write as (...args:unknown[])=>Promise<{bytesWritten:number}>).apply(target,writeArgs);
        if(archive)faults.nativeWritten+=result.bytesWritten;return result;
      };
      if(key==='sync')return async()=>{
        const before=faults.mode==='sync-before'&&path===join(faults.output,'database.dump.enc');
        const after=faults.mode==='sync-after'&&faults.published&&path===faults.output;
        if((before||after)&&!faults.fired){faults.fired=true;throw failure('EIO');}
        return target.sync();
      };
      if(key==='close')return async()=>{try{await target.close();}finally{if(!closed){closed=true;faults.ownedHandles--;}}};
      const value=Reflect.get(target,key,target);return typeof value==='function'?value.bind(target):value;
    }});
  }};
});
afterEach(()=>{faults.beforeWrite=undefined;faults.mode='';vi.restoreAllMocks();});
const posix=it.skipIf(process.platform==='win32');
const cipher=Buffer.concat([Buffer.from('CCPBK001'),Buffer.alloc(512*1024,7)]);
const manifest=()=>({version:1,status:'captured_unverified',startedAt:'2026-11-02T00:00:00.000Z',capturedAt:'2026-11-02T00:00:01.000Z',recoveryVerified:false,offhostVerified:false,latestStateVerified:false,source:{database:'fixture',schemaVersion:22,releaseVerified:false},fingerprints:['["public.tenants", 1, -9223372036854775808]'],archive:{file:'database.dump.enc',format:'CCPBK001',bytes:cipher.length,sha256:createHash('sha256').update(cipher).digest('hex'),plaintextBytes:cipher.length-36,plaintextSha256:'a'.repeat(64)}});
type Fixture={sourceDirectory:string;destinationRoot:string;id:string;expectedSchema:number;maxBytes:number;timeoutMs:number};
async function fixture(run:(options:Fixture)=>Promise<void>){
  const parent=await mkdtemp(join(tmpdir(),'ccpanel-local-fault-')),sourceDirectory=join(parent,'source'),destinationRoot=join(parent,'destination');
  await mkdir(sourceDirectory,{mode:0o700});await mkdir(destinationRoot,{mode:0o700});
  await writeFile(join(sourceDirectory,'capture.json'),JSON.stringify(manifest()),{mode:0o600});await writeFile(join(sourceDirectory,'database.dump.enc'),cipher,{mode:0o600});
  faults.output=join(destinationRoot,'candidate-001');faults.published=false;faults.fired=false;faults.nativeWritten=0;
  try{await run({sourceDirectory,destinationRoot,id:'candidate-001',expectedSchema:22,maxBytes:cipher.length,timeoutMs:5000});}
  finally{
    faults.beforeWrite=undefined;faults.mode='';
    for(const item of await readdir(destinationRoot,{withFileTypes:true})){
      const directory=join(destinationRoot,item.name);for(const file of await readdir(directory))await unlink(join(directory,file));await rmdir(directory);
    }
    await rmdir(destinationRoot);for(const file of await readdir(sourceDirectory))await unlink(join(sourceDirectory,file));await rmdir(sourceDirectory);await rmdir(parent);
  }
}
async function preservedIncomplete(options:Fixture){
  expect(faults.ownedHandles).toBe(0);expect(faults.published).toBe(false);
  const names=await readdir(faults.output);expect(names).not.toContain('capture.json');expect(names).toContain('database.dump.enc');
  const partial=await readFile(join(faults.output,'database.dump.enc'));expect(partial.length).toBeGreaterThan(0);
  faults.mode='';faults.beforeWrite=undefined;
  await expect(copyBackupCandidateToLocal(options)).rejects.toMatchObject({code:'local_copy_exists'});
  expect(await readFile(join(faults.output,'database.dump.enc'))).toEqual(partial);expect(faults.ownedHandles).toBe(0);
}
posix('mid-copy abort closes native handles and preserves an unpublished partial candidate',async()=>fixture(async options=>{
  let release!:()=>void,reached!:()=>void;
  const barrier=new Promise<void>(resolve=>{release=resolve;}),ready=new Promise<void>(resolve=>{reached=resolve;});
  faults.beforeWrite=async index=>{if(index===2){reached();await barrier;}};
  const controller=new AbortController();
  const operation=copyBackupCandidateToLocal({...options,signal:controller.signal}).then(value=>({value,error:undefined}),error=>({value:undefined,error}));
  try{
    await Promise.race([ready,operation.then(()=>{throw Error('copy ended before second native write');})]);
    expect(faults.nativeWritten).toBeGreaterThan(0);controller.abort();release();
    expect((await operation).error?.code).toBe('local_copy_aborted');await preservedIncomplete(options);
  }finally{release();await operation;}
}));
posix('simulated ENOSPC after a native chunk never publishes capture.json',async()=>fixture(async options=>{
  faults.mode='write';await expect(copyBackupCandidateToLocal(options)).rejects.toMatchObject({code:'local_copy_failed'});
  expect(faults.fired).toBe(true);expect(faults.nativeWritten).toBeGreaterThan(0);expect(faults.nativeWritten).toBeLessThan(cipher.length);await preservedIncomplete(options);
}));
posix('simulated file fsync failure retains ciphertext without publishing a manifest',async()=>fixture(async options=>{
  faults.mode='sync-before';await expect(copyBackupCandidateToLocal(options)).rejects.toMatchObject({code:'local_copy_failed'});
  expect(faults.fired).toBe(true);expect((await stat(join(faults.output,'database.dump.enc'))).size).toBe(cipher.length);await preservedIncomplete(options);
}));
posix('post-link directory fsync failure reports publication uncertainty without deleting files',async()=>fixture(async options=>{
  faults.mode='sync-after';await expect(copyBackupCandidateToLocal(options)).rejects.toMatchObject({code:'local_copy_publish_uncertain'});
  expect(faults.fired).toBe(true);expect(faults.published).toBe(true);expect(faults.ownedHandles).toBe(0);
  const captured=await readFile(join(faults.output,'capture.json')),data=await readFile(join(faults.output,'database.dump.enc'));
  expect(JSON.parse(captured.toString()).status).toBe('captured_unverified');expect(data).toEqual(cipher);
  faults.mode='';await expect(copyBackupCandidateToLocal(options)).rejects.toMatchObject({code:'local_copy_exists'});
  expect(await readFile(join(faults.output,'capture.json'))).toEqual(captured);expect(faults.ownedHandles).toBe(0);
}));
