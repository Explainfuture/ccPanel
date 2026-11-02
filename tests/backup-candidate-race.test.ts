import {it,expect,vi,afterEach} from 'vitest';
import {mkdtemp,writeFile,readFile,readdir,unlink,rmdir} from 'node:fs/promises';
import {join,basename} from 'node:path';
import {tmpdir} from 'node:os';
import {createHash} from 'node:crypto';
import {inspectBackupCandidate} from '../packages/server/src/backup-candidate';

const instrumentation=vi.hoisted(()=>({
  beforeArchiveRead:undefined as undefined|((index:number)=>Promise<void>),
  afterReaddir:undefined as undefined|(()=>void),
  openHandles:0,nativeArchiveBytes:0,
}));
// Test-only scheduling barrier around actual FileHandle.read. File bytes,
// metadata, open flags and close all use native fs; no production hooks, fake
// file content or guessed delay are used to reach the second archive read.
vi.mock('node:fs/promises',async importOriginal=>{
  const native=await importOriginal<typeof import('node:fs/promises')>();
  return {...native,readdir:async(...args:Parameters<typeof native.readdir>)=>{
    const result=await native.readdir(...args);instrumentation.afterReaddir?.();return result;
  },open:async(...args:Parameters<typeof native.open>)=>{
    const handle=await native.open(...args),archive=basename(String(args[0]))==='database.dump.enc';
    let reads=0,closed=false;instrumentation.openHandles++;
    return new Proxy(handle,{get(target,key){
      if(key==='read')return async(...readArgs:unknown[])=>{
        if(archive)await instrumentation.beforeArchiveRead?.(++reads);
        const result=await (target.read as (...args:unknown[])=>Promise<{bytesRead:number}>).apply(target,readArgs);
        if(archive)instrumentation.nativeArchiveBytes+=result.bytesRead;
        return result;
      };
      if(key==='close')return async()=>{try{await target.close();}finally{if(!closed){closed=true;instrumentation.openHandles--;}}};
      const value=Reflect.get(target,key,target);return typeof value==='function'?value.bind(target):value;
    }});
  }};
});
afterEach(()=>{instrumentation.beforeArchiveRead=undefined;instrumentation.afterReaddir=undefined;vi.restoreAllMocks();});
const posix=it.skipIf(process.platform==='win32');
const deferred=()=>{let resolve!:()=>void;const promise=new Promise<void>(done=>{resolve=done;});return{promise,resolve};};
async function fixture(run:(directory:string)=>Promise<void>){
  const directory=await mkdtemp(join(tmpdir(),'ccpanel-inspect-race-'));
  const cipher=Buffer.concat([Buffer.from('CCPBK001'),Buffer.alloc(1024*1024,7)]);
  const manifest={version:1,status:'captured_unverified',startedAt:'2026-10-24T00:00:00.000Z',capturedAt:'2026-10-24T00:00:01.000Z',recoveryVerified:false,offhostVerified:false,latestStateVerified:false,source:{database:'fixture',schemaVersion:22,releaseVerified:false},fingerprints:['["public.tenants", 1, -9223372036854775808]'],archive:{file:'database.dump.enc',format:'CCPBK001',bytes:cipher.length,sha256:createHash('sha256').update(cipher).digest('hex'),plaintextBytes:cipher.length-36,plaintextSha256:'a'.repeat(64)}};
  instrumentation.nativeArchiveBytes=0;
  try{
    await writeFile(join(directory,'capture.json'),JSON.stringify(manifest),{mode:0o600});
    await writeFile(join(directory,'database.dump.enc'),cipher,{mode:0o600});await run(directory);
  }finally{
    instrumentation.beforeArchiveRead=undefined;instrumentation.afterReaddir=undefined;
    for(const file of await readdir(directory))await unlink(join(directory,file));await rmdir(directory);
  }
}
const options=(directory:string)=>({directory,expectedSchema:22,maxBytes:2*1024*1024,timeoutMs:2000});
async function whileReading(directory:string,mutate:()=>Promise<void>|void,expectedCode:string,signal?:AbortSignal){
  const reached=deferred(),release=deferred();
  instrumentation.beforeArchiveRead=async index=>{if(index===2){reached.resolve();await release.promise;}};
  // Attach rejection handling immediately, before waiting at the barrier.
  const result=inspectBackupCandidate({...options(directory),signal}).then(value=>({value,error:undefined}),error=>({value:undefined,error}));
  try{
    await Promise.race([reached.promise,result.then(()=>{throw Error('inspection ended before second native archive read');})]);
    expect(instrumentation.nativeArchiveBytes).toBeGreaterThan(0);expect(instrumentation.nativeArchiveBytes).toBeLessThanOrEqual(65536);
    await mutate();release.resolve();
    expect((await result).error?.code).toBe(expectedCode);
    expect(instrumentation.openHandles).toBe(0);
    expect((await readdir(directory)).sort()).toEqual(['capture.json','database.dump.enc']);
  }finally{release.resolve();await result;}
}
posix('cancels between real archive chunks and closes every owned handle',async()=>fixture(async directory=>{
  const before=await readFile(join(directory,'capture.json')),controller=new AbortController();
  await whileReading(directory,()=>controller.abort(),'backup_candidate_aborted',controller.signal);
  expect(await readFile(join(directory,'capture.json'))).toEqual(before);
}));
posix('expires a monotonic deadline during a real multi-chunk read',async()=>fixture(async directory=>{
  // Only the monotonic clock is controlled. Advance after observing the first
  // native chunk, rather than relying on filesystem speed or sleeping.
  let now=1000;vi.spyOn(performance,'now').mockImplementation(()=>now);
  await whileReading(directory,()=>{now=3001;},'backup_candidate_timeout');
}));
posix('rejects a manifest rewritten while the native archive handle is open',async()=>fixture(async directory=>{
  await whileReading(directory,async()=>{
    const path=join(directory,'capture.json'),manifest=JSON.parse(await readFile(path,'utf8'));
    manifest.capturedAt='2026-10-24T00:00:02.000Z';await writeFile(path,JSON.stringify(manifest));
  },'backup_candidate_changed');
}));
posix('rechecks the deadline after the final native directory listing',async()=>fixture(async directory=>{
  let now=1000,listings=0;vi.spyOn(performance,'now').mockImplementation(()=>now);
  instrumentation.afterReaddir=()=>{if(++listings===2)now=3001;};
  await expect(inspectBackupCandidate(options(directory))).rejects.toMatchObject({code:'backup_candidate_timeout'});
  expect(listings).toBe(2);expect(instrumentation.openHandles).toBe(0);
}));
