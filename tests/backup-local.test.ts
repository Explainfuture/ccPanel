import {it,expect} from 'vitest';
import {mkdtemp,mkdir,writeFile,readFile,readdir,unlink,rmdir,chmod,stat,symlink} from 'node:fs/promises';
import {join,sep} from 'node:path';
import {tmpdir} from 'node:os';
import {createHash} from 'node:crypto';
import {copyBackupCandidateToLocal} from '../packages/server/src/backup-local';

// These synthetic bytes exercise transport/hash integrity only, not AES-GCM
// authentication. Real encrypted PG candidates are covered by the PG suite.
const cipher=Buffer.concat([Buffer.from('CCPBK001'),Buffer.alloc(512*1024,7)]);
const manifest=()=>({version:1,status:'captured_unverified',startedAt:'2026-11-02T00:00:00.000Z',capturedAt:'2026-11-02T00:00:01.000Z',recoveryVerified:false,offhostVerified:false,latestStateVerified:false,source:{database:'fixture',schemaVersion:22,releaseVerified:false},fingerprints:['["public.tenants", 1, -9223372036854775808]'],archive:{file:'database.dump.enc',format:'CCPBK001',bytes:cipher.length,sha256:createHash('sha256').update(cipher).digest('hex'),plaintextBytes:cipher.length-36,plaintextSha256:'a'.repeat(64)}});
const posix=it.skipIf(process.platform==='win32');
type Fixture={sourceDirectory:string;destinationRoot:string;parent:string};
async function fixture(run:(value:Fixture)=>Promise<void>){
  const parent=await mkdtemp(join(tmpdir(),'ccpanel-local-copy-'));
  const sourceDirectory=join(parent,'source'),destinationRoot=join(parent,'destination');
  await mkdir(sourceDirectory,{mode:0o700});await mkdir(destinationRoot,{mode:0o700});
  await writeFile(join(sourceDirectory,'capture.json'),JSON.stringify(manifest()),{mode:0o600});
  await writeFile(join(sourceDirectory,'database.dump.enc'),cipher,{mode:0o600});
  const cleanup=async(path:string):Promise<void>=>{
    if(path!==parent&&!path.startsWith(parent+sep))throw Error('fixture_cleanup_outside_owned_parent');
    await chmod(path,0o700);
    for(const entry of await readdir(path,{withFileTypes:true})){
      const child=join(path,entry.name);
      if(entry.isDirectory()&&!entry.isSymbolicLink())await cleanup(child);else await unlink(child);
    }
    await rmdir(path);
  };
  try{await run({sourceDirectory,destinationRoot,parent});}finally{await cleanup(parent);}
}
const options=({sourceDirectory,destinationRoot}:Fixture)=>({sourceDirectory,destinationRoot,id:'candidate-001',expectedSchema:22,maxBytes:cipher.length,timeoutMs:5000});

posix('copies exact private files, re-reads destination and never promotes candidate trust',async()=>fixture(async value=>{
  const before=await readFile(join(value.sourceDirectory,'capture.json'));
  const result=await copyBackupCandidateToLocal(options(value));
  expect(result).toMatchObject({status:'copied_unverified',bytes:cipher.length,sha256:manifest().archive.sha256,destinationReadbackChecked:true,sourceManifestUnchanged:true,recoveryVerified:false,offhostVerified:false,latestStateVerified:false,sourceAuthenticated:false});
  const destination=join(value.destinationRoot,'candidate-001');
  expect((await readdir(value.destinationRoot)).sort()).toEqual(['candidate-001']);
  expect((await readdir(destination)).sort()).toEqual(['capture.json','database.dump.enc']);
  expect(await readFile(join(destination,'capture.json'))).toEqual(before);
  expect(await readFile(join(destination,'database.dump.enc'))).toEqual(cipher);
  expect(await readFile(join(value.sourceDirectory,'capture.json'))).toEqual(before);
  expect(await readFile(join(value.sourceDirectory,'database.dump.enc'))).toEqual(cipher);
  expect((await stat(destination)).mode&0o777).toBe(0o700);
  for(const name of ['capture.json','database.dump.enc'])expect((await stat(join(destination,name))).mode&0o777).toBe(0o600);
}));

posix('refuses repeat IDs and leaves an existing completed candidate byte-for-byte intact',async()=>fixture(async value=>{
  await copyBackupCandidateToLocal(options(value));
  const destination=join(value.destinationRoot,'candidate-001'),before=await readFile(join(destination,'capture.json'));
  await expect(copyBackupCandidateToLocal(options(value))).rejects.toMatchObject({code:'local_copy_exists'});
  expect(await readFile(join(destination,'capture.json'))).toEqual(before);
  expect(await readFile(join(destination,'database.dump.enc'))).toEqual(cipher);
}));

posix('one of two racing publishers wins without replacing its files',async()=>fixture(async value=>{
  const results=await Promise.allSettled([copyBackupCandidateToLocal(options(value)),copyBackupCandidateToLocal(options(value))]);
  expect(results.filter(result=>result.status==='fulfilled')).toHaveLength(1);
  const failure=results.find(result=>result.status==='rejected') as PromiseRejectedResult;
  expect(failure.reason).toMatchObject({code:'local_copy_exists'});
  expect(await readFile(join(value.destinationRoot,'candidate-001','database.dump.enc'))).toEqual(cipher);
  expect((await readdir(value.destinationRoot)).sort()).toEqual(['candidate-001']);
}));

posix('does not reuse or remove an incomplete set or unknown files',async()=>fixture(async value=>{
  const destination=join(value.destinationRoot,'candidate-001');await mkdir(destination,{mode:0o700});
  await writeFile(join(destination,'operator-note'),'keep-existing',{mode:0o600});
  await expect(copyBackupCandidateToLocal(options(value))).rejects.toMatchObject({code:'local_copy_exists'});
  expect(await readFile(join(destination,'operator-note'),'utf8')).toBe('keep-existing');
  expect(await readdir(destination)).toEqual(['operator-note']);
}));

posix('rejects a corrupt source before reserving destination space',async()=>fixture(async value=>{
  const changed=Buffer.from(cipher);changed[100]^=1;await writeFile(join(value.sourceDirectory,'database.dump.enc'),changed);
  await expect(copyBackupCandidateToLocal(options(value))).rejects.toMatchObject({code:'backup_candidate_cipher_mismatch'});
  expect(await readdir(value.destinationRoot)).toEqual([]);
  expect(await readFile(join(value.sourceDirectory,'database.dump.enc'))).toEqual(changed);
}));

posix('refuses unsafe destination permissions, symlink roots and overlapping trees',async()=>fixture(async value=>{
  await chmod(value.destinationRoot,0o755);
  await expect(copyBackupCandidateToLocal(options(value))).rejects.toMatchObject({code:'local_copy_root_unsafe'});
  await chmod(value.destinationRoot,0o700);
  const alias=join(value.parent,'alias');await symlink(value.destinationRoot,alias,'dir');
  await expect(copyBackupCandidateToLocal({...options(value),destinationRoot:alias})).rejects.toMatchObject({code:'local_copy_root_unsafe'});
  await expect(copyBackupCandidateToLocal({...options(value),destinationRoot:value.sourceDirectory})).rejects.toMatchObject({code:'local_copy_overlap'});
  expect(await readdir(value.destinationRoot)).toEqual([]);
}));

it('rejects invalid IDs before touching any path',async()=>{
  const base={sourceDirectory:join(tmpdir(),'unused-source'),destinationRoot:join(tmpdir(),'unused-destination'),id:'../overwrite',expectedSchema:22,maxBytes:1024,timeoutMs:5000};
  await expect(copyBackupCandidateToLocal(base)).rejects.toMatchObject({code:'local_copy_options_invalid'});
});
it('rejects pre-aborted work before touching any path',async()=>{
  const controller=new AbortController();controller.abort();
  await expect(copyBackupCandidateToLocal({sourceDirectory:join(tmpdir(),'unused-source'),destinationRoot:join(tmpdir(),'unused-destination'),id:'aborted',expectedSchema:22,maxBytes:1024,timeoutMs:5000,signal:controller.signal})).rejects.toMatchObject({code:'local_copy_aborted'});
});
it.skipIf(process.platform!=='win32')('refuses unverified Windows ACLs',async()=>{
  await expect(copyBackupCandidateToLocal({sourceDirectory:join(tmpdir(),'unused-source'),destinationRoot:join(tmpdir(),'unused-destination'),id:'windows',expectedSchema:22,maxBytes:1024,timeoutMs:5000})).rejects.toMatchObject({code:'local_copy_permissions_unverified'});
});
