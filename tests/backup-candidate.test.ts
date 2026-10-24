import {it,expect} from 'vitest';
import {mkdtemp,writeFile,readFile,chmod,symlink,unlink,readdir,rmdir} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {createHash} from 'node:crypto';
import {parseBackupCandidate,inspectBackupCandidate} from '../packages/server/src/backup-candidate';
const cipher=Buffer.concat([Buffer.from('CCPBK001'),Buffer.alloc(100,7)]);
const manifest=()=>({version:1,status:'captured_unverified',startedAt:'2026-10-24T00:00:00.000Z',capturedAt:'2026-10-24T00:00:01.000Z',recoveryVerified:false,offhostVerified:false,latestStateVerified:false,source:{database:'fixture',schemaVersion:22,releaseVerified:false},fingerprints:['["public.tenants", 1, -9223372036854775808]'],archive:{file:'database.dump.enc',format:'CCPBK001',bytes:cipher.length,sha256:createHash('sha256').update(cipher).digest('hex'),plaintextBytes:cipher.length-36,plaintextSha256:'a'.repeat(64)}});
it('keeps exact PG bigint text and rejects promoted, unknown or unsafe metadata',()=>{
  expect(parseBackupCandidate(manifest()).fingerprints).toEqual(manifest().fingerprints);
  for(const change of [
    (m:any)=>m.recoveryVerified=true,(m:any)=>m.source.releaseVerified=true,(m:any)=>m.extra=true,
    (m:any)=>m.archive.file='../key',(m:any)=>m.archive.bytes++, (m:any)=>m.archive.sha256='X'.repeat(64),
    (m:any)=>m.startedAt='2026-02-30T00:00:00.000Z',(m:any)=>m.capturedAt='2025-10-24T00:00:00.000Z',
    (m:any)=>m.fingerprints.push(m.fingerprints[0]),(m:any)=>m.fingerprints=['["public.tenants", 0, 9223372036854775808]'],
    (m:any)=>m.fingerprints=['["public.recovery_gate", 0, 0]'],
  ]){const value=manifest();change(value);expect(()=>parseBackupCandidate(value)).toThrow('backup_candidate_manifest_invalid');}
});
const posix=it.skipIf(process.platform==='win32');
async function fixture(run:(dir:string)=>Promise<void>){
  const dir=await mkdtemp(join(tmpdir(),'ccpanel-inspect-'));
  try{await writeFile(join(dir,'capture.json'),JSON.stringify(manifest()),{mode:0o600});await writeFile(join(dir,'database.dump.enc'),cipher,{mode:0o600});await run(dir);}
  finally{await chmod(dir,0o700);for(const file of await readdir(dir))await unlink(join(dir,file));await rmdir(dir);}
}
const options=(directory:string)=>({directory,expectedSchema:22,maxBytes:1024,timeoutMs:2000});
posix('checks a bounded ciphertext without authenticating or editing its candidate flags',async()=>fixture(async dir=>{
  const before=await readFile(join(dir,'capture.json'));
  const result=await inspectBackupCandidate(options(dir));expect(result).toMatchObject({status:'captured_unverified',cipherIntegrityChecked:true,bytes:cipher.length,recoveryVerified:false,offhostVerified:false,latestStateVerified:false,sourceAuthenticated:false});
  expect(await readFile(join(dir,'capture.json'))).toEqual(before);expect((await readdir(dir)).sort()).toEqual(['capture.json','database.dump.enc']);
}));
posix('rejects corruption, mismatched schema and byte limits',async()=>fixture(async dir=>{
  await expect(inspectBackupCandidate({...options(dir),expectedSchema:21})).rejects.toThrow('backup_candidate_schema_mismatch');
  await expect(inspectBackupCandidate({...options(dir),maxBytes:10})).rejects.toThrow('backup_candidate_size_invalid');
  const bad=Buffer.from(cipher);bad[50]^=1;await writeFile(join(dir,'database.dump.enc'),bad);
  await expect(inspectBackupCandidate(options(dir))).rejects.toThrow('backup_candidate_cipher_mismatch');
}));
posix('rejects extra plaintext, missing data, unsafe permissions and symlinks',async()=>fixture(async dir=>{
  await writeFile(join(dir,'database.dump'),'secret');await expect(inspectBackupCandidate(options(dir))).rejects.toThrow('backup_candidate_files_invalid');await unlink(join(dir,'database.dump'));
  await chmod(join(dir,'capture.json'),0o644);await expect(inspectBackupCandidate(options(dir))).rejects.toThrow('backup_candidate_file_unsafe');await chmod(join(dir,'capture.json'),0o600);
  await unlink(join(dir,'database.dump.enc'));await expect(inspectBackupCandidate(options(dir))).rejects.toThrow('backup_candidate_files_invalid');
  await symlink('capture.json',join(dir,'database.dump.enc'));await expect(inspectBackupCandidate(options(dir))).rejects.toThrow('backup_candidate_inspection_failed');
}));
it('refuses a cancelled inspection before filesystem access',async()=>{
  const controller=new AbortController();controller.abort();await expect(inspectBackupCandidate({...options(tmpdir()),signal:controller.signal})).rejects.toThrow('backup_candidate_aborted');
});
it.skipIf(process.platform!=='win32')('does not infer Windows ACL safety from mode bits',async()=>{
  await expect(inspectBackupCandidate(options(tmpdir()))).rejects.toThrow('backup_candidate_permissions_unverified');
});
