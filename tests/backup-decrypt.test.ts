import {it,expect} from 'vitest';
import {mkdtemp,mkdir,writeFile,readFile,readdir,unlink,rmdir,chmod,stat,symlink} from 'node:fs/promises';
import {join,sep} from 'node:path';
import {tmpdir} from 'node:os';
import {createCipheriv,createHash,randomBytes} from 'node:crypto';
import {decryptBackupCandidate} from '../packages/server/src/backup-decrypt';

const hash=(bytes:Buffer)=>createHash('sha256').update(bytes).digest('hex');
const posix=it.skipIf(process.platform==='win32');
type Fixture={parent:string;directory:string;keyFile:string;output:string;work:string;plain:Buffer;cipher:Buffer;manifest:ReturnType<typeof metadata>};
const metadata=(plain:Buffer,cipher:Buffer)=>({version:1,status:'captured_unverified',startedAt:'2026-11-02T00:00:00.000Z',capturedAt:'2026-11-02T00:00:01.000Z',recoveryVerified:false,offhostVerified:false,latestStateVerified:false,source:{database:'fixture',schemaVersion:22,releaseVerified:false},fingerprints:['["public.tenants", 1, -9223372036854775808]'],archive:{file:'database.dump.enc',format:'CCPBK001',bytes:cipher.length,sha256:hash(cipher),plaintextBytes:plain.length,plaintextSha256:hash(plain)}});
async function fixture(run:(value:Fixture)=>Promise<void>){
  const parent=await mkdtemp(join(tmpdir(),'ccpanel-decrypt-')),directory=join(parent,'candidate'),work=join(parent,'work'),keyFile=join(parent,'key'),output=join(work,'authenticated.dump');
  await mkdir(directory,{mode:0o700});await mkdir(work,{mode:0o700});
  const plain=Buffer.concat([Buffer.from('synthetic dump fixture\0'),Buffer.alloc(512*1024,42)]),key=randomBytes(32),iv=randomBytes(12),magic=Buffer.from('CCPBK001');
  const encryption=createCipheriv('aes-256-gcm',key,iv);encryption.setAAD(magic);
  const cipher=Buffer.concat([magic,iv,encryption.update(plain),encryption.final(),encryption.getAuthTag()]),manifest=metadata(plain,cipher);
  await writeFile(keyFile,key,{mode:0o600});key.fill(0);
  await writeFile(join(directory,'capture.json'),JSON.stringify(manifest),{mode:0o600});await writeFile(join(directory,'database.dump.enc'),cipher,{mode:0o600});
  const cleanup=async(path:string):Promise<void>=>{
    if(path!==parent&&!path.startsWith(parent+sep))throw Error('fixture_cleanup_outside_owned_parent');
    await chmod(path,0o700);
    for(const entry of await readdir(path,{withFileTypes:true})){
      const child=join(path,entry.name);
      if(entry.isDirectory()&&!entry.isSymbolicLink())await cleanup(child);else await unlink(child);
    }
    await rmdir(path);
  };
  try{await run({parent,directory,keyFile,output,work,plain,cipher,manifest});}finally{await cleanup(parent);}
}
const options=(value:Fixture)=>({directory:value.directory,keyFile:value.keyFile,output:value.output,expectedSchema:22,maxBytes:value.cipher.length,timeoutMs:5000});

posix('authenticates actual GCM bytes, checks plaintext hash and keeps source trust unverified',async()=>fixture(async value=>{
  const manifestBefore=await readFile(join(value.directory,'capture.json')),keyBefore=await readFile(value.keyFile);
  const result=await decryptBackupCandidate(options(value));
  expect(result).toMatchObject({status:'decrypted_unverified',authenticatedDecryption:true,plaintextHashChecked:true,bytes:value.plain.length,sha256:hash(value.plain),manifestSha256:hash(manifestBefore),recoveryVerified:false,offhostVerified:false,latestStateVerified:false,sourceAuthenticated:false});
  expect(await readFile(value.output)).toEqual(value.plain);expect((await stat(value.output)).mode&0o777).toBe(0o600);
  expect(await readdir(value.work)).toEqual(['authenticated.dump']);
  expect(await readFile(join(value.directory,'capture.json'))).toEqual(manifestBefore);expect(await readFile(join(value.directory,'database.dump.enc'))).toEqual(value.cipher);expect(await readFile(value.keyFile)).toEqual(keyBefore);
}));

posix('refuses a wrong key and removes unpublished plaintext bytes',async()=>fixture(async value=>{
  const replacement=randomBytes(32);await writeFile(value.keyFile,replacement);
  await expect(decryptBackupCandidate(options(value))).rejects.toMatchObject({code:'decrypt_authentication_failed'});
  expect(await readdir(value.work)).toEqual([]);expect(await readFile(value.keyFile)).toEqual(replacement);
}));

posix.each(['body','tag'])('rejects authenticated %s tampering even with a matching rewritten outer hash',async location=>fixture(async value=>{
  const changed=Buffer.from(value.cipher);changed[location==='body'?25:changed.length-1]^=1;
  await writeFile(join(value.directory,'database.dump.enc'),changed);
  const rewritten={...value.manifest,archive:{...value.manifest.archive,sha256:hash(changed)}};
  await writeFile(join(value.directory,'capture.json'),JSON.stringify(rewritten));
  await expect(decryptBackupCandidate(options(value))).rejects.toMatchObject({code:'decrypt_authentication_failed'});
  expect(await readdir(value.work)).toEqual([]);
}));

posix('refuses a forged plaintext hash after valid GCM authentication without publishing output',async()=>fixture(async value=>{
  const rewritten={...value.manifest,archive:{...value.manifest.archive,plaintextSha256:'0'.repeat(64)}};
  await writeFile(join(value.directory,'capture.json'),JSON.stringify(rewritten));
  await expect(decryptBackupCandidate(options(value))).rejects.toMatchObject({code:'decrypt_plaintext_mismatch'});
  expect(await readdir(value.work)).toEqual([]);
}));

posix('refuses over-budget candidates and wrong schema before producing plaintext',async()=>fixture(async value=>{
  await expect(decryptBackupCandidate({...options(value),maxBytes:value.cipher.length-1})).rejects.toMatchObject({code:'backup_candidate_size_invalid'});
  await expect(decryptBackupCandidate({...options(value),expectedSchema:21})).rejects.toMatchObject({code:'backup_candidate_schema_mismatch'});
  expect(await readdir(value.work)).toEqual([]);
}));

posix('preserves an existing output exactly instead of replacing or cleaning it',async()=>fixture(async value=>{
  const existing=Buffer.from('existing operator output');await writeFile(value.output,existing,{mode:0o600});
  await expect(decryptBackupCandidate(options(value))).rejects.toMatchObject({code:'decrypt_output_exists'});
  expect(await readFile(value.output)).toEqual(existing);expect(await readdir(value.work)).toEqual(['authenticated.dump']);
}));

posix('refuses an unsafe output parent, unsafe key mode and a key symlink',async()=>fixture(async value=>{
  await chmod(value.work,0o755);await expect(decryptBackupCandidate(options(value))).rejects.toMatchObject({code:'decrypt_parent_unsafe'});await chmod(value.work,0o700);
  await chmod(value.keyFile,0o644);await expect(decryptBackupCandidate(options(value))).rejects.toMatchObject({code:'decrypt_key_unsafe'});await chmod(value.keyFile,0o600);
  const alias=join(value.parent,'key-alias');await symlink(value.keyFile,alias);
  await expect(decryptBackupCandidate({...options(value),keyFile:alias})).rejects.toMatchObject({code:'decrypt_key_unsafe'});
  expect(await readdir(value.work)).toEqual([]);
}));

posix('never creates a missing key or accepts a wrong-sized one',async()=>fixture(async value=>{
  await unlink(value.keyFile);await expect(decryptBackupCandidate(options(value))).rejects.toMatchObject({code:'decrypt_key_unsafe'});
  expect(await readdir(value.parent)).not.toContain('key');expect(await readdir(value.work)).toEqual([]);
  await writeFile(value.keyFile,Buffer.alloc(31,3),{mode:0o600});await expect(decryptBackupCandidate(options(value))).rejects.toMatchObject({code:'decrypt_key_unsafe'});
  expect((await stat(value.keyFile)).size).toBe(31);expect(await readdir(value.work)).toEqual([]);
}));

it('refuses a pre-aborted operation before touching filesystem paths',async()=>{
  const controller=new AbortController();controller.abort();
  await expect(decryptBackupCandidate({directory:join(tmpdir(),'unused-candidate'),keyFile:join(tmpdir(),'unused-key'),output:join(tmpdir(),'unused.dump'),expectedSchema:22,maxBytes:1024,timeoutMs:5000,signal:controller.signal})).rejects.toMatchObject({code:'decrypt_aborted'});
});
it('rejects invalid bounds before filesystem access',async()=>{
  await expect(decryptBackupCandidate({directory:join(tmpdir(),'unused-candidate'),keyFile:join(tmpdir(),'unused-key'),output:join(tmpdir(),'unused.dump'),expectedSchema:22,maxBytes:0,timeoutMs:5000})).rejects.toMatchObject({code:'decrypt_options_invalid'});
});
it.skipIf(process.platform!=='win32')('does not assume POSIX mode checks prove Windows ACL privacy',async()=>{
  await expect(decryptBackupCandidate({directory:join(tmpdir(),'unused-candidate'),keyFile:join(tmpdir(),'unused-key'),output:join(tmpdir(),'unused.dump'),expectedSchema:22,maxBytes:1024,timeoutMs:5000})).rejects.toMatchObject({code:'decrypt_permissions_unverified'});
});
