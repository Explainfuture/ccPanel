import {it,expect} from 'vitest';
import {mkdtemp,writeFile,readFile,readdir,unlink,rmdir,chmod,symlink} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {spawnSync} from 'node:child_process';
import {randomBytes} from 'node:crypto';
const run=(...args:string[])=>spawnSync(process.execPath,['scripts/encrypt-backup.mjs',...args],{timeout:10000,maxBuffer:4*1024*1024});
async function fixture(fn:(dir:string)=>Promise<void>){const dir=await mkdtemp(join(tmpdir(),'ccpanel-stream-crypto-'));try{await fn(dir);}finally{for(const name of await readdir(dir))await unlink(join(dir,name));await rmdir(dir);}}
it.skipIf(process.platform==='win32')('stream cipher is compatible with authenticated file decryption and preserves the existing key',async()=>fixture(async dir=>{
  const source=join(dir,'source'),key=join(dir,'key'),cipher=join(dir,'cipher'),restored=join(dir,'restored');
  const original=Buffer.alloc(512*1024,42),originalKey=randomBytes(32);
  await writeFile(source,original,{mode:0o600});await writeFile(key,originalKey,{mode:0o600});
  const encrypted=run('encrypt-stream',source,key);expect(encrypted.status).toBe(0);expect(encrypted.stderr.length).toBe(0);expect(encrypted.stdout.length).toBe(original.length+36);expect(encrypted.stdout.subarray(0,8).toString()).toBe('CCPBK001');
  await writeFile(cipher,encrypted.stdout,{mode:0o600});expect(run('decrypt',cipher,restored,key).status).toBe(0);expect(await readFile(restored)).toEqual(original);expect(await readFile(key)).toEqual(originalKey);
  const again=run('encrypt-stream',source,key);expect(again.status).toBe(0);expect(again.stdout).not.toEqual(encrypted.stdout);
  const damaged=Buffer.from(encrypted.stdout);damaged[100]!^=1;await writeFile(cipher,damaged);
  expect(run('decrypt',cipher,join(dir,'invalid'),key).status).not.toBe(0);expect(await readdir(dir)).not.toContain('invalid');expect((await readdir(dir)).some(name=>name.endsWith('.tmp'))).toBe(false);
}));
it.skipIf(process.platform==='win32')('automation refuses missing, oversized, public or symbolic-link keys without creating one',async()=>fixture(async dir=>{
  const source=join(dir,'source'),key=join(dir,'private-name-key'),alias=join(dir,'alias');await writeFile(source,'fixture',{mode:0o600});
  const rejects=()=>{const result=run('encrypt-stream',source,key);expect(result.status).toBe(1);expect(result.stdout.length).toBe(0);expect(JSON.parse(result.stderr.toString())).toEqual({code:'backup_stream_encryption_failed'});expect(result.stderr.toString()).not.toContain(dir);};
  rejects();expect(await readdir(dir)).toEqual(['source']);
  await writeFile(key,Buffer.alloc(33),{mode:0o600});rejects();await writeFile(key,Buffer.alloc(32));await chmod(key,0o644);rejects();await chmod(key,0o600);
  await symlink(key,alias);const linked=run('encrypt-stream',source,alias);expect(linked.status).toBe(1);expect(linked.stdout.length).toBe(0);
}));
it.skipIf(process.platform==='win32')('refuses encrypting the key itself and accepts an empty private source',async()=>fixture(async dir=>{
  const source=join(dir,'empty'),key=join(dir,'key');await writeFile(source,'',{mode:0o600});await writeFile(key,randomBytes(32),{mode:0o600});
  const reused=run('encrypt-stream',key,key);expect(reused.status).toBe(1);expect(reused.stdout.length).toBe(0);
  const encrypted=run('encrypt-stream',source,key);expect(encrypted.status).toBe(0);expect(encrypted.stdout.length).toBe(36);
}));
it.skipIf(process.platform!=='win32')('refuses Windows automation until ACL validation is available',()=>{
  const result=run('encrypt-stream','missing-source','missing-key');expect(result.status).toBe(1);expect(result.stdout.length).toBe(0);expect(JSON.parse(result.stderr.toString())).toEqual({code:'backup_stream_encryption_failed'});
});
