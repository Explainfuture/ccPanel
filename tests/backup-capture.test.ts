import {it,expect} from 'vitest';
import {mkdtemp,writeFile,readdir,unlink,rmdir} from 'node:fs/promises';
import {join,resolve} from 'node:path';
import {tmpdir} from 'node:os';
import {spawnSync} from 'node:child_process';
import {captureSnapshotCandidate} from '../packages/server/src/backup-capture';
const opts=(root:string)=>({connectionString:'postgresql://invalid.invalid/unused',output:join(root,'candidate'),keyFile:join(root,'key'),cryptoScript:resolve('scripts/encrypt-backup.mjs'),expectedSchema:22,maxBytes:1024,timeoutMs:1000});
it('rejects unsafe options before filesystem or source access',async()=>{
  await expect(captureSnapshotCandidate({...opts(tmpdir()),maxBytes:Number.MAX_SAFE_INTEGER},()=>{throw Error('must not run');})).rejects.toThrow('backup_capture_options_invalid');
});
it('real CLI rejects malformed invocation without echoing input credentials',()=>{
  const result=spawnSync(process.execPath,[resolve('node_modules/tsx/dist/cli.mjs'),'scripts/capture-backup.ts'],{encoding:'utf8',env:{...process.env,CCPANEL_BACKUP_DATABASE_URL:'private-credential-must-not-appear'},timeout:10000});
  expect(result.status).toBe(1);expect(result.stdout).toBe('');expect(JSON.parse(result.stderr)).toEqual({code:'usage_backup_capture_output_key_schema'});expect(result.stderr).not.toContain('private-credential');
});
it.skipIf(process.platform!=='win32')('refuses unsupported Windows ACLs before touching paths or source',async()=>{
  await expect(captureSnapshotCandidate(opts(tmpdir()),()=>{throw Error('must not run');})).rejects.toThrow('backup_capture_permissions_unverified');
});
it.skipIf(process.platform==='win32')('preflights key and working-space budget before creating a set or connecting',async()=>{
  const directory=await mkdtemp(join(tmpdir(),'ccpanel-capture-preflight-'));
  try{
    await expect(captureSnapshotCandidate(opts(directory),()=>{throw Error('must not run');})).rejects.toThrow('backup_capture_failed');expect(await readdir(directory)).toEqual([]);
    await writeFile(join(directory,'key'),Buffer.alloc(32),{mode:0o600});
    await expect(captureSnapshotCandidate({...opts(directory),maxBytes:Number.MAX_SAFE_INTEGER-36},()=>{throw Error('must not run');})).rejects.toThrow('backup_capture_space_low');expect(await readdir(directory)).toEqual(['key']);
  }finally{for(const file of await readdir(directory))await unlink(join(directory,file));await rmdir(directory);}
});
