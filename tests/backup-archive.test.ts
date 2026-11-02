import {it,expect} from 'vitest';
import {mkdtemp,readFile,writeFile,readdir,unlink,rmdir,stat,chmod} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createHash} from 'node:crypto';
import {captureBackupArchive} from '../packages/server/src/backup-archive';
const posix=it.skipIf(process.platform==='win32');
async function fixture(run:(directory:string)=>Promise<void>){
  const directory=await mkdtemp(join(tmpdir(),'ccpanel-archive-'));
  try{await run(directory);}finally{for(const name of await readdir(directory))await unlink(join(directory,name));await rmdir(directory);}
}
const options=(directory:string,code:string)=>({output:join(directory,'database.dump'),command:process.execPath,args:['-e',code],maxBytes:1024*1024,timeoutMs:2000});
posix('streams exact bytes and hash with private mode and no temporary residue',async()=>fixture(async directory=>{
  const expected=Buffer.alloc(1024*1024,42);
  const result=await captureBackupArchive(options(directory,'process.stdout.write(Buffer.alloc(1024*1024,42))'));
  expect(result).toEqual({status:'captured_unverified',bytes:expected.length,sha256:createHash('sha256').update(expected).digest('hex')});
  expect(await readFile(join(directory,'database.dump'))).toEqual(expected);expect((await stat(join(directory,'database.dump'))).mode&0o777).toBe(0o600);
  expect(await readdir(directory)).toEqual(['database.dump']);
}));
posix.each([
  ['size limit','process.stdout.write(Buffer.alloc(1024*1024+1))','archive_size_limit'],
  ['failed child',"process.stdout.write('partial');process.stderr.write('secret-fixture');process.exitCode=9",'archive_child_failed'],
  ['empty output','process.exit(0)','archive_empty'],
  ['timeout','setInterval(()=>{},1000)','archive_timeout'],
])('rejects %s without publishing or retaining plaintext',(async(_label,code,error)=>fixture(async directory=>{
  await expect(captureBackupArchive(options(directory,code))).rejects.toThrow(error);expect(await readdir(directory)).toEqual([]);
})));
posix('refuses existing output before spawning and preserves its exact bytes',async()=>fixture(async directory=>{
  await writeFile(join(directory,'database.dump'),'existing');
  await expect(captureBackupArchive(options(directory,"require('fs').writeFileSync("+JSON.stringify(join(directory,'must-not-be-created'))+",'bad')"))).rejects.toThrow('archive_output_exists');
  expect((await readFile(join(directory,'database.dump'))).toString()).toBe('existing');expect(await readdir(directory)).toEqual(['database.dump']);
}));
posix('cancels a real writer, waits for close and removes temporary bytes',async()=>fixture(async directory=>{
  const controller=new AbortController();
  const operation=captureBackupArchive({...options(directory,"process.stdout.write('partial');setInterval(()=>{},1000)"),signal:controller.signal});
  // Observe actual temp-file bytes, rather than guessing when the child started.
  const end=Date.now()+1500;
  while(Date.now()<end){const files=await readdir(directory);if(files.length&&(await stat(join(directory,files[0]!))).size>0)break;await new Promise(r=>setTimeout(r,20));}
  const files=await readdir(directory);expect(files.length).toBe(1);expect((await stat(join(directory,files[0]!))).size).toBeGreaterThan(0);
  controller.abort();await expect(operation).rejects.toThrow('archive_aborted');expect(await readdir(directory)).toEqual([]);
}));
posix('does not overwrite a racing publisher',async()=>fixture(async directory=>{
  const winner=join(directory,'database.dump');
  const code="setTimeout(()=>{process.stdout.write('loser');},300)";
  const operation=captureBackupArchive(options(directory,code));
  const end=Date.now()+1000;while(!(await readdir(directory)).some(name=>name.endsWith('.tmp'))&&Date.now()<end)await new Promise(r=>setTimeout(r,10));
  await writeFile(winner,'winner',{flag:'wx'});await expect(operation).rejects.toThrow('archive_output_exists');expect((await readFile(winner)).toString()).toBe('winner');expect(await readdir(directory)).toEqual(['database.dump']);
}));
posix('rejects unsafe directory permissions and a failed spawn without payload errors',async()=>fixture(async directory=>{
  await chmod(directory,0o755);await expect(captureBackupArchive(options(directory,'process.exit(0)'))).rejects.toThrow('archive_directory_unsafe');await chmod(directory,0o700);
  await expect(captureBackupArchive({...options(directory,''),command:join(directory,'missing-secret-name')})).rejects.toThrow('archive_spawn_failed');expect(await readdir(directory)).toEqual([]);
}));
it('refuses an already cancelled operation without touching output paths',async()=>{
  const controller=new AbortController();controller.abort();await expect(captureBackupArchive({...options('unused',''),output:join(tmpdir(),'unused.dump'),signal:controller.signal})).rejects.toThrow('archive_aborted');
});
it.skipIf(process.platform!=='win32')('refuses Windows files until ACL validation exists',async()=>{
  await expect(captureBackupArchive(options(tmpdir(),'process.exit(0)'))).rejects.toThrow('archive_permissions_unverified');
});
