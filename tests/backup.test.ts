import { expect,it } from 'vitest';
import { mkdtempSync,writeFileSync,readFileSync,readdirSync,rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join,resolve,basename } from 'node:path';
import { spawnSync } from 'node:child_process';
it('backup envelope round-trips, rejects tampering, and preserves existing output',()=>{
  const folder=mkdtempSync(join(tmpdir(),'ccpanel-backup-test-'));
  const file=(name:string)=>join(folder,name);
  const run=(...args:string[])=>spawnSync(process.execPath,['scripts/encrypt-backup.mjs',...args],{encoding:'utf8'});
  try{
    const data=Buffer.alloc(2*1024*1024,42);writeFileSync(file('source'),data);
    expect(run('encrypt',file('source'),file('cipher'),file('key')).status).toBe(0);
    expect(run('decrypt',file('cipher'),file('restored'),file('key')).status).toBe(0);
    expect(readFileSync(file('restored')).equals(data)).toBe(true);
    expect(run('decrypt',file('cipher'),file('restored'),file('key')).status).not.toBe(0);
    const damaged=readFileSync(file('cipher'));damaged[100]!^=1;writeFileSync(file('damaged'),damaged);
    expect(run('decrypt',file('damaged'),file('invalid'),file('key')).status).not.toBe(0);
    expect(readdirSync(folder)).not.toContain('invalid');
    expect(readdirSync(folder).some(name=>name.endsWith('.tmp'))).toBe(false);
    writeFileSync(file('empty'),Buffer.alloc(0));
    expect(run('encrypt',file('empty'),file('empty.enc'),file('key')).status).toBe(0);
    expect(run('decrypt',file('empty.enc'),file('empty.out'),file('key')).status).toBe(0);
    expect(readFileSync(file('empty.out')).length).toBe(0);
  }finally{
    if(resolve(folder).startsWith(resolve(tmpdir())+requireSeparator())&&basename(folder).startsWith('ccpanel-backup-test-'))rmSync(folder,{recursive:true,force:true});
  }
});
function requireSeparator(){return process.platform==='win32'?'\\':'/';}
