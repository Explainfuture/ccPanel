import {it,expect,vi} from 'vitest';
import {mkdtemp,writeFile,chmod,unlink,rmdir,symlink} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {resolve,join} from 'node:path';
import {spawnSync} from 'node:child_process';
import example from '../infra/backup/offhost.example.json';
import {parseBackupConfig} from '../packages/server/src/backup-config';
import {inspectFile,preflightBackup} from '../scripts/backup-config-check';
function configured(){
  const config=parseBackupConfig(example);
  config.enabled=true;config.worker={identity:'offhost-fixture',independentFailureDomainConfirmed:true};
  Object.assign(config.source,{host:'source.example.invalid',user:'backup',knownHostsFile:resolve('test-known-hosts'),identityFile:resolve('test-identity'),expectedRelease:'m4-sdk-013'});
  Object.assign(config.encryption,{keyFile:resolve('test-encryption-key')});Object.assign(config.destination,{root:resolve('test-destination')});
  return config;
}
it('keeps disabled template inert even when referenced credential paths do not exist',async()=>{
  const config=configured();config.enabled=false;
  const inspect=vi.fn(async()=>{throw Error('must not touch credentials');});
  expect(await preflightBackup(parseBackupConfig(config),inspect)).toEqual({status:'disabled',transferEnabled:false,credentialsInspected:0});expect(inspect).not.toHaveBeenCalled();
});
it.each([
  ['nonboolean enable',(c:any)=>{c.enabled='false';}],
  ['unknown root field',(c:any)=>{c.password='should-not-be-printed';}],
  ['unknown nested field',(c:any)=>{c.source.token='should-not-be-printed';}],
  ['wrong version',(c:any)=>{c.version=2;}],
  ['missing source key',(c:any)=>{delete c.source.host;}],
  ['missing encryption key',(c:any)=>{c.encryption.keyFile=null;}],
  ['relative file',(c:any)=>{c.source.identityFile='relative.key';}],
  ['parent traversal',(c:any)=>{c.source.identityFile=resolve('safe')+'/../key';}],
  ['path control character',(c:any)=>{c.encryption.keyFile=resolve('key')+'\n';}],
  ['invalid host',(c:any)=>{c.source.host='-oProxyCommand=secret';}],
  ['host credentials',(c:any)=>{c.source.host='user:password@example.invalid';}],
  ['invalid user',(c:any)=>{c.source.user='backup;command';}],
  ['port overflow',(c:any)=>{c.source.port=65536;}],
  ['unconfirmed independence',(c:any)=>{c.worker.independentFailureDomainConfirmed=false;}],
  ['null local destination',(c:any)=>{c.destination.root=null;}],
  ['wrong adapter',(c:any)=>{c.destination.kind='s3';}],
  ['mixed adapter fields',(c:any)=>{c.destination.sftp={};}],
  ['unsafe prefix',(c:any)=>{c.destination.prefix='../backup';}],
  ['unsupported encryption',(c:any)=>{c.encryption.format='plaintext';}],
  ['encryption reuses SSH file',(c:any)=>{c.encryption.keyFile=c.source.identityFile;}],
  ['key inside destination',(c:any)=>{c.encryption.keyFile=join(c.destination.root,'backup.key');}],
  ['noninteger retention',(c:any)=>{c.retention.daily=1.5;}],
  ['unbounded bytes',(c:any)=>{c.limits.exportMaxBytes=Infinity;}],
  ['unsafe integer',(c:any)=>{c.limits.exportMaxBytes=Number.MAX_SAFE_INTEGER+1;}],
  ['zero timeout',(c:any)=>{c.limits.runTimeoutSeconds=0;}],
  ['too many attempts',(c:any)=>{c.limits.maxAttempts=11;}],
  ['state too old',(c:any)=>{c.limits.stateMaxAgeSeconds=3600;}],
  ['state schedule too slow',(c:any)=>{c.schedule.stateEveryMinutes=10;}],
])('rejects %s without reflecting supplied values',(_label,change)=>{
  const config=configured();change(config);
  expect(()=>parseBackupConfig(config)).toThrow();
  try{parseBackupConfig(config);}catch(error){expect(String(error)).not.toMatch(/should-not-be-printed|ProxyCommand|user:password/);}
});
it('rejects malformed configuration even when disabled',()=>{
  const config=structuredClone(example);(config as any).enabled='false';expect(()=>parseBackupConfig(config)).toThrow();
});
it('validates SFTP separation and rejects source host reuse',()=>{
  const config:any=configured();config.destination={kind:'sftp',root:null,prefix:'ccpanel-backups',sftp:{host:'other.example.invalid',port:22,user:'backup',knownHostsFile:resolve('dest-known'),identityFile:resolve('dest-id'),remoteRoot:'/private/backups'}};
  expect(parseBackupConfig(config).destination.kind).toBe('sftp');
  config.destination.sftp.host='SOURCE.EXAMPLE.INVALID';expect(()=>parseBackupConfig(config)).toThrow('source_destination_must_differ');
  config.destination.sftp.host='other.example.invalid';config.destination.sftp.remoteRoot='/private/../backups';expect(()=>parseBackupConfig(config)).toThrow();
});
it('returns only non-executable metadata after credential inspection',async()=>{
  const inspect=vi.fn(async()=>{}),config=parseBackupConfig(configured());
  expect(await preflightBackup(config,inspect)).toEqual({status:'configured_executor_unavailable',transferEnabled:false,credentialsInspected:3,hostKeyVerified:false,independentFailureDomainVerified:false});
  expect(inspect).toHaveBeenCalledTimes(3);
});
it('checks actual regular credential files, size, POSIX mode and symlinks',async()=>{
  const dir=await mkdtemp(join(tmpdir(),'ccpanel-backup-preflight-')),file=join(dir,'key'),link=join(dir,'link');
  try{
    await writeFile(file,Buffer.alloc(32,1),{mode:0o600});
    if(process.platform==='win32')await expect(inspectFile(file,'encryption.keyFile',32,true)).rejects.toThrow('credential_permissions_unverified');
    else{
      await expect(inspectFile(file,'encryption.keyFile',32,true)).resolves.toBeUndefined();
      await chmod(file,0o644);await expect(inspectFile(file,'encryption.keyFile',32,true)).rejects.toThrow('credential_permissions_too_open');
      await chmod(file,0o600);await symlink(file,link);await expect(inspectFile(link,'encryption.keyFile',32,true)).rejects.toThrow('credential_not_regular_file');await unlink(link);
    }
    await writeFile(file,Buffer.alloc(31));await expect(inspectFile(file,'encryption.keyFile',32,true)).rejects.toThrow('credential_size_invalid');
    await expect(inspectFile(join(dir,'missing-sensitive-name'),'source.identityFile',65536)).rejects.toThrow('credential_unavailable:source.identityFile');
  }finally{await unlink(file);await rmdir(dir);}
});
it('real CLI reports disabled or redacted failure without creating files',async()=>{
  const dir=await mkdtemp(join(tmpdir(),'ccpanel-backup-cli-')),file=join(dir,'config.json');
  const run=()=>spawnSync(process.execPath,[resolve('node_modules/tsx/dist/cli.mjs'),'scripts/backup-config-check.ts',file],{encoding:'utf8',timeout:10000});
  try{
    await writeFile(file,JSON.stringify(example));const disabled=run();expect(disabled.status).toBe(0);expect(JSON.parse(disabled.stdout).status).toBe('disabled');expect(disabled.stderr).toBe('');
    await writeFile(file,'{"password":"private-fixture-never-output"}');const invalid=run();expect(invalid.status).toBe(1);expect(invalid.stdout).toBe('');expect(JSON.parse(invalid.stderr)).toEqual({code:'invalid_backup_config',field:'config'});expect(invalid.stderr).not.toContain('private-fixture-never-output');
    // rmdir below also proves the CLI did not create keys, logs or directories here.
  }finally{await unlink(file);await rmdir(dir);}
});
it('real enabled CLI never reports executable even with all POSIX metadata checks passed',async()=>{
  const dir=await mkdtemp(join(tmpdir(),'ccpanel-backup-ready-')),configFile=join(dir,'config.json');
  const known=join(dir,'known'),identity=join(dir,'identity'),key=join(dir,'key');
  try{
    await writeFile(known,'synthetic-known-hosts',{mode:0o600});await writeFile(identity,'synthetic-identity',{mode:0o600});await writeFile(key,Buffer.alloc(32,1),{mode:0o600});
    const config=configured();config.source.knownHostsFile=known;config.source.identityFile=identity;config.encryption.keyFile=key;config.destination.root=join(dir,'must-not-be-created');
    await writeFile(configFile,JSON.stringify(config),{mode:0o600});
    const result=spawnSync(process.execPath,[resolve('node_modules/tsx/dist/cli.mjs'),'scripts/backup-config-check.ts',configFile],{encoding:'utf8',timeout:10000});
    if(process.platform==='win32'){
      expect(result.status).toBe(1);expect(JSON.parse(result.stderr)).toEqual({code:'credential_permissions_unverified',field:'config'});
    }else{
      expect(result.status).toBe(2);expect(result.stderr).toBe('');expect(JSON.parse(result.stdout)).toEqual({status:'configured_executor_unavailable',transferEnabled:false,credentialsInspected:3,hostKeyVerified:false,independentFailureDomainVerified:false});
    }
    expect(result.stdout+result.stderr).not.toContain('synthetic-');expect(result.stdout+result.stderr).not.toContain(dir);
  }finally{for(const file of [configFile,known,identity,key])await unlink(file);await rmdir(dir);}
});
