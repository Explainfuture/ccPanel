import {it,expect,vi} from 'vitest';
import {mkdtemp,writeFile,unlink,rmdir} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {resolve,join,sep} from 'node:path';
import {spawnSync} from 'node:child_process';
import v1Example from '../infra/backup/offhost.example.json';
import example from '../infra/backup/offhost-state.example.json';
import {parseBackupConfig,type BackupConfig} from '../packages/server/src/backup-config';
import {preflightBackup} from '../scripts/backup-config-check';

function configured():BackupConfig{
  const config=parseBackupConfig(example);
  config.enabled=true;config.worker={identity:'offhost-fixture',independentFailureDomainConfirmed:true};
  Object.assign(config.source,{host:'source.example.invalid',user:'backup',knownHostsFile:resolve('state-known-hosts'),identityFile:resolve('state-identity'),expectedRelease:'m4-sdk-013'});
  config.encryption.keyFile=resolve('state-encryption-key');config.destination.root=resolve('state-destination');
  config.state={enabled:true,sourceId:'source-state-fixture',signingKeyFile:resolve('state-signing-key'),publicKeyFile:resolve('state-public-key'),journalRoot:resolve('state-journal'),anchorRoot:resolve('state-anchors')};
  return config;
}
function sftpConfigured():BackupConfig{
  const config=configured();
  config.destination={kind:'sftp',root:null,prefix:'ccpanel-backups',sftp:{host:'destination.example.invalid',port:22,user:'backup',knownHostsFile:resolve('state-dest-known'),identityFile:resolve('state-dest-identity'),remoteRoot:'/private/backups'}};
  return config;
}
it('keeps strict v1 shape and requires an exact explicit state shape in v2',()=>{
  expect(parseBackupConfig(v1Example)).toEqual(v1Example);expect(parseBackupConfig(example)).toEqual(example);
  expect(Object.hasOwn(parseBackupConfig(v1Example),'state')).toBe(false);
  expect(()=>parseBackupConfig({...v1Example,state:example.state})).toThrow('invalid_backup_config:config');
  const missing:any=structuredClone(example);delete missing.state;
  expect(()=>parseBackupConfig(missing)).toThrow('invalid_backup_config:config');
  for(const field of Object.keys(example.state)){
    const value:any=structuredClone(example);delete value.state[field];expect(()=>parseBackupConfig(value)).toThrow('invalid_backup_config:state');
  }
  expect(()=>parseBackupConfig({...example,state:{...example.state,latestStateVerified:true}})).toThrow('invalid_backup_config:state');
  expect(()=>parseBackupConfig({...v1Example,version:3})).toThrow('invalid_backup_config:version');
});
it('requires state references only when both global and state switches are enabled',()=>{
  const globallyDisabled=structuredClone(example);globallyDisabled.state.enabled=true;
  expect(parseBackupConfig(globallyDisabled).state?.signingKeyFile).toBeNull();
  const stateDisabled=configured();stateDisabled.state={...example.state};
  expect(parseBackupConfig(stateDisabled).state?.enabled).toBe(false);
  for(const field of ['sourceId','signingKeyFile','publicKeyFile','journalRoot','anchorRoot']){
    const value:any=configured();value.state[field]=null;expect(()=>parseBackupConfig(value)).toThrow('invalid_backup_config:state.'+field);
  }
});
it.each([
  ['state object',(c:any)=>{c.state=null;}],
  ['nonboolean state switch',(c:any)=>{c.state.enabled='false';}],
  ['source name',(c:any)=>{c.state.sourceId='source/other';}],
  ['source length',(c:any)=>{c.state.sourceId='s'.repeat(129);}],
  ['relative signer',(c:any)=>{c.state.signingKeyFile='private.pem';}],
  ['public key traversal',(c:any)=>{c.state.publicKeyFile=resolve('safe')+sep+'..'+sep+'public.pem';}],
  ['journal control character',(c:any)=>{c.state.journalRoot=resolve('journal')+'\n';}],
  ['anchor traversal',(c:any)=>{c.state.anchorRoot=resolve('safe')+sep+'.'+sep+'anchors';}],
])('rejects malformed %s even with the entire configuration disabled',(_label,change)=>{
  const config:any=structuredClone(example);change(config);expect(()=>parseBackupConfig(config)).toThrow();
});
it.each(['journal-in-anchor','anchor-in-journal','same','journal-in-destination','destination-in-journal','anchor-in-destination','destination-in-anchor'])('rejects overlapping state storage roots: %s',kind=>{
  const config=configured(),state=config.state!;
  if(kind==='journal-in-anchor')state.journalRoot=join(state.anchorRoot!,'nested');
  if(kind==='anchor-in-journal')state.anchorRoot=join(state.journalRoot!,'nested');
  if(kind==='same')state.anchorRoot=state.journalRoot!+sep;
  if(kind==='journal-in-destination')state.journalRoot=join(config.destination.root!,'nested');
  if(kind==='destination-in-journal')config.destination.root=join(state.journalRoot!,'nested');
  if(kind==='anchor-in-destination')state.anchorRoot=join(config.destination.root!,'nested');
  if(kind==='destination-in-anchor')config.destination.root=join(state.anchorRoot!,'nested');
  expect(()=>parseBackupConfig(config)).toThrow('state_storage_roots_overlap');
});
it('uses path boundaries rather than string prefixes and respects host path case semantics',()=>{
  const config=configured(),state=config.state!;
  state.journalRoot=config.destination.root+'-journal';state.anchorRoot=state.journalRoot+'-anchors';
  state.signingKeyFile=state.journalRoot+'-signer';state.publicKeyFile=state.anchorRoot+'-public';
  expect(parseBackupConfig(config)).toEqual(config);
  state.anchorRoot=state.journalRoot!.toUpperCase();
  if(process.platform==='win32')expect(()=>parseBackupConfig(config)).toThrow('state_storage_roots_overlap');
  else expect(parseBackupConfig(config)).toEqual(config);
});
it.each(['signer-public','signer-aes','public-source-identity','signer-source-known','ssh-ssh','dest-signing','dest-source'])('separates signer, public pin, AES and SSH references: %s',kind=>{
  const config=sftpConfigured(),state=config.state!,sftp=config.destination.sftp!;
  if(kind==='signer-public')state.publicKeyFile=state.signingKeyFile!+sep;
  if(kind==='signer-aes')state.signingKeyFile=config.encryption.keyFile;
  if(kind==='public-source-identity')state.publicKeyFile=config.source.identityFile;
  if(kind==='signer-source-known')state.signingKeyFile=config.source.knownHostsFile;
  if(kind==='ssh-ssh')config.source.identityFile=config.source.knownHostsFile;
  if(kind==='dest-signing')sftp.identityFile=state.signingKeyFile;
  if(kind==='dest-source')sftp.knownHostsFile=config.source.knownHostsFile;
  expect(()=>parseBackupConfig(config)).toThrow('state_key_must_be_separate');
});
it('detects Windows case aliases between key roles without rejecting distinct POSIX paths',()=>{
  const config=configured();config.state!.publicKeyFile=config.state!.signingKeyFile!.toUpperCase();
  if(process.platform==='win32')expect(()=>parseBackupConfig(config)).toThrow('state_key_must_be_separate');
  else expect(parseBackupConfig(config)).toEqual(config);
});
const localFiles=['signer','public','aes','identity','known'] as const;
it.each(localFiles.flatMap(field=>['journal','anchor','destination'].map(root=>[field,root] as const)))('keeps %s metadata/key outside %s storage', (field,root)=>{
  const config=configured(),state=config.state!,storage=root==='journal'?state.journalRoot!:root==='anchor'?state.anchorRoot!:config.destination.root!;
  const path=join(storage,'credential');
  if(field==='signer')state.signingKeyFile=path;
  if(field==='public')state.publicKeyFile=path;
  if(field==='aes')config.encryption.keyFile=path;
  if(field==='identity')config.source.identityFile=path;
  if(field==='known')config.source.knownHostsFile=path;
  expect(()=>parseBackupConfig(config)).toThrow(field==='aes'&&root==='destination'?'encryption_key_inside_destination':'state_key_inside_storage');
});
it.each(['identityFile','knownHostsFile'] as const)('keeps SFTP %s outside state storage',field=>{
  const config=sftpConfigured();config.destination.sftp![field]=join(config.state!.journalRoot!,'credential');
  expect(()=>parseBackupConfig(config)).toThrow('state_key_inside_storage');
});
it('preserves existing v1 and state-disabled separation rules',()=>{
  const config=configured();config.state!.enabled=false;config.source.identityFile=config.source.knownHostsFile;
  config.state!.anchorRoot=config.state!.journalRoot;
  expect(parseBackupConfig(config)).toEqual(config);
  const v1:any=structuredClone(config);v1.version=1;delete v1.state;
  expect(parseBackupConfig(v1)).toEqual(v1);
});
it.each([false,true])('does no credential inspection or storage I/O when globally disabled (state %s)',async enabled=>{
  const config=configured();config.enabled=false;config.state!.enabled=enabled;
  const inspect=vi.fn(async()=>{throw Error('must not touch credentials');});
  expect(await preflightBackup(parseBackupConfig(config),inspect)).toEqual({status:'disabled',transferEnabled:false,credentialsInspected:0});expect(inspect).not.toHaveBeenCalled();
});
it('preflights state credential metadata without claiming an executor or authenticating key bytes',async()=>{
  const config=configured(),inspect=vi.fn(async()=>{});
  expect(await preflightBackup(parseBackupConfig(config),inspect)).toEqual({status:'configured_executor_unavailable',transferEnabled:false,credentialsInspected:5,hostKeyVerified:false,independentFailureDomainVerified:false});
  expect(inspect.mock.calls).toEqual([
    [config.source.knownHostsFile,'source.knownHostsFile',1048576,undefined],
    [config.source.identityFile,'source.identityFile',65536,undefined],
    [config.encryption.keyFile,'encryption.keyFile',32,true],
    [config.state!.signingKeyFile,'state.signingKeyFile',65536,undefined],
    [config.state!.publicKeyFile,'state.publicKeyFile',16384,undefined],
  ]);
  config.state!.enabled=false;inspect.mockClear();
  expect((await preflightBackup(parseBackupConfig(config),inspect)).credentialsInspected).toBe(3);
  expect(inspect.mock.calls).toHaveLength(3);
});
it('includes both destination and state metadata for SFTP without starting a transfer',async()=>{
  const inspect=vi.fn(async()=>{}),result=await preflightBackup(parseBackupConfig(sftpConfigured()),inspect);
  expect(result.credentialsInspected).toBe(7);expect(result.transferEnabled).toBe(false);expect(inspect).toHaveBeenCalledTimes(7);
});
it('the real v2 disabled CLI remains inert with null references and creates no resources',async()=>{
  const dir=await mkdtemp(join(tmpdir(),'ccpanel-state-config-')),file=join(dir,'config.json');
  try{
    await writeFile(file,JSON.stringify(example));
    const result=spawnSync(process.execPath,[resolve('node_modules/tsx/dist/cli.mjs'),'scripts/backup-config-check.ts',file],{encoding:'utf8',timeout:10000});
    expect(result.status).toBe(0);expect(result.stderr).toBe('');expect(JSON.parse(result.stdout)).toEqual({status:'disabled',transferEnabled:false,credentialsInspected:0});
  }finally{await unlink(file);await rmdir(dir);}
});
