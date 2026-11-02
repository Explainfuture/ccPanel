import {it,expect,vi,afterEach} from 'vitest';
import {mkdtemp,writeFile,unlink,rmdir,readdir,chmod} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join,resolve,posix} from 'node:path';
import example from '../infra/backup/offhost.example.json';
import {parseBackupConfig,type BackupConfig} from '../packages/server/src/backup-config';
import {transferBackupCandidateSftp,type SftpBackupOptions} from '../packages/server/src/backup-sftp';
import {main} from '../scripts/copy-backup-sftp';

afterEach(()=>vi.restoreAllMocks());
const posixOnly=it.skipIf(process.platform==='win32');
const options=():SftpBackupOptions=>({sourceDirectory:join(tmpdir(),'unused-sftp-source'),downloadRoot:join(tmpdir(),'unused-sftp-download'),id:'candidate-001',remoteRoot:'/private/remote',expectedSchema:22,maxBytes:1024,timeoutMs:5000,connection:{host:'127.0.0.1',port:2222,user:'backup',knownHostsFile:join(tmpdir(),'unused-known-hosts'),identityFile:join(tmpdir(),'unused-identity')}});
const logCapture=()=>({out:vi.spyOn(console,'log').mockImplementation(()=>{}),error:vi.spyOn(console,'error').mockImplementation(()=>{})});
function configured(directory:string):BackupConfig{
  const config=parseBackupConfig(example);config.enabled=true;
  config.worker={identity:'fixture-coordinator',independentFailureDomainConfirmed:true};
  Object.assign(config.source,{host:'source.example.invalid',port:22,user:'backup',knownHostsFile:join(directory,'missing-source-hosts'),identityFile:join(directory,'missing-source-key'),expectedRelease:'m4-sdk-013',expectedSchema:22});
  config.encryption.keyFile=join(directory,'missing-encryption-key');
  config.destination={kind:'sftp',root:null,prefix:'fixture-prefix',sftp:{host:'destination.example.invalid',port:2222,user:'backup',knownHostsFile:join(directory,'missing-destination-hosts'),identityFile:join(directory,'missing-destination-key'),remoteRoot:'/private/destination'}};
  config.limits.exportMaxBytes=1234;config.limits.runTimeoutSeconds=42;
  return config;
}
async function fixture(run:(directory:string,file:string)=>Promise<void>){
  const directory=await mkdtemp(join(tmpdir(),'ccpanel-sftp-cli-')),file=join(directory,'config.json');
  try{await run(directory,file);}finally{for(const name of await readdir(directory))await unlink(join(directory,name));await rmdir(directory);}
}
it('rejects malformed core options before touching paths or connecting',async()=>{
  for(const invalid of [
    {...options(),id:'../overwrite'}, {...options(),sourceDirectory:'relative'}, {...options(),maxBytes:0},
    {...options(),remoteRoot:'/private/../escape'}, {...options(),connection:{...options().connection,port:0}},
  ])await expect(transferBackupCandidateSftp(invalid)).rejects.toMatchObject({code:'sftp_options_invalid'});
});
it('rejects a pre-aborted core operation before filesystem or network work',async()=>{
  const controller=new AbortController();controller.abort();
  await expect(transferBackupCandidateSftp({...options(),signal:controller.signal})).rejects.toMatchObject({code:'sftp_aborted'});
});
it.skipIf(process.platform!=='win32')('refuses core execution until Windows ACL validation exists',async()=>{
  await expect(transferBackupCandidateSftp(options())).rejects.toMatchObject({code:'sftp_permissions_unverified'});
});
it('keeps the real disabled template inert with nonexistent source and download paths',async()=>{
  const capture=logCapture(),transfer=vi.fn(async()=>{throw Error('must not transfer');});
  expect(await main([resolve('infra/backup/offhost.example.json'),'nonexistent-source','nonexistent-download','candidate-001'],transfer)).toBe(0);
  expect(transfer).not.toHaveBeenCalled();expect(capture.error).not.toHaveBeenCalled();
  expect(JSON.parse(capture.out.mock.calls[0]![0])).toEqual({status:'disabled',transferEnabled:false,credentialsInspected:0});
});
it('rejects invalid JSON and unknown fields with redacted fixed errors',async()=>fixture(async(_directory,file)=>{
  const capture=logCapture(),transfer=vi.fn(async()=>{throw Error('must not transfer');}),secret='sensitive-fixture-value-never-log';
  for(const content of ['{"secret":"'+secret+'"',JSON.stringify({...example,password:secret})]){
    await writeFile(file,content,{mode:0o600});
    expect(await main([file,'nonexistent-source','nonexistent-download','candidate-001'],transfer)).toBe(1);
  }
  expect(transfer).not.toHaveBeenCalled();expect(capture.out).not.toHaveBeenCalled();
  expect(capture.error.mock.calls).toHaveLength(2);
  for(const [message]of capture.error.mock.calls){expect(Object.keys(JSON.parse(message))).toEqual(['code']);expect(message).not.toContain(secret);expect(message).not.toContain(file);}
}));
posixOnly('maps enabled private configuration to explicit transfer limits without reading referenced credentials',async()=>fixture(async(directory,file)=>{
  const config=configured(directory),capture=logCapture(),receipt={status:'transferred_unverified',hostKeyVerified:true,offhostVerified:false};
  await writeFile(file,JSON.stringify(config),{mode:0o600});
  const transfer=vi.fn(async(_options:SftpBackupOptions)=>receipt),source=join(directory,'missing-source'),download=join(directory,'missing-download');
  expect(await main([file,source,download,'candidate-001'],transfer)).toBe(2);
  expect(transfer).toHaveBeenCalledExactlyOnceWith({sourceDirectory:resolve(source),downloadRoot:resolve(download),id:'candidate-001',remoteRoot:posix.join(config.destination.sftp!.remoteRoot!,config.destination.prefix),expectedSchema:22,maxBytes:1234+36,timeoutMs:42000,connection:{host:'destination.example.invalid',port:2222,user:'backup',knownHostsFile:config.destination.sftp!.knownHostsFile,identityFile:config.destination.sftp!.identityFile}});
  expect(capture.error).not.toHaveBeenCalled();expect(JSON.parse(capture.out.mock.calls[0]![0])).toEqual(receipt);
  // Only the config exists: injection isolates CLI orchestration, not transport.
  expect(await readdir(directory)).toEqual(['config.json']);
}));
posixOnly('rejects an enabled config with group/other access before transfer',async()=>fixture(async(directory,file)=>{
  const capture=logCapture(),transfer=vi.fn(async()=>({status:'must-not-run'}));await writeFile(file,JSON.stringify(configured(directory)),{mode:0o600});await chmod(file,0o644);
  expect(await main([file,'missing-source','missing-download','candidate-001'],transfer)).toBe(1);
  expect(transfer).not.toHaveBeenCalled();expect(JSON.parse(capture.error.mock.calls[0]![0])).toEqual({code:'sftp_config_permissions_unverified'});
}));
posixOnly('refuses unbounded configured transfer arithmetic before invoking transport',async()=>fixture(async(directory,file)=>{
  const capture=logCapture(),transfer=vi.fn(async()=>({status:'must-not-run'}));
  for(const change of [(config:BackupConfig)=>{config.limits.exportMaxBytes=Number.MAX_SAFE_INTEGER;},(config:BackupConfig)=>{config.limits.runTimeoutSeconds=1801;}]){
    const config=configured(directory);change(config);await writeFile(file,JSON.stringify(config),{mode:0o600});
    expect(await main([file,'missing-source','missing-download','candidate-001'],transfer)).toBe(1);
  }
  expect(transfer).not.toHaveBeenCalled();for(const [message]of capture.error.mock.calls)expect(JSON.parse(message)).toEqual({code:'sftp_options_invalid'});
}));
it.skipIf(process.platform!=='win32')('does not pass enabled configuration to injected transport when Windows ACL safety is unverified',async()=>fixture(async(directory,file)=>{
  const capture=logCapture(),transfer=vi.fn(async()=>({status:'must-not-run'}));await writeFile(file,JSON.stringify(configured(directory)),{mode:0o600});
  expect(await main([file,'missing-source','missing-download','candidate-001'],transfer)).toBe(1);
  expect(transfer).not.toHaveBeenCalled();expect(capture.out).not.toHaveBeenCalled();
  expect(JSON.parse(capture.error.mock.calls[0]![0])).toEqual({code:'sftp_config_permissions_unverified'});
}));
