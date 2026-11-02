// This module validates configuration only. It never loads credentials or connects.
import {isAbsolute,posix,resolve,relative,sep} from 'node:path';
import {isIP} from 'node:net';

interface Ssh {host:string|null;port:number;user:string|null;knownHostsFile:string|null;identityFile:string|null}
export interface BackupConfig {
  version:1|2;enabled:boolean;
  worker:{identity:string|null;independentFailureDomainConfirmed:boolean};
  source:Ssh&{expectedRelease:string|null;expectedSchema:number};
  encryption:{format:'CCPBK001';keyFile:string|null};
  destination:{kind:'local'|'sftp';root:string|null;sftp:(Ssh&{remoteRoot:string|null})|null;prefix:string};
  schedule:{fullEveryMinutes:number;stateEveryMinutes:number};
  retention:{daily:number;weekly:number;monthly:number};
  limits:{runTimeoutSeconds:number;exportMaxBytes:number;stateMaxAgeSeconds:number;incompleteRetentionHours:number;maxAttempts:number};
  state?:{enabled:boolean;sourceId:string|null;signingKeyFile:string|null;publicKeyFile:string|null;journalRoot:string|null;anchorRoot:string|null};
}
export class BackupConfigError extends Error {
  constructor(public code:string,public field:string){super(code+':'+field);this.name='BackupConfigError';}
}
const fail=(field:string,code='invalid_backup_config'):never=>{throw new BackupConfigError(code,field);};
function object(value:unknown,keys:string[],field:string):asserts value is Record<string,unknown>{
  if(!value||typeof value!=='object'||Array.isArray(value)||![Object.prototype,null].includes(Object.getPrototypeOf(value))||Object.keys(value).length!==keys.length||keys.some(k=>!Object.hasOwn(value,k)))fail(field);
}
function positive(value:unknown,field:string,max=Number.MAX_SAFE_INTEGER){if(typeof value!=='number'||!Number.isSafeInteger(value)||value<1||value>max)fail(field);}
function text(value:unknown,field:string,required:boolean,check:(v:string)=>boolean){
  if(value===null&&!required)return;
  if(typeof value!=='string'||!value.length||value.length>4096||!check(value))fail(field);
}
const clean=(value:string)=>!/[\x00-\x1f\x7f]/.test(value)&&!value.split(/[\\/]/).some(s=>s==='.'||s==='..');
const localPath=(value:string)=>isAbsolute(value)&&clean(value);
const remotePath=(value:string)=>posix.isAbsolute(value)&&!value.includes('\\')&&clean(value);
const hostname=(value:string)=>value.length<=253&&(isIP(value)!==0||value.split('.').every(label=>/^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/i.test(label)));
const canonical=(value:string)=>process.platform==='win32'?resolve(value).toLowerCase():resolve(value);
// Lexical separation only; executors must still reject links and inspect opened files.
const inside=(path:string,root:string)=>{
  const from=relative(canonical(root),canonical(path));
  return !from||(!isAbsolute(from)&&from!=='..'&&!from.startsWith('..'+sep));
};
function ssh(value:Record<string,unknown>,field:string,required:boolean){
  text(value.host,field+'.host',required,hostname);positive(value.port,field+'.port',65535);
  text(value.user,field+'.user',required,v=>/^[a-z_][a-z0-9_-]{0,63}$/i.test(v));
  for(const k of ['knownHostsFile','identityFile'])text(value[k],field+'.'+k,required,localPath);
}
export function parseBackupConfig(input:unknown):BackupConfig{
  const keys=['version','enabled','worker','source','encryption','destination','schedule','retention','limits'];
  if(input&&typeof input==='object'&&!Array.isArray(input)&&(input as Record<string,unknown>).version===2)keys.push('state');
  object(input,keys,'config');
  if(input.version!==1&&input.version!==2)fail('version');if(typeof input.enabled!=='boolean')fail('enabled');const enabled=input.enabled===true;
  object(input.worker,['identity','independentFailureDomainConfirmed'],'worker');
  text(input.worker.identity,'worker.identity',enabled,v=>/^[A-Za-z0-9_-]{1,128}$/.test(v));
  if(typeof input.worker.independentFailureDomainConfirmed!=='boolean'||(enabled&&!input.worker.independentFailureDomainConfirmed))fail('worker.independentFailureDomainConfirmed');
  object(input.source,['host','port','user','knownHostsFile','identityFile','expectedRelease','expectedSchema'],'source');ssh(input.source,'source',enabled);
  text(input.source.expectedRelease,'source.expectedRelease',enabled,v=>/^[A-Za-z0-9_-]{1,128}$/.test(v));positive(input.source.expectedSchema,'source.expectedSchema');
  object(input.encryption,['format','keyFile'],'encryption');if(input.encryption.format!=='CCPBK001')fail('encryption.format');text(input.encryption.keyFile,'encryption.keyFile',enabled,localPath);
  object(input.destination,['kind','root','sftp','prefix'],'destination');
  const dest=input.destination;
  text(dest.prefix,'destination.prefix',true,v=>/^[A-Za-z0-9_-]{1,128}$/.test(v));
  if(dest.kind==='local'){
    text(dest.root,'destination.root',enabled,localPath);if(dest.sftp!==null)fail('destination.sftp');
  }else if(dest.kind==='sftp'){
    if(dest.root!==null)fail('destination.root');
    if(dest.sftp!==null){
      object(dest.sftp,['host','port','user','knownHostsFile','identityFile','remoteRoot'],'destination.sftp');ssh(dest.sftp,'destination.sftp',enabled);
      text(dest.sftp.remoteRoot,'destination.sftp.remoteRoot',enabled,remotePath);
      if(enabled&&String(dest.sftp.host).toLowerCase()===String(input.source.host).toLowerCase())fail('destination.sftp.host','source_destination_must_differ');
    }else if(enabled)fail('destination.sftp');
  }else fail('destination.kind');
  if(enabled){
    const key=canonical(input.encryption.keyFile as string);
    const sshFiles=[input.source.identityFile,input.source.knownHostsFile];
    if(dest.sftp)sshFiles.push((dest.sftp as Record<string,unknown>).identityFile,(dest.sftp as Record<string,unknown>).knownHostsFile);
    if(sshFiles.some(file=>canonical(file as string)===key))fail('encryption.keyFile','encryption_key_must_be_separate');
    if(dest.kind==='local'){
      if(inside(key,dest.root as string))fail('encryption.keyFile','encryption_key_inside_destination');
    }
  }
  if(input.version===2){
    object(input.state,['enabled','sourceId','signingKeyFile','publicKeyFile','journalRoot','anchorRoot'],'state');
    const state=input.state;
    if(typeof state.enabled!=='boolean')fail('state.enabled');
    const active=enabled&&state.enabled===true;
    text(state.sourceId,'state.sourceId',active,v=>/^[A-Za-z0-9_-]{1,128}$/.test(v));
    for(const k of ['signingKeyFile','publicKeyFile','journalRoot','anchorRoot'])text(state[k],'state.'+k,active,localPath);
    if(active){
      const roots:[string,string][]=[['state.journalRoot',state.journalRoot as string],['state.anchorRoot',state.anchorRoot as string]];
      if(dest.kind==='local')roots.push(['destination.root',dest.root as string]);
      for(let i=0;i<roots.length;i++)for(let j=0;j<i;j++){
        if(inside(roots[i][1],roots[j][1])||inside(roots[j][1],roots[i][1]))fail(roots[i][0],'state_storage_roots_overlap');
      }
      const files:[string,string][]=[
        ['encryption.keyFile',input.encryption.keyFile as string],
        ['source.knownHostsFile',input.source.knownHostsFile as string],
        ['source.identityFile',input.source.identityFile as string],
        ['state.signingKeyFile',state.signingKeyFile as string],
        ['state.publicKeyFile',state.publicKeyFile as string],
      ];
      if(dest.sftp){
        const sftp=dest.sftp as Record<string,unknown>;
        files.push(['destination.sftp.knownHostsFile',sftp.knownHostsFile as string],['destination.sftp.identityFile',sftp.identityFile as string]);
      }
      const seen=new Set<string>();
      for(const [field,path]of files){
        const normalized=canonical(path);
        if(seen.has(normalized))fail(field,'state_key_must_be_separate');seen.add(normalized);
        if(roots.some(([,root])=>inside(path,root)))fail(field,'state_key_inside_storage');
      }
    }
  }
  object(input.schedule,['fullEveryMinutes','stateEveryMinutes'],'schedule');
  for(const k of ['fullEveryMinutes','stateEveryMinutes'])positive(input.schedule[k],'schedule.'+k);
  object(input.retention,['daily','weekly','monthly'],'retention');for(const k of ['daily','weekly','monthly'])positive(input.retention[k],'retention.'+k);
  object(input.limits,['runTimeoutSeconds','exportMaxBytes','stateMaxAgeSeconds','incompleteRetentionHours','maxAttempts'],'limits');
  for(const k of ['runTimeoutSeconds','exportMaxBytes','stateMaxAgeSeconds','incompleteRetentionHours','maxAttempts'])positive(input.limits[k],'limits.'+k,k==='maxAttempts'?10:Number.MAX_SAFE_INTEGER);
  positive(input.limits.stateMaxAgeSeconds,'limits.stateMaxAgeSeconds',3599);
  if((input.schedule.stateEveryMinutes as number)>=(input.limits.stateMaxAgeSeconds as number)/60)fail('schedule.stateEveryMinutes','state_schedule_not_fresh');
  return structuredClone(input) as unknown as BackupConfig;
}
