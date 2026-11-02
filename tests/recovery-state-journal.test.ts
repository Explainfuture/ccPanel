import {it,expect} from 'vitest';
import {generateKeyPairSync,randomBytes,createDecipheriv} from 'node:crypto';
import {mkdtemp,mkdir,writeFile,readFile,readdir,unlink,rmdir,chmod,stat,symlink} from 'node:fs/promises';
import {join,resolve,sep} from 'node:path';
import {tmpdir} from 'node:os';
import {signRecoveryStateRecord} from '../packages/server/src/recovery-state-record';
import type {RecoverySourceState} from '../packages/server/src/recovery-state-format';
import {appendRecoveryStateJournal,verifyRecoveryStateJournal} from '../packages/server/src/recovery-state-journal';

const posix=it.skipIf(process.platform==='win32');
const sourceId='source-journal-fixture';
const state=(at:string):RecoverySourceState=>({version:1,status:'exported_unverified',source:{id:sourceId,database:'fixture',schema_version:22,authenticated:false},exported_at:at,deletion:{schema_version:2,exported_at:at,projects:[],jobs:[],barriers:[],retention:[]},integrity:{blocked:false,reason:null,detected_at:null,details:{},audit_high_water:'0',streams:[]},kafka_observation_verified:false,latest_state_verified:false});
const backup={id:'journal-backup-fixture',manifest_sha256:'a'.repeat(64)};
async function fixture(run:(value:Awaited<ReturnType<typeof createFixture>>)=>Promise<void>){
  const value=await createFixture();
  const cleanup=async(path:string):Promise<void>=>{
    const absolute=resolve(path);if(absolute!==value.parent&&!absolute.startsWith(value.parent+sep))throw Error('fixture_cleanup_outside_owned_root');
    await chmod(absolute,0o700);
    for(const entry of await readdir(absolute,{withFileTypes:true})){
      const child=join(absolute,entry.name);if(entry.isDirectory()&&!entry.isSymbolicLink())await cleanup(child);else await unlink(child);
    }
    await rmdir(absolute);
  };
  try{await run(value);}finally{await cleanup(value.parent);}
}
async function createFixture(){
  const parent=await mkdtemp(join(tmpdir(),'ccpanel-state-journal-')),journalRoot=join(parent,'records'),anchorRoot=join(parent,'anchors'),encryptionKeyFile=join(parent,'encryption-key');
  await mkdir(journalRoot,{mode:0o700});await mkdir(anchorRoot,{mode:0o700});
  const key=randomBytes(32);await writeFile(encryptionKeyFile,key,{mode:0o600,flag:'wx'});
  const keys=generateKeyPairSync('ed25519'),now=Date.now();
  const first=signRecoveryStateRecord({state:state(new Date(now-1000).toISOString()),backup,previous:null,privateKey:keys.privateKey,now});
  const second=signRecoveryStateRecord({state:state(new Date(now).toISOString()),backup,previous:first.anchor,privateKey:keys.privateKey,now});
  return{parent,journalRoot,anchorRoot,encryptionKeyFile,key,publicKey:keys.publicKey,first,second};
}
type Fixture=Awaited<ReturnType<typeof createFixture>>;
const base=(value:Fixture)=>({journalRoot:value.journalRoot,anchorRoot:value.anchorRoot,encryptionKeyFile:value.encryptionKeyFile,publicKey:value.publicKey,expectedSourceId:sourceId,timeoutMs:5000});
const objectPath=(value:Fixture,which:'first'|'second'='first')=>join(value.journalRoot,value[which].anchor.sequence+'-'+value[which].anchor.record_hash+'.state.enc');
function independentlyDecrypt(bytes:Buffer,key:Buffer){
  expect(bytes.subarray(0,8).toString('ascii')).toBe('CCPBK001');
  const decipher=createDecipheriv('aes-256-gcm',key,bytes.subarray(8,20));decipher.setAAD(bytes.subarray(0,8));decipher.setAuthTag(bytes.subarray(-16));
  return JSON.parse(Buffer.concat([decipher.update(bytes.subarray(20,-16)),decipher.final()]).toString('utf8'));
}

posix('stores only private authenticated ciphertext and a local cursor without promoting recovery trust',async()=>fixture(async value=>{
  await writeFile(join(value.journalRoot,'operator-note'),'keep-object-note',{mode:0o600});
  await writeFile(join(value.anchorRoot,'operator-note'),'keep-anchor-note',{mode:0o600});
  const result=await appendRecoveryStateJournal({...base(value),expectedHead:null,record:value.first.record});
  expect(result).toMatchObject({status:'stored_unverified',anchor:value.first.anchor,encryptedAtRest:true,previousHeadVerified:true,historicalRecordsReverified:false,sourceEndpointVerified:false,anchorCustodyVerified:false,latestStateVerified:false});
  const encrypted=await readFile(objectPath(value));expect(independentlyDecrypt(encrypted,value.key)).toEqual(value.first.record);
  expect(encrypted.includes(Buffer.from(value.first.record.payload))).toBe(false);
  expect((await stat(objectPath(value))).mode&0o777).toBe(0o600);expect((await stat(join(value.anchorRoot,'head.json'))).mode&0o777).toBe(0o600);
  expect(JSON.parse(await readFile(join(value.anchorRoot,'head.json'),'utf8'))).toEqual(value.first.anchor);
  expect((await readdir(value.anchorRoot)).sort()).toEqual(['head.json','operator-note']);
  expect((await readdir(value.journalRoot)).sort()).toEqual([value.first.anchor.sequence+'-'+value.first.anchor.record_hash+'.state.enc','operator-note'].sort());
  const checked=await verifyRecoveryStateJournal({...base(value),trustedTip:value.first.anchor});
  expect(checked).toMatchObject({status:'journal_verified_unadmitted',recordsVerified:1,encryptedAtRest:true,signaturesVerified:true,anchorMatched:true,sourceEndpointVerified:false,anchorCustodyVerified:false,latestStateVerified:false});
  expect(checked.state.source.authenticated).toBe(false);
  expect(await readFile(join(value.journalRoot,'operator-note'),'utf8')).toBe('keep-object-note');expect(await readFile(join(value.anchorRoot,'operator-note'),'utf8')).toBe('keep-anchor-note');
}));
posix('appends by exact expected head and verifies the complete encrypted prefix against an external tip',async()=>fixture(async value=>{
  await appendRecoveryStateJournal({...base(value),expectedHead:null,record:value.first.record});
  const firstBytes=await readFile(objectPath(value)),firstHead=await readFile(join(value.anchorRoot,'head.json'));
  await expect(appendRecoveryStateJournal({...base(value),expectedHead:null,record:value.first.record})).rejects.toMatchObject({code:'journal_head_conflict'});
  expect(await readFile(objectPath(value))).toEqual(firstBytes);expect(await readFile(join(value.anchorRoot,'head.json'))).toEqual(firstHead);
  const result=await appendRecoveryStateJournal({...base(value),expectedHead:value.first.anchor,record:value.second.record});expect(result.anchor).toEqual(value.second.anchor);
  expect(await readFile(objectPath(value))).toEqual(firstBytes);expect(independentlyDecrypt(await readFile(objectPath(value,'second')),value.key)).toEqual(value.second.record);
  const checked=await verifyRecoveryStateJournal({...base(value),trustedTip:value.second.anchor});expect(checked.recordsVerified).toBe(2);expect(checked.anchor).toEqual(value.second.anchor);
  await expect(verifyRecoveryStateJournal({...base(value),trustedTip:value.first.anchor})).rejects.toMatchObject({code:'journal_head_conflict'});
}));
posix('allows only one competing append for the same expected head',async()=>fixture(async value=>{
  const results=await Promise.allSettled([appendRecoveryStateJournal({...base(value),expectedHead:null,record:value.first.record}),appendRecoveryStateJournal({...base(value),expectedHead:null,record:value.first.record})]);
  expect(results.filter(result=>result.status==='fulfilled')).toHaveLength(1);
  const rejected=results.find(result=>result.status==='rejected') as PromiseRejectedResult;expect(['journal_busy','journal_head_conflict']).toContain(rejected.reason.code);
  expect((await readdir(value.journalRoot))).toEqual([value.first.anchor.sequence+'-'+value.first.anchor.record_hash+'.state.enc']);
  expect((await verifyRecoveryStateJournal({...base(value),trustedTip:value.first.anchor})).recordsVerified).toBe(1);
}));
posix('does not steal a retained writer lock or touch its evidence',async()=>fixture(async value=>{
  const lock=join(value.anchorRoot,'.append.lock');await mkdir(lock,{mode:0o700});await writeFile(join(lock,'operator-evidence'),'keep',{mode:0o600});
  await expect(appendRecoveryStateJournal({...base(value),expectedHead:null,record:value.first.record})).rejects.toMatchObject({code:'journal_busy'});
  expect(await readFile(join(lock,'operator-evidence'),'utf8')).toBe('keep');expect(await readdir(value.journalRoot)).toEqual([]);
}));
posix.each(['missing-head','orphan','duplicate-sequence'])('requires explicit recovery for %s instead of resetting an existing journal',async mode=>fixture(async value=>{
  await appendRecoveryStateJournal({...base(value),expectedHead:null,record:value.first.record});const original=await readFile(objectPath(value));
  if(mode==='missing-head')await unlink(join(value.anchorRoot,'head.json'));
  if(mode==='orphan')await writeFile(objectPath(value,'second'),original,{mode:0o600,flag:'wx'});
  if(mode==='duplicate-sequence')await writeFile(join(value.journalRoot,'1-'+'0'.repeat(64)+'.state.enc'),original,{mode:0o600,flag:'wx'});
  await expect(appendRecoveryStateJournal({...base(value),expectedHead:mode==='missing-head'?null:value.first.anchor,record:mode==='missing-head'?value.first.record:value.second.record})).rejects.toMatchObject({code:'journal_recovery_required'});
  expect(await readFile(objectPath(value))).toEqual(original);
}));
posix('checks historical ciphertext as well as the tip and refuses a wrong encryption key',async()=>fixture(async value=>{
  await appendRecoveryStateJournal({...base(value),expectedHead:null,record:value.first.record});
  await appendRecoveryStateJournal({...base(value),expectedHead:value.first.anchor,record:value.second.record});
  const original=await readFile(objectPath(value)),head=await readFile(join(value.anchorRoot,'head.json'));
  const changed=Buffer.from(original);changed[25]^=1;await writeFile(objectPath(value),changed);
  await expect(verifyRecoveryStateJournal({...base(value),trustedTip:value.second.anchor})).rejects.toMatchObject({code:'journal_authentication_failed'});
  await writeFile(objectPath(value),original);await writeFile(value.encryptionKeyFile,randomBytes(32));
  await expect(verifyRecoveryStateJournal({...base(value),trustedTip:value.second.anchor})).rejects.toMatchObject({code:'journal_authentication_failed'});
  expect(await readFile(join(value.anchorRoot,'head.json'))).toEqual(head);
}));
posix('refuses unsafe directory/key permissions, symlinks and overlapping roots',async()=>fixture(async value=>{
  await chmod(value.journalRoot,0o755);await expect(appendRecoveryStateJournal({...base(value),expectedHead:null,record:value.first.record})).rejects.toMatchObject({code:'journal_directory_unsafe'});await chmod(value.journalRoot,0o700);
  await chmod(value.encryptionKeyFile,0o644);await expect(appendRecoveryStateJournal({...base(value),expectedHead:null,record:value.first.record})).rejects.toMatchObject({code:'journal_key_unsafe'});await chmod(value.encryptionKeyFile,0o600);
  const alias=join(value.parent,'alias');await symlink(value.journalRoot,alias,'dir');
  await expect(appendRecoveryStateJournal({...base(value),journalRoot:alias,expectedHead:null,record:value.first.record})).rejects.toMatchObject({code:'journal_directory_unsafe'});
  await expect(appendRecoveryStateJournal({...base(value),anchorRoot:value.journalRoot,expectedHead:null,record:value.first.record})).rejects.toMatchObject({code:'journal_directory_unsafe'});
  expect(await readdir(value.journalRoot)).toEqual([]);
}));
it('refuses cancellation before touching any filesystem paths',async()=>{
  const keys=generateKeyPairSync('ed25519'),controller=new AbortController();controller.abort();
  await expect(appendRecoveryStateJournal({journalRoot:join(tmpdir(),'unused-journal'),anchorRoot:join(tmpdir(),'unused-anchor'),encryptionKeyFile:join(tmpdir(),'unused-key'),publicKey:keys.publicKey,expectedSourceId:sourceId,expectedHead:null,record:{},signal:controller.signal})).rejects.toMatchObject({code:'journal_aborted'});
});
it('never substitutes the local head for a missing external trusted tip',async()=>{
  const keys=generateKeyPairSync('ed25519');
  await expect(verifyRecoveryStateJournal({journalRoot:join(tmpdir(),'unused-journal'),anchorRoot:join(tmpdir(),'unused-anchor'),encryptionKeyFile:join(tmpdir(),'unused-key'),publicKey:keys.publicKey,expectedSourceId:sourceId,trustedTip:null})).rejects.toMatchObject({code:'journal_anchor_required'});
});
it.skipIf(process.platform!=='win32')('does not infer private Windows ACLs from mode bits',async()=>{
  const keys=generateKeyPairSync('ed25519');
  await expect(appendRecoveryStateJournal({journalRoot:join(tmpdir(),'unused-journal'),anchorRoot:join(tmpdir(),'unused-anchor'),encryptionKeyFile:join(tmpdir(),'unused-key'),publicKey:keys.publicKey,expectedSourceId:sourceId,expectedHead:null,record:{}})).rejects.toMatchObject({code:'journal_permissions_unverified'});
});
