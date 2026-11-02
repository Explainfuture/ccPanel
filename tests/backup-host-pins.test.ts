import {it,expect} from 'vitest';
import {createHmac} from 'node:crypto';
import {createBackupHostVerifier} from '../packages/server/src/backup-host-pins';

const wireString=(value:Buffer|string)=>{const bytes=Buffer.from(value),length=Buffer.alloc(4);length.writeUInt32BE(bytes.length);return Buffer.concat([length,bytes]);};
// Public SSH wire bytes only. SSH2 validates key mathematics during handshake;
// these parser tests verify exact host and complete byte pinning independently.
const key=(fill=7,algorithm='ssh-ed25519')=>Buffer.concat([wireString(algorithm),wireString(Buffer.alloc(32,fill))]);
const entry=(host='backup.example.invalid',bytes=key(),algorithm='ssh-ed25519')=>host+' '+algorithm+' '+bytes.toString('base64');

it('pins complete raw public keys with exact literal host and DNS case normalization',()=>{
  const verify=createBackupHostVerifier('# dedicated pins\n'+entry('BACKUP.EXAMPLE.INVALID')+' operator comment\n','Backup.Example.Invalid',22);
  expect(verify(key())).toBe(true);expect(verify(key(8))).toBe(false);expect(verify(key().subarray(1))).toBe(false);
  expect(verify('not-a-buffer' as unknown as Buffer)).toBe(false);
});
it('supports exact comma aliases and multiple matching rotation pins',()=>{
  const verify=createBackupHostVerifier(entry('other.example.invalid,backup.example.invalid')+'\n'+entry('backup.example.invalid',key(8)),'backup.example.invalid',22);
  expect(verify(key())).toBe(true);expect(verify(key(8))).toBe(true);expect(verify(key(9))).toBe(false);
});
it('binds nonstandard ports and literal IPv4/IPv6 endpoints without alias matching',()=>{
  expect(createBackupHostVerifier(entry('[backup.example.invalid]:2222'),'backup.example.invalid',2222)(key())).toBe(true);
  expect(()=>createBackupHostVerifier(entry('[backup.example.invalid]:2222'),'backup.example.invalid',22)).toThrow('sftp_host_pin_missing');
  expect(createBackupHostVerifier(entry('192.0.2.3'),'192.0.2.3',22)(key())).toBe(true);
  expect(createBackupHostVerifier(entry('[2001:db8::1]:2222'),'2001:db8::1',2222)(key())).toBe(true);
});
it('matches canonical OpenSSH hashed host tokens including their nonstandard port',()=>{
  const salt=Buffer.alloc(20,23),host='[backup.example.invalid]:2222';
  const hashed='|1|'+salt.toString('base64')+'|'+createHmac('sha1',salt).update(host).digest('base64');
  expect(createBackupHostVerifier(entry(hashed),'BACKUP.EXAMPLE.INVALID',2222)(key())).toBe(true);
  expect(()=>createBackupHostVerifier(entry(hashed),'backup.example.invalid',22)).toThrow('sftp_host_pin_missing');
  expect(()=>createBackupHostVerifier(entry(hashed),'other.example.invalid',2222)).toThrow('sftp_host_pin_missing');
});
it.each(['@revoked','@cert-authority','@unknown'])('refuses unsupported %s markers even alongside an otherwise valid pin',marker=>{
  expect(()=>createBackupHostVerifier(entry()+'\n'+marker+' '+entry('other.example.invalid'),'backup.example.invalid',22)).toThrow('sftp_host_pins_invalid');
});
it.each(['*.example.invalid','!backup.example.invalid','backup?.example.invalid','backup.example.invalid,','[backup.example.invalid]:0','[backup.example.invalid]:65536','bad/name'])('refuses unsupported or malformed host field %s',host=>{
  expect(()=>createBackupHostVerifier(entry(host),'backup.example.invalid',22)).toThrow('sftp_host_pins_invalid');
});
it('rejects noncanonical base64, wire-type mismatches and unsupported key algorithms',()=>{
  for(const value of [entry()+'=',entry().replace(/.$/,'!'),entry('backup.example.invalid',key(),'ssh-rsa'),entry('backup.example.invalid',key(7,'ssh-dss'),'ssh-dss'),'backup.example.invalid ssh-ed25519 '+Buffer.from([0,0,0,50,1]).toString('base64')]){
    expect(()=>createBackupHostVerifier(value,'backup.example.invalid',22)).toThrow('sftp_host_pins_invalid');
  }
});
it('does not trust missing pins or malformed hashed entries',()=>{
  expect(()=>createBackupHostVerifier('# empty\n','backup.example.invalid',22)).toThrow('sftp_host_pin_missing');
  expect(()=>createBackupHostVerifier(entry('other.example.invalid'),'backup.example.invalid',22)).toThrow('sftp_host_pin_missing');
  for(const hosts of ['|2|abc|abc','|1|YQ==|YQ==','|1|bad|bad','|1|'+Buffer.alloc(20).toString('base64')+'|'+Buffer.alloc(20).toString('base64')+'|extra'])expect(()=>createBackupHostVerifier(entry(hosts),'backup.example.invalid',22)).toThrow('sftp_host_pins_invalid');
});
it('rejects invalid endpoint values and oversized dedicated pin inputs',()=>{
  for(const host of ['', '-option', 'name/other', '[2001:db8::1]'])expect(()=>createBackupHostVerifier(entry(),host,22)).toThrow('sftp_host_pins_invalid');
  for(const port of [0,65536,22.5,NaN])expect(()=>createBackupHostVerifier(entry(),'backup.example.invalid',port)).toThrow('sftp_host_pins_invalid');
  for(const text of ['#'.repeat(1048577),'\n'.repeat(4096),'#'+ 'a'.repeat(32768),entry()+'\x00'])expect(()=>createBackupHostVerifier(text,'backup.example.invalid',22)).toThrow('sftp_host_pins_invalid');
});
