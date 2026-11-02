import {createHmac,timingSafeEqual} from 'node:crypto';
import {isIP} from 'node:net';

export class BackupHostPinError extends Error{
  constructor(public code:'sftp_host_pins_invalid'|'sftp_host_pin_missing'){super(code);this.name='BackupHostPinError';}
}
const invalid=():never=>{throw new BackupHostPinError('sftp_host_pins_invalid');};
const hostname=(value:string)=>value.length>0&&value.length<=253&&(isIP(value)!==0||value.split('.').every(label=>/^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/i.test(label)));
const algorithms=new Set(['ssh-ed25519','ssh-rsa','ecdsa-sha2-nistp256','ecdsa-sha2-nistp384','ecdsa-sha2-nistp521']);
function base64(value:string,max:number){
  if(!value.length||value.length>Math.ceil(max/3)*4||!/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(value))invalid();
  const bytes=Buffer.from(value,'base64');if(!bytes.length||bytes.length>max||bytes.toString('base64')!==value)invalid();return bytes;
}
function literal(value:string){
  if(value.startsWith('[')){
    const match=/^\[([^\]]+)\]:([1-9][0-9]{0,4})$/.exec(value);
    if(!match||!hostname(match[1]!)||Number(match[2])>65535)invalid();
    return '['+match![1]!.toLowerCase()+']:'+match![2]!;
  }
  if(!hostname(value))invalid();return value.toLowerCase();
}

// Dedicated known_hosts subset: exact names/ports or OpenSSH |1| hashes only.
// Reject unsupported markers and patterns anywhere, never fall back to TOFU.
// SSH itself validates key mathematics; this parser validates the first wire
// algorithm tag and compares the complete raw public key byte-for-byte.
export function createBackupHostVerifier(text:string,host:string,port:number):(rawKey:Buffer)=>boolean{
  if(typeof text!=='string'||Buffer.byteLength(text)>1048576||typeof host!=='string'||!hostname(host)||!Number.isInteger(port)||port<1||port>65535||/[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]/.test(text))invalid();
  const lines=text.split(/\r?\n/);if(lines.length>4096)invalid();
  const normalizedHost=host.toLowerCase(),target=port===22?normalizedHost:'['+normalizedHost+']:'+port;
  const pins:Buffer[]=[];
  for(const raw of lines){
    if(raw.length>32768)invalid();const line=raw.trim();if(!line||line.startsWith('#'))continue;
    const fields=line.split(/[ \t]+/);if(fields.length<3||fields[0]!.startsWith('@'))invalid();
    const [hosts,algorithm,encoded]=fields as [string,string,string,...string[]];
    if(!algorithms.has(algorithm))invalid();
    const key=base64(encoded,16384);if(key.length<5)invalid();const typeLength=key.readUInt32BE(0);
    if(typeLength!==Buffer.byteLength(algorithm)||typeLength>key.length-5||!key.subarray(4,4+typeLength).equals(Buffer.from(algorithm)))invalid();
    let matches=false;
    if(hosts.startsWith('|')){
      const hashed=/^\|1\|([^|]+)\|([^|]+)$/.exec(hosts);if(!hashed)invalid();
      const salt=base64(hashed![1]!,20),digest=base64(hashed![2]!,20);if(salt.length!==20||digest.length!==20)invalid();
      matches=timingSafeEqual(createHmac('sha1',salt).update(target).digest(),digest);
    }else{
      // Parsing every entry also refuses unrelated wildcard/negative aliases.
      const names=hosts.split(',');if(names.some(name=>!name))invalid();
      matches=names.map(literal).includes(target);
    }
    if(matches)pins.push(Buffer.from(key));
  }
  if(!pins.length)throw new BackupHostPinError('sftp_host_pin_missing');
  return rawKey=>Buffer.isBuffer(rawKey)&&pins.some(pin=>pin.length===rawKey.length&&timingSafeEqual(pin,rawKey));
}
