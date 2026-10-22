// AES-256-GCM envelope for off-host backup copies. Never print the key or plaintext.
import { createCipheriv, createDecipheriv, randomBytes } from 'node:crypto';
import { createReadStream, createWriteStream } from 'node:fs';
import { open, readFile, link, stat, unlink, writeFile } from 'node:fs/promises';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
const [mode,input,output,keyPath]=process.argv.slice(2);
if(!['encrypt','decrypt'].includes(mode)||!input||!output||!keyPath)throw new Error('Usage: node scripts/encrypt-backup.mjs encrypt|decrypt input output key-file');
try{await stat(output);throw new Error('Output already exists');}catch(e){if(e.code!=='ENOENT')throw e;}
if(mode==='encrypt'){
  try{await writeFile(keyPath,randomBytes(32),{flag:'wx',mode:0o600});}catch(e){if(e.code!=='EEXIST')throw e;}
}
const key=await readFile(keyPath);if(key.length!==32)throw new Error('Expected a 32-byte backup key');
const magic=Buffer.from('CCPBK001'),temporary=output+'.'+randomBytes(8).toString('hex')+'.tmp';
try{
  if(mode==='encrypt'){
    const iv=randomBytes(12),cipher=createCipheriv('aes-256-gcm',key,iv);cipher.setAAD(magic);
    const destination=createWriteStream(temporary,{flags:'wx',mode:0o600});
    destination.write(Buffer.concat([magic,iv]));
    await pipeline(createReadStream(input),cipher,destination,{end:false});
    await new Promise((resolve,reject)=>{destination.once('error',reject);destination.end(cipher.getAuthTag(),resolve);});
  }else{
    const source=await open(input,'r');let header,tag,size;
    try{size=(await source.stat()).size;if(size<36)throw new Error('Invalid encrypted backup');header=Buffer.alloc(20);tag=Buffer.alloc(16);await source.read(header,0,20,0);await source.read(tag,0,16,size-16);}finally{await source.close();}
    if(!header.subarray(0,8).equals(magic))throw new Error('Unknown backup format');
    const decipher=createDecipheriv('aes-256-gcm',key,header.subarray(8));decipher.setAAD(magic);decipher.setAuthTag(tag);
    await pipeline(size===36?Readable.from([]):createReadStream(input,{start:20,end:size-17}),decipher,createWriteStream(temporary,{flags:'wx',mode:0o600}));
  }
  const file=await open(temporary,'r+');try{await file.sync();}finally{await file.close();}
  await link(temporary,output);await unlink(temporary);
  console.log(JSON.stringify({mode,bytes:(await stat(output)).size,completed:true}));
}catch(e){await unlink(temporary).catch(()=>{});throw e;}finally{key.fill(0);}
