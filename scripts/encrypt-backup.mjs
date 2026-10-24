// AES-256-GCM envelope for off-host backup copies. Never print the key or plaintext.
import { createCipheriv, createDecipheriv, randomBytes } from 'node:crypto';
import { createReadStream, createWriteStream,constants } from 'node:fs';
import { open, readFile, link, stat, unlink, writeFile } from 'node:fs/promises';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
const [mode,input,output,keyPath]=process.argv.slice(2);
if(mode==='encrypt-stream'){
  // Automation-only output mode. stdout contains ciphertext bytes, never JSON.
  // Keep legacy manual encrypt/decrypt behavior below unchanged.
  let key,source,keyFile,sourceStream;
  try{
    if(process.argv.slice(2).length!==3||!input||!output||process.platform==='win32')throw Error('unsupported');
    const flags=constants.O_RDONLY|(constants.O_NOFOLLOW??0)|(constants.O_NONBLOCK??0);
    keyFile=await open(output,flags);const metadata=await keyFile.stat();
    if(!metadata.isFile()||metadata.size!==32||(metadata.mode&0o077)!==0||metadata.uid!==process.geteuid())throw Error('unsafe_key');
    const bytes=Buffer.alloc(33);
    try{if((await keyFile.read(bytes,0,33,0)).bytesRead!==32)throw Error('changed_key');key=Buffer.from(bytes.subarray(0,32));}finally{bytes.fill(0);}
    source=await open(input,flags);const before=await source.stat();
    if(!before.isFile()||(before.mode&0o077)!==0||before.uid!==process.geteuid()||(before.dev===metadata.dev&&before.ino===metadata.ino))throw Error('unsafe_source');
    const magic=Buffer.from('CCPBK001'),iv=randomBytes(12),cipher=createCipheriv('aes-256-gcm',key,iv);cipher.setAAD(magic);
    const write=bytes=>new Promise((resolve,reject)=>process.stdout.write(bytes,error=>error?reject(error):resolve()));
    process.stdout.on('error',()=>{});
    await write(Buffer.concat([magic,iv]));
    sourceStream=source.createReadStream({autoClose:false});
    await pipeline(sourceStream,cipher,process.stdout,{end:false});sourceStream.destroy();
    const after=await source.stat();
    if(before.size!==after.size||before.mtimeMs!==after.mtimeMs||before.ctimeMs!==after.ctimeMs)throw Error('source_changed');
    await write(cipher.getAuthTag());
  }catch{
    console.error(JSON.stringify({code:'backup_stream_encryption_failed'}));process.exitCode=1;
  }finally{
    key?.fill(0);sourceStream?.destroy();
    const closed=await Promise.allSettled([source?.close(),keyFile?.close()]);
    if(closed.some(result=>result.status==='rejected')){console.error(JSON.stringify({code:'backup_stream_encryption_failed'}));process.exitCode=1;}
  }
}else{
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
}
