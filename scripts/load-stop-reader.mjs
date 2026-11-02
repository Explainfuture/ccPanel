import {constants,promises as fs} from 'node:fs';

export class StopReadError extends Error{
  constructor(){super('invalid_stop_request');this.name='StopReadError';this.code='invalid_stop_request';}
}
const reject=()=>{throw new StopReadError();};
const regular=stat=>stat.isFile()&&!stat.isSymbolicLink()&&stat.size>=0n&&stat.size<=1024n;
const metadata=(left,right)=>left.size===right.size&&left.mtimeNs===right.mtimeNs&&left.ctimeNs===right.ctimeNs;
const stable=(left,right)=>regular(right)&&left.dev===right.dev&&left.ino===right.ino&&metadata(left,right);
const sameFile=(path,file)=>metadata(path,file)&&(process.platform==='win32'
  ?path.ino>0n&&file.ino>0n&&path.ino===file.ino
  :path.dev===file.dev&&path.ino===file.ino);
// One JSON string key/value pair only. JSON.parse validates escapes after this
// bounded grammar has excluded duplicate/extra members and non-ASCII spacing.
const pair=/^[\t\n\r ]*\{[\t\n\r ]*("(?:[^"\\\x00-\x1f]|\\.)*")[\t\n\r ]*:[\t\n\r ]*("(?:[^"\\\x00-\x1f]|\\.)*")[\t\n\r ]*\}[\t\n\r ]*$/u;

// The caller owns a stable private parent. These observations reject changes
// during the read; they are not an atomic snapshot of a hostile directory.
export async function readStopReason(path){
  let file;
  try{
    if(typeof path!=='string'||!path||path.includes('\0'))reject();
    let before;
    try{before=await fs.lstat(path,{bigint:true});}
    catch(error){if(error?.code==='ENOENT')return null;throw error;}
    if(!regular(before))reject();
    file=await fs.open(path,constants.O_RDONLY|(constants.O_NOFOLLOW??0)|(constants.O_NONBLOCK??0));
    const opened=await file.stat({bigint:true});
    if(!regular(opened)||!sameFile(before,opened))reject();
    const buffer=Buffer.alloc(1025);let bytes=0;
    while(bytes<buffer.length){
      const {bytesRead}=await file.read(buffer,bytes,buffer.length-bytes,null);
      if(bytesRead===0)break;
      bytes+=bytesRead;if(bytes>1024)reject();
    }
    const finished=await file.stat({bigint:true}),after=await fs.lstat(path,{bigint:true});
    if(BigInt(bytes)!==before.size||!stable(before,after)||!stable(opened,finished)||!sameFile(after,finished))reject();
    const text=new TextDecoder('utf-8',{fatal:true,ignoreBOM:true}).decode(buffer.subarray(0,bytes)),match=pair.exec(text);
    if(!match||JSON.parse(match[1])!=='reason')reject();
    const reason=JSON.parse(match[2]);
    if(reason!=='resource_safety'&&reason!=='operator_requested')reject();
    return reason;
  }catch{throw new StopReadError();}
  finally{if(file)try{await file.close();}catch{throw new StopReadError();}}
}
