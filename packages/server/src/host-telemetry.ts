import { constants } from 'node:fs';
import { open } from 'node:fs/promises';
import type { DiskPressure } from './ingest-pressure';

export interface HostTelemetry {
  version: 1;
  sampledAt: number;
  disks: DiskPressure[];
  host: {
    memoryBytes: {total:string;available:string;swapTotal:string;swapFree:string;dirty:string};
    swapInPages: string;
    swapOutPages: string;
    pageBytes: number;
    cpu: {userTicks:string;systemTicks:string;idleTicks:string;ioWaitTicks:string;stealTicks:string;totalTicks:string;ticksPerSecond:number};
    loadAverage: [number,number,number];
  };
}
export class HostTelemetryError extends Error {
  constructor(public code:'host_probe_invalid'|'host_probe_unavailable') {super(code);this.name='HostTelemetryError';}
}
const invalid=():never=>{throw new HostTelemetryError('host_probe_invalid');};
function fields(value:unknown,names:string[]):value is Record<string,unknown>{
  return !!value&&typeof value==='object'&&!Array.isArray(value)&&Object.keys(value).length===names.length&&names.every(k=>Object.hasOwn(value,k));
}
function counter(value:unknown):bigint{
  if(typeof value!=='string'||!/^(0|[1-9]\d{0,18})$/.test(value))return invalid();
  const number=BigInt(value);if(number>9223372036854775807n)return invalid();return number;
}
const integer=(value:unknown,min=0,max=Number.MAX_SAFE_INTEGER):value is number=>typeof value==='number'&&Number.isSafeInteger(value)&&value>=min&&value<=max;
export function parseHostTelemetry(input:unknown):HostTelemetry{
  if(!fields(input,['version','sampledAt','disks','host'])||input.version!==1||!integer(input.sampledAt)||!Array.isArray(input.disks)||input.disks.length!==2)return invalid();
  const seen=new Set<string>();
  for(const disk of input.disks){
    if(!fields(disk,['kind','sampledAt','totalBytes','availableBytes','totalInodes','availableInodes'])||typeof disk.kind!=='string'||!['kafka','postgres'].includes(disk.kind)||seen.has(disk.kind)||disk.sampledAt!==input.sampledAt)return invalid();
    seen.add(disk.kind);
    const total=counter(disk.totalBytes),free=counter(disk.availableBytes),files=counter(disk.totalInodes),freeFiles=counter(disk.availableInodes);
    if(total===0n||free>total||freeFiles>files)return invalid();
  }
  const host=input.host;
  if(!fields(host,['memoryBytes','swapInPages','swapOutPages','pageBytes','cpu','loadAverage'])||!fields(host.memoryBytes,['total','available','swapTotal','swapFree','dirty']))return invalid();
  const memory=host.memoryBytes;
  for(const value of Object.values(memory))counter(value);
  if(counter(memory.total)===0n||counter(memory.available)>counter(memory.total)||counter(memory.dirty)>counter(memory.total)||counter(memory.swapFree)>counter(memory.swapTotal))return invalid();
  counter(host.swapInPages);counter(host.swapOutPages);
  if(!integer(host.pageBytes,512,1073741824)||(host.pageBytes&(host.pageBytes-1))!==0)return invalid();
  const cpu=host.cpu;
  if(!fields(cpu,['userTicks','systemTicks','idleTicks','ioWaitTicks','stealTicks','totalTicks','ticksPerSecond'])||!integer(cpu.ticksPerSecond,1,10000000))return invalid();
  const sum=['userTicks','systemTicks','idleTicks','ioWaitTicks','stealTicks'].reduce((total,key)=>total+counter(cpu[key]),0n);
  if(sum!==counter(cpu.totalTicks))return invalid();
  if(!Array.isArray(host.loadAverage)||host.loadAverage.length!==3||host.loadAverage.some(v=>typeof v!=='number'||!Number.isFinite(v)||v<0))return invalid();
  return input as unknown as HostTelemetry;
}
export function decodeHostTelemetry(bytes:Uint8Array):HostTelemetry{
  if(bytes.byteLength===0||bytes.byteLength>32768)return invalid();
  try{return parseHostTelemetry(JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(bytes)));}
  catch{return invalid();}
}
export async function readHostTelemetry(path='/telemetry/host.json'):Promise<HostTelemetry>{
  let file;
  try{file=await open(path,constants.O_RDONLY|(constants.O_NOFOLLOW??0)|(constants.O_NONBLOCK??0));}
  catch{throw new HostTelemetryError('host_probe_unavailable');}
  try{
    const stat=await file.stat();
    if(!stat.isFile()||stat.size<1||stat.size>32768)return invalid();
    const bytes=Buffer.alloc(32769),result=await file.read(bytes,0,bytes.length,0);
    if(result.bytesRead!==stat.size)return invalid();
    return decodeHostTelemetry(bytes.subarray(0,result.bytesRead));
  }catch{return invalid();}finally{await file.close();}
}
