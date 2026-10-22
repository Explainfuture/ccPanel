import {parseHostTelemetry,type HostTelemetry} from '../packages/server/src/host-telemetry';
type Container={memoryBytes:number;memoryEvents:Record<string,number>;cpu:Record<string,number>};
export type CapacityFrame={capturedAt:number;elapsedMs:number;error?:string;telemetry?:HostTelemetry;containers?:Record<string,Container>};
export type ContainerState={id:string;restartCount:number;oomKilled:boolean;startedAt:string;running?:boolean};
export interface CapacityEvidence {
  startedAt:number;finishedAt:number;releaseStart:string;releaseEnd:string;
  frames:CapacityFrame[];baseline:Record<string,ContainerState>;final:Record<string,ContainerState>;
}
const names=['api','worker','kafka','postgres'] as const;
const integer=(value:unknown):value is number=>typeof value==='number'&&Number.isSafeInteger(value)&&value>=0;
const round=(value:number)=>Math.round(value*100)/100;
export function capacityReport(input:CapacityEvidence){
  if(!integer(input.startedAt)||!integer(input.finishedAt)||input.finishedAt<=input.startedAt)throw new Error('invalid_capacity_window');
  const reasons=new Set<string>();
  if(input.frames.some((f,i)=>i>0&&f.capturedAt<=input.frames[i-1]!.capturedAt))reasons.add('invalid_capture_order');
  if(!input.releaseStart||input.releaseStart!==input.releaseEnd)reasons.add('release_changed');
  const sorted=[...input.frames].sort((a,b)=>a.capturedAt-b.capturedAt);
  if(sorted.some((f,i)=>!integer(f.capturedAt)||!integer(f.elapsedMs)||(i>0&&f.capturedAt===sorted[i-1]!.capturedAt)))reasons.add('invalid_capture_order');
  // Bracket the full load with observations, including boundary counter deltas.
  const before=sorted.reduce((found,f,index)=>f.capturedAt<=input.startedAt?index:found,-1),after=sorted.findIndex(f=>f.capturedAt>=input.finishedAt&&typeof f.telemetry?.sampledAt==='number'&&f.telemetry.sampledAt>=input.finishedAt);
  if(before<0||after<0)reasons.add('window_not_covered');
  const frames=before>=0&&after>=before?sorted.slice(before,after+1):[];
  const valid:{frame:CapacityFrame;host:HostTelemetry;containers:Record<string,Container>}[]=[];
  for(const frame of frames){
    if(frame.error){reasons.add('sample_error');continue;}
    try{
      const host=parseHostTelemetry(frame.telemetry),containers=frame.containers;
      if(frame.capturedAt<host.sampledAt||frame.capturedAt-host.sampledAt>30000)throw new Error();
      if(!containers||names.some(name=>{
        const c=containers[name];return !c||!integer(c.memoryBytes)||!c.memoryEvents||!integer(c.memoryEvents.oom_kill)||!integer(c.memoryEvents.oom)||!c.cpu||!integer(c.cpu.usage_usec)||Object.values(c.memoryEvents).some(v=>!integer(v))||Object.values(c.cpu).some(v=>!integer(v));
      }))throw new Error();
      valid.push({frame,host,containers});
    }catch{reasons.add('invalid_sample');}
  }
  let restarts=0,oom=false;const notRunning:string[]=[];
  for(const name of names){
    const b=input.baseline[name],f=input.final[name];
    if(!b||!f||!integer(b.restartCount)||!integer(f.restartCount)||typeof b.oomKilled!=='boolean'||typeof f.oomKilled!=='boolean'||typeof f.running!=='boolean'||!Number.isFinite(Date.parse(b.startedAt))||!Number.isFinite(Date.parse(f.startedAt))||!/^[a-f0-9]{64}$/.test(b.id)||!/^[a-f0-9]{64}$/.test(f.id)){reasons.add('container_state_missing');continue;}
    if(!f.running)notRunning.push(name);
    if(b.id!==f.id||b.startedAt!==f.startedAt||f.restartCount<b.restartCount)reasons.add('container_replaced_or_reset');
    restarts+=Math.max(0,f.restartCount-b.restartCount);oom ||=b.oomKilled||f.oomKilled;
  }
  let totalTicks=0n,idleTicks=0n,swapIn=0n,swapOut=0n,activeIntervals=0,longestRun=0,run=0,maxIn=0,maxOut=0;
  let diskFreeMin=100,availableMin:bigint|null=null;
  const peaks=Object.fromEntries(names.map(name=>[name,0])),oomDeltas=Object.fromEntries(names.map(name=>[name,0])),oomEvents=Object.fromEntries(names.map(name=>[name,0]));
  let previous:typeof valid[number]|undefined;
  for(const row of valid){
    for(const disk of row.host.disks)diskFreeMin=Math.min(diskFreeMin,Number(BigInt(disk.availableBytes)*10000n/BigInt(disk.totalBytes))/100);
    const available=BigInt(row.host.host.memoryBytes.available);availableMin=availableMin===null||available<availableMin?available:availableMin;
    for(const name of names)peaks[name]=Math.max(peaks[name]!,row.containers[name]!.memoryBytes);
    if(previous){
      const elapsed=(row.frame.capturedAt-previous.frame.capturedAt)/1000;
      if(elapsed<=0||elapsed>20)reasons.add('capture_gap');
      for(const name of names){
        const a=previous.containers[name]!,b=row.containers[name]!;
        if(b.cpu.usage_usec!<a.cpu.usage_usec!||b.memoryEvents.oom_kill!<a.memoryEvents.oom_kill!||b.memoryEvents.oom!<a.memoryEvents.oom!)reasons.add('container_counter_reset');
        else {oomDeltas[name]!+=b.memoryEvents.oom_kill!-a.memoryEvents.oom_kill!;oomEvents[name]!+=b.memoryEvents.oom!-a.memoryEvents.oom!;}
      }
      if(row.host.sampledAt<previous.host.sampledAt)reasons.add('host_counter_reset');
      // Repeated host frames are allowed briefly but never counted as new data.
      if(row.host.sampledAt>previous.host.sampledAt){
        const a=previous.host.host,b=row.host.host,seconds=(row.host.sampledAt-previous.host.sampledAt)/1000;
        if(seconds>30)reasons.add('host_sample_gap');
        const di=BigInt(b.swapInPages)-BigInt(a.swapInPages),dout=BigInt(b.swapOutPages)-BigInt(a.swapOutPages),ticks=BigInt(b.cpu.totalTicks)-BigInt(a.cpu.totalTicks),idle=BigInt(b.cpu.idleTicks)-BigInt(a.cpu.idleTicks);
        if(di<0n||dout<0n||ticks<=0n||idle<0n||idle>ticks||a.pageBytes!==b.pageBytes||(['userTicks','systemTicks','ioWaitTicks','stealTicks'] as const).some(key=>BigInt(b.cpu[key])<BigInt(a.cpu[key])))reasons.add('host_counter_reset');
        else{
          totalTicks+=ticks;idleTicks+=idle;swapIn+=di*BigInt(b.pageBytes);swapOut+=dout*BigInt(b.pageBytes);
          maxIn=Math.max(maxIn,Number(di)*b.pageBytes/1024/seconds);maxOut=Math.max(maxOut,Number(dout)*b.pageBytes/1024/seconds);
          if(di||dout){activeIntervals++;run+=seconds;longestRun=Math.max(longestRun,run);}else run=0;
        }
      }
    }
    previous=row;
  }
  // Host counters can lag the cgroup capture. Require host coverage too.
  if(!valid.length||valid[0]!.host.sampledAt>input.startedAt||valid.at(-1)!.host.sampledAt<input.finishedAt)reasons.add('host_window_not_covered');
  if(valid.length!==frames.length||valid.length<2||totalTicks===0n)reasons.add('insufficient_valid_samples');
  const evidenceValid=reasons.size===0,oomKills=Object.values(oomDeltas).reduce((a,b)=>a+b,0),oomCount=Object.values(oomEvents).reduce((a,b)=>a+b,0);
  return {evidenceValid,reasons:[...reasons].sort(),durationSeconds:(input.finishedAt-input.startedAt)/1000,captureFrames:frames.length,validFrames:valid.length,
    cpuIdlePercent:totalTicks?round(Number(idleTicks*10000n/totalTicks)/100):null,memoryAvailableMinBytes:availableMin?.toString()??null,containerMemoryPeakBytes:peaks,
    swapInBytes:swapIn.toString(),swapOutBytes:swapOut.toString(),swapActiveIntervals:activeIntervals,longestSwapActivitySeconds:round(longestRun),swapInMaxKiBPerSecond:round(maxIn),swapOutMaxKiBPerSecond:round(maxOut),diskAvailableMinPercent:valid.length?round(diskFreeMin):null,restarts,oomObserved:oom||oomKills>0||oomCount>0,oomKillDeltas:oomDeltas,oomEventDeltas:oomEvents,
    // Zero swap is a sufficient conservative pass. Any activity still needs
    // explicit assessment against PRD's sustained-activity requirement.
    notRunning,
    conservativeResourcePass:evidenceValid&&notRunning.length===0&&restarts===0&&!oom&&oomKills===0&&oomCount===0&&diskFreeMin>=20&&activeIntervals===0,
    swapReviewRequired:activeIntervals>0};
}
