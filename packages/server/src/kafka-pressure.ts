import type { Admin } from 'kafkajs';
import type { DiskPressure,PressurePolicy,PressureSnapshot } from './ingest-pressure';

type Reader=Pick<Admin,'fetchOffsets'|'fetchTopicOffsetsByTimestamp'|'fetchTopicOffsets'>;
const counter=(value:unknown):value is string=>typeof value==='string'&&/^(0|[1-9]\d{0,18})$/.test(value)&&BigInt(value)<=9223372036854775807n;
const partition=(value:unknown):value is number=>typeof value==='number'&&Number.isSafeInteger(value)&&value>=0;
const invalid=():never=>{throw new Error('invalid_kafka_pressure_observation');};
export async function readKafkaPressure(admin:Reader,topic:string,group:string,policy:PressurePolicy,disks:DiskPressure[],clock=Date.now):Promise<PressureSnapshot>{
  const startedAt=clock();
  // resolveOffsets=true is not a read-only option in KafkaJS: it calls
  // setOffsets(). Never let an observation reset/advance the live consumer.
  const groups=await admin.fetchOffsets({groupId:group,topics:[topic],resolveOffsets:false});
  if(groups.length!==1||groups[0]?.topic!==topic)return invalid();
  const commits=new Map<number,string|null>();
  for(const item of groups[0].partitions){
    if(!partition(item.partition)||commits.has(item.partition)||(item.offset!=='-1'&&!counter(item.offset)))return invalid();
    commits.set(item.partition,item.offset==='-1'?null:item.offset);
  }
  const ages=await admin.fetchTopicOffsetsByTimestamp(topic,startedAt-policy.maxLagAgeMs);
  const boundaries=new Map<number,string>();
  for(const item of ages){
    if(!partition(item.partition)||boundaries.has(item.partition)||!counter(item.offset))return invalid();
    boundaries.set(item.partition,item.offset);
  }
  // Fetch ranges last: committed and age observations must not be newer than
  // our high watermark. Segment expiry during sampling is still caught by low.
  const ranges=await admin.fetchTopicOffsets(topic);
  if(ranges.length<1||ranges.length>1024||ranges.length!==commits.size||ranges.length!==boundaries.size)return invalid();
  const seen=new Set<number>();
  const partitions=ranges.map(item=>{
    if(!partition(item.partition)||seen.has(item.partition)||!counter(item.low)||!counter(item.high)||!commits.has(item.partition)||!boundaries.has(item.partition))return invalid();
    seen.add(item.partition);
    const low=BigInt(item.low),high=BigInt(item.high),boundary=BigInt(boundaries.get(item.partition)!);
    if(low>high||boundary>high)return invalid();
    // If a segment expired after the timestamp lookup, the first remaining
    // qualifying offset is at least low. Do not move the consumer commit.
    return {partition:item.partition,low:item.low,high:item.high,committed:commits.get(item.partition)!,ageBoundary:(boundary<low?low:boundary).toString()};
  });
  return {startedAt,partitions,disks:disks.map(disk=>({...disk}))};
}
