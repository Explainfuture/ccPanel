import type {Admin} from 'kafkajs';
import kafkaPackage from 'kafkajs';
const {ConfigResourceTypes}=kafkaPackage;
export interface KafkaDurability {replicationFactor:number;minInSyncReplicas:number}
export function kafkaDurability(env:Record<string,string|undefined>):Readonly<KafkaDurability>{
  const parse=(value:string|undefined)=>{
    const text=value??'1';
    if(!/^[1-9]\d{0,4}$/.test(text)||Number(text)>32767)throw new Error('invalid_kafka_durability_configuration');
    return Number(text);
  };
  const replicationFactor=parse(env.KAFKA_RF),minInSyncReplicas=parse(env.KAFKA_MIN_ISR);
  if(minInSyncReplicas>replicationFactor)throw new Error('invalid_kafka_durability_configuration');
  return Object.freeze({replicationFactor,minInSyncReplicas});
}
type Reader=Pick<Admin,'fetchTopicMetadata'|'describeConfigs'>;
const fail=():never=>{throw new Error('kafka_durability_mismatch');};
// Startup snapshot only: never alter topic configuration, replicas or offsets.
// acks=all still enforces broker confirmation for every subsequent publication.
export async function assertKafkaDurability(admin:Reader,topics:readonly string[],policy:Readonly<KafkaDurability>){
  if(topics.length!==2||new Set(topics).size!==2)return fail();
  const metadata=await admin.fetchTopicMetadata({topics:[...topics]});
  const configs=await admin.describeConfigs({includeSynonyms:false,resources:topics.map(name=>({type:ConfigResourceTypes.TOPIC,name,configNames:['min.insync.replicas','unclean.leader.election.enable']}))});
  if(metadata.topics.length!==2||configs.resources.length!==2||new Set(metadata.topics.map(t=>t.name)).size!==2||new Set(configs.resources.map(r=>r.resourceName)).size!==2)return fail();
  for(const topic of topics){
    const value=metadata.topics.find(t=>t.name===topic),config=configs.resources.find(r=>r.resourceName===topic);
    if(!value||!config||config.errorCode!==0||config.resourceType!==ConfigResourceTypes.TOPIC||config.configEntries.length!==2)return fail();
    const entry=(name:string)=>{const rows=config.configEntries.filter(c=>c.configName===name);if(rows.length!==1||rows[0]!.isSensitive)return fail();return rows[0]!.configValue;};
    const minText=entry('min.insync.replicas');
    if(!/^[1-9]\d{0,4}$/.test(minText)||Number(minText)>32767||Number(minText)<policy.minInSyncReplicas||entry('unclean.leader.election.enable')!=='false')return fail();
    if(value.partitions.length<1||value.partitions.length>1024)return fail();
    const partitions=[...value.partitions].sort((a,b)=>a.partitionId-b.partitionId);
    for(const [index,p] of partitions.entries()){
      const validIds=(ids:number[])=>ids.length>0&&ids.every(n=>Number.isSafeInteger(n)&&n>=0)&&new Set(ids).size===ids.length;
      if(p.partitionErrorCode!==0||p.partitionId!==index||!validIds(p.replicas)||!validIds(p.isr)||p.replicas.length<policy.replicationFactor||Number(minText)>p.replicas.length||p.isr.length<Number(minText)||!p.isr.every(id=>p.replicas.includes(id))||!p.isr.includes(p.leader))return fail();
    }
  }
}
