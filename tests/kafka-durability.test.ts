import {expect,it,vi} from 'vitest';
import type {Admin} from 'kafkajs';
import {assertKafkaDurability,kafkaDurability} from '../packages/server/src/kafka-durability';
const topics=['events','dead'];
const policy=kafkaDurability({KAFKA_RF:'3',KAFKA_MIN_ISR:'2'});
function fixture(){
  const partition=(partitionId:number)=>({partitionErrorCode:0,partitionId,leader:1,replicas:[1,2,3],isr:[1,2]});
  const metadata={topics:[{name:'events',partitions:[partition(0),partition(1)]},{name:'dead',partitions:[partition(0)]}]};
  const configs:Awaited<ReturnType<Admin['describeConfigs']>>={throttleTime:0,resources:topics.map(resourceName=>({resourceType:2,resourceName,errorCode:0,errorMessage:'',configEntries:[['min.insync.replicas','2'],['unclean.leader.election.enable','false']].map(([configName,configValue])=>({configName:configName!,configValue:configValue!,isDefault:false,configSource:1,isSensitive:false,readOnly:false,configSynonyms:[]}))}))};
  const admin={fetchTopicMetadata:vi.fn(async()=>metadata),describeConfigs:vi.fn(async()=>configs)};
  return {admin,metadata,configs};
}
it('defaults to the explicit preview policy and rejects impossible or ambiguous durability settings',()=>{
  expect(kafkaDurability({})).toEqual({replicationFactor:1,minInSyncReplicas:1});
  expect(Object.isFrozen(policy)).toBe(true);
  for(const value of ['', '0','-1','1.0','1e1','01',' 1','1 ','NaN','32768'])for(const field of ['KAFKA_RF','KAFKA_MIN_ISR'])expect(()=>kafkaDurability({[field]:value})).toThrow('invalid_kafka_durability_configuration');
  expect(()=>kafkaDurability({KAFKA_RF:'1',KAFKA_MIN_ISR:'2'})).toThrow('invalid_kafka_durability_configuration');
});
it('reads both topics without weakening a stronger broker confirmation policy',async()=>{
  const f=fixture();await assertKafkaDurability(f.admin,topics,policy);
  expect(f.admin.fetchTopicMetadata).toHaveBeenCalledWith({topics});
  expect(f.admin.describeConfigs).toHaveBeenCalledWith({includeSynonyms:false,resources:topics.map(name=>({type:2,name,configNames:['min.insync.replicas','unclean.leader.election.enable']}))});
  for(const topic of f.metadata.topics)for(const p of topic.partitions)p.isr=[1,2,3];
  for(const r of f.configs.resources)r.configEntries[0]!.configValue='3';
  await expect(assertKafkaDurability(f.admin,topics,policy)).resolves.toBeUndefined();
});
it('refuses a pre-existing weak topic, unsafe election or capped minimum ISR on either stream',async()=>{
  for(const index of [0,1])for(const damage of ['replicas','minimum','capped','isr','election','sensitive']){
    const f=fixture(),p=f.metadata.topics[index]!.partitions[0]!,c=f.configs.resources[index]!.configEntries;
    if(damage==='replicas'){p.replicas=[1];p.isr=[1];}
    if(damage==='minimum')c[0]!.configValue='1';
    if(damage==='capped')c[0]!.configValue='4';
    if(damage==='isr')p.isr=[1];
    if(damage==='election')c[1]!.configValue='true';
    if(damage==='sensitive')c[0]!.isSensitive=true;
    await expect(assertKafkaDurability(f.admin,topics,policy)).rejects.toThrow('kafka_durability_mismatch');
  }
});
it('rejects incomplete, duplicated or failed broker observations instead of assuming defaults',async()=>{
  const cases:Array<(f:ReturnType<typeof fixture>)=>void>=[
    f=>{f.metadata.topics.pop();},f=>{f.metadata.topics[1]=f.metadata.topics[0]!;},
    f=>{f.configs.resources.pop();},f=>{f.configs.resources[1]=f.configs.resources[0]!;},
    f=>{f.configs.resources[0]!.errorCode=29;},f=>{f.configs.resources[0]!.configEntries.pop();},
    f=>{f.configs.resources[0]!.configEntries[1]=f.configs.resources[0]!.configEntries[0]!;},
    f=>{f.metadata.topics[0]!.partitions[0]!.partitionErrorCode=5;},
    f=>{f.metadata.topics[0]!.partitions[1]!.partitionId=0;},
    f=>{f.metadata.topics[0]!.partitions[0]!.replicas=[1,1,2];},
    f=>{f.metadata.topics[0]!.partitions[0]!.isr=[1,4];},
    f=>{f.metadata.topics[0]!.partitions[0]!.leader=-1;},
  ];
  for(const damage of cases){const f=fixture();damage(f);await expect(assertKafkaDurability(f.admin,topics,policy)).rejects.toThrow('kafka_durability_mismatch');}
});
it('propagates broker read failures and never substitutes environment values for observations',async()=>{
  const f=fixture();f.admin.describeConfigs.mockRejectedValueOnce(new Error('broker_unavailable'));
  await expect(assertKafkaDurability(f.admin,topics,policy)).rejects.toThrow('broker_unavailable');
  await expect(assertKafkaDurability(f.admin,['events','events'],policy)).rejects.toThrow('kafka_durability_mismatch');
  expect(f.admin.fetchTopicMetadata).toHaveBeenCalledTimes(1);
});
