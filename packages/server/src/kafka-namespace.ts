export function kafkaNamespace(env:Record<string,string|undefined>){
  const events=env.KAFKA_EVENTS_TOPIC??'ccpanel.events',dlq=env.KAFKA_DLQ_TOPIC??'ccpanel.events.dlq',group=env.KAFKA_GROUP_ID??'ccpanel-writer-v1';
  const topic=(v:string)=>/^[a-zA-Z0-9._-]{1,249}$/.test(v)&&v!=='.'&&v!=='..';
  if(!topic(events)||!topic(dlq)||events===dlq||!/^[a-zA-Z0-9._-]{1,128}$/.test(group))throw new Error('invalid_kafka_namespace');
  return{events,dlq,group};
}
