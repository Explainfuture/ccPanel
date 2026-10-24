import { expect,it } from 'vitest';
import { kafkaNamespace } from '../packages/server/src/kafka-namespace';
it('preserves existing production topics and offsets by default',()=>{
  expect(kafkaNamespace({})).toEqual({events:'ccpanel.events',dlq:'ccpanel.events.dlq',group:'ccpanel-writer-v1'});
});
it('permits explicit independent replay namespaces and refuses a DLQ feedback loop',()=>{
  expect(kafkaNamespace({KAFKA_EVENTS_TOPIC:'ccpanel.restore.x.events',KAFKA_DLQ_TOPIC:'ccpanel.restore.x.dlq',KAFKA_GROUP_ID:'ccpanel.restore.x.writer'}).group).toBe('ccpanel.restore.x.writer');
  for(const env of [{KAFKA_EVENTS_TOPIC:'same',KAFKA_DLQ_TOPIC:'same'},{KAFKA_EVENTS_TOPIC:''},{KAFKA_DLQ_TOPIC:'..'},{KAFKA_GROUP_ID:'bad group'}])expect(()=>kafkaNamespace(env)).toThrow('invalid_kafka_namespace');
});
