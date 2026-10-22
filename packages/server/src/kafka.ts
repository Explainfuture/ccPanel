import { Kafka, logLevel } from 'kafkajs';
import { kafkaNamespace } from './kafka-namespace';
export const kafka = new Kafka({ clientId: 'ccpanel', brokers: (process.env.KAFKA_BROKERS || '127.0.0.1:9092').split(','), logLevel: logLevel.ERROR, requestTimeout: 8000, retry: { retries: 3 } });
const namespace=kafkaNamespace(process.env);
export const eventsTopic = namespace.events;
export const dlqTopic = namespace.dlq;
export const consumerGroup = namespace.group;
export interface TopicCreationObservation {changed:boolean;observedAt:number}
export async function ensureTopics() {
  const admin = kafka.admin(); await admin.connect();
  try {
    const observedAt=Date.now(),existing=new Set(await admin.listTopics());
    const missing=!existing.has(eventsTopic)||!existing.has(dlqTopic);
    const created=await admin.createTopics({ waitForLeaders: true, topics: [
      { topic: eventsTopic, numPartitions: 2, replicationFactor: Number(process.env.KAFKA_RF || 1), configEntries: [{ name: 'retention.ms', value: '259200000' }, { name: 'retention.bytes', value: '134217728' }, { name: 'min.insync.replicas', value: process.env.KAFKA_MIN_ISR || '1' }, { name: 'unclean.leader.election.enable', value: 'false' }] },
      { topic: dlqTopic, numPartitions: 1, replicationFactor: Number(process.env.KAFKA_RF || 1), configEntries: [{ name: 'retention.ms', value: '1209600000' }, { name: 'retention.bytes', value: '67108864' }, { name: 'min.insync.replicas', value: process.env.KAFKA_MIN_ISR || '1' }] },
    ] });
    // KafkaJS can return false for a mixed request where an existing topic and
    // a newly created topic were submitted together. Preserve the preflight.
    return {changed:missing||created,observedAt} satisfies TopicCreationObservation;
  } finally { await admin.disconnect(); }
}
