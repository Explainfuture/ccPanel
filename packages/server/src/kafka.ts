import { Kafka, logLevel } from 'kafkajs';
export const kafka = new Kafka({ clientId: 'ccpanel', brokers: (process.env.KAFKA_BROKERS || '127.0.0.1:9092').split(','), logLevel: logLevel.ERROR, requestTimeout: 8000, retry: { retries: 3 } });
export const eventsTopic = 'ccpanel.events';
export const dlqTopic = 'ccpanel.events.dlq';
export async function ensureTopics() {
  const admin = kafka.admin(); await admin.connect();
  try {
    await admin.createTopics({ waitForLeaders: true, topics: [
      { topic: eventsTopic, numPartitions: 2, replicationFactor: Number(process.env.KAFKA_RF || 1), configEntries: [{ name: 'retention.ms', value: '259200000' }, { name: 'retention.bytes', value: '134217728' }, { name: 'min.insync.replicas', value: process.env.KAFKA_MIN_ISR || '1' }, { name: 'unclean.leader.election.enable', value: 'false' }] },
      { topic: dlqTopic, numPartitions: 1, replicationFactor: Number(process.env.KAFKA_RF || 1), configEntries: [{ name: 'retention.ms', value: '1209600000' }, { name: 'retention.bytes', value: '67108864' }, { name: 'min.insync.replicas', value: process.env.KAFKA_MIN_ISR || '1' }] },
    ] });
  } finally { await admin.disconnect(); }
}
