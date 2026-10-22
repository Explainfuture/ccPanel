import { kafka, ensureTopics, eventsTopic, dlqTopic } from '../../../packages/server/src/kafka';
import { persist, EventConflict } from '../../../packages/server/src/consume';
import { validateEvent, type StoredEnvelope } from '../../../packages/schema/src/index';
import { query, pool } from '../../../packages/server/src/db';
import { digest } from '../../../packages/server/src/security';
await ensureTopics();
const consumer = kafka.consumer({ groupId: 'ccpanel-writer-v1', sessionTimeout: 30000 });
const producer = kafka.producer({ idempotent: true, maxInFlightRequests: 1 });
await producer.connect(); await consumer.connect(); await consumer.subscribe({ topic: eventsTopic, fromBeginning: true });
await consumer.run({ autoCommit: false, eachBatchAutoResolve: false, eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning, isStale }) => {
  for (const message of batch.messages) {
    if (!isRunning() || isStale()) break;
    let record: StoredEnvelope | undefined;
    let reason: string | undefined;
    try {
      record = JSON.parse(message.value?.toString() || 'null') as StoredEnvelope;
      reason = !record || !record.tenant_id || !record.project_id ? 'invalid_envelope' : validateEvent(record.event, Date.now(), false);
    } catch { reason = 'invalid_json'; }
    if (!reason && record) {
      try { await persist(record); }
      catch (e) { if (e instanceof EventConflict) reason = e.reason; else throw e; }
    }
    if (reason) {
      const id = digest(`${batch.topic}:${batch.partition}:${message.offset}`);
      const dead = { id, reason, source: { topic: batch.topic, partition: batch.partition, offset: message.offset }, record, processor_version: 1 };
      await producer.send({ topic: dlqTopic, acks: -1, messages: [{ key: id, value: JSON.stringify(dead) }] });
      // The DLQ acknowledgement precedes the offset. A failed admin-index write retries safely.
      await query('INSERT INTO dlq_records(id,tenant_id,project_id,reason,payload) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING', [id,record?.tenant_id || null,record?.project_id || null,reason,JSON.stringify(dead)]);
    }
    resolveOffset(message.offset);
    await consumer.commitOffsets([{ topic: batch.topic, partition: batch.partition, offset: (BigInt(message.offset) + 1n).toString() }]);
    await heartbeat();
  }
} });
console.log(JSON.stringify({ code: 'worker_ready' }));
for (const signal of ['SIGTERM','SIGINT'] as const) process.once(signal, async () => { await consumer.disconnect(); await producer.disconnect(); await pool.end(); process.exit(0); });
