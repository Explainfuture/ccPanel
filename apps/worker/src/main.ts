import { kafka, ensureTopics, eventsTopic, dlqTopic } from '../../../packages/server/src/kafka';
import { persist, EventConflict } from '../../../packages/server/src/consume';
import { validateEvent, type StoredEnvelope } from '../../../packages/schema/src/index';
import { query, pool } from '../../../packages/server/src/db';
import { digest } from '../../../packages/server/src/security';
import { discardDeletedDeadLetter,identityHash } from '../../../packages/server/src/privacy';
await ensureTopics();
const consumer = kafka.consumer({ groupId: 'ccpanel-writer-v1', sessionTimeout: 30000 });
const producer = kafka.producer({ idempotent: true, maxInFlightRequests: 1 });
consumer.on(consumer.events.CRASH, ({ payload }) => {
  if (!payload.restart) { console.error(JSON.stringify({code:'consumer_halted'})); process.exit(1); }
});
await producer.connect(); await consumer.connect(); await consumer.subscribe({ topic: eventsTopic, fromBeginning: true });
await consumer.run({ autoCommit: false, eachBatchAutoResolve: false, eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning, isStale }) => {
  for (const message of batch.messages) {
    if (!isRunning() || isStale()) break;
    let record: StoredEnvelope | undefined;
    let reason: string | undefined;
    try {
      record = JSON.parse(message.value?.toString() || 'null') as StoredEnvelope;
      const uuid=/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
      reason = !record || !uuid.test(record.tenant_id) || !uuid.test(record.project_id) || !Number.isFinite(Date.parse(record.received_at)) ? 'invalid_envelope' : validateEvent(record.event, Date.now(), false);
    } catch { reason = 'invalid_json'; }
    if (!reason && record) {
      while (isRunning() && !isStale()) {
        try { await persist(record); break; }
        catch (e) {
          if (e instanceof EventConflict) { reason = e.reason; break; }
          console.error(JSON.stringify({code:'database_write_retry',partition:batch.partition}));
          await heartbeat();
          await new Promise(resolve=>setTimeout(resolve,2000));
        }
      }
      if (!isRunning() || isStale()) break;
    }
    if(reason&&await discardDeletedDeadLetter(record))reason=undefined;
    if (reason) {
      const id = digest(`${batch.topic}:${batch.partition}:${message.offset}`);
      const validScope=(value:unknown)=>typeof value==='string'&&/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value)?value:null;
      const tenant=validScope(record?.tenant_id),project=validScope(record?.project_id);
      const identities=tenant&&project?[record?.event?.distinct_id,record?.event?.anonymous_id].filter((v):v is string=>typeof v==='string'&&v.length>0&&v.length<=256).map(v=>identityHash(tenant,project,v)):[];
      const dead = { id, reason, source: { topic: batch.topic, partition: batch.partition, offset: message.offset },tenant_id:tenant,project_id:project,identity_hashes:identities,raw_base64:message.value?.toString('base64')??null,processor_version:2 };
      await producer.send({ topic: dlqTopic, acks: -1, messages: [{ key: id, value: JSON.stringify(dead) }] });
      // The exact original bytes remain in a storage-safe envelope. The DLQ
      // acknowledgement precedes the offset; a failed index write retries.
      await query('INSERT INTO dlq_records(id,tenant_id,project_id,reason,payload) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING', [id,validScope(record?.tenant_id),validScope(record?.project_id),reason,JSON.stringify(dead)]);
    }
    resolveOffset(message.offset);
    await consumer.commitOffsets([{ topic: batch.topic, partition: batch.partition, offset: (BigInt(message.offset) + 1n).toString() }]);
    await heartbeat();
  }
} });
console.log(JSON.stringify({ code: 'worker_ready' }));
for (const signal of ['SIGTERM','SIGINT'] as const) process.once(signal, async () => { await consumer.disconnect(); await producer.disconnect(); await pool.end(); process.exit(0); });
