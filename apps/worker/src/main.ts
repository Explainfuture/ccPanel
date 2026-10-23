import { kafka, ensureTopics, eventsTopic, dlqTopic,consumerGroup } from '../../../packages/server/src/kafka';
import { persist, EventConflict } from '../../../packages/server/src/consume';
import { validateEvent, type StoredEnvelope } from '../../../packages/schema/src/index';
import { pool } from '../../../packages/server/src/db';
import { digest } from '../../../packages/server/src/security';
import { discardDeletedDeadLetter,identityHash } from '../../../packages/server/src/privacy';
import { holdRecoveryLease } from '../../../packages/server/src/recovery-gate';
import { indexDeadLetter,type DeadLetter } from '../../../packages/server/src/dead-letter';
import { assertConsumerIntegrity,assertTopicCreationSafe,ensureConsumerProgress,withConsumerProgress,ConsumerProgressError } from '../../../packages/server/src/consumer-progress';
import { readKafkaPressure } from '../../../packages/server/src/kafka-pressure';
import { defaultPressurePolicy } from '../../../packages/server/src/ingest-pressure';
import { startAggregations } from '../../../packages/server/src/aggregations';
import { startRetention } from '../../../packages/server/src/retention';
import { startPartitions } from '../../../packages/server/src/partitions';
import { WorkerMetrics,startWorkerMetricLog } from '../../../packages/server/src/worker-metrics';
const metrics=new WorkerMetrics();
const releaseRecoveryLease=await holdRecoveryLease();
const stream={topic:eventsTopic,group:consumerGroup};
await assertTopicCreationSafe(stream,await ensureTopics());
const progressAdmin=kafka.admin();await progressAdmin.connect();
const verifyProgress=async()=>{
  await assertConsumerIntegrity();
  const positions=await readKafkaPressure(progressAdmin,eventsTopic,consumerGroup,defaultPressurePolicy,[]);
  await ensureConsumerProgress(stream,positions.partitions,undefined,()=>progressAdmin.fetchTopicOffsets(eventsTopic));
};
await verifyProgress();
const consumer = kafka.consumer({ groupId: consumerGroup, sessionTimeout: 30000 });
const producer = kafka.producer({ idempotent: true, maxInFlightRequests: 1 });
consumer.on(consumer.events.CRASH, ({ payload }) => {
  if (!payload.restart) { console.error(JSON.stringify({code:'consumer_halted'})); process.exit(1); }
});
await producer.connect(); await consumer.connect(); await consumer.subscribe({ topic: eventsTopic, fromBeginning: true });
await consumer.run({ autoCommit: false, eachBatchAutoResolve: false, eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning, isStale }) => {
  for (const message of batch.messages) {
    if (!isRunning() || isStale()) break;
    const complete=await withConsumerProgress(stream,batch.partition,message.offset,async()=>{
    let record: StoredEnvelope | undefined;
    let reason: string | undefined;
    try {
      record = JSON.parse(message.value?.toString() || 'null') as StoredEnvelope;
      const uuid=/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
      reason = !record || !uuid.test(record.tenant_id) || !uuid.test(record.project_id) || !Number.isFinite(Date.parse(record.received_at)) ? 'invalid_envelope' : validateEvent(record.event, Date.now(), false);
    } catch { reason = 'invalid_json'; }
    if (!reason && record) {
      while (isRunning() && !isStale()) {
        try { await metrics.measure('persist',()=>persist(record!),value=>value,error=>error instanceof EventConflict?'conflict':'failed'); break; }
        catch (e) {
          if (e instanceof EventConflict) { reason = e.reason; break; }
          console.error(JSON.stringify({code:'database_write_retry',partition:batch.partition}));
          await heartbeat();
          await new Promise(resolve=>setTimeout(resolve,2000));
        }
      }
      if (!isRunning() || isStale()) return false;
    }
    if(reason&&await discardDeletedDeadLetter(record))reason=undefined;
    if (reason) {
      const id = digest(`${batch.topic}:${batch.partition}:${message.offset}`);
      const validScope=(value:unknown)=>typeof value==='string'&&/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value)?value:null;
      const tenant=validScope(record?.tenant_id),project=validScope(record?.project_id);
      const identities=tenant&&project?[record?.event?.distinct_id,record?.event?.anonymous_id].filter((v):v is string=>typeof v==='string'&&v.length>0&&v.length<=256).map(v=>identityHash(tenant,project,v)):[];
      const dead:DeadLetter = { id, reason, source: { topic: batch.topic, partition: batch.partition, offset: message.offset },tenant_id:tenant,project_id:project,identity_hashes:identities,raw_base64:message.value?.toString('base64')??null,processor_version:2 };
      await metrics.measure('dlq_publish',()=>producer.send({ topic: dlqTopic, acks: -1, messages: [{ key: id, value: JSON.stringify(dead) }] }));
      // The exact original bytes remain in a storage-safe envelope. The DLQ
      // acknowledgement precedes the offset; a failed index write retries.
      await metrics.measure('dlq_index',()=>indexDeadLetter(dead,record));
    }
    return true;
    });
    if(!complete||!isRunning()||isStale())break;
    resolveOffset(message.offset);
    await metrics.measure('kafka_checkpoint',()=>consumer.commitOffsets([{ topic: batch.topic, partition: batch.partition, offset: (BigInt(message.offset) + 1n).toString() }]));
    await heartbeat();
  }
} });
console.log(JSON.stringify({ code: 'worker_ready' }));
const stopMetrics=startWorkerMetricLog(metrics);
const stopAggregations=startAggregations();
const stopRetention=startRetention();
const stopPartitions=startPartitions();
let checking=false;
const progressTimer=setInterval(async()=>{
  if(checking)return;checking=true;
  try{await verifyProgress();}
  catch(error){console.error(JSON.stringify({code:error instanceof ConsumerProgressError?'consumer_progress_halted':'consumer_progress_probe_failed',kind:error instanceof ConsumerProgressError?error.kind:undefined}));if(error instanceof ConsumerProgressError)process.exit(1);}
  finally{checking=false;}
},5000);
progressTimer.unref();
for (const signal of ['SIGTERM','SIGINT'] as const) process.once(signal, async () => { stopPartitions();stopRetention();stopAggregations();clearInterval(progressTimer);await consumer.disconnect();stopMetrics(); await producer.disconnect();await progressAdmin.disconnect();await releaseRecoveryLease();await pool.end(); process.exit(0); });
