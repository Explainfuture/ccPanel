// Root/operator-only source baseline. Never reset Kafka offsets implicitly.
import { kafka,eventsTopic,consumerGroup,ensureTopics } from '../packages/server/src/kafka';
import { pool } from '../packages/server/src/db';
import { readKafkaPressure } from '../packages/server/src/kafka-pressure';
import { defaultPressurePolicy } from '../packages/server/src/ingest-pressure';
import { ensureConsumerProgress,inspectConsumerProgress,assertTopicCreationSafe } from '../packages/server/src/consumer-progress';
const mode=process.argv[2],admin=kafka.admin(),stream={topic:eventsTopic,group:consumerGroup};
try{
  if(!['prepare','check','bootstrap'].includes(mode||''))throw new Error('Usage: consumer-progress.mjs prepare|check|bootstrap');
  await admin.connect();
  if(mode==='prepare'){
    const observation=await ensureTopics();await assertTopicCreationSafe(stream,observation);console.log(JSON.stringify({mode,passed:true,topics_created:observation.changed}));
  }else{
  const snapshot=await readKafkaPressure(admin,eventsTopic,consumerGroup,defaultPressurePolicy,[]);
  let plan;
  if(mode==='bootstrap'){
    const reason=process.env.CCPANEL_PROGRESS_REASON;
    if(!reason)throw new Error('progress_reason_required');
    plan=await ensureConsumerProgress(stream,snapshot.partitions,reason,()=>admin.fetchTopicOffsets(eventsTopic));
  }else plan=await inspectConsumerProgress(stream,snapshot.partitions,()=>admin.fetchTopicOffsets(eventsTopic));
  const control=(await pool.query('SELECT blocked,reason FROM ingest_integrity_gate WHERE id=true')).rows[0];
  if(!control)throw new Error('integrity_control_missing');
  if(mode==='check'&&control.blocked)throw new Error('ingest_integrity_paused');
  console.log(JSON.stringify({mode,passed:true,new_partitions:plan.initialize.length,partition_count:plan.partitionCount,integrity_paused:control.blocked,reason:control.reason}));
  }
}finally{await admin.disconnect();await pool.end();}
