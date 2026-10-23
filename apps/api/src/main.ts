import { serve } from '@hono/node-server';
import { createApp } from './app';
import {startOperationalCollection} from '../../../packages/server/src/operational-collection';
import {IntakeMetrics,startIntakeMetricLog} from '../../../packages/server/src/intake-metrics';
import { kafka, ensureTopics, eventsTopic } from '../../../packages/server/src/kafka';
import { pool } from '../../../packages/server/src/db';
import { startQueryJobs } from '../../../packages/server/src/query-jobs';
import { startDeletions } from '../../../packages/server/src/deletion-service';
import { holdRecoveryLease } from '../../../packages/server/src/recovery-gate';
import { startReplayJobs } from '../../../packages/server/src/dlq-replay';
import { IngestAdmission,publishWithAdmission } from '../../../packages/server/src/ingest-admission';
import { pressurePolicy } from '../../../packages/server/src/ingest-pressure';
import { readHostTelemetry } from '../../../packages/server/src/host-telemetry';
import {HostResourceStatus} from '../../../packages/server/src/host-resource-status';
import { readKafkaPressure } from '../../../packages/server/src/kafka-pressure';
import { refreshIngestPressure } from '../../../packages/server/src/ingest-runtime';
import { consumerGroup } from '../../../packages/server/src/kafka';
import { assertTopicCreationSafe,inspectConsumerProgress,pauseForProgress,ConsumerProgressError } from '../../../packages/server/src/consumer-progress';
const releaseRecoveryLease=await holdRecoveryLease();
await assertTopicCreationSafe({topic:eventsTopic,group:consumerGroup},await ensureTopics());
const producer=kafka.producer({idempotent:true,maxInFlightRequests:1});
await producer.connect();
let connected=true;
const admin=kafka.admin();await admin.connect();
let probing=false;
const policy=pressurePolicy(process.env),admission=new IngestAdmission(policy);
const hostResources=new HostResourceStatus();
let lastAdmission='';
const checkPressure=async()=>{
  if(probing)return;probing=true;
  try{
    const status=await refreshIngestPressure(admission,{
      readControl:async()=>{
        const row=(await pool.query('SELECT blocked,reason FROM ingest_integrity_gate WHERE id=true')).rows[0];
        if(!row||typeof row.blocked!=='boolean'||(row.blocked&&!['retained_offset_gap','consumer_offset_unknown'].includes(row.reason)))throw new Error('integrity_control_missing');
        return row;
      },
      readHost:async()=>{try{const value=await readHostTelemetry(process.env.HOST_TELEMETRY_PATH||'/telemetry/host.json');try{hostResources.observe(value);}catch{hostResources.failed();}return value;}catch(error){hostResources.failed();throw error;}},
      readKafka:async disks=>{try{const value=await readKafkaPressure(admin,eventsTopic,consumerGroup,policy,disks);connected=true;return value;}catch(error){connected=false;throw error;}},
      validateProgress:async snapshot=>{
        try{const plan=await inspectConsumerProgress({topic:eventsTopic,group:consumerGroup},snapshot.partitions,()=>admin.fetchTopicOffsets(eventsTopic));for(const p of snapshot.partitions)p.high=plan.positions.find(value=>value.partition===p.partition)!.high;}
        catch(error){if(error instanceof ConsumerProgressError)await pauseForProgress({topic:eventsTopic,group:consumerGroup},error);throw error;}
      },
      persistGap:async(reason,snapshot)=>{
        await pauseForProgress({topic:eventsTopic,group:consumerGroup},new ConsumerProgressError(reason,'pressure_retention_gap',{partitions:snapshot.partitions}));
      },
    });
    const signature=JSON.stringify({allowed:status.allowed,reason:status.reason});
    if(signature!==lastAdmission){console.log(JSON.stringify({code:'ingest_admission_changed',allowed:status.allowed,reason:status.reason}));lastAdmission=signature;}
  }finally{probing=false;}
};
await checkPressure();
const probe=setInterval(()=>void checkPressure(),5000);
probe.unref();
producer.on('producer.disconnect',()=>{connected=false;});
producer.on('producer.connect',()=>{connected=true;});
const intakeMetrics=new IntakeMetrics();
const stopIntakeMetrics=startIntakeMetricLog(intakeMetrics);
const stopOperationalCollection=startOperationalCollection(()=>intakeMetrics.snapshot());
const app=createApp({maxIntakeInFlight:Number(process.env.INGEST_MAX_INFLIGHT??16),ready:()=>connected,observeIntake:value=>intakeMetrics.observe(value),intakeStatus:()=>intakeMetrics.snapshot(),hostResourceStatus:()=>hostResources.snapshot(),admissionStatus:()=>admission.status(),pressureAlerts:()=>admission.pressureAlerts(),publish:records=>publishWithAdmission(admission,records.length,async()=>{
  await producer.send({topic:eventsTopic,acks:-1,timeout:8000,messages:records.map(r=>({key:r.project_id+':'+r.event.distinct_id,value:JSON.stringify(r)}))});
})});
const server=serve({fetch:app.fetch,port:Number(process.env.PORT||3000),hostname:process.env.HOST||'127.0.0.1'});
console.log(JSON.stringify({code:'api_ready'}));
const stopJobs=startQueryJobs();
const stopDeletions=startDeletions();
const stopReplay=startReplayJobs((raw,key)=>publishWithAdmission(admission,1,async()=>{
  const receipts=await producer.send({topic:eventsTopic,acks:-1,timeout:8000,messages:[{key,value:raw}]});
  const receipt=receipts[0];
  if(!receipt||receipt.baseOffset===undefined)throw new Error('missing_replay_receipt');
  return {topic:receipt.topicName,partition:receipt.partition,offset:receipt.baseOffset};
}));
for(const signal of ['SIGTERM','SIGINT'] as const) process.once(signal,()=>{stopJobs();stopDeletions();stopReplay();clearInterval(probe);server.close(async()=>{stopIntakeMetrics();await stopOperationalCollection();await admin.disconnect();await producer.disconnect();await releaseRecoveryLease();await pool.end();process.exit(0);});});
