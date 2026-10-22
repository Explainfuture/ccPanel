import { serve } from '@hono/node-server';
import { createApp } from './app';
import { kafka, ensureTopics, eventsTopic } from '../../../packages/server/src/kafka';
import { pool } from '../../../packages/server/src/db';
import { startQueryJobs } from '../../../packages/server/src/query-jobs';
await ensureTopics();
const producer=kafka.producer({idempotent:true,maxInFlightRequests:1});
await producer.connect();
let connected=true;
const admin=kafka.admin();await admin.connect();
let probing=false;
const probe=setInterval(async()=>{
  if(probing)return;probing=true;
  try{await admin.fetchTopicMetadata({topics:[eventsTopic]});connected=true;}catch{connected=false;}finally{probing=false;}
},5000);
probe.unref();
producer.on('producer.disconnect',()=>{connected=false;});
producer.on('producer.connect',()=>{connected=true;});
const app=createApp({ready:()=>connected,publish:async records=>{
  await producer.send({topic:eventsTopic,acks:-1,timeout:8000,messages:records.map(r=>({key:r.project_id+':'+r.event.distinct_id,value:JSON.stringify(r)}))});
}});
const server=serve({fetch:app.fetch,port:Number(process.env.PORT||3000),hostname:process.env.HOST||'127.0.0.1'});
console.log(JSON.stringify({code:'api_ready'}));
const stopJobs=startQueryJobs();
for(const signal of ['SIGTERM','SIGINT'] as const) process.once(signal,()=>{stopJobs();clearInterval(probe);server.close(async()=>{await admin.disconnect();await producer.disconnect();await pool.end();process.exit(0);});});
