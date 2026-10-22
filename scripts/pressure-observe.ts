// Read-only operator check. stdin contains only disk counters, not credentials.
import { readFileSync } from 'node:fs';
import { kafka,eventsTopic,consumerGroup } from '../packages/server/src/kafka';
import { readKafkaPressure } from '../packages/server/src/kafka-pressure';
import { evaluatePressure,pressurePolicy } from '../packages/server/src/ingest-pressure';
const policy=pressurePolicy(process.env),admin=kafka.admin();
try{
  await admin.connect();
  const snapshot=await readKafkaPressure(admin,eventsTopic,consumerGroup,policy,JSON.parse(readFileSync(0,'utf8')));
  console.log(JSON.stringify({decision:evaluatePressure(snapshot,policy),partitions:snapshot.partitions,disks:snapshot.disks}));
}finally{await admin.disconnect();}
