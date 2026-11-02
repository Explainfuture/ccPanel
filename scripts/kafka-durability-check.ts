// Read-only operator snapshot. Does not create topics, alter configs or offsets.
import kafkaPackage from 'kafkajs';
import {kafkaNamespace} from '../packages/server/src/kafka-namespace';
import {kafkaDurability,assertKafkaDurability} from '../packages/server/src/kafka-durability';
const policy=kafkaDurability(process.env),namespace=kafkaNamespace(process.env);
const client=new kafkaPackage.Kafka({clientId:'ccpanel-durability-check',brokers:(process.env.KAFKA_BROKERS||'127.0.0.1:9092').split(','),logLevel:kafkaPackage.logLevel.NOTHING,requestTimeout:8000,retry:{retries:3}}),admin=client.admin();
try{
  await admin.connect();await assertKafkaDurability(admin,[namespace.events,namespace.dlq],policy);
  console.log(JSON.stringify({passed:true,mode:'readonly_startup_snapshot',checked_topics:2,required_replication_factor:policy.replicationFactor,required_min_in_sync_replicas:policy.minInSyncReplicas}));
}finally{await admin.disconnect();}
