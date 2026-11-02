// Source-only operator. No application startup, global pool, broker writes or
// automatic recovery. All successful cleanup paths re-read the bound artifacts.
import pg from 'pg';
import kafkaPackage from 'kafkajs';
import {constants} from 'node:fs';
import {open,lstat,realpath,readdir} from 'node:fs/promises';
import {resolve,join,isAbsolute,dirname} from 'node:path';
import {fileURLToPath} from 'node:url';
import {createHash,randomUUID,randomBytes,scryptSync} from 'node:crypto';
import {kafkaNamespace} from '../packages/server/src/kafka-namespace';
import {kafkaDurability,assertKafkaDurability} from '../packages/server/src/kafka-durability';
import {pressurePolicy,type PressurePolicy} from '../packages/server/src/ingest-pressure';
import {capacityWorkloadReport} from './capacity-workload-report';

const {Kafka,logLevel}=kafkaPackage;
export type WindowDbCommand='baseline'|'status'|'create'|'revoke'|'verify'|'cleanup';
export interface WindowDbRequest{command:WindowDbCommand;directory:string;sequence?:string}
export interface WindowDbConfig{
  connectionString:string;brokers:readonly string[];namespace:{events:string;dlq:string;group:string};origin:string;expectedSchema:22;
  durability:{replicationFactor:number;minInSyncReplicas:number};pressure:PressurePolicy;
  productRoot:string;toolsDirectory:string;operatorFile:string;configurationSha256:string;
}
export interface BrokerOffsetDiagnostic{phase:'broker_offsets';protocol_code:number|null;protocol_type:'coordinator_loading'|'coordinator_unavailable'|'not_coordinator'|'other_protocol'|'unclassified';error_class:'coordinator_not_found'|'retries_exhausted'|'protocol_error'|'other'}
export class WindowDbError extends Error{diagnostic?:BrokerOffsetDiagnostic;constructor(public code:string){super(code);this.name='WindowDbError';}}
export function brokerOffsetDiagnostic(error:unknown):BrokerOffsetDiagnostic{
  let value:any=error;const error_class:BrokerOffsetDiagnostic['error_class']=({KafkaJSGroupCoordinatorNotFound:'coordinator_not_found',KafkaJSNumberOfRetriesExceeded:'retries_exhausted',KafkaJSProtocolError:'protocol_error'} as Record<string,BrokerOffsetDiagnostic['error_class']>)[value?.name]??'other';
  for(let depth=0;depth<4&&value;depth++,value=value.cause){if(Number.isInteger(value.code)&&value.code>=0&&value.code<=32767)return{phase:'broker_offsets',protocol_code:value.code,protocol_type:({14:'coordinator_loading',15:'coordinator_unavailable',16:'not_coordinator'} as Record<number,BrokerOffsetDiagnostic['protocol_type']>)[value.code]??'other_protocol',error_class};}
  return{phase:'broker_offsets',protocol_code:null,protocol_type:'unclassified',error_class};
}
const fail=(code:string):never=>{throw new WindowDbError('window_db_'+code);};
const sha=(bytes:Uint8Array|string)=>createHash('sha256').update(bytes).digest('hex');
const digest=(value:unknown):value is string=>typeof value==='string'&&/^[a-f0-9]{64}$/.test(value);
const int=(value:unknown):value is string=>typeof value==='string'&&/^(0|[1-9][0-9]{0,18})$/.test(value)&&BigInt(value)<=9223372036854775807n;
const positive=(value:unknown):value is string=>int(value)&&BigInt(value)>0n;
const uuid=(value:unknown):value is string=>typeof value==='string'&&/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/.test(value);
const object=(value:unknown):value is Record<string,any>=>!!value&&typeof value==='object'&&!Array.isArray(value);
function shape(value:unknown,keys:readonly string[]):asserts value is Record<string,any>{if(!object(value)||Object.keys(value).length!==keys.length||keys.some(key=>!Object.hasOwn(value,key)))fail('artifact_invalid');}
const canonical=(value:any):any=>Array.isArray(value)?value.map(canonical):object(value)?Object.fromEntries(Object.keys(value).sort().map(key=>[key,canonical(value[key])])):value;
const equal=(left:unknown,right:unknown)=>JSON.stringify(canonical(left))===JSON.stringify(canonical(right));
function assert(condition:unknown,code:string):asserts condition{if(!condition)fail(code);}
const clientFiles=['load-check-public.mjs','load-evidence.mjs','load-control-lease.mjs','load-stop-reader.mjs'] as const;
const monitor=new Set(['operational_samples','operational_alert_states','operational_alert_history','pressure_signal_states','pressure_signal_history','dlq_alert_states','dlq_alert_history']);
const controls=['recovery_gate','recovery_import_scopes','ingest_integrity_gate','consumer_streams','consumer_progress_audit','schema_migrations'];
const id=(name:string)=>'"'+name.replaceAll('"','""')+'"';
type Binding=Record<string,any>;
type Fixture={tenant:string;project:string;user:string;write_key_id:string;key:string;email:string;password:string;origin:string;created_at:string;run_id:string};
type Fingerprint={table:string;count:string;hash:string};
type Position={partition:number;low:string;high:string;committed:string|null};
type PgState={database:{name:string;oid:string;postmaster_started_us:string;server_version_num:string;schema:string[]};at_us:string;gates:{recovery:boolean;integrity:boolean};controls:Fingerprint[];progress:{topic:string;group:string;partition:number;next:string;hash:string}[];streams:{topic:string;group:string;partitions:number}[];monitor:Record<string,any>;fingerprints?:Fingerprint[];fingerprint_scope?:string};

function validateBinding(value:unknown):asserts value is Binding{
  shape(value,['version','kind','run_id','planned_events','product','containers','tools','namespace','configuration_sha256']);
  assert(value.version===1&&value.kind==='capacity_window_binding'&&/^[a-f0-9]{32}$/.test(value.run_id)&&value.planned_events===1800,'binding_invalid');
  shape(value.product,['release','source_commit','schema','api_sha256','worker_sha256','sdk_sha256','compose_sha256']);
  assert(value.product.release==='m4-polling-014'&&value.product.source_commit==='b9b213f2313151c1e21be30e924a9af3a1f84a82'&&value.product.schema===22&&['api_sha256','worker_sha256','sdk_sha256','compose_sha256'].every(key=>digest(value.product[key])),'binding_invalid');
  shape(value.containers,['api','worker','postgres','kafka']);
  for(const item of Object.values(value.containers)){shape(item,['id','started_at','restart_count','image_id']);assert(/^[a-f0-9]{64}$/.test(item.id)&&typeof item.started_at==='string'&&Number.isFinite(Date.parse(item.started_at))&&Number.isSafeInteger(item.restart_count)&&item.restart_count>=0&&/^sha256:[a-f0-9]{64}$/.test(item.image_id),'binding_invalid');}
  shape(value.tools,['db_operator_sha256','generator_sha256','evidence_sha256','lease_sha256','reader_sha256','sampler_sha256','controller_sha256']);assert(Object.values(value.tools).every(digest)&&digest(value.configuration_sha256),'binding_invalid');
  shape(value.namespace,['events','dlq','group']);kafkaNamespace({KAFKA_EVENTS_TOPIC:value.namespace.events,KAFKA_DLQ_TOPIC:value.namespace.dlq,KAFKA_GROUP_ID:value.namespace.group});
}

function validateFixture(value:unknown,binding:Binding):asserts value is Fixture{
  shape(value,['tenant','project','user','write_key_id','key','email','password','origin','created_at','run_id']);
  assert(['tenant','project','user','write_key_id'].every(key=>uuid(value[key]))&&/^[a-f0-9]{48}$/.test(value.key)&&/^[a-f0-9]{48}$/.test(value.password)&&value.email===`public-load-${value.user}@integration.invalid`&&value.run_id===binding.run_id&&Number.isFinite(Date.parse(value.created_at))&&typeof value.origin==='string','fixture_invalid');
}
function bound(value:any,binding:Binding,bindingSha:string,kind:string,status:string){assert(value.version===1&&value.run_id===binding.run_id&&value.binding_sha256===bindingSha&&value.kind===kind&&value.status===status&&value.capacity_verified===false&&value.source_authenticated===false,'artifact_binding_invalid');}
function pgShape(value:any,full:boolean){
  shape(value,['database','at_us','gates','controls','progress','streams','monitor',...(full?['fingerprints','fingerprint_scope']:[])]);shape(value.database,['name','oid','postmaster_started_us','server_version_num','schema']);shape(value.gates,['recovery','integrity']);
  assert(typeof value.database.name==='string'&&[value.at_us,value.database.oid,value.database.postmaster_started_us,value.database.server_version_num].every(positive)&&Array.isArray(value.database.schema)&&value.database.schema.every(positive)&&value.database.schema.at(-1)==='22'&&typeof value.gates.recovery==='boolean'&&typeof value.gates.integrity==='boolean','artifact_invalid');
  for(const rows of [value.controls,...(full?[value.fingerprints]:[])]){assert(Array.isArray(rows)&&rows.length>0&&rows.length<=1024&&new Set(rows.map((row:any)=>row.table)).size===rows.length,'artifact_invalid');for(const row of rows){shape(row,['table','count','hash']);assert(/^[a-z_][a-z0-9_]*$/.test(row.table)&&int(row.count)&&typeof row.hash==='string'&&/^(0|-?[1-9][0-9]{0,18})$/.test(row.hash),'artifact_invalid');}}
  assert(equal([...value.controls.map((row:any)=>row.table)].sort(),[...controls].sort()),'artifact_invalid');
  assert(Array.isArray(value.progress)&&value.progress.length<=1024&&Array.isArray(value.streams)&&value.streams.length<=1024,'artifact_invalid');
  for(const row of value.progress){shape(row,['topic','group','partition','next','hash']);assert(typeof row.topic==='string'&&typeof row.group==='string'&&Number.isSafeInteger(row.partition)&&row.partition>=0&&int(row.next)&&digest(row.hash),'artifact_invalid');}
  for(const row of value.streams){shape(row,['topic','group','partitions']);assert(typeof row.topic==='string'&&typeof row.group==='string'&&Number.isSafeInteger(row.partitions)&&row.partitions>=1&&row.partitions<=1024,'artifact_invalid');}
  shape(value.monitor,['operational_alert_states','pressure_signal_states','dlq_alert_states','samples','samples_fresh','pressure_expired']);for(const key of ['operational_alert_states','pressure_signal_states','dlq_alert_states']){shape(value.monitor[key],['rules','fresh','assessed_at']);assert(Array.isArray(value.monitor[key].rules)&&Array.isArray(value.monitor[key].assessed_at)&&typeof value.monitor[key].fresh==='boolean','artifact_invalid');}
  assert(Array.isArray(value.monitor.samples)&&typeof value.monitor.samples_fresh==='boolean'&&int(value.monitor.pressure_expired),'artifact_invalid');for(const sample of value.monitor.samples)shape(sample,['source','at']);
  if(full)assert(value.fingerprint_scope==='public_business_controls_excludes_seven_monitor_relations','artifact_invalid');
}
function observationShape(value:any){shape(value,['state','positions','dlq','cluster_id']);pgShape(value.state,true);assert(typeof value.cluster_id==='string'&&value.cluster_id.length>0&&value.cluster_id.length<=128&&Array.isArray(value.positions)&&value.positions.length>=1&&value.positions.length<=1024&&Array.isArray(value.dlq)&&value.dlq.length>=1&&value.dlq.length<=1024,'artifact_invalid');for(const row of value.positions){shape(row,['partition','low','high','committed']);assert(Number.isSafeInteger(row.partition)&&row.partition>=0&&int(row.low)&&int(row.high)&&(row.committed===null||int(row.committed)),'artifact_invalid');}for(const row of value.dlq){shape(row,['partition','low','high']);assert(Number.isSafeInteger(row.partition)&&row.partition>=0&&int(row.low)&&int(row.high),'artifact_invalid');}}

function metadataSame(a:any,b:any){return a.dev===b.dev&&a.ino===b.ino&&a.size===b.size&&a.mtimeNs===b.mtimeNs&&a.ctimeNs===b.ctimeNs;}
async function privateDirectory(directory:string){
  assert(process.platform!=='win32'&&isAbsolute(directory),'private_directory_required');const initial=await lstat(directory,{bigint:true});
  assert(initial.isDirectory()&&!initial.isSymbolicLink()&&initial.uid===BigInt(process.geteuid!())&&(initial.mode&0o077n)===0n&&await realpath(directory)===directory,'private_directory_required');return initial;
}
async function artifact(directory:string,name:string,maximum=1024*1024){
  assert(/^[a-z0-9][a-z0-9.-]*\.json$/.test(name),'artifact_invalid');const path=join(directory,name),before=await lstat(path,{bigint:true});
  assert(before.isFile()&&!before.isSymbolicLink()&&before.nlink===1n&&before.uid===BigInt(process.geteuid!())&&(before.mode&0o077n)===0n&&before.size>0n&&before.size<=BigInt(maximum),'artifact_invalid');
  const file=await open(path,constants.O_RDONLY|(constants.O_NOFOLLOW??0)|(constants.O_NONBLOCK??0));
  try{
    const opened=await file.stat({bigint:true});assert(metadataSame(before,opened),'artifact_changed');const bytes=Buffer.alloc(maximum+1);let at=0;
    while(at<bytes.length){const result=await file.read(bytes,at,bytes.length-at,null);if(!result.bytesRead)break;at+=result.bytesRead;}
    assert(at<=maximum&&BigInt(at)===before.size&&metadataSame(opened,await file.stat({bigint:true}))&&metadataSame(before,await lstat(path,{bigint:true})),'artifact_changed');
    const raw=bytes.subarray(0,at),text=new TextDecoder('utf-8',{fatal:true,ignoreBOM:true}).decode(raw);let value:any;try{value=JSON.parse(text);}catch{fail('artifact_invalid');}
    assert(JSON.stringify(value)===text||JSON.stringify(value)+'\n'===text,'artifact_invalid');return{value,sha256:sha(raw),bytes:at};
  }finally{await file.close();}
}
async function publish(directory:string,name:string,value:unknown){
  await privateDirectory(directory);const file=await open(join(directory,name),constants.O_CREAT|constants.O_EXCL|constants.O_WRONLY|(constants.O_NOFOLLOW??0),0o600);
  const raw=Buffer.from(JSON.stringify(value)+'\n');try{await file.writeFile(raw);await file.sync();}finally{await file.close();}
  const parent=await open(directory,constants.O_RDONLY|(constants.O_DIRECTORY??0));try{await parent.sync();}finally{await parent.close();}return sha(raw);
}
async function absent(directory:string,names:readonly string[]){for(const name of names){try{await lstat(join(directory,name));fail('output_exists');}catch(error){if((error as NodeJS.ErrnoException).code!=='ENOENT')throw error;}}}
async function fileHash(path:string){const file=await open(path,constants.O_RDONLY|(constants.O_NOFOLLOW??0));try{const stat=await file.stat();assert(stat.isFile()&&stat.size<=4*1024*1024,'source_invalid');const hash=createHash('sha256'),buffer=Buffer.alloc(65536);while(true){const read=await file.read(buffer,0,buffer.length,null);if(!read.bytesRead)break;hash.update(buffer.subarray(0,read.bytesRead));}return hash.digest('hex');}finally{await file.close();}}
async function sourceBinding(config:WindowDbConfig,binding:Binding){
  assert(config.expectedSchema===22&&equal(config.namespace,binding.namespace)&&config.configurationSha256===binding.configuration_sha256&&new URL(config.origin).protocol==='https:','source_mismatch');
  for(const [path,key]of [['dist/api.mjs','api_sha256'],['dist/worker.mjs','worker_sha256'],['dist/web/sdk/ccpanel.js','sdk_sha256'],['infra/compose.yaml','compose_sha256']])assert(await fileHash(join(config.productRoot,path!))===binding.product[key!],'source_mismatch');
  assert(await fileHash(config.operatorFile)===binding.tools.db_operator_sha256,'source_mismatch');
  for(const [index,key]of ['generator_sha256','evidence_sha256','lease_sha256','reader_sha256'].entries())assert(await fileHash(join(config.toolsDirectory,clientFiles[index]!))===binding.tools[key],'source_mismatch');
}

async function fingerprints(db:pg.Client,fixture?:Fixture,names?:readonly string[]):Promise<Fingerprint[]>{
  const tables=(await db.query(`SELECT c.relname AS name,EXISTS(SELECT 1 FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attname='tenant_id' AND NOT a.attisdropped) AS tenant
    FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relkind IN ('r','p') ORDER BY c.relname`)).rows;
  assert(tables.length>0&&tables.length<=1024,'relation_inventory_invalid');const values:Fingerprint[]=[];
  for(const table of tables){if(monitor.has(table.name)||(names&&!names.includes(table.name)))continue;let where='',params:string[]=[];
    if(fixture){if(table.name==='tenants'){where=' WHERE id<>$1::uuid';params=[fixture.tenant];}else if(table.name==='users'){where=' WHERE id<>$1::uuid';params=[fixture.user];}else if(table.name==='sessions'){where=' WHERE user_id<>$1::uuid';params=[fixture.user];}else if(table.tenant){where=' WHERE tenant_id IS DISTINCT FROM $1::uuid';params=[fixture.tenant];}}
    const row=(await db.query(`SELECT count(*)::text AS count,coalesce(bit_xor(hashtextextended(row_to_json(t)::text,0)),0)::text AS hash FROM (SELECT * FROM public.${id(table.name)}${where}) t`,params)).rows[0];values.push({table:table.name,count:row.count,hash:row.hash});
  }return values;
}

export async function readWindowPgState(db:pg.Client,full=false,fixture?:Fixture):Promise<PgState>{
  const data=(await db.query(`SELECT current_database() AS name,(SELECT oid::text FROM pg_database WHERE datname=current_database()) AS oid,
    current_setting('server_version_num') AS server_version_num,((extract(epoch FROM pg_postmaster_start_time())*1000000)::bigint)::text AS postmaster_started_us,
    ((extract(epoch FROM clock_timestamp())*1000000)::bigint)::text AS at_us,(SELECT array_agg(version::text ORDER BY version) FROM schema_migrations) AS schema`)).rows[0];
  assert(Array.isArray(data.schema)&&data.schema.at(-1)==='22'&&[data.oid,data.server_version_num,data.postmaster_started_us,data.at_us].every(positive),'schema_mismatch');
  const recovery=(await db.query('SELECT blocked FROM recovery_gate WHERE id=true')).rows,integrity=(await db.query('SELECT blocked,reason FROM ingest_integrity_gate WHERE id=true')).rows;
  assert(recovery.length===1&&integrity.length===1,'control_missing');
  const progress=(await db.query(`SELECT p.topic,p.group_id AS "group",p.partition_id AS partition,p.next_offset::text AS next,encode(sha256(convert_to(row_to_json(p)::text,'UTF8')),'hex') AS hash FROM consumer_progress p ORDER BY topic,group_id,partition_id`)).rows;
  const streams=(await db.query('SELECT topic,group_id AS "group",partition_count AS partitions FROM consumer_streams ORDER BY topic,group_id')).rows;
  const monitorState:Record<string,any>={};
  for(const [table,clock,rules]of [['operational_alert_states','evaluated_at',['api_sampling','api_server_errors','worker_failures','worker_sampling']],['pressure_signal_states','assessed_at',['ingest_integrity','kafka_backlog','storage_headroom']],['dlq_alert_states','assessed_at',['dlq_pending','dlq_retention_risk']]] as const){
    const rows=(await db.query(`SELECT rule,${clock}::text AS at,revision::text FROM ${table} ORDER BY rule`)).rows;
    assert(equal(rows.map(row=>row.rule),rules)&&rows.every(row=>positive(row.revision)&&Number.isFinite(Date.parse(row.at))),'monitor_inventory_invalid');
    monitorState[table]={rules:rows.map(row=>row.rule),fresh:rows.every(row=>{const age=Number(BigInt(data.at_us)/1000n)-Date.parse(row.at);return age>=0&&age<=90000;}),assessed_at:rows.map(row=>row.at)};
  }
  monitorState.samples=(await db.query("SELECT source,max(observed_at)::text AS at FROM operational_samples WHERE source IN ('api','worker') GROUP BY source ORDER BY source")).rows;
  assert(equal(monitorState.samples.map((row:any)=>row.source),['api','worker']),'monitor_inventory_invalid');
  monitorState.samples_fresh=monitorState.samples.every((row:any)=>{const age=Number(BigInt(data.at_us)/1000n)-Date.parse(row.at);return age>=-5000&&age<=90000;});
  monitorState.pressure_expired=(await db.query('SELECT count(*)::text AS count FROM pressure_signal_states WHERE signal_expires_at IS NULL OR signal_expires_at<clock_timestamp()')).rows[0].count;
  return{database:{name:data.name,oid:data.oid,postmaster_started_us:data.postmaster_started_us,server_version_num:data.server_version_num,schema:data.schema},at_us:data.at_us,gates:{recovery:recovery[0].blocked===false,integrity:integrity[0].blocked===false&&integrity[0].reason===null},controls:await fingerprints(db,undefined,controls),progress,streams,monitor:monitorState,...(full?{fingerprints:await fingerprints(db,fixture),fingerprint_scope:'public_business_controls_excludes_seven_monitor_relations'}:{})};
}
function admitted(state:PgState){assert(state.gates.recovery&&state.gates.integrity,'gate_closed');assert(state.monitor.samples_fresh&&['operational_alert_states','pressure_signal_states','dlq_alert_states'].every(key=>state.monitor[key].fresh),'monitor_stale');}
function sourceUnchanged(baseline:PgState,state:PgState,ns:WindowDbConfig['namespace']){
  admitted(state);assert(equal(state.database,baseline.database)&&equal(state.controls,baseline.controls)&&equal(state.streams,baseline.streams),'source_changed');
  const other=(value:PgState)=>value.progress.filter(p=>p.topic!==ns.events||p.group!==ns.group);assert(equal(other(state),other(baseline)),'other_progress_changed');
}
function businessUnchanged(before:Fingerprint[],after:Fingerprint[]){
  const left=new Map(before.filter(row=>!monitor.has(row.table)&&row.table!=='consumer_progress').map(row=>[row.table,row])),right=new Map(after.filter(row=>!monitor.has(row.table)&&row.table!=='consumer_progress').map(row=>[row.table,row]));
  for(const name of new Set([...left.keys(),...right.keys()])){const a=left.get(name),b=right.get(name);if(!a||!b){const value=a??b!;assert(/^events_[0-9]{4}_(0[1-9]|1[0-2])$/.test(name)&&value.count==='0'&&value.hash==='0','business_changed');}else assert(equal(a,b),'business_changed');}
}
async function round(db:pg.Client,admin:ReturnType<InstanceType<typeof Kafka>['admin']>,config:WindowDbConfig,fixture?:Fixture,full=false,stable=true){
  const ns=config.namespace,cluster=await admin.describeCluster(),before=await admin.fetchTopicOffsets(ns.events);let commits;
  try{commits=await admin.fetchOffsets({groupId:ns.group,topics:[ns.events],resolveOffsets:false});}catch(error){const failed=new WindowDbError('window_db_broker_offsets_unavailable');failed.diagnostic=brokerOffsetDiagnostic(error);throw failed;}
  await db.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');let state:PgState;
  try{state=await readWindowPgState(db,full,fixture);}finally{await db.query('ROLLBACK');}
  const after=await admin.fetchTopicOffsets(ns.events),dead=await admin.fetchTopicOffsets(ns.dlq);
  assert(typeof cluster.clusterId==='string'&&cluster.clusterId.length>0&&commits.length===1&&commits[0]?.topic===ns.events&&commits[0].partitions.length===after.length&&new Set(commits[0].partitions.map(p=>p.partition)).size===after.length&&before.length===after.length&&before.length>=1,'broker_observation_invalid');
  const positions:Position[]=after.map(value=>{const old=before.find(p=>p.partition===value.partition),commit=commits[0]!.partitions.find(p=>p.partition===value.partition);assert(old&&commit&&int(old.high)&&int(value.high)&&int(value.low)&&(stable?old.high===value.high:BigInt(old.high)<=BigInt(value.high)),'broker_not_stable');return{partition:value.partition,low:value.low,high:value.high,committed:commit.offset==='-1'?null:commit.offset};}).sort((a,b)=>a.partition-b.partition);
  assert(dead.length>0&&dead.every(p=>int(p.low)&&int(p.high)),'broker_observation_invalid');const dlq=dead.map(p=>({partition:p.partition,low:p.low,high:p.high})).sort((a,b)=>a.partition-b.partition);assert(dlq.every((p,index)=>p.partition===index&&BigInt(p.low)<=BigInt(p.high)),'broker_observation_invalid');return{state,positions,dlq,cluster_id:cluster.clusterId};
}
function drained(value:Awaited<ReturnType<typeof round>>,ns:WindowDbConfig['namespace'],requireEmpty=true){
  const stream=value.state.streams.filter(s=>s.topic===ns.events&&s.group===ns.group),rows=value.state.progress.filter(p=>p.topic===ns.events&&p.group===ns.group);
  assert(stream.length===1&&stream[0]!.partitions===value.positions.length&&rows.length===value.positions.length,'progress_inventory_invalid');
  for(const [index,p]of value.positions.entries()){const row=rows.find(r=>r.partition===p.partition);assert(p.partition===index&&int(p.high)&&int(p.low)&&row&&int(row.next)&&BigInt(p.low)<=BigInt(row.next)&&BigInt(row.next)<=BigInt(p.high)&&(p.committed===null?p.low==='0':int(p.committed)&&BigInt(p.committed)<=BigInt(row.next)&&BigInt(p.committed)>=BigInt(p.low)),'progress_invalid');if(requireEmpty)assert(row.next===p.high&&(p.committed===p.high||(p.committed===null&&p.high==='0'&&p.low==='0')),'not_drained');}
}

export async function inspectWindowFixture(db:pg.Client,f:Fixture,ids:readonly string[]){
  const schema=(await db.query("SELECT to_regclass('public.event_count_buckets') IS NOT NULL AND to_regclass('public.visible_event_count_buckets') IS NOT NULL AS valid")).rows[0];assert(schema.valid,'aggregate_schema_missing');
  const row=(await db.query(`WITH expected AS (SELECT unnest($3::uuid[]) AS id),raw AS MATERIALIZED(SELECT event_id AS id FROM events WHERE tenant_id=$1 AND project_id=$2),
    registered AS MATERIALIZED(SELECT event_id AS id,status FROM event_dedup WHERE tenant_id=$1 AND project_id=$2),jobs AS MATERIALIZED(
      SELECT a.event_id AS id,a.processed_at,a.last_error_code,a.processed_epoch,p.privacy_epoch FROM aggregation_jobs a JOIN projects p ON p.tenant_id=a.tenant_id AND p.id=a.project_id WHERE a.tenant_id=$1 AND a.project_id=$2)
    SELECT (SELECT count(*)::text FROM raw) AS raw,(SELECT count(DISTINCT id)::text FROM raw) AS unique_raw,(SELECT count(*)::text FROM registered) AS dedup,(SELECT count(*)::text FROM jobs) AS jobs,
      NOT EXISTS((SELECT id FROM expected EXCEPT SELECT id FROM raw) UNION ALL(SELECT id FROM raw EXCEPT SELECT id FROM expected)) AS raw_exact,
      NOT EXISTS((SELECT id FROM expected EXCEPT SELECT id FROM registered) UNION ALL(SELECT id FROM registered EXCEPT SELECT id FROM expected)) AS dedup_exact,
      NOT EXISTS((SELECT id FROM expected EXCEPT SELECT id FROM jobs) UNION ALL(SELECT id FROM jobs EXCEPT SELECT id FROM expected)) AS jobs_exact,
      (SELECT count(*)::text FROM registered WHERE status<>'stored') AS failed,
      (SELECT count(*)::text FROM jobs WHERE processed_at IS NOT NULL AND last_error_code IS NULL AND processed_epoch IS NOT DISTINCT FROM privacy_epoch) AS processed,
      (SELECT coalesce(sum(event_count),0)::text FROM visible_event_count_buckets WHERE tenant_id=$1 AND project_id=$2) AS buckets,
      (SELECT count(*)::text FROM dlq_records WHERE tenant_id=$1 OR project_id=$2) AS dlq`,[f.tenant,f.project,ids])).rows[0];
  assert([row.raw,row.unique_raw,row.dedup,row.jobs].every(value=>value===String(ids.length))&&row.raw_exact&&row.dedup_exact&&row.jobs_exact,'event_set_mismatch');assert(row.failed==='0'&&row.dlq==='0','event_outcome_invalid');
  return{...row,complete:row.processed===String(ids.length)&&row.buckets===String(ids.length)};
}

async function ownership(db:pg.Client,f:Fixture,strict=false){
  const row=(await db.query(`SELECT p.privacy_epoch,p.schema_mode,p.retention_days,p.retention_floor::text,p.origins,k.revoked_at::text,
    (SELECT count(*)::text FROM projects WHERE tenant_id=$1) AS projects,(SELECT count(*)::text FROM tenant_memberships WHERE tenant_id=$1 OR user_id=$3) AS memberships,
    (SELECT count(*)::text FROM write_keys WHERE tenant_id=$1 OR project_id=$2) AS keys
    FROM tenants t JOIN projects p ON p.tenant_id=t.id JOIN write_keys k ON k.tenant_id=t.id AND k.project_id=p.id
    JOIN tenant_memberships m ON m.tenant_id=t.id JOIN users u ON u.id=m.user_id
    WHERE t.id=$1 AND p.id=$2 AND u.id=$3 AND k.id=$4 AND k.key_hash=$5 AND k.label='capacity-external'
    AND t.name='CAPACITY EXTERNAL TEMP' AND p.name='CAPACITY EXTERNAL TEMP' AND m.role='admin' AND u.email=$6`,[f.tenant,f.project,f.user,f.write_key_id,sha(f.key),f.email])).rows;
  assert(row.length===1&&row[0].projects==='1'&&row[0].memberships==='1'&&row[0].keys==='1','ownership_mismatch');
  assert(equal(row[0].origins,[f.origin]),'ownership_mismatch');
  if(strict){for(const table of ['replay_items','replay_jobs','query_jobs','deletion_jobs','deletion_barriers','reports','dashboards','identity_links','event_schemas','tenant_invitations','company_provision_requests','platform_audit','audit_logs'])assert((await db.query(`SELECT count(*)::text AS count FROM ${table} WHERE tenant_id=$1`,[f.tenant])).rows[0].count==='0','unexpected_fixture_resource');assert((await db.query('SELECT count(*)::text AS count FROM platform_operators WHERE user_id=$1',[f.user])).rows[0].count==='0','unexpected_fixture_resource');}
  return row[0];
}

async function clientProof(directory:string,binding:Binding,bindingSha:string){
  const result=await artifact(directory,'client-result.json'),manifest=await artifact(directory,'client-manifest.json'),terminal=await artifact(directory,'client-terminal.json',32768);
  shape(result.value,['tenant','project','started_at','completed_at','duration_seconds','generation_seconds','max_generation_lag_ms','target_eps','generated','acknowledged','duplicate_requests','backlog','not_yet_visible','ingest_p95_ms','observed_visible_p95_ms','query_p95_ms','errors','passed','event_ids','workload','termination','confirmed_event_ids','artifacts','control']);
  shape(result.value.workload,['ingest_attempts','ingest_accepted_requests','ingest_failed_requests','ingest_attempted_events','ingest_body_bytes_total','ingest_body_bytes_min','ingest_body_bytes_max','query_opportunities','query_attempts','query_successes','query_failures','query_skipped_busy','visibility_requests','ingest_body_bytes_mean','query_schedule_coverage']);
  shape(result.value.termination,['state','reason','planned_events']);
  shape(manifest.value,['version','writer_pid','artifacts','control','status','termination_reason','tenant','project','planned_events','started_at','event_ids','confirmed_event_ids','completed_at']);
  shape(terminal.value,['version','kind','run_id','binding_sha256','pid','close','observed_at','result','manifest','artifacts','control']);shape(terminal.value.close,['code','signal','watchdog_fired']);shape(terminal.value.result,['sha256','bytes']);shape(terminal.value.manifest,['sha256','bytes']);
  const t=terminal.value,m=manifest.value,r=result.value,report=capacityWorkloadReport(r,binding.planned_events);
  assert(report.evidenceValid&&report.dataLoadPass&&r.workload.ingest_failed_requests===0&&r.workload.query_failures===0&&r.workload.ingest_attempted_events===r.generated+r.duplicate_requests,'client_not_successful');
  assert(t.version===1&&t.kind==='capacity_client_terminal'&&t.run_id===binding.run_id&&t.binding_sha256===bindingSha&&t.close.code===0&&t.close.signal===null&&t.close.watchdog_fired===false&&Number.isSafeInteger(t.pid)&&t.pid>0&&Number.isFinite(Date.parse(t.observed_at)),'client_terminal_invalid');
  assert(t.result.sha256===result.sha256&&t.result.bytes===result.bytes&&t.manifest.sha256===manifest.sha256&&t.manifest.bytes===manifest.bytes&&t.pid===m.writer_pid,'client_terminal_invalid');
  assert(m.version===1&&m.status==='completed'&&m.termination_reason===null&&m.planned_events===binding.planned_events&&m.tenant===r.tenant&&m.project===r.project&&m.started_at===r.started_at&&m.completed_at===r.completed_at&&equal(m.event_ids,r.event_ids)&&equal(m.confirmed_event_ids,r.confirmed_event_ids),'client_manifest_mismatch');
  const artifacts=Object.fromEntries(clientFiles.map((file,index)=>[file,binding.tools[['generator_sha256','evidence_sha256','lease_sha256','reader_sha256'][index]!] as string]));
  for(const value of [r,m,t]){shape(value.artifacts,clientFiles);shape(value.control,['mode','run_id','lease_ms']);assert(equal(value.artifacts,artifacts)&&value.control.mode==='stdin_monotonic_lease'&&value.control.run_id===binding.run_id&&value.control.lease_ms===5000,'client_source_mismatch');}
  const generated=[...r.event_ids].map((v:string)=>v.toLowerCase()).sort(),confirmed=[...r.confirmed_event_ids].map((v:string)=>v.toLowerCase()).sort();assert(equal(generated,confirmed),'client_event_set_invalid');
  return{result:r,hashes:{'client-result.json':result.sha256,'client-manifest.json':manifest.sha256,'client-terminal.json':terminal.sha256},event_ids_sha256:sha(JSON.stringify(generated))};
}

async function runNativeCapacityWindowDb(request:WindowDbRequest,config:WindowDbConfig){
  const directory=resolve(request.directory),initial=await privateDirectory(directory),bindingFile=await artifact(directory,'binding.json',16384),binding=bindingFile.value;validateBinding(binding);await sourceBinding(config,binding);
  assert(['baseline','status','create','revoke','verify','cleanup'].includes(request.command),'command_invalid');assert(request.command==='status'?positive(request.sequence):request.sequence===undefined,'command_invalid');
  let uri:URL;try{uri=new URL(config.connectionString);}catch{fail('connection_invalid');}
  assert(['postgres:','postgresql:'].includes(uri!.protocol)&&!!uri!.hostname&&!!uri!.username&&!!uri!.pathname.slice(1)&&!uri!.hash&&[...uri!.searchParams.keys()].every(key=>key==='sslmode')&&uri!.searchParams.getAll('sslmode').length<=1,'connection_invalid');
  assert(!uri!.searchParams.has('sslmode')||['disable','allow','prefer','require','verify-ca','verify-full'].includes(uri!.searchParams.get('sslmode')!),'connection_invalid');
  const db=new pg.Client({connectionString:config.connectionString,application_name:'ccpanel_window225_db',options:'-c timezone=UTC -c client_connection_check_interval=100ms',connectionTimeoutMillis:5000,statement_timeout:5000});
  let admin:ReturnType<InstanceType<typeof Kafka>['admin']>|undefined,commitAttempted=false;
  const base={version:1,run_id:binding.run_id,binding_sha256:bindingFile.sha256,capacity_verified:false,source_authenticated:false};
  const receipt=async(name:string,value:Record<string,unknown>)=>{const current=await privateDirectory(directory);assert(current.dev===initial.dev&&current.ino===initial.ino,'directory_changed');const hash=await publish(directory,name,{...base,...value});return{version:1,kind:'capacity_window_db_receipt',run_id:binding.run_id,command:request.command,status:value.status,output_file:name,output_sha256:hash,capacity_verified:false,source_authenticated:false};};
  db.on('error',()=>{});
  try{
    await db.connect();assert((await db.query('SHOW client_connection_check_interval')).rows[0]?.client_connection_check_interval==='100ms','session_invalid');
    for(const lock of [169202610,169202617])assert((await db.query('SELECT pg_try_advisory_lock_shared($1) AS held',[lock])).rows[0]?.held===true,'source_busy');
    await db.query("SET lock_timeout='200ms'");
    if(!['create','revoke'].includes(request.command)){const kafka=new Kafka({clientId:'ccpanel-window225-operator',brokers:[...config.brokers],logLevel:logLevel.NOTHING,connectionTimeout:2000,requestTimeout:3000,retry:{retries:0}});admin=kafka.admin();await admin.connect();await assertKafkaDurability(admin,[config.namespace.events,config.namespace.dlq],config.durability);}
    if(request.command==='baseline'){
      await absent(directory,['baseline.json']);
      const observation=await round(db,admin!,config,undefined,true);admitted(observation.state);drained(observation,config.namespace);
      assert((await db.query("SELECT count(*)::text AS count FROM tenants WHERE name='CAPACITY EXTERNAL TEMP'")).rows[0].count==='0','existing_fixture');
      for(const table of ['query_jobs','replay_jobs','deletion_jobs'])assert((await db.query(`SELECT count(*)::text AS count FROM ${table} WHERE status IN ('queued','running')`)).rows[0].count==='0','source_jobs_pending');
      assert((await db.query('SELECT count(*)::text AS count FROM aggregation_jobs a JOIN projects p ON p.tenant_id=a.tenant_id AND p.id=a.project_id WHERE a.processed_at IS NULL OR a.processed_epoch IS DISTINCT FROM p.privacy_epoch OR a.last_error_code IS NOT NULL')).rows[0].count==='0','source_jobs_pending');
      return await receipt('baseline.json',{kind:'capacity_window_baseline',status:'baseline_recorded',observation});
    }
    const baselineFile=await artifact(directory,'baseline.json',262144),baseline=baselineFile.value;
    shape(baseline,['version','run_id','binding_sha256','capacity_verified','source_authenticated','kind','status','observation']);bound(baseline,binding,bindingFile.sha256,'capacity_window_baseline','baseline_recorded');observationShape(baseline.observation);
    if(request.command==='status'){
      const names=await readdir(directory),sequences=names.flatMap(name=>/^status-([1-9][0-9]{0,18})\.json$/.exec(name)?.slice(1)??[]);assert(sequences.every(value=>positive(value)&&BigInt(value)<BigInt(request.sequence!)),'status_sequence_invalid');
      const observation=await round(db,admin!,config,undefined,false,false);assert(observation.cluster_id===baseline.observation.cluster_id,'source_changed');sourceUnchanged(baseline.observation.state,observation.state,config.namespace);drained(observation,config.namespace,false);return await receipt('status-'+request.sequence+'.json',{kind:'capacity_window_status',status:'status_recorded',sequence:request.sequence,observation,host_admission_evaluated:false});
    }
    if(request.command==='create'){
      await absent(directory,['fixture.json','fixture-created.json']);
      await db.query('BEGIN');try{
        const state=await readWindowPgState(db);sourceUnchanged(baseline.observation.state,state,config.namespace);
        assert((await db.query("SELECT count(*)::text AS count FROM tenants WHERE name='CAPACITY EXTERNAL TEMP'")).rows[0].count==='0','existing_fixture');
        const f:Fixture={tenant:randomUUID(),project:randomUUID(),user:randomUUID(),write_key_id:randomUUID(),key:randomBytes(24).toString('hex'),password:randomBytes(24).toString('hex'),email:'',origin:config.origin,created_at:new Date().toISOString(),run_id:binding.run_id};f.email=`public-load-${f.user}@integration.invalid`;
        const fixtureSha=await publish(directory,'fixture.json',f),salt=randomBytes(16).toString('hex');
        await db.query('INSERT INTO tenants VALUES($1,$2)',[f.tenant,'CAPACITY EXTERNAL TEMP']);await db.query('INSERT INTO users VALUES($1,$2,$3)',[f.user,f.email,salt+':'+scryptSync(f.password,salt,64).toString('hex')]);
        await db.query("INSERT INTO tenant_memberships VALUES($1,$2,'admin')",[f.tenant,f.user]);await db.query('INSERT INTO projects(id,tenant_id,name,origins) VALUES($1,$2,$3,$4)',[f.project,f.tenant,'CAPACITY EXTERNAL TEMP',[f.origin]]);await db.query('INSERT INTO write_keys(id,tenant_id,project_id,key_hash,label) VALUES($1,$2,$3,$4,$5)',[f.write_key_id,f.tenant,f.project,sha(f.key),'capacity-external']);
        const scope=await ownership(db,f);commitAttempted=true;await db.query('COMMIT');
        return await receipt('fixture-created.json',{kind:'capacity_fixture_created',status:'created',baseline_sha256:baselineFile.sha256,fixture_sha256:fixtureSha,scope});
      }catch(error){await db.query('ROLLBACK');throw error;}
    }
    const fixtureFile=await artifact(directory,'fixture.json',8192),f=fixtureFile.value;validateFixture(f,binding);assert(f.origin===config.origin,'fixture_invalid');
    let createdCandidate:Awaited<ReturnType<typeof artifact>>|undefined;
    try{createdCandidate=await artifact(directory,'fixture-created.json',16384);shape(createdCandidate.value,['version','run_id','binding_sha256','capacity_verified','source_authenticated','kind','status','baseline_sha256','fixture_sha256','scope']);bound(createdCandidate.value,binding,bindingFile.sha256,'capacity_fixture_created','created');shape(createdCandidate.value.scope,['privacy_epoch','schema_mode','retention_days','retention_floor','origins','revoked_at','projects','memberships','keys']);assert(createdCandidate.value.baseline_sha256===baselineFile.sha256&&createdCandidate.value.fixture_sha256===fixtureFile.sha256,'fixture_invalid');}
    catch(error){if(request.command!=='revoke')throw error;createdCandidate=undefined;}
    const created=createdCandidate;
    if(request.command==='revoke'){
      const source=(await db.query("SELECT current_database() AS name,(SELECT oid::text FROM pg_database WHERE datname=current_database()) AS oid,((extract(epoch FROM pg_postmaster_start_time())*1000000)::bigint)::text AS postmaster_started_us,current_setting('server_version_num') AS server_version_num,(SELECT array_agg(version::text ORDER BY version) FROM schema_migrations) AS schema")).rows[0];assert(equal(source,baseline.observation.state.database),'source_changed');
      let previous:Awaited<ReturnType<typeof artifact>>|undefined;try{previous=await artifact(directory,'key-revoked.json',16384);}catch(error){if((error as NodeJS.ErrnoException).code!=='ENOENT')throw error;}
      if(previous){shape(previous.value,['version','run_id','binding_sha256','capacity_verified','source_authenticated','kind','status','fixture_sha256','created_sha256','revoked_at']);bound(previous.value,binding,bindingFile.sha256,'capacity_key_revoked','revoked');const rows=(await db.query("SELECT revoked_at::text FROM write_keys WHERE id=$1 AND tenant_id=$2 AND project_id=$3 AND key_hash=$4 AND label='capacity-external'",[f.write_key_id,f.tenant,f.project,sha(f.key)])).rows;assert(previous.value.fixture_sha256===fixtureFile.sha256&&previous.value.created_sha256===(created?.sha256??null)&&rows.length===1&&rows[0].revoked_at===previous.value.revoked_at,'revoke_invalid');return{version:1,kind:'capacity_window_db_receipt',run_id:binding.run_id,command:request.command,status:'revoked',output_file:'key-revoked.json',output_sha256:previous.sha256,capacity_verified:false,source_authenticated:false};}
      await db.query('BEGIN');let updated;try{updated=(await db.query("UPDATE write_keys SET revoked_at=coalesce(revoked_at,clock_timestamp()) WHERE id=$1 AND tenant_id=$2 AND project_id=$3 AND key_hash=$4 AND label='capacity-external' RETURNING revoked_at::text",[f.write_key_id,f.tenant,f.project,sha(f.key)])).rows;assert(updated.length===1,'ownership_mismatch');commitAttempted=true;await db.query('COMMIT');}catch(error){await db.query('ROLLBACK');throw error;}
      return await receipt('key-revoked.json',{kind:'capacity_key_revoked',status:'revoked',fixture_sha256:fixtureFile.sha256,created_sha256:created?.sha256??null,revoked_at:updated[0].revoked_at});
    }
    assert(created,'creation_unverified');
    const proof=await clientProof(directory,binding,bindingFile.sha256);assert(proof.result.tenant===f.tenant&&proof.result.project===f.project,'client_scope_mismatch');
    const revoked=await artifact(directory,'key-revoked.json',16384);shape(revoked.value,['version','run_id','binding_sha256','capacity_verified','source_authenticated','kind','status','fixture_sha256','created_sha256','revoked_at']);bound(revoked.value,binding,bindingFile.sha256,'capacity_key_revoked','revoked');assert(revoked.value.fixture_sha256===fixtureFile.sha256&&revoked.value.created_sha256===created.sha256,'revoke_invalid');
    const inputs={...proof.hashes,'binding.json':bindingFile.sha256,'baseline.json':baselineFile.sha256,'fixture.json':fixtureFile.sha256,'fixture-created.json':created.sha256,'key-revoked.json':revoked.sha256};
    const verifyRound=async()=>{const value=await round(db,admin!,config,f,true);assert(value.cluster_id===baseline.observation.cluster_id,'source_changed');sourceUnchanged(baseline.observation.state,value.state,config.namespace);drained(value,config.namespace);businessUnchanged(baseline.observation.state.fingerprints,value.state.fingerprints!);assert(value.dlq.every(p=>p.high===baseline.observation.dlq.find((old:any)=>old.partition===p.partition)?.high)&&value.dlq.length===baseline.observation.dlq.length,'dlq_changed');const scope=await ownership(db,f,true);assert(scope.revoked_at===revoked.value.revoked_at&&equal({...scope,revoked_at:null},created.value.scope),'fixture_changed');return value;};
    if(request.command==='verify'){
      const first=await verifyRound();let exact;const until=Date.now()+15000;
      do{await db.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');try{exact=await inspectWindowFixture(db,f,proof.result.event_ids);}finally{await db.query('ROLLBACK');}if(exact.complete)break;assert(Date.now()<until,'aggregation_pending');await new Promise(done=>setTimeout(done,500));}while(true);
      const second=await verifyRound();assert(equal(first.positions.map(p=>p.high),second.positions.map(p=>p.high)),'broker_not_stable');
      const delta=second.positions.reduce((total,p)=>total+BigInt(p.high)-BigInt(baseline.observation.positions.find((old:any)=>old.partition===p.partition).high),0n);assert(delta===BigInt(proof.result.workload.ingest_attempted_events),'broker_delta_mismatch');
      return await receipt('verified.json',{kind:'capacity_window_verified',status:'verified',inputs,event_ids_sha256:proof.event_ids_sha256,first,second,exact,client_terminal_receipt_bound:true,native_close_independently_observed_by_db_helper:false});
    }
    const verifiedFile=await artifact(directory,'verified.json',262144),verified=verifiedFile.value;shape(verified,['version','run_id','binding_sha256','capacity_verified','source_authenticated','kind','status','inputs','event_ids_sha256','first','second','exact','client_terminal_receipt_bound','native_close_independently_observed_by_db_helper']);bound(verified,binding,bindingFile.sha256,'capacity_window_verified','verified');observationShape(verified.first);observationShape(verified.second);assert(equal(verified.inputs,inputs)&&verified.event_ids_sha256===proof.event_ids_sha256&&verified.client_terminal_receipt_bound===true&&verified.native_close_independently_observed_by_db_helper===false,'verify_proof_invalid');
    // Match worker ordering: checkpoint row before project privacy. NOWAIT
    // prevents this operator from queueing behind production message work.
    const commits=await admin!.fetchOffsets({groupId:config.namespace.group,topics:[config.namespace.events],resolveOffsets:false});await db.query('BEGIN');
    try{
      const locked=(await db.query('SELECT partition_id FROM consumer_progress WHERE topic=$1 AND group_id=$2 ORDER BY partition_id FOR UPDATE NOWAIT',[config.namespace.events,config.namespace.group])).rows;assert(locked.length===verified.second.positions.length,'progress_inventory_invalid');
      await db.query('SELECT pg_advisory_xact_lock(hashtextextended($1,0))',['ccpanel:privacy:'+f.project]);
      const state=await readWindowPgState(db,true,f);sourceUnchanged(baseline.observation.state,state,config.namespace);businessUnchanged(baseline.observation.state.fingerprints,state.fingerprints!);
      const exact=await inspectWindowFixture(db,f,proof.result.event_ids);assert(exact.complete,'aggregation_pending');const scope=await ownership(db,f,true);assert(scope.revoked_at===revoked.value.revoked_at&&equal({...scope,revoked_at:null},created.value.scope),'fixture_changed');
      const cluster=await admin!.describeCluster();assert(cluster.clusterId===baseline.observation.cluster_id,'source_changed');const ranges=await admin!.fetchTopicOffsets(config.namespace.events),dead=await admin!.fetchTopicOffsets(config.namespace.dlq);assert(ranges.length===verified.second.positions.length&&commits.length===1&&commits[0]?.topic===config.namespace.events,'broker_observation_invalid');
      for(const p of ranges){const expected=verified.second.positions.find((v:any)=>v.partition===p.partition),progress=state.progress.find(v=>v.topic===config.namespace.events&&v.group===config.namespace.group&&v.partition===p.partition),commit=commits[0]!.partitions.find(v=>v.partition===p.partition);assert(expected&&progress&&commit&&p.high===expected.high&&p.high===progress.next&&(commit.offset===p.high||(p.high==='0'&&commit.offset==='-1')),'not_drained');}
      assert(dead.length===baseline.observation.dlq.length&&dead.every(p=>p.high===baseline.observation.dlq.find((v:any)=>v.partition===p.partition)?.high),'dlq_changed');
      for(const table of ['event_count_buckets','aggregation_jobs','ingest_metrics','events','event_dedup']){const removed=await db.query(`DELETE FROM ${table} WHERE tenant_id=$1 AND project_id=$2`,[f.tenant,f.project]);if(['aggregation_jobs','events','event_dedup'].includes(table))assert(removed.rowCount===proof.result.generated,'event_set_mismatch');}
      await db.query('DELETE FROM sessions WHERE user_id=$1',[f.user]);assert((await db.query('DELETE FROM write_keys WHERE id=$1 AND tenant_id=$2 AND project_id=$3',[f.write_key_id,f.tenant,f.project])).rowCount===1,'ownership_mismatch');
      for(const [sql,params]of [['DELETE FROM tenant_memberships WHERE tenant_id=$1 AND user_id=$2',[f.tenant,f.user]],['DELETE FROM projects WHERE tenant_id=$1 AND id=$2',[f.tenant,f.project]],['DELETE FROM users WHERE id=$1',[f.user]],['DELETE FROM tenants WHERE id=$1',[f.tenant]]] as const)assert((await db.query(sql,[...params])).rowCount===1,'ownership_mismatch');
      commitAttempted=true;await db.query('COMMIT');
      await db.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');let after:PgState;
      try{after=await readWindowPgState(db,true);sourceUnchanged(baseline.observation.state,after,config.namespace);businessUnchanged(baseline.observation.state.fingerprints,after.fingerprints!);assert((await db.query('SELECT (SELECT count(*) FROM tenants WHERE id=$1)+(SELECT count(*) FROM users WHERE id=$2)+(SELECT count(*) FROM projects WHERE id=$3)+(SELECT count(*) FROM write_keys WHERE id=$4) AS remaining',[f.tenant,f.user,f.project,f.write_key_id])).rows[0].remaining==='0','cleanup_residual');}finally{await db.query('ROLLBACK');}
      return await receipt('cleanup.json',{kind:'capacity_window_cleaned',status:'cleaned',inputs,verified_sha256:verifiedFile.sha256,event_ids_sha256:proof.event_ids_sha256,after});
    }catch(error){await db.query('ROLLBACK');throw error;}
  }catch(error){
    if(commitAttempted)fail('publication_uncertain');
    if(request.command==='cleanup'){try{return await receipt('cleanup-deferred.json',{kind:'capacity_window_cleanup_deferred',status:'cleanup_deferred',reason:error instanceof WindowDbError?error.code:'window_db_operation_failed',project_retained:true});}catch{fail('cleanup_deferred_uncertain');}}
    if(error instanceof WindowDbError)throw error;fail('operation_failed');
  }finally{await admin?.disconnect().catch(()=>{});await db.end().catch(()=>{});}
}

export async function runCapacityWindowDb(request:WindowDbRequest,config:WindowDbConfig){try{return await runNativeCapacityWindowDb(request,config);}catch(error){if(error instanceof WindowDbError)throw error;fail('operation_failed');}}

export function windowDbConfigFromEnv(env:NodeJS.ProcessEnv):WindowDbConfig{
  for(const key of ['DATABASE_URL','KAFKA_BROKERS','PUBLIC_ORIGIN','CCPANEL_WINDOW_CONFIGURATION_SHA256','CCPANEL_WINDOW_PRODUCT_ROOT','CCPANEL_WINDOW_TOOL_ROOT'])assert(typeof env[key]==='string'&&env[key]!.length>0,'configuration_invalid');
  return{connectionString:env.DATABASE_URL!,brokers:env.KAFKA_BROKERS!.split(','),namespace:kafkaNamespace(env),origin:env.PUBLIC_ORIGIN!,expectedSchema:22,durability:kafkaDurability(env),pressure:pressurePolicy(env),productRoot:env.CCPANEL_WINDOW_PRODUCT_ROOT!,toolsDirectory:env.CCPANEL_WINDOW_TOOL_ROOT!,operatorFile:fileURLToPath(import.meta.url),configurationSha256:env.CCPANEL_WINDOW_CONFIGURATION_SHA256!};
}
