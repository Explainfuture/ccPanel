import pg from'pg';
import{Kafka,logLevel}from'kafkajs';
import{randomUUID,randomBytes,createHash}from'node:crypto';
import{readFile,writeFile,unlink}from'node:fs/promises';
import assert from'node:assert/strict';
const file=process.env.CCPANEL_FAULT_STATE_FILE||'/tmp/ccpanel-fault-check.json',pool=new pg.Pool({connectionString:process.env.DATABASE_URL,max:1,connectionTimeoutMillis:5000});
const mode=process.argv[2],base=process.env.TEST_API||'http://127.0.0.1:3000';
const hash=v=>createHash('sha256').update(v).digest('hex');
let state;
try{
  if(mode==='prepare'){
    try{await readFile(file);throw new Error('Previous fault fixture must be cleaned first');}catch(e){if(e.code!=='ENOENT')throw e;}
    state={tenant:randomUUID(),project:randomUUID(),key:randomBytes(24).toString('hex'),events:Array.from({length:3},()=>({event_id:randomUUID(),event_name:'fault_recovery',distinct_id:'fault-check',event_time:new Date().toISOString(),properties:{test:true},context:{source:'fault-check'},schema_version:1}))};
    await pool.query('INSERT INTO tenants VALUES($1,$2)',[state.tenant,'FAULT TEST TEMP']);
    await pool.query('INSERT INTO projects(id,tenant_id,name) VALUES($1,$2,$3)',[state.project,state.tenant,'FAULT TEST TEMP']);
    await pool.query('INSERT INTO write_keys(id,tenant_id,project_id,key_hash,label) VALUES($1,$2,$3,$4,$5)',[randomUUID(),state.tenant,state.project,hash(state.key),'fault-check']);
    await writeFile(file,JSON.stringify(state),{mode:0o600});console.log('Fault fixture prepared');
  }else{
    state=JSON.parse(await readFile(file,'utf8'));
    if(mode==='send'||mode==='unavailable'){
      const event=state.events[Number(process.argv[3])-1];
      const r=await fetch(base+'/v1/batch',{method:'POST',headers:{'content-type':'text/plain'},body:JSON.stringify({write_key:state.key,schema_version:1,events:[event]}),signal:AbortSignal.timeout(25000)});
      assert.equal(r.status,mode==='send'?202:503);console.log(mode==='send'?'PASS: event acknowledged by Kafka':'PASS: unavailable broker does not return false 202');
    }else if(mode==='count'||mode==='wait'){
      const count=Number(process.argv[3]),end=Date.now()+(mode==='wait'?90000:0);
      while(true){const n=Number((await pool.query('SELECT count(*) FROM events WHERE tenant_id=$1',[state.tenant])).rows[0].count);if(n===count){console.log('PASS: unique persisted events = '+count);break;}if(Date.now()>=end)throw new Error('Unexpected persisted count '+n+' expected '+count);await new Promise(r=>setTimeout(r,500));}
    }else if(mode==='checkpoint'||mode==='noadvance'){
      const k=new Kafka({clientId:'fault-check',brokers:process.env.KAFKA_BROKERS.split(','),logLevel:logLevel.NOTHING}),admin=k.admin();await admin.connect();
      try{
        const offsets=await admin.fetchOffsets({groupId:'ccpanel-writer-v1',topics:['ccpanel.events']});
        const stable=offsets.map(t=>({topic:t.topic,partitions:t.partitions.map(p=>({partition:p.partition,offset:p.offset})).sort((a,b)=>a.partition-b.partition)}));
        if(mode==='checkpoint'){state.offsets=stable;await writeFile(file,JSON.stringify(state),{mode:0o600});console.log('Consumer offsets checkpointed');}
        else{assert.deepEqual(stable,state.offsets);console.log('PASS: database outage did not advance consumer offsets');}
      }finally{await admin.disconnect();}
    }else if(mode==='poison'){
      const k=new Kafka({clientId:'fault-check',brokers:process.env.KAFKA_BROKERS.split(','),logLevel:logLevel.NOTHING}),p=k.producer();await p.connect();
      try{await p.send({topic:'ccpanel.events',acks:-1,messages:[{key:state.project,value:JSON.stringify({tenant_id:state.tenant,project_id:state.project,received_at:new Date().toISOString(),event:{...state.events[0],event_id:'invalid-id'}})}]});}finally{await p.disconnect();}
      const end=Date.now()+25000;while(true){if((await pool.query("SELECT 1 FROM dlq_records WHERE tenant_id=$1 AND reason='invalid_event_id'",[state.tenant])).rowCount){console.log('PASS: poison event isolated in DLQ');break;}if(Date.now()>end)throw new Error('Missing poison DLQ record');await new Promise(r=>setTimeout(r,500));}
    }else if(mode==='cleanup'){
      assert.equal((await pool.query("SELECT 1 FROM tenants t JOIN projects p ON p.tenant_id=t.id WHERE t.id=$1 AND p.id=$2 AND t.name='FAULT TEST TEMP' AND p.name='FAULT TEST TEMP'",[state.tenant,state.project])).rowCount,1,'only the synthetic fault fixture can be cleaned');
      for(const table of ['dlq_records','aggregation_jobs','ingest_metrics','events','event_dedup','write_keys','projects'])await pool.query(`DELETE FROM ${table} WHERE tenant_id=$1`,[state.tenant]);await pool.query('DELETE FROM tenants WHERE id=$1',[state.tenant]);await unlink(file);console.log('Fault fixture cleaned');
    }else throw new Error('Unknown fault mode');
  }
}finally{await pool.end();}
