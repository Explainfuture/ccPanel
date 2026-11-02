// Real HTTP/Kafka/PG integration with the shared SDK in its memory queue mode.
// The HTTPS logical endpoint is bridged to a private container URL by fetch;
// responses and retries are never mocked. No browser durability claim is made.
import assert from 'node:assert/strict';
import {randomUUID,randomBytes} from 'node:crypto';
import {readFileSync,writeFileSync} from 'node:fs';
import {pool} from '../packages/server/src/db';
import {digest} from '../packages/server/src/security';
import {Analytics} from '../packages/sdk-browser/src/index';
import {MemoryQueue} from '../packages/sdk-browser/src/queue';
const mode=process.argv[2],base=process.env.TEST_API!;
const sleep=(ms:number)=>new Promise(r=>setTimeout(r,ms));
async function until(fn:()=>Promise<boolean>,label:string,ms=90000){const end=Date.now()+ms;while(Date.now()<end){if(await fn())return;await sleep(300);}throw new Error('pressure_test_timeout:'+label);}
try{
  assert.match((await pool.query('SELECT current_database() AS name')).rows[0].name,/^ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$/);
  if(mode==='create'){
    assert.equal((await pool.query('SELECT count(*)::int AS n FROM tenants')).rows[0].n,0);
    const fixture={tenant:randomUUID(),project:randomUUID(),key:randomBytes(24).toString('hex')};
    await pool.query('INSERT INTO tenants VALUES($1,$2)',[fixture.tenant,'PRESSURE LIVE TEMP']);
    await pool.query('INSERT INTO projects(id,tenant_id,name) VALUES($1,$2,$3)',[fixture.project,fixture.tenant,'PRESSURE LIVE TEMP']);
    await pool.query('INSERT INTO write_keys(id,tenant_id,project_id,key_hash,label) VALUES($1,$2,$3,$4,$5)',[randomUUID(),fixture.tenant,fixture.project,digest(fixture.key),'TEMP']);
    console.log(JSON.stringify(fixture));
  }else if(mode==='verify'){
    const f=JSON.parse(readFileSync('/control/fixture.json','utf8'));
    assert.equal((await pool.query("SELECT 1 FROM projects p JOIN tenants t ON t.id=p.tenant_id WHERE p.id=$1 AND t.id=$2 AND t.name='PRESSURE LIVE TEMP'",[f.project,f.tenant])).rowCount,1);
    await until(async()=>{try{return(await fetch(base+'/health',{signal:AbortSignal.timeout(3000)})).ok;}catch{return false;}},'private API live');
    const attempts:{at:number;status:number;id:string}[]=[];
    let retryMs=0,body='',client:Analytics|undefined;
    try{
      client=new Analytics({endpoint:'https://pressure-test.invalid/v1/batch',projectId:f.project,writeKey:f.key,consent:true,queue:new MemoryQueue(),flushInterval:1000,fetch:async(_url,init)=>{
        body=String(init?.body);const batch=JSON.parse(body);assert.equal(batch.events.length,1);
        const response=await fetch(base+'/v1/batch',init);
        attempts.push({at:Date.now(),status:response.status,id:batch.events[0].event_id});
        if(attempts.length===1){
          assert.equal(response.status,503);assert.equal((await response.clone().json()).reason,'disk_space_low');
          retryMs=Number(response.headers.get('Retry-After'))*1000;assert.ok(retryMs>=5000&&retryMs<=60000);
        }
        return response;
      }});
      await client.flush();const tracked=await client.track('pressure_live_click',{test:true});assert.equal(tracked.status,'queued-memory');
      await client.flush();assert.equal(attempts.length,1);assert.equal((await client.getQueueStatus()).count,1);
      assert.equal((await pool.query('SELECT count(*)::int AS n FROM events')).rows[0].n,0);
      await sleep(3000);assert.equal(attempts.length,1);assert.equal((await client.getQueueStatus()).count,1);
      writeFileSync('/control/blocked.json',JSON.stringify({blocked:true,retry_ms:retryMs}),{mode:0o600});
      await until(async()=>attempts.some(a=>a.status===202)&&(await client!.getQueueStatus()).count===0,'SDK retry accepted');
      await client.flush();assert.equal((await client.getQueueStatus()).count,0);
      assert.ok(attempts.every(a=>a.id===tracked.event_id));
      assert.ok(attempts[1]!.at-attempts[0]!.at>=retryMs-100,'Retry-After respected');
      const duplicate=await fetch(base+'/v1/batch',{method:'POST',headers:{'content-type':'text/plain;charset=UTF-8'},body});assert.equal(duplicate.status,202);
      await until(async()=>Number((await pool.query("SELECT value FROM ingest_metrics WHERE project_id=$1 AND metric='duplicates'",[f.project])).rows[0]?.value)>=1,'duplicate consumed');
      const counts=(await pool.query('SELECT (SELECT count(*)::int FROM events) AS raw,(SELECT count(*)::int FROM aggregation_jobs) AS aggregate')).rows[0];
      assert.deepEqual(counts,{raw:1,aggregate:1});
      assert.equal((await pool.query('SELECT blocked FROM ingest_integrity_gate WHERE id=true')).rows[0].blocked,false);
      console.log(JSON.stringify({passed:true,actual_http_sdk_kafka_pg:true,queue_mode:'memory',rejected_503:true,retained_before_ack:true,stable_event_id:true,retry_after_ms:retryMs,retry_elapsed_ms:attempts[1]!.at-attempts[0]!.at,attempt_statuses:attempts.map(a=>a.status),duplicate_acknowledged:true,raw_events:1,aggregation_jobs:1}));
    }finally{client?.destroy();}
  }else throw new Error('invalid_pressure_test_mode');
}finally{await pool.end();}
