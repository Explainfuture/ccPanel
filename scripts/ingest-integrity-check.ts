// Only a newly created disposable database; never mutates the running gate.
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { pool } from '../packages/server/src/db';
import { migrate } from '../packages/server/src/migrate';
import { IngestAdmission } from '../packages/server/src/ingest-admission';
import { refreshIngestPressure } from '../packages/server/src/ingest-runtime';
import { decodeHostTelemetry } from '../packages/server/src/host-telemetry';
try{
  const database=(await pool.query('SELECT current_database() AS name')).rows[0].name;
  assert.match(database,/^ccpanel_restore_[0-9]{14}_[a-f0-9]{8}$/);
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM tenants')).rows[0].n,0);
  const read=async()=>(await pool.query('SELECT * FROM ingest_integrity_gate WHERE id=true')).rows[0];
  assert.equal((await read()).blocked,false);
  await pool.query("UPDATE ingest_integrity_gate SET blocked=true,reason='retained_offset_gap',detected_at=clock_timestamp(),details='{}' WHERE id=true");
  const first=await read(),host=decodeHostTelemetry(readFileSync(0));
  const status=await refreshIngestPressure(new IngestAdmission(),{
    readControl:read,readHost:async()=>host,
    readKafka:async disks=>({startedAt:Date.now(),partitions:[{partition:0,low:'10',high:'10',committed:'10',ageBoundary:'10'}],disks}),
    validateProgress:async()=>{},
    persistGap:async()=>{throw new Error('existing pause must not be rewritten');},
  });
  assert.equal(status.allowed,false);assert.equal(status.reason,'retained_offset_gap');
  await migrate();assert.equal((await read()).blocked,true);assert.equal((await read()).detected_at.getTime(),first.detected_at.getTime());
  await pool.query('DELETE FROM ingest_integrity_gate WHERE id=true');await migrate();
  assert.equal((await read()).blocked,true);assert.equal((await read()).reason,'consumer_offset_unknown','a missing control row cannot silently reopen on migration');
  await assert.rejects(()=>pool.query('UPDATE ingest_integrity_gate SET blocked=true,reason=NULL WHERE id=true'),(e:unknown)=>(e as {code?:string}).code==='23514');
  console.log(JSON.stringify({passed:true,fresh_control:true,durable_pause_restored:true,migration_preserves_pause:true,missing_row_fails_closed:true,invalid_pause_rejected:true}));
}finally{await pool.end();}
