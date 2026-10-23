// Run after pnpm build. Real child, HTTP stall, clock and durable outbox.
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {createServer} from 'node:http';
import {once} from 'node:events';
import {mkdtemp,readFile,unlink,rmdir} from 'node:fs/promises';
import {existsSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve,dirname} from 'node:path';
import {pathToFileURL} from 'node:url';
const dir=await mkdtemp(join(tmpdir(),'ccpanel-real-timeout-')),outbox=join(dir,'outbox.json'),received=[];let accept=false;
const server=createServer(async(req,res)=>{let bytes=0;const chunks=[];for await(const chunk of req){bytes+=chunk.length;if(bytes>49152){res.writeHead(413);return res.end();}chunks.push(chunk);}const body=JSON.parse(Buffer.concat(chunks));received.push(body);if(accept){res.writeHead(202,{'content-type':'application/json'});res.end(JSON.stringify({accepted_event_ids:body.events.map(e=>e.event_id)}));}/* first request deliberately has no response */});
server.listen(0,'127.0.0.1');await once(server,'listening');
let child,exited,watchdog,killed=false;
try{
 const entry=pathToFileURL(resolve('packages/sdk-node/dist/index.js')).href,endpoint=`http://127.0.0.1:${server.address().port}/v1/batch`;
 const source=`import{createNodeAnalytics}from ${JSON.stringify(entry)};const errors=[];const c=createNodeAnalytics({endpoint:process.argv[1],outboxPath:process.argv[2],projectId:'real-timeout-fixture',writeKey:'synthetic-public-key',consent:true,flushInterval:60000,onError:code=>errors.push(code)});await c.flush();const queued=await c.track('unanswered_http',{synthetic:true});const started=performance.now();let outcome;try{outcome=await c.flush();}catch(e){outcome={error:e.message};}const flushElapsedMs=performance.now()-started;c.destroy();await c.shutdown();console.log(JSON.stringify({event_id:queued.event_id,queued:queued.status,outcome,flushElapsedMs,errors}));`;
 child=spawn(process.execPath,['--input-type=module','-e',source,endpoint,outbox],{stdio:['ignore','pipe','pipe']});exited=once(child,'exit');
 let stdout='',stderr='';child.stdout.on('data',chunk=>{stdout=(stdout+chunk).slice(-8192);});child.stderr.on('data',chunk=>{stderr=(stderr+chunk).slice(-8192);});
 watchdog=setTimeout(()=>{killed=true;child.kill('SIGKILL');},25000);
 const [code,signal]=await exited;clearTimeout(watchdog);assert.equal(killed,false,'child required forced termination');assert.equal(code,0,`child failed (${stderr.length} diagnostic characters)`);assert.equal(signal,null);
 const result=JSON.parse(stdout.trim());assert.equal(result.queued,'queued-persistent');assert.equal(result.outcome.error,'flush_timeout');assert.ok(result.errors.includes('flush_timeout'));assert.ok(result.flushElapsedMs>=9000&&result.flushElapsedMs<16000,'deadline outside scheduling tolerance');
 assert.equal(received.length,1,'timeout caused another request');assert.equal(received[0].events[0].event_id,result.event_id);
 const rows=JSON.parse(await readFile(outbox,'utf8')).rows;assert.equal(rows.length,1);assert.equal(rows[0].event.event_id,result.event_id);assert.equal(existsSync(outbox+'.lock'),false);
 const retryDelay=Math.max(0,rows[0].next-Date.now());assert.ok(retryDelay<=1500,'unexpected first retry delay');await new Promise(r=>setTimeout(r,retryDelay+25));
 accept=true;
 const recoverySource=`import{createNodeAnalytics}from ${JSON.stringify(entry)};const c=createNodeAnalytics({endpoint:process.argv[1],outboxPath:process.argv[2],projectId:'real-timeout-fixture',writeKey:'synthetic-public-key',consent:true,flushInterval:60000});const flushed=await c.flush(),status=await c.getQueueStatus();await c.shutdown();console.log(JSON.stringify({flushed,pending:status.count}));`;
 child=spawn(process.execPath,['--input-type=module','-e',recoverySource,endpoint,outbox],{stdio:['ignore','pipe','pipe']});exited=once(child,'exit');stdout='';stderr='';
 child.stdout.on('data',chunk=>{stdout=(stdout+chunk).slice(-8192);});child.stderr.on('data',chunk=>{stderr=(stderr+chunk).slice(-8192);});
 watchdog=setTimeout(()=>{killed=true;child.kill('SIGKILL');},15000);
 const [recoveryCode,recoverySignal]=await exited;clearTimeout(watchdog);assert.equal(killed,false,'recovery required forced termination');assert.equal(recoveryCode,0,`recovery child failed (${stderr.length} diagnostic characters)`);assert.equal(recoverySignal,null);
 const recovery=JSON.parse(stdout.trim());assert.equal(recovery.flushed.confirmed,1);assert.equal(recovery.pending,0);assert.equal(received.length,2);assert.deepEqual(received[1].events,received[0].events,'restart changed full event payload');assert.equal(JSON.parse(await readFile(outbox,'utf8')).rows.length,0);assert.equal(existsSync(outbox+'.lock'),false);
 console.log(JSON.stringify({passed:true,real_child_exit:true,forced_termination:false,real_http_stall:true,clock_mocked:false,fetch_mocked:false,flushElapsedMs:Math.round(result.flushElapsedMs),initial_requests:1,recovery_requests:1,requests:received.length,unconfirmed_file_event_retained:true,restarted_child_confirmed:true,restart_payload_identical:true,outbox_empty_after_202:true,stable_id:true,lock_released_after_shutdown:true}));
}finally{
 clearTimeout(watchdog);if(child&&child.exitCode===null&&child.signalCode===null){child.kill('SIGKILL');await exited;}
 const closed=once(server,'close');server.close();server.closeAllConnections();await closed;
 assert.equal(dirname(resolve(dir)),resolve(tmpdir()));assert.ok(dir.includes('ccpanel-real-timeout-'));
 for(const name of ['outbox.json','outbox.json.tmp','outbox.json.lock'])try{await unlink(join(dir,name));}catch(e){if(e.code!=='ENOENT')throw e;}
 await rmdir(dir);
}
