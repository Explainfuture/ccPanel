// Run after pnpm build. Exercises the actual distributable in separate processes.
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {mkdtemp,readFile,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join,resolve,dirname} from 'node:path';
import {pathToFileURL} from 'node:url';
import {once} from 'node:events';
import {FileQueue} from '../packages/sdk-node/dist/index.js';
const dir=await mkdtemp(join(tmpdir(),'ccpanel-node-crash-')),path=join(dir,'outbox.json');
const event={event_id:crypto.randomUUID(),event_name:'crash_recovery',distinct_id:'synthetic-user',event_time:new Date().toISOString(),properties:{test:true},context:{},schema_version:1};
let child,queue;
try{
  const entry=pathToFileURL(resolve('packages/sdk-node/dist/index.js')).href;
  const source=`import {FileQueue} from ${JSON.stringify(entry)};const q=new FileQueue(process.argv[1]);await q.add(JSON.parse(process.argv[2]),1000,100000);process.stdout.write('CCPANEL_PERSISTED');setInterval(()=>{},1000);`;
  child=spawn(process.execPath,['--input-type=module','-e',source,path,JSON.stringify(event)],{stdio:['ignore','pipe','pipe']});
  let timer;
  await new Promise((ok,no)=>{
    let text='';timer=setTimeout(()=>no(Error('child_persistence_timeout')),10000);
    child.stdout.on('data',chunk=>{text+=chunk;if(text.includes('CCPANEL_PERSISTED'))ok();});
    child.once('error',no);child.once('exit',()=>no(Error('child_exited_before_persistence')));
  }).finally(()=>clearTimeout(timer));
  const before=await readFile(path,'utf8');assert.equal(JSON.parse(before).rows[0].event.event_id,event.event_id);
  const competing=new FileQueue(path);await assert.rejects(()=>competing.stats(),/outbox_already_open/);await assert.rejects(()=>competing.close(),/outbox_already_open/);
  const terminated=once(child,'exit');assert.equal(child.kill('SIGKILL'),true);await terminated;
  queue=new FileQueue(path);assert.equal((await queue.stats()).count,1);assert.equal((await queue.peek(20))[0].event.event_id,event.event_id);
  assert.equal(await readFile(path,'utf8'),before,'reclaim alone does not rewrite persisted events');
  const claimed=await queue.claim(Date.now(),20,100000,'after-crash');assert.equal(claimed.length,1);await queue.finish([event.event_id],'after-crash');await queue.close();queue=undefined;
  queue=new FileQueue(path);assert.equal((await queue.stats()).count,0);await queue.close();queue=undefined;
  console.log(JSON.stringify({passed:true,built_package:true,live_owner_denied:true,forced_child_exit:true,dead_owner_reclaimed:true,event_id_preserved:true,confirmed_removal_persisted:true}));
}finally{
  if(child&&child.exitCode===null&&child.signalCode===null){const exit=once(child,'exit');child.kill('SIGKILL');await exit;}
  await queue?.close().catch(()=>{});
  assert.equal(dirname(dir),tmpdir());assert.ok(dir.includes('ccpanel-node-crash-'));await rm(dir,{recursive:true,force:true});
}
