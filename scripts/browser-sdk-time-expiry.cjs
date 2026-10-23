async page=>{
  const assert=(ok,message)=>{if(!ok)throw Error(message);};
  const origin='http://127.0.0.1:5195',project='browser175-'+Date.now(),batches=[];
  let fail=true;
  await page.unroute('**/v1/sdk175');
  await page.route('**/v1/sdk175',async route=>{
    const body=route.request().postDataJSON();batches.push(body);
    await route.fulfill(fail?{status:503,headers:{'retry-after':'60'},body:''}:{status:202,contentType:'application/json',body:JSON.stringify({accepted_event_ids:body.events.map(e=>e.event_id)})});
  });
  await page.goto(origin+'/');
  const created=await page.evaluate(async({origin,project})=>{
    const {createAnalytics}=await import(origin+'/packages/sdk-browser/dist/index.js');
    const c=createAnalytics({endpoint:origin+'/v1/sdk175',projectId:project,writeKey:'synthetic-public-key',consent:true,flushInterval:60000});window.sdk175=c;await c.flush();
    const time=new Date(Date.now()-86400000).toISOString(),options={event_time:time},pending=c.track('business_paid',{amount:5},options);options.event_time=new Date().toISOString();const result=await pending;await c.flush();
    return{time,id:result.event_id,status:result.status,queue:await c.getQueueStatus()};
  },{origin,project});
  assert(created.status==='queued-persistent'&&created.queue.count===1,'real IndexedDB did not retain failed event');
  assert(batches.length===1&&batches[0].events[0].event_time===created.time,'caller mutation rewrote business time');
  fail=false;
  await page.reload();
  const restored=await page.evaluate(async({origin,project})=>{
    const realNow=Date.now.bind(Date);window.sdk175Now=realNow()+61000;Date.now=()=>window.sdk175Now;
    const {createAnalytics}=await import(origin+'/packages/sdk-browser/dist/index.js');
    const c=createAnalytics({endpoint:origin+'/v1/sdk175',projectId:project,writeKey:'synthetic-public-key',consent:true,flushInterval:60000});window.sdk175=c;await c.flush();return c.getQueueStatus();
  },{origin,project});
  assert(restored.count===0&&batches.length===2,'reload did not drain real IndexedDB');
  assert(JSON.stringify(batches[0].events)===JSON.stringify(batches[1].events),'recovery changed ID/time/payload');
  const queued=await page.evaluate(async()=>{
    const c=window.sdk175;await c.track('old_exit',{}, {event_time:new Date(Date.now()).toISOString()});window.sdk175Now+=7*86400000+1;
    const fresh=await c.track('fresh_exit',{}, {event_time:new Date(Date.now()).toISOString()});return{fresh:fresh.event_id,status:await c.getQueueStatus()};
  });
  assert(queued.status.count===2,'mixed queue absent');
  const request=page.waitForRequest(r=>r.url()===origin+'/v1/sdk175');
  const beacon=await page.evaluate(()=>({first:window.sdk175.beacon(),second:window.sdk175.beacon()}));
  const exitBody=(await request).postDataJSON();
  assert(beacon.first===true&&beacon.second===false,'beacon hidden-cycle bound failed');
  assert(exitBody.events.length===1&&exitBody.events[0].event_id===queued.fresh,'expired exit event sent');
  const beforeSweep=await page.evaluate(()=>window.sdk175.getQueueStatus());
  assert(beforeSweep.count===2&&beforeSweep.counters.expired===0,'beacon incorrectly confirmed or counted expiry');
  const final=await page.evaluate(async()=>{const c=window.sdk175;await c.flush();const status=await c.getQueueStatus();c.destroy();return{status,afterDestroy:c.beacon()};});
  assert(final.status.count===0&&final.status.counters.expired===1&&!final.afterDestroy,'sweep or destroyed-client invariant failed');
  await page.evaluate(async()=>{localStorage.clear();const databases=await indexedDB.databases();await Promise.all(databases.map(db=>new Promise((resolve,reject)=>{const r=indexedDB.deleteDatabase(db.name);r.onsuccess=resolve;r.onerror=reject;r.onblocked=resolve;})));});
  return {passed:true,browser:await page.context().browser().version(),real_indexeddb:true,real_sendBeacon:true,transport:'Playwright route interception',clock:'Date.now advanced explicitly',business_time_snapshot:true,reload_same_event:true,expired_exit_filtered:true,beacon_not_confirmation:true,expiry_count:final.status.counters.expired,full_device_matrix:false};
}

