async page=>{
  const origin='http://127.0.0.1:5195',project='pagehide176-'+Date.now(),batches=[];
  const assert=(ok,message)=>{if(!ok)throw Error(message);};
  await page.unroute('**/v1/sdk176');
  await page.context().unroute('**/v1/sdk176');
  let accept=false;
  await page.context().route('**/v1/sdk176',async route=>{
    const body=route.request().postDataJSON();batches.push(body);
    await route.fulfill(accept?{status:202,contentType:'application/json',body:JSON.stringify({accepted_event_ids:body.events.map(e=>e.event_id)})}:{status:503,body:''});
  });
  await page.goto(origin+'/');
  const original=await page.evaluate(async({origin,project})=>{
    const {createAnalytics}=await import(origin+'/packages/sdk-browser/dist/index.js');
    const c=createAnalytics({endpoint:origin+'/v1/sdk176',projectId:project,writeKey:'synthetic-public-key',consent:true,flushInterval:60000});window.sdk176=c;await c.flush();
    sessionStorage.removeItem('sdk176-pagehide');sessionStorage.removeItem('sdk176-beacon');
    const nativeBeacon=navigator.sendBeacon.bind(navigator);
    navigator.sendBeacon=(url,body)=>{const queued=nativeBeacon(url,body);sessionStorage.setItem('sdk176-beacon',JSON.stringify({queued,bytes:body.size}));return queued;};
    window.addEventListener('pagehide',e=>sessionStorage.setItem('sdk176-pagehide',JSON.stringify({trusted:e.isTrusted,persisted:e.persisted})),{once:true});
    const result=await c.track('navigation_click',{button:'next'},{event_time:new Date(Date.now()-60000).toISOString()});
    return{result,status:await c.getQueueStatus()};
  },{origin,project});
  assert(original.result.status==='queued-persistent'&&original.status.count===1,'event was not persisted before navigation');
  await page.goto(origin+'/?sdk176=next');
  const exitObserved=batches.length;
  const persisted=await page.evaluate(async project=>{
    const marker=JSON.parse(sessionStorage.getItem('sdk176-pagehide'));
    const databases=await indexedDB.databases(),name=databases.find(d=>d.name.includes(project)&&d.name.endsWith(':queue'))?.name;
    if(!name)throw Error('persistent queue missing after navigation');
    const db=await new Promise((resolve,reject)=>{const r=indexedDB.open(name);r.onsuccess=()=>resolve(r.result);r.onerror=()=>reject(r.error);});
    const rows=await new Promise((resolve,reject)=>{const r=db.transaction('events').objectStore('events').getAll();r.onsuccess=()=>resolve(r.result);r.onerror=()=>reject(r.error);});db.close();return{marker,rows,beacon:JSON.parse(sessionStorage.getItem('sdk176-beacon'))};
  },project);
  assert(persisted.marker?.trusted===true,'navigation did not dispatch a trusted pagehide');
  assert(persisted.beacon?.queued===true,'native pagehide did not queue beacon');
  assert(persisted.rows.length===1&&persisted.rows[0].event.event_id===original.result.event_id,'exit transport deleted or changed unconfirmed IndexedDB event');
  accept=true;
  const recovered=await page.evaluate(async({origin,project})=>{
    const {createAnalytics}=await import(origin+'/packages/sdk-browser/dist/index.js');
    const c=createAnalytics({endpoint:origin+'/v1/sdk176',projectId:project,writeKey:'synthetic-public-key',consent:true,flushInterval:60000});await c.flush();const status=await c.getQueueStatus();c.destroy();localStorage.clear();sessionStorage.removeItem('sdk176-pagehide');sessionStorage.removeItem('sdk176-beacon');return status;
  },{origin,project});
  assert(recovered.count===0&&batches.length===exitObserved+1,'ordinary exact acknowledgement did not drain the recovered event');
  assert(JSON.stringify([persisted.rows[0].event])===JSON.stringify(batches[batches.length-1].events),'navigation recovery did not preserve exact payload');
  await page.context().unroute('**/v1/sdk176');
  return{passed:true,scope:'native lifecycle and unconfirmed persistence/recovery only',exit_delivery_proven:exitObserved>0,browser:await page.context().browser().version(),trusted_pagehide:true,pagehide_persisted:persisted.marker.persisted,real_indexeddb:true,actual_navigation:true,unconfirmed_event_retained:true,same_id_payload_on_recovery:true,ordinary_202_cleared:true,native_beacon_queued:true,exit_requests_observed:exitObserved,requests:batches.length,transport:'Playwright route 503/202',os_close_or_kill:false,full_device_matrix:false};
}
