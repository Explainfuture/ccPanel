async page=>{
  const origin='http://127.0.0.1:5196',project='native177-'+Date.now();
  const assert=(ok,message)=>{if(!ok)throw Error(message);};
  await page.goto(origin+'/');
  const original=await page.evaluate(async({origin,project})=>{
    const {createAnalytics}=await import(origin+'/sdk.js');
    const c=createAnalytics({endpoint:origin+'/v1/native',projectId:project,writeKey:'synthetic-public-key',consent:true,flushInterval:60000});window.sdk177=c;await c.flush();
    sessionStorage.removeItem('sdk177-pagehide');window.addEventListener('pagehide',e=>sessionStorage.setItem('sdk177-pagehide',JSON.stringify({trusted:e.isTrusted,persisted:e.persisted})),{once:true});
    const result=await c.track('native_navigation',{button:'next'},{event_time:new Date(Date.now()-60000).toISOString()});return{result,status:await c.getQueueStatus()};
  },{origin,project});
  assert(original.result.status==='queued-persistent'&&original.status.count===1,'event not persisted');
  await page.goto(origin+'/?next=1');
  await page.waitForFunction(async project=>{const r=await fetch('/receipts?project='+project);return(await r.json()).receipts.length>=1;},project,{timeout:10000});
  const before=await page.evaluate(async project=>{
    const receipts=(await(await fetch('/receipts?project='+project)).json()).receipts,marker=JSON.parse(sessionStorage.getItem('sdk177-pagehide'));
    const name=(await indexedDB.databases()).find(d=>d.name.includes(project)&&d.name.endsWith(':queue'))?.name;
    if(!name)throw Error('queue missing');const db=await new Promise((resolve,reject)=>{const r=indexedDB.open(name);r.onsuccess=()=>resolve(r.result);r.onerror=()=>reject(r.error);});
    const rows=await new Promise((resolve,reject)=>{const r=db.transaction('events').objectStore('events').getAll();r.onsuccess=()=>resolve(r.result);r.onerror=()=>reject(r.error);});db.close();return{receipts,marker,rows};
  },project);
  assert(before.marker?.trusted===true&&before.receipts.length===1&&before.receipts[0].mode==='no-cors','native pagehide did not produce exactly one receipt');
  assert(before.rows.length===1&&before.rows[0].event.event_id===original.result.event_id,'beacon 202 wrongly cleared persistent event');
  assert(JSON.stringify(before.receipts[0].events)===JSON.stringify([before.rows[0].event]),'received beacon differs from persisted event');
  const final=await page.evaluate(async({origin,project})=>{
    const {createAnalytics}=await import(origin+'/sdk.js');const c=createAnalytics({endpoint:origin+'/v1/native',projectId:project,writeKey:'synthetic-public-key',consent:true,flushInterval:60000});await c.flush();const status=await c.getQueueStatus();c.destroy();const receipts=(await(await fetch('/receipts?project='+project)).json()).receipts;localStorage.clear();sessionStorage.removeItem('sdk177-pagehide');return{status,receipts};
  },{origin,project});
  assert(final.status.count===0&&final.receipts.length===2&&final.receipts[1].mode==='cors','ordinary recovery did not confirm and clear');
  assert(JSON.stringify(final.receipts[0].events)===JSON.stringify(final.receipts[1].events),'retry changed ID or payload');
  return{passed:true,browser:await page.context().browser().version(),trusted_pagehide:true,pagehide_persisted:before.marker.persisted,real_indexeddb:true,native_http_receipt:true,exit_receipts:1,recovery_receipts:1,modes:final.receipts.map(r=>r.mode),beacon_202_did_not_clear:true,exact_retry:true,ordinary_202_cleared:true,transport:'real loopback HTTP, no route interception',kafka_pg:false,full_device_matrix:false};
}
