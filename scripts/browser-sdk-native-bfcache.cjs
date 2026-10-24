async page=>{
  const origin='http://127.0.0.1:5196',project='native177-bf-'+Date.now();
  const assert=(ok,message)=>{if(!ok)throw Error(message);};
  const pageErrors=[];const onError=error=>pageErrors.push(error.message);page.on('pageerror',onError);
  try{
  await page.goto(origin+'/bf-start');
  const original=await page.evaluate(async({origin,project})=>{
    const {createAnalytics}=await import(origin+'/sdk.js');
    const sdk=createAnalytics({endpoint:origin+'/v1/native',projectId:project,writeKey:'synthetic-public-key',consent:true,flushInterval:60000});window.sdkBF208=sdk;await sdk.flush();
    window.documentBF208=crypto.randomUUID();window.lifecycleBF208=[];
    const record=e=>{window.lifecycleBF208.push({type:e.type,trusted:e.isTrusted,persisted:e.persisted??null,visibility:document.visibilityState});sessionStorage.setItem('bf208-lifecycle',JSON.stringify(window.lifecycleBF208));};
    window.addEventListener('pagehide',record);window.addEventListener('pageshow',record);document.addEventListener('visibilitychange',record);
    const event=await sdk.track('native_bfcache',{synthetic:true},{event_time:new Date(Date.now()-60000).toISOString()});
    return{event,nonce:window.documentBF208,status:await sdk.getQueueStatus()};
  },{origin,project});
  assert(original.event.status==='queued-persistent'&&original.status.count===1,'initial event not persisted');
  await page.goto(origin+'/bf-next');
  await page.waitForFunction(async project=>(await(await fetch('/receipts?project='+project)).json()).receipts.length>=1,project,{timeout:10000});
  const away=await page.evaluate(async project=>{
    const lifecycle=JSON.parse(sessionStorage.getItem('bf208-lifecycle'));
    const receipts=(await(await fetch('/receipts?project='+project)).json()).receipts;
    const name=(await indexedDB.databases()).find(d=>d.name.includes(project)&&d.name.endsWith(':queue'))?.name;if(!name)throw Error('queue missing');
    const db=await new Promise((resolve,reject)=>{const r=indexedDB.open(name);r.onsuccess=()=>resolve(r.result);r.onerror=()=>reject(r.error);});
    const rows=await new Promise((resolve,reject)=>{const r=db.transaction('events').objectStore('events').getAll();r.onsuccess=()=>resolve(r.result);r.onerror=()=>reject(r.error);});db.close();return{lifecycle,receipts,rows};
  },project);
  assert(away.receipts.length===1&&away.receipts[0].mode==='no-cors','exit did not produce one native beacon');
  assert(away.rows.length===1&&away.rows[0].event.event_id===original.event.event_id,'exit ack incorrectly cleared queue');
  await page.goBack({waitUntil:'commit'});
  await page.waitForFunction(()=>window.lifecycleBF208?.some(e=>e.type==='pageshow'&&e.persisted===true),null,{timeout:3000}).catch(()=>{});
  const restored=await page.evaluate(()=>({nonce:window.documentBF208??null,lifecycle:window.lifecycleBF208??null,notRestored:performance.getEntriesByType('navigation')[0]?.notRestoredReasons?.toJSON?.()??null}));
  const persisted=restored.nonce===original.nonce&&restored.lifecycle?.some(e=>e.type==='pageshow'&&e.trusted&&e.persisted===true);
  if(!persisted)return{passed:false,precondition:'native BFCache restore not observed',browser:await page.context().browser().version(),exit:away.lifecycle,restored,product_failure_proven:false};
  // Do not call flush/reinitialize SDK after history restore. Observe its native
  // lifecycle-driven recovery well before the configured 60-second timer.
  await page.waitForFunction(async()=>window.sdkBF208&&(await window.sdkBF208.getQueueStatus()).count===0,null,{timeout:10000});
  const second=await page.evaluate(async()=>{
    const result=await window.sdkBF208.track('native_bfcache_again',{synthetic:true});return{result,status:await window.sdkBF208.getQueueStatus()};
  });
  assert(second.result.status==='queued-persistent'&&second.status.count===1&&second.result.event_id!==original.event.event_id,'second operation not a new persistent event');
  await page.goto(origin+'/bf-next');
  await page.waitForFunction(async project=>(await(await fetch('/receipts?project='+project)).json()).receipts.length===3,project,{timeout:10000});
  await page.goBack({waitUntil:'commit'});
  await page.waitForFunction(()=>window.lifecycleBF208?.filter(e=>e.type==='pageshow'&&e.trusted&&e.persisted===true).length===2,null,{timeout:3000});
  assert(await page.evaluate(nonce=>window.documentBF208===nonce,original.nonce),'second return replaced the document');
  await page.waitForFunction(async()=>(await window.sdkBF208.getQueueStatus()).count===0,null,{timeout:10000});
  const final=await page.evaluate(async project=>{
    const status=await window.sdkBF208.getQueueStatus(),receipts=(await(await fetch('/receipts?project='+project)).json()).receipts;
    const lifecycle=window.lifecycleBF208;window.sdkBF208.destroy();
    return{status,receipts,lifecycle};
  },project);
  assert(final.receipts.length===4&&JSON.stringify(final.receipts.map(r=>r.mode))===JSON.stringify(['no-cors','cors','no-cors','cors']),'two exits and recoveries did not produce exact native requests');
  assert(JSON.stringify(final.receipts[0].events)===JSON.stringify(final.receipts[1].events),'BFCache recovery changed payload or ID');
  assert(JSON.stringify(final.receipts[2].events)===JSON.stringify(final.receipts[3].events)&&final.receipts[2].events[0].event_id===second.result.event_id,'second BFCache recovery changed payload or ID');
  assert(final.lifecycle.filter(e=>e.type==='pagehide'&&e.trusted&&e.persisted===true).length===2,'two real persisted exits missing');
  assert(pageErrors.length===0,'unhandled page error');
  return{passed:true,browser:await page.context().browser().version(),native_bfcache:true,restore_cycles:2,same_document_instance:true,trusted_persisted_pageshow:true,real_indexeddb:true,exit_ack_retained:true,automatic_recovery_without_flush:true,queue_after_ack:final.status.count,modes:final.receipts.map(r=>r.mode),exact_payload_retry:true,new_operation_new_id:true,unhandled_page_errors:pageErrors.length,lifecycle:final.lifecycle,transport:'real loopback HTTP',kafka_pg:false,full_device_matrix:false};
  }finally{
    await page.evaluate(()=>{window.sdkBF208?.destroy();sessionStorage.removeItem('bf208-lifecycle');}).catch(()=>{});
    const cdp=await page.context().newCDPSession(page);
    try{await cdp.send('Storage.clearDataForOrigin',{origin,storageTypes:'indexeddb,local_storage'});}finally{await cdp.detach();}
    page.off('pageerror',onError);
  }
}
