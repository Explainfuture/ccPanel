async page=>{
 const origin='http://127.0.0.1:5196',project='native177-idb187-'+Date.now(),errors=[];
 const onError=e=>errors.push(e.message);page.on('pageerror',onError);const cdp=await page.context().newCDPSession(page);
 const assert=(ok,message)=>{if(!ok)throw Error(message);};await page.goto(origin+'/');
 try{
  await page.evaluate(async project=>{const{createAnalytics}=await import('/sdk.js');window.errors187=[];window.sdk187=createAnalytics({endpoint:location.origin+'/v1/native',projectId:project,writeKey:'synthetic-public-key',consent:true,flushInterval:60000,onError:code=>window.errors187.push(code)});await window.sdk187.flush();},project);
  await cdp.send('Storage.clearDataForOrigin',{origin,storageTypes:'indexeddb'});
  const fallback=await page.evaluate(async()=>{const result=await window.sdk187.track('native_storage_probe',{text:'x'.repeat(1024)});return{result,status:await window.sdk187.getQueueStatus(),errors:window.errors187};});
  assert(fallback.result.status==='queued-memory'&&fallback.status.count===1&&fallback.status.memory_count===1,'native connection invalidation did not produce bounded memory fallback');assert(fallback.errors.includes('storage_error'),'storage failure not observable');
  const recovered=await page.evaluate(async project=>{const c=window.sdk187,flushed=await c.flush(),status=await c.getQueueStatus();c.destroy();const receipts=(await(await fetch('/receipts?project='+project)).json()).receipts;const persistentDatabase=(await indexedDB.databases()).some(d=>d.name.includes(project)&&d.name.endsWith(':queue'));localStorage.clear();return{flushed,status,receipts,persistentDatabase};},project);
  assert(recovered.persistentDatabase&&recovered.flushed.confirmed===1&&recovered.status.count===0&&recovered.status.storage==='persistent-with-memory-fallback','persistent recovery did not confirm');assert(recovered.receipts.length===1&&recovered.receipts[0].events[0].event_id===fallback.result.event_id,'fallback recovery replaced ID');assert(errors.length===0,'unhandled page errors: '+errors.join(';'));
  return{passed:true,browser:await page.context().browser().version(),native_indexeddb_cleared:true,indexeddb_mocked:false,network_intercepted:false,fallback:'queued-memory',storage_error_observed:true,restored_persistent_store:true,same_id:true,ordinary_202_cleared:true,unhandled_page_errors:errors.length,full_device_matrix:false};
 }finally{await cdp.send('Storage.clearDataForOrigin',{origin,storageTypes:'indexeddb'}).catch(()=>{});await page.evaluate(()=>window.sdk187?.destroy()).catch(()=>{});await cdp.detach();page.off('pageerror',onError);}
}
