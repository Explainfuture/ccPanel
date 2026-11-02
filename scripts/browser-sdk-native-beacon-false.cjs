async page=>{
 const origin='http://127.0.0.1:5196',project='native177-false185-'+Date.now();
 const assert=(ok,message)=>{if(!ok)throw Error(message);};await page.goto(origin+'/');
 const queued=await page.evaluate(async project=>{const{createAnalytics}=await import('/sdk.js'),c=createAnalytics({endpoint:location.origin+'/v1/native',projectId:project,writeKey:'synthetic-public-key',consent:true,flushInterval:60000});window.sdk185=c;await c.flush();const r=await c.track('beacon_quota_probe',{synthetic:true});window.hold185=fetch('/hold',{method:'POST',body:'x'.repeat(65536),keepalive:true}).then(r=>r.text());return r;},project);
 assert(queued.status==='queued-persistent','not persistent');
 try{
  await page.waitForFunction(async()=> (await(await fetch('/hold-state')).json()).ready,null,{timeout:5000});
  const blocked=await page.evaluate(async project=>({result:window.sdk185.beacon(),status:await window.sdk185.getQueueStatus(),receipts:(await(await fetch('/receipts?project='+project)).json()).receipts}),project);
  assert(blocked.result===false,'native beacon was not rejected by occupied keepalive quota');assert(blocked.status.count===1&&blocked.receipts.length===0,'false beacon lost event or produced receipt');
  await page.evaluate(async()=>{await fetch('/release-hold');await window.hold185;});
  await page.reload();
  const restored=await page.evaluate(async project=>{const{createAnalytics}=await import('/sdk.js'),c=createAnalytics({endpoint:location.origin+'/v1/native',projectId:project,writeKey:'synthetic-public-key',consent:true,flushInterval:60000});const flushed=await c.flush(),status=await c.getQueueStatus();c.destroy();const receipts=(await(await fetch('/receipts?project='+project)).json()).receipts;localStorage.clear();return{flushed,status,receipts};},project);
  assert(restored.flushed.confirmed===1&&restored.status.count===0&&restored.receipts.length===1,'reload did not confirm one event');assert(restored.receipts[0].events[0].event_id===queued.event_id&&restored.receipts[0].mode==='cors','recovery replaced original ID or used exit transport');
  return{passed:true,browser:await page.context().browser().version(),native_beacon_return:false,occupied_keepalive_bytes:65536,real_indexeddb:true,retained_after_false:true,reload_exact_id:true,ordinary_202_cleared:true,real_http_receipts:1,navigator_mocked:false,network_intercepted:false,clock_mocked:false,full_device_matrix:false};
 }finally{await page.evaluate(async()=>{await fetch('/release-hold');window.sdk185?.destroy();}).catch(()=>{});}
}
