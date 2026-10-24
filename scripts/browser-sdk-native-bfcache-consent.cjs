async page=>{
  const origin='http://127.0.0.1:5196',cases=[],errors=[];
  const assert=(value,message)=>{if(!value)throw Error(message);};
  const onError=error=>errors.push(error.message);page.on('pageerror',onError);
  const rows=async project=>{
    const name=(await indexedDB.databases()).find(d=>d.name.includes(project)&&d.name.endsWith(':queue'))?.name;if(!name)throw Error('queue missing');
    const db=await new Promise((resolve,reject)=>{const r=indexedDB.open(name);r.onsuccess=()=>resolve(r.result);r.onerror=()=>reject(r.error);});
    try{return await new Promise((resolve,reject)=>{const r=db.transaction('events').objectStore('events').getAll();r.onsuccess=()=>resolve(r.result);r.onerror=()=>reject(r.error);});}finally{db.close();}
  };
  try{
    for(const regrantElsewhere of [false,true]){
      const project='native177-bf-consent-'+Date.now()+'-'+regrantElsewhere;
      await page.goto(origin+'/bf-start');
      const original=await page.evaluate(async({origin,project})=>{
        const {createAnalytics}=await import(origin+'/sdk.js');
        window.sdkBF209=createAnalytics({endpoint:origin+'/v1/native',projectId:project,writeKey:'synthetic-public-key',consent:true,flushInterval:60000});await window.sdkBF209.flush();
        window.nonceBF209=crypto.randomUUID();window.eventsBF209=[];
        for(const type of ['pagehide','pageshow'])window.addEventListener(type,e=>window.eventsBF209.push({type:e.type,trusted:e.isTrusted,persisted:e.persisted}));
        const tracked=await window.sdkBF209.track('before_withdrawal',{synthetic:true});return{tracked,nonce:window.nonceBF209,status:await window.sdkBF209.getQueueStatus()};
      },{origin,project});
      assert(original.tracked.status==='queued-persistent'&&original.status.count===1,'initial event not persistent');
      await page.goto(origin+'/bf-next');
      await page.waitForFunction(async project=>(await(await fetch('/receipts?project='+project)).json()).receipts.length===1,project,{timeout:10000});
      const before=await page.evaluate(rows,project);assert(before.length===1&&before[0].event.event_id===original.tracked.event_id,'exit beacon wrongly cleared original queue');
      const changed=await page.evaluate(async({origin,project,regrantElsewhere})=>{
        const {createAnalytics}=await import(origin+'/sdk.js');
        const other=createAnalytics({endpoint:origin+'/v1/native',projectId:project,writeKey:'synthetic-public-key',consent:false,flushInterval:60000});
        try{
          await other.flush();await other.setConsent(false);const afterWithdrawal=await other.getQueueStatus();let tracked=null;
          if(regrantElsewhere){await other.setConsent(true);await other.flush();tracked=await other.track('new_grant_other_page',{synthetic:true});}
          return{afterWithdrawal,tracked,status:await other.getQueueStatus()};
        }finally{other.destroy();}
      },{origin,project,regrantElsewhere});
      assert(changed.afterWithdrawal.count===0,'withdrawal did not clear shared persistent queue');
      const validRows=await page.evaluate(rows,project);
      assert(validRows.length===(regrantElsewhere?1:0),'new consent generation queue mismatch');
      if(regrantElsewhere)assert(changed.tracked.status==='queued-persistent'&&validRows[0].event.event_id===changed.tracked.event_id,'other-page event not persistent');
      await page.goBack({waitUntil:'commit'});
      await page.waitForFunction(()=>window.eventsBF209?.some(e=>e.type==='pageshow'&&e.trusted&&e.persisted),null,{timeout:3000}).catch(()=>{});
      const restored=await page.evaluate(()=>({nonce:window.nonceBF209??null,events:window.eventsBF209??null,notRestored:performance.getEntriesByType('navigation')[0]?.notRestoredReasons?.toJSON?.()??null}));
      if(restored.nonce!==original.nonce||!restored.events?.some(e=>e.type==='pageshow'&&e.trusted&&e.persisted))return{passed:false,precondition:'native BFCache restore not observed during consent change',regrantElsewhere,restored,product_failure_proven:false};
      // Finite negative observation: do not manually flush or grant on return.
      await page.waitForTimeout(2000);
      const blocked=await page.evaluate(async project=>{
        const receiptCount=(await(await fetch('/receipts?project='+project)).json()).receipts.length;
        const tracked=await window.sdkBF209.track('must_stay_denied',{synthetic:true});
        const beacon=window.sdkBF209.beacon(),flushed=await window.sdkBF209.flush();return{receiptCount,tracked,beacon,flushed,status:await window.sdkBF209.getQueueStatus()};
      },project);
      assert(blocked.receiptCount===1,'restored stale instance automatically sent after withdrawal');
      assert(blocked.tracked.status==='rejected'&&blocked.tracked.reason==='consent_required'&&!blocked.beacon&&blocked.flushed.confirmed===0,'old instance reused its stale grant');
      const preserved=await page.evaluate(rows,project);assert(JSON.stringify(preserved)===JSON.stringify(validRows),'stale withdrawal erased or changed a newer authorized row');
      const authorized=await page.evaluate(async regrantElsewhere=>{
        await window.sdkBF209.setConsent(true);await window.sdkBF209.flush();
        let tracked=null;if(!regrantElsewhere){tracked=await window.sdkBF209.track('new_explicit_grant',{synthetic:true});await window.sdkBF209.flush();}
        return{tracked,status:await window.sdkBF209.getQueueStatus()};
      },regrantElsewhere);
      const receipts=await page.evaluate(async project=>(await(await fetch('/receipts?project='+project)).json()).receipts,project);
      assert(receipts.length===2&&receipts[0].mode==='no-cors'&&receipts[1].mode==='cors','authorized delivery count/mode mismatch');
      const expectedId=regrantElsewhere?changed.tracked.event_id:authorized.tracked.event_id;
      assert(receipts[1].events[0].event_id===expectedId&&expectedId!==original.tracked.event_id&&authorized.status.count===0,'old event revived or new grant not delivered');
      assert(receipts[1].events[0].anonymous_id!==receipts[0].events[0].anonymous_id,'withdrawal did not reset identity');
      if(regrantElsewhere)assert(JSON.stringify(receipts[1].events[0])===JSON.stringify(validRows[0].event),'newer event payload changed across stale instance recovery');
      cases.push({regrant_elsewhere:regrantElsewhere,native_bfcache:true,same_document:true,withdrawal_cleared_old_queue:true,stale_client_denied:true,automatic_receipts_after_restore:0,newer_authorized_queue_preserved:true,explicit_grant_required:true,old_event_not_replayed:true,new_identity:true,queue_final:0,modes:receipts.map(r=>r.mode)});
      await page.evaluate(()=>window.sdkBF209.destroy());
      const cdp=await page.context().newCDPSession(page);try{await cdp.send('Storage.clearDataForOrigin',{origin,storageTypes:'indexeddb,local_storage'});}finally{await cdp.detach();}
    }
    assert(errors.length===0,'unhandled page errors');
    return{passed:true,browser:await page.context().browser().version(),cases,real_indexeddb:true,real_http:true,unhandled_page_errors:errors.length,negative_observation_ms:2000,kafka_pg:false,full_device_matrix:false};
  }finally{
    await page.evaluate(()=>window.sdkBF209?.destroy()).catch(()=>{});
    const cdp=await page.context().newCDPSession(page);try{await cdp.send('Storage.clearDataForOrigin',{origin,storageTypes:'indexeddb,local_storage'});}finally{await cdp.detach();page.off('pageerror',onError);}
  }
}
