async (page) => {
  const context=page.context(),origin=new URL(page.url()).origin,project='consent-'+Date.now();
  if(!['localhost','127.0.0.1'].includes(new URL(origin).hostname))throw new Error('Local SDK lab only');
  for(const other of context.pages())if(other!==page)await other.close();
  await page.goto(origin+'/sdk-lab.html');
  const check=(value,message)=>{if(!value)throw new Error(message);};
  await context.route('**/v1/batch',async route=>{const b=route.request().postDataJSON();await route.fulfill({status:202,contentType:'application/json',body:JSON.stringify({accepted_event_ids:b.events.map(e=>e.event_id)})});});
  const init=async(p,id,ack=false)=>p.evaluate(async({id,ack})=>{
    const {createAnalytics}=await import('/sdk/ccpanel.js');window.__sent=[];
    window.__sdk=createAnalytics({endpoint:location.origin+'/v1/batch',projectId:id,writeKey:'public-browser-fixture',consent:true,flushInterval:60000,fetch:async(_u,init)=>{
      const ids=JSON.parse(init.body).events.map(e=>e.event_id);window.__sent.push(...ids);
      return ack?Response.json({accepted_event_ids:ids},{status:202}):new Response('',{status:503});
    }});await window.__sdk.flush();return window.__sdk.getQueueStatus();
  },{id,ack});
  await init(page,project);const second=await context.newPage();await second.goto(origin+'/sdk-lab.html');await init(second,project);
  await page.evaluate(()=>window.__sdk.setConsent(true));
  check((await second.evaluate(()=>window.__sdk.track('already_consented'))).status==='queued-persistent','grant interrupted existing consent');
  const first=await page.evaluate(()=>window.__sdk.track('before_revoke'));check(first.status==='queued-persistent','not persistent');
  await page.evaluate(()=>window.__sdk.setConsent(false));
  const denied=await second.evaluate(()=>window.__sdk.track('after_revoke'));check(denied.reason==='consent_required','other tab still collecting');
  check((await second.evaluate(()=>window.__sdk.getQueueStatus())).count===0,'shared queue not cleared');
  const third=await context.newPage();await third.goto(origin+'/sdk-lab.html');await init(third,project);
  check((await third.evaluate(()=>window.__sdk.track('reopen_revoked'))).reason==='consent_required','reopen bypassed revocation');
  await third.evaluate(()=>window.__sdk.setConsent(true));check((await third.evaluate(()=>window.__sdk.track('explicit_grant'))).status==='queued-persistent','explicit grant failed');
  check((await second.evaluate(()=>window.__sdk.track('no_auto_grant'))).reason==='consent_required','other tab automatically granted');
  await third.evaluate(()=>window.__sdk.setConsent(false));
  for(const p of [page,second,third])await p.evaluate(()=>window.__sdk.destroy());
  await second.close();await third.close();
  const reopenProject='reopen-'+Date.now();await init(page,reopenProject);
  const queued=await page.evaluate(()=>window.__sdk.track('close_before_threshold'));check(queued.status==='queued-persistent','close event not persistent');
  const oldId=queued.event_id;await page.close();
  const reopened=await context.newPage();await reopened.goto(origin+'/sdk-lab.html');await init(reopened,reopenProject,true);
  await reopened.waitForTimeout(1500);await reopened.evaluate(()=>window.__sdk.flush());
  const restored=await reopened.evaluate(async()=>({queue:await window.__sdk.getQueueStatus(),sent:window.__sent,ua:navigator.userAgent}));
  check(restored.sent.includes(oldId),'persisted ID not replayed');check(restored.queue.count===0,'confirmed queue not cleared');
  await reopened.evaluate(()=>window.__sdk.destroy());
  return {passed:true,actual_indexeddb:true,cross_tab_revocation:true,reopened_revocation:true,explicit_grant_only:true,close_reopen_stable_id:true,ack_clears_queue:true,transport:'mock 503/202, not Kafka',user_agent:restored.ua};
}

