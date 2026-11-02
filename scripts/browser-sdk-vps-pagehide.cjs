async page=>{
 const origin='http://127.0.0.1:5197';await page.goto(origin+'/');
 const queued=await page.evaluate(async()=>{const f=await(await fetch('/fixture')).json(),{createAnalytics}=await import('/sdk.js');const c=createAnalytics({...f,consent:true,flushInterval:60000});window.sdk178=c;await c.flush();window.addEventListener('pagehide',e=>sessionStorage.setItem('browser178-pagehide',JSON.stringify({trusted:e.isTrusted,persisted:e.persisted})),{once:true});const event_time=new Date(Date.now()-86400000).toISOString(),r=await c.track('vps_browser178',{synthetic:true},{event_time});if(r.status!=='queued-persistent')throw Error('not persistent');await fetch('/result',{method:'POST',body:JSON.stringify({event_id:r.event_id,event_time})});return{status:r.status,count:(await c.getQueueStatus()).count};});
 await page.goto(origin+'/?next=1');
 const marker=await page.evaluate(()=>JSON.parse(sessionStorage.getItem('browser178-pagehide')));if(!marker?.trusted)throw Error('native pagehide missing');
 return{queued,native_pagehide:marker,browser:await page.context().browser().version(),transport:'real VPS HTTPS, no interception',phase:'await independent server visibility before recovery'};
}
