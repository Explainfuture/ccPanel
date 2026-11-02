import {createAnalytics} from '/sdk/ccpanel.js';
let analytics;
const out=document.querySelector('#output');
const show=async(extra={})=>{out.textContent=JSON.stringify({...extra,queue:await analytics.getQueueStatus()},null,2);};
document.querySelector('#config').addEventListener('submit',async e=>{
  e.preventDefault();analytics?.destroy();
  const f=new FormData(e.currentTarget);
  analytics=createAnalytics({endpoint:location.origin+'/v1/batch',projectId:String(f.get('project')),writeKey:String(f.get('key')),consent:true,flushInterval:60000,onError:code=>{out.textContent=JSON.stringify({error:code});}});
  for(const b of document.querySelectorAll('section button'))b.disabled=false;
  await analytics.flush();await show({initialized:true});
});
document.querySelector('#track').addEventListener('click',async()=>{const event=await analytics.track('sdk_lifecycle_test',{source:'SDK 验收页'});await show({event});});
document.querySelector('#flush').addEventListener('click',async()=>show({flush:await analytics.flush()}));
document.querySelector('#status').addEventListener('click',async()=>show());
document.querySelector('#revoke').addEventListener('click',async()=>{await analytics.setConsent(false);await show({consent:false});});
