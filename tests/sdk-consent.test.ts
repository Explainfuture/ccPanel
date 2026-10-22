import {afterEach,expect,it,vi} from 'vitest';
import {Analytics} from '../packages/sdk-browser/src/index';
import {MemoryQueue} from '../packages/sdk-browser/src/queue';
afterEach(()=>vi.unstubAllGlobals());
function storage(){const values=new Map<string,string>();vi.stubGlobal('localStorage',{getItem:(k:string)=>values.get(k)??null,setItem:(k:string,v:string)=>values.set(k,String(v)),removeItem:(k:string)=>values.delete(k)});}
const options=(projectId='consent-project')=>({endpoint:'https://consent.invalid/v1/batch',projectId,writeKey:'public-test-key',consent:true,queue:new MemoryQueue(),flushInterval:60000,fetch:async()=>new Response('',{status:503})});
it('observes another client withdrawal before track even without a storage event, and never automatically grants',async()=>{
  storage();const a=new Analytics(options()),b=new Analytics(options()),other=new Analytics(options('other-project'));let reopened:Analytics|undefined;
  try{
    await Promise.all([a.flush(),b.flush(),other.flush()]);await a.setConsent(true);
    expect((await b.track('already_consented')).status).toBe('queued-memory');await a.track('before');
    await a.setConsent(false);expect((await b.track('after')).reason).toBe('consent_required');expect(b.beacon()).toBe(false);
    reopened=new Analytics(options());expect((await reopened.track('reopened')).reason).toBe('consent_required');
    await reopened.setConsent(true);expect((await reopened.track('explicit_grant')).status).toBe('queued-memory');
    expect((await b.track('still_revoked')).reason).toBe('consent_required');
    expect((await other.track('unaffected')).status).toBe('queued-memory');
  }finally{a.destroy();b.destroy();other.destroy();reopened?.destroy();}
});
it('a newer withdrawal in another client prevents an older queued grant from reopening collection',async()=>{
  storage();let release!:()=>void;const gate=new Promise<void>(r=>{release=r;});
  class GatedQueue extends MemoryQueue{override async clear(){await gate;return super.clear();}}
  const a=new Analytics({...options(),queue:new GatedQueue()}),b=new Analytics(options());
  try{
    await Promise.all([a.flush(),b.flush()]);
    const withdraw=a.setConsent(false),grant=a.setConsent(true);await b.setConsent(false);release();await Promise.all([withdraw,grant]);
    expect((await a.track('must_stay_revoked')).reason).toBe('consent_required');
  }finally{release();a.destroy();b.destroy();}
});
it('clears records left by a crashed withdrawal before explicitly granting a reopened client',async()=>{
  storage();const queue=new MemoryQueue(),a=new Analytics({...options(),queue});let reopened:Analytics|undefined;
  try{
    await a.flush();await a.track('old_private_event');expect((await queue.stats()).count).toBe(1);a.destroy();
    localStorage.setItem('ccpanel:identity:https://consent.invalid/v1/batch:consent-project:consent','revoked:crashed-withdrawal');
    reopened=new Analytics({...options(),queue});expect((await reopened.track('denied')).reason).toBe('consent_required');
    await reopened.setConsent(true);expect((await queue.stats()).count).toBe(0);expect((await reopened.track('new_consent')).status).toBe('queued-memory');
  }finally{a.destroy();reopened?.destroy();}
});
