import {mkdtemp,readFile,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {basename,dirname,join,resolve} from 'node:path';
import {afterEach,expect,it,vi} from 'vitest';
import {createNodeAnalytics,type NodeClient} from '../packages/sdk-node/src/index';
import type {AnalyticsEvent} from '../packages/schema/src/index';
afterEach(()=>{vi.useRealTimers();vi.restoreAllMocks();});
it('freezes concurrent explicit users without changing legacy identity and retains them through file retry/reopen',async()=>{
  vi.useFakeTimers();vi.spyOn(Math,'random').mockReturnValue(.5);
  const parent=resolve(tmpdir()),directory=await mkdtemp(join(parent,'ccpanel-node-identity-')),outbox=join(directory,'outbox.json');
  let accepted=false,client:NodeClient|undefined;const batches:AnalyticsEvent[][]=[];
  const options={endpoint:'https://consumer.invalid/v1/batch',projectId:'fixture',writeKey:'fixture',consent:true,outboxPath:outbox,batchSize:100,flushInterval:60000,fetch:async(_url:unknown,init?:RequestInit)=>{
    const events=JSON.parse(init!.body as string).events as AnalyticsEvent[];batches.push(events);
    return accepted?Response.json({accepted_event_ids:events.map(e=>e.event_id)},{status:202}):new Response('',{status:503});
  }};
  try{
    client=createNodeAnalytics(options);await client.flush();await client.identify('legacy-user');
    const properties={amount:1,nested:{value:'original',password:'filtered'}};
    const a=client.trackFor('user-A','purchase',properties),b=client.trackFor('user-B','purchase',{amount:2}),legacy=client.track('legacy');properties.nested.value='changed';
    const results=await Promise.all([a,b,legacy]);expect(results.every(r=>r.status==='queued-persistent')).toBe(true);expect(new Set(results.map(r=>r.event_id)).size).toBe(3);
    const rows=JSON.parse(await readFile(outbox,'utf8')).rows as {event:AnalyticsEvent}[];
    const captured=rows.map(r=>r.event),explicit=captured.filter(e=>e.event_name==='purchase');
    expect(explicit.map(e=>e.distinct_id)).toEqual(['user-A','user-B']);expect(explicit.every(e=>!('anonymous_id' in e))).toBe(true);
    expect(explicit[0].properties.nested).toEqual({value:'original'});expect(captured.find(e=>e.event_name==='legacy')!.distinct_id).toBe('legacy-user');
    await client.flush();expect(batches).toHaveLength(1);await client.shutdown();client=undefined;
    accepted=true;vi.setSystemTime(Date.now()+120000);client=createNodeAnalytics(options);await client.flush();
    expect(batches).toHaveLength(2);expect(batches[1]).toEqual(batches[0]);expect((await client.getQueueStatus()).count).toBe(0);
  }finally{
    await client?.shutdown();
    const target=resolve(directory);expect(dirname(target)).toBe(parent);expect(basename(target).startsWith('ccpanel-node-identity-')).toBe(true);await rm(target,{recursive:true,force:true});
  }
});
it('applies consent and validation to per-event users without queueing invalid identity links',async()=>{
  const client=createNodeAnalytics({endpoint:'https://consumer.invalid/v1/batch',projectId:'fixture',writeKey:'fixture',consent:true,flushInterval:60000,fetch:async()=>new Response('',{status:503})});
  try{
    await client.flush();expect((await client.trackFor('','purchase')).reason).toBe('invalid_distinct_id');expect((await client.trackFor('bad\0id','purchase')).reason).toBe('invalid_text');expect((await client.trackFor('user-A','$identify')).reason).toBe('missing_anonymous_id');
    expect((await client.getQueueStatus()).count).toBe(0);
    const pending=client.trackFor('user-A','purchase');await client.setConsent(false);expect((await pending).status).toBe('rejected');expect((await client.getQueueStatus()).count).toBe(0);
    expect((await client.trackFor('user-B','purchase')).reason).toBe('consent_required');
  }finally{await client.shutdown();}
});
