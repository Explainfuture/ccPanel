import {expect,it} from 'vitest';
import {Analytics} from '../packages/sdk-browser/src/index';
import {MemoryQueue} from '../packages/sdk-browser/src/queue';
function fixture(persistent=false){
  let release!:()=>void;const gate=new Promise<void>(r=>{release=r;});
  class SlowQueue extends MemoryQueue{override readonly persistent=persistent;calls=0;override async add(...args:Parameters<MemoryQueue['add']>){this.calls++;await gate;return super.add(...args);}}
  const queue=new SlowQueue(),errors:string[]=[],client=new Analytics({endpoint:'https://example.invalid/v1/batch',projectId:'pending',writeKey:'fixture',consent:true,queue,batchSize:100,flushInterval:60000,onError:code=>errors.push(code),fetch:async()=>new Response('',{status:503})});
  return {client,queue,release,errors};
}
it('rejects excess pending writes before reading caller properties and releases reservations after storage completes',async()=>{
  const {client,queue,release,errors}=fixture();const pending:ReturnType<Analytics['track']>[]=[];
  try{
    await client.flush();for(let i=0;i<100;i++)pending.push(client.track('burst',{i}));
    const properties={get value():string{throw Error('must not be read');}};
    const rejected=await client.track('overflow',properties);expect(rejected).toEqual({status:'rejected',reason:'rejected_pending_overflow'});expect(queue.calls).toBe(100);expect(errors).toContain('rejected_pending_overflow');
    expect((await client.getQueueStatus()).pending_writes).toBe(100);
    release();await Promise.all(pending);expect((await client.getQueueStatus()).pending_writes).toBe(0);expect((await client.getQueueStatus()).pending_bytes).toBe(0);
  }finally{release();await Promise.all(pending);client.destroy();}
});
it('uses the durable pending budget and admits new work after rejected storage capacity is released',async()=>{
  const {client,queue,release}=fixture(true);const pending:ReturnType<Analytics['track']>[]=[];
  try{
    await client.flush();for(let i=0;i<1000;i++)pending.push(client.track('durable',{i}));
    expect(await client.track('over_budget')).toEqual({status:'rejected',reason:'rejected_pending_overflow'});expect(queue.calls).toBe(1000);
    const status=await client.getQueueStatus();expect(status.pending_writes).toBe(1000);expect(status.pending_limit_count).toBe(1000);expect(status.pending_limit_bytes).toBe(5*1024*1024);
    release();await Promise.all(pending);expect((await client.getQueueStatus()).pending_writes).toBe(0);
    // The pending reservation is free even though the durable queue itself is full.
    expect((await client.track('queue_full')).reason).toBe('rejected_overflow');expect((await client.getQueueStatus()).pending_bytes).toBe(0);
  }finally{release();await Promise.all(pending);client.destroy();}
});
it('bounds serialized pending bytes independently of record count and drains revoked pending writes without resurrection',async()=>{
  const {client,queue,release}=fixture();const pending:ReturnType<Analytics['track']>[]=[];
  try{
    await client.flush();for(let i=0;i<90;i++)pending.push(client.track('large',{text:'x'.repeat(12000)}));
    expect(queue.calls).toBeLessThan(90);const status=await client.getQueueStatus();expect(status.pending_writes).toBeGreaterThan(0);expect(status.pending_writes).toBeLessThan(90);expect(status.pending_bytes).toBeLessThanOrEqual(512*1024);expect(queue.calls).toBe(status.pending_writes);
    const withdraw=client.setConsent(false);release();await withdraw;const results=await Promise.all(pending);expect(results.every(r=>r.status==='rejected')).toBe(true);
    expect((await client.getQueueStatus()).count).toBe(0);expect((await client.getQueueStatus()).pending_bytes).toBe(0);
  }finally{release();await Promise.all(pending);client.destroy();}
});
