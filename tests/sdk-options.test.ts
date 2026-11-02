import {afterEach,expect,it,vi} from 'vitest';
import {Analytics,type AnalyticsOptions} from '../packages/sdk-browser/src/index';
import {createNodeAnalytics} from '../packages/sdk-node/src/index';
import {MemoryQueue} from '../packages/sdk-browser/src/queue';
const options={endpoint:'https://example.invalid/v1/batch',projectId:'timer-test',writeKey:'synthetic',consent:false,fetch:async()=>new Response('',{status:503})};
afterEach(()=>vi.restoreAllMocks());
it('rejects invalid timer delays before creating a timer in both SDK entry points',()=>{
  const timer=vi.spyOn(globalThis,'setTimeout');
  for(const flushInterval of [NaN,Infinity,-Infinity,0,99,100.5,2147483648,Number.MAX_SAFE_INTEGER,'3000',null,{},true]){
    const input={...options,flushInterval} as unknown as AnalyticsOptions;
    expect(()=>new Analytics(input)).toThrow('invalid_flush_interval');
    expect(()=>createNodeAnalytics(input)).toThrow('invalid_flush_interval');
  }
  expect(timer).not.toHaveBeenCalled();
});
it('preserves the default and valid integer timer boundaries without coercion',async()=>{
  const timer=vi.spyOn(globalThis,'setTimeout');
  for(const [delay,expected]of [[undefined,3000],[100,100],[3000,3000],[2147483647,2147483647]] as const){
    const client=new Analytics({...options,consent:true,queue:new MemoryQueue(),flushInterval:delay});
    try{await client.flush();await client.track('pending');expect(timer.mock.calls.at(-1)?.[1]).toBe(expected);}finally{client.destroy();}
  }
});

it('rejects endpoint credentials/query/fragment and invalid runtime credentials before scheduling',()=>{
  const timer=vi.spyOn(globalThis,'setTimeout');
  for(const endpoint of ['https://user:password@example.invalid/v1/batch','https://example.invalid/v1/batch?write_key=secret','https://example.invalid/v1/batch#secret','bad',123,null]){
    const input={...options,endpoint} as unknown as AnalyticsOptions;
    expect(()=>new Analytics(input)).toThrow('invalid_endpoint');expect(()=>createNodeAnalytics(input)).toThrow('invalid_endpoint');
  }
  for(const pair of [{projectId:123},{projectId:' '},{writeKey:123},{writeKey:' '}])expect(()=>new Analytics({...options,...pair} as unknown as AnalyticsOptions)).toThrow('projectId and writeKey required');
  expect(timer).not.toHaveBeenCalled();
});
it('freezes the destination and queue owner while allowing explicit key rotation',async()=>{
  const requests:{url:string;project:string;key:string}[]=[],input={...options,consent:true,queue:new MemoryQueue(),endpoint:'https://EXAMPLE.invalid:443/v1/batch',fetch:async(url:RequestInfo|URL,init?:RequestInit)=>{const body=JSON.parse(init!.body as string);requests.push({url:String(url),project:body.project_id,key:body.write_key});return Response.json({accepted_event_ids:body.events.map((e:{event_id:string})=>e.event_id)},{status:202});}};
  const client=new Analytics(input);
  try{
    await client.flush();await client.track('first');input.endpoint='https://other.invalid/v1/batch?secret=bad';input.projectId='other-project';input.writeKey='mutated';await client.flush();
    expect(requests[0]).toEqual({url:'https://example.invalid/v1/batch',project:'timer-test',key:'synthetic'});
    expect(()=>client.updateWriteKey(123 as unknown as string)).toThrow('invalid_key');await client.track('second');client.updateWriteKey('rotated');await client.flush();expect(requests[1]).toEqual({...requests[0],key:'rotated'});
  }finally{client.destroy();}
});
