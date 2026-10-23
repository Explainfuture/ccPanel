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

