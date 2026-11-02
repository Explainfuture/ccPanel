import {it,expect,vi,beforeEach,afterEach} from 'vitest';
import {spawnSync} from 'node:child_process';
import {resolve} from 'node:path';
import {startApiPolling} from '../packages/server/src/api-polling';

beforeEach(()=>{vi.useFakeTimers();vi.setSystemTime(0);});
afterEach(()=>{vi.clearAllTimers();vi.useRealTimers();vi.restoreAllMocks();});

it('retains the default first poll at two seconds and the two-second cadence',()=>{
  const calls:number[]=[],stop=startApiPolling(()=>{calls.push(Date.now());});
  expect(calls).toEqual([]);vi.advanceTimersByTime(1999);expect(calls).toEqual([]);
  vi.advanceTimersByTime(1);expect(calls).toEqual([2000]);vi.advanceTimersByTime(4000);expect(calls).toEqual([2000,4000,6000]);
  stop();expect(vi.getTimerCount()).toBe(0);
});
it('keeps independent startup phases while each runner continues every two seconds',()=>{
  const calls:{job:number;at:number}[]=[],stops=[0,667,1333].map((delay,job)=>startApiPolling(()=>{calls.push({job,at:Date.now()});},delay));
  expect(calls).toEqual([]);vi.advanceTimersByTime(6000);
  expect(calls).toEqual([{job:0,at:0},{job:1,at:667},{job:2,at:1333},{job:0,at:2000},{job:1,at:2667},{job:2,at:3333},{job:0,at:4000},{job:1,at:4667},{job:2,at:5333},{job:0,at:6000}]);
  for(const stop of stops)stop();expect(vi.getTimerCount()).toBe(0);
});
it.each([0,667,2000])('can stop idempotently before the first poll at phase %i',delay=>{
  const tick=vi.fn(),stop=startApiPolling(tick,delay);stop();stop();
  vi.advanceTimersByTime(10000);expect(tick).not.toHaveBeenCalled();expect(vi.getTimerCount()).toBe(0);
});
it('lets the first tick stop its own already-armed interval',()=>{
  const calls:number[]=[];let stop!:()=>void;
  stop=startApiPolling(()=>{calls.push(Date.now());stop();},0);
  vi.advanceTimersByTime(0);expect(calls).toEqual([0]);expect(vi.getTimerCount()).toBe(0);
  stop();vi.advanceTimersByTime(10000);expect(calls).toEqual([0]);
});
it('stops a running job without shifting or cancelling another job',()=>{
  const first:number[]=[],second:number[]=[],stopFirst=startApiPolling(()=>{first.push(Date.now());},0),stopSecond=startApiPolling(()=>{second.push(Date.now());},667);
  vi.advanceTimersByTime(2500);stopFirst();stopFirst();vi.advanceTimersByTime(3500);
  expect(first).toEqual([0,2000]);expect(second).toEqual([667,2667,4667]);stopSecond();expect(vi.getTimerCount()).toBe(0);
});
it('does not await a pending task or serialize another runner behind it',async()=>{
  let release!:()=>void;const gate=new Promise<void>(resolve=>{release=resolve;}),started:number[]=[],completed:number[]=[],other:number[]=[],pending:Promise<void>[]=[];
  const slow=startApiPolling(()=>{started.push(Date.now());pending.push(gate.then(()=>{completed.push(Date.now());}));},0);
  const fast=startApiPolling(()=>{other.push(Date.now());},667);
  try{
    await vi.advanceTimersByTimeAsync(5000);expect(completed).toEqual([]);expect(started).toEqual([0,2000,4000]);expect(other).toEqual([667,2667,4667]);
  }finally{slow();fast();release();await Promise.all(pending);}
  expect(completed).toHaveLength(3);expect(vi.getTimerCount()).toBe(0);
});
it('keeps independent phases when a runner handles its own asynchronous task failures',async()=>{
  // As in the existing runners, the callback owns its rejection handling. The
  // timer helper neither awaits work nor swallows arbitrary callback errors.
  const errors:number[]=[],other:number[]=[],task=vi.fn(async()=>{throw Error('synthetic_task_failure');});
  const failing=startApiPolling(()=>{void task().catch(()=>{errors.push(Date.now());});},0),healthy=startApiPolling(()=>{other.push(Date.now());},1333);
  await vi.advanceTimersByTimeAsync(5000);expect(errors).toEqual([0,2000,4000]);expect(other).toEqual([1333,3333]);expect(task).toHaveBeenCalledTimes(3);
  failing();healthy();expect(vi.getTimerCount()).toBe(0);
});
it.each([-1,2001,0.5,NaN,Infinity,-Infinity,'0',null])('rejects invalid initial delay %s before scheduling any timer',delay=>{
  const tick=vi.fn();expect(()=>startApiPolling(tick,delay as number)).toThrow('invalid_api_polling_delay');expect(vi.getTimerCount()).toBe(0);expect(tick).not.toHaveBeenCalled();
});

// Actual child-process exit tests cover unref semantics rather than relying on
// fake-timer handle methods. No stop callback is called to force these exits.
const run=(body:string)=>spawnSync(process.execPath,[resolve('node_modules/tsx/dist/cli.mjs'),'--eval',`import {startApiPolling} from ${JSON.stringify(resolve('packages/server/src/api-polling.ts'))};${body}`],{encoding:'utf8',timeout:5000});
it('does not keep an otherwise idle process alive for its initial timeout',()=>{
  const child=run("startApiPolling(()=>console.log('unexpected_tick'));console.log('scheduled');");
  expect(child.error).toBeUndefined();expect(child.status).toBe(0);expect(child.stdout.trim()).toBe('scheduled');expect(child.stderr).toBe('');
});
it('does not keep an otherwise idle process alive after the first tick arms the interval',()=>{
  const child=run("startApiPolling(()=>console.log('tick'),0);setTimeout(()=>console.log('released'),20);");
  expect(child.error).toBeUndefined();expect(child.status).toBe(0);expect(child.stdout.trim().split(/\r?\n/)).toEqual(['tick','released']);expect(child.stderr).toBe('');
});
