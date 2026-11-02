import {afterEach,expect,it,vi} from 'vitest';
import {IntakeMetrics} from '../packages/server/src/intake-metrics';
import {startOperationalCollector} from '../packages/server/src/operational-collector';
afterEach(()=>vi.useRealTimers());
it('has one in-flight snapshot with no backlog, then stops and drains',async()=>{
  vi.useFakeTimers();let release!:()=>void;
  const snapshot=vi.fn(()=>new IntakeMetrics().snapshot()),write=vi.fn(()=>new Promise<void>(resolve=>{release=resolve;})),failed=vi.fn();
  const stop=startOperationalCollector({snapshot,write,failed});
  expect(snapshot).not.toHaveBeenCalled();await vi.advanceTimersByTimeAsync(30000);expect(write).toHaveBeenCalledTimes(1);
  await vi.advanceTimersByTimeAsync(300000);expect(snapshot).toHaveBeenCalledTimes(1);
  let drained=false;const closing=stop().then(()=>{drained=true;});await Promise.resolve();expect(drained).toBe(false);
  release();await closing;await vi.advanceTimersByTimeAsync(300000);expect(write).toHaveBeenCalledTimes(1);expect(failed).not.toHaveBeenCalled();
});
it('contains snapshot/write/log failures and retries only on the next interval',async()=>{
  vi.useFakeTimers();const snapshot=vi.fn(()=>new IntakeMetrics().snapshot()).mockImplementationOnce(()=>{throw new Error('private');});
  const write=vi.fn().mockRejectedValueOnce(new Error('private')).mockResolvedValue(undefined),failed=vi.fn(()=>{throw new Error('logger');});
  const stop=startOperationalCollector({snapshot,write,failed});
  await vi.advanceTimersByTimeAsync(30000);expect(failed).toHaveBeenCalledTimes(1);expect(write).not.toHaveBeenCalled();
  await vi.advanceTimersByTimeAsync(30000);expect(failed).toHaveBeenCalledTimes(2);
  await vi.advanceTimersByTimeAsync(30000);expect(write).toHaveBeenCalledTimes(2);await stop();
});
