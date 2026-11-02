import {afterEach,expect,it,vi} from 'vitest';
vi.mock('../packages/server/src/db',()=>({pool:{connect:vi.fn()}}));
import {pool} from '../packages/server/src/db';
import {startPartitions} from '../packages/server/src/partitions';
afterEach(()=>{vi.useRealTimers();vi.unstubAllEnvs();vi.restoreAllMocks();vi.clearAllMocks();});
it('does not acquire a database connection when maintenance is disabled',async()=>{
  vi.useFakeTimers();vi.stubEnv('RETENTION_ENABLED','false');const stop=startPartitions();
  await vi.advanceTimersByTimeAsync(7200000);expect(pool.connect).not.toHaveBeenCalled();stop();
});
it('keeps one in-flight acquisition and releases it after stop without scheduling another tick',async()=>{
  vi.useFakeTimers();vi.stubEnv('RETENTION_ENABLED','true');vi.spyOn(console,'log').mockImplementation(()=>{});
  let releaseConnection!:(db:unknown)=>void;
  vi.mocked(pool.connect).mockReturnValue(new Promise(resolve=>{releaseConnection=resolve;}) as never);
  const stop=startPartitions();await vi.advanceTimersByTimeAsync(120000);
  expect(pool.connect).toHaveBeenCalledTimes(1);stop();
  const release=vi.fn(),query=vi.fn().mockResolvedValue({rows:[{held:false}],rowCount:0});releaseConnection({query,release});
  await vi.advanceTimersByTimeAsync(0);expect(release).toHaveBeenCalledTimes(1);
  await vi.advanceTimersByTimeAsync(7200000);expect(pool.connect).toHaveBeenCalledTimes(1);expect(vi.getTimerCount()).toBe(0);
});
