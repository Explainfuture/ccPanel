import {afterEach,expect,it,vi} from 'vitest';
import {WorkerMetrics,startWorkerMetricLog} from '../packages/server/src/worker-metrics';
afterEach(()=>{vi.restoreAllMocks();vi.useRealTimers();});
it('tracks overlapping operations and exact failure identity without exposing payload or dynamic labels',async()=>{
  const metrics=new WorkerMetrics();let release!:(value:string)=>void;
  const pending=metrics.measure('persist',()=>new Promise<string>(r=>{release=r;}),()=> 'inserted');
  expect(metrics.snapshot().operations[0].in_flight).toBe(1);
  const error={secret:'sensitive-business-payload'};
  await expect(metrics.measure('persist',async()=>{throw error;},undefined,()=> 'conflict')).rejects.toBe(error);
  await expect(metrics.measure('dlq_publish',()=>{throw error;})).rejects.toBe(error);
  release('unchanged');expect(await pending).toBe('unchanged');
  const snapshot=metrics.snapshot(),row=snapshot.operations[0];
  expect(row.in_flight).toBe(0);expect(row.outcomes.find(x=>x.outcome==='inserted')?.count).toBe('1');expect(row.outcomes.find(x=>x.outcome==='conflict')?.count).toBe('1');
  expect(row.duration_ms.reduce((sum,x)=>sum+BigInt(x.count),0n)).toBe(2n);
  expect(JSON.stringify(snapshot)).not.toContain('sensitive-business-payload');expect(snapshot.operations).toHaveLength(4);
  row.outcomes[0].count='999';expect(metrics.snapshot().operations[0].outcomes[0].count).toBe('1');
  expect(new WorkerMetrics().snapshot().instance_id).not.toBe(snapshot.instance_id);
});
it('uses monotonic non-cumulative latency buckets, separates invalid samples and protects results from diagnostic callbacks',async()=>{
  const metrics=new WorkerMetrics(),clock=vi.spyOn(performance,'now');
  clock.mockReturnValueOnce(10).mockReturnValueOnce(15);
  await metrics.measure('persist',async()=> 'duplicate',()=> 'duplicate');
  clock.mockReturnValueOnce(20).mockReturnValueOnce(31021);
  await metrics.measure('dlq_index',async()=> undefined);
  clock.mockReturnValueOnce(30).mockReturnValueOnce(NaN);
  expect(await metrics.measure('persist',async()=> 'result',()=>{throw Error('diagnostic');})).toBe('result');
  const rows=metrics.snapshot().operations;expect(rows[0].duration_ms.find(x=>x.upper_bound_ms===5)?.count).toBe('1');expect(rows[0].duration_unknown).toBe('1');expect(rows[2].duration_ms.at(-1)?.count).toBe('1');
});
it('emits bounded snapshots periodically and once after stop without allowing logger failure to throw',()=>{
  vi.useFakeTimers();const logger=vi.spyOn(console,'log').mockImplementation(()=>{throw Error('logger unavailable');});
  const stop=startWorkerMetricLog(new WorkerMetrics());expect(()=>vi.advanceTimersByTime(20000)).not.toThrow();expect(logger).toHaveBeenCalledTimes(2);expect(stop).not.toThrow();vi.advanceTimersByTime(20000);expect(logger).toHaveBeenCalledTimes(3);
});
