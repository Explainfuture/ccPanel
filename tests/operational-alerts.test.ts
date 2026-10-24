import {expect,it} from 'vitest';
import {evaluateOperationalAlerts} from '../packages/server/src/operational-alerts';
import {IntakeMetrics} from '../packages/server/src/intake-metrics';
import {WorkerMetrics} from '../packages/server/src/worker-metrics';
const now=Date.now();
function api(){const latest=new IntakeMetrics().snapshot();latest.started_at=new Date(now-120000).toISOString();latest.observed_at=new Date(now).toISOString();const prior=structuredClone(latest);prior.observed_at=new Date(now-30000).toISOString();return{latest,prior};}
it('reports fixed rules, exact per-label increases and no reset-masking sums',()=>{
  const {latest,prior}=api();for(const row of [latest,prior])row.responses.find(r=>r.status==='500')!.batches='9007199254740993';latest.responses.find(r=>r.status==='500')!.batches='9007199254740995';
  let alerts=evaluateOperationalAlerts({api:[prior,latest],worker:[]},now);expect(alerts).toHaveLength(4);expect(alerts.find(r=>r.rule==='api_sampling')).toMatchObject({state:'clear'});expect(alerts.find(r=>r.rule==='api_server_errors')).toMatchObject({state:'firing',increase:'2',interval_ms:30000});expect(alerts.find(r=>r.rule==='worker_sampling')).toMatchObject({state:'firing',reason:'missing_sample'});
  prior.responses.find(r=>r.status==='503')!.batches='10';alerts=evaluateOperationalAlerts({api:[prior,latest],worker:[]},now);expect(alerts.find(r=>r.rule==='api_server_errors')).toMatchObject({state:'unknown',reason:'counter_reset',increase:null});
});
it('never treats missing, stale, gap, duplicate, restart or invalid evidence as zero failures',()=>{
  const cases=['stale','gap','duplicate','restart','invalid','future','single'] as const;
  for(const kind of cases){const {latest,prior}=api();if(kind==='stale'){latest.observed_at=new Date(now-100000).toISOString();prior.observed_at=new Date(now-110000).toISOString();}if(kind==='gap')prior.observed_at=new Date(now-100000).toISOString();if(kind==='duplicate')prior.observed_at=latest.observed_at;if(kind==='restart')prior.instance_id=new IntakeMetrics().snapshot().instance_id;if(kind==='invalid')latest.responses[0]!.batches='bad';if(kind==='future')latest.observed_at=new Date(now+1).toISOString();const alerts=evaluateOperationalAlerts({api:kind==='single'?[latest]:[latest,prior],worker:[]},now);expect(alerts.find(r=>r.rule==='api_server_errors')!.state,kind).toBe('unknown');}
  expect(evaluateOperationalAlerts({api:[],worker:[]},now).every(row=>row.increase===null)).toBe(true);
});
it('uses actual Worker failure labels and clears only a valid unchanged pair',async()=>{
  const worker=new WorkerMetrics(),prior=worker.snapshot();prior.started_at=new Date(now-120000).toISOString();prior.observed_at=new Date(now-30000).toISOString();
  await expect(worker.measure('persist',async()=>{throw new Error('private');})).rejects.toThrow();const latest=worker.snapshot();latest.started_at=prior.started_at;latest.observed_at=new Date(now).toISOString();
  let rows=evaluateOperationalAlerts({api:[],worker:[latest,prior]},now);expect(rows.find(row=>row.rule==='worker_failures')).toMatchObject({state:'firing',increase:'1'});
  prior.operations=structuredClone(latest.operations).reverse();rows=evaluateOperationalAlerts({api:[],worker:[latest,prior]},now);expect(rows.find(row=>row.rule==='worker_failures')).toMatchObject({state:'clear',increase:'0'});
});
