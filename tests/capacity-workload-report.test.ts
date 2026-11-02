import {expect,it} from 'vitest';
import {randomUUID} from 'node:crypto';
import {capacityWorkloadReport} from '../scripts/capacity-workload-report';
const fixture=()=>({started_at:'2026-10-23T00:00:00.000Z',completed_at:'2026-10-23T00:30:00.500Z',duration_seconds:1801,generation_seconds:1799.02,target_eps:1,generated:1800,acknowledged:1800,duplicate_requests:35,backlog:0,not_yet_visible:0,max_generation_lag_ms:20,ingest_p95_ms:600,observed_visible_p95_ms:9000,query_p95_ms:1700,errors:[] as string[],passed:true,event_ids:Array.from({length:1800},()=>randomUUID()),workload:{ingest_attempts:1835,ingest_accepted_requests:1835,ingest_failed_requests:0,ingest_attempted_events:1835,ingest_body_bytes_total:1835000,ingest_body_bytes_min:1000,ingest_body_bytes_max:1000,ingest_body_bytes_mean:1000,query_opportunities:180,query_attempts:180,query_successes:180,query_failures:0,query_skipped_busy:0,visibility_requests:1000,query_schedule_coverage:true}});
it('reports final byte/query accounting without replacing independent PG and host proof',()=>{expect(capacityWorkloadReport(fixture())).toMatchObject({evidenceValid:true,dataLoadPass:true,queryAttempts:180,bodyBytesMean:1000});});
it('accepts honest failed load evidence but never calls it a passing resource/data result',()=>{const value=fixture();value.observed_visible_p95_ms=11000;value.passed=false;expect(capacityWorkloadReport(value)).toMatchObject({evidenceValid:true,dataLoadPass:false});});
it('requires the full declared generation and wall-clock window',()=>{const value=fixture();value.completed_at='2026-10-23T00:00:01.000Z';value.duration_seconds=1;value.generation_seconds=.9;expect(capacityWorkloadReport(value)).toMatchObject({evidenceValid:false,dataLoadPass:false});});
it('refuses legacy evidence rather than inventing missing workload counters',()=>{expect(()=>capacityWorkloadReport({passed:true})).toThrow('missing_final_workload_evidence');});
it('rejects inconsistent counts, byte means, coverage claims, event IDs, windows and false pass flags',()=>{
  for(const change of [(v:ReturnType<typeof fixture>)=>{v.workload.query_successes=179;},(v:ReturnType<typeof fixture>)=>{v.workload.ingest_body_bytes_mean=999;},(v:ReturnType<typeof fixture>)=>{v.workload.query_schedule_coverage=false;},(v:ReturnType<typeof fixture>)=>{v.event_ids[1]=v.event_ids[0]!;},(v:ReturnType<typeof fixture>)=>{v.completed_at=v.started_at;},(v:ReturnType<typeof fixture>)=>{v.observed_visible_p95_ms=11000;},(v:ReturnType<typeof fixture>)=>{v.workload.ingest_attempts=Infinity;}]){const value=fixture();change(value);expect(capacityWorkloadReport(value).evidenceValid).toBe(false);}
});
it('rejects an aborted run even if all event and timing targets were reached',()=>{
  const v=fixture(),termination={state:'aborted',reason:'resource_safety',planned_events:1800};
  const report=capacityWorkloadReport({...v,passed:false,termination,confirmed_event_ids:v.event_ids});
  expect(report.dataLoadPass).toBe(false);expect(report.reasons).toContain('aborted_workload');expect(report.reasons).not.toContain('reported_pass_mismatch');
  expect(capacityWorkloadReport({...v,termination,confirmed_event_ids:v.event_ids}).reasons).toContain('reported_pass_mismatch');
});
it('checks the exact acknowledged subset and terminal metadata without rewriting legacy evidence',()=>{
  const v=fixture(),termination={state:'completed',reason:null,planned_events:1800};
  expect(capacityWorkloadReport({...v,termination,confirmed_event_ids:v.event_ids})).toMatchObject({evidenceValid:true,dataLoadPass:true});
  for(const ids of [[...v.event_ids.slice(1),randomUUID()],[...v.event_ids.slice(1),v.event_ids[1]],v.event_ids.slice(1)])expect(capacityWorkloadReport({...v,termination,confirmed_event_ids:ids}).reasons).toContain('invalid_confirmed_event_set');
  expect(capacityWorkloadReport({...v,termination}).reasons).toContain('missing_confirmed_event_set');
  expect(capacityWorkloadReport({...v,termination:{...termination,planned_events:1799},confirmed_event_ids:v.event_ids}).reasons).toContain('invalid_termination_evidence');
});
