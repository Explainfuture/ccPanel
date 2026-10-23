// Checks the final public CLI artifact; not a substitute for raw latency traces,
// independent database ID proof or host resource acceptance.
export function capacityWorkloadReport(input:unknown,expectedEvents=1800){
  if(!Number.isSafeInteger(expectedEvents)||expectedEvents<10||expectedEvents>3600)throw new Error('invalid_expected_workload');
  const reasons=new Set<string>(),object=(v:unknown):v is Record<string,unknown>=>!!v&&typeof v==='object'&&!Array.isArray(v);
  if(!object(input)||!object(input.workload))throw new Error('missing_final_workload_evidence');
  const value=input,w=value.workload as Record<string,unknown>;
  const integer=(v:unknown):v is number=>typeof v==='number'&&Number.isSafeInteger(v)&&v>=0;
  const number=(v:unknown):v is number=>typeof v==='number'&&Number.isFinite(v)&&v>=0;
  const timestamp=(v:unknown)=>typeof v==='string'&&Number.isFinite(Date.parse(v))&&new Date(v).toISOString()===v?Date.parse(v):null;
  const start=timestamp(value.started_at),end=timestamp(value.completed_at);
  if(start===null||end===null||end<=start)reasons.add('invalid_window');
  const fields=['generated','acknowledged','duplicate_requests','backlog','not_yet_visible','max_generation_lag_ms','duration_seconds'] as const;
  for(const name of fields)if(!integer(value[name]))reasons.add('invalid_final_counters');
  const counters=['ingest_attempts','ingest_accepted_requests','ingest_failed_requests','ingest_attempted_events','ingest_body_bytes_total','query_opportunities','query_attempts','query_successes','query_failures','query_skipped_busy','visibility_requests'] as const;
  for(const name of counters)if(!integer(w[name]))reasons.add('invalid_workload_counters');
  if(reasons.size) return{evidenceValid:false,dataLoadPass:false,reasons:[...reasons],expectedEvents};
  const n=value as Record<typeof fields[number],number>&Record<string,unknown>,c=w as Record<typeof counters[number],number>&Record<string,unknown>;
  if(n.target_eps!==1||!number(n.generation_seconds)||n.generation_seconds<expectedEvents-1-.1||n.generation_seconds> (end!-start!)/1000||(end!-start!)<expectedEvents*1000||Math.abs(n.duration_seconds*1000-(end!-start!))>1000)reasons.add('invalid_timing');
  const uuid=/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
  if(!Array.isArray(n.event_ids)||n.event_ids.length!==n.generated||n.event_ids.some(id=>typeof id!=='string'||!uuid.test(id))||new Set(n.event_ids.map(id=>String(id).toLowerCase())).size!==n.generated)reasons.add('invalid_event_set');
  if(n.termination!==undefined){
    const t=n.termination;
    if(!object(t)||!['completed','aborted'].includes(String(t.state))||t.planned_events!==expectedEvents||(t.state==='completed'?t.reason!==null:typeof t.reason!=='string'||!/^[a-z_]{1,64}$/.test(t.reason)))reasons.add('invalid_termination_evidence');
    if(object(t)&&t.state==='aborted')reasons.add('aborted_workload');
    if(!Array.isArray(n.confirmed_event_ids))reasons.add('missing_confirmed_event_set');
  }
  if(n.confirmed_event_ids!==undefined){
    const accepted=n.confirmed_event_ids,generated=new Set(Array.isArray(n.event_ids)?n.event_ids.map(id=>String(id).toLowerCase()):[]);
    if(!Array.isArray(accepted)||accepted.length!==n.acknowledged||accepted.some(id=>typeof id!=='string'||!uuid.test(id)||!generated.has(id.toLowerCase()))||new Set(accepted.map(id=>String(id).toLowerCase())).size!==accepted.length)reasons.add('invalid_confirmed_event_set');
  }
  if(!Array.isArray(n.errors)||n.errors.some(e=>typeof e!=='string')||typeof n.passed!=='boolean')reasons.add('invalid_final_result');
  const eq=(a:number,b:number,d:number)=>BigInt(a)===BigInt(b)+BigInt(d);
  if(!eq(c.ingest_attempts,c.ingest_accepted_requests,c.ingest_failed_requests)||!eq(c.query_opportunities,c.query_attempts,c.query_skipped_busy)||!eq(c.query_attempts,c.query_successes,c.query_failures))reasons.add('request_accounting_mismatch');
  if(n.acknowledged>n.generated||n.not_yet_visible>n.generated||n.backlog>n.generated||c.ingest_attempted_events<c.ingest_attempts||BigInt(c.ingest_attempted_events)>20n*BigInt(c.ingest_attempts)||c.ingest_accepted_requests<n.duplicate_requests||BigInt(n.acknowledged)+BigInt(n.duplicate_requests)>20n*BigInt(c.ingest_accepted_requests)||BigInt(n.acknowledged)+BigInt(n.duplicate_requests)<BigInt(c.ingest_accepted_requests))reasons.add('event_accounting_mismatch');
  if(c.ingest_attempts===0){if(c.ingest_body_bytes_total!==0||c.ingest_body_bytes_min!==null||c.ingest_body_bytes_max!==null||c.ingest_body_bytes_mean!==null)reasons.add('body_byte_accounting_mismatch');}
  else if(!integer(c.ingest_body_bytes_min)||!integer(c.ingest_body_bytes_max)||c.ingest_body_bytes_min===0||c.ingest_body_bytes_min>c.ingest_body_bytes_max||BigInt(c.ingest_body_bytes_total)<BigInt(c.ingest_body_bytes_min)*BigInt(c.ingest_attempts)||BigInt(c.ingest_body_bytes_total)>BigInt(c.ingest_body_bytes_max)*BigInt(c.ingest_attempts)||c.ingest_body_bytes_mean!==c.ingest_body_bytes_total/c.ingest_attempts)reasons.add('body_byte_accounting_mismatch');
  const coverage=c.query_opportunities>0&&c.query_skipped_busy===0;
  if(c.query_schedule_coverage!==coverage)reasons.add('query_coverage_claim_mismatch');
  for(const [key,count]of [['ingest_p95_ms',c.ingest_accepted_requests],['observed_visible_p95_ms',n.generated-n.not_yet_visible],['query_p95_ms',c.query_successes]] as const)if(count>0?!number(n[key]):n[key]!==null)reasons.add('invalid_latency_summary');
  const errors=Array.isArray(n.errors)?n.errors.length:0;
  const declaredPass=!(object(n.termination)&&n.termination.state==='aborted')&&n.generated===expectedEvents&&n.acknowledged===expectedEvents&&n.max_generation_lag_ms<1000&&n.not_yet_visible===0&&n.backlog===0&&n.duplicate_requests===Math.floor((expectedEvents-1)/50)&&errors===0&&number(n.ingest_p95_ms)&&n.ingest_p95_ms<1000&&number(n.observed_visible_p95_ms)&&n.observed_visible_p95_ms<10000&&coverage&&c.query_successes>0;
  if(n.passed!==declaredPass)reasons.add('reported_pass_mismatch');
  if(BigInt(c.ingest_failed_requests)+BigInt(c.query_failures)>BigInt(errors))reasons.add('error_accounting_mismatch');
  return{evidenceValid:reasons.size===0,dataLoadPass:reasons.size===0&&declaredPass,reasons:[...reasons],expectedEvents,generated:n.generated,acknowledged:n.acknowledged,duplicates:n.duplicate_requests,durationSeconds:(end!-start!)/1000,queryAttempts:c.query_attempts,querySuccesses:c.query_successes,querySkipped:c.query_skipped_busy,bodyBytesTotal:c.ingest_body_bytes_total,bodyBytesMean:c.ingest_body_bytes_mean};
}
