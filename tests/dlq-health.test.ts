import {expect,it} from 'vitest';
import {evaluateDlqHealth} from '../packages/server/src/dlq-health';
const at=Date.UTC(2026,9,23),iso=(v:number)=>new Date(v).toISOString();
const snapshot=(days=1)=>({observed_at:iso(at),counts:{unresolved:'1',replaying:'0',resolved:'0',suppressed:'0'},oldest_pending_at:iso(at-days*86400000),future_records:'0'});
it('alerts immediately on pending sources including queued replays without treating them as resolved',()=>{
  expect(evaluateDlqHealth(snapshot(),at).signals.map(s=>s.state)).toEqual(['firing','clear']);
  const replay=snapshot();replay.counts.unresolved='0';replay.counts.replaying='1';expect(evaluateDlqHealth(replay,at).signals[0]!.state).toBe('firing');
  const done={...snapshot(),counts:{unresolved:'0',replaying:'0',resolved:'4',suppressed:'2'},oldest_pending_at:null};expect(evaluateDlqHealth(done,at).signals.map(s=>s.state)).toEqual(['clear','clear']);
});
it('uses inclusive warning/expiry age and still alerts for physically retained expired sources',()=>{
  const value=snapshot(12);expect(evaluateDlqHealth(value,at-1).signals.every(s=>s.state==='unknown')).toBe(true);
  expect(evaluateDlqHealth({...value,oldest_pending_at:iso(at-12*86400000+1)},at).signals[1]!.state).toBe('clear');
  expect(evaluateDlqHealth(value,at).signals[1]!.reason).toBe('retention_window_near');
  expect(evaluateDlqHealth(snapshot(14),at).signals[1]!.reason).toBe('retention_window_exceeded');expect(evaluateDlqHealth(snapshot(20),at).signals[1]!.state).toBe('firing');
});
it('preserves exact counters and source freshness without renewing the observation time',()=>{
  const value=snapshot();value.counts.unresolved='9007199254740993';value.counts.replaying='2';const result=evaluateDlqHealth(value,at+90000);
  expect(result.signals[0]!.pending_records).toBe('9007199254740995');expect(result.signals[0]!.expires_at).toBe(iso(at+90000));
  expect(evaluateDlqHealth(value,at+90001).signals.every(s=>s.state==='unknown'&&s.pending_records===null)).toBe(true);
});
it('refuses malformed precision, missing evidence, impossible totals and future records',()=>{
  const valid=snapshot();for(const input of[null,{}, {...valid,extra:true},{...valid,oldest_pending_at:null},{...valid,oldest_pending_at:iso(at+1)},{...valid,counts:{...valid.counts,unresolved:1}},{...valid,counts:{...valid.counts,unresolved:'01'}},{...valid,counts:{...valid.counts,unresolved:'9223372036854775808'}},{...valid,future_records:'2'}])expect(evaluateDlqHealth(input,at).signals.every(s=>s.state==='unknown')).toBe(true);
  expect(evaluateDlqHealth({...valid,future_records:'1'},at).signals[0]!.reason).toBe('future_record');expect(()=>evaluateDlqHealth(valid,NaN)).toThrow('invalid_dlq_health_time');
});
