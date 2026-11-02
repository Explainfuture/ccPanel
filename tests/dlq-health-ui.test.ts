import {expect,it} from 'vitest';
import {evaluateDlqHealth} from '../packages/server/src/dlq-health';
import {parseDlqHealth} from '../apps/web/src/dlq-health-view';
const now=Date.UTC(2026,9,23),fixture=()=>evaluateDlqHealth({observed_at:new Date(now).toISOString(),counts:{unresolved:'9007199254740993',replaying:'2',resolved:'0',suppressed:'0'},oldest_pending_at:new Date(now-13*86400000).toISOString(),future_records:'0'},now);
it('validates shared evaluation and accepts only complete conservative unknown evidence',()=>{
  expect(parseDlqHealth(fixture())?.signals[0]?.pending_records).toBe('9007199254740995');expect(parseDlqHealth(evaluateDlqHealth(null,now))?.signals.every(s=>s.state==='unknown')).toBe(true);
  const malformed=fixture();malformed.signals[0]!.state='clear';expect(parseDlqHealth(malformed)).toBeNull();
  const unknown=evaluateDlqHealth(null,now);unknown.signals[0]!.pending_records='0';expect(parseDlqHealth(unknown)).toBeNull();
});
it('clears forged reasons, time, scope, duplicate rules and policy changes',()=>{
  const bad=fixture();bad.signals[1]={...bad.signals[0]!};expect(parseDlqHealth(bad)).toBeNull();
  for(const patch of[{scope:'project'}, {computed_at:'not-time'}, {policy:{source_retention_days:14,retention_warning_days:13,freshness_ms:90000}}, {categories:{unresolved:9007199254740993,replaying:'2',resolved:'0',suppressed:'0'}}])expect(parseDlqHealth({...fixture(),...patch})).toBeNull();
  const reason=fixture();reason.signals[0]!.reason='no_retained_pending';expect(parseDlqHealth(reason)).toBeNull();
});
