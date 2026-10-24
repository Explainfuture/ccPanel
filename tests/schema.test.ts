import { describe, expect, it } from 'vitest';
import { canonical, validateEvent, type AnalyticsEvent } from '../packages/schema/src/index';
const event = (): AnalyticsEvent => ({ event_id: crypto.randomUUID(), event_name: 'button_clicked', distinct_id: 'user-A', event_time: new Date().toISOString(), properties: {}, context: {}, schema_version: 1 });
describe('event protocol security boundaries', () => {
  it('accepts stable IDs on retries but new operations use new IDs', () => {
    const a = event(), b = event(); expect(a.event_id).not.toBe(b.event_id); expect(validateEvent(a)).toBeUndefined();
    expect(validateEvent(structuredClone(a))).toBeUndefined();
  });
  it('rejects forged tenant scope, non-JSON values, oversized and stale events', () => {
    expect(validateEvent({ ...event(), tenant_id: 'other' })).toBe('unknown_event_field');
    expect(validateEvent({ ...event(), properties: { amount: NaN } })).toBe('invalid_properties');
    expect(validateEvent({ ...event(), properties: { secret: () => 1 } })).toBe('invalid_properties');
    expect(validateEvent({ ...event(), properties: { long: 'a'.repeat(17000) } })).toBe('event_too_large');
    expect(validateEvent({ ...event(), event_time: '2000-01-01T00:00:00.000Z' })).toBe('invalid_event_time');
  });
  it('canonicalizes key order but preserves meaningful payload changes', () => {
    expect(canonical({ b: 2, a: 1 })).toBe(canonical({ a: 1, b: 2 }));
    expect(canonical({ a: 1 })).not.toBe(canonical({ a: 2 }));
  });
});
