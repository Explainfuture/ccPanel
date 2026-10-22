export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };
export interface AnalyticsEvent {
  event_id: string;
  event_name: string;
  distinct_id: string;
  anonymous_id?: string;
  event_time: string;
  properties: Record<string, Json>;
  context: Record<string, Json>;
  schema_version: 1;
}
export interface Envelope { write_key: string; project_id?: string; schema_version: 1; events: AnalyticsEvent[] }
export interface StoredEnvelope { tenant_id: string; project_id: string; received_at: string; event: AnalyticsEvent }
export const limits = { eventBytes: 16384, batchBytes: 49152, requestBytes: 65536, batchEvents: 20, serverEvents: 100, ttl: 7 * 86400000 } as const;
export const byteLength = (v: unknown) => new TextEncoder().encode(JSON.stringify(v)).byteLength;
const uuidV4 = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const record = (v: unknown): v is Record<string, unknown> => !!v && typeof v === 'object' && !Array.isArray(v) && Object.getPrototypeOf(v) === Object.prototype;
function jsonSafe(v: unknown, depth = 0, seen = new Set<object>()): boolean {
  if (v === null || typeof v === 'string' || typeof v === 'boolean') return true;
  if (typeof v === 'number') return Number.isFinite(v);
  if (typeof v !== 'object' || depth >= 5 || seen.has(v) || (!Array.isArray(v) && !record(v))) return false;
  seen.add(v);
  const ok = Object.values(v).every(x => jsonSafe(x, depth + 1, seen));
  seen.delete(v);
  return ok;
}
export function validateEvent(value: unknown, now = Date.now(), checkTime = true): string | undefined {
  if (!record(value)) return 'invalid_event';
  const allowed = new Set(['event_id','event_name','distinct_id','anonymous_id','event_time','properties','context','schema_version']);
  if (Object.keys(value).some(k => !allowed.has(k))) return 'unknown_event_field';
  if (typeof value.event_id !== 'string' || !uuidV4.test(value.event_id)) return 'invalid_event_id';
  if (typeof value.event_name !== 'string' || !value.event_name.trim() || value.event_name.length > 128) return 'invalid_event_name';
  if (typeof value.distinct_id !== 'string' || !value.distinct_id || value.distinct_id.length > 256) return 'invalid_distinct_id';
  if (value.anonymous_id !== undefined && (typeof value.anonymous_id !== 'string' || !value.anonymous_id || value.anonymous_id.length > 256)) return 'invalid_anonymous_id';
  if (value.schema_version !== 1) return 'invalid_schema_version';
  const time = typeof value.event_time === 'string' && /Z$/.test(value.event_time) ? Date.parse(value.event_time) : NaN;
  if (!Number.isFinite(time) || (checkTime && (time < now - limits.ttl || time > now + 300000))) return 'invalid_event_time';
  if (!record(value.properties) || Object.keys(value.properties).length > 100 || !jsonSafe(value.properties)) return 'invalid_properties';
  if (!record(value.context) || !jsonSafe(value.context)) return 'invalid_context';
  if (byteLength(value) > limits.eventBytes) return 'event_too_large';
}
export function canonical(value: Json | AnalyticsEvent): string {
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return '[' + value.map(canonical).join(',') + ']';
  return '{' + Object.keys(value).sort().map(k => JSON.stringify(k) + ':' + canonical((value as Record<string, Json>)[k]!)).join(',') + '}';
}
