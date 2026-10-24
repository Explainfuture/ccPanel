export interface PressurePolicy {
  maxLagMessages: number;
  maxLagAgeMs: number;
  maxProbeAgeMs: number;
  maxDiskAgeMs: number;
  minFreeBytes: number;
  minFreePercent: number;
  minFreeInodes: number;
}
export const defaultPressurePolicy: Readonly<PressurePolicy> = Object.freeze({
  maxLagMessages: 256,
  maxLagAgeMs: 24 * 3600000,
  maxProbeAgeMs: 15000,
  maxDiskAgeMs: 30000,
  minFreeBytes: 1024 ** 3,
  minFreePercent: 10,
  minFreeInodes: 1024,
});
export interface PartitionPressure {
  partition: number;
  low: string;
  high: string;
  committed: string | null;
  // First offset at/after the age cutoff; use high when no newer record exists.
  ageBoundary: string;
}
export interface DiskPressure {
  kind: 'kafka' | 'postgres';
  sampledAt: number;
  totalBytes: string;
  availableBytes: string;
  totalInodes: string;
  availableInodes: string;
}
export interface PressureSnapshot {
  startedAt: number;
  partitions: PartitionPressure[];
  disks: DiskPressure[];
}
export type PressureReason = 'pressure_probe_invalid' | 'pressure_probe_stale' |
  'disk_probe_stale' | 'disk_space_low' | 'disk_inodes_low' |
  'consumer_offset_unknown' | 'retained_offset_gap' | 'consumer_lag_high' | 'consumer_lag_old';
export interface PressureDecision {
  integrityReason:'retained_offset_gap'|'consumer_offset_unknown'|null;
  allowed: boolean;
  reason: PressureReason | null;
  retryAfterSeconds: number;
  maxLagMessages: string | null;
  totalLagMessages: string | null;
  // Worst-case partition headroom; publication reservations must be subtracted
  // by the caller. This snapshot alone is not a byte-accurate storage guarantee.
  remainingMessages: number;
}
export function pressurePolicy(env: Record<string, string | undefined>): PressurePolicy {
  const policy = { ...defaultPressurePolicy };
  const settings: [keyof PressurePolicy, string, number, number][] = [
    ['maxLagMessages', 'INGEST_MAX_LAG_MESSAGES', 100, 1000000],
    ['maxLagAgeMs', 'INGEST_MAX_LAG_AGE_MS', 60000, 48 * 3600000],
    ['maxProbeAgeMs', 'INGEST_PROBE_MAX_AGE_MS', 1000, 60000],
    ['maxDiskAgeMs', 'INGEST_DISK_MAX_AGE_MS', 1000, 120000],
    ['minFreeBytes', 'INGEST_MIN_FREE_BYTES', 1024 ** 2, Number.MAX_SAFE_INTEGER],
    ['minFreePercent', 'INGEST_MIN_FREE_PERCENT', 1, 90],
    ['minFreeInodes', 'INGEST_MIN_FREE_INODES', 1, 1000000000],
  ];
  for (const [key, name, min, max] of settings) {
    const value = env[name];
    if (value === undefined) continue;
    if (!/^\d+$/.test(value)) throw new Error('invalid_pressure_config:' + name);
    const number = Number(value);
    if (!Number.isSafeInteger(number) || number < min || number > max)
      throw new Error('invalid_pressure_config:' + name);
    policy[key] = number;
  }
  return policy;
}
const maxInt64 = 9223372036854775807n;
function integer(value: unknown): bigint {
  if (typeof value !== 'string' || !/^(0|[1-9]\d{0,18})$/.test(value)) throw new Error('invalid_counter');
  const number = BigInt(value);
  if (number > maxInt64) throw new Error('invalid_counter');
  return number;
}
const validTime = (time: unknown, now: number): time is number =>
  typeof time === 'number' && Number.isFinite(time) && time >= 0 && time <= now + 1000;
export function evaluatePressure(input: unknown, policy = defaultPressurePolicy, now = Date.now()): PressureDecision {
  let maxLag: bigint | null = null, totalLag: bigint | null = null, headroom = 0;
  let integrityReason:PressureDecision['integrityReason']=null;
  const stop = (reason: PressureReason): PressureDecision => ({
    allowed: false, reason, integrityReason, retryAfterSeconds: reason.startsWith('disk_') ? 30 : 5,
    maxLagMessages: maxLag?.toString() ?? null,
    totalLagMessages: totalLag?.toString() ?? null, remainingMessages: 0,
  });
  try {
    if (!Number.isFinite(now) || !input || typeof input !== 'object') return stop('pressure_probe_invalid');
    const snapshot = input as PressureSnapshot;
    if (!validTime(snapshot.startedAt, now)) return stop('pressure_probe_invalid');
    if (now - snapshot.startedAt > policy.maxProbeAgeMs) return stop('pressure_probe_stale');
    if (!Array.isArray(snapshot.partitions) || snapshot.partitions.length < 1 || snapshot.partitions.length > 1024 ||
      !Array.isArray(snapshot.disks) || snapshot.disks.length !== 2) return stop('pressure_probe_invalid');
    const seen = new Set<number>();
    let gap = false, unknown = false, old = false;
    maxLag = 0n; totalLag = 0n;
    for (const partition of snapshot.partitions) {
      if (!partition || !Number.isSafeInteger(partition.partition) || partition.partition < 0 || seen.has(partition.partition))
        return stop('pressure_probe_invalid');
      seen.add(partition.partition);
      const low = integer(partition.low), high = integer(partition.high), boundary = integer(partition.ageBoundary);
      if (low > high || boundary < low || boundary > high) return stop('pressure_probe_invalid');
      // A truly new group can begin at zero. If old segments are already gone,
      // no commit cannot be silently interpreted as the current earliest offset.
      if (partition.committed === null && low > 0n) unknown = true;
      const committed = partition.committed === null ? 0n : integer(partition.committed);
      if (committed > high) return stop('pressure_probe_invalid');
      if (committed < low) gap = true;
      const lag = high - committed;
      if (lag > maxLag) maxLag = lag;
      totalLag += lag;
      if (lag > 0n && committed < boundary) old = true;
    }
    integrityReason=unknown?'consumer_offset_unknown':gap?'retained_offset_gap':null;
    const kinds = new Set<string>();
    for (const disk of snapshot.disks) {
      if (!disk || !['kafka', 'postgres'].includes(disk.kind) || kinds.has(disk.kind) || !validTime(disk.sampledAt, now))
        return stop('pressure_probe_invalid');
      kinds.add(disk.kind);
      if (now - disk.sampledAt > policy.maxDiskAgeMs) return stop('disk_probe_stale');
      const total = integer(disk.totalBytes), available = integer(disk.availableBytes), files = integer(disk.totalInodes), freeFiles = integer(disk.availableInodes);
      if (total === 0n || files === 0n || available > total || freeFiles > files) return stop('pressure_probe_invalid');
      const fraction = (total * BigInt(policy.minFreePercent) + 99n) / 100n;
      const reserve = fraction > BigInt(policy.minFreeBytes) ? fraction : BigInt(policy.minFreeBytes);
      if (available <= reserve) return stop('disk_space_low');
      if (freeFiles <= BigInt(policy.minFreeInodes)) return stop('disk_inodes_low');
    }
    if (unknown) return stop('consumer_offset_unknown');
    if (gap) return stop('retained_offset_gap');
    if (maxLag >= BigInt(policy.maxLagMessages)) return stop('consumer_lag_high');
    if (old) return stop('consumer_lag_old');
    headroom = policy.maxLagMessages - Number(maxLag);
    return {allowed: true, reason: null, integrityReason, retryAfterSeconds: 0, maxLagMessages: maxLag.toString(), totalLagMessages: totalLag.toString(), remainingMessages: headroom};
  } catch { return stop('pressure_probe_invalid'); }
}
