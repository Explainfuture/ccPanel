import { randomBytes, scryptSync, timingSafeEqual, createHash } from 'node:crypto';
export const token = () => randomBytes(32).toString('base64url');
export const digest = (value: string) => createHash('sha256').update(value).digest('hex');
export function passwordHash(password: string) {
  const salt = randomBytes(16).toString('hex');
  return salt + ':' + scryptSync(password, salt, 64).toString('hex');
}
export function passwordMatches(password: string, encoded: string) {
  const [salt, expected] = encoded.split(':');
  if (!salt || !expected || expected.length !== 128) return false;
  return timingSafeEqual(scryptSync(password, salt, 64), Buffer.from(expected, 'hex'));
}
