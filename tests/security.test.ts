import { expect,it } from 'vitest';
import { passwordHash,passwordMatches,digest } from '../packages/server/src/security';
it('stores salted password hashes and checks only the exact password',()=>{
  const a=passwordHash('not-a-real-secret-123'),b=passwordHash('not-a-real-secret-123');
  expect(a).not.toBe(b);expect(passwordMatches('not-a-real-secret-123',a)).toBe(true);
  expect(passwordMatches('incorrect',a)).toBe(false);expect(passwordMatches('abc','corrupt')).toBe(false);
});
it('does not retain raw session or write tokens as database lookup keys',()=>{
  expect(digest('public-test-key')).toHaveLength(64);expect(digest('public-test-key')).not.toContain('public-test-key');
});
