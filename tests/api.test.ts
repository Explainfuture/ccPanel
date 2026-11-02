import { expect,it } from 'vitest';
import { createApp } from '../apps/api/src/app';
const app=createApp({ready:()=>false,publish:async()=>{throw new Error('not reachable');}});
it('never exposes tenant data without a session',async()=>{
  const res=await app.request('/api/tenants/00000000-0000-4000-8000-000000000001/projects');
  expect(res.status).toBe(401);
});
it('rejects oversized batches before any database or Kafka work',async()=>{
  const res=await app.request('/v1/batch',{method:'POST',body:'x'.repeat(65537)});
  expect(res.status).toBe(413);
});
it('rejects invalid protocol and login origins without external services',async()=>{
  const invalid=await app.request('/v1/batch',{method:'POST',body:'not json'});
  expect(invalid.status).toBe(400);expect(invalid.headers.get('Access-Control-Allow-Origin')).toBe('*');expect(invalid.headers.get('Access-Control-Expose-Headers')).toBe('Retry-After');
  expect((await app.request('/api/auth/login',{method:'POST',headers:{origin:'https://other.invalid'},body:'{}'})).status).toBe(403);
});
