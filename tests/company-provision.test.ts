import {expect,it} from 'vitest';
import {parseCompanyProvision} from '../packages/server/src/company-provision';
it('normalizes company identity without weakening owner credentials',()=>{
  expect(parseCompanyProvision({name:' A公司 ',owner_email:' Owner@Example.com ',owner_password:'keep-password-unchanged'})).toEqual({name:'A公司',owner_email:'owner@example.com',owner_password:'keep-password-unchanged'});
});
it('rejects injected scope, unsafe names and missing or oversized passwords',()=>{
  const valid={name:'A公司',owner_email:'owner@example.com',owner_password:'test-only-password'};
  for(const value of [null,[],{...valid,tenant_id:'other'},{...valid,name:''},{...valid,name:'a\0b'},{...valid,owner_email:'bad'},{...valid,owner_password:'short'},{...valid,owner_password:'x'.repeat(1025)},{...valid,owner_password:'long-password\n'}])expect(()=>parseCompanyProvision(value)).toThrow('invalid_company_provision');
});
