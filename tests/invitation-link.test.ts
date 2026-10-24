import {expect,it} from 'vitest';
import {invitationFromHash,invitationLink,parseInvitationInput} from '../apps/web/src/invitation-link';
const secret='a'.repeat(43),origin='https://analytics.example.com';
it('puts the invitation only in the fragment and parses it without navigating',()=>{
  const link=invitationLink(secret,origin),url=new URL(link);expect(url.search).toBe('');expect(url.pathname).toBe('/');expect(invitationFromHash(url.hash)).toBe(secret);expect(parseInvitationInput(link,origin)).toBe(secret);expect(parseInvitationInput(secret,origin)).toBe(secret);
});
it('rejects foreign hosts, userinfo, duplicate tokens and query-string credentials',()=>{
  for(const link of [invitationLink(secret,'https://other.example.com'),origin+'/?invitation='+secret,origin+'/#invitation='+secret+'&invitation='+secret,'https://user@analytics.example.com/#invitation='+secret])expect(()=>parseInvitationInput(link,origin)).toThrow('invalid_invitation_link');
  expect(invitationFromHash('#invitation='+secret+'&role=admin')).toBe('');expect(()=>invitationLink('invalid',origin)).toThrow();
});
