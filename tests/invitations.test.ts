import {expect,it} from 'vitest';
import {parseInvitation,parseInvitationToken} from '../packages/server/src/invitations';
it('normalizes invitation email and defaults employees to analyst',()=>{
  expect(parseInvitation({email:' Employee@Example.com '})).toEqual({email:'employee@example.com',role:'analyst'});
  expect(parseInvitation({email:'a@example.com',role:'viewer'}).role).toBe('viewer');
});
it('rejects injected scope, unknown permissions and storage-unsafe identities',()=>{
  for(const value of [null,[],{email:'a@example.com',tenant_id:'other'},{email:'a@example.com',role:'owner'},{email:'a@example.com',role:['admin']},{email:'a\0@example.com'},{email:'a\ud800@example.com'},{email:'invalid'}])expect(()=>parseInvitation(value)).toThrow('invalid_invitation');
});
it('accepts only an exact opaque token envelope rather than caller-supplied identity or role',()=>{
  const token='a'.repeat(43);expect(parseInvitationToken({token})).toBe(token);
  for(const value of [{token,role:'admin'},{token,user_id:'other'},{token:'short'},{token:'a'.repeat(44)},{token:'+'.repeat(43)},null])expect(()=>parseInvitationToken(value)).toThrow('invalid_invitation');
});
