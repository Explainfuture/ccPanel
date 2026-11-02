const valid=(value:string)=>/^[A-Za-z0-9_-]{43}$/.test(value);
export function invitationFromHash(hash:string){
  const params=new URLSearchParams(hash.replace(/^#/,'')),values=params.getAll('invitation');
  return values.length===1&&[...params.keys()].every(k=>k==='invitation')&&valid(values[0]!)?values[0]!:'';
}
export function invitationLink(secret:string,origin:string){
  const base=new URL(origin);if(!valid(secret)||base.origin!==origin||!['https:','http:'].includes(base.protocol))throw new Error('invalid_invitation_link');
  return origin+'/#invitation='+secret;
}
export function parseInvitationInput(value:string,origin:string){
  value=value.trim();if(valid(value))return value;
  let url:URL;try{url=new URL(value);}catch{throw new Error('invalid_invitation_link');}
  const secret=invitationFromHash(url.hash);
  if(url.origin!==origin||!secret||url.username||url.password)throw new Error('invalid_invitation_link');return secret;
}
