import { expect,it } from 'vitest';
import { validateEvent,storageSafeText } from '../packages/schema/src/index';
import { storageFailureReason } from '../packages/server/src/consume';
const event={event_id:crypto.randomUUID(),event_name:'click',distinct_id:'user',event_time:new Date().toISOString(),properties:{},context:{},schema_version:1};
it('accepts Unicode pairs but rejects NUL and unpaired surrogates before storage',()=>{
  expect(storageSafeText('中文😀')).toBe(true);
  for(const text of ['\u0000','\ud800','\udc00']){
    expect(storageSafeText(text)).toBe(false);
    expect(validateEvent({...event,properties:{nested:{text}}})).toBe('invalid_properties');
    expect(validateEvent({...event,distinct_id:text})).toBe('invalid_text');
    expect(validateEvent({...event,properties:{[text]:'value'}})).toBe('invalid_properties');
  }
  expect(validateEvent({...event,properties:{text:'中文😀'}})).toBeUndefined();
});
it('routes deterministic data failures to DLQ without classifying outages as poison',()=>{
  expect(storageFailureReason({code:'22P05'})).toBe('invalid_storage_value');
  expect(storageFailureReason({code:'23514'})).toBe('invalid_storage_value');
  for(const code of ['08006','53100','53200','57014','40001','23503'])expect(storageFailureReason({code})).toBeUndefined();
});
