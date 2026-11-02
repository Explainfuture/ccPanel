import {expect,it,vi} from 'vitest';
import {operationalCollectionEnabled} from '../packages/server/src/operational-collection-config';
import {startOperationalCollection} from '../packages/server/src/operational-collection';
it('accepts only explicit boolean settings and preserves enabled default',async()=>{
  expect(operationalCollectionEnabled({})).toBe(true);expect(operationalCollectionEnabled({OPERATIONAL_COLLECTION_ENABLED:'true'})).toBe(true);expect(operationalCollectionEnabled({OPERATIONAL_COLLECTION_ENABLED:'false'})).toBe(false);
  for(const value of ['','0','FALSE',' false','true '])expect(()=>operationalCollectionEnabled({OPERATIONAL_COLLECTION_ENABLED:value})).toThrow('invalid_operational_collection_configuration');
  vi.stubEnv('OPERATIONAL_COLLECTION_ENABLED','false');const snapshot=vi.fn();
  try{const stop=startOperationalCollection(snapshot);await stop();expect(snapshot).not.toHaveBeenCalled();}finally{vi.unstubAllEnvs();}
});
