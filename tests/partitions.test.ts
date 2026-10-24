import {expect,it} from 'vitest';
import {monthPartition} from '../packages/server/src/partitions';
it('plans UTC month boundaries across year changes without local-day drift',()=>{
  expect(monthPartition(new Date('2026-12-31T23:59:59.999Z'),1)).toEqual({name:'events_2027_01',from:'2027-01-01T00:00:00.000Z',to:'2027-02-01T00:00:00.000Z'});
  expect(monthPartition(new Date('2028-03-01T00:00:00Z'),-1)).toEqual({name:'events_2028_02',from:'2028-02-01T00:00:00.000Z',to:'2028-03-01T00:00:00.000Z'});
});
