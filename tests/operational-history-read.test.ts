import {expect,it} from 'vitest';
import {parseOperationalHistoryQuery} from '../packages/server/src/operational-history-read';
it('accepts only bounded fixed filters and exact keyset cursors',()=>{
  const tuple=['2026-10-23T00:00:00.123456Z','api','12345678-1234-1234-1234-123456789abc'];
  const cursor=Buffer.from(JSON.stringify(tuple)).toString('base64url');
  expect(parseOperationalHistoryQuery(new URLSearchParams({source:'api',cursor}))).toEqual({source:'api',cursor:tuple});
  expect(parseOperationalHistoryQuery(new URLSearchParams())).toEqual({source:null,cursor:null});
  for(const query of ['limit=999','source=host','source=api&source=worker','cursor=','cursor='+('x'.repeat(257)),'cursor='+cursor+'&source=worker','cursor='+Buffer.from(JSON.stringify(['2026-02-30T00:00:00.000000Z',...tuple.slice(1)])).toString('base64url')])expect(()=>parseOperationalHistoryQuery(new URLSearchParams(query))).toThrow('operational_history_read_denied');
});
