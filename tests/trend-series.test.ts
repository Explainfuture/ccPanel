import {expect,it} from 'vitest';
import {trendSeries,type TrendPoint} from '../apps/web/src/trend-series';
const p=(day:string,value:number|null):TrendPoint=>({day:'2026-10-'+day,value,excluded:value===null?2:0});
it('keeps null distinct from zero and never connects across null or absent days',()=>{
  const r=trendSeries([p('01',-5),p('02',0),p('03',null),p('04',10),p('06',20)]);
  expect(r.rows[1]!.position).toBe(r.zero);expect(r.rows[2]!.position).toBeNull();
  expect(r.rows[0]!.position).toBeLessThan(r.zero);expect(r.rows[3]!.position).toBeGreaterThan(r.zero);
  expect(r.segments.map(s=>s.map(p=>p.day))).toEqual([['2026-10-01','2026-10-02'],['2026-10-04'],['2026-10-06']]);
});
it('uses calendar spacing and centers a single date without inventing buckets',()=>{
  const r=trendSeries([p('01',1),p('02',2),p('04',3)]);
  expect(r.rows.map(p=>p.x)).toEqual([0,1/3,1]);expect(trendSeries([p('01',2)]).rows[0]!.x).toBe(.5);
  expect(trendSeries([]).rows).toEqual([]);
});
it('keeps extreme finite values, tiny values, all-negative and zero-only scales finite',()=>{
  for(const magnitude of [Number.MAX_VALUE,Number.MIN_VALUE,1]){
    const r=trendSeries([p('01',-magnitude),p('02',0),p('03',magnitude)]);
    expect(r.zero).toBe(.5);expect(r.rows.map(p=>p.position)).toEqual([0,.5,1]);
    expect(Number.isFinite(r.lower)&&Number.isFinite(r.upper)).toBe(true);
  }
  expect(trendSeries([p('01',-10),p('02',-5)]).zero).toBe(1);
  expect(trendSeries([p('01',0)]).rows[0]!.position).toBe(0);
  expect(trendSeries([p('01',null)]).segments).toEqual([]);
});
it('refuses malformed dates, ordering, nonfinite values and invalid exclusion counts',()=>{
  for(const rows of [[p('01',1),p('01',2)],[p('02',1),p('01',2)],[{...p('01',1),day:'2026-02-30'}],[p('01',Infinity)],[p('01',NaN)],[{...p('01',1),excluded:-1}],[{...p('01',1),excluded:.5}]])expect(()=>trendSeries(rows)).toThrow('invalid_trend_series');
});
