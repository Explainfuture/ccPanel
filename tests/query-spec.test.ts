import {expect,it}from'vitest';
import{parseTrend,compileFilters}from'../packages/server/src/query-spec';
const base=()=>({kind:'trend',from:new Date(Date.now()-86400000).toISOString(),to:new Date().toISOString(),metric:'count',filters:[],group_by:[]});
it('rejects arbitrary SQL, forged scope, unbounded ranges and invalid numeric operands',()=>{
  expect(()=>parseTrend({...base(),tenant_id:'other'})).toThrow('unknown_query_field');
  expect(()=>parseTrend({...base(),sql:'select * from users'})).toThrow();
  expect(()=>parseTrend({...base(),from:'2000-01-01'})).toThrow('invalid_time_range');
  expect(()=>parseTrend({...base(),filters:[{key:'price',op:'gt',value:'100'}]})).toThrow('numeric_filter_required');
});
it('binds hostile property names and values instead of concatenating SQL',()=>{
  const values:unknown[]=['company','project'];const hostile="x') OR true --";
  const sql=compileFilters([{key:hostile,op:'eq',value:hostile}],values);
  expect(sql).not.toContain(hostile);expect(values).toContain(hostile);expect(sql).toContain('$3');expect(sql).toContain('$4');
});
it('requires existence for inequality and does not coerce strings for numeric comparisons',()=>{
  expect(compileFilters([{key:'tag',op:'neq',value:null}],[])).toContain('properties ?');
  const sql=compileFilters([{key:'price',op:'gt',value:10}],[]);expect(sql).toContain("='number'");expect(sql).toContain('CASE WHEN');
  expect(parseTrend({...base(),metric:'avg',property:'price'}).metric).toBe('avg');
});
