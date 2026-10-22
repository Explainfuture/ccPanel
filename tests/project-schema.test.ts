import{expect,it}from'vitest';
import{propertyRules,checkRequired,typesDefinition}from'../packages/server/src/project-schema';
import type{AnalyticsEvent}from'../packages/schema/src/index';
const event:AnalyticsEvent={event_id:crypto.randomUUID(),event_name:'purchase_completed',distinct_id:'A',event_time:new Date().toISOString(),properties:{items:[],metadata:{},none:null,amount:10},context:{},schema_version:1};
it('distinguishes array object null and required values at runtime',()=>{
  const rules=propertyRules({items:'array',metadata:'object',none:'null',amount:'number'});expect(checkRequired(event,rules)).toBeUndefined();
  expect(checkRequired(event,{items:'object'})).toBe('invalid_required_property');expect(checkRequired(event,{absent:'null'})).toBe('invalid_required_property');expect(()=>propertyRules({value:'sql'})).toThrow();
});
it('escapes project event names and property names in generated TypeScript',()=>{
  const output=typesDefinition([{event_name:'click";bad',required_properties:{'name"':'string',items:'array'}}]);expect(output).toContain('click\\";bad');expect(output).toContain('Json[]');expect(output).not.toContain('tenant_id');
});
