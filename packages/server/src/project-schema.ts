import type{AnalyticsEvent}from'../../schema/src/index';
export type PropertyType='string'|'number'|'boolean'|'object'|'array'|'null';
export function propertyRules(input:unknown):Record<string,PropertyType>{
  if(!input||typeof input!=='object'||Array.isArray(input)||Object.keys(input).length>100)throw new Error('invalid_property_rules');
  for(const[key,type]of Object.entries(input))if(!key||key.length>128||typeof type!=='string'||!['string','number','boolean','object','array','null'].includes(type))throw new Error('invalid_property_rules');
  return input as Record<string,PropertyType>;
}
export function checkRequired(event:AnalyticsEvent,rules:Record<string,PropertyType>){
  for(const[key,type]of Object.entries(rules)){
    if(!Object.hasOwn(event.properties,key))return'invalid_required_property';
    const value=event.properties[key];
    const valid=type==='null'?value===null:type==='array'?Array.isArray(value):type==='object'?value!==null&&typeof value==='object'&&!Array.isArray(value):typeof value===type;
    if(!valid)return'invalid_required_property';
  }
}
export function typesDefinition(schemas:{event_name:string;required_properties:Record<string,PropertyType>}[]){
  const type={string:'string',number:'number',boolean:'boolean',object:'Record<string, Json>',array:'Json[]',null:'null'};
  return'// Generated from this project event registry. Runtime validation still applies.\nexport type Json = null | boolean | number | string | Json[] | { [key: string]: Json };\nexport type ProjectEvents = {\n'+schemas.map(s=>'  '+JSON.stringify(s.event_name)+': { '+Object.entries(s.required_properties).map(([key,value])=>JSON.stringify(key)+': '+type[value]).join('; ')+' };').join('\n')+'\n};\n';
}
