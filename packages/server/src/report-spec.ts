import {parseQuerySpec,type AnalysisSpec} from './analysis-spec';
import {storageSafeText} from '../../schema/src/index';
export class ReportError extends Error{constructor(public status:400|403|404|409,message:string){super(message);}}
export const reportId=(value:unknown):value is string=>typeof value==='string'&&/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value);
const object=(input:unknown):input is Record<string,unknown>=>!!input&&typeof input==='object'&&!Array.isArray(input);
function named(value:unknown){if(typeof value!=='string'||!value.trim()||value.length>128||!storageSafeText(value))throw new ReportError(400,'invalid_report_name');return value.trim();}
function version(input:unknown){if(!Number.isSafeInteger(input)||Number(input)<1)throw new ReportError(400,'expected_version_required');return input as number;}
export function reportDraft(input:unknown,editing=false){
  if(!object(input)||Object.keys(input).some(k=>!['name','spec','timezone',...(editing?['expected_version']:[])].includes(k)))throw new ReportError(400,'invalid_report');
  let timezone:string|undefined;
  if(input.timezone!==undefined){if(typeof input.timezone!=='string'||input.timezone.length>100)throw new ReportError(400,'invalid_timezone');try{timezone=new Intl.DateTimeFormat('en',{timeZone:input.timezone}).resolvedOptions().timeZone;}catch{throw new ReportError(400,'invalid_timezone');}}
  return{name:named(input.name),spec:parseQuerySpec(input.spec),timezone,expected_version:editing?version(input.expected_version):undefined};
}
export function reportRun(spec:unknown,input:unknown):{spec:AnalysisSpec;expected_version?:number}{
  if(!object(input)||Object.keys(input).some(k=>!['from','to','filters','expected_version'].includes(k)))throw new ReportError(400,'invalid_report_run');
  if(!object(spec))throw new ReportError(400,'invalid_report');
  if((input.from===undefined)!==(input.to===undefined))throw new ReportError(400,'invalid_time_range');
  const saved=spec as AnalysisSpec;
  if(input.filters!==undefined&&!Array.isArray(input.filters))throw new ReportError(400,'invalid_filter');
  return{spec:parseQuerySpec({...saved,...(input.from!==undefined?{from:input.from,to:input.to}:{}),filters:[...(saved.filters??[]),...(input.filters as unknown[]??[])]}),expected_version:input.expected_version===undefined?undefined:version(input.expected_version)};
}
export function dashboardDraft(input:unknown,editing=false){
  if(!object(input)||Object.keys(input).some(k=>!['name','report_ids',...(editing?['expected_version']:[])].includes(k))||!Array.isArray(input.report_ids)||input.report_ids.length>50||!input.report_ids.every(reportId))throw new ReportError(400,'invalid_dashboard');
  const ids=(input.report_ids as string[]).map(id=>id.toLowerCase());if(new Set(ids).size!==ids.length)throw new ReportError(400,'invalid_dashboard');
  return{name:named(input.name),report_ids:ids,expected_version:editing?version(input.expected_version):undefined};
}
export function reportReference(input:unknown){
  if(!object(input)||Object.keys(input).some(k=>!['report_id','expected_version','overrides'].includes(k))||!reportId(input.report_id))throw new ReportError(400,'invalid_report_reference');
  const overrides=input.overrides??{};
  if(!object(overrides)||Object.keys(overrides).some(k=>!['from','to','filters'].includes(k)))throw new ReportError(400,'invalid_report_reference');
  return{report_id:input.report_id.toLowerCase(),expected_version:version(input.expected_version),overrides};
}
