import {expect,it} from 'vitest';
import {formatQueryExport,ExportError,exportMaxBytes} from '../packages/server/src/query-export-format';
it('keeps complete typed JSON metadata, nulls, numeric strings and result flags',()=>{
 const data={timezone:'UTC',report_snapshot:{version:2},result:{kind:'retention',rate:null,mature:false,code:'20',empty:[],groups_truncated:true}};
 expect(JSON.parse(formatQueryExport(data,'json'))).toEqual(data);
});
it('writes typed CSV pointers and quotes without activating text formula prefixes',()=>{
 const data={result:{'a/b~c':'=1+2",;@bad',code:'20',negative:-2,missing:null,empty:[],empty_object:{},full:'＝1+2',control:'\r\n=1+2'}};
 const csv=formatQueryExport(data,'csv');
 expect(csv.startsWith('\ufeff"path","type","value"\r\n')).toBe(true);
 expect(csv).toContain('"\t/result/a~1b~0c","string","\t=1+2"",;@bad"\r\n');
 expect(csv).toContain('"\t/result/code","string","\t20"');
 expect(csv).toContain('"\t/result/full","string","\t＝1+2"');
 expect(csv).toContain('"\t/result/negative","number","-2"');
 expect(csv).toContain('"\t/result/missing","null",""');
 expect(csv).toContain('"array","[]"');expect(csv).toContain('"object","{}"');
 expect(JSON.parse(formatQueryExport(data,'json'))).toEqual(data);
});
it('rejects output, input, row and nesting amplification instead of truncating exports',()=>{
 for(const format of ['json','csv'] as const)expect(()=>formatQueryExport({value:'x'.repeat(exportMaxBytes+1)},format)).toThrow(ExportError);
 expect(()=>formatQueryExport({value:'x'.repeat(2*exportMaxBytes+1)},'csv')).toThrow('export_limits_exceeded');
 expect(()=>formatQueryExport({values:Array.from({length:10001},()=>0)},'csv')).toThrow('export_limits_exceeded');
 let nested:unknown=0;for(let i=0;i<34;i++)nested={a:nested};expect(()=>formatQueryExport({nested},'csv')).toThrow('export_limits_exceeded');
});
