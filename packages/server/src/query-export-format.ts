export class ExportError extends Error{
  constructor(public status:400|403|404|409|413|429,message:string){super(message);}
}
export const exportMaxBytes=1024*1024;
// Keep JSON types explicit in CSV, including null/empty collections and numeric
// strings. JSON-pointer paths cover every result kind without dropping groups,
// immature retention cells, report snapshots or truncation indicators.
export function formatQueryExport(input:Record<string,unknown>,format:'json'|'csv'){
  const encoded=JSON.stringify(input);if(Buffer.byteLength(encoded)>2*1024*1024)throw new ExportError(413,'export_limits_exceeded');
  if(format==='json'){
    const body=JSON.stringify(JSON.parse(encoded),null,2)+'\n';
    if(Buffer.byteLength(body)>exportMaxBytes)throw new ExportError(413,'export_limits_exceeded');return body;
  }
  const rows=['\ufeff"path","type","value"\r\n'];let bytes=Buffer.byteLength(rows[0]!),count=0;
  // Text prefixes follow OWASP's spreadsheet-view mitigation; raw values are
  // available in JSON. Never interpret the CSV display text as a lossless API.
  const quote=(value:string,untrusted=false)=>'"'+((untrusted?'\t':'')+value).replaceAll('"','""')+'"';
  const append=(path:string,type:string,value:string)=>{
    const row=[quote(path,true),quote(type),quote(value,type==='string')].join(',')+'\r\n';bytes+=Buffer.byteLength(row);
    if(++count>10000||bytes>exportMaxBytes)throw new ExportError(413,'export_limits_exceeded');rows.push(row);
  };
  function walk(value:unknown,path:string,depth:number){
    if(depth>32)throw new ExportError(413,'export_limits_exceeded');
    if(value===null){append(path,'null','');return;}
    if(typeof value!=='object'){append(path,typeof value,String(value));return;}
    const entries=Object.entries(value);if(!entries.length){append(path,Array.isArray(value)?'array':'object',Array.isArray(value)?'[]':'{}');return;}
    for(const [key,next]of entries)walk(next,path+'/'+key.replaceAll('~','~0').replaceAll('/','~1'),depth+1);
  }
  walk(JSON.parse(encoded),'',0);return rows.join('');
}
