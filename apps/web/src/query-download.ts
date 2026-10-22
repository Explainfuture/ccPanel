import {ApiError} from './api';
const uuid='[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}';
const maxBytes=1024*1024;
export async function downloadQueryResult(prefix:string,id:string,format:'json'|'csv',signal:AbortSignal){
  if(!new RegExp('^/api/tenants/'+uuid+'/projects/'+uuid+'$','i').test(prefix)||!new RegExp('^'+uuid+'$','i').test(id))throw new ApiError('not_found',404);
  const response=await fetch(`${prefix}/query-jobs/${id}/download?format=${format}`,{credentials:'same-origin',signal});
  if(!response.ok){const body=await response.json().catch(()=>({error:'request_failed'}));throw new ApiError(body.error||'request_failed',response.status);}
  const type=response.headers.get('content-type')||'';
  if(!type.startsWith(format==='csv'?'text/csv':'application/json')||!response.body)throw new ApiError('invalid_response',502);
  const reader=response.body.getReader(),chunks:ArrayBuffer[]=[];let size=0;
  try{
    while(true){const part=await reader.read();if(part.done)break;size+=part.value.byteLength;
      if(size>maxBytes){await reader.cancel();throw new ApiError('export_limits_exceeded',413);}
      const copy=new Uint8Array(part.value.byteLength);copy.set(part.value);chunks.push(copy.buffer);
    }
  }finally{reader.releaseLock();}
  if(size===0)throw new ApiError('invalid_response',502);
  if(signal.aborted)throw new DOMException('Download aborted','AbortError');
  const length=response.headers.get('content-length');
  if(!response.headers.has('content-encoding')&&length!==null&&Number(length)!==size)throw new ApiError('invalid_response',502);
  const url=URL.createObjectURL(new Blob(chunks,{type})),anchor=document.createElement('a');
  anchor.href=url;anchor.download=`ccpanel-${id.toLowerCase()}.${format}`;
  try{document.body.append(anchor);anchor.click();}finally{anchor.remove();setTimeout(()=>URL.revokeObjectURL(url),1000);}
}
