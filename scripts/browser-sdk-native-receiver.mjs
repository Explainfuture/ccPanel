// Isolated synthetic SDK acceptance receiver. No credentials, filesystem writes,
// database or external network; listen on loopback only and bound request memory.
import {createServer} from 'node:http';
import {readFileSync} from 'node:fs';
const sdk=readFileSync(new URL('../packages/sdk-browser/dist/index.js',import.meta.url));
const receipts=[];let held;
const server=createServer(async(req,res)=>{
  const url=new URL(req.url,'http://127.0.0.1:5196');
  const reply=(status,body,type='application/json')=>{res.writeHead(status,{'content-type':type,'cache-control':'no-store'});res.end(typeof body==='string'?body:JSON.stringify(body));};
  if(req.method==='GET'&&['/bf-start','/bf-next'].includes(url.pathname)){
    res.writeHead(200,{'content-type':'text/html','cache-control':'private,max-age=60'});
    return res.end('<!doctype html><title>SDK BFCache fixture</title><p>Synthetic history navigation</p><a href="/bf-next">Next</a><a href="/bf-start">Start</a>');
  }
  if(req.method==='GET'&&url.pathname==='/')return reply(200,'<!doctype html><title>SDK isolated receiver</title><p>Synthetic SDK check</p>','text/html');
  if(req.method==='GET'&&url.pathname==='/sdk.js')return reply(200,sdk.toString(),'text/javascript');
  if(req.method==='GET'&&url.pathname==='/receipts')return reply(200,{receipts:receipts.filter(r=>r.project===url.searchParams.get('project'))});
  if(req.method==='GET'&&url.pathname==='/hold-state')return reply(200,{ready:held?.ready===true});
  if(req.method==='GET'&&url.pathname==='/release-hold'){if(held)held.res.end('released');return reply(200,{released:true});}
  if(req.method==='POST'&&url.pathname==='/hold'){
    if(held)return reply(429,{error:'hold_busy'});
    const hold={res,ready:false};held=hold;
    const timeout=setTimeout(()=>res.end('hold_timeout'),15000);timeout.unref();
    res.on('close',()=>{clearTimeout(timeout);if(held===hold)held=undefined;});
    try{let bytes=0;for await(const chunk of req){bytes+=chunk.length;if(bytes>65536)return reply(413,{error:'too_large'});}hold.ready=true;return;}
    catch{return reply(400,{error:'invalid_hold'});}
  }
  if(req.method!=='POST'||url.pathname!=='/v1/native')return reply(404,{error:'not_found'});
  if(receipts.length>=100)return reply(429,{error:'test_receiver_full'});
  try{
    let bytes=0;const chunks=[];
    for await(const chunk of req){bytes+=chunk.length;if(bytes>65536)return reply(413,{error:'too_large'});chunks.push(chunk);}
    const body=JSON.parse(Buffer.concat(chunks).toString());
    if(body.write_key!=='synthetic-public-key'||typeof body.project_id!=='string'||!body.project_id.startsWith('native177-')||!Array.isArray(body.events)||body.events.length!==1)return reply(400,{error:'synthetic_fixture_required'});
    if(receipts.length>=100)return reply(429,{error:'test_receiver_full'});
    receipts.push({project:body.project_id,events:body.events,mode:req.headers['sec-fetch-mode']??null});
    return reply(202,{accepted_event_ids:body.events.map(e=>e.event_id)});
  }catch{return reply(400,{error:'invalid_request'});}
});
server.maxConnections=8;
server.requestTimeout=10000;server.headersTimeout=10000;
server.listen(5196,'127.0.0.1',()=>console.log('synthetic SDK receiver listening on 127.0.0.1:5196'));
