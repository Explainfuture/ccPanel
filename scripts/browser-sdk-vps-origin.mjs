import {createServer} from 'node:http';
import {readFileSync,writeFileSync} from 'node:fs';
const fixture=JSON.parse(readFileSync('.local/browser178-fixture.json','utf8')),sdk=readFileSync('packages/sdk-browser/dist/index.js');
createServer(async(req,res)=>{
 const url=new URL(req.url,'http://127.0.0.1:5197');res.setHeader('cache-control','no-store');
 if(req.method==='GET'&&url.pathname==='/'){res.setHeader('content-type','text/html');return res.end('<!doctype html><title>Isolated VPS SDK acceptance</title>');}
 if(req.method==='GET'&&url.pathname==='/sdk.js'){res.setHeader('content-type','text/javascript');return res.end(sdk);}
 if(req.method==='GET'&&url.pathname==='/fixture'){res.setHeader('content-type','application/json');return res.end(JSON.stringify({endpoint:fixture.origin+'/v1/batch',projectId:fixture.project,writeKey:fixture.key}));}
 if(req.method==='POST'&&url.pathname==='/result'){const chunks=[];let size=0;for await(const c of req){size+=c.length;if(size>4096){res.writeHead(413);return res.end();}chunks.push(c);}const r=JSON.parse(Buffer.concat(chunks));if(!r.event_id||!r.event_time){res.writeHead(400);return res.end();}writeFileSync('.local/browser178-event.json',JSON.stringify(r),{mode:0o600});return res.end('saved');}
 res.writeHead(404);res.end();
}).listen(5197,'127.0.0.1',()=>console.log('browser178 synthetic SDK origin ready'));
