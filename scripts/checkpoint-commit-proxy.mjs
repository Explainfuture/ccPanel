// Disposable plaintext PG protocol fault proxy. Never logs SQL/auth/payloads.
// Stop the first checkpoint COMMIT response only AFTER the server confirms it.
import net from 'node:net';
import {writeFileSync} from 'node:fs';
const max=1024*1024,connections=new Set();let used=false;
function query(packet){
  const type=String.fromCharCode(packet[0]);
  if(type==='Q')return packet.subarray(5,-1).toString('utf8');
  if(type==='P'){const end=packet.indexOf(0,5);if(end<0)return '';const last=packet.indexOf(0,end+1);return last<0?'':packet.subarray(end+1,last).toString('utf8');}
  return '';
}
function parser(startup,onPacket){
  let buffer=Buffer.alloc(0),initial=startup;
  return chunk=>{
    buffer=Buffer.concat([buffer,chunk]);if(buffer.length>max*2)throw new Error('oversized_frame');
    while(buffer.length>=(initial?4:5)){
      const length=buffer.readInt32BE(initial?0:1),total=length+(initial?0:1);
      if(length<(initial?8:4)||total>max)throw new Error('invalid_frame');
      if(buffer.length<total)return;
      const packet=buffer.subarray(0,total);buffer=buffer.subarray(total);
      if(initial&&packet.readInt32BE(4)!==196608)throw new Error('plaintext_startup_required');
      onPacket(packet,initial);initial=false;
    }
  };
}
const server=net.createServer(client=>{
  const upstream=net.connect({host:'postgres',port:5432});connections.add(client);connections.add(upstream);
  let checkpoint=false,awaiting=false,held=false;
  const close=()=>{client.destroy();upstream.destroy();connections.delete(client);connections.delete(upstream);};
  function forward(source,target,packet){if(!target.write(packet)){source.pause();target.once('drain',()=>{if(!source.destroyed)source.resume();});}}
  const frontend=parser(true,(packet,initial)=>{
    if(!initial){const sql=query(packet).trim();
      if(/^BEGIN\b/i.test(sql))checkpoint=false;
      if(/^UPDATE\s+consumer_progress\b/i.test(sql))checkpoint=true;
      if(/^COMMIT\b/i.test(sql)&&checkpoint)awaiting=true;
    }
    forward(client,upstream,packet);
  });
  const backend=parser(false,packet=>{
    if(held)return;
    if(awaiting&&packet[0]===67&&packet.subarray(5).toString('utf8')==='COMMIT\0'&&!used){
      used=true;held=true;
      writeFileSync('/control/server-committed',JSON.stringify({checkpoint_commit_confirmed:true,response_withheld:true}),{mode:0o600});
      return;
    }
    if(packet[0]===90)awaiting=false;
    forward(upstream,client,packet);
  });
  client.on('data',chunk=>{try{frontend(chunk);}catch{console.error('{"code":"fault_proxy_protocol_error"}');close();}});
  upstream.on('data',chunk=>{try{backend(chunk);}catch{console.error('{"code":"fault_proxy_protocol_error"}');close();}});
  for(const socket of [client,upstream]){socket.on('error',close);socket.on('close',close);}
});
server.listen(15432,'0.0.0.0',()=>writeFileSync('/control/proxy-ready','ready',{mode:0o600}));
for(const signal of ['SIGINT','SIGTERM'])process.once(signal,()=>{for(const socket of connections)socket.destroy();server.close(()=>process.exit(0));});
