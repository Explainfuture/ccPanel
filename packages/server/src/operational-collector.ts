import type {OperationalSnapshot} from './operational-snapshot';

// One snapshot in flight, no pending backlog and no payload/error logging.
export function startOperationalCollector(options:{snapshot:()=>OperationalSnapshot;write:(value:OperationalSnapshot)=>Promise<unknown>;failed:()=>void}){
  let stopped=false,inFlight:Promise<void>|undefined;
  const tick=()=>{
    if(stopped||inFlight)return;
    inFlight=Promise.resolve().then(()=>options.write(options.snapshot())).then(()=>{},()=>{try{options.failed();}catch{/* Monitoring must not become an unhandled rejection. */}}).finally(()=>{inFlight=undefined;});
  };
  const timer=setInterval(tick,30000);timer.unref();
  return async()=>{stopped=true;clearInterval(timer);await inFlight;};
}
