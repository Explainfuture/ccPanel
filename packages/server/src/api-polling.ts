// Each API runner keeps its own phase and existing busy/error handling. The
// initial delay changes only startup timing, not the fixed polling cadence.
export function startApiPolling(tick:()=>void,initialDelayMs=2000):()=>void{
  if(!Number.isInteger(initialDelayMs)||initialDelayMs<0||initialDelayMs>2000)throw new Error('invalid_api_polling_delay');
  let stopped=false,interval:ReturnType<typeof setInterval>|undefined;
  const first=setTimeout(()=>{
    if(stopped)return;
    // Arm first so a tick that stops its own runner also cancels the interval.
    interval=setInterval(()=>{if(!stopped)tick();},2000);interval.unref();
    tick();
  },initialDelayMs);
  first.unref();
  return()=>{
    if(stopped)return;
    stopped=true;clearTimeout(first);if(interval)clearInterval(interval);
  };
}
