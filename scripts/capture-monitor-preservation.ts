import {captureMonitorPreservation,MonitorCaptureError} from '../packages/server/src/monitor-preservation-capture';
try{
  const [sourceLabel,output,...extra]=process.argv.slice(2),connectionString=process.env.CCPANEL_MONITOR_DATABASE_URL;
  if(!sourceLabel||!output||extra.length||!connectionString)throw new MonitorCaptureError('usage_capture_monitor_source_output');
  const timeout=process.env.CCPANEL_MONITOR_CAPTURE_TIMEOUT_MS;
  if(timeout!==undefined&&!/^[1-9][0-9]*$/.test(timeout))throw new MonitorCaptureError('monitor_capture_options_invalid');
  const result=await captureMonitorPreservation({connectionString,sourceLabel,output,expectedSchema:22,timeoutMs:timeout===undefined?undefined:Number(timeout)});
  console.log(JSON.stringify(result));
}catch(error){console.error(JSON.stringify({code:error instanceof MonitorCaptureError?error.code:'monitor_capture_failed'}));process.exitCode=1;}
