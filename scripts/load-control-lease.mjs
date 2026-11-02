import {Readable} from 'node:stream';
import {performance} from 'node:perf_hooks';

const token=/^[A-Za-z0-9_-]{16,128}$/;
const sequence=/^[1-9][0-9]{0,18}$/;
const maximumSequence=9223372036854775807n;
const maximumFrameBytes=256; // Includes the required LF.

export class ControlLeaseError extends Error{
  constructor(code){super(code);this.name='ControlLeaseError';this.code=code;}
}

/**
 * Opt-in safety lease over a dedicated, unencoded stdin pipe. A valid frame is
 * exactly JSON.stringify({v:1,run_id:runId,seq:positiveInt64Text}) + '\n'.
 * ready only authorizes the first lease; onAbort remains authoritative later.
 * No remote wall clock, files, requests, or background manifest writes are used.
 */
export function attachControlLease({input,runId,onAbort,timeoutMs=5000}={}){
  if(!(input instanceof Readable)||input.isTTY===true||input.readableEncoding!==null||input.destroyed||input.readableEnded||input.readableDidRead===true||input.readableFlowing===true||input.listenerCount('data')!==0||input.listenerCount('readable')!==0||typeof runId!=='string'||!token.test(runId)||typeof onAbort!=='function'||!Number.isSafeInteger(timeoutMs)||timeoutMs<100||timeoutMs>60000)throw new TypeError('invalid_control_lease_options');
  let active=true,readySettled=false,lastSequence=0n,pending=Buffer.alloc(0),timer;
  let resolveReady,rejectReady;
  const ready=new Promise((resolve,reject)=>{resolveReady=resolve;rejectReady=reject;});
  // The caller may await readiness after another async setup operation. Keep
  // the original promise rejecting without creating an unhandled rejection.
  void ready.catch(()=>{});
  let deadline=performance.now()+timeoutMs;

  const dispose=()=>{
    active=false;clearTimeout(timer);pending=Buffer.alloc(0);
    input.removeListener('data',data);input.removeListener('end',eof);input.removeListener('close',eof);input.removeListener('error',inputError);
    input.pause();
    // Some native stdin sockets remain referenced after pausing a delivered
    // chunk. This stream is exclusively owned; release its public ref when
    // available without destroying the caller's stream or touching _handle.
    if(typeof input.unref==='function')input.unref();
  };
  const fail=reason=>{
    if(!active)return;
    dispose();
    if(!readySettled){readySettled=true;rejectReady(new ControlLeaseError(reason));}
    // Never silently resume a workload if its abort callback itself fails.
    // Only the fixed safety error escapes, without the callback's payload.
    try{onAbort(reason);}catch{throw new ControlLeaseError('control_abort_callback_failed');}
  };
  const expired=()=>{
    if(!active)return true;
    if(performance.now()>=deadline){fail('control_timeout');return true;}
    return false;
  };
  const schedule=()=>{
    clearTimeout(timer);
    timer=setTimeout(()=>{if(!expired())schedule();},Math.max(1,Math.ceil(deadline-performance.now())));
  };
  function accept(frame){
    if(expired())return;
    // The entire protocol is ASCII. Reject invalid UTF-8/control bytes rather
    // than permitting Buffer's replacement decoding to change the protocol.
    for(const byte of frame)if(byte<32||byte>126){fail('control_frame_invalid');return;}
    const raw=frame.toString('ascii');let value;
    try{value=JSON.parse(raw);}catch{fail('control_frame_invalid');return;}
    if(!value||typeof value!=='object'||Array.isArray(value)||Object.keys(value).length!==3||value.v!==1||typeof value.run_id!=='string'||!token.test(value.run_id)||typeof value.seq!=='string'||!sequence.test(value.seq)||BigInt(value.seq)>maximumSequence||JSON.stringify({v:1,run_id:value.run_id,seq:value.seq})!==raw){fail('control_frame_invalid');return;}
    if(value.run_id!==runId){fail('control_run_mismatch');return;}
    const next=BigInt(value.seq);
    if(next<lastSequence){fail('control_sequence_regressed');return;}
    if(next===lastSequence)return; // Replay is valid syntax, never a renewal.
    lastSequence=next;deadline=performance.now()+timeoutMs;schedule();
    if(!readySettled){readySettled=true;resolveReady();}
  }
  function data(chunk){
    if(expired())return;
    if(!Buffer.isBuffer(chunk)&&!(chunk instanceof Uint8Array)){fail('control_frame_invalid');return;}
    const bytes=Buffer.isBuffer(chunk)?chunk:Buffer.from(chunk.buffer,chunk.byteOffset,chunk.byteLength);
    let offset=0;
    while(active&&offset<bytes.length){
      if(expired())return;
      const lf=bytes.indexOf(10,offset),end=lf===-1?bytes.length:lf+1,length=pending.length+end-offset;
      if(length>maximumFrameBytes||(lf===-1&&length===maximumFrameBytes)){fail('control_frame_too_large');return;}
      const part=bytes.subarray(offset,end);
      pending=pending.length?Buffer.concat([pending,part],length):Buffer.from(part);
      offset=end;
      if(lf===-1)return;
      const frame=pending.subarray(0,-1);pending=Buffer.alloc(0);accept(frame);
    }
  }
  function eof(){fail('control_eof');}
  function inputError(){fail('control_input_error');}
  input.on('data',data);input.on('end',eof);input.on('close',eof);input.on('error',inputError);
  schedule();input.resume();
  return{ready,close(){
    if(!active)return;
    dispose();
    if(!readySettled){readySettled=true;rejectReady(new ControlLeaseError('control_lease_closed'));}
  }};
}
