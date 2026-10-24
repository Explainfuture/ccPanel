import type { AdmissionStatus } from './ingest-admission';
import { IngestAdmission } from './ingest-admission';
import type { DiskPressure,PressureSnapshot } from './ingest-pressure';
import { HostTelemetryError,type HostTelemetry } from './host-telemetry';
import { ConsumerProgressError } from './consumer-progress';

export type IntegrityReason='retained_offset_gap'|'consumer_offset_unknown';
export interface IngestProbeDeps {
  readControl:()=>Promise<{blocked:boolean;reason:IntegrityReason|null}>;
  readHost:()=>Promise<HostTelemetry>;
  readKafka:(disks:DiskPressure[])=>Promise<PressureSnapshot>;
  validateProgress:(snapshot:PressureSnapshot)=>Promise<void>;
  persistGap:(reason:IntegrityReason,snapshot:PressureSnapshot)=>Promise<void>;
}
export async function refreshIngestPressure(gate:IngestAdmission,deps:IngestProbeDeps):Promise<AdmissionStatus>{
  let phase:'control'|'host'|'kafka'='control';
  try{
    const control=await deps.readControl();
    if(control.blocked){if(!control.reason)throw new Error('invalid_integrity_control');gate.restoreIntegrityGap(control.reason);}
    phase='host';const host=await deps.readHost();
    phase='kafka';const snapshot=await deps.readKafka(host.disks);
    phase='control';await deps.validateProgress(snapshot);
    gate.observe(snapshot);
    const reason=gate.status().integrityReason;
    if(reason&&!control.blocked){phase='control';await deps.persistGap(reason,snapshot);}
  }catch(error){
    if(error instanceof ConsumerProgressError)gate.restoreIntegrityGap(error.reason);
    gate.probeFailed(error instanceof HostTelemetryError?error.code:phase==='kafka'?'kafka_probe_unavailable':'pressure_probe_unavailable');
  }
  return gate.status();
}
