import { expect,it,vi } from 'vitest';
import { IngestAdmission } from '../packages/server/src/ingest-admission';
import { refreshIngestPressure,type IngestProbeDeps } from '../packages/server/src/ingest-runtime';
import { defaultPressurePolicy,type PressureSnapshot } from '../packages/server/src/ingest-pressure';
import { HostTelemetryError,type HostTelemetry } from '../packages/server/src/host-telemetry';
const now=Date.parse('2026-10-23T00:00:00Z');
const snapshot=():PressureSnapshot=>({startedAt:now,partitions:[{partition:0,low:'0',high:'0',committed:'0',ageBoundary:'0'}],disks:['kafka','postgres'].map(kind=>({kind:kind as 'kafka'|'postgres',sampledAt:now,totalBytes:'21474836480',availableBytes:'12884901888',totalInodes:'100000',availableInodes:'50000'}))});
function setup(){const gate=new IngestAdmission({...defaultPressurePolicy},()=>now),deps={readControl:vi.fn().mockResolvedValue({blocked:false,reason:null}),readHost:vi.fn().mockResolvedValue({disks:snapshot().disks} as HostTelemetry),readKafka:vi.fn().mockResolvedValue(snapshot()),persistGap:vi.fn().mockResolvedValue(undefined)};return {gate,deps};}
it('opens only after control, host and Kafka observations all succeed',async()=>{const {gate,deps}=setup();expect((await refreshIngestPressure(gate,deps)).allowed).toBe(true);expect(deps.persistGap).not.toHaveBeenCalled();});
it('keeps unavailable host, broker and control observations distinct and closed',async()=>{
  for(const [operation,error,reason] of [['readHost',new HostTelemetryError('host_probe_unavailable'),'host_probe_unavailable'],['readKafka',new Error('broker offline'),'kafka_probe_unavailable'],['readControl',new Error('database offline'),'pressure_probe_unavailable']] as const){const {gate,deps}=setup();deps[operation].mockRejectedValue(error);expect(await refreshIngestPressure(gate,deps)).toMatchObject({allowed:false,reason});}
});
it('does not reopen when persisting an observed retention gap fails',async()=>{
  const {gate,deps}=setup(),gap=snapshot();gap.partitions[0]={partition:0,low:'20',high:'100',committed:'10',ageBoundary:'20'};deps.readKafka.mockResolvedValue(gap);deps.persistGap.mockRejectedValue(new Error('database unavailable'));
  expect(await refreshIngestPressure(gate,deps)).toMatchObject({allowed:false,integrityReason:'retained_offset_gap'});expect(deps.persistGap).toHaveBeenCalledWith('retained_offset_gap',gap);
});
it('restores a durable pause into a new process even after offsets appear healthy',async()=>{
  const {gate,deps}=setup();deps.readControl.mockResolvedValue({blocked:true,reason:'retained_offset_gap'});
  expect(await refreshIngestPressure(gate,deps as IngestProbeDeps)).toMatchObject({allowed:false,reason:'retained_offset_gap'});expect(deps.persistGap).not.toHaveBeenCalled();
});
