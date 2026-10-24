import { expect,it,vi } from 'vitest';
import type { Admin } from 'kafkajs';
import { readKafkaPressure } from '../packages/server/src/kafka-pressure';
import { defaultPressurePolicy,evaluatePressure } from '../packages/server/src/ingest-pressure';
const now=Date.parse('2026-10-23T00:00:00Z');
function reader(){
  return {fetchOffsets:vi.fn().mockResolvedValue([{topic:'events',partitions:[{partition:1,offset:'-1'},{partition:0,offset:'9007199254741000'}]}]),fetchTopicOffsetsByTimestamp:vi.fn().mockResolvedValue([{partition:0,offset:'9007199254740990'},{partition:1,offset:'0'}]),fetchTopicOffsets:vi.fn().mockResolvedValue([{partition:0,low:'9007199254740990',high:'9007199254741001'},{partition:1,low:'0',high:'0'}])};
}
it('reads commits without resolving or mutating them and aligns partition IDs rather than array positions',async()=>{
  const r=reader();const value=await readKafkaPressure(r as unknown as Admin,'events','writer',defaultPressurePolicy,[],()=>now);
  expect(r.fetchOffsets).toHaveBeenCalledWith({groupId:'writer',topics:['events'],resolveOffsets:false});
  expect(r.fetchTopicOffsetsByTimestamp).toHaveBeenCalledWith('events',now-defaultPressurePolicy.maxLagAgeMs);
  expect(r.fetchOffsets.mock.invocationCallOrder[0]).toBeLessThan(r.fetchTopicOffsetsByTimestamp.mock.invocationCallOrder[0]!);
  expect(r.fetchTopicOffsetsByTimestamp.mock.invocationCallOrder[0]).toBeLessThan(r.fetchTopicOffsets.mock.invocationCallOrder[0]!);
  expect(value.partitions[0]).toMatchObject({committed:'9007199254741000',high:'9007199254741001'});
  expect(value.partitions[1]?.committed).toBeNull();
});
it('preserves the actual commit when retention moves during observation',async()=>{
  const r=reader();r.fetchTopicOffsets.mockResolvedValue([{partition:0,low:'9007199254741001',high:'9007199254741001'},{partition:1,low:'0',high:'0'}]);
  const disks=['kafka','postgres'].map(kind=>({kind:kind as 'kafka'|'postgres',sampledAt:now,totalBytes:'21474836480',availableBytes:'12884901888',totalInodes:'100000',availableInodes:'50000'}));
  const value=await readKafkaPressure(r as unknown as Admin,'events','writer',defaultPressurePolicy,disks,()=>now);
  expect(value.partitions[0]?.ageBoundary).toBe('9007199254741001');
  expect(value.partitions[0]?.committed).toBe('9007199254741000');
  expect(evaluatePressure(value,defaultPressurePolicy,now).reason).toBe('retained_offset_gap');
});
it('rejects partial metadata and partition changes instead of inventing missing commits',async()=>{
  for(const alter of [(r:ReturnType<typeof reader>)=>r.fetchOffsets.mockResolvedValue([]),(r:ReturnType<typeof reader>)=>r.fetchTopicOffsets.mockResolvedValue([{partition:0,low:'0',high:'9007199254741001'}]),(r:ReturnType<typeof reader>)=>r.fetchTopicOffsetsByTimestamp.mockResolvedValue([{partition:0,offset:'0'},{partition:0,offset:'0'}]),(r:ReturnType<typeof reader>)=>r.fetchOffsets.mockResolvedValue([{topic:'events',partitions:[{partition:0,offset:'-2'},{partition:1,offset:'0'}]}])]){const r=reader();alter(r);await expect(readKafkaPressure(r as unknown as Admin,'events','writer',defaultPressurePolicy,[],()=>now)).rejects.toThrow('invalid_kafka_pressure_observation');}
});
