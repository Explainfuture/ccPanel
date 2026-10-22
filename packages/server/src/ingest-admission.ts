import { defaultPressurePolicy,evaluatePressure,type PressureDecision,type PressurePolicy,type PressureReason,type PressureSnapshot } from './ingest-pressure';

export type AdmissionReason=PressureReason|'pressure_probe_unavailable'|'host_probe_unavailable'|'host_probe_invalid'|'kafka_probe_unavailable'|'ingest_reservations_full';
export interface AdmissionStatus extends Omit<PressureDecision,'reason'> {
  reason:AdmissionReason|null;
  reservedMessages:number;
  unknownMessages:number;
  pendingRequests:number;
  reservationCount:number;
}
export class IngestionPaused extends Error {
  constructor(public reason:AdmissionReason,public retryAfterSeconds:number){super('ingestion_paused');this.name='IngestionPaused';}
}
type Reservation={count:number;settledAt:number|null;confirmed:boolean};
// One intake process owns this budget. A multi-instance deployment needs shared
// admission coordination; do not mistake these reservations for a cluster quota.
export class IngestAdmission {
  private snapshot:PressureSnapshot|null=null;
  private unavailable:AdmissionReason|null='pressure_probe_unavailable';
  private integrityGap:PressureDecision['integrityReason']=null;
  private reservations=new Map<symbol,Reservation>();
  constructor(private policy:PressurePolicy={...defaultPressurePolicy},private clock=Date.now){}
  observe(snapshot:PressureSnapshot){
    const now=this.clock();
    // Late probe completions must not overwrite a newer observation.
    if(this.snapshot&&snapshot.startedAt<this.snapshot.startedAt)return;
    const decision=evaluatePressure(snapshot,this.policy,now);
    if(decision.integrityReason)this.integrityGap=decision.integrityReason;
    this.snapshot=structuredClone(snapshot);this.unavailable=null;
    if(decision.allowed){
      for(const [id,reservation] of this.reservations){
        // An acknowledged write completed before the probe began is reflected
        // in its later high watermark. Unknown sends retain an extra minute of
        // reserve; this is a conservative operating margin, not proof that an
        // arbitrarily delayed broker write can never arrive afterwards.
        const delay=reservation.confirmed?0:60000;
        if(reservation.settledAt!==null&&reservation.settledAt+delay<=snapshot.startedAt)this.reservations.delete(id);
      }
    }
  }
  restoreIntegrityGap(reason:'retained_offset_gap'|'consumer_offset_unknown'){this.integrityGap=reason;}
  probeFailed(reason:'host_probe_unavailable'|'host_probe_invalid'|'kafka_probe_unavailable'|'pressure_probe_unavailable'){
    this.unavailable=reason;
  }
  status():AdmissionStatus{
    let reservedMessages=0,unknownMessages=0,pendingRequests=0;
    for(const r of this.reservations.values()){reservedMessages+=r.count;if(r.settledAt===null)pendingRequests++;else if(!r.confirmed)unknownMessages+=r.count;}
    const decision=evaluatePressure(this.snapshot,this.policy,this.clock());
    const reason=this.integrityGap??this.unavailable??decision.reason;
    const remainingMessages=Math.max(0,decision.remainingMessages-reservedMessages);
    return {...decision,integrityReason:this.integrityGap??decision.integrityReason,allowed:!reason&&remainingMessages>0,reason:reason??(remainingMessages===0?'ingest_reservations_full':null),remainingMessages:reason?0:remainingMessages,
      retryAfterSeconds:reason?decision.retryAfterSeconds||5:remainingMessages===0?5:0,
      reservedMessages,unknownMessages,pendingRequests,reservationCount:this.reservations.size};
  }
  reserve(count:number):{settle:(confirmed:boolean)=>void}{
    if(!Number.isSafeInteger(count)||count<1||count>100)throw new Error('invalid_ingest_reservation');
    const status=this.status();
    if(!status.allowed||count>status.remainingMessages)throw new IngestionPaused(status.reason??'ingest_reservations_full',status.retryAfterSeconds||5);
    // There is no await between evaluating headroom and consuming it. Every
    // concurrent HTTP/replay attempt is counted, including duplicate event IDs.
    const id=Symbol('publication'),reservation:Reservation={count,settledAt:null,confirmed:false};
    this.reservations.set(id,reservation);
    return {settle:(confirmed)=>{if(reservation.settledAt!==null)return;reservation.confirmed=confirmed;reservation.settledAt=this.clock();}};
  }
}
