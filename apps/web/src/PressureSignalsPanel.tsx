import {useEffect,useState} from 'react';
import type {PressureAlerts,PressureSignal} from '../../../packages/server/src/pressure-alerts';
const labels={kafka_backlog:'Kafka 消费积压',ingest_integrity:'消费完整性保护',storage_headroom:'存储余量'};
const reasons:Record<string,string>={missing_snapshot:'尚无采样',invalid_snapshot:'采样未通过校验',future_sample:'采样时间位于未来',probe_unavailable:'最近一次探测失败',stale_sample:'采样已过期',consumer_offset_unknown:'消费位点无法确认',retained_offset_gap:'发现消息保留缺口',offsets_consistent:'消费位点在可用范围内',consumer_lag_high:'积压达到条数预算',consumer_lag_old:'存在早于年龄界线的积压',lag_within_budget:'积压在预算内',disk_space_low:'可用空间达到预留线',disk_inodes_low:'可用文件节点达到预留线',headroom_available:'空间与文件节点余量充足'};
const timestamp=(v:unknown):v is string=>typeof v==='string'&&Number.isFinite(Date.parse(v))&&new Date(v).toISOString()===v;
const decimal=(v:unknown):v is string=>typeof v==='string'&&/^(0|[1-9]\d{0,39})$/.test(v);
export {labels as pressureLabels,reasons as pressureReasons,valid as validPressureAlerts};
const count=(v:string|null)=>v===null?'未确认':new Intl.NumberFormat('zh-CN').format(BigInt(v));
function valid(data:PressureAlerts|null|undefined):data is PressureAlerts{
  if(!data||data.scope!=='platform_pressure'||!timestamp(data.computed_at)||!Array.isArray(data.signals)||data.signals.length!==3||new Set(data.signals.map(r=>r?.rule)).size!==3||!data.policy)return false;
  const p=data.policy;if(![p.max_lag_messages,p.min_free_bytes,p.min_free_inodes].every(decimal)||BigInt(p.max_lag_messages)<=0n||![p.max_lag_age_ms,p.max_probe_age_ms,p.max_disk_age_ms,p.min_free_percent].every(v=>Number.isSafeInteger(v)&&v>=0)||p.max_probe_age_ms<1||p.max_probe_age_ms>60000||p.max_disk_age_ms<1||p.max_disk_age_ms>120000||p.min_free_percent<1||p.min_free_percent>100)return false;
  return data.signals.every(r=>{
    if(!r||!Object.hasOwn(labels,r.rule)||!['clear','firing','unknown'].includes(r.state)||!Object.hasOwn(reasons,r.reason))return false;
    if(r.observed_at===null||r.expires_at===null){if(r.observed_at!==null||r.expires_at!==null||r.state!=='unknown')return false;}
    else if(!timestamp(r.observed_at)||!timestamp(r.expires_at)||Date.parse(r.observed_at)>Date.parse(data.computed_at)||Date.parse(r.expires_at)<Date.parse(data.computed_at))return false;
    if(r.rule==='kafka_backlog'&&r.state!=='unknown')return decimal(r.max_lag_messages)&&decimal(r.total_lag_messages)&&BigInt(r.max_lag_messages)<=BigInt(r.total_lag_messages);
    return r.max_lag_messages===null&&r.total_lag_messages===null;
  });
}
export default function PressureSignalsPanel({data,requestedAt}:{data:PressureAlerts|null|undefined;requestedAt:number}){
  const [clock,setClock]=useState(()=>performance.now()),usable=valid(data);
  const elapsed=Math.max(0,clock-requestedAt);
  const expired=(r:PressureSignal)=>r.expires_at!==null&&Date.parse(r.expires_at)-Date.parse(data!.computed_at)<elapsed;
  useEffect(()=>{
    if(!usable)return;
    const fresh=(at:number)=>data.signals.some(r=>r.state!=='unknown'&&r.expires_at!==null&&Date.parse(r.expires_at)-Date.parse(data.computed_at)>=Math.max(0,at-requestedAt));
    if(!fresh(performance.now()))return;
    const tick=()=>{const now=performance.now();setClock(now);if(!fresh(now))clearInterval(timer);},timer=setInterval(tick,1000);
    document.addEventListener('visibilitychange',tick);
    return()=>{clearInterval(timer);document.removeEventListener('visibilitychange',tick);};
  },[data,requestedAt,usable]);
  return <section className="panel" aria-label="平台压力信号"><div className="panel-heading"><div><h2>平台压力信号</h2><p className="muted">当前 API 实例缓存的共享 Kafka 与磁盘汇总，需平台运维资格。</p></div></div>
    {!usable?<div className="panel-body"><p className="muted" role="status">尚无有效压力信号，请刷新监控。</p></div>:<><div className="table-scroll" tabIndex={0} aria-label="压力信号明细，可横向滚动"><table><thead><tr><th>信号</th><th>状态</th><th>读取时原因</th><th>读取时积压：总量 / 最大分区</th></tr></thead><tbody>{data.signals.map(r=><tr key={r.rule}><td>{labels[r.rule]}</td><td><span role="status" className={expired(r)||r.state==='unknown'?'text-amber-700':r.state==='firing'?'text-red-700':'text-emerald-700'}>{expired(r)?'已过期，当前未知':r.state==='clear'?'未触发':r.state==='firing'?'触发':'未知'}</span></td><td>{reasons[r.reason]}</td><td>{r.rule==='kafka_backlog'?count(r.total_lag_messages)+' / '+count(r.max_lag_messages):'不适用'}</td></tr>)}</tbody></table></div>
      <div className="panel-body"><p className="fine-print">评估于 {new Date(data.computed_at).toLocaleString('zh-CN')}。页面计时包含本次请求等待；过期后当前状态未知，刷新监控可重新读取。窄屏可横向滚动查看积压明细。</p><p className="fine-print">分区积压触发线 {count(data.policy.max_lag_messages)} 条；年龄预算 {(data.policy.max_lag_age_ms/1000).toLocaleString('zh-CN')} 秒。可用空间预留 {count(data.policy.min_free_bytes)} 字节或 {data.policy.min_free_percent}%（取较大值），文件节点预留 {count(data.policy.min_free_inodes)} 个。</p><p className="fine-print">完整性暂停由离线核对流程处理。下方可手动读取采样历史；外部通知仍待接入。</p></div></>}
  </section>;
}
