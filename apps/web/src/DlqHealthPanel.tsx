import {useEffect,useState} from 'react';
import {api,ApiError} from './api';
import {Button} from './components/ui/button';
import {parseDlqHealth} from './dlq-health-view';
import type {DlqHealth,DlqHealthSignal} from '../../../packages/server/src/dlq-health';
const labels={dlq_pending:'待处理来源',dlq_retention_risk:'保留窗口风险'};
const categories={unresolved:'未解决',replaying:'等待重放回执',resolved:'有成功回执',suppressed:'隐私抑制'};
const reasons:Record<string,string>={missing_snapshot:'尚无采样',invalid_snapshot:'采样未通过校验',future_sample:'采样时间位于未来',stale_sample:'采样已过期',future_record:'存在未来时间的来源，无法可靠判断',retained_pending:'仍有未解决或等待回执的来源',no_retained_pending:'未发现保留的待处理来源',below_retention_warning:'待处理来源尚未达到保留预警线',retention_window_near:'最老待处理来源已达到12天',retention_window_exceeded:'最老待处理来源已达到或超过14天'};
const count=(v:string|null)=>v===null?'未确认':new Intl.NumberFormat('zh-CN').format(BigInt(v));
export default function DlqHealthPanel({prefix}:{prefix:string}){
  const [request,setRequest]=useState(0),[data,setData]=useState<DlqHealth|null>(null),[busy,setBusy]=useState(false),[error,setError]=useState(''),[requestedAt,setRequestedAt]=useState(0),[clock,setClock]=useState(0);
  useEffect(()=>{
    if(!request)return;const controller=new AbortController(),started=performance.now();setData(null);setError('');setBusy(true);
    void api<unknown>(prefix+'/dlq-health',{signal:AbortSignal.any([controller.signal,AbortSignal.timeout(10000)])}).then(raw=>{
      if(controller.signal.aborted)return;const parsed=parseDlqHealth(raw);if(!parsed){setError('DLQ 风险响应异常，旧数据已清除。');return;}setData(parsed);setRequestedAt(started);setClock(performance.now());
    },reason=>{if(!controller.signal.aborted)setError(reason instanceof ApiError&&[401,403,404].includes(reason.status)?'DLQ 风险读取权限已失效，旧数据已清除。':reason instanceof ApiError&&reason.status===429?'读取过于频繁，请稍后重试。':'DLQ 风险读取失败，旧数据已清除。');}).finally(()=>{if(!controller.signal.aborted)setBusy(false);});
    return()=>controller.abort();
  },[prefix,request]);
  useEffect(()=>{
    if(!data)return;const fresh=(at:number)=>data.signals.some(s=>s.expires_at!==null&&Date.parse(s.expires_at)-Date.parse(data.computed_at)>=Math.max(0,at-requestedAt));
    if(!fresh(performance.now()))return;
    const tick=()=>{const now=performance.now();setClock(now);if(!fresh(now))clearInterval(timer);},timer=setInterval(tick,1000);
    document.addEventListener('visibilitychange',tick);return()=>{clearInterval(timer);document.removeEventListener('visibilitychange',tick);};
  },[data,requestedAt]);
  const expired=(s:DlqHealthSignal)=>s.expires_at!==null&&Date.parse(s.expires_at)-Date.parse(data!.computed_at)<Math.max(0,clock-requestedAt);
  return <section className="panel" aria-label="平台 DLQ 风险"><div className="panel-heading"><div><h2>平台 DLQ 风险</h2><p className="muted">跨公司来源索引的脱敏汇总，仅平台运维可见。</p></div><Button variant="outline" disabled={busy} onClick={()=>setRequest(v=>v+1)}>读取 DLQ 风险</Button></div>
    <div className="panel-body"><p role="status" className="muted">{busy?'正在读取 DLQ 风险…':data?'评估于 '+new Date(data.computed_at).toLocaleString('zh-CN'):'手动读取，不自动轮询。'}</p>{error?<p role="alert" className="error">{error}</p>:null}<p className="fine-print">重放请求或发布不等于成功；等待回执仍属待处理。12天开始提示保留风险，14天以上仍在索引内的来源继续提示。</p></div>
    {data?<>{data.categories?<section className="summary-strip">{Object.entries(categories).map(([key,label])=><div key={key}><span>{label}</span><strong>{count(data.categories![key as keyof typeof categories])}</strong></div>)}</section>:null}
      <div className="table-scroll" tabIndex={0} aria-label="DLQ 风险明细，可横向滚动"><table><thead><tr><th>规则</th><th>当前状态</th><th>读取时依据</th><th>待处理来源数</th><th>最老来源年龄</th></tr></thead><tbody>{data.signals.map(s=><tr key={s.rule}><td>{labels[s.rule]}</td><td><span role="status" className={expired(s)||s.state==='unknown'?'text-amber-700':s.state==='firing'?'text-red-700':'text-emerald-700'}>{expired(s)?'已过期，当前未知':s.state==='unknown'?'未知':s.state==='firing'?'触发':'未触发'}</span></td><td>{reasons[s.reason]}</td><td>{count(s.pending_records)}</td><td>{s.oldest_pending_age_seconds===null?'未确认':count(s.oldest_pending_age_seconds)+' 秒'}</td></tr>)}</tbody></table></div>
      <div className="panel-body"><p className="fine-print">计数和年龄表示读取时证据，90秒后当前状态未知，页面计时包含网络等待。来源记录数不等于唯一事件数；未触发不证明事件已修复或 Kafka DLQ 完整。</p><p className="fine-print">此处含无公司归属和已过期但尚未物理清理的索引记录；不会返回载荷或公司标识。尚未提供 DLQ 风险持久历史和外部通知。窄屏可横向滚动。</p></div></>:null}
  </section>;
}
