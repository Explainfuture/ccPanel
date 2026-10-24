import {useEffect,useState} from 'react';
import {api,ApiError} from './api';
import {Button} from './components/ui/button';
import {dlqLabels,dlqReasons} from './DlqHealthPanel';
import {parseDlqAlertHistory,type DlqAlertHistory} from './dlq-alert-history';
const labels={clear:'未触发',firing:'触发',unknown:'未知'};
const changes:Record<string,string>={initial:'首次记录',state:'状态变化',reason:'原因变化',policy:'策略变化',gap:'观察中断'};
const time=(v:string|null)=>v===null?'未确认':new Date(v).toLocaleString('zh-CN');
const numberFormat=new Intl.NumberFormat('zh-CN');
const count=(v:string|null)=>v===null?'未确认':numberFormat.format(BigInt(v));
export default function DlqAlertHistoryPanel({prefix}:{prefix:string}){
  const [request,setRequest]=useState(0),[data,setData]=useState<DlqAlertHistory|null>(null),[busy,setBusy]=useState(false),[error,setError]=useState(''),[requestedAt,setRequestedAt]=useState(0),[clock,setClock]=useState(0);
  useEffect(()=>{
    if(!request)return;const controller=new AbortController(),started=performance.now();setData(null);setError('');setBusy(true);
    void api<unknown>(prefix+'/dlq-alert-history',{signal:AbortSignal.any([controller.signal,AbortSignal.timeout(10000)])}).then(raw=>{
      if(controller.signal.aborted)return;const parsed=parseDlqAlertHistory(raw);if(!parsed){setError('DLQ 历史格式异常，旧数据已清除。');return;}setRequestedAt(started);setClock(performance.now());setData(parsed);
    },reason=>{if(!controller.signal.aborted)setError(reason instanceof ApiError&&[401,403,404].includes(reason.status)?'DLQ 历史访问权限已失效，旧数据已清除。':reason instanceof ApiError&&reason.status===429?'读取过于频繁，请稍后重试。':'DLQ 历史读取失败，旧数据已清除。');}).finally(()=>{if(!controller.signal.aborted)setBusy(false);});
    return()=>controller.abort();
  },[prefix,request]);
  useEffect(()=>{
    if(!data)return;const fresh=(at:number)=>data.states.some(r=>r.current_state!=='unknown'&&r.source_expires_at!==null&&Date.parse(r.source_expires_at)-Date.parse(data.computed_at)>=Math.max(0,at-requestedAt));
    if(!fresh(performance.now()))return;
    const tick=()=>{const now=performance.now();setClock(now);if(!fresh(now))clearInterval(timer);},timer=setInterval(tick,1000);
    document.addEventListener('visibilitychange',tick);return()=>{clearInterval(timer);document.removeEventListener('visibilitychange',tick);};
  },[data,requestedAt]);
  return <section className="panel" aria-label="平台 DLQ 历史"><div className="panel-heading"><div><h2>平台 DLQ 历史</h2><p className="muted">保留的风险评估与变化，仅平台运维可见。</p></div><Button variant="outline" disabled={busy} onClick={()=>setRequest(v=>v+1)}>读取 DLQ 历史</Button></div>
    <div className="panel-body"><p role="status" className="muted">{busy?'正在读取 DLQ 历史…':!data?'手动读取，不自动轮询。':data.persistence_state==='invalid'?'最新持久证据异常，无法判断当前状态。':data.persistence_state==='unavailable'?'尚无持久 DLQ 评估。':'读取于 '+time(data.computed_at)+'，当前状态按源有效期更新。'}</p>{error?<p role="alert" className="error">{error}</p>:null}<p className="fine-print">约30秒后台评估，源证据90秒有效；计时包含请求等待。当时状态不表示当前仍有效，未触发不证明来源已修复。窄屏可横向滚动。</p></div>
    {data?<><div className="panel-body"><h3>最近持久证据</h3></div>{data.states.length?<div className="table-scroll" tabIndex={0} aria-label="DLQ 持久证据，可横向滚动"><table><thead><tr><th>规则</th><th>当前状态</th><th>当时状态 / 依据</th><th>当时待处理数</th><th>源有效至</th><th>评估时间 / 修订</th></tr></thead><tbody>{data.states.map(r=>{const current=r.current_state!=='unknown'&&r.source_expires_at!==null&&Date.parse(r.source_expires_at)-Date.parse(data.computed_at)>=Math.max(0,clock-requestedAt)?r.current_state:'unknown';return <tr key={r.rule}><td>{dlqLabels[r.rule]}</td><td><span role="status" className={current==='unknown'?'text-amber-700':current==='firing'?'text-red-700':'text-emerald-700'}>{current==='unknown'?'未知（证据不足或过期）':labels[current]}</span></td><td>{labels[r.state_at_assessment]} · {dlqReasons[r.reason]}</td><td>{count(r.signal.pending_records)}</td><td>{time(r.source_expires_at)}</td><td>{time(r.assessed_at)}<br/>修订 {count(r.revision)}</td></tr>;})}</tbody></table></div>:null}
      <div className="panel-body"><h3>最近变化</h3><p className="fine-print">仅最近50条，最长14天且全局最多5000条，可能提前淘汰；不是完整事故审计或通知送达记录。</p>{data.discarded_transitions?<p role="alert" className="error">有 {data.discarded_transitions} 条异常历史被跳过，记录不完整。</p>:null}</div>{data.transitions.length?<div className="table-scroll" tabIndex={0} aria-label="DLQ 变化历史，可横向滚动"><table><thead><tr><th>评估时间</th><th>规则 / 变化</th><th>当时状态 / 依据</th><th>当时待处理数</th><th>修订</th></tr></thead><tbody>{data.transitions.map(r=><tr key={r.id}><td>{time(r.assessed_at)}</td><td>{dlqLabels[r.rule]}<br/>{changes[r.change_kind]}</td><td>{labels[r.state_at_assessment]} · {dlqReasons[r.reason]}</td><td>{count(r.signal.pending_records)}</td><td>{count(r.revision)}</td></tr>)}</tbody></table></div>:<div className="panel-body"><p className="muted">保留范围内没有有效变化记录。</p></div>}
      <div className="panel-body"><p className="fine-print">仅PG来源索引汇总，不证明Kafka DLQ完整。数据库或API停机时无法依赖此页面自评估；独立外部通知仍待接入。</p></div></>:null}
  </section>;
}
