import {useEffect,useState} from 'react';
import {api,ApiError} from './api';
import {Button} from './components/ui/button';
import {pressureLabels,pressureReasons} from './PressureSignalsPanel';
import {parsePressureHistory,type PressureHistory,type PressureRow} from './pressure-history';
const names={clear:'未触发',firing:'触发',unknown:'未知'};
const changes:Record<string,string>={initial:'首次记录',state:'状态变化',reason:'原因变化',policy:'阈值变化',source:'实例切换',gap:'观察中断'};
const time=(v:string|null)=>v===null?'无有效期':new Date(v).toLocaleString('zh-CN');
const count=(v:string)=>new Intl.NumberFormat('zh-CN').format(BigInt(v));
export default function PressureHistoryPanel({prefix}:{prefix:string}){
  const [request,setRequest]=useState(0),[data,setData]=useState<PressureHistory|null>(null),[busy,setBusy]=useState(false),[error,setError]=useState(''),[requestedAt,setRequestedAt]=useState(0),[clock,setClock]=useState(0);
  useEffect(()=>{
    if(!request)return;const controller=new AbortController(),started=performance.now();
    setBusy(true);setData(null);setError('');
    void api<unknown>(prefix+'/pressure-history',{signal:AbortSignal.any([controller.signal,AbortSignal.timeout(10000)])}).then(raw=>{
      if(controller.signal.aborted)return;const parsed=parsePressureHistory(raw);if(!parsed){setError('压力历史格式异常，旧数据已清除。');return;}setRequestedAt(started);setClock(performance.now());setData(parsed);
    },reason=>{if(!controller.signal.aborted)setError(reason instanceof ApiError&&[401,403,404].includes(reason.status)?'压力历史访问权限已失效，旧数据已清除。':reason instanceof ApiError&&reason.status===429?'读取过于频繁，请稍后重试。':'压力历史读取失败，旧数据已清除。');}).finally(()=>{if(!controller.signal.aborted)setBusy(false);});
    return()=>controller.abort();
  },[prefix,request]);
  useEffect(()=>{
    if(!data)return;const pending=(at:number)=>data.states.some(r=>r.current_state!=='unknown'&&r.signal_expires_at!==null&&Date.parse(r.signal_expires_at)-Date.parse(data.computed_at)>=Math.max(0,at-requestedAt));
    if(!pending(performance.now()))return;
    const tick=()=>{const now=performance.now();setClock(now);if(!pending(now))clearInterval(timer);},timer=setInterval(tick,1000);
    document.addEventListener('visibilitychange',tick);return()=>{clearInterval(timer);document.removeEventListener('visibilitychange',tick);};
  },[data,requestedAt]);
  const current=(r:PressureRow&{current_state:keyof typeof names})=>r.current_state!=='unknown'&&r.signal_expires_at!==null&&Date.parse(r.signal_expires_at)-Date.parse(data!.computed_at)>=Math.max(0,clock-requestedAt)?r.current_state:'unknown';
  return <section className="panel" aria-label="平台压力历史"><div className="panel-heading"><div><h2>平台压力历史</h2><p className="muted">平台共享资源的采样记录，仅平台运维可见。</p></div><Button variant="outline" disabled={busy} onClick={()=>setRequest(v=>v+1)}>读取压力历史</Button></div>
    <div className="panel-body"><p role="status" className="muted">{busy?'正在读取压力历史…':!data?'手动读取，无自动轮询。':data.persistence_state==='invalid'?'持久证据不完整或异常，当前状态未知。':data.persistence_state==='unavailable'?'尚无持久压力证据。':'读取于 '+time(data.computed_at)+'，当前状态按源有效期更新。'}</p>{error?<p role="alert" className="error">{error}</p>:null}<p className="fine-print">约30秒采样；Kafka证据默认15秒有效，采样之间可能未知。页面计时包含请求等待，历史记录不代表连续健康或通知送达。窄屏可横向滚动查看明细。</p></div>
    {data?<><div className="panel-body"><h3>最近持久证据</h3></div>{data.states.length?<div className="table-scroll" tabIndex={0} aria-label="最近压力证据，可横向滚动"><table><thead><tr><th>信号</th><th>当前状态</th><th>当时状态 / 原因</th><th>源有效至</th><th>评估时间 / 修订</th></tr></thead><tbody>{data.states.map(r=>{const state=current(r);return <tr key={r.rule}><td>{pressureLabels[r.rule]}</td><td><span role="status" className={state==='unknown'?'text-amber-700':state==='firing'?'text-red-700':'text-emerald-700'}>{state==='unknown'?'未知（证据不足或过期）':names[state]}</span></td><td>{names[r.state_at_assessment]} · {pressureReasons[r.reason]}</td><td>{time(r.signal_expires_at)}</td><td>{time(r.assessed_at)}<br/>修订 {count(r.revision)}</td></tr>;})}</tbody></table></div>:null}
    <div className="panel-body"><h3>最近变化</h3><p className="fine-print">仅最近50条，最多保留14天且全局上限5000条；可能提前淘汰。以下始终表示当时状态。</p></div>{data.transitions.length?<div className="table-scroll" tabIndex={0} aria-label="压力变化历史，可横向滚动"><table><thead><tr><th>评估时间</th><th>信号 / 变化</th><th>当时状态 / 原因</th><th>修订</th></tr></thead><tbody>{data.transitions.map(r=><tr key={r.id}><td>{time(r.assessed_at)}</td><td>{pressureLabels[r.rule]}<br/>{changes[r.change_kind]}</td><td>{names[r.state_at_assessment]} · {pressureReasons[r.reason]}</td><td>{count(r.revision)}</td></tr>)}</tbody></table></div>:<div className="panel-body"><p className="muted">保留范围内没有变化记录。</p></div>}
    <div className="panel-body"><p className="fine-print">外部通知尚未接入；API与数据库同时不可用时，平台不能依靠自身记录发出告警。</p></div></>:null}
  </section>;
}
