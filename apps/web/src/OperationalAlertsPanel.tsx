import {useEffect,useState} from 'react';
import {api,ApiError} from './api';
import {Button} from './components/ui/button';
import type {OperationalAlert} from '../../../packages/server/src/operational-alerts';
type Persisted={rule:OperationalAlert['rule'];state:OperationalAlert['state'];reason:string;evaluated_at:string;state_since:string;revision:string;signal:OperationalAlert};
type AlertPage={scope:'platform_aggregate';computed_at:string;freshness_ms:number;rules:OperationalAlert[];persistence_state:'fresh'|'stale'|'invalid'|'unavailable';persisted_states:Persisted[];transitions:(Persisted&{id:string})[]};
const names:Record<OperationalAlert['rule'],string>={api_sampling:'API采样',worker_sampling:'Worker采样',api_server_errors:'API服务端错误',worker_failures:'Worker操作失败'};
const states={clear:'正常',firing:'触发',unknown:'证据不足'};
const reasons:Record<string,string>={fresh_sample:'采样新鲜',missing_sample:'没有保留采样',stale_sample:'采样超过90秒',invalid_snapshot:'采样未通过校验',future_sample:'采样时间在未来',baseline_required:'等待同一实例基线',duplicate_time:'观察时间未推进',sample_gap:'两次采样间隔过大',counter_reset:'相关计数发生回退',failure_increase:'区间内失败计数增加',no_failure_increase:'区间内相关计数未增加'};
const integer=new Intl.NumberFormat('zh-CN');
const count=(value:string|null)=>value!==null&&/^\d+$/.test(value)?integer.format(BigInt(value)):'未记录';
const time=(value:string|null)=>value&&Number.isFinite(Date.parse(value))?new Date(value).toLocaleString('zh-CN'):'未记录';
export default function OperationalAlertsPanel({prefix}:{prefix:string}){
  const [request,setRequest]=useState(0),[data,setData]=useState<AlertPage|null>(null),[busy,setBusy]=useState(false),[error,setError]=useState(''),[received,setReceived]=useState(0),[clock,setClock]=useState(0);
  useEffect(()=>{
    if(!request)return;
    const controller=new AbortController();setBusy(true);setData(null);setError('');
    void api<AlertPage>(prefix+'/operational-alerts',{signal:AbortSignal.any([controller.signal,AbortSignal.timeout(10000)])}).then(value=>{if(!controller.signal.aborted){const now=performance.now();setReceived(now);setClock(now);setData(value);}},reason=>{if(!controller.signal.aborted)setError(reason instanceof ApiError&&[401,403,404].includes(reason.status)?'告警访问权限已失效，旧数据已清除。':reason instanceof ApiError&&reason.status===429?'读取过于频繁，请稍后重试。':'告警读取失败，旧数据已清除。');}).finally(()=>{if(!controller.signal.aborted)setBusy(false);});
    return()=>controller.abort();
  },[prefix,request]);
  useEffect(()=>{
    if(!data||data.persistence_state!=='fresh')return;
    const timer=setInterval(()=>{const now=performance.now();setClock(now);if(data.persisted_states.some(row=>Date.parse(data.computed_at)-Date.parse(row.evaluated_at)+Math.max(0,now-received)>data.freshness_ms))clearInterval(timer);},1000);
    return()=>clearInterval(timer);
  },[data,received]);
  const aged=data?.persisted_states.some(row=>Date.parse(data.computed_at)-Date.parse(row.evaluated_at)+Math.max(0,clock-received)>data.freshness_ms),fresh=data?.persistence_state==='fresh'&&!aged;
  const persistenceMessage=!data?'尚未读取告警。':fresh?'持久评估在新鲜度范围内。':data.persistence_state==='invalid'?'持久评估时间异常，不能据此判断当前健康。':data.persistence_state==='unavailable'?'尚无完整持久评估，不能据此判断当前健康。':'持久评估已过期，旧状态不能代表当前健康，请重新读取。';
  return <section className="panel" aria-label="平台运维告警"><div className="panel-heading"><div><h2>平台运维告警</h2><p className="muted">跨公司平台信号，仅平台运维可见。当前覆盖两来源采样及失败增量。</p></div><Button variant="outline" disabled={busy} onClick={()=>setRequest(value=>value+1)}>读取告警</Button></div>
    <div className="panel-body"><p role="status" className={data&&!fresh?'notice delivery-paused':'muted'}>{busy?'正在读取告警…':persistenceMessage}</p>{error?<p role="alert" className="error">{error}</p>:null}<p className="fine-print">手动读取，不自动轮询。约30秒后台评估，90秒新鲜度；API和Worker均停机或数据库不可用时无法自评估。页面会使已读持久状态随时间过期。</p></div>
    {data?<><div className="panel-body"><h3>读取时即时评估</h3><p className="fine-print">评估于 {time(data.computed_at)}。正常只表示该规则的证据满足，不代表生产容量或所有副本健康。</p></div><div className="table-scroll"><table><thead><tr><th>规则</th><th>读取时信号</th><th>依据</th><th>失败增量</th><th>观测区间</th></tr></thead><tbody>{data.rules.map(row=><tr key={row.rule}><td>{names[row.rule]}</td><td>{states[row.state]}</td><td>{reasons[row.reason]??'依据暂不可用'}</td><td>{count(row.increase)}</td><td>{row.interval_ms===null?'未记录':row.interval_ms/1000+' 秒'}</td></tr>)}</tbody></table></div>
    <div className="panel-body"><h3>最近持久状态</h3><p className="fine-print">修订号只在状态或原因变化时增加。证据不足不是恢复正常；旧状态始终保留评估时间。</p></div>{data.persisted_states.length?<div className="table-scroll"><table><thead><tr><th>规则</th><th>持久状态</th><th>状态开始</th><th>最近评估</th><th>修订</th></tr></thead><tbody>{data.persisted_states.map(row=><tr key={row.rule}><td>{names[row.rule]}</td><td>{fresh?'':'旧状态：'}{states[row.state]}</td><td>{time(row.state_since)}</td><td>{time(row.evaluated_at)}</td><td>{count(row.revision)}</td></tr>)}</tbody></table></div>:<div className="panel-body"><p className="muted">尚无持久状态。</p></div>}
    <div className="panel-body"><h3>最近变化</h3><p className="fine-print">仅最近50条保留记录，目标14天且全局最多5000条；不是完整事故审计或通知送达记录。</p></div>{data.transitions.length?<div className="table-scroll"><table><thead><tr><th>变化时间</th><th>规则</th><th>变为</th><th>依据</th><th>修订</th></tr></thead><tbody>{data.transitions.map(row=><tr key={row.id}><td>{time(row.evaluated_at)}</td><td>{names[row.rule]}</td><td>{states[row.state]}</td><td>{reasons[row.reason]??'依据暂不可用'}</td><td>{count(row.revision)}</td></tr>)}</tbody></table></div>:<div className="panel-body"><p className="muted">保留范围内没有变化记录。</p></div>}
    <div className="panel-body"><p className="fine-print">失败增量来自同一实例的累计计数差，不是错误率或唯一事件数。当前未提供确认、静默或外部通知，也未覆盖Kafka、磁盘与备份规则。</p></div></>:null}
  </section>;
}
