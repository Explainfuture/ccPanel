import {useEffect,useRef,useState,type FormEvent} from 'react';
import {api,ApiError} from './api';
import {Button} from './components/ui/button';
import {Input} from './components/ui/input';
type Policy={raw_days:number;version:number;floor:string|null;effective_from:string};
type Snapshot={policy:Policy;observed_at:string;limits:{raw_min_days:number;raw_max_days:number;dedup_days:number;dlq_days:number;sdk_queue_days:number}};
const timestamp=(value:string)=>new Date(value).toLocaleString('zh-CN',{hour12:false});
export default function RetentionSettings(props:{prefix:string;canEdit:boolean}){
  return <RetentionForm key={props.prefix+':'+props.canEdit} {...props}/>;
}
function RetentionForm({prefix,canEdit}:{prefix:string;canEdit:boolean}){
  const [data,setData]=useState<Snapshot|null>(null),[days,setDays]=useState(''),[busy,setBusy]=useState(false),[error,setError]=useState(''),[notice,setNotice]=useState('');
  const active=useRef<AbortController|null>(null);
  function request(){active.current?.abort();const controller=new AbortController();active.current=controller;return{controller,signal:AbortSignal.any([controller.signal,AbortSignal.timeout(10000)])};}
  async function load(){
    const {controller,signal}=request();setBusy(true);setData(null);setDays('');setError('');setNotice('');
    try{const value=await api<Snapshot>(prefix+'/retention',{signal});if(!controller.signal.aborted){setData(value);setDays(String(value.policy.raw_days));}}
    catch(reason){if(!controller.signal.aborted)setError(reason instanceof ApiError&&[401,403,404].includes(reason.status)?'访问权限已失效，策略已清除。请刷新公司权限。':'策略读取失败，请重新读取。');}
    finally{if(!controller.signal.aborted)setBusy(false);}
  }
  useEffect(()=>{void load();return()=>active.current?.abort();},[]);
  const value=Number(days),valid=!!data&&days.trim()!==''&&Number.isInteger(value)&&value>=data.limits.raw_min_days&&value<=data.limits.raw_max_days;
  async function save(event:FormEvent){
    event.preventDefault();if(!data||!canEdit||busy||!valid||value===data.policy.raw_days)return;
    const {controller,signal}=request();setBusy(true);setError('');setNotice('');
    try{
      const result=await api<Snapshot&{changed:boolean}>(prefix+'/retention',{method:'PATCH',signal,body:JSON.stringify({raw_days:value,expected_version:data.policy.version})});
      if(!controller.signal.aborted){setData(result);setDays(String(result.policy.raw_days));setNotice(result.changed?'保留策略已更新。较早数据会逐批清理，延长时间不会恢复已过期数据。':'保留策略没有变化。');}
    }catch(reason){
      if(!controller.signal.aborted){
        setData(null);setDays('');
        setError(reason instanceof ApiError&&reason.code==='retention_policy_conflict'?'策略已有新版本，请重新读取后修改。':reason instanceof ApiError&&reason.code==='retention_policy_busy'?'项目正在查询或维护数据，请重新读取后再试。':reason instanceof ApiError&&[401,403,404].includes(reason.status)?'访问权限已失效，策略已清除。请刷新公司权限。':reason instanceof ApiError&&reason.status===429?'修改过于频繁，请稍后重新读取。':'保存结果未确认，请重新读取核对当前策略。');
      }
    }finally{if(!controller.signal.aborted)setBusy(false);}
  }
  return <section className="panel" aria-label="数据保留策略"><div className="panel-heading"><div><h2>数据保留策略</h2><p className="muted">按事件发生时间保留原始数据，仅作用于当前项目。</p></div><Button type="button" variant="outline" disabled={busy} onClick={()=>void load()}>重新读取策略</Button></div>
    <div className="panel-body space-y-4">
      {busy?<p role="status">正在处理保留策略…</p>:null}{error?<p className="error" role="alert">{error}</p>:null}{notice?<p className="notice" role="status">{notice}</p>:null}
      {data?<><dl className="grid gap-3 sm:grid-cols-2"><div><dt className="muted">当前原始数据保留期</dt><dd>{data.policy.raw_days} 天 · 版本 {data.policy.version}</dd></div><div><dt className="muted">本次读取的有效数据起点</dt><dd>{timestamp(data.policy.effective_from)}</dd></div></dl><p className="fine-print">观察于 {timestamp(data.observed_at)}（本机时区）。起点会随时间前移，不代表物理清理进度。</p>
      <p className="fine-print">去重记录 {data.limits.dedup_days} 天 · DLQ {data.limits.dlq_days} 天 · SDK 本地待发最长 {data.limits.sdk_queue_days} 天。这些期限独立于原始数据保留期，仍受容量与重试条件限制。</p>
      {canEdit?<form className="space-y-3" onSubmit={save}><label className="block max-w-xs">原始数据保留天数<Input type="number" min={data.limits.raw_min_days} max={data.limits.raw_max_days} step="1" required value={days} disabled={busy} onChange={event=>setDays(event.target.value)}/></label><p className="fine-print">可设置 {data.limits.raw_min_days}–{data.limits.raw_max_days} 天。缩短后，过期事件立即不再参与查询并异步清理；延长不会恢复已过期数据。此前的查询任务结果会失效，需重新计算。</p><Button disabled={busy||!valid||value===data.policy.raw_days}>保存保留策略</Button></form>:<p className="muted">当前为只读视图，请联系公司管理员修改。</p>}</>:null}
    </div></section>;
}
