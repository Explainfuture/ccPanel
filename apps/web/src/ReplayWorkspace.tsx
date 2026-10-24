import { useEffect,useRef,useState,type FormEvent } from 'react';
import { RefreshCw,RotateCcw } from 'lucide-react';
import { api,ApiError } from './api';
import { Button } from './components/ui/button';
type RecordRow={id:string;reason:string;created_at:string};
type Job={id:string;reason:string;status:string;error:string|null;created_at:string};
type Item={record_id:string;status:string;attempts:number;error:string|null;outcome_dlq_id:string|null};
type Detail=Job&{items:Item[]};
type Request={request_id:string;record_ids:string[];reason:string};
const labels:Record<string,string>={queued:'等待投递',running:'处理中',published:'Kafka 已确认，等待入库结果',stored:'已入库',duplicate:'已存在，未重复写入',suppressed:'删除屏障已拦截',rejected:'拒绝重放',failed:'处理失败',completed:'处理完成'};
const reasons:Record<string,string>={invalid_storage_value:'数据库写入约束不满足',payload_conflict:'同一事件 ID 的载荷冲突',identity_conflict:'身份关联冲突',invalid_event_id:'事件 ID 无效',invalid_json:'消息格式无效',invalid_properties:'事件属性无效',scope_mismatch:'公司或项目归属不匹配'};
const errors:Record<string,string>={forbidden:'仅公司管理员可管理重放。',invalid_replay_request:'请选择 1–20 条记录，并填写 5–500 个字符的重放原因。',manual_review_required:'这批记录包含载荷或身份冲突，需要人工审查，不能直接重放。',payload_unavailable:'原载荷已脱敏，不能重放。',unsupported_replay_payload:'原消息格式不支持普通重放，需要人工审查。',replay_scope_mismatch:'原消息的公司或项目不匹配，已拒绝重放。',event_still_invalid:'事件仍不符合格式要求，请先排查接入端或处理器。',invalid_replay_envelope:'原消息封装无效，已拒绝重放。',replay_window_expired:'原事件或接收时间已超过 30 天，不能普通重放。',dedup_registration_missing:'去重登记已缺失，不能普通重放，需要审计迁移。',dedup_registration_expired:'去重登记已过期，不能普通重放。',deleted_scope:'数据已受删除屏障保护，不会重新导入。',replay_request_conflict:'此请求编号已用于其他内容，请先核对原任务。',replay_queue_full:'重放任务队列已满，请等待已有任务完成。',not_found:'记录或任务不存在，或不属于当前公司和项目。',admin_permission_revoked:'发起人的管理员权限已撤销，未继续投递。',publish_unconfirmed:'投递结果尚未确认，后台保留任务并退避重试。',ingestion_paused:'接入压力保护已启用，任务保留并等待恢复。',consumer_rejected:'消费者再次拒绝了这条消息，请检查关联的新 DLQ 记录。',outcome_unresolved:'消息已投递，但目前无法确认最终结果，需要人工核对。',source_unavailable:'原 DLQ 记录已不可用，未继续投递。'};
const errorText=(e:unknown)=>e instanceof ApiError?(errors[e.code]??`请求未完成（${e.code}），请核对任务状态。`):'网络连接失败，请先核对任务列表，再使用原请求重试。';
const at=(s:string)=>new Date(s).toLocaleString('zh-CN',{hour12:false});
function Status({value}:{value:string}){return <span className={`replay-state replay-state-${value}`}>{labels[value]??value}</span>;}
export default function ReplayWorkspace({prefix,canManage}:{prefix:string;canManage:boolean}){
  const [records,setRecords]=useState<RecordRow[]>([]),[jobs,setJobs]=useState<Job[]>([]),[selected,setSelected]=useState<string[]>([]),[reason,setReason]=useState(''),[active,setActive]=useState(''),[detail,setDetail]=useState<Detail|null>(null),[error,setError]=useState(''),[notice,setNotice]=useState(''),[busy,setBusy]=useState(false),[refresh,setRefresh]=useState(0),[uncertain,setUncertain]=useState(false),[loading,setLoading]=useState(true),[loaded,setLoaded]=useState(false);
  const pending=useRef<Request|null>(null),sending=useRef(false);
  useEffect(()=>{
    if(!canManage)return;
    const abort=new AbortController();let timer:ReturnType<typeof setTimeout>;
    const load=async()=>{
      try{
        const [source,list]=await Promise.all([api<{records:RecordRow[]}>(prefix+'/dlq',{signal:abort.signal}),api<{jobs:Job[]}>(prefix+'/dlq/replays',{signal:abort.signal})]);
        const value=active?await api<Detail>(prefix+'/dlq/replays/'+active,{signal:abort.signal}):null;
        if(abort.signal.aborted)return;
        setRecords(source.records);setJobs(list.jobs);setDetail(value);setLoading(false);setLoaded(true);
        if(!sending.current&&pending.current&&list.jobs.some(j=>j.id===pending.current?.request_id)){
          const id=pending.current.request_id;pending.current=null;setUncertain(false);setSelected([]);setReason('');setActive(id);setNotice('任务已记录。Kafka 确认后仍需等待数据库处理结果。');
        }
        if(list.jobs.some(j=>['queued','running'].includes(j.status)))timer=setTimeout(()=>void load(),2500);
      }catch(e){if(!abort.signal.aborted){setError(errorText(e));setLoading(false);}}
    };void load();return()=>{abort.abort();clearTimeout(timer);};
  },[prefix,canManage,active,refresh]);
  async function submit(e:FormEvent){
    e.preventDefault();if(busy)return;
    const body=pending.current??{request_id:crypto.randomUUID(),record_ids:[...selected].sort(),reason:reason.trim()};pending.current=body;sending.current=true;setBusy(true);setError('');setNotice('');
    try{
      const result=await api<{id:string}>(prefix+'/dlq/replays',{method:'POST',body:JSON.stringify(body)});
      if(result.id!==body.request_id)throw new Error('unconfirmed_replay_response');
      pending.current=null;setUncertain(false);setSelected([]);setReason('');setActive(result.id);setRefresh(v=>v+1);setNotice('任务已记录。Kafka 确认后仍需等待数据库处理结果。');
    }catch(e){
      setError(errorText(e));
      if(e instanceof ApiError&&e.status>=400&&e.status<500){pending.current=null;setUncertain(false);}else{setUncertain(true);setRefresh(v=>v+1);}
    }finally{sending.current=false;setBusy(false);}
  }
  if(!canManage)return <section className="panel"><div className="panel-body">仅公司管理员可管理 DLQ 重放。</div></section>;
  return <div className="replay-workspace">
    {error?<div className="replay-alert" role="alert">{error}</div>:null}
    {notice?<div className="replay-notice" role="status">{notice}</div>:null}
    <section className="panel">
      <div className="panel-heading"><div><h2>失败消息</h2><p className="muted">当前项目最近 50 条 DLQ 记录。重放保留原事件 ID 与载荷。</p></div><Button variant="outline" onClick={()=>{setError('');setRefresh(v=>v+1);}}><RefreshCw size={15}/>刷新记录</Button></div>
      <div className="panel-body">
        <p className="replay-boundary">先修复失败原因，再选择记录。已删除、载荷冲突、超过 30 天或缺少有效去重登记的数据不能直接重放。重放不会删除原 DLQ 记录。</p>
        {loading?<p role="status">正在读取失败消息…</p>:!loaded?<p>未能读取记录，请刷新重试。</p>:records.length===0?<div className="replay-empty"><RotateCcw size={23}/><p>当前项目没有 DLQ 记录。</p><span className="muted">消费者拒绝的消息会显示在这里；SDK 本地待发事件不属于 DLQ。</span></div>:<div className="replay-scroll"><table className="replay-table"><thead><tr><th>选择</th><th>记录编号</th><th>失败原因</th><th>记录时间</th></tr></thead><tbody>{records.map(r=><tr key={r.id}><td><input aria-label={`选择记录 ${r.id}`} type="checkbox" disabled={busy||uncertain||(!selected.includes(r.id)&&selected.length>=20)} checked={selected.includes(r.id)} onChange={e=>setSelected(old=>e.target.checked?[...old,r.id]:old.filter(id=>id!==r.id))}/></td><td><code title={r.id}>{r.id.slice(0,14)}…</code></td><td>{reasons[r.reason]??r.reason}</td><td>{at(r.created_at)}</td></tr>)}</tbody></table></div>}
        <form className="replay-form" onSubmit={submit}>
          <label htmlFor="replay-reason">重放原因 <span className="muted">将进入审计记录</span></label>
          <textarea id="replay-reason" required minLength={5} maxLength={500} value={reason} disabled={busy||uncertain} onChange={e=>setReason(e.target.value)} placeholder="例如：已修复事件写入约束，重试原始消息" rows={2}/>
          <div className="replay-actions"><span className="muted">已选择 {selected.length}/20 条</span><Button type="submit" disabled={busy||(!uncertain&&(selected.length===0||reason.trim().length<5))}><RotateCcw size={15}/>{busy?'正在提交…':uncertain?'使用原请求重试':'创建重放任务'}</Button></div>
          {uncertain?<p className="replay-boundary">提交结果未知，选择和原因已锁定。先刷新任务列表；重试会沿用同一请求编号。当前编号：<code>{pending.current?.request_id}</code></p>:null}
        </form>
      </div>
    </section>
    <section className="panel"><div className="panel-heading"><div><h2>重放任务</h2><p className="muted">最近 50 个任务；处理中会自动刷新。</p></div></div><div className="panel-body">
      {!loaded?<p>任务列表尚未读取。</p>:jobs.length===0?<p className="muted">尚未创建重放任务。</p>:<div className="replay-scroll"><table className="replay-table"><thead><tr><th>任务编号</th><th>原因</th><th>状态</th><th>创建时间</th></tr></thead><tbody>{jobs.map(j=><tr key={j.id} className={active===j.id?'replay-active':''}><td><button className="replay-link" onClick={()=>{setActive(j.id);setDetail(null);}} aria-label={`查看任务 ${j.id}`} aria-pressed={active===j.id}>{j.id.slice(0,8)}</button></td><td className="replay-reason-cell">{j.reason}</td><td><Status value={j.status}/></td><td>{at(j.created_at)}</td></tr>)}</tbody></table></div>}
    </div></section>
    {active?<section className="panel" aria-label="重放任务详情"><div className="panel-heading"><div><h2>逐条处理结果</h2><p className="muted replay-id">{active}</p></div><Button variant="outline" onClick={()=>{setActive('');setDetail(null);}}>收起详情</Button></div><div className="panel-body">{!detail?<p role="status">正在读取任务结果…</p>:<><p className="replay-boundary">“Kafka 已确认”只表示消息队列已接收。只有“已入库”或“已存在”能说明原事件已经保存；被删除屏障拦截的数据不会重新导入。</p><div className="replay-scroll"><table className="replay-table"><thead><tr><th>原记录</th><th>结果</th><th>投递尝试</th><th>说明与失败去向</th></tr></thead><tbody>{detail.items.map(i=><tr key={i.record_id}><td><code title={i.record_id}>{i.record_id.slice(0,14)}…</code></td><td><Status value={i.status}/></td><td>{i.attempts}</td><td>{i.error?(errors[i.error]??i.error):'—'}{i.outcome_dlq_id?<div className="replay-outcome">新 DLQ：<code>{i.outcome_dlq_id}</code></div>:null}</td></tr>)}</tbody></table></div></>}</div></section>:null}
  </div>;
}
