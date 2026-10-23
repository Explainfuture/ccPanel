import {useEffect,useState} from 'react';
import {api,ApiError} from './api';
import {Button} from './components/ui/button';
import IntakeMetricsPanel from './IntakeMetricsPanel';
import type {OperationalSnapshot} from '../../../packages/server/src/operational-snapshot';
type Sample={source:'api'|'worker';instance_id:string;started_at:string;observed_at:string;snapshot:OperationalSnapshot};
type Page={scope:'platform_aggregate';samples:Sample[];next_cursor:string|null;computed_at:string};
const formatter=new Intl.NumberFormat('zh-CN');
const labels:Record<string,string>={persist:'事件落库',dlq_publish:'DLQ发布',dlq_index:'DLQ索引',kafka_checkpoint:'Kafka位点提交',inserted:'插入',duplicate:'重复',suppressed:'删除抑制',conflict:'冲突',failed:'失败',completed:'完成'};
const count=(value:string)=>/^\d+$/.test(value)?formatter.format(BigInt(value)):'未记录';
export default function OperationalHistoryPanel({prefix}:{prefix:string}){
  const [source,setSource]=useState(''),[request,setRequest]=useState<{cursor:string|null;version:number}|null>(null),[data,setData]=useState<Page|null>(null),[selected,setSelected]=useState<Sample|null>(null),[busy,setBusy]=useState(false),[error,setError]=useState('');
  useEffect(()=>{
    if(!request)return;
    const controller=new AbortController();setBusy(true);setData(null);setSelected(null);setError('');
    const params=new URLSearchParams();if(source)params.set('source',source);if(request.cursor)params.set('cursor',request.cursor);
    void api<Page>(prefix+'/operational-history?'+params,{signal:AbortSignal.any([controller.signal,AbortSignal.timeout(10000)])}).then(value=>{if(!controller.signal.aborted)setData(value);},reason=>{if(!controller.signal.aborted)setError(reason instanceof ApiError&&[401,403,404].includes(reason.status)?'访问权限已失效，历史数据已清除。':reason instanceof ApiError&&reason.status===429?'读取过于频繁，请稍后重新读取。':'历史读取失败，旧数据已清除，请重试。');}).finally(()=>{if(!controller.signal.aborted)setBusy(false);});
    return()=>controller.abort();
  },[prefix,source,request]);
  const reload=()=>setRequest(value=>({cursor:null,version:(value?.version??0)+1}));
  return <section className="panel" aria-label="平台监控历史"><div className="panel-heading"><div><h2>平台监控历史</h2><p className="muted">API与Worker进程快照，跨公司运维汇总；每条计数从该实例启动时累计。</p></div><Button variant="outline" disabled={busy} onClick={reload}>读取最新历史</Button></div>
    <div className="panel-body"><label>采样来源 <select aria-label="历史采样来源" value={source} disabled={busy} onChange={event=>{setSource(event.target.value);setData(null);setSelected(null);setError('');setRequest(null);}}><option value="">全部来源</option><option value="api">API</option><option value="worker">Worker</option></select></label><p className="fine-print">最多保留7天和全局5万条，每页50条。故障或忙锁会产生采样空缺；累计值不可逐行相加，不表示区间事件量或成功率。手动读取，不自动轮询。</p>
    {busy?<p role="status">正在读取历史…</p>:error?<p role="alert" className="error">{error}</p>:!data?<p className="muted">选择来源后读取历史。</p>:<p role="status" className="muted">本页 {data.samples.length} 条 · 读取于 {new Date(data.computed_at).toLocaleString('zh-CN')}{data.samples.length===0?' · 此范围尚无保留快照':''}</p>}</div>
    {data&&data.samples.length>0?<><div className="table-scroll"><table><thead><tr><th>观察时间</th><th>来源</th><th>进程实例</th><th>启动时间</th><th>详情</th></tr></thead><tbody>{data.samples.map(row=><tr key={row.source+row.instance_id+row.observed_at}><td>{new Date(row.observed_at).toLocaleString('zh-CN')}</td><td>{row.source==='api'?'API':'Worker'}</td><td><code title={row.instance_id}>{row.instance_id.slice(0,8)}</code></td><td>{new Date(row.started_at).toLocaleString('zh-CN')}</td><td><Button variant="outline" onClick={()=>setSelected(row)}>查看快照</Button></td></tr>)}</tbody></table></div><div className="panel-body"><Button variant="outline" disabled={busy||!data.next_cursor} onClick={()=>setRequest(value=>({cursor:data.next_cursor,version:(value?.version??0)+1}))}>下一页历史</Button><p className="fine-print">分页期间可能发生保留期裁剪；读取最新历史可回到首页。</p></div></>:null}
    {selected?<div className="panel-body"><h3>{selected.source==='api'?'API':'Worker'}快照详情 · {selected.instance_id.slice(0,8)}</h3><p className="fine-print">观察于 {new Date(selected.observed_at).toLocaleString('zh-CN')}，实例启动于 {new Date(selected.started_at).toLocaleString('zh-CN')}。</p>{'responses' in selected.snapshot?<IntakeMetricsPanel historical data={selected.snapshot}/>:<div className="table-scroll"><table><thead><tr><th>操作</th><th>观察时进行中</th><th>启动以来累计结果</th></tr></thead><tbody>{selected.snapshot.operations.map(row=><tr key={row.operation}><td>{labels[row.operation]??row.operation}</td><td>{row.in_flight}</td><td>{row.outcomes.map(outcome=><div key={outcome.outcome}>{labels[outcome.outcome]??outcome.outcome}：{count(outcome.count)}</div>)}</td></tr>)}</tbody></table></div>}</div>:null}
  </section>;
}
