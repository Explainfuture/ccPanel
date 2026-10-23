import {useEffect,useState} from 'react';
import {RefreshCw} from 'lucide-react';
import {api,ApiError} from './api';
import {Button} from './components/ui/button';
import QueryStatusPanel,{type QueryStatus} from './QueryStatusPanel';
import IntakeMetricsPanel,{type IntakeSnapshot} from './IntakeMetricsPanel';
import HostResourcesPanel,{type HostResources} from './HostResourcesPanel';
import OperationalHistoryPanel from './OperationalHistoryPanel';
import OperationalAlertsPanel from './OperationalAlertsPanel';
import PressureSignalsPanel from './PressureSignalsPanel';
import PressureHistoryPanel from './PressureHistoryPanel';
import type {PressureAlerts} from '../../../packages/server/src/pressure-alerts';
import type {AdmissionStatus} from '../../../packages/server/src/ingest-admission';
type Metric={metric:string;value:string};
type DeadLetters={categories:{category:string;count:string;oldest_created_at:string|null;oldest_age_seconds:string|null;observed_at:string}[]};
const dlqLabels:Record<string,string>={unresolved:'未解决',replaying:'重放中',resolved:'已有成功回执',suppressed:'隐私删除抑制'};
type Snapshot={scope:'platform_aggregate';computed_at:string;admission:AdmissionStatus|null;pressure_alerts?:PressureAlerts|null;intake?:IntakeSnapshot|null;host_resources?:HostResources|null;processing?:{in_flight:number;max_in_flight:number;at_capacity:boolean}};
const count=(value:string|number|null|undefined)=>value!==null&&value!==undefined&&/^\d+$/.test(String(value))?new Intl.NumberFormat('zh-CN').format(BigInt(value)):'未记录';
const reasons:Record<string,string>={consumer_offset_unknown:'消费位点无法确认，需要管理员核对来源与进度。',retained_offset_gap:'发现消息保留缺口，完整性保护已暂停接入，需要离线核对。',consumer_lag_high:'消费积压达到保护阈值，等待消费者追赶。',consumer_lag_old:'存在超过时间预算的消费积压。',disk_space_low:'存储可用空间不足。',disk_inodes_low:'存储可用文件节点不足。',disk_probe_stale:'磁盘采样已过期。',host_probe_unavailable:'宿主机采样暂不可用。',host_probe_invalid:'宿主机采样未通过校验。',kafka_probe_unavailable:'Kafka采样暂不可用。',pressure_probe_unavailable:'压力采样暂不可用。',pressure_probe_stale:'压力采样已过期。',pressure_probe_invalid:'压力采样未通过校验。',ingest_reservations_full:'本接入实例的保留余量已用尽。'};
const counters=[['inserted','累计成功落库'],['duplicates','重复上报命中'],['deletion_suppressed','删除屏障拦截'],['retention_suppressed','消费时保留期拦截'],['retention_raw_expired','保留期清理事件']] as const;
export default function DeliveryMonitor({prefix,canManage,canObservePlatform=false}:{prefix:string;canManage:boolean;canObservePlatform?:boolean}){
  const [platformRequestedAt,setPlatformRequestedAt]=useState(0);
  const [dlq,setDlq]=useState<DeadLetters|null>(null),[dlqError,setDlqError]=useState('');
  const [queries,setQueries]=useState<QueryStatus|null>(null),[queryError,setQueryError]=useState('');
  const [metrics,setMetrics]=useState<Metric[]|null>(null),[snapshot,setSnapshot]=useState<Snapshot|null>(null),[error,setError]=useState(''),[platformError,setPlatformError]=useState(''),[retrieved,setRetrieved]=useState(''),[busy,setBusy]=useState(false),[auto,setAuto]=useState(false),[refresh,setRefresh]=useState(0);
  useEffect(()=>{
    const controller=new AbortController();let timer:ReturnType<typeof setTimeout>|undefined;
    setMetrics(null);setSnapshot(null);setDlq(null);setDlqError('');setQueries(null);setQueryError('');setRetrieved('');setError('');setPlatformError('');
    const load=async()=>{
      if(controller.signal.aborted)return;
      if(auto&&document.visibilityState==='hidden'){timer=setTimeout(()=>void load(),15000);return;}
      setBusy(true);
      const signal=AbortSignal.any([controller.signal,AbortSignal.timeout(10000)]);
      const requestedAt=performance.now();
      const [project,platform,deadLetters,queryStatus]=await Promise.allSettled([api<{metrics:Metric[]}>(prefix+'/metrics',{signal}),canObservePlatform?api<Snapshot>(prefix+'/ingest-status',{signal}):Promise.resolve(null),canManage?api<DeadLetters>(prefix+'/dlq-summary',{signal}):Promise.resolve(null),api<QueryStatus>(prefix+'/query-status',{signal})]);
      if(controller.signal.aborted)return;
      if(project.status==='rejected'){
        setMetrics(null);setSnapshot(null);setDlq(null);setQueries(null);setRetrieved('');setError(project.reason instanceof ApiError&&[401,403,404].includes(project.reason.status)?'项目不存在或访问权限已失效，请重新选择公司或登录。':'项目计数读取失败，请重试。');
      }else{
        setMetrics(project.value.metrics);setRetrieved(new Date().toISOString());setError('');
        if(platform.status==='fulfilled'){setSnapshot(platform.value);setPlatformRequestedAt(requestedAt);setPlatformError('');}
        else{setSnapshot(null);setPlatformError('平台接入状态读取失败，旧状态已清除，请刷新后重试。');}
        if(deadLetters.status==='fulfilled'){setDlq(deadLetters.value);setDlqError('');}
        else{setDlq(null);setDlqError(deadLetters.reason instanceof ApiError&&[401,403,404].includes(deadLetters.reason.status)?'DLQ访问权限已失效，旧快照已清除。':'DLQ统计读取失败，旧快照已清除，请重试。');}
        if(queryStatus.status==='fulfilled'){setQueries(queryStatus.value);setQueryError('');}
        else{setQueries(null);setQueryError(queryStatus.reason instanceof ApiError&&[401,403,404].includes(queryStatus.reason.status)?'查询任务访问权限已失效，旧快照已清除。':'查询任务统计读取失败，旧快照已清除，请重试。');}
      }
      setBusy(false);if(auto)timer=setTimeout(()=>void load(),15000);
    };
    void load();return()=>{controller.abort();if(timer)clearTimeout(timer);};
  },[prefix,canManage,canObservePlatform,auto,refresh]);
  const admission=snapshot?.admission,processing=snapshot?.processing,accepts=admission?.allowed&&!processing?.at_capacity,reason=admission?.reason?(reasons[admission.reason]??'保护状态暂不可用，请检查平台运行情况。'):processing?.at_capacity?'本实例上报处理已达并发预算，请等待重试。':null,values=new Map(metrics?.map(row=>[row.metric,row.value]));
  return <>
    <section className="panel delivery-monitor">
      <div className="panel-heading"><div><h2>项目投递计数</h2><p className="muted">仅当前公司与项目，累计值不等于当前保留的事件数。</p></div><Button type="button" variant="outline" disabled={busy} onClick={()=>setRefresh(v=>v+1)}><RefreshCw size={14}/>刷新监控</Button></div>
      <div className="panel-body"><label className="delivery-toggle"><input type="checkbox" checked={auto} onChange={e=>setAuto(e.target.checked)}/> 页面可见时约15秒自动读取</label><p className="fine-print">{retrieved?'最近读取 '+new Date(retrieved).toLocaleString('zh-CN'):'尚未取得计数快照'} · 未记录的计数不推断为0。</p>{error?<p role="alert" className="error">{error}</p>:null}</div>
      {metrics?<><section className="summary-strip">{counters.map(([key,label])=><div key={key}><span>{label}</span><strong>{count(values.get(key))}</strong></div>)}</section><div className="table-scroll"><table><thead><tr><th>计量项</th><th>累计数量</th></tr></thead><tbody>{[['retention_dedup_expired','过期去重登记'],['retention_tasks_expired','过期聚合任务'],['retention_buckets_expired','过期小时桶'],['retention_dlq_expired','过期DLQ记录']].map(([key,label])=><tr key={key}><td>{label}</td><td>{count(values.get(key))}</td></tr>)}</tbody></table></div></>:null}
    </section>
    <QueryStatusPanel data={queries} error={queryError}/>
    {canManage?<section className="panel"><div className="panel-heading"><div><h2>项目 DLQ 状态</h2><p className="muted">当前项目14天内保留的故障来源，按重放回执分类。</p></div></div>
      {dlqError?<div className="panel-body"><p role="alert" className="error">{dlqError}</p></div>:null}
      {dlq?<><div className="table-scroll"><table><thead><tr><th>状态</th><th>来源记录数</th><th>最老记录年龄</th></tr></thead><tbody>{Object.entries(dlqLabels).map(([key,label])=>{const row=dlq.categories.find(r=>r.category===key);return <tr key={key}><td>{label}</td><td>{count(row?.count)}</td><td>{row?.oldest_age_seconds!==null&&row?.oldest_age_seconds!==undefined?count(row.oldest_age_seconds)+' 秒':row?.count==='0'?'无记录':'未记录'}</td></tr>;})}</tbody></table></div><div className="panel-body"><p className="fine-print">数据库快照时间 {dlq.categories[0]?.observed_at?new Date(dlq.categories[0].observed_at).toLocaleString('zh-CN'):'未记录'}。年龄从来源记录创建时计算，刷新后更新。</p><p className="fine-print">失败或拒绝重放仍属未解决；已发布尚无成功回执属于重放中。已有成功回执不等于历史记录已删除。过期移除不代表修复，同一事件可能对应多个来源。</p></div></>:!dlqError?<div className="panel-body"><p className="muted">尚未取得DLQ快照。</p></div>:null}
    </section>:null}
    {canObservePlatform?<section className="panel"><div className="panel-heading"><div><h2>平台接入保护</h2><p className="muted">平台脱敏汇总，不是本项目的独立Kafka积压。</p></div></div><div className="panel-body">
      {platformError?<p role="alert" className="error">{platformError}</p>:null}
      {admission?<><p role="status" className={'notice '+(accepts?'':'delivery-paused')}>{accepts?'读取时允许接入':'读取时暂停接入'}{reason?' · '+reason:''}</p><p className="fine-print">状态时间 {new Date(snapshot!.computed_at).toLocaleString('zh-CN')}。允许接入表示当前保护条件满足，不代表已通过生产容量验收。</p><dl className="delivery-values"><div><dt>总消费积压</dt><dd>{count(admission.totalLagMessages)}</dd></div><div><dt>最繁忙分区积压</dt><dd>{count(admission.maxLagMessages)}</dd></div><div><dt>本实例保留余量</dt><dd>{count(admission.remainingMessages)}</dd></div><div><dt>确认结果未知的保留量</dt><dd>{count(admission.unknownMessages)}</dd></div><div><dt>同时处理的上报请求</dt><dd>{count(processing?.in_flight)}</dd></div><div><dt>本实例处理上限</dt><dd>{count(processing?.max_in_flight)}</dd></div></dl><p className="fine-print">确认结果未知不表示事件已丢失。接入暂停时SDK保留原ID重试；完整性暂停需要核对，不能在此直接清除。</p></>:!platformError?<p className="muted">尚无可用平台状态，无法判断接入是否开放。</p>:null}
    </div></section>:null}
    {canObservePlatform&&!platformError&&!error?<PressureSignalsPanel key={prefix+':'+platformRequestedAt} data={snapshot?.pressure_alerts} requestedAt={platformRequestedAt}/>:null}
    {canObservePlatform?<HostResourcesPanel data={snapshot?.host_resources}/>:null}
    {canObservePlatform?<IntakeMetricsPanel data={snapshot?.intake}/>:null}
    {canObservePlatform&&!platformError&&!error?<OperationalHistoryPanel key={prefix} prefix={prefix}/>:null}
    {canObservePlatform&&!platformError&&!error?<OperationalAlertsPanel key={prefix} prefix={prefix}/>:null}
    {canObservePlatform&&!platformError&&!error?<PressureHistoryPanel key={prefix} prefix={prefix}/>:null}
  </>;
}
