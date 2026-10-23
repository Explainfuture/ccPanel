import type {IntakeMetrics} from '../../../packages/server/src/intake-metrics';
export type IntakeSnapshot=ReturnType<IntakeMetrics['snapshot']>;
const integer=(value:unknown)=>typeof value==='string'&&/^\d+$/.test(value)?BigInt(value):null;
const format=(value:unknown)=>{const n=integer(value);return n===null?'未记录':new Intl.NumberFormat('zh-CN').format(n);};
export default function IntakeMetricsPanel({data}:{data:IntakeSnapshot|null|undefined}){
  const peak=data?.accepted_processing_ms.reduce((n,row)=>{const v=integer(row.count);return v!==null&&v>n?v:n;},0n)??0n;
  return <section className="panel"><div className="panel-heading"><div><h2>接入响应与确认耗时</h2><p className="muted">当前API实例的跨公司脱敏汇总，从本次进程启动累计。</p></div></div>
    {data?<><div className="panel-body"><p className="fine-print">实例 {data.instance_id} · 启动 {new Date(data.started_at).toLocaleString('zh-CN')} · 观察 {new Date(data.observed_at).toLocaleString('zh-CN')}</p><p className="fine-print">重启更换实例并从0开始，不是最近15分钟或全部实例合计。刷新读取快照，不把前后快照相加。</p></div>
      <div className="table-scroll"><table><thead><tr><th>HTTP响应</th><th>请求批次</th><th>已知请求事件数</th><th>事件数未知批次</th></tr></thead><tbody>{data.responses.map(row=><tr key={row.status}><td>{row.status==='other'?'其他':row.status}</td><td>{format(row.batches)}</td><td>{format(row.known_events)}</td><td>{format(row.unknown_event_batches)}</td></tr>)}</tbody></table></div>
      <div className="panel-body"><p className="fine-print">批次和事件数包含重试，不是唯一事件或PG入库数。未知事件数单独计批次，不当作0条事件；TLS/代理提前拒绝不在本实例计数内。</p><h3>202处理时间分布</h3><p className="fine-print">互斥分桶，耗时包含等待Kafka确认，到响应构造完成；不包含网络回程或PG可查询等待。</p></div>
      <div className="table-scroll"><table><thead><tr><th>处理时间</th><th>样本数</th><th>相对分布</th></tr></thead><tbody>{data.accepted_processing_ms.map((row,i)=>{const prior=i?data.accepted_processing_ms[i-1]!.upper_bound_ms:0,value=integer(row.count),width=value!==null&&peak>0n?Number(value*10000n/peak)/100:0;return <tr key={i}><td>{row.upper_bound_ms===null?'超过 '+prior+' 毫秒':i?prior+' 至 '+row.upper_bound_ms+' 毫秒（不含左端）':'0 至 '+row.upper_bound_ms+' 毫秒'}</td><td>{format(row.count)}</td><td><div aria-hidden="true" className="h-2 w-24 rounded bg-slate-100"><div className="h-2 rounded bg-[#126783]" style={{width:width+'%'}}/></div></td></tr>;})}</tbody></table></div>
      <div className="panel-body"><p className="fine-print">无效耗时样本：{format(data.accepted_processing_unknown)}。条形按样本最多的桶归一，不代表成功率或分位数。日志/快照非持久账本，异常退出可能缺失末段数据。</p></div>
    </>:<div className="panel-body"><p className="muted">尚无当前实例的接入计量快照，无法判断响应分布。</p></div>}
  </section>;
}
