export type QueryStatus={states:{status:string;count:string;oldest_age_seconds:string|null;oldest_running_seconds:string|null;duration_samples:string;max_completed_duration_ms:string|null;observed_at:string}[]};
const labels={queued:'排队等待',running:'正在执行',succeeded:'已成功',failed:'已失败',cancelled:'已取消'};
const number=(value:string|null|undefined)=>typeof value==='string'&&/^\d+(\.\d+)?$/.test(value)?new Intl.NumberFormat('zh-CN').format(BigInt(value.split('.')[0]!))+(value.includes('.')&&value.split('.')[1]!.replace(/0+$/,'')?'.'+value.split('.')[1]!.replace(/0+$/,''):''):'未记录';
export default function QueryStatusPanel({data,error}:{data:QueryStatus|null;error:string}){
  const running=data?.states.find(r=>r.status==='running');
  return <section className="panel"><div className="panel-heading"><div><h2>项目查询任务</h2><p className="muted">当前可见、未过期的后台查询任务。</p></div></div>
    {error?<div className="panel-body"><p role="alert" className="error">{error}</p></div>:null}
    {data?<><div className="table-scroll"><table><thead><tr><th>任务状态</th><th>数量</th><th>最老任务年龄</th></tr></thead><tbody>{Object.entries(labels).map(([key,label])=>{const row=data.states.find(r=>r.status===key);return <tr key={key}><td>{label}</td><td>{number(row?.count)}</td><td>{row?.oldest_age_seconds!=null?number(row.oldest_age_seconds)+' 秒':row?.count==='0'?'无任务':'未记录'}</td></tr>;})}</tbody></table></div>
      <div className="panel-body"><p className="fine-print">最老运行任务已执行：{running?.oldest_running_seconds!=null?number(running.oldest_running_seconds)+' 秒':running?.count==='0'?'无运行任务':'未记录'}。任务年龄从创建起计算，已执行时间从开始运行起计算，均为快照值。</p></div>
      <div className="table-scroll"><table><thead><tr><th>完成状态</th><th>有效耗时样本</th><th>最长执行耗时</th></tr></thead><tbody>{(['succeeded','failed','cancelled'] as const).map(key=>{const row=data.states.find(r=>r.status===key);return <tr key={key}><td>{labels[key]}</td><td>{number(row?.duration_samples)}</td><td>{row?.max_completed_duration_ms!=null?number(row.max_completed_duration_ms)+' 毫秒':row?.duration_samples==='0'?'无有效样本':'未记录'}</td></tr>;})}</tbody></table></div>
      <div className="panel-body"><p className="fine-print">数据库快照时间 {data.states[0]?.observed_at?new Date(data.states[0].observed_at).toLocaleString('zh-CN'):'未记录'}。耗时不包含排队；取消记录不证明底层SQL已中断。</p><p className="fine-print">任务过期、隐私版本变化或创建者被移出公司后不在此统计。这里不是全平台物理积压，也不是长期成功率或延迟承诺。</p></div>
    </>:!error?<div className="panel-body"><p className="muted">尚未取得查询任务快照。</p></div>:null}
  </section>;
}
