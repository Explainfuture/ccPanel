import type {HostResourceStatus} from '../../../packages/server/src/host-resource-status';
export type HostResources=ReturnType<HostResourceStatus['snapshot']>;
const decimal=new Intl.NumberFormat('zh-CN',{maximumFractionDigits:2}),integer=new Intl.NumberFormat('zh-CN');
const validBytes=(value:unknown):value is string=>typeof value==='string'&&/^\d+$/.test(value);
function bytes(value:unknown){
  if(!validBytes(value))return '未记录';const n=BigInt(value),units=['B','KiB','MiB','GiB','TiB','PiB','EiB'];let scale=1n,i=0;
  while(i<units.length-1&&n>=scale*1024n){scale*=1024n;i++;}
  const fraction=(n%scale*100n/scale).toString().padStart(2,'0').replace(/0+$/,'');
  return integer.format(n/scale)+(fraction?'.'+fraction:'')+' '+units[i];
}
const ByteValue=({value}:{value:string|null|undefined})=>validBytes(value)?<data value={value} title={value+' 字节'}>{bytes(value)}</data>:<>未记录</>;
const percent=(value:unknown)=>typeof value==='number'&&Number.isFinite(value)&&value>=0&&value<=100?decimal.format(value)+'%':'未记录';
const states:Record<string,string>={unavailable:'尚未取得宿主机采样。',invalid:'宿主机采样未通过校验，旧数据已清除。',stale:'宿主机采样已过期或时间异常，旧数据不再显示。'};
const rateReasons:Record<string,string>={baseline_required:'等待第二个有效采样，暂不推断速率为0。',sample_gap:'采样间隔过长，等待新的连续区间。',counter_reset:'计数或采样规格变化，等待重新建立速率基线。'};
const cpu=[['cpu_user_percent','CPU用户态'],['cpu_system_percent','CPU系统态'],['cpu_idle_percent','CPU空闲'],['cpu_iowait_percent','CPU等待I/O'],['cpu_steal_percent','CPU虚拟化等待']] as const;
export default function HostResourcesPanel({data}:{data:HostResources|null|undefined}){
  const gauges=data?.state==='ready'?data.gauges:null,rates=gauges?data?.rates:null;
  return <section className="panel"><div className="panel-heading"><div><h2>宿主机资源</h2><p className="muted">平台运维可见的服务器整体资源，不是当前公司的独立用量。</p></div></div>
    {gauges?<><div className="panel-body"><p className="fine-print">采样 {data?.sampled_at!=null?new Date(data.sampled_at).toLocaleString('zh-CN'):'未记录'} · 读取时样本年龄 {data?.age_ms!=null?decimal.format(data.age_ms/1000)+' 秒':'未记录'}。采样新鲜不代表容量验收通过。</p></div>
      <section className="summary-strip"><div><span>宿主机可用内存</span><strong><ByteValue value={gauges.memory_bytes.available}/></strong></div><div><span>宿主机总内存</span><strong><ByteValue value={gauges.memory_bytes.total}/></strong></div><div><span>Swap已占用</span><strong><ByteValue value={gauges.swap_used_bytes}/></strong></div><div><span>系统负载（1分钟）</span><strong>{typeof gauges.load_average[0]==='number'&&Number.isFinite(gauges.load_average[0])?decimal.format(gauges.load_average[0]):'未记录'}</strong></div></section>
      <div className="panel-body"><p className="fine-print">{rates?'区间 '+new Date(rates.started_at).toLocaleTimeString('zh-CN')+' 至 '+new Date(rates.finished_at).toLocaleTimeString('zh-CN')+'，不是读取瞬间速率。':rateReasons[data?.rate_state??'']??'区间速率尚未取得。'}</p></div>
      <div className="table-scroll"><table><thead><tr><th>区间计量</th><th>观察值</th></tr></thead><tbody>{cpu.map(([key,label])=><tr key={key}><td>{label}</td><td>{percent(rates?.[key])}</td></tr>)}<tr><td>Swap换入</td><td>{rates?bytes(rates.swap_in_bytes_per_second)+' /秒':'未记录'}</td></tr><tr><td>Swap换出</td><td>{rates?bytes(rates.swap_out_bytes_per_second)+' /秒':'未记录'}</td></tr></tbody></table></div>
      <div className="table-scroll"><table><thead><tr><th>存储用途</th><th>可用空间</th><th>空间余量</th><th>文件节点余量</th></tr></thead><tbody>{gauges.disks.map(d=><tr key={d.kind}><td>{d.kind==='kafka'?'Kafka':'PostgreSQL'}</td><td><ByteValue value={d.available_bytes}/></td><td>{percent(d.available_percent)}</td><td>{percent(d.available_inode_percent)}</td></tr>)}</tbody></table></div>
      <div className="panel-body"><p className="fine-print">Swap占用不是换入换出活动，宿主机内存不是容器RSS；系统负载不是CPU百分比。同一磁盘可承担多个用途，余量不可相加。</p><p className="fine-print">容量采用二进制单位并截取两位小数，悬停查看原始字节。未知不填0，单次快照不证明没有持续Swap，也不是历史SLO。</p></div>
    </>:<div className="panel-body"><p role="status" className="muted">{states[data?.state??'unavailable']??'尚无有效宿主机资源快照。'}</p></div>}
  </section>;
}
