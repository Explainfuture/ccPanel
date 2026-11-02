import {useId,useState} from 'react';
import {trendSeries,type TrendPoint} from './trend-series';
const value=(n:number|null)=>n===null?'无有效数值':String(n);
const axis=(n:number)=>new Intl.NumberFormat('zh-CN',{maximumSignificantDigits:4,notation:Math.abs(n)>=1e6||n!==0&&Math.abs(n)<.001?'scientific':'standard'}).format(n);
export default function TrendSeriesView({series}:{series:readonly TrendPoint[]}){
  const [view,setView]=useState('bar'),id=useId();
  let plot:ReturnType<typeof trendSeries>;
  try{plot=trendSeries(series);}catch{return <p role="alert" className="error panel-body">趋势数据格式异常，无法展示日期序列。</p>;}
  return <section className="trend-series" aria-label="趋势日期序列">
    <div className="trend-view-control"><label htmlFor={id}>趋势视图</label><select id={id} value={view} onChange={e=>setView(e.target.value)}><option value="bar">柱状</option><option value="line">折线</option><option value="table">表格</option></select><span className="fine-print">切换视图不重新查询</span></div>
    {!series.length?<p className="empty-inline">当前条件下没有事件。</p>:view==='table'?<div className="table-scroll" tabIndex={0} aria-label="趋势日期明细，可横向滚动"><table><thead><tr><th>日期</th><th>结果</th><th>排除的非数值事件</th></tr></thead><tbody>{series.map(p=><tr key={p.day}><td>{p.day}</td><td>{value(p.value)}</td><td>{p.excluded}</td></tr>)}</tbody></table></div>:view==='line'&&plot.rows.every(p=>p.position===null)?<p className="empty-inline">所有日期均无有效数值，切换表格可查看排除数。</p>:view==='line'?<figure className="trend-line-figure">
      <div className="trend-line-scroll" tabIndex={0} aria-label="趋势折线，可横向滚动"><svg viewBox="0 0 720 230" role="img" aria-labelledby={id+'-title '+id+'-description'}>
        <title id={id+'-title'}>日期趋势折线</title><desc id={id+'-description'}>按日期展示结果；零基线区分正负值，空值和未返回的日期中断连线。完整值可切换表格查看。</desc>
        {[{position:1,label:axis(plot.upper)},{position:0,label:axis(plot.lower)}].map(t=><g key={t.position}><line x1="96" x2="692" y1={180-t.position*154} y2={180-t.position*154} className="trend-grid"/><text x="88" y={184-t.position*154} textAnchor="end">{t.label}</text></g>)}
        <line x1="96" x2="692" y1={180-plot.zero*154} y2={180-plot.zero*154} className="trend-zero"/>
        {plot.zero>0&&plot.zero<1?<text x="88" y={184-plot.zero*154} textAnchor="end">0</text>:null}
        {plot.segments.map((segment,i)=><polyline key={i} fill="none" className="trend-line" points={segment.map(p=>`${96+p.x*596},${180-p.position!*154}`).join(' ')}/>)}
        {plot.rows.filter(p=>p.position!==null).map(p=><circle key={p.day} cx={96+p.x*596} cy={180-p.position!*154} r="4" className={p.value!<0?'trend-dot negative':'trend-dot'}><title>{p.day+'：'+value(p.value)+'；排除 '+p.excluded}</title></circle>)}
        <text x={series.length===1?394:96} y="214" textAnchor={series.length===1?'middle':'start'}>{series[0]!.day}</text>{series.length>1?<text x="692" y="214" textAnchor="end">{series.at(-1)!.day}</text>:null}
      </svg></div><figcaption className="fine-print">仅连接相邻日期的有效值；空值与未返回日期不补零。日期沿用本次查询的时区分桶。窄幅时可横向滚动查看后续日期。</figcaption>
    </figure>:<div className="query-bars trend-bars">{plot.rows.map(p=><div key={p.day}><time>{p.day}</time><span className="trend-bar-track"><i style={{left:`${plot.zero*100}%`}}/>{p.position!==null?<span className={p.value!<0?'negative':undefined} style={{left:`${Math.min(plot.zero,p.position)*100}%`,width:`${Math.abs(p.position-plot.zero)*100}%`}}/>:null}</span><strong>{value(p.value)}</strong></div>)}<p className="fine-print">柱条以零为基线；负值向左，空值不补零。</p></div>}
  </section>;
}
