import {useEffect,useRef,useState,type FormEvent,type ReactNode} from 'react';
import {LayoutDashboard,Plus,Trash2} from 'lucide-react';
import {api,ApiError} from './api';
import {Button} from './components/ui/button';
import DashboardEditor,{type Dashboard,type DashboardReport} from './DashboardEditor';
import {FilterFields,filterValues,type FilterRow} from './FilterFields';
import type {Filter} from '../../../packages/server/src/query-spec';
type Reference={report_id:string;expected_version:number;overrides:{from?:string;to?:string;filters:Filter[]}};
type Plan={dashboard:{id:string;name:string;version:number;columns:1|2};planned_at:string;cards:({report_id:string;name?:string|null;error:string}|(Reference&{name:string}))[]};
type Card<T>={id:string;name:string;reference?:Reference;status:'waiting'|'running'|'succeeded'|'failed';result?:T;error?:string};
const explain=(reason:unknown)=>reason instanceof ApiError?({dashboard_conflict:'仪表盘已有新版本，请刷新列表再打开。',report_conflict:'报表已有新版本，请重新打开仪表盘。',report_timezone_unrecorded:'历史报表缺少时区，请编辑后重新运行。',not_found:'仪表盘或报表不存在，或当前权限已失效。',query_busy:'项目正在执行其他查询，请稍后重试。',query_timeout:'查询超时，可缩小范围或选择后台计算。',query_requires_narrower_range:'超出当前查询预算，请缩小范围或选择后台计算。',too_many_filters:'默认与附加筛选合计不能超过20条。',forbidden:'当前角色没有此操作权限。'}[reason.code]||'操作未完成，请检查条件后重试。'):'网络结果未确认，请重试查询或刷新列表核对。';
export default function DashboardWorkspace<T>({prefix,boards,reports,canEdit,disabled,onChanged,onBusy,onAsync,renderResult}:{prefix:string;boards:Dashboard[];reports:DashboardReport[];canEdit:boolean;disabled:boolean;onChanged:()=>Promise<void>;onBusy:(value:boolean)=>void;onAsync:(reference:Reference)=>Promise<void>;renderResult:(result:T)=>ReactNode}){
  const [editing,setEditing]=useState<Dashboard|null|undefined>(),[active,setActive]=useState<Dashboard|null>(null),[plan,setPlan]=useState<Plan|null>(null),[cards,setCards]=useState<Card<T>[]>([]);
  const [running,setRunning]=useState(false),[error,setError]=useState(''),[range,setRange]=useState('saved'),[filters,setFilters]=useState<FilterRow[]>([]),[dirty,setDirty]=useState(false);
  const generation=useRef(0),abortRef=useRef<AbortController|null>(null);
  useEffect(()=>{onBusy(running);},[running,onBusy]);
  useEffect(()=>()=>{generation.current++;abortRef.current?.abort();onBusy(false);},[prefix,onBusy]);
  function stop(){generation.current++;abortRef.current?.abort();setRunning(false);setCards(rows=>rows.map(r=>['running','waiting'].includes(r.status)?{...r,status:'failed',error:'已停止加载；已开始的服务端计算仍受原超时预算限制。'}:r));}
  async function run(board:Dashboard,rangeValue='saved',rows:FilterRow[]=[]){
    abortRef.current?.abort();const abort=new AbortController();abortRef.current=abort;const current=++generation.current;
    setActive(board);setRunning(true);setError('');setDirty(false);setCards([]);setPlan(null);
    try{
      const value=await api<Plan>(prefix+`/dashboards/${board.id}/run`,{method:'POST',signal:abort.signal,body:JSON.stringify({expected_version:board.version,...(rangeValue==='saved'?{}:{range_days:rangeValue==='report'?null:Number(rangeValue)}),filters:filterValues(rows)})});
      if(current!==generation.current)return;setPlan(value);
      const initial:Card<T>[]=value.cards.map(card=>'error' in card?{id:card.report_id,name:card.name||'已删除的报表',status:'failed',error:explain(new ApiError(card.error,404))}:{id:card.report_id,name:card.name,status:'waiting',reference:{report_id:card.report_id,expected_version:card.expected_version,overrides:card.overrides}});
      setCards(initial);
      for(const [index,card]of initial.entries()){
        if(current!==generation.current)return;if(!card.reference)continue;
        setCards(list=>list.map((r,i)=>i===index?{...r,status:'running'}:r));
        try{
          const result=await api<T>(prefix+`/reports/${card.id}/run`,{method:'POST',signal:abort.signal,body:JSON.stringify({...card.reference.overrides,expected_version:card.reference.expected_version})});
          if(current===generation.current)setCards(list=>list.map((r,i)=>i===index?{...r,result,status:'succeeded'}:r));
        }catch(reason){if(current!==generation.current||abort.signal.aborted)return;setCards(list=>list.map((r,i)=>i===index?{...r,status:'failed',error:explain(reason)}:r));}
      }
    }catch(reason){if(current===generation.current&&!abort.signal.aborted)setError(explain(reason));}
    finally{if(current===generation.current)setRunning(false);}
  }
  function apply(event:FormEvent){event.preventDefault();if(active)void run(active,range,filters);}
  async function changed(){setEditing(undefined);stop();setActive(null);setPlan(null);setCards([]);try{await onChanged();}catch(reason){setError(explain(reason));}}
  return <>
    <section className="panel">
      <div className="panel-heading"><h2>仪表盘</h2><LayoutDashboard size={19}/></div>
      <div className="panel-body">
        {error?<p role="alert" className="error">{error}</p>:null}
        {boards.map(board=><div key={board.id} className="dashboard-list-row">
          <Button variant="outline" className="dashboard-link" disabled={disabled||running} onClick={()=>{setRange('saved');setFilters([]);void run(board);}}><LayoutDashboard/>{board.name}<span className="muted">{board.report_ids.length} 张卡片 · v{board.version}</span></Button>
          {canEdit?<div className="dashboard-row-actions"><Button size="sm" variant="ghost" disabled={editing!==undefined||running} onClick={()=>setEditing(board)}>编辑仪表盘</Button><Button size="sm" variant="ghost" disabled={editing!==undefined||running} aria-label={'删除仪表盘 '+board.name} onClick={async()=>{try{await api(prefix+'/dashboards/'+board.id,{method:'DELETE'});await changed();}catch(reason){setError(explain(reason));}}}><Trash2/></Button></div>:null}
        </div>)}
        {!boards.length?<p className="muted">将共享报表组成仪表盘，统一比较分析窗口。</p>:null}
        <Button type="button" variant="outline" disabled={running} onClick={async()=>{try{await onChanged();setError('');}catch(reason){setError(explain(reason));}}}>刷新仪表盘列表</Button>
        {canEdit&&editing===undefined?<Button type="button" variant="outline" disabled={running} onClick={()=>setEditing(null)}><Plus/>新建仪表盘</Button>:null}
        {canEdit&&editing!==undefined?<DashboardEditor key={editing?editing.id+':'+editing.version:'new'} board={editing} reports={reports} prefix={prefix} onSaved={changed} onCancel={()=>setEditing(undefined)} onReload={changed}/>:null}
      </div>
    </section>
    {active?<section className="panel dashboard-run-controls">
      <div className="panel-heading"><div><h2>{active.name}</h2><p className="muted">{plan?`版本 ${plan.dashboard.version} · 查询计划生成于 ${new Date(plan.planned_at).toLocaleString('zh-CN')}`:running?'正在读取查询计划':'查询计划尚未生成，可检查条件后重新应用。'}</p></div>{running?<Button type="button" variant="outline" onClick={stop}>停止加载</Button>:null}</div>
      <form className="panel-body" onSubmit={apply}>
        <label>仪表盘查询范围<select value={range} disabled={running} onChange={e=>{setRange(e.target.value);setDirty(true);}}><option value="saved">使用仪表盘默认范围</option><option value="report">各报表自己的范围</option><option value="1">最近24小时</option><option value="7">最近7天</option><option value="30">最近30天</option></select></label>
        <FilterFields rows={filters} label="本次附加筛选" disabled={running} onChange={rows=>{setFilters(rows);setDirty(true);}}/>
        <p className="fine-print">本次筛选叠加在仪表盘默认与报表条件上，不会修改保存内容。后台计算结果显示在页面上方。</p>
        {dirty?<p role="status" className="notice">条件已修改，当前仍显示上一次结果；应用后更新。</p>:null}
        <Button disabled={disabled||running}>应用条件并刷新</Button>
      </form>
    </section>:null}
    {plan?<div className={'dashboard-results '+(plan.dashboard.columns===2?'dashboard-results-two':'')} aria-label="仪表盘卡片">
      {!cards.length?<p className="empty-inline">此仪表盘尚未选择报表。</p>:null}
      {cards.map(card=><section key={card.id} className="panel dashboard-card">
        <div className="panel-heading"><h2>{card.name}</h2><span className="muted">{{waiting:'等待查询',running:'查询中',succeeded:'已更新',failed:'未完成'}[card.status]}</span></div>
        {card.status==='succeeded'&&card.result?renderResult(card.result):<p className={'panel-body '+(card.error?'error':'muted')}>{card.error||'按卡片顺序逐张加载。'}</p>}
        {card.reference?<div className="panel-body"><Button type="button" variant="outline" disabled={running||disabled} onClick={()=>void onAsync(card.reference!)}>后台计算此卡片</Button></div>:null}
      </section>)}
    </div>:null}
  </>;
}


