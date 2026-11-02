import {useEffect,useRef,useState,type FormEvent} from 'react';
import {Plus,X} from 'lucide-react';
import {api,ApiError} from './api';
import {Input} from './components/ui/input';
import {Button} from './components/ui/button';
import type {AnalysisSpec} from '../../../packages/server/src/analysis-spec';
import type {Filter} from '../../../packages/server/src/query-spec';
export type EditableReport={id:string;name:string;version:number;timezone:string|null;spec:AnalysisSpec};
const labels={trend:'趋势',funnel:'漏斗',retention:'留存',cohort:'分群'};
const inputDate=(value:string)=>new Date(value).toISOString().slice(0,-1);
const operations={eq:'等于',neq:'不等于',in:'属于集合',exists:'存在',not_exists:'不存在',gt:'大于',gte:'大于等于',lt:'小于',lte:'小于等于'};
export default function ReportEditor({report,prefix,onSaved,onCancel,onReload}:{report:EditableReport;prefix:string;onSaved:()=>Promise<void>;onCancel:()=>void;onReload:()=>Promise<void>}){
  const [spec,setSpec]=useState<AnalysisSpec>(()=>structuredClone(report.spec));
  const [filters,setFilters]=useState(()=>report.spec.filters.map(f=>({key:f.key,op:f.op,value:f.value===undefined?'':JSON.stringify(f.value)})));
  const [saving,setSaving]=useState(false),[error,setError]=useState(''),[conflict,setConflict]=useState(false);
  const live=useRef(true),flight=useRef(false);
  useEffect(()=>{live.current=true;return()=>{live.current=false;};},[]);
  async function save(event:FormEvent<HTMLFormElement>){
    event.preventDefault();if(flight.current)return;
    const form=new FormData(event.currentTarget);
    let conditions:Filter[];
    try{conditions=filters.map(row=>{
      if(['exists','not_exists'].includes(row.op))return{key:row.key,op:row.op};
      let value:Filter['value'];try{value=JSON.parse(row.value);}catch{value=row.value;}
      return{key:row.key,op:row.op,value};
    });}catch{setError('请检查属性条件。');return;}
    flight.current=true;setSaving(true);setError('');setConflict(false);
    try{
      await api(prefix+'/reports/'+report.id,{method:'PATCH',body:JSON.stringify({
        name:form.get('name'),timezone:form.get('timezone'),expected_version:report.version,
        spec:{...spec,...(spec.kind==='trend'?{group_by:spec.group_by.filter(Boolean)}:{}),from:new Date(String(form.get('from'))+'Z').toISOString(),to:new Date(String(form.get('to'))+'Z').toISOString(),filters:conditions},
      })});
      if(live.current)await onSaved();
    }catch(reason){if(live.current){
      const code=reason instanceof ApiError?reason.code:'';
      setConflict(code==='report_conflict');
      setError(code==='report_conflict'?'报表已有新版本。当前修改仍保留，请核对后重新编辑。':code==='forbidden'?'当前角色不能修改报表。':code==='invalid_timezone'?'请输入有效时区，例如 Asia/Shanghai。':code==='invalid_time_range'?'请检查开始/结束时间，范围不能超出项目保留期。':reason instanceof ApiError?'保存未完成，请检查事件、数值属性和条件。':'保存结果尚未确认，请关闭后刷新报表核对版本。');
    }}finally{flight.current=false;if(live.current)setSaving(false);}
  }
  return <form className="panel-body query-form report-editor" onSubmit={save}>
    <p className="muted">编辑{labels[spec.kind]}报表 · 版本 {report.version}</p>
    {error?<p role="alert" className="error">{error}</p>:null}
    <div className="query-fields">
      <label>报表名称<Input name="name" defaultValue={report.name} required maxLength={128}/></label>
      <label>报表时区<Input name="timezone" defaultValue={report.timezone||''} required placeholder="例如 Asia/Shanghai"/></label>
      <label>开始时间（UTC）<Input type="datetime-local" name="from" defaultValue={inputDate(spec.from)} step="0.001" required/></label>
      <label>结束时间（UTC）<Input type="datetime-local" name="to" defaultValue={inputDate(spec.to)} step="0.001" required/></label>
      {spec.kind==='trend'?<>
        <label>报表事件名称<Input value={spec.event_name||''} maxLength={128} onChange={e=>setSpec({...spec,event_name:e.target.value||undefined})} placeholder="留空包含全部业务事件"/></label>
        <label>报表指标<select value={spec.metric} onChange={e=>setSpec({...spec,metric:e.target.value as typeof spec.metric})}><option value="count">事件次数</option><option value="users">去重用户数</option><option value="sum">数值求和</option><option value="avg">数值平均值</option></select></label>
        {['sum','avg'].includes(spec.metric)?<label>报表数值属性<Input required value={spec.property||''} onChange={e=>setSpec({...spec,property:e.target.value})}/></label>:null}
        {[0,1].map(i=><label key={i}>报表分组属性 {i+1}<Input value={spec.group_by[i]||''} onChange={e=>{const groups=[spec.group_by[0]||'',spec.group_by[1]||''];groups[i]=e.target.value;setSpec({...spec,group_by:groups});}}/></label>)}
      </>:spec.kind==='funnel'?<>
        <label>报表转化窗口（小时）<Input type="number" min={1} max={168} step="any" required value={spec.window_hours??24} onChange={e=>setSpec({...spec,window_hours:Number(e.target.value)})}/></label>
        <label>报表首步分组属性<Input value={spec.group||''} onChange={e=>setSpec({...spec,group:e.target.value||undefined})}/></label>
      </>:spec.kind==='retention'?<>
        <label>报表起始事件<Input required maxLength={128} value={spec.start_event||''} onChange={e=>setSpec({...spec,start_event:e.target.value})}/></label>
        <label>报表返回事件<Input required maxLength={128} value={spec.return_event||''} onChange={e=>setSpec({...spec,return_event:e.target.value})}/></label>
        <label>报表观察天数<Input type="number" min={1} max={30} required value={spec.days??7} onChange={e=>setSpec({...spec,days:Number(e.target.value)})}/></label>
      </>:<>
        <label>报表事件名称<Input required maxLength={128} value={spec.event_name||''} onChange={e=>setSpec({...spec,event_name:e.target.value})}/></label>
        <label>报表次数条件<select value={spec.count_op||'gte'} onChange={e=>setSpec({...spec,count_op:e.target.value as 'gte'|'eq'|'lte'})}><option value="gte">至少</option><option value="eq">等于</option><option value="lte">最多</option></select></label>
        <label>报表事件次数<Input type="number" min={0} max={1000000} required value={spec.count??1} onChange={e=>setSpec({...spec,count:Number(e.target.value)})}/></label>
      </>}
    </div>
    {spec.kind==='funnel'?<div className="filter-list">{spec.steps?.map((step,i)=><div key={i}>
      <Input aria-label={`报表第 ${i+1} 步事件`} value={step} required maxLength={128} onChange={e=>setSpec({...spec,steps:spec.steps!.map((s,n)=>n===i?e.target.value:s)})}/>
      <Button type="button" variant="ghost" size="icon" disabled={spec.steps!.length<=2} aria-label={`移除报表步骤 ${i+1}`} onClick={()=>setSpec({...spec,steps:spec.steps!.filter((_,n)=>n!==i)})}><X/></Button>
    </div>)}<Button type="button" variant="outline" disabled={(spec.steps?.length||0)>=5} onClick={()=>setSpec({...spec,steps:[...(spec.steps||[]),'']})}><Plus/>增加报表步骤</Button></div>:null}
    <div className="filter-list">{filters.map((row,i)=><div key={i}>
      <Input aria-label={`报表条件 ${i+1} 属性`} value={row.key} required onChange={e=>setFilters(v=>v.map((r,n)=>n===i?{...r,key:e.target.value}:r))}/>
      <select aria-label={`报表条件 ${i+1} 运算`} value={row.op} onChange={e=>setFilters(v=>v.map((r,n)=>n===i?{...r,op:e.target.value as Filter['op']}:r))}>{Object.entries(operations).map(([value,label])=><option key={value} value={value}>{label}</option>)}</select>
      {!['exists','not_exists'].includes(row.op)?<Input aria-label={`报表条件 ${i+1} 值`} value={row.value} onChange={e=>setFilters(v=>v.map((r,n)=>n===i?{...r,value:e.target.value}:r))} placeholder={'文本或 JSON，如 "paid"、20'}/>:null}
      <Button type="button" variant="ghost" size="icon" aria-label={`移除报表条件 ${i+1}`} onClick={()=>setFilters(v=>v.filter((_,n)=>n!==i))}><X/></Button>
    </div>)}</div>
    <p className="fine-print">时间范围为固定 UTC 时刻；报表时区决定自然日统计与留存日期。数值条件输入数字，数字形式的文本请加双引号。保存后生成新版本，已排队任务保留原版本。</p>
    <div className="query-actions">
      <Button type="button" variant="outline" disabled={filters.length>=20||saving} onClick={()=>setFilters(v=>[...v,{key:'',op:'eq',value:''}])}><Plus/>添加报表条件</Button>
      <Button disabled={saving}>{saving?'正在保存…':'保存修改'}</Button>
      <Button type="button" variant="ghost" disabled={saving} onClick={onCancel}>取消</Button>
    </div>
    {conflict?<Button type="button" variant="outline" disabled={saving} onClick={()=>void onReload()}>放弃修改并重新加载</Button>:null}
  </form>;
}
