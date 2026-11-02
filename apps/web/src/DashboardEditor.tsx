import {useState,useRef,type FormEvent} from 'react';
import {ArrowUp,ArrowDown,X} from 'lucide-react';
import {api,ApiError} from './api';
import {Input} from './components/ui/input';
import {Button} from './components/ui/button';
import {FilterFields,filterRows,filterValues} from './FilterFields';
import type {Filter} from '../../../packages/server/src/query-spec';
export type Dashboard={id:string;name:string;version:number;report_ids:string[];columns:1|2;range_days:1|7|30|null;filters:Filter[]};
export type DashboardReport={id:string;name:string};
export default function DashboardEditor({board,reports,prefix,onSaved,onCancel,onReload}:{board:Dashboard|null;reports:DashboardReport[];prefix:string;onSaved:()=>Promise<void>;onCancel:()=>void;onReload:()=>Promise<void>}){
  const [ids,setIds]=useState(board?.report_ids||[]),[filters,setFilters]=useState(()=>filterRows(board?.filters||[]));
  const [busy,setBusy]=useState(false),[error,setError]=useState(''),[conflict,setConflict]=useState(false),[uncertain,setUncertain]=useState(false);
  const flight=useRef(false);
  function move(index:number,direction:number){setIds(rows=>{const next=[...rows];[next[index],next[index+direction]]=[next[index+direction]!,next[index]!];return next;});}
  async function save(event:FormEvent<HTMLFormElement>){
    event.preventDefault();if(flight.current||uncertain)return;flight.current=true;setBusy(true);setError('');setConflict(false);
    const form=new FormData(event.currentTarget);
    try{
      await api(prefix+'/dashboards'+(board?'/'+board.id:''),{method:board?'PATCH':'POST',body:JSON.stringify({name:form.get('name'),report_ids:ids,columns:Number(form.get('columns')),range_days:form.get('range')==='report'?null:Number(form.get('range')),filters:filterValues(filters),...(board?{expected_version:board.version}:{})})});
      await onSaved();
    }catch(reason){
      const code=reason instanceof ApiError?reason.code:'';setConflict(code==='dashboard_conflict');setUncertain(!(reason instanceof ApiError));
      setError(code==='dashboard_conflict'?'仪表盘已有新版本，当前草稿已保留。':code==='forbidden'?'当前角色不能修改仪表盘。':code==='not_found'?'部分报表已删除或不可访问，请刷新核对。':reason instanceof ApiError?'保存未完成，请检查名称、范围、属性类型和卡片数量。':'保存结果尚未确认，请先刷新核对，避免重复创建。');
    }finally{flight.current=false;setBusy(false);}
  }
  return <form className="dashboard-form dashboard-editor" onSubmit={save}>
    <h3>{board?`编辑仪表盘 · 版本 ${board.version}`:'创建仪表盘'}</h3>
    {error?<p role="alert" className="error">{error}</p>:null}
    <label>仪表盘名称<Input name="name" defaultValue={board?.name||''} maxLength={128} required/></label>
    <div className="query-fields">
      <label>卡片布局<select name="columns" defaultValue={board?.columns||1}><option value="1">单列</option><option value="2">双列</option></select></label>
      <label>默认时间范围<select name="range" defaultValue={board?.range_days||'report'}><option value="report">各报表自己的范围</option><option value="1">最近24小时</option><option value="7">最近7天</option><option value="30">最近30天</option></select></label>
    </div>
    <p className="fine-print">卡片按下列顺序逐张查询，手机自动单列。默认筛选叠加在报表条件之上。</p>
    <div className="report-checks">{reports.map(report=><label key={report.id}><input type="checkbox" checked={ids.includes(report.id)} disabled={busy||(!ids.includes(report.id)&&ids.length>=50)} onChange={e=>setIds(rows=>e.target.checked?[...rows,report.id]:rows.filter(id=>id!==report.id))}/>{report.name}</label>)}</div>
    <ol className="dashboard-order">{ids.map((id,i)=><li key={id}><span>{reports.find(r=>r.id===id)?.name||'已删除的报表'}</span>
      <Button type="button" size="icon" variant="ghost" aria-label={`上移卡片 ${i+1}`} disabled={busy||i===0} onClick={()=>move(i,-1)}><ArrowUp/></Button>
      <Button type="button" size="icon" variant="ghost" aria-label={`下移卡片 ${i+1}`} disabled={busy||i===ids.length-1} onClick={()=>move(i,1)}><ArrowDown/></Button>
      <Button type="button" size="icon" variant="ghost" aria-label={`移除卡片 ${i+1}`} disabled={busy} onClick={()=>setIds(rows=>rows.filter(value=>value!==id))}><X/></Button>
    </li>)}</ol>
    <FilterFields rows={filters} onChange={setFilters} label="默认筛选" disabled={busy}/>
    <div className="query-actions"><Button disabled={busy||uncertain||ids.length>50}>{busy?'正在保存…':board?'保存仪表盘':'创建仪表盘'}</Button><Button type="button" variant="ghost" disabled={busy} onClick={onCancel}>取消</Button></div>
    {conflict||uncertain?<Button type="button" variant="outline" disabled={busy} onClick={()=>void onReload()}>放弃草稿并刷新列表</Button>:null}
  </form>;
}
