import {Plus,X} from 'lucide-react';
import {Input} from './components/ui/input';
import {Button} from './components/ui/button';
import type {Filter} from '../../../packages/server/src/query-spec';
export type FilterRow={key:string;op:Filter['op'];value:string};
export const filterRows=(filters:Filter[])=>filters.map(f=>({key:f.key,op:f.op,value:f.value===undefined?'':JSON.stringify(f.value)}));
export const filterValues=(rows:FilterRow[]):Filter[]=>rows.map(row=>{
  if(['exists','not_exists'].includes(row.op))return{key:row.key,op:row.op};
  let value:Filter['value'];try{value=JSON.parse(row.value);}catch{value=row.value;}
  return{key:row.key,op:row.op,value};
});
export function FilterFields({rows,onChange,label,disabled=false}:{rows:FilterRow[];onChange:(rows:FilterRow[])=>void;label:string;disabled?:boolean}){
  return <div className="filter-list">{rows.map((row,i)=><div key={i}>
    <Input aria-label={`${label} ${i+1} 属性`} required value={row.key} disabled={disabled} onChange={e=>onChange(rows.map((r,n)=>n===i?{...r,key:e.target.value}:r))} placeholder="属性名称"/>
    <select aria-label={`${label} ${i+1} 运算`} value={row.op} disabled={disabled} onChange={e=>onChange(rows.map((r,n)=>n===i?{...r,op:e.target.value as Filter['op']}:r))}>
      {Object.entries({eq:'等于',neq:'不等于',in:'属于集合',exists:'存在',not_exists:'不存在',gt:'大于',gte:'大于等于',lt:'小于',lte:'小于等于'}).map(([value,text])=><option key={value} value={value}>{text}</option>)}
    </select>
    {!['exists','not_exists'].includes(row.op)?<Input aria-label={`${label} ${i+1} 值`} value={row.value} disabled={disabled} onChange={e=>onChange(rows.map((r,n)=>n===i?{...r,value:e.target.value}:r))} placeholder={'文本或 JSON，如 "paid"、20'}/>:null}
    <Button type="button" variant="ghost" size="icon" disabled={disabled} aria-label={`移除${label} ${i+1}`} onClick={()=>onChange(rows.filter((_,n)=>n!==i))}><X/></Button>
  </div>)}<Button type="button" variant="outline" disabled={disabled||rows.length>=20} onClick={()=>onChange([...rows,{key:'',op:'eq',value:''}])}><Plus/>添加{label}</Button></div>;
}
