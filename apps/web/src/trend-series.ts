export interface TrendPoint {day:string;value:number|null;excluded:number}
// Normalize before subtracting bounds: opposite finite float8 extremes can
// otherwise create Infinity and silently collapse every point onto an axis.
export function trendSeries(points:readonly TrendPoint[]){
  if(points.length>3660)throw new Error('invalid_trend_series');
  let previous=-Infinity;
  const days=points.map(p=>{
    const time=Date.parse(p.day+'T00:00:00Z');
    if(!/^\d{4}-\d{2}-\d{2}$/.test(p.day)||!Number.isFinite(time)||new Date(time).toISOString().slice(0,10)!==p.day||time<=previous||!(p.value===null||typeof p.value==='number'&&Number.isFinite(p.value))||!Number.isSafeInteger(p.excluded)||p.excluded<0)throw new Error('invalid_trend_series');
    previous=time;return time/86400000;
  });
  const scale=Math.max(0,...points.map(p=>Math.abs(p.value??0)))||1;
  const values=points.filter(p=>p.value!==null).map(p=>p.value!/scale);
  const lower=Math.min(0,...values);let upper=Math.max(0,...values);
  if(upper===lower)upper=lower+1;
  const span=upper-lower;
  const zero=-lower/span,first=days[0]??0,last=days.at(-1)??first;
  const rows=points.map((p,i)=>({...p,x:first===last ? .5 : (days[i]!-first)/(last-first),position:p.value===null?null:(p.value/scale-lower)/span}));
  const segments:Array<typeof rows>=[];
  for(const [i,p] of rows.entries()){
    if(p.position===null)continue;
    const contiguous=i>0&&rows[i-1]!.position!==null&&(days[i]! - days[i-1]!)===1;
    if(contiguous)segments.at(-1)!.push(p);else segments.push([p]);
  }
  return {rows,segments,zero,lower:lower*scale,upper:upper*scale};
}
