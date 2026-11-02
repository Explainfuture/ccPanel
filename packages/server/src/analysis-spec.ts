import {parseTrend,type TrendSpec} from './query-spec';
import {parseBehavior,type BehaviorSpec} from './behavior';
export type AnalysisSpec=TrendSpec|BehaviorSpec;
export function parseQuerySpec(input:unknown,now=Date.now()):AnalysisSpec{return(input as {kind?:unknown})?.kind==='trend'?parseTrend(input,now):parseBehavior(input,now);}
