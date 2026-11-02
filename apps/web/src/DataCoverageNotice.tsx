import type {dataCoverage} from '../../../packages/server/src/data-coverage';
export type DataCoverage=ReturnType<typeof dataCoverage>;
export function DataCoverageNotice({coverage}:{coverage?:DataCoverage}){
  if(!coverage?.incomplete)return null;
  return <p role="status" className="notice">
    数据范围不完整。
    {coverage.expired_overlap?`部分请求时间已超过30天保留期；当前可用起点为 ${new Date(coverage.retained_from).toLocaleString('zh-CN')}。缺失数据不表示当时没有事件或没有转化。`:null}
    {coverage.future_overlap?'请求包含尚未到来的时间，后续事件尚未可观测。':null}
  </p>;
}
