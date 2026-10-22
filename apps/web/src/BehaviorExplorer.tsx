import { useEffect, useRef, useState, type FormEvent } from "react";
import { ArrowDown, ChartNoAxesCombined, BookmarkPlus, Plus, Play, X } from "lucide-react";
import { api, ApiError } from "./api";
import { Button } from "./components/ui/button";
import { Input } from "./components/ui/input";
import type {BehaviorSpec} from "../../../packages/server/src/behavior";
import { useQueryJob } from "./useQueryJob";
type FunnelResult = {
  kind: "funnel";
  steps: { name: string; users: number; conversion: number | null }[];
  groups: {
    dimension: { present: boolean; value: unknown };
    users: number[];
  }[];
  other: { users: number[] } | null;
  incomplete_windows: boolean;
  computed_at: string;
};
type RetentionResult = {
  kind: "retention";
  timezone: string;
  cohorts: {
    day: string;
    size: number;
    retention: {
      day: number;
      users: number | null;
      rate: number | null;
      mature: boolean;
    }[];
  }[];
  computed_at: string;
};
type CohortResult = {
  kind: "cohort";
  total: number;
  base_users: number;
  users: { user: string; count: number }[];
  users_truncated: boolean;
  computed_at: string;
};
export type BehaviorResult = FunnelResult | RetentionResult | CohortResult;
type Result = BehaviorResult;
const percent = (v: number | null) =>
  v === null ? "—" : (v * 100).toFixed(1) + "%";
export default function BehaviorExplorer({ prefix,canEdit=false }: { prefix: string;canEdit?:boolean }) {
  const [kind, setKind] = useState<BehaviorSpec["kind"]>("funnel"),
    [steps, setSteps] = useState(["", ""]),
    [result, setResult] = useState<Result | null>(null),
    [busy, setBusy] = useState(false),
    [error, setError] = useState(""),
    [completedSpec,setCompletedSpec]=useState<BehaviorSpec|null>(null),
    [saving,setSaving]=useState(false),
    [notice,setNotice]=useState("");
  const generation=useRef(0);
  useEffect(()=>()=>{generation.current++;},[prefix]);
  const background = useQueryJob<Result>(
    prefix,
    (value,context) => {
      setCompletedSpec(context.spec && typeof context.spec==="object" && "kind" in context.spec && context.spec.kind===value.kind ? structuredClone(context.spec as BehaviorSpec) : null);
      setResult(value);
      setKind(value.kind);
    },
    "funnel,retention,cohort",
    canEdit,
  );
  async function run(e: FormEvent<HTMLFormElement>) {
    e.preventDefault();
    const current=++generation.current;
    background.clear();setCompletedSpec(null);setNotice("");setResult(null);
    const f = new FormData(e.currentTarget),
      now = new Date(),
      days = Number(f.get("range"));
    const base = {
      kind,
      from: new Date(now.getTime() - days * 86400000).toISOString(),
      to: now.toISOString(),
      filters: [],
    };
    const spec:BehaviorSpec =
      kind === "funnel"
        ? {
            ...base,
            steps:[...steps],
            window_hours: Number(f.get("window")),
            group: String(f.get("group") || "") || undefined,
          }
        : kind === "retention"
          ? {
              ...base,
              start_event: String(f.get("start") || ""),
              return_event: String(f.get("return") || ""),
              days: Number(f.get("days")),
            }
          : {
              ...base,
              event_name: String(f.get("event") || ""),
              count_op: f.get("count_op") as BehaviorSpec["count_op"],
              count: Number(f.get("count")),
            };
    if (
      (e.nativeEvent as SubmitEvent).submitter?.getAttribute("value") ===
      "async"
    ) {
      setError("");
      void background.submit(spec);
      return;
    }
    setBusy(true);
    setError("");
    setResult(null);
    try {
      const value=await api<Result>(prefix + "/behavior", {method:"POST",body:JSON.stringify(spec)});
      if(current===generation.current){setResult(value);setCompletedSpec(structuredClone(spec));}
    } catch (e) {
      if(current!==generation.current)return;
      setError(
        e instanceof ApiError && e.code === "query_requires_narrower_range"
          ? "数据超出当前试验环境的安全预算，请缩小时间范围。结果没有被截断返回。"
          : e instanceof ApiError && e.code === "query_busy"
            ? "当前项目正在查询，请稍后重试。"
            : "分析失败，请检查事件名和参数后重试。",
      );
    } finally {
      if(current===generation.current)setBusy(false);
    }
  }
  async function saveReport(e:FormEvent<HTMLFormElement>){
    e.preventDefault();if(!completedSpec||!result||!canEdit||saving)return;
    const current=generation.current;setSaving(true);setError("");setNotice("");
    try{
      await api(prefix+"/reports",{method:"POST",body:JSON.stringify({name:new FormData(e.currentTarget).get("name"),spec:completedSpec,...(result.kind==='retention'?{timezone:result.timezone}:{})})});
      if(current===generation.current)setNotice("报表已保存，可在“分析与报表”中查看或加入仪表盘。");
    }catch(e){if(current===generation.current)setError(e instanceof ApiError&&e.code==='forbidden'?"当前角色不能保存报表。":"保存失败，请重试；时间范围过期时需重新运行分析。");}
    finally{setSaving(false);}
  }
  return (
    <div>
      <section className="panel">
        <div className="panel-heading">
          <div>
            <h2>行为分析</h2>
            <p className="muted">
              使用关联后的用户身份，分析转化、留存和行为分群。
            </p>
          </div>
          <ChartNoAxesCombined size={20} />
        </div>
        <form className="panel-body" onSubmit={run}>
          <div className="query-fields">
            <label>
              分析方式
              <select
                value={kind}
                onChange={(e) => {
                  generation.current++;background.clear();setBusy(false);setCompletedSpec(null);setNotice("");
                  setKind(e.target.value as BehaviorSpec["kind"]);
                  setResult(null);
                  setError("");
                }}
              >
                <option value="funnel">转化漏斗</option>
                <option value="retention">自然日留存</option>
                <option value="cohort">行为分群</option>
              </select>
            </label>
            <label>
              起始时间范围
              <select name="range">
                <option value="7">最近 7 天</option>
                <option value="1">最近 24 小时</option>
                <option value="30">最近 30 天</option>
              </select>
            </label>
            {kind === "funnel" ? (
              <>
                <label>
                  转化窗口
                  <select name="window">
                    <option value="24">24 小时</option>
                    <option value="1">1 小时</option>
                    <option value="168">7 天</option>
                  </select>
                </label>
                <label>
                  按首步属性分组
                  <Input name="group" placeholder="可选，例如 channel" />
                </label>
              </>
            ) : kind === "retention" ? (
              <>
                <label>
                  起始事件
                  <Input
                    name="start"
                    required
                    maxLength={128}
                    placeholder="例如 signup_completed"
                  />
                </label>
                <label>
                  返回事件
                  <Input
                    name="return"
                    required
                    maxLength={128}
                    placeholder="例如 app_opened"
                  />
                </label>
                <label>
                  观察天数
                  <select name="days">
                    <option value="7">7 天</option>
                    <option value="14">14 天</option>
                    <option value="30">30 天</option>
                  </select>
                </label>
              </>
            ) : (
              <>
                <label>
                  事件名称
                  <Input name="event" required maxLength={128} />
                </label>
                <label>
                  次数条件
                  <select name="count_op">
                    <option value="gte">至少</option>
                    <option value="eq">等于（0 表示未做过）</option>
                    <option value="lte">最多</option>
                  </select>
                </label>
                <label>
                  次数
                  <Input
                    name="count"
                    type="number"
                    min={0}
                    max={1000000}
                    required
                    defaultValue={1}
                  />
                </label>
              </>
            )}
          </div>
          {kind === "funnel" ? (
            <div className="funnel-step-inputs">
              {steps.map((s, i) => (
                <label key={i}>
                  <span>第 {i + 1} 步</span>
                  <Input
                    aria-label={`第 ${i + 1} 步事件`}
                    value={s}
                    onChange={(e) =>
                      setSteps((v) =>
                        v.map((x, n) => (n === i ? e.target.value : x)),
                      )
                    }
                    required
                    maxLength={128}
                    placeholder={
                      i === 0
                        ? "例如 product_viewed"
                        : "例如 purchase_completed"
                    }
                  />
                  {steps.length > 2 ? (
                    <Button
                      type="button"
                      variant="ghost"
                      size="icon"
                      aria-label={`删除第 ${i + 1} 步`}
                      onClick={() =>
                        setSteps((v) => v.filter((_, n) => n !== i))
                      }
                    >
                      <X />
                    </Button>
                  ) : null}
                </label>
              ))}
            </div>
          ) : null}
          <div className="query-actions">
            {kind === "funnel" ? (
              <Button
                type="button"
                variant="outline"
                disabled={steps.length >= 5}
                onClick={() => setSteps((v) => [...v, ""])}
              >
                <Plus />
                增加步骤
              </Button>
            ) : (
              <span />
            )}
            <Button disabled={busy}>
              <Play />
              {busy ? "正在计算…" : "开始分析"}
            </Button>
            <Button
              variant="outline"
              value="async"
              disabled={busy || background.busy}
            >
              后台计算
            </Button>
          </div>
          <p className="fine-print">
            试验环境单次最多处理 5000
            条相关事件，并受字节预算限制；所有结果按公司与项目隔离。
          </p>
        </form>
        {background.panel}
      </section>
      {error ? (
        <div role="alert" className="error banner">
          {error}
        </div>
      ) : null}
      {notice?<p role="status" className="notice">{notice}</p>:null}
      {result ? <BehaviorResultView result={result} /> : null}
      {result && completedSpec && canEdit ? <section className="panel"><form className="save-report" onSubmit={saveReport}>
        <Input name="name" aria-label="行为报表名称" placeholder="为本次分析命名" required maxLength={128}/>
        <Button variant="outline" disabled={saving}><BookmarkPlus/>{saving?"正在保存…":"保存行为报表"}</Button>
      </form><p className="fine-print panel-body">保存本次结果对应的条件；未重新运行的表单修改不会改变此报表。</p></section>:null}
    </div>
  );
}

export function BehaviorResultView({result}:{result:BehaviorResult}) {
  return (
        <section className="panel">
          <div className="panel-heading">
            <h2>
              {result.kind === "funnel"
                ? "漏斗结果"
                : result.kind === "retention"
                  ? "留存矩阵"
                  : "分群结果"}
            </h2>
            <span className="muted">
              更新于{" "}
              {new Date(result.computed_at).toLocaleTimeString("zh-CN", {
                hour12: false,
              })}
            </span>
          </div>
          {result.kind === "funnel" ? (
            <>
              <div className="funnel-results panel-body">
                {result.steps.map((s, i) => (
                  <div key={i}>
                    <div className="funnel-result-label">
                      <span>
                        {i + 1}. {s.name}
                      </span>
                      <strong>
                        {s.users} 人 <small>{percent(s.conversion)}</small>
                      </strong>
                    </div>
                    <div className="funnel-bar-track">
                      <div style={{ width: `${(s.conversion || 0) * 100}%` }} />
                    </div>
                    {i < result.steps.length - 1 ? (
                      <ArrowDown size={15} />
                    ) : null}
                  </div>
                ))}
              </div>
              {result.incomplete_windows ? (
                <p className="fine-print panel-body">
                  部分用户的转化窗口尚未结束，后续事件可能改变结果。
                </p>
              ) : null}
              {result.groups.length ? (
                <div className="table-scroll">
                  <table>
                    <thead>
                      <tr>
                        <th>首步属性分组</th>
                        {result.steps.map((s, i) => (
                          <th key={i}>{s.name}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {result.groups.map((g, i) => (
                        <tr key={i}>
                          <td>
                            {!g.dimension.present
                              ? "[缺失]"
                              : JSON.stringify(g.dimension.value)}
                          </td>
                          {g.users.map((n, j) => (
                            <td key={j}>{n}</td>
                          ))}
                        </tr>
                      ))}
                      {result.other ? (
                        <tr>
                          <td>其他分组</td>
                          {result.other.users.map((n, j) => (
                            <td key={j}>{n}</td>
                          ))}
                        </tr>
                      ) : null}
                    </tbody>
                  </table>
                </div>
              ) : null}
              <p className="fine-print panel-body">
                严格按事件时间递增，同一时间不推断步骤顺序；每用户最多计算一次转化。
              </p>
            </>
          ) : result.kind === "retention" ? (
            <>
              {result.cohorts.length ? (
                <div className="table-scroll">
                  <table className="retention-table">
                    <thead>
                      <tr>
                        <th>起始日期</th>
                        <th>用户数</th>
                        {result.cohorts[0]!.retention.map((c) => (
                          <th key={c.day}>D{c.day}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {result.cohorts.map((c) => (
                        <tr key={c.day}>
                          <td>{c.day}</td>
                          <td>{c.size}</td>
                          {c.retention.map((cell) => (
                            <td
                              key={cell.day}
                              title={
                                cell.mature
                                  ? `${cell.users} / ${c.size} 人`
                                  : "观察期尚未完整"
                              }
                              style={{
                                background: cell.mature
                                  ? `rgba(18,103,131,${(cell.rate || 0) * 0.45 + 0.03})`
                                  : undefined,
                              }}
                            >
                              {cell.mature ? percent(cell.rate) : "未成熟"}
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <p className="empty-inline">起始范围内没有满足条件的用户。</p>
              )}
              <p className="fine-print panel-body">
                按 {result.timezone} 自然日计算；未成熟单元格不按 0
                处理。“首次”指所选起始范围内首次事件。
              </p>
            </>
          ) : (
            <>
              <div className="query-summary">
                <div>
                  <span>符合规则</span>
                  <strong>{result.total} 人</strong>
                </div>
                <div>
                  <span>基础用户集合</span>
                  <strong>{result.base_users} 人</strong>
                </div>
              </div>
              <div className="table-scroll">
                <table>
                  <thead>
                    <tr>
                      <th>用户标识</th>
                      <th>匹配事件次数</th>
                    </tr>
                  </thead>
                  <tbody>
                    {result.users.map((u) => (
                      <tr key={u.user}>
                        <td className="mono">{u.user}</td>
                        <td>{u.count}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <p className="fine-print panel-body">
                基础用户取项目当前 30 天保留范围内出现过的身份；最多展示前 100
                人
                {result.users_truncated
                  ? "，结果已注明展示截断，人数为完整匹配值"
                  : ""}
                。
              </p>
            </>
          )}
        </section>
  );
}
