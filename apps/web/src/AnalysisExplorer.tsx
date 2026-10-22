import { useEffect, useRef, useState, type FormEvent } from "react";
import {
  BarChart3,
  BookmarkPlus,
  LayoutDashboard,
  Play,
  Plus,
  Trash2,
  X,
} from "lucide-react";
import { api, ApiError } from "./api";
import { Button } from "./components/ui/button";
import { Input } from "./components/ui/input";
import { useQueryJob } from "./useQueryJob";
import type {
  Filter,
  TrendSpec,
} from "../../../packages/server/src/query-spec";
type Result = {
  spec: TrendSpec;
  summary: {
    value: number | null;
    excluded: number;
    events: number;
    users: number;
  };
  series: { day: string; value: number | null; excluded: number }[];
  groups: {
    dimensions: { present: boolean; value: unknown }[];
    value: number | null;
    excluded: number;
  }[];
  other: { value: number | null; excluded: number } | null;
  groups_truncated: boolean;
  computed_at: string;
};
type Report = { id: string; name: string; spec: TrendSpec; updated_at: string };
type Dashboard = { id: string; name: string; report_ids: string[] };
const format = (value: number | null) =>
  value === null
    ? "—"
    : new Intl.NumberFormat("zh-CN", { maximumFractionDigits: 3 }).format(
        value,
      );
const message = (e: unknown) =>
  e instanceof ApiError
    ? {
        query_busy: "项目正在执行其他查询，请稍后重试",
        invalid_time_range: "查询时间已超过保留范围，请重新选择时间",
        numeric_property_required: "数值聚合需要填写属性名称",
        numeric_filter_required: "数值比较条件必须填写数字",
        forbidden: "当前角色没有此操作权限",
        not_found: "报表不存在或不属于当前公司",
      }[e.code] || "查询或保存失败，请检查条件后重试"
    : "连接失败，请稍后重试";
function ResultView({ result }: { result: Result }) {
  const max = Math.max(1, ...result.series.map((r) => Math.abs(r.value || 0)));
  return (
    <div className="query-result">
      <div className="query-summary">
        <div>
          <span>查询结果</span>
          <strong>{format(result.summary.value)}</strong>
        </div>
        <div>
          <span>匹配事件</span>
          <strong>{format(result.summary.events)}</strong>
        </div>
        <div>
          <span>排除的非数值事件</span>
          <strong>{format(result.summary.excluded)}</strong>
        </div>
      </div>
      <div className="result-range">
        {new Date(result.spec.from).toLocaleString("zh-CN")} 至{" "}
        {new Date(result.spec.to).toLocaleString("zh-CN")} · 左闭右开
      </div>
      <div className="query-bars">
        {result.series.length ? (
          result.series.map((row) => (
            <div key={row.day}>
              <time>{row.day}</time>
              <span className="query-bar-track">
                <span
                  style={{
                    width: `${(Math.abs(row.value || 0) / max) * 100}%`,
                    background: (row.value || 0) < 0 ? "#c37a67" : undefined,
                  }}
                />
              </span>
              <strong>{format(row.value)}</strong>
            </div>
          ))
        ) : (
          <p className="empty-inline">当前条件下没有事件。</p>
        )}
      </div>
      {result.spec.group_by.length ? (
        <div className="table-scroll">
          <table>
            <thead>
              <tr>
                <th>分组（{result.spec.group_by.join(" / ")}）</th>
                <th>结果</th>
                <th>排除数</th>
              </tr>
            </thead>
            <tbody>
              {result.groups.map((g, i) => (
                <tr key={i}>
                  <td>
                    {g.dimensions
                      .map((d) =>
                        !d.present
                          ? "[缺失]"
                          : d.value === null
                            ? "null"
                            : JSON.stringify(d.value),
                      )
                      .join(" / ")}
                  </td>
                  <td>{format(g.value)}</td>
                  <td>{g.excluded}</td>
                </tr>
              ))}
              {result.other ? (
                <tr>
                  <td>其他分组</td>
                  <td>{format(result.other.value)}</td>
                  <td>{result.other.excluded}</td>
                </tr>
              ) : null}
            </tbody>
          </table>
          {result.groups_truncated ? (
            <p className="fine-print panel-body">
              展示前 20
              组，其余按原始事件重新聚合到“其他”，不累加去重用户数或平均数。
            </p>
          ) : null}
        </div>
      ) : null}
      <div className="panel-footer">
        更新于 {new Date(result.computed_at).toLocaleString("zh-CN")} ·
        迟到事件可能影响后续查询结果
      </div>
    </div>
  );
}
export default function AnalysisExplorer({
  prefix,
  canEdit,
}: {
  prefix: string;
  canEdit: boolean;
}) {
  const [reports, setReports] = useState<Report[]>([]),
    [dashboards, setDashboards] = useState<Dashboard[]>([]),
    [result, setResult] = useState<Result | null>(null),
    [busy, setBusy] = useState(false),
    [error, setError] = useState(""),
    [notice, setNotice] = useState("");
  const [metric, setMetric] = useState("count"),
    [filters, setFilters] = useState<
      { key: string; op: Filter["op"]; value: string }[]
    >([]),
    [selected, setSelected] = useState<string[]>([]),
    [cards, setCards] = useState<
      { name: string; result?: Result; error?: string }[]
    >([]);
  const generation = useRef(0);
  const background = useQueryJob<Result>(prefix, setResult, "trend");
  const refresh = async () => {
    const [r, d] = await Promise.all([
      api<{ reports: Report[] }>(prefix + "/reports"),
      api<{ dashboards: Dashboard[] }>(prefix + "/dashboards"),
    ]);
    setReports(r.reports);
    setDashboards(d.dashboards);
  };
  useEffect(() => {
    let live = true;
    setResult(null);
    setCards([]);
    setSelected([]);
    setError("");
    Promise.all([
      api<{ reports: Report[] }>(prefix + "/reports"),
      api<{ dashboards: Dashboard[] }>(prefix + "/dashboards"),
    ])
      .then(([r, d]) => {
        if (live) {
          setReports(r.reports);
          setDashboards(d.dashboards);
        }
      })
      .catch((e) => {
        if (live) setError(message(e));
      });
    return () => {
      live = false;
      generation.current++;
    };
  }, [prefix]);
  async function execute(spec: TrendSpec) {
    setBusy(true);
    setError("");
    setNotice("");
    const version = ++generation.current;
    try {
      const r = await api<Result>(prefix + "/query", {
        method: "POST",
        body: JSON.stringify(spec),
      });
      if (version === generation.current) setResult(r);
    } catch (e) {
      if (version === generation.current) {
        setResult(null);
        setError(message(e));
      }
    } finally {
      if (version === generation.current) setBusy(false);
    }
  }
  function run(e: FormEvent<HTMLFormElement>) {
    e.preventDefault();
    const f = new FormData(e.currentTarget),
      end = new Date(),
      days = Number(f.get("days"));
    try {
      const parsed = filters.map((row) => {
        let value: Filter["value"];
        if (!["exists", "not_exists"].includes(row.op)) {
          try {
            value = JSON.parse(row.value);
          } catch {
            value = row.value;
          }
        }
        return {
          key: row.key,
          op: row.op,
          ...(value !== undefined ? { value } : {}),
        };
      });
      const spec: TrendSpec = {
        kind: "trend",
        from: new Date(end.getTime() - days * 86400000).toISOString(),
        to: end.toISOString(),
        metric: metric as TrendSpec["metric"],
        event_name: String(f.get("event_name") || "") || undefined,
        property: String(f.get("property") || "") || undefined,
        group_by: [
          String(f.get("group_1") || ""),
          String(f.get("group_2") || ""),
        ].filter(Boolean),
        filters: parsed,
      };
      if (
        (e.nativeEvent as SubmitEvent).submitter?.getAttribute("value") ===
        "async"
      )
        void background.submit(spec);
      else void execute(spec);
    } catch (e) {
      setError(message(e));
    }
  }
  async function save(e: FormEvent<HTMLFormElement>) {
    e.preventDefault();
    if (!result) return;
    try {
      await api(prefix + "/reports", {
        method: "POST",
        body: JSON.stringify({
          name: new FormData(e.currentTarget).get("name"),
          spec: result.spec,
        }),
      });
      setNotice("报表已保存，公司内所有成员均可查看。");
      await refresh();
    } catch (e) {
      setError(message(e));
    }
  }
  async function createDashboard(e: FormEvent<HTMLFormElement>) {
    e.preventDefault();
    try {
      await api(prefix + "/dashboards", {
        method: "POST",
        body: JSON.stringify({
          name: new FormData(e.currentTarget).get("name"),
          report_ids: selected,
        }),
      });
      await refresh();
      setNotice("仪表盘已保存。");
    } catch (e) {
      setError(message(e));
    }
  }
  async function openDashboard(board: Dashboard) {
    setBusy(true);
    setCards([]);
    setError("");
    const version = ++generation.current;
    const next: typeof cards = [];
    for (const id of board.report_ids) {
      if (version !== generation.current) break;
      const report = reports.find((r) => r.id === id);
      if (!report) {
        next.push({ name: "已删除的报表", error: "此卡片对应的报表已不存在" });
        continue;
      }
      try {
        const value = await api<Result>(prefix + "/query", {
          method: "POST",
          body: JSON.stringify(report.spec),
        });
        next.push({ name: report.name, result: value });
      } catch (e) {
        next.push({ name: report.name, error: message(e) });
      }
      if (version === generation.current) setCards([...next]);
    }
    if (version === generation.current) setBusy(false);
  }
  return (
    <div className="analysis-explorer">
      {error ? (
        <div role="alert" className="error banner">
          {error}
        </div>
      ) : null}
      {notice ? (
        <div role="status" className="notice">
          {notice}
        </div>
      ) : null}
      <section className="panel">
        <div className="panel-heading">
          <div>
            <h2>趋势分析</h2>
            <p className="muted">设置指标、属性条件和分组，查询真实事件。</p>
          </div>
          <BarChart3 size={19} />
        </div>
        <form onSubmit={run} className="panel-body query-form">
          <div className="query-fields">
            <label>
              事件名称
              <Input
                name="event_name"
                placeholder="留空查询全部业务事件"
                maxLength={128}
              />
            </label>
            <label>
              指标
              <select
                name="metric"
                value={metric}
                onChange={(e) => setMetric(e.target.value)}
              >
                <option value="count">事件次数</option>
                <option value="users">去重用户数</option>
                <option value="sum">数值求和</option>
                <option value="avg">数值平均值</option>
              </select>
            </label>
            <label>
              时间范围
              <select name="days">
                <option value="7">最近 7 天</option>
                <option value="1">最近 24 小时</option>
                <option value="30">最近 30 天</option>
              </select>
            </label>
            {["sum", "avg"].includes(metric) ? (
              <label>
                数值属性
                <Input name="property" required placeholder="例如 amount" />
              </label>
            ) : null}
            <label>
              分组属性 1
              <Input name="group_1" placeholder="可选，例如 channel" />
            </label>
            <label>
              分组属性 2<Input name="group_2" placeholder="可选，例如 plan" />
            </label>
          </div>
          <div className="filter-list">
            {filters.map((f, i) => (
              <div key={i}>
                <Input
                  aria-label={`条件 ${i + 1} 属性`}
                  value={f.key}
                  onChange={(e) =>
                    setFilters((v) =>
                      v.map((r, n) =>
                        n === i ? { ...r, key: e.target.value } : r,
                      ),
                    )
                  }
                  placeholder="属性名称"
                  required
                />
                <select
                  aria-label={`条件 ${i + 1} 运算`}
                  value={f.op}
                  onChange={(e) =>
                    setFilters((v) =>
                      v.map((r, n) =>
                        n === i
                          ? { ...r, op: e.target.value as Filter["op"] }
                          : r,
                      ),
                    )
                  }
                >
                  {Object.entries({
                    eq: "等于",
                    neq: "不等于",
                    in: "属于集合",
                    exists: "存在",
                    not_exists: "不存在",
                    gt: "大于",
                    gte: "大于等于",
                    lt: "小于",
                    lte: "小于等于",
                  }).map(([v, t]) => (
                    <option key={v} value={v}>
                      {t}
                    </option>
                  ))}
                </select>
                {!["exists", "not_exists"].includes(f.op) ? (
                  <Input
                    aria-label={`条件 ${i + 1} 值`}
                    value={f.value}
                    onChange={(e) =>
                      setFilters((v) =>
                        v.map((r, n) =>
                          n === i ? { ...r, value: e.target.value } : r,
                        ),
                      )
                    }
                    placeholder={
                      f.op === "in"
                        ? 'JSON 数组，如 ["web","app"]'
                        : "文本、JSON 数字或 null"
                    }
                  />
                ) : null}
                <Button
                  type="button"
                  variant="ghost"
                  size="icon"
                  aria-label={`删除条件 ${i + 1}`}
                  onClick={() => setFilters((v) => v.filter((_, n) => n !== i))}
                >
                  <X />
                </Button>
              </div>
            ))}
          </div>
          <div className="query-actions">
            <Button
              type="button"
              variant="outline"
              disabled={filters.length >= 20}
              onClick={() =>
                setFilters((v) => [...v, { key: "", op: "eq", value: "" }])
              }
            >
              <Plus />
              添加条件
            </Button>
            <Button disabled={busy}>
              <Play />
              {busy ? "正在查询…" : "运行查询"}
            </Button>
            <Button
              variant="outline"
              value="async"
              disabled={busy || background.busy}
            >
              后台计算
            </Button>
          </div>
        </form>
        {background.panel}
        {result ? <ResultView result={result} /> : null}
        {result && canEdit ? (
          <form className="save-report" onSubmit={save}>
            <Input
              name="name"
              aria-label="报表名称"
              placeholder="为本次查询命名"
              required
              maxLength={128}
            />
            <Button variant="outline">
              <BookmarkPlus />
              保存报表
            </Button>
          </form>
        ) : null}
      </section>
      <div className="connect-grid">
        <section className="panel">
          <div className="panel-heading">
            <h2>公司共享报表</h2>
            <BookmarkPlus size={19} />
          </div>
          {reports.length ? (
            <div className="report-list">
              {reports.map((r) => (
                <div key={r.id}>
                  <div>
                    <strong>{r.name}</strong>
                    <small>
                      {new Date(r.updated_at).toLocaleString("zh-CN")} ·
                      固定时间范围
                    </small>
                  </div>
                  <Button
                    size="sm"
                    variant="outline"
                    disabled={busy}
                    onClick={() => void execute(r.spec)}
                  >
                    查看
                  </Button>
                  {canEdit ? (
                    <Button
                      size="sm"
                      variant="ghost"
                      aria-label={"删除报表 " + r.name}
                      onClick={async () => {
                        try {
                          await api(prefix + "/reports/" + r.id, {
                            method: "DELETE",
                          });
                          await refresh();
                        } catch (e) {
                          setError(message(e));
                        }
                      }}
                    >
                      <Trash2 />
                    </Button>
                  ) : null}
                </div>
              ))}
            </div>
          ) : (
            <p className="empty-inline">运行查询并保存后，报表会显示在这里。</p>
          )}
        </section>
        <section className="panel">
          <div className="panel-heading">
            <h2>仪表盘</h2>
            <LayoutDashboard size={19} />
          </div>
          <div className="panel-body">
            {dashboards.map((d) => (
              <Button
                key={d.id}
                variant="outline"
                className="dashboard-link"
                disabled={busy}
                onClick={() => void openDashboard(d)}
              >
                <LayoutDashboard />
                {d.name}
                <span className="muted">{d.report_ids.length} 张卡片</span>
              </Button>
            ))}
            {!dashboards.length ? (
              <p className="muted">选择报表，组成公司共享的分析视图。</p>
            ) : null}
            {canEdit ? (
              <form onSubmit={createDashboard} className="dashboard-form">
                <label>
                  仪表盘名称
                  <Input name="name" required maxLength={128} />
                </label>
                <div className="report-checks">
                  {reports.map((r) => (
                    <label key={r.id}>
                      <input
                        type="checkbox"
                        checked={selected.includes(r.id)}
                        onChange={(e) =>
                          setSelected((v) =>
                            e.target.checked
                              ? [...v, r.id]
                              : v.filter((id) => id !== r.id),
                          )
                        }
                      />
                      {r.name}
                    </label>
                  ))}
                </div>
                <Button disabled={!selected.length || selected.length > 50}>
                  <Plus />
                  创建仪表盘
                </Button>
              </form>
            ) : null}
          </div>
        </section>
      </div>
      {cards.map((card, i) => (
        <section key={i} className="panel">
          <div className="panel-heading">
            <h2>{card.name}</h2>
          </div>
          {card.result ? (
            <ResultView result={card.result} />
          ) : (
            <p className="error panel-body">{card.error}</p>
          )}
        </section>
      ))}
    </div>
  );
}
