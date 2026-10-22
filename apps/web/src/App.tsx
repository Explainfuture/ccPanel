import {
  useCallback,
  useEffect,
  useRef,
  useState,
  type FormEvent,
  lazy,
  Suspense,
} from "react";
import {
  Activity,
  ArrowDownToLine,
  ArrowRight,
  BarChart3,
  Braces,
  Check,
  ChevronRight,
  Code2,
  Copy,
  Database,
  Layers3,
  LogOut,
  Plus,
  RefreshCw,
  Search,
  Settings2,
  ShieldCheck,
  Users,
  X,
} from "lucide-react";
import { Button } from "./components/ui/button";
import { Input } from "./components/ui/input";
const AnalysisExplorer = lazy(() => import('./AnalysisExplorer'));
const BehaviorExplorer = lazy(() => import('./BehaviorExplorer'));
const SchemaWorkspace = lazy(() => import('./SchemaWorkspace'));
const UserTimeline = lazy(() => import('./UserTimeline'));
import { api, ApiError, setCsrf } from "./api";
import {
  createAnalytics,
  type Analytics,
} from "../../../packages/sdk-browser/src/index";
type Tenant = {
  id: string;
  name: string;
  role: "admin" | "analyst" | "viewer";
};
type Project = {
  id: string;
  name: string;
  timezone: string;
  origins: string[];
  schema_mode: string;
};
type Me = { user: { email: string }; tenants: Tenant[]; csrf_token: string };
type EventRow = {
  registered?:boolean;
  project_schema_version?:number;
  event_id: string;
  event_name: string;
  distinct_id: string;
  event_time: string;
  received_at: string;
  properties: Record<string, unknown>;
  context: Record<string, unknown>;
};
type Trends = {
  summary: { events: number; users: number; latest_received_at: string | null };
  series: { day: string; events: number; users: number }[];
  top_events: { event_name: string; count: number }[];
  from: string;
  to: string;
  computed_at: string;
};
const number = new Intl.NumberFormat("zh-CN");
const errorText = (e: unknown) =>
  e instanceof ApiError
    ? {
        invalid_credentials: "邮箱或密码不正确",
        origin_not_allowed: "当前地址不在允许列表，请使用配置的 HTTPS 地址",
        csrf_failed: "会话校验失败，请刷新页面后重试",
        query_busy: "当前项目正在查询，请稍后重试",
        rate_limited: "请求过于频繁，请稍后重试",
        not_found: "当前公司下没有此项目，或访问权限已失效",
        account_requires_invitation_acceptance:
          "该邮箱已有账号；邀请加入已有账号的功能尚未开放",
        invalid_member: "请输入有效邮箱、至少 12 位的密码和角色",
      }[e.code] || "操作失败，请稍后重试"
    : "网络连接失败，请检查后重试";
function Logo() {
  return (
    <span className="brand">
      <span className="brand-mark">
        <Activity size={20} />
      </span>
      ccPanel<span className="preview-tag">PREVIEW</span>
    </span>
  );
}
function Login({ onLogin }: { onLogin: (me: Me) => void }) {
  const [busy, setBusy] = useState(false),
    [error, setError] = useState("");
  async function submit(e: FormEvent<HTMLFormElement>) {
    e.preventDefault();
    setBusy(true);
    setError("");
    const f = new FormData(e.currentTarget);
    try {
      await api("/api/auth/login", {
        method: "POST",
        body: JSON.stringify({
          email: f.get("email"),
          password: f.get("password"),
        }),
      });
      const me = await api<Me>("/api/me");
      setCsrf(me.csrf_token);
      onLogin(me);
    } catch (e) {
      setError(errorText(e));
    } finally {
      setBusy(false);
    }
  }
  return (
    <main className="login-layout">
      <section className="login-story">
        <Logo />
        <div>
          <p className="eyebrow">YOUR EVENTS. YOUR ANSWERS.</p>
          <h1>
            让每一次行为，
            <br />
            都有迹可循。
          </h1>
          <p className="login-description">
            从一次点击到一段完整旅程，
            <br />
            在自己的数据里，找到产品的下一步。
          </p>
          <div className="story-flow">
            <span>
              <Activity />
              采集
            </span>
            <ChevronRight />
            <span>
              <Database />
              沉淀
            </span>
            <ChevronRight />
            <span>
              <BarChart3 />
              洞察
            </span>
          </div>
        </div>
        <p className="story-foot">公司数据独立隔离 · 数据由你掌握</p>
      </section>
      <section className="login-form">
        <div className="login-inner">
          <p className="eyebrow">工作区登录</p>
          <h2>欢迎回来</h2>
          <p className="muted">登录你的公司，继续探索用户行为。</p>
          <form onSubmit={submit}>
            <label>
              邮箱
              <Input
                name="email"
                type="email"
                autoComplete="username"
                required
                placeholder="you@company.com"
              />
            </label>
            <label>
              密码
              <Input
                name="password"
                type="password"
                autoComplete="current-password"
                required
              />
            </label>
            {error ? (
              <p role="alert" className="error">
                {error}
              </p>
            ) : null}
            <Button disabled={busy} className="w-full">
              {busy ? "正在登录…" : "登录工作区"}
              <ArrowRight />
            </Button>
          </form>
          <p className="fine-print">
            <ShieldCheck size={15} />
            仅限受邀成员访问。账号由公司管理员创建。
          </p>
        </div>
      </section>
    </main>
  );
}
function TrendChart({ data, timezone }: { data: Trends; timezone: string }) {
  const container = useRef<HTMLDivElement>(null),
    [width, setWidth] = useState(850);
  useEffect(() => {
    if (!container.current) return;
    const observer = new ResizeObserver(([entry]) => {
      if (entry) setWidth(Math.max(280, entry.contentRect.width));
    });
    observer.observe(container.current);
    return () => observer.disconnect();
  }, []);
  const rows: { day: string; events: number; users: number }[] = [];
  const fmt = new Intl.DateTimeFormat("en-CA", {
    timeZone: timezone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });
  const start = new Date(data.from).getTime(),
    end = new Date(data.to).getTime();
  for (let t = start; t < end; t += 86400000) {
    const day = fmt.format(new Date(t));
    if (!rows.some((r) => r.day === day))
      rows.push(
        data.series.find((r) => r.day === day) || { day, events: 0, users: 0 },
      );
  }
  for (const row of data.series)
    if (!rows.some((r) => r.day === row.day)) rows.push(row);
  rows.sort((a, b) => a.day.localeCompare(b.day));
  const max = Math.max(
      4,
      Math.ceil(Math.max(0, ...rows.map((r) => r.events)) / 4) * 4,
    ),
    w = width,
    h = 220,
    pad = 32;
  const x = (i: number) =>
      pad + (w - pad * 2) * (i / Math.max(1, rows.length - 1)),
    y = (n: number) => h - pad - ((h - pad * 2) * n) / max;
  const labelEvery = Math.max(
    1,
    Math.ceil(rows.length / Math.max(2, Math.floor(w / 90))),
  );
  const points = rows.map((r, i) => `${x(i)},${y(r.events)}`).join(" ");
  return (
    <div className="chart" ref={container}>
      <svg
        viewBox={`0 0 ${w} ${h}`}
        role="img"
        aria-label="按项目时区统计的每日事件数量"
      >
        <title>每日事件次数</title>
        {[0, 0.25, 0.5, 0.75, 1].map((n) => (
          <g key={n}>
            <line
              x1={pad}
              x2={w - pad}
              y1={y(max * n)}
              y2={y(max * n)}
              stroke="#e8edf1"
              strokeDasharray="3 4"
            />
            <text x="0" y={y(max * n) + 4} fill="#718096" fontSize="10">
              {Math.round(max * n)}
            </text>
          </g>
        ))}
        {rows.length ? (
          <>
            <polygon
              points={`${pad},${y(0)} ${points} ${x(rows.length - 1)},${y(0)}`}
              fill="#e9f5fa"
            />
            <polyline
              points={points}
              fill="none"
              stroke="#087f9c"
              strokeWidth="2.5"
              strokeLinejoin="round"
            />
            {rows.map((r, i) => (
              <g key={r.day}>
                <circle cx={x(i)} cy={y(r.events)} r="3" fill="#087f9c">
                  <title>
                    {r.day}：{r.events} 次
                  </title>
                </circle>
                {i === rows.length - 1 ||
                (i < rows.length - 2 && i % labelEvery === 0) ? (
                  <text
                    x={x(i)}
                    y={h - 6}
                    textAnchor="middle"
                    fontSize="10"
                    fill="#718096"
                  >
                    {r.day.slice(5)}
                  </text>
                ) : null}
              </g>
            ))}
          </>
        ) : null}
      </svg>
    </div>
  );
}
export default function App() {
  const [me, setMe] = useState<Me | null>(null),
    [checking, setChecking] = useState(true),
    [tenantId, setTenantId] = useState(""),
    [projects, setProjects] = useState<Project[]>([]),
    [projectId, setProjectId] = useState("");
  const [tab, setTab] = useState("overview"),
    [error, setError] = useState(""),
    [busy, setBusy] = useState(false),
    [refresh, setRefresh] = useState(0),
    [days, setDays] = useState("7"),
    [name, setName] = useState("");
  const [trends, setTrends] = useState<Trends | null>(null),
    [events, setEvents] = useState<EventRow[]>([]),
    [page, setPage] = useState(0),
    [filter, setFilter] = useState(""),
    [detail, setDetail] = useState<EventRow | null>(null);
  const [timelineUser,setTimelineUser]=useState<string|null>(null);
  const [showCreate, setShowCreate] = useState(false),
    [writeKey, setWriteKey] = useState(""),
    [keys, setKeys] = useState<
      { id: string; label: string; revoked_at: string | null }[]
    >([]),
    [copied, setCopied] = useState(false);
  const [members, setMembers] = useState<
      { id: string; email: string; role: string }[]
    >([]),
    [notice, setNotice] = useState("");
  const analytics = useRef<Analytics | null>(null);
  useEffect(() => {
    if (!showCreate && !detail && !timelineUser) return;
    const previous = document.activeElement as HTMLElement | null;
    const focusable = () => Array.from(document.querySelector<HTMLElement>('.modal')?.querySelectorAll<HTMLElement>('button:not(:disabled), input, select, summary, a[href], [tabindex="0"]') || []);
    focusable()[0]?.focus();
    const keydown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') { setShowCreate(false); setDetail(null); setTimelineUser(null); }
      if (e.key !== 'Tab') return;
      const items = focusable(), first = items[0], last = items.at(-1);
      if (e.shiftKey && document.activeElement === first) { e.preventDefault(); last?.focus(); }
      else if (!e.shiftKey && document.activeElement === last) { e.preventDefault(); first?.focus(); }
    };
    document.addEventListener('keydown', keydown);
    return () => { document.removeEventListener('keydown', keydown); previous?.focus(); };
  }, [showCreate, detail, timelineUser]);
  useEffect(() => {
    let active = true;
    api<Me>("/api/me")
      .then((v) => {
        if (active) {
          setMe(v);
          setCsrf(v.csrf_token);
        }
      })
      .catch((e) => {
        if (!(e instanceof ApiError && e.status === 401) && active)
          setError(errorText(e));
      })
      .finally(() => {
        if (active) setChecking(false);
      });
    return () => {
      active = false;
    };
  }, []);
  useEffect(() => {
    if (me && !me.tenants.some((t) => t.id === tenantId))
      setTenantId(me.tenants[0]?.id || "");
  }, [me, tenantId]);
  const tenant = me?.tenants.find((t) => t.id === tenantId),
    project = projects.find((p) => p.id === projectId);
  useEffect(() => {
    if (!tenantId) return;
    let active = true;
    setError("");
    setProjects([]);
    setProjectId("");
    setTrends(null);
    setEvents([]);
    setWriteKey("");
    api<{ projects: Project[] }>(`/api/tenants/${tenantId}/projects`)
      .then((v) => {
        if (active) {
          setProjects(v.projects);
          setProjectId(v.projects[0]?.id || "");
        }
      })
      .catch((e) => {
        if (active) setError(errorText(e));
      });
    return () => {
      active = false;
    };
  }, [tenantId]);
  const prefix = `/api/tenants/${tenantId}/projects/${projectId}`;
  useEffect(() => {
    analytics.current?.destroy();
    analytics.current = null;
    setWriteKey("");
    setKeys([]);
    setPage(0);
    setDetail(null);
    setTimelineUser(null);
    setTrends(null);
    setEvents([]);
  }, [tenantId, projectId]);
  useEffect(() => {
    if (!tenantId || (tab !== 'members' && !projectId)) return;
    let active = true;
    setBusy(true);
    setError("");
    const end = new Date(),
      from = new Date(end.getTime() - Number(days) * 86400000);
    const params = new URLSearchParams({
      from: from.toISOString(),
      to: end.toISOString(),
      ...(name ? { event_name: name } : {}),
    });
    const work =
      tab === "overview"
        ? api<Trends>(prefix + "/trends?" + params).then((v) => {
            if (active) setTrends(v);
          })
        : tab === "events"
          ? api<{ events: EventRow[] }>(
              prefix +
                "/events?" +
                new URLSearchParams({
                  page: String(page),
                  ...(filter ? { event_name: filter } : {}),
                }),
            ).then((v) => {
              if (active) setEvents(v.events);
            })
          : tab === "connect" && tenant?.role === "admin"
            ? api<{ keys: typeof keys }>(prefix + "/keys").then((v) => {
                if (active) setKeys(v.keys);
              })
            : tab === "members"
              ? api<{ members: typeof members }>(
                  `/api/tenants/${tenantId}/members`,
                ).then((v) => {
                  if (active) setMembers(v.members);
                })
              : Promise.resolve();
    work
      .catch((e) => {
        if (active) {
          setError(errorText(e));
          setTrends(null);
          setEvents([]);
        }
      })
      .finally(() => {
        if (active) setBusy(false);
      });
    return () => {
      active = false;
    };
  }, [
    prefix,
    projectId,
    tenantId,
    tab,
    refresh,
    days,
    name,
    page,
    filter,
    tenant?.role,
  ]);
  const reload = useCallback(() => setRefresh((n) => n + 1), []);
  const reloadProjects=async()=>{const data=await api<{projects:Project[]}>(`/api/tenants/${tenantId}/projects`);setProjects(data.projects);};
  async function createProject(e: FormEvent<HTMLFormElement>) {
    e.preventDefault();
    const f = new FormData(e.currentTarget);
    setError("");
    try {
      const p = await api<{ id: string }>(`/api/tenants/${tenantId}/projects`, {
        method: "POST",
        body: JSON.stringify({
          name: f.get("name"),
          origins: String(f.get("origins"))
            .split(",")
            .map((s) => s.trim())
            .filter(Boolean),
        }),
      });
      const all = await api<{ projects: Project[] }>(
        `/api/tenants/${tenantId}/projects`,
      );
      setProjects(all.projects);
      setProjectId(p.id);
      setShowCreate(false);
      setTab("connect");
    } catch (e) {
      setError(errorText(e));
    }
  }
  async function createKey() {
    try {
      const k = await api<{ write_key: string }>(prefix + "/keys", {
        method: "POST",
        body: "{}",
      });
      setWriteKey(k.write_key);
      analytics.current?.updateWriteKey(k.write_key);
      reload();
    } catch (e) {
      setError(errorText(e));
    }
  }
  async function testEvent() {
    try {
      if (!analytics.current)
        analytics.current = createAnalytics({
          endpoint: location.origin + "/v1/batch",
          projectId,
          writeKey,
          consent: true,
        });
      const r = await analytics.current.track("sdk_connection_test", {
        source: "接入验证按钮",
      });
      if (r.status === "rejected") throw new Error(r.reason);
      const f = await analytics.current.flush();
      setNotice(
        f.confirmed
          ? "接入端已确认接收；稍后刷新事件列表查看入库结果。"
          : "事件仍在本地待发队列，请检查接入配置。",
      );
    } catch (e) {
      setError(errorText(e));
    }
  }
  async function addMember(e: FormEvent<HTMLFormElement>) {
    e.preventDefault();
    const form = e.currentTarget,
      f = new FormData(form);
    try {
      await api(`/api/tenants/${tenantId}/members`, {
        method: "POST",
        body: JSON.stringify(Object.fromEntries(f)),
      });
      form.reset();
      reload();
    } catch (e) {
      setError(errorText(e));
    }
  }
  if (checking)
    return (
      <main className="loading-screen">
        <Logo />
        <p>正在连接工作区…</p>
      </main>
    );
  if (!me && error) return <main className="loading-screen"><Logo/><p role="alert">暂时无法连接工作区，请稍后重试。</p><Button onClick={() => location.reload()}>重新连接</Button></main>;
  if (!me) return <Login onLogin={setMe} />;
  return (
    <div className="app-shell">
      <aside className="sidebar" inert={showCreate || !!detail || !!timelineUser}>
        <Logo />
        <label className="company-label">
          当前公司
          <select
            aria-label="选择公司"
            value={tenantId}
            onChange={(e) => {
              setTenantId(e.target.value);
              setNotice("");
            }}
          >
            {me.tenants.map((t) => (
              <option key={t.id} value={t.id}>
                {t.name}
              </option>
            ))}
          </select>
        </label>
        <p className="nav-label">数据工作台</p>
        <nav>
          {[
            { id: "overview", label: "概览", icon: BarChart3 },
            { id: "events", label: "事件探索", icon: Activity },
            { id: "analysis", label: "分析与报表", icon: BarChart3 },
            { id: "behavior", label: "漏斗与留存", icon: Layers3 },
            { id: "schemas", label: "事件规范", icon: Settings2 },
            { id: "connect", label: "数据接入", icon: Code2 },
            { id: "members", label: "公司成员", icon: Users },
          ].map((n) => (
            <button
              key={n.id}
              className={`nav-item ${tab === n.id ? "active" : ""}`}
              onClick={() => {
                setTab(n.id);
                setNotice("");
              }}
            >
              <n.icon size={18} />
              {n.label}
              {tab === n.id ? <span className="nav-active-dot" /> : null}
            </button>
          ))}
        </nav>
        <div className="sidebar-note">
          <ShieldCheck size={19} />
          <strong>公司数据独立隔离</strong>
          <p>
            所有成员共享本公司数据，
            <br />
            其他公司无法访问。
          </p>
        </div>
        <div className="account">
          <span className="avatar">
            {me.user.email.slice(0, 1).toUpperCase()}
          </span>
          <div>
            <strong>{me.user.email.split("@")[0]}</strong>
            <small>
              {tenant?.role === "admin"
                ? "公司管理员"
                : tenant?.role === "analyst"
                  ? "分析成员"
                  : "只读成员"}
            </small>
          </div>
          <button
            aria-label="退出登录"
            onClick={async () => {
              await api("/api/auth/logout", { method: "POST", body: "{}" });
              analytics.current?.destroy();
              setCsrf("");
              setMe(null);
            }}
          >
            <LogOut size={17} />
          </button>
        </div>
      </aside>
      <div className="main-shell" inert={showCreate || !!detail || !!timelineUser}>
        <header className="topbar">
          <div className="breadcrumb">
            工作区
            <ChevronRight size={14} />
            <span>{tenant?.name || "未加入公司"}</span>
          </div>
          <span className="live-badge">
            <span />
            开发预览
          </span>
          <button className="mobile-logout" aria-label="退出登录" onClick={async () => {
            try { await api('/api/auth/logout', { method: 'POST', body: '{}' }); analytics.current?.destroy(); setCsrf(''); setMe(null); }
            catch (e) { setError(errorText(e)); }
          }}><LogOut size={16} /></button>
        </header>
        <main className="workspace">
          <div className="page-heading">
            <div>
              <p className="eyebrow">PRODUCT ANALYTICS</p>
              <h1>
                {
                  {
                    overview: "数据概览",
                    events: "事件探索",
                    analysis: "分析与报表",
                    behavior: "理解用户旅程",
                    schemas: "事件规范与设置",
                    connect: "连接你的数据",
                    members: "公司成员",
                  }[tab]
                }
              </h1>
              <p className="muted">
                {
                  {
                    overview: "从真实行为出发，看见产品正在发生的变化。",
                    events: "每一条事件，都是理解用户的线索。",
                    analysis: "组合指标与属性，保存公司共享的分析视图。",
                    behavior: "从转化到回访，探索关联用户的真实行为。",
                    schemas: "统一业务事件口径，让每一次分析都有清晰定义。",
                    connect: "安装 SDK，让业务行为进入你的分析工作台。",
                    members: "同一公司的成员，共享全部项目与分析数据。",
                  }[tab]
                }
              </p>
            </div>
            <div className="page-actions">
              {projects.length ? (
                <select
                  aria-label="选择项目"
                  value={projectId}
                  onChange={(e) => setProjectId(e.target.value)}
                >
                  {projects.map((p) => (
                    <option key={p.id} value={p.id}>
                      {p.name}
                    </option>
                  ))}
                </select>
              ) : null}
              {tenant?.role === "admin" ? (
                <Button variant="outline" onClick={() => setShowCreate(true)}>
                  <Plus />
                  新建项目
                </Button>
              ) : null}
            </div>
          </div>
          {error ? (
            <div className="error banner" role="alert">
              {error}
              <Button variant="ghost" size="sm" onClick={reload}>
                重试
              </Button>
            </div>
          ) : null}
          {notice ? (
            <div className="notice" role="status">
              <Check size={17} />
              {notice}
            </div>
          ) : null}
          {!tenant ? (
            <div className="empty-state">
              <Users />
              <h2>你还没有加入公司</h2>
              <p>请联系平台管理员开通公司或添加成员关系。</p>
            </div>
          ) : !project && tab !== "members" ? (
            <div className="empty-state">
              <Layers3 />
              <h2>从第一个项目开始</h2>
              <p>为网站或应用创建项目，再通过 SDK 记录第一条事件。</p>
              {tenant.role === "admin" ? (
                <Button onClick={() => setShowCreate(true)}>
                  <Plus />
                  创建第一个项目
                </Button>
              ) : (
                <p>请联系公司管理员创建项目。</p>
              )}
            </div>
          ) : (
            <>
              {tab === "overview" && project ? (
                <>
                  <div className="analysis-toolbar">
                    <div className="toolbar-left">
                      <span className="small-icon">
                        <Layers3 size={16} />
                      </span>
                      <strong>{project.name}</strong>
                      <span className="muted">/</span>
                      <span className="muted">{project.timezone}</span>
                    </div>
                    <div className="toolbar-right">
                      <select
                        aria-label="事件筛选"
                        value={name}
                        onChange={(e) => setName(e.target.value)}
                      >
                        <option value="">全部事件</option>
                        {trends?.top_events.map((e) => (
                          <option key={e.event_name} value={e.event_name}>
                            {e.event_name}
                          </option>
                        ))}
                      </select>
                      <select
                        aria-label="时间范围"
                        value={days}
                        onChange={(e) => setDays(e.target.value)}
                      >
                        <option value="1">最近 24 小时</option>
                        <option value="7">最近 7 天</option>
                        <option value="30">最近 30 天</option>
                      </select>
                      <Button
                        variant="outline"
                        size="icon"
                        aria-label="刷新数据"
                        onClick={reload}
                        disabled={busy}
                      >
                        <RefreshCw className={busy ? "spin" : ""} />
                      </Button>
                    </div>
                  </div>
                  <section className="summary-strip">
                    <div>
                      <span>事件总次数</span>
                      <strong>
                        {busy
                          ? "—"
                          : number.format(trends?.summary.events || 0)}
                      </strong>
                      <small>去重后的独立事件</small>
                    </div>
                    <div>
                      <span>活跃用户</span>
                      <strong>
                        {busy ? "—" : number.format(trends?.summary.users || 0)}
                      </strong>
                      <small>按关联身份去重</small>
                    </div>
                    <div>
                      <span>已记录的事件类型</span>
                      <strong>
                        {busy
                          ? "—"
                          : trends
                            ? `${trends.top_events.length}${trends.top_events.length === 20 ? "+" : ""}`
                            : "0"}
                      </strong>
                      <small>当前范围内，最多展示 20 类</small>
                    </div>
                    <div className="summary-freshness">
                      <span>最近接收时间</span>
                      <strong>
                        {trends?.summary.latest_received_at
                          ? new Date(
                              trends.summary.latest_received_at,
                            ).toLocaleTimeString("zh-CN", { hour12: false })
                          : "等待接入"}
                      </strong>
                      <small>入库后的最新事件</small>
                    </div>
                  </section>
                  <section className="panel trend-panel">
                    <div className="panel-heading">
                      <div>
                        <h2>事件趋势</h2>
                        <p className="muted">按天观察用户行为的变化</p>
                      </div>
                      <span className="chart-legend">
                        <i />
                        事件次数
                      </span>
                    </div>
                    {busy ? (
                      <div className="chart-loading">正在查询事件…</div>
                    ) : trends && trends.summary.events > 0 ? (
                      <TrendChart data={trends} timezone={project.timezone} />
                    ) : (
                      <div className="chart-empty">
                        <Activity size={30} />
                        <h3>等待你的第一条事件</h3>
                        <p>连接 SDK 后，这里将展示真实的行为趋势。</p>
                        <Button
                          variant="outline"
                          onClick={() => setTab("connect")}
                        >
                          配置数据接入
                          <ArrowRight />
                        </Button>
                      </div>
                    )}
                    <div className="panel-footer">
                      <span>按事件发生时间统计 · 当前自然日可能尚未完整</span>
                      <span>
                        {trends
                          ? "更新于 " +
                            new Date(trends.computed_at).toLocaleTimeString(
                              "zh-CN",
                              { hour12: false },
                            )
                          : "尚未查询"}
                      </span>
                    </div>
                  </section>
                  <section className="panel">
                    <div className="panel-heading">
                      <h2>主要事件</h2>
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() => setTab("events")}
                      >
                        查看全部事件
                        <ArrowRight />
                      </Button>
                    </div>
                    {trends?.top_events.length ? (
                      <div className="top-events">
                        {trends.top_events.map((e, i) => (
                          <button
                            key={e.event_name}
                            onClick={() => {
                              setFilter(e.event_name);
                              setPage(0);
                              setTab("events");
                            }}
                          >
                            <span className="event-rank">
                              {String(i + 1).padStart(2, "0")}
                            </span>
                            <span className="event-symbol">
                              <Braces size={16} />
                            </span>
                            <strong>{e.event_name}</strong>
                            <div className="event-meter">
                              <span
                                style={{
                                  width: `${(e.count / trends.top_events[0]!.count) * 100}%`,
                                }}
                              />
                            </div>
                            <span className="mono">
                              {number.format(e.count)}
                            </span>
                            <ChevronRight size={15} />
                          </button>
                        ))}
                      </div>
                    ) : (
                      <p className="empty-inline">
                        还没有可展示的事件。先完成数据接入，再回来看看。
                      </p>
                    )}
                  </section>
                </>
              ) : null}
              {tab === 'analysis' && project ? <Suspense fallback={<div className="chart-loading">正在打开分析工作台…</div>}><AnalysisExplorer key={prefix} prefix={prefix} canEdit={tenant.role !== 'viewer'} /></Suspense> : null}
              {tab === 'behavior' && project ? <Suspense fallback={<div className="chart-loading">正在打开行为分析…</div>}><BehaviorExplorer key={prefix} prefix={prefix}/></Suspense> : null}
              {tab === 'schemas' && project ? <Suspense fallback={<div className="chart-loading">正在读取事件规范…</div>}><SchemaWorkspace key={prefix} prefix={prefix} project={project} canEdit={tenant.role==='admin'} onSaved={reloadProjects}/></Suspense> : null}
              {tab === "events" && project ? (
                <section className="panel">
                  <div className="panel-heading">
                    <div>
                      <h2>事件明细</h2>
                      <p className="muted">
                        最近 30 天，每页 50 条 · 点击查看事件内容
                      </p>
                    </div>
                    <form
                      className="search-form"
                      onSubmit={(e) => {
                        e.preventDefault();
                        setFilter(
                          String(
                            new FormData(e.currentTarget).get("event_name") ||
                              "",
                          ),
                        );
                        setPage(0);
                      }}
                    >
                      <Search size={16} />
                      <Input
                        name="event_name"
                        aria-label="筛选事件名称"
                        defaultValue={filter}
                        placeholder="按完整事件名筛选"
                      />
                      <Button variant="outline" size="sm">
                        筛选
                      </Button>
                    </form>
                  </div>
                  <div className="table-scroll">
                    <table>
                      <thead>
                        <tr>
                          <th>事件名称</th>
                          <th>用户标识</th>
                          <th>发生时间</th>
                          <th>事件 ID</th>
                        </tr>
                      </thead>
                      <tbody>
                        {events.map((e) => (
                          <tr key={e.event_id}>
                            <td>
                              <button
                                className="event-link"
                                onClick={() => setDetail(e)}
                              >
                                <Braces size={15} />
                                {e.event_name}
                              </button>
                            </td>
                            <td className="mono"><button className="user-link" onClick={()=>setTimelineUser(e.distinct_id)}>{e.distinct_id}</button></td>
                            <td>
                              {new Date(e.event_time).toLocaleString("zh-CN")}
                            </td>
                            <td className="mono muted">
                              {e.event_id.slice(0, 8)}…
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                  {!events.length ? (
                    <p className="empty-inline">
                      {busy ? "正在读取…" : "没有找到符合条件的事件。"}
                    </p>
                  ) : null}
                  <div className="panel-footer">
                    <Button
                      variant="outline"
                      size="sm"
                      disabled={!page || busy}
                      onClick={() => setPage((n) => n - 1)}
                    >
                      上一页
                    </Button>
                    <span>第 {page + 1} 页</span>
                    <Button
                      variant="outline"
                      size="sm"
                      disabled={events.length < 50 || busy}
                      onClick={() => setPage((n) => n + 1)}
                    >
                      下一页
                    </Button>
                  </div>
                </section>
              ) : null}
              {tab === "connect" && project ? (
                <div className="connect-grid">
                  <section className="panel">
                    <div className="panel-heading">
                      <div>
                        <h2>接入 Browser SDK</h2>
                        <p className="muted">
                          显式记录业务事件，发送过程由 SDK 处理。
                        </p>
                      </div>
                      <Code2 size={20} />
                    </div>
                    <div className="panel-body">
                      <p className="field-caption">项目 ID</p>
                      <code className="code-line">{project.id}</code>
                      <p className="field-caption">接入地址</p>
                      <code className="code-line">
                        {location.origin}/v1/batch
                      </code>
                      <p className="field-caption">允许的网页来源</p>
                      <p>
                        {project.origins.join(", ") ||
                          "未配置浏览器来源；请创建项目时填写。"}
                      </p>
                      <p className="field-caption">调用示例</p>
                      <pre>{`import { createAnalytics } from '@ccpanel/sdk-browser';\n\nconst analytics = createAnalytics({\n  endpoint: '${location.origin}/v1/batch',\n  projectId: '${project.id}',\n  writeKey: 'YOUR_WRITE_KEY',\n  consent: true, // 由业务确认已取得授权\n});\n\nanalytics.track('button_clicked', {\n  button_name: '立即购买',\n});`}</pre>
                      <p className="fine-print">
                        当前 SDK 为预览包，通过项目构建产物安装，尚未发布 npm。
                      </p>
                    </div>
                  </section>
                  <section className="panel">
                    <div className="panel-heading">
                      <h2>写入凭证</h2>
                      <ShieldCheck size={19} />
                    </div>
                    <div className="panel-body">
                      {tenant.role === "admin" ? (
                        <>
                          <p className="muted">
                            凭证仅能写入此项目，不提供任何查询权限。新凭证只展示一次。
                          </p>
                          <Button onClick={createKey}>
                            <Plus />
                            创建写入凭证
                          </Button>
                          {writeKey ? (
                            <div className="key-box">
                              <code>{writeKey}</code>
                              <Button
                                variant="outline"
                                size="sm"
                                onClick={async () => {
                                  await navigator.clipboard.writeText(writeKey);
                                  setCopied(true);
                                  setTimeout(() => setCopied(false), 2000);
                                }}
                              >
                                {copied ? <Check /> : <Copy />}
                                {copied ? "已复制" : "复制"}
                              </Button>
                              <Button variant="outline" onClick={testEvent}>
                                发送一条接入测试事件
                              </Button>
                            </div>
                          ) : null}
                          <div className="key-list">
                            {keys.map((k) => (
                              <div key={k.id}>
                                <div>
                                  <strong>{k.label}</strong>
                                  <small className="mono">
                                    {k.id.slice(0, 8)} ·{" "}
                                    {k.revoked_at ? "已撤销" : "有效"}
                                  </small>
                                </div>
                                <Button
                                  variant="ghost"
                                  size="sm"
                                  disabled={!!k.revoked_at}
                                  onClick={async () => {
                                    try {
                                      await api(prefix + "/keys/" + k.id, {
                                        method: "DELETE",
                                      });
                                      if (writeKey) setWriteKey("");
                                      reload();
                                    } catch (e) {
                                      setError(errorText(e));
                                    }
                                  }}
                                >
                                  撤销
                                </Button>
                              </div>
                            ))}
                          </div>
                        </>
                      ) : (
                        <p className="muted">
                          请联系公司管理员创建或轮换写入凭证。
                        </p>
                      )}
                    </div>
                  </section>
                </div>
              ) : null}
              {tab === "members" ? (
                <>
                  <section className="panel">
                    <div className="panel-heading">
                      <div>
                        <h2>公司成员</h2>
                        <p className="muted">
                          每位成员都能查看本公司全部项目，角色只区分操作权限。
                        </p>
                      </div>
                      <Users size={20} />
                    </div>
                    <div className="table-scroll">
                      <table>
                        <thead>
                          <tr>
                            <th>邮箱</th>
                            <th>角色</th>
                            <th>数据范围</th>
                          </tr>
                        </thead>
                        <tbody>
                          {members.map((m) => (
                            <tr key={m.id}>
                              <td>{m.email}</td>
                              <td>
                                {
                                  {
                                    admin: "管理员",
                                    analyst: "分析成员",
                                    viewer: "只读成员",
                                  }[m.role]
                                }
                              </td>
                              <td>
                                <span className="scope-badge">
                                  本公司全部项目
                                </span>
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </section>
                  {tenant.role === "admin" ? (
                    <section className="panel">
                      <div className="panel-heading">
                        <h2>添加新成员</h2>
                      </div>
                      <form
                        className="member-form panel-body"
                        onSubmit={addMember}
                      >
                        <label>
                          邮箱
                          <Input name="email" type="email" required />
                        </label>
                        <label>
                          初始密码
                          <Input
                            name="password"
                            type="password"
                            minLength={12}
                            autoComplete="new-password"
                            required
                          />
                        </label>
                        <label>
                          角色
                          <select name="role">
                            <option value="analyst">分析成员</option>
                            <option value="viewer">只读成员</option>
                            <option value="admin">管理员</option>
                          </select>
                        </label>
                        <Button>创建账号</Button>
                      </form>
                      <p className="fine-print panel-body">
                        现有平台账号的跨公司邀请流程仍在开发；这里仅创建新账号。
                      </p>
                    </section>
                  ) : null}
                </>
              ) : null}
            </>
          )}
          <footer className="workspace-footer">
            <span>ccPanel / 产品分析工作台</span>
            <span>数据属于当前公司 · 开发预览版本</span>
          </footer>
        </main>
      </div>
      {showCreate ? (
        <div className="modal-backdrop">
          <section
            className="modal"
            role="dialog"
            aria-modal="true"
            aria-labelledby="create-heading"
          >
            <div className="panel-heading">
              <h2 id="create-heading">创建项目</h2>
              <Button
                variant="ghost"
                size="icon"
                aria-label="关闭"
                onClick={() => setShowCreate(false)}
              >
                <X />
              </Button>
            </div>
            <form className="panel-body" onSubmit={createProject}>
              <label>
                项目名称
                <Input
                  name="name"
                  required
                  maxLength={128}
                  placeholder="例如：官网 / 商城"
                  autoFocus
                />
              </label>
              <label>
                允许的网页来源
                <Input
                  name="origins"
                  defaultValue={location.origin}
                  required
                  placeholder="https://your-site.com"
                />
              </label>
              <p className="fine-print">
                多个来源用逗号分隔，只填协议和域名，不含路径。
              </p>
              {error ? <p className="error">{error}</p> : null}
              <Button>
                创建项目
                <ArrowRight />
              </Button>
            </form>
          </section>
        </div>
      ) : null}
      {timelineUser ? <Suspense fallback={<div className="modal-backdrop"><p className="notice">正在读取用户时间线…</p></div>}><UserTimeline key={prefix+timelineUser} prefix={prefix} identity={timelineUser} onClose={()=>setTimelineUser(null)}/></Suspense> : null}
      {detail ? (
        <div className="modal-backdrop">
          <section
            className="modal detail-modal"
            role="dialog"
            aria-modal="true"
            aria-labelledby="detail-heading"
          >
            <div className="panel-heading">
              <h2 id="detail-heading">{detail.event_name}</h2>
              <Button
                variant="ghost"
                size="icon"
                aria-label="关闭详情"
                onClick={() => setDetail(null)}
              >
                <X />
              </Button>
            </div>
            <div className="panel-body">
              <p className="field-caption">事件 ID</p>
              <code>{detail.event_id}</code>
              <p className="field-caption">发生 / 接收时间</p>
              <p>
                {new Date(detail.event_time).toLocaleString("zh-CN")} /{" "}
                {new Date(detail.received_at).toLocaleString("zh-CN")}
              </p>
              <p className="field-caption">事件属性</p>
              <pre>{JSON.stringify(detail.properties, null, 2)}</pre>
              <p className="field-caption">上下文</p>
              <pre>{JSON.stringify(detail.context, null, 2)}</pre>
            </div>
          </section>
        </div>
      ) : null}
    </div>
  );
}
