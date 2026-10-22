import { useEffect, useState, type FormEvent } from "react";
import { ShieldCheck, RefreshCw, Trash2 } from "lucide-react";
import { api, ApiError } from "./api";
import { Button } from "./components/ui/button";
import { Input } from "./components/ui/input";
type Job = {
  id: string;
  scope: "user" | "project";
  subject_reference: string | null;
  status: "queued" | "running" | "completed" | "failed";
  removed_events: string;
  error: string | null;
  requested_at: string;
  blocked_before: string;
  stream_expires_at: string;
  stream_status: string;
};
const state = {
  queued: "等待清理",
  running: "正在清理",
  completed: "数据库清理完成",
  failed: "清理失败",
};
const message = (e: unknown) =>
  e instanceof ApiError
    ? {
        forbidden: "仅公司管理员可管理删除任务。",
        invalid_confirmation: "确认内容与目标不一致，请重新填写。",
        invalid_deletion: "请填写完整目标、至少 5 个字符的原因及确认内容。",
        deletion_busy: "当前项目有待处理删除任务，请等待完成后再提交。",
        deletion_queue_full: "删除队列已满，请稍后重试。",
        not_found: "任务不存在、状态已变化或无权访问。",
      }[e.code] || "删除请求未完成，请检查任务列表后再重试。"
    : "连接失败，请检查任务列表后再重试。";
export default function PrivacyWorkspace({
  prefix,
  projectName,
  canManage,
}: {
  prefix: string;
  projectName: string;
  canManage: boolean;
}) {
  const [scope, setScope] = useState<"user" | "project">("user"),
    [jobs, setJobs] = useState<Job[]>([]),
    [busy, setBusy] = useState(false),
    [error, setError] = useState(""),
    [notice, setNotice] = useState(""),
    [refresh, setRefresh] = useState(0);
  useEffect(() => {
    if (!canManage) return;
    const abort = new AbortController();
    let timer: ReturnType<typeof setTimeout>;
    const load = async () => {
      try {
        const result = await api<{ jobs: Job[] }>(prefix + "/deletions", {
          signal: abort.signal,
        });
        if (abort.signal.aborted) return;
        setJobs(result.jobs);
        if (
          result.jobs.some(
            (j) => j.status === "queued" || j.status === "running",
          )
        )
          timer = setTimeout(() => void load(), 2000);
      } catch (e) {
        if (!abort.signal.aborted) setError(message(e));
      }
    };
    void load();
    return () => {
      abort.abort();
      clearTimeout(timer);
    };
  }, [prefix, canManage, refresh]);
  async function submit(e: FormEvent<HTMLFormElement>) {
    e.preventDefault();
    const form = e.currentTarget,
      f = new FormData(form);
    setBusy(true);
    setError("");
    setNotice("");
    try {
      const result = await api<{ id: string }>(prefix + "/deletions", {
        method: "POST",
        body: JSON.stringify({
          scope,
          ...(scope === "user"
            ? { distinct_id: String(f.get("identity")) }
            : {}),
          confirmation: f.get("confirmation"),
          reason: f.get("reason"),
        }),
      });
      form.reset();
      setNotice(
        "删除屏障已建立。清理进度见下方任务列表，旧数据重入将被阻止。任务编号：" +
          result.id,
      );
      setRefresh((v) => v + 1);
    } catch (e) {
      setError(message(e));
      setRefresh((v) => v + 1);
    } finally {
      setBusy(false);
    }
  }
  async function retry(id: string) {
    setBusy(true);
    setError("");
    try {
      await api(prefix + "/deletions/" + id + "/retry", { method: "POST" });
      setRefresh((v) => v + 1);
    } catch (e) {
      setError(message(e));
    } finally {
      setBusy(false);
    }
  }
  if (!canManage)
    return (
      <section className="panel">
        <div className="panel-body">
          <p>仅公司管理员可管理删除任务。数据分析的公司可见范围不变。</p>
        </div>
      </section>
    );
  return (
    <div className="privacy-workspace">
      {error ? (
        <div className="alert error" role="alert">
          {error}
        </div>
      ) : null}
      {notice ? (
        <div className="alert success" role="status">
          {notice}
        </div>
      ) : null}
      <section className="panel">
        <div className="panel-heading">
          <div>
            <h2>数据删除</h2>
            <p className="muted">删除不可撤销。先确认目标，再提交后台清理。</p>
          </div>
          <ShieldCheck size={20} />
        </div>
        <form className="panel-body query-form" onSubmit={submit}>
          <div className="query-fields">
            <label>
              删除范围
              <select
                value={scope}
                onChange={(e) => {
                  setScope(e.target.value as typeof scope);
                  setError("");
                }}
              >
                <option value="user">用户及已关联匿名身份</option>
                <option value="project">当前项目全部事件与身份</option>
              </select>
            </label>
            {scope === "user" ? (
              <label>
                目标用户 ID
                <Input
                  name="identity"
                  required
                  maxLength={256}
                  autoComplete="off"
                  placeholder="业务 distinct_id 或匿名 ID"
                />
              </label>
            ) : (
              <label>
                当前项目
                <Input value={projectName} readOnly />
              </label>
            )}
            <label>
              {scope === "user" ? "再次输入相同用户 ID" : "输入项目名称以确认"}
              <Input
                key={scope}
                name="confirmation"
                required
                autoComplete="off"
                maxLength={256}
              />
            </label>
            <label>
              删除原因
              <Input
                name="reason"
                required
                minLength={5}
                maxLength={512}
                placeholder="至少 5 个字符，请勿包含额外个人信息"
              />
            </label>
          </div>
          <div className="privacy-boundary">
            <p>
              {scope === "user"
                ? "将清理用户及当前已知匿名身份的旧历史。为涵盖设备时钟偏差，屏障截止时间包含提交后的 5 分钟；截止后新产生且重新授权的事件可重新采集。迟到身份关联发现的旧历史会继续清理。"
                : "当前项目停止接入，全部写入凭证撤销，不能通过新建凭证恢复接入。项目配置和删除审计仍会保留。"}
            </p>
            <p>
              Kafka/DLQ
              中尚未过期的记录不能即时逐条擦除。后台显示等待流保留期，备份按保留策略过期；恢复旧备份前必须应用最新删除清单。已被查看或下载到平台外的数据无法由本任务收回。
            </p>
          </div>
          <label className="privacy-confirm">
            <input name="understood" type="checkbox" required />
            我已确认目标和范围，理解此操作不可撤销。
          </label>
          <div>
            <Button variant="destructive" disabled={busy}>
              <Trash2 size={15} />
              {busy ? "正在提交…" : "建立删除屏障并清理"}
            </Button>
          </div>
        </form>
      </section>
      <section className="panel">
        <div className="panel-heading">
          <div>
            <h2>删除任务</h2>
            <p className="muted">数据库完成与流、备份的保留边界分别记录。</p>
          </div>
          <Button
            type="button"
            variant="outline"
            disabled={busy}
            onClick={() => setRefresh((v) => v + 1)}
          >
            <RefreshCw size={15} />
            刷新
          </Button>
        </div>
        <div className="table-scroll">
          <table>
            <thead>
              <tr>
                <th>提交时间 / 任务</th>
                <th>范围</th>
                <th>数据库状态</th>
                <th>已删除事件</th>
                <th>流保留期</th>
                <th>操作</th>
              </tr>
            </thead>
            <tbody>
              {jobs.map((j) => (
                <tr key={j.id}>
                  <td>
                    {new Date(j.requested_at).toLocaleString("zh-CN")}
                    <br />
                    <span className="muted">{j.id.slice(0, 8)}</span>
                  </td>
                  <td>
                    {j.scope === "project"
                      ? "整个项目"
                      : "用户 · " + j.subject_reference}
                  </td>
                  <td>
                    {state[j.status]}
                    {j.error ? (
                      <p className="muted">
                        {j.error === "deletion_timeout"
                          ? "执行超时，可重试。"
                          : "执行失败，可重试。"}
                      </p>
                    ) : null}
                  </td>
                  <td>{Number(j.removed_events).toLocaleString("zh-CN")}</td>
                  <td>
                    {j.stream_status === "pending-stream-expiry"
                      ? "等待流保留期"
                      : "配置保留窗口已结束"}
                    <br />
                    <span className="muted">
                      {new Date(j.stream_expires_at).toLocaleDateString(
                        "zh-CN",
                      )}
                    </span>
                  </td>
                  <td>
                    {j.status === "failed" ? (
                      <Button
                        type="button"
                        variant="outline"
                        disabled={busy}
                        onClick={() => void retry(j.id)}
                      >
                        重试清理
                      </Button>
                    ) : (
                      "—"
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        {!jobs.length ? (
          <p className="empty-inline">当前项目没有删除任务。</p>
        ) : null}
        <div className="panel-body">
          <p className="muted">
            “数据库清理完成”不等于全链路物理擦除完成。流窗口按配置保守计算，备份和外部副本需遵守对应保留/恢复流程。
          </p>
        </div>
      </section>
    </div>
  );
}
