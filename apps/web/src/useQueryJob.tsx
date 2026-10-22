import { useEffect, useRef, useState } from "react";
import { api, ApiError } from "./api";
import { Button } from "./components/ui/button";

type Job = {
  id: string;
  kind?: string;
  spec?: { kind: string };
  status: "queued" | "running" | "succeeded" | "failed" | "cancelled";
  error?: string;
  created_at: string;
  finished_at?: string;
  expires_at: string;
  result?: unknown;
  report_id?:string|null;
  report_version?:number|null;
  timezone?:string|null;
};
const statusText = {
  queued: "排队中",
  running: "计算中",
  succeeded: "已完成",
  failed: "失败",
  cancelled: "已取消",
};
const errorText = (code?: string) =>
  ({
    query_queue_full: "项目查询队列已满，请稍后重试。",
    query_timeout: "计算超时，请缩小范围后重试。",
    query_requires_narrower_range: "数据超出试验环境预算，请缩小范围。",
    query_result_too_large: "结果超出安全大小，请缩小范围。",
    membership_revoked: "发起人的公司成员权限已失效。",
    query_expired_or_invalid: "查询时间已过期，请重新选择范围。",
    not_found: "任务不存在、已过期或无权访问。",
    report_conflict:"报表已被修改，请刷新报表后重新提交。",
    report_timezone_unrecorded:"该历史报表尚未记录时区，请重新保存后再运行。",
    invalid_report_reference:"报表引用或版本无效，请重新选择报表。",
    privacy_changed:"数据删除范围已变更，旧结果已失效，请重新查询。",
  })[code || ""] || "任务请求失败，请重试。";

export function useQueryJob<T>(
  prefix: string,
  onResult: (result: T, context: {spec:unknown;timezone?:string|null}) => void,
  kinds: string,
) {
  const [id, setId] = useState(""),
    [job, setJob] = useState<Job | null>(null),
    [jobs, setJobs] = useState<Job[]>([]),
    [posting, setPosting] = useState(false),
    [error, setError] = useState("");
  const activePoll=useRef<AbortController|null>(null);
  const callback = useRef(onResult);
  callback.current = onResult;
  const [refresh, setRefresh] = useState(0);
  const version = useRef(0);
  useEffect(
    () => () => {
      version.current++;
    },
    [prefix],
  );
  useEffect(() => {
    const abort = new AbortController();
    api<{ jobs: Job[] }>(prefix + "/query-jobs", { signal: abort.signal })
      .then((r) =>
        setJobs(r.jobs.filter((j) => kinds.split(",").includes(j.kind || ""))),
      )
      .catch((e) => {
        if (!abort.signal.aborted)
          setError(errorText(e instanceof ApiError ? e.code : undefined));
      });
    return () => abort.abort();
  }, [prefix, id, kinds]);
  useEffect(() => {
    if (!id) return;
    const abort = new AbortController();
    activePoll.current=abort;
    let timer: ReturnType<typeof setTimeout>;
    const poll = async () => {
      try {
        const value = await api<Job>(prefix + "/query-jobs/" + id, {
          signal: abort.signal,
        });
        if (abort.signal.aborted) return;
        setJob(value);
        setJobs((rows) =>
          [value, ...rows.filter((r) => r.id !== value.id)].slice(0, 50),
        );
        if (
          value.status === "succeeded" &&
          value.result &&
          kinds.split(",").includes(value.spec?.kind || "")
        )
          callback.current(value.result as T, {spec:value.spec,timezone:value.timezone});
        else if (value.status === "queued" || value.status === "running")
          timer = setTimeout(() => void poll(), 2000);
      } catch (e) {
        if (!abort.signal.aborted)
          setError(errorText(e instanceof ApiError ? e.code : undefined));
      }
    };
    void poll();
    return () => {
      abort.abort();
      clearTimeout(timer);
    };
  }, [prefix, id, kinds, refresh]);
  async function submit(spec: unknown) {
    activePoll.current?.abort();
    setId("");setJob(null);
    setPosting(true);
    setError("");
    const current = ++version.current;
    try {
      const value = await api<{ id: string }>(prefix + "/query-jobs", {
        method: "POST",
        body: JSON.stringify(spec),
      });
      if (current === version.current) {
        setJob(null);
        setId(value.id);
      }
    } catch (e) {
      if (current === version.current)
        setError(errorText(e instanceof ApiError ? e.code : undefined));
    } finally {
      if (current === version.current) setPosting(false);
    }
  }
  async function cancel() {
    const current=version.current;
    try {
      await api(prefix + "/query-jobs/" + id, { method: "DELETE" });
      if(current!==version.current)return;
      activePoll.current?.abort();
      setJob((old) =>
        old ? { ...old, status: "cancelled", result: undefined } : null,
      );
      setId("");
      setJobs((rows) =>
        rows.map((r) => (r.id === id ? { ...r, status: "cancelled" } : r)),
      );
    } catch (e) {
      setError(errorText(e instanceof ApiError ? e.code : undefined));
    }
  }
  const busy =
    posting ||
    (!!id && (!job || job.status === "queued" || job.status === "running"));
  const panel = (
    <section className="panel query-job-panel" aria-label="后台查询任务">
      <div className="panel-heading">
        <div>
          <h2>后台查询</h2>
          <p className="muted">
            任务保留 24 小时，可离开页面后回来查看。计算结果显示生成时间。
          </p>
        </div>
      </div>
      <div className="panel-body">
        {error ? (
          <p className="error" role="alert">
            {error}
          </p>
        ) : null}
        {job ? (
          <p role="status">
            当前任务：{statusText[job.status]}
            {job.status === "failed" ? " · " + errorText(job.error) : ""}
            {job.report_id ? ` · 报表版本 ${job.report_version} · ${job.timezone}` : ''}
          </p>
        ) : null}
        {id && job && (job.status === "queued" || job.status === "running") ? (
          <Button type="button" variant="outline" onClick={() => void cancel()}>
            取消任务
          </Button>
        ) : null}
        {jobs.length ? (
          <div className="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>提交时间</th>
                  <th>状态</th>
                  <th>操作</th>
                </tr>
              </thead>
              <tbody>
                {jobs.slice(0, 10).map((row) => (
                  <tr key={row.id}>
                    <td>{new Date(row.created_at).toLocaleString("zh-CN")}</td>
                    <td>{statusText[row.status]}{row.report_id?<div className="fine-print">报表版本 {row.report_version} · {row.timezone}</div>:null}</td>
                    <td>
                      <Button
                        type="button"
                        variant="ghost"
                        onClick={() => {
                          activePoll.current?.abort();version.current++;setPosting(false);
                          setJob(null);
                          setError("");
                          setId(row.id);
                          setRefresh((v) => v + 1);
                        }}
                      >
                        查看
                      </Button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <p className="empty-inline">
            还没有查询任务。较慢的分析可以选择“后台计算”。
          </p>
        )}
        <p className="muted">
          取消会丢弃结果；已开始的计算可能继续短暂运行，最长受 30 秒预算限制。
        </p>
      </div>
    </section>
  );
  function clear(){activePoll.current?.abort();version.current++;setId("");setJob(null);setPosting(false);setError("");}
  return { submit, busy, panel, clear };
}
